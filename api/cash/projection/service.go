package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

type itemInfo struct {
	ID        string
	Amount    float64
	Recurring bool
	Pattern   string
}

func uploadCashflowProposalService(
	ctx context.Context,
	pgxPool *pgxpool.Pool,
	fh *multipart.FileHeader,
	userEmail, proposalName, recurrenceType, effectiveDate, currency string,
) (string, int, int, error) {
	fileExt := strings.ToLower(filepath.Ext(fh.Filename))
	file, err := fh.Open()
	if err != nil {
		return "", 0, http.StatusBadRequest, fmt.Errorf("Failed to open uploaded file: %s", err.Error())
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return "", 0, http.StatusBadRequest, fmt.Errorf("Invalid or empty file: %s", fh.Filename)
	}

	records, err := parseUploadFile(bytes.NewReader(fileBytes), fileExt)
	if err != nil || len(records) < 2 {
		return "", 0, http.StatusBadRequest, fmt.Errorf("Invalid or empty file: %s", fh.Filename)
	}

	headers := make([]string, len(records[0]))
	for i, h := range records[0] {
		headers[i] = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(h, " ", "_")))
	}

	required := []string{"description", "type", "categoryname", "entity", "department", "expectedamount"}
	for _, req := range required {
		if !contains(headers, req) {
			return "", 0, http.StatusBadRequest, fmt.Errorf("CSV missing required column: %s", req)
		}
	}

	tx, err := pgxPool.Begin(ctx)
	if err != nil {
		return "", 0, http.StatusInternalServerError, errors.New(constants.ErrTxBeginFailed + err.Error())
	}
	committed := false
	s3Key := ""
	s3Uploaded := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			if s3Uploaded && s3Key != "" {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					log.Printf("[CASHFLOW-PROJECTION-UPLOAD] cleanup failed for key=%s: %v", s3Key, cleanupErr)
				}
			}
		}
	}()

	var proposalID string
	err = tx.QueryRow(ctx, `
			INSERT INTO cashflow_proposal (proposal_name, currency_code, effective_date, recurrence_type, status)
			VALUES ($1, $2, $3, $4, 'Active')
			RETURNING proposal_id;
		`, proposalName, currency, effectiveDate, recurrenceType).Scan(&proposalID)
	if err != nil {
		return "", 0, http.StatusUnprocessableEntity, errors.New(parseConstraintError(err))
	}
	log.Printf("Created proposal %s", proposalID)

	if s3storage.IsS3UploadEnabled() {
		hash := sha256.Sum256(fileBytes)
		fileHash := fmt.Sprintf("%x", hash[:])
		folder := s3storage.GetStoragePrefix("projection")
		s3Key = s3storage.BuildS3Key(folder, "cashflow-projection", fileHash, fileExt)
		contentType := s3storage.DetectContentType(fileBytes)
		if uploadErr := s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); uploadErr != nil {
			return "", 0, http.StatusInternalServerError, fmt.Errorf("Failed to upload file to S3: %s", uploadErr.Error())
		}
		s3Uploaded = true
		if _, err := tx.Exec(ctx, `
			UPDATE cashflow_proposal
			SET upload_s3_key = $1,
			    upload_link = NULL
			WHERE proposal_id = $2
		`, s3Key, proposalID); err != nil {
			return "", 0, http.StatusInternalServerError, fmt.Errorf("Failed to update proposal upload key: %s", err.Error())
		}
	}

	dataRows := records[1:]
	itemCols := []string{
		"proposal_id", "description", "cashflow_type", "category_id",
		"expected_amount", "is_recurring", "recurrence_pattern",
		"start_date", "end_date", "entity_name", "department_id",
		"counterparty_name", "recurrence_frequency",
	}

	copyRows := make([][]interface{}, 0, len(dataRows))
	itemInfos := make([]itemInfo, 0, len(dataRows))

	for _, row := range dataRows {
		get := func(col string) string {
			idx := indexOf(headers, col)
			if idx >= 0 && idx < len(row) {
				return strings.TrimSpace(row[idx])
			}
			return ""
		}

		cfType := Capitalize(get("type"))
		if cfType != "Inflow" && cfType != "Outflow" {
			continue
		}
		amount, _ := strconv.ParseFloat(get("expectedamount"), 64)
		recurring := strings.ToLower(get("recurring")) == "true"
		pattern := get("frequency")

		categoryInput := get("categoryname")
		categoryID := lookupCategoryFromContextV1(ctx, categoryInput)

		copyRows = append(copyRows, []interface{}{
			proposalID, get("description"), cfType, categoryID, amount,
			recurring, pattern, effectiveDate, nil,
			get("entity"), get("department"), get("counterparty_name"), pattern,
		})

		itemInfos = append(itemInfos, itemInfo{
			Amount:    amount,
			Recurring: recurring,
			Pattern:   pattern,
		})
	}

	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"cashflow_proposal_item"}, itemCols, pgx.CopyFromRows(copyRows)); err != nil {
		return "", 0, http.StatusUnprocessableEntity, errors.New(parseConstraintError(err))
	}

	rows, err := tx.Query(ctx, `SELECT item_id FROM cashflow_proposal_item WHERE proposal_id=$1 ORDER BY created_at`, proposalID)
	if err != nil {
		return "", 0, http.StatusInternalServerError, fmt.Errorf("Failed to read item IDs: %s", err.Error())
	}
	i := 0
	for rows.Next() && i < len(itemInfos) {
		rows.Scan(&itemInfos[i].ID)
		i++
	}
	rows.Close()

	eff, _ := time.Parse(constants.DateFormat, effectiveDate)
	projCols := []string{"item_id", "year", "month", "projected_amount"}
	year := eff.Year()

	batch := make([][]interface{}, 0, 25000)
	seen := make(map[string]bool, len(itemInfos)*12)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"cashflow_projection_monthly"}, projCols, pgx.CopyFromRows(batch))
		batch = batch[:0]
		return err
	}

	for _, it := range itemInfos {
		pattern := strings.Title(strings.ToLower(strings.TrimSpace(it.Pattern)))
		if !it.Recurring {
			pattern = "Yearly"
		}
		switch pattern {
		case "Monthly":
			monthly := it.Amount / 12.0
			for m := 1; m <= 12; m++ {
				key := fmt.Sprintf(constants.FormatTransactionID, it.ID, year, m)
				if seen[key] {
					continue
				}
				seen[key] = true
				batch = append(batch, []interface{}{it.ID, year, m, monthly})
				if len(batch) >= cap(batch) {
					if err := flushBatch(); err != nil {
						return "", 0, http.StatusInternalServerError, errors.New(constants.ErrFailedToInsertMonthlyProjections + err.Error())
					}
				}
			}
		case "Quarterly":
			perQuarter := it.Amount / 4.0
			for m := 1; m <= 12; m++ {
				amount := 0.0
				if (m-1)%3 == 0 {
					amount = perQuarter
				}
				key := fmt.Sprintf(constants.FormatTransactionID, it.ID, year, m)
				if seen[key] {
					continue
				}
				seen[key] = true
				batch = append(batch, []interface{}{it.ID, year, m, amount})
				if len(batch) >= cap(batch) {
					if err := flushBatch(); err != nil {
						return "", 0, http.StatusInternalServerError, errors.New(constants.ErrFailedToInsertMonthlyProjections + err.Error())
					}
				}
			}
		default:
			for m := 1; m <= 12; m++ {
				amount := 0.0
				if m == 1 {
					amount = it.Amount
				}
				key := fmt.Sprintf(constants.FormatTransactionID, it.ID, year, m)
				if seen[key] {
					continue
				}
				seen[key] = true
				batch = append(batch, []interface{}{it.ID, year, m, amount})
				if len(batch) >= cap(batch) {
					if err := flushBatch(); err != nil {
						return "", 0, http.StatusInternalServerError, errors.New(constants.ErrFailedToInsertMonthlyProjections + err.Error())
					}
				}
			}
		}
	}
	if err := flushBatch(); err != nil {
		return "", 0, http.StatusInternalServerError, fmt.Errorf("Failed to finalize monthly projections: %s", err.Error())
	}

	dedupMap := make(map[string]bool, len(batch))
	finalBatch := make([][]interface{}, 0, len(batch))
	for _, r := range batch {
		key := fmt.Sprintf("%s-%v-%v", r[0], r[1], r[2])
		if _, ok := dedupMap[key]; ok {
			continue
		}
		dedupMap[key] = true
		finalBatch = append(finalBatch, r)
	}

	batch = finalBatch

	if _, err := tx.Exec(ctx, `
			INSERT INTO audit_action_cashflow_proposal
			(proposal_id, action_type, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL','Imported via uploader',$2,now())
		`, proposalID, userEmail); err != nil {
		return "", 0, http.StatusInternalServerError, errors.New(parseConstraintError(err))
	}

	if err := tx.Commit(ctx); err != nil {
		return "", 0, http.StatusInternalServerError, errors.New(constants.ErrCommitFailedCapitalized + err.Error())
	}
	committed = true

	log.Printf("Committed proposal %s (%d items, %d monthly rows)", proposalID, len(itemInfos), len(itemInfos)*12)
	return proposalID, len(copyRows), 0, nil
}

func contains(arr []string, v string) bool {
	for _, s := range arr {
		if s == v {
			return true
		}
	}
	return false
}

func indexOf(arr []string, v string) int {
	for i, s := range arr {
		if s == v {
			return i
		}
	}
	return -1
}

func parseUploadFile(file io.Reader, ext string) ([][]string, error) {
	if ext == ".csv" {
		r := csv.NewReader(file)
		return r.ReadAll()
	}
	if ext == ".xlsx" || ext == ".xls" {
		f, err := excelize.OpenReader(file)
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	return nil, errors.New(constants.ErrUnsupportedFileType)
}

func lookupCategoryFromContextV1(ctx context.Context, categoryName string) string {
	if categoryName == "" {
		return ""
	}

	categories := api.GetCashFlowCategoriesFromCtx(ctx)
	for _, cat := range categories {
		if cat["category_id"] == categoryName {
			return categoryName
		}
		if strings.EqualFold(strings.TrimSpace(cat["category_name"]), strings.TrimSpace(categoryName)) {
			return cat["category_id"]
		}
	}

	return categoryName
}
