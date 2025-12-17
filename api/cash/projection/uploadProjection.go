package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

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

func UploadCashflowProposalSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("[UploadCashflowProposalSimple] Start %s %s", r.Method, r.URL.Path)
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[UploadCashflowProposalSimple] Panic recovered: %v", rec)
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrInternalServer)
			}
			log.Printf("[UploadCashflowProposalSimple] Finished in %s", time.Since(start))
		}()

		ctx := r.Context()

		// Parse multipart form
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}

		userID := r.FormValue(constants.KeyUserID)
		proposalName := strings.TrimSpace(r.FormValue("proposal_name"))
		recurrenceType := strings.TrimSpace(r.FormValue("proposal_type"))
		effectiveDate := strings.TrimSpace(r.FormValue("effective_date"))
		currency := strings.TrimSpace(r.FormValue("currency"))

		if userID == "" || proposalName == "" || currency == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id, proposal_name and currency are required")
			return
		}
		if effectiveDate == "" {
			effectiveDate = time.Now().Format(constants.DateFormat)
		}

		// Validate session or fallback
		userEmail := "admin@example.com"
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Email
				break
			}
		}

		// Get uploaded file
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded")
			return
		}

		fh := files[0]
		fileExt := strings.ToLower(filepath.Ext(fh.Filename))
		file, err := fh.Open()
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to open uploaded file: "+err.Error())
			return
		}
		defer file.Close()

		records, err := parseUploadFile(file, fileExt)
		if err != nil || len(records) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
			return
		}

		headers := make([]string, len(records[0]))
		for i, h := range records[0] {
			headers[i] = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(h, " ", "_")))
		}

		required := []string{"description", "type", "categoryname", "entity", "department", "expectedamount"}
		for _, req := range required {
			if !contains(headers, req) {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("CSV missing required column: %s", req))
				return
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Insert proposal
		var proposalID string
		err = tx.QueryRow(ctx, `
			INSERT INTO cashflow_proposal (proposal_name, currency_code, effective_date, recurrence_type, status)
			VALUES ($1, $2, $3, $4, 'Active')
			RETURNING proposal_id;
		`, proposalName, currency, effectiveDate, recurrenceType).Scan(&proposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert proposal: "+err.Error())
			return
		}
		log.Printf("Created proposal %s", proposalID)

		// Build item rows + metadata for projections
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

			copyRows = append(copyRows, []interface{}{
				proposalID, get("description"), cfType, get("categoryname"), amount,
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
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert items: "+err.Error())
			return
		}

		// Fetch item IDs (same order)
		rows, err := tx.Query(ctx, `SELECT item_id FROM cashflow_proposal_item WHERE proposal_id=$1 ORDER BY created_at`, proposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to read item IDs: "+err.Error())
			return
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
		// dedupe map to ensure uniqueness across entire run: item-year-month
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
							api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToInsertMonthlyProjections+err.Error())
							return
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
							api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToInsertMonthlyProjections+err.Error())
							return
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
							api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToInsertMonthlyProjections+err.Error())
							return
						}
					}
				}
			}
		}
		if err := flushBatch(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to finalize monthly projections: "+err.Error())
			return
		}

		// Deduplicate projections by item-year-month to avoid pkey conflicts
		dedupMap := make(map[string]bool, len(batch))
		finalBatch := make([][]interface{}, 0, len(batch))
		for _, r := range batch {
			// r = {item_id, year, month, amount}
			key := fmt.Sprintf("%s-%v-%v", r[0], r[1], r[2])
			if _, ok := dedupMap[key]; ok {
				continue
			}
			dedupMap[key] = true
			finalBatch = append(finalBatch, r)
		}

		// replace batch with finalBatch for the final CopyFrom
		batch = finalBatch

		// Insert audit record
		if _, err := tx.Exec(ctx, `
			INSERT INTO audit_action_cashflow_proposal
			(proposal_id, action_type, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL','Imported via uploader',$2,now())
		`, proposalID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit record: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		resp := map[string]interface{}{
			constants.ValueSuccess: true,
			"proposal_id":          proposalID,
			"imported_rows":        len(copyRows),
			"message":              "Proposal, items, projections & audit committed successfully",
		}
		json.NewEncoder(w).Encode(resp)
		log.Printf("Committed proposal %s (%d items, %d monthly rows)", proposalID, len(itemInfos), len(itemInfos)*12)
	}
}

// Helpers
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

func parseUploadFile(file multipart.File, ext string) ([][]string, error) {
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
