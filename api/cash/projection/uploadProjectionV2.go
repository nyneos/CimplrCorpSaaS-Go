package projection

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

type itemInfoV2 struct {
	ID        string
	Amount    float64
	Recurring bool
	Pattern   string
}

// UploadCashflowProposalV2 handles CSV/XLSX upload for V2 schema (simplified UX like V1)
// Form fields:
// - user_id (required)
// - proposal_name (required)
// - base_currency_code (required, 3-char code like USD, EUR)
// - effective_date (optional, defaults to today)
//
// CSV/XLSX columns (V2 schema):
// - description (required)
// - type (required: Inflow/Outflow)
// - categoryname (required: category_id)
// - entity (required)
// - department (optional)
// - expectedamount (required)
// - recurring (optional: true/false, defaults to false)
// - frequency (optional: Monthly/Quarterly/Yearly, defaults to Yearly)
// - maturity_date (optional: YYYY-MM-DD)
// - bank_name (optional)
// - bank_account_number (optional)
// - counterparty_name (optional)
// - currency_code (optional: per-item currency, defaults to base_currency_code)
//
// Monthly projections are AUTO-CALCULATED based on recurring + frequency
func UploadCashflowProposalV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("[UploadCashflowProposalV2] Start %s %s", r.Method, r.URL.Path)
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[UploadCashflowProposalV2] Panic recovered: %v", rec)
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrInternalServer)
			}
			log.Printf("[UploadCashflowProposalV2] Finished in %s", time.Since(start))
		}()

		ctx := r.Context()

		// Parse multipart form
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}

		// Get form values for proposal metadata
		userID := r.FormValue(constants.KeyUserID)
		proposalName := strings.TrimSpace(r.FormValue("proposal_name"))
		baseCurrencyCode := strings.TrimSpace(r.FormValue("base_currency_code"))
		effectiveDate := strings.TrimSpace(r.FormValue("effective_date"))

		if userID == "" || proposalName == "" || baseCurrencyCode == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id, proposal_name and base_currency_code are required")
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

		records, err := parseUploadFileV2(file, fileExt)
		if err != nil || len(records) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
			return
		}

		headers := make([]string, len(records[0]))
		for i, h := range records[0] {
			headers[i] = strings.ToLower(strings.TrimSpace(strings.ReplaceAll(h, " ", "_")))
		}

		required := []string{"description", "type", "categoryname", "entity", "expectedamount"}
		for _, req := range required {
			if !containsCol(headers, req) {
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

		// Insert proposal (V2 schema: base_currency_code, effective_date)
		var proposalID string
		err = tx.QueryRow(ctx, `
			INSERT INTO cimplrcorpsaas.cashflow_proposal (proposal_name, base_currency_code, effective_date)
			VALUES ($1, $2, $3)
			RETURNING proposal_id;
		`, proposalName, baseCurrencyCode, effectiveDate).Scan(&proposalID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert proposal: "+err.Error())
			return
		}
		log.Printf("Created V2 proposal %s", proposalID)

		// Build item rows + metadata for projections
		dataRows := records[1:]
		itemCols := []string{
			"proposal_id", "description", "cashflow_type", "category_id",
			"expected_amount", "is_recurring", "recurrence_frequency",
			"maturity_date", "entity_name", "department_id",
			"counterparty_name", "currency_code", "bank_name", "bank_account_number",
		}

		copyRows := make([][]interface{}, 0, len(dataRows))
		itemInfos := make([]itemInfoV2, 0, len(dataRows))

		for _, row := range dataRows {
			get := func(col string) string {
				idx := indexOfCol(headers, col)
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
			frequency := get("frequency")
			if frequency == "" {
				frequency = "Yearly" // default
			}

			// Per-item currency (defaults to base_currency_code)
			itemCurrency := get("currency_code")
			if itemCurrency == "" {
				itemCurrency = baseCurrencyCode
			}

			// Optional V2 fields
			maturityDate := get("maturity_date")
			bankName := get("bank_name")
			bankAccountNumber := get("bank_account_number")

			var maturityDateVal interface{}
			if maturityDate != "" {
				maturityDateVal = maturityDate
			}

			copyRows = append(copyRows, []interface{}{
				proposalID, get("description"), cfType, get("categoryname"), amount,
				recurring, frequency, maturityDateVal,
				get("entity"), get("department"), get("counterparty_name"), itemCurrency,
				nullStringV2(bankName), nullStringV2(bankAccountNumber),
			})

			itemInfos = append(itemInfos, itemInfoV2{
				Amount:    amount,
				Recurring: recurring,
				Pattern:   frequency,
			})
		}

		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"cimplrcorpsaas", "cashflow_proposal_item"}, itemCols, pgx.CopyFromRows(copyRows)); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert items: "+err.Error())
			return
		}

		// Fetch item IDs (same order)
		rows, err := tx.Query(ctx, `SELECT item_id FROM cimplrcorpsaas.cashflow_proposal_item WHERE proposal_id=$1 ORDER BY created_at`, proposalID)
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

		// Generate monthly projections based on recurring + frequency (SAME AS V1)
		eff, _ := time.Parse(constants.DateFormat, effectiveDate)
		projCols := []string{"item_id", "year", "month", "projected_amount"}
		year := eff.Year()

		batch := make([][]interface{}, 0, 25000)
		seen := make(map[string]bool, len(itemInfos)*12)

		flushBatch := func() error {
			if len(batch) == 0 {
				return nil
			}
			_, err := tx.CopyFrom(ctx, pgx.Identifier{"cimplrcorpsaas", "cashflow_projection_monthly"}, projCols, pgx.CopyFromRows(batch))
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
			default: // Yearly
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

		// Insert audit record
		if _, err := tx.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.audit_action_cashflow_proposal
			(proposal_id, action_type, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL','Imported via V2 uploader',$2,now())
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
			"message":              "V2 Proposal, items, projections & audit committed successfully",
		}
		json.NewEncoder(w).Encode(resp)
		log.Printf("Committed V2 proposal %s (%d items, %d monthly rows)", proposalID, len(itemInfos), len(itemInfos)*12)
	}
}

// Helpers for V2
func containsCol(arr []string, v string) bool {
	for _, s := range arr {
		if s == v {
			return true
		}
	}
	return false
}

func indexOfCol(arr []string, v string) int {
	for i, s := range arr {
		if s == v {
			return i
		}
	}
	return -1
}

func parseUploadFileV2(file multipart.File, ext string) ([][]string, error) {
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

func nullStringV2(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
