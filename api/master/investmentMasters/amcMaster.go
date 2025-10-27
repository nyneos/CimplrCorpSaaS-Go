package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// local helpers (kept local so this file is self-contained)
func getFileExt(filename string) string {
	return strings.ToLower(filepath.Ext(filename))
}

func parseCashFlowCategoryFile(file multipart.File, ext string) ([][]string, error) {
	if ext == ".csv" {
		r := csv.NewReader(file)
		r.FieldsPerRecord = -1
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
	return nil, errors.New("unsupported file type")
}

func normalizeHeader(row []string) []string {
	out := make([]string, len(row))
	for i, h := range row {
		hn := strings.TrimSpace(h)
		hn = strings.ToLower(hn)
		hn = strings.ReplaceAll(hn, " ", "_")
		hn = strings.Trim(hn, "\"'`")
		out[i] = hn
	}
	return out
}

func ifaceToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case []byte:
		return string(t)
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprint(t)
	}
}

func UploadAMCSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// === Step 1: Identify user ===
		userID := r.FormValue("user_id")
		if userID == "" {
			var req struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			userID = req.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "User not found in active sessions")
			return
		}

		// === Step 2: Parse uploaded CSV ===
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse form: "+err.Error())
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded")
			return
		}

		// === Step 3: Define allowed fields for AMC master ===
		allowed := map[string]bool{
			"amc_name":              true,
			"internal_amc_code":     true,
			"primary_contact_name":  true,
			"primary_contact_email": true,
			"sebi_registration_no":  true,
			"amc_beneficiary_name":  true,
			"amc_bank_account_no":   true,
			"amc_bank_name":         true,
			"amc_bank_ifsc":         true,
			"mfu_amc_code":          true,
			"cams_amc_code":         true,
			"erp_vendor_code":       true,
			// "country":               true,
			"status": true,
			// "source":                true,
		}

		batchIDs := []string{}

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file")
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty CSV file")
				return
			}

			headers := normalizeHeader(records[0])
			dataRows := records[1:]

			validCols := []string{}
			for _, h := range headers {
				if allowed[h] {
					validCols = append(validCols, h)
				}
			}

			if !(slices.Contains(validCols, "amc_name") && slices.Contains(validCols, "internal_amc_code")) {
				api.RespondWithError(w, http.StatusBadRequest, "CSV must include amc_name and internal_amc_code")
				return
			}

			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
			}

			copyRows := make([][]interface{}, len(dataRows))
			amcCodes := make([]string, 0, len(dataRows))

			for i, row := range dataRows {
				vals := make([]interface{}, len(validCols))
				for j, c := range validCols {
					if pos, ok := headerPos[c]; ok && pos < len(row) {
						cell := strings.TrimSpace(row[pos])
						if cell == "" {
							vals[j] = nil
						} else {
							vals[j] = cell
						}
					}
				}
				if pos, ok := headerPos["internal_amc_code"]; ok && pos < len(row) {
					code := strings.TrimSpace(row[pos])
					if code != "" {
						amcCodes = append(amcCodes, code)
					}
				}
				copyRows[i] = vals
			}

			// === Step 4: Transaction (COPY + audit insert) ===
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			_, _ = tx.Exec(ctx, "SET LOCAL statement_timeout = '10min'")

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "masteramc"}, validCols, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
				return
			}

			// After COPY success, auto-populate `source`
			if _, err := tx.Exec(ctx, `
    UPDATE investment.masteramc
    SET source = 'Upload'
    WHERE internal_amc_code = ANY($1)
`, amcCodes); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to auto-populate source: "+err.Error())
				return
			}

			if len(amcCodes) > 0 {
				auditSQL := `
					INSERT INTO investment.auditactionamc(amc_id, actiontype, processing_status, reason, requested_by, requested_at)
					SELECT amc_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM investment.masteramc
					WHERE internal_amc_code = ANY($2);
				`
				if _, err := tx.Exec(ctx, auditSQL, userName, amcCodes); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true
			batchIDs = append(batchIDs, uuid.New().String())
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"success":   true,
			"batch_ids": batchIDs,
		})
	}
}

type CreateAMCRequestsingle struct {
	UserID              string `json:"user_id"`
	AmcName             string `json:"amc_name"`
	InternalAmcCode     string `json:"internal_amc_code"`
	Status              string `json:"status,omitempty"`
	PrimaryContactName  string `json:"primary_contact_name,omitempty"`
	PrimaryContactEmail string `json:"primary_contact_email,omitempty"`
	SebiRegistrationNo  string `json:"sebi_registration_no,omitempty"`
	AmcBeneficiaryName  string `json:"amc_beneficiary_name,omitempty"`
	AmcBankAccountNo    string `json:"amc_bank_account_no,omitempty"`
	AmcBankName         string `json:"amc_bank_name,omitempty"`
	AmcBankIfsc         string `json:"amc_bank_ifsc,omitempty"`
	MfuAmcCode          string `json:"mfu_amc_code,omitempty"`
	CamsAmcCode         string `json:"cams_amc_code,omitempty"`
	ErpVendorCode       string `json:"erp_vendor_code,omitempty"`
}

// --- Main handler --- //
func CreateAMCsingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateAMCRequestsingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		// --- Validate required fields ---
		if strings.TrimSpace(req.AmcName) == "" || strings.TrimSpace(req.InternalAmcCode) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "amc_name and internal_amc_code are required")
			return
		}

		// --- Get user email from active sessions ---
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid or inactive user session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction start failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// --- Insert into masteramc (amc_id auto, source='Manual') ---
		insertQuery := `
			INSERT INTO investment.masteramc (
				amc_name, internal_amc_code, status,
				primary_contact_name, primary_contact_email,
				sebi_registration_no, amc_beneficiary_name,
				amc_bank_account_no, amc_bank_name, amc_bank_ifsc,
				mfu_amc_code, cams_amc_code, erp_vendor_code, source
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'Manual')
			RETURNING amc_id
		`

		var amcID string
		err = tx.QueryRow(ctx, insertQuery,
			req.AmcName,
			req.InternalAmcCode,
			defaultIfEmpty(req.Status, "Active"),
			req.PrimaryContactName,
			req.PrimaryContactEmail,
			req.SebiRegistrationNo,
			req.AmcBeneficiaryName,
			req.AmcBankAccountNo,
			req.AmcBankName,
			req.AmcBankIfsc,
			req.MfuAmcCode,
			req.CamsAmcCode,
			req.ErpVendorCode,
		).Scan(&amcID)

		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// --- Insert audit entry ---
		auditQuery := `
			INSERT INTO investment.auditactionamc (
				amc_id, actiontype, processing_status, requested_by, requested_at
			) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`
		if _, err := tx.Exec(ctx, auditQuery, amcID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"success":   true,
			"amc_id":    amcID,
			"amc_name":  req.AmcName,
			"source":    "Manual",
			"requested": userEmail,
			"status":    defaultIfEmpty(req.Status, "Active"),
		})
	}
}

// --- Request types --- //
type CreateAMCRequest struct {
	UserID string     `json:"user_id"`
	Rows   []AMCInput `json:"rows"`
}

type AMCInput struct {
	AmcName             string `json:"amc_name"`
	InternalAmcCode     string `json:"internal_amc_code"`
	Status              string `json:"status,omitempty"`
	PrimaryContactName  string `json:"primary_contact_name,omitempty"`
	PrimaryContactEmail string `json:"primary_contact_email,omitempty"`
	SebiRegistrationNo  string `json:"sebi_registration_no,omitempty"`
	AmcBeneficiaryName  string `json:"amc_beneficiary_name,omitempty"`
	AmcBankAccountNo    string `json:"amc_bank_account_no,omitempty"`
	AmcBankName         string `json:"amc_bank_name,omitempty"`
	AmcBankIfsc         string `json:"amc_bank_ifsc,omitempty"`
	MfuAmcCode          string `json:"mfu_amc_code,omitempty"`
	CamsAmcCode         string `json:"cams_amc_code,omitempty"`
	ErpVendorCode       string `json:"erp_vendor_code,omitempty"`
}

// --- Utility --- //
func defaultIfEmpty(val, def string) string {
	if strings.TrimSpace(val) == "" {
		return def
	}
	return val
}

// --- Main handler --- //
func CreateAMC(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateAMCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		// 🔍 Identify user
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid or inactive user session")
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			name := strings.TrimSpace(row.AmcName)
			code := strings.TrimSpace(row.InternalAmcCode)
			if name == "" || code == "" {
				results = append(results, map[string]interface{}{
					"success": false,
					"error":   "Missing amc_name or internal_amc_code",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "error": "TX failed: " + err.Error(),
				})
				continue
			}
			rollback := func() { _ = tx.Rollback(ctx) }

			q := `
				INSERT INTO investment.masteramc (
					amc_name, internal_amc_code, status,
					primary_contact_name, primary_contact_email,
					sebi_registration_no, amc_beneficiary_name,
					amc_bank_account_no, amc_bank_name, amc_bank_ifsc,
					mfu_amc_code, cams_amc_code, erp_vendor_code, source
				)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'Manual')
				RETURNING amc_id
			`

			var amcID string
			err = tx.QueryRow(ctx, q,
				name,
				code,
				defaultIfEmpty(row.Status, "Active"),
				row.PrimaryContactName,
				row.PrimaryContactEmail,
				row.SebiRegistrationNo,
				row.AmcBeneficiaryName,
				row.AmcBankAccountNo,
				row.AmcBankName,
				row.AmcBankIfsc,
				row.MfuAmcCode,
				row.CamsAmcCode,
				row.ErpVendorCode,
			).Scan(&amcID)

			if err != nil {
				rollback()
				results = append(results, map[string]interface{}{
					"success": false,
					"error":   fmt.Sprintf("Insert failed for %s: %v", name, err),
				})
				continue
			}

			audit := `
				INSERT INTO investment.auditactionamc (
					amc_id, actiontype, processing_status, requested_by, requested_at
				)
				VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`
			if _, err := tx.Exec(ctx, audit, amcID, userEmail); err != nil {
				rollback()
				results = append(results, map[string]interface{}{
					"success": false,
					"error":   "Audit insert failed: " + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					"success": false,
					"error":   "Commit failed: " + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":   true,
				"amc_id":    amcID,
				"amc_name":  name,
				"source":    "Manual",
				"requested": userEmail,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func UpdateAMCBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				AmcID  string                 `json:"amc_id"`
				Fields map[string]interface{} `json:"fields"`
				Reason string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid user session")
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.AmcID == "" {
				results = append(results, map[string]interface{}{
					"success": false, "error": "Missing amc_id",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "error": "Begin TX failed: " + err.Error(),
				})
				continue
			}

			func() {
				defer tx.Rollback(ctx)

				// Fetch existing row to get "old" values
				sel := `
					SELECT amc_name, internal_amc_code, status,
						primary_contact_name, primary_contact_email,
						sebi_registration_no, amc_beneficiary_name,
						amc_bank_account_no, amc_bank_name, amc_bank_ifsc,
						mfu_amc_code, cams_amc_code, erp_vendor_code,
						source
					FROM investment.masteramc
					WHERE amc_id=$1
					FOR UPDATE`
				var oldVals [14]interface{}
				if err := tx.QueryRow(ctx, sel, row.AmcID).Scan(
					&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4],
					&oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
					&oldVals[9], &oldVals[10], &oldVals[11], &oldVals[12], &oldVals[13],
				); err != nil {

					results = append(results, map[string]interface{}{
						"success": false, "amc_id": row.AmcID, "error": "Fetch failed: " + err.Error(),
					})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1

				//  Map field -> (current, old)
				fieldPairs := map[string]int{
					"amc_name":              0,
					"internal_amc_code":     1,
					"status":                2,
					"primary_contact_name":  3,
					"primary_contact_email": 4,
					"sebi_registration_no":  5,
					"amc_beneficiary_name":  6,
					"amc_bank_account_no":   7,
					"amc_bank_name":         8,
					"amc_bank_ifsc":         9,
					"mfu_amc_code":          10,
					"cams_amc_code":         11,
					"erp_vendor_code":       12,
				}

				for k, v := range row.Fields {
					k = strings.ToLower(k)
					if idx, ok := fieldPairs[k]; ok {
						oldField := "old_" + k
						sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", k, pos, oldField, pos+1))
						args = append(args, v, oldVals[idx])
						pos += 2
					}
				}

				if len(sets) == 0 {
					results = append(results, map[string]interface{}{
						"success": false, "amc_id": row.AmcID, "error": "No updatable fields found",
					})
					return
				}

				q := fmt.Sprintf("UPDATE investment.masteramc SET %s WHERE amc_id=$%d",
					strings.Join(sets, ", "), pos)
				args = append(args, row.AmcID)

				if _, err := tx.Exec(ctx, q, args...); err != nil {
					results = append(results, map[string]interface{}{
						"success": false, "amc_id": row.AmcID, "error": "Update failed: " + err.Error(),
					})
					return
				}

				// Insert audit record
				audit := `
					INSERT INTO investment.auditactionamc
						(amc_id, actiontype, processing_status, reason, requested_by, requested_at)
					VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())`
				if _, err := tx.Exec(ctx, audit, row.AmcID, row.Reason, userEmail); err != nil {
					results = append(results, map[string]interface{}{
						"success": false, "amc_id": row.AmcID, "error": "Audit insert failed: " + err.Error(),
					})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{
						"success": false, "amc_id": row.AmcID, "error": "Commit failed: " + err.Error(),
					})
					return
				}

				results = append(results, map[string]interface{}{
					"success": true, "amc_id": row.AmcID,
				})
			}()
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

type UpdateAMCRequest struct {
	UserID string                 `json:"user_id"`
	AmcID  string                 `json:"amc_id"`
	Fields map[string]interface{} `json:"fields"`
	Reason string                 `json:"reason"`
}

func UpdateAMC(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateAMCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if req.AmcID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "amc_id is required")
			return
		}

		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No fields provided for update")
			return
		}

		// --- Identify user ---
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid or inactive user session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction start failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// --- Fetch existing row for "old" values ---
		sel := `
			SELECT amc_name, internal_amc_code, status,
				primary_contact_name, primary_contact_email,
				sebi_registration_no, amc_beneficiary_name,
				amc_bank_account_no, amc_bank_name, amc_bank_ifsc,
				mfu_amc_code, cams_amc_code, erp_vendor_code, source
			FROM investment.masteramc
			WHERE amc_id=$1
			FOR UPDATE`
		var oldVals [14]interface{}
		if err := tx.QueryRow(ctx, sel, req.AmcID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4],
			&oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
			&oldVals[9], &oldVals[10], &oldVals[11], &oldVals[12], &oldVals[13],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch failed: "+err.Error())
			return
		}

		// --- Define field mapping ---
		fieldPairs := map[string]int{
			"amc_name":              0,
			"internal_amc_code":     1,
			"status":                2,
			"primary_contact_name":  3,
			"primary_contact_email": 4,
			"sebi_registration_no":  5,
			"amc_beneficiary_name":  6,
			"amc_bank_account_no":   7,
			"amc_bank_name":         8,
			"amc_bank_ifsc":         9,
			"mfu_amc_code":          10,
			"cams_amc_code":         11,
			"erp_vendor_code":       12,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if idx, ok := fieldPairs[k]; ok {
				oldField := "old_" + k
				sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", k, pos, oldField, pos+1))
				args = append(args, v, oldVals[idx])
				pos += 2
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}

		// --- Update masteramc ---
		q := fmt.Sprintf("UPDATE investment.masteramc SET %s WHERE amc_id=$%d",
			strings.Join(sets, ", "), pos)
		args = append(args, req.AmcID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Update failed: "+err.Error())
			return
		}

		// --- Insert audit record ---
		audit := `
			INSERT INTO investment.auditactionamc
				(amc_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())`
		if _, err := tx.Exec(ctx, audit, req.AmcID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"success":   true,
			"amc_id":    req.AmcID,
			"requested": userEmail,
			"reason":    req.Reason,
		})
	}
}

func DeleteAMC(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			AmcIDs []string `json:"amc_ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		if len(req.AmcIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "amc_ids required")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.AmcIDs {
			q := `
				INSERT INTO investment.auditactionamc(amc_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, q, id, req.Reason, requestedBy); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"deleted_requested": req.AmcIDs})
	}
}

func BulkRejectAMCActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			AmcIDs  []string `json:"amc_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid user session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (amc_id) action_id, amc_id, processing_status
			FROM investment.auditactionamc
			WHERE amc_id = ANY($1)
			ORDER BY amc_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.AmcIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		for rows.Next() {
			var aid, cid, ps string
			_ = rows.Scan(&aid, &cid, &ps)
			if strings.ToUpper(ps) != "APPROVED" {
				actionIDs = append(actionIDs, aid)
			}
		}

		if len(actionIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rejectable AMC actions found")
			return
		}

		upd := `
			UPDATE investment.auditactionamc
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)`
		if _, err := tx.Exec(ctx, upd, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Update failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

func BulkApproveAMCActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			AmcIDs  []string `json:"amc_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		// 🔍 Identify the checker
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid user session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (amc_id) 
				action_id, amc_id, actiontype, processing_status
			FROM investment.auditactionamc
			WHERE amc_id = ANY($1)
			ORDER BY amc_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.AmcIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		var deleteIDs []string = []string{}
		var actionIDs []string = []string{}
		var markDeletedActionIDs []string

		for rows.Next() {
			var actionID, amcID, actionType, pStatus string
			if err := rows.Scan(&actionID, &amcID, &actionType, &pStatus); err != nil {
				continue
			}

			statusUpper := strings.ToUpper(pStatus)
			// actionUpper := strings.ToUpper(actionType)

			switch statusUpper {
			case "PENDING_DELETE_APPROVAL":
				//  Mark as DELETED in audit + is_deleted in master
				markDeletedActionIDs = append(markDeletedActionIDs, actionID)
				deleteIDs = append(deleteIDs, amcID)
			case "PENDING_APPROVAL", "PENDING_EDIT_APPROVAL":
				// Normal approve → set APPROVED
				actionIDs = append(actionIDs, actionID)
			default:
				// Already approved/rejected/cancelled/deleted → skip
				continue
			}
		}

		if len(actionIDs) == 0 && len(markDeletedActionIDs) == 0 {
			api.RespondWithPayload(w, false, "No approvable actions found", map[string]any{
				"approved_action_ids": []string{},
				"deleted_amcs":        []string{},
			})
			return
		}
		if len(actionIDs) > 0 {
			upd := `
				UPDATE investment.auditactionamc
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)`
			if _, err := tx.Exec(ctx, upd, checkerBy, req.Comment, actionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Approve update failed: "+err.Error())
				return
			}
		}

		if len(markDeletedActionIDs) > 0 {
			updDel := `
				UPDATE investment.auditactionamc
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)`
			if _, err := tx.Exec(ctx, updDel, checkerBy, req.Comment, markDeletedActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Delete approve update failed: "+err.Error())
				return
			}

			del := `
				UPDATE investment.masteramc 
				SET is_deleted=true, status='Inactive'
				WHERE amc_id = ANY($1)`
			if _, err := tx.Exec(ctx, del, deleteIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Master soft delete failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_action_ids": actionIDs,
			"deleted_amcs": deleteIDs,
		})
	}
}

func GetApprovedActiveAMCs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest AS (
				SELECT DISTINCT ON (amc_id) amc_id, processing_status
				FROM investment.auditactionamc
				ORDER BY amc_id, requested_at DESC
			)
			SELECT m.amc_id, m.amc_name, m.internal_amc_code
			FROM investment.masteramc m
			JOIN latest l ON l.amc_id = m.amc_id
			WHERE UPPER(l.processing_status)='APPROVED'
			  AND UPPER(m.status)='ACTIVE'
			  AND COALESCE(m.is_deleted,false)=false
			ORDER BY m.amc_name;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var id, name, code string
			_ = rows.Scan(&id, &name, &code)
			out = append(out, map[string]interface{}{
				"amc_id":            id,
				"amc_name":          name,
				"internal_amc_code": code,
			})
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

func GetAMCsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.amc_id)
					a.amc_id,
					a.processing_status,
					a.actiontype,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactionamc a
				ORDER BY a.amc_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					amc_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionamc
				GROUP BY amc_id
			)
			SELECT
				m.amc_id,
				COALESCE(m.amc_name,'') AS amc_name,
				COALESCE(m.old_amc_name,'') AS old_amc_name,
				COALESCE(m.internal_amc_code,'') AS internal_amc_code,
				COALESCE(m.old_internal_amc_code,'') AS old_internal_amc_code,
				COALESCE(m.status,'') AS status,
				COALESCE(m.old_status,'') AS old_status,
				COALESCE(m.primary_contact_name,'') AS primary_contact_name,
				COALESCE(m.old_primary_contact_name,'') AS old_primary_contact_name,
				COALESCE(m.primary_contact_email,'') AS primary_contact_email,
				COALESCE(m.old_primary_contact_email,'') AS old_primary_contact_email,
				COALESCE(m.sebi_registration_no,'') AS sebi_registration_no,
				COALESCE(m.old_sebi_registration_no,'') AS old_sebi_registration_no,
				COALESCE(m.amc_beneficiary_name,'') AS amc_beneficiary_name,
				COALESCE(m.old_amc_beneficiary_name,'') AS old_amc_beneficiary_name,
				COALESCE(m.amc_bank_account_no,'') AS amc_bank_account_no,
				COALESCE(m.old_amc_bank_account_no,'') AS old_amc_bank_account_no,
				COALESCE(m.amc_bank_name,'') AS amc_bank_name,
				COALESCE(m.old_amc_bank_name,'') AS old_amc_bank_name,
				COALESCE(m.amc_bank_ifsc,'') AS amc_bank_ifsc,
				COALESCE(m.old_amc_bank_ifsc,'') AS old_amc_bank_ifsc,
				COALESCE(m.mfu_amc_code,'') AS mfu_amc_code,
				COALESCE(m.old_mfu_amc_code,'') AS old_mfu_amc_code,
				COALESCE(m.cams_amc_code,'') AS cams_amc_code,
				COALESCE(m.old_cams_amc_code,'') AS old_cams_amc_code,
				COALESCE(m.erp_vendor_code,'') AS erp_vendor_code,
				COALESCE(m.old_erp_vendor_code,'') AS old_erp_vendor_code,
				COALESCE(m.source,'') AS source,
				COALESCE(m.old_source,'') AS old_source,
				COALESCE(m.is_deleted,false) AS is_deleted,

				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,

				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at

			FROM investment.masteramc m
			LEFT JOIN latest_audit l ON l.amc_id = m.amc_id
			LEFT JOIN history h ON h.amc_id = m.amc_id
			WHERE COALESCE(m.is_deleted,false)=false
			ORDER BY m.amc_name;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)

		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			out = append(out, row)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row scan error: "+rows.Err().Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"rows": out,
		})
	}
}
