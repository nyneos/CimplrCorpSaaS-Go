package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlySchemeError converts database errors into user-friendly messages
// Returns (user-friendly message, HTTP status code)
func getUserFriendlySchemeError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errMsg := err.Error()

	// Unique constraint violations (HTTP 200 - user errors)
	if strings.Contains(errMsg, "unique_scheme_isin_not_deleted") ||
		strings.Contains(errMsg, "ISIN already exists") {
		return "Scheme with this ISIN already exists. Please use a different ISIN.", http.StatusOK
	}
	if strings.Contains(errMsg, "unique_scheme_name_not_deleted") ||
		strings.Contains(errMsg, "scheme_name") && strings.Contains(errMsg, constants.ErrAlreadyExists) {
		return "Scheme name already exists. Please use a different name.", http.StatusOK
	}
	if strings.Contains(errMsg, "unique_internal_scheme_code_not_deleted") ||
		strings.Contains(errMsg, "internal_scheme_code") && strings.Contains(errMsg, constants.ErrAlreadyExists) {
		return "Internal scheme code already exists. Please use a different code.", http.StatusOK
	}
	if strings.Contains(errMsg, "unique_amfi_scheme_code_not_deleted") ||
		strings.Contains(errMsg, "amfi_scheme_code") && strings.Contains(errMsg, constants.ErrAlreadyExists) {
		return "AMFI scheme code already exists. Please use a different code.", http.StatusOK
	}
	if strings.Contains(errMsg, constants.ErrDuplicateKey) {
		return "This scheme already exists in the system.", http.StatusOK
	}

	// Foreign key violations (HTTP 200 - user errors)
	if strings.Contains(errMsg, "foreign key") || strings.Contains(errMsg, "fk_") {
		if strings.Contains(errMsg, "amc") {
			return "Cannot perform this operation. AMC reference is invalid or has been deleted.", http.StatusOK
		}
		if strings.Contains(errMsg, "scheme") {
			return "Cannot perform this operation. Scheme is referenced by other records (folios, transactions).", http.StatusOK
		}
		return "Cannot perform this operation. Referenced data is invalid or missing.", http.StatusOK
	}

	// Check constraint violations (HTTP 200 - user errors)
	if strings.Contains(errMsg, "masterscheme_status_ck") ||
		strings.Contains(errMsg, "invalid input value") && strings.Contains(errMsg, "status") {
		return "Invalid status. Must be 'Active' or 'Inactive'.", http.StatusOK
	}
	if strings.Contains(errMsg, "masterscheme_source_ck") ||
		strings.Contains(errMsg, "invalid input value") && strings.Contains(errMsg, "source") {
		return "Invalid source. Must be 'AMFI', 'Manual', or 'Upload'.", http.StatusOK
	}
	if strings.Contains(errMsg, "actiontype_check") {
		return "Invalid action type. Must be CREATE, EDIT, or DELETE.", http.StatusOK
	}
	if strings.Contains(errMsg, "processing_status_check") {
		return "Invalid processing status.", http.StatusOK
	}

	// Not-null constraint violations (HTTP 200 - user errors)
	if strings.Contains(errMsg, "not-null") || strings.Contains(errMsg, "null value") {
		if strings.Contains(errMsg, "scheme_name") {
			return "Scheme name is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "amc_name") {
			return "AMC name is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "internal_scheme_code") {
			return "Internal scheme code is required.", http.StatusOK
		}
		return "Required field is missing.", http.StatusOK
	}

	// Connection/timeout errors (HTTP 503 - server errors, retry-able)
	if strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "no connection") {
		return "Database connection error. Please try again.", http.StatusServiceUnavailable
	}

	// Unknown errors (HTTP 500 - server errors)
	return context + ": " + errMsg, http.StatusInternalServerError
}

// ---------------------------
// Types
// ---------------------------

type CreateSchemeRequestSingle struct {
	UserID             string `json:"user_id"`
	SchemeName         string `json:"scheme_name"`
	ISIN               string `json:"isin"`
	AmcName            string `json:"amc_name"`
	InternalSchemeCode string `json:"internal_scheme_code"`
	InternalRiskRating string `json:"internal_risk_rating"`
	ErpGlAccount       string `json:"erp_gl_account"`
	AmfiSchemeCode     string `json:"amfi_scheme_code"`
	Status             string `json:"status,omitempty"`
	Source             string `json:"source,omitempty"` // ignored, we set Manual
	Method             string `json:"method,omitempty"`
}

type UpdateSchemeRequest struct {
	UserID   string                 `json:"user_id"`
	SchemeID string                 `json:"scheme_id"`
	Fields   map[string]interface{} `json:"fields"`
	Reason   string                 `json:"reason"`
}

type UploadSchemeResult struct {
	Success bool   `json:"success"`
	BatchID string `json:"batch_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ---------------------------
// Index creation helper (run once)
// ---------------------------

// ---------------------------
// Util: check AMC exists and is approved
// ---------------------------
func isAMCApproved(ctx context.Context, pgxPool *pgxpool.Pool, amcName string) (bool, error) {
	// We need to ensure there is an AMC with given name, not deleted, and its latest audit is APPROVED.
	q := `
		WITH latest AS (
			SELECT DISTINCT ON (amc_id) amc_id, processing_status
			FROM investment.auditactionamc
			ORDER BY amc_id, GREATEST(COALESCE(requested_at, '1970-01-01'::timestamp), COALESCE(checker_at, '1970-01-01'::timestamp)) DESC
		)
		SELECT 1
		FROM investment.masteramc m
		JOIN latest l ON l.amc_id = m.amc_id
		WHERE m.amc_name = $1
		  AND COALESCE(m.is_deleted,false) = false
		  AND UPPER(l.processing_status) = 'APPROVED'
		LIMIT 1
	`
	var dummy int
	err := pgxPool.QueryRow(ctx, q, amcName).Scan(&dummy)
	if err != nil {
		// nil row => not approved / not found
		return false, nil
	}
	return true, nil
}

// ---------------------------
// UploadSchemeSimple (bulk CSV/XLSX -> COPY -> audit)
// ---------------------------
func UploadSchemeSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// identify user
		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			var tmp struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&tmp)
			userID = tmp.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
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
			api.RespondWithError(w, http.StatusUnauthorized, "user not in active sessions")
			return
		}

		// parse multipart
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrFailedToParseForm)
			api.RespondWithError(w, status, msg)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded")
			return
		}

		// allowed columns for masterscheme
		allowed := map[string]bool{
			"scheme_name":          true,
			"isin":                 true,
			"amc_name":             true,
			"internal_scheme_code": true,
			"internal_risk_rating": true,
			"erp_gl_account":       true,
			"amfi_scheme_code":     true,
			constants.KeyStatus:    true,
			"source":               true,
		}

		results := make([]UploadSchemeResult, 0, len(files))

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToOpenFile)
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidOrEmptyFile)
				return
			}

			headers := normalizeHeader(records[0])
			dataRows := records[1:]

			// determine validCols and positions
			validCols := []string{}
			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
				if allowed[h] {
					validCols = append(validCols, h)
				}
			}

			// require mandatory columns
			mandatories := []string{"scheme_name", "isin", "amc_name", "internal_scheme_code", "internal_risk_rating", "erp_gl_account"}
			for _, m := range mandatories {
				if !slices.Contains(validCols, m) {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("CSV must include column: %s", m))
					return
				}
			}

			// collect referenced AMC names to validate they exist & are approved
			amcSet := map[string]bool{}
			for _, row := range dataRows {
				pos := headerPos["amc_name"]
				if pos < len(row) {
					name := strings.TrimSpace(row[pos])
					if name != "" {
						amcSet[name] = true
					}
				}
			}
			amcNames := make([]string, 0, len(amcSet))
			for k := range amcSet {
				amcNames = append(amcNames, k)
			}
			// Validate AMC(s) from context - for upload we require they are approved and active
			approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
			for _, an := range amcNames {
				found := false
				for _, amc := range approvedAMCs {
					if strings.EqualFold(amc["amc_name"], an) {
						found = true
						break
					}
				}
				if !found {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("AMC not found or not approved/active: %s", an))
					return
				}
			}

			// build copy rows aligned to validCols
			copyRows := make([][]interface{}, len(dataRows))
			isinList := make([]string, 0, len(dataRows))
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
					} else {
						vals[j] = nil
					}
				}
				// capture isin for audit commit step
				if pos, ok := headerPos["isin"]; ok && pos < len(row) {
					code := strings.TrimSpace(row[pos])
					if code != "" {
						isinList = append(isinList, code)
					}
				}
				copyRows[i] = vals
			}

			// transaction: COPY -> set source -> audit insert
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				msg, status := getUserFriendlySchemeError(err, constants.ErrTxBeginFailed)
				api.RespondWithError(w, status, msg)
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			_, _ = tx.Exec(ctx, "SET LOCAL statement_timeout = '10min'")

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "masterscheme"}, validCols, pgx.CopyFromRows(copyRows)); err != nil {
				msg, status := getUserFriendlySchemeError(err, "COPY failed")
				api.RespondWithError(w, status, msg)
				return
			}

			// auto-populate source to 'Upload' for newly inserted rows by internal_scheme_code or isin
			if len(isinList) > 0 {
				if _, err := tx.Exec(ctx, `
					UPDATE investment.masterscheme
					SET source = 'Upload'
					WHERE isin = ANY($1)
				`, isinList); err != nil {
					msg, status := getUserFriendlySchemeError(err, "Failed to set source")
					api.RespondWithError(w, status, msg)
					return
				}
			}

			// insert audit rows for created schemes using isin -> scheme_id mapping
			if len(isinList) > 0 {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactionscheme(scheme_id, actiontype, processing_status, reason, requested_by, requested_at)
					SELECT scheme_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM investment.masterscheme
					WHERE isin = ANY($2)
				`, userName, isinList)
				if err != nil {
					msg, status := getUserFriendlySchemeError(err, constants.ErrAuditInsertFailed)
					api.RespondWithError(w, status, msg)
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				msg, status := getUserFriendlySchemeError(err, constants.ErrCommitFailedCapitalized)
				api.RespondWithError(w, status, msg)
				return
			}
			committed = true

			results = append(results, UploadSchemeResult{Success: true, BatchID: uuid.New().String()})
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

// ---------------------------
// CreateSchemeSingle (single create, source='Manual')
// ---------------------------
func CreateSchemeSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateSchemeRequestSingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		// Validate required
		if strings.TrimSpace(req.SchemeName) == "" ||
			strings.TrimSpace(req.AmcName) == "" ||
			strings.TrimSpace(req.InternalSchemeCode) == "" ||
			strings.TrimSpace(req.InternalRiskRating) == "" ||
			strings.TrimSpace(req.ErpGlAccount) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required scheme fields")
			return
		}

		// user
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// validate AMC is approved and active from context
		approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
		found := false
		for _, amc := range approvedAMCs {
			if strings.EqualFold(amc["amc_name"], req.AmcName) {
				found = true
				break
			}
		}
		if !found {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrAMCNotFoundOrNotApprovedActive+req.AmcName)
			return
		}

		// check ISIN uniqueness (non-deleted) only if ISIN is provided
		if strings.TrimSpace(req.ISIN) != "" {
			var tmp int
			err := pgxPool.QueryRow(ctx, `SELECT 1 FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, req.ISIN).Scan(&tmp)
			if err == nil {
				api.RespondWithError(w, http.StatusBadRequest, "Scheme with ISIN already exists")
				return
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrTxBeginFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// validate method if provided
		allowed := map[string]bool{"FIFO": true, "LIFO": true, "WEIGHTED_AVERAGE": true}
		m := strings.ToUpper(strings.TrimSpace(req.Method))
		if m != "" && !allowed[m] {
			api.RespondWithError(w, http.StatusBadRequest, "method must be FIFO, LIFO or WEIGHTED_AVERAGE")
			return
		}

		insertQ := `
			INSERT INTO investment.masterscheme (
				scheme_name, isin, amc_name, internal_scheme_code,
				internal_risk_rating, erp_gl_account, amfi_scheme_code, status, method, source
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'Manual')
			RETURNING scheme_id
		`

		var schemeID string
		if err := tx.QueryRow(ctx, insertQ,
			req.SchemeName, req.ISIN, req.AmcName, req.InternalSchemeCode,
			req.InternalRiskRating, req.ErpGlAccount, req.AmfiSchemeCode, defaultIfEmpty(req.Status, "Active"), m,
		).Scan(&schemeID); err != nil {
			msg, status := getUserFriendlySchemeError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// audit insert
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, schemeID, userEmail); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"scheme_id":        schemeID,
			"scheme_name":      req.SchemeName,
			"amfi_scheme_code": req.AmfiSchemeCode,
			"source":           "Manual",
			"method":           m,
		})
	}
}

// ---------------------------
// UpdateScheme (single)
// ---------------------------
func UpdateScheme(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateSchemeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.SchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFieldsToUpdateUser)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrTxBeginFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// fetch existing values for old_ columns
		sel := `
			SELECT scheme_name, isin, amc_name, internal_scheme_code, internal_risk_rating, erp_gl_account, amfi_scheme_code, status, source, method
			FROM investment.masterscheme
			WHERE scheme_id=$1
			FOR UPDATE
		`
		var oldVals [10]interface{}
		if err := tx.QueryRow(ctx, sel, req.SchemeID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9],
		); err != nil {
			msg, status := getUserFriendlySchemeError(err, "fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// allowed fields mapping
		fieldPairs := map[string]int{
			"scheme_name":          0,
			"isin":                 1,
			"amc_name":             2,
			"internal_scheme_code": 3,
			"internal_risk_rating": 4,
			"erp_gl_account":       5,
			"amfi_scheme_code":     6,
			"status":               7,
			"method":               9,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				// validate method if present
				if lk == "method" {
					allowed := map[string]bool{"FIFO": true, "LIFO": true, "WEIGHTED_AVERAGE": true}
					mm := strings.ToUpper(strings.TrimSpace(fmt.Sprint(v)))
					if mm != "" && !allowed[mm] {
						api.RespondWithError(w, http.StatusBadRequest, "method must be FIFO, LIFO or WEIGHTED_AVERAGE")
						return
					}
					v = mm
				}
				// if amc_name provided, validate AMC is approved
				if lk == "amc_name" {
					amcName := fmt.Sprint(v)
					approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
					found := false
					for _, amc := range approvedAMCs {
						if strings.EqualFold(amc["amc_name"], amcName) {
							found = true
							break
						}
					}
					if !found {
						api.RespondWithError(w, http.StatusBadRequest, constants.ErrAMCNotFoundOrNotApprovedActive+amcName)
						return
					}
				}
				// if isin present and different, ensure uniqueness
				if lk == "isin" {
					newIsin := strings.TrimSpace(fmt.Sprint(v))
					if newIsin != "" && newIsin != ifaceToString(oldVals[1]) {
						var exists int
						err := tx.QueryRow(ctx, `SELECT 1 FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false AND scheme_id <> $2 LIMIT 1`, newIsin, req.SchemeID).Scan(&exists)
						if err == nil {
							api.RespondWithError(w, http.StatusBadRequest, "ISIN already exists for another scheme")
							return
						}
					}
				}
				oldField := "old_" + lk
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, lk, pos, oldField, pos+1))
				args = append(args, v, oldVals[idx])
				pos += 2
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.masterscheme SET %s WHERE scheme_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.SchemeID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrUpdateFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// insert audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, req.SchemeID, req.Reason, userEmail); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrCommitFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"scheme_id": req.SchemeID,
			"requested": userEmail,
		})
	}
}

// ---------------------------
// DeleteScheme (bulk-style input of ids)
// ---------------------------
func DeleteScheme(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			SchemeIDs []string `json:"scheme_ids"`
			Reason    string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.SchemeIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_ids required")
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrTxBeginFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.SchemeIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())
			`, id, req.Reason, requestedBy); err != nil {
				msg, status := getUserFriendlySchemeError(err, "insert failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrCommitFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.SchemeIDs})
	}
}

// ---------------------------
// BulkApproveSchemeActions
// - Approves PENDING_APPROVAL and PENDING_EDIT_APPROVAL -> set APPROVED
// - If action is PENDING_DELETE_APPROVAL then mark audit row as DELETED and master is_deleted=true + status='Inactive'
// ---------------------------
func BulkApproveSchemeActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			SchemeIDs []string `json:"scheme_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrTxBeginFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (scheme_id) action_id, scheme_id, actiontype, processing_status
			FROM investment.auditactionscheme
			WHERE scheme_id = ANY($1)
			ORDER BY scheme_id, GREATEST(COALESCE(requested_at, '1970-01-01'::timestamp), COALESCE(checker_at, '1970-01-01'::timestamp)) DESC
		`
		rows, err := tx.Query(ctx, sel, req.SchemeIDs)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		var toApprove []string = []string{}
		var toDeleteActionIDs []string = []string{}
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, sid, atype, pstatus string
			if err := rows.Scan(&aid, &sid, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			// at := strings.ToUpper(strings.TrimSpace(atype))
			if ps == "APPROVED" {
				continue
			}
			if ps == "PENDING_DELETE_APPROVAL" {
				// mark audit as DELETED, and soft-delete master
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, sid)
				continue
			}
			// PENDING_APPROVAL / PENDING_EDIT_APPROVAL -> APPROVED
			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions, map[string]any{
				"approved_action_ids": []string{},
				"deleted_schemes":     []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionscheme
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				msg, status := getUserFriendlySchemeError(err, "approve update failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionscheme
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				msg, status := getUserFriendlySchemeError(err, "mark deleted failed")
				api.RespondWithError(w, status, msg)
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.masterscheme
				SET is_deleted=true, status='Inactive'
				WHERE scheme_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				msg, status := getUserFriendlySchemeError(err, "master soft-delete failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrCommitFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_action_ids": toApprove,
			"deleted_schemes":     deleteMasterIDs,
		})
	}
}

// ---------------------------
// BulkRejectSchemeActions
// ---------------------------
func BulkRejectSchemeActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			SchemeIDs []string `json:"scheme_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrTxBeginFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (scheme_id) action_id, scheme_id, processing_status
			FROM investment.auditactionscheme
			WHERE scheme_id = ANY($1)
			ORDER BY scheme_id, GREATEST(COALESCE(requested_at, '1970-01-01'::timestamp), COALESCE(checker_at, '1970-01-01'::timestamp)) DESC
		`
		rows, err := tx.Query(ctx, sel, req.SchemeIDs)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		var actionIDs []string
		var cannotReject []string
		found := map[string]bool{}
		for rows.Next() {
			var aid, sid, ps string
			if err := rows.Scan(&aid, &sid, &ps); err != nil {
				continue
			}
			found[sid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == "APPROVED" {
				cannotReject = append(cannotReject, sid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.SchemeIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for scheme_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved scheme_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactionscheme
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrUpdateFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrCommitFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

func GetApprovedActiveSchemes(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_scheme AS (
				SELECT DISTINCT ON (scheme_id) scheme_id, processing_status
				FROM investment.auditactionscheme
				ORDER BY scheme_id, requested_at DESC
			),
			latest_amc AS (
				SELECT DISTINCT ON (amc.amc_id) amc.amc_id, m.amc_name, amc.processing_status, m.status
				FROM investment.auditactionamc amc
				JOIN investment.masteramc m ON amc.amc_id = m.amc_id
				ORDER BY amc.amc_id, amc.requested_at DESC
			)
			SELECT m.scheme_id, m.scheme_name, m.isin, m.internal_scheme_code, m.amc_name, m.amfi_scheme_code, COALESCE(m.method,'') AS method
			FROM investment.masterscheme m
			JOIN latest_scheme l ON l.scheme_id = m.scheme_id
			JOIN latest_amc la ON UPPER(la.amc_name) = UPPER(m.amc_name)
			WHERE UPPER(l.processing_status) = 'APPROVED' 
			  AND UPPER(m.status) = 'ACTIVE'
			  AND COALESCE(m.is_deleted,false) = false
			  AND UPPER(la.processing_status) = 'APPROVED'
			  AND UPPER(la.status) = 'ACTIVE'
			ORDER BY m.scheme_name;
		`
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()
		out := []map[string]interface{}{}
		for rows.Next() {
			var id, name, isin, code, amc, amfiCode, method string
			_ = rows.Scan(&id, &name, &isin, &code, &amc, &amfiCode, &method)
			out = append(out, map[string]interface{}{
				"scheme_id": id, "scheme_name": name, "isin": isin, "internal_scheme_code": code, "amc_name": amc, "amfi_scheme_code": amfiCode, "method": method,
			})
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

func GetApprovedActiveSchemesByAMC(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Read amc_name from JSON body
		var body struct {
			AMCName string `json:"amc"`
			AMC     string `json:"amc_name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		amcName := strings.TrimSpace(body.AMCName)
		if amcName == "" {
			amcName = strings.TrimSpace(body.AMC)
		}

		// Base query
		q := `
			WITH latest_scheme AS (
				SELECT DISTINCT ON (scheme_id) scheme_id, processing_status
				FROM investment.auditactionscheme
				ORDER BY scheme_id, requested_at DESC
			),
			latest_amc AS (
				SELECT DISTINCT ON (amc.amc_id) amc.amc_id, m.amc_name, amc.processing_status, m.status
				FROM investment.auditactionamc amc
				JOIN investment.masteramc m ON amc.amc_id = m.amc_id
				ORDER BY amc.amc_id, amc.requested_at DESC
			)
			SELECT 
				m.scheme_id,
				COALESCE(m.scheme_name, '') AS scheme_name,
				COALESCE(m.isin, '') AS isin,
				COALESCE(m.internal_scheme_code, '') AS internal_scheme_code,
				COALESCE(m.amc_name, '') AS amc_name,
				COALESCE(m.amfi_scheme_code, '') AS amfi_scheme_code,
				COALESCE(m.method,'') AS method
			FROM investment.masterscheme m
			JOIN latest_scheme l ON l.scheme_id = m.scheme_id
			JOIN latest_amc la ON UPPER(la.amc_name) = UPPER(m.amc_name)
			WHERE UPPER(l.processing_status) = 'APPROVED'
			  AND UPPER(m.status) = 'ACTIVE'
			  AND COALESCE(m.is_deleted,false) = false
			  AND UPPER(la.processing_status) = 'APPROVED'
			  AND UPPER(la.status) = 'ACTIVE'
		`

		args := []interface{}{}
		if amcName != "" {
			q += ` AND UPPER(m.amc_name) = UPPER($1)`
			args = append(args, amcName)
		}

		q += ` ORDER BY m.scheme_name;`

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var id, name, isin, code, amc, amfiCode, method string
			if err := rows.Scan(&id, &name, &isin, &code, &amc, &amfiCode, &method); err != nil {
				msg, status := getUserFriendlySchemeError(err, constants.ErrScanFailedPrefix)
				api.RespondWithError(w, status, msg)
				return
			}
			out = append(out, map[string]interface{}{
				"scheme_id":            id,
				"scheme_name":          name,
				"isin":                 isin,
				"internal_scheme_code": code,
				"amc_name":             amc,
				"amfi_scheme_code":     amfiCode,
				"method":               method,
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetSchemesWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.scheme_id)
					a.scheme_id, a.actiontype, a.processing_status, a.action_id,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason
				FROM investment.auditactionscheme a
				ORDER BY a.scheme_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					scheme_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionscheme
				GROUP BY scheme_id
			)
			SELECT
				m.*,

				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
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

			FROM investment.masterscheme m
			LEFT JOIN latest_audit l ON l.scheme_id = m.scheme_id
			LEFT JOIN history h ON h.scheme_id = m.scheme_id
			WHERE COALESCE(m.is_deleted,false)=false
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC	;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlySchemeError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					rec[string(f.Name)] = ""
				} else {
					rec[string(f.Name)] = vals[i]
				}
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// CreateScheme (bulk JSON array)
// ---------------------------
type CreateSchemeRequest struct {
	UserID string        `json:"user_id"`
	Rows   []SchemeInput `json:"rows"`
}

type SchemeInput struct {
	SchemeName         string `json:"scheme_name"`
	ISIN               string `json:"isin"`
	AmcName            string `json:"amc_name"`
	InternalSchemeCode string `json:"internal_scheme_code"`
	InternalRiskRating string `json:"internal_risk_rating"`
	ErpGlAccount       string `json:"erp_gl_account"`
	AmfiSchemeCode     string `json:"amfi_scheme_code"`
	Status             string `json:"status,omitempty"`
	Method             string `json:"method,omitempty"`
}

func CreateScheme(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateSchemeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "rows required")
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			// basic validation
			if strings.TrimSpace(row.SchemeName) == "" ||
				strings.TrimSpace(row.AmcName) == "" || strings.TrimSpace(row.InternalSchemeCode) == "" ||
				strings.TrimSpace(row.InternalRiskRating) == "" || strings.TrimSpace(row.ErpGlAccount) == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrMissingRequiredFieldsUser,
				})
				continue
			}

			// validate AMC approval from context
			approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
			found := false
			for _, amc := range approvedAMCs {
				if strings.EqualFold(amc["amc_name"], row.AmcName) {
					found = true
					break
				}
			}
			if !found {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: constants.ErrAMCNotFoundOrNotApprovedActive + row.AmcName,
				})
				continue
			}

			// ensure ISIN uniqueness only if ISIN is provided
			if strings.TrimSpace(row.ISIN) != "" {
				var tmp int
				err := pgxPool.QueryRow(ctx, `SELECT 1 FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, row.ISIN).Scan(&tmp)
				if err == nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false, constants.ValueError: fmt.Sprintf("ISIN already exists: %s", row.ISIN),
					})
					continue
				}
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: constants.ErrTxBeginFailed + err.Error(),
				})
				continue
			}
			defer tx.Rollback(ctx)

			// validate method if provided
			allowed := map[string]bool{"FIFO": true, "LIFO": true, "WEIGHTED_AVERAGE": true}
			m := strings.ToUpper(strings.TrimSpace(row.Method))
			if m != "" && !allowed[m] {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: fmt.Sprintf("Invalid method for %s", row.SchemeName)})
				continue
			}

			insertQ := `
				INSERT INTO investment.masterscheme (
					scheme_name, isin, amc_name, internal_scheme_code,
					internal_risk_rating, erp_gl_account, amfi_scheme_code, status, method, source
				)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'Manual')
				RETURNING scheme_id
			`
			var schemeID string
			err = tx.QueryRow(ctx, insertQ,
				row.SchemeName,
				row.ISIN,
				row.AmcName,
				row.InternalSchemeCode,
				row.InternalRiskRating,
				row.ErpGlAccount,
				row.AmfiSchemeCode,
				defaultIfEmpty(row.Status, "Active"), m,
			).Scan(&schemeID)

			if err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: fmt.Sprintf("Insert failed for %s: %v", row.SchemeName, err),
				})
				continue
			}

			audit := `
				INSERT INTO investment.auditactionscheme (
					scheme_id, actiontype, processing_status, requested_by, requested_at
				) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
			`
			if _, err := tx.Exec(ctx, audit, schemeID, userEmail); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailedCapitalized + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				constants.ValueSuccess: true,
				"scheme_id":            schemeID,
				"scheme_name":          row.SchemeName,
				"amfi_scheme_code":     row.AmfiSchemeCode,
				"source":               "Manual",
				"requested":            userEmail,
				"method":               m,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func UpdateSchemeBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				SchemeID string                 `json:"scheme_id"`
				Fields   map[string]interface{} `json:"fields"`
				Reason   string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.SchemeID == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: "scheme_id missing",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "scheme_id": row.SchemeID, constants.ValueError: constants.ErrTxBeginFailedCapitalized + err.Error(),
				})
				continue
			}
			defer tx.Rollback(ctx)

			sel := `
				SELECT scheme_name, isin, amc_name, internal_scheme_code, internal_risk_rating, erp_gl_account, amfi_scheme_code, status, source, method
				FROM investment.masterscheme WHERE scheme_id=$1 FOR UPDATE`
			var oldVals [10]interface{}
			if err := tx.QueryRow(ctx, sel, row.SchemeID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9],
			); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "scheme_id": row.SchemeID, constants.ValueError: "fetch failed: " + err.Error(),
				})
				continue
			}

			fieldPairs := map[string]int{
				"scheme_name":          0,
				"isin":                 1,
				"amc_name":             2,
				"internal_scheme_code": 3,
				"internal_risk_rating": 4,
				"erp_gl_account":       5,
				"amfi_scheme_code":     6,
				constants.KeyStatus:    7,
				"method":               9,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					// validate method if present
					if lk == "method" {
						allowed := map[string]bool{"FIFO": true, "LIFO": true, "WEIGHTED_AVERAGE": true}
						mm := strings.ToUpper(strings.TrimSpace(fmt.Sprint(v)))
						if mm != "" && !allowed[mm] {
							results = append(results, map[string]interface{}{
								constants.ValueSuccess: false, "scheme_id": row.SchemeID, constants.ValueError: "Invalid method: " + fmt.Sprint(v),
							})
							continue
						}
						v = mm
					}
					if lk == "amc_name" {
						approvedAMCs, _ := ctx.Value("ApprovedAMCs").([]map[string]string)
						found := false
						for _, amc := range approvedAMCs {
							if strings.EqualFold(amc["amc_name"], fmt.Sprint(v)) {
								found = true
								break
							}
						}
						if !found {
							results = append(results, map[string]interface{}{
								constants.ValueSuccess: false, constants.ValueError: constants.ErrAMCNotFoundOrNotApprovedActive + fmt.Sprint(v),
							})
							continue
						}
					}
					if lk == "isin" {
						newIsin := strings.TrimSpace(fmt.Sprint(v))
						if newIsin != "" && newIsin != ifaceToString(oldVals[1]) {
							var exists int
							err := tx.QueryRow(ctx, `SELECT 1 FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false AND scheme_id <> $2 LIMIT 1`, newIsin, row.SchemeID).Scan(&exists)
							if err == nil {
								results = append(results, map[string]interface{}{
									constants.ValueSuccess: false, constants.ValueError: "ISIN already exists: " + newIsin,
								})
								continue
							}
						}
					}
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}

			if len(sets) == 0 {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "scheme_id": row.SchemeID, constants.ValueError: "No valid fields",
				})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.masterscheme SET %s WHERE scheme_id=$%d",
				strings.Join(sets, ", "), pos)
			args = append(args, row.SchemeID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "scheme_id": row.SchemeID, constants.ValueError: constants.ErrUpdateFailed + err.Error(),
				})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
			`, row.SchemeID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "scheme_id": row.SchemeID, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, "scheme_id": row.SchemeID, constants.ValueError: constants.ErrCommitFailed + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				constants.ValueSuccess: true,
				"scheme_id":            row.SchemeID,
				"requested":            userEmail,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}
