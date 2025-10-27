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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
	Status             string `json:"status,omitempty"`
	Source             string `json:"source,omitempty"` // ignored, we set Manual
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
			ORDER BY amc_id, requested_at DESC
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
		userID := r.FormValue("user_id")
		if userID == "" {
			var tmp struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&tmp)
			userID = tmp.UserID
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
			api.RespondWithError(w, http.StatusUnauthorized, "user not in active sessions")
			return
		}

		// parse multipart
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse form: "+err.Error())
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
			"status":               true,
			"source":               true,
		}

		results := make([]UploadSchemeResult, 0, len(files))

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file")
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file")
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
			// Validate AMC(s) - for upload we require they are approved
			for _, an := range amcNames {
				ok, err := isAMCApproved(ctx, pgxPool, an)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "AMC validation failed: "+err.Error())
					return
				}
				if !ok {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("AMC not found or not approved: %s", an))
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

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "masterscheme"}, validCols, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
				return
			}

			// auto-populate source to 'Upload' for newly inserted rows by internal_scheme_code or isin
			if len(isinList) > 0 {
				if _, err := tx.Exec(ctx, `
					UPDATE investment.masterscheme
					SET source = 'Upload'
					WHERE isin = ANY($1)
				`, isinList); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to set source: "+err.Error())
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
					api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		// Validate required
		if strings.TrimSpace(req.SchemeName) == "" ||
			strings.TrimSpace(req.ISIN) == "" ||
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()

		// validate AMC is approved
		ok, err := isAMCApproved(ctx, pgxPool, req.AmcName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "AMC validation failed: "+err.Error())
			return
		}
		if !ok {
			api.RespondWithError(w, http.StatusBadRequest, "AMC not found or not approved: "+req.AmcName)
			return
		}

		// check ISIN uniqueness (non-deleted)
		var tmp int
		err = pgxPool.QueryRow(ctx, `SELECT 1 FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, req.ISIN).Scan(&tmp)
		if err == nil {
			api.RespondWithError(w, http.StatusBadRequest, "Scheme with ISIN already exists")
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
			INSERT INTO investment.masterscheme (
				scheme_name, isin, amc_name, internal_scheme_code,
				internal_risk_rating, erp_gl_account, status, source
			) VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual')
			RETURNING scheme_id
		`

		var schemeID string
		if err := tx.QueryRow(ctx, insertQ,
			req.SchemeName, req.ISIN, req.AmcName, req.InternalSchemeCode,
			req.InternalRiskRating, req.ErpGlAccount, defaultIfEmpty(req.Status, "Active"),
		).Scan(&schemeID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// audit insert
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, schemeID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"scheme_id":   schemeID,
			"scheme_name": req.SchemeName,
			"source":      "Manual",
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.SchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no fields to update")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// fetch existing values for old_ columns
		sel := `
			SELECT scheme_name, isin, amc_name, internal_scheme_code, internal_risk_rating, erp_gl_account, status, source
			FROM investment.masterscheme
			WHERE scheme_id=$1
			FOR UPDATE
		`
		var oldVals [8]interface{}
		if err := tx.QueryRow(ctx, sel, req.SchemeID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
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
			"status":               6,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				// if amc_name provided, validate AMC is approved
				if lk == "amc_name" {
					amcName := fmt.Sprint(v)
					ok, err := isAMCApproved(ctx, pgxPool, amcName)
					if err != nil {
						api.RespondWithError(w, http.StatusInternalServerError, "AMC validation failed: "+err.Error())
						return
					}
					if !ok {
						api.RespondWithError(w, http.StatusBadRequest, "AMC not found or not approved: "+amcName)
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
				sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
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
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// insert audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, req.SchemeID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.SchemeIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())
			`, id, req.Reason, requestedBy); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "insert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (scheme_id) action_id, scheme_id, actiontype, processing_status
			FROM investment.auditactionscheme
			WHERE scheme_id = ANY($1)
			ORDER BY scheme_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.SchemeIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
			api.RespondWithPayload(w, false, "No approvable actions found", map[string]any{
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
				api.RespondWithError(w, http.StatusInternalServerError, "approve update failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionscheme
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "mark deleted failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.masterscheme
				SET is_deleted=true, status='Inactive'
				WHERE scheme_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "master soft-delete failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (scheme_id) action_id, scheme_id, processing_status
			FROM investment.auditactionscheme
			WHERE scheme_id = ANY($1)
			ORDER BY scheme_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.SchemeIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

func GetApprovedActiveSchemes(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest AS (
				SELECT DISTINCT ON (scheme_id) scheme_id, processing_status
				FROM investment.auditactionscheme
				ORDER BY scheme_id, requested_at DESC
			)
			SELECT m.scheme_id, m.scheme_name, m.isin, m.internal_scheme_code, m.amc_name
			FROM investment.masterscheme m
			JOIN latest l ON l.scheme_id = m.scheme_id
			WHERE UPPER(l.processing_status) = 'APPROVED' 
			  AND UPPER(m.status) = 'ACTIVE'
			  AND COALESCE(m.is_deleted,false) = false
			ORDER BY m.scheme_name;
		`
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]interface{}{}
		for rows.Next() {
			var id, name, isin, code, amc string
			_ = rows.Scan(&id, &name, &isin, &code, &amc)
			out = append(out, map[string]interface{}{
				"scheme_id": id, "scheme_name": name, "isin": isin, "internal_scheme_code": code, "amc_name": amc,
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
			ORDER BY m.scheme_name;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, "rows scan failed: "+rows.Err().Error())
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
	Status             string `json:"status,omitempty"`
}

func CreateScheme(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateSchemeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid or inactive user session")
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			// basic validation
			if strings.TrimSpace(row.SchemeName) == "" || strings.TrimSpace(row.ISIN) == "" ||
				strings.TrimSpace(row.AmcName) == "" || strings.TrimSpace(row.InternalSchemeCode) == "" ||
				strings.TrimSpace(row.InternalRiskRating) == "" || strings.TrimSpace(row.ErpGlAccount) == "" {
				results = append(results, map[string]interface{}{
					"success": false,
					"error":   "Missing required fields",
				})
				continue
			}

			// validate AMC approval
			ok, err := isAMCApproved(ctx, pgxPool, row.AmcName)
			if err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "error": "AMC check failed: " + err.Error(),
				})
				continue
			}
			if !ok {
				results = append(results, map[string]interface{}{
					"success": false, "error": "AMC not approved or not found: " + row.AmcName,
				})
				continue
			}

			// ensure ISIN uniqueness
			var tmp int
			err = pgxPool.QueryRow(ctx, `SELECT 1 FROM investment.masterscheme WHERE isin=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, row.ISIN).Scan(&tmp)
			if err == nil {
				results = append(results, map[string]interface{}{
					"success": false, "error": fmt.Sprintf("ISIN already exists: %s", row.ISIN),
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "error": "TX begin failed: " + err.Error(),
				})
				continue
			}
			defer tx.Rollback(ctx)

			insertQ := `
				INSERT INTO investment.masterscheme (
					scheme_name, isin, amc_name, internal_scheme_code,
					internal_risk_rating, erp_gl_account, status, source
				)
				VALUES ($1,$2,$3,$4,$5,$6,$7,'Manual')
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
				defaultIfEmpty(row.Status, "Active"),
			).Scan(&schemeID)

			if err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "error": fmt.Sprintf("Insert failed for %s: %v", row.SchemeName, err),
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
					"success": false, "error": "Audit insert failed: " + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "error": "Commit failed: " + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":     true,
				"scheme_id":   schemeID,
				"scheme_name": row.SchemeName,
				"source":      "Manual",
				"requested":   userEmail,
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid or inactive session")
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.SchemeID == "" {
				results = append(results, map[string]interface{}{
					"success": false, "error": "scheme_id missing",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "scheme_id": row.SchemeID, "error": "tx begin failed: " + err.Error(),
				})
				continue
			}
			defer tx.Rollback(ctx)

			sel := `
				SELECT scheme_name, isin, amc_name, internal_scheme_code, internal_risk_rating, erp_gl_account, status, source
				FROM investment.masterscheme WHERE scheme_id=$1 FOR UPDATE`
			var oldVals [8]interface{}
			if err := tx.QueryRow(ctx, sel, row.SchemeID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
			); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "scheme_id": row.SchemeID, "error": "fetch failed: " + err.Error(),
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
				"status":               6,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					if lk == "amc_name" {
						ok, err := isAMCApproved(ctx, pgxPool, fmt.Sprint(v))
						if err != nil {
							results = append(results, map[string]interface{}{
								"success": false, "error": "AMC validation failed: " + err.Error(),
							})
							continue
						}
						if !ok {
							results = append(results, map[string]interface{}{
								"success": false, "error": "AMC not approved: " + fmt.Sprint(v),
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
									"success": false, "error": "ISIN already exists: " + newIsin,
								})
								continue
							}
						}
					}
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}

			if len(sets) == 0 {
				results = append(results, map[string]interface{}{
					"success": false, "scheme_id": row.SchemeID, "error": "No valid fields",
				})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.masterscheme SET %s WHERE scheme_id=$%d",
				strings.Join(sets, ", "), pos)
			args = append(args, row.SchemeID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "scheme_id": row.SchemeID, "error": "update failed: " + err.Error(),
				})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
			`, row.SchemeID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "scheme_id": row.SchemeID, "error": "audit insert failed: " + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "scheme_id": row.SchemeID, "error": "commit failed: " + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":   true,
				"scheme_id": row.SchemeID,
				"requested": userEmail,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}
