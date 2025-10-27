package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)


type CreateDPRequestSingle struct {
	UserID     string `json:"user_id"`
	DPName     string `json:"dp_name"`
	DPCode     string `json:"dp_code"`
	Depository string `json:"depository"`
	Status     string `json:"status,omitempty"`
}

type UpdateDPRequest struct {
	UserID string                 `json:"user_id"`
	DPID   string                 `json:"dp_id"`
	Fields map[string]interface{} `json:"fields"`
	Reason string                 `json:"reason"`
}

type UploadDPResult struct {
	Success bool   `json:"success"`
	BatchID string `json:"batch_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ---------------------------
// UploadDPSimple (bulk CSV/XLSX -> COPY -> audit)
// ---------------------------

func UploadDPSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
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

		// allowed columns for masterdepositoryparticipant
		allowed := map[string]bool{
			"dp_name":    true,
			"dp_code":    true,
			"depository": true,
			"status":     true,
			// source is auto-populated
		}

		results := make([]UploadDPResult, 0, len(files))

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

			validCols := []string{}
			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
				if allowed[h] {
					validCols = append(validCols, h)
				}
			}

			// require mandatory columns
			mandatories := []string{"dp_name", "dp_code", "depository"}
			for _, m := range mandatories {
				if !contains(validCols, m) {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("CSV must include column: %s", m))
					return
				}
			}

			// build copy rows aligned to validCols
			copyRows := make([][]interface{}, len(dataRows))
			dpCodes := make([]string, 0, len(dataRows))
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
				if pos, ok := headerPos["dp_code"]; ok && pos < len(row) {
					code := strings.TrimSpace(row[pos])
					if code != "" {
						dpCodes = append(dpCodes, code)
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

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "masterdepositoryparticipant"}, validCols, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
				return
			}

			// auto-populate source to 'Upload' for newly inserted rows by dp_code
			if len(dpCodes) > 0 {
				if _, err := tx.Exec(ctx, `
					UPDATE investment.masterdepositoryparticipant
					SET source = 'Upload'
					WHERE dp_code = ANY($1)
				`, dpCodes); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to set source: "+err.Error())
					return
				}
			}

			// insert audit rows for created DP using dp_code -> dp_id mapping
			if len(dpCodes) > 0 {
				if _, err := tx.Exec(ctx, `
					INSERT INTO investment.auditactiondp(dp_id, actiontype, processing_status, reason, requested_by, requested_at)
					SELECT dp_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM investment.masterdepositoryparticipant
					WHERE dp_code = ANY($2)
				`, userName, dpCodes); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true

			results = append(results, UploadDPResult{Success: true, BatchID: uuid.New().String()})
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

// contains helper (small local helper; safe to include)
func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// ---------------------------
// CreateDPSingle (single create, source='Manual')
// ---------------------------

func CreateDPSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateDPRequestSingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.DPName) == "" || strings.TrimSpace(req.DPCode) == "" || strings.TrimSpace(req.Depository) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "dp_name, dp_code and depository required")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()

		// check dp_code uniqueness
		var tmp int
		err := pgxPool.QueryRow(ctx, `SELECT 1 FROM investment.masterdepositoryparticipant WHERE dp_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`, req.DPCode).Scan(&tmp)
		if err == nil {
			api.RespondWithError(w, http.StatusBadRequest, "DP with dp_code already exists")
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
			INSERT INTO investment.masterdepositoryparticipant (
				dp_name, dp_code, depository, status
			) VALUES ($1,$2,$3,$4)
			RETURNING dp_id
		`
		var dpID string
		if err := tx.QueryRow(ctx, insertQ,
			req.DPName, req.DPCode, req.Depository, defaultIfEmpty(req.Status, "Active"),
		).Scan(&dpID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, dpID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"dp_id":     dpID,
			"dp_name":   req.DPName,
			"source":    "Manual",
			"requested": userEmail,
		})
	}
}

// ---------------------------
// UpdateDP (single update, stores old_* values & inserts audit)
// ---------------------------

func UpdateDP(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateDPRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.DPID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "dp_id required")
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
			SELECT dp_name, dp_code, depository, status
			FROM investment.masterdepositoryparticipant
			WHERE dp_id=$1
			FOR UPDATE
		`
		var oldVals [4]interface{}
		if err := tx.QueryRow(ctx, sel, req.DPID).Scan(&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3]); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"dp_name":    0,
			"dp_code":    1,
			"depository": 2,
			"status":     3,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				// if dp_code changed, ensure uniqueness
				if lk == "dp_code" {
					newCode := strings.TrimSpace(fmt.Sprint(v))
					if newCode != "" && newCode != ifaceToString(oldVals[1]) {
						var exists int
						err := tx.QueryRow(ctx, `SELECT 1 FROM investment.masterdepositoryparticipant WHERE dp_code=$1 AND COALESCE(is_deleted,false)=false AND dp_id <> $2 LIMIT 1`, newCode, req.DPID).Scan(&exists)
						if err == nil {
							api.RespondWithError(w, http.StatusBadRequest, "DP code already exists")
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

		q := fmt.Sprintf("UPDATE investment.masterdepositoryparticipant SET %s WHERE dp_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.DPID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// insert audit record
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, req.DPID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"dp_id":     req.DPID,
			"requested": userEmail,
		})
	}
}

// ---------------------------
// UpdateDPBulk (multiple dp edits in one request)
// ---------------------------

func UpdateDPBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				DPID   string                 `json:"dp_id"`
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid or inactive session")
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.DPID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "dp_id missing"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "dp_id": row.DPID, "error": "tx begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// fetch existing
			sel := `
				SELECT dp_name, dp_code, depository, status
				FROM investment.masterdepositoryparticipant WHERE dp_id=$1 FOR UPDATE`
			var oldVals [4]interface{}
			if err := tx.QueryRow(ctx, sel, row.DPID).Scan(&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3]); err != nil {
				results = append(results, map[string]interface{}{"success": false, "dp_id": row.DPID, "error": "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"dp_name":    0,
				"dp_code":    1,
				"depository": 2,
				"status":     3,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					if lk == "dp_code" {
						newCode := strings.TrimSpace(fmt.Sprint(v))
						if newCode != "" && newCode != ifaceToString(oldVals[1]) {
							var exists int
							err := tx.QueryRow(ctx, `SELECT 1 FROM investment.masterdepositoryparticipant WHERE dp_code=$1 AND COALESCE(is_deleted,false)=false AND dp_id <> $2 LIMIT 1`, newCode, row.DPID).Scan(&exists)
							if err == nil {
								results = append(results, map[string]interface{}{"success": false, "dp_id": row.DPID, "error": "DP code already exists: " + newCode})
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
				results = append(results, map[string]interface{}{"success": false, "dp_id": row.DPID, "error": "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.masterdepositoryparticipant SET %s WHERE dp_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.DPID)
			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{"success": false, "dp_id": row.DPID, "error": "update failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
			`, row.DPID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{"success": false, "dp_id": row.DPID, "error": "audit insert failed: " + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{"success": false, "dp_id": row.DPID, "error": "commit failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{"success": true, "dp_id": row.DPID, "requested": userEmail})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// DeleteDP (request deletion -> audit PENDING_DELETE_APPROVAL)
// ---------------------------

func DeleteDP(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			DPIDs  []string `json:"dp_ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if len(req.DPIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "dp_ids required")
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

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.DPIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, reason, requested_by, requested_at)
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
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.DPIDs})
	}
}

// ---------------------------
// BulkApproveDPActions
// ---------------------------

func BulkApproveDPActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			DPIDs   []string `json:"dp_ids"`
			Comment string   `json:"comment"`
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
			SELECT DISTINCT ON (dp_id) action_id, dp_id, actiontype, processing_status
			FROM investment.auditactiondp
			WHERE dp_id = ANY($1)
			ORDER BY dp_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.DPIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		var toApprove []string
		var toDeleteActionIDs []string
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, did, atype, pstatus string
			if err := rows.Scan(&aid, &did, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == "APPROVED" {
				continue
			}
			if ps == "PENDING_DELETE_APPROVAL" {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, did)
				continue
			}
			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, "No approvable actions found", map[string]any{
				"approved_action_ids": []string{},
				"deleted_dps":         []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactiondp
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve update failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactiondp
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "mark deleted failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.masterdepositoryparticipant
				SET is_deleted=true, status='Inactive'
				WHERE dp_id = ANY($1)
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
			"deleted_dps":         deleteMasterIDs,
		})
	}
}

// ---------------------------
// BulkRejectDPActions
// ---------------------------

func BulkRejectDPActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			DPIDs   []string `json:"dp_ids"`
			Comment string   `json:"comment"`
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
			SELECT DISTINCT ON (dp_id) action_id, dp_id, processing_status
			FROM investment.auditactiondp
			WHERE dp_id = ANY($1)
			ORDER BY dp_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.DPIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
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
		for _, id := range req.DPIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for dp_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved dp_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactiondp
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

// ---------------------------
// GetApprovedActiveDPs
// ---------------------------

func GetApprovedActiveDPs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest AS (
				SELECT DISTINCT ON (dp_id) dp_id, processing_status
				FROM investment.auditactiondp
				ORDER BY dp_id, requested_at DESC
			)
			SELECT m.dp_id, m.dp_name, m.dp_code, m.depository
			FROM investment.masterdepositoryparticipant m
			JOIN latest l ON l.dp_id = m.dp_id
			WHERE UPPER(l.processing_status) = 'APPROVED'
			  AND UPPER(m.status) = 'ACTIVE'
			  AND COALESCE(m.is_deleted,false) = false
			ORDER BY m.dp_name;
		`
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]interface{}{}
		for rows.Next() {
			var id, name, code, depository string
			_ = rows.Scan(&id, &name, &code, &depository)
			out = append(out, map[string]interface{}{
				"dp_id": id, "dp_name": name, "dp_code": code, "depository": depository,
			})
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// GetDPsWithAudit (full master fields + latest audit + history)
// ---------------------------

func GetDPsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.dp_id)
					a.dp_id, a.actiontype, a.processing_status, a.action_id,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason
				FROM investment.auditactiondp a
				ORDER BY a.dp_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					dp_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactiondp
				GROUP BY dp_id
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
			FROM investment.masterdepositoryparticipant m
			LEFT JOIN latest_audit l ON l.dp_id = m.dp_id
			LEFT JOIN history h ON h.dp_id = m.dp_id
			WHERE COALESCE(m.is_deleted,false)=false
			ORDER BY m.dp_name;
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
					// convert time.Time to formatted string for well-formed JSON
					if t, ok := vals[i].(time.Time); ok {
						rec[string(f.Name)] = t.Format("2006-01-02 15:04:05")
					} else {
						rec[string(f.Name)] = vals[i]
					}
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
// CreateDPBulk (multiple JSON rows -> insert + audit)
// ---------------------------

func CreateDPBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				DPName     string `json:"dp_name"`
				DPCode     string `json:"dp_code"`
				Depository string `json:"depository"`
				Status     string `json:"status,omitempty"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
			return
		}

		// find user
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
			name := strings.TrimSpace(row.DPName)
			code := strings.TrimSpace(row.DPCode)
			dep := strings.TrimSpace(row.Depository)
			if name == "" || code == "" || dep == "" {
				results = append(results, map[string]interface{}{
					"success": false, "error": "Missing dp_name, dp_code, or depository",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "TX begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// duplicate check
			var exists int
			err = tx.QueryRow(ctx, `
				SELECT 1 FROM investment.masterdepositoryparticipant
				WHERE dp_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1
			`, code).Scan(&exists)
			if err == nil {
				results = append(results, map[string]interface{}{
					"success": false, "dp_code": code, "error": "Duplicate dp_code in DB",
				})
				continue
			}

			var dpID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.masterdepositoryparticipant (
					dp_name, dp_code, depository, status, source
				) VALUES ($1,$2,$3,$4,'Manual')
				RETURNING dp_id
			`, name, code, dep, defaultIfEmpty(row.Status, "Active")).Scan(&dpID); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "dp_code": code, "error": "Insert failed: " + err.Error(),
				})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
			`, dpID, userEmail); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "dp_id": dpID, "error": "Audit insert failed: " + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "dp_id": dpID, "error": "Commit failed: " + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				"success": true,
				"dp_id":   dpID,
				"dp_name": name,
				"dp_code": code,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}
