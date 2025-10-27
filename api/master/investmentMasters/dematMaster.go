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


type CreateDematRequestSingle struct {
	UserID                   string `json:"user_id"`
	EntityName               string `json:"entity_name"`
	DPID                     string `json:"dp_id"`
	Depository               string `json:"depository"`
	DematAccountNumber       string `json:"demat_account_number"`
	DepositoryParticipant    string `json:"depository_participant,omitempty"`
	ClientID                 string `json:"client_id,omitempty"`
	DefaultSettlementAccount string `json:"default_settlement_account"`
	Status                   string `json:"status,omitempty"`
	Source                   string `json:"source,omitempty"` // ignored; set to Manual
}

type CreateDematBulkRequest struct {
	UserID string `json:"user_id"`
	Rows   []struct {
		EntityName               string `json:"entity_name"`
		DPID                     string `json:"dp_id"`
		Depository               string `json:"depository"`
		DematAccountNumber       string `json:"demat_account_number"`
		DepositoryParticipant    string `json:"depository_participant,omitempty"`
		ClientID                 string `json:"client_id,omitempty"`
		DefaultSettlementAccount string `json:"default_settlement_account"`
		Status                   string `json:"status,omitempty"`
	} `json:"rows"`
}

type UpdateDematRequest struct {
	UserID  string                 `json:"user_id"`
	DematID string                 `json:"demat_id"`
	Fields  map[string]interface{} `json:"fields"`
	Reason  string                 `json:"reason"`
}

type UploadDematResult struct {
	Success bool   `json:"success"`
	BatchID string `json:"batch_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

func UploadDematSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

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

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse form: "+err.Error())
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No file uploaded")
			return
		}

		allowed := map[string]bool{
			"entity_name":                true,
			"dp_id":                      true,
			"depository":                 true,
			"demat_account_number":       true,
			"depository_participant":     true,
			"client_id":                  true,
			"default_settlement_account": true,
			"status":                     true,
			// source auto-populated
		}

		results := make([]UploadDematResult, 0, len(files))

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
			mandatories := []string{"entity_name", "dp_id", "depository", "demat_account_number", "default_settlement_account"}
			for _, m := range mandatories {
				if !contains(validCols, m) {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("CSV must include column: %s", m))
					return
				}
			}

			// prepare COPY rows
			copyRows := make([][]interface{}, len(dataRows))
			dematNumbers := make([]string, 0, len(dataRows))
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
				if pos, ok := headerPos["demat_account_number"]; ok && pos < len(row) {
					if code := strings.TrimSpace(row[pos]); code != "" {
						dematNumbers = append(dematNumbers, code)
					}
				}
				copyRows[i] = vals
			}

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

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "masterdemataccount"}, validCols, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
				return
			}

			// set source = 'Upload' for inserted rows
			if len(dematNumbers) > 0 {
				if _, err := tx.Exec(ctx, `
					UPDATE investment.masterdemataccount
					SET source = 'Upload'
					WHERE demat_account_number = ANY($1)
				`, dematNumbers); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to set source: "+err.Error())
					return
				}

				// insert audit rows for created demats (map demat_account_number -> demat_id)
				if _, err := tx.Exec(ctx, `
					INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, reason, requested_by, requested_at)
					SELECT demat_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM investment.masterdemataccount
					WHERE demat_account_number = ANY($2)
				`, userName, dematNumbers); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true

			results = append(results, UploadDematResult{Success: true, BatchID: uuid.New().String()})
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

func CreateDematSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateDematRequestSingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		// required fields
		if strings.TrimSpace(req.EntityName) == "" ||
			strings.TrimSpace(req.DPID) == "" ||
			strings.TrimSpace(req.Depository) == "" ||
			strings.TrimSpace(req.DematAccountNumber) == "" ||
			strings.TrimSpace(req.DefaultSettlementAccount) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required demat fields")
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

		// uniqueness: entity_name + demat_account_number
		var tmp int
		err := pgxPool.QueryRow(ctx, `SELECT 1 FROM investment.masterdemataccount WHERE entity_name=$1 AND demat_account_number=$2 AND COALESCE(is_deleted,false)=false LIMIT 1`, req.EntityName, req.DematAccountNumber).Scan(&tmp)
		if err == nil {
			api.RespondWithError(w, http.StatusBadRequest, "Demat account (entity_name + demat_account_number) already exists")
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
			INSERT INTO investment.masterdemataccount (
				entity_name, dp_id, depository, demat_account_number,
				depository_participant, client_id, default_settlement_account, status, source
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Manual')
			RETURNING demat_id
		`
		var dematID string
		if err := tx.QueryRow(ctx, insertQ,
			req.EntityName,
			req.DPID,
			req.Depository,
			req.DematAccountNumber,
			req.DepositoryParticipant,
			req.ClientID,
			req.DefaultSettlementAccount,
			defaultIfEmpty(req.Status, "Active"),
		).Scan(&dematID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, dematID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"demat_id":  dematID,
			"entity":    req.EntityName,
			"source":    "Manual",
			"requested": userEmail,
		})
	}
}

// ---------------------------
// CreateDematBulk (JSON array) - similar to DP CreateBulk
// ---------------------------
func CreateDematBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateDematBulkRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
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
			name := strings.TrimSpace(row.EntityName)
			dpid := strings.TrimSpace(row.DPID)
			dep := strings.TrimSpace(row.Depository)
			dacc := strings.TrimSpace(row.DematAccountNumber)
			settle := strings.TrimSpace(row.DefaultSettlementAccount)
			if name == "" || dpid == "" || dep == "" || dacc == "" || settle == "" {
				results = append(results, map[string]interface{}{
					"success": false, "error": "Missing required fields",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "TX begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// uniqueness check
			var exists int
			err = tx.QueryRow(ctx, `
				SELECT 1 FROM investment.masterdemataccount
				WHERE entity_name=$1 AND demat_account_number=$2 AND COALESCE(is_deleted,false)=false LIMIT 1
			`, name, dacc).Scan(&exists)
			if err == nil {
				results = append(results, map[string]interface{}{"success": false, "entity": name, "error": "Duplicate demat (entity+demat_account_number)"})
				continue
			}

			var dematID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.masterdemataccount (
					entity_name, dp_id, depository, demat_account_number,
					depository_participant, client_id, default_settlement_account, status, source
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'Manual')
				RETURNING demat_id
			`, name, dpid, dep, dacc, row.DepositoryParticipant, row.ClientID, settle, defaultIfEmpty(row.Status, "Active")).Scan(&dematID); err != nil {
				results = append(results, map[string]interface{}{"success": false, "entity": name, "error": "Insert failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
			`, dematID, userEmail); err != nil {
				results = append(results, map[string]interface{}{"success": false, "demat_id": dematID, "error": "Audit insert failed: " + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{"success": false, "demat_id": dematID, "error": "Commit failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{"success": true, "demat_id": dematID, "entity_name": name})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func UpdateDemat(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateDematRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.DematID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "demat_id required")
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

		sel := `
			SELECT entity_name, dp_id, depository, demat_account_number,
			       depository_participant, client_id, default_settlement_account, status, source
			FROM investment.masterdemataccount
			WHERE demat_id=$1
			FOR UPDATE
		`
		var oldVals [9]interface{}
		if err := tx.QueryRow(ctx, sel, req.DematID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"entity_name":                0,
			"dp_id":                      1,
			"depository":                 2,
			"demat_account_number":       3,
			"depository_participant":     4,
			"client_id":                  5,
			"default_settlement_account": 6,
			"status":                     7,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				// if demat_account_number changed, ensure uniqueness
				if lk == "demat_account_number" {
					newVal := strings.TrimSpace(fmt.Sprint(v))
					if newVal != "" && newVal != ifaceToString(oldVals[3]) {
						var exists int
						err := tx.QueryRow(ctx, `SELECT 1 FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false AND demat_id <> $2 LIMIT 1`, newVal, req.DematID).Scan(&exists)
						if err == nil {
							api.RespondWithError(w, http.StatusBadRequest, "demat_account_number already exists")
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

		q := fmt.Sprintf("UPDATE investment.masterdemataccount SET %s WHERE demat_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.DematID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// insert audit record
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, req.DematID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"demat_id":  req.DematID,
			"requested": userEmail,
		})
	}
}
func UpdateDematBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				DematID string                 `json:"demat_id"`
				Fields  map[string]interface{} `json:"fields"`
				Reason  string                 `json:"reason"`
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
			if row.DematID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "demat_id missing"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "demat_id": row.DematID, "error": "tx begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// fetch existing
			sel := `
				SELECT entity_name, dp_id, depository, demat_account_number,
				       depository_participant, client_id, default_settlement_account, status, source
				FROM investment.masterdemataccount WHERE demat_id=$1 FOR UPDATE`
			var oldVals [9]interface{}
			if err := tx.QueryRow(ctx, sel, row.DematID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
			); err != nil {
				results = append(results, map[string]interface{}{"success": false, "demat_id": row.DematID, "error": "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"entity_name":                0,
				"dp_id":                      1,
				"depository":                 2,
				"demat_account_number":       3,
				"depository_participant":     4,
				"client_id":                  5,
				"default_settlement_account": 6,
				"status":                     7,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					if lk == "demat_account_number" {
						newVal := strings.TrimSpace(fmt.Sprint(v))
						if newVal != "" && newVal != ifaceToString(oldVals[3]) {
							var exists int
							err := tx.QueryRow(ctx, `SELECT 1 FROM investment.masterdemataccount WHERE demat_account_number=$1 AND COALESCE(is_deleted,false)=false AND demat_id <> $2 LIMIT 1`, newVal, row.DematID).Scan(&exists)
							if err == nil {
								results = append(results, map[string]interface{}{"success": false, "demat_id": row.DematID, "error": "demat_account_number already exists: " + newVal})
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
				results = append(results, map[string]interface{}{"success": false, "demat_id": row.DematID, "error": "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.masterdemataccount SET %s WHERE demat_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.DematID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{"success": false, "demat_id": row.DematID, "error": "update failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
			`, row.DematID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{"success": false, "demat_id": row.DematID, "error": "audit insert failed: " + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{"success": false, "demat_id": row.DematID, "error": "commit failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{"success": true, "demat_id": row.DematID, "requested": userEmail})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func DeleteDemat(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			DematIDs []string `json:"demat_ids"`
			Reason   string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if len(req.DematIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "demat_ids required")
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

		for _, id := range req.DematIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, reason, requested_by, requested_at)
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
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.DematIDs})
	}
}

func BulkApproveDematActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			DematIDs []string `json:"demat_ids"`
			Comment  string   `json:"comment"`
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
			SELECT DISTINCT ON (demat_id) action_id, demat_id, actiontype, processing_status
			FROM investment.auditactiondemat
			WHERE demat_id = ANY($1)
			ORDER BY demat_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.DematIDs)
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
				"deleted_demats":      []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactiondemat
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve update failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactiondemat
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "mark deleted failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.masterdemataccount
				SET is_deleted=true, status='Inactive'
				WHERE demat_id = ANY($1)
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
			"deleted_demats":      deleteMasterIDs,
		})
	}
}

func BulkRejectDematActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			DematIDs []string `json:"demat_ids"`
			Comment  string   `json:"comment"`
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
			SELECT DISTINCT ON (demat_id) action_id, demat_id, processing_status
			FROM investment.auditactiondemat
			WHERE demat_id = ANY($1)
			ORDER BY demat_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.DematIDs)
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
		for _, id := range req.DematIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for demat_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved demat_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactiondemat
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

func GetDematsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.demat_id)
					a.demat_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactiondemat a
				ORDER BY a.demat_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					demat_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactiondemat
				GROUP BY demat_id
			),
			clearing AS (
				SELECT 
					c.account_id,
					STRING_AGG(c.code_type || ':' || c.code_value, ', ') AS clearing_codes
				FROM public.masterclearingcode c
				GROUP BY c.account_id
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
				COALESCE(h.deleted_at,'') AS deleted_at,
				ba.account_id,
				ba.account_number,
				ba.account_nickname,
				b.bank_name,
				COALESCE(e.entity_name, ec.entity_name) AS account_entity_name,
				cl.clearing_codes
			FROM investment.masterdemataccount m
			LEFT JOIN latest_audit l ON l.demat_id = m.demat_id
			LEFT JOIN history h ON h.demat_id = m.demat_id
			LEFT JOIN public.masterbankaccount ba ON ba.account_number = m.default_settlement_account
			LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
			LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
			LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
			LEFT JOIN clearing cl ON cl.account_id = ba.account_id
			WHERE COALESCE(m.is_deleted,false)=false
			ORDER BY m.entity_name;
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
			rec := make(map[string]interface{}, len(fields))

			for i, f := range fields {
				name := string(f.Name)
				if vals[i] == nil {
					rec[name] = ""
				} else if t, ok := vals[i].(time.Time); ok {
					rec[name] = t.Format("2006-01-02 15:04:05")
				} else {
					rec[name] = vals[i]
				}
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Rows scan failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetApprovedActiveDemats(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest AS (
				SELECT DISTINCT ON (demat_id)
					demat_id,
					processing_status
				FROM investment.auditactiondemat
				ORDER BY demat_id, requested_at DESC
			)
			SELECT
				m.demat_id,
				m.dp_id,
				m.depository,
				m.demat_account_number,
				m.depository_participant,
				m.client_id,
				m.default_settlement_account
			FROM investment.masterdemataccount m
			JOIN latest l ON l.demat_id = m.demat_id
			WHERE 
				UPPER(l.processing_status) = 'APPROVED'
				AND UPPER(m.status) = 'ACTIVE'
				AND COALESCE(m.is_deleted, false) = false
			ORDER BY m.demat_account_number;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		for rows.Next() {
			var dematID, dpID, depository, dematAccountNumber, defaultAccount string
			var depositoryParticipant, clientID *string

			if err := rows.Scan(&dematID, &dpID, &depository, &dematAccountNumber, &depositoryParticipant, &clientID, &defaultAccount); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "row scan failed: "+err.Error())
				return
			}

			out = append(out, map[string]interface{}{
				"demat_id":                   dematID,
				"dp_id":                      dpID,
				"depository":                 depository,
				"demat_account_number":       dematAccountNumber,
				"depository_participant":     ifNotNil(depositoryParticipant),
				"client_id":                  ifNotNil(clientID),
				"default_settlement_account": defaultAccount,
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows iteration failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func ifNotNil(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}
