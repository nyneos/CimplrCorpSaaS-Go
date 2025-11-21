package investmentsuite

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// ---------------------------
// Request/Response Types
// ---------------------------

type CreateInitiationRequestSingle struct {
	UserID          string  `json:"user_id"`
	ProposalID      string  `json:"proposal_id,omitempty"`
	TransactionDate string  `json:"transaction_date"` // YYYY-MM-DD
	EntityName      string  `json:"entity_name"`
	SchemeID        string  `json:"scheme_id"`
	FolioID         string  `json:"folio_id,omitempty"`
	DematID         string  `json:"demat_id,omitempty"`
	Amount          float64 `json:"amount"`
	Status          string  `json:"status,omitempty"`
	Source          string  `json:"source,omitempty"`
}

type UpdateInitiationRequest struct {
	UserID       string                 `json:"user_id"`
	InitiationID string                 `json:"initiation_id"`
	Fields       map[string]interface{} `json:"fields"`
	Reason       string                 `json:"reason"`
}

type UploadInitiationResult struct {
	Success bool   `json:"success"`
	BatchID string `json:"batch_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ---------------------------
// UploadInitiationSimple (bulk CSV/XLSX -> COPY -> audit)
// ---------------------------

func UploadInitiationSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
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

		// allowed columns
		allowed := map[string]bool{
			"proposal_id":      true,
			"transaction_date": true,
			"entity_name":      true,
			"scheme_id":        true,
			"folio_id":         true,
			"amount":           true,
			"status":           true,
			"source":           true,
		}

		results := make([]UploadInitiationResult, 0, len(files))

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
			mandatories := []string{"proposal_id", "transaction_date", "entity_name", "scheme_id", "amount"}
			for _, m := range mandatories {
				if !contains(validCols, m) {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("CSV must include column: %s", m))
					return
				}
			}

			// build copy rows
			copyRows := make([][]interface{}, len(dataRows))
			initiationIDs := make([]string, 0, len(dataRows))
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
				copyRows[i] = vals
			}

			// transaction
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

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"investment", "investment_initiation"}, validCols, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
				return
			}

			// get inserted initiation_ids
			rows, err := tx.Query(ctx, `
				SELECT initiation_id FROM investment.investment_initiation
				WHERE source = 'Manual' OR source IS NULL
				ORDER BY updated_at DESC
				LIMIT $1
			`, len(dataRows))
			if err == nil {
				for rows.Next() {
					var id string
					if rows.Scan(&id) == nil {
						initiationIDs = append(initiationIDs, id)
					}
				}
				rows.Close()
			}

			// insert audit rows
			if len(initiationIDs) > 0 {
				for _, id := range initiationIDs {
					_, _ = tx.Exec(ctx, `
						INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, requested_by, requested_at)
						VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
					`, id, userName)
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true

			results = append(results, UploadInitiationResult{Success: true, BatchID: uuid.New().String()})
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

// ---------------------------
// CreateInitiationSingle (single create, source='Manual')
// ---------------------------

func CreateInitiationSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateInitiationRequestSingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// validate required fields
		if strings.TrimSpace(req.TransactionDate) == "" ||
			strings.TrimSpace(req.EntityName) == "" ||
			strings.TrimSpace(req.SchemeID) == "" ||
			req.Amount <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required fields: transaction_date, entity_name, scheme_id, amount")
			return
		}

		// Either folio_id OR demat_id must be provided
		if strings.TrimSpace(req.FolioID) == "" && strings.TrimSpace(req.DematID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Either folio_id or demat_id is required")
			return
		}

		// if source is Proposal, proposal_id is required
		source := defaultIfEmpty(req.Source, "Manual")
		if strings.ToUpper(source) == "PROPOSAL" && strings.TrimSpace(req.ProposalID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "proposal_id is required when source is 'Proposal'")
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
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
			INSERT INTO investment.investment_initiation (
				proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, status, source
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING initiation_id
		`
		var initiationID string
		proposalID := nullIfEmpty(req.ProposalID)
		folioID := nullIfEmpty(req.FolioID)
		dematID := nullIfEmpty(req.DematID)
		status := defaultIfEmpty(req.Status, "Active")

		if err := tx.QueryRow(ctx, insertQ,
			proposalID,
			req.TransactionDate,
			req.EntityName,
			req.SchemeID,
			folioID,
			dematID,
			req.Amount,
			status,
			source,
		).Scan(&initiationID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, initiationID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		response := map[string]any{
			"initiation_id": initiationID,
			"entity_name":   req.EntityName,
			"source":        source,
			"requested":     userEmail,
		}
		if req.ProposalID != "" {
			response["proposal_id"] = req.ProposalID
		}
		api.RespondWithPayload(w, true, "", response)
	}
}

// ---------------------------
// CreateInitiationBulk (multiple JSON rows)
// ---------------------------

func CreateInitiationBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				ProposalID      string  `json:"proposal_id,omitempty"`
				TransactionDate string  `json:"transaction_date"`
				EntityName      string  `json:"entity_name"`
				SchemeID        string  `json:"scheme_id"`
				FolioID         string  `json:"folio_id,omitempty"`
				DematID         string  `json:"demat_id,omitempty"`
				Amount          float64 `json:"amount"`
				Status          string  `json:"status,omitempty"`
				Source          string  `json:"source,omitempty"`
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
			proposalID := strings.TrimSpace(row.ProposalID)
			txnDate := strings.TrimSpace(row.TransactionDate)
			entityName := strings.TrimSpace(row.EntityName)
			schemeID := strings.TrimSpace(row.SchemeID)
			source := defaultIfEmpty(row.Source, "Manual")

			if txnDate == "" || entityName == "" || schemeID == "" || row.Amount <= 0 {
				results = append(results, map[string]interface{}{
					"success": false, "error": "Missing required fields: transaction_date, entity_name, scheme_id, amount",
				})
				continue
			}

			// Either folio_id OR demat_id must be provided
			if strings.TrimSpace(row.FolioID) == "" && strings.TrimSpace(row.DematID) == "" {
				results = append(results, map[string]interface{}{
					"success": false, "error": "Either folio_id or demat_id is required",
				})
				continue
			}

			// if source is Proposal, proposal_id is required
			if strings.ToUpper(source) == "PROPOSAL" && proposalID == "" {
				results = append(results, map[string]interface{}{
					"success": false, "error": "proposal_id is required when source is 'Proposal'",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "TX begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			var initiationID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.investment_initiation (
					proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, status, source
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				RETURNING initiation_id
			`, nullIfEmpty(proposalID), txnDate, entityName, schemeID, nullIfEmpty(row.FolioID), nullIfEmpty(row.DematID), row.Amount, defaultIfEmpty(row.Status, "Active"), source).Scan(&initiationID); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "proposal_id": proposalID, "error": "Insert failed: " + err.Error(),
				})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
			`, initiationID, userEmail); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "initiation_id": initiationID, "error": "Audit insert failed: " + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "initiation_id": initiationID, "error": "Commit failed: " + err.Error(),
				})
				continue
			}

			result := map[string]interface{}{
				"success":       true,
				"initiation_id": initiationID,
				"entity_name":   entityName,
				"source":        source,
			}
			if proposalID != "" {
				result["proposal_id"] = proposalID
			}
			results = append(results, result)
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// UpdateInitiation (single update)
// ---------------------------

func UpdateInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateInitiationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.InitiationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "initiation_id required")
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

		// fetch existing values
		sel := `
			SELECT proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, status, source
			FROM investment.investment_initiation
			WHERE initiation_id=$1
			FOR UPDATE
		`
		var oldVals [9]interface{}
		if err := tx.QueryRow(ctx, sel, req.InitiationID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"proposal_id":      0,
			"transaction_date": 1,
			"entity_name":      2,
			"scheme_id":        3,
			"folio_id":         4,
			"demat_id":         5,
			"amount":           6,
			"status":           7,
			"source":           8,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
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

		q := fmt.Sprintf("UPDATE investment.investment_initiation SET %s, updated_at=now() WHERE initiation_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.InitiationID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
		`, req.InitiationID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"initiation_id": req.InitiationID,
			"requested":     userEmail,
		})
	}
}

// ---------------------------
// UpdateInitiationBulk
// ---------------------------

func UpdateInitiationBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				InitiationID string                 `json:"initiation_id"`
				Fields       map[string]interface{} `json:"fields"`
				Reason       string                 `json:"reason"`
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
			if row.InitiationID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "initiation_id missing"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "initiation_id": row.InitiationID, "error": "tx begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			sel := `
				SELECT proposal_id, transaction_date, entity_name, scheme_id, folio_id, amount, status, source
				FROM investment.investment_initiation WHERE initiation_id=$1 FOR UPDATE`
			var oldVals [8]interface{}
			if err := tx.QueryRow(ctx, sel, row.InitiationID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
			); err != nil {
				results = append(results, map[string]interface{}{"success": false, "initiation_id": row.InitiationID, "error": "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"proposal_id":      0,
				"transaction_date": 1,
				"entity_name":      2,
				"scheme_id":        3,
				"folio_id":         4,
				"amount":           5,
				"status":           6,
				"source":           7,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}

			if len(sets) == 0 {
				results = append(results, map[string]interface{}{"success": false, "initiation_id": row.InitiationID, "error": "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.investment_initiation SET %s, updated_at=now() WHERE initiation_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.InitiationID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{"success": false, "initiation_id": row.InitiationID, "error": "update failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
			`, row.InitiationID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{"success": false, "initiation_id": row.InitiationID, "error": "audit insert failed: " + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{"success": false, "initiation_id": row.InitiationID, "error": "commit failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{"success": true, "initiation_id": row.InitiationID, "requested": userEmail})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// DeleteInitiation
// ---------------------------

func DeleteInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			InitiationIDs []string `json:"initiation_ids"`
			Reason        string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if len(req.InitiationIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "initiation_ids required")
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

		for _, id := range req.InitiationIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninitiation (initiation_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now())
			`, id, req.Reason, requestedBy); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "insert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.InitiationIDs})
	}
}

// ---------------------------
// BulkApproveInitiationActions
// ---------------------------

func BulkApproveInitiationActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			InitiationIDs []string `json:"initiation_ids"`
			Comment       string   `json:"comment"`
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
			SELECT DISTINCT ON (initiation_id) action_id, initiation_id, actiontype, processing_status
			FROM investment.auditactioninitiation
			WHERE initiation_id = ANY($1)
			ORDER BY initiation_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.InitiationIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		var toApprove []string            // action_ids to approve
		var toApproveInitiations []string // initiation_ids corresponding to actions being approved
		var toDeleteActionIDs []string
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, iid, atype, pstatus string
			if err := rows.Scan(&aid, &iid, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == "APPROVED" {
				continue
			}
			if ps == "PENDING_DELETE_APPROVAL" {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, iid)
				continue
			}
			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
				toApproveInitiations = append(toApproveInitiations, iid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, "No approvable actions found", map[string]any{
				"approved_initiation_ids": []string{},
				"deleted_initiations":     []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactioninitiation
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve update failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactioninitiation
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "mark deleted failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.investment_initiation
				SET is_deleted=true, status='Inactive', updated_at=now()
				WHERE initiation_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "master soft-delete failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		// ensure slices are non-nil so JSON marshals empty arrays instead of null
		if toApproveInitiations == nil {
			toApproveInitiations = []string{}
		}
		if deleteMasterIDs == nil {
			deleteMasterIDs = []string{}
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_initiation_ids": toApproveInitiations,
			"deleted_initiations":     deleteMasterIDs,
		})
	}
}

// ---------------------------
// BulkRejectInitiationActions
// ---------------------------

func BulkRejectInitiationActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			InitiationIDs []string `json:"initiation_ids"`
			Comment       string   `json:"comment"`
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
			SELECT DISTINCT ON (initiation_id) action_id, initiation_id, processing_status
			FROM investment.auditactioninitiation
			WHERE initiation_id = ANY($1)
			ORDER BY initiation_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.InitiationIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
		found := map[string]bool{}
		for rows.Next() {
			var aid, iid, ps string
			if err := rows.Scan(&aid, &iid, &ps); err != nil {
				continue
			}
			found[iid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == "APPROVED" {
				cannotReject = append(cannotReject, iid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.InitiationIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for initiation_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved initiation_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactioninitiation
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
// GetApprovedActiveInitiations
// ---------------------------

func GetApprovedActiveInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.initiation_id)
					a.initiation_id, a.actiontype, a.processing_status, a.action_id,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason
				FROM investment.auditactioninitiation a
				ORDER BY a.initiation_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					initiation_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactioninitiation
				GROUP BY initiation_id
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
			FROM investment.investment_initiation m
			LEFT JOIN latest_audit l ON l.initiation_id = m.initiation_id
			LEFT JOIN history h ON h.initiation_id = m.initiation_id
			WHERE UPPER(COALESCE(l.processing_status,'')) = 'APPROVED'
			  AND UPPER(m.status) = 'ACTIVE'
			  AND COALESCE(m.is_deleted, false) = false
			ORDER BY m.transaction_date DESC, m.entity_name;
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
// GetInitiationsWithAudit
// ---------------------------

func GetInitiationsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.initiation_id)
					a.initiation_id, a.actiontype, a.processing_status, a.action_id,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason
				FROM investment.auditactioninitiation a
				ORDER BY a.initiation_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					initiation_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactioninitiation
				GROUP BY initiation_id
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
			FROM investment.investment_initiation m
			LEFT JOIN latest_audit l ON l.initiation_id = m.initiation_id
			LEFT JOIN history h ON h.initiation_id = m.initiation_id
			WHERE COALESCE(m.is_deleted, false) = false
			ORDER BY m.transaction_date DESC, m.entity_name;
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
// Helper functions
// ---------------------------

func parseCashFlowCategoryFile(file multipart.File, ext string) ([][]string, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)

	switch strings.ToLower(ext) {
	case ".csv", "csv":
		br := bufio.NewReader(r)
		peek, _ := br.Peek(1024)
		delimiter := ','
		if bytes.Contains(peek, []byte(";")) {
			delimiter = ';'
		} else if bytes.Contains(peek, []byte("\t")) {
			delimiter = '\t'
		}

		if len(peek) >= 3 && peek[0] == 0xEF && peek[1] == 0xBB && peek[2] == 0xBF {
			br.Discard(3)
		}

		csvr := csv.NewReader(br)
		csvr.Comma = delimiter
		csvr.TrimLeadingSpace = true
		csvr.FieldsPerRecord = -1 // allow variable length rows
		csvr.ReuseRecord = false

		records, err := csvr.ReadAll()
		if err != nil {
			return nil, err
		}

		// remove any empty rows
		clean := make([][]string, 0, len(records))
		for _, row := range records {
			if len(strings.Join(row, "")) == 0 {
				continue
			}
			clean = append(clean, row)
		}

		return clean, nil

	case ".xlsx", ".xls", "xlsx", "xls":
		f, err := excelize.OpenReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil

	default:
		return nil, errors.New("unsupported file type")
	}
}

func getFileExt(filename string) string {
	parts := strings.Split(filename, ".")
	if len(parts) > 1 {
		return strings.ToLower(parts[len(parts)-1])
	}
	return ""
}

func normalizeHeader(row []string) []string {
	normalized := make([]string, len(row))
	for i, h := range row {
		normalized[i] = strings.ToLower(strings.TrimSpace(h))
	}
	return normalized
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func defaultIfEmpty(val, defaultVal string) string {
	if strings.TrimSpace(val) == "" {
		return defaultVal
	}
	return val
}

func stringOrEmpty(val *string) string {
	if val == nil {
		return ""
	}
	return *val
}

// GetInvestmentProposalDetails fetches detailed information about investment proposals
// including folio details, bank account metadata, and scheme/AMC information
func GetInvestmentProposalDetails(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()
		var req struct {
			SchemeID           string `json:"scheme_id"`
			SchemeName         string `json:"scheme_name"`
			InternalSchemeCode string `json:"internal_scheme_code"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		schemeID := strings.TrimSpace(req.SchemeID)
		schemeName := strings.TrimSpace(req.SchemeName)
		internalSchemeCode := strings.TrimSpace(req.InternalSchemeCode)

		if schemeID == "" && schemeName == "" && internalSchemeCode == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id, scheme_name, or internal_scheme_code is required")
			return
		}

		// Query to fetch comprehensive proposal and initiation details
		result, err := fetchProposalDetailsByScheme(ctx, pool, schemeID, schemeName, internalSchemeCode)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("failed to fetch proposal details: %v", err))
			return
		}

		api.RespondWithPayload(w, true, "", result)
	}
}

func fetchProposalDetailsByScheme(ctx context.Context, pool *pgxpool.Pool, schemeID, schemeName, internalSchemeCode string) (map[string]interface{}, error) {
	// Main query to fetch proposal, allocation, scheme, AMC, folio, and demat details
	query := `
		WITH latest_proposal_audit AS (
			SELECT DISTINCT ON (proposal_id)
				proposal_id,
				processing_status
			FROM investment.auditactionproposal
			ORDER BY proposal_id, requested_at DESC
		),
		latest_folio_audit AS (
			SELECT DISTINCT ON (folio_id)
				folio_id,
				processing_status
			FROM investment.auditactionfolio
			ORDER BY folio_id, requested_at DESC
		),
		latest_scheme_audit AS (
			SELECT DISTINCT ON (scheme_id)
				scheme_id,
				processing_status
			FROM investment.auditactionscheme
			ORDER BY scheme_id, requested_at DESC
		),
		latest_amc_audit AS (
			SELECT DISTINCT ON (amc_id)
				amc_id,
				processing_status
			FROM investment.auditactionamc
			ORDER BY amc_id, requested_at DESC
		),
		latest_demat_audit AS (
			SELECT DISTINCT ON (demat_id)
				demat_id,
				processing_status
			FROM investment.auditactiondemat
			ORDER BY demat_id, requested_at DESC
		),
		subscription_account AS (
			SELECT 
				ba.account_number,
				ba.account_nickname,
				ba.account_id,
				b.bank_name,
				b.bank_id,
				COALESCE(e.entity_name, ec.entity_name) AS entity_name,
				STRING_AGG(c.code_type || ':' || c.code_value, ', ') AS clearing_codes
			FROM public.masterbankaccount ba
			LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
			LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
			LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
			LEFT JOIN public.masterclearingcode c ON c.account_id = ba.account_id
			GROUP BY ba.account_number, ba.account_nickname, ba.account_id, 
			         b.bank_name, b.bank_id, 
			         e.entity_name, ec.entity_name
		),
		redemption_account AS (
			SELECT 
				ba.account_number,
				ba.account_nickname,
				ba.account_id,
				b.bank_name,
				b.bank_id,
				COALESCE(e.entity_name, ec.entity_name) AS entity_name,
				STRING_AGG(c.code_type || ':' || c.code_value, ', ') AS clearing_codes
			FROM public.masterbankaccount ba
			LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
			LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
			LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
			LEFT JOIN public.masterclearingcode c ON c.account_id = ba.account_id
			GROUP BY ba.account_number, ba.account_nickname, ba.account_id, 
			         b.bank_name, b.bank_id, 
			         e.entity_name, ec.entity_name
		),
		settlement_account AS (
			SELECT 
				ba.account_number,
				ba.account_nickname,
				ba.account_id,
				b.bank_name,
				b.bank_id,
				COALESCE(e.entity_name, ec.entity_name) AS entity_name,
				STRING_AGG(c.code_type || ':' || c.code_value, ', ') AS clearing_codes
			FROM public.masterbankaccount ba
			LEFT JOIN public.masterbank b ON b.bank_id = ba.bank_id
			LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
			LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
			LEFT JOIN public.masterclearingcode c ON c.account_id = ba.account_id
			GROUP BY ba.account_number, ba.account_nickname, ba.account_id, 
			         b.bank_name, b.bank_id, 
			         e.entity_name, ec.entity_name
		)
		SELECT
			-- Proposal details
			p.proposal_id,
			p.proposal_name,
			p.entity_name AS proposal_entity,
			p.total_amount,
			p.horizon_days,
			p.source AS proposal_source,
			p.status AS proposal_status,
			p.batch_id AS proposal_batch_id,
			lpa.processing_status AS proposal_approval_status,
			
			-- Allocation details (full)
			pa.id AS allocation_id,
			pa.scheme_id AS allocation_scheme_id,
			pa.scheme_internal_code AS allocation_scheme_code,
			pa.amount AS allocation_amount,
			pa.percent AS allocation_percent,
			pa.old_amount AS allocation_old_amount,
			pa.old_percent AS allocation_old_percent,
			pa.policy_status,
			pa.post_trade_holding,
			pa.old_post_trade_holding,
			pa.current_holding,
			pa.old_current_holding,
			
			-- Scheme details
			ms.scheme_id,
			ms.scheme_name,
			ms.isin,
			ms.internal_scheme_code,
			ms.internal_risk_rating,
			ms.erp_gl_account,
			ms.status AS scheme_status,
			ms.source AS scheme_source,
			lsa.processing_status AS scheme_approval_status,
			
			-- AMC details
			ma.amc_id,
			ma.amc_name,
			ma.internal_amc_code,
			ma.primary_contact_name,
			ma.primary_contact_email,
			ma.sebi_registration_no,
			ma.amc_beneficiary_name,
			ma.amc_bank_account_no,
			ma.amc_bank_name,
			ma.amc_bank_ifsc,
			ma.mfu_amc_code,
			ma.cams_amc_code,
			ma.erp_vendor_code,
			ma.status AS amc_status,
			laa.processing_status AS amc_approval_status,
			
			-- Folio details
			mf.folio_id,
			mf.entity_name AS folio_entity,
			mf.folio_number,
			mf.first_holder_name,
			mf.default_subscription_account,
			mf.default_redemption_account,
			mf.status AS folio_status,
			lfa.processing_status AS folio_approval_status,
			
			-- Demat details for the proposal entity
			dm.demat_id,
			dm.entity_name AS demat_entity,
			dm.dp_id,
			dm.depository,
			dm.demat_account_number,
			dm.depository_participant,
			dm.client_id,
			dm.default_settlement_account AS demat_settlement_account,
			dm.status AS demat_status,
			dm.source AS demat_source,
			lda.processing_status AS demat_approval_status,
			
			-- Subscription bank account details
			sub.account_nickname AS sub_account_nickname,
			sub.account_id AS sub_account_id,
			sub.bank_name AS sub_bank_name,
			sub.bank_id AS sub_bank_id,
			sub.entity_name AS sub_entity_name,
			sub.clearing_codes AS sub_clearing_codes,
			
			-- Redemption bank account details
			red.account_nickname AS red_account_nickname,
			red.account_id AS red_account_id,
			red.bank_name AS red_bank_name,
			red.bank_id AS red_bank_id,
			red.entity_name AS red_entity_name,
			red.clearing_codes AS red_clearing_codes,
			
			-- Settlement bank account details (for demat)
			sett.account_nickname AS sett_account_nickname,
			sett.account_id AS sett_account_id,
			sett.bank_name AS sett_bank_name,
			sett.bank_id AS sett_bank_id,
			sett.entity_name AS sett_entity_name,
			sett.clearing_codes AS sett_clearing_codes
			
		FROM investment.investment_proposal_allocation pa
		INNER JOIN investment.investment_proposal p ON p.proposal_id = pa.proposal_id
		INNER JOIN investment.masterscheme ms ON ms.scheme_id = pa.scheme_id
		INNER JOIN investment.masteramc ma ON ma.amc_name = ms.amc_name
		LEFT JOIN investment.folioschememapping fsm ON fsm.scheme_id = ms.scheme_id
		LEFT JOIN investment.masterfolio mf ON mf.folio_id = fsm.folio_id AND mf.amc_name = ma.amc_name
		LEFT JOIN investment.masterdemataccount dm ON dm.entity_name = p.entity_name AND COALESCE(dm.is_deleted, false) = false
		LEFT JOIN latest_proposal_audit lpa ON lpa.proposal_id = p.proposal_id
		LEFT JOIN latest_folio_audit lfa ON lfa.folio_id = mf.folio_id
		LEFT JOIN latest_scheme_audit lsa ON lsa.scheme_id = ms.scheme_id
		LEFT JOIN latest_amc_audit laa ON laa.amc_id = ma.amc_id
		LEFT JOIN latest_demat_audit lda ON lda.demat_id = dm.demat_id
		LEFT JOIN subscription_account sub ON sub.account_number = mf.default_subscription_account
		LEFT JOIN redemption_account red ON red.account_number = mf.default_redemption_account
		LEFT JOIN settlement_account sett ON sett.account_number = dm.default_settlement_account
		WHERE (
			($1 != '' AND pa.scheme_id = $1) OR
			($2 != '' AND ms.scheme_name = $2) OR
			($3 != '' AND ms.internal_scheme_code = $3)
		)
		  AND UPPER(lpa.processing_status) = 'APPROVED'
		  AND (lsa.processing_status IS NULL OR UPPER(lsa.processing_status) = 'APPROVED')
		  AND (laa.processing_status IS NULL OR UPPER(laa.processing_status) = 'APPROVED')
		  AND UPPER(ms.status) = 'ACTIVE'
		  AND UPPER(ma.status) = 'ACTIVE'
		  AND COALESCE(ms.is_deleted, false) = false
		  AND COALESCE(ma.is_deleted, false) = false
		  AND (lfa.processing_status IS NULL OR UPPER(lfa.processing_status) = 'APPROVED')
		  AND (mf.folio_id IS NULL OR (UPPER(mf.status) = 'ACTIVE' AND COALESCE(mf.is_deleted, false) = false))
		  AND (lda.processing_status IS NULL OR UPPER(lda.processing_status) = 'APPROVED')
		  AND (dm.demat_id IS NULL OR (UPPER(dm.status) = 'ACTIVE' AND COALESCE(dm.is_deleted, false) = false))
		ORDER BY p.proposal_name, mf.folio_number, dm.demat_account_number;
	`

	rows, err := pool.Query(ctx, query, schemeID, schemeName, internalSchemeCode)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Parse results
	var proposals []map[string]interface{}
	folioMap := make(map[string][]map[string]interface{})
	dematMap := make(map[string][]map[string]interface{})
	proposalMap := make(map[string]map[string]interface{})

	for rows.Next() {
		var (
			// Proposal
			proposalID, proposalName, proposalEntity, proposalSource, proposalStatus string
			proposalApprovalStatus                                                   string
			proposalBatchID                                                          *string
			totalAmount                                                              float64
			horizonDays                                                              *int

			// Allocation (full)
			allocationID                              int64
			allocationSchemeID, allocationSchemeCode  *string
			allocationAmount, allocationPercent       *float64
			allocationOldAmount, allocationOldPercent *float64
			policyStatus                              *bool
			postTradeHolding, oldPostTradeHolding     *float64
			currentHolding, oldCurrentHolding         *float64

			// Scheme
			schemeID, schemeName, isin, internalSchemeCode, internalRiskRating *string
			erpGLAccount, schemeStatus, schemeSource, schemeApprovalStatus     *string

			// AMC
			amcID, amcName, internalAmcCode, primaryContactName, primaryContactEmail *string
			sebiRegistrationNo, amcBeneficiaryName, amcBankAccountNo, amcBankName    *string
			amcBankIfsc, mfuAmcCode, camsAmcCode, erpVendorCode                      *string
			amcStatus, amcApprovalStatus                                             *string

			// Folio
			folioID, folioEntity, folioNumber, firstHolderName *string
			defaultSubscriptionAcct, defaultRedemptionAcct     *string
			folioStatus, folioApprovalStatus                   *string

			// Demat
			dematID, dematEntity, dpID, depository, dematAccountNumber *string
			depositoryParticipant, clientID, dematSettlementAccount    *string
			dematStatus, dematSource, dematApprovalStatus              *string

			// Subscription account
			subAccountNickname, subAccountID      *string
			subBankName, subBankID, subEntityName *string
			subClearingCodes                      *string

			// Redemption account
			redAccountNickname, redAccountID      *string
			redBankName, redBankID, redEntityName *string
			redClearingCodes                      *string

			// Settlement account (demat)
			settAccountNickname, settAccountID       *string
			settBankName, settBankID, settEntityName *string
			settClearingCodes                        *string
		)

		err := rows.Scan(
			// Proposal
			&proposalID, &proposalName, &proposalEntity, &totalAmount, &horizonDays,
			&proposalSource, &proposalStatus,
			&proposalBatchID, &proposalApprovalStatus,

			// Allocation (full)
			&allocationID, &allocationSchemeID, &allocationSchemeCode,
			&allocationAmount, &allocationPercent,
			&allocationOldAmount, &allocationOldPercent,
			&policyStatus, &postTradeHolding, &oldPostTradeHolding,
			&currentHolding, &oldCurrentHolding,

			// Scheme
			&schemeID, &schemeName, &isin, &internalSchemeCode, &internalRiskRating,
			&erpGLAccount, &schemeStatus, &schemeSource, &schemeApprovalStatus,

			// AMC
			&amcID, &amcName, &internalAmcCode, &primaryContactName, &primaryContactEmail,
			&sebiRegistrationNo, &amcBeneficiaryName, &amcBankAccountNo, &amcBankName,
			&amcBankIfsc, &mfuAmcCode, &camsAmcCode, &erpVendorCode,
			&amcStatus, &amcApprovalStatus,

			// Folio
			&folioID, &folioEntity, &folioNumber, &firstHolderName,
			&defaultSubscriptionAcct, &defaultRedemptionAcct,
			&folioStatus, &folioApprovalStatus,

			// Demat
			&dematID, &dematEntity, &dpID, &depository, &dematAccountNumber,
			&depositoryParticipant, &clientID, &dematSettlementAccount,
			&dematStatus, &dematSource, &dematApprovalStatus,

			// Subscription account
			&subAccountNickname, &subAccountID,
			&subBankName, &subBankID, &subEntityName,
			&subClearingCodes,

			// Redemption account
			&redAccountNickname, &redAccountID,
			&redBankName, &redBankID, &redEntityName,
			&redClearingCodes,

			// Settlement account
			&settAccountNickname, &settAccountID,
			&settBankName, &settBankID, &settEntityName,
			&settClearingCodes,
		)

		if err != nil {
			return nil, fmt.Errorf("row scan failed: %w", err)
		}

		// Build proposal if not exists
		if _, exists := proposalMap[proposalID]; !exists {
			proposalMap[proposalID] = map[string]interface{}{
				"proposal_id":     proposalID,
				"proposal_name":   proposalName,
				"entity_name":     proposalEntity,
				"total_amount":    totalAmount,
				"horizon_days":    horizonDays,
				"source":          proposalSource,
				"status":          proposalStatus,
				"batch_id":        stringOrEmpty(proposalBatchID),
				"approval_status": proposalApprovalStatus,
				"allocation": map[string]interface{}{
					"allocation_id":          allocationID,
					"scheme_id":              stringOrEmpty(allocationSchemeID),
					"scheme_internal_code":   stringOrEmpty(allocationSchemeCode),
					"amount":                 floatOrNil(allocationAmount),
					"percent":                floatOrNil(allocationPercent),
					"old_amount":             floatOrNil(allocationOldAmount),
					"old_percent":            floatOrNil(allocationOldPercent),
					"policy_status":          policyStatus,
					"post_trade_holding":     floatOrNil(postTradeHolding),
					"old_post_trade_holding": floatOrNil(oldPostTradeHolding),
					"current_holding":        floatOrNil(currentHolding),
					"old_current_holding":    floatOrNil(oldCurrentHolding),
				},
				"scheme": map[string]interface{}{
					"scheme_id":            stringOrEmpty(schemeID),
					"scheme_name":          stringOrEmpty(schemeName),
					"isin":                 stringOrEmpty(isin),
					"internal_scheme_code": stringOrEmpty(internalSchemeCode),
					"internal_risk_rating": stringOrEmpty(internalRiskRating),
					"erp_gl_account":       stringOrEmpty(erpGLAccount),
					"status":               stringOrEmpty(schemeStatus),
					"source":               stringOrEmpty(schemeSource),
					"approval_status":      stringOrEmpty(schemeApprovalStatus),
				},
				"amc": map[string]interface{}{
					"amc_id":                stringOrEmpty(amcID),
					"amc_name":              stringOrEmpty(amcName),
					"internal_amc_code":     stringOrEmpty(internalAmcCode),
					"primary_contact_name":  stringOrEmpty(primaryContactName),
					"primary_contact_email": stringOrEmpty(primaryContactEmail),
					"sebi_registration_no":  stringOrEmpty(sebiRegistrationNo),
					"amc_beneficiary_name":  stringOrEmpty(amcBeneficiaryName),
					"amc_bank_account_no":   stringOrEmpty(amcBankAccountNo),
					"amc_bank_name":         stringOrEmpty(amcBankName),
					"amc_bank_ifsc":         stringOrEmpty(amcBankIfsc),
					"mfu_amc_code":          stringOrEmpty(mfuAmcCode),
					"cams_amc_code":         stringOrEmpty(camsAmcCode),
					"erp_vendor_code":       stringOrEmpty(erpVendorCode),
					"status":                stringOrEmpty(amcStatus),
					"approval_status":       stringOrEmpty(amcApprovalStatus),
				},
			}
		}

		// Build folio details
		if folioID != nil && *folioID != "" {
			folioKey := *folioID
			folioDetails := map[string]interface{}{
				"folio_id":          stringOrEmpty(folioID),
				"entity_name":       stringOrEmpty(folioEntity),
				"folio_number":      stringOrEmpty(folioNumber),
				"first_holder_name": stringOrEmpty(firstHolderName),
				"status":            stringOrEmpty(folioStatus),
				"approval_status":   stringOrEmpty(folioApprovalStatus),
				"subscription_account": map[string]interface{}{
					"account_number":   stringOrEmpty(defaultSubscriptionAcct),
					"account_nickname": stringOrEmpty(subAccountNickname),
					"account_id":       stringOrEmpty(subAccountID),
					"bank_name":        stringOrEmpty(subBankName),
					"bank_id":          stringOrEmpty(subBankID),
					"entity_name":      stringOrEmpty(subEntityName),
					"clearing_codes":   stringOrEmpty(subClearingCodes),
				},
				"redemption_account": map[string]interface{}{
					"account_number":   stringOrEmpty(defaultRedemptionAcct),
					"account_nickname": stringOrEmpty(redAccountNickname),
					"account_id":       stringOrEmpty(redAccountID),
					"bank_name":        stringOrEmpty(redBankName),
					"bank_id":          stringOrEmpty(redBankID),
					"entity_name":      stringOrEmpty(redEntityName),
					"clearing_codes":   stringOrEmpty(redClearingCodes),
				},
			}

			// Avoid duplicate folios
			if _, exists := folioMap[folioKey]; !exists {
				folioMap[folioKey] = []map[string]interface{}{folioDetails}
			}
		}

		// Build demat details
		if dematID != nil && *dematID != "" {
			dematKey := *dematID
			dematDetails := map[string]interface{}{
				"demat_id":               stringOrEmpty(dematID),
				"entity_name":            stringOrEmpty(dematEntity),
				"dp_id":                  stringOrEmpty(dpID),
				"depository":             stringOrEmpty(depository),
				"demat_account_number":   stringOrEmpty(dematAccountNumber),
				"depository_participant": stringOrEmpty(depositoryParticipant),
				"client_id":              stringOrEmpty(clientID),
				"status":                 stringOrEmpty(dematStatus),
				"source":                 stringOrEmpty(dematSource),
				"approval_status":        stringOrEmpty(dematApprovalStatus),
				"settlement_account": map[string]interface{}{
					"account_number":   stringOrEmpty(dematSettlementAccount),
					"account_nickname": stringOrEmpty(settAccountNickname),
					"account_id":       stringOrEmpty(settAccountID),
					"bank_name":        stringOrEmpty(settBankName),
					"bank_id":          stringOrEmpty(settBankID),
					"entity_name":      stringOrEmpty(settEntityName),
					"clearing_codes":   stringOrEmpty(settClearingCodes),
				},
			}

			// Avoid duplicate demats
			if _, exists := dematMap[dematKey]; !exists {
				dematMap[dematKey] = []map[string]interface{}{dematDetails}
			}
		}
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("row iteration failed: %w", rows.Err())
	}

	// Consolidate folios and demats into proposal
	for _, proposal := range proposalMap {
		var allFolios []map[string]interface{}
		for _, folios := range folioMap {
			allFolios = append(allFolios, folios...)
		}
		proposal["folios"] = allFolios

		var allDemats []map[string]interface{}
		for _, demats := range dematMap {
			allDemats = append(allDemats, demats...)
		}
		proposal["demats"] = allDemats

		proposals = append(proposals, proposal)
	}

	if len(proposals) == 0 {
		return map[string]interface{}{
			"proposals": []map[string]interface{}{},
			"message":   "No approved active proposals found for the given scheme",
		}, nil
	}

	return map[string]interface{}{
		"proposals": proposals,
		"count":     len(proposals),
	}, nil
}

// Helper function to safely handle nullable floats
func floatOrNil(f *float64) interface{} {
	if f == nil {
		return nil
	}
	return *f
}
