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
					proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, source
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				RETURNING initiation_id
			`
		var initiationID string
		proposalID := nullIfEmpty(req.ProposalID)
		folioID := nullIfEmpty(req.FolioID)
		dematID := nullIfEmpty(req.DematID)
		if err := tx.QueryRow(ctx, insertQ,
			proposalID,
			req.TransactionDate,
			req.EntityName,
			req.SchemeID,
			folioID,
			dematID,
			req.Amount,
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

		// if this initiation is tied to a proposal+scheme, mark the allocation's initiation_status
		if strings.TrimSpace(req.ProposalID) != "" && strings.TrimSpace(req.SchemeID) != "" {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.investment_proposal_allocation
				SET initiation_status = true, updated_at = now()
				WHERE proposal_id = $1 AND scheme_id = $2
			`, req.ProposalID, req.SchemeID); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to update allocation initiation_status: "+err.Error())
				return
			}
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
					proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, source
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				RETURNING initiation_id
			`, nullIfEmpty(proposalID), txnDate, entityName, schemeID, nullIfEmpty(row.FolioID), nullIfEmpty(row.DematID), row.Amount, source).Scan(&initiationID); err != nil {
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

			// mark allocation initiation_status true for this row if linked to a proposal+scheme
			if proposalID != "" && schemeID != "" {
				if _, err := pgxPool.Exec(ctx, `
					UPDATE investment.investment_proposal_allocation
					SET initiation_status = true, updated_at = now()
					WHERE proposal_id = $1 AND scheme_id = $2
				`, proposalID, schemeID); err != nil {
					// attach error to result but continue processing other rows
					result["warning"] = "failed to flag allocation initiation_status: " + err.Error()
				}
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
			SELECT proposal_id, transaction_date, entity_name, scheme_id, folio_id, demat_id, amount, source
			FROM investment.investment_initiation
			WHERE initiation_id=$1
			FOR UPDATE
		`
		var oldVals [8]interface{}
		if err := tx.QueryRow(ctx, sel, req.InitiationID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
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
			"source":           7,
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
				SELECT proposal_id, transaction_date, entity_name, scheme_id, folio_id, amount, source
				FROM investment.investment_initiation WHERE initiation_id=$1 FOR UPDATE`
			var oldVals [7]interface{}
			if err := tx.QueryRow(ctx, sel, row.InitiationID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6],
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
				"source":           6,
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
				COALESCE(s.scheme_id::text, m.scheme_id::text) AS scheme_id,
				COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
				COALESCE(s.amc_name,'') AS amc_name,
				COALESCE(f.folio_number,'') AS folio_number,
				COALESCE(f.folio_id::text,'') AS folio_id,
				COALESCE(d.default_settlement_account,'') AS demat_number,
				COALESCE(d.demat_id::text,'') AS demat_id,
				DATE_PART('day', now()::timestamp - m.transaction_date::timestamp)::int AS age_days,
				COALESCE(m.amount,0) AS gross_investment_amount,
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
				COALESCE(nav.nav_value,0) AS nav,
				TO_CHAR(nav.nav_date,'YYYY-MM-DD') AS applicable_nav_date
			FROM investment.investment_initiation m
			LEFT JOIN latest_audit l ON l.initiation_id = m.initiation_id
			LEFT JOIN history h ON h.initiation_id = m.initiation_id
			-- allow flexible matching: the initiation column may contain either the id or the human-friendly value
			-- LEFT JOIN investment.masterscheme s ON (s.scheme_id::text = m.scheme_id OR s.scheme_name = m.scheme_id)
			LEFT JOIN investment.masterscheme s ON (
       s.scheme_id::text = m.scheme_id
    OR s.scheme_name = m.scheme_id
    OR s.internal_scheme_code = m.scheme_id
    OR s.isin = m.scheme_id
)

			LEFT JOIN investment.masterfolio f ON (f.folio_id::text = m.folio_id OR f.folio_number = m.folio_id)
			LEFT JOIN investment.masterdemataccount d ON (
				d.demat_id::text = m.demat_id OR
				d.default_settlement_account = m.demat_id OR
				d.demat_account_number = m.demat_id
			)
			LEFT JOIN LATERAL (
				SELECT ans.nav_value, ans.nav_date
				FROM investment.amfi_nav_staging ans
				WHERE (
					(ans.scheme_code::text = s.internal_scheme_code) OR
					(ans.isin_div_payout_growth = s.isin) OR
					(ans.scheme_name = s.scheme_name)
				)
				ORDER BY ans.nav_date DESC, ans.file_date DESC
				LIMIT 1
			) nav ON true
			WHERE UPPER(COALESCE(l.processing_status,'')) = 'APPROVED'
					AND COALESCE(m.is_deleted, false) = false
					AND m.initiation_id NOT IN (
						SELECT initiation_id FROM investment.investment_confirmation 
					)
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
				COALESCE(s.scheme_id::text, m.scheme_id::text) AS scheme_id,
				COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
				COALESCE(s.amc_name,'') AS amc_name,
				COALESCE(f.folio_number,'') AS folio_number,
				COALESCE(f.folio_id::text,'') AS folio_id,
				COALESCE(d.default_settlement_account,'') AS demat_number,
				COALESCE(d.demat_id::text,'') AS demat_id,
				DATE_PART('day', now()::timestamp - m.transaction_date::timestamp)::int AS age_days,
				COALESCE(m.amount,0) AS gross_investment_amount,
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
				COALESCE(nav.nav_value,0) AS nav,
				TO_CHAR(nav.nav_date,'YYYY-MM-DD') AS applicable_nav_date
			FROM investment.investment_initiation m
			LEFT JOIN latest_audit l ON l.initiation_id = m.initiation_id
			LEFT JOIN history h ON h.initiation_id = m.initiation_id
			-- allow flexible matching: the initiation column may contain either the id or the human-friendly value
			LEFT JOIN investment.masterscheme s ON (
			   s.scheme_id::text = m.scheme_id
			OR s.scheme_name = m.scheme_id
			OR s.internal_scheme_code = m.scheme_id
			OR s.isin = m.scheme_id
		)

			LEFT JOIN investment.masterfolio f ON (f.folio_id::text = m.folio_id OR f.folio_number = m.folio_id)
			LEFT JOIN investment.masterdemataccount d ON (
				d.demat_id::text = m.demat_id OR
				d.default_settlement_account = m.demat_id OR
				d.demat_account_number = m.demat_id
			)
			LEFT JOIN LATERAL (
				SELECT ans.nav_value, ans.nav_date
				FROM investment.amfi_nav_staging ans
				WHERE (
					(ans.scheme_code::text = s.internal_scheme_code) OR
					(ans.isin_div_payout_growth = s.isin) OR
					(ans.scheme_name = s.scheme_name)
				)
				ORDER BY ans.nav_date DESC, ans.file_date DESC
				LIMIT 1
			) nav ON true
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
