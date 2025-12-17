package investment

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// -------------------------
// Bulk Approve/Reject Operations based on batch_id
// -------------------------

type BatchApprovalRequest struct {
	UserID   string   `json:"user_id"`
	BatchIDs []string `json:"batch_ids"`          // Array of batch IDs for bulk operations
	BatchID  string   `json:"batch_id,omitempty"` // Single batch ID for backward compatibility
	Action   string   `json:"action"`             // "APPROVE" or "REJECT"
	Comment  string   `json:"comment,omitempty"`
}

type BatchApprovalResponse struct {
	Success        bool                   `json:"success"`
	BatchIDs       []string               `json:"batch_ids"`
	Action         string                 `json:"action"`
	BatchResults   map[string]interface{} `json:"batch_results"` // Results per batch ID
	TotalProcessed int                    `json:"total_processed"`
	Message        string                 `json:"message"`
}

// BulkApproveBatch - Approves all pending audit actions for records created in a specific batch
func BulkApproveBatch(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req BatchApprovalRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		// Handle both single batch_id and bulk batch_ids
		batchIDs := req.BatchIDs
		if len(batchIDs) == 0 && req.BatchID != "" {
			// Backward compatibility: single batch_id
			batchIDs = []string{req.BatchID}
		}

		if len(batchIDs) == 0 {
			api.RespondWithError(w, 400, "batch_ids or batch_id required")
			return
		}

		if req.Action != "APPROVE" && req.Action != "REJECT" {
			api.RespondWithError(w, 400, "action must be APPROVE or REJECT")
			return
		}

		// Get user email from session
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, constants.ErrInvalidSession)
			return
		}

		tx, err := pgxPool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			api.RespondWithError(w, 500, "Failed to begin transaction: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		batchResults := make(map[string]interface{})
		totalProcessed := 0

		// Process each batch
		for _, batchID := range batchIDs {
			batchResult := make(map[string]interface{})

			// Process AMCs
			amcResults, err := processBatchAuditAMC(ctx, tx, batchID, req.Action, userEmail, req.Comment)
			if err != nil {
				api.RespondWithError(w, 500, fmt.Sprintf("Batch %s AMC processing failed: %s", batchID, err.Error()))
				return
			}
			batchResult["amc"] = amcResults

			// Process Schemes
			schemeResults, err := processBatchAuditScheme(ctx, tx, batchID, req.Action, userEmail, req.Comment)
			if err != nil {
				api.RespondWithError(w, 500, fmt.Sprintf("Batch %s Scheme processing failed: %s", batchID, err.Error()))
				return
			}
			batchResult["scheme"] = schemeResults

			// Process DPs
			dpResults, err := processBatchAuditDP(ctx, tx, batchID, req.Action, userEmail, req.Comment)
			if err != nil {
				api.RespondWithError(w, 500, fmt.Sprintf("Batch %s DP processing failed: %s", batchID, err.Error()))
				return
			}
			batchResult["dp"] = dpResults

			// Process Demats
			dematResults, err := processBatchAuditDemat(ctx, tx, batchID, req.Action, userEmail, req.Comment)
			if err != nil {
				api.RespondWithError(w, 500, fmt.Sprintf("Batch %s Demat processing failed: %s", batchID, err.Error()))
				return
			}
			batchResult["demat"] = dematResults

			// Process Folios
			folioResults, err := processBatchAuditFolio(ctx, tx, batchID, req.Action, userEmail, req.Comment)
			if err != nil {
				api.RespondWithError(w, 500, fmt.Sprintf("Batch %s Folio processing failed: %s", batchID, err.Error()))
				return
			}
			batchResult["folio"] = folioResults

			// Count total processed for this batch
			if processed, ok := amcResults["processed"].(int); ok {
				totalProcessed += processed
			}
			if processed, ok := schemeResults["processed"].(int); ok {
				totalProcessed += processed
			}
			if processed, ok := dpResults["processed"].(int); ok {
				totalProcessed += processed
			}
			if processed, ok := dematResults["processed"].(int); ok {
				totalProcessed += processed
			}
			if processed, ok := folioResults["processed"].(int); ok {
				totalProcessed += processed
			}

			batchResults[batchID] = batchResult
		}

		// Update all batch approval statuses in bulk
		batchApprovalStatus := "APPROVED"
		if req.Action == "REJECT" {
			batchApprovalStatus = "REJECTED"
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.onboard_batch 
			SET approval_status = $1, completed_at = now() 
			WHERE batch_id::text = ANY($2)
		`, batchApprovalStatus, batchIDs)
		if err != nil {
			api.RespondWithError(w, 500, "Failed to update batch approval statuses: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrTxCommitFailed+err.Error())
			return
		}

		message := fmt.Sprintf("%d batches %sd successfully", len(batchIDs), strings.ToLower(req.Action))
		if len(batchIDs) == 1 {
			message = fmt.Sprintf("Batch %s %sd successfully", batchIDs[0], strings.ToLower(req.Action))
		}

		response := BatchApprovalResponse{
			Success:        true,
			BatchIDs:       batchIDs,
			Action:         req.Action,
			BatchResults:   batchResults,
			TotalProcessed: totalProcessed,
			Message:        message,
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// Helper functions for processing each audit table

// processBatchAuditAMC handles AMC audit approvals/rejections
func processBatchAuditAMC(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	// Get all AMC IDs for this batch from mapping table
	amcIDs := []string{}
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT amc_id 
		FROM investment.portfolio_onboarding_map 
		WHERE batch_id = $1 AND amc_id IS NOT NULL
	`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		rows.Scan(&id)
		amcIDs = append(amcIDs, id)
	}

	if len(amcIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	// Get latest audit actions for these AMCs (may or may not exist for enriched entities)
	auditQuery := `
		SELECT DISTINCT ON (amc_id) action_id, amc_id, actiontype, processing_status
		FROM investment.auditactionamc
		WHERE amc_id = ANY($1)
		ORDER BY amc_id, requested_at DESC
	`

	auditRows, err := tx.Query(ctx, auditQuery, amcIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteAMCIDs := []string{}
	amcsWithAudit := make(map[string]bool)

	for auditRows.Next() {
		var actionID, amcID, actionType, status string
		auditRows.Scan(&actionID, &amcID, &actionType, &status)
		amcsWithAudit[amcID] = true

		if action == "APPROVE" {
			if status == "PENDING_DELETE_APPROVAL" {
				// For delete approval, mark as DELETED and soft-delete the master record
				_, err = tx.Exec(ctx, `
					UPDATE investment.auditactionamc 
					SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
					WHERE action_id=$3
				`, userEmail, comment, actionID)
				if err != nil {
					return nil, err
				}

				// Soft delete the master record
				_, err = tx.Exec(ctx, `
					UPDATE investment.masteramc 
					SET is_deleted=true, status='Inactive' 
					WHERE amc_id=$1
				`, amcID)
				if err != nil {
					return nil, err
				}

				deleteAMCIDs = append(deleteAMCIDs, amcID)
			} else if status == "PENDING_APPROVAL" || status == "PENDING_EDIT_APPROVAL" {
				// For create/edit approval, mark as APPROVED
				actionIDs = append(actionIDs, actionID)
			}
		} else if action == "REJECT" {
			if status == "PENDING_APPROVAL" || status == "PENDING_EDIT_APPROVAL" || status == "PENDING_DELETE_APPROVAL" {
				actionIDs = append(actionIDs, actionID)
			}
		}
	}

	// For enriched entities without audit records, create approval audit if action is APPROVE
	if action == "APPROVE" {
		for _, amcID := range amcIDs {
			if !amcsWithAudit[amcID] {
				// Create an approval audit record for enriched entities
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactionamc (amc_id, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment)
					VALUES ($1, 'CREATE', 'APPROVED', $2, now(), $2, now(), $3)
				`, amcID, userEmail, constants.ErrAutoApprovedEnrichedEntity)
				if err != nil {
					return nil, err
				}
				actionIDs = append(actionIDs, amcID) // Use amcID as placeholder for count
			}
		}
	}

	// Update audit records for approve/reject (non-delete actions)
	if len(actionIDs) > 0 {
		status := "APPROVED"
		if action == "REJECT" {
			status = "REJECTED"
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.auditactionamc 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3
			WHERE action_id = ANY($4)
		`, status, userEmail, comment, actionIDs)
		// Ignore errors for enriched entities without audit
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteAMCIDs),
		"approved_ids": actionIDs,
		"deleted_ids":  deleteAMCIDs,
	}, nil
}

// processBatchAuditScheme handles Scheme audit approvals/rejections
func processBatchAuditScheme(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	// Get all Scheme IDs for this batch from mapping table
	schemeIDs := []string{}
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT scheme_id 
		FROM investment.portfolio_onboarding_map 
		WHERE batch_id = $1 AND scheme_id IS NOT NULL
	`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		rows.Scan(&id)
		schemeIDs = append(schemeIDs, id)
	}

	if len(schemeIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	// Get latest audit actions
	auditQuery := `
		SELECT DISTINCT ON (scheme_id) action_id, scheme_id, actiontype, processing_status
		FROM investment.auditactionscheme
		WHERE scheme_id = ANY($1)
		ORDER BY scheme_id, requested_at DESC
	`

	auditRows, err := tx.Query(ctx, auditQuery, schemeIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteSchemeIDs := []string{}
	schemesWithAudit := make(map[string]bool)

	for auditRows.Next() {
		var actionID, schemeID, actionType, status string
		auditRows.Scan(&actionID, &schemeID, &actionType, &status)
		schemesWithAudit[schemeID] = true

		if action == "APPROVE" && status == "PENDING_DELETE_APPROVAL" {
			_, err = tx.Exec(ctx, `
				UPDATE investment.auditactionscheme 
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id=$3
			`, userEmail, comment, actionID)
			if err != nil {
				return nil, err
			}

			_, err = tx.Exec(ctx, `
				UPDATE investment.masterscheme 
				SET is_deleted=true, status='Inactive' 
				WHERE scheme_id=$1
			`, schemeID)
			if err != nil {
				return nil, err
			}

			deleteSchemeIDs = append(deleteSchemeIDs, schemeID)
		} else if status == "PENDING_APPROVAL" || status == "PENDING_EDIT_APPROVAL" || (action == "REJECT" && status == "PENDING_DELETE_APPROVAL") {
			actionIDs = append(actionIDs, actionID)
		}
	}

	// For enriched entities without audit records, create approval audit if action is APPROVE
	if action == "APPROVE" {
		for _, schemeID := range schemeIDs {
			if !schemesWithAudit[schemeID] {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment)
					VALUES ($1, 'CREATE', 'APPROVED', $2, now(), $2, now(), $3)
				`, schemeID, userEmail, constants.ErrAutoApprovedEnrichedEntity)
				if err != nil {
					return nil, err
				}
				actionIDs = append(actionIDs, schemeID)
			}
		}
	}

	if len(actionIDs) > 0 {
		status := "APPROVED"
		if action == "REJECT" {
			status = "REJECTED"
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.auditactionscheme 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3
			WHERE action_id = ANY($4)
		`, status, userEmail, comment, actionIDs)
		// Ignore errors for enriched entities
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteSchemeIDs),
		"approved_ids": actionIDs,
		"deleted_ids":  deleteSchemeIDs,
	}, nil
}

// processBatchAuditDP handles DP audit approvals/rejections
func processBatchAuditDP(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	// Get all DP IDs for this batch from mapping table
	// Note: DPs don't have a dedicated column, so we query from master table with batch_id
	dpIDs := []string{}
	rows, err := tx.Query(ctx, `SELECT dp_id FROM investment.masterdepositoryparticipant WHERE batch_id = $1`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		rows.Scan(&id)
		dpIDs = append(dpIDs, id)
	}

	if len(dpIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	auditQuery := `
		SELECT DISTINCT ON (dp_id) action_id, dp_id, actiontype, processing_status
		FROM investment.auditactiondp
		WHERE dp_id = ANY($1)
		ORDER BY dp_id, requested_at DESC
	`

	auditRows, err := tx.Query(ctx, auditQuery, dpIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteDPIDs := []string{}
	dpsWithAudit := make(map[string]bool)

	for auditRows.Next() {
		var actionID, dpID, actionType, status string
		auditRows.Scan(&actionID, &dpID, &actionType, &status)
		dpsWithAudit[dpID] = true

		if action == "APPROVE" && status == "PENDING_DELETE_APPROVAL" {
			_, err = tx.Exec(ctx, `
				UPDATE investment.auditactiondp 
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id=$3
			`, userEmail, comment, actionID)
			if err != nil {
				return nil, err
			}

			_, err = tx.Exec(ctx, `
				UPDATE investment.masterdepositoryparticipant 
				SET is_deleted=true, status='Inactive' 
				WHERE dp_id=$1
			`, dpID)
			if err != nil {
				return nil, err
			}

			deleteDPIDs = append(deleteDPIDs, dpID)
		} else if status == "PENDING_APPROVAL" || status == "PENDING_EDIT_APPROVAL" || (action == "REJECT" && status == "PENDING_DELETE_APPROVAL") {
			actionIDs = append(actionIDs, actionID)
		}
	}

	if action == "APPROVE" {
		for _, dpID := range dpIDs {
			if !dpsWithAudit[dpID] {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment)
					VALUES ($1, 'CREATE', 'APPROVED', $2, now(), $2, now(), $3)
				`, dpID, userEmail, constants.ErrAutoApprovedEnrichedEntity)
				if err != nil {
					return nil, err
				}
				actionIDs = append(actionIDs, dpID)
			}
		}
	}

	if len(actionIDs) > 0 {
		status := "APPROVED"
		if action == "REJECT" {
			status = "REJECTED"
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.auditactiondp 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3
			WHERE action_id = ANY($4)
		`, status, userEmail, comment, actionIDs)
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteDPIDs),
		"approved_ids": actionIDs,
		"deleted_ids":  deleteDPIDs,
	}, nil
}

// processBatchAuditDemat handles Demat audit approvals/rejections
func processBatchAuditDemat(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	// Get all Demat IDs for this batch from mapping table
	dematIDs := []string{}
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT demat_id 
		FROM investment.portfolio_onboarding_map 
		WHERE batch_id = $1 AND demat_id IS NOT NULL
	`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		rows.Scan(&id)
		dematIDs = append(dematIDs, id)
	}

	if len(dematIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	auditQuery := `
		SELECT DISTINCT ON (demat_id) action_id, demat_id, actiontype, processing_status
		FROM investment.auditactiondemat
		WHERE demat_id = ANY($1)
		ORDER BY demat_id, requested_at DESC
	`

	auditRows, err := tx.Query(ctx, auditQuery, dematIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteDematIDs := []string{}
	dematsWithAudit := make(map[string]bool)

	for auditRows.Next() {
		var actionID, dematID, actionType, status string
		auditRows.Scan(&actionID, &dematID, &actionType, &status)
		dematsWithAudit[dematID] = true

		if action == "APPROVE" && status == "PENDING_DELETE_APPROVAL" {
			_, err = tx.Exec(ctx, `
				UPDATE investment.auditactiondemat 
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id=$3
			`, userEmail, comment, actionID)
			if err != nil {
				return nil, err
			}

			_, err = tx.Exec(ctx, `
				UPDATE investment.masterdemataccount 
				SET is_deleted=true, status='Inactive' 
				WHERE demat_id=$1
			`, dematID)
			if err != nil {
				return nil, err
			}

			deleteDematIDs = append(deleteDematIDs, dematID)
		} else if status == "PENDING_APPROVAL" || status == "PENDING_EDIT_APPROVAL" || (action == "REJECT" && status == "PENDING_DELETE_APPROVAL") {
			actionIDs = append(actionIDs, actionID)
		}
	}

	if action == "APPROVE" {
		for _, dematID := range dematIDs {
			if !dematsWithAudit[dematID] {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment)
					VALUES ($1, 'CREATE', 'APPROVED', $2, now(), $2, now(), $3)
				`, dematID, userEmail, constants.ErrAutoApprovedEnrichedEntity)
				if err != nil {
					return nil, err
				}
				actionIDs = append(actionIDs, dematID)
			}
		}
	}

	if len(actionIDs) > 0 {
		status := "APPROVED"
		if action == "REJECT" {
			status = "REJECTED"
		}

		_, _ = tx.Exec(ctx, `
			UPDATE investment.auditactiondemat 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3
			WHERE action_id = ANY($4)
		`, status, userEmail, comment, actionIDs)
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteDematIDs),
		"approved_ids": actionIDs,
		"deleted_ids":  deleteDematIDs,
	}, nil
}

// processBatchAuditFolio handles Folio audit approvals/rejections
func processBatchAuditFolio(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	// Get all Folio IDs for this batch from mapping table
	folioIDs := []string{}
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT folio_id 
		FROM investment.portfolio_onboarding_map 
		WHERE batch_id = $1 AND folio_id IS NOT NULL
	`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		rows.Scan(&id)
		folioIDs = append(folioIDs, id)
	}

	if len(folioIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	auditQuery := `
		SELECT DISTINCT ON (folio_id) action_id, folio_id, actiontype, processing_status
		FROM investment.auditactionfolio
		WHERE folio_id = ANY($1)
		ORDER BY folio_id, requested_at DESC
	`

	auditRows, err := tx.Query(ctx, auditQuery, folioIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteFolioIDs := []string{}
	foliosWithAudit := make(map[string]bool)

	for auditRows.Next() {
		var actionID, folioID, actionType, status string
		auditRows.Scan(&actionID, &folioID, &actionType, &status)
		foliosWithAudit[folioID] = true

		if action == "APPROVE" && status == "PENDING_DELETE_APPROVAL" {
			_, err = tx.Exec(ctx, `
				UPDATE investment.auditactionfolio 
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id=$3
			`, userEmail, comment, actionID)
			if err != nil {
				return nil, err
			}

			_, err = tx.Exec(ctx, `
				UPDATE investment.masterfolio 
				SET is_deleted=true, status='Inactive' 
				WHERE folio_id=$1
			`, folioID)
			if err != nil {
				return nil, err
			}

			deleteFolioIDs = append(deleteFolioIDs, folioID)
		} else if status == "PENDING_APPROVAL" || status == "PENDING_EDIT_APPROVAL" || (action == "REJECT" && status == "PENDING_DELETE_APPROVAL") {
			actionIDs = append(actionIDs, actionID)
		}
	}

	if action == "APPROVE" {
		for _, folioID := range folioIDs {
			if !foliosWithAudit[folioID] {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment)
					VALUES ($1, 'CREATE', 'APPROVED', $2, now(), $2, now(), $3)
				`, folioID, userEmail, constants.ErrAutoApprovedEnrichedEntity)
				if err != nil {
					return nil, err
				}
				actionIDs = append(actionIDs, folioID)
			}
		}
	}

	if len(actionIDs) > 0 {
		status := "APPROVED"
		if action == "REJECT" {
			status = "REJECTED"
		}

		_, _ = tx.Exec(ctx, `
			UPDATE investment.auditactionfolio 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3
			WHERE action_id = ANY($4)
		`, status, userEmail, comment, actionIDs)
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteFolioIDs),
		"approved_ids": actionIDs,
		"deleted_ids":  deleteFolioIDs,
	}, nil
}
