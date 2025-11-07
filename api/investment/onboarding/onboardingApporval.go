package  investment

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
)

// -------------------------
// Bulk Approve/Reject Operations based on batch_id
// -------------------------

type BatchApprovalRequest struct {
	UserID   string   `json:"user_id"`
	BatchIDs []string `json:"batch_ids"` // Array of batch IDs for bulk operations
	BatchID  string   `json:"batch_id,omitempty"` // Single batch ID for backward compatibility
	Action   string   `json:"action"` // "APPROVE" or "REJECT"
	Comment  string   `json:"comment,omitempty"`
}

type BatchApprovalResponse struct {
	Success     bool                     `json:"success"`
	BatchIDs    []string                 `json:"batch_ids"`
	Action      string                   `json:"action"`
	BatchResults map[string]interface{}  `json:"batch_results"` // Results per batch ID
	TotalProcessed int                   `json:"total_processed"`
	Message     string                   `json:"message"`
}

// BulkApproveBatch - Approves all pending audit actions for records created in a specific batch
func BulkApproveBatch(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req BatchApprovalRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, "Invalid JSON: "+err.Error())
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
			api.RespondWithError(w, 401, "Invalid user session")
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
			api.RespondWithError(w, 500, "Failed to commit transaction: "+err.Error())
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
	// Get all AMC IDs for this batch
	amcIDs := []string{}
	rows, err := tx.Query(ctx, `SELECT amc_id FROM investment.masteramc WHERE batch_id = $1`, batchID)
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

	// Get latest pending audit actions for these AMCs
	auditQuery := `
		SELECT DISTINCT ON (amc_id) action_id, amc_id, actiontype, processing_status
		FROM investment.auditactionamc
		WHERE amc_id = ANY($1) AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
		ORDER BY amc_id, requested_at DESC
	`
	
	auditRows, err := tx.Query(ctx, auditQuery, amcIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteAMCIDs := []string{}
	
	for auditRows.Next() {
		var actionID, amcID, actionType, status string
		auditRows.Scan(&actionID, &amcID, &actionType, &status) 
		
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
			} else {
				// For create/edit approval, mark as APPROVED
				actionIDs = append(actionIDs, actionID)
			}
		} else if action == "REJECT" {
			actionIDs = append(actionIDs, actionID)
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
		if err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":     len(actionIDs) + len(deleteAMCIDs),
		"approved_ids":  actionIDs,
		"deleted_ids":   deleteAMCIDs,
	}, nil
}

// processBatchAuditScheme handles Scheme audit approvals/rejections
func processBatchAuditScheme(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	// Get all Scheme IDs for this batch
	schemeIDs := []string{}
	rows, err := tx.Query(ctx, `SELECT scheme_id FROM investment.masterscheme WHERE batch_id = $1`, batchID)
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

	// Get latest pending audit actions
	auditQuery := `
		SELECT DISTINCT ON (scheme_id) action_id, scheme_id, actiontype, processing_status
		FROM investment.auditactionscheme
		WHERE scheme_id = ANY($1) AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
		ORDER BY scheme_id, requested_at DESC
	`
	
	auditRows, err := tx.Query(ctx, auditQuery, schemeIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteSchemeIDs := []string{}
	
	for auditRows.Next() {
		var actionID, schemeID, actionType, status string
		auditRows.Scan(&actionID, &schemeID, &actionType, &status)
		
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
		} else {
			actionIDs = append(actionIDs, actionID)
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
		if err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":     len(actionIDs) + len(deleteSchemeIDs),
		"approved_ids":  actionIDs,
		"deleted_ids":   deleteSchemeIDs,
	}, nil
}

// processBatchAuditDP handles DP audit approvals/rejections
func processBatchAuditDP(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
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
		WHERE dp_id = ANY($1) AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
		ORDER BY dp_id, requested_at DESC
	`
	
	auditRows, err := tx.Query(ctx, auditQuery, dpIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteDPIDs := []string{}
	
	for auditRows.Next() {
		var actionID, dpID, actionType, status string
		auditRows.Scan(&actionID, &dpID, &actionType, &status)
		
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
		} else {
			actionIDs = append(actionIDs, actionID)
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
		if err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":     len(actionIDs) + len(deleteDPIDs),
		"approved_ids":  actionIDs,
		"deleted_ids":   deleteDPIDs,
	}, nil
}

// processBatchAuditDemat handles Demat audit approvals/rejections
func processBatchAuditDemat(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	dematIDs := []string{}
	rows, err := tx.Query(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE batch_id = $1`, batchID)
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
		WHERE demat_id = ANY($1) AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
		ORDER BY demat_id, requested_at DESC
	`
	
	auditRows, err := tx.Query(ctx, auditQuery, dematIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteDematIDs := []string{}
	
	for auditRows.Next() {
		var actionID, dematID, actionType, status string
		auditRows.Scan(&actionID, &dematID, &actionType, &status)
		
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
		} else {
			actionIDs = append(actionIDs, actionID)
		}
	}

	if len(actionIDs) > 0 {
		status := "APPROVED"
		if action == "REJECT" {
			status = "REJECTED"
		}
		
		_, err = tx.Exec(ctx, `
			UPDATE investment.auditactiondemat 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3
			WHERE action_id = ANY($4)
		`, status, userEmail, comment, actionIDs)
		if err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":     len(actionIDs) + len(deleteDematIDs),
		"approved_ids":  actionIDs,
		"deleted_ids":   deleteDematIDs,
	}, nil
}

// processBatchAuditFolio handles Folio audit approvals/rejections
func processBatchAuditFolio(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	folioIDs := []string{}
	rows, err := tx.Query(ctx, `SELECT folio_id FROM investment.masterfolio WHERE batch_id = $1`, batchID)
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
		WHERE folio_id = ANY($1) AND processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
		ORDER BY folio_id, requested_at DESC
	`
	
	auditRows, err := tx.Query(ctx, auditQuery, folioIDs)
	if err != nil {
		return nil, err
	}
	defer auditRows.Close()

	actionIDs := []string{}
	deleteFolioIDs := []string{}
	
	for auditRows.Next() {
		var actionID, folioID, actionType, status string
		auditRows.Scan(&actionID, &folioID, &actionType, &status)
		
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
		} else {
			actionIDs = append(actionIDs, actionID)
		}
	}

	if len(actionIDs) > 0 {
		status := "APPROVED"
		if action == "REJECT" {
			status = "REJECTED"
		}
		
		_, err = tx.Exec(ctx, `
			UPDATE investment.auditactionfolio 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3
			WHERE action_id = ANY($4)
		`, status, userEmail, comment, actionIDs)
		if err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":     len(actionIDs) + len(deleteFolioIDs),
		"approved_ids":  actionIDs,
		"deleted_ids":   deleteFolioIDs,
	}, nil
}
