package investment

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	jobs "CimplrCorpSaas/internal/jobs/investment"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func distinctPOMIDs(ctx context.Context, tx pgx.Tx, batchID, column string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT %s
		FROM investment.portfolio_onboarding_map
		WHERE batch_id::text = $1 AND COALESCE(%s, '') <> ''`, column, column)
	rows, err := tx.Query(ctx, query, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func distinctDPIDsFromPOM(ctx context.Context, tx pgx.Tx, batchID string) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT m.dp_id
		FROM investment.portfolio_onboarding_map pom
		JOIN investment.masterdepositoryparticipant m ON m.dp_name = pom.entity_name
		WHERE pom.batch_id::text = $1
		  AND pom.folio_id IS NULL AND pom.demat_id IS NULL
		  AND pom.amc_id IS NULL AND pom.scheme_id IS NULL
		  AND COALESCE(m.dp_id, '') <> ''`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

type batchAuditRow struct {
	actionID   string
	entityID   string
	actionType string
	status     string
}

func queryLatestAudits(ctx context.Context, tx pgx.Tx, auditTable, entityIDCol string, entityIDs []string) ([]batchAuditRow, error) {
	q := fmt.Sprintf(`
		SELECT DISTINCT ON (%[1]s) action_id, %[1]s, actiontype, processing_status
		FROM investment.%[2]s
		WHERE %[1]s = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')
		ORDER BY %[1]s, requested_at DESC`, entityIDCol, auditTable)
	rows, err := tx.Query(ctx, q, entityIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []batchAuditRow
	for rows.Next() {
		var r batchAuditRow
		if err := rows.Scan(&r.actionID, &r.entityID, &r.actionType, &r.status); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// -------------------------
// Bulk Approve/Reject Operations based on batch_id
// -------------------------

type BatchApprovalRequest struct {
	UserID   string   `json:"user_id"`
	BatchIDs []string `json:"batch_ids"`          // Array of batch IDs for bulk operations
	BatchID  string   `json:"batch_id,omitempty"` // Single batch ID for backward compatibility
	Action   string   `json:"action"`             // constants.AuditActionApprove or constants.AuditActionReject
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

		if req.Action != constants.AuditActionApprove && req.Action != constants.AuditActionReject {
			api.RespondWithError(w, 400, "action must be APPROVE or REJECT")
			return
		}

		userEmail := api.GetUserNameFromCtx(r.Context())
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
		batchApprovalStatus := constants.StatusApproved
		if req.Action == constants.AuditActionReject {
			batchApprovalStatus = constants.StatusRejected
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

		// Cascade status to onboard_transaction
		_, err = tx.Exec(ctx, `
			UPDATE investment.onboard_transaction
			SET approval_status = $1
			WHERE batch_id::text = ANY($2)
		`, batchApprovalStatus, batchIDs)
		if err != nil {
			api.RespondWithError(w, 500, "Failed to update transaction approval statuses: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrTxCommitFailed+err.Error())
			return
		}

		// Fire notifications asynchronously for each approved/rejected batch
		actionCopy := req.Action
		userEmailCopy := userEmail
		batchIDsCopy := append([]string(nil), batchIDs...)
		poolRef := pgxPool
		go func() {
			for _, bid := range batchIDsCopy {
				BuildOnboardApprovalNotifPayload(context.Background(), poolRef, bid, actionCopy, userEmailCopy)
			}
			
			// Refresh portfolio snapshot in background if APPROVED
			if actionCopy == constants.AuditActionApprove {
				if err := jobs.RefreshPortfolioSnapshotsJob(context.Background(), poolRef, nil); err != nil {
					logger.LogError("Background portfolio snapshot refresh failed after onboard approval: %v", err)
				}
			}
		}()

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
	amcIDs, err := distinctPOMIDs(ctx, tx, batchID, "amc_id")
	if err != nil {
		return nil, err
	}

	if len(amcIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	// Get latest audit actions for these AMCs (may or may not exist for enriched entities)
	audits, err := queryLatestAudits(ctx, tx, "auditactionamc", "amc_id", amcIDs)
	if err != nil {
		return nil, err
	}

	actionIDs := []string{}
	deleteAMCIDs := []string{}
	amcsWithAudit := make(map[string]bool)
	autoApproved := 0

	for _, row := range audits {
		amcsWithAudit[row.entityID] = true

		if action == constants.AuditActionApprove {
			if row.status == constants.StatusPendingDeleteApproval {
				if _, err = tx.Exec(ctx, `
					UPDATE investment.auditactionamc 
					SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
					WHERE action_id=$4
				`, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), row.actionID); err != nil {
					return nil, err
				}

				if _, err = tx.Exec(ctx, `
					UPDATE investment.masteramc 
					SET is_deleted=true, status='Inactive' 
					WHERE amc_id=$1
				`, row.entityID); err != nil {
					return nil, err
				}

				deleteAMCIDs = append(deleteAMCIDs, row.entityID)
			} else if row.status == constants.StatusPendingApproval || row.status == constants.StatusPendingEditApproval {
				actionIDs = append(actionIDs, row.actionID)
			}
		} else if action == constants.AuditActionReject {
			if row.status == constants.StatusPendingApproval || row.status == constants.StatusPendingEditApproval || row.status == constants.StatusPendingDeleteApproval {
				actionIDs = append(actionIDs, row.actionID)
			}
		}
	}

	// For enriched entities without audit records, create approval audit if action is APPROVE
	if action == constants.AuditActionApprove {
		for _, amcID := range amcIDs {
			if !amcsWithAudit[amcID] {
				// Create an approval audit record for enriched entities
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactionamc (amc_id, actiontype, processing_status, requested_by, requested_at, requested_ip, checker_by, checker_at, checker_comment, checker_ip)
					VALUES ($1, $4, $5, $2, now(), $6, $2, now(), $3, $6)
				`, amcID, userEmail, constants.ErrAutoApprovedEnrichedEntity, constants.AuditActionCreate, constants.StatusApproved, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
				if err != nil {
					return nil, err
				}
				autoApproved++
			}
		}
	}

	// Update audit records for approve/reject (non-delete actions)
	if len(actionIDs) > 0 {
		status := constants.StatusApproved
		if action == constants.AuditActionReject {
			status = constants.StatusRejected
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.auditactionamc 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3, checker_ip=$4
			WHERE action_id = ANY($5)
		`, status, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), actionIDs); err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteAMCIDs) + autoApproved,
		"approved_ids": actionIDs,
		"deleted_ids":  deleteAMCIDs,
	}, nil
}

// processBatchAuditScheme handles Scheme audit approvals/rejections
func processBatchAuditScheme(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	schemeIDs, err := distinctPOMIDs(ctx, tx, batchID, "scheme_id")
	if err != nil {
		return nil, err
	}

	if len(schemeIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	audits, err := queryLatestAudits(ctx, tx, "auditactionscheme", "scheme_id", schemeIDs)
	if err != nil {
		return nil, err
	}

	actionIDs := []string{}
	deleteSchemeIDs := []string{}
	schemesWithAudit := make(map[string]bool)
	autoApproved := 0

	for _, row := range audits {
		schemesWithAudit[row.entityID] = true

		if action == constants.AuditActionApprove && row.status == constants.StatusPendingDeleteApproval {
			if _, err = tx.Exec(ctx, `
				UPDATE investment.auditactionscheme 
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id=$4
			`, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), row.actionID); err != nil {
				return nil, err
			}

			if _, err = tx.Exec(ctx, `
				UPDATE investment.masterscheme 
				SET is_deleted=true, status='Inactive' 
				WHERE scheme_id=$1
			`, row.entityID); err != nil {
				return nil, err
			}

			deleteSchemeIDs = append(deleteSchemeIDs, row.entityID)
		} else if row.status == constants.StatusPendingApproval || row.status == constants.StatusPendingEditApproval || (action == constants.AuditActionReject && row.status == constants.StatusPendingDeleteApproval) {
			actionIDs = append(actionIDs, row.actionID)
		}
	}

	// For enriched entities without audit records, create approval audit if action is APPROVE
	if action == constants.AuditActionApprove {
		for _, schemeID := range schemeIDs {
			if !schemesWithAudit[schemeID] {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactionscheme (scheme_id, actiontype, processing_status, requested_by, requested_at, requested_ip, checker_by, checker_at, checker_comment, checker_ip)
					VALUES ($1, $4, $5, $2, now(), $6, $2, now(), $3, $6)
				`, schemeID, userEmail, constants.ErrAutoApprovedEnrichedEntity, constants.AuditActionCreate, constants.StatusApproved, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
				if err != nil {
					return nil, err
				}
				autoApproved++
			}
		}
	}

	if len(actionIDs) > 0 {
		status := constants.StatusApproved
		if action == constants.AuditActionReject {
			status = constants.StatusRejected
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.auditactionscheme 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3, checker_ip=$4
			WHERE action_id = ANY($5)
		`, status, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), actionIDs); err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteSchemeIDs) + autoApproved,
		"approved_ids": actionIDs,
		"deleted_ids":  deleteSchemeIDs,
	}, nil
}

// processBatchAuditDP handles DP audit approvals/rejections
func processBatchAuditDP(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	dpIDs, err := distinctDPIDsFromPOM(ctx, tx, batchID)
	if err != nil {
		return nil, err
	}

	if len(dpIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	audits, err := queryLatestAudits(ctx, tx, "auditactiondp", "dp_id", dpIDs)
	if err != nil {
		return nil, err
	}

	actionIDs := []string{}
	deleteDPIDs := []string{}
	dpsWithAudit := make(map[string]bool)
	autoApproved := 0

	for _, row := range audits {
		dpsWithAudit[row.entityID] = true

		if action == constants.AuditActionApprove && row.status == constants.StatusPendingDeleteApproval {
			if _, err = tx.Exec(ctx, `
				UPDATE investment.auditactiondp 
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id=$4
			`, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), row.actionID); err != nil {
				return nil, err
			}

			if _, err = tx.Exec(ctx, `
				UPDATE investment.masterdepositoryparticipant 
				SET is_deleted=true, status='Inactive' 
				WHERE dp_id=$1
			`, row.entityID); err != nil {
				return nil, err
			}

			deleteDPIDs = append(deleteDPIDs, row.entityID)
		} else if row.status == constants.StatusPendingApproval || row.status == constants.StatusPendingEditApproval || (action == constants.AuditActionReject && row.status == constants.StatusPendingDeleteApproval) {
			actionIDs = append(actionIDs, row.actionID)
		}
	}

	if action == constants.AuditActionApprove {
		for _, dpID := range dpIDs {
			if !dpsWithAudit[dpID] {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactiondp (dp_id, actiontype, processing_status, requested_by, requested_at, requested_ip, checker_by, checker_at, checker_comment, checker_ip)
					VALUES ($1, $4, $5, $2, now(), $6, $2, now(), $3, $6)
				`, dpID, userEmail, constants.ErrAutoApprovedEnrichedEntity, constants.AuditActionCreate, constants.StatusApproved, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
				if err != nil {
					return nil, err
				}
				autoApproved++
			}
		}
	}

	if len(actionIDs) > 0 {
		status := constants.StatusApproved
		if action == constants.AuditActionReject {
			status = constants.StatusRejected
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.auditactiondp 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3, checker_ip=$4
			WHERE action_id = ANY($5)
		`, status, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), actionIDs); err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteDPIDs) + autoApproved,
		"approved_ids": actionIDs,
		"deleted_ids":  deleteDPIDs,
	}, nil
}

// processBatchAuditDemat handles Demat audit approvals/rejections
func processBatchAuditDemat(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	dematIDs, err := distinctPOMIDs(ctx, tx, batchID, "demat_id")
	if err != nil {
		return nil, err
	}

	if len(dematIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	audits, err := queryLatestAudits(ctx, tx, "auditactiondemat", "demat_id", dematIDs)
	if err != nil {
		return nil, err
	}

	actionIDs := []string{}
	deleteDematIDs := []string{}
	dematsWithAudit := make(map[string]bool)
	autoApproved := 0

	for _, row := range audits {
		dematsWithAudit[row.entityID] = true

		if action == constants.AuditActionApprove && row.status == constants.StatusPendingDeleteApproval {
			if _, err = tx.Exec(ctx, `
				UPDATE investment.auditactiondemat 
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id=$4
			`, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), row.actionID); err != nil {
				return nil, err
			}

			if _, err = tx.Exec(ctx, `
				UPDATE investment.masterdemataccount 
				SET is_deleted=true, status='Inactive' 
				WHERE demat_id=$1
			`, row.entityID); err != nil {
				return nil, err
			}

			deleteDematIDs = append(deleteDematIDs, row.entityID)
		} else if row.status == constants.StatusPendingApproval || row.status == constants.StatusPendingEditApproval || (action == constants.AuditActionReject && row.status == constants.StatusPendingDeleteApproval) {
			actionIDs = append(actionIDs, row.actionID)
		}
	}

	if action == constants.AuditActionApprove {
		for _, dematID := range dematIDs {
			if !dematsWithAudit[dematID] {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactiondemat (demat_id, actiontype, processing_status, requested_by, requested_at, requested_ip, checker_by, checker_at, checker_comment, checker_ip)
					VALUES ($1, $4, $5, $2, now(), $6, $2, now(), $3, $6)
				`, dematID, userEmail, constants.ErrAutoApprovedEnrichedEntity, constants.AuditActionCreate, constants.StatusApproved, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
				if err != nil {
					return nil, err
				}
				autoApproved++
			}
		}
	}

	if len(actionIDs) > 0 {
		status := constants.StatusApproved
		if action == constants.AuditActionReject {
			status = constants.StatusRejected
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.auditactiondemat 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3, checker_ip=$4
			WHERE action_id = ANY($5)
		`, status, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), actionIDs); err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteDematIDs) + autoApproved,
		"approved_ids": actionIDs,
		"deleted_ids":  deleteDematIDs,
	}, nil
}

// processBatchAuditFolio handles Folio audit approvals/rejections
func processBatchAuditFolio(ctx context.Context, tx pgx.Tx, batchID, action, userEmail, comment string) (map[string]interface{}, error) {
	folioIDs, err := distinctPOMIDs(ctx, tx, batchID, "folio_id")
	if err != nil {
		return nil, err
	}

	if len(folioIDs) == 0 {
		return map[string]interface{}{"processed": 0, "ids": []string{}}, nil
	}

	audits, err := queryLatestAudits(ctx, tx, "auditactionfolio", "folio_id", folioIDs)
	if err != nil {
		return nil, err
	}

	actionIDs := []string{}
	deleteFolioIDs := []string{}
	foliosWithAudit := make(map[string]bool)
	autoApproved := 0

	for _, row := range audits {
		foliosWithAudit[row.entityID] = true

		if action == constants.AuditActionApprove && row.status == constants.StatusPendingDeleteApproval {
			if _, err = tx.Exec(ctx, `
				UPDATE investment.auditactionfolio 
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id=$4
			`, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), row.actionID); err != nil {
				return nil, err
			}

			if _, err = tx.Exec(ctx, `
				UPDATE investment.masterfolio 
				SET is_deleted=true, status='Inactive' 
				WHERE folio_id=$1
			`, row.entityID); err != nil {
				return nil, err
			}

			deleteFolioIDs = append(deleteFolioIDs, row.entityID)
		} else if row.status == constants.StatusPendingApproval || row.status == constants.StatusPendingEditApproval || (action == constants.AuditActionReject && row.status == constants.StatusPendingDeleteApproval) {
			actionIDs = append(actionIDs, row.actionID)
		}
	}

	if action == constants.AuditActionApprove {
		for _, folioID := range folioIDs {
			if !foliosWithAudit[folioID] {
				_, err = tx.Exec(ctx, `
					INSERT INTO investment.auditactionfolio (folio_id, actiontype, processing_status, requested_by, requested_at, requested_ip, checker_by, checker_at, checker_comment, checker_ip)
					VALUES ($1, $4, $5, $2, now(), $6, $2, now(), $3, $6)
				`, folioID, userEmail, constants.ErrAutoApprovedEnrichedEntity, constants.AuditActionCreate, constants.StatusApproved, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
				if err != nil {
					return nil, err
				}
				autoApproved++
			}
		}
	}

	if len(actionIDs) > 0 {
		status := constants.StatusApproved
		if action == constants.AuditActionReject {
			status = constants.StatusRejected
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.auditactionfolio 
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3, checker_ip=$4
			WHERE action_id = ANY($5)
		`, status, api.SystemIfBlank(userEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), actionIDs); err != nil {
			return nil, err
		}
	}

	return map[string]interface{}{
		"processed":    len(actionIDs) + len(deleteFolioIDs) + autoApproved,
		"approved_ids": actionIDs,
		"deleted_ids":  deleteFolioIDs,
	}, nil
}
