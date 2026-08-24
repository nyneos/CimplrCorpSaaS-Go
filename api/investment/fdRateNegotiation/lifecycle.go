package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/policyengine/common"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	rateNegRecordTable = "investment.fd_rate_negotiation"
	rateNegAuditTable  = "investment.fd_audit_rate_negotiation"
	rateNegPK          = "rate_request_id"
	rateNegModule      = "FIXED_DEPOSIT"
)

type bulkRateIDs struct {
	UserID         string   `json:"user_id"`
	RateRequestIDs []string `json:"rate_request_ids"`
	RateRequestID  string   `json:"rate_request_id,omitempty"`
	Comment        string   `json:"comment"`
	Reason         string   `json:"reason"`
}

func collectRateIDs(req bulkRateIDs) []string {
	ids := make([]string, 0, len(req.RateRequestIDs)+1)
	seen := map[string]struct{}{}
	for _, id := range req.RateRequestIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if extra := strings.TrimSpace(req.RateRequestID); extra != "" {
		if _, ok := seen[extra]; !ok {
			ids = append(ids, extra)
		}
	}
	return ids
}

func txTypeForAction(actionType string) string {
	switch strings.ToUpper(strings.TrimSpace(actionType)) {
	case "EDIT":
		return "FD_RATE_NEGOTIATION_EDIT"
	case "DELETE":
		return "FD_RATE_NEGOTIATION_DELETE"
	default:
		return "FD_RATE_NEGOTIATION_CREATE"
	}
}

func fireRateNegotiationInstance(pool *pgxpool.Pool, recordID, userID, userEmail, actionType, matrixID string, amount float64) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := approvalengine.CancelPendingInstances(ctx, pool, rateNegModule, recordID, userEmail); err != nil {
		api.LogError("[FDRateNeg] CancelPendingInstances %s: %v", recordID, err)
		return
	}
	instID, err := approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
		ModuleCode:       rateNegModule,
		EntityCode:       "",
		TransactionType:  txTypeForAction(actionType),
		RecordID:         recordID,
		RecordTable:      rateNegRecordTable,
		AuditTable:       rateNegAuditTable,
		AuditIDColumn:    rateNegPK,
		ActionType:       strings.ToUpper(actionType),
		Amount:           amount,
		SubmittedBy:      userID,
		SubmittedByEmail: userEmail,
		MatrixID:         matrixID,
	})
	if err != nil {
		api.LogError("[FDRateNeg] CreateInstance(%s) %s: %v", actionType, recordID, err)
		return
	}
	if instID != "" {
		api.LogInfo("[FDRateNeg] CreateInstance(%s) %s → %s", actionType, instID, recordID)
	}
}

func stampPendingAudits(ctx context.Context, tx pgx.Tx, ids []string, status, checker, comment, ip string) (int64, error) {
	tag, err := tx.Exec(ctx, `
		UPDATE investment.fd_audit_rate_negotiation t
		SET processing_status = $1,
			checker_by = $2,
			checker_at = now(),
			checker_comment = $3,
			checker_ip = $4
		WHERE t.audit_id IN (
			SELECT DISTINCT ON (a.rate_request_id) a.audit_id
			FROM investment.fd_audit_rate_negotiation a
			WHERE a.rate_request_id = ANY($5::uuid[])
			  AND a.processing_status LIKE 'PENDING%'
			ORDER BY a.rate_request_id, a.requested_at DESC, a.audit_id DESC
		)`,
		status, api.SystemIfBlank(checker), comment, api.SystemIfBlank(ip), ids)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// revertEditOnMaster copies old_* from the latest EDIT audit (pending or just rejected)
// back onto the master row. This is mandatory on EDIT reject.

func revertEditOnMaster(ctx context.Context, tx pgx.Tx, ids []string, checker string) error {
	_, err := tx.Exec(ctx, `
		UPDATE investment.fd_rate_negotiation m
		SET
			proposed_fd_amount = a.old_proposed_fd_amount,
			currency_code = a.old_currency_code,
			tenure_type = a.old_tenure_type,
			tenure_value = a.old_tenure_value,
			expected_start_date = a.old_expected_start_date,
			expected_maturity_date = a.old_expected_maturity_date,
			interest_type = a.old_interest_type,
			interest_payout_mode = a.old_interest_payout_mode,
			target_bank_ids = a.old_target_bank_ids,
			target_bank_names = a.old_target_bank_names,
			internal_notes = a.old_internal_notes,
			request_status = CASE
				WHEN a.old_request_status IS NULL OR a.old_request_status = '' OR a.old_request_status LIKE 'PENDING%'
				THEN 'APPROVED'
				ELSE a.old_request_status
			END,
			is_active = COALESCE(a.old_is_active, m.is_active),
			selected_offer_id = a.old_selected_offer_id,
			selection_remarks = a.old_selection_remarks,
			booking_id = a.old_booking_id,
			updated_by = $2,
			updated_at = now()
		FROM investment.fd_audit_rate_negotiation a
		WHERE m.rate_request_id = a.rate_request_id
		  AND m.rate_request_id = ANY($1::uuid[])
		  AND a.audit_id = (
			SELECT a2.audit_id
			FROM investment.fd_audit_rate_negotiation a2
			WHERE a2.rate_request_id = m.rate_request_id
			  AND a2.action_type = 'EDIT'
			ORDER BY a2.requested_at DESC, a2.audit_id DESC
			LIMIT 1
		  )`, ids, api.SystemIfBlank(checker))
	return err
}

func applyApproveMaster(ctx context.Context, tx pgx.Tx, id, actionType string) error {
	switch strings.ToUpper(actionType) {
	case "DELETE":
		_, err := tx.Exec(ctx, `
			UPDATE investment.fd_rate_negotiation
			SET is_deleted = true, request_status = 'DELETED', updated_at = now()
			WHERE rate_request_id = $1::uuid`, id)
		return err
	default:
		_, err := tx.Exec(ctx, `
			UPDATE investment.fd_rate_negotiation
			SET request_status = 'APPROVED', updated_at = now()
			WHERE rate_request_id = $1::uuid
			  AND COALESCE(is_deleted,false) = false`, id)
		return err
	}
}

func applyRejectMaster(ctx context.Context, tx pgx.Tx, id, actionType, oldStatus, checker string) error {
	switch strings.ToUpper(actionType) {
	case "EDIT":
		return revertEditOnMaster(ctx, tx, []string{id}, checker)
	case "DELETE":
		restore := oldStatus
		if restore == "" || strings.HasPrefix(restore, "PENDING") {
			restore = "APPROVED"
		}
		_, err := tx.Exec(ctx, `
			UPDATE investment.fd_rate_negotiation
			SET request_status = $2, is_deleted = false, updated_at = now()
			WHERE rate_request_id = $1::uuid`, id, restore)
		return err
	default:
		_, err := tx.Exec(ctx, `
			UPDATE investment.fd_rate_negotiation
			SET request_status = 'REJECTED', updated_at = now()
			WHERE rate_request_id = $1::uuid
			  AND COALESCE(is_deleted,false) = false`, id)
		return err
	}
}

// DeleteRateRequests creates DELETE audits in PENDING_DELETE_APPROVAL.
// is_deleted is NOT set until approve.
func DeleteRateRequests(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req bulkRateIDs
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := collectRateIDs(req)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "rate_request_ids is required")
			return
		}
		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		userID := strings.TrimSpace(req.UserID)
		if userID == "" {
			userID = api.GetUserIDFromCtx(r.Context())
		}
		reason := strings.TrimSpace(req.Reason)
		if reason == "" {
			api.RespondWithError(w, http.StatusBadRequest, "reason is required")
			return
		}

		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		rows, err := tx.Query(ctx, `
			SELECT rate_request_id::text, COALESCE(request_status,''), COALESCE(proposed_fd_amount,0), COALESCE(entity_id,'')
			FROM investment.fd_rate_negotiation
			WHERE rate_request_id = ANY($1::uuid[])
			  AND COALESCE(is_deleted,false) = false
			  AND request_status NOT IN ('DELETED','CONVERTED_TO_FD','PENDING_DELETE_APPROVAL')
			FOR UPDATE`, ids)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to load rate requests")
			return
		}

		type meta struct {
			id       string
			status   string
			amount   float64
			entityID string
		}
		var valid []meta
		for rows.Next() {
			var m meta
			if err := rows.Scan(&m.id, &m.status, &m.amount, &m.entityID); err != nil {
				rows.Close()
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed")
				return
			}
			valid = append(valid, m)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(valid) == 0 {
			api.RespondWithPayload(w, false, "No eligible rate requests to delete", nil)
			return
		}

		deleteMatrixByID := map[string]string{}
		var policyErrors []string
		for _, m := range valid {
			ok, pmsg, matrixID := fdEnforceInlineWithMatrix(ctx, r, pool, enforceCtx{
				EventCode:   common.TriggerPreDelete,
				HandlerName: "DeleteRateRequests",
				APIPath:     "/investment/fd/rate-negotiation/delete",
				EntityCode:  m.entityID,
				Actor:       userEmail,
			}, map[string]interface{}{
				"rate_request_id":    m.id,
				"entity_id":          m.entityID,
				"entity_code":        m.entityID,
				"proposed_fd_amount": m.amount,
				"request_status":     m.status,
			})
			if !ok {
				policyErrors = append(policyErrors, m.id+": "+pmsg)
				continue
			}
			deleteMatrixByID[m.id] = matrixID
		}
		if len(policyErrors) > 0 {
			api.RespondWithPayload(w, false, "Policy check blocked delete", map[string]interface{}{
				"errors": policyErrors,
			})
			return
		}

		ip := api.ClientIPFromContext(ctx)
		for _, m := range valid {
			if _, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_audit_rate_negotiation (
					rate_request_id, action_type, processing_status, reason,
					requested_by, requested_at, requested_ip,
					old_request_status
				) VALUES (
					$1::uuid, 'DELETE', 'PENDING_DELETE_APPROVAL', $2,
					$3, now(), $4, $5
				)`, m.id, reason, api.SystemIfBlank(userEmail), api.SystemIfBlank(ip), m.status); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed")
				return
			}
			if _, err = tx.Exec(ctx, `
				UPDATE investment.fd_rate_negotiation
				SET request_status = 'PENDING_DELETE_APPROVAL', updated_by = $2, updated_at = now()
				WHERE rate_request_id = $1::uuid`, m.id, userEmail); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Status update failed")
				return
			}
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed")
			return
		}

		for _, m := range valid {
			fireRateNegotiationInstance(pool, m.id, userID, userEmail, "DELETE", deleteMatrixByID[m.id], m.amount)
			fireRateNegotiationNotification(pool, m.id, userEmail, "DELETE")
		}

		results := make([]map[string]interface{}, 0, len(ids))
		validSet := map[string]bool{}
		for _, m := range valid {
			validSet[m.id] = true
		}
		for _, id := range ids {
			if validSet[id] {
				results = append(results, map[string]interface{}{"success": true, "rate_request_id": id})
			} else {
				results = append(results, map[string]interface{}{
					"success": false, "rate_request_id": id, "error": "Not eligible for delete",
				})
			}
		}
		api.RespondWithPayload(w, true, "", results)
	}
}

// ApproveRateRequests stamps pending audits APPROVED.
// DELETE approvals set is_deleted=true. CREATE/EDIT keep applied values.
func ApproveRateRequests(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bulkDecide(w, r, pool, true)
	}
}

// RejectRateRequests stamps pending audits REJECTED.
// EDIT reject re-applies old_* onto the master. DELETE reject does not soft-delete.
func RejectRateRequests(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bulkDecide(w, r, pool, false)
	}
}

func bulkDecide(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, approve bool) {
	var req bulkRateIDs
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
		return
	}
	ids := collectRateIDs(req)
	if len(ids) == 0 {
		api.RespondWithError(w, http.StatusBadRequest, "rate_request_ids is required")
		return
	}
	userEmail := api.GetUserEmailFromCtx(r.Context())
	if userEmail == "" {
		api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
		return
	}
	userID := strings.TrimSpace(req.UserID)
	if userID == "" {
		userID = api.GetUserIDFromCtx(r.Context())
	}
	if !approve && strings.TrimSpace(req.Comment) == "" && strings.TrimSpace(req.Reason) == "" {
		api.RespondWithError(w, http.StatusBadRequest, "comment is required to reject")
		return
	}
	comment := strings.TrimSpace(req.Comment)
	if comment == "" {
		comment = strings.TrimSpace(req.Reason)
	}

	ctx := r.Context()
	ip := api.ClientIPFromContext(ctx)
	engineActed := 0
	directActed := 0
	var errors []string

	engineAction := approvalengine.ActionApproved
	stampStatus := "APPROVED"
	if !approve {
		engineAction = approvalengine.ActionRejected
		stampStatus = "REJECTED"
	}

	for _, id := range ids {
		// Capture pending action_type BEFORE the engine stamps the audit row.
		var actionType, oldStatus string
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(action_type,''), COALESCE(old_request_status,'')
			FROM investment.fd_audit_rate_negotiation
			WHERE rate_request_id = $1::uuid
			  AND processing_status LIKE 'PENDING%'
			ORDER BY requested_at DESC, audit_id DESC
			LIMIT 1`, id).Scan(&actionType, &oldStatus)

		actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
			ModuleCode: rateNegModule,
			RecordID:   id,
			UserID:     userID,
			UserEmail:  userEmail,
			RoleID:     "",
			Action:     engineAction,
			Comment:    comment,
		})
		if actionErr != nil {
			api.LogError("[FDRateNeg] ActOnPending %s: %v", id, actionErr)
			errors = append(errors, id+": "+actionErr.Error())
			continue
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			errors = append(errors, id+": tx begin failed")
			continue
		}

		if actionRes.Acted {
			// Engine already stamped the audit (and flipped is_deleted on approved DELETE).
			// Still apply request_status / EDIT revert — finalizeRecord does not revert old_*.
			if actionType == "" {
				_ = tx.QueryRow(ctx, `
					SELECT COALESCE(action_type,''), COALESCE(old_request_status,'')
					FROM investment.fd_audit_rate_negotiation
					WHERE rate_request_id = $1::uuid
					ORDER BY COALESCE(checker_at, requested_at) DESC, audit_id DESC
					LIMIT 1`, id).Scan(&actionType, &oldStatus)
			}
			var applyErr error
			if approve {
				if actionRes.InstanceStatus == constants.StatusApproved || actionRes.InstanceStatus == "APPROVED" {
					applyErr = applyApproveMaster(ctx, tx, id, actionType)
				}
			} else {
				applyErr = applyRejectMaster(ctx, tx, id, actionType, oldStatus, userEmail)
			}
			if applyErr != nil {
				_ = tx.Rollback(ctx)
				errors = append(errors, id+": master apply failed: "+applyErr.Error())
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				errors = append(errors, id+": commit failed")
				continue
			}
			engineActed++
			if approve {
				fireRateNegotiationNotification(pool, id, userEmail, "APPROVE")
			} else {
				fireRateNegotiationNotification(pool, id, userEmail, "REJECT")
			}
			continue
		}

		if actionRes.CancelledStale {
			api.LogInfo("[FDRateNeg] stale instance cancelled for %s: %s", id, actionRes.Reason)
		} else if actionRes.Reason != "" {
			_ = tx.Rollback(ctx)
			errors = append(errors, id+": "+actionRes.Reason)
			continue
		}

		if actionType == "" {
			_ = tx.Rollback(ctx)
			errors = append(errors, id+": no pending audit action found")
			continue
		}

		n, err := stampPendingAudits(ctx, tx, []string{id}, stampStatus, userEmail, comment, ip)
		if err != nil {
			_ = tx.Rollback(ctx)
			errors = append(errors, id+": audit stamp failed")
			continue
		}
		if n == 0 {
			_ = tx.Rollback(ctx)
			errors = append(errors, id+": no pending audit action found")
			continue
		}

		var applyErr error
		if approve {
			applyErr = applyApproveMaster(ctx, tx, id, actionType)
		} else {
			applyErr = applyRejectMaster(ctx, tx, id, actionType, oldStatus, userEmail)
		}
		if applyErr != nil {
			_ = tx.Rollback(ctx)
			errors = append(errors, id+": master apply failed: "+applyErr.Error())
			continue
		}
		if err := tx.Commit(ctx); err != nil {
			errors = append(errors, id+": commit failed")
			continue
		}
		directActed++
		if approve {
			fireRateNegotiationNotification(pool, id, userEmail, "APPROVE")
		} else {
			fireRateNegotiationNotification(pool, id, userEmail, "REJECT")
		}
	}

	total := engineActed + directActed
	success := total > 0 || len(errors) == 0
	msg := ""
	if !success {
		if approve {
			msg = "No rate requests were approved"
		} else {
			msg = "No rate requests were rejected"
		}
	}
	api.RespondWithPayload(w, success, msg, map[string]interface{}{
		"engine_acted": engineActed,
		"direct_acted": directActed,
		"errors":       errors,
		"checker":      userEmail,
	})
}
