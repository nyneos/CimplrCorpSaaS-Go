package fdReceipt

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EditVariance updates resolution fields when workflow is not CLOSED (audit trail preserved).
// POST /investment/fd/exception/edit
func EditVariance(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID             string `json:"user_id"`
			ExceptionID        string `json:"exception_id"`
			ProposedResolution string `json:"proposed_resolution"`
			ReasonCode         string `json:"reason_code"`
			ResolutionRemarks  string `json:"resolution_remarks"`
			Attachment         string `json:"attachment"`
			Comment            string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ExceptionID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "exception_id is required")
			return
		}
		canonicalResolution, valErr := validateExceptionResolveInput(req.ProposedResolution, req.ReasonCode, req.ResolutionRemarks)
		if valErr != "" {
			api.RespondWithError(w, http.StatusBadRequest, valErr)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		hdr, err := loadVarianceCase(ctx, pool, req.ExceptionID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrExceptionNotFound)
			return
		}
		if hdr.WorkflowStatus == "CLOSE" {
			api.RespondWithError(w, http.StatusBadRequest, "cannot edit a closed variance")
			return
		}
		if hdr.WorkflowStatus != "IN_REVIEW" {
			api.RespondWithError(w, http.StatusBadRequest, "edit is only allowed while IN_REVIEW; use /exception/resolve while OPEN")
			return
		}
		if hasVarianceCheckerApproval(ctx, pool, req.ExceptionID) {
			api.RespondWithError(w, http.StatusBadRequest, "cannot edit after checker approval; close to lock or reject to reopen")
			return
		}

		auditOld := auditOldFromHeader(hdr)

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_receipt_exception SET
				proposed_resolution=$1, reason_code=$2, resolution_remarks=$3, attachment=$4
			WHERE exception_id=$5 AND exception_status='IN_REVIEW'`,
			canonicalResolution, req.ReasonCode, req.ResolutionRemarks, nullStr(req.Attachment), req.ExceptionID)
		if err != nil {
			respondExceptionWriteError(w, err)
			return
		}
		if err = insertVarianceAudit(ctx, tx, req.ExceptionID, varianceAuditInsert{
			ActionType: "EDIT", ProcessingStatus: constants.StatusPendingEditApproval,
			Reason: req.Comment, RequestedBy: userEmail, Old: auditOld,
		}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}
		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		go func(eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] EditVariance engine panic for %s: %v", eID, rec)
				}
			}()
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer bgCancel()
			_ = approvalengine.CancelPendingInstances(bgCtx, pool, "FIXED_DEPOSIT", eID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				TransactionType:  "FD_EXCEPTION_EDIT",
				RecordID:         eID,
				RecordTable:      constants.QuerryReceiptException,
				AuditTable:       constants.QuerryReceiptExceptionAudit,
				AuditIDColumn:    "exception_id",
				ActionType:       "EDIT",
				SubmittedBy:      uEmail,
				SubmittedByEmail: uEmail,
			})
		}(req.ExceptionID, userEmail)

		go func(eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] EditVariance notification panic for %s: %v", eID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pool,
				"/investment/fd/exception/edit", eID, map[string]interface{}{
					"record_id":   eID,
					"event":       "FD_EXCEPTION_EDIT_SUBMITTED",
					"actor_email": uEmail,
				})
		}(req.ExceptionID, userEmail)

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"exception_id":      req.ExceptionID,
			"workflow_status":   hdr.WorkflowStatus,
			"exception_status":  hdr.WorkflowStatus,
			"processing_status": constants.StatusPendingEditApproval,
		})
	}
}

func resolveVarianceHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID             string `json:"user_id"`
			ExceptionID        string `json:"exception_id"`
			ProposedResolution string `json:"proposed_resolution"`
			ReasonCode         string `json:"reason_code"`
			ResolutionRemarks  string `json:"resolution_remarks"`
			Attachment         string `json:"attachment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ExceptionID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "exception_id is required")
			return
		}
		canonicalResolution, valErr := validateExceptionResolveInput(req.ProposedResolution, req.ReasonCode, req.ResolutionRemarks)
		if valErr != "" {
			api.RespondWithError(w, http.StatusBadRequest, valErr)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		hdr, err := loadVarianceCase(ctx, pool, req.ExceptionID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrExceptionNotFound)
			return
		}
		if hdr.WorkflowStatus != "OPEN" && hdr.WorkflowStatus != "IN_REVIEW" {
			api.RespondWithError(w, http.StatusBadRequest, "variance must be OPEN or IN_REVIEW to resolve (current: "+hdr.WorkflowStatus+")")
			return
		}
		if hasVarianceCheckerApproval(ctx, pool, req.ExceptionID) {
			api.RespondWithError(w, http.StatusBadRequest, "cannot change resolution after checker approval; reject to reopen or close to lock")
			return
		}

		auditOld := auditOldFromHeader(hdr)

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_receipt_exception SET
				exception_status='IN_REVIEW',
				proposed_resolution=$1, reason_code=$2, resolution_remarks=$3, attachment=$4
			WHERE exception_id=$5 AND exception_status IN ('OPEN','IN_REVIEW')`,
			canonicalResolution, req.ReasonCode, req.ResolutionRemarks, nullStr(req.Attachment), req.ExceptionID)
		if err != nil {
			respondExceptionWriteError(w, err)
			return
		}
		if err = insertVarianceAudit(ctx, tx, req.ExceptionID, varianceAuditInsert{
			ActionType: "EDIT", ProcessingStatus: constants.StatusPendingApproval,
			Reason: req.ResolutionRemarks, RequestedBy: userEmail, Old: auditOld,
		}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}
		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		go func(eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] ResolveException engine panic for %s: %v", eID, rec)
				}
			}()
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer bgCancel()
			_ = approvalengine.CancelPendingInstances(bgCtx, pool, "FIXED_DEPOSIT", eID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				TransactionType:  "FD_EXCEPTION_RESOLVE",
				RecordID:         eID,
				RecordTable:      constants.QuerryReceiptException,
				AuditTable:       constants.QuerryReceiptExceptionAudit,
				AuditIDColumn:    "exception_id",
				ActionType:       "EDIT",
				SubmittedBy:      uEmail,
				SubmittedByEmail: uEmail,
			})
		}(req.ExceptionID, userEmail)

		go func(eID, uEmail string) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDReceipt] ResolveException notification panic for %s: %v", eID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pool,
				"/investment/fd/exception/resolve", eID, map[string]interface{}{
					"record_id":   eID,
					"event":       "FD_EXCEPTION_RESOLVED",
					"actor_email": uEmail,
				})
		}(req.ExceptionID, userEmail)

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"exception_id":      req.ExceptionID,
			"workflow_status":   "IN_REVIEW",
			"exception_status":  "IN_REVIEW",
			"processing_status": constants.StatusPendingApproval,
			"case_type":         "VARIANCE",
		})
	}
}

func approveVarianceHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			ExceptionID  string   `json:"exception_id"`
			ExceptionIDs []string `json:"exception_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := normalizeExceptionIDs(req.ExceptionID, req.ExceptionIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ExceptionIDsRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(ids))
		approved := 0

		for _, eid := range ids {
			res := map[string]interface{}{"exception_id": eid, "success": false}
			hdr, err := loadVarianceCase(ctx, pool, eid)
			if err != nil {
				res["error"] = constants.ErrExceptionNotFound
				results = append(results, res)
				continue
			}
			if hdr.WorkflowStatus != "IN_REVIEW" {
				res["error"] = "must be IN_REVIEW (current: " + hdr.WorkflowStatus + ")"
				results = append(results, res)
				continue
			}
			if hasVarianceCheckerApproval(ctx, pool, eid) {
				res["error"] = "already approved; use close to lock or reject to reopen"
				results = append(results, res)
				continue
			}

			// Try engine first; fall through to direct stamp on no-matrix path.
			approveActionRes, approveActionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: "FIXED_DEPOSIT", RecordID: eid,
				UserID: req.UserID, UserEmail: userEmail, RoleID: "",
				Action: approvalengine.ActionApproved, Comment: req.Comment,
			})
			if approveActionErr != nil {
				api.LogError("[FDReceipt] ApproveException engine error exception=%s: %v", eid, approveActionErr)
			}
			if approveActionRes.Acted && approveActionRes.InstanceStatus != approvalengine.InstStatusApproved {
				// Multi-level approval in progress — not final eye yet.
				res["success"] = true
				res["exception_status"] = "IN_REVIEW"
				res["processing_status"] = approveActionRes.InstanceStatus
				results = append(results, res)
				continue
			}
			if !approveActionRes.Acted && !approveActionRes.CancelledStale && approveActionRes.Reason != "" {
				res["error"] = approveActionRes.Reason
				results = append(results, res)
				continue
			}

			tx, err := pool.Begin(ctx)
			if err != nil {
				res["error"] = constants.ErrTransactionFailed
				results = append(results, res)
				continue
			}
			if err = insertVarianceAudit(ctx, tx, eid, varianceAuditInsert{
				ActionType: "EDIT", ProcessingStatus: constants.StatusApproved, Reason: req.Comment,
				RequestedBy: userEmail, CheckerBy: userEmail, CheckerComment: req.Comment,
				Old: varianceAuditOld{ExceptionStatus: strPtr("IN_REVIEW")},
			}); err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			if err = tx.Commit(ctx); err != nil {
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			approved++
			res["success"] = true
			res["exception_status"] = "IN_REVIEW"
			res["processing_status"] = constants.StatusApproved
			res["checker_approved"] = true
			res["awaiting_close"] = true
			res["allowed_actions"] = []string{"close", "reject"}
			results = append(results, res)

			go func(id, u string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDReceipt] ApproveException notification panic for %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/exception/approve", id, map[string]interface{}{
					"record_id": id, "event": "FD_RECEIPT_EXCEPTION_APPROVED", "actor_email": u,
				})
			}(eid, userEmail)
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"approved": approved, "failed": len(ids) - approved, "results": results,
		})
	}
}

func closeOneVariance(ctx context.Context, pool *pgxpool.Pool, exceptionID, userEmail, comment string) map[string]interface{} {
	res := map[string]interface{}{"exception_id": exceptionID, "success": false}

	hdr, err := loadVarianceCase(ctx, pool, exceptionID)
	if err != nil {
		res["error"] = constants.ErrExceptionNotFound
		return res
	}
	if hdr.WorkflowStatus != "IN_REVIEW" {
		res["error"] = "must be IN_REVIEW (current: " + hdr.WorkflowStatus + ")"
		return res
	}
	if !hasVarianceCheckerApproval(ctx, pool, exceptionID) {
		res["error"] = "checker approval required before close"
		return res
	}
	if strings.TrimSpace(hdr.ProposedResolution) == "" || strings.TrimSpace(hdr.ReasonCode) == "" {
		res["error"] = "resolution required: submit /exception/resolve first"
		return res
	}

	canonRes, _ := normalizeProposedResolution(hdr.ProposedResolution)
	accept := canonRes == "ACCEPT"
	caseType := "VARIANCE"
	outcome := "PENDING"
	if accept {
		caseType = "EXCEPTION"
		outcome = "ACCEPTED"
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		res["error"] = constants.ErrTransactionFailed
		return res
	}

	_, err = tx.Exec(ctx, `
		UPDATE investment.fd_receipt_exception SET
			exception_status='CLOSE',
			case_type=$1,
			variance_outcome=$2
		WHERE exception_id=$3 AND exception_status='IN_REVIEW'`,
		caseType, outcome, exceptionID)
	if err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		res["error"] = err.Error()
		return res
	}
	auditOld := auditOldFromHeader(hdr)
	if err = insertVarianceAudit(ctx, tx, exceptionID, varianceAuditInsert{
		ActionType: "EDIT", ProcessingStatus: constants.StatusApproved, Reason: comment, RequestedBy: userEmail,
		Old: auditOld,
	}); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		res["error"] = err.Error()
		return res
	}
	if err = tx.Commit(ctx); err != nil {
		res["error"] = err.Error()
		return res
	}

	applyExceptionClosure(ctx, pool, exceptionID)

	go func(eID, uEmail string) {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDReceipt] CloseException engine panic for %s: %v", eID, rec)
			}
		}()
		bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer bgCancel()
		_, _ = approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
			ModuleCode:       "FIXED_DEPOSIT",
			TransactionType:  "FD_EXCEPTION_CLOSE",
			RecordID:         eID,
			RecordTable:      constants.QuerryReceiptException,
			AuditTable:       constants.QuerryReceiptExceptionAudit,
			AuditIDColumn:    "exception_id",
			ActionType:       "EDIT",
			SubmittedBy:      uEmail,
			SubmittedByEmail: uEmail,
		})
	}(exceptionID, userEmail)

	go func(eID, uEmail string) {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDReceipt] CloseException notification panic for %s: %v", eID, rec)
			}
		}()
		notifcatalog.TriggerNotification(context.Background(), pool,
			"/investment/fd/exception/close", eID, map[string]interface{}{
				"record_id":   eID,
				"event":       "FD_EXCEPTION_CLOSED",
				"actor_email": uEmail,
			})
	}(exceptionID, userEmail)

	canPost := false
	if receiptID, _ := resolveExceptionReceiptLinks(ctx, pool, hdr.ReceiptID, hdr.TDSID); receiptID != "" {
		canPost = checkReceiptPostingEligibility(ctx, pool, receiptID) == ""
	}

	res["success"] = true
	res["exception_status"] = "CLOSE"
	res["workflow_status"] = "CLOSE"
	res["processing_status"] = constants.StatusApproved
	res["case_type"] = caseType
	res["variance_outcome"] = outcome
	res["proposed_resolution"] = canonRes
	res["variance_accepted"] = accept
	res["can_post_journals"] = canPost
	res["is_locked"] = true
	res["match_status_updated"] = accept
	if accept {
		res["reconcile_match_status"] = "VARIANCE_ACCEPTED"
	} else {
		res["reconcile_match_status"] = "UNMATCHED"
	}
	return res
}

func closeVarianceHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			ExceptionID  string   `json:"exception_id"`
			ExceptionIDs []string `json:"exception_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := normalizeExceptionIDs(req.ExceptionID, req.ExceptionIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ExceptionIDsRequired)
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(ids))
		closed := 0
		for _, eid := range ids {
			res := closeOneVariance(ctx, pool, eid, userEmail, req.Comment)
			if res["success"] == true {
				closed++
			}
			results = append(results, res)
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"closed": closed, "failed": len(ids) - closed, "results": results,
		})
	}
}

func rejectVarianceHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			ExceptionID  string   `json:"exception_id"`
			ExceptionIDs []string `json:"exception_ids"`
			Reason       string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := normalizeExceptionIDs(req.ExceptionID, req.ExceptionIDs)
		if len(ids) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ExceptionIDsRequired)
			return
		}
		if strings.TrimSpace(req.Reason) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "reason is required when rejecting")
			return
		}
		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(ids))
		rejected := 0

		for _, eid := range ids {
			res := map[string]interface{}{"exception_id": eid, "success": false}
			hdr, err := loadVarianceCase(ctx, pool, eid)
			if err != nil {
				res["error"] = constants.ErrExceptionNotFound
				results = append(results, res)
				continue
			}
			if hdr.WorkflowStatus == "CLOSE" {
				res["error"] = "already CLOSE"
				results = append(results, res)
				continue
			}
			if hdr.WorkflowStatus != "OPEN" && hdr.WorkflowStatus != "IN_REVIEW" {
				res["error"] = "must be OPEN or IN_REVIEW"
				results = append(results, res)
				continue
			}

			// Try engine first; direct stamp on no-matrix path.
			rejectActionRes, rejectActionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: "FIXED_DEPOSIT", RecordID: eid,
				UserID: req.UserID, UserEmail: userEmail, RoleID: "",
				Action: approvalengine.ActionRejected, Comment: req.Reason,
			})
			if rejectActionErr != nil {
				api.LogError("[FDReceipt] RejectException engine error exception=%s: %v", eid, rejectActionErr)
			}
			if !rejectActionRes.Acted && !rejectActionRes.CancelledStale && rejectActionRes.Reason != "" {
				res["error"] = rejectActionRes.Reason
				results = append(results, res)
				continue
			}

			auditOld := auditOldFromHeader(hdr)
			tx, err := pool.Begin(ctx)
			if err != nil {
				res["error"] = constants.ErrTransactionFailed
				results = append(results, res)
				continue
			}

			_, err = tx.Exec(ctx, `
				UPDATE investment.fd_receipt_exception SET
					exception_status='OPEN',
					case_type='VARIANCE',
					variance_outcome=NULL,
					proposed_resolution=NULL,
					reason_code=NULL,
					resolution_remarks=NULL,
					attachment=NULL
				WHERE exception_id=$1 AND exception_status IN ('OPEN','IN_REVIEW')`, eid)
			if err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			if err = insertVarianceAudit(ctx, tx, eid, varianceAuditInsert{
				ActionType: "EDIT", ProcessingStatus: constants.StatusRejected, Reason: req.Reason,
				RequestedBy: userEmail, CheckerBy: userEmail, CheckerComment: req.Reason, Old: auditOld,
			}); err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}
			if err = tx.Commit(ctx); err != nil {
				res["error"] = err.Error()
				results = append(results, res)
				continue
			}

			applyExceptionReject(ctx, pool, eid)
			rejected++
			res["success"] = true
			res["exception_status"] = "OPEN"
			res["processing_status"] = constants.StatusRejected
			results = append(results, res)

			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDReceipt] RejectException notification panic for %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pool,
					"/investment/fd/exception/reject", id, map[string]interface{}{
						"record_id":   id,
						"event":       "FD_EXCEPTION_REJECTED",
						"actor_email": uEmail,
					})
			}(eid, userEmail)
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"rejected": rejected, "failed": len(ids) - rejected, "results": results,
		})
	}
}

func normalizeExceptionIDs(single string, ids []string) []string {
	if len(ids) > 0 {
		return ids
	}
	if strings.TrimSpace(single) != "" {
		return []string{strings.TrimSpace(single)}
	}
	return nil
}

// RejectException delegates to rejectVarianceHandler.
func RejectException(pool *pgxpool.Pool) http.HandlerFunc { return rejectVarianceHandler(pool) }
