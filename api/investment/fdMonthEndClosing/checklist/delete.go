package checklist

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DeleteChecklistItem handles POST /investment/fd-closing/checklist/delete.
// Stages PENDING_DELETE_APPROVAL; master is soft-deleted only on approve
// (then reseeded as NOT_STARTED — see ApplyDeleteToMaster).
func DeleteChecklistItem(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ItemID  string   `json:"item_id"`
			ItemIDs []string `json:"item_ids"`
			Reason  string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := mergeItemIDs(req.ItemID, req.ItemIDs)
		if len(ids) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "item_id or item_ids is required")
			return
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		submitted := 0
		var errs []string

		for _, itemID := range ids {
			if err := stageChecklistDelete(ctx, pool, itemID, actor, req.Reason); err != nil {
				errs = append(errs, itemID+": "+err.Error())
				continue
			}
			submitted++
		}

		payload := map[string]interface{}{
			"submitted": submitted, "errors": errs, "requester": actor.Email,
		}
		if submitted == 0 {
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No delete requests were submitted", payload)
			return
		}
		fdclosingcommon.RespondSuccess(w, "Checklist delete submitted for approval", payload)
	}
}

func stageChecklistDelete(ctx context.Context, pool *pgxpool.Pool, itemID string, actor fdclosingcommon.Actor, reason string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var entityID, cycleStatus string
	var isDeleted bool
	var oldStatus string
	var oldBlockedComment, oldEvidenceRef, oldEvidenceType *string
	var oldExceptionCount int
	err = tx.QueryRow(ctx, `
		SELECT c.entity_id, c.status, i.is_deleted, i.status, i.blocked_comment, i.exception_count,
		       i.evidence_ref, i.evidence_type
		FROM investment.fd_closing_checklist_item i
		JOIN investment.fd_closing_cycle c ON c.cycle_id = i.cycle_id
		WHERE i.item_id = $1
		FOR UPDATE OF i`,
		itemID,
	).Scan(&entityID, &cycleStatus, &isDeleted, &oldStatus, &oldBlockedComment, &oldExceptionCount,
		&oldEvidenceRef, &oldEvidenceType)
	if err != nil {
		return err
	}
	if isDeleted {
		return errItem("already deleted")
	}
	scope := ctxutil.FromContext(ctx)
	if !scope.HasEntityAccess(entityID) {
		return errItem("entity not in access scope")
	}
	if cycleStatus == "LOCKED" || cycleStatus == "CLOSED" {
		return errItem("cycle is " + strings.ToLower(cycleStatus))
	}

	if _, err = tx.Exec(ctx, `
		UPDATE investment.fd_closing_checklist_item_audit
		SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(),
		    checker_comment = 'Superseded by new delete request'
		WHERE item_id = $1 AND processing_status IN ('PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL')`,
		itemID, api.SystemIfBlank(actor.Email),
	); err != nil {
		return err
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO investment.fd_closing_checklist_item_audit (
			item_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
			old_status, old_blocked_comment, old_exception_count,
			old_evidence_ref, old_evidence_type
		) VALUES (
			$1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now(),$4,
			$5,$6,$7,$8,$9
		)`,
		itemID,
		nullIfEmpty(reason),
		api.SystemIfBlank(actor.Email),
		api.SystemIfBlank(api.ClientIPFromContext(ctx)),
		oldStatus, oldBlockedComment, oldExceptionCount,
		oldEvidenceRef, oldEvidenceType,
	); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	actorEmail, actorUserID := actor.Email, actor.UserID
	runEngineInBackground(func(bgCtx context.Context) {
		if err := approvalengine.CancelPendingInstances(bgCtx, pool, moduleCode, itemID, actorEmail); err != nil {
			api.LogError("[FDClosingChecklist] CancelPendingInstances(DELETE) failed for item %s: %v", itemID, err)
			return
		}
		instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
			ModuleCode:          moduleCode,
			EntityCode:          entityID,
			TransactionType:     TxDeleteChecklist,
			RecordID:            itemID,
			RecordTable:         checklistTable,
			AuditTable:          checklistAuditTable,
			AuditIDColumn:       "item_id",
			ActionType:          "DELETE",
			SubmittedBy:         actorUserID,
			SubmittedByEmail:    actorEmail,
			RequirePinnedMatrix: true,
			AutoApplyIfUnpinned: false,
		})
		if err != nil {
			api.LogError("[FDClosingChecklist] CreateInstance(DELETE) failed for item %s: %v", itemID, err)
			return
		}
		if instID != "" {
			return
		}
		tx2, err := pool.Begin(bgCtx)
		if err != nil {
			return
		}
		defer tx2.Rollback(bgCtx) //nolint:errcheck
		if err := ApplyDeleteToMaster(bgCtx, tx2, itemID, api.SystemIfBlank(actorEmail), "Auto-applied (no approval matrix)", "PENDING_DELETE_APPROVAL", true); err != nil {
			api.LogError("[FDClosingChecklist] no-matrix DELETE apply failed for item %s: %v", itemID, err)
			return
		}
		_ = tx2.Commit(bgCtx)
	})
	return nil
}

type simpleErr string

func (e simpleErr) Error() string { return string(e) }

func errItem(msg string) error { return simpleErr(msg) }
