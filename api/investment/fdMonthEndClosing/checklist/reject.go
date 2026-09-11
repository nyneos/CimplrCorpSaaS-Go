package checklist

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RejectChecklistItem handles POST /investment/fd-closing/checklist/reject
// and /bulk-reject. Reject only flips the pending audit row — master was
// never changed for EDIT/DELETE.
func RejectChecklistItem(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ItemID  string   `json:"item_id"`
			ItemIDs []string `json:"item_ids"`
			Comment string   `json:"comment"`
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
		engineActed := 0
		directActed := 0
		var errs []string

		for _, itemID := range ids {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: moduleCode, RecordID: itemID,
				UserID: actor.UserID, UserEmail: actor.Email, RoleID: "",
				Action: approvalengine.ActionRejected, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[FDClosingChecklist] ActOnPendingOrDiagnose reject failed for %s: %v", itemID, actionErr)
				errs = append(errs, itemID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				engineActed++
				continue
			}
			if !actionRes.CancelledStale && actionRes.Reason != "" {
				errs = append(errs, itemID+": "+actionRes.Reason)
				continue
			}
			if err := directRejectChecklist(ctx, pool, itemID, actor.Email, req.Comment); err != nil {
				errs = append(errs, itemID+": "+err.Error())
				continue
			}
			directActed++
		}

		totalActed := engineActed + directActed
		payload := map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errs, "checker": actor.Email,
		}
		if totalActed == 0 {
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No checklist items were rejected", payload)
			return
		}
		fdclosingcommon.RespondSuccess(w, "Checklist item(s) rejected", payload)
	}
}

func directRejectChecklist(ctx context.Context, pool *pgxpool.Pool, itemID, checkerEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tx begin failed: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	pending, err := lookupPendingChecklistAction(ctx, tx, itemID)
	if err != nil {
		return fmt.Errorf("no pending action found")
	}

	if _, err = tx.Exec(ctx, `
		UPDATE investment.fd_closing_checklist_item_audit
		SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(), checker_comment = $3
		WHERE audit_id = $1`,
		pending.AuditID, api.SystemIfBlank(checkerEmail), comment,
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
