package scope

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

// RejectScope handles both POST /investment/fd-closing/scope/reject (single)
// and POST /investment/fd-closing/scope/bulk-reject.
//
// Reject is the simple half here too (mirrors cycle/reject.go): for a
// rejected CREATE (Add), no checklist items were ever created — they only get
// seeded on approve via applyScopeAddApproval — so there is nothing to clean
// up. For a rejected DELETE (Remove), is_deleted was never flipped (that only
// happens on approve), so there is nothing to revert either. Every reject —
// engine-mediated or direct — only flips the audit row's processing_status to
// REJECTED.
func RejectScope(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ScopeID  string   `json:"scope_id"`
			ScopeIDs []string `json:"scope_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := mergeIDs(req.ScopeID, req.ScopeIDs)
		if len(ids) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "scope_id or scope_ids is required")
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

		for _, scopeID := range ids {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: moduleCode, RecordID: scopeID,
				UserID: actor.UserID, UserEmail: actor.Email, RoleID: "",
				Action: approvalengine.ActionRejected, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[FDClosingScope] ActOnPendingOrDiagnose reject failed for %s: %v", scopeID, actionErr)
				errs = append(errs, scopeID+": "+actionErr.Error())
				continue
			}

			if actionRes.Acted {
				engineActed++
				continue
			}

			if !actionRes.CancelledStale && actionRes.Reason != "" {
				errs = append(errs, scopeID+": "+actionRes.Reason)
				continue
			}

			if err := directRejectScope(ctx, pool, scopeID, actor.Email, req.Comment); err != nil {
				errs = append(errs, scopeID+": "+err.Error())
				continue
			}
			directActed++
		}

		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errs) == 0
		payload := map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errs, "checker": actor.Email,
		}
		if !success {
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No scope rows were rejected", payload)
		} else {
			fdclosingcommon.RespondSuccess(w, "Scope row(s) rejected", payload)
		}
		api.LogInfo("[FDClosingScope] RejectScope: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errs), actor.Email)
	}
}

// directRejectScope is the no-approval-matrix-configured fallback: flip the
// single pending audit row to REJECTED. No master-level revert — see the
// package doc comment on RejectScope.
func directRejectScope(ctx context.Context, pool *pgxpool.Pool, scopeID, checkerEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tx begin failed: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle_fd_scope_audit
		SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(), checker_comment = $3
		WHERE scope_id = $1 AND processing_status LIKE 'PENDING%'`,
		scopeID, api.SystemIfBlank(checkerEmail), comment,
	)
	if err != nil {
		return fmt.Errorf("audit flip failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no pending action found (already actioned or not found)")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}
