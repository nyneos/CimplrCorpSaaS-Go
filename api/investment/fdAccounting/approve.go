package fdAccounting

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

type actionRequest struct {
	EntryID  string   `json:"entry_id"`
	EntryIDs []string `json:"entry_ids"`
	Comment  string   `json:"comment"`
}

// ApproveJournal / RejectJournal implement AP-04 for workbench-created entries.
// Gating copies fdMonthEndClosing/lock/approve.go verbatim: ask the approval
// engine first; if no instance/matrix applies, stamp the audit row directly.
func ApproveJournal(pool *pgxpool.Pool) http.HandlerFunc {
	return actOnJournals(pool, approvalengine.ActionApproved)
}

func RejectJournal(pool *pgxpool.Pool) http.HandlerFunc {
	return actOnJournals(pool, approvalengine.ActionRejected)
}

func actOnJournals(pool *pgxpool.Pool, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req actionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := mergeIDs(req.EntryID, req.EntryIDs)
		if len(ids) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "entry_id or entry_ids is required")
			return
		}
		if action == approvalengine.ActionRejected && req.Comment == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "comment is required to reject")
			return
		}
		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		engineActed, directActed := 0, 0
		var errs []string

		for _, entryID := range ids {
			res, err := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: moduleCode, RecordID: entryID,
				UserID: actor.UserID, UserEmail: actor.Email, RoleID: "",
				Action: action, Comment: req.Comment,
			})
			if err != nil {
				api.LogError("[FDAccounting] ActOnPendingOrDiagnose %s failed for %s: %v", action, entryID, err)
				errs = append(errs, entryID+": "+err.Error())
				continue
			}
			if res.Acted {
				engineActed++
			} else if !res.CancelledStale && res.Reason != "" {
				errs = append(errs, entryID+": "+res.Reason)
				continue
			} else if err := directStampAudit(ctx, pool, entryID, action, actor.Email, req.Comment); err != nil {
				errs = append(errs, entryID+": "+err.Error())
				continue
			} else {
				directActed++
			}
			// Keep entry.status in step with the audit outcome (multi-eye
			// instances may still be pending after one approver acts).
			syncEntryStatus(ctx, pool, entryID)
		}

		acted := engineActed + directActed
		payload := map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed, "errors": errs, "checker": actor.Email,
		}
		verb := "approved"
		if action == approvalengine.ActionRejected {
			verb = "rejected"
		}
		if acted == 0 && len(errs) > 0 {
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No journal entries were "+verb, payload)
			return
		}
		fdclosingcommon.RespondSuccess(w, fmt.Sprintf("Journal entr%s %s", plural(acted), verb), payload)
	}
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

// directStampAudit is the no-matrix fallback: flip the newest PENDING audit row.
func directStampAudit(ctx context.Context, pool *pgxpool.Pool, entryID, action, checkerEmail, comment string) error {
	tag, err := pool.Exec(ctx, `
		UPDATE `+journalAuditTable+`
		SET processing_status = $2, checker_by = $3, checker_at = now(), checker_comment = NULLIF($4,''), checker_ip = $5
		WHERE action_id = (
			SELECT action_id FROM `+journalAuditTable+`
			WHERE entry_id = $1 AND processing_status LIKE 'PENDING%'
			ORDER BY requested_at DESC LIMIT 1)`,
		entryID, action, api.SystemIfBlank(checkerEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no pending journal action found (already actioned or not found)")
	}
	return nil
}

// syncEntryStatus mirrors the newest audit processing_status onto entry.status
// for entries that are still in the approval part of their lifecycle.
func syncEntryStatus(ctx context.Context, pool *pgxpool.Pool, entryID string) {
	latest, err := latestAuditStatus(ctx, pool, entryID)
	if err != nil || latest == "" {
		return
	}
	var next string
	switch latest {
	case constants.StatusApproved:
		next = statusApproved
	case constants.StatusRejected:
		next = statusRejected
	default:
		return
	}
	_, _ = pool.Exec(ctx, `
		UPDATE `+journalTable+` SET status = $2
		WHERE entry_id = $1 AND status IN ('DRAFT','PENDING_APPROVAL')`, entryID, next)
	_, _ = pool.Exec(ctx, `
		UPDATE investment.accounting_activity SET status = $2
		WHERE activity_id = (SELECT activity_id FROM `+journalTable+` WHERE entry_id = $1)
		  AND status = 'PENDING_APPROVAL'`, entryID, next)
}
