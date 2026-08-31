package scope

import (
	"context"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// init registers the post-finalize hook this package needs, mirroring
// ../cycle's approach (its hook lives in the parent fdMonthEndClosing package
// because it references the cycle package's exported symbols from outside;
// this hook lives entirely inside scope itself since applyScopeAddApproval is
// package-private here) and fdBookingWorkbench/approvalHooks.go's shape
// (approvalengine.RegisterPostFinalizeHook called from an init()). Registering
// from this package's own init() — rather than editing the shared top-level
// fdMonthEndClosing/approvalHooks.go — means this file needs no coordination
// with the sibling agents concurrently adding checklist/lock/reopen/evidencePack
// hooks: Go runs every package's init() exactly once, and
// approvalengine.RegisterPostFinalizeHook just adds another entry to a shared
// map keyed by transaction type, so many packages can each register their own
// hook with zero risk of clobbering another's registration.
//
// Only TxScopeAdd (FD_CLOSING_SCOPE_ADD) needs a hook. TxScopeRemove does not:
// the generic finalizer (api/approvalengine/finalizer.go's finalizeRecord)
// already flips is_deleted=true on investment.fd_closing_cycle_fd_scope by
// itself, synchronously, inside the engine's own transaction, because
// ActionType=="DELETE" and the instance's RecordTable/AuditIDColumn point
// straight at investment.fd_closing_cycle_fd_scope/scope_id — no per-table
// opt-in needed for that generic column flip. See delete.go's doc comment.
//
// ADD is different because approval must also flip selection_status='APPROVED'
// and seed the 5 fd_closing_checklist_item rows — both custom to this module,
// so finalizeRecord has no way to know about them. Once the engine fully
// approves a CREATE (Add) instance, this hook applies both via
// applyScopeAddApproval. It runs OUTSIDE the approval-engine transaction
// (RecordAction fires post-finalize hooks in a goroutine, after its own
// transaction commits — see RunPostFinalizeHook in
// api/approvalengine/moduleconfig.go), so it opens its own short transaction
// here.
func init() {
	approvalengine.RegisterPostFinalizeHook(TxScopeAdd, func(ctx context.Context, pool *pgxpool.Pool, scopeID, transactionType, finalStatus, actorEmail, comment string) {
		if finalStatus != approvalengine.InstStatusApproved {
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogError("[FDClosingScope] post-finalize ADD begin tx failed for scope=%s: %v", scopeID, err)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// By the time this hook runs, finalizeRecord has ALREADY flipped the
		// audit row's processing_status to APPROVED inside the engine's own
		// transaction — so match on APPROVED here, not PENDING_APPROVAL, and do
		// not flip it again (flipAuditStatus=false).
		if err := applyScopeAddApproval(ctx, tx, scopeID, api.SystemIfBlank(actorEmail), comment, constants.StatusApproved, false); err != nil {
			api.LogError("[FDClosingScope] post-finalize ADD apply failed for scope=%s: %v", scopeID, err)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.LogError("[FDClosingScope] post-finalize ADD commit failed for scope=%s: %v", scopeID, err)
			return
		}
		api.LogInfo("[FDClosingScope] post-finalize ADD applied selection_status + checklist seed for scope=%s", scopeID)
	})
}
