package fdMonthEndClosing

import (
	"context"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/fdMonthEndClosing/cycle"

	"github.com/jackc/pgx/v5/pgxpool"
)

// init registers the post-finalize hooks this module needs, mirroring
// fdBookingWorkbench/approvalHooks.go's shape (approvalengine.RegisterPostFinalizeHook
// called from an init() in the module's top-level package).
//
// Only the EDIT transaction type needs a hook. DELETE does not: the generic
// finalizer (api/approvalengine/finalizer.go's finalizeRecord) already flips
// is_deleted=true on investment.fd_closing_cycle by itself, synchronously,
// inside the engine's own transaction, because ActionType=="DELETE" and the
// instance's RecordTable/AuditIDColumn point straight at
// investment.fd_closing_cycle/cycle_id — no per-table opt-in needed for that
// generic column flip. See cycle/delete.go's doc comment.
//
// EDIT is different because the staged new_bank_id/new_currency_code/
// new_include_matured values are custom to this module — finalizeRecord has
// no way to know about them. So once the engine fully approves an EDIT
// instance, this hook copies those staged values onto the master row. It
// runs OUTSIDE the approval-engine transaction (RecordAction fires post-finalize
// hooks in a goroutine, after its own transaction commits — see
// RunPostFinalizeHook in api/approvalengine/moduleconfig.go), so it opens its
// own short transaction here.
func init() {
	approvalengine.RegisterPostFinalizeHook(cycle.TxCreateCycle, func(ctx context.Context, pool *pgxpool.Pool, cycleID, transactionType, finalStatus, actorEmail, comment string) {
		if finalStatus != approvalengine.InstStatusRejected {
			return
		}
		if _, err := pool.Exec(ctx, `UPDATE investment.fd_closing_cycle SET is_deleted = true WHERE cycle_id = $1`, cycleID); err != nil {
			api.LogError("[FDClosingCycle] post-finalize CREATE reject soft-delete failed for cycle=%s: %v", cycleID, err)
		}
	})

	approvalengine.RegisterPostFinalizeHook(cycle.TxEditCycle, func(ctx context.Context, pool *pgxpool.Pool, cycleID, transactionType, finalStatus, actorEmail, comment string) {
		if finalStatus != approvalengine.InstStatusApproved {
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogError("[FDClosingCycle] post-finalize EDIT begin tx failed for cycle=%s: %v", cycleID, err)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// By the time this hook runs, finalizeRecord has ALREADY flipped the
		// audit row's processing_status to APPROVED inside the engine's own
		// transaction — so match on APPROVED here, not PENDING_EDIT_APPROVAL,
		// and do not flip it again (flipAuditStatus=false).
		if err := cycle.ApplyEditToMaster(ctx, tx, cycleID, api.SystemIfBlank(actorEmail), comment, constants.StatusApproved, false); err != nil {
			api.LogError("[FDClosingCycle] post-finalize EDIT apply failed for cycle=%s: %v", cycleID, err)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.LogError("[FDClosingCycle] post-finalize EDIT commit failed for cycle=%s: %v", cycleID, err)
			return
		}
		api.LogInfo("[FDClosingCycle] post-finalize EDIT applied new_* onto master for cycle=%s", cycleID)
	})
}
