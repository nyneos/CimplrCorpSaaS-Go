package checklist

import (
	"context"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Register checklist post-finalize hooks. DELETE cannot rely on the generic
// finalizer alone because we soft-delete AND reseed a fresh NOT_STARTED row.
func init() {
	approvalengine.RegisterPostFinalizeHook(TxEditChecklist, func(ctx context.Context, pool *pgxpool.Pool, itemID, transactionType, finalStatus, actorEmail, comment string) {
		if finalStatus != approvalengine.InstStatusApproved {
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogError("[FDClosingChecklist] post-finalize EDIT begin tx failed for item=%s: %v", itemID, err)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		if err := ApplyEditToMaster(ctx, tx, itemID, api.SystemIfBlank(actorEmail), comment, constants.StatusApproved, false); err != nil {
			api.LogError("[FDClosingChecklist] post-finalize EDIT apply failed for item=%s: %v", itemID, err)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.LogError("[FDClosingChecklist] post-finalize EDIT commit failed for item=%s: %v", itemID, err)
			return
		}
		api.LogInfo("[FDClosingChecklist] post-finalize EDIT applied for item=%s", itemID)
	})

	approvalengine.RegisterPostFinalizeHook(TxDeleteChecklist, func(ctx context.Context, pool *pgxpool.Pool, itemID, transactionType, finalStatus, actorEmail, comment string) {
		if finalStatus != approvalengine.InstStatusApproved {
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogError("[FDClosingChecklist] post-finalize DELETE begin tx failed for item=%s: %v", itemID, err)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		// Audit may already be APPROVED by finalizeRecord — match APPROVED.
		if err := ApplyDeleteToMaster(ctx, tx, itemID, api.SystemIfBlank(actorEmail), comment, constants.StatusApproved, false); err != nil {
			api.LogError("[FDClosingChecklist] post-finalize DELETE apply failed for item=%s: %v", itemID, err)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.LogError("[FDClosingChecklist] post-finalize DELETE commit failed for item=%s: %v", itemID, err)
			return
		}
		api.LogInfo("[FDClosingChecklist] post-finalize DELETE soft-deleted+reseeded for item=%s", itemID)
	})
}
