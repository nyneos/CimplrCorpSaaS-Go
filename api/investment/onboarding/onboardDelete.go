package investment

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/policyengine/common"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// DeleteOnboardBatch hard-resets a batch:
//   - hard-deletes master records that were CREATED by this batch (batch_id matches)
//   - leaves enriched (pre-existing) records untouched
//   - removes audit rows for batch-created records
//   - removes all batch-level rows (transactions, mappings, snapshot, batch itself)
//
// Only PENDING_APPROVAL records can be deleted this way — once approved, use the
// individual master delete flows so the checker/maker trail is preserved.
func DeleteOnboardBatch(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			BatchID string `json:"batch_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.BatchID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "batch_id is required")
			return
		}

		// Verify batch exists and is still in a deletable state
		batchRow, err := loadMFOnboardBatchRow(ctx, pgxPool, req.BatchID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, fmt.Sprintf("Batch '%s' not found", req.BatchID))
			return
		}

		userEmail := api.GetUserNameFromCtx(ctx)
		if !mfEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreDelete, HandlerName: "DeleteOnboardBatch",
			APIPath: "/investment/onboard/batch/delete", EntityCode: req.BatchID, Actor: userEmail},
			buildMFOnboardBatchPolicyFields(batchRow, "DELETE", "")) {
			return
		}

		tx, err := pgxPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBegin+err.Error())
			return
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		deleted := map[string]int64{}

		// ── 1. Master records are no longer deleted here since batch_id is dropped ───
		// The onboarding batch now only acts as a container for transactions and snapshots.

		// ── 2. Delete batch-level rows ───────────────────────────────────────────
		for _, tbl := range []string{
			"investment.onboard_transaction",
			"investment.portfolio_onboarding_map",
			"investment.onboard_mapping",
			"investment.portfolio_snapshot",
		} {
			tag, err := tx.Exec(ctx,
				fmt.Sprintf(`DELETE FROM %s WHERE batch_id=$1::uuid`, tbl),
				req.BatchID,
			)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError,
					fmt.Sprintf(constants.ErrFailedToDeleteItem, tbl, err.Error()))
				return
			}
			deleted[tbl] = tag.RowsAffected()
		}

		// ── 3. Delete the batch record itself ────────────────────────────────────
		if _, err := tx.Exec(ctx,
			`DELETE FROM investment.onboard_batch WHERE batch_id=$1::uuid`,
			req.BatchID,
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "delete batch failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		tx = nil

		logger.LogInfo("[batch-delete] batch %s fully deleted. summary: %+v", req.BatchID, deleted)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"batch_id": req.BatchID,
			"deleted":  deleted,
			"message":  "Batch and all non-enriched records have been deleted.",
		})
	}
}

// DeleteOnboardTransaction hard-deletes a single onboard_transaction row by ID.
// The parent batch and master records are NOT touched.
func DeleteOnboardTransaction(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req struct {
			TransactionID string `json:"transaction_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TransactionID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "transaction_id is required")
			return
		}

		userEmail := api.GetUserNameFromCtx(ctx)
		txnRow, err := loadMFOnboardTransactionRow(ctx, pgxPool, req.TransactionID)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, fmt.Sprintf("transaction '%s' not found", req.TransactionID))
			return
		}
		if !mfEnforce(ctx, w, r, pgxPool, enforceCtx{EventCode: common.TriggerPreDelete, HandlerName: "DeleteOnboardTransaction",
			APIPath: "/investment/onboard/batch/delete-transaction", EntityCode: txnRow.BatchID, Actor: userEmail},
			buildMFOnboardTransactionPolicyFields(txnRow)) {
			return
		}

		tag, err := pgxPool.Exec(ctx,
			`DELETE FROM investment.onboard_transaction WHERE id=$1::bigint`,
			req.TransactionID,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "delete transaction failed: "+err.Error())
			return
		}
		if tag.RowsAffected() == 0 {
			api.RespondWithError(w, http.StatusNotFound, fmt.Sprintf("transaction '%s' not found", req.TransactionID))
			return
		}

		logger.LogInfo("[tx-delete] onboard_transaction %s deleted", req.TransactionID)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"transaction_id": req.TransactionID,
			"message":        "Transaction deleted.",
		})
	}
}
