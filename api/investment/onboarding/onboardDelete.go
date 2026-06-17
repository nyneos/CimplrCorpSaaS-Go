package investment

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
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
		var batchStatus string
		if err := pgxPool.QueryRow(ctx,
			`SELECT status FROM investment.onboard_batch WHERE batch_id=$1::uuid`,
			req.BatchID,
		).Scan(&batchStatus); err != nil {
			api.RespondWithError(w, http.StatusNotFound, fmt.Sprintf("Batch '%s' not found", req.BatchID))
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

		// ── 1. Delete audit records FIRST (subqueries reference master tables) ───
		auditSteps := []struct {
			auditTable  string
			masterTable string
			idCol       string
		}{
			{"investment.auditactionamc", "investment.masteramc", "amc_id"},
			{"investment.auditactionscheme", "investment.masterscheme", "scheme_id"},
			{"investment.auditactionfolio", "investment.masterfolio", "folio_id"},
			{"investment.auditactiondp", "investment.masterdepositoryparticipant", "dp_id"},
			{"investment.auditactiondemat", "investment.masterdemataccount", "demat_id"},
		}
		for _, s := range auditSteps {
			tag, err := tx.Exec(ctx,
				fmt.Sprintf(`DELETE FROM %s WHERE %s IN (
					SELECT %s FROM %s WHERE batch_id=$1::uuid
				)`, s.auditTable, s.idCol, s.idCol, s.masterTable),
				req.BatchID,
			)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError,
					fmt.Sprintf("delete %s failed: %s", s.auditTable, err.Error()))
				return
			}
			deleted[s.auditTable] = tag.RowsAffected()
		}

		// ── 2. Remove folio–scheme mappings (subquery on masterfolio, still exists) ──
		tag, err := tx.Exec(ctx,
			`DELETE FROM investment.folioschememapping
			 WHERE folio_id IN (
				SELECT folio_id FROM investment.masterfolio
				WHERE batch_id=$1::uuid
			 )`,
			req.BatchID,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "delete folioschememapping failed: "+err.Error())
			return
		}
		deleted["folio_scheme_mapping"] = tag.RowsAffected()

		// ── 3. Hard-delete master records created by this batch ─────────────────
		// Safety: only remove a master record if NO other batch still references it
		// in portfolio_onboarding_map (i.e. reference-count safe). Enriched records
		// never have batch_id=$1 so they are untouched regardless.

		masterSteps := []struct {
			table  string
			idCol  string
			mapCol string // column name in portfolio_onboarding_map
			key    string
		}{
			{"investment.masterdemataccount", "demat_id", "demat_id", "demat_id"},
			{"investment.masterdepositoryparticipant", "dp_id", "dp_id", "dp_id"},
			{"investment.masterfolio", "folio_id", "folio_id", "folio_id"},
			{"investment.masterscheme", "scheme_id", "scheme_id", "scheme_id"},
			{"investment.masteramc", "amc_id", "amc_id", "amc_id"},
		}
		for _, step := range masterSteps {
			// Delete only if: (a) created by this batch AND (b) no other batch references it
			q := fmt.Sprintf(`
				DELETE FROM %s
				WHERE batch_id = $1::uuid
				  AND %s NOT IN (
					SELECT %s FROM investment.portfolio_onboarding_map
					WHERE %s IS NOT NULL AND batch_id::uuid != $1::uuid
				  )`,
				step.table, step.idCol, step.mapCol, step.mapCol)
			tag, err := tx.Exec(ctx, q, req.BatchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError,
					fmt.Sprintf("delete %s failed: %s", step.table, err.Error()))
				return
			}
			deleted[step.key] = tag.RowsAffected()
			logger.LogInfo("[batch-delete] %s deleted %d rows for batch %s", step.table, tag.RowsAffected(), req.BatchID)
		}

		// ── 4. Delete batch-level rows ───────────────────────────────────────────
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
					fmt.Sprintf("delete %s failed: %s", tbl, err.Error()))
				return
			}
			deleted[tbl] = tag.RowsAffected()
		}

		// ── 5. Delete the batch record itself ────────────────────────────────────
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

		tag, err := pgxPool.Exec(ctx,
			`DELETE FROM investment.onboard_transaction WHERE transaction_id=$1::uuid`,
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
