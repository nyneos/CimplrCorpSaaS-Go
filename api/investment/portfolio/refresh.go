package portfolio

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"

	// "context"
	"encoding/json"
	"net/http"
	"strings"

	// "time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RefreshRequest optionally scopes refresh to an entity
type RefreshRequest struct {
	EntityName *string `json:"entity_name,omitempty"`
}

// RefreshPortfolioSnapshots rebuilds portfolio snapshots. If `entity_name` is provided,
// it refreshes only for that entity. Otherwise it refreshes all snapshots (may be slow).
func RefreshPortfolioSnapshots(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req RefreshRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && r.ContentLength > 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		var entityNames []string
		if req.EntityName != nil && strings.TrimSpace(*req.EntityName) != "" {
			entityName := strings.TrimSpace(*req.EntityName)
			if !scope.HasEntityNameAccess(entityName) {
				api.RespondWithError(w, http.StatusForbidden, "entity_name is not within your authorized access scope")
				return
			}
			entityNames = []string{entityName}
		} else if len(scope.EntityNames) > 0 {
			entityNames = scope.EntityNames
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		batchID := uuid.New().String()

		// Delete existing snapshots for the scoped entity set. Only admin/no-scope contexts refresh all.
		if len(entityNames) > 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM investment.portfolio_snapshot WHERE entity_name = ANY($1::text[])`, entityNames); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete snapshots failed: "+err.Error())
				return
			}
		} else {
			// Careful: this will remove all snapshots. Keep it explicit.
			if _, err := tx.Exec(ctx, `DELETE FROM investment.portfolio_snapshot`); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete snapshots failed: "+err.Error())
				return
			}
		}

		// Build and insert aggregated snapshots from onboard_transaction
		insertQ := `
WITH scheme_resolved AS (
    SELECT
        ot.transaction_date,
        ot.transaction_type,
        ot.amount,
        ot.units,
        ot.nav,
        COALESCE(mf.entity_name, md.entity_name) AS entity_name,
        ot.folio_number,
        ot.demat_acc_number,
        ot.folio_id,
        ot.demat_id,
        COALESCE(ot.scheme_id, fsm.scheme_id, ms2.scheme_id, ms.scheme_id) AS scheme_id,
        COALESCE(ms2.scheme_name, ms.scheme_name) AS scheme_name,
        COALESCE(ms2.isin, ms.isin) AS isin
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf ON mf.folio_id = ot.folio_id
    LEFT JOIN investment.masterdemataccount md ON md.demat_id = ot.demat_id
    LEFT JOIN investment.folioschememapping fsm ON fsm.folio_id = ot.folio_id
    LEFT JOIN investment.masterscheme ms ON ms.scheme_id = fsm.scheme_id
    LEFT JOIN investment.masterscheme ms2 ON (ms2.scheme_id::text = ot.scheme_id OR ms2.internal_scheme_code = ot.scheme_internal_code)
    WHERE ($1::text[] IS NULL OR COALESCE(mf.entity_name, md.entity_name, ot.entity_name) = ANY($1::text[]))
    
    UNION ALL
    
    SELECT
        i.transaction_date,
        'Purchase' AS transaction_type,
        c.net_amount AS amount,
        c.allotted_units AS units,
        c.nav,
        i.entity_name,
        mf.folio_number,
        md.demat_account_number AS demat_acc_number,
        i.folio_id,
        i.demat_id,
        COALESCE(s.scheme_id::text, i.scheme_id) AS scheme_id,
        COALESCE(s.scheme_name, i.scheme_id) AS scheme_name,
        s.isin
    FROM investment.investment_confirmation c
    JOIN investment.investment_initiation i ON i.initiation_id = c.initiation_id
    LEFT JOIN investment.masterfolio mf ON (mf.folio_id::text = i.folio_id OR mf.folio_number = i.folio_id)
    LEFT JOIN investment.masterdemataccount md ON (md.demat_id::text = i.demat_id OR md.demat_account_number = i.demat_id)
    LEFT JOIN investment.masterscheme s ON (s.scheme_id::text = i.scheme_id OR s.internal_scheme_code = i.scheme_id OR s.isin = i.scheme_id OR s.scheme_name = i.scheme_id)
    WHERE c.status = 'CONFIRMED' AND COALESCE(c.is_deleted, false) = false
      AND ($1::text[] IS NULL OR i.entity_name = ANY($1::text[]))
      
    UNION ALL
    
    SELECT
        i.requested_date AS transaction_date,
        'Sell' AS transaction_type,
        c.gross_proceeds AS amount,
        c.actual_units AS units,
        c.actual_nav AS nav,
        i.entity_name,
        mf.folio_number,
        md.demat_account_number AS demat_acc_number,
        i.folio_id,
        i.demat_id,
        COALESCE(s.scheme_id::text, i.scheme_id) AS scheme_id,
        COALESCE(s.scheme_name, i.scheme_id) AS scheme_name,
        s.isin
    FROM investment.redemption_confirmation c
    JOIN investment.redemption_initiation i ON i.redemption_id = c.redemption_id
    LEFT JOIN investment.masterfolio mf ON (mf.folio_id::text = i.folio_id OR mf.folio_number = i.folio_id)
    LEFT JOIN investment.masterdemataccount md ON (md.demat_id::text = i.demat_id OR md.demat_account_number = i.demat_id)
    LEFT JOIN investment.masterscheme s ON (s.scheme_id::text = i.scheme_id OR s.internal_scheme_code = i.scheme_id OR s.isin = i.scheme_id OR s.scheme_name = i.scheme_id)
    WHERE c.status = 'CONFIRMED' AND COALESCE(c.is_deleted, false) = false
      AND ($1::text[] IS NULL OR i.entity_name = ANY($1::text[]))
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
        folio_id,
        demat_id,
        scheme_id,
        scheme_name,
        isin,
        SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN units
                 WHEN LOWER(transaction_type) IN ('sell','redemption') THEN units
                 ELSE units END) AS total_units,
        SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN amount ELSE 0 END) AS total_invested_amount,
        CASE WHEN SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN units ELSE 0 END) = 0 THEN 0
             ELSE SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN nav * units ELSE 0 END) /
                  SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN units ELSE 0 END)
        END AS avg_nav
    FROM scheme_resolved
    GROUP BY entity_name, folio_number, demat_acc_number, folio_id, demat_id, scheme_id, scheme_name, isin
),
latest_nav AS (
    SELECT DISTINCT ON (scheme_name)
        scheme_name,
        isin_div_payout_growth as isin,
        nav_value,
        nav_date
    FROM investment.amfi_nav_staging
    ORDER BY scheme_name, nav_date DESC
)
INSERT INTO investment.portfolio_snapshot (
  batch_id, entity_name, folio_number, demat_acc_number, folio_id, demat_id,
  scheme_id, scheme_name, isin, total_units, avg_nav, current_nav, current_value,
  total_invested_amount, gain_loss, gain_losss_percent, created_at
)
SELECT
  $2 AS batch_id,
  ts.entity_name,
  ts.folio_number,
  ts.demat_acc_number,
  ts.folio_id,
  ts.demat_id,
  ts.scheme_id,
  ts.scheme_name,
  ts.isin,
  ts.total_units,
  ts.avg_nav,
  COALESCE(ln.nav_value, 0) AS current_nav,
  ts.total_units * COALESCE(ln.nav_value, 0) AS current_value,
  ts.total_invested_amount,
  (ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount AS gain_loss,
  CASE WHEN ts.total_invested_amount = 0 THEN 0 ELSE (((ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount) / ts.total_invested_amount) * 100 END AS gain_losss_percent,
  NOW()
FROM transaction_summary ts
LEFT JOIN latest_nav ln ON (ln.scheme_name = ts.scheme_name OR ln.isin = ts.isin)
WHERE ts.total_units > 0;
`

		// Execute insert with entity filter (nil => NULL)
		var entityArg interface{}
		if len(entityNames) > 0 {
			entityArg = entityNames
		} else {
			entityArg = nil
		}

		if _, err := tx.Exec(ctx, insertQ, entityArg, batchID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "insert snapshots failed: "+err.Error())
			return
		}

		// Commit
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "snapshots refreshed", map[string]any{"batch_id": batchID, "entity_names": entityNames})
	}
}
