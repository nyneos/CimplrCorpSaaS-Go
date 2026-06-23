package portfolio

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/schemejoin"
	"CimplrCorpSaas/internal/ctxutil"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HoldingsFilter struct {
	EntityName string `json:"entity_name"`
	AmcName    string `json:"amc_name"`
	SchemeName string `json:"scheme_name"`
}

type PortfolioHoldingsRow struct {
	EntityName          string                   `json:"entity_name"`
	FolioNumber         string                   `json:"folio_number"`
	DematAccountNumber  string                   `json:"demat_account_number"`
	SchemeID            string                   `json:"scheme_id"`
	SchemeName          string                   `json:"scheme_name"`
	ISIN                string                   `json:"isin"`
	AmcName             string                   `json:"amc_name"`
	TotalUnits          float64                  `json:"total_units"`
	AvgNav              float64                  `json:"avg_nav"`
	CurrentNav          float64                  `json:"current_nav"`
	CurrentValue        float64                  `json:"current_value"`
	GainLoss            float64                  `json:"gain_loss"`             // unrealized on remaining units
	GainLossPercent     float64                  `json:"gain_loss_percent"`
	RealizedGainLoss    float64                  `json:"realized_gain_loss"`    // profit/loss from redemptions
	TotalGainLoss       float64                  `json:"total_gain_loss"`       // realized + unrealized
	TotalInvestedAmount float64                  `json:"total_invested_amount"` // remaining cost basis
	UpdatedAt           string                   `json:"updated_at"`
	Transactions        []map[string]interface{} `json:"transactions"`
}

func GetPortfolioHoldings(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		var f HoldingsFilter
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil && r.ContentLength > 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		f.EntityName = strings.TrimSpace(f.EntityName)
		f.AmcName = strings.TrimSpace(f.AmcName)
		f.SchemeName = strings.TrimSpace(f.SchemeName)

		if f.EntityName != "" && !scope.HasEntityNameAccess(f.EntityName) {
			api.RespondWithError(w, http.StatusForbidden, "entity_name is not within your authorized access scope")
			return
		}

		dumpAll := f.EntityName == "" && f.AmcName == "" && f.SchemeName == ""

		results, err := queryHoldingsFromSnapshot(ctx, pgxPool, f, dumpAll, scope.EntityNames)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		if len(results) == 0 {
			results, err = queryHoldingsFromTransactions(ctx, pgxPool, f, dumpAll, scope.EntityNames)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "live holdings query failed: "+err.Error())
				return
			}
		}

		if len(results) > 0 {
			txQuery := `
WITH scheme_resolved AS (
    SELECT
        ot.transaction_date,
        ot.transaction_type,
        ot.amount,
        ot.units,
        ot.nav,
        TRIM(COALESCE(mf.entity_name, md.entity_name, ot.entity_name, '')) AS entity_name,
        ot.folio_number,
        ot.demat_acc_number,
        COALESCE(ms.scheme_id::text, ot.scheme_id) AS scheme_id,
        COALESCE(ms.scheme_name, '') AS scheme_name,
        COALESCE(ms.isin, '') AS isin,
        ot.created_at AS sort_at
    FROM investment.approved_onboard_transaction ot
    LEFT JOIN investment.masterfolio mf ON mf.folio_id = ot.folio_id
    LEFT JOIN investment.masterdemataccount md ON md.demat_id = ot.demat_id
    LEFT JOIN investment.masterscheme ms ON (` + schemejoin.JoinOnboardTx + `)

    UNION ALL

    SELECT
        i.transaction_date,
        'Purchase' AS transaction_type,
        c.net_amount AS amount,
        c.allotted_units AS units,
        c.nav,
        TRIM(i.entity_name) AS entity_name,
        mf.folio_number,
        md.demat_account_number AS demat_acc_number,
        COALESCE(s.scheme_id::text, i.scheme_id) AS scheme_id,
        COALESCE(s.scheme_name, '') AS scheme_name,
        COALESCE(s.isin, '') AS isin,
        COALESCE(c.confirmed_at, c.updated_at, i.transaction_date::timestamptz) AS sort_at
    FROM investment.investment_confirmation c
    JOIN investment.investment_initiation i ON i.initiation_id = c.initiation_id
    LEFT JOIN investment.masterfolio mf ON (mf.folio_id::text = i.folio_id OR mf.folio_number = i.folio_id)
    LEFT JOIN investment.masterdemataccount md ON (md.demat_id::text = i.demat_id OR md.demat_account_number = i.demat_id)
    LEFT JOIN investment.masterscheme s ON (` + schemejoin.JoinInitiationRef + `)
    WHERE c.status = 'CONFIRMED' AND COALESCE(c.is_deleted, false) = false
)
SELECT 
    entity_name, scheme_name, isin, scheme_id,
    folio_number, demat_acc_number,
    transaction_date, ` + SQLNormalizeTransactionType + ` AS transaction_type, amount, units, nav
FROM scheme_resolved
ORDER BY transaction_date ASC, sort_at ASC
`
			txRows, txErr := pgxPool.Query(ctx, txQuery)
			if txErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "transactions query failed: "+txErr.Error())
				return
			}
			defer txRows.Close()
				for txRows.Next() {
					var eName, sName, isin, sID *string
					var fNum, dNum *string
					var tDate *time.Time
					var tType string
					var amount, units, nav float64

					if err := txRows.Scan(&eName, &sName, &isin, &sID, &fNum, &dNum, &tDate, &tType, &amount, &units, &nav); err == nil {
						// Match transaction to holding
						for i := range results {
							h := &results[i]
							enMatch := eName != nil && strings.TrimSpace(*eName) == strings.TrimSpace(h.EntityName)
							sMatch := false
							if sID != nil && strings.TrimSpace(*sID) != "" && strings.TrimSpace(h.SchemeID) != "" {
								sMatch = strings.TrimSpace(*sID) == strings.TrimSpace(h.SchemeID)
							} else if isin != nil && strings.TrimSpace(*isin) != "" && strings.TrimSpace(h.ISIN) != "" {
								sMatch = strings.TrimSpace(*isin) == strings.TrimSpace(h.ISIN)
							} else if sName != nil && strings.TrimSpace(*sName) != "" {
								sMatch = strings.TrimSpace(*sName) == strings.TrimSpace(h.SchemeName)
							}
							fMatch := fNum == nil || *fNum == "" || *fNum == h.FolioNumber
							dMatch := dNum == nil || *dNum == "" || *dNum == h.DematAccountNumber

							if enMatch && sMatch && fMatch && dMatch {
								dateStr := ""
								if tDate != nil {
									dateStr = tDate.Format(constants.DateFormat)
								}
								h.Transactions = append(h.Transactions, map[string]interface{}{
									"transaction_date": dateStr,
									"transaction_type": NormalizeTransactionType(tType),
									"amount":           amount,
									"units":            units,
									"nav":              nav,
									"folio_number":     fNum,
									"demat_acc_number": dNum,
								})
							}
						}
					}
				}
		}

		api.RespondWithPayload(w, true, "", results)
	}
}
