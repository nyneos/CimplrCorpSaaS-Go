package portfolio

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
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
	AmfiSchemeCode      string                   `json:"amfi_scheme_code"`
	TotalUnits          float64                  `json:"total_units"`
	AvgNav              float64                  `json:"avg_nav"`
	CurrentNav          float64                  `json:"current_nav"`
	CurrentValue        float64                  `json:"current_value"`
	GainLoss            float64                  `json:"gain_loss"` // unrealized on remaining units
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

		if f.EntityName != "" && !scope.HasEntityNameAccess(f.EntityName) && !scope.HasEntityAccess(f.EntityName) {
			api.RespondWithError(w, http.StatusForbidden, "entity_name is not within your authorized access scope")
			return
		}

		dumpAll := f.EntityName == "" && f.AmcName == "" && f.SchemeName == ""

		results, err := queryHoldingsFromTransactions(ctx, pgxPool, f, dumpAll, scope.EntityNames)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "live holdings query failed: "+err.Error())
			return
		}
		results = dedupeHoldingsRows(results)

		if len(results) > 0 {
			txFilter := TxFilter{AllowedEntities: scope.EntityNames}
			if f.EntityName != "" {
				txFilter.EntityName = f.EntityName
			}
			from := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
			allTx, txErr := QueryPortfolioTransactions(ctx, pgxPool, TxFilter{
				EntityName:      txFilter.EntityName,
				AllowedEntities: txFilter.AllowedEntities,
				DateFrom:        from.Format("2006-01-02"),
				DateTo:          time.Now().UTC().Format("2006-01-02"),
			})
			if txErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "transactions query failed: "+txErr.Error())
				return
			}
			attachTransactionsToHoldings(results, allTx)
		}

		api.RespondWithPayload(w, true, "", results)
	}
}
