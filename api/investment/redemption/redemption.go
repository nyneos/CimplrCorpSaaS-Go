package redemption

import (
	"CimplrCorpSaas/api"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types
// ---------------------------

// GetPortfolioRequest models the payload to fetch portfolio with transactions
type GetPortfolioRequest struct {
	EntityName string `json:"entity_name"`
}

// PortfolioHolding represents a single portfolio position
type PortfolioHolding struct {
	ID                  int64               `json:"id,omitempty"`
	BatchID             string              `json:"batch_id,omitempty"`
	EntityName          string              `json:"entity_name"`
	FolioNumber         *string             `json:"folio_number,omitempty"`
	DematAccNumber      *string             `json:"demat_acc_number,omitempty"`
	SchemeID            string              `json:"scheme_id"`
	SchemeName          string              `json:"scheme_name"`
	ISIN                *string             `json:"isin,omitempty"`
	TotalUnits          float64             `json:"total_units"`
	AvgNAV              float64             `json:"avg_nav"`
	CurrentNAV          float64             `json:"current_nav"`
	CurrentValue        float64             `json:"current_value"`
	TotalInvestedAmount float64             `json:"total_invested_amount"`
	GainLoss            float64             `json:"gain_loss"`
	GainLossPercent     float64             `json:"gain_loss_percent"`
	CreatedAt           string              `json:"created_at,omitempty"`
	Transactions        []TransactionDetail `json:"transactions"`
}

// TransactionDetail represents a single transaction
type TransactionDetail struct {
	ID                 int64   `json:"id"`
	BatchID            string  `json:"batch_id,omitempty"`
	TransactionDate    string  `json:"transaction_date"`
	TransactionType    string  `json:"transaction_type"`
	SchemeInternalCode *string `json:"scheme_internal_code,omitempty"`
	FolioNumber        *string `json:"folio_number,omitempty"`
	DematAccNumber     *string `json:"demat_acc_number,omitempty"`
	Amount             float64 `json:"amount"`
	Units              float64 `json:"units"`
	NAV                float64 `json:"nav"`
	SchemeID           *string `json:"scheme_id,omitempty"`
	FolioID            *string `json:"folio_id,omitempty"`
	DematID            *string `json:"demat_id,omitempty"`
	CreatedAt          string  `json:"created_at,omitempty"`
}

// CalculateRedemptionRequest models the payload for FIFO redemption calculation
type CalculateRedemptionRequest struct {
	EntityName     string  `json:"entity_name"`
	SchemeID       string  `json:"scheme_id"`
	FolioNumber    *string `json:"folio_number,omitempty"`
	DematAccNumber *string `json:"demat_acc_number,omitempty"`
	// Only one of the following should be provided
	Units   *float64 `json:"units,omitempty"`
	Amount  *float64 `json:"amount,omitempty"`
	Percent *float64 `json:"percent,omitempty"`
}

// FIFOAllocation represents a single FIFO allocation result
type FIFOAllocation struct {
	TransactionID      int64   `json:"transaction_id"`
	TransactionDate    string  `json:"transaction_date"`
	OriginalUnits      float64 `json:"original_units"`
	OriginalAmount     float64 `json:"original_amount"`
	OriginalNAV        float64 `json:"original_nav"`
	AllocatedUnits     float64 `json:"allocated_units"`
	AllocatedAmount    float64 `json:"allocated_amount"`
	RemainingUnits     float64 `json:"remaining_units"`
	GainLoss           float64 `json:"gain_loss"`
	GainLossPercent    float64 `json:"gain_loss_percent"`
	SchemeInternalCode *string `json:"scheme_internal_code,omitempty"`
	FolioNumber        *string `json:"folio_number,omitempty"`
	DematAccNumber     *string `json:"demat_acc_number,omitempty"`
}

// RedemptionCalculation represents the complete FIFO calculation result
type RedemptionCalculation struct {
	EntityName              string           `json:"entity_name"`
	SchemeID                string           `json:"scheme_id"`
	SchemeName              string           `json:"scheme_name"`
	FolioNumber             *string          `json:"folio_number,omitempty"`
	DematAccNumber          *string          `json:"demat_acc_number,omitempty"`
	TotalHolding            float64          `json:"total_holding"`
	CurrentNAV              float64          `json:"current_nav"`
	RedemptionUnits         float64          `json:"redemption_units"`
	RedemptionAmount        float64          `json:"redemption_amount"`
	RedemptionNAV           float64          `json:"redemption_nav"`
	RealizedGainLoss        float64          `json:"realized_gain_loss"`
	RealizedGainLossPercent float64          `json:"realized_gain_loss_percent"`
	RemainingUnits          float64          `json:"remaining_units"`
	RemainingValue          float64          `json:"remaining_value"`
	Allocations             []FIFOAllocation `json:"allocations"`
}

// ---------------------------
// GetPortfolioWithTransactions
// Returns portfolio holdings grouped by entity with transaction details
// ---------------------------

func GetPortfolioWithTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req GetPortfolioRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		if strings.TrimSpace(req.EntityName) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_name is required")
			return
		}

		ctx := r.Context()

		// Aggregate holdings by entity -> scheme (resolved by scheme_id/internal code/isin) and folio/demat
		// We join onboard_transaction -> portfolio_snapshot (to get entity_name via batch_id)
		aggQuery := `
			WITH ot AS (
				SELECT
					t.*, ps.entity_name,
					ms.scheme_name AS ms_scheme_name, ms.isin AS ms_isin, ms.scheme_id AS ms_scheme_id, ms.internal_scheme_code AS ms_internal_code
				FROM investment.onboard_transaction t
				LEFT JOIN investment.portfolio_snapshot ps ON t.batch_id = ps.batch_id
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = t.scheme_id OR ms.internal_scheme_code = t.scheme_internal_code OR ms.isin = t.scheme_id
				)
				WHERE ps.entity_name = $1
				  AND LOWER(COALESCE(t.transaction_type,'')) IN ('buy','purchase','subscription')
			)
			SELECT
				ot.entity_name,
				COALESCE(ot.folio_number,'') AS folio_number,
				COALESCE(ot.demat_acc_number,'') AS demat_acc_number,
				COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id) AS scheme_id,
				COALESCE(ot.ms_scheme_name, ot.scheme_internal_code) AS scheme_name,
				COALESCE(ot.ms_isin,'') AS isin,
				SUM(COALESCE(ot.units,0)) AS total_units,
				CASE WHEN SUM(COALESCE(ot.units,0))=0 THEN 0 ELSE SUM(COALESCE(ot.nav,0)*COALESCE(ot.units,0))/SUM(COALESCE(ot.units,0)) END AS avg_nav,
				SUM(COALESCE(ot.amount,0)) AS total_invested_amount
			FROM ot
			GROUP BY ot.entity_name, COALESCE(ot.folio_number,''), COALESCE(ot.demat_acc_number,''), COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id), COALESCE(ot.ms_scheme_name, ot.scheme_internal_code), COALESCE(ot.ms_isin,'')
			ORDER BY COALESCE(ot.folio_number,''), COALESCE(ot.demat_acc_number,''), COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id)
		`

		rows, err := pgxPool.Query(ctx, aggQuery, req.EntityName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		holdings := []PortfolioHolding{}
		for rows.Next() {
			var entityName, folioNumber, dematAcc, schemeID, schemeName, isin string
			var totalUnits, avgNav, totalInvested float64
			if err := rows.Scan(&entityName, &folioNumber, &dematAcc, &schemeID, &schemeName, &isin, &totalUnits, &avgNav, &totalInvested); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				return
			}

			// Fetch latest snapshot for current_nav if available
			var currentNav float64
			var totalInvestedSnapshot float64
			var snapshotNav sql.NullFloat64
			err := pgxPool.QueryRow(ctx, `
				SELECT COALESCE(current_nav,0), COALESCE(total_invested_amount,0)
				FROM investment.portfolio_snapshot
				WHERE entity_name=$1 AND scheme_id=$2 AND (folio_number=$3 OR demat_acc_number=$4)
				ORDER BY created_at DESC LIMIT 1
			`, entityName, schemeID, nullIfEmptyString(folioNumber), nullIfEmptyString(dematAcc)).Scan(&snapshotNav, &totalInvestedSnapshot)
			if err == nil {
				if snapshotNav.Valid {
					currentNav = snapshotNav.Float64
				}
			} else {
				// fallback to avgNav
				currentNav = avgNav
			}

			h := PortfolioHolding{
				ID:                  0,
				BatchID:             "",
				EntityName:          entityName,
				FolioNumber:         nil,
				DematAccNumber:      nil,
				SchemeID:            schemeID,
				SchemeName:          schemeName,
				ISIN:                nil,
				TotalUnits:          totalUnits,
				AvgNAV:              avgNav,
				CurrentNAV:          currentNav,
				CurrentValue:        totalUnits * currentNav,
				TotalInvestedAmount: totalInvested,
				GainLoss:            (totalUnits * currentNav) - totalInvested,
				GainLossPercent:     0,
				CreatedAt:           "",
				Transactions:        []TransactionDetail{},
			}
			if isin != "" {
				h.ISIN = &isin
			}
			if folioNumber != "" {
				h.FolioNumber = &folioNumber
			}
			if dematAcc != "" {
				h.DematAccNumber = &dematAcc
			}
			if totalInvested != 0 {
				h.GainLossPercent = (h.GainLoss / totalInvested) * 100.0
			}

			// Fetch individual BUY transactions for this holding (ordered FIFO)
			txQuery := `
								SELECT 
										ot.id,
										ot.batch_id,
										TO_CHAR(ot.transaction_date, 'YYYY-MM-DD') AS transaction_date,
										ot.transaction_type,
										ot.scheme_internal_code,
										ot.folio_number,
										ot.demat_acc_number,
										COALESCE(ot.amount, 0) AS amount,
										COALESCE(ot.units, 0) AS units,
										COALESCE(ot.nav, 0) AS nav,
										ot.scheme_id,
										ot.folio_id,
										ot.demat_id,
										TO_CHAR(ot.created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at
								FROM investment.onboard_transaction ot
								LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
								WHERE LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription')
									AND ( (ot.folio_number = $1) OR (ot.demat_acc_number = $2) )
									AND ( ot.scheme_id = $3 OR ot.scheme_internal_code = $4 OR ms.isin = $5 OR ms.scheme_name = $6 )
								ORDER BY ot.transaction_date ASC, ot.id ASC
						`

			txRows, err := pgxPool.Query(ctx, txQuery, folioNumber, dematAcc, schemeID, schemeID, isin, schemeName)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Transaction query failed: "+err.Error())
				return
			}

			transactions := []TransactionDetail{}
			for txRows.Next() {
				var tx TransactionDetail
				if err := txRows.Scan(
					&tx.ID,
					&tx.BatchID,
					&tx.TransactionDate,
					&tx.TransactionType,
					&tx.SchemeInternalCode,
					&tx.FolioNumber,
					&tx.DematAccNumber,
					&tx.Amount,
					&tx.Units,
					&tx.NAV,
					&tx.SchemeID,
					&tx.FolioID,
					&tx.DematID,
					&tx.CreatedAt,
				); err != nil {
					txRows.Close()
					api.RespondWithError(w, http.StatusInternalServerError, "Transaction scan failed: "+err.Error())
					return
				}
				transactions = append(transactions, tx)
			}
			txRows.Close()

			h.Transactions = transactions
			holdings = append(holdings, h)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"entity_name":    req.EntityName,
			"holdings":       holdings,
			"total_holdings": len(holdings),
		})
	}
}

// ---------------------------
// CalculateRedemptionFIFO
// Calculates FIFO-based redemption allocations
// ---------------------------

func CalculateRedemptionFIFO(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CalculateRedemptionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.EntityName) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_name is required")
			return
		}
		if strings.TrimSpace(req.SchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id is required")
			return
		}
		if req.FolioNumber == nil && req.DematAccNumber == nil {
			api.RespondWithError(w, http.StatusBadRequest, "Either folio_number or demat_acc_number is required")
			return
		}

		// Validate that only one of units/amount/percent is provided
		providedCount := 0
		if req.Units != nil {
			providedCount++
		}
		if req.Amount != nil {
			providedCount++
		}
		if req.Percent != nil {
			providedCount++
		}
		if providedCount != 1 {
			api.RespondWithError(w, http.StatusBadRequest, "Provide exactly one of: units, amount, or percent")
			return
		}

		ctx := context.Background()

		// We'll compute holdings from transaction rows so totals are aggregated across batches.
		// Fetch all BUY/PURCHASE transactions ordered by date (FIFO)
		txQuery := `
			SELECT 
				id,
				TO_CHAR(transaction_date, 'YYYY-MM-DD') AS transaction_date,
				COALESCE(units, 0) AS units,
				COALESCE(amount, 0) AS amount,
				COALESCE(nav, 0) AS nav,
				scheme_internal_code,
				folio_number,
				demat_acc_number
			FROM investment.onboard_transaction
			WHERE scheme_id = $1
				AND (
					($2::text IS NOT NULL AND folio_number = $2)
					OR ($3::text IS NOT NULL AND demat_acc_number = $3)
				)
				AND LOWER(transaction_type) IN ('buy', 'purchase', 'subscription')
			ORDER BY transaction_date ASC, id ASC
		`

		rows, err := pgxPool.Query(ctx, txQuery, req.SchemeID, req.FolioNumber, req.DematAccNumber)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction query failed: "+err.Error())
			return
		}
		defer rows.Close()

		type txItem struct {
			ID                 int64
			TransactionDate    string
			Units              float64
			Amount             float64
			NAV                float64
			SchemeInternalCode *string
			FolioNumber        *string
			DematAccNumber     *string
		}

		purchases := []txItem{}
		for rows.Next() {
			var tx txItem
			if err := rows.Scan(
				&tx.ID,
				&tx.TransactionDate,
				&tx.Units,
				&tx.Amount,
				&tx.NAV,
				&tx.SchemeInternalCode,
				&tx.FolioNumber,
				&tx.DematAccNumber,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Transaction scan failed: "+err.Error())
				return
			}
			purchases = append(purchases, tx)
		}
		// compute sum of purchased units and total invested from purchases
		var sumPurchaseUnits float64
		var sumPurchaseAmount float64
		var weightedNavNum float64
		for _, p := range purchases {
			sumPurchaseUnits += p.Units
			sumPurchaseAmount += p.Amount
			weightedNavNum += p.NAV * p.Units
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Rows error: "+rows.Err().Error())
			return
		}

		if len(purchases) == 0 {
			api.RespondWithError(w, http.StatusNotFound, "No purchase transactions found for this holding")
			return
		}

		// Fetch all existing SELL/REDEMPTION transactions to calculate already redeemed units
		sellQuery := `
			SELECT 
				TO_CHAR(transaction_date, 'YYYY-MM-DD') AS transaction_date,
				COALESCE(units, 0) AS units
			FROM investment.onboard_transaction
			WHERE scheme_id = $1
				AND (
					($2::text IS NOT NULL AND folio_number = $2)
					OR ($3::text IS NOT NULL AND demat_acc_number = $3)
				)
				AND LOWER(transaction_type) IN ('sell', 'redemption')
			ORDER BY transaction_date ASC, id ASC
		`

		sellRows, err := pgxPool.Query(ctx, sellQuery, req.SchemeID, req.FolioNumber, req.DematAccNumber)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Sell query failed: "+err.Error())
			return
		}
		defer sellRows.Close()

		existingSells := []txItem{}
		for sellRows.Next() {
			var tx txItem
			if err := sellRows.Scan(&tx.TransactionDate, &tx.Units); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Sell scan failed: "+err.Error())
				return
			}
			existingSells = append(existingSells, tx)
		}

		// compute sum of sold units
		var sumSellUnits float64
		for _, s := range existingSells {
			sumSellUnits += s.Units
		}

		// total holding available = purchases - sells
		totalUnits := sumPurchaseUnits - sumSellUnits

		// determine average NAV from purchases (weighted)
		var avgNav float64
		if sumPurchaseUnits > 0 {
			avgNav = weightedNavNum / sumPurchaseUnits
		}

		// try get current NAV from latest portfolio_snapshot; fallback to avgNav
		var currentNAV float64
		var schemeName string
		var tmpNav sql.NullFloat64
		var tmpInvest sql.NullFloat64
		if err := pgxPool.QueryRow(ctx, `
			SELECT COALESCE(current_nav,0), COALESCE(total_invested_amount,0) FROM investment.portfolio_snapshot
			WHERE entity_name=$1 AND scheme_id=$2 AND (folio_number=$3 OR demat_acc_number=$4)
			ORDER BY created_at DESC LIMIT 1
		`, req.EntityName, req.SchemeID, req.FolioNumber, req.DematAccNumber).Scan(&tmpNav, &tmpInvest); err == nil {
			if tmpNav.Valid {
				currentNAV = tmpNav.Float64
			} else {
				currentNAV = avgNav
			}
		} else {
			currentNAV = avgNav
		}

		// try to lookup scheme name from masterscheme
		if err := pgxPool.QueryRow(ctx, `SELECT COALESCE(scheme_name,'') FROM investment.masterscheme WHERE scheme_id=$1 OR internal_scheme_code=$1 OR isin=$1 LIMIT 1`, req.SchemeID).Scan(&schemeName); err != nil {
			schemeName = ""
		}

		// Calculate redemption units based on input (now that we have currentNAV and totalUnits)
		var redemptionUnits float64
		if req.Units != nil {
			redemptionUnits = *req.Units
		} else if req.Amount != nil {
			if currentNAV <= 0 {
				api.RespondWithError(w, http.StatusBadRequest, "Current NAV is zero or invalid, cannot calculate units from amount")
				return
			}
			redemptionUnits = *req.Amount / currentNAV
		} else if req.Percent != nil {
			if *req.Percent <= 0 || *req.Percent > 100 {
				api.RespondWithError(w, http.StatusBadRequest, "Percent must be between 0 and 100")
				return
			}
			redemptionUnits = totalUnits * (*req.Percent / 100.0)
		}

		// Validate redemption units
		if redemptionUnits <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Redemption units must be positive")
			return
		}
		if redemptionUnits > totalUnits {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Redemption units (%.6f) exceed total holding (%.6f)", redemptionUnits, totalUnits))
			return
		}

		if sellRows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Sell rows error: "+sellRows.Err().Error())
			return
		}

		// Apply existing redemptions to purchases (FIFO)
		purchasesCopy := make([]txItem, len(purchases))
		copy(purchasesCopy, purchases)

		for _, sell := range existingSells {
			remaining := math.Abs(sell.Units)
			for i := range purchasesCopy {
				if remaining <= 0 {
					break
				}
				if purchasesCopy[i].Units <= 0 {
					continue
				}
				if remaining >= purchasesCopy[i].Units {
					remaining -= purchasesCopy[i].Units
					purchasesCopy[i].Units = 0
				} else {
					purchasesCopy[i].Units -= remaining
					remaining = 0
				}
			}
		}

		// Now apply the new redemption using FIFO
		allocations := []FIFOAllocation{}
		remainingToAllocate := redemptionUnits
		totalAllocatedAmount := 0.0
		totalRealizedGainLoss := 0.0

		for _, purchase := range purchasesCopy {
			if remainingToAllocate <= 0 {
				break
			}
			if purchase.Units <= 0 {
				continue
			}

			var allocatedUnits float64
			if remainingToAllocate >= purchase.Units {
				allocatedUnits = purchase.Units
			} else {
				allocatedUnits = remainingToAllocate
			}

			// Calculate allocated amount at purchase NAV
			allocatedPurchaseAmount := allocatedUnits * purchase.NAV
			// Calculate realized amount at current NAV
			realizedAmount := allocatedUnits * currentNAV
			// Gain/Loss for this allocation
			gainLoss := realizedAmount - allocatedPurchaseAmount
			gainLossPercent := 0.0
			if allocatedPurchaseAmount > 0 {
				gainLossPercent = (gainLoss / allocatedPurchaseAmount) * 100.0
			}

			allocation := FIFOAllocation{
				TransactionID:      purchase.ID,
				TransactionDate:    purchase.TransactionDate,
				OriginalUnits:      purchase.Units + allocatedUnits, // Original before this allocation
				OriginalAmount:     (purchase.Units + allocatedUnits) * purchase.NAV,
				OriginalNAV:        purchase.NAV,
				AllocatedUnits:     allocatedUnits,
				AllocatedAmount:    allocatedPurchaseAmount,
				RemainingUnits:     purchase.Units,
				GainLoss:           gainLoss,
				GainLossPercent:    gainLossPercent,
				SchemeInternalCode: purchase.SchemeInternalCode,
				FolioNumber:        purchase.FolioNumber,
				DematAccNumber:     purchase.DematAccNumber,
			}

			allocations = append(allocations, allocation)
			totalAllocatedAmount += allocatedPurchaseAmount
			totalRealizedGainLoss += gainLoss
			remainingToAllocate -= allocatedUnits
		}

		// Calculate redemption metrics
		redemptionAmount := redemptionUnits * currentNAV
		realizedGainLossPercent := 0.0
		if totalAllocatedAmount > 0 {
			realizedGainLossPercent = (totalRealizedGainLoss / totalAllocatedAmount) * 100.0
		}

		result := RedemptionCalculation{
			EntityName:              req.EntityName,
			SchemeID:                req.SchemeID,
			SchemeName:              schemeName,
			FolioNumber:             req.FolioNumber,
			DematAccNumber:          req.DematAccNumber,
			TotalHolding:            totalUnits,
			CurrentNAV:              currentNAV,
			RedemptionUnits:         redemptionUnits,
			RedemptionAmount:        redemptionAmount,
			RedemptionNAV:           currentNAV,
			RealizedGainLoss:        totalRealizedGainLoss,
			RealizedGainLossPercent: realizedGainLossPercent,
			RemainingUnits:          totalUnits - redemptionUnits,
			RemainingValue:          (totalUnits - redemptionUnits) * currentNAV,
			Allocations:             allocations,
		}

		api.RespondWithPayload(w, true, "", result)
	}
}
