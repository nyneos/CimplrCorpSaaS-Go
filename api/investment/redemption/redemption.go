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

	"CimplrCorpSaas/api/constants"

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
	DefaultRedemptionAccount   string        `json:"default_redemption_account"`
	DefaultSettlementAccount   string        `json:"default_settlement_account"`
	CreditBankAccount          string        `json:"credit_bank_account"`
	SchemeID            string              `json:"scheme_id"`
	SchemeName          string              `json:"scheme_name"`
	ISIN                *string             `json:"isin,omitempty"`
	TotalUnits          float64             `json:"total_units"`
	BlockedUnits        float64             `json:"blocked_units"`
	AvailableUnits      float64             `json:"available_units"`
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		if strings.TrimSpace(req.EntityName) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrEntityNameRequired)
			return
		}

		ctx := r.Context()

		// Aggregate holdings by entity -> scheme (resolved by scheme_id/internal code/isin) and folio/demat
		// We compute net units = sum(buys) - sum(sells) but compute avg_nav and invested amount from buys only.
		// blocked_units: Sum directly from onboard_transaction.blocked_units field
		aggQuery := `
			WITH latest_snapshots AS (
				SELECT DISTINCT ON (entity_name, scheme_id, COALESCE(folio_id, folio_number), COALESCE(demat_id, demat_acc_number))
					entity_name, scheme_id, folio_id, folio_number, demat_id, demat_acc_number
				FROM investment.portfolio_snapshot
				WHERE entity_name = $1
				ORDER BY entity_name, scheme_id, COALESCE(folio_id, folio_number), COALESCE(demat_id, demat_acc_number), created_at DESC
			),
			ot AS (
				SELECT
					t.id, t.batch_id, t.transaction_date, t.transaction_type, t.scheme_internal_code,
					t.folio_number, t.demat_acc_number, t.amount, t.units, t.nav,
					t.scheme_id, t.folio_id, t.demat_id, t.created_at,
					COALESCE(t.blocked_units, 0) AS blocked_units,
					COALESCE(t.entity_name, ls.entity_name) AS entity_name,
					ms.scheme_name AS ms_scheme_name, ms.isin AS ms_isin, ms.scheme_id AS ms_scheme_id, ms.internal_scheme_code AS ms_internal_code
				FROM investment.onboard_transaction t
				LEFT JOIN latest_snapshots ls ON 
					t.scheme_id = ls.scheme_id AND (
						(t.folio_id IS NOT NULL AND t.folio_id = ls.folio_id) OR
						(t.demat_id IS NOT NULL AND t.demat_id = ls.demat_id) OR
						(t.folio_number IS NOT NULL AND t.folio_number = ls.folio_number) OR
						(t.demat_acc_number IS NOT NULL AND t.demat_acc_number = ls.demat_acc_number)
					)
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = t.scheme_id OR ms.internal_scheme_code = t.scheme_internal_code OR ms.isin = t.scheme_id
				)
				-- filter strictly by resolved entity to avoid cross-entity leakage
				WHERE COALESCE(t.entity_name, ls.entity_name) = $1
					AND LOWER(COALESCE(t.transaction_type,'')) IN ('buy','purchase','subscription','sell','redemption')
			)
			SELECT
				ot.entity_name,
				COALESCE(ot.folio_number,'') AS folio_number,
				COALESCE(ot.demat_acc_number,'') AS demat_acc_number,
				COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id) AS scheme_id,
				COALESCE(ot.ms_scheme_name, ot.scheme_internal_code) AS scheme_name,
				COALESCE(ot.ms_isin,'') AS isin,
				COALESCE(ot.folio_id, '') AS folio_id,
				COALESCE(ot.demat_id, '') AS demat_id,
				-- total units: (buys - sells). Do NOT subtract blocked here; blocked is derived from redemption initiations.
				(SUM(CASE WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') THEN COALESCE(ot.units,0) ELSE 0 END)
					- SUM(CASE WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption') THEN ABS(COALESCE(ot.units,0)) ELSE 0 END)) AS units,
				-- avg_nav based only on buy transactions
				CASE WHEN SUM(CASE WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') THEN COALESCE(ot.units,0) ELSE 0 END)=0 THEN 0
					ELSE SUM(CASE WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') THEN COALESCE(ot.nav,0)*COALESCE(ot.units,0) ELSE 0 END)
						/ SUM(CASE WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') THEN COALESCE(ot.units,0) ELSE 0 END)
				END AS avg_nav,
			-- invested amount based only on buy transactions
			SUM(CASE WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') THEN COALESCE(ot.amount,0) ELSE 0 END) AS total_invested_amount
			FROM ot
			GROUP BY ot.entity_name, COALESCE(ot.folio_number,''), COALESCE(ot.demat_acc_number,''),
				COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id), 
				COALESCE(ot.ms_scheme_name, ot.scheme_internal_code), 
				COALESCE(ot.ms_isin,''),
				COALESCE(ot.folio_id, ''),
				COALESCE(ot.demat_id, '')
			HAVING (SUM(CASE WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription') THEN COALESCE(ot.units,0) ELSE 0 END)
					- SUM(CASE WHEN LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption') THEN ABS(COALESCE(ot.units,0)) ELSE 0 END)) > 0
			ORDER BY COALESCE(ot.folio_number,''), COALESCE(ot.demat_acc_number,''), COALESCE(ot.scheme_id, ot.scheme_internal_code, ot.ms_scheme_id)
		`

		rows, err := pgxPool.Query(ctx, aggQuery, req.EntityName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		holdings := []PortfolioHolding{}
		for rows.Next() {
			var entityName, folioNumber, dematAcc, schemeID, schemeName, isin, folioIDStr, dematIDStr string
			var units, avgNav, totalInvested float64
			if err := rows.Scan(&entityName, &folioNumber, &dematAcc, &schemeID, &schemeName, &isin, &folioIDStr, &dematIDStr, &units, &avgNav, &totalInvested); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScanFailedPrefix+err.Error())
				return
			}

			// Ensure units are not negative
			if units < 0 {
				units = 0
			}

			// Fetch latest snapshot for current_nav if available
			// Use the folio_id/demat_id we got from the query
			var folioID, dematID sql.NullString
			if folioIDStr != "" {
				folioID.String = folioIDStr
				folioID.Valid = true
			} else if folioNumber != "" {
				_ = pgxPool.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND entity_name=$2 LIMIT 1`, folioNumber, entityName).Scan(&folioID)
			}
			if dematIDStr != "" {
				dematID.String = dematIDStr
				dematID.Valid = true
			} else if dematAcc != "" {
				_ = pgxPool.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number=$1 AND entity_name=$2 LIMIT 1`, dematAcc, entityName).Scan(&dematID)
			}

			// Pull default accounts from masters (as defined in folio/demat masters)
			var defaultRedAcct, defaultSettlementAcct sql.NullString
			if folioID.Valid {
				_ = pgxPool.QueryRow(ctx, `
					SELECT COALESCE(default_redemption_account,'')
					FROM investment.masterfolio
					WHERE folio_id=$1 AND entity_name=$2 AND COALESCE(is_deleted,false)=false
					LIMIT 1
				`, folioID.String, entityName).Scan(&defaultRedAcct)
			} else if folioNumber != "" {
				_ = pgxPool.QueryRow(ctx, `
					SELECT COALESCE(default_redemption_account,'')
					FROM investment.masterfolio
					WHERE folio_number=$1 AND entity_name=$2 AND COALESCE(is_deleted,false)=false
					LIMIT 1
				`, folioNumber, entityName).Scan(&defaultRedAcct)
			}

			if dematID.Valid {
				_ = pgxPool.QueryRow(ctx, `
					SELECT COALESCE(default_settlement_account,'')
					FROM investment.masterdemataccount
					WHERE demat_id=$1 AND entity_name=$2 AND COALESCE(is_deleted,false)=false
					LIMIT 1
				`, dematID.String, entityName).Scan(&defaultSettlementAcct)
			} else if dematAcc != "" {
				_ = pgxPool.QueryRow(ctx, `
					SELECT COALESCE(default_settlement_account,'')
					FROM investment.masterdemataccount
					WHERE demat_account_number=$1 AND entity_name=$2 AND COALESCE(is_deleted,false)=false
					LIMIT 1
				`, dematAcc, entityName).Scan(&defaultSettlementAcct)
			}

			var currentNav float64
			var totalInvestedSnapshot float64
			var snapshotNav sql.NullFloat64

			// Try fetching snapshot by folio_id/demat_id first, fallback to folio_number/demat_acc_number
			err := pgxPool.QueryRow(ctx, `
				SELECT COALESCE(current_nav,0), COALESCE(total_invested_amount,0)
				FROM investment.portfolio_snapshot
				WHERE entity_name=$1 AND scheme_id=$2 
					AND ((folio_id IS NOT NULL AND folio_id=$3) OR (demat_id IS NOT NULL AND demat_id=$4)
						OR (folio_number IS NOT NULL AND folio_number=$5) OR (demat_acc_number IS NOT NULL AND demat_acc_number=$6))
				ORDER BY created_at DESC LIMIT 1
			`, entityName, schemeID, folioID, dematID, nullIfEmptyString(folioNumber), nullIfEmptyString(dematAcc)).Scan(&snapshotNav, &totalInvestedSnapshot)
			if err == nil {
				if snapshotNav.Valid {
					currentNav = snapshotNav.Float64
				} else {
					// fallback to avgNav if snapshot nav is null
					currentNav = avgNav
				}
			} else {
				// fallback to avgNav if no snapshot found
				currentNav = avgNav
			}

			h := PortfolioHolding{
				ID:                  0,
				BatchID:             "",
				EntityName:          entityName,
				FolioNumber:         nil,
				DematAccNumber:      nil,
				DefaultRedemptionAccount:   "",
				DefaultSettlementAccount:   "",
				CreditBankAccount:          "",
				SchemeID:            schemeID,
				SchemeName:          schemeName,
				ISIN:                nil,
				TotalUnits:          units,
				BlockedUnits:        0,
				AvailableUnits:      units,
				AvgNAV:              avgNav,
				CurrentNAV:          currentNav,
				CurrentValue:        units * currentNav,
				TotalInvestedAmount: totalInvested,
				GainLoss:            (units * currentNav) - totalInvested,
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
			h.DefaultRedemptionAccount = resolveMasterBankAccountNumber(ctx, pgxPool, entityName, defaultRedAcct.String)
			h.DefaultSettlementAccount = resolveMasterBankAccountNumber(ctx, pgxPool, entityName, defaultSettlementAcct.String)
			if h.FolioNumber != nil && strings.TrimSpace(*h.FolioNumber) != "" {
				h.CreditBankAccount = h.DefaultRedemptionAccount
			} else if h.DematAccNumber != nil && strings.TrimSpace(*h.DematAccNumber) != "" {
				h.CreditBankAccount = h.DefaultSettlementAccount
			} else if h.DefaultRedemptionAccount != "" {
				h.CreditBankAccount = h.DefaultRedemptionAccount
			} else {
				h.CreditBankAccount = h.DefaultSettlementAccount
			}

			// Blocked units = sum of open (pending/approved) redemptions for this holding.
			// Derived from redemption_initiation + latest audit (not from onboard_transaction.blocked_units).
			var blockedUnits float64
			_ = pgxPool.QueryRow(ctx, `
				WITH latest AS (
					SELECT DISTINCT ON (redemption_id)
						redemption_id,
						processing_status,
						requested_at
					FROM investment.auditactionredemption
					ORDER BY redemption_id, requested_at DESC
				)
				SELECT COALESCE(SUM(COALESCE(ri.by_units,0)),0)
				FROM investment.redemption_initiation ri
				JOIN latest l ON l.redemption_id = ri.redemption_id
				WHERE COALESCE(ri.is_deleted,false)=false
					AND UPPER(COALESCE(l.processing_status,'')) IN ('PENDING_APPROVAL','APPROVED')
					AND LOWER(TRIM(COALESCE(ri.entity_name,''))) = LOWER(TRIM($1))
					AND ri.scheme_id = $2
					AND (
						($3::text IS NOT NULL AND (ri.folio_id = $3 OR ri.folio_id = $5))
						OR ($4::text IS NOT NULL AND (ri.demat_id = $4 OR ri.demat_id = $6))
					)
			`, entityName, schemeID,
				nullIfEmptyString(folioID.String), nullIfEmptyString(dematID.String),
				nullIfEmptyString(folioNumber), nullIfEmptyString(dematAcc)).Scan(&blockedUnits)
			if blockedUnits < 0 {
				blockedUnits = 0
			}
			h.BlockedUnits = blockedUnits
			h.AvailableUnits = h.TotalUnits - blockedUnits
			if h.AvailableUnits < 0 {
				h.AvailableUnits = 0
			}
			h.CurrentValue = h.TotalUnits * h.CurrentNAV
			h.GainLoss = (h.TotalUnits * h.CurrentNAV) - h.TotalInvestedAmount
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
							LEFT JOIN investment.portfolio_snapshot ps ON ot.batch_id = ps.batch_id
								LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
								WHERE LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription')
								AND (COALESCE(ot.entity_name,'') = $7 OR ps.entity_name = $7)
									AND ( (ot.folio_number = $1) OR (ot.demat_acc_number = $2) )
									AND ( ot.scheme_id = $3 OR ot.scheme_internal_code = $4 OR ms.isin = $5 OR ms.scheme_name = $6 )
								ORDER BY ot.transaction_date ASC, ot.id ASC
						`

			txRows, err := pgxPool.Query(ctx, txQuery, folioNumber, dematAcc, schemeID, schemeID, isin, schemeName, entityName)
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.EntityName) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrEntityNameRequired)
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
		// Fetch all BUY/PURCHASE transactions ordered by date (FIFO) with flexible scheme matching and entity filter
		txQuery := `
			SELECT 
				ot.id,
				TO_CHAR(ot.transaction_date, 'YYYY-MM-DD') AS transaction_date,
				COALESCE(ot.units, 0) AS units,
				COALESCE(ot.amount, 0) AS amount,
				COALESCE(ot.nav, 0) AS nav,
				ot.scheme_internal_code,
				ot.folio_number,
				ot.demat_acc_number
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.portfolio_snapshot ps ON ot.batch_id = ps.batch_id
			LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
			WHERE ps.entity_name = $1
				AND LOWER(ot.transaction_type) IN ('buy', 'purchase', 'subscription')
				AND (( $2::text IS NOT NULL AND ot.folio_number = $2) OR ($3::text IS NOT NULL AND ot.demat_acc_number = $3))
				AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $4 OR ms.isin = $4 OR ms.scheme_name = $4 )
			ORDER BY ot.transaction_date ASC, ot.id ASC
		`

		rows, err := pgxPool.Query(ctx, txQuery, req.EntityName, req.FolioNumber, req.DematAccNumber, req.SchemeID)
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
				TO_CHAR(ot.transaction_date, 'YYYY-MM-DD') AS transaction_date,
				COALESCE(ot.units, 0) AS units
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.portfolio_snapshot ps ON ot.batch_id = ps.batch_id
			LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
			WHERE ps.entity_name = $1
				AND LOWER(ot.transaction_type) IN ('sell', 'redemption')
				AND (( $2::text IS NOT NULL AND ot.folio_number = $2) OR ($3::text IS NOT NULL AND ot.demat_acc_number = $3))
				AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $4 OR ms.isin = $4 OR ms.scheme_name = $4 )
			ORDER BY ot.transaction_date ASC, ot.id ASC
		`

		sellRows, err := pgxPool.Query(ctx, sellQuery, req.EntityName, req.FolioNumber, req.DematAccNumber, req.SchemeID)
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

		// compute sum of sold units (treat sells as absolute units)
		var sumSellUnits float64
		for _, s := range existingSells {
			sumSellUnits += math.Abs(s.Units)
		}

		// total holding available = purchases - sells
		totalUnits := sumPurchaseUnits - sumSellUnits

		// determine average NAV from purchases (weighted)
		var avgNav float64
		if sumPurchaseUnits > 0 {
			avgNav = weightedNavNum / sumPurchaseUnits
		}

		// Get folio_id or demat_id from master tables for snapshot lookup
		var folioID, dematID sql.NullString
		if req.FolioNumber != nil && *req.FolioNumber != "" {
			_ = pgxPool.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND entity_name=$2 LIMIT 1`, *req.FolioNumber, req.EntityName).Scan(&folioID)
		}
		if req.DematAccNumber != nil && *req.DematAccNumber != "" {
			_ = pgxPool.QueryRow(ctx, `SELECT demat_id FROM investment.masterdemataccount WHERE demat_account_number=$1 AND entity_name=$2 LIMIT 1`, *req.DematAccNumber, req.EntityName).Scan(&dematID)
		}

		// Fetch current NAV from latest portfolio_snapshot with fallback to folio_number/demat_acc_number
		var currentNAV float64
		var schemeName string
		var tmpNav sql.NullFloat64
		var tmpInvest sql.NullFloat64

		folioNumberStr := ""
		if req.FolioNumber != nil {
			folioNumberStr = *req.FolioNumber
		}
		dematAccStr := ""
		if req.DematAccNumber != nil {
			dematAccStr = *req.DematAccNumber
		}

		if err := pgxPool.QueryRow(ctx, `
		SELECT COALESCE(current_nav,0), COALESCE(total_invested_amount,0) 
		FROM investment.portfolio_snapshot
		WHERE entity_name=$1 AND scheme_id=$2 
			AND ((folio_id IS NOT NULL AND folio_id=$3) OR (demat_id IS NOT NULL AND demat_id=$4)
				OR (folio_number IS NOT NULL AND folio_number=$5) OR (demat_acc_number IS NOT NULL AND demat_acc_number=$6))
		ORDER BY created_at DESC LIMIT 1
	`, req.EntityName, req.SchemeID, folioID, dematID, nullIfEmptyString(folioNumberStr), nullIfEmptyString(dematAccStr)).Scan(&tmpNav, &tmpInvest); err == nil {
			if tmpNav.Valid && tmpNav.Float64 > 0 {
				currentNAV = tmpNav.Float64
			} else {
				currentNAV = avgNav
			}
		} else {
			currentNAV = avgNav
		} // try to lookup scheme name from masterscheme
		if err := pgxPool.QueryRow(ctx, `SELECT COALESCE(scheme_name,'') FROM investment.masterscheme WHERE scheme_id=$1 OR internal_scheme_code=$1 OR isin=$1 LIMIT 1`, req.SchemeID).Scan(&schemeName); err != nil {
			schemeName = ""
		}

		// Calculate blocked units from pending/approved redemption initiations
		var blockedUnits float64
		blockedQuery := `
		SELECT COALESCE(SUM(ri.by_units), 0)
		FROM investment.redemption_initiation ri
		LEFT JOIN investment.auditactionredemption aar ON aar.redemption_id = ri.redemption_id
		WHERE ri.entity_name = $1
			AND ri.scheme_id = $2
			AND (ri.folio_id = $3 OR ri.folio_id = $4 OR ri.demat_id = $5 OR ri.demat_id = $6)
			AND ri.is_deleted = false
			AND aar.action_id = (
				SELECT action_id FROM investment.auditactionredemption 
				WHERE redemption_id = ri.redemption_id 
				ORDER BY requested_at DESC LIMIT 1
			)
			AND aar.processing_status IN ('PENDING_APPROVAL', 'APPROVED')
	`
		if err := pgxPool.QueryRow(ctx, blockedQuery, req.EntityName, req.SchemeID,
			folioID, nullIfEmptyString(folioNumberStr),
			dematID, nullIfEmptyString(dematAccStr)).Scan(&blockedUnits); err != nil {
			blockedUnits = 0
		}

		// Calculate available units
		availableUnits := totalUnits - blockedUnits
		if availableUnits < 0 {
			availableUnits = 0
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

		// CRITICAL: Check against available units (total - blocked)
		if redemptionUnits > availableUnits {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Redemption units (%.6f) exceed available units (%.6f). %.6f units are blocked in pending/approved redemptions.", redemptionUnits, availableUnits, blockedUnits))
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

func resolveMasterBankAccountNumber(ctx context.Context, pgxPool *pgxpool.Pool, entityName string, accountIDOrNumber string) string {
	needle := strings.TrimSpace(accountIDOrNumber)
	if needle == "" {
		return ""
	}

	// 1) Preferred: entity-scoped match (prevents cross-entity collisions on account_number)
	var accountNumber string
	if err := pgxPool.QueryRow(ctx, `
		SELECT ba.account_number
		FROM public.masterbankaccount ba
		LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
		LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
		WHERE COALESCE(ba.is_deleted,false)=false
			AND LOWER(TRIM(COALESCE(e.entity_name, ec.entity_name))) = LOWER(TRIM($2))
			AND (ba.account_number = $1 OR ba.account_id = $1)
		LIMIT 1
	`, needle, entityName).Scan(&accountNumber); err == nil {
		return strings.TrimSpace(accountNumber)
	}

	// 2) Safe fallback: if the stored value is an account_id, it's unique globally
	if err := pgxPool.QueryRow(ctx, `
		SELECT ba.account_number
		FROM public.masterbankaccount ba
		WHERE COALESCE(ba.is_deleted,false)=false
			AND ba.account_id = $1
		LIMIT 1
	`, needle).Scan(&accountNumber); err == nil {
		return strings.TrimSpace(accountNumber)
	}

	// 3) Last resort: if it's already an account number-like value, return it
	// (helps when masterbankaccount is missing but the stored value is actually a number)
	isDigits := true
	for _, r := range needle {
		if r < '0' || r > '9' {
			isDigits = false
			break
		}
	}
	if isDigits {
		return needle
	}

	return ""
}
