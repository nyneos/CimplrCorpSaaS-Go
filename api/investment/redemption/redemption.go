package redemption

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/validation"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/portfolio"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types
// ---------------------------

// GetPortfolioRequest models the payload to fetch portfolio with transactions
type GetPortfolioRequest struct {
	EntityName   string `json:"entity_name"`
	RedemptionID string `json:"redemption_id,omitempty"`
	UserID       string `json:"user_id,omitempty"`
}

// PortfolioHolding represents a single portfolio position
type PortfolioHolding struct {
	ID                                int64               `json:"id,omitempty"`
	BatchID                           string              `json:"batch_id,omitempty"`
	EntityName                        string              `json:"entity_name"`
	FolioNumber                       *string             `json:"folio_number,omitempty"`
	DematAccNumber                    *string             `json:"demat_acc_number,omitempty"`
	DefaultRedemptionAccount          string              `json:"default_redemption_account"`
	DefaultSettlementAccount          string              `json:"default_settlement_account"`
	CreditBankAccount                 string              `json:"credit_bank_account"`
	CreditBankName                    string              `json:"credit_bank_name"`
	SchemeID                          string              `json:"scheme_id"`
	SchemeName                        string              `json:"scheme_name"`
	ISIN                              *string             `json:"isin,omitempty"`
	Method                            string              `json:"method"`
	TotalUnits                        float64             `json:"total_units"`
	BlockedUnits                      float64             `json:"blocked_units"`
	BlockedUnitsExcludingRedemption   float64             `json:"blocked_units_excluding_redemption,omitempty"`
	AvailableUnits                    float64             `json:"available_units"`
	AvailableUnitsExcludingRedemption float64             `json:"available_units_excluding_redemption,omitempty"`
	AvgNAV                            float64             `json:"avg_nav"`
	CurrentNAV                        float64             `json:"current_nav"`
	CurrentValue                      float64             `json:"current_value"`
	TotalInvestedAmount               float64             `json:"total_invested_amount"`
	GainLoss                          float64             `json:"gain_loss"`
	GainLossPercent                   float64             `json:"gain_loss_percent"`
	CreatedAt                         string              `json:"created_at,omitempty"`
	Transactions                      []TransactionDetail `json:"transactions"`
}

// TransactionDetail represents a single transaction (BUY lot)
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
	// Lot-level unit breakdown
	TotalUnits     float64 `json:"total_units"`     // Original units from this BUY lot
	BlockedUnits   float64 `json:"blocked_units"`   // Units blocked by pending redemptions
	AvailableUnits float64 `json:"available_units"` // Units available for redemption
}

// CalculateRedemptionRequest models the payload for FIFO/LIFO redemption calculation
type CalculateRedemptionRequest struct {
	EntityName     string  `json:"entity_name"`
	SchemeID       string  `json:"scheme_id"`
	FolioNumber    *string `json:"folio_number,omitempty"`
	DematAccNumber *string `json:"demat_acc_number,omitempty"`
	Method         string  `json:"method,omitempty"` // FIFO (default) or LIFO
	// Only one of the following should be provided
	Units   *float64 `json:"units,omitempty"`
	Amount  *float64 `json:"amount,omitempty"`
	Percent *float64 `json:"percent,omitempty"`
}

// AllocationDetail represents a single FIFO/LIFO allocation result
type AllocationDetail struct {
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

// RedemptionCalculation represents the complete FIFO/LIFO calculation result
type RedemptionCalculation struct {
	EntityName              string             `json:"entity_name"`
	SchemeID                string             `json:"scheme_id"`
	SchemeName              string             `json:"scheme_name"`
	FolioNumber             *string            `json:"folio_number,omitempty"`
	DematAccNumber          *string            `json:"demat_acc_number,omitempty"`
	Method                  string             `json:"method"`
	TotalHolding            float64            `json:"total_holding"`
	CurrentNAV              float64            `json:"current_nav"`
	RedemptionUnits         float64            `json:"redemption_units"`
	RedemptionAmount        float64            `json:"redemption_amount"`
	RedemptionNAV           float64            `json:"redemption_nav"`
	RealizedGainLoss        float64            `json:"realized_gain_loss"`
	RealizedGainLossPercent float64            `json:"realized_gain_loss_percent"`
	RemainingUnits          float64            `json:"remaining_units"`
	RemainingValue          float64            `json:"remaining_value"`
	Allocations             []AllocationDetail `json:"allocations"`
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

		req.EntityName = strings.TrimSpace(req.EntityName)
		if req.EntityName == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_name is required")
			return
		}
		if errMsg := validation.ValidateMFMasterReferences(r.Context(), map[string]interface{}{"entity_name": req.EntityName}); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		ctx := r.Context()
		excludeRedemptionID := strings.TrimSpace(req.RedemptionID)

		portfolioRows, err := portfolio.QueryEntityHoldings(ctx, pgxPool, req.EntityName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}

		// Pre-fetch all active bank accounts for this entity so we can fall back to them
		// when masterfolio.default_redemption_account is not set.
		var entityBankAccounts []string
		{
			acctRows, acctErr := pgxPool.Query(ctx, `
				SELECT ba.account_number
				FROM public.masterbankaccount ba
				LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
				LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
				WHERE COALESCE(ba.is_deleted,false)=false
				  AND COALESCE(ba.status,'') = 'Active'
				  AND LOWER(TRIM(COALESCE(e.entity_name, ec.entity_name))) = LOWER(TRIM($1))
				ORDER BY ba.account_id
			`, req.EntityName)
			if acctErr == nil {
				for acctRows.Next() {
					var acct string
					if err := acctRows.Scan(&acct); err == nil && strings.TrimSpace(acct) != "" {
						entityBankAccounts = append(entityBankAccounts, strings.TrimSpace(acct))
					}
				}
				acctRows.Close()
			}
		}

		holdings := []PortfolioHolding{}
		for _, pr := range portfolioRows {
			entityName := pr.EntityName
			folioNumber := pr.FolioNumber
			dematAcc := pr.DematAccountNumber
			schemeID := pr.SchemeID
			schemeName := pr.SchemeName
			isin := pr.ISIN
			units := pr.TotalUnits
			avgNav := pr.AvgNav
			totalInvested := pr.TotalInvestedAmount

			var method string
			_ = pgxPool.QueryRow(ctx, `
				SELECT COALESCE(method, 'FIFO')
				FROM investment.masterscheme
				WHERE scheme_id::text = $1 AND COALESCE(is_deleted, false) = false
				LIMIT 1
			`, schemeID).Scan(&method)
			if strings.TrimSpace(method) == "" {
				method = "FIFO"
			}

			// Ensure units are not negative
			if units < 0 {
				units = 0
			}

			var folioID, dematID sql.NullString
			if folioNumber != "" {
				_ = pgxPool.QueryRow(ctx, `SELECT folio_id FROM investment.masterfolio WHERE folio_number=$1 AND entity_name=$2 LIMIT 1`, folioNumber, entityName).Scan(&folioID)
			}
			if dematAcc != "" {
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

			currentNav := pr.CurrentNav
			if currentNav <= 0 {
				currentNav = avgNav
			}

			h := PortfolioHolding{
				ID:                       0,
				BatchID:                  "",
				EntityName:               entityName,
				FolioNumber:              nil,
				DematAccNumber:           nil,
				DefaultRedemptionAccount: "",
				DefaultSettlementAccount: "",
				CreditBankAccount:        "",
				CreditBankName:           "",
				SchemeID:                 schemeID,
				SchemeName:               schemeName,
				ISIN:                     nil,
				Method:                   method,
				TotalUnits:               units,
				BlockedUnits:             0,
				AvailableUnits:           units,
				AvgNAV:                   avgNav,
				CurrentNAV:               currentNav,
				CurrentValue:             units * currentNav,
				TotalInvestedAmount:      totalInvested,
				GainLoss:                 (units * currentNav) - totalInvested,
				GainLossPercent:          0,
				CreatedAt:                "",
				Transactions:             []TransactionDetail{},
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
			// Fallback: if masterfolio/demat master has no default account, use the first
			// active bank account for this entity so the UI dropdown is never empty.
			if h.CreditBankAccount == "" && len(entityBankAccounts) > 0 {
				h.CreditBankAccount = entityBankAccounts[0]
			}
			// Resolve bank name from credit bank account number
			if h.CreditBankAccount != "" {
				h.CreditBankName = resolveBankName(ctx, pgxPool, entityName, h.CreditBankAccount)
			} // Blocked units = sum of open (pending/approved) redemption initiations that do NOT yet have a CONFIRMED confirmation.
			// Once a confirmation is CONFIRMED, the SELL transaction is created and units are no longer blocked.
			// We return both totals:
			// - blocked_units: includes ALL open redemptions (true freeze)
			// - blocked_units_excluding_redemption: excludes req.RedemptionID if provided (useful for update/preview)
			var blockedUnitsTotal float64
			var blockedUnitsExcluding float64
			_ = pgxPool.QueryRow(ctx, `
				WITH latest_initiation_audit AS (
					SELECT DISTINCT ON (redemption_id)
						redemption_id,
						processing_status,
						requested_at
					FROM investment.auditactionredemption
					ORDER BY redemption_id, requested_at DESC
				),
				confirmed_redemptions AS (
					-- Redemptions that have a CONFIRMED confirmation (SELL already created)
					SELECT DISTINCT rc.redemption_id
					FROM investment.redemption_confirmation rc
					WHERE UPPER(COALESCE(rc.status,'')) = 'CONFIRMED'
				)
				SELECT
					COALESCE(SUM(COALESCE(ri.by_units,0)),0) AS blocked_total,
					COALESCE(SUM(CASE WHEN ($7::text IS NULL OR ri.redemption_id <> $7) THEN COALESCE(ri.by_units,0) ELSE 0 END),0) AS blocked_excluding
				FROM investment.redemption_initiation ri
				JOIN latest_initiation_audit l ON l.redemption_id = ri.redemption_id
				WHERE COALESCE(ri.is_deleted,false)=false
					AND UPPER(COALESCE(l.processing_status,'')) IN ('PENDING_APPROVAL','APPROVED')
					AND ri.redemption_id NOT IN (SELECT redemption_id FROM confirmed_redemptions)
					AND LOWER(TRIM(COALESCE(ri.entity_name,''))) = LOWER(TRIM($1))
					AND ri.scheme_id = $2
					AND (
						($3::text IS NOT NULL AND (ri.folio_id = $3 OR ri.folio_id = $5))
						OR ($4::text IS NOT NULL AND (ri.demat_id = $4 OR ri.demat_id = $6))
					)
			`, entityName, schemeID,
				nullIfEmptyString(folioID.String), nullIfEmptyString(dematID.String),
				nullIfEmptyString(folioNumber), nullIfEmptyString(dematAcc), nullIfEmptyString(excludeRedemptionID)).Scan(&blockedUnitsTotal, &blockedUnitsExcluding)
			if blockedUnitsTotal < 0 {
				blockedUnitsTotal = 0
			}
			if blockedUnitsExcluding < 0 {
				blockedUnitsExcluding = 0
			}
			h.BlockedUnits = blockedUnitsTotal
			h.AvailableUnits = h.TotalUnits - blockedUnitsTotal
			if h.AvailableUnits < 0 {
				h.AvailableUnits = 0
			}
			if excludeRedemptionID != "" {
				h.BlockedUnitsExcludingRedemption = blockedUnitsExcluding
				h.AvailableUnitsExcludingRedemption = h.TotalUnits - blockedUnitsExcluding
				if h.AvailableUnitsExcludingRedemption < 0 {
					h.AvailableUnitsExcludingRedemption = 0
				}
			}
			h.CurrentValue = h.TotalUnits * h.CurrentNAV
			h.GainLoss = (h.TotalUnits * h.CurrentNAV) - h.TotalInvestedAmount
			if totalInvested != 0 {
				h.GainLossPercent = (h.GainLoss / totalInvested) * 100.0
			}

			// Fetch individual BUY transactions for this holding (ordered FIFO) with lot-level blocked/available units
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
				TO_CHAR(ot.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
				COALESCE(ot.blocked_units, 0) AS blocked_units
			FROM investment.onboard_transaction ot
			WHERE LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription')
				AND COALESCE(ot.entity_name,'') = $5
				AND ( (ot.folio_number = $1) OR (ot.demat_acc_number = $2) )
				AND ( ot.scheme_id = $3 OR ot.scheme_internal_code = $4 )
			GROUP BY ot.id, ot.batch_id, ot.transaction_date, ot.transaction_type, 
				ot.scheme_internal_code, ot.folio_number, ot.demat_acc_number, 
				ot.amount, ot.units, ot.nav, ot.scheme_id, ot.folio_id, 
				ot.demat_id, ot.created_at, ot.blocked_units
			ORDER BY ot.transaction_date ASC, ot.id ASC
		`

			txRows, err := pgxPool.Query(ctx, txQuery, folioNumber, dematAcc, schemeID, schemeID, entityName)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Transaction query failed: "+err.Error())
				return
			}

			// First, collect all BUY lots
			buyLots := []TransactionDetail{}
			for txRows.Next() {
				var tx TransactionDetail
				var blockedUnits float64
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
					&blockedUnits,
				); err != nil {
					txRows.Close()
					api.RespondWithError(w, http.StatusInternalServerError, "Transaction scan failed: "+err.Error())
					return
				}
				tx.BlockedUnits = blockedUnits
				buyLots = append(buyLots, tx)
			}
			txRows.Close()

			// Now fetch total SELL units for this holding (same entity/scheme/folio or demat)
			var totalSellUnits float64
			_ = pgxPool.QueryRow(ctx, `
				SELECT COALESCE(SUM(ABS(COALESCE(ot.units,0))),0)
				FROM investment.onboard_transaction ot
				LEFT JOIN LATERAL (
					SELECT isin, scheme_name 
					FROM investment.masterscheme 
					WHERE scheme_id = ot.scheme_id OR internal_scheme_code = ot.scheme_internal_code OR isin = ot.scheme_id
					LIMIT 1
				) ms ON true
				WHERE LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption')
					AND COALESCE(ot.entity_name,'') = $1
					AND ( (ot.folio_number = $2) OR (ot.demat_acc_number = $3) )
					AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $5 OR ms.scheme_id::text = $4 OR ms.internal_scheme_code = $5 OR ms.amfi_scheme_code = $6 )
			`, entityName, folioNumber, dematAcc, schemeID, schemeID, isin, schemeName).Scan(&totalSellUnits)

			// Apply FIFO: deduct sell units from buy lots in order
			remainingSellUnits := totalSellUnits
			transactions := []TransactionDetail{}
			for _, tx := range buyLots {
				originalUnits := tx.Units
				// Deduct SELL units from this lot (FIFO)
				if remainingSellUnits > 0 {
					if remainingSellUnits >= originalUnits {
						// This entire lot is consumed by sells
						remainingSellUnits -= originalUnits
						tx.TotalUnits = 0
					} else {
						// Partial consumption
						tx.TotalUnits = originalUnits - remainingSellUnits
						remainingSellUnits = 0
					}
				} else {
					tx.TotalUnits = originalUnits
				}
				// Available = remaining after sells - blocked by pending redemptions
				tx.AvailableUnits = tx.TotalUnits - tx.BlockedUnits
				if tx.AvailableUnits < 0 {
					tx.AvailableUnits = 0
				}
				// Only include lots that still have units remaining
				if tx.TotalUnits > 0 {
					transactions = append(transactions, tx)
				}
			}

			h.Transactions = transactions
			holdings = append(holdings, h)
		}

		if entityBankAccounts == nil {
			entityBankAccounts = []string{}
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"entity_name":    req.EntityName,
			"holdings":       holdings,
			"total_holdings": len(holdings),
			"bank_accounts":  entityBankAccounts,
		})
	}
}

// ---------------------------
// CalculateRedemptionFIFO
// Calculates FIFO/LIFO-based redemption allocations
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
		if errMsg := validation.ValidateMFMasterReferences(r.Context(), map[string]interface{}{
			"entity_name":      req.EntityName,
			"scheme_id":        req.SchemeID,
			"folio_number":     req.FolioNumber,
			"demat_acc_number": req.DematAccNumber,
		}); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		// Default method to FIFO if not provided
		method := strings.ToUpper(strings.TrimSpace(req.Method))
		if method == "" {
			method = "FIFO"
		}
		if method != "FIFO" && method != "LIFO" && method != "WEIGHTED_AVERAGE" {
			api.RespondWithError(w, http.StatusBadRequest, "method must be FIFO, LIFO, or WEIGHTED_AVERAGE")
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

		ctx := r.Context()

		// We'll compute holdings from transaction rows so totals are aggregated across batches.
		// Fetch all BUY/PURCHASE transactions ordered by date based on method (FIFO/LIFO)
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
			WHERE COALESCE(ot.entity_name, '') = $1
				AND LOWER(ot.transaction_type) IN ('buy', 'purchase', 'subscription')
				AND (( $2::text IS NOT NULL AND ot.folio_number = $2) OR ($3::text IS NOT NULL AND ot.demat_acc_number = $3))
				AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $4 )
			ORDER BY
				CASE WHEN $5 = 'FIFO' THEN ot.transaction_date END ASC,
				CASE WHEN $5 = 'LIFO' THEN ot.transaction_date END DESC,
				ot.id ASC
		`

		rows, err := pgxPool.Query(ctx, txQuery, req.EntityName, req.FolioNumber, req.DematAccNumber, req.SchemeID, method)
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
			WHERE COALESCE(ot.entity_name, '') = $1
				AND LOWER(ot.transaction_type) IN ('sell', 'redemption')
				AND (( $2::text IS NOT NULL AND ot.folio_number = $2) OR ($3::text IS NOT NULL AND ot.demat_acc_number = $3))
				AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $4 )
			ORDER BY
				CASE WHEN $5 IN ('FIFO', 'WEIGHTED_AVERAGE') THEN ot.transaction_date END ASC,
				CASE WHEN $5 = 'LIFO' THEN ot.transaction_date END DESC,
				ot.id ASC
		`

		sellRows, err := pgxPool.Query(ctx, sellQuery, req.EntityName, req.FolioNumber, req.DematAccNumber, req.SchemeID, method)
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

		// Apply existing redemptions to purchases (using method order - already sorted)
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

		// Now apply the new redemption using the specified method (FIFO/LIFO/WEIGHTED_AVERAGE)
		allocations := []AllocationDetail{}
		remainingToAllocate := redemptionUnits
		totalAllocatedAmount := 0.0
		totalRealizedGainLoss := 0.0

		if method == "WEIGHTED_AVERAGE" {
			// Weighted Average: Calculate average cost across all remaining lots
			var totalRemainingUnits float64
			var totalRemainingCost float64
			for _, purchase := range purchasesCopy {
				if purchase.Units > 0 {
					totalRemainingUnits += purchase.Units
					totalRemainingCost += purchase.Units * purchase.NAV
				}
			}

			var weightedAvgNAV float64
			if totalRemainingUnits > 0 {
				weightedAvgNAV = totalRemainingCost / totalRemainingUnits
			}

			// For weighted average, allocate proportionally from all lots
			for _, purchase := range purchasesCopy {
				if remainingToAllocate <= 0 {
					break
				}
				if purchase.Units <= 0 {
					continue
				}

				// Allocate proportionally based on lot's percentage of total remaining
				var allocatedUnits float64
				if totalRemainingUnits > 0 {
					lotProportion := purchase.Units / totalRemainingUnits
					allocatedUnits = remainingToAllocate * lotProportion
					// Ensure we don't over-allocate from this lot
					if allocatedUnits > purchase.Units {
						allocatedUnits = purchase.Units
					}
				} else {
					break
				}

				// Calculate using weighted average NAV for cost basis
				allocatedPurchaseAmount := allocatedUnits * weightedAvgNAV
				realizedAmount := allocatedUnits * currentNAV
				gainLoss := realizedAmount - allocatedPurchaseAmount
				gainLossPercent := 0.0
				if allocatedPurchaseAmount > 0 {
					gainLossPercent = (gainLoss / allocatedPurchaseAmount) * 100.0
				}

				allocation := AllocationDetail{
					TransactionID:      purchase.ID,
					TransactionDate:    purchase.TransactionDate,
					OriginalUnits:      purchase.Units + allocatedUnits,
					OriginalAmount:     (purchase.Units + allocatedUnits) * purchase.NAV,
					OriginalNAV:        weightedAvgNAV, // Use weighted average NAV
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
		} else {
			// FIFO/LIFO: Allocate sequentially from lots
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

				allocation := AllocationDetail{
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
		} // Calculate redemption metrics
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
			Method:                  method,
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

func resolveBankName(ctx context.Context, pgxPool *pgxpool.Pool, entityName string, accountNumber string) string {
	needle := strings.TrimSpace(accountNumber)
	if needle == "" {
		return ""
	}

	// Try to get bank name from masterbankaccount using entity-scoped lookup
	var bankName string
	if err := pgxPool.QueryRow(ctx, `
		SELECT COALESCE(ba.bank_name, '')
		FROM public.masterbankaccount ba
		LEFT JOIN public.masterentity e ON e.entity_id::text = ba.entity_id
		LEFT JOIN public.masterentitycash ec ON ec.entity_id::text = ba.entity_id
		WHERE COALESCE(ba.is_deleted,false)=false
			AND LOWER(TRIM(COALESCE(e.entity_name, ec.entity_name))) = LOWER(TRIM($2))
			AND ba.account_number = $1
		LIMIT 1
	`, needle, entityName).Scan(&bankName); err == nil {
		return strings.TrimSpace(bankName)
	}

	// Fallback: global lookup by account_number
	if err := pgxPool.QueryRow(ctx, `
		SELECT COALESCE(ba.bank_name, '')
		FROM public.masterbankaccount ba
		WHERE COALESCE(ba.is_deleted,false)=false
			AND ba.account_number = $1
		LIMIT 1
	`, needle).Scan(&bankName); err == nil {
		return strings.TrimSpace(bankName)
	}

	return ""
}
