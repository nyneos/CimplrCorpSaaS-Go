package bankbalances

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// KPIResponse is the main response structure for treasury dashboard KPIs
type KPIResponse struct {
	Overall    OverallKPI    `json:"overall"`
	EntityWise []EntityKPI   `json:"entitywise"`
	BankWise   []BankKPI     `json:"bankwise"`
	MoM        MoMComparison `json:"mom_comparison"`
}

// OverallKPI contains consolidated balance and yield metrics
type OverallKPI struct {
	TotalBalance         float64 `json:"total_balance"`
	WeightedYieldPercent float64 `json:"weighted_yield_percent"`
	ActiveAccounts       int     `json:"active_accounts"`
}

// EntityKPI contains entity-level balance breakdown
type EntityKPI struct {
	EntityID      string  `json:"entity_id"`
	EntityName    string  `json:"entity_name"`
	Vertical      string  `json:"vertical"`
	Balance       float64 `json:"balance"`
	WeightPercent float64 `json:"weight_percent"`
	AccountCount  int     `json:"account_count"`
}

// BankKPI contains bank-level balance and yield breakdown
type BankKPI struct {
	BankName      string  `json:"bank_name"`
	BankID        string  `json:"bank_id"`
	Balance       float64 `json:"balance"`
	YieldPercent  float64 `json:"yield_percent"`
	WeightPercent float64 `json:"weight_percent"`
	AccountCount  int     `json:"account_count"`
}

// MoMComparison contains month-on-month comparison metrics
type MoMComparison struct {
	CurrentMonth  MonthData `json:"current_month"`
	PreviousMonth MonthData `json:"previous_month"`
	ChangeAmount  float64   `json:"change_amount"`
	ChangePercent float64   `json:"change_percent"`
}

// MonthData contains balance data for a specific month
type MonthData struct {
	Month        string  `json:"month"`
	TotalBalance float64 `json:"total_balance"`
}

// GetTreasuryKPI returns comprehensive KPI dashboard data
func GetTreasuryKPI(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID    string   `json:"user_id"`
			EntityIDs []string `json:"entity_ids,omitempty"`
			BankNames []string `json:"bank_names,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, "Missing user_id")
			return
		}

		// Validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// Get middleware context filters (default) but allow request overrides
		entityIDs := api.GetEntityIDsFromCtx(ctx) // Use entity IDs by default
		bankNames := api.GetBankNamesFromCtx(ctx)
		// If request provided explicit filters, prefer them
		if len(req.EntityIDs) > 0 {
			entityIDs = req.EntityIDs
		}
		if len(req.BankNames) > 0 {
			bankNames = req.BankNames
		}

		fmt.Printf("\n[KPI DEBUG] ========================================\n")
		fmt.Printf("[KPI DEBUG] User ID: %s\n", req.UserID)
		fmt.Printf("[KPI DEBUG] Entity IDs from context: %v (count: %d)\n", entityIDs, len(entityIDs))
		fmt.Printf("[KPI DEBUG] Bank Names from context: %v (count: %d)\n", bankNames, len(bankNames))

		// Build entity filter for SQL (using entity_id from masterbankaccount)
		entityFilter := ""
		bankFilter := ""
		if len(entityIDs) > 0 {
			normEntities := make([]string, 0, len(entityIDs))
			for _, n := range entityIDs {
				if s := strings.TrimSpace(n); s != "" {
					normEntities = append(normEntities, "'"+s+"'")
				}
			}
			if len(normEntities) > 0 {
				entityFilter = " AND mba.entity_id IN (" + strings.Join(normEntities, ",") + ")"
			}
		}

		if len(bankNames) > 0 {
			normBanks := make([]string, 0, len(bankNames))
			for _, n := range bankNames {
				if s := strings.TrimSpace(n); s != "" {
					normBanks = append(normBanks, "'"+strings.ToLower(s)+"'")
				}
			}
			if len(normBanks) > 0 {
				bankFilter = " AND LOWER(TRIM(mba.bank_name)) IN (" + strings.Join(normBanks, ",") + ")"
			}
		}

		fmt.Printf("[KPI DEBUG] Entity Filter SQL: %s\n", entityFilter)
		fmt.Printf("[KPI DEBUG] Bank Filter SQL: %s\n", bankFilter)
		fmt.Printf("[KPI DEBUG] ========================================\n\n")

		// Execute KPI queries
		fmt.Printf("[KPI DEBUG] Executing getOverallKPI...\n")
		overall, err := getOverallKPI(ctx, pgxPool, entityFilter, bankFilter)
		if err != nil {
			fmt.Printf("[KPI ERROR] getOverallKPI failed: %v\n", err)
			api.RespondWithResult(w, false, "Failed to fetch overall KPI: "+err.Error())
			return
		}
		fmt.Printf("[KPI DEBUG] Overall KPI: TotalBalance=%.2f, ActiveAccounts=%d\n", overall.TotalBalance, overall.ActiveAccounts)

		fmt.Printf("[KPI DEBUG] Executing getEntityWiseKPI...\n")
		entityWise, err := getEntityWiseKPI(ctx, pgxPool, overall.TotalBalance, entityFilter, bankFilter)
		if err != nil {
			fmt.Printf("[KPI ERROR] getEntityWiseKPI failed: %v\n", err)
			api.RespondWithResult(w, false, "Failed to fetch entity-wise KPI: "+err.Error())
			return
		}
		fmt.Printf("[KPI DEBUG] Entity-wise KPI: %d entities found\n", len(entityWise))

		fmt.Printf("[KPI DEBUG] Executing getBankWiseKPI...\n")
		bankWise, err := getBankWiseKPI(ctx, pgxPool, overall.TotalBalance, entityFilter, bankFilter)
		if err != nil {
			fmt.Printf("[KPI ERROR] getBankWiseKPI failed: %v\n", err)
			api.RespondWithResult(w, false, "Failed to fetch bank-wise KPI: "+err.Error())
			return
		}
		fmt.Printf("[KPI DEBUG] Bank-wise KPI: %d banks found\n", len(bankWise))

		fmt.Printf("[KPI DEBUG] Executing getMoMComparison...\n")
		mom, err := getMoMComparison(ctx, pgxPool, entityFilter, bankFilter)
		if err != nil {
			fmt.Printf("[KPI ERROR] getMoMComparison failed: %v\n", err)
			api.RespondWithResult(w, false, "Failed to fetch MoM comparison: "+err.Error())
			return
		}
		fmt.Printf("[KPI DEBUG] MoM: Current=%.2f, Previous=%.2f, Change=%.2f%%\n",
			mom.CurrentMonth.TotalBalance, mom.PreviousMonth.TotalBalance, mom.ChangePercent)

		response := KPIResponse{
			Overall:    overall,
			EntityWise: entityWise,
			BankWise:   bankWise,
			MoM:        mom,
		}

		fmt.Printf("[KPI DEBUG] ✅ SUCCESS - Sending response\n\n")
		api.RespondWithPayload(w, true, "", response)
	}
}

// getOverallKPI calculates consolidated balance and weighted yield
func getOverallKPI(ctx context.Context, db *pgxpool.Pool, entityFilter, bankFilter string) (OverallKPI, error) {
	query := `
		WITH latest_approved_balance AS (
			SELECT DISTINCT ON (bbm.account_no)
				bbm.account_no,
				COALESCE(bbm.closing_balance, 0) AS balance_amount
			FROM public.bank_balances_manual bbm
			JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
			WHERE a.processing_status = 'APPROVED'
			ORDER BY bbm.account_no, bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
		),
		enriched_balance AS (
			SELECT
				mba.account_id,
				mba.bank_name,
				mba.bank_id,
				mba.entity_id,
				lab.balance_amount
			FROM latest_approved_balance lab
			JOIN public.masterbankaccount mba ON mba.account_number = lab.account_no
			WHERE mba.is_deleted = false
				AND COALESCE(mba.status, 'Active') = 'Active'
				` + entityFilter + bankFilter + `
		)
		SELECT
			COALESCE(SUM(eb.balance_amount), 0) AS total_balance,
			COUNT(DISTINCT eb.account_id) AS active_accounts
		FROM enriched_balance eb
	`

	fmt.Printf("[SQL DEBUG - Overall KPI] Query:\n%s\n", query)

	var kpi OverallKPI
	err := db.QueryRow(ctx, query).Scan(
		&kpi.TotalBalance,
		&kpi.ActiveAccounts,
	)

	if err != nil {
		fmt.Printf("[SQL ERROR - Overall KPI] Query execution failed: %v\n", err)
	} else {
		fmt.Printf("[SQL DEBUG - Overall KPI] Result: Balance=%.2f, Accounts=%d\n", kpi.TotalBalance, kpi.ActiveAccounts)
	}

	// TODO: Calculate weighted yield when yield data is available
	// For now, set to 0 as placeholder
	kpi.WeightedYieldPercent = 0.0

	return kpi, err
}

// getEntityWiseKPI calculates entity-level balance breakdown
func getEntityWiseKPI(ctx context.Context, db *pgxpool.Pool, totalBalance float64, entityFilter, bankFilter string) ([]EntityKPI, error) {
	query := `
		WITH latest_approved_balance AS (
			SELECT DISTINCT ON (bbm.account_no)
				bbm.account_no,
				COALESCE(bbm.closing_balance, 0) AS balance_amount
			FROM public.bank_balances_manual bbm
			JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
			WHERE a.processing_status = 'APPROVED'
			ORDER BY bbm.account_no, bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
		),
		enriched_balance AS (
			SELECT
				mba.account_id,
				mba.entity_id,
				lab.balance_amount
			FROM latest_approved_balance lab
			JOIN public.masterbankaccount mba ON mba.account_number = lab.account_no
			WHERE mba.is_deleted = false
				AND COALESCE(mba.status, 'Active') = 'Active'
				` + entityFilter + bankFilter + `
		)
		SELECT
			eb.entity_id,
			COALESCE(e.entity_name, eb.entity_id) AS entity_name,
			COALESCE(e.entity_level::text, 'Unknown') AS vertical,
			COALESCE(SUM(eb.balance_amount), 0) AS balance,
			COUNT(DISTINCT eb.account_id) AS account_count
		FROM enriched_balance eb
		LEFT JOIN public.masterentitycash e ON e.entity_id::text = eb.entity_id
		GROUP BY eb.entity_id, e.entity_name, e.entity_level
		ORDER BY balance DESC
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entities []EntityKPI
	for rows.Next() {
		var entity EntityKPI
		err := rows.Scan(
			&entity.EntityID,
			&entity.EntityName,
			&entity.Vertical,
			&entity.Balance,
			&entity.AccountCount,
		)
		if err != nil {
			continue
		}

		// Calculate weight percentage
		if totalBalance > 0 {
			entity.WeightPercent = (entity.Balance / totalBalance) * 100
		}

		entities = append(entities, entity)
	}

	return entities, rows.Err()
}

// getBankWiseKPI calculates bank-level balance and yield breakdown
func getBankWiseKPI(ctx context.Context, db *pgxpool.Pool, totalBalance float64, entityFilter, bankFilter string) ([]BankKPI, error) {
	query := `
		WITH latest_approved_balance AS (
			SELECT DISTINCT ON (bbm.account_no)
				bbm.account_no,
				COALESCE(bbm.closing_balance, 0) AS balance_amount
			FROM public.bank_balances_manual bbm
			JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
			WHERE a.processing_status = 'APPROVED'
			ORDER BY bbm.account_no, bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
		),
		enriched_balance AS (
			SELECT
				mba.account_id,
				mba.bank_name,
				COALESCE(mba.bank_id, '') AS bank_id,
				lab.balance_amount
			FROM latest_approved_balance lab
			JOIN public.masterbankaccount mba ON mba.account_number = lab.account_no
			WHERE mba.is_deleted = false
				AND COALESCE(mba.status, 'Active') = 'Active'
				` + entityFilter + bankFilter + `
		)
		SELECT
			eb.bank_name,
			COALESCE(eb.bank_id, '') AS bank_id,
			COALESCE(SUM(eb.balance_amount), 0) AS balance,
			COUNT(DISTINCT eb.account_id) AS account_count
		FROM enriched_balance eb
		GROUP BY eb.bank_name, eb.bank_id
		ORDER BY balance DESC
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var banks []BankKPI
	for rows.Next() {
		var bank BankKPI
		err := rows.Scan(
			&bank.BankName,
			&bank.BankID,
			&bank.Balance,
			&bank.AccountCount,
		)
		if err != nil {
			continue
		}

		// Calculate weight percentage
		if totalBalance > 0 {
			bank.WeightPercent = (bank.Balance / totalBalance) * 100
		}

		// TODO: Calculate yield when data is available
		// For now, set to 0 as placeholder
		bank.YieldPercent = 0.0

		banks = append(banks, bank)
	}

	return banks, rows.Err()
}

// getMoMComparison calculates month-on-month balance comparison
func getMoMComparison(ctx context.Context, db *pgxpool.Pool, entityFilter, bankFilter string) (MoMComparison, error) {
	query := `
		WITH latest_balances AS (
			SELECT DISTINCT ON (bbm.account_no, DATE_TRUNC('month', bbm.as_of_date))
				bbm.account_no,
				COALESCE(bbm.closing_balance, 0) AS balance_amount,
				DATE_TRUNC('month', bbm.as_of_date) AS month_start
			FROM public.bank_balances_manual bbm
			JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
			WHERE a.processing_status = 'APPROVED'
				AND bbm.as_of_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
			ORDER BY bbm.account_no, DATE_TRUNC('month', bbm.as_of_date), bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
		),
		enriched_balance AS (
			SELECT
				lab.balance_amount,
				lab.month_start
			FROM latest_balances lab
			JOIN public.masterbankaccount mba ON mba.account_number = lab.account_no
			WHERE mba.is_deleted = false
				AND COALESCE(mba.status, 'Active') = 'Active'
				` + entityFilter + bankFilter + `
		)
		SELECT
			TO_CHAR(DATE_TRUNC('month', CURRENT_DATE), 'YYYY-MM') AS current_month,
			COALESCE(
				SUM(CASE WHEN month_start = DATE_TRUNC('month', CURRENT_DATE) 
					THEN balance_amount ELSE 0 END),
				0
			) AS current_balance,
			TO_CHAR(DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month'), 'YYYY-MM') AS previous_month,
			COALESCE(
				SUM(CASE WHEN month_start = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') 
					THEN balance_amount ELSE 0 END),
				0
			) AS previous_balance
		FROM enriched_balance
	`

	var currentMonth, previousMonth string
	var currentBalance, previousBalance float64

	err := db.QueryRow(ctx, query).Scan(
		&currentMonth,
		&currentBalance,
		&previousMonth,
		&previousBalance,
	)

	if err != nil {
		return MoMComparison{}, err
	}

	changeAmount := currentBalance - previousBalance
	changePercent := 0.0
	if previousBalance != 0 {
		changePercent = (changeAmount / previousBalance) * 100
	}

	return MoMComparison{
		CurrentMonth: MonthData{
			Month:        currentMonth,
			TotalBalance: currentBalance,
		},
		PreviousMonth: MonthData{
			Month:        previousMonth,
			TotalBalance: previousBalance,
		},
		ChangeAmount:  changeAmount,
		ChangePercent: changePercent,
	}, nil
}
