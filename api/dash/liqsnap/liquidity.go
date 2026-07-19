package liqsnap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	// "github.com/jackc/pgx/v5"
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
)

type liquidityScope struct {
	Entities   []string
	Currencies []string
	Accounts   []string
}

func resolveLiquidityScope(ctx context.Context, entityName, currency string) (liquidityScope, string) {
	entityName = strings.TrimSpace(entityName)
	currency = strings.ToUpper(strings.TrimSpace(currency))

	entities := api.GetEntityNamesFromCtx(ctx)
	if entityName != "" {
		if !api.IsEntityAllowed(ctx, entityName) {
			return liquidityScope{}, constants.ErrNoAccessibleBusinessUnit
		}
		entities = []string{entityName}
	}

	currencies := normalizeUpper(api.GetCurrencyCodesFromCtx(ctx))
	if currency != "" {
		if !api.IsCurrencyAllowed(ctx, currency) {
			return liquidityScope{}, constants.ErrNoAccessibleBusinessUnit
		}
		currencies = []string{currency}
	}

	if len(entities) == 0 || len(currencies) == 0 {
		return liquidityScope{}, constants.ErrNoAccessibleBusinessUnit
	}
	return liquidityScope{Entities: entities, Currencies: currencies, Accounts: approvedAccounts(ctx)}, ""
}

func normalizeUpper(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if s := strings.ToUpper(strings.TrimSpace(value)); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func approvedAccounts(ctx context.Context) []string {
	accounts, _ := ctx.Value(api.ApprovedBankAccountsKey).([]map[string]string)
	out := make([]string, 0, len(accounts))
	for _, account := range accounts {
		if s := strings.TrimSpace(account["account_number"]); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Handler for /dash/liquidity/total-cash-balance-by-entity
type UserRequest struct {
	UserID string `json:"user_id"`
}

func TotalCashBalanceByEntityHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		scope, msg := resolveLiquidityScope(r.Context(), "", "")
		if msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		balances, err := TotalCashBalanceByEntity(r.Context(), pgxPool, scope)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		api.RespondEnvelopeSuccess(w, "Success", balances)
	}
}

// Handler for /dash/liquidity/liquidity-coverage-ratio
func LiquidityCoverageRatioHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		scope, msg := resolveLiquidityScope(r.Context(), "", "")
		if msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		ratio, err := LiquidityCoverageRatio(r.Context(), pgxPool, scope)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"liquidity_coverage_ratio": ratio})
	}
}

// Handler for /dash/liquidity/entity-currency-wise-cash
func EntityCurrencyWiseCashHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID     string `json:"user_id"`
			EntityName string `json:"entity_name,omitempty"`
			Currency   string `json:"currency,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		scope, msg := resolveLiquidityScope(r.Context(), req.EntityName, req.Currency)
		if msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		data, err := entitycurrencywiseCash(r.Context(), pgxPool, scope)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		api.RespondEnvelopeSuccess(w, "Success", data)
	}
}

type CurrencyBalance struct {
	Currency string  `json:"currency"`
	Balance  float64 `json:"balance"`
}

type BankBalance struct {
	Bank       string            `json:"bank"`
	Currencies []CurrencyBalance `json:"currencies"`
}

type EntityBankBalance struct {
	Entity string        `json:"entity"`
	Banks  []BankBalance `json:"banks"`
}

func TotalCashBalanceByEntity(ctx context.Context, pgxPool *pgxpool.Pool, scope liquidityScope) ([]EntityBankBalance, error) {
	// Query for entity, bank, currency, balance
	query := `SELECT 
		m.entity_name,
		mb.bank_name,
		bs.currency_code,
		COALESCE(SUM(bs.closingbalance), 0) AS balance
	FROM bank_statement bs
	JOIN masterentity m ON bs.entityid = m.entity_id
	JOIN masterbankaccount mba ON bs.account_no = mba.account_no
	JOIN masterbank mb ON mba.bank_id = mb.bank_id
	WHERE bs.status = 'Approved'
	  AND bs.account_no = ANY($1)
	  AND m.entity_name = ANY($2)
	  AND UPPER(TRIM(COALESCE(bs.currency_code, ''))) = ANY($3)
	GROUP BY m.entity_name, mb.bank_name, bs.currency_code;`
	rows, err := pgxPool.Query(ctx, query, scope.Accounts, scope.Entities, scope.Currencies)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// Build nested response
	entityMap := make(map[string]map[string]map[string]float64) // entity -> bank -> currency -> balance
	for rows.Next() {
		var entity, bank, currency string
		var balance float64
		if err := rows.Scan(&entity, &bank, &currency, &balance); err != nil {
			return nil, err
		}
		if _, ok := entityMap[entity]; !ok {
			entityMap[entity] = make(map[string]map[string]float64)
		}
		if _, ok := entityMap[entity][bank]; !ok {
			entityMap[entity][bank] = make(map[string]float64)
		}
		entityMap[entity][bank][currency] += balance
	}
	// Convert to response struct
	var response []EntityBankBalance
	for entity, banks := range entityMap {
		var bankList []BankBalance
		for bank, currencies := range banks {
			var currencyList []CurrencyBalance
			for currency, balance := range currencies {
				currencyList = append(currencyList, CurrencyBalance{
					Currency: currency,
					Balance:  balance,
				})
			}
			bankList = append(bankList, BankBalance{
				Bank:       bank,
				Currencies: currencyList,
			})
		}
		response = append(response, EntityBankBalance{
			Entity: entity,
			Banks:  bankList,
		})
	}
	return response, nil
}

func LiquidityCoverageRatio(ctx context.Context, pgxPool *pgxpool.Pool, scope liquidityScope) (float64, error) {
	var inflows, outflows float64

	inflowQuery := `SELECT COALESCE(SUM(cpi.expected_amount), 0)
        FROM cashflow_proposal_item cpi
        JOIN cashflow_proposal cp ON cp.proposal_id = cpi.proposal_id
        WHERE cpi.cashflow_type = 'Inflow'
        AND COALESCE(cpi.is_deleted, false) = false
        AND cpi.start_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '14 days'
        AND cpi.entity_name = ANY($1)
        AND UPPER(TRIM(COALESCE(cp.currency_code, ''))) = ANY($2);`
	err := pgxPool.QueryRow(ctx, inflowQuery, scope.Entities, scope.Currencies).Scan(&inflows)
	if err != nil {
		return 0, err
	}

	outflowQuery := `SELECT COALESCE(SUM(cpi.expected_amount), 0)
        FROM cashflow_proposal_item cpi
        JOIN cashflow_proposal cp ON cp.proposal_id = cpi.proposal_id
        WHERE cpi.cashflow_type = 'Outflow'
        AND COALESCE(cpi.is_deleted, false) = false
        AND cpi.start_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '14 days'
        AND cpi.entity_name = ANY($1)
        AND UPPER(TRIM(COALESCE(cp.currency_code, ''))) = ANY($2);`
	err = pgxPool.QueryRow(ctx, outflowQuery, scope.Entities, scope.Currencies).Scan(&outflows)
	if err != nil {
		return 0, err
	}

	if outflows == 0 {
		return 0, nil
	}
	return inflows / (outflows), nil
}

type EntityCurrencyCash struct {
	EntityName   string  `json:"entity_name"`
	CurrencyCode string  `json:"currency_code"`
	TotalBalance float64 `json:"total_balance"`
	Normalized   float64 `json:"normalized_total"`
}

func entitycurrencywiseCash(ctx context.Context, pgxPool *pgxpool.Pool, scope liquidityScope) ([]EntityCurrencyCash, error) {
	results := make([]EntityCurrencyCash, 0) // ensures JSON [] instead of null
	today := time.Now().UTC().Format(constants.DateFormat)

	fetchQuery := `SELECT me.entity_name, t.currency_code, 
		COALESCE(SUM(t.balance_amount),0)::float8 as total_balance
		FROM (
			SELECT DISTINCT ON (COALESCE(account_no, iban, nickname)) 
				account_no, currency_code, balance_amount
			FROM bank_balances_manual
			WHERE as_of_date <= $1
			ORDER BY COALESCE(account_no, iban, nickname), as_of_date DESC, as_of_time DESC
		) t
		JOIN masterbankaccount mba ON t.account_no = mba.account_number
		JOIN masterentitycash me ON mba.entity_id = me.entity_id
		WHERE t.account_no = ANY($2)
		  AND me.entity_name = ANY($3)
		  AND UPPER(TRIM(COALESCE(t.currency_code, ''))) = ANY($4)
	`

	args := []interface{}{today, scope.Accounts, scope.Entities, scope.Currencies}
	fetchQuery += ` GROUP BY me.entity_name, t.currency_code ORDER BY me.entity_name, t.currency_code;`

	rows, err := pgxPool.Query(ctx, fetchQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var rec EntityCurrencyCash
		if err := rows.Scan(&rec.EntityName, &rec.CurrencyCode, &rec.TotalBalance); err != nil {
			return nil, err
		}
		rate := 1.0
		if r, ok := rates[rec.CurrencyCode]; ok {
			rate = r
		}
		rec.Normalized = rec.TotalBalance * rate
		results = append(results, rec)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return results, nil
}

// KPI structs
type KPICards struct {
	TotalCashBalance    float64 `json:"total_cash_balance"`
	LiquidityCoverage   float64 `json:"liquidity_coverage"`
	SurplusDeficit      bool    `json:"surplus_deficit"`
	MinProjectedClosing float64 `json:"min_projected_closing"`
	NetPositionToday    float64 `json:"net_position_today"`
}

type DetailedDailyCashFlowRow struct {
	Date           string  `json:"date"`
	OpeningBalance float64 `json:"opening_balance"`
	Inflows        float64 `json:"inflows"`
	Outflows       float64 `json:"outflows"`
	NetFlow        float64 `json:"net_flow"`
	ClosingBalance float64 `json:"closing_balance"`
}

// KpiCardsHandler expects JSON: { "horizon": 14 }
func KpiCardsHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 14
		}
		scope, msg := resolveLiquidityScope(r.Context(), req.EntityName, req.Currency)
		if msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		kpi, err := GetKpiCards(r.Context(), pgxPool, req.Horizon, scope)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		api.RespondEnvelopeSuccess(w, "Success", kpi)
	}
}

// DetailedDailyCashFlowHandler expects JSON: { "horizon": 14 }
func DetailedDailyCashFlowHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 14
		}
		scope, msg := resolveLiquidityScope(r.Context(), req.EntityName, req.Currency)
		if msg != "" {
			http.Error(w, msg, http.StatusForbidden)
			return
		}
		rows, err := DetailedDailyCashFlowRows(r.Context(), pgxPool, req.Horizon, scope)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		api.RespondEnvelopeSuccess(w, "Success", rows)
	}
}

// GetKpiCards computes KPI cards for the given horizon (days) from today.
var rates = map[string]float64{
	"USD": 1.0,
	"AUD": 0.68,
	"CAD": 0.75,
	"CHF": 1.1,
	"CNY": 0.14,
	"RMB": 0.14,
	"EUR": 1.09,
	"GBP": 1.28,
	"JPY": 0.0067,
	"SEK": 0.095,
	"INR": 0.0117,
}

func GetKpiCards(ctx context.Context, pgxPool *pgxpool.Pool, horizon int, scope liquidityScope) (KPICards, error) {
	var k KPICards
	// 1) Total cash balance: sum of latest balance_amount per account as of today
	today := time.Now().UTC()
	dateToday := today.Format(constants.DateFormat)

	// Use DISTINCT ON to pick latest record per account identifier and sum balance_amount
	// We'll fetch per-account latest balances and convert to USD in Go
	fetchQuery := `SELECT DISTINCT ON (COALESCE(account_no, iban, nickname)) balance_amount, currency_code
		FROM bank_balances_manual
		WHERE as_of_date <= $1
		  AND account_no = ANY($2)
		  AND UPPER(TRIM(COALESCE(currency_code, ''))) = ANY($3)`
	fetchArgs := []interface{}{dateToday, scope.Accounts, scope.Currencies}
	fetchQuery += " ORDER BY COALESCE(account_no, iban, nickname), as_of_date DESC, as_of_time DESC"

	fetchRows, err := pgxPool.Query(ctx, fetchQuery, fetchArgs...)
	if err != nil {
		return k, fmt.Errorf("total balance fetch: %w", err)
	}
	defer fetchRows.Close()
	var totalUSD float64
	for fetchRows.Next() {
		var amt float64
		var currencyCode string
		if err := fetchRows.Scan(&amt, &currencyCode); err != nil {
			return k, err
		}
		rate := rates[currencyCode]
		if rate == 0 {
			rate = 1.0
		}
		totalUSD += amt * rate
	}
	k.TotalCashBalance = totalUSD

	// 2) Liquidity coverage: sum inflows / sum outflows over horizon using approved payables/receivables
	start := today
	end := today.AddDate(0, 0, horizon-1)
	startDate := start.Format(constants.DateFormat)
	endDate := end.Format(constants.DateFormat)

	// Apply optional filters to inflow/outflow queries
	inflowQuery := `SELECT COALESCE(SUM(invoice_amount),0)::float8, COALESCE(currency_code,'USD') FROM tr_receivables r
		JOIN auditactionreceivable a ON a.receivable_id = r.receivable_id AND a.processing_status='APPROVED'
		WHERE r.due_date BETWEEN $1 AND $2`
	inflowArgs := []interface{}{startDate, endDate}
	inflowQuery += " AND r.entity_name = ANY($3)"
	inflowArgs = append(inflowArgs, scope.Entities)
	inflowQuery += " AND UPPER(TRIM(COALESCE(r.currency_code, ''))) = ANY($4)"
	inflowArgs = append(inflowArgs, scope.Currencies)
	inflowQuery += " GROUP BY r.currency_code"
	var inflows float64
	// We may get multiple rows (one per currency). Sum after converting to USD.
	inflowRows2, err := pgxPool.Query(ctx, inflowQuery, inflowArgs...)
	if err != nil {
		return k, fmt.Errorf("inflows query: %w", err)
	}
	var inflowsUSD float64
	for inflowRows2.Next() {
		var amt float64
		var cur string
		if err := inflowRows2.Scan(&amt, &cur); err != nil {
			inflowRows2.Close()
			return k, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		inflowsUSD += amt * rate
	}
	inflowRows2.Close()
	inflows = inflowsUSD

	outflowQuery := `SELECT COALESCE(SUM(amount),0)::float8, COALESCE(currency_code,'USD') FROM tr_payables p
		JOIN auditactionpayable a ON a.payable_id = p.payable_id AND a.processing_status='APPROVED'
		WHERE p.due_date BETWEEN $1 AND $2`
	outflowArgs := []interface{}{startDate, endDate}
	outflowQuery += " AND p.entity_name = ANY($3)"
	outflowArgs = append(outflowArgs, scope.Entities)
	outflowQuery += " AND UPPER(TRIM(COALESCE(p.currency_code, ''))) = ANY($4)"
	outflowArgs = append(outflowArgs, scope.Currencies)
	outflowQuery += " GROUP BY p.currency_code"
	var outflows float64
	outflowRows2, err := pgxPool.Query(ctx, outflowQuery, outflowArgs...)
	if err != nil {
		return k, fmt.Errorf("outflows query: %w", err)
	}
	var outflowsUSD float64
	for outflowRows2.Next() {
		var amt float64
		var cur string
		if err := outflowRows2.Scan(&amt, &cur); err != nil {
			outflowRows2.Close()
			return k, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		outflowsUSD += amt * rate
	}
	outflowRows2.Close()
	outflows = outflowsUSD
	if outflows == 0 {
		k.LiquidityCoverage = 0
	} else {
		k.LiquidityCoverage = inflows / outflows
	}

	// 3) Build detailed rows and derive min projected closing and net position today
	detailedRows, err := DetailedDailyCashFlowRows(ctx, pgxPool, horizon, scope)
	if err != nil {
		return k, fmt.Errorf("detailed rows: %w", err)
	}
	minClosing := 1e308 * -1
	if len(detailedRows) > 0 {
		minClosing = detailedRows[0].ClosingBalance
		for _, r := range detailedRows {
			if r.ClosingBalance < minClosing {
				minClosing = r.ClosingBalance
			}
		}
	} else {
		minClosing = 0
	}
	k.MinProjectedClosing = minClosing
	k.SurplusDeficit = minClosing < 0

	// Net position today: opening balance today - outflows(today) + inflows(today)
	var inflowToday, outflowToday float64
	if len(detailedRows) > 0 {
		inflowToday = detailedRows[0].Inflows
		outflowToday = detailedRows[0].Outflows
		k.NetPositionToday = detailedRows[0].OpeningBalance - outflowToday + inflowToday
	} else {
		k.NetPositionToday = k.TotalCashBalance
	}
	return k, nil
}

// DetailedDailyCashFlowRows returns the daily flow rows for horizon days starting today.
// Implementation: fetch opening (latest closing) balance as of today, fetch aggregated inflows/outflows per date
// over the range in two queries, then iterate days and compute net/closing balances in Go (single pass).
func DetailedDailyCashFlowRows(ctx context.Context, pgxPool *pgxpool.Pool, horizon int, scope liquidityScope) ([]DetailedDailyCashFlowRow, error) {
	today := time.Now().UTC()
	start := today
	end := today.AddDate(0, 0, horizon-1)
	startDate := start.Format(constants.DateFormat)
	endDate := end.Format(constants.DateFormat)

	// Opening balance: latest balance_amount per account as of start date
	openingQuery := `SELECT DISTINCT ON (COALESCE(account_no, iban, nickname)) balance_amount, currency_code
		FROM bank_balances_manual
		WHERE as_of_date <= $1
		  AND account_no = ANY($2)
		  AND UPPER(TRIM(COALESCE(currency_code, ''))) = ANY($3)`
	openingArgs := []interface{}{startDate, scope.Accounts, scope.Currencies}
	openingQuery += ` ORDER BY COALESCE(account_no, iban, nickname), as_of_date DESC, as_of_time DESC`
	// Sum per-account latest closing balances and convert to USD using rates
	openingRows, err := pgxPool.Query(ctx, openingQuery, openingArgs...)
	if err != nil {
		return nil, fmt.Errorf("opening balance query: %w", err)
	}
	defer openingRows.Close()
	var openingBalance float64
	for openingRows.Next() {
		var amt float64
		var cur string
		if err := openingRows.Scan(&amt, &cur); err != nil {
			return nil, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		openingBalance += amt * rate
	}

	// Aggregated inflows per date (receivables due_date)
	inflowsQ := `SELECT r.due_date::date, COALESCE(SUM(r.invoice_amount),0)::float8, COALESCE(r.currency_code,'USD')
		FROM tr_receivables r
		JOIN auditactionreceivable a ON a.receivable_id = r.receivable_id AND a.processing_status='APPROVED'
		WHERE r.due_date BETWEEN $1 AND $2`
	inflowArgs := []interface{}{startDate, endDate}
	inflowsQ += " AND r.entity_name = ANY($3)"
	inflowArgs = append(inflowArgs, scope.Entities)
	inflowsQ += " AND UPPER(TRIM(COALESCE(r.currency_code, ''))) = ANY($4)"
	inflowArgs = append(inflowArgs, scope.Currencies)
	inflowsQ += " GROUP BY r.due_date::date, r.currency_code;"
	inflowRows, err := pgxPool.Query(ctx, inflowsQ, inflowArgs...)
	if err != nil {
		return nil, fmt.Errorf("inflows per date: %w", err)
	}
	defer inflowRows.Close()
	inflowsMap := make(map[string]float64)
	for inflowRows.Next() {
		var d time.Time
		var amt float64
		var cur string
		if err := inflowRows.Scan(&d, &amt, &cur); err != nil {
			return nil, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		inflowsMap[d.Format(constants.DateFormat)] += amt * rate
	}

	// Aggregated outflows per date (payables due_date)
	outflowsQ := `SELECT p.due_date::date, COALESCE(SUM(p.amount),0)::float8, COALESCE(p.currency_code,'USD')
		FROM tr_payables p
		JOIN auditactionpayable a ON a.payable_id = p.payable_id AND a.processing_status='APPROVED'
		WHERE p.due_date BETWEEN $1 AND $2`
	outflowArgs := []interface{}{startDate, endDate}
	outflowsQ += " AND p.entity_name = ANY($3)"
	outflowArgs = append(outflowArgs, scope.Entities)
	outflowsQ += " AND UPPER(TRIM(COALESCE(p.currency_code, ''))) = ANY($4)"
	outflowArgs = append(outflowArgs, scope.Currencies)
	outflowsQ += " GROUP BY p.due_date::date, p.currency_code;"
	outflowRows, err := pgxPool.Query(ctx, outflowsQ, outflowArgs...)
	if err != nil {
		return nil, fmt.Errorf("outflows per date: %w", err)
	}
	defer outflowRows.Close()
	outflowsMap := make(map[string]float64)
	for outflowRows.Next() {
		var d time.Time
		var amt float64
		var cur string
		if err := outflowRows.Scan(&d, &amt, &cur); err != nil {
			return nil, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		outflowsMap[d.Format(constants.DateFormat)] += amt * rate
	}

	// Iterate dates and compute rows
	var rows []DetailedDailyCashFlowRow
	opening := openingBalance
	for i := 0; i < horizon; i++ {
		date := start.AddDate(0, 0, i)
		ds := date.Format(constants.DateFormat)
		inflow := inflowsMap[ds]
		outflow := outflowsMap[ds]
		net := inflow - outflow
		closing := opening + net
		rows = append(rows, DetailedDailyCashFlowRow{
			Date:           ds,
			OpeningBalance: opening,
			Inflows:        inflow,
			Outflows:       outflow,
			NetFlow:        net,
			ClosingBalance: closing,
		})
		opening = closing
	}
	return rows, nil
}
