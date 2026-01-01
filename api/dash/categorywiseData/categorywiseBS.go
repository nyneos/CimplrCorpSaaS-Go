package categorywisedata

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CategoryAgg struct {
	Category string  `json:"category"`
	Inflow   float64 `json:"inflow"`
	Outflow  float64 `json:"outflow"`
}

type StatementTxnRow struct {
	Date           string `json:"date"`
	OpeningBalance string `json:"opening_balance"`
	Inflow         string `json:"inflow"`
	Outflow        string `json:"outflow"`
	ClosingBalance string `json:"closing_balance"`
	Narration      string `json:"narration,omitempty"`
	Category       string `json:"category,omitempty"`
}

type EntityBreakdownRow struct {
	ID        string            `json:"id"`
	Entity    string            `json:"entity"`
	Bank      string            `json:"bank"`
	Account   string            `json:"account"`
	Inflow    float64           `json:"inflow"`
	Outflow   float64           `json:"outflow"`
	Net       float64           `json:"net"`
	Statement []StatementTxnRow `json:"statement,omitempty"`
}

type TransactionRow struct {
	TxID          string  `json:"txId"`
	Date          string  `json:"date"`
	Category      string  `json:"category,omitempty"`
	SubCategory   string  `json:"subCategory,omitempty"`
	Entity        string  `json:"entity"`
	Bank          string  `json:"bank"`
	Account       string  `json:"account"`
	Amount        float64 `json:"amount"`
	Direction     string  `json:"direction"`
	Narration     string  `json:"narration,omitempty"`
	Misclassified bool    `json:"misclassified_flag,omitempty"`
}

type BankBalanceKPI struct {
	Bank  string  `json:"bank"`
	Total float64 `json:"total"`
}

type LowestBalanceAccount struct {
	Bank     string  `json:"bank"`
	Account  string  `json:"account"`
	Balance  float64 `json:"balance"`
	Currency string  `json:"currency"`
	AsOfDate string  `json:"as_of_date"`
}

func ctxApprovedAccountNumbers(ctx context.Context) []string {
	v := ctx.Value("ApprovedBankAccounts")
	if v == nil {
		return nil
	}
	accounts, ok := v.([]map[string]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(accounts))
	for _, a := range accounts {
		acct := strings.TrimSpace(a["account_number"])
		if acct != "" {
			out = append(out, acct)
		}
	}
	return out
}

func normalizeLowerTrimSlice(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.ToLower(strings.TrimSpace(s))
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func normalizeUpperTrimSlice(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func GetCategorywiseBreakdownHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {

	// numeric mask wide enough to avoid overflow display
	const wideNumMask = "FM9999999999999999999999999999990.00"

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := r.URL.Query()
		debug := strings.TrimSpace(q.Get("debug")) == "1"

		entityF := q.Get("entity")
		bankF := q.Get("bank")
		currencyF := q.Get("currency")
		horizon := q.Get("horizon")

		// dynamic horizon (any positive integer, default 30)
		days, err := strconv.Atoi(horizon)
		if err != nil || days <= 0 {
			days = 30
		}
		fromDate := time.Now().AddDate(0, 0, -days).Format(constants.DateFormat)

		allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
		allowedAccountNumbers := ctxApprovedAccountNumbers(ctx)
		allowedBanksNorm := normalizeLowerTrimSlice(api.GetBankNamesFromCtx(ctx))
		allowedCurrenciesNorm := normalizeUpperTrimSlice(api.GetCurrencyCodesFromCtx(ctx))

		if len(allowedEntityIDs) == 0 || len(allowedAccountNumbers) == 0 || len(allowedBanksNorm) == 0 || len(allowedCurrenciesNorm) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}

		var args []interface{}
		arg := 1
		var filters []string

		// date filter on COALESCE(transaction_date, value_date)
		filters = append(filters, fmt.Sprintf("COALESCE(t.transaction_date, t.value_date) >= $%d::date", arg))
		args = append(args, fromDate)
		arg++

		// mandatory scope filters from prevalidation context
		filters = append(filters, fmt.Sprintf("bs.entity_id = ANY($%d)", arg))
		args = append(args, allowedEntityIDs)
		arg++
		filters = append(filters, fmt.Sprintf("bs.account_number = ANY($%d)", arg))
		args = append(args, allowedAccountNumbers)
		arg++
		filters = append(filters, fmt.Sprintf("lower(trim(COALESCE(mba.bank_name, mb.bank_name, ''))) = ANY($%d)", arg))
		args = append(args, allowedBanksNorm)
		arg++
		filters = append(filters, fmt.Sprintf("upper(trim(COALESCE(mba.currency, ''))) = ANY($%d)", arg))
		args = append(args, allowedCurrenciesNorm)
		arg++

		if entityF != "" {
			if !api.IsEntityAllowed(ctx, entityF) {
				http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
				return
			}
			filters = append(filters, fmt.Sprintf("bs.entity_id = $%d", arg))
			args = append(args, entityF)
			arg++
		}
		if bankF != "" {
			if !api.IsBankAllowed(ctx, bankF) {
				http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
				return
			}
			// bank name may exist on mba.bank_name or masterbank.mb.bank_name
			filters = append(filters, fmt.Sprintf("(mba.bank_name = $%d OR mb.bank_name = $%d)", arg, arg))
			args = append(args, bankF)
			arg++
		}
		if currencyF != "" {
			if !api.IsCurrencyAllowed(ctx, currencyF) {
				http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
				return
			}
			filters = append(filters, fmt.Sprintf("mba.currency = $%d", arg))
			args = append(args, currencyF)
			arg++
		}

		whereClause := ""
		if len(filters) > 0 {
			whereClause = "WHERE " + strings.Join(filters, " AND ")
		}

		/* ---------------- Category aggregation ---------------- */
		categorySQL := `
SELECT 
	COALESCE(tc.category_name, 'Uncategorized') AS category,
	SUM(COALESCE(t.deposit_amount,0)) AS inflow,
	SUM(COALESCE(t.withdrawal_amount,0)) AS outflow
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs
	ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN cimplrcorpsaas.transaction_categories tc
	ON t.category_id = tc.category_id
LEFT JOIN public.masterbankaccount mba
	ON mba.account_number = bs.account_number
LEFT JOIN public.masterbank mb
	ON mb.bank_id = mba.bank_id
JOIN (
	SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
	FROM cimplrcorpsaas.auditactionbankstatement
	ORDER BY bankstatementid, requested_at DESC
) a ON a.bankstatementid = bs.bank_statement_id
	AND a.processing_status = 'APPROVED'
` + whereClause + `
GROUP BY category
ORDER BY inflow DESC, outflow DESC;
`

		catRows, err := pgxPool.Query(ctx, categorySQL, args...)
		if err != nil {
			http.Error(w, "error querying category aggregation: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer catRows.Close()

		categories := make([]CategoryAgg, 0)
		for catRows.Next() {
			var c CategoryAgg
			if err := catRows.Scan(&c.Category, &c.Inflow, &c.Outflow); err == nil {
				categories = append(categories, c)
			}
		}

		/* ---------------- Entity breakdown + transaction statements ----------------
		   Strategy:
		   - Aggregate inflow/outflow per bs.account_number in a subquery `x` (same filters applied).
		   - LEFT JOIN LATERAL: build a JSON array of all transactions for approved statements for that account.
		   - Each transaction item contains opening/closing balances and category + narration.
		--------------------------------------------------------------- */

		entitySQL := `
SELECT 
  x.entity_id,
  x.entity_name,
  x.bank_name,
  x.account_number,
  x.inflow,
  x.outflow,
  COALESCE(stmts.transactions_json, '[]') AS transactions_json
FROM (
  SELECT 
    bs.entity_id,
    me.entity_name,
    COALESCE(mba.bank_name, mb.bank_name) AS bank_name,
    bs.account_number,
    SUM(COALESCE(t.deposit_amount,0)) AS inflow,
    SUM(COALESCE(t.withdrawal_amount,0)) AS outflow
  FROM cimplrcorpsaas.bank_statement_transactions t
  JOIN cimplrcorpsaas.bank_statements bs
    ON t.bank_statement_id = bs.bank_statement_id
  LEFT JOIN public.masterbankaccount mba
    ON mba.account_number = bs.account_number
  LEFT JOIN public.masterbank mb
    ON mb.bank_id = mba.bank_id
  LEFT JOIN public.masterentitycash me
    ON bs.entity_id = me.entity_id
  JOIN (
    SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
    FROM cimplrcorpsaas.auditactionbankstatement
    ORDER BY bankstatementid, requested_at DESC
  ) ap ON ap.bankstatementid = bs.bank_statement_id
     AND ap.processing_status = 'APPROVED'
` + whereClause + `
  GROUP BY 
    bs.entity_id,
    me.entity_name,
    COALESCE(mba.bank_name, mb.bank_name),
    bs.account_number
) x
LEFT JOIN LATERAL (
  SELECT json_agg(
    json_build_object(
      'date', to_char(COALESCE(tx.transaction_date, tx.value_date), 'YYYY-MM-DD'),
      'opening_balance', COALESCE(to_char((tx.balance - COALESCE(tx.deposit_amount,0) + COALESCE(tx.withdrawal_amount,0)), '` + wideNumMask + `'), '0.00'),
      'inflow', COALESCE(to_char(COALESCE(tx.deposit_amount,0), '` + wideNumMask + `'), '0.00'),
      'outflow', COALESCE(to_char(COALESCE(tx.withdrawal_amount,0), '` + wideNumMask + `'), '0.00'),
      'closing_balance', COALESCE(to_char(COALESCE(tx.balance,0), '` + wideNumMask + `'), '0.00'),
      'narration', COALESCE(tx.description, ''),
      'category', CASE WHEN tx.category_id IS NULL THEN 'Uncategorized' ELSE COALESCE(tc.category_name, '') END
    ) ORDER BY COALESCE(tx.transaction_date, tx.value_date), tx.transaction_id
  ) AS transactions_json
  FROM cimplrcorpsaas.bank_statements bs2
  JOIN cimplrcorpsaas.bank_statement_transactions tx
    ON tx.bank_statement_id = bs2.bank_statement_id
  LEFT JOIN cimplrcorpsaas.transaction_categories tc
    ON tx.category_id = tc.category_id
  JOIN (
    SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
    FROM cimplrcorpsaas.auditactionbankstatement
    ORDER BY bankstatementid, requested_at DESC
  ) ap2 ON ap2.bankstatementid = bs2.bank_statement_id
     AND ap2.processing_status = 'APPROVED'
  WHERE bs2.account_number = x.account_number
    AND COALESCE(tx.transaction_date, tx.value_date) >= $1::date
) stmts ON TRUE
ORDER BY x.entity_name, x.bank_name, x.account_number;
`

		// Note: above, the lateral already filters transactions by date using placeholder $1; additional filters
		// for entity/bank/currency are applied in the outer subquery (x) so entities list is already filtered.
		// If you want to further restrict transactions inside statements by bank or currency, extend the WHERE accordingly.

		entityRows, err := pgxPool.Query(ctx, entitySQL, args...)
		if err != nil {
			http.Error(w, "error querying entity breakdown: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer entityRows.Close()

		entities := make([]EntityBreakdownRow, 0)
		for entityRows.Next() {
			var id, entityName, bankName, acct string
			var inflow, outflow float64
			var txnsJSON []byte
			if err := entityRows.Scan(&id, &entityName, &bankName, &acct, &inflow, &outflow, &txnsJSON); err != nil {
				continue
			}

			var txns []StatementTxnRow
			if len(txnsJSON) > 0 {
				_ = json.Unmarshal(txnsJSON, &txns)
			} else {
				txns = []StatementTxnRow{}
			}

			entities = append(entities, EntityBreakdownRow{
				ID:        id,
				Entity:    entityName,
				Bank:      bankName,
				Account:   acct,
				Inflow:    inflow,
				Outflow:   outflow,
				Net:       inflow - outflow,
				Statement: txns,
			})
		}

		/* ---------------- Transactions list (flat) ---------------- */
		txnSQL := `
	SELECT
		t.transaction_id::text,
		to_char(COALESCE(t.transaction_date, t.value_date), 'YYYY-MM-DD') AS dt,
		COALESCE(tc.category_name, '') AS category,
		COALESCE(tc.description, '') AS subcategory,
		me.entity_name,
		COALESCE(mba.bank_name, mb.bank_name) AS bank_name,
		bs.account_number,
		(COALESCE(t.deposit_amount,0) - COALESCE(t.withdrawal_amount,0))::numeric::float8 AS amount,
		CASE WHEN COALESCE(t.deposit_amount,0) > COALESCE(t.withdrawal_amount,0) THEN 'INFLOW' ELSE 'OUTFLOW' END AS direction,
		COALESCE(t.description, ''),
		COALESCE(t.misclassified_flag, false) AS misclassified_flag
	FROM cimplrcorpsaas.bank_statement_transactions t
	JOIN cimplrcorpsaas.bank_statements bs
		ON t.bank_statement_id = bs.bank_statement_id
	LEFT JOIN cimplrcorpsaas.transaction_categories tc
		ON t.category_id = tc.category_id
	LEFT JOIN public.masterbankaccount mba
		ON mba.account_number = bs.account_number
	LEFT JOIN public.masterbank mb
		ON mb.bank_id = mba.bank_id
	LEFT JOIN public.masterentitycash me
		ON bs.entity_id = me.entity_id
	JOIN (
		SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
		FROM cimplrcorpsaas.auditactionbankstatement
		ORDER BY bankstatementid, requested_at DESC
		) ap ON ap.bankstatementid = bs.bank_statement_id
	   AND ap.processing_status = 'APPROVED'
	` + whereClause + `
	ORDER BY COALESCE(t.transaction_date, t.value_date) DESC
	LIMIT 2000;
	`

		txnRows, err := pgxPool.Query(ctx, txnSQL, args...)
		if err != nil {
			http.Error(w, "error querying transactions: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer txnRows.Close()

		txns := make([]TransactionRow, 0)
		for txnRows.Next() {
			var tr TransactionRow
			if err := txnRows.Scan(
				&tr.TxID,
				&tr.Date,
				&tr.Category,
				&tr.SubCategory,
				&tr.Entity,
				&tr.Bank,
				&tr.Account,
				&tr.Amount,
				&tr.Direction,
				&tr.Narration,
				&tr.Misclassified,
			); err == nil {
				txns = append(txns, tr)
			}
		}

		/* ---------------- Realtime KPIs (bank_balances_manual) ---------------- */
		kpiArgs := []interface{}{}
		kArg := 1
		var kpiFilters []string

		// limit to recent data by horizon
		kpiFilters = append(kpiFilters, fmt.Sprintf("COALESCE(b.as_of_date, CURRENT_DATE) >= $%d::date", kArg))
		kpiArgs = append(kpiArgs, fromDate)
		kArg++

		// mandatory scope filters from prevalidation context
		kpiFilters = append(kpiFilters, fmt.Sprintf("b.account_no = ANY($%d)", kArg))
		kpiArgs = append(kpiArgs, allowedAccountNumbers)
		kArg++
		kpiFilters = append(kpiFilters, fmt.Sprintf("mba.entity_id = ANY($%d)", kArg))
		kpiArgs = append(kpiArgs, allowedEntityIDs)
		kArg++
		kpiFilters = append(kpiFilters, fmt.Sprintf("lower(trim(COALESCE(b.bank_name, ''))) = ANY($%d)", kArg))
		kpiArgs = append(kpiArgs, allowedBanksNorm)
		kArg++
		kpiFilters = append(kpiFilters, fmt.Sprintf("upper(trim(COALESCE(b.currency_code, ''))) = ANY($%d)", kArg))
		kpiArgs = append(kpiArgs, allowedCurrenciesNorm)
		kArg++

		if bankF != "" {
			kpiFilters = append(kpiFilters, fmt.Sprintf("lower(trim(COALESCE(b.bank_name, ''))) = $%d", kArg))
			kpiArgs = append(kpiArgs, strings.ToLower(strings.TrimSpace(bankF)))
			kArg++
		}
		if currencyF != "" {
			kpiFilters = append(kpiFilters, fmt.Sprintf("upper(trim(COALESCE(b.currency_code, ''))) = $%d", kArg))
			kpiArgs = append(kpiArgs, strings.ToUpper(strings.TrimSpace(currencyF)))
			kArg++
		}

		kpiWhere := ""
		if len(kpiFilters) > 0 {
			kpiWhere = "WHERE " + strings.Join(kpiFilters, " AND ")
		}

		highestSQL := `
	WITH approved AS (
	  SELECT DISTINCT ON (balance_id) balance_id, processing_status
	  FROM public.auditactionbankbalances
	  ORDER BY balance_id, requested_at DESC
	)
	SELECT
	  COALESCE(b.bank_name, 'Unknown') AS bank,
	  SUM(COALESCE(b.balance_amount, b.closing_balance, 0)) AS total
	FROM public.bank_balances_manual b
	JOIN public.masterbankaccount mba ON b.account_no = mba.account_number AND mba.is_deleted = false
	JOIN approved ap
	  ON ap.balance_id = b.balance_id
	 AND ap.processing_status = 'APPROVED'
	` + kpiWhere + `
	GROUP BY COALESCE(b.bank_name, 'Unknown')
	ORDER BY total DESC
	LIMIT 1;
	`

		var highestBank *BankBalanceKPI
		highestRows, err := pgxPool.Query(ctx, highestSQL, kpiArgs...)
		if err != nil {
			http.Error(w, "error querying highest contributing bank: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer highestRows.Close()
		if highestRows.Next() {
			var bank string
			var total float64
			if err := highestRows.Scan(&bank, &total); err == nil {
				highestBank = &BankBalanceKPI{Bank: bank, Total: total}
			}
		}

		lowestSQL := `
	WITH approved AS (
	  SELECT DISTINCT ON (balance_id) balance_id, processing_status
	  FROM public.auditactionbankbalances
	  ORDER BY balance_id, requested_at DESC
	)
	SELECT
	  COALESCE(b.bank_name, 'Unknown') AS bank,
	  COALESCE(b.account_no, '') AS account,
	  COALESCE(b.balance_amount, b.closing_balance, 0) AS balance,
	  COALESCE(b.currency_code, '') AS currency_code,
	  COALESCE(to_char(b.as_of_date, 'YYYY-MM-DD'), '') AS as_of_date
	FROM public.bank_balances_manual b
	JOIN public.masterbankaccount mba ON b.account_no = mba.account_number AND mba.is_deleted = false
	JOIN approved ap
	  ON ap.balance_id = b.balance_id
	 AND ap.processing_status = 'APPROVED'
	` + kpiWhere + `
	ORDER BY balance ASC
	LIMIT 1;
	`

		var lowestAcct *LowestBalanceAccount
		lowestRows, err := pgxPool.Query(ctx, lowestSQL, kpiArgs...)
		if err != nil {
			http.Error(w, "error querying lowest balance account: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer lowestRows.Close()
		if lowestRows.Next() {
			var bank, acct, currency, asOf string
			var balance float64
			if err := lowestRows.Scan(&bank, &acct, &balance, &currency, &asOf); err == nil {
				lowestAcct = &LowestBalanceAccount{
					Bank:     bank,
					Account:  acct,
					Balance:  balance,
					Currency: currency,
					AsOfDate: asOf,
				}
			}
		}

		/* ---------------- Misclassified transactions section ---------------- */
		misclassifiedSQL := `
SELECT
	t.transaction_id::text,
	to_char(COALESCE(t.transaction_date, t.value_date), 'YYYY-MM-DD') AS dt,
	COALESCE(tc.category_name, '') AS category,
	COALESCE(tc.description, '') AS subcategory,
	me.entity_name,
	COALESCE(mba.bank_name, mb.bank_name) AS bank_name,
	bs.account_number,
	(COALESCE(t.deposit_amount,0) - COALESCE(t.withdrawal_amount,0))::numeric::float8 AS amount,
	CASE WHEN COALESCE(t.deposit_amount,0) > COALESCE(t.withdrawal_amount,0) THEN 'INFLOW' ELSE 'OUTFLOW' END AS direction,
	COALESCE(t.description, ''),
	COALESCE(t.misclassified_flag, false) AS misclassified_flag
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs
	ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN cimplrcorpsaas.transaction_categories tc
	ON t.category_id = tc.category_id
LEFT JOIN public.masterbankaccount mba
	ON mba.account_number = bs.account_number
LEFT JOIN public.masterbank mb
	ON mb.bank_id = mba.bank_id
LEFT JOIN public.masterentitycash me
	ON bs.entity_id = me.entity_id
JOIN (
	SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
	FROM cimplrcorpsaas.auditactionbankstatement
	ORDER BY bankstatementid, requested_at DESC
) ap ON ap.bankstatementid = bs.bank_statement_id
   AND ap.processing_status = 'APPROVED'
` + whereClause + `
AND COALESCE(t.misclassified_flag, false) = true
ORDER BY COALESCE(t.transaction_date, t.value_date) DESC
LIMIT 2000;
`

		misclassifiedRows, err := pgxPool.Query(ctx, misclassifiedSQL, args...)
		if err != nil {
			http.Error(w, "error querying misclassified transactions: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer misclassifiedRows.Close()

		misclassifiedTxns := make([]TransactionRow, 0)
		for misclassifiedRows.Next() {
			var tr TransactionRow
			if err := misclassifiedRows.Scan(
				&tr.TxID,
				&tr.Date,
				&tr.Category,
				&tr.SubCategory,
				&tr.Entity,
				&tr.Bank,
				&tr.Account,
				&tr.Amount,
				&tr.Direction,
				&tr.Narration,
				&tr.Misclassified,
			); err == nil {
				misclassifiedTxns = append(misclassifiedTxns, tr)
			}
		}

		resp := map[string]interface{}{
			"success":                    true,
			"categories":                 categories,
			"entities":                   entities,
			"transactions":               txns,
			"misclassified_transactions": misclassifiedTxns,
			"highest_contributing_bank":  highestBank,
			"lowest_balance_account":     lowestAcct,
		}

		if debug {
			// Count how many rows exist after applying the exact same scope + approval + horizon filters.
			// This helps identify if the emptiness is due to lack of uploads/approvals or due to over-scoping.
			baseCountSQL := `
				SELECT COUNT(*)
				FROM cimplrcorpsaas.bank_statement_transactions t
				JOIN cimplrcorpsaas.bank_statements bs
					ON t.bank_statement_id = bs.bank_statement_id
				LEFT JOIN public.masterbankaccount mba
					ON mba.account_number = bs.account_number
				LEFT JOIN public.masterbank mb
					ON mb.bank_id = mba.bank_id
				JOIN (
					SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
					FROM cimplrcorpsaas.auditactionbankstatement
					ORDER BY bankstatementid, requested_at DESC
				) ap ON ap.bankstatementid = bs.bank_statement_id
					AND ap.processing_status = 'APPROVED'
				` + whereClause + `
			`
			var txnCount int64
			if err := pgxPool.QueryRow(ctx, baseCountSQL, args...).Scan(&txnCount); err != nil {
				txnCount = -1
			}

			stmtCountSQL := `
				SELECT COUNT(DISTINCT bs.bank_statement_id)
				FROM cimplrcorpsaas.bank_statements bs
				LEFT JOIN public.masterbankaccount mba
					ON mba.account_number = bs.account_number
				LEFT JOIN public.masterbank mb
					ON mb.bank_id = mba.bank_id
				JOIN (
					SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
					FROM cimplrcorpsaas.auditactionbankstatement
					ORDER BY bankstatementid, requested_at DESC
				) ap ON ap.bankstatementid = bs.bank_statement_id
					AND ap.processing_status = 'APPROVED'
				WHERE bs.entity_id = ANY($1)
					AND bs.account_number = ANY($2)
					AND lower(trim(COALESCE(mba.bank_name, mb.bank_name, ''))) = ANY($3)
					AND upper(trim(COALESCE(mba.currency, ''))) = ANY($4)
					AND bs.statement_period_end >= $5::date
			`
			var stmtCount int64
			if err := pgxPool.QueryRow(ctx, stmtCountSQL,
				allowedEntityIDs,
				allowedAccountNumbers,
				allowedBanksNorm,
				allowedCurrenciesNorm,
				fromDate,
			).Scan(&stmtCount); err != nil {
				stmtCount = -1
			}

			resp["debug"] = map[string]interface{}{
				"horizon_days":          days,
				"from_date":             fromDate,
				"entity_filter":         entityF,
				"bank_filter":           bankF,
				"currency_filter":        currencyF,
				"allowed_entity_ids":     len(allowedEntityIDs),
				"allowed_accounts":       len(allowedAccountNumbers),
				"allowed_banks":          len(allowedBanksNorm),
				"allowed_currencies":     len(allowedCurrenciesNorm),
				"approved_statement_count": stmtCount,
				"approved_txn_count":     txnCount,
				"note": "If approved_statement_count or approved_txn_count is 0, this is typically because statements are not uploaded/approved for the scoped accounts, or all data is older than the horizon.",
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
