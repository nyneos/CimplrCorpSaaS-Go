package categorywisedata

import (
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

func GetCategorywiseBreakdownHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// numeric mask wide enough to avoid overflow display
	const wideNumMask = "FM9999999999999999999999999999990.00"

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
		q := r.URL.Query()

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

		var args []interface{}
		arg := 1
		var filters []string

		// date filter on COALESCE(transaction_date, value_date)
		filters = append(filters, fmt.Sprintf("COALESCE(t.transaction_date, t.value_date) >= $%d::date", arg))
		args = append(args, fromDate)
		arg++

		if entityF != "" {
			filters = append(filters, fmt.Sprintf("bs.entity_id = $%d", arg))
			args = append(args, entityF)
			arg++
		}
		if bankF != "" {
			// bank name may exist on mba.bank_name or masterbank.mb.bank_name
			filters = append(filters, fmt.Sprintf("(mba.bank_name = $%d OR mb.bank_name = $%d)", arg, arg))
			args = append(args, bankF)
			arg++
		}
		if currencyF != "" {
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

		var categories []CategoryAgg
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

		var entities []EntityBreakdownRow
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

		var txns []TransactionRow
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

		var misclassifiedTxns []TransactionRow
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
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
