package categorywisedata

import (
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

type EntityBreakdownRow struct {
	ID      string  `json:"id"`
	Entity  string  `json:"entity"`
	Bank    string  `json:"bank"`
	Account string  `json:"account"`
	Inflow  float64 `json:"inflow"`
	Outflow float64 `json:"outflow"`
	Net     float64 `json:"net"`
}

type TransactionRow struct {
	TxID        string  `json:"txId"`
	Date        string  `json:"date"`
	Category    string  `json:"category,omitempty"`
	SubCategory string  `json:"subCategory,omitempty"`
	Entity      string  `json:"entity"`
	Bank        string  `json:"bank"`
	Account     string  `json:"account"`
	Amount      float64 `json:"amount"`
	Direction   string  `json:"direction"`
	Narration   string  `json:"narration,omitempty"`
}

func debugLog(label string, value any) {
	fmt.Println("========== DEBUG:", label, "==========")
	fmt.Printf("%+v\n\n", value)
}

func GetCategorywiseBreakdownHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
		q := r.URL.Query()

		entity := q.Get("entity")
		bank := q.Get("bank")
		currency := q.Get("currency")
		horizon := q.Get("horizon")

		// horizon
		var days int
		switch horizon {
		case "7":
			days = 7
		case "30":
			days = 30
		case "90":
			days = 90
		default:
			days = 30
		}
		fromDate := time.Now().AddDate(0, 0, -days).Format("2006-01-02")

		// WHERE builder
		var filters []string
		var args []interface{}
		arg := 1

		filters = append(filters, "t.transaction_date >= $"+strconv.Itoa(arg)+"::date")
		args = append(args, fromDate)
		arg++

		if entity != "" {
			filters = append(filters, "bs.entity_id = $"+strconv.Itoa(arg))
			args = append(args, entity)
			arg++
		}
		if bank != "" {
			filters = append(filters,
				"(mba.bank_name = $"+strconv.Itoa(arg)+" OR mb.bank_name = $"+strconv.Itoa(arg)+")")
			args = append(args, bank)
			arg++
		}
		if currency != "" {
			filters = append(filters, "mba.currency = $"+strconv.Itoa(arg))
			args = append(args, currency)
			arg++
		}

		where := ""
		if len(filters) > 0 {
			where = "WHERE " + strings.Join(filters, " AND ")
		}

		debugLog("WHERE", where)
		debugLog("ARGS", args)

		// ---------------- CATEGORY QUERY ------------------
		catSQL := `
SELECT 
	COALESCE(tc.category_name, 'Uncategorized') AS category,
	SUM(COALESCE(t.deposit_amount,0)) AS inflow,
	SUM(COALESCE(t.withdrawal_amount,0)) AS outflow
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN cimplrcorpsaas.transaction_categories tc ON t.category_id = tc.category_id
LEFT JOIN public.masterbankaccount mba ON mba.account_number = bs.account_number
LEFT JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
JOIN (
	SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
	FROM cimplrcorpsaas.auditactionbankstatement
	ORDER BY bankstatementid, requested_at DESC
) a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
` + where + `
GROUP BY category;
`

		debugLog("CATEGORY SQL", catSQL)

		catRows, err := pool.Query(ctx, catSQL, args...)
		if err != nil {
			debugLog("CATEGORY ERROR", err.Error())
			http.Error(w, err.Error(), 500)
			return
		}

		var categories []CategoryAgg
		for catRows.Next() {
			var c CategoryAgg
			_ = catRows.Scan(&c.Category, &c.Inflow, &c.Outflow)
			categories = append(categories, c)
		}
		debugLog("CATEGORY ROW COUNT", len(categories))

		// ---------------- ENTITY QUERY ------------------
		entitySQL := `
SELECT 
	bs.entity_id,
	me.entity_name,
	COALESCE(mba.bank_name, mb.bank_name) AS bank_name,
	bs.account_number,
	SUM(COALESCE(t.deposit_amount,0)) AS inflow,
	SUM(COALESCE(t.withdrawal_amount,0)) AS outflow
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN public.masterbankaccount mba ON mba.account_number = bs.account_number
LEFT JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
LEFT JOIN public.masterentitycash me ON bs.entity_id = me.entity_id
JOIN (
	SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
	FROM cimplrcorpsaas.auditactionbankstatement
	ORDER BY bankstatementid, requested_at DESC
) a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
` + where + `
GROUP BY bs.entity_id, me.entity_name, COALESCE(mba.bank_name, mb.bank_name), bs.account_number;
`

		debugLog("ENTITY SQL", entitySQL)

		entityRows, err := pool.Query(ctx, entitySQL, args...)
		if err != nil {
			debugLog("ENTITY ERROR", err.Error())
			http.Error(w, err.Error(), 500)
			return
		}

		var entities []EntityBreakdownRow
		for entityRows.Next() {
			var e EntityBreakdownRow
			var inflow, outflow float64
			entityRows.Scan(&e.ID, &e.Entity, &e.Bank, &e.Account, &inflow, &outflow)
			e.Inflow = inflow
			e.Outflow = outflow
			e.Net = inflow - outflow
			entities = append(entities, e)
		}
		debugLog("ENTITY ROW COUNT", len(entities))

		// ---------------- TRANSACTION QUERY ------------------
		txnSQL := `
SELECT 
	t.transaction_id::text,
	TO_CHAR(t.transaction_date, 'YYYY-MM-DD') AS dt,
	COALESCE(tc.category_name, '') AS category,
	COALESCE(tc.description, '') AS subcat,
	me.entity_name,
	COALESCE(mba.bank_name, mb.bank_name) AS bank_name,
	bs.account_number,
	(COALESCE(t.deposit_amount,0) - COALESCE(t.withdrawal_amount,0))::float8 AS amount,
	CASE WHEN COALESCE(t.deposit_amount,0) > COALESCE(t.withdrawal_amount,0) 
		 THEN 'INFLOW' ELSE 'OUTFLOW' END AS direction,
	t.description
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
LEFT JOIN cimplrcorpsaas.transaction_categories tc ON t.category_id = tc.category_id
LEFT JOIN public.masterbankaccount mba ON mba.account_number = bs.account_number
LEFT JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
LEFT JOIN public.masterentitycash me ON bs.entity_id = me.entity_id
JOIN (
	SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
	FROM cimplrcorpsaas.auditactionbankstatement
	ORDER BY bankstatementid, requested_at DESC
) a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
` + where + `
ORDER BY t.transaction_date DESC
LIMIT 500;
`

		debugLog("TRANSACTION SQL", txnSQL)

		txnRows, err := pool.Query(ctx, txnSQL, args...)
		if err != nil {
			debugLog("TRANSACTION ERROR", err.Error())
			http.Error(w, err.Error(), 500)
			return
		}

		var txns []TransactionRow
		for txnRows.Next() {
			var t TransactionRow
			txnRows.Scan(&t.TxID, &t.Date, &t.Category, &t.SubCategory,
				&t.Entity, &t.Bank, &t.Account, &t.Amount, &t.Direction, &t.Narration)
			txns = append(txns, t)
		}
		debugLog("TRANSACTION ROW COUNT", len(txns))

		json.NewEncoder(w).Encode(map[string]any{
			"success":      true,
			"categories":   categories,
			"entities":     entities,
			"transactions": txns,
		})
	}
}
