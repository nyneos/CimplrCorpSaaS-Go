// GetTransactionPoolHandler returns an http.HandlerFunc for the transaction pool dashboard

package commonpool

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ConsolidatedTransactionPool struct {
	Date          string `json:"date"`
	Entity        string `json:"entity"`
	Bank          string `json:"bank"`
	AccountNo     string `json:"account_no"`
	StatementID   string `json:"statement_id"`
	TransactionID int64  `json:"transaction_id"`
	Currency      string `json:"currency"`
	Category      string `json:"category"`
	Type          string `json:"type"` // credit/debit
	Amount        string `json:"amount"`
	Narration     string `json:"Narration"`
}

// TransactionDBRow represents a row from bank_statement_transactions joined with transaction_categories
type TransactionDBRow struct {
	ValueDate        time.Time
	Entity           string
	Bank             string
	AccountNo        string
	StatementID      string
	TransactionID    int64
	Currency         string
	Category         sql.NullString
	CategoryType     sql.NullString
	Description      string
	WithdrawalAmount sql.NullFloat64
	DepositAmount    sql.NullFloat64
}

func nullToString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return "Uncategorized"
}

// mergeBodyIntoQuery sets queryKey from the JSON body when any listed body key has a non-empty string.
// Body wins over existing query values (POST payload is primary).
func mergeBodyIntoQuery(q url.Values, body map[string]any, queryKey string, bodyKeys ...string) {
	if body == nil {
		return
	}
	for _, bk := range bodyKeys {
		if v, ok := body[bk].(string); ok {
			if s := strings.TrimSpace(v); s != "" {
				q.Set(queryKey, s)
				return
			}
		}
	}
}

func GetTransactionPoolHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := r.URL.Query()
		if bodyBytes, err := io.ReadAll(r.Body); err == nil && len(bodyBytes) > 0 {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			var body map[string]any
			if json.Unmarshal(bodyBytes, &body) == nil {
				mergeBodyIntoQuery(q, body, "from_date", "from_date", "fromDate")
				mergeBodyIntoQuery(q, body, "to_date", "to_date", "toDate")
				mergeBodyIntoQuery(q, body, "as_on_date", "as_on_date", "asOnDate")
				mergeBodyIntoQuery(q, body, "entity", "entity")
				mergeBodyIntoQuery(q, body, "bank", "bank")
				mergeBodyIntoQuery(q, body, "currency", "currency")
			}
		}
		transactions, err := FetchConsolidatedTransactionPool(ctx, pool, q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(transactions)
	}
}

// FetchConsolidatedTransactionPool fetches and maps all transactions for dashboard.
// Optional query params: from_date, to_date, as_on_date (YYYY-MM-DD) cap the value/transaction date range; as_on_date is an inclusive upper bound.
// Optional narrowing: entity, bank (name/short_name or bank_id), currency — must remain within prevalidation scope.
func FetchConsolidatedTransactionPool(ctx context.Context, pool *pgxpool.Pool, q url.Values) ([]ConsolidatedTransactionPool, error) {
	allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
	allowedBankNames := api.GetBankNamesFromCtx(ctx)
	allowedCurrencyCodes := api.GetCurrencyCodesFromCtx(ctx)
	allowedAccountNumbers := ctxApprovedAccountNumbers(ctx)

	if len(allowedEntityIDs) == 0 || len(allowedAccountNumbers) == 0 {
		return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
	}
	if len(allowedBankNames) == 0 || len(allowedCurrencyCodes) == 0 {
		return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
	}

	normBanks := make([]string, 0, len(allowedBankNames))
	for _, b := range allowedBankNames {
		b = strings.ToLower(strings.TrimSpace(b))
		if b != "" {
			normBanks = append(normBanks, b)
		}
	}
	normCurrencies := make([]string, 0, len(allowedCurrencyCodes))
	for _, c := range allowedCurrencyCodes {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c != "" {
			normCurrencies = append(normCurrencies, c)
		}
	}
	if len(normBanks) == 0 || len(normCurrencies) == 0 {
		return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
	}

	effectiveEntityIDs := allowedEntityIDs
	entityF := strings.TrimSpace(q.Get("entity"))
	if entityF != "" {
		if !api.IsEntityAllowed(ctx, entityF) {
			return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
		}
		eid, ok := api.ResolveEntityIDForFilter(ctx, entityF)
		if !ok {
			return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
		}
		effectiveEntityIDs = []string{eid}
	}

	effectiveNormBanks := normBanks
	var filterBankID interface{}
	bankF := strings.TrimSpace(q.Get("bank"))
	if bankF != "" {
		if !api.IsBankAllowed(ctx, bankF) {
			return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
		}
		if bid, ok := api.ResolveBankIDForFilter(ctx, bankF); ok {
			filterBankID = bid
		} else {
			bn, ok := api.ResolveBankNameNormForFilter(ctx, bankF)
			if !ok {
				return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
			}
			effectiveNormBanks = []string{bn}
		}
	}

	effectiveNormCurrencies := normCurrencies
	currencyF := strings.TrimSpace(q.Get("currency"))
	if currencyF != "" {
		if !api.IsCurrencyAllowed(ctx, currencyF) {
			return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
		}
		cc, ok := api.ResolveCurrencyCodeUpperForFilter(ctx, currencyF)
		if !ok {
			return nil, fmt.Errorf(constants.ErrNoAccessibleBusinessUnit)
		}
		effectiveNormCurrencies = []string{cc}
	}

	fromD := strings.TrimSpace(q.Get("from_date"))
	toD := strings.TrimSpace(q.Get("to_date"))
	asOn := strings.TrimSpace(q.Get("as_on_date"))
	if asOn != "" && toD == "" {
		toD = asOn
	}
	const dashboardFromMin = "1970-01-01"
	if toD != "" && fromD == "" {
		fromD = dashboardFromMin
	}

	args := []interface{}{
		effectiveEntityIDs,
		allowedAccountNumbers,
	}
	argN := 3
	extra := ""
	if filterBankID != nil {
		extra += fmt.Sprintf(" AND mba.bank_id = $%d", argN)
		args = append(args, filterBankID)
		argN++
	} else if bankF != "" {
		extra += fmt.Sprintf(" AND lower(trim(COALESCE(mba.bank_name, mb.bank_name, ''))) = ANY($%d)", argN)
		args = append(args, effectiveNormBanks)
		argN++
	}
	if currencyF != "" {
		extra += fmt.Sprintf(" AND upper(trim(COALESCE(mba.currency, ''))) = ANY($%d)", argN)
		args = append(args, effectiveNormCurrencies)
		argN++
	}
	if fromD != "" {
		extra += fmt.Sprintf(" AND COALESCE(t.transaction_date, t.value_date) >= $%d::date", argN)
		args = append(args, fromD)
		argN++
	}
	if toD != "" {
		extra += fmt.Sprintf(" AND COALESCE(t.transaction_date, t.value_date) <= $%d::date", argN)
		args = append(args, toD)
		argN++
	}

	baseSQL := `
SELECT DISTINCT ON (t.transaction_id)
	COALESCE(t.transaction_date, t.value_date) AS value_date,
	COALESCE(me.entity_name, '') AS entity_name,
	COALESCE(mb.bank_name, '') AS bank_name,
	s.account_number,
	s.bank_statement_id,
	t.transaction_id,
	COALESCE(mba.currency, '') AS currency,
	c.category_name,
	c.category_type,
	COALESCE(t.description, '') AS description,
	t.withdrawal_amount,
	t.deposit_amount
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements s
	ON t.bank_statement_id = s.bank_statement_id
	AND s.is_deleted = false
	AND s.current_status = 'APPROVED'
LEFT JOIN public.masterbankaccount mba
	ON mba.account_number = s.account_number
	AND COALESCE(mba.is_deleted, false) = false
LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
LEFT JOIN public.mastercashflowcategory c ON t.category_id = c.category_id
LEFT JOIN public.masterentitycash me ON s.entity_id = me.entity_id
WHERE s.entity_id = ANY($1)
  AND s.account_number = ANY($2)` + extra + `
ORDER BY t.transaction_id`

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE _ctp_pool ON COMMIT DROP AS `+baseSQL, args...); err != nil {
		return nil, fmt.Errorf("materialize pool: %w", err)
	}

	rows, err := tx.Query(ctx, `
SELECT value_date, entity_name, bank_name, account_number, bank_statement_id, transaction_id, currency, category_name, category_type, description, withdrawal_amount, deposit_amount
FROM _ctp_pool
ORDER BY value_date DESC`)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var result []ConsolidatedTransactionPool
	for rows.Next() {
		var r TransactionDBRow
		err := rows.Scan(
			&r.ValueDate,
			&r.Entity,
			&r.Bank,
			&r.AccountNo,
			&r.StatementID,
			&r.TransactionID,
			&r.Currency,
			&r.Category,
			&r.CategoryType,
			&r.Description,
			&r.WithdrawalAmount,
			&r.DepositAmount,
		)
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		var typ, amt string
		if r.WithdrawalAmount.Valid && r.WithdrawalAmount.Float64 > 0 {
			typ = "debit"
			amt = fmt.Sprintf("%.2f", r.WithdrawalAmount.Float64)
		} else if r.DepositAmount.Valid && r.DepositAmount.Float64 > 0 {
			typ = "credit"
			amt = fmt.Sprintf("%.2f", r.DepositAmount.Float64)
		} else {
			typ = ""
			amt = ""
		}

		result = append(result, ConsolidatedTransactionPool{
			Date:          r.ValueDate.Format(constants.DateFormat),
			Entity:        r.Entity,
			Bank:          r.Bank,
			AccountNo:     r.AccountNo,
			StatementID:   r.StatementID,
			TransactionID: r.TransactionID,
			Currency:      r.Currency,
			Category:      nullToString(r.Category),
			Type:          typ,
			Amount:        amt,
			Narration:     r.Description,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return result, nil
}

func ctxApprovedAccountNumbers(ctx context.Context) []string {
	v := ctx.Value(api.ApprovedBankAccountsKey)
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
