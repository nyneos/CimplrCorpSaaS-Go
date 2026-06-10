package realtimebalances

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AccountStatement struct {
	Date           string  `json:"date"`
	OpeningBalance float64 `json:"opening_balance"`
	Inflow         float64 `json:"inflow"`
	Outflow        float64 `json:"outflow"`
	ClosingBalance float64 `json:"closing_balance"`
	Narration      string  `json:"narration,omitempty"`
	Category       string  `json:"category,omitempty"`
}

type Trend struct {
	Label      string             `json:"label"`
	Value      float64            `json:"value"`
	Delta      float64            `json:"delta"`
	Currency   string             `json:"currency"`
	BankName   string             `json:"bank_name,omitempty"`
	AccountNo  string             `json:"account_no,omitempty"`
	Statements []AccountStatement `json:"statements"`
}

type Kpi struct {
	TotalBalance   float64 `json:"totalBalance"`
	DayChangeAbs   float64 `json:"dayChangeAbs"`
	DayChangePct   float64 `json:"dayChangePct"`
	ActiveEntities int     `json:"activeEntities"`
	BankAccounts   int     `json:"bankAccounts"`
	Opening        float64 `json:"opening"`
	Closing        float64 `json:"closing"`
	Delta          float64 `json:"delta"`
}

type VarianceBar struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

type DonutSlice struct {
	ID    string  `json:"id"`
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

type EntityRow struct {
	ID      string  `json:"id"`
	Entity  string  `json:"entity"`
	Bank    string  `json:"bank"`
	CCY     string  `json:"ccy"`
	Opening float64 `json:"opening"`
	Closing float64 `json:"closing"`
	Delta   float64 `json:"delta"`
}

type TopCurrency struct {
	Code   string  `json:"code"`
	Label  string  `json:"label"`
	Amount float64 `json:"amount"`
	Color  string  `json:"color"`
}

type DailyStatement struct {
	Date    string  `json:"date"`
	Inflow  float64 `json:"inflow"`
	Outflow float64 `json:"outflow"`
	Net     float64 `json:"net"`
}

type AccountBalance struct {
	EntityID   string             `json:"entity_id"`
	EntityName string             `json:"entity_name"`
	BankName   string             `json:"bank_name"`
	AccountNo  string             `json:"account_no"`
	Currency   string             `json:"currency"`
	Balance    float64            `json:"balance"`
	Opening    float64            `json:"opening"`
	Closing    float64            `json:"closing"`
	Delta      float64            `json:"delta"`
	Statements []AccountStatement `json:"statements"`
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

func normalizeAccountNumber(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Some sources drop leading zeros; normalize to match across tables.
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	return s
}

// strFromBodyThenQuery returns the first non-empty string from JSON body keys (queryKey + camelCase),
// else the URL query value for queryKey.
func strFromBodyThenQuery(body map[string]any, q url.Values, queryKey string) string {
	if s := strFromBodyKeys(body, queryKey, camelJSONKey(queryKey)); s != "" {
		return s
	}
	return strings.TrimSpace(q.Get(queryKey))
}

func strFromBodyKeysThenQuery(body map[string]any, q url.Values, queryKey string, bodyKeys ...string) string {
	if s := strFromBodyKeys(body, bodyKeys...); s != "" {
		return s
	}
	return strings.TrimSpace(q.Get(queryKey))
}

func strFromBodyKeys(body map[string]any, keys ...string) string {
	if body == nil {
		return ""
	}
	for _, k := range keys {
		if v, ok := body[k].(string); ok {
			if s := strings.TrimSpace(v); s != "" {
				return s
			}
		}
	}
	return ""
}

func camelJSONKey(snake string) string {
	parts := strings.Split(snake, "_")
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		if i == 0 {
			parts[i] = strings.ToLower(parts[i][:1]) + parts[i][1:]
		} else {
			parts[i] = strings.ToUpper(parts[i][:1]) + strings.ToLower(parts[i][1:])
		}
	}
	return strings.Join(parts, "")
}

func GetKpiHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
		allowedAccountNumbers := ctxApprovedAccountNumbers(ctx)
		allowedBanksNorm := normalizeLowerTrimSlice(api.GetBankNamesFromCtx(ctx))
		allowedCurrenciesNorm := normalizeUpperTrimSlice(api.GetCurrencyCodesFromCtx(ctx))
		if len(allowedEntityIDs) == 0 || len(allowedAccountNumbers) == 0 || len(allowedBanksNorm) == 0 || len(allowedCurrenciesNorm) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}

		// JSON body is the primary source for filters; query string fills only missing fields.
		var body map[string]any
		if bodyBytes, err := io.ReadAll(r.Body); err == nil && len(bodyBytes) > 0 {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			_ = json.Unmarshal(bodyBytes, &body)
		}
		q := r.URL.Query()
		entity := strFromBodyThenQuery(body, q, "entity")
		bank := strFromBodyThenQuery(body, q, "bank")
		currency := strFromBodyThenQuery(body, q, "currency")
		asOnDate := strFromBodyKeysThenQuery(body, q, "as_on_date",
			"as_on_date", "asOnDate", "snapshot_date", "snapshotDate")

		if entity != "" && !api.IsEntityAllowed(ctx, entity) {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}
		if bank != "" && !api.IsBankAllowed(ctx, bank) {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}
		if currency != "" && !api.IsCurrencyAllowed(ctx, currency) {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}
		bankNorm := ""
		if bank != "" {
			bn, ok := api.ResolveBankNameNormForFilter(ctx, bank)
			if !ok {
				http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
				return
			}
			bankNorm = bn
		}
		currencyNorm := ""
		if currency != "" {
			cc, ok := api.ResolveCurrencyCodeUpperForFilter(ctx, currency)
			if !ok {
				http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
				return
			}
			currencyNorm = cc
		}

		// Snapshot date: balances are derived from the last running balance on approved
		// bank-statement transactions on or before this date (not bank_balances_manual).
		evalAsOn := strings.TrimSpace(asOnDate)
		if evalAsOn == "" {
			evalAsOn = time.Now().Format(constants.DateFormat)
		}
		if _, err := time.Parse(constants.DateFormat, evalAsOn); err != nil {
			http.Error(w, fmt.Sprintf(constants.ErrInvalidDateFormat, "as_on_date"), http.StatusBadRequest)
			return
		}

		// Warm snapshot: one scan of approved statement transactions (not bank_balances_manual).
		// $1 accounts, $2 entities, $3 bank norms, $4 currency norms, $5 as-on date inclusive
		warmTxSelectTmpl := `
			SELECT
				bs.entity_id,
				mba.account_number AS account_no,
				COALESCE(NULLIF(LTRIM(TRIM(mba.account_number), '0'), ''), '0') AS acct_key,
				COALESCE(mba.bank_name, mb.bank_name, '') AS bank_name,
				upper(trim(COALESCE(mba.currency, ''))) AS currency_code,
				tx.transaction_id,
				(COALESCE(tx.transaction_date, tx.value_date))::date AS eff_date,
				COALESCE(tx.transaction_date, tx.value_date) AS eff_ts,
				COALESCE(tx.balance, 0)::float8 AS run_balance,
				COALESCE(tx.deposit_amount, 0)::float8 AS dep_amt,
				COALESCE(tx.withdrawal_amount, 0)::float8 AS wd_amt,
				COALESCE(tx.description, '') AS description,
				tx.category_id
			FROM cimplrcorpsaas.bank_statement_transactions tx
			INNER JOIN cimplrcorpsaas.bank_statements bs ON tx.bank_statement_id = bs.bank_statement_id
			INNER JOIN public.masterbankaccount mba ON bs.account_number = mba.account_number AND COALESCE(mba.is_deleted, false) = false
			INNER JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
			INNER JOIN (
				SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
				FROM cimplrcorpsaas.auditactionbankstatement
				ORDER BY bankstatementid, requested_at DESC
			) a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
			WHERE bs.account_number = ANY($1)
				AND bs.entity_id = ANY($2)
				AND lower(trim(COALESCE(mba.bank_name, mb.bank_name, ''))) = ANY($3)
				AND upper(trim(COALESCE(mba.currency, ''))) = ANY($4)
				AND (COALESCE(tx.transaction_date, tx.value_date))::date <= $5::date
				%s
		`

		extraFilters := make([]string, 0, 3)
		args := []interface{}{
			allowedAccountNumbers,
			allowedEntityIDs,
			allowedBanksNorm,
			allowedCurrenciesNorm,
			evalAsOn,
		}
		argIdx := 6
		if entity != "" {
			eid, ok := api.ResolveEntityIDForFilter(ctx, entity)
			if !ok {
				http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
				return
			}
			extraFilters = append(extraFilters, "mba.entity_id = $"+strconv.Itoa(argIdx))
			args = append(args, eid)
			argIdx++
		}
		if bank != "" {
			// Prefer bank_id: many accounts share the same display name (e.g. "ICICI Bank") under different bank_id.
			if bid, ok := api.ResolveBankIDForFilter(ctx, bank); ok {
				extraFilters = append(extraFilters, "mba.bank_id = $"+strconv.Itoa(argIdx))
				args = append(args, bid)
				argIdx++
			} else if bankNorm != "" {
				extraFilters = append(extraFilters, "lower(trim(COALESCE(mba.bank_name, mb.bank_name, ''))) = $"+strconv.Itoa(argIdx))
				args = append(args, bankNorm)
				argIdx++
			}
		}
		if currencyNorm != "" {
			extraFilters = append(extraFilters, "upper(trim(COALESCE(mba.currency, ''))) = $"+strconv.Itoa(argIdx))
			args = append(args, currencyNorm)
			argIdx++
		}
		extraWhere := ""
		if len(extraFilters) > 0 {
			extraWhere = " AND " + strings.Join(extraFilters, " AND ")
		}
		warmTxSelect := fmt.Sprintf(warmTxSelectTmpl, extraWhere)

		const tempTx = "_rtb_kpi_tx"
		const tempAb = "_rtb_kpi_ab"

		txn, err := pool.Begin(ctx)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		committed := false
		defer func() {
			if !committed {
				_ = txn.Rollback(ctx)
			}
		}()

		_, err = txn.Exec(ctx, `DROP TABLE IF EXISTS `+tempTx+`; DROP TABLE IF EXISTS `+tempAb)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = txn.Exec(ctx, `CREATE TEMP TABLE `+tempTx+` ON COMMIT DROP AS `+warmTxSelect, args...)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		// Latest row per (entity, account, currency) on or before as_on_date: ROW_NUMBER so ordering is
		// deterministic (eff_date, eff_ts, transaction_id). Do not aggregate balances.
		approvedFromTempSQL := `
			CREATE TEMP TABLE ` + tempAb + ` ON COMMIT DROP AS
			WITH tx_scoped AS (SELECT * FROM ` + tempTx + `),
			closing_pick AS (
				SELECT entity_id, bank_name, currency_code, account_no, acct_key, closing_balance, as_of_date
				FROM (
					SELECT
						entity_id,
						bank_name,
						currency_code,
						account_no,
						acct_key,
						run_balance AS closing_balance,
						eff_date AS as_of_date,
						ROW_NUMBER() OVER (
							PARTITION BY entity_id, acct_key, currency_code
							ORDER BY eff_date DESC, eff_ts DESC, transaction_id DESC
						) AS rn
					FROM tx_scoped
				) z WHERE rn = 1
			),
			opening_pick AS (
				SELECT entity_id, acct_key, currency_code, opening_balance
				FROM (
					SELECT
						entity_id,
						acct_key,
						currency_code,
						run_balance AS opening_balance,
						ROW_NUMBER() OVER (
							PARTITION BY entity_id, acct_key, currency_code
							ORDER BY eff_date DESC, eff_ts DESC, transaction_id DESC
						) AS rn
					FROM tx_scoped
					WHERE eff_date < $1::date
				) z WHERE rn = 1
			),
			first_txn_opening AS (
				SELECT entity_id, acct_key, currency_code, derived_opening
				FROM (
					SELECT
						entity_id,
						acct_key,
						currency_code,
						(run_balance - COALESCE(dep_amt, 0) + COALESCE(wd_amt, 0))::float8 AS derived_opening,
						ROW_NUMBER() OVER (
							PARTITION BY entity_id, acct_key, currency_code
							ORDER BY eff_ts ASC, transaction_id ASC
						) AS rn
					FROM tx_scoped
					WHERE eff_date = $1::date
				) z WHERE rn = 1
			)
			SELECT
				c.entity_id,
				c.bank_name,
				c.currency_code,
				c.account_no AS account_no,
				COALESCE(o.opening_balance, f.derived_opening, 0)::float8 AS opening_balance,
				c.closing_balance::float8 AS closing_balance,
				c.as_of_date
			FROM closing_pick c
			LEFT JOIN opening_pick o
				ON o.entity_id = c.entity_id
				AND o.acct_key = c.acct_key
				AND o.currency_code = c.currency_code
			LEFT JOIN first_txn_opening f
				ON f.entity_id = c.entity_id
				AND f.acct_key = c.acct_key
				AND f.currency_code = c.currency_code
		`
		_, err = txn.Exec(ctx, approvedFromTempSQL, evalAsOn)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		// Build old-style response structures — all reads hit small temp tables (no repeated base joins).
		varianceBars := []VarianceBar{}
		vbRows, err := txn.Query(ctx, `
			SELECT mec.entity_name, COALESCE(b.bank_name,'') AS bank_name, COALESCE(SUM(b.closing_balance),0) AS value
			FROM `+tempAb+` b
			JOIN public.masterentitycash mec ON b.entity_id = mec.entity_id
			GROUP BY mec.entity_name, b.bank_name
			ORDER BY value DESC`)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		for vbRows.Next() {
			var entityName, bankName string
			var value float64
			if err := vbRows.Scan(&entityName, &bankName, &value); err == nil {
				varianceBars = append(varianceBars, VarianceBar{Name: entityName + " . " + bankName, Value: value})
			}
		}
		vbRows.Close()

		donutSlices := []DonutSlice{}
		dRows, err := txn.Query(ctx, `
			SELECT currency_code, COALESCE(SUM(closing_balance),0) AS value
			FROM `+tempAb+`
			GROUP BY currency_code
			ORDER BY value DESC`)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		for dRows.Next() {
			var code string
			var value float64
			if err := dRows.Scan(&code, &value); err == nil {
				donutSlices = append(donutSlices, DonutSlice{ID: code, Label: code, Value: value})
			}
		}
		dRows.Close()

		entityRows := []EntityRow{}
		eRows, err := txn.Query(ctx, `
			SELECT
				b.entity_id,
				mec.entity_name,
				COALESCE(b.bank_name,'') AS bank_name,
				COALESCE(b.currency_code,'INR') AS currency_code,
				COALESCE(SUM(b.opening_balance),0) AS opening,
				COALESCE(SUM(b.closing_balance),0) AS closing
			FROM `+tempAb+` b
			JOIN public.masterentitycash mec ON b.entity_id = mec.entity_id
			GROUP BY b.entity_id, mec.entity_name, b.bank_name, b.currency_code
			ORDER BY mec.entity_name, b.bank_name, b.currency_code`)
		if err == nil {
			for eRows.Next() {
				var id, entityName, bankName, ccy string
				var opening, closing float64
				if err := eRows.Scan(&id, &entityName, &bankName, &ccy, &opening, &closing); err == nil {
					entityRows = append(entityRows, EntityRow{ID: id, Entity: entityName, Bank: bankName, CCY: ccy, Opening: opening, Closing: closing, Delta: closing - opening})
				}
			}
			eRows.Close()
		}

		currencyColors := map[string]string{
			"INR": "#1976d2",
			"USD": "#388e3c",
			"EUR": "#fbc02d",
			"GBP": "#7b1fa2",
			"JPY": "#e64a19",
		}
		topCurrencies := []TopCurrency{}
		tcRows, err := txn.Query(ctx, `
			SELECT currency_code, COALESCE(SUM(closing_balance),0) AS amount
			FROM `+tempAb+`
			GROUP BY currency_code
			ORDER BY amount DESC
			LIMIT 5`)
		if err == nil {
			for tcRows.Next() {
				var code string
				var amount float64
				if err := tcRows.Scan(&code, &amount); err == nil {
					color := currencyColors[code]
					if color == "" {
						color = "#888888"
					}
					topCurrencies = append(topCurrencies, TopCurrency{Code: code, Label: code, Amount: amount, Color: color})
				}
			}
			tcRows.Close()
		}

		// Build new Trends structure
		trends := []Trend{}

		aggSQL := `
			SELECT
				mec.entity_name,
				COALESCE(b.bank_name,'') AS bank_name,
				COALESCE(b.currency_code,'INR') AS currency_code,
				COALESCE(SUM(b.opening_balance),0) AS opening,
				COALESCE(SUM(b.closing_balance),0) AS closing
			FROM ` + tempAb + ` b
			JOIN public.masterentitycash mec ON b.entity_id = mec.entity_id
			GROUP BY mec.entity_name, b.bank_name, b.currency_code
			ORDER BY mec.entity_name, b.bank_name, b.currency_code`

		aggRows, err := txn.Query(ctx, aggSQL)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		trendMap := map[string]int{}

		for aggRows.Next() {
			var entityName, bankName, ccy string
			var opening, closing float64
			if err := aggRows.Scan(&entityName, &bankName, &ccy, &opening, &closing); err != nil {
				continue
			}
			key := entityName + "_" + bankName + "_" + ccy
			trends = append(trends, Trend{
				Label:      entityName + " - " + bankName,
				Value:      closing,
				Delta:      closing - opening,
				Currency:   ccy,
				BankName:   bankName,
				Statements: []AccountStatement{},
			})
			trendMap[key] = len(trends) - 1
		}
		aggRows.Close()

		// Trend lines: read only from temp tx snapshot; join approved rows on entity + account + currency.
		trendTxnSQL := `
			WITH tx_rows AS (
				SELECT
					mec.entity_name,
					COALESCE(ab.bank_name,'') AS bank_name,
					COALESCE(ab.currency_code,'INR') AS currency_code,
					ts.eff_ts AS dt,
					ts.transaction_id,
					COALESCE((ts.run_balance - ts.dep_amt + ts.wd_amt), 0) AS opening_balance,
					ts.dep_amt AS inflow,
					ts.wd_amt AS outflow,
					ts.run_balance AS closing_balance,
					ts.description AS narration,
					CASE WHEN ts.category_id IS NULL THEN 'Uncategorized' ELSE COALESCE(tc.category_name,'') END AS category,
					row_number() OVER (
						PARTITION BY ts.entity_id, ts.account_no, ts.currency_code
						ORDER BY ts.eff_ts DESC, ts.transaction_id DESC
					) AS rn
				FROM ` + tempAb + ` ab
				JOIN public.masterentitycash mec ON ab.entity_id = mec.entity_id
				JOIN ` + tempTx + ` ts ON ts.entity_id = ab.entity_id
					AND ts.account_no = ab.account_no
					AND ts.currency_code = ab.currency_code
				LEFT JOIN public.mastercashflowcategory tc ON ts.category_id = tc.category_id
			)
			SELECT
				entity_name,
				bank_name,
				currency_code,
				dt,
				transaction_id,
				opening_balance,
				inflow,
				outflow,
				closing_balance,
				narration,
				category
			FROM tx_rows
			WHERE rn <= 200
			ORDER BY entity_name, bank_name, currency_code, dt DESC, transaction_id DESC`

		trendTxnRows, err := txn.Query(ctx, trendTxnSQL)
		if err == nil {
			stmtsByKey := map[string][]AccountStatement{}
			for trendTxnRows.Next() {
				var entityName, bankName, ccy, narration, category string
				var dt time.Time
				var txnID int64
				var opening, inflow, outflow, closing float64
				if err := trendTxnRows.Scan(&entityName, &bankName, &ccy, &dt, &txnID, &opening, &inflow, &outflow, &closing, &narration, &category); err != nil {
					continue
				}
				_ = txnID
				key := entityName + "_" + bankName + "_" + ccy
				stmt := AccountStatement{
					Date:           dt.Format(constants.DateFormat),
					OpeningBalance: opening,
					Inflow:         inflow,
					Outflow:        outflow,
					ClosingBalance: closing,
					Narration:      narration,
					Category:       category,
				}
				stmtsByKey[key] = append(stmtsByKey[key], stmt)
			}
			trendTxnRows.Close()

			for key, idx := range trendMap {
				if stmts, ok := stmtsByKey[key]; ok {
					trends[idx].Statements = stmts
				}
			}
		}

		var kpi Kpi
		var dayChangeAbs, yesterdayClosing float64
		kpiRow := txn.QueryRow(ctx, `
			SELECT
				COALESCE((SELECT SUM(closing_balance) FROM `+tempAb+`), 0),
				COALESCE((SELECT SUM(opening_balance) FROM `+tempAb+`), 0),
				COALESCE((SELECT SUM(closing_balance) FROM `+tempAb+`), 0),
				COALESCE((SELECT COUNT(DISTINCT account_no) FROM `+tempAb+`), 0),
				COALESCE((SELECT SUM(dep_amt - wd_amt) FROM `+tempTx+` WHERE eff_date = $1::date), 0),
				COALESCE((
					SELECT SUM(run_balance) FROM (
						SELECT run_balance,
							ROW_NUMBER() OVER (
								PARTITION BY entity_id, acct_key, currency_code
								ORDER BY eff_date DESC, eff_ts DESC, transaction_id DESC
							) AS rn
						FROM `+tempTx+`
						WHERE eff_date < $1::date
					) s WHERE rn = 1
				), 0)
		`, evalAsOn)
		var acctCnt int64
		if err := kpiRow.Scan(&kpi.TotalBalance, &kpi.Opening, &kpi.Closing, &acctCnt, &dayChangeAbs, &yesterdayClosing); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		kpi.Delta = kpi.Closing - kpi.Opening
		kpi.DayChangeAbs = dayChangeAbs
		if yesterdayClosing != 0 {
			kpi.DayChangePct = (dayChangeAbs / yesterdayClosing) * 100
		} else {
			kpi.DayChangePct = 0
		}
		if entity != "" {
			kpi.ActiveEntities = 1
		} else {
			kpi.ActiveEntities = len(allowedEntityIDs)
		}
		kpi.BankAccounts = int(acctCnt)

		// Build simple account-level balances with statements (one level of nesting)
		accountBalances := []AccountBalance{}
		// Key: entity_id + normalized account (same account number can exist under different entities).
		accountIndex := map[string]int{}

		abSQL := `
			SELECT
				b.entity_id,
				mec.entity_name,
				COALESCE(b.bank_name,'') AS bank_name,
				b.account_no,
				COALESCE(b.currency_code,'INR') AS currency_code,
				COALESCE(b.opening_balance,0) AS opening,
				COALESCE(b.closing_balance,0) AS closing
			FROM ` + tempAb + ` b
			JOIN public.masterentitycash mec ON b.entity_id = mec.entity_id
			ORDER BY mec.entity_name, b.bank_name, b.account_no`

		abRows, err := txn.Query(ctx, abSQL)
		if err == nil {
			for abRows.Next() {
				var entityID, entityName, bankName, accountNo, ccy string
				var opening, closing float64
				if err := abRows.Scan(&entityID, &entityName, &bankName, &accountNo, &ccy, &opening, &closing); err != nil {
					continue
				}
				accountBalances = append(accountBalances, AccountBalance{
					EntityID:   entityID,
					EntityName: entityName,
					BankName:   bankName,
					AccountNo:  accountNo,
					Currency:   ccy,
					Balance:    closing,
					Opening:    opening,
					Closing:    closing,
					Delta:      closing - opening,
					Statements: []AccountStatement{},
				})
				// Index for fast lookup - normalize account number
				normalizedAcctNo := normalizeAccountNumber(accountNo)
				if normalizedAcctNo != "" {
					accountIndex[entityID+"\x1f"+normalizedAcctNo] = len(accountBalances) - 1
				}
			}
			abRows.Close()
		}

		stmtSQL2 := `
			SELECT
				ts.entity_id,
				ts.account_no,
				ts.eff_ts AS date,
				COALESCE((ts.run_balance - ts.dep_amt + ts.wd_amt), 0) AS opening_balance,
				ts.dep_amt AS inflow,
				ts.wd_amt AS outflow,
				ts.run_balance AS closing_balance,
				ts.description AS narration,
				CASE WHEN ts.category_id IS NULL THEN 'Uncategorized' ELSE COALESCE(tc.category_name,'') END AS category
			FROM ` + tempTx + ` ts
			JOIN ` + tempAb + ` ab ON ts.entity_id = ab.entity_id
				AND ts.account_no = ab.account_no
				AND ts.currency_code = ab.currency_code
			LEFT JOIN public.mastercashflowcategory tc ON ts.category_id = tc.category_id
			ORDER BY ts.account_no, ts.eff_ts DESC, ts.transaction_id DESC
			LIMIT 5000`

		stmt2Rows, err := txn.Query(ctx, stmtSQL2)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		for stmt2Rows.Next() {
			var entID, accountNo string
			var dt time.Time
			var opening, inflow, outflow, closing float64
			var narration, category string
			if err := stmt2Rows.Scan(&entID, &accountNo, &dt, &opening, &inflow, &outflow, &closing, &narration, &category); err != nil {
				continue
			}

			stmt := AccountStatement{
				Date:           dt.Format(constants.DateFormat),
				OpeningBalance: opening,
				Inflow:         inflow,
				Outflow:        outflow,
				ClosingBalance: closing,
				Narration:      narration,
				Category:       category,
			}

			normalizedAcctNo := normalizeAccountNumber(accountNo)
			if normalizedAcctNo != "" {
				if idx, ok := accountIndex[entID+"\x1f"+normalizedAcctNo]; ok {
					accountBalances[idx].Statements = append(accountBalances[idx].Statements, stmt)
				}
			}
		}
		stmt2Rows.Close()

		if err := txn.Commit(ctx); err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		committed = true

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":         true,
			"data":            kpi,
			"trends":          trends,
			"accountBalances": accountBalances,
			"varianceBars":    varianceBars,
			"donutSlices":     donutSlices,
			"entityRows":      entityRows,
			"topCurrencies":   topCurrencies,
		})
	})
}
