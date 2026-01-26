package realtimebalances

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
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

func GetKpiHandler(db *sql.DB) http.Handler {
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

		entity := strings.TrimSpace(r.URL.Query().Get("entity"))
		bank := strings.TrimSpace(r.URL.Query().Get("bank"))
		currency := strings.TrimSpace(r.URL.Query().Get("currency"))
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
		bankNorm := strings.ToLower(strings.TrimSpace(bank))
		currencyNorm := strings.ToUpper(strings.TrimSpace(currency))

		approvedBalancesCTETmpl := `
			WITH approved_balances AS (
				SELECT b.*, mba.entity_id
				FROM bank_balances_manual b
				JOIN LATERAL (
					SELECT aa.*
					FROM auditactionbankbalances aa
					WHERE aa.balance_id = b.balance_id
					ORDER BY aa.requested_at DESC
					LIMIT 1
				) latest_audit ON latest_audit.processing_status = 'APPROVED'
				JOIN public.masterbankaccount mba
					ON b.account_no = mba.account_number
					AND mba.is_deleted = false
				WHERE b.account_no = ANY($1)
					AND mba.entity_id = ANY($2)
					AND lower(trim(COALESCE(b.bank_name, ''))) = ANY($3)
					AND upper(trim(COALESCE(b.currency_code, ''))) = ANY($4)
					%s
			)
		`

		extraFilters := make([]string, 0, 3)
		args := []interface{}{pq.Array(allowedAccountNumbers), pq.Array(allowedEntityIDs), pq.Array(allowedBanksNorm), pq.Array(allowedCurrenciesNorm)}
		argIdx := 5
		if entity != "" {
			extraFilters = append(extraFilters, "mba.entity_id = $"+strconv.Itoa(argIdx))
			args = append(args, entity)
			argIdx++
		}
		if bankNorm != "" {
			extraFilters = append(extraFilters, "lower(trim(COALESCE(b.bank_name, ''))) = $"+strconv.Itoa(argIdx))
			args = append(args, bankNorm)
			argIdx++
		}
		if currencyNorm != "" {
			extraFilters = append(extraFilters, "upper(trim(COALESCE(b.currency_code, ''))) = $"+strconv.Itoa(argIdx))
			args = append(args, currencyNorm)
			argIdx++
		}
		extraWhere := ""
		if len(extraFilters) > 0 {
			extraWhere = " AND " + strings.Join(extraFilters, " AND ")
		}
		approvedBalancesCTE := fmt.Sprintf(approvedBalancesCTETmpl, extraWhere)

		// Build old-style response structures for backward compatibility
		varianceBars := []VarianceBar{}
		vbRows, err := db.QueryContext(ctx, approvedBalancesCTE+`
			SELECT mec.entity_name, COALESCE(b.bank_name,'') AS bank_name, COALESCE(SUM(b.closing_balance),0) as value
			FROM approved_balances b
			JOIN public.masterentitycash mec ON b.entity_id = mec.entity_id
			GROUP BY mec.entity_name, bank_name
			ORDER BY value DESC`, args...)
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
		dRows, err := db.QueryContext(ctx, approvedBalancesCTE+`
			SELECT currency_code, COALESCE(SUM(closing_balance),0) as value
			FROM approved_balances
			GROUP BY currency_code
			ORDER BY value DESC`, args...)
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
		eRows, err := db.QueryContext(ctx, approvedBalancesCTE+`
			SELECT
				b.entity_id,
				mec.entity_name,
				COALESCE(b.bank_name,'') AS bank_name,
				COALESCE(b.currency_code,'INR') AS currency_code,
				COALESCE(SUM(b.opening_balance),0) AS opening,
				COALESCE(SUM(b.closing_balance),0) AS closing
			FROM approved_balances b
			JOIN public.masterentitycash mec ON b.entity_id = mec.entity_id
			GROUP BY b.entity_id, mec.entity_name, bank_name, currency_code
			ORDER BY mec.entity_name, bank_name, currency_code`, args...)
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
		tcRows, err := db.QueryContext(ctx, approvedBalancesCTE+`
			SELECT currency_code, COALESCE(SUM(closing_balance),0) as amount
			FROM approved_balances
			GROUP BY currency_code
			ORDER BY amount DESC
			LIMIT 5`, args...)
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

		aggSQL := approvedBalancesCTE + `
			SELECT
				mec.entity_name,
				COALESCE(b.bank_name,'') AS bank_name,
				COALESCE(b.currency_code,'INR') AS currency_code,
				COALESCE(SUM(b.opening_balance),0) AS opening,
				COALESCE(SUM(b.closing_balance),0) AS closing
			FROM approved_balances b
			JOIN public.masterentitycash mec ON b.entity_id = mec.entity_id
			GROUP BY mec.entity_name, bank_name, currency_code
			ORDER BY mec.entity_name, bank_name, currency_code`

		aggRows, err := db.QueryContext(ctx, aggSQL, args...)
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

		// Attach transaction-level rows (like categorywise) to each trend.
		trendTxnSQL := approvedBalancesCTE + `
			, tx_rows AS (
				SELECT
					mec.entity_name,
					COALESCE(ab.bank_name,'') AS bank_name,
					COALESCE(ab.currency_code,'INR') AS currency_code,
					COALESCE(tx.transaction_date, tx.value_date) AS dt,
					tx.transaction_id,
					COALESCE((tx.balance - COALESCE(tx.deposit_amount,0) + COALESCE(tx.withdrawal_amount,0)),0) AS opening_balance,
					COALESCE(tx.deposit_amount,0) AS inflow,
					COALESCE(tx.withdrawal_amount,0) AS outflow,
					COALESCE(tx.balance,0) AS closing_balance,
					COALESCE(tx.description,'') AS narration,
					CASE WHEN tx.category_id IS NULL THEN 'Uncategorized' ELSE COALESCE(tc.category_name,'') END AS category,
					row_number() OVER (
						PARTITION BY mec.entity_name, COALESCE(ab.bank_name,''), COALESCE(ab.currency_code,'INR')
						ORDER BY COALESCE(tx.transaction_date, tx.value_date) DESC, tx.transaction_id DESC
					) AS rn
				FROM approved_balances ab
				JOIN public.masterentitycash mec
					ON ab.entity_id = mec.entity_id
				JOIN cimplrcorpsaas.bank_statements bs
					ON bs.entity_id = ab.entity_id
					AND COALESCE(NULLIF(ltrim(trim(bs.account_number), '0'), ''), '0') = COALESCE(NULLIF(ltrim(trim(ab.account_no), '0'), ''), '0')
				JOIN (
					SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
					FROM cimplrcorpsaas.auditactionbankstatement
					ORDER BY bankstatementid, requested_at DESC
				) a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
				JOIN cimplrcorpsaas.bank_statement_transactions tx
					ON tx.bank_statement_id = bs.bank_statement_id
				LEFT JOIN cimplrcorpsaas.transaction_categories tc
					ON tx.category_id = tc.category_id
			)
			SELECT
				entity_name,
				bank_name,
				currency_code,
				dt,
				opening_balance,
				inflow,
				outflow,
				closing_balance,
				narration,
				category
			FROM tx_rows
			WHERE rn <= 200
			ORDER BY entity_name, bank_name, currency_code, dt DESC, transaction_id DESC`

		trendTxnRows, err := db.QueryContext(ctx, trendTxnSQL, args...)
		if err == nil {
			stmtsByKey := map[string][]AccountStatement{}
			for trendTxnRows.Next() {
				var entityName, bankName, ccy, narration, category string
				var dt time.Time
				var opening, inflow, outflow, closing float64
				if err := trendTxnRows.Scan(&entityName, &bankName, &ccy, &dt, &opening, &inflow, &outflow, &closing, &narration, &category); err != nil {
					continue
				}
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
		row := db.QueryRowContext(ctx, approvedBalancesCTE+`SELECT COALESCE(SUM(closing_balance),0) FROM approved_balances`, args...)
		_ = row.Scan(&kpi.TotalBalance)
		row = db.QueryRowContext(ctx, approvedBalancesCTE+`SELECT COALESCE(SUM(opening_balance),0), COALESCE(SUM(closing_balance),0) FROM approved_balances`, args...)
		_ = row.Scan(&kpi.Opening, &kpi.Closing)
		kpi.Delta = kpi.Closing - kpi.Opening
		// Day change based on today's net transactions vs yesterday's closing balance.
		var dayChangeAbs, yesterdayClosing float64
		dayChangeSQL := approvedBalancesCTE + `
			, yesterday AS (
				SELECT SUM(closing_balance) AS closing
				FROM approved_balances
				WHERE as_of_date = CURRENT_DATE - INTERVAL '1 day'
			),
			today_tx AS (
				SELECT
					COALESCE(SUM(tx.deposit_amount), 0) AS inflow,
					COALESCE(SUM(tx.withdrawal_amount), 0) AS outflow
				FROM cimplrcorpsaas.bank_statements bs
				JOIN approved_balances ab
					ON COALESCE(NULLIF(ltrim(trim(bs.account_number),'0'),''),'0') = COALESCE(NULLIF(ltrim(trim(ab.account_no),'0'),''),'0')
				JOIN cimplrcorpsaas.bank_statement_transactions tx
					ON tx.bank_statement_id = bs.bank_statement_id
				JOIN (
					SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
					FROM cimplrcorpsaas.auditactionbankstatement
					ORDER BY bankstatementid, requested_at DESC
				) a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
				WHERE COALESCE(tx.transaction_date, tx.value_date) = CURRENT_DATE
			)
			SELECT
				COALESCE(t.inflow, 0) - COALESCE(t.outflow, 0) AS day_change_abs,
				COALESCE(y.closing, 0) AS yesterday_closing
			FROM (SELECT 1) dummy
			LEFT JOIN today_tx t ON true
			LEFT JOIN yesterday y ON true
		`
		if err := db.QueryRowContext(ctx, dayChangeSQL, args...).Scan(&dayChangeAbs, &yesterdayClosing); err == nil {
			kpi.DayChangeAbs = dayChangeAbs
			if yesterdayClosing > 0 {
				kpi.DayChangePct = (dayChangeAbs / yesterdayClosing) * 100
			} else {
				kpi.DayChangePct = 0
			}
		}
		if entity != "" {
			kpi.ActiveEntities = 1
		} else {
			kpi.ActiveEntities = len(allowedEntityIDs)
		}
		row = db.QueryRowContext(ctx, approvedBalancesCTE+`SELECT COUNT(DISTINCT account_no) FROM approved_balances`, args...)
		_ = row.Scan(&kpi.BankAccounts)

		// Build simple account-level balances with statements (one level of nesting)
		accountBalances := []AccountBalance{}
		accountIndex := map[string]int{} // For O(1) statement attachment

		abSQL := approvedBalancesCTE + `
			SELECT
				b.entity_id,
				mec.entity_name,
				COALESCE(b.bank_name,'') AS bank_name,
				b.account_no,
				COALESCE(b.currency_code,'INR') AS currency_code,
				COALESCE(b.opening_balance,0) AS opening,
				COALESCE(b.closing_balance,0) AS closing
			FROM approved_balances b
			JOIN public.masterentitycash mec ON b.entity_id = mec.entity_id
			ORDER BY mec.entity_name, bank_name, b.account_no`

		abRows, err := db.QueryContext(ctx, abSQL, args...)
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
					accountIndex[normalizedAcctNo] = len(accountBalances) - 1
				}
			}
			abRows.Close()
		}

		// Attach daily statements to accounts - drive from the SAME approved_balances set
		stmtSQL2 := approvedBalancesCTE + `
			SELECT
				bs.account_number,
				COALESCE(tx.transaction_date, tx.value_date) AS date,
				COALESCE((tx.balance - COALESCE(tx.deposit_amount,0) + COALESCE(tx.withdrawal_amount,0)),0) AS opening_balance,
				COALESCE(tx.deposit_amount,0) AS inflow,
				COALESCE(tx.withdrawal_amount,0) AS outflow,
				COALESCE(tx.balance,0) AS closing_balance,
				COALESCE(tx.description,'') AS narration,
				CASE WHEN tx.category_id IS NULL THEN 'Uncategorized' ELSE COALESCE(tc.category_name,'') END AS category
			FROM cimplrcorpsaas.bank_statements bs
			JOIN approved_balances ab
				ON COALESCE(NULLIF(ltrim(trim(bs.account_number), '0'), ''), '0') = COALESCE(NULLIF(ltrim(trim(ab.account_no), '0'), ''), '0')
			JOIN cimplrcorpsaas.bank_statement_transactions tx
				ON tx.bank_statement_id = bs.bank_statement_id
			LEFT JOIN cimplrcorpsaas.transaction_categories tc
				ON tx.category_id = tc.category_id
			JOIN (
				SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
				FROM cimplrcorpsaas.auditactionbankstatement
				ORDER BY bankstatementid, requested_at DESC
			) a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
			ORDER BY bs.account_number, COALESCE(tx.transaction_date, tx.value_date) DESC, tx.transaction_id DESC
			LIMIT 5000`

		stmt2Rows, err := db.QueryContext(ctx, stmtSQL2, args...)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		for stmt2Rows.Next() {
			var accountNo string
			var dt time.Time
			var opening, inflow, outflow, closing float64
			var narration, category string
			if err := stmt2Rows.Scan(&accountNo, &dt, &opening, &inflow, &outflow, &closing, &narration, &category); err != nil {
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
			if idx, ok := accountIndex[normalizedAcctNo]; ok {
				accountBalances[idx].Statements = append(accountBalances[idx].Statements, stmt)
			}
		}
		stmt2Rows.Close()

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
