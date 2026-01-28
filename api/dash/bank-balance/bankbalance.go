package bankbalance

import (
	"CimplrCorpSaas/api"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

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

// Handler: GetCurrencyWiseDashboard
func GetCurrencyWiseDashboard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// Spot rates for conversion (INR stays 1.0 to avoid double converting)
	var spotRates = map[string]float64{
		"USD": 90.0,
		"AUD": 60.5,
		"CAD": 66.8,
		"CHF": 101.0,
		"CNY": 13.42,
		"RMB": 13.42,
		"EUR": 98.0,
		"GBP": 114.0,
		"JPY": 0.60,
		"SEK": 8.6,
		"INR": 1.0,
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		// Parse user_id from JSON body
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}

		// Get allowed business units from context (set by BU middleware)
		// buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		// if !ok || len(buNames) == 0 {
		// 	http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusNotFound)
		// 	return
		// }

		ctx := r.Context()
		allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
		allowedAccountNumbers := ctxApprovedAccountNumbers(ctx)
		allowedBanksNorm := normalizeLowerTrimSlice(api.GetBankNamesFromCtx(ctx))
		allowedCurrenciesNorm := normalizeUpperTrimSlice(api.GetCurrencyCodesFromCtx(ctx))
		if len(allowedEntityIDs) == 0 || len(allowedAccountNumbers) == 0 || len(allowedBanksNorm) == 0 || len(allowedCurrenciesNorm) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}

		// Query: fetch entity, bank, account number, currency, balance for status=Approved, filtered by allowed BUs
		rows, err := pgxPool.Query(ctx, `
			WITH latest_approved AS (
			    SELECT s.account_number,
			           MAX(s.statement_period_end) AS maxdate
			    FROM cimplrcorpsaas.bank_statements s
			    JOIN (
			        SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status, requested_at
			        FROM cimplrcorpsaas.auditactionbankstatement
			        ORDER BY bankstatementid, requested_at DESC
			    ) a ON a.bankstatementid = s.bank_statement_id
			    WHERE a.processing_status = 'APPROVED'
			    GROUP BY s.account_number
			)
			SELECT 
			       e.entity_name, 
			       b.bank_name, 
			       mba.account_number, 
			       mba.currencycode, 
			       s.opening_balance,
			       s.closing_balance
			FROM cimplrcorpsaas.bank_statements s
			JOIN masterbankaccount mba ON s.account_number = mba.account_number
			JOIN masterentitycash e ON mba.entity_id = e.entity_id
			JOIN masterbank b ON mba.bank_id = b.bank_id
			JOIN (
			    SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
			    FROM cimplrcorpsaas.auditactionbankstatement
			    ORDER BY bankstatementid, requested_at DESC
			) a ON a.bankstatementid = s.bank_statement_id AND a.processing_status = 'APPROVED'
			JOIN latest_approved la ON la.account_number = s.account_number AND la.maxdate = s.statement_period_end
			WHERE mba.is_deleted = false
			  AND mba.entity_id = ANY($1)
			  AND mba.account_number = ANY($2)
			  AND lower(trim(b.bank_name)) = ANY($3)
			  AND upper(trim(mba.currencycode)) = ANY($4)
		`, allowedEntityIDs, allowedAccountNumbers, allowedBanksNorm, allowedCurrenciesNorm)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Build flat response
		resp := []map[string]interface{}{}
		dayWise := []map[string]interface{}{}
		for rows.Next() {
			var entity, bank, accountNumber, currency string
			var openingBalance, closingBalance float64
			if err := rows.Scan(&entity, &bank, &accountNumber, &currency, &openingBalance, &closingBalance); err != nil {
				continue
			}
			spot := spotRates[currency]
			if spot == 0 {
				spot = 1.0 // fallback if unknown currency
			}
			resp = append(resp, map[string]interface{}{
				"entity":            entity,
				"bank":              bank,
				"accountNumber":     accountNumber,
				"currency":          currency,
				"openingBalance":    openingBalance,
				"closingBalance":    closingBalance,
				"openingBalanceINR": openingBalance * spot,
				"closingBalanceINR": closingBalance * spot,
			})
		}

		// Day-wise balances (per statement end date, per account)
		dayRows, err := pgxPool.Query(ctx, `
			WITH approved AS (
			    SELECT s.statement_period_end AS day,
			           e.entity_name,
			           b.bank_name,
			           mba.account_number,
			           mba.currencycode,
			           s.closing_balance
			    FROM cimplrcorpsaas.bank_statements s
			    JOIN masterbankaccount mba ON s.account_number = mba.account_number
			    JOIN masterentitycash e ON mba.entity_id = e.entity_id
			    JOIN masterbank b ON mba.bank_id = b.bank_id
			    JOIN (
			        SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
			        FROM cimplrcorpsaas.auditactionbankstatement
			        ORDER BY bankstatementid, requested_at DESC
			    ) a ON a.bankstatementid = s.bank_statement_id AND a.processing_status = 'APPROVED'
			    WHERE mba.is_deleted = false
			      AND mba.entity_id = ANY($1)
			      AND mba.account_number = ANY($2)
			      AND lower(trim(b.bank_name)) = ANY($3)
			      AND upper(trim(mba.currencycode)) = ANY($4)
			)
			SELECT day, entity_name, bank_name, account_number, currencycode, closing_balance
			FROM approved
			ORDER BY day DESC, entity_name, bank_name, account_number;
		`, allowedEntityIDs, allowedAccountNumbers, allowedBanksNorm, allowedCurrenciesNorm)
		if err == nil {
			for dayRows.Next() {
				var day time.Time
				var currency, entityName, bankName, accountNo string
				var closing float64
				if err := dayRows.Scan(&day, &entityName, &bankName, &accountNo, &currency, &closing); err != nil {
					continue
				}
				spot := spotRates[currency]
				if spot == 0 {
					spot = 1.0
				}
				dayWise = append(dayWise, map[string]interface{}{
					"date":              day.Format(constants.DateFormat),
					"entity":            entityName,
					"bank":              bankName,
					"accountNumber":     accountNo,
					"currency":          currency,
					"closingBalance":    closing,
					"closingBalanceINR": closing * spot,
				})
			}
			dayRows.Close()
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"balances":        resp,
			"dayWiseBalances": dayWise,
		})
	}
}

func GetApprovedBankBalances(pgxPool *pgxpool.Pool) http.HandlerFunc {

	// Spot rates for conversion (INR = 1.0 baseline)
	var spotRates = map[string]float64{
		"USD": 90.0,
		"AUD": 60.5,
		"CAD": 66.8,
		"CHF": 101.0,
		"CNY": 13.42,
		"RMB": 13.42,
		"EUR": 98.0,
		"GBP": 114.0,
		"JPY": 0.60,
		"SEK": 8.6,
		"INR": 1.0,
	}

	return func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		// Parse body
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
		allowedAccountNumbers := ctxApprovedAccountNumbers(ctx)
		allowedBanksNorm := normalizeLowerTrimSlice(api.GetBankNamesFromCtx(ctx))
		allowedCurrenciesNorm := normalizeUpperTrimSlice(api.GetCurrencyCodesFromCtx(ctx))
		if len(allowedEntityIDs) == 0 || len(allowedAccountNumbers) == 0 || len(allowedBanksNorm) == 0 || len(allowedCurrenciesNorm) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}

		/* ---------------------------------------------------------
		   1️⃣ LATEST APPROVED BALANCE PER ACCOUNT
		----------------------------------------------------------*/

		rows, err := pgxPool.Query(ctx, `
			WITH latest_approved_balance AS (
				SELECT DISTINCT ON (balance_id)
					   balance_id,
					   processing_status
				FROM public.auditactionbankbalances
				ORDER BY balance_id, requested_at DESC
			)
			SELECT
				e.entity_name,
				b.bank_name,
				bbm.account_no,
				bbm.currency_code,
				bbm.closing_balance
			FROM public.bank_balances_manual bbm
			JOIN latest_approved_balance lab
			     ON lab.balance_id = bbm.balance_id
			    AND lab.processing_status = 'APPROVED'
			JOIN masterbankaccount mba ON bbm.account_no = mba.account_number
			JOIN masterentitycash e ON mba.entity_id = e.entity_id
			JOIN masterbank b ON mba.bank_id = b.bank_id
			WHERE mba.is_deleted = false
			  AND mba.entity_id = ANY($1)
			  AND bbm.account_no = ANY($2)
			  AND lower(trim(b.bank_name)) = ANY($3)
			  AND upper(trim(COALESCE(bbm.currency_code,''))) = ANY($4)
			ORDER BY e.entity_name, b.bank_name, bbm.account_no;
		`, allowedEntityIDs, allowedAccountNumbers, allowedBanksNorm, allowedCurrenciesNorm)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// entity -> bank -> accounts
		entityMap := map[string]map[string][]map[string]interface{}{}

		for rows.Next() {
			var entity, bank, accountNo, currency string
			var closing float64

			if err := rows.Scan(&entity, &bank, &accountNo, &currency, &closing); err != nil {
				continue
			}

			spot := spotRates[currency]
			if spot == 0 {
				spot = 1.0
			}

			if _, ok := entityMap[entity]; !ok {
				entityMap[entity] = map[string][]map[string]interface{}{}
			}

			entityMap[entity][bank] = append(
				entityMap[entity][bank],
				map[string]interface{}{
					"accountNumber":     accountNo,
					"currency":          currency,
					"closingBalance":    closing,
					"closingBalanceINR": closing * spot,
				},
			)
		}

		/* ---------------------------------------------------------
		   2️⃣ DAY-WISE APPROVED BALANCE HISTORY
		----------------------------------------------------------*/

		dayWise := []map[string]interface{}{}

		dayRows, err := pgxPool.Query(ctx, `
			WITH latest_approved_action AS (
				SELECT DISTINCT ON (balance_id)
					balance_id,
					requested_at
				FROM public.auditactionbankbalances
				WHERE processing_status = 'APPROVED'
				ORDER BY balance_id, requested_at DESC
			),
			rows_scoped AS (
				SELECT
					COALESCE(bbm.as_of_date, laa.requested_at::date) AS day,
					e.entity_name,
					b.bank_name,
					bbm.account_no,
					bbm.currency_code,
					bbm.closing_balance,
					bbm.balance_id
				FROM public.bank_balances_manual bbm
				JOIN latest_approved_action laa
					ON laa.balance_id = bbm.balance_id
				JOIN masterbankaccount mba ON bbm.account_no = mba.account_number
				JOIN masterentitycash e ON mba.entity_id = e.entity_id
				JOIN masterbank b ON mba.bank_id = b.bank_id
				WHERE mba.is_deleted = false
					AND mba.entity_id = ANY($1)
					AND bbm.account_no = ANY($2)
					AND lower(trim(b.bank_name)) = ANY($3)
					AND upper(trim(COALESCE(bbm.currency_code,''))) = ANY($4)
			)
			SELECT DISTINCT ON (account_no, day)
				day,
				entity_name,
				bank_name,
				account_no,
				currency_code,
				closing_balance
			FROM rows_scoped
			ORDER BY account_no, day DESC, balance_id DESC;
		`, allowedEntityIDs, allowedAccountNumbers, allowedBanksNorm, allowedCurrenciesNorm)
		if err == nil {
			for dayRows.Next() {
				var day time.Time
				var entity, bank, accountNo, currency string
				var closing float64

				if err := dayRows.Scan(
					&day,
					&entity,
					&bank,
					&accountNo,
					&currency,
					&closing,
				); err != nil {
					continue
				}

				spot := spotRates[currency]
				if spot == 0 {
					spot = 1.0
				}

				dayWise = append(dayWise, map[string]interface{}{
					"date":              day.Format(constants.DateFormat),
					"entity":            entity,
					"bank":              bank,
					"accountNumber":     accountNo,
					"currency":          currency,
					"closingBalance":    closing,
					"closingBalanceINR": closing * spot,
				})
			}
			dayRows.Close()
		}

		/* ---------------------------------------------------------
		   3️⃣ RESPONSE FORMAT
		----------------------------------------------------------*/

		resp := []map[string]interface{}{}

		for entity, banks := range entityMap {
			banksArr := []map[string]interface{}{}
			for bank, accounts := range banks {
				banksArr = append(banksArr, map[string]interface{}{
					"bank":     bank,
					"accounts": accounts,
				})
			}
			resp = append(resp, map[string]interface{}{
				"entity": entity,
				"banks":  banksArr,
			})
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"balances":        resp,
			"dayWiseBalances": dayWise,
		})
	}
}

// Handler: GetCurrencyWiseBalancesFromManual
// Aggregates balances from bank_balances_manual using the latest audit status (only APPROVED)
func GetCurrencyWiseBalancesFromManual(pgxPool *pgxpool.Pool) http.HandlerFunc {

	var spotRates = map[string]float64{
		"USD": 90.0,
		"AUD": 60.5,
		"CAD": 66.8,
		"CHF": 101.0,
		"CNY": 13.42,
		"RMB": 13.42,
		"EUR": 98.0,
		"GBP": 114.0,
		"JPY": 0.60,
		"SEK": 8.6,
		"INR": 1.0,
	}

	return func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
		allowedAccountNumbers := ctxApprovedAccountNumbers(ctx)
		allowedBanksNorm := normalizeLowerTrimSlice(api.GetBankNamesFromCtx(ctx))
		allowedCurrenciesNorm := normalizeUpperTrimSlice(api.GetCurrencyCodesFromCtx(ctx))
		if len(allowedEntityIDs) == 0 || len(allowedAccountNumbers) == 0 || len(allowedBanksNorm) == 0 || len(allowedCurrenciesNorm) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}

		/* ---------------------------------------------------------
		   1️⃣ LATEST APPROVED BALANCE PER ACCOUNT
		----------------------------------------------------------*/

		rows, err := pgxPool.Query(ctx, `
			WITH latest_audit AS (
			    SELECT DISTINCT ON (balance_id)
			           balance_id,
			           processing_status
			    FROM auditactionbankbalances
			    ORDER BY balance_id, requested_at DESC
			),
			approved AS (
			    SELECT mb.*,
			           COALESCE(mb.as_of_date, CURRENT_DATE) AS dt
			    FROM bank_balances_manual mb
			    JOIN latest_audit la
			      ON la.balance_id = mb.balance_id
			     AND la.processing_status = 'APPROVED'
			),
			latest_per_account AS (
			    SELECT DISTINCT ON (mb.account_no)
			           COALESCE(e.entity_name, '') AS entity_name,
			           COALESCE(mb.bank_name, b.bank_name, '') AS bank_name,
			           mb.account_no,
			           mb.currency_code,
			           mb.balance_amount,
			           mb.dt
			    FROM approved mb
			    JOIN masterbankaccount mba ON mb.account_no = mba.account_number
			    LEFT JOIN masterentitycash e ON mba.entity_id = e.entity_id
			    LEFT JOIN masterbank b ON mba.bank_id = b.bank_id
			    WHERE mba.entity_id = ANY($1)
			      AND mb.account_no = ANY($2)
			      AND lower(trim(COALESCE(mb.bank_name, b.bank_name, ''))) = ANY($3)
			      AND upper(trim(COALESCE(mb.currency_code, ''))) = ANY($4)
			    ORDER BY mb.account_no, mb.dt DESC, mb.balance_id DESC
			)
			SELECT entity_name, bank_name, account_no, currency_code, balance_amount
			FROM latest_per_account;
		`, allowedEntityIDs, allowedAccountNumbers, allowedBanksNorm, allowedCurrenciesNorm)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		resp := []map[string]interface{}{}

		for rows.Next() {
			var entity, bankName, accountNo, currency string
			var closing float64

			if err := rows.Scan(&entity, &bankName, &accountNo, &currency, &closing); err != nil {
				continue
			}

			spot := spotRates[currency]
			if spot == 0 {
				spot = 1.0
			}

			bal := math.Round(math.Abs(closing)*100) / 100
			inr := math.Round((bal*spot)*100) / 100

			resp = append(resp, map[string]interface{}{
				"entity":            entity,
				"bank":              bankName,
				"accountNumber":     accountNo,
				"currency":          currency,
				"closingBalance":    bal,
				"closingBalanceINR": inr,
			})
		}

		/* ---------------------------------------------------------
		   2️⃣ DAY-WISE APPROVED BALANCE HISTORY
		----------------------------------------------------------*/

		dayWise := []map[string]interface{}{}

		dayRows, err := pgxPool.Query(ctx, `
			WITH latest_audit AS (
			    SELECT DISTINCT ON (balance_id)
			           balance_id,
			           processing_status
			    FROM auditactionbankbalances
			    ORDER BY balance_id, requested_at DESC
			),
			approved AS (
			    SELECT mb.*,
			           COALESCE(mb.as_of_date, CURRENT_DATE) AS dt
			    FROM bank_balances_manual mb
			    JOIN latest_audit la
			      ON la.balance_id = mb.balance_id
			     AND la.processing_status = 'APPROVED'
			)
			SELECT
			    COALESCE(e.entity_name, '') AS entity_name,
			    COALESCE(mb.bank_name, b.bank_name, '') AS bank_name,
			    mb.account_no,
			    mb.currency_code,
			    mb.balance_amount,
			    mb.dt
			FROM approved mb
			JOIN masterbankaccount mba ON mb.account_no = mba.account_number AND mba.is_deleted = false
			LEFT JOIN masterentitycash e ON mba.entity_id = e.entity_id
			LEFT JOIN masterbank b ON mba.bank_id = b.bank_id
			WHERE mba.entity_id = ANY($1)
			  AND mb.account_no = ANY($2)
			  AND lower(trim(COALESCE(mb.bank_name, b.bank_name, ''))) = ANY($3)
			  AND upper(trim(COALESCE(mb.currency_code, ''))) = ANY($4)
			ORDER BY mb.dt DESC, entity_name, bank_name, mb.account_no;
		`, allowedEntityIDs, allowedAccountNumbers, allowedBanksNorm, allowedCurrenciesNorm)
		if err == nil {
			for dayRows.Next() {
				var entity, bank, accountNo, currency string
				var closing float64
				var day time.Time

				if err := dayRows.Scan(
					&entity,
					&bank,
					&accountNo,
					&currency,
					&closing,
					&day,
				); err != nil {
					continue
				}

				spot := spotRates[currency]
				if spot == 0 {
					spot = 1.0
				}

				bal := math.Round(math.Abs(closing)*100) / 100
				inr := math.Round((bal*spot)*100) / 100

				dayWise = append(dayWise, map[string]interface{}{
					"date":              day.Format(constants.DateFormat),
					"entity":            entity,
					"bank":              bank,
					"accountNumber":     accountNo,
					"currency":          currency,
					"closingBalance":    bal,
					"closingBalanceINR": inr,
				})
			}
			dayRows.Close()
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"balances":        resp,
			"dayWiseBalances": dayWise,
		})
	}
}

// Handler: GetApprovedBalancesFromManual
// Aggregates approved balances filtered by allowed business units (entity names) - resolves entity from masterbankaccount.entity_id
func GetApprovedBalancesFromManual(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}

		// buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		// if !ok || len(buNames) == 0 {
		// 	http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusNotFound)
		// 	return
		// }

		ctx := r.Context()
		allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
		allowedAccountNumbers := ctxApprovedAccountNumbers(ctx)
		allowedBanksNorm := normalizeLowerTrimSlice(api.GetBankNamesFromCtx(ctx))
		allowedCurrenciesNorm := normalizeUpperTrimSlice(api.GetCurrencyCodesFromCtx(ctx))
		if len(allowedEntityIDs) == 0 || len(allowedAccountNumbers) == 0 || len(allowedBanksNorm) == 0 || len(allowedCurrenciesNorm) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}

		// join bank_balances_manual to masterbankaccount to get entity_id, then entity name from masterentity, filter by entity_name
		rows, err := pgxPool.Query(ctx, `
			SELECT e.entity_short_name as entity_name, mb.bank_name, mb.currency_code, SUM(mb.balance_amount) AS total_closing_balance
			FROM bank_balances_manual mb
			JOIN masterbankaccount mba ON mb.account_no = mba.account_number
			JOIN masterentitycash e ON mba.entity_id = e.entity_id
			JOIN masterbank b ON mba.bank_id = b.bank_id
			JOIN (
				SELECT DISTINCT ON (balance_id) balance_id, processing_status
				FROM auditactionbankbalances
				ORDER BY balance_id, requested_at DESC
			) a ON a.balance_id = mb.balance_id AND a.processing_status = 'APPROVED'
			WHERE mba.is_deleted = false
			  AND mba.entity_id = ANY($1)
			  AND mb.account_no = ANY($2)
			  AND lower(trim(COALESCE(mb.bank_name, b.bank_name, ''))) = ANY($3)
			  AND upper(trim(COALESCE(mb.currency_code, ''))) = ANY($4)
			GROUP BY e.entity_short_name, mb.bank_name, mb.currency_code;
		`, allowedEntityIDs, allowedAccountNumbers, allowedBanksNorm, allowedCurrenciesNorm)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		entityMap := map[string]map[string]map[string]float64{}
		for rows.Next() {
			var entity, bank, currency string
			var balance float64
			if err := rows.Scan(&entity, &bank, &currency, &balance); err != nil {
				continue
			}
			if _, ok := entityMap[entity]; !ok {
				entityMap[entity] = map[string]map[string]float64{}
			}
			if _, ok := entityMap[entity][bank]; !ok {
				entityMap[entity][bank] = map[string]float64{}
			}
			// normalize stored balance
			b := math.Abs(balance)
			b = math.Round(b*100) / 100
			entityMap[entity][bank][currency] = b
		}

		resp := []map[string]interface{}{}
		for entity, banks := range entityMap {
			banksArr := []map[string]interface{}{}
			for bank, currencies := range banks {
				currArr := []map[string]interface{}{}
				for currency, balance := range currencies {
					currArr = append(currArr, map[string]interface{}{"currency": currency, "balance": balance})
				}
				banksArr = append(banksArr, map[string]interface{}{"bank": bank, "currencies": currArr})
			}
			resp = append(resp, map[string]interface{}{"entity": entity, "banks": banksArr})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}
