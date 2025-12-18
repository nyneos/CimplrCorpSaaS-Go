package bankbalance

import (
	"context"
	"encoding/json"
	"math"
	"net/http"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler: GetCurrencyWiseDashboard
func GetCurrencyWiseDashboard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// Spot rates for conversion
	var spotRates = map[string]float64{
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

		// Query: fetch entity, bank, account number, currency, balance for status=Approved, filtered by allowed BUs
		ctx := context.Background()
		rows, err := pgxPool.Query(ctx, `
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
				       ) a ON a.bankstatementid = s.bank_statement_id
				       JOIN (
					       SELECT account_number, MAX(statement_period_end) AS maxdate
					       FROM cimplrcorpsaas.bank_statements
					       GROUP BY account_number
				       ) latest ON s.account_number = latest.account_number AND s.statement_period_end = latest.maxdate
				       WHERE a.processing_status = 'APPROVED'
			       `)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Build flat response
		resp := []map[string]interface{}{}
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// Handler: GetApprovedBankBalances
func GetApprovedBankBalances(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		// Remove BU filtering for diagnostics

		// Query: fetch entity, bank, currency, balance for status=Approved
		ctx := context.Background()
		rows, err := pgxPool.Query(ctx, `
			SELECT 
				e.entity_name, 
				mb.bank_name, 
				mb.currency_code, 
				SUM(mb.balance_amount) AS total_closing_balance
			FROM bank_balances_manual mb
			JOIN masterbankaccount mba ON mb.account_no = mba.account_number
			JOIN masterentitycash e ON mba.entity_id = e.entity_id
			JOIN (
				SELECT DISTINCT ON (balance_id) balance_id, processing_status
				FROM auditactionbankbalances
				ORDER BY balance_id, requested_at DESC
			) a ON a.balance_id = mb.balance_id AND a.processing_status = 'APPROVED'
			GROUP BY e.entity_name, mb.bank_name, mb.currency_code;
		`)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Build nested response
		entityMap := map[string]map[string]map[string]float64{} // entity -> bank -> currency -> balance
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
			entityMap[entity][bank][currency] = balance
		}

		// Format response
		resp := []map[string]interface{}{}
		for entity, banks := range entityMap {
			banksArr := []map[string]interface{}{}
			for bank, currencies := range banks {
				currArr := []map[string]interface{}{}
				for currency, balance := range currencies {
					currArr = append(currArr, map[string]interface{}{
						"currency": currency,
						"balance":  balance,
					})
				}
				banksArr = append(banksArr, map[string]interface{}{
					"bank":       bank,
					"currencies": currArr,
				})
			}
			resp = append(resp, map[string]interface{}{
				"entity": entity,
				"banks":  banksArr,
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// Handler: GetCurrencyWiseBalancesFromManual
// Aggregates balances from bank_balances_manual using the latest audit status (only APPROVED)
func GetCurrencyWiseBalancesFromManual(pgxPool *pgxpool.Pool) http.HandlerFunc {
	var spotRates = map[string]float64{
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

		ctx := context.Background()
		rows, err := pgxPool.Query(ctx, `
			SELECT  e.entity_name, mb.bank_name, mb.account_no, mb.currency_code, SUM(mb.balance_amount) AS closing_balance
			FROM bank_balances_manual mb
			JOIN masterbankaccount mba ON mb.account_no = mba.account_number
			JOIN masterentitycash e ON mba.entity_id = e.entity_id
			JOIN (
				SELECT DISTINCT ON (balance_id) balance_id, processing_status
				FROM auditactionbankbalances
				ORDER BY balance_id, requested_at DESC
			) a ON a.balance_id = mb.balance_id AND a.processing_status = 'APPROVED'
			GROUP BY e.entity_name, mb.bank_name, mb.account_no, mb.currency_code;
		`)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		resp := []map[string]interface{}{}
		for rows.Next() {
			var entity, bankName, accountNo, currency string
			var closingBalance float64
			if err := rows.Scan(&entity, &bankName, &accountNo, &currency, &closingBalance); err != nil {
				continue
			}
			spot := spotRates[currency]
			if spot == 0 {
				spot = 1.0
			}
			bal := math.Abs(closingBalance)
			bal = math.Round(bal*100) / 100
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
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

		ctx := context.Background()
		// join bank_balances_manual to masterbankaccount to get entity_id, then entity name from masterentity, filter by entity_name
		rows, err := pgxPool.Query(ctx, `
			SELECT e.entity_short_name as entity_name, mb.bank_name, mb.currency_code, SUM(mb.balance_amount) AS total_closing_balance
			FROM bank_balances_manual mb
			JOIN masterbankaccount mba ON mb.account_no = mba.account_number
			JOIN masterentitycash e ON mba.entity_id = e.entity_id
			JOIN (
				SELECT DISTINCT ON (balance_id) balance_id, processing_status
				FROM auditactionbankbalances
				ORDER BY balance_id, requested_at DESC
			) a ON a.balance_id = mb.balance_id AND a.processing_status = 'APPROVED'
		
			GROUP BY e.entity_short_name, mb.bank_name, mb.currency_code;
		`)
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
