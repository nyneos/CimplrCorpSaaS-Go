package bankbalance

import (
	"CimplrCorpSaas/api"
	"context"
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler: GetCurrencyWiseDashboard
func GetCurrencyWiseDashboard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// Spot rates for conversion
	var spotRates = map[string]float64{
		"USD": 82.5,
		"EUR": 89.5,
		"INR": 1.0,
		// Add more as needed
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse user_id from JSON body
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, "Missing or invalid user_id in body", http.StatusBadRequest)
			return
		}

		// Get allowed business units from context (set by BU middleware)
		// buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		// if !ok || len(buNames) == 0 {
		// 	http.Error(w, "No accessible business units found", http.StatusNotFound)
		// 	return
		// }

		// Query: fetch entity, bank, account number, currency, balance for status=Approved, filtered by allowed BUs
		ctx := context.Background()
		rows, err := pgxPool.Query(ctx, `
		       SELECT 
			       e.entity_name, 
			       b.bank_name, 
			       mba.account_number, 
			       s.currencycode, 
			       SUM(s.closingbalance) AS balance_account_ccy
		       FROM bank_statement s
		       JOIN masterbankaccount mba ON s.account_number = mba.account_number
		       JOIN masterentity e ON mba.entity_id = e.entity_id
		       JOIN masterbank b ON mba.bank_id = b.bank_id
		       WHERE s.status = 'Approved'
		       GROUP BY e.entity_name, b.bank_name, mba.account_number, s.currencycode;
	       `)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Build flat response
		resp := []map[string]interface{}{}
		for rows.Next() {
			var entity, bank, accountNumber, currency string
			var balanceAccountCcy float64
			if err := rows.Scan(&entity, &bank, &accountNumber, &currency, &balanceAccountCcy); err != nil {
				continue
			}
			spot := spotRates[currency]
			if spot == 0 {
				spot = 1.0 // fallback if unknown currency
			}
			equivalentINR := balanceAccountCcy * spot
			resp = append(resp, map[string]interface{}{
				"entity":            entity,
				"bank":              bank,
				"accountNumber":     accountNumber,
				"currency":          currency,
				"balanceAccountCcy": balanceAccountCcy,
				"equivalentINR":     equivalentINR,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// Handler: GetApprovedBankBalances
func GetApprovedBankBalances(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse user_id from JSON body
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, "Missing or invalid user_id in body", http.StatusBadRequest)
			return
		}

		// Get allowed business units from context (set by BU middleware)
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusNotFound)
			return
		}

		// Query: fetch entity, bank, currency, balance for status=Approved, filtered by allowed BUs
		ctx := context.Background()
		rows, err := pgxPool.Query(ctx, `
		       SELECT 
			       e.entity_name, 
			       b.bank_name, 
			       s.currencycode, 
			       SUM(s.closingbalance) AS total_closing_balance
		       FROM bank_statement s
		       JOIN masterbankaccount mba ON s.account_number = mba.account_number
		       JOIN masterentity e ON mba.entity_id = e.entity_id
		       JOIN masterbank b ON mba.bank_id = b.bank_id
		       WHERE s.status = 'Approved' AND e.entity_name = ANY($1)
		       GROUP BY e.entity_name, b.bank_name, s.currencycode;
	       `, buNames)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}
