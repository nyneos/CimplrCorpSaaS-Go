package liqsnap

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	// "github.com/jackc/pgx/v5"
)

// Handler for /dash/liquidity/total-cash-balance-by-entity
type UserRequest struct {
	UserID string `json:"user_id"`
}

func TotalCashBalanceByEntityHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		// You can use req.UserID for filtering if needed
		balances, err := TotalCashBalanceByEntity(pgxPool)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(balances)
	}
}

// Handler for /dash/liquidity/liquidity-coverage-ratio
func LiquidityCoverageRatioHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		// You can use req.UserID for filtering if needed
		ratio, err := LiquidityCoverageRatio(pgxPool)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]float64{"liquidity_coverage_ratio": ratio})
	}
}

// Handler for /dash/liquidity/entity-currency-wise-cash
func EntityCurrencyWiseCashHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		// You can use req.UserID for filtering if needed
		data, err := entitycurrencywiseCash(pgxPool)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

type EntityCurrencyBalance struct {
	TotalBalance float64 `json:"total_balance"`
	CurrencyCode string  `json:"currency_code"`
	EntityName   string  `json:"entity_name"`
}

func TotalCashBalanceByEntity(pgxPool *pgxpool.Pool) ([]EntityCurrencyBalance, error) {
	var results []EntityCurrencyBalance
	query := `SELECT 
		COALESCE(SUM(bs.closingbalance), 0) AS total_balance,
		bs.currencycode,
		m.entity_name
	FROM bank_statement bs
	JOIN masterentity m ON bs.entityid = m.entity_id
	WHERE bs.status = 'Approved'
	GROUP BY bs.currencycode, m.entity_name;`
	rows, err := pgxPool.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var rec EntityCurrencyBalance
		if err := rows.Scan(&rec.TotalBalance, &rec.CurrencyCode, &rec.EntityName); err != nil {
			return nil, err
		}
		results = append(results, rec)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return results, nil
}

func LiquidityCoverageRatio(pgxPool *pgxpool.Pool) (float64, error) {
	var inflows, outflows float64

	inflowQuery := `SELECT COALESCE(SUM(expected_amount), 0)
        FROM cashflow_proposal_item
        WHERE cashflow_type = 'Inflow'
        AND start_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '14 days';`
	err := pgxPool.QueryRow(context.Background(), inflowQuery).Scan(&inflows)
	if err != nil {
		return 0, err
	}

	outflowQuery := `SELECT COALESCE(SUM(expected_amount), 0)
        FROM cashflow_proposal_item
        WHERE cashflow_type = 'Outflow'
        AND start_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '14 days';`
	err = pgxPool.QueryRow(context.Background(), outflowQuery).Scan(&outflows)
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
}

func entitycurrencywiseCash(pgxPool *pgxpool.Pool) ([]EntityCurrencyCash, error) {
	var results []EntityCurrencyCash
	query := `SELECT 
        m.entity_name,
        bs.currencycode,
        COALESCE(SUM(bs.closingbalance), 0) AS total_balance
    FROM bank_statement bs
    JOIN masterentity m ON bs.entityid = m.entity_id
    WHERE bs.status = 'Approved'
    GROUP BY m.entity_name, bs.currencycode;`
	rows, err := pgxPool.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var rec EntityCurrencyCash
		if err := rows.Scan(&rec.EntityName, &rec.CurrencyCode, &rec.TotalBalance); err != nil {
			return nil, err
		}
		results = append(results, rec)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return results, nil
}
