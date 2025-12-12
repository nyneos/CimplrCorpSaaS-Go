// GetTransactionPoolHandler returns an http.HandlerFunc for the transaction pool dashboard

package commonpool

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type ConsolidatedTransactionPool struct {
	Date      string `json:"date"`
	Entity    string `json:"entity"`
	Bank      string `json:"bank"`
	Currency  string `json:"currency"`
	Category  string `json:"category"`
	Type      string `json:"type"` // credit/debit
	Amount    string `json:"amount"`
	Narration string `json:"Narration"`
}

// TransactionDBRow represents a row from bank_statement_transactions joined with transaction_categories
type TransactionDBRow struct {
	ValueDate        time.Time
	Entity           string
	Bank             string
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
	return ""
}

func GetTransactionPoolHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		transactions, err := FetchConsolidatedTransactionPool(ctx, db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(transactions)
	}
}

// FetchConsolidatedTransactionPool fetches and maps all transactions for dashboard
func FetchConsolidatedTransactionPool(ctx context.Context, db *sql.DB) ([]ConsolidatedTransactionPool, error) {
	// Adjust the query to join with entity, bank, currency as needed
	query := `
		SELECT 
			t.value_date, 
			s.entity_id, 
			s.account_number, 
			mb.bank_name, 
			mba.currency, 
			c.category_name, 
			c.category_type, 
			t.description, 
			t.withdrawal_amount, 
			t.deposit_amount
		FROM cimplrcorpsaas.bank_statement_transactions t
		LEFT JOIN cimplrcorpsaas.bank_statements s ON t.bank_statement_id = s.bank_statement_id
		LEFT JOIN public.masterbankaccount mba ON t.account_number = mba.account_number
		LEFT JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
		LEFT JOIN cimplrcorpsaas.transaction_categories c ON t.category_id = c.category_id
		ORDER BY t.value_date DESC
	`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var result []ConsolidatedTransactionPool
	for rows.Next() {
		var r TransactionDBRow
		var entity, bank, currency sql.NullString
		err := rows.Scan(
			&r.ValueDate,
			&entity,
			&r.Bank,
			&bank,
			&currency,
			&r.Category,
			&r.CategoryType,
			&r.Description,
			&r.WithdrawalAmount,
			&r.DepositAmount,
		)
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		// Determine type and amount
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
			Date:      r.ValueDate.Format("2006-01-02"),
			Entity:    entity.String,
			Bank:      bank.String,
			Currency:  currency.String,
			Category:  nullToString(r.Category),
			Type:      typ,
			Amount:    amt,
			Narration: r.Description,
		})
		// nullToString safely converts sql.NullString to string

	}
	return result, nil
}
