// GetTransactionPoolHandler returns an http.HandlerFunc for the transaction pool dashboard

package commonpool

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"
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
	return "Uncategorized"
}

func GetTransactionPoolHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		transactions, err := FetchConsolidatedTransactionPool(ctx, db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(transactions)
	}
}

// FetchConsolidatedTransactionPool fetches and maps all transactions for dashboard
func FetchConsolidatedTransactionPool(ctx context.Context, db *sql.DB) ([]ConsolidatedTransactionPool, error) {
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

	// Adjust the query to join with entity, bank, currency as needed
	query := `
		SELECT 
			t.value_date, 
			me.entity_name, 
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
		JOIN public.masterbankaccount mba ON t.account_number = mba.account_number AND mba.is_deleted = false
		JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
		LEFT JOIN public.mastercashflowcategory c ON t.category_id = c.category_id
		LEFT JOIN public.masterentitycash me ON s.entity_id = me.entity_id
		WHERE s.entity_id = ANY($1)
		  AND s.account_number = ANY($2)
		  AND lower(trim(mb.bank_name)) = ANY($3)
		  AND upper(trim(mba.currency)) = ANY($4)
		ORDER BY t.value_date DESC
	`
	rows, err := db.QueryContext(ctx, query, pq.Array(allowedEntityIDs), pq.Array(allowedAccountNumbers), pq.Array(normBanks), pq.Array(normCurrencies))
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
			Date:      r.ValueDate.Format(constants.DateFormat),
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
