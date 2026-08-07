package bankbalances

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// lookupEntityIDForAccountNo resolves the owning entity for a bank balance.
// bank_balances_manual carries no entity column of its own — the entity comes
// from the account, the same join the list query uses. Without it EnforceInput
// carries no EntityCode, so an entity include/exclude scope cannot filter and
// the policy runs against every entity.
func lookupEntityIDForAccountNo(ctx context.Context, pool *pgxpool.Pool, accountNo string) string {
	accountNo = strings.TrimSpace(accountNo)
	if accountNo == "" || pool == nil {
		return ""
	}
	var entityID string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id, '')
		FROM public.masterbankaccount
		WHERE account_number = $1 AND is_deleted = false
		LIMIT 1`, accountNo).Scan(&entityID); err != nil {
		return ""
	}
	return strings.TrimSpace(entityID)
}

// bankBalanceRow is the canonical business-field shape for BANK_BALANCE — one
// field per domain_catalog.field row for this sub-module. Every policy-check
// call site in this package builds its Fields{} map from a value of this
// type instead of hand-picking its own ad hoc subset. Before this file,
// Create passed 3 of 17 real fields, Approve/Reject/Delete passed only
// balance_id, and Update passed a raw unvalidated edit blob — see
// database/2026-07-27.sql for the full audit.
type bankBalanceRow struct {
	BalanceID      string
	BankName       string
	AccountNo      string
	IBAN           string
	CurrencyCode   string
	Nickname       string
	Country        string
	AsOfDate       string
	AsOfTime       string
	BalanceType    string
	BalanceAmount  *float64
	StatementType  string
	SourceChannel  string
	OpeningBalance *float64
	TotalCredits   *float64
	TotalDebits    *float64
	ClosingBalance *float64
}

// buildBankBalancePolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for BANK_BALANCE (see
// cmd/seedDomainCatalog/gapSeeds2.go).
func buildBankBalancePolicyFields(row bankBalanceRow) map[string]interface{} {
	return map[string]interface{}{
		"balance_id":      row.BalanceID,
		"bank_name":       row.BankName,
		"account_no":      row.AccountNo,
		"iban":            row.IBAN,
		"currency_code":   row.CurrencyCode,
		"nickname":        row.Nickname,
		"country":         row.Country,
		"as_of_date":      row.AsOfDate,
		"as_of_time":      row.AsOfTime,
		"balance_type":    row.BalanceType,
		"balance_amount":  row.BalanceAmount,
		"statement_type":  row.StatementType,
		"source_channel":  row.SourceChannel,
		"opening_balance": row.OpeningBalance,
		"total_credits":   row.TotalCredits,
		"total_debits":    row.TotalDebits,
		"closing_balance": row.ClosingBalance,
	}
}

// loadBankBalanceRow fetches the full canonical row by balance_id — used by
// Approve/Reject/Delete, which only ever receive an ID in the request, never
// the business data itself.
func loadBankBalanceRow(ctx context.Context, pool *pgxpool.Pool, balanceID string) (bankBalanceRow, error) {
	var row bankBalanceRow
	err := pool.QueryRow(ctx, `
		SELECT balance_id, COALESCE(bank_name,''), COALESCE(account_no,''), COALESCE(iban,''),
		       COALESCE(currency_code,''), COALESCE(nickname,''), COALESCE(country,''),
		       COALESCE(as_of_date::text,''), COALESCE(as_of_time::text,''), COALESCE(balance_type,''),
		       balance_amount, COALESCE(statement_type,''), COALESCE(source_channel,''),
		       opening_balance, total_credits, total_debits, closing_balance
		FROM bank_balances_manual WHERE balance_id = $1`, balanceID,
	).Scan(&row.BalanceID, &row.BankName, &row.AccountNo, &row.IBAN,
		&row.CurrencyCode, &row.Nickname, &row.Country,
		&row.AsOfDate, &row.AsOfTime, &row.BalanceType,
		&row.BalanceAmount, &row.StatementType, &row.SourceChannel,
		&row.OpeningBalance, &row.TotalCredits, &row.TotalDebits, &row.ClosingBalance)
	if err != nil {
		return bankBalanceRow{}, err
	}
	return row, nil
}

// applyBankBalanceEdits overlays a partial edit map (UpdateBankBalance's
// req.Fields — arbitrary subset, whatever the user actually changed) onto an
// already-loaded canonical row, so the policy check sees the full row
// as-it-will-be-after-the-edit, not just the touched keys.
func applyBankBalanceEdits(row bankBalanceRow, edits map[string]interface{}) bankBalanceRow {
	str := func(v interface{}) (string, bool) {
		s, ok := v.(string)
		return s, ok
	}
	num := func(v interface{}) (*float64, bool) {
		switch n := v.(type) {
		case float64:
			return &n, true
		case int:
			f := float64(n)
			return &f, true
		}
		return nil, false
	}
	for k, v := range edits {
		switch k {
		case "bank_name":
			if s, ok := str(v); ok {
				row.BankName = s
			}
		case "account_no":
			if s, ok := str(v); ok {
				row.AccountNo = s
			}
		case "iban":
			if s, ok := str(v); ok {
				row.IBAN = s
			}
		case "currency_code":
			if s, ok := str(v); ok {
				row.CurrencyCode = s
			}
		case "nickname":
			if s, ok := str(v); ok {
				row.Nickname = s
			}
		case "country":
			if s, ok := str(v); ok {
				row.Country = s
			}
		case "as_of_date":
			if s, ok := str(v); ok {
				row.AsOfDate = s
			}
		case "as_of_time":
			if s, ok := str(v); ok {
				row.AsOfTime = s
			}
		case "balance_type":
			if s, ok := str(v); ok {
				row.BalanceType = s
			}
		case "balance_amount":
			if n, ok := num(v); ok {
				row.BalanceAmount = n
			}
		case "statement_type":
			if s, ok := str(v); ok {
				row.StatementType = s
			}
		case "source_channel":
			if s, ok := str(v); ok {
				row.SourceChannel = s
			}
		case "opening_balance":
			if n, ok := num(v); ok {
				row.OpeningBalance = n
			}
		case "total_credits":
			if n, ok := num(v); ok {
				row.TotalCredits = n
			}
		case "total_debits":
			if n, ok := num(v); ok {
				row.TotalDebits = n
			}
		case "closing_balance":
			if n, ok := num(v); ok {
				row.ClosingBalance = n
			}
		}
	}
	return row
}
