package api

import (
	"context"
	"log"
	"strings"

	"CimplrCorpSaas/api/auth"
)

func RequestedByFromCtx(ctx context.Context, userID string) string {
	if v := ctx.Value("session"); v != nil {
		if s, ok := v.(*auth.UserSession); ok && s != nil {
			if strings.TrimSpace(s.Name) != "" {
				return s.Name
			}
			if strings.TrimSpace(s.UserID) != "" {
				return s.UserID
			}
		}
	}
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			return s.Name
		}
	}
	return ""
}

func CtxApprovedAccountNumbers(ctx context.Context) []string {
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
		if strings.TrimSpace(a["account_number"]) != "" {
			out = append(out, strings.TrimSpace(a["account_number"]))
		}
	}
	return out
}

func CtxApprovedBankNames(ctx context.Context) []string {
	v := ctx.Value("BankInfo")
	if v == nil {
		return nil
	}
	banks, ok := v.([]map[string]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(banks))
	for _, b := range banks {
		if s := strings.TrimSpace(b["bank_name"]); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func CtxApprovedCurrencies(ctx context.Context) []string {
	v := ctx.Value("CurrencyInfo")
	if v == nil {
		v = ctx.Value("ApprovedCurrencies")
	}
	if v == nil {
		return nil
	}
	curList, ok := v.([]map[string]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(curList))
	for _, c := range curList {
		if s := strings.TrimSpace(c["currency_code"]); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func CtxHasApprovedCurrency(ctx context.Context, currency string) bool {
	currency = strings.TrimSpace(currency)
	if currency == "" {
		return false
	}
	v := ctx.Value("CurrencyInfo")
	if v == nil {
		v = ctx.Value("ApprovedCurrencies")
	}
	if v == nil {
		return true
	}
	curList, ok := v.([]map[string]string)
	if !ok {
		return true
	}
	for _, c := range curList {
		if strings.EqualFold(strings.TrimSpace(c["currency_code"]), currency) {
			return true
		}
	}
	return false
}

func CtxHasApprovedBankAccount(ctx context.Context, accountNumber string) bool {
	accountNumber = strings.TrimSpace(accountNumber)
	if accountNumber == "" {
		return false
	}
	v := ctx.Value("ApprovedBankAccounts")
	if v == nil {
		return true
	}
	accounts, ok := v.([]map[string]string)
	if !ok {
		return true
	}
	for _, a := range accounts {
		if strings.EqualFold(strings.TrimSpace(a["account_number"]), accountNumber) {
			return true
		}
	}
	return false
}

func CtxHasApprovedBankName(ctx context.Context, bankName string) bool {
	bankName = strings.TrimSpace(bankName)
	if bankName == "" {
		return false
	}
	v := ctx.Value("BankInfo")
	if v == nil {
		return true
	}
	banks, ok := v.([]map[string]string)
	if !ok {
		return true
	}
	for _, b := range banks {
		if strings.EqualFold(strings.TrimSpace(b["bank_name"]), bankName) ||
			strings.EqualFold(strings.TrimSpace(b["bank_short_name"]), bankName) ||
			strings.EqualFold(strings.TrimSpace(b["bank_id"]), bankName) {
			return true
		}
	}
	return false
}

func CtxEntityIDs(ctx context.Context) []string {
	v := ctx.Value(EntityIDsKey)
	if v == nil {
		v = ctx.Value("entityIDs")
	}
	if v == nil {
		return nil
	}
	entityIDs, ok := v.([]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(entityIDs))
	for _, e := range entityIDs {
		if strings.TrimSpace(e) != "" {
			out = append(out, strings.TrimSpace(e))
		}
	}
	return out
}

// DebugPrevalidationContext logs common prevalidation context fields for debugging.
func DebugPrevalidationContext(ctx context.Context) {
	if ctx == nil {
		log.Printf("[DEBUG context_helpers] context is nil")
		return
	}
	user := ""
	if v := ctx.Value("user_id"); v != nil {
		if s, ok := v.(string); ok {
			user = s
		}
	}
	entityIDs := CtxEntityIDs(ctx)
	bankNames := CtxApprovedBankNames(ctx)
	currencies := CtxApprovedCurrencies(ctx)
	accounts := CtxApprovedAccountNumbers(ctx)
	admin := ctx.Value("is_admin_override")
	adminBy := ctx.Value("admin_override_by")
	sessionPresent := ctx.Value("session") != nil

	log.Printf("[DEBUG context_helpers] user=%s entities=%v banks=%v accounts_count=%d currencies=%v is_admin_override=%v admin_override_by=%v session_present=%v",
		user, entityIDs, bankNames, len(accounts), currencies, admin, adminBy, sessionPresent)
}
