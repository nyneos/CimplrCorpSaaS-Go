// Package ctxutil provides a single-line context extraction pattern for handlers.
// Every handler that needs session or master data should start with:
//
//	scope := ctxutil.FromContext(r.Context())
//
// Fields are zero-valued when the corresponding middleware was not in the chain.
package ctxutil

import (
	"context"
	"strings"

	"CimplrCorpSaas/api"
)

// RequestScope aggregates every piece of data the middleware chain loads into
// the request context. Handlers use this instead of calling ctx.Value directly.
type RequestScope struct {
	// ── Session (always set by SessionMiddleware) ────────────────────────────
	UserID      string
	EntityIDs   []string // use in WHERE entity_id = ANY($1)
	EntityNames []string

	// IsAdminOverride is true when the session middleware detected that this
	// user is either in ADMIN_USER_IDS or holds a role listed in ADMIN_ROLES
	// (context key "is_admin_override"). All Has* scope checks return true
	// immediately when this is set, giving admins full access.
	IsAdminOverride bool

	// ── Global Independent (set by GlobalIndependentMiddleware) ──────────────
	Banks            []map[string]string // keys: bank_id, bank_name, bank_code
	Currencies       []map[string]string // keys: currency_id, currency_code
	HolidayCalendars []map[string]string // keys: calendar_id, calendar_code, calendar_name

	// ── Global Dependent (set by GlobalDependentMiddleware) ──────────────────
	BankAccounts []map[string]string // keys: account_id, account_number, bank_id
	GLAccounts   []map[string]string // keys: gl_account_id, gl_account_code, gl_account_name

	// ── Cash (set by CashMiddleware) ─────────────────────────────────────────
	Counterparties     []map[string]string // keys: counterparty_id, counterparty_name
	CashFlowCategories []map[string]string // keys: category_id, category_name
	PayableReceivables []map[string]string // keys: type_id, type_name
	CostProfitCenters  []map[string]string // keys: centre_id, centre_name

	// ── Investment MF (set by InvestmentMFMiddleware) ────────────────────────
	AMCs    []map[string]string // keys: amc_id, amc_name
	Schemes []map[string]string // keys: scheme_id, scheme_name, isin
	DPs     []map[string]string // keys: dp_id, dp_name
	Demats  []map[string]string // keys: demat_id, demat_account_number
	Folios  []map[string]string // keys: folio_id, folio_number

	// ── Investment FD (set by InvestmentFDMiddleware) ────────────────────────
	InterestTypes          []map[string]string // keys: interest_id, interest_type_code
	CompoundingFrequencies []map[string]string // keys: frequency_id, frequency_code
	DayCounts              []map[string]string // keys: day_count_code, day_count_name
	TDSPlans               []map[string]string // keys: tds_plan_id, tds_plan_code
	PenaltyStructures      []map[string]string // keys: penalty_id, bank_code
	BankConfigs            []map[string]string // keys: config_id, bank_code
	BankRateCards          []map[string]string // keys: rate_card_id, bank_code
}

// FromContext extracts all middleware-loaded data from ctx in one call.
// Zero values in the returned struct mean that middleware was not in the chain.
func FromContext(ctx context.Context) RequestScope {
	var s RequestScope

	// Session
	if v, ok := ctx.Value("user_id").(string); ok {
		s.UserID = v
	}
	if v, ok := ctx.Value("is_admin_override").(bool); ok {
		s.IsAdminOverride = v
	}
	// EntityIDsKey is a typed contextKey; "entity_ids" plain string is also set for
	// backward-compat with investment handlers — check both.
	s.EntityIDs = api.GetEntityIDsFromCtx(ctx)
	if len(s.EntityIDs) == 0 {
		if v, ok := ctx.Value("entity_ids").([]string); ok {
			s.EntityIDs = v
		}
	}
	if v, ok := ctx.Value("entity_names").([]string); ok {
		s.EntityNames = v
	}

	// Global Independent
	if v, ok := ctx.Value("BankInfo").([]map[string]string); ok {
		s.Banks = v
	}
	if v, ok := ctx.Value("ActiveCurrencies").([]map[string]string); ok {
		s.Currencies = v
	}
	if v, ok := ctx.Value("ApprovedHolidayCalendars").([]map[string]string); ok {
		s.HolidayCalendars = v
	}

	// Global Dependent
	if v, ok := ctx.Value(api.ApprovedBankAccountsKey).([]map[string]string); ok {
		s.BankAccounts = v
	}
	if v, ok := ctx.Value("ApprovedGLAccounts").([]map[string]string); ok {
		s.GLAccounts = v
	}

	// Cash
	if v, ok := ctx.Value("ApprovedCounterparties").([]map[string]string); ok {
		s.Counterparties = v
	}
	if v, ok := ctx.Value("CashFlowCategories").([]map[string]string); ok {
		s.CashFlowCategories = v
	}
	if v, ok := ctx.Value("ApprovedPayableReceivables").([]map[string]string); ok {
		s.PayableReceivables = v
	}
	if v, ok := ctx.Value("ApprovedCostProfitCenters").([]map[string]string); ok {
		s.CostProfitCenters = v
	}

	// Investment MF
	if v, ok := ctx.Value("ApprovedAMCs").([]map[string]string); ok {
		s.AMCs = v
	}
	if v, ok := ctx.Value("ApprovedSchemes").([]map[string]string); ok {
		s.Schemes = v
	}
	if v, ok := ctx.Value("ApprovedDPs").([]map[string]string); ok {
		s.DPs = v
	}
	if v, ok := ctx.Value("ApprovedDemats").([]map[string]string); ok {
		s.Demats = v
	}
	if v, ok := ctx.Value("ApprovedFolios").([]map[string]string); ok {
		s.Folios = v
	}

	// Investment FD
	if v, ok := ctx.Value("ApprovedInterestTypes").([]map[string]string); ok {
		s.InterestTypes = v
	}
	if v, ok := ctx.Value("ApprovedCompoundingFrequencies").([]map[string]string); ok {
		s.CompoundingFrequencies = v
	}
	if v, ok := ctx.Value("ApprovedDayCounts").([]map[string]string); ok {
		s.DayCounts = v
	}
	if v, ok := ctx.Value("ApprovedTDSPlans").([]map[string]string); ok {
		s.TDSPlans = v
	}
	if v, ok := ctx.Value("ApprovedPenaltyStructures").([]map[string]string); ok {
		s.PenaltyStructures = v
	}
	if v, ok := ctx.Value("ApprovedBankConfigs").([]map[string]string); ok {
		s.BankConfigs = v
	}
	if v, ok := ctx.Value("ApprovedBankRateCards").([]map[string]string); ok {
		s.BankRateCards = v
	}

	return s
}

// ── Convenience extractors ────────────────────────────────────────────────────
// These return the IDs/codes from the slice maps, ready for SQL IN clauses.

// BankIDs returns all approved bank IDs.
func (s RequestScope) BankIDs() []string { return pluck(s.Banks, "bank_id") }

// BankAccountIDs returns all approved bank account IDs.
func (s RequestScope) BankAccountIDs() []string { return pluck(s.BankAccounts, "account_id") }

// CurrencyCodes returns all active currency codes.
func (s RequestScope) CurrencyCodes() []string { return pluck(s.Currencies, "currency_code") }

// AMCIDs returns all approved AMC IDs.
func (s RequestScope) AMCIDs() []string { return pluck(s.AMCs, "amc_id") }

// SchemeIDs returns all approved scheme IDs.
func (s RequestScope) SchemeIDs() []string { return pluck(s.Schemes, "scheme_id") }

// FolioIDs returns all approved folio IDs.
func (s RequestScope) FolioIDs() []string { return pluck(s.Folios, "folio_id") }

// DematIDs returns all approved demat account IDs.
func (s RequestScope) DematIDs() []string { return pluck(s.Demats, "demat_id") }

// CounterpartyIDs returns all approved counterparty IDs.
func (s RequestScope) CounterpartyIDs() []string { return pluck(s.Counterparties, "counterparty_id") }

// CategoryIDs returns all approved cash-flow category IDs.
func (s RequestScope) CategoryIDs() []string { return pluck(s.CashFlowCategories, "category_id") }

// GLAccountIDs returns all approved GL account IDs.
func (s RequestScope) GLAccountIDs() []string { return pluck(s.GLAccounts, "gl_account_id") }

// BankConfigIDs returns all approved FD bank config IDs.
func (s RequestScope) BankConfigIDs() []string { return pluck(s.BankConfigs, "config_id") }

// HasEntityAccess reports whether id is within the user's entity scope.
// Always returns true for admin overrides and when EntityIDs is empty.
func (s RequestScope) HasEntityAccess(id string) bool {
	if s.IsAdminOverride || len(s.EntityIDs) == 0 {
		return true
	}
	for _, eid := range s.EntityIDs {
		if strings.EqualFold(eid, id) {
			return true
		}
	}
	return false
}

// HasEntityNameAccess reports whether name is in the user's accessible entity names.
// Always returns true for admin overrides and when EntityNames is empty.
func (s RequestScope) HasEntityNameAccess(name string) bool {
	if s.IsAdminOverride || len(s.EntityNames) == 0 {
		return true
	}
	upper := strings.ToUpper(strings.TrimSpace(name))
	for _, n := range s.EntityNames {
		if strings.ToUpper(strings.TrimSpace(n)) == upper {
			return true
		}
	}
	return false
}

// HasApprovedBankAccount reports whether an account with the given account number
// is in the approved list loaded by GlobalDependentMiddleware.
// Always returns true for admin overrides and when BankAccounts is empty.
func (s RequestScope) HasApprovedBankAccount(accountNumber string) bool {
	if s.IsAdminOverride || len(s.BankAccounts) == 0 {
		return true
	}
	upper := strings.ToUpper(strings.TrimSpace(accountNumber))
	for _, a := range s.BankAccounts {
		if strings.ToUpper(strings.TrimSpace(a["account_number"])) == upper {
			return true
		}
	}
	return false
}

// HasApprovedBank reports whether a bank with the given name, code, or ID is approved.
// Always returns true for admin overrides and when Banks is empty.
func (s RequestScope) HasApprovedBank(bankIdentifier string) bool {
	if s.IsAdminOverride || len(s.Banks) == 0 {
		return true
	}
	upper := strings.ToUpper(strings.TrimSpace(bankIdentifier))
	for _, b := range s.Banks {
		if strings.ToUpper(strings.TrimSpace(b["bank_name"])) == upper ||
			strings.ToUpper(strings.TrimSpace(b["bank_code"])) == upper ||
			strings.ToUpper(strings.TrimSpace(b["bank_id"])) == upper {
			return true
		}
	}
	return false
}

// HasApprovedCurrency reports whether the currency code is in the approved list.
// Always returns true for admin overrides and when Currencies is empty.
func (s RequestScope) HasApprovedCurrency(currencyCode string) bool {
	if s.IsAdminOverride || len(s.Currencies) == 0 {
		return true
	}
	upper := strings.ToUpper(strings.TrimSpace(currencyCode))
	for _, c := range s.Currencies {
		if strings.ToUpper(strings.TrimSpace(c["currency_code"])) == upper {
			return true
		}
	}
	return false
}

// pluck extracts one key from each map, skipping empty values.
func pluck(rows []map[string]string, key string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if v := strings.TrimSpace(r[key]); v != "" {
			out = append(out, v)
		}
	}
	return out
}
