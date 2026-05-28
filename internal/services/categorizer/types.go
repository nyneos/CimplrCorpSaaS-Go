package categorizer

import "time"

// ─────────────────────────────────────────────────────────────
// Waterfall step labels — written to classification_audit_log
// ─────────────────────────────────────────────────────────────
const (
	StepRule           = "RULE"
	StepCounterparty   = "COUNTERPARTY"
	StepGL             = "GL"
	StepCorrection     = "CORRECTION"
	StepSimilarity     = "SIMILARITY"
	StepAccountDefault = "ACCOUNT_DEFAULT" // account-level inflow/outflow/charges/interest defaults
	StepAIInference    = "AI_INFERENCE"    // LLM-based intelligent inference (Step 9)
	StepUnallocated    = "UNALLOCATED"

	// MinConfidenceForActuals: below this → review queue, category NOT set.
	MinConfidenceForActuals = 0.70
)

// ─────────────────────────────────────────────────────────────
// TxnInput — minimal fields needed for the waterfall
// ─────────────────────────────────────────────────────────────
type TxnInput struct {
	TransactionID int64
	AccountNumber string
	EntityID      string
	Description   string
	Withdrawal    *float64
	Deposit       *float64
	ValueDate     *time.Time
	Currency      string
	GLAccountID   string // optional — populated from ERP sync
}

// ─────────────────────────────────────────────────────────────
// ClassificationResult — output of SmartCategorize
// ─────────────────────────────────────────────────────────────
type ClassificationResult struct {
	CategoryID   string
	CategoryName string
	Confidence   float64 // 0.0–1.0
	Step         string  // one of Step* constants above
	RuleID       *int64  // set only when Step == StepRule
	SourceRef    string  // human-readable explanation
}

// ─────────────────────────────────────────────────────────────
// BatchClassification — used by PersistBatch
// ─────────────────────────────────────────────────────────────
type BatchClassification struct {
	TxnID     int64
	Narration NarrationResult
	Result    ClassificationResult
}

// ─────────────────────────────────────────────────────────────
// smartRuleComp — one component of a category rule (pgx-native)
// pgx v5 scans NULL columns into nil pointers automatically.
// ─────────────────────────────────────────────────────────────
type smartRuleComp struct {
	RuleID         int64
	Priority       int
	CategoryID     string
	CategoryName   string
	CategoryType   string
	ComponentType  string
	MatchType      *string
	MatchValue     *string
	AmountOperator *string
	AmountValue    *float64
	TxnFlow        *string
	CurrencyCode   *string
	EffectiveDate  *time.Time
	// Scope (used to filter rules per-transaction)
	ScopeType     string
	ScopeAccount  *string
	ScopeEntity   *string
	ScopeBank     *string
	ScopeCurrency *string
}

// ─────────────────────────────────────────────────────────────
// SmartCaches — preloaded at the start of each batch job run
// ─────────────────────────────────────────────────────────────

// cpDefault is a counterparty entry with a default category.
type cpDefault struct {
	CPName     string // original case
	CategoryID string
	CatName    string
}

// glDefault is a GL account entry with a mapped category.
type glDefault struct {
	CategoryID string
	CatName    string
}

// accountDefault holds the per-account category defaults configured in masterbankaccount.
// These map transaction direction / type → category, as set by the user in bank account master.
type accountDefault struct {
	// cat_inflow / cat_outflow: used when no more-specific step matched
	CatInflow  string // category_id for deposits
	CatOutflow string // category_id for withdrawals
	CatCharges string // category_id when narration suggests bank charges
	CatIntInc  string // category_id for interest income
	CatIntExp  string // category_id for interest expense
}

// SmartCaches holds in-memory lookups for Steps 2, 3, and 6.
// Load once per batch run with LoadSmartCaches.
type SmartCaches struct {
	// key: lowercase counterparty name
	CounterpartyByName map[string]cpDefault
	// key: gl_account_id
	GLMapping map[string]glDefault
	// key: account_number — per-account category defaults (Step 6)
	AccountDefaults map[string]accountDefault
	// key: account_number → gl_account_id (ERP enrichment, Step 3)
	AccountGLID map[string]string
}

// GLAccountIDForAccount returns the ERP GL account ID linked to a bank account,
// or "" if no mapping exists.
func (c *SmartCaches) GLAccountIDForAccount(accountNumber string) string {
	if c == nil {
		return ""
	}
	return c.AccountGLID[accountNumber]
}
