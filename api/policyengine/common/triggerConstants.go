package common

import "strings"

// Trigger event vocabulary — single Go source of truth.
// Mirror: CimplrCorpSaaS-Web/src/features/policyEngine/triggerConstants.ts
// DB seeds must stay ⊆ this set (policyengine_svc.trigger_event).
//
// Two layers (BRD §6.2 + app CRUD refinement):
//   1) Handler CRUD PRE_/POST_ × create|upload|edit|delete|approve|reject
//   2) BRD semantic: PRE_SAVE, POST_TRADE, WORKFLOW_TRANSITION, SCHEDULED_*, ON_*

// --- Handler / CRUD (canonical for RunCheck callers) ---

const (
	TriggerPreCreate   = "PRE_CREATE"
	TriggerPostCreate  = "POST_CREATE"
	TriggerPreUpload   = "PRE_UPLOAD"
	TriggerPostUpload  = "POST_UPLOAD"
	TriggerPreEdit     = "PRE_EDIT"
	TriggerPostEdit    = "POST_EDIT"
	TriggerPreDelete   = "PRE_DELETE"
	TriggerPostDelete  = "POST_DELETE"
	TriggerPreApprove  = "PRE_APPROVE"
	TriggerPostApprove = "POST_APPROVE"
	TriggerPreReject   = "PRE_REJECT"
	TriggerPostReject  = "POST_REJECT"
)

// --- BRD semantic / orthogonal ---

const (
	TriggerPreSave            = "PRE_SAVE"
	TriggerPreSubmit          = "PRE_SUBMIT"
	TriggerPostSave           = "POST_SAVE"
	TriggerOnFieldChange      = "ON_FIELD_CHANGE"
	TriggerPostTrade          = "POST_TRADE"
	TriggerWorkflowTransition = "WORKFLOW_TRANSITION"
	TriggerScheduledDaily     = "SCHEDULED_DAILY"
	TriggerScheduledIntraday  = "SCHEDULED_INTRADAY"
	TriggerScheduledMonthly   = "SCHEDULED_MONTHLY"
	TriggerOnDataImport       = "ON_DATA_IMPORT"
	TriggerOnStatusChange     = "ON_STATUS_CHANGE"
)

// --- Breach actions (BRD §4.3 / §11.2) ---

const (
	BreachHardBlock       = "HardBlock"
	BreachSoftWarning     = "SoftWarning"
	BreachTriggerApproval = "TriggerApproval"
	BreachNotifyOnly      = "NotifyOnly"
)

// Timing categories for trigger_event.timing_category
const (
	TimingSynchronous = "Synchronous"
	TimingAsynchronous = "Asynchronous"
	TimingScheduled   = "Scheduled"
	TimingAsyncEvent  = "Async Event"
)

// NonBlockingTriggers — BRD §6.3: Scheduled + Async Event → NotifyOnly | TriggerApproval only.
// POST_* are Asynchronous; SoftWarning is not allowed on these in the Workbench.
var NonBlockingTriggers = map[string]struct{}{
	TriggerPostCreate:         {},
	TriggerPostUpload:         {},
	TriggerPostEdit:           {},
	TriggerPostDelete:         {},
	TriggerPostApprove:        {},
	TriggerPostReject:         {},
	TriggerPostSave:           {},
	TriggerScheduledDaily:     {},
	TriggerScheduledIntraday:  {},
	TriggerScheduledMonthly:   {},
	TriggerOnDataImport:       {},
	TriggerOnStatusChange:     {},
}

// IsNonBlockingTrigger reports whether HardBlock / SoftWarning are forbidden.
func IsNonBlockingTrigger(code string) bool {
	_, ok := NonBlockingTriggers[code]
	return ok
}

// ForbidsTriggerApproval reports whether TriggerApproval would loop: a PRE_APPROVE
// (or PRE_REJECT) policy that itself queues another approval never completes.
// Matches PRE_/POST_ APPROVE and REJECT, plus any future *_APPROVE / *_REJECT code.
func ForbidsTriggerApproval(code string) bool {
	u := strings.ToUpper(strings.TrimSpace(code))
	if u == "" {
		return false
	}
	return strings.HasSuffix(u, "_APPROVE") || strings.HasSuffix(u, "_REJECT") || u == "APPROVE" || u == "REJECT"
}

// AllSystemTriggerCodes is the closed vocabulary (CRUD + BRD semantic).
func AllSystemTriggerCodes() []string {
	return []string{
		TriggerPreCreate, TriggerPostCreate,
		TriggerPreUpload, TriggerPostUpload,
		TriggerPreEdit, TriggerPostEdit,
		TriggerPreDelete, TriggerPostDelete,
		TriggerPreApprove, TriggerPostApprove,
		TriggerPreReject, TriggerPostReject,
		TriggerPreSave, TriggerPreSubmit, TriggerPostSave,
		TriggerOnFieldChange, TriggerPostTrade, TriggerWorkflowTransition,
		TriggerScheduledDaily, TriggerScheduledIntraday, TriggerScheduledMonthly,
		TriggerOnDataImport, TriggerOnStatusChange,
	}
}

// Module codes used by RunCheck / domain_catalog POLICY aliases.
const (
	ModuleCash         = "CASH"
	ModuleFX           = "FX"
	ModuleInvestmentFD = "INVESTMENT_FD"
	ModuleInvestmentMF = "INVESTMENT_MF"
	ModuleEmail        = "EMAIL"
)
