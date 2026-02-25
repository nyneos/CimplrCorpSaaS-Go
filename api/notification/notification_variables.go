// Package notification provides canonical variable definitions for the notification
// template engine. Each sub-module exposes the set of template variables ({{Key}})
// that templates may reference. Variables are drawn directly from the
// TriggerNotification payload maps scattered across the codebase.
//
// Sub-module codes correspond to the sub_module_code column in the
// cimplrcorpsaas.notificationtemplate table.
package notification

// ─────────────────────────────────────────────────────────────────────────────
// Sub-module codes (must match DB values exactly)
// ─────────────────────────────────────────────────────────────────────────────

const (
	SubModuleBankStatement   = "BANK_STATEMENT"
	SubModuleBankLimit       = "BANK_LIMIT"
	SubModuleLimitUtil       = "LIMIT_UTILIZATION"
	SubModuleSweepCfg        = "SWEEP_CFG"
	SubModuleSweepInit       = "SWEEP_INIT"
	SubModuleSweepExec       = "SWEEP_EXEC"
	SubModuleSweep           = "SWEEP"
	SubModuleProjection      = "PROJECTION"
)

// ─────────────────────────────────────────────────────────────────────────────
// Event codes (must match event_code column in DB)
// ─────────────────────────────────────────────────────────────────────────────

const (
	// BANK_STATEMENT events
	EventBSApprove   = "BS_APPROVE"
	EventBSReject    = "BS_REJECT"
	EventBSDelete    = "BS_DELETE"
	EventBSCommit    = "BS_COMMIT"
	EventBSPreview   = "BS_PREVIEW"
	EventBSZipUpload = "BS_ZIP_UPLOAD"

	// BANK_LIMIT events
	EventLimitBulkCreate     = "LIMIT_BULK_CREATE"
	EventLimitUpdated        = "LIMIT_UPDATED"
	EventLimitDeleteRequest  = "LIMIT_DELETE_REQUESTED"
	EventLimitApproved       = "LIMIT_APPROVED"
	EventLimitRejected       = "LIMIT_REJECTED"

	// LIMIT_UTILIZATION events
	EventUtilBulkCreate    = "UTIL_BULK_CREATE"
	EventUtilUpdated       = "UTIL_UPDATED"
	EventUtilDeleteRequest = "UTIL_DELETE_REQUESTED"
	EventUtilApproved      = "UTIL_APPROVED"
	EventUtilRejected      = "UTIL_REJECTED"
	EventUtilFileUploaded  = "UTIL_FILE_UPLOADED"

	// SWEEP_CFG events
	EventSweepCfgCreate  = "SWEEPCFG_CREATE"
	EventSweepCfgUpdate  = "SWEEPCFG_UPDATE"
	EventSweepCfgApprove = "SWEEPCFG_APPROVE"
	EventSweepCfgReject  = "SWEEPCFG_REJECT"
	EventSweepCfgDelete  = "SWEEPCFG_DELETE"

	// SWEEP_INIT events
	EventSweepInitCreate  = "SWEEPINIT_CREATE"
	EventSweepInitUpdate  = "SWEEPINIT_UPDATE"
	EventSweepInitApprove = "SWEEPINIT_APPROVE"
	EventSweepInitReject  = "SWEEPINIT_REJECT"
	EventSweepInitDelete  = "SWEEPINIT_DELETE"

	// SWEEP_EXEC events
	EventSweepExecManual = "SWEEPEXEC_MANUAL"
	EventSweepExecBulk   = "SWEEPEXEC_BULK"

	// SWEEP (automated processor) events
	EventSweepExecuted = "SWEEP_EXECUTED"

	// PROJECTION events
	EventProjCreate = "PROJ_CREATE"
	EventProjUpdate = "PROJ_UPDATE"
	EventProjDelete = "PROJ_DELETE"
	EventProjReject = "PROJ_REJECT"
	EventProjApprove = "PROJ_APPROVE"
	EventProjUpload  = "PROJ_UPLOAD"
)

// ─────────────────────────────────────────────────────────────────────────────
// NotifVariable describes a single template placeholder available to a
// notification template. Use Key as {{Key}} in the template body / subject.
// ─────────────────────────────────────────────────────────────────────────────

type NotifVariable struct {
	Key         string // Template token, used as {{Key}} in templates
	Label       string // Human-readable label shown in the UI variable picker
	Description string // Optional: describes what value this holds
	IsList      bool   // True when the value is a list (TABLE_HTML / LIST_HTML functions apply)
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared / common variables available in every event payload
// ─────────────────────────────────────────────────────────────────────────────

var CommonVariables = []NotifVariable{
	{Key: "Action", Label: "Action", Description: "The action that triggered this event (e.g. CREATE, APPROVE, REJECT)"},
	{Key: "ActionAt", Label: "Action Timestamp", Description: "RFC3339 timestamp of when the action occurred"},
}

// ─────────────────────────────────────────────────────────────────────────────
// BANK_STATEMENT variables
// Source files:
//   api/cash/bankstatement/bankstatUplV2.go  (BS_APPROVE, BS_REJECT, BS_DELETE)
//   api/cash/bankstatement/stream_handlers.go (BS_COMMIT, BS_PREVIEW, BS_ZIP_UPLOAD)
//   api/cash/bankstatement/notif_payload.go  (BankStatementNotifPayload.ToMap)
// ─────────────────────────────────────────────────────────────────────────────

// BankStatementVariables lists every template variable available across ALL
// BANK_STATEMENT events. Not every variable is present in every event;
// see EventPayloadKeys for the exact set per event.
var BankStatementVariables = []NotifVariable{
	// Identity / header
	{Key: "BankStatementID", Label: "Bank Statement ID", Description: "UUID of the bank statement"},
	{Key: "BankStatementIDs", Label: "Bank Statement IDs (list)", Description: "List of bank statement UUIDs (bulk actions)", IsList: true},
	{Key: "Count", Label: "Count", Description: "Number of statements affected"},

	// Account / bank
	{Key: "AccountNumber", Label: "Account Number"},
	{Key: "AccountName", Label: "Account Name"},
	{Key: "BankName", Label: "Bank Name"},
	{Key: "IFSC", Label: "IFSC Code"},
	{Key: "CurrencyCode", Label: "Currency Code"},

	// Period
	{Key: "PeriodStart", Label: "Statement Period Start"},
	{Key: "PeriodEnd", Label: "Statement Period End"},
	{Key: "StatementPeriodEnd", Label: "Statement Period End (short)", Description: "Used in approve payload"},

	// Balance KPIs
	{Key: "OpeningBalance", Label: "Opening Balance"},
	{Key: "ClosingBalance", Label: "Closing Balance"},
	{Key: "TotalDebit", Label: "Total Debit"},
	{Key: "TotalCredit", Label: "Total Credit"},
	{Key: "NetFlow", Label: "Net Flow"},

	// Transaction counts
	{Key: "TotalTransactions", Label: "Total Transactions"},
	{Key: "CategorizedCount", Label: "Categorized Transactions"},
	{Key: "UncategorizedCount", Label: "Uncategorized Transactions"},
	{Key: "CategorizedPercent", Label: "Categorized %"},
	{Key: "UncategorizedPercent", Label: "Uncategorized %"},

	// Audit / actors
	{Key: "UploadedBy", Label: "Uploaded By"},
	{Key: "ApprovedBy", Label: "Approved By"},
	{Key: "UserID", Label: "User ID", Description: "User performing the action (reject / delete)"},
	{Key: "Comment", Label: "Checker Comment"},

	// File / meta
	{Key: "FileName", Label: "File Name"},
	{Key: "UploadedAt", Label: "Uploaded At"},
	{Key: "EntityID", Label: "Entity ID"},
	{Key: "Status", Label: "Status"},

	// List variables (require TABLE_HTML / LIST_HTML)
	{Key: "Transactions", Label: "All Transactions", IsList: true},
	{Key: "UncategorizedTransactions", Label: "Uncategorized Transactions (list)", IsList: true},
	{Key: "CreditTransactions", Label: "Credit Transactions (list)", IsList: true},
	{Key: "DebitTransactions", Label: "Debit Transactions (list)", IsList: true},
	{Key: "CategoryKPIs", Label: "Category KPIs (list)", IsList: true},
}

// ─────────────────────────────────────────────────────────────────────────────
// BANK_LIMIT variables
// Source file: api/cash/limit/limit.go
// ─────────────────────────────────────────────────────────────────────────────

var BankLimitVariables = []NotifVariable{
	// Identity
	{Key: "LimitID", Label: "Limit ID", Description: "Single limit UUID (update events)"},
	{Key: "LimitIDs", Label: "Limit IDs (list)", Description: "List of limit UUIDs (bulk events)", IsList: true},
	{Key: "Count", Label: "Count", Description: "Number of limits affected"},
	{Key: "DeletedIDs", Label: "Deleted IDs (list)", Description: "IDs soft-deleted after approval of DELETE actions", IsList: true},

	// Actors
	{Key: "RequestedBy", Label: "Requested By"},
	{Key: "ApprovedBy", Label: "Approved By"},
	{Key: "RejectedBy", Label: "Rejected By"},

	// Status / audit
	{Key: "Action", Label: "Action"},
	{Key: "Status", Label: "Status"},
	{Key: "ActionAt", Label: "Action Timestamp"},
}

// ─────────────────────────────────────────────────────────────────────────────
// LIMIT_UTILIZATION variables
// Source file: api/cash/limit/utilization.go
// ─────────────────────────────────────────────────────────────────────────────

var LimitUtilizationVariables = []NotifVariable{
	// Identity
	{Key: "UtilizationID", Label: "Utilization ID", Description: "Single utilization UUID (update events)"},
	{Key: "UtilizationIDs", Label: "Utilization IDs (list)", Description: "List of utilization UUIDs (bulk events)", IsList: true},
	{Key: "Count", Label: "Count", Description: "Number of utilization records affected"},
	{Key: "DeletedIDs", Label: "Deleted IDs (list)", Description: "IDs soft-deleted after approval of DELETE actions", IsList: true},

	// Actors
	{Key: "RequestedBy", Label: "Requested By"},
	{Key: "ApprovedBy", Label: "Approved By"},
	{Key: "RejectedBy", Label: "Rejected By"},
	{Key: "UploadedBy", Label: "Uploaded By", Description: "Present on UTIL_FILE_UPLOADED"},

	// File upload
	{Key: "FileName", Label: "File Name", Description: "Present on UTIL_FILE_UPLOADED"},
	{Key: "RowsUploaded", Label: "Rows Uploaded", Description: "Number of rows in uploaded file"},

	// Status / audit
	{Key: "Action", Label: "Action"},
	{Key: "Status", Label: "Status"},
	{Key: "ActionAt", Label: "Action Timestamp"},
}

// ─────────────────────────────────────────────────────────────────────────────
// SWEEP_CFG variables
// Source file: api/cash/sweepConfig/sweepConfigV2.go
// ─────────────────────────────────────────────────────────────────────────────

var SweepConfigVariables = []NotifVariable{
	// Identity
	{Key: "SweepID", Label: "Sweep ID", Description: "Single sweep config UUID (update events)"},
	{Key: "SweepIDs", Label: "Sweep IDs (list)", Description: "List of sweep config UUIDs (bulk events)", IsList: true},
	{Key: "Count", Label: "Count", Description: "Number of sweep configs affected"},
	{Key: "DeletedIDs", Label: "Deleted IDs (list)", Description: "IDs soft-deleted after approval of DELETE actions", IsList: true},

	// Update-specific
	{Key: "Fields", Label: "Updated Fields", Description: "List of field names changed during update (SWEEPCFG_UPDATE)", IsList: true},
	{Key: "Reason", Label: "Reason / Comment"},

	// Actors
	{Key: "RequestedBy", Label: "Requested By"},
	{Key: "CheckerBy", Label: "Checker (Approver/Rejecter)"},
	{Key: "CheckerComment", Label: "Checker Comment"},

	// Status / audit
	{Key: "Action", Label: "Action"},
	{Key: "Status", Label: "Status"},
	{Key: "ActionAt", Label: "Action Timestamp"},
}

// ─────────────────────────────────────────────────────────────────────────────
// SWEEP_INIT variables
// Source file: api/cash/sweepConfig/sweepInitiationV2.go
// ─────────────────────────────────────────────────────────────────────────────

var SweepInitVariables = []NotifVariable{
	// Identity
	{Key: "InitiationID", Label: "Initiation ID", Description: "Single initiation UUID (update events)"},
	{Key: "InitiationIDs", Label: "Initiation IDs (list)", Description: "List of initiation UUIDs (bulk events)", IsList: true},
	{Key: "SweepID", Label: "Sweep Config ID", Description: "Parent sweep config UUID"},
	{Key: "Count", Label: "Count", Description: "Number of initiations affected"},

	// Create-specific: list of initiation summary objects
	{Key: "Initiations", Label: "Initiations (list)", IsList: true,
		Description: "List of {initiation_id, sweep_id, processing_status, auto_created_sweep} for SWEEPINIT_CREATE"},

	// Update-specific
	{Key: "Reason", Label: "Reason / Comment"},

	// Actors
	{Key: "RequestedBy", Label: "Requested By"},
	{Key: "CheckerBy", Label: "Checker (Approver/Rejecter)"},
	{Key: "CheckerComment", Label: "Checker Comment"},

	// Status / audit
	{Key: "Action", Label: "Action"},
	{Key: "Status", Label: "Status"},
	{Key: "ActionAt", Label: "Action Timestamp"},
}

// ─────────────────────────────────────────────────────────────────────────────
// SWEEP_EXEC variables
// Source file: api/cash/sweepConfig/sweepExecutorV2.go
// ─────────────────────────────────────────────────────────────────────────────

var SweepExecVariables = []NotifVariable{
	// Identity
	{Key: "SweepID", Label: "Sweep Config ID"},

	// Single manual trigger (SWEEPEXEC_MANUAL)
	{Key: "ExecutionResult", Label: "Execution Result", Description: "Result of a single manual sweep execution"},

	// Bulk trigger (SWEEPEXEC_BULK)
	{Key: "Results", Label: "Results (list)", IsList: true, Description: "Per-sweep execution results for bulk trigger"},
	{Key: "Total", Label: "Total Sweeps"},
	{Key: "Successful", Label: "Successful Sweeps"},
	{Key: "Failed", Label: "Failed Sweeps"},

	// Actors / audit
	{Key: "RequestedBy", Label: "Requested By"},
	{Key: "Action", Label: "Action"},
	{Key: "ActionAt", Label: "Action Timestamp"},
}

// ─────────────────────────────────────────────────────────────────────────────
// SWEEP (automated processor) variables
// Source file: internal/jobs/sweep_processorV2.go
// ─────────────────────────────────────────────────────────────────────────────

var SweepExecutedVariables = []NotifVariable{
	{Key: "SweepID", Label: "Sweep Config ID"},
	{Key: "InitiationID", Label: "Initiation ID"},
	{Key: "FromAccount", Label: "Source Account"},
	{Key: "ToAccount", Label: "Destination Account"},
	{Key: "SweepType", Label: "Sweep Type"},
	{Key: "AmountSwept", Label: "Amount Swept"},
	{Key: "BalanceBefore", Label: "Balance Before Sweep"},
	{Key: "BalanceAfter", Label: "Balance After Sweep"},
	{Key: "BufferAmount", Label: "Buffer Amount"},
	{Key: "ExecutedAt", Label: "Executed At"},
	{Key: "Status", Label: "Status"},
}

// ─────────────────────────────────────────────────────────────────────────────
// PROJECTION variables
// Source files: api/cash/projection/cashFlowProjectionV2.go
//               api/cash/projection/uploadProjectionV2.go
// ─────────────────────────────────────────────────────────────────────────────

var ProjectionVariables = []NotifVariable{
	// Identity
	{Key: "ProposalID", Label: "Proposal ID"},
	{Key: "ProposalIDs", Label: "Proposal IDs (list)", IsList: true, Description: "Bulk operations"},
	{Key: "ProposalName", Label: "Proposal Name"},
	{Key: "Count", Label: "Count", Description: "Number of proposals affected"},
	{Key: "DeletedIDs", Label: "Deleted IDs (list)", IsList: true, Description: "IDs removed on approve-of-delete"},

	// Content
	{Key: "ItemCount", Label: "Item Count", Description: "Number of line items in the proposal"},

	// Actors
	{Key: "CreatedBy", Label: "Created By"},
	{Key: "UpdatedBy", Label: "Updated By"},
	{Key: "RequestedBy", Label: "Requested By"},
	{Key: "CheckerBy", Label: "Checker (Approver/Rejecter)"},
	{Key: "CheckerComment", Label: "Checker Comment"},
	{Key: "UploadedBy", Label: "Uploaded By"},
	{Key: "Reason", Label: "Reason / Comment"},

	// Upload-specific
	{Key: "FileName", Label: "File Name"},
	{Key: "ImportedRows", Label: "Imported Rows", Description: "Number of rows imported from the uploaded file"},

	// Status / audit
	{Key: "Action", Label: "Action"},
	{Key: "Status", Label: "Status"},
	{Key: "ActionAt", Label: "Action Timestamp"},
}

// ─────────────────────────────────────────────────────────────────────────────
// VariablesForSubModule returns the canonical variable list for the given
// sub_module_code. Returns nil if the sub-module is unknown.
// ─────────────────────────────────────────────────────────────────────────────

func VariablesForSubModule(subModule string) []NotifVariable {
	switch subModule {
	case SubModuleBankStatement:
		return BankStatementVariables
	case SubModuleBankLimit:
		return BankLimitVariables
	case SubModuleLimitUtil:
		return LimitUtilizationVariables
	case SubModuleSweepCfg:
		return SweepConfigVariables
	case SubModuleSweepInit:
		return SweepInitVariables
	case SubModuleSweepExec:
		return SweepExecVariables
	case SubModuleSweep:
		return SweepExecutedVariables
	case SubModuleProjection:
		return ProjectionVariables
	default:
		return nil
	}
}

// VariableKeysForSubModule returns only the Key strings for the given sub-module,
// useful for quick lookup / validation without the full NotifVariable struct.
func VariableKeysForSubModule(subModule string) []string {
	vars := VariablesForSubModule(subModule)
	if vars == nil {
		return nil
	}
	keys := make([]string, 0, len(vars))
	for _, v := range vars {
		keys = append(keys, v.Key)
	}
	return keys
}

// ─────────────────────────────────────────────────────────────────────────────
// EventPayloadKeys maps each event_code to the EXACT payload keys that
// TriggerNotification receives at runtime. These are the only variables
// guaranteed to be present when the template is evaluated for that event.
//
// For BS_COMMIT / BS_PREVIEW / BS_ZIP_UPLOAD the full BankStatementNotifPayload
// is sent (see notif_payload.go), so all BankStatementVariables keys apply.
// ─────────────────────────────────────────────────────────────────────────────

var EventPayloadKeys = map[string][]string{
	// ── BANK_STATEMENT ────────────────────────────────────────────────────────
	EventBSApprove: {
		"BankStatementID", "AccountNumber", "BankName", "CurrencyCode",
		"OpeningBalance", "ClosingBalance", "StatementPeriodEnd", "ApprovedBy",
	},
	EventBSReject: {
		"BankStatementIDs", "Count", "UserID", "Comment", "Action", "ActionAt",
	},
	EventBSDelete: {
		"BankStatementIDs", "Count", "UserID", "Comment", "Action", "ActionAt",
	},
	// Full BankStatementNotifPayload.ToMap() — all scalar + list vars
	EventBSCommit: {
		"BankStatementID", "AccountNumber", "AccountName", "BankName", "IFSC",
		"CurrencyCode", "PeriodStart", "PeriodEnd", "OpeningBalance", "ClosingBalance",
		"TotalTransactions", "TotalDebit", "TotalCredit", "NetFlow",
		"CategorizedCount", "UncategorizedCount", "CategorizedPercent", "UncategorizedPercent",
		"UploadedBy", "ApprovedBy", "FileName", "UploadedAt", "EntityID", "Status",
		// list keys (use TABLE_HTML / LIST_HTML in templates)
		"Transactions", "UncategorizedTransactions", "CreditTransactions", "DebitTransactions", "CategoryKPIs",
	},
	EventBSPreview: {
		"BankStatementID", "AccountNumber", "AccountName", "BankName", "IFSC",
		"CurrencyCode", "PeriodStart", "PeriodEnd", "OpeningBalance", "ClosingBalance",
		"TotalTransactions", "TotalDebit", "TotalCredit", "NetFlow",
		"CategorizedCount", "UncategorizedCount", "CategorizedPercent", "UncategorizedPercent",
		"UploadedBy", "ApprovedBy", "FileName", "UploadedAt", "EntityID", "Status",
		"Transactions", "UncategorizedTransactions", "CreditTransactions", "DebitTransactions", "CategoryKPIs",
	},
	EventBSZipUpload: {
		"BankStatementID", "AccountNumber", "AccountName", "BankName", "IFSC",
		"CurrencyCode", "PeriodStart", "PeriodEnd", "OpeningBalance", "ClosingBalance",
		"TotalTransactions", "TotalDebit", "TotalCredit", "NetFlow",
		"CategorizedCount", "UncategorizedCount", "CategorizedPercent", "UncategorizedPercent",
		"UploadedBy", "ApprovedBy", "FileName", "UploadedAt", "EntityID", "Status",
		"Transactions", "UncategorizedTransactions", "CreditTransactions", "DebitTransactions", "CategoryKPIs",
	},

	// ── BANK_LIMIT ────────────────────────────────────────────────────────────
	EventLimitBulkCreate: {
		"RequestedBy", "Count", "Action", "Status", "ActionAt",
	},
	EventLimitUpdated: {
		"LimitID", "RequestedBy", "Action", "Status", "ActionAt",
	},
	EventLimitDeleteRequest: {
		"LimitIDs", "Count", "RequestedBy", "Action", "Status", "ActionAt",
	},
	EventLimitApproved: {
		"LimitIDs", "Count", "ApprovedBy", "DeletedIDs", "Action", "Status", "ActionAt",
	},
	EventLimitRejected: {
		"LimitIDs", "Count", "RejectedBy", "Action", "Status", "ActionAt",
	},

	// ── LIMIT_UTILIZATION ─────────────────────────────────────────────────────
	EventUtilBulkCreate: {
		"RequestedBy", "Count", "Action", "Status", "ActionAt",
	},
	EventUtilUpdated: {
		"UtilizationID", "RequestedBy", "Action", "Status", "ActionAt",
	},
	EventUtilDeleteRequest: {
		"UtilizationIDs", "Count", "RequestedBy", "Action", "Status", "ActionAt",
	},
	EventUtilApproved: {
		"UtilizationIDs", "Count", "ApprovedBy", "DeletedIDs", "Action", "Status", "ActionAt",
	},
	EventUtilRejected: {
		"UtilizationIDs", "Count", "RejectedBy", "Action", "Status", "ActionAt",
	},
	EventUtilFileUploaded: {
		"UploadedBy", "FileName", "RowsUploaded", "Action", "Status", "ActionAt",
	},

	// ── SWEEP_CFG ─────────────────────────────────────────────────────────────
	EventSweepCfgCreate: {
		"SweepIDs", "Count", "RequestedBy", "Action", "Status", "ActionAt",
	},
	EventSweepCfgUpdate: {
		"SweepID", "RequestedBy", "Reason", "Fields", "Action", "Status", "ActionAt",
	},
	EventSweepCfgApprove: {
		"SweepIDs", "Count", "DeletedIDs", "CheckerBy", "CheckerComment", "Action", "ActionAt",
	},
	EventSweepCfgReject: {
		"SweepIDs", "Count", "CheckerBy", "CheckerComment", "Action", "ActionAt",
	},
	EventSweepCfgDelete: {
		"SweepIDs", "Count", "RequestedBy", "Reason", "Action", "ActionAt",
	},

	// ── SWEEP_INIT ────────────────────────────────────────────────────────────
	EventSweepInitCreate: {
		"Initiations", "Count", "RequestedBy", "Action", "Status", "ActionAt",
	},
	EventSweepInitUpdate: {
		"InitiationID", "SweepID", "RequestedBy", "Reason", "Action", "Status", "ActionAt",
	},
	EventSweepInitApprove: {
		"InitiationIDs", "Count", "CheckerBy", "CheckerComment", "Action", "ActionAt",
	},
	EventSweepInitReject: {
		"InitiationIDs", "Count", "CheckerBy", "CheckerComment", "Action", "ActionAt",
	},
	EventSweepInitDelete: {
		"InitiationIDs", "Count", "RequestedBy", "Action", "ActionAt",
	},

	// ── SWEEP_EXEC ────────────────────────────────────────────────────────────
	EventSweepExecManual: {
		"SweepID", "RequestedBy", "ExecutionResult", "Action", "ActionAt",
	},
	EventSweepExecBulk: {
		"Results", "Total", "Successful", "Failed", "RequestedBy", "Action", "ActionAt",
	},

	// ── SWEEP (automated processor) ───────────────────────────────────────────
	EventSweepExecuted: {
		"SweepID", "InitiationID", "FromAccount", "ToAccount", "SweepType",
		"AmountSwept", "BalanceBefore", "BalanceAfter", "BufferAmount", "ExecutedAt", "Status",
	},

	// ── PROJECTION ────────────────────────────────────────────────────────────
	EventProjCreate: {
		"ProposalID", "ProposalName", "ItemCount", "CreatedBy", "Action", "Status", "ActionAt",
	},
	EventProjUpdate: {
		"ProposalID", "ItemCount", "UpdatedBy", "Reason", "Action", "Status", "ActionAt",
	},
	EventProjDelete: {
		"ProposalIDs", "Count", "RequestedBy", "Reason", "Action", "ActionAt",
	},
	EventProjReject: {
		"ProposalIDs", "Count", "CheckerBy", "CheckerComment", "Action", "ActionAt",
	},
	EventProjApprove: {
		"ProposalIDs", "Count", "DeletedIDs", "CheckerBy", "CheckerComment", "Action", "ActionAt",
	},
	EventProjUpload: {
		"ProposalID", "ProposalName", "FileName", "ImportedRows", "UploadedBy", "Action", "Status", "ActionAt",
	},
}

// PayloadKeysForEvent returns the actual payload keys sent by TriggerNotification
// for the given event_code, or nil if the event code is unknown.
func PayloadKeysForEvent(eventCode string) []string {
	return EventPayloadKeys[eventCode]
}

// EventsForSubModule returns all event codes that belong to a given sub-module.
var subModuleEvents = map[string][]string{
	SubModuleBankStatement: {
		EventBSApprove, EventBSReject, EventBSDelete,
		EventBSCommit, EventBSPreview, EventBSZipUpload,
	},
	SubModuleBankLimit: {
		EventLimitBulkCreate, EventLimitUpdated, EventLimitDeleteRequest,
		EventLimitApproved, EventLimitRejected,
	},
	SubModuleLimitUtil: {
		EventUtilBulkCreate, EventUtilUpdated, EventUtilDeleteRequest,
		EventUtilApproved, EventUtilRejected, EventUtilFileUploaded,
	},
	SubModuleSweepCfg: {
		EventSweepCfgCreate, EventSweepCfgUpdate, EventSweepCfgApprove,
		EventSweepCfgReject, EventSweepCfgDelete,
	},
	SubModuleSweepInit: {
		EventSweepInitCreate, EventSweepInitUpdate, EventSweepInitApprove,
		EventSweepInitReject, EventSweepInitDelete,
	},
	SubModuleSweepExec: {
		EventSweepExecManual, EventSweepExecBulk,
	},
	SubModuleSweep: {
		EventSweepExecuted,
	},
	SubModuleProjection: {
		EventProjCreate, EventProjUpdate, EventProjDelete,
		EventProjReject, EventProjApprove, EventProjUpload,
	},
}

// EventsForSubModule returns all known event codes for the given sub_module_code.
func EventsForSubModule(subModule string) []string {
	return subModuleEvents[subModule]
}
