package approvalengine

import (
	"CimplrCorpSaas/api/constants"
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// txTableConfig holds the audit table and primary-key column name for a given
// transaction type. These values cannot be stored in uam.approval_instance
// (the DB schema does not have those columns), so they are looked up at
// runtime from this registry using the transaction_type that IS stored.
type txTableConfig struct {
	AuditTable    string
	AuditIDColumn string
}

// txTypeRegistry maps transaction_type strings (as passed to CreateInstance)
// to their corresponding audit table and PK column. Add new entries here
// whenever a new transaction type is introduced in any module.
var txTypeRegistry = map[string]txTableConfig{
	// ── Email Inbox configuration ─────────────────────────────────────────
	"EMAIL_INBOX_CREATE": {AuditTable: constants.EmailSvcInboxAudit, AuditIDColumn: "inbox_id"},
	"EMAIL_INBOX_EDIT":   {AuditTable: constants.EmailSvcInboxAudit, AuditIDColumn: "inbox_id"},
	"EMAIL_INBOX_DELETE": {AuditTable: constants.EmailSvcInboxAudit, AuditIDColumn: "inbox_id"},

	// ── FD Booking Workbench ──────────────────────────────────────────────
	"FD_BOOKING":        {AuditTable: constants.QuerryAuditBookingRequest, AuditIDColumn: "booking_id"},
	"FD_BOOKING_CREATE": {AuditTable: constants.QuerryAuditBookingRequest, AuditIDColumn: "booking_id"},
	"FD_BOOKING_EDIT":   {AuditTable: constants.QuerryAuditBookingRequest, AuditIDColumn: "booking_id"},
	"FD_BOOKING_DELETE": {AuditTable: constants.QuerryAuditBookingRequest, AuditIDColumn: "booking_id"},

	// ── FD Rate Negotiation ───────────────────────────────────────────────
	"FD_RATE_NEGOTIATION_CREATE": {AuditTable: constants.QuerryAuditRateNegotiation, AuditIDColumn: "rate_request_id"},
	"FD_RATE_NEGOTIATION_EDIT":   {AuditTable: constants.QuerryAuditRateNegotiation, AuditIDColumn: "rate_request_id"},
	"FD_RATE_NEGOTIATION_DELETE": {AuditTable: constants.QuerryAuditRateNegotiation, AuditIDColumn: "rate_request_id"},

	// ── FD Confirmation ───────────────────────────────────────────────────
	"FD_CONFIRMATION_CREATE":           {AuditTable: constants.QuerryAuditConfirmation, AuditIDColumn: "confirmation_id"},
	"FD_CONFIRMATION_EDIT":             {AuditTable: constants.QuerryAuditConfirmation, AuditIDColumn: "confirmation_id"},
	"FD_CONFIRMATION_DELETE":           {AuditTable: constants.QuerryAuditConfirmation, AuditIDColumn: "confirmation_id"},
	"FD_CONFIRMATION_VARIANCE_RESOLVE": {AuditTable: constants.QuerryAuditConfirmation, AuditIDColumn: "confirmation_id"},

	// ── FD Master / Activation ────────────────────────────────────────────
	"FD_ACTIVATION":      {AuditTable: constants.QuerryAuditMaster, AuditIDColumn: "fd_id"},
	"FD_ACTIVATE":        {AuditTable: constants.QuerryAuditMaster, AuditIDColumn: "fd_id"}, // legacy alias
	"FD_MASTER_CREATE":   {AuditTable: constants.QuerryAuditMaster, AuditIDColumn: "fd_id"}, // legacy activation type
	"FD_MASTER_EDIT":     {AuditTable: constants.QuerryAuditMaster, AuditIDColumn: "fd_id"},
	"FD_MASTER_DELETE":   {AuditTable: constants.QuerryAuditMaster, AuditIDColumn: "fd_id"},
	"FD_CASHFLOW_EDIT":   {AuditTable: constants.QuerryAuditCashflowSchedule, AuditIDColumn: "audit_id"},
	"FD_CASHFLOW_DELETE": {AuditTable: constants.QuerryAuditCashflowSchedule, AuditIDColumn: "audit_id"},

	// ── FD Accrual ────────────────────────────────────────────────────────
	"FD_ACCRUAL_RUN":              {AuditTable: "investment.fd_accrual_run_audit", AuditIDColumn: "run_id"},
	"FD_ACCRUAL_APPROVE":          {AuditTable: "investment.fd_accrual_run_audit", AuditIDColumn: "run_id"}, // legacy alias
	"FD_ACCRUAL_OVERRIDE":         {AuditTable: "investment.fd_accrual_ledger_audit", AuditIDColumn: "ledger_id"},
	"FD_ACCRUAL_SCHEDULE_CREATE":  {AuditTable: constants.QuerryAccrualScheduleConfigAudit, AuditIDColumn: "config_id"},
	"FD_ACCRUAL_SCHEDULE_EDIT":    {AuditTable: constants.QuerryAccrualScheduleConfigAudit, AuditIDColumn: "config_id"},
	"FD_ACCRUAL_SCHEDULE_DELETE":  {AuditTable: constants.QuerryAccrualScheduleConfigAudit, AuditIDColumn: "config_id"},
	"FD_ACCRUAL_SCHEDULE_APPROVE": {AuditTable: constants.QuerryAccrualScheduleConfigAudit, AuditIDColumn: "config_id"}, // legacy alias

	// ── FD Receipt ───────────────────────────────────────────────────────
	"FD_RECEIPT_CREATE":    {AuditTable: constants.QuerryAuditInterestReceipt, AuditIDColumn: "receipt_id"},
	"FD_RECEIPT_EDIT":      {AuditTable: constants.QuerryAuditInterestReceipt, AuditIDColumn: "receipt_id"},
	"FD_RECEIPT_DELETE":    {AuditTable: constants.QuerryAuditInterestReceipt, AuditIDColumn: "receipt_id"},
	"FD_TDS_EDIT":          {AuditTable: constants.QuerryAuditTDSReceipt, AuditIDColumn: "tds_id"},
	"FD_RECONCILE_CREATE":  {AuditTable: "investment.fd_receipt_reconcile_run", AuditIDColumn: "reconcile_run_id"},
	"FD_EXCEPTION_EDIT":    {AuditTable: constants.QuerryReceiptExceptionAudit, AuditIDColumn: "exception_id"},
	"FD_EXCEPTION_RESOLVE": {AuditTable: constants.QuerryReceiptExceptionAudit, AuditIDColumn: "exception_id"},
	"FD_EXCEPTION_CLOSE":   {AuditTable: constants.QuerryReceiptExceptionAudit, AuditIDColumn: "exception_id"},

	// ── FD TDS Register ──────────────────────────────────────────────────
	"FD_TDS_REGISTER_CREATE":    {AuditTable: constants.QuerryAuditTDSReceipt, AuditIDColumn: "tds_id"},
	"FD_TDS_REGISTER_EDIT":      {AuditTable: constants.QuerryAuditTDSReceipt, AuditIDColumn: "tds_id"},
	"FD_TDS_REGISTER_DELETE":    {AuditTable: constants.QuerryAuditTDSReceipt, AuditIDColumn: "tds_id"},
	"FD_TDS_REGISTER_RECONCILE": {AuditTable: constants.QuerryAuditTDSReceipt, AuditIDColumn: "tds_id"},

	// ── Cash / Sweep Initiation ────────────────────────────────────────────
	"SWEEP_INITIATION_CREATE": {AuditTable: "cimplrcorpsaas.auditactionsweepinitiation", AuditIDColumn: "initiation_id"},
	"SWEEP_INITIATION_EDIT":   {AuditTable: "cimplrcorpsaas.auditactionsweepinitiation", AuditIDColumn: "initiation_id"},
	"SWEEP_INITIATION_DELETE": {AuditTable: "cimplrcorpsaas.auditactionsweepinitiation", AuditIDColumn: "initiation_id"},

	// ── Cash / Sweep Configuration ────────────────────────────────────────
	"SWEEP_CONFIG_CREATE": {AuditTable: "cimplrcorpsaas.auditactionsweepconfiguration", AuditIDColumn: "sweep_id"},
	"SWEEP_CONFIG_EDIT":   {AuditTable: "cimplrcorpsaas.auditactionsweepconfiguration", AuditIDColumn: "sweep_id"},
	"SWEEP_CONFIG_DELETE": {AuditTable: "cimplrcorpsaas.auditactionsweepconfiguration", AuditIDColumn: "sweep_id"},

	// ── Cash / Payables & Receivables ─────────────────────────────────────
	// (add specific types here as the cash module is wired up further)

	// ── Cash / Fund Planning ──────────────────────────────────────────────
	"FUND_PLANNING_CREATE": {AuditTable: "public.auditaction_fund_plan_groups", AuditIDColumn: "group_id"},
	"FUND_PLANNING_EDIT":   {AuditTable: "public.auditaction_fund_plan_groups", AuditIDColumn: "group_id"},
	"FUND_PLANNING_DELETE": {AuditTable: "public.auditaction_fund_plan_groups", AuditIDColumn: "group_id"},

	// ── Cash / Bank Statement ─────────────────────────────────────────────
	"BANK_STATEMENT_CREATE": {AuditTable: "public.auditactionbankstatement", AuditIDColumn: "bankstatementid"},
	"BANK_STATEMENT_EDIT":   {AuditTable: "public.auditactionbankstatement", AuditIDColumn: "bankstatementid"},
	"BANK_STATEMENT_DELETE": {AuditTable: "public.auditactionbankstatement", AuditIDColumn: "bankstatementid"},

	// ── Cash / Cash Flow Projection ───────────────────────────────────────
	"CASH_FLOW_PROJECTION_CREATE": {AuditTable: "cimplrcorpsaas.audit_action_cashflow_proposal", AuditIDColumn: "proposal_id"},
	"CASH_FLOW_PROJECTION_EDIT":   {AuditTable: "cimplrcorpsaas.audit_action_cashflow_proposal", AuditIDColumn: "proposal_id"},
	"CASH_FLOW_PROJECTION_DELETE": {AuditTable: "cimplrcorpsaas.audit_action_cashflow_proposal", AuditIDColumn: "proposal_id"},

	// ── Cash / Bank Balances ──────────────────────────────────────────────
	"BANK_BALANCE_CREATE": {AuditTable: "public.auditactionbankbalances", AuditIDColumn: "balance_id"},
	"BANK_BALANCE_EDIT":   {AuditTable: "public.auditactionbankbalances", AuditIDColumn: "balance_id"},
	"BANK_BALANCE_DELETE": {AuditTable: "public.auditactionbankbalances", AuditIDColumn: "balance_id"},

	// ── Cash / Bank Limits ────────────────────────────────────────────────
	"BANK_LIMIT_CREATE": {AuditTable: "cimplrcorpsaas.auditactionbanklimit", AuditIDColumn: "limit_id"},
	"BANK_LIMIT_EDIT":   {AuditTable: "cimplrcorpsaas.auditactionbanklimit", AuditIDColumn: "limit_id"},
	"BANK_LIMIT_DELETE": {AuditTable: "cimplrcorpsaas.auditactionbanklimit", AuditIDColumn: "limit_id"},

	// ── Cash / Limit Utilization ──────────────────────────────────────────
	"LIMIT_UTILIZATION_CREATE": {AuditTable: "cimplrcorpsaas.auditactionbanklimitutilization", AuditIDColumn: "utilization_id"},
	"LIMIT_UTILIZATION_EDIT":   {AuditTable: "cimplrcorpsaas.auditactionbanklimitutilization", AuditIDColumn: "utilization_id"},
	"LIMIT_UTILIZATION_DELETE": {AuditTable: "cimplrcorpsaas.auditactionbanklimitutilization", AuditIDColumn: "utilization_id"},
	// ── Cash / Payables & Receivables ─────────────────────────────────────
	"PAYABLE_CREATE": {AuditTable: "public.auditactionpayable", AuditIDColumn: "payable_id"},
	"PAYABLE_EDIT":   {AuditTable: "public.auditactionpayable", AuditIDColumn: "payable_id"},
	"PAYABLE_DELETE": {AuditTable: "public.auditactionpayable", AuditIDColumn: "payable_id"},

	"RECEIVABLE_CREATE": {AuditTable: "public.auditactionreceivable", AuditIDColumn: "receivable_id"},
	"RECEIVABLE_EDIT":   {AuditTable: "public.auditactionreceivable", AuditIDColumn: "receivable_id"},
	"RECEIVABLE_DELETE": {AuditTable: "public.auditactionreceivable", AuditIDColumn: "receivable_id"},

	// ── FX ───────────────────────────────────────────────────────────────
	"FX_EXPOSURE_CREATE": {AuditTable: "public.auditactionexposure", AuditIDColumn: "exposure_header_id"},
	"FX_EXPOSURE_EDIT":   {AuditTable: "public.auditactionexposure", AuditIDColumn: "exposure_header_id"},
	"FX_EXPOSURE_DELETE": {AuditTable: "public.auditactionexposure", AuditIDColumn: "exposure_header_id"},

	"FX_BUCKETING_CREATE": {AuditTable: "public.auditactionexposurebucketing", AuditIDColumn: "exposure_header_id"},
	"FX_BUCKETING_EDIT":   {AuditTable: "public.auditactionexposurebucketing", AuditIDColumn: "exposure_header_id"},
	"FX_BUCKETING_DELETE": {AuditTable: "public.auditactionexposurebucketing", AuditIDColumn: "exposure_header_id"},

	"FX_LINKAGE_CREATE": {AuditTable: "public.auditactionhedgelink", AuditIDColumn: "exposure_header_id"},
	"FX_LINKAGE_EDIT":   {AuditTable: "public.auditactionhedgelink", AuditIDColumn: "exposure_header_id"},
	"FX_LINKAGE_DELETE": {AuditTable: "public.auditactionhedgelink", AuditIDColumn: "exposure_header_id"},

	"FX_FORWARD_CREATE":              {AuditTable: "public.auditactionforwardbooking", AuditIDColumn: "system_transaction_id"},
	"FX_FORWARD_EDIT":                {AuditTable: "public.auditactionforwardbooking", AuditIDColumn: "system_transaction_id"},
	"FX_FORWARD_DELETE":              {AuditTable: "public.auditactionforwardbooking", AuditIDColumn: "system_transaction_id"},
	"FX_FORWARD_CONFIRMATION_CREATE": {AuditTable: "public.auditactionforwardbooking", AuditIDColumn: "system_transaction_id"},

	"FX_FORWARD_ROLLOVER":     {AuditTable: "public.auditactionforwardrollover", AuditIDColumn: "booking_id"},
	"FX_FORWARD_CANCELLATION": {AuditTable: "public.auditactionforwardcancellation", AuditIDColumn: "booking_id"},

	"FX_HEDGE_PROPOSAL_CREATE": {AuditTable: "public.auditactionhedgingproposaldocument", AuditIDColumn: "proposal_id"},
	"FX_HEDGE_PROPOSAL_EDIT":   {AuditTable: "public.auditactionhedgingproposaldocument", AuditIDColumn: "proposal_id"},
	"FX_HEDGE_PROPOSAL_DELETE": {AuditTable: "public.auditactionhedgingproposaldocument", AuditIDColumn: "proposal_id"},

	"FX_SETTLEMENT_CREATE": {AuditTable: "public.auditactionexposuresettlement", AuditIDColumn: "settlement_id"},
	"FX_SETTLEMENT_EDIT":   {AuditTable: "public.auditactionexposuresettlement", AuditIDColumn: "settlement_id"},
	"FX_SETTLEMENT_DELETE": {AuditTable: "public.auditactionexposuresettlement", AuditIDColumn: "settlement_id"},

	"FX_SETTLEMENT_ROLLOVER":     {AuditTable: "public.auditactionexposuresettlement", AuditIDColumn: "settlement_id"},
	"FX_SETTLEMENT_CANCELLATION": {AuditTable: "public.auditactionexposuresettlement", AuditIDColumn: "settlement_id"},

	"FX_MTM_UPDATE": {AuditTable: "public.auditactionforwardmtm", AuditIDColumn: "mtm_id"},

	// ── Counterparty Hub ──────────────────────────────────────────────────
	"COUNTERPARTY_CREATE":        {AuditTable: constants.ErrAuditCounterpartyServiceTable, AuditIDColumn: "counterparty_id"},
	"COUNTERPARTY_EDIT":          {AuditTable: constants.ErrAuditCounterpartyServiceTable, AuditIDColumn: "counterparty_id"},
	"COUNTERPARTY_MASTER_CREATE": {AuditTable: constants.ErrAuditCounterpartyServiceTable, AuditIDColumn: "counterparty_id"},
	"COUNTERPARTY_MASTER_EDIT":   {AuditTable: constants.ErrAuditCounterpartyServiceTable, AuditIDColumn: "counterparty_id"},
	"BANK_CREATE":                {AuditTable: "apibox_svc.audit_bank_master", AuditIDColumn: "bank_id"},
	"EXCHANGE_MASTER_CREATE":     {AuditTable: "apibox_svc.audit_exchange_master", AuditIDColumn: "exchange_id"},
	"DATA_PROVIDER_CREATE":       {AuditTable: "apibox_svc.audit_data_provider_master", AuditIDColumn: "provider_id"},
	"CCP_CSD_CREATE":             {AuditTable: "apibox_svc.audit_ccp_csd_master", AuditIDColumn: "ccp_csd_id"},
	"PAYMENT_NETWORK_CREATE":     {AuditTable: "apibox_svc.audit_payment_network_master", AuditIDColumn: "network_id"},
	"ERP_SYSTEM_CREATE":          {AuditTable: "apibox_svc.audit_erp_system_master", AuditIDColumn: "erp_id"},

	// ── FD Closure ───────────────────────────────────────────────────────
	"FD_CLOSURE_MATURITY":     {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},
	"FD_CLOSURE_PREMATURE":    {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},
	"FD_CLOSURE_ROLLOVER":     {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},
	"FD_CLOSURE_AUTO_RENEWAL": {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},
	"FD_CLOSURE_DELETE":       {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},

	// ── FD Closure / Maturity & Rollover (cimplr schema) ─────────────────
	"FD_CLOSURE_INITIATE_PAYOUT_CREATE":   {AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id"},
	"FD_CLOSURE_INITIATE_PAYOUT_EDIT":     {AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id"},
	"FD_CLOSURE_INITIATE_PAYOUT_DELETE":   {AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id"},
	"FD_CLOSURE_INITIATE_ROLLOVER_CREATE": {AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id"},
	"FD_CLOSURE_INITIATE_ROLLOVER_EDIT":   {AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id"},
	"FD_CLOSURE_INITIATE_ROLLOVER_DELETE": {AuditTable: constants.QuerryAuditClosureInitiate, AuditIDColumn: "closure_initiate_id"},

	"FD_CLOSURE_CONFIRM_PAYOUT_CREATE":   {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},
	"FD_CLOSURE_CONFIRM_PAYOUT_EDIT":     {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},
	"FD_CLOSURE_CONFIRM_PAYOUT_DELETE":   {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},
	"FD_CLOSURE_CONFIRM_ROLLOVER_CREATE": {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},
	"FD_CLOSURE_CONFIRM_ROLLOVER_EDIT":   {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},
	"FD_CLOSURE_CONFIRM_ROLLOVER_DELETE": {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},

	"FD_CLOSURE_PREMATURE_CREATE": {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},
	"FD_CLOSURE_PREMATURE_EDIT":   {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},
	"FD_CLOSURE_PREMATURE_DELETE": {AuditTable: constants.QuerryAuditClosureConfirmAudit, AuditIDColumn: "closure_confirm_id"},

	// ── Mutual Fund ───────────────────────────────────────────────────────
	"MF_PROPOSAL_CREATE": {AuditTable: "investment.auditactionproposal", AuditIDColumn: "proposal_id"},
	"MF_PROPOSAL_EDIT":   {AuditTable: "investment.auditactionproposal", AuditIDColumn: "proposal_id"},
	"MF_PROPOSAL_DELETE": {AuditTable: "investment.auditactionproposal", AuditIDColumn: "proposal_id"},

	"MF_INITIATION_CREATE": {AuditTable: "investment.auditactioninitiation", AuditIDColumn: "initiation_id"},
	"MF_INITIATION_EDIT":   {AuditTable: "investment.auditactioninitiation", AuditIDColumn: "initiation_id"},
	"MF_INITIATION_DELETE": {AuditTable: "investment.auditactioninitiation", AuditIDColumn: "initiation_id"},

	"MF_CONFIRMATION_CREATE": {AuditTable: "investment.auditactioninvestmentconfirmation", AuditIDColumn: "confirmation_id"},
	"MF_CONFIRMATION_EDIT":   {AuditTable: "investment.auditactioninvestmentconfirmation", AuditIDColumn: "confirmation_id"},
	"MF_CONFIRMATION_DELETE": {AuditTable: "investment.auditactioninvestmentconfirmation", AuditIDColumn: "confirmation_id"},

	"MF_REDEMPTION_INITIATION_CREATE": {AuditTable: "investment.auditactionredemption", AuditIDColumn: "redemption_id"},
	"MF_REDEMPTION_INITIATION_EDIT":   {AuditTable: "investment.auditactionredemption", AuditIDColumn: "redemption_id"},
	"MF_REDEMPTION_INITIATION_DELETE": {AuditTable: "investment.auditactionredemption", AuditIDColumn: "redemption_id"},

	"MF_REDEMPTION_CONFIRMATION_CREATE": {AuditTable: "investment.auditactionredemptionconfirmation", AuditIDColumn: "redemption_confirm_id"},
	"MF_REDEMPTION_CONFIRMATION_EDIT":   {AuditTable: "investment.auditactionredemptionconfirmation", AuditIDColumn: "redemption_confirm_id"},
	"MF_REDEMPTION_CONFIRMATION_DELETE": {AuditTable: "investment.auditactionredemptionconfirmation", AuditIDColumn: "redemption_confirm_id"},
}

// LookupTxTableConfig returns the audit table name and audit ID column for the
// given transaction type. If the type is not registered, both strings are empty.
func LookupTxTableConfig(transactionType string) (auditTable, auditIDColumn string) {
	if cfg, ok := txTypeRegistry[transactionType]; ok {
		return cfg.AuditTable, cfg.AuditIDColumn
	}
	return "", ""
}

// ─── Post-finalize hooks ──────────────────────────────────────────────────────

// PostFinalizeFunc is called by RecordAction after an instance is fully resolved
// (all eyes approved, or one rejected). Runs OUTSIDE the approval engine
// transaction so modules can do their own DB work (e.g. updating closure_status).
type PostFinalizeFunc func(ctx context.Context, pool *pgxpool.Pool, recordID, transactionType, finalStatus, actorEmail, comment string)

// postFinalizeRegistry maps transaction_type → hook.
var postFinalizeRegistry = map[string]PostFinalizeFunc{}

// RegisterPostFinalizeHook lets a module register a callback to be invoked
// after the engine fully resolves an instance of the given transactionType.
func RegisterPostFinalizeHook(transactionType string, fn PostFinalizeFunc) {
	postFinalizeRegistry[transactionType] = fn
}

// RunPostFinalizeHook executes the registered hook (if any) for transactionType.
func RunPostFinalizeHook(ctx context.Context, pool *pgxpool.Pool, transactionType, recordID, finalStatus, actorEmail, comment string) {
	if fn, ok := postFinalizeRegistry[transactionType]; ok {
		fn(ctx, pool, recordID, transactionType, finalStatus, actorEmail, comment)
	}
}
