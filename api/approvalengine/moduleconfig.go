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
	// ── FD Booking Workbench ──────────────────────────────────────────────
	"FD_BOOKING":        {AuditTable: constants.QuerryAuditBookingRequest, AuditIDColumn: "booking_id"},
	"FD_BOOKING_CREATE": {AuditTable: constants.QuerryAuditBookingRequest, AuditIDColumn: "booking_id"},
	"FD_BOOKING_EDIT":   {AuditTable: constants.QuerryAuditBookingRequest, AuditIDColumn: "booking_id"},
	"FD_BOOKING_DELETE": {AuditTable: constants.QuerryAuditBookingRequest, AuditIDColumn: "booking_id"},

        // ── FD Confirmation ───────────────────────────────────────────────────
        "FD_CONFIRMATION_CREATE":           {AuditTable: constants.QuerryAuditConfirmation, AuditIDColumn: "confirmation_id"},
        "FD_CONFIRMATION_EDIT":             {AuditTable: constants.QuerryAuditConfirmation, AuditIDColumn: "confirmation_id"},
        "FD_CONFIRMATION_DELETE":           {AuditTable: constants.QuerryAuditConfirmation, AuditIDColumn: "confirmation_id"},
        "FD_CONFIRMATION_VARIANCE_RESOLVE": {AuditTable: constants.QuerryAuditConfirmation, AuditIDColumn: "confirmation_id"},

        // ── FD Master / Activation ────────────────────────────────────────────
        "FD_MASTER_CREATE": {AuditTable: "investment.fd_audit_master", AuditIDColumn: "fd_id"},
        "FD_MASTER_EDIT":   {AuditTable: "investment.fd_audit_master", AuditIDColumn: "fd_id"},
        "FD_MASTER_DELETE": {AuditTable: "investment.fd_audit_master", AuditIDColumn: "fd_id"},
        "FD_ACTIVATE":      {AuditTable: "investment.fd_audit_master", AuditIDColumn: "fd_id"},

        // ── FD Accrual ────────────────────────────────────────────────────────
        "FD_ACCRUAL_APPROVE":          {AuditTable: "investment.fd_accrual_run_audit", AuditIDColumn: "run_id"},
        "FD_ACCRUAL_SCHEDULE_APPROVE": {AuditTable: "investment.fd_accrual_schedule_config_audit", AuditIDColumn: "config_id"},

        // ── FD Receipt ───────────────────────────────────────────────────────
        "FD_RECEIPT_APPROVE": {AuditTable: constants.QuerryAuditInterestReceipt, AuditIDColumn: "receipt_id"},
        "FD_RECEIPT_EDIT":    {AuditTable: constants.QuerryAuditInterestReceipt, AuditIDColumn: "receipt_id"},
        "FD_RECEIPT_DELETE":  {AuditTable: constants.QuerryAuditInterestReceipt, AuditIDColumn: "receipt_id"},

        // ── FD TDS Register ──────────────────────────────────────────────────
        "FD_TDS_REGISTER_CREATE":     {AuditTable: "investment.fd_tds_receipt_audit", AuditIDColumn: "tds_id"},
        "FD_TDS_REGISTER_RECONCILE":  {AuditTable: "investment.fd_tds_receipt_audit", AuditIDColumn: "tds_id"},

        // ── Cash / Payables & Receivables ─────────────────────────────────────
        // (add specific types here as the cash module is wired up)
        // ── FD Closure ───────────────────────────────────────────────────────
        "FD_CLOSURE_MATURITY":     {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},
        "FD_CLOSURE_PREMATURE":    {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},
        "FD_CLOSURE_ROLLOVER":     {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},
        "FD_CLOSURE_AUTO_RENEWAL": {AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id"},
}// LookupTxTableConfig returns the audit table name and audit ID column for the
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
