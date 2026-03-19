package approvalengine

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
	"FD_BOOKING":        {AuditTable: "investment.fd_audit_booking_request", AuditIDColumn: "booking_id"},
	"FD_BOOKING_EDIT":   {AuditTable: "investment.fd_audit_booking_request", AuditIDColumn: "booking_id"},
	"FD_BOOKING_DELETE": {AuditTable: "investment.fd_audit_booking_request", AuditIDColumn: "booking_id"},

	// ── FD Confirmation ───────────────────────────────────────────────────
	"FD_CONFIRMATION_CREATE":            {AuditTable: "investment.fd_audit_confirmation", AuditIDColumn: "confirmation_id"},
	"FD_CONFIRMATION_VARIANCE_RESOLVE":  {AuditTable: "investment.fd_audit_confirmation", AuditIDColumn: "confirmation_id"},

	// ── FD Master / Activation ────────────────────────────────────────────
	"FD_MASTER_CREATE": {AuditTable: "investment.fd_audit_master", AuditIDColumn: "fd_id"},

	// ── Cash / Payables & Receivables ─────────────────────────────────────
	// (add specific types here as the cash module is wired up)
}

// LookupTxTableConfig returns the audit table name and audit ID column for the
// given transaction type. If the type is not registered, both strings are empty.
func LookupTxTableConfig(transactionType string) (auditTable, auditIDColumn string) {
	if cfg, ok := txTypeRegistry[transactionType]; ok {
		return cfg.AuditTable, cfg.AuditIDColumn
	}
	return "", ""
}
