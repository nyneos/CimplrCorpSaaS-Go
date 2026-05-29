package dependency

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ChildCheck describes one child relationship. IsCore=true means its existence
// blocks deletion of every ancestor. IsCore=false means it can be cascade-deleted
// (with an audit row) when the parent has no core records below it.
type ChildCheck struct {
	Schema     string // empty = public
	Table      string
	FKCol      string
	Label      string // shown in error messages
	IsCore     bool   // true → block | false → cascade soft-delete
	AuditTable string // only for IsCore=false: where the audit row goes
	AuditFKCol string // PK column of Table, used to write the audit row
}

// BlockingDep is returned when a core record prevents deletion.
type BlockingDep struct {
	Label string
	Count int
}

// DependencyGraph maps a resource type string to its direct children.
// Resource type strings match the master table names (without schema), e.g. "masterbank".
// Add new entries here whenever a new master or core table is introduced.
var DependencyGraph = map[string][]ChildCheck{

	// ── Global: Bank ────────────────────────────────────────────────────────
	"masterbank": {
		// Master children (cascade-deletable when no core below)
		{Table: "masterbankaccount", FKCol: "bank_id", Label: "Bank Accounts",
			IsCore: false, AuditTable: "auditactionbankaccount", AuditFKCol: "account_id"},
		// FD sub-masters that reference bank by bank_code — cascade-deletable
		{Schema: "investment", Table: "fd_bank_config_master", FKCol: "bank_code", Label: "FD Bank Configs",
			IsCore: false, AuditTable: "investment.fd_audit_bank_config", AuditFKCol: "config_id"},
		{Schema: "investment", Table: "fd_bank_rate_card_master", FKCol: "bank_code", Label: "FD Rate Cards",
			IsCore: false, AuditTable: "investment.fd_audit_bank_rate_card", AuditFKCol: "rate_card_id"},
		{Schema: "investment", Table: "fd_penalty_structure_master", FKCol: "bank_code", Label: "Penalty Structures",
			IsCore: false, AuditTable: "investment.fd_audit_penalty_structure", AuditFKCol: "penalty_id"},
	},

	// ── Global: Currency ────────────────────────────────────────────────────
	// Currency has no direct children in current schema — standalone delete allowed.

	// ── Entity ──────────────────────────────────────────────────────────────
	"masterentitycash": {
		{Table: "masterbankaccount", FKCol: "entity_id", Label: "Bank Accounts",
			IsCore: false, AuditTable: "auditactionbankaccount", AuditFKCol: "account_id"},
		{Table: "masterglaccount", FKCol: "entity_id", Label: "GL Accounts",
			IsCore: false, AuditTable: "auditactionglaccount", AuditFKCol: "gl_account_id"},
		{Schema: "investment", Table: "masterfolio", FKCol: "entity_id", Label: "Folios",
			IsCore: false, AuditTable: "investment.auditactionfolio", AuditFKCol: "folio_id"},
		{Schema: "investment", Table: "masterdemataccount", FKCol: "entity_id", Label: "Demat Accounts",
			IsCore: false, AuditTable: "investment.auditactiondemat", AuditFKCol: "demat_id"},
	},

	// ── Bank Account ────────────────────────────────────────────────────────
	// Bank account is referenced by FD booking (core) and bank statements (core).
	"masterbankaccount": {
		// FD booking is CORE — blocks deletion
		{Schema: "investment", Table: "fd_booking_request", FKCol: "source_account_id",
			Label: "Fixed Deposit Bookings", IsCore: true},
		// Bank statements are CORE
		{Table: "bank_statements", FKCol: "bank_account_id",
			Label: "Bank Statements", IsCore: true},
		// Cash transactions are CORE
		{Table: "cash_transactions", FKCol: "bank_account_id",
			Label: "Cash Transactions", IsCore: true},
		// Bank balances — cascade-deletable
		{Table: "bank_balances", FKCol: "bank_account_id", Label: "Bank Balances",
			IsCore: false, AuditTable: "bank_balance_audit", AuditFKCol: "balance_id"},
	},

	// ── GL Account ──────────────────────────────────────────────────────────
	// GL accounts used in journal entries → CORE
	"masterglaccount": {
		{Schema: "investment", Table: "accounting_journal_entry", FKCol: "gl_account_id",
			Label: "Journal Entries", IsCore: true},
		{Table: "cash_gl_postings", FKCol: "gl_account_id",
			Label: "Cash GL Postings", IsCore: true},
	},

	// ── Investment MF: AMC ──────────────────────────────────────────────────
	"masteramc": {
		{Schema: "investment", Table: "masterscheme", FKCol: "amc_id", Label: "Schemes",
			IsCore: false, AuditTable: "investment.auditactionscheme", AuditFKCol: "scheme_id"},
	},

	// ── Investment MF: Scheme ───────────────────────────────────────────────
	"masterscheme": {
		{Schema: "investment", Table: "folioschememapping", FKCol: "scheme_id", Label: "Folio Mappings",
			IsCore: false, AuditTable: "investment.auditactionfolio", AuditFKCol: "folio_id"},
		// MF transactions are CORE
		{Schema: "investment", Table: "investment_initiation", FKCol: "scheme_id",
			Label: "MF Initiation Records", IsCore: true},
		{Schema: "investment", Table: "investment_confirmation", FKCol: "scheme_id",
			Label: "MF Confirmation Records", IsCore: true},
	},

	// ── Investment MF: DP ───────────────────────────────────────────────────
	"masterdepositoryparticipant": {
		{Schema: "investment", Table: "masterdemataccount", FKCol: "dp_id", Label: "Demat Accounts",
			IsCore: false, AuditTable: "investment.auditactiondemat", AuditFKCol: "demat_id"},
	},

	// ── Investment MF: Demat ────────────────────────────────────────────────
	"masterdemataccount": {
		// Equity holdings / redemption transactions are CORE
		{Schema: "investment", Table: "investment_redemption_initiation", FKCol: "demat_id",
			Label: "Redemption Records", IsCore: true},
	},

	// ── Investment MF: Folio ────────────────────────────────────────────────
	"masterfolio": {
		{Schema: "investment", Table: "investment_initiation", FKCol: "folio_id",
			Label: "MF Initiation Records", IsCore: true},
		{Schema: "investment", Table: "investment_confirmation", FKCol: "folio_id",
			Label: "MF Confirmation Records", IsCore: true},
	},

	// ── Investment FD: Bank Config ──────────────────────────────────────────
	"fd_bank_config_master": {
		{Schema: "investment", Table: "fd_booking_request", FKCol: "bank_config_id",
			Label: "FD Bookings", IsCore: true},
	},

	// ── Investment FD: Interest Type ────────────────────────────────────────
	"fd_interest_type_master": {
		{Schema: "investment", Table: "fd_booking_request", FKCol: "interest_type_id",
			Label: "FD Bookings", IsCore: true},
	},

	// ── Investment FD: Compounding Frequency ────────────────────────────────
	"fd_compounding_frequency_master": {
		{Schema: "investment", Table: "fd_booking_request", FKCol: "frequency_id",
			Label: "FD Bookings", IsCore: true},
	},

	// ── Investment FD: Day Count Convention ─────────────────────────────────
	"fd_day_count_convention_master": {
		{Schema: "investment", Table: "fd_booking_request", FKCol: "day_count_code",
			Label: "FD Bookings", IsCore: true},
	},

	// ── Investment FD: TDS Plan ─────────────────────────────────────────────
	"fd_tds_plan_master": {
		{Schema: "investment", Table: "fd_booking_request", FKCol: "tds_plan_id",
			Label: "FD Bookings", IsCore: true},
	},

	// ── Investment FD: Penalty Structure ────────────────────────────────────
	"fd_penalty_structure_master": {
		{Schema: "investment", Table: "fd_booking_request", FKCol: "penalty_structure_id",
			Label: "FD Bookings", IsCore: true},
	},

	// ── Investment FD: Rate Card ─────────────────────────────────────────────
	// Rate cards are config — no direct FK from fd_booking_request found; standalone delete allowed.

	// ── Cash: CashFlow Category ─────────────────────────────────────────────
	"mastercashflowcategory": {
		{Table: "cash_transactions", FKCol: "category_id", Label: "Cash Transactions", IsCore: true},
	},

	// ── Cash: Counterparty ──────────────────────────────────────────────────
	"mastercounterparty": {
		{Table: "cash_transactions", FKCol: "counterparty_id", Label: "Cash Transactions", IsCore: true},
	},

	// ── Cash: Payable/Receivable Type ───────────────────────────────────────
	"masterpayablereceivabletype": {
		{Table: "cash_transactions", FKCol: "type_id", Label: "Cash Transactions", IsCore: true},
	},

	// ── Cash: Cost/Profit Center ────────────────────────────────────────────
	"mastercostprofitcenter": {
		{Table: "cash_transactions", FKCol: "centre_id", Label: "Cash Transactions", IsCore: true},
	},

	// ── Investment: Holiday Calendar ────────────────────────────────────────
	// Calendars referenced by FD booking for holiday-adjusted maturity dates.
	// FD bookings are CORE.
	"mastercalendar": {
		{Schema: "investment", Table: "fd_booking_request", FKCol: "calendar_id",
			Label: "FD Bookings", IsCore: true},
	},
}

// HasCoreBelow recursively checks whether any CORE records exist beneath the
// given resource. Returns a list of blockers (non-empty = delete is blocked).
// resourceType must match a key in DependencyGraph (e.g. "masterbank").
func HasCoreBelow(ctx context.Context, db *pgxpool.Pool, resourceType, resourceID string) ([]BlockingDep, error) {
	children, ok := DependencyGraph[resourceType]
	if !ok {
		return nil, nil
	}

	var blockers []BlockingDep

	for _, child := range children {
		table := qualifiedTable(child)

		if child.IsCore {
			count, err := queryCount(ctx, db, table, child.FKCol, resourceID)
			if err != nil {
				// Log but don't block on query errors — defensive
				continue
			}
			if count > 0 {
				blockers = append(blockers, BlockingDep{Label: child.Label, Count: count})
			}
		} else {
			// Master child — recurse into each of its rows
			childIDs, err := queryChildIDs(ctx, db, table, child.FKCol, child.AuditFKCol, resourceID)
			if err != nil {
				continue
			}
			for _, childID := range childIDs {
				deeper, _ := HasCoreBelow(ctx, db, child.Table, childID)
				blockers = append(blockers, deeper...)
			}
		}
	}

	return blockers, nil
}

// CascadeDelete soft-deletes the resource and all non-core children bottom-up,
// writing an audit row to each child's AuditTable.
// Call HasCoreBelow first — CascadeDelete does NOT re-check for core records.
func CascadeDelete(ctx context.Context, db *pgxpool.Pool, resourceType, resourceID, deletedBy string) error {
	children, ok := DependencyGraph[resourceType]
	if !ok {
		return nil
	}

	for _, child := range children {
		if child.IsCore || child.AuditTable == "" {
			continue
		}

		table := qualifiedTable(child)
		childIDs, err := queryChildIDs(ctx, db, table, child.FKCol, child.AuditFKCol, resourceID)
		if err != nil {
			continue
		}

		for _, childID := range childIDs {
			// Recurse bottom-up first
			_ = CascadeDelete(ctx, db, child.Table, childID, deletedBy)

			// Soft-delete the child row
			softDeleteQuery := fmt.Sprintf(
				`UPDATE %s SET is_deleted = true WHERE %s = $1`,
				table, child.AuditFKCol,
			)
			_, _ = db.Exec(ctx, softDeleteQuery, childID)

			// Write audit row
			writeAuditRow(ctx, db, child.AuditTable, child.AuditFKCol, childID, deletedBy)
		}
	}
	return nil
}

// BlockersSummary formats blockers as a human-readable string for API error responses.
func BlockersSummary(blockers []BlockingDep) string {
	parts := make([]string, 0, len(blockers))
	for _, b := range blockers {
		parts = append(parts, fmt.Sprintf("%d %s", b.Count, b.Label))
	}
	return strings.Join(parts, ", ")
}

// ── helpers ──────────────────────────────────────────────────────────────────

func qualifiedTable(c ChildCheck) string {
	if c.Schema != "" {
		return c.Schema + "." + c.Table
	}
	return c.Table
}

func queryCount(ctx context.Context, db *pgxpool.Pool, table, fkCol, parentID string) (int, error) {
	q := fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE %s = $1 AND COALESCE(is_deleted, false) = false`,
		table, fkCol,
	)
	var count int
	err := db.QueryRow(ctx, q, parentID).Scan(&count)
	return count, err
}

func queryChildIDs(ctx context.Context, db *pgxpool.Pool, table, fkCol, pkCol, parentID string) ([]string, error) {
	q := fmt.Sprintf(
		`SELECT %s::text FROM %s WHERE %s = $1 AND COALESCE(is_deleted, false) = false`,
		pkCol, table, fkCol,
	)
	rows, err := db.Query(ctx, q, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil && id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func writeAuditRow(ctx context.Context, db *pgxpool.Pool, auditTable, fkCol, recordID, deletedBy string) {
	q := fmt.Sprintf(
		`INSERT INTO %s (%s, actiontype, processing_status, requested_by, requested_at)
		 VALUES ($1, 'DELETE', 'APPROVED', $2, NOW())
		 ON CONFLICT DO NOTHING`,
		auditTable, fkCol,
	)
	_, _ = db.Exec(ctx, q, recordID, deletedBy)
}
