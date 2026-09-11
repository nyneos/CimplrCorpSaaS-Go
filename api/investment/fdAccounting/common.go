// Package fdAccounting implements the FD Accounting Preview & Posting workbench
// (BRD AP-01 … AP-08) over the shared journal store
// investment.accounting_journal_entry(+_line). There is no ERP: "posting" means
// flipping an approved workbench-created entry to POSTED in our own ledger.
//
// Producer-created journals (activation, accrual, receipt, closure) are already
// POSTED when they arrive — their maker-checker happened upstream. The
// lifecycle here (DRAFT → PENDING_APPROVAL → APPROVED → POSTED / FAILED)
// governs only what this workbench creates itself: reversals.
//
// Maker-checker follows the repo's auditaction_* convention: the latest row in
// investment.auditaction_fd_accounting_journal per entry_id is the
// processing_status, exactly like auditactionaccountingactivity.
package fdAccounting

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	moduleCode = "FIXED_DEPOSIT"

	journalTable       = "investment.accounting_journal_entry"
	journalLineTable   = "investment.accounting_journal_entry_line"
	journalAuditTable  = "investment.auditaction_fd_accounting_journal"
	glMappingTable     = "investment.fd_gl_mapping"
	glMappingLineTable = "investment.fd_gl_mapping_line"
	glMappingAudit     = "investment.auditaction_fd_gl_mapping"

	txJournalReversal = "FD_JOURNAL_REVERSAL"
	txGlMappingCreate = "FD_GL_MAPPING_CREATE"

	// Journal lifecycle (entry.status). Producers write POSTED directly.
	statusDraft           = "DRAFT"
	statusPendingApproval = "PENDING_APPROVAL"
	statusApproved        = "APPROVED"
	statusPosted          = "POSTED"
	statusFailed          = "FAILED"
	statusRejected        = "REJECTED"
	statusReversed        = "REVERSED"

	entryTypeReversal = "REVERSAL"
	postingModeValue  = "INTERNAL"
)

// dbExec is satisfied by both *pgxpool.Pool and pgx.Tx.
type dbExec interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

// scanRowsToMaps mirrors fdMonthEndClosing/lock.scanRowsToMaps.
func scanRowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0, 64)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[string(f.Name)] = ""
			} else {
				row[string(f.Name)] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// fdJournalPredicate keeps the queue to FD journals only — the same table also
// holds mutual-fund entries (scheme_id / folio_id / demat_id).
const fdJournalPredicate = ` AND (
	je.fd_id IS NOT NULL OR je.receipt_id IS NOT NULL OR je.accrual_run_id IS NOT NULL
	OR je.closure_request_id IS NOT NULL OR je.entry_type LIKE 'FD\_%'
	OR je.entry_type IN ('CLOSURE','REVERSAL')
)`

// scopeClause appends the entity restriction from the session scope.
func scopeClause(scope ctxutil.RequestScope, args *[]interface{}) string {
	if scope.IsAdminOverride {
		return ""
	}
	clauses := []string{}
	if len(scope.EntityIDs) > 0 {
		*args = append(*args, scope.EntityIDs)
		clauses = append(clauses, "je.entity_id = ANY($"+strconv.Itoa(len(*args))+"::text[])")
	}
	if len(scope.EntityNames) > 0 {
		*args = append(*args, scope.EntityNames)
		clauses = append(clauses, "je.entity_name = ANY($"+strconv.Itoa(len(*args))+"::text[])")
	}
	if len(clauses) == 0 {
		return ""
	}
	return " AND (" + strings.Join(clauses, " OR ") + ")"
}

// insertJournalAudit writes one auditaction_* row. When checker is true the row
// is stamped as already checked (used for POST/RETRY outcomes and backfills).
func insertJournalAudit(ctx context.Context, exec dbExec, entryID, actionType, processingStatus, reason, actorEmail string, checker bool) error {
	ip := api.SystemIfBlank(api.ClientIPFromContext(ctx))
	if checker {
		_, err := exec.Exec(ctx, `
			INSERT INTO `+journalAuditTable+`
				(entry_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip, checker_by, checker_at, checker_ip)
			VALUES ($1,$2,$3,NULLIF($4,''),$5,now(),$6,$5,now(),$6)`,
			entryID, actionType, processingStatus, reason, api.SystemIfBlank(actorEmail), ip)
		return err
	}
	_, err := exec.Exec(ctx, `
		INSERT INTO `+journalAuditTable+`
			(entry_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
		VALUES ($1,$2,$3,NULLIF($4,''),$5,now(),$6)`,
		entryID, actionType, processingStatus, reason, api.SystemIfBlank(actorEmail), ip)
	return err
}

// latestAuditStatus returns the processing_status of the newest non-file audit
// row for the entry ("" when none).
func latestAuditStatus(ctx context.Context, exec dbExec, entryID string) (string, error) {
	var status string
	err := exec.QueryRow(ctx, `
		SELECT processing_status FROM `+journalAuditTable+`
		WHERE entry_id = $1 AND UPPER(COALESCE(actiontype,'')) NOT IN ('UPLOAD_FILE','DOWNLOAD')
		ORDER BY requested_at DESC LIMIT 1`, entryID).Scan(&status)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return status, err
}

// periodLocked reports whether a LOCKED/CLOSED closing cycle covers the
// entry's entity + date. This is the "period must be open" gate (BRD AP-05).
func periodLocked(ctx context.Context, exec dbExec, entityID, entityName string, entryDate time.Time) (bool, string, error) {
	var cycleID, status string
	err := exec.QueryRow(ctx, `
		SELECT cycle_id, status FROM investment.fd_closing_cycle
		WHERE (entity_id = $1 OR entity_name = $2)
		  AND status IN ('LOCKED','CLOSED')
		  AND $3::date BETWEEN period_start AND period_end
		  AND COALESCE(is_deleted,false) = false
		ORDER BY period_end DESC LIMIT 1`, entityID, entityName, entryDate).Scan(&cycleID, &status)
	if err == pgx.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, fmt.Sprintf("accounting period is %s by closing cycle %s", status, cycleID), nil
}

type journalLineRec struct {
	LineNumber    int
	AccountNumber string
	AccountName   string
	AccountType   string
	Debit         float64
	Credit        float64
	Narration     string
}

func loadLines(ctx context.Context, exec dbExec, entryID string) ([]journalLineRec, error) {
	rows, err := exec.Query(ctx, `
		SELECT line_number, COALESCE(account_number,''), COALESCE(account_name,''), COALESCE(account_type,''),
		       COALESCE(debit_amount,0), COALESCE(credit_amount,0), COALESCE(narration,'')
		FROM `+journalLineTable+` WHERE entry_id = $1 ORDER BY line_number`, entryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []journalLineRec{}
	for rows.Next() {
		var l journalLineRec
		if err := rows.Scan(&l.LineNumber, &l.AccountNumber, &l.AccountName, &l.AccountType, &l.Debit, &l.Credit, &l.Narration); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// validateForPosting is the whole "ERP response" in an ERP-less world.
func validateForPosting(lines []journalLineRec, totalDebit, totalCredit float64) string {
	if len(lines) < 2 {
		return "journal needs at least one debit and one credit line"
	}
	var dr, cr float64
	for _, l := range lines {
		if strings.TrimSpace(l.AccountNumber) == "" {
			return fmt.Sprintf("line %d has no GL account", l.LineNumber)
		}
		if l.Debit < 0 || l.Credit < 0 {
			return fmt.Sprintf("line %d has a negative amount", l.LineNumber)
		}
		dr += l.Debit
		cr += l.Credit
	}
	if math.Abs(dr-cr) > 0.005 {
		return fmt.Sprintf("journal is unbalanced: debit %.2f vs credit %.2f", dr, cr)
	}
	if math.Abs(dr-totalDebit) > 0.005 || math.Abs(cr-totalCredit) > 0.005 {
		return "line totals do not match the entry header totals"
	}
	return ""
}

func mergeIDs(single string, many []string) []string {
	out := make([]string, 0, len(many)+1)
	seen := map[string]bool{}
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	add(single)
	for _, id := range many {
		add(id)
	}
	return out
}

// runEngineInBackground mirrors fdMonthEndClosing/lock.runEngineInBackground.
func runEngineInBackground(fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDAccounting] engine goroutine panic: %v", rec)
			}
		}()
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		fn(bgCtx)
	}()
}
