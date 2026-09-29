package fdAccounting

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// StatusPendingApproval is the ledger status ("pending-posted") every
// producer-generated journal (activation, accrual, receipt, TDS, closure) and
// workbench reversal enters with. From there the BRD lifecycle applies:
// Approve/Reject (AP-04) → Post to Ledger (AP-05) → Failed/Retry (AP-06) →
// Reverse (AP-07). There is no PENDING_POSTED enum — PENDING_APPROVAL is it.
const StatusPendingApproval = statusPendingApproval

// JournalExec is the subset of pgx.Tx / *pgxpool.Pool producers pass in.
type JournalExec = dbExec

// StageJournalForApproval records the maker-side CREATE audit row for a journal
// a producer module has just inserted with status = StatusPendingApproval.
// The workbench reads the newest audit row as the entry's processing_status,
// so without this row the journal would never show up as Pending Approval.
// Call it inside the same transaction as the journal INSERT.
func StageJournalForApproval(ctx context.Context, exec JournalExec, entryID, makerEmail, reason string) error {
	if entryID == "" {
		return fmt.Errorf("stage journal: entry_id is empty")
	}
	if err := insertJournalAudit(ctx, exec, journalAuditWrite{
		EntryID: entryID, ActionType: "CREATE", ProcessingStatus: statusPendingApproval,
		Reason: reason, ActorEmail: makerEmail, Checker: false,
	}); err != nil {
		return fmt.Errorf("stage journal %s: %w", entryID, err)
	}
	return nil
}

// JournalStatusPair is ledger status (accounting_journal_entry.status) plus the
// newest maker-checker processing_status from auditaction_fd_accounting_journal.
type JournalStatusPair struct {
	LedgerStatus     string // PENDING_APPROVAL | APPROVED | POSTED | FAILED | …
	ProcessingStatus string // latest audit processing_status
}

// LookupJournalStatuses joins the journal row with its latest audit. Used by
// closure / activation / receipt accounting preview APIs so they surface the
// real ledger + approval state instead of the producer-side "POSTED" flag.
func LookupJournalStatuses(ctx context.Context, exec JournalExec, entryID string) (JournalStatusPair, error) {
	out := JournalStatusPair{}
	if strings.TrimSpace(entryID) == "" {
		return out, nil
	}
	err := exec.QueryRow(ctx, `
		SELECT COALESCE(je.status,''),
		       COALESCE((
		         SELECT a.processing_status FROM `+journalAuditTable+` a
		         WHERE a.entry_id = je.entry_id
		           AND UPPER(COALESCE(a.actiontype,'')) NOT IN ('UPLOAD_FILE','DOWNLOAD')
		         ORDER BY a.requested_at DESC LIMIT 1
		       ),'')
		FROM `+journalTable+` je
		WHERE je.entry_id = $1 AND COALESCE(je.is_deleted,false) = false
		LIMIT 1`, strings.TrimSpace(entryID),
	).Scan(&out.LedgerStatus, &out.ProcessingStatus)
	if err == pgx.ErrNoRows {
		return out, nil
	}
	return out, err
}
