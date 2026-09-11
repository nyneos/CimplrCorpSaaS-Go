package fdAccounting

import (
	"context"
	"fmt"
)

// StatusPendingApproval is the ledger status every producer-generated journal
// (activation, accrual, receipt, closure) now enters the workbench with. From
// there the BRD lifecycle applies: Approve/Reject (AP-04) → Post to Ledger
// (AP-05) → Failed/Retry (AP-06) → Reverse (AP-07).
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
	if err := insertJournalAudit(ctx, exec, entryID, "CREATE", statusPendingApproval, reason, makerEmail, false); err != nil {
		return fmt.Errorf("stage journal %s: %w", entryID, err)
	}
	return nil
}
