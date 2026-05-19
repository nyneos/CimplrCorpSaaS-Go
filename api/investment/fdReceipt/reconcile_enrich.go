package fdReceipt

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// applyReconcilePostingEnrichment copies live receipt/TDS posting fields onto a result row.
func applyReconcilePostingEnrichment(ctx context.Context, pool *pgxpool.Pool,
	receiptID, tdsID, resultType string,
	receiptStatus, reconcileStatus, journalEntryID, tdsStatus, tdsJournalEntryID *string,
	cashflows *[]CashflowLine,
) {
	snap := loadReconcilePostingSnapshot(ctx, pool, receiptID, tdsID)
	if receiptStatus != nil {
		*receiptStatus = snap.ReceiptStatus
	}
	if reconcileStatus != nil {
		*reconcileStatus = snap.ReconcileStatus
		if resultType == "TDS" && tdsID != "" {
			var tdsRecon string
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(reconcile_status,'')
				FROM investment.fd_tds_receipt
				WHERE tds_id=$1 AND COALESCE(is_deleted,false)=false`,
				tdsID,
			).Scan(&tdsRecon)
			if tdsRecon != "" {
				*reconcileStatus = tdsRecon
			}
		}
	}
	if journalEntryID != nil {
		*journalEntryID = snap.JournalEntryID
	}
	if tdsStatus != nil {
		*tdsStatus = snap.TDSStatus
	}
	if tdsJournalEntryID != nil {
		*tdsJournalEntryID = snap.TDSJournalEntryID
	}
	if cashflows != nil && snap.ReceiptStatus == "POSTED" {
		refreshCashflowPostingStatus(*cashflows)
	}
}

func refreshCashflowPostingStatus(lines []CashflowLine) {
	for i := range lines {
		lines[i].PostingStatus = "POSTED"
	}
}
