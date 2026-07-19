package fdReceipt

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type reconcileEnrichOutputs struct {
	ReceiptStatus     *string
	ReconcileStatus   *string
	JournalEntryID    *string
	TDSStatus         *string
	TDSJournalEntryID *string
	Cashflows         *[]CashflowLine
}

// applyReconcilePostingEnrichment copies live receipt/TDS posting fields onto a result row.
func applyReconcilePostingEnrichment(ctx context.Context, pool *pgxpool.Pool, receiptID, tdsID, resultType string, out reconcileEnrichOutputs) {
	snap := loadReconcilePostingSnapshot(ctx, pool, receiptID, tdsID)
	if out.ReceiptStatus != nil {
		*out.ReceiptStatus = snap.ReceiptStatus
	}
	if out.ReconcileStatus != nil {
		*out.ReconcileStatus = snap.ReconcileStatus
		if resultType == "TDS" && tdsID != "" {
			var tdsRecon string
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(reconcile_status,'')
				FROM investment.fd_tds_receipt
				WHERE tds_id=$1 AND COALESCE(is_deleted,false)=false`,
				tdsID,
			).Scan(&tdsRecon)
			if tdsRecon != "" {
				*out.ReconcileStatus = tdsRecon
			}
		}
	}
	if out.JournalEntryID != nil {
		*out.JournalEntryID = snap.JournalEntryID
	}
	if out.TDSStatus != nil {
		*out.TDSStatus = snap.TDSStatus
	}
	if out.TDSJournalEntryID != nil {
		*out.TDSJournalEntryID = snap.TDSJournalEntryID
	}
	if out.Cashflows != nil && snap.ReceiptStatus == "POSTED" {
		refreshCashflowPostingStatus(*out.Cashflows)
	}
}

func refreshCashflowPostingStatus(lines []CashflowLine) {
	for i := range lines {
		lines[i].PostingStatus = "POSTED"
	}
}
