package fdReceipt

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReceiptForPosting holds the data needed to generate journal entries for a receipt.
type ReceiptForPosting struct {
	ReceiptID             string
	FDID                  string
	FdRefNo               string
	EntityID              string
	EntityName            string
	ReceiptDate           time.Time
	PeriodStart           time.Time
	PeriodEnd             time.Time
	GrossInterestReceived float64
	TDSAmountDeducted     float64
	NetAmountReceived     float64
	ReceiptStatus         string
}

// buildReceiptPeriod formats a time into "JAN 2006" accounting period string.
func buildReceiptPeriod(t time.Time) string {
	return strings.ToUpper(t.Format("Jan 2006"))
}

// postReceiptJournals generates and persists journal entries for an interest receipt.
// Returns (interestEntryID, tdsEntryID, error).
func postReceiptJournals(ctx context.Context, pool *pgxpool.Pool, rec ReceiptForPosting, userEmail string) (string, string, error) {
	period := buildReceiptPeriod(rec.ReceiptDate)
	entryDate := rec.ReceiptDate
	if entryDate.IsZero() {
		entryDate = time.Now()
	}

	interestEntryID := fmt.Sprintf("JE_%d_%04d", time.Now().UnixMilli(), rand.Intn(10000))
	tdsEntryID := ""

	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", "", fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Step 1: INSERT accounting_activity
	var activityID string
	err = tx.QueryRow(ctx, `
		INSERT INTO investment.accounting_activity (
			activity_id, activity_type, activity_subtype,
			entity_id, entity_name, fd_id, fd_ref_no,
			receipt_id, amount, currency,
			activity_date, accounting_period,
			created_by, created_at
		) VALUES (
			'ACT-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
			'FIXED_DEPOSIT','INTEREST_RECEIPT',
			$1,$2,$3,$4,$5,$6,'INR',$7,$8,$9,now()
		) RETURNING activity_id`,
		rec.EntityID, rec.EntityName, rec.FDID, rec.FdRefNo,
		rec.ReceiptID, rec.GrossInterestReceived,
		entryDate, period, userEmail,
	).Scan(&activityID)
	if err != nil {
		return "", "", fmt.Errorf("activity insert: %w", err)
	}

	// Step 2: INSERT interest journal entry (2 lines: Dr Bank, Cr Accrued Interest)
	_, err = tx.Exec(ctx, `
		INSERT INTO investment.accounting_journal_entry (
			entry_id, activity_id, entry_type, entry_date, accounting_period,
			entity_id, fd_id, receipt_id,
			description, currency, total_amount,
			created_by, created_at, is_deleted
		) VALUES ($1,$2,'FD_INTEREST_RECEIPT',$3,$4,$5,$6,$7,$8,'INR',$9,$10,now(),false)`,
		interestEntryID, activityID, entryDate, period,
		rec.EntityID, rec.FDID, rec.ReceiptID,
		fmt.Sprintf("Interest receipt for FD %s period %s", rec.FdRefNo, period),
		rec.GrossInterestReceived, userEmail)
	if err != nil {
		return "", "", fmt.Errorf("interest journal insert: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO investment.accounting_journal_entry_line
			(entry_id, line_no, account_code, account_name, debit_amount, credit_amount, receipt_id)
		VALUES
			($1, 1, '1001-BANK', 'Bank Account', $2, 0, $3),
			($1, 2, '1201-ACCRUED-INT', 'Accrued Interest Income', 0, $2, $3)`,
		interestEntryID, rec.GrossInterestReceived, rec.ReceiptID)
	if err != nil {
		return "", "", fmt.Errorf("interest journal lines insert: %w", err)
	}

	// Step 3: INSERT TDS journal entry if TDS > 0
	if rec.TDSAmountDeducted > 0 {
		tdsEntryID = fmt.Sprintf("JE_%d_%04d", time.Now().UnixMilli(), rand.Intn(10000))
		_, err = tx.Exec(ctx, `
			INSERT INTO investment.accounting_journal_entry (
				entry_id, activity_id, entry_type, entry_date, accounting_period,
				entity_id, fd_id, receipt_id,
				description, currency, total_amount,
				created_by, created_at, is_deleted
			) VALUES ($1,$2,'FD_TDS_DEDUCTED',$3,$4,$5,$6,$7,$8,'INR',$9,$10,now(),false)`,
			tdsEntryID, activityID, entryDate, period,
			rec.EntityID, rec.FDID, rec.ReceiptID,
			fmt.Sprintf("TDS deducted for FD %s period %s", rec.FdRefNo, period),
			rec.TDSAmountDeducted, userEmail)
		if err != nil {
			return "", "", fmt.Errorf("tds journal insert: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.accounting_journal_entry_line
				(entry_id, line_no, account_code, account_name, debit_amount, credit_amount, receipt_id)
			VALUES
				($1, 1, '1301-TDS-RECV', 'TDS Receivable', $2, 0, $3),
				($1, 2, '1001-BANK', 'Bank Account', 0, $2, $3)`,
			tdsEntryID, rec.TDSAmountDeducted, rec.ReceiptID)
		if err != nil {
			return "", "", fmt.Errorf("tds journal lines insert: %w", err)
		}
	}

	// Step 4: UPDATE receipt with journal_entry_id + POSTED status
	_, err = tx.Exec(ctx, `
		UPDATE investment.fd_interest_receipt
		SET journal_entry_id=$1, receipt_status='POSTED', updated_by=$2, updated_at=now()
		WHERE receipt_id=$3`, interestEntryID, userEmail, rec.ReceiptID)
	if err != nil {
		return "", "", fmt.Errorf("receipt update: %w", err)
	}

	// UPDATE TDS row with journal_entry_id if present
	if tdsEntryID != "" {
		pool.Exec(ctx, `
			UPDATE investment.fd_tds_receipt
			SET journal_entry_id=$1, updated_by=$2, updated_at=now()
			WHERE receipt_id=$3`, tdsEntryID, userEmail, rec.ReceiptID) //nolint:errcheck
	}

	if err = tx.Commit(ctx); err != nil {
		return "", "", fmt.Errorf("commit: %w", err)
	}

	return interestEntryID, tdsEntryID, nil
}
