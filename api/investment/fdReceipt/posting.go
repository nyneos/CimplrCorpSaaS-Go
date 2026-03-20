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

	// Fetch entity_name from public.masterentity if not already populated.
	entityName := rec.EntityName
	if entityName == "" {
		_ = pool.QueryRow(ctx,
			`SELECT COALESCE(entity_name,'') FROM public.masterentity WHERE entity_id = $1`,
			rec.EntityID,
		).Scan(&entityName)
	}

	interestEntryID := fmt.Sprintf("JE_%d_%04d", time.Now().UnixMilli(), rand.Intn(10000))
	tdsEntryID := ""

	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", "", fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Step 1: INSERT accounting_activity (required parent row for FK)
	var activityID string
	err = tx.QueryRow(ctx, `
		INSERT INTO investment.accounting_activity (
			activity_type, activity_subtype,
			effective_date, accounting_period,
			data_source, status
		) VALUES (
			'FIXED_DEPOSIT', 'INTEREST_RECEIPT',
			$1, $2,
			'Manual', 'POSTED'
		) RETURNING activity_id`,
		entryDate, period,
	).Scan(&activityID)
	if err != nil {
		return "", "", fmt.Errorf("activity insert: %w", err)
	}

	// Step 2: INSERT interest journal entry (Dr Bank, Cr Accrued Interest)
	_, err = tx.Exec(ctx, `
		INSERT INTO investment.accounting_journal_entry (
			entry_id, activity_id, entry_type, entry_date, accounting_period,
			entity_id, entity_name, fd_id, receipt_id,
			description, total_debit, total_credit,
			created_by, created_at, is_deleted
		) VALUES ($1,$2,'FD_INTEREST_RECEIPT',$3,$4,$5,$6,$7,$8,$9,$10,$10,$11,now(),false)`,
		interestEntryID, activityID, entryDate, period,
		rec.EntityID, entityName, rec.FDID, rec.ReceiptID,
		fmt.Sprintf("Interest receipt for FD %s period %s", rec.FdRefNo, period),
		rec.GrossInterestReceived, userEmail)
	if err != nil {
		return "", "", fmt.Errorf("interest journal insert: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO investment.accounting_journal_entry_line
			(entry_id, line_number, account_number, account_name, debit_amount, credit_amount)
		VALUES
			($1, 1, '1001-BANK', 'Bank Account', $2, 0),
			($1, 2, '1201-ACCRUED-INT', 'Accrued Interest Income', 0, $2)`,
		interestEntryID, rec.GrossInterestReceived)
	if err != nil {
		return "", "", fmt.Errorf("interest journal lines insert: %w", err)
	}

	// Step 2: INSERT TDS journal entry if TDS > 0
	if rec.TDSAmountDeducted > 0 {
		tdsEntryID = fmt.Sprintf("JE_%d_%04d", time.Now().UnixMilli(), rand.Intn(10000))
		_, err = tx.Exec(ctx, `
			INSERT INTO investment.accounting_journal_entry (
				entry_id, activity_id, entry_type, entry_date, accounting_period,
				entity_id, entity_name, fd_id, receipt_id,
				description, total_debit, total_credit,
				created_by, created_at, is_deleted
			) VALUES ($1,$2,'FD_TDS_DEDUCTED',$3,$4,$5,$6,$7,$8,$9,$10,$10,$11,now(),false)`,
			tdsEntryID, activityID, entryDate, period,
			rec.EntityID, entityName, rec.FDID, rec.ReceiptID,
			fmt.Sprintf("TDS deducted for FD %s period %s", rec.FdRefNo, period),
			rec.TDSAmountDeducted, userEmail)
		if err != nil {
			return "", "", fmt.Errorf("tds journal insert: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.accounting_journal_entry_line
				(entry_id, line_number, account_number, account_name, debit_amount, credit_amount)
			VALUES
				($1, 1, '1301-TDS-RECV', 'TDS Receivable', $2, 0),
				($1, 2, '1001-BANK', 'Bank Account', 0, $2)`,
			tdsEntryID, rec.TDSAmountDeducted)
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
