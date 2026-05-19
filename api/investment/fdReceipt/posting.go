package fdReceipt

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
		SET journal_entry_id=$1, receipt_status='POSTED'
		WHERE receipt_id=$2`, interestEntryID, rec.ReceiptID)
	if err != nil {
		return "", "", fmt.Errorf("receipt update: %w", err)
	}

	// UPDATE TDS row with journal_entry_id + POSTED if TDS was deducted
	if tdsEntryID != "" {
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_tds_receipt
			SET journal_entry_id=$1, tds_status='POSTED'
			WHERE receipt_id=$2 AND COALESCE(is_deleted,false)=false`,
			tdsEntryID, rec.ReceiptID)
		if err != nil {
			return "", "", fmt.Errorf("tds receipt update: %w", err)
		}
	}

	if err = markReceiptSchedulePosted(ctx, tx, rec); err != nil {
		return "", "", err
	}

	if err = tx.Commit(ctx); err != nil {
		return "", "", fmt.Errorf("commit: %w", err)
	}

	return interestEntryID, tdsEntryID, nil
}

// checkReceiptPostingEligibility returns an error message when posting must be blocked.
// Posting requires reconcile MATCHED (or variance accepted after exception close) and no open exceptions.
func checkReceiptPostingEligibility(ctx context.Context, pool *pgxpool.Pool, receiptID string) string {
	var receiptStatus, reconcileStatus string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(receipt_status,''), COALESCE(reconcile_status,'PENDING')
		FROM investment.fd_interest_receipt
		WHERE receipt_id=$1 AND COALESCE(is_deleted,false)=false`,
		receiptID,
	).Scan(&receiptStatus, &reconcileStatus)
	if err != nil {
		return "receipt not found"
	}
	if receiptStatus != "APPROVED" {
		return "receipt must be APPROVED before posting (current: " + receiptStatus + ")"
	}
	switch reconcileStatus {
	case "MATCHED", "VARIANCE_ACCEPTED":
		// ok — still require no open exceptions below
	case "PENDING", "":
		return "receipt must be reconciled before posting (reconcile_status=PENDING)"
	default:
		return "reconcile_status " + reconcileStatus + " does not allow posting; reconcile or accept variance first"
	}

	if blocksReceiptPosting(ctx, pool, receiptID) {
		return "variance must be closed with ACCEPT (becomes EXCEPTION) before posting journals"
	}
	return ""
}

// markReceiptSchedulePosted updates cashflow schedule and reconcile map rows after GL posting.
func markReceiptSchedulePosted(ctx context.Context, tx pgx.Tx, rec ReceiptForPosting) error {
	if rec.FDID == "" {
		return nil
	}
	if !rec.PeriodStart.IsZero() && !rec.PeriodEnd.IsZero() {
		_, err := tx.Exec(ctx, `
			UPDATE investment.fd_cashflow_schedule
			SET posting_status='POSTED'
			WHERE fd_id=$1
			  AND event_date BETWEEN $2::date AND $3::date
			  AND COALESCE(is_deleted,false)=false`,
			rec.FDID, rec.PeriodStart, rec.PeriodEnd)
		if err != nil {
			return fmt.Errorf("cashflow posting_status update: %w", err)
		}
	}
	// Best-effort: junction tables may not exist in all environments.
	_, _ = tx.Exec(ctx, `
		UPDATE investment.fd_receipt_cashflow_map
		SET match_status='POSTED'
		WHERE receipt_id=$1 AND COALESCE(is_deleted,false)=false`,
		rec.ReceiptID)
	_, _ = tx.Exec(ctx, `
		UPDATE investment.fd_receipt_accrual_map
		SET match_status='POSTED'
		WHERE receipt_id=$1 AND COALESCE(is_deleted,false)=false`,
		rec.ReceiptID)
	return nil
}

// reconcilePostingSnapshot is live status from receipt/TDS tables for reconcile UI.
type reconcilePostingSnapshot struct {
	ReceiptStatus     string
	ReconcileStatus   string
	JournalEntryID    string
	TDSStatus         string
	TDSJournalEntryID string
}

func loadReconcilePostingSnapshot(ctx context.Context, pool *pgxpool.Pool, receiptID, tdsID string) reconcilePostingSnapshot {
	var snap reconcilePostingSnapshot
	if receiptID != "" {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(receipt_status,''),
			       COALESCE(reconcile_status,''),
			       COALESCE(journal_entry_id,'')
			FROM investment.fd_interest_receipt
			WHERE receipt_id=$1 AND COALESCE(is_deleted,false)=false`,
			receiptID,
		).Scan(&snap.ReceiptStatus, &snap.ReconcileStatus, &snap.JournalEntryID)
	}
	if tdsID != "" {
		var tdsReconcile string
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(tds_status,''),
			       COALESCE(journal_entry_id,''),
			       COALESCE(reconcile_status,'')
			FROM investment.fd_tds_receipt
			WHERE tds_id=$1 AND COALESCE(is_deleted,false)=false`,
			tdsID,
		).Scan(&snap.TDSStatus, &snap.TDSJournalEntryID, &tdsReconcile)
		if receiptID == "" {
			snap.ReconcileStatus = tdsReconcile
		}
	} else if receiptID != "" {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(tds_status,''),
			       COALESCE(journal_entry_id,'')
			FROM investment.fd_tds_receipt
			WHERE receipt_id=$1 AND COALESCE(is_deleted,false)=false
			ORDER BY created_at DESC NULLS LAST
			LIMIT 1`,
			receiptID,
		).Scan(&snap.TDSStatus, &snap.TDSJournalEntryID)
	}
	return snap
}

// resolveExceptionReceiptLinks fills missing receipt_id / tds_id from linked rows.
func resolveExceptionReceiptLinks(ctx context.Context, pool *pgxpool.Pool, receiptID, tdsID string) (string, string) {
	if receiptID == "" && tdsID != "" {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(receipt_id,'')
			FROM investment.fd_tds_receipt
			WHERE tds_id=$1 AND COALESCE(is_deleted,false)=false`,
			tdsID,
		).Scan(&receiptID)
	}
	if tdsID == "" && receiptID != "" {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(tds_id,'')
			FROM investment.fd_tds_receipt
			WHERE receipt_id=$1 AND COALESCE(is_deleted,false)=false
			ORDER BY created_at DESC NULLS LAST
			LIMIT 1`,
			receiptID,
		).Scan(&tdsID)
	}
	return receiptID, tdsID
}

// stampVarianceAccepted marks linked receipt/TDS and reconcile result after ACCEPT close.
func stampVarianceAccepted(ctx context.Context, pool *pgxpool.Pool, receiptID, tdsID, resultID string) {
	if receiptID != "" {
		_, _ = pool.Exec(ctx, `
			UPDATE investment.fd_interest_receipt
			SET reconcile_status='VARIANCE_ACCEPTED'
			WHERE receipt_id=$1 AND COALESCE(is_deleted,false)=false`,
			receiptID)
	}
	if tdsID != "" {
		_, _ = pool.Exec(ctx, `
			UPDATE investment.fd_tds_receipt
			SET reconcile_status='VARIANCE_ACCEPTED'
			WHERE tds_id=$1 AND COALESCE(is_deleted,false)=false`,
			tdsID)
	}
	if resultID != "" {
		_, _ = pool.Exec(ctx, `
			UPDATE investment.fd_receipt_reconcile_result
			SET has_exception=true, match_status='VARIANCE_ACCEPTED'
			WHERE result_id=$1`, resultID)
	}
}

func stampVariancePending(ctx context.Context, pool *pgxpool.Pool, resultID string) {
	if resultID == "" {
		return
	}
	_, _ = pool.Exec(ctx, `
		UPDATE investment.fd_receipt_reconcile_result
		SET has_exception=false, match_status='UNMATCHED'
		WHERE result_id=$1`, resultID)
}

// applyExceptionClosure applies receipt/result side-effects after close (ACCEPT → EXCEPTION + postable).
func applyExceptionClosure(ctx context.Context, pool *pgxpool.Pool, exceptionID string) {
	var receiptID, tdsID, resultID, proposedResolution, outcome string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(receipt_id,''), COALESCE(tds_id,''), COALESCE(result_id,''),
		       COALESCE(proposed_resolution,''), COALESCE(variance_outcome,'')
		FROM investment.fd_receipt_exception
		WHERE exception_id=$1 AND COALESCE(is_deleted,false)=false`,
		exceptionID,
	).Scan(&receiptID, &tdsID, &resultID, &proposedResolution, &outcome)
	if err != nil {
		return
	}
	receiptID, tdsID = resolveExceptionReceiptLinks(ctx, pool, receiptID, tdsID)
	if outcome == "ACCEPTED" {
		stampVarianceAccepted(ctx, pool, receiptID, tdsID, resultID)
		return
	}
	stampVariancePending(ctx, pool, resultID)
}

// repairClosedAcceptException re-applies close side-effects for legacy rows (idempotent).
func repairClosedAcceptException(ctx context.Context, pool *pgxpool.Pool, exceptionID string) {
	var status, outcome string
	err := pool.QueryRow(ctx, `
		SELECT exception_status, COALESCE(variance_outcome,'')
		FROM investment.fd_receipt_exception
		WHERE exception_id=$1 AND COALESCE(is_deleted,false)=false`,
		exceptionID,
	).Scan(&status, &outcome)
	if err != nil || status != "CLOSE" {
		return
	}
	if outcome == "" {
		var proposedResolution string
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(proposed_resolution,'')
			FROM investment.fd_receipt_exception WHERE exception_id=$1`, exceptionID,
		).Scan(&proposedResolution)
		res, _ := normalizeProposedResolution(proposedResolution)
		if res == "ACCEPT" {
			outcome = "ACCEPTED"
			_, _ = pool.Exec(ctx, `
				UPDATE investment.fd_receipt_exception
				SET case_type='EXCEPTION', variance_outcome='ACCEPTED'
				WHERE exception_id=$1`, exceptionID)
		} else {
			outcome = "PENDING"
			_, _ = pool.Exec(ctx, `
				UPDATE investment.fd_receipt_exception
				SET case_type='VARIANCE', variance_outcome='PENDING'
				WHERE exception_id=$1`, exceptionID)
		}
	}
	applyExceptionClosure(ctx, pool, exceptionID)
}
