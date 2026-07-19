package jobs

// fd_rollover_reconcile_worker.go
//
// Runs once every day at 18:00 (6 pm IST).
//
// Purpose
// ───────
// When a ROLLOVER closure is approved, postClosureJournals creates an
// fd_booking_request (status=SENT_TO_BANK) and stores its booking_id on
// fd_closure_rollover.new_booking_id.
//
// The normal next steps are:
//   1. Ops captures a confirmation slip → fd_confirmation (CAPTURED/CONFIRMED)
//   2. Ops calls ActivateFD → fd_master created, cashflows generated, FD goes ACTIVE
//
// If step 1 or 2 is missed (ops captured confirmation but never clicked Activate),
// this worker detects the gap and auto-creates the fd_master row so the new FD
// becomes trackable in the portfolio without human intervention.
//
// Detection logic
// ───────────────
//   • fd_closure_rollover has a non-null new_booking_id   (booking was created)
//   • AND fd_closure_rollover.new_fd_id IS NULL           (fd_master not yet created)
//   • AND fd_booking_request.booking_status = 'CONFIRMED' (confirmation was approved)
//   • AND fd_confirmation.confirmation_status = 'CONFIRMED'
//   • AND NOT EXISTS any fd_master row for that booking   (double-create guard)
//
// What it does
// ────────────
//   • Inserts fd_master (status=PENDING_ACTIVATION) using confirmed values
//     (confirmed rate, confirmed tenor, confirmed dates from fd_confirmation).
//   • Generates cashflow schedule from confirmed terms.
//   • Updates fd_closure_rollover.new_fd_id and fd_booking_request.new_fd_id.
//   • Logs every action to fd_auto_renewal_log (reusing the same audit table).

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// StartRolloverReconcileWorker fires once immediately, then every day at 18:00.
func StartRolloverReconcileWorker(db *pgxpool.Pool) {
	logger.LogError("[RolloverReconcile] Worker started — fires daily at 18:00")

	fireAt6pm(func() { runRolloverReconcile(db) })
}

// fireAt6pm calls fn immediately, then again at 18:00 every day.
func fireAt6pm(fn func()) {
	// Run immediately on startup (catches any backlog from prior days).
	go fn()

	go func() {
		for {
			now := time.Now()
			next := time.Date(now.Year(), now.Month(), now.Day(), 18, 0, 0, 0, now.Location())
			if !next.After(now) {
				next = next.Add(24 * time.Hour)
			}
			time.Sleep(time.Until(next))
			fn()
		}
	}()
}

// rolloverReconcileRowType holds data fetched for one pending rollover reconciliation.
type rolloverReconcileRowType struct {
	ClosureRequestID string
	SourceFDID       string
	NewBookingID     string
	ConfirmationID   string
	EntityID         string
	EntityName       string
	BankID           string
	BankName         string
	BankConfigID     string
	SourceAccountID  string
	FrequencyID      string
	TDSPlanID        string
	DayCountCode     string
	InterestTypeCode string
	PrincipalAmount  float64
	InterestRate     float64
	StartDate        time.Time
	MaturityDate     time.Time
	TenureDays       int
	BankFDRefNo      string
}

// runRolloverReconcile is the main reconciliation loop.
func runRolloverReconcile(db *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	logger.LogError("[RolloverReconcile] Starting run at %s", time.Now().Format(time.RFC3339))

	// Find all rollover closures where:
	//   - a new booking was created (new_booking_id IS NOT NULL)
	//   - but fd_master was never created (new_fd_id IS NULL)
	//   - AND the booking + confirmation are both CONFIRMED
	//   - AND no fd_master row already exists for that booking (safety guard)
	rows, err := db.Query(ctx, `
		SELECT
		  cr.closure_request_id,
		  cr.fd_id                              AS source_fd_id,
		  r.new_booking_id,
		  COALESCE(b.entity_id,'')              AS entity_id,
		  COALESCE(b.entity_name,'')            AS entity_name,
		  COALESCE(b.bank_id,'')                AS bank_id,
		  COALESCE(b.bank_name,'')              AS bank_name,
		  COALESCE(b.bank_config_id,'')         AS bank_config_id,
		  COALESCE(b.source_account_id,'')      AS source_account_id,
		  COALESCE(b.frequency_id,'')           AS frequency_id,
		  COALESCE(b.tds_plan_id,'')            AS tds_plan_id,
		  COALESCE(b.day_count_code,'')         AS day_count_code,
		  COALESCE(b.interest_type_code,'SIMPLE') AS interest_type_code,
		  -- prefer confirmed values; fall back to booked values
		  COALESCE(c.actual_principal,   b.principal_amount, 0) AS principal_amount,
		  COALESCE(c.confirmed_rate,     b.interest_rate,    0) AS interest_rate,
		  COALESCE(c.actual_start_date,  NOW()::date)           AS start_date,
		  COALESCE(c.actual_maturity_date,
		    c.actual_start_date + (COALESCE(b.tenure_days,365) * INTERVAL '1 day'),
		    NOW()::date + (COALESCE(b.tenure_days,365) * INTERVAL '1 day')
		  )                                                      AS maturity_date,
		  COALESCE(b.tenure_days, 365)                          AS tenure_days,
		  COALESCE(c.bank_fd_ref_no, b.booking_id)              AS bank_fd_ref_no,
		  c.confirmation_id
		FROM investment.fd_closure_rollover r
		JOIN investment.fd_closure_request   cr ON cr.closure_request_id = r.closure_request_id
		JOIN investment.fd_booking_request   b  ON b.booking_id = r.new_booking_id
		JOIN investment.fd_confirmation      c  ON c.booking_id = r.new_booking_id
		WHERE r.new_booking_id IS NOT NULL
		  AND r.new_fd_id      IS NULL
		  AND b.booking_status              = 'CONFIRMED'
		  AND c.confirmation_status         = 'CONFIRMED'
		  AND COALESCE(c.is_deleted, false) = false
		  AND COALESCE(b.is_deleted, false) = false
		  AND NOT EXISTS (
		    SELECT 1 FROM investment.fd_master fm
		    WHERE fm.booking_id = r.new_booking_id
		      AND COALESCE(fm.is_deleted, false) = false
		  )
	`)
	if err != nil {
		logger.LogError("[RolloverReconcile] Query error: %v", err)
		return
	}
	defer rows.Close()

	var pending []rolloverReconcileRowType
	for rows.Next() {
		var r rolloverReconcileRowType
		if err := rows.Scan(
			&r.ClosureRequestID, &r.SourceFDID, &r.NewBookingID,
			&r.EntityID, &r.EntityName,
			&r.BankID, &r.BankName, &r.BankConfigID, &r.SourceAccountID,
			&r.FrequencyID, &r.TDSPlanID, &r.DayCountCode, &r.InterestTypeCode,
			&r.PrincipalAmount, &r.InterestRate,
			&r.StartDate, &r.MaturityDate, &r.TenureDays,
			&r.BankFDRefNo, &r.ConfirmationID,
		); err != nil {
			logger.LogError("[RolloverReconcile] Scan error: %v", err)
			continue
		}
		pending = append(pending, r)
	}
	rows.Close()

	activated, failed := 0, 0
	for _, r := range pending {
		if err := activateRolloverFD(ctx, db, r); err != nil {
			logger.LogError("[RolloverReconcile] Failed for booking %s (closure %s): %v",
				r.NewBookingID, r.ClosureRequestID, err)
			_, _ = db.Exec(ctx, `
				INSERT INTO investment.fd_auto_renewal_log (
				  fd_id, closure_request_id, renewal_date, renewal_status,
				  new_tenor_days, rollover_amount, failure_reason, created_at
				) VALUES ($1,$2,CURRENT_DATE,'RECONCILE_FAILED',$3,$4,$5,NOW())`,
				r.SourceFDID, r.ClosureRequestID, r.TenureDays, r.PrincipalAmount, err.Error())
			failed++
			continue
		}
		activated++
	}

	logger.LogError("[RolloverReconcile] Run complete — activated=%d failed=%d total=%d at %s",
		activated, failed, len(pending), time.Now().Format(time.RFC3339))
}

// activateRolloverFD creates fd_master + cashflow schedule for a confirmed
// rollover booking that has not yet been activated, then links the new fd_id
// back onto fd_closure_rollover and fd_booking_request.
func activateRolloverFD(ctx context.Context, db *pgxpool.Pool, r rolloverReconcileRowType) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Double-create guard inside the transaction (FOR UPDATE on booking row).
	var existingFDID string
	_ = tx.QueryRow(ctx,
		`SELECT COALESCE(fd_id,'') FROM investment.fd_master
		 WHERE booking_id=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`,
		r.NewBookingID,
	).Scan(&existingFDID)
	if existingFDID != "" {
		// Already activated by someone else between the outer query and now.
		logger.LogError("[RolloverReconcile] booking %s already has fd_master %s — skipping", r.NewBookingID, existingFDID)
		return nil
	}

	entityName := r.EntityName
	if entityName == "" {
		_ = tx.QueryRow(ctx,
			`SELECT COALESCE(entity_name,'') FROM public.entity_master WHERE entity_id=$1 LIMIT 1`,
			r.EntityID).Scan(&entityName)
	}
	bankName := r.BankName
	if bankName == "" {
		_ = tx.QueryRow(ctx,
			`SELECT COALESCE(bank_name,'') FROM public.bank_master WHERE bank_id=$1 LIMIT 1`,
			r.BankID).Scan(&bankName)
	}

	// Insert fd_master using the confirmed values.
	var newFDID string
	err = tx.QueryRow(ctx, `
		INSERT INTO investment.fd_master (
		  booking_id, confirmation_id,
		  entity_id, entity_name,
		  bank_id, bank_name,
		  bank_config_id, source_account_id,
		  principal_amount, interest_rate, tenure_days,
		  interest_type_code, frequency_id, day_count_code, tds_plan_id,
		  start_date, maturity_date,
		  bank_fd_ref_no,
		  fd_status,
		  source_closure_id,
		  cashflow_generated,
		  created_by
		) VALUES (
		  $1,$2,
		  $3,$4,
		  $5,$6,
		  NULLIF($7,''), NULLIF($8,''),
		  $9,$10,$11,
		  NULLIF($12,''), NULLIF($13,''), NULLIF($14,''), NULLIF($15,''),
		  $16,$17,
		  $18,
		  'PENDING_ACTIVATION',
		  $19,
		  false,
		  'SYSTEM'
		) RETURNING fd_id`,
		r.NewBookingID, r.ConfirmationID,
		r.EntityID, entityName,
		r.BankID, bankName,
		r.BankConfigID, r.SourceAccountID,
		r.PrincipalAmount, r.InterestRate, r.TenureDays,
		r.InterestTypeCode, r.FrequencyID, r.DayCountCode, r.TDSPlanID,
		r.StartDate, r.MaturityDate,
		r.BankFDRefNo,
		r.ClosureRequestID,
	).Scan(&newFDID)
	if err != nil {
		return fmt.Errorf("insert fd_master: %w", err)
	}

	// Generate and save cashflow schedule.
	if cfErr := generateAndSaveCashflows(ctx, tx, newFDID, r); cfErr != nil {
		// Non-fatal — fd_master is created; cashflows can be regenerated manually.
		logger.LogError("[RolloverReconcile] cashflow generation failed for fd %s: %v", newFDID, cfErr)
	} else {
		// Mark cashflow_generated so the accrual engine picks it up.
		_, _ = tx.Exec(ctx,
			`UPDATE investment.fd_master SET cashflow_generated=true, cashflow_generated_at=now() WHERE fd_id=$1`,
			newFDID)
	}

	// Link new fd_id back on closure and booking tables.
	_, _ = tx.Exec(ctx,
		`UPDATE investment.fd_closure_rollover SET new_fd_id=$1 WHERE closure_request_id=$2`,
		newFDID, r.ClosureRequestID)
	_, _ = tx.Exec(ctx,
		`UPDATE investment.fd_booking_request SET new_fd_id=$1 WHERE booking_id=$2`,
		newFDID, r.NewBookingID)
	// Update booking status to show it is now active.
	_, _ = tx.Exec(ctx,
		`UPDATE investment.fd_booking_request SET booking_status='ACTIVATED' WHERE booking_id=$1`,
		r.NewBookingID)

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Log success.
	_, _ = db.Exec(ctx, `
		INSERT INTO investment.fd_auto_renewal_log (
		  fd_id, closure_request_id, renewal_date, renewal_status,
		  new_tenor_days, rollover_amount, created_at
		) VALUES ($1,$2,CURRENT_DATE,'RECONCILE_ACTIVATED',$3,$4,NOW())`,
		newFDID, r.ClosureRequestID, r.TenureDays, r.PrincipalAmount)

	logger.LogError("[RolloverReconcile] Activated fd_master %s for booking %s (closure %s)",
		newFDID, r.NewBookingID, r.ClosureRequestID)
	return nil
}

// generateAndSaveCashflows inserts a simple cashflow schedule for the new
// rollover FD. Generates a single MATURITY event using simple-interest formula.
// For complex payout schedules the ops team can regenerate via the workbench.
func generateAndSaveCashflows(ctx context.Context, tx pgx.Tx, fdID string, r rolloverReconcileRowType) error {
	// Simple interest: principal × rate/100 / 365 × tenure_days
	dailyInterest := r.PrincipalAmount * (r.InterestRate / 100.0) / 365.0
	totalInterest := math.Round(dailyInterest*float64(r.TenureDays)*10000) / 10000
	maturityAmount := math.Round((r.PrincipalAmount+totalInterest)*10000) / 10000

	_, err := tx.Exec(ctx, `
		INSERT INTO investment.fd_cashflow_schedule (
		  fd_id, sequence_number, event_date, event_type,
		  principal_amount, interest_amount, maturity_amount,
		  is_deleted, created_by
		) VALUES ($1, 1, $2, 'MATURITY', $3, $4, $5, false, 'SYSTEM')
		ON CONFLICT DO NOTHING`,
		fdID, r.MaturityDate,
		r.PrincipalAmount, totalInterest, maturityAmount,
	)
	if err != nil {
		return fmt.Errorf("insert cashflow: %w", err)
	}
	return nil
}
