package jobs

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartAutoRenewalWorker runs daily and processes FDs with auto_renewal=true
// whose maturity_date has passed. It creates an AUTO_RENEWAL closure request and
// logs the result in fd_auto_renewal_log.
func StartAutoRenewalWorker(db *pgxpool.Pool) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	// Run immediately at startup, then on every 24h tick.
	runAutoRenewal(db)

	for range ticker.C {
		runAutoRenewal(db)
	}
}

func runAutoRenewal(db *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	log.Printf("[AutoRenewal] Starting auto-renewal run at %s", time.Now().Format(time.RFC3339))

	// Fetch all ACTIVE FDs with auto_renewal=true and maturity_date <= today
	// that don't already have a non-rejected / non-cancelled closure request.
	rows, err := db.Query(ctx, `
		SELECT
		  m.fd_id, m.booking_id, m.confirmation_id,
		  COALESCE(b.entity_id,''::text) AS entity_id,
		  COALESCE(b.entity_name,''::text) AS entity_name,
		  m.principal_amount, m.maturity_date,
		  m.principal_amount AS rollover_amount,
		  COALESCE(m.tenure_days, 365) AS tenor_days
		FROM investment.fd_master m
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
		WHERE m.fd_status = 'ACTIVE'
		  AND m.auto_renewal = true
		  AND m.maturity_date <= CURRENT_DATE
		  AND m.is_deleted = false
		  AND NOT EXISTS (
			SELECT 1 FROM investment.fd_closure_request cr
			WHERE cr.fd_id = m.fd_id
			  AND cr.is_deleted = false
			  AND cr.closure_status NOT IN ('REJECTED','CANCELLED')
		  )
	`)
	if err != nil {
		log.Printf("[AutoRenewal] Query error: %v", err)
		return
	}
	defer rows.Close()

	type fdRow struct {
		FDID, BookingID, ConfirmationID string
		EntityID, EntityName           string
		PrincipalAmount, RolloverAmount float64
		MaturityDate                    time.Time
		TenorDays                       int
	}

	var fds []fdRow
	for rows.Next() {
		var f fdRow
		if sErr := rows.Scan(&f.FDID, &f.BookingID, &f.ConfirmationID,
			&f.EntityID, &f.EntityName,
			&f.PrincipalAmount, &f.MaturityDate,
			&f.RolloverAmount, &f.TenorDays); sErr != nil {
			log.Printf("[AutoRenewal] Scan error: %v", sErr)
			continue
		}
		fds = append(fds, f)
	}
	rows.Close()

	processed, failed := 0, 0
	for _, fd := range fds {
		if processErr := processAutoRenewalFD(ctx, db, fd.FDID, fd.BookingID, fd.ConfirmationID,
			fd.EntityID, fd.EntityName, fd.PrincipalAmount, fd.RolloverAmount,
			fd.MaturityDate, fd.TenorDays); processErr != nil {
			log.Printf("[AutoRenewal] Failed for FD %s: %v", fd.FDID, processErr)
			// Log failure in fd_auto_renewal_log
			_, _ = db.Exec(ctx, `
				INSERT INTO investment.fd_auto_renewal_log (
				  fd_id, renewal_date, renewal_status, new_tenor_days,
				  rollover_amount, failure_reason, created_at
				) VALUES ($1, CURRENT_DATE, 'FAILED', $2, $3, $4, NOW())`,
				fd.FDID, fd.TenorDays, fd.RolloverAmount, processErr.Error())
			failed++
			continue
		}
		processed++
	}

	log.Printf("[AutoRenewal] Run complete — processed=%d failed=%d total=%d at %s",
		processed, failed, len(fds), time.Now().Format(time.RFC3339))
}

func processAutoRenewalFD(ctx context.Context, db *pgxpool.Pool,
	fdID, bookingID, confirmationID, entityID, entityName string,
	principalAmount, rolloverAmount float64,
	maturityDate time.Time, tenorDays int) error {

	var closureRequestID string
	err := db.QueryRow(ctx, `
		INSERT INTO investment.fd_closure_request (
		  fd_id, booking_id, confirmation_id, entity_id, entity_name,
		  closure_type, closure_status,
		  initiation_date, effective_closure_date, maturity_date,
		  principal_amount, accrued_interest, tds_deducted,
		  penalty_amount, net_payout_amount,
		  rollover_amount, rollover_tenor_days,
		  maturity_instructions, closure_reason,
		  submitted_by, submitted_by_email,
		  accounting_posted, is_deleted,
		  created_by, created_at, updated_by, updated_at
		) VALUES (
		  $1,
		  NULLIF($2,''), NULLIF($3,''),
		  NULLIF($4,''), NULLIF($5,''),
		  'AUTO_RENEWAL','PENDING_APPROVAL',
		  CURRENT_DATE, $6::date, $6::date,
		  $7, 0, 0, 0, $8,
		  $8, $9,
		  'RENEW', 'System auto-renewal triggered at maturity',
		  'SYSTEM', 'system@cimplr.in',
		  false, false,
		  'SYSTEM', NOW(), 'SYSTEM', NOW()
		) RETURNING closure_request_id`,
		fdID, bookingID, confirmationID, entityID, entityName,
		maturityDate.Format("2006-01-02"),
		principalAmount, rolloverAmount, tenorDays,
	).Scan(&closureRequestID)
	if err != nil {
		return fmt.Errorf("insert fd_closure_request: %w", err)
	}

	// Update fd_master with the closure request link
	_, err = db.Exec(ctx, `UPDATE investment.fd_master SET closure_request_id=$1,updated_at=NOW() WHERE fd_id=$2`, closureRequestID, fdID)
	if err != nil {
		return fmt.Errorf("update fd_master closure_request_id: %w", err)
	}

	// Record in fd_auto_renewal_log
	_, err = db.Exec(ctx, `
		INSERT INTO investment.fd_auto_renewal_log (
		  fd_id, closure_request_id, renewal_date, renewal_status,
		  new_tenor_days, rollover_amount, created_at
		) VALUES ($1,$2,CURRENT_DATE,'INITIATED',$3,$4,NOW())`,
		fdID, closureRequestID, tenorDays, rolloverAmount)
	if err != nil {
		// Non-fatal — closure request was already created
		log.Printf("[AutoRenewal] fd_auto_renewal_log insert failed for FD %s: %v", fdID, err)
	}

	log.Printf("[AutoRenewal] Created closure %s for FD %s (AUTO_RENEWAL, rollover=%.2f, tenor=%d days)",
		closureRequestID, fdID, rolloverAmount, tenorDays)
	return nil
}
