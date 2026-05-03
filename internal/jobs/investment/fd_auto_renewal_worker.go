package jobs

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger")

// StartAutoRenewalWorker runs daily and processes FDs with auto_renewal=true
// whose maturity_date has passed. It creates a ROLLOVER closure request (type
// FD_CLOSURE_AUTO_RENEWAL) and logs the result in fd_auto_renewal_log.
//
// Fixed bugs (vs original):
//  1. closure_type is now 'ROLLOVER' (was 'AUTO_RENEWAL' which had no approval hook).
//  2. Reads accrued_interest and tds_deducted from fd_accrual_ledger so accounting
//     entries reflect what was actually earned (was hardcoded 0, 0).
//  3. rollover_amount = principal + net_interest (was just principal).
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

	logger.LogInfo("[AutoRenewal] Starting auto-renewal run at %s", time.Now().Format(time.RFC3339))

	// Fetch all ACTIVE FDs with auto_renewal=true and maturity_date <= today
	// that don't already have a non-rejected / non-cancelled closure request.
	// Also pull accrued interest and TDS from fd_accrual_ledger so we can
	// create an accurate closure record (not zeroed-out).
	rows, err := db.Query(ctx, `
		SELECT
		  m.fd_id,
		  COALESCE(m.booking_id,'')         AS booking_id,
		  COALESCE(m.confirmation_id,'')    AS confirmation_id,
		  COALESCE(b.entity_id,'')          AS entity_id,
		  COALESCE(b.entity_name,'')        AS entity_name,
		  m.principal_amount,
		  m.maturity_date,
		  COALESCE(m.tenure_days, 365)      AS tenor_days,
		  COALESCE(al.total_accrued, 0)     AS accrued_interest,
		  COALESCE(al.total_tds, 0)         AS tds_deducted
		FROM investment.fd_master m
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
		LEFT JOIN LATERAL (
		  SELECT
		    SUM(COALESCE(period_interest_accrued,0)) AS total_accrued,
		    SUM(COALESCE(tds_deducted_in_period,0))  AS total_tds
		  FROM investment.fd_accrual_ledger
		  WHERE fd_id = m.fd_id
		    AND COALESCE(is_deleted, false) = false
		) al ON true
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
		logger.LogError("[AutoRenewal] Query error: %v", err)
		return
	}
	defer rows.Close()

	type fdRow struct {
		FDID, BookingID, ConfirmationID string
		EntityID, EntityName            string
		PrincipalAmount                 float64
		MaturityDate                    time.Time
		TenorDays                       int
		AccruedInterest                 float64
		TDSDeducted                     float64
	}

	var fds []fdRow
	for rows.Next() {
		var f fdRow
		if sErr := rows.Scan(
			&f.FDID, &f.BookingID, &f.ConfirmationID,
			&f.EntityID, &f.EntityName,
			&f.PrincipalAmount, &f.MaturityDate, &f.TenorDays,
			&f.AccruedInterest, &f.TDSDeducted,
		); sErr != nil {
			logger.LogError("[AutoRenewal] Scan error: %v", sErr)
			continue
		}
		fds = append(fds, f)
	}
	rows.Close()

	processed, failed := 0, 0
	for _, fd := range fds {
		// rollover_amount = full reinvestment: principal + net interest (after TDS)
		rolloverAmount := fd.PrincipalAmount + fd.AccruedInterest - fd.TDSDeducted
		if rolloverAmount < fd.PrincipalAmount {
			// Safety: never roll over less than principal (e.g. negative interest edge case)
			rolloverAmount = fd.PrincipalAmount
		}

		if processErr := processAutoRenewalFD(ctx, AutoRenewalFDParams{
			DB: db,
			FDID: fd.FDID, BookingID: fd.BookingID, ConfirmationID: fd.ConfirmationID,
			EntityID: fd.EntityID, EntityName: fd.EntityName,
			PrincipalAmount: fd.PrincipalAmount, AccruedInterest: fd.AccruedInterest,
			TDSDeducted: fd.TDSDeducted, RolloverAmount: rolloverAmount,
			MaturityDate: fd.MaturityDate, TenorDays: fd.TenorDays,
		}); processErr != nil {
			logger.LogError("[AutoRenewal] Failed for FD %s: %v", fd.FDID, processErr)
			_, _ = db.Exec(ctx, `
				INSERT INTO investment.fd_auto_renewal_log (
				  fd_id, renewal_date, renewal_status, new_tenor_days,
				  rollover_amount, failure_reason, created_at
				) VALUES ($1, CURRENT_DATE, 'FAILED', $2, $3, $4, NOW())`,
				fd.FDID, fd.TenorDays, rolloverAmount, processErr.Error())
			failed++
			continue
		}
		processed++
	}

	logger.LogError("[AutoRenewal] Run complete — processed=%d failed=%d total=%d at %s",
		processed, failed, len(fds), time.Now().Format(time.RFC3339))
}

// AutoRenewalFDParams holds all inputs for processAutoRenewalFD.
type AutoRenewalFDParams struct {
	DB              *pgxpool.Pool
	FDID            string
	BookingID       string
	ConfirmationID  string
	EntityID        string
	EntityName      string
	PrincipalAmount float64
	AccruedInterest float64
	TDSDeducted     float64
	RolloverAmount  float64
	MaturityDate    time.Time
	TenorDays       int
}

func processAutoRenewalFD(ctx context.Context, p AutoRenewalFDParams) error {
	db := p.DB
	fdID := p.FDID
	bookingID := p.BookingID
	confirmationID := p.ConfirmationID
	entityID := p.EntityID
	entityName := p.EntityName
	principalAmount := p.PrincipalAmount
	accruedInterest := p.AccruedInterest
	tdsDeducted := p.TDSDeducted
	rolloverAmount := p.RolloverAmount
	maturityDate := p.MaturityDate
	tenorDays := p.TenorDays
	rolloverType := "FULL"
	// net_payout_amount = full rollover amount (principal + net interest)
	netPayout := rolloverAmount

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
		  'ROLLOVER','PENDING_APPROVAL',
		  CURRENT_DATE, $6::date, $6::date,
		  $7, $8, $9, 0, $10,
		  $11, $12,
		  'RENEW', 'System auto-renewal triggered at maturity',
		  'SYSTEM', 'system@cimplr.in',
		  false, false,
		  'SYSTEM', NOW(), 'SYSTEM', NOW()
		) RETURNING closure_request_id`,
		fdID, bookingID, confirmationID, entityID, entityName,
		maturityDate.Format(constants.DateFormat),
		principalAmount, accruedInterest, tdsDeducted, netPayout,
		rolloverAmount, tenorDays,
	).Scan(&closureRequestID)
	if err != nil {
		return fmt.Errorf("insert fd_closure_request: %w", err)
	}

	// Insert sub-table row for rollover detail
	_, _ = db.Exec(ctx, `
		INSERT INTO investment.fd_closure_rollover (
		  closure_request_id, source_fd_id, rollover_date,
		  rollover_amount, rollover_principal, interest_credited, tds_deducted,
		  new_tenor_days, rollover_type, created_by
		) VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,'SYSTEM')`,
		closureRequestID, fdID, maturityDate.Format(constants.DateFormat),
		rolloverAmount, principalAmount, accruedInterest, tdsDeducted,
		tenorDays, rolloverType)

	// Read source FD config to mirror onto the new booking request.
	var srcBankID, srcBankName, srcBankConfigID, srcSourceAccountID string
	var srcInterestTypeCode, srcFrequencyID, srcDayCountCode, srcTDSPlanID string
	var srcInterestRate float64
	_ = db.QueryRow(ctx, `
		SELECT
		  COALESCE(m.bank_id,''), COALESCE(m.bank_name,''),
		  COALESCE(b.bank_config_id,''), COALESCE(b.source_account_id,''),
		  COALESCE(m.interest_type_code,'SIMPLE'), COALESCE(b.frequency_id,''),
		  COALESCE(m.day_count_code,''), COALESCE(b.tds_plan_id,''),
		  COALESCE(m.interest_rate,0)
		FROM investment.fd_master m
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
		WHERE m.fd_id = $1 LIMIT 1`, fdID,
	).Scan(
		&srcBankID, &srcBankName,
		&srcBankConfigID, &srcSourceAccountID,
		&srcInterestTypeCode, &srcFrequencyID,
		&srcDayCountCode, &srcTDSPlanID,
		&srcInterestRate,
	)

	// Create fd_booking_request (BOOKING_PENDING) so ops can place the new FD
	// with the bank. The entry mirrors the source FD's configuration.
	var newBookingID string
	bookErr := db.QueryRow(ctx, `
		INSERT INTO investment.fd_booking_request (
		  entity_id, entity_name,
		  bank_id, bank_name,
		  bank_config_id, source_account_id,
		  principal_amount, interest_rate, tenure_days,
		  interest_type_code, frequency_id, day_count_code, tds_plan_id,
		  booking_status, booking_remarks, source_closure_request_id,
		  created_by, created_at, updated_by, updated_at
		) VALUES (
		  NULLIF($1,''), NULLIF($2,''),
		  NULLIF($3,''), NULLIF($4,''),
		  NULLIF($5,''), NULLIF($6,''),
		  $7, $8, $9,
		  NULLIF($10,''), NULLIF($11,''), NULLIF($12,''), NULLIF($13,''),
		  'BOOKING_PENDING',
		  $14, $15,
		  'SYSTEM', NOW(), 'SYSTEM', NOW()
		) RETURNING booking_id`,
		entityID, entityName,
		srcBankID, srcBankName,
		srcBankConfigID, srcSourceAccountID,
		rolloverAmount, srcInterestRate, tenorDays,
		srcInterestTypeCode, srcFrequencyID, srcDayCountCode, srcTDSPlanID,
		fmt.Sprintf("Auto-renewal booking — source closure %s source FD %s", closureRequestID, fdID),
		closureRequestID,
	).Scan(&newBookingID)
	if bookErr != nil {
		logger.LogError("[AutoRenewal] fd_booking_request insert failed for FD %s closure %s: %v", fdID, closureRequestID, bookErr)
	} else if newBookingID != "" {
		_, _ = db.Exec(ctx,
			`UPDATE investment.fd_closure_rollover SET new_fd_booking_id=$1 WHERE closure_request_id=$2`,
			newBookingID, closureRequestID)
		logger.LogInfo("[AutoRenewal] Created booking_request %s (BOOKING_PENDING) for closure %s FD %s", newBookingID, closureRequestID, fdID)
	}

	// Link closure request on fd_master
	_, err = db.Exec(ctx,
		`UPDATE investment.fd_master SET closure_request_id=$1, updated_at=NOW() WHERE fd_id=$2`,
		closureRequestID, fdID)
	if err != nil {
		return fmt.Errorf("update fd_master closure_request_id: %w", err)
	}

	// Log the auto-renewal
	_, err = db.Exec(ctx, `
		INSERT INTO investment.fd_auto_renewal_log (
		  fd_id, closure_request_id, renewal_date, renewal_status,
		  new_tenor_days, rollover_amount, created_at
		) VALUES ($1,$2,CURRENT_DATE,'INITIATED',$3,$4,NOW())`,
		fdID, closureRequestID, tenorDays, rolloverAmount)
	if err != nil {
		logger.LogError("[AutoRenewal] fd_auto_renewal_log insert failed for FD %s: %v", fdID, err)
	}

	logger.LogInfo("[AutoRenewal] Created closure %s for FD %s (ROLLOVER/auto-renewal, principal=%.2f accrued=%.2f tds=%.2f rollover=%.2f tenor=%d days)",
		closureRequestID, fdID, principalAmount, accruedInterest, tdsDeducted, rolloverAmount, tenorDays)
	return nil
}
