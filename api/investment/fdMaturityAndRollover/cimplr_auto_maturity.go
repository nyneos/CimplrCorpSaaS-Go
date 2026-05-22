package fdMaturityAndRollover

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RunCimplrAutoMaturityDue creates and finalizes payout/rollover confirms for approved
// initiates whose FD maturity date has arrived. Each initiate is processed at most once.
func RunCimplrAutoMaturityDue(ctx context.Context, pool *pgxpool.Pool) (processed, skipped, failed int) {
	_ = ensureCimplrExecutionLogTable(ctx, pool)
	generateAutoRenewalInitiates(ctx, pool)

	rows, err := pool.Query(ctx, `
		SELECT
			i.closure_initiate_id,
			i.fd_id,
			i.closure_type,
			COALESCE(i.requested_closure_date::text, '') AS requested_closure_date,
			COALESCE(i.rollover_amount_basis, 'PRINCIPAL_ONLY') AS rollover_amount_basis,
			COALESCE(i.tentative_new_tenor_days, m.tenure_days, 365) AS new_tenor_days,
			COALESCE(m.interest_rate, 0) AS new_interest_rate,
			COALESCE(ia.checker_by, ia.requested_by, i.created_by, '') AS actor_email,
			COALESCE(ia.requested_by, i.created_by, '') AS actor_user_id
		FROM cimplr.fd_closure_initiate i
		JOIN investment.fd_master m ON m.fd_id = i.fd_id
		LEFT JOIN LATERAL (
			SELECT requested_by, checker_by
			FROM cimplr.fd_closure_initiate_audit a
			WHERE a.closure_initiate_id = i.closure_initiate_id
			  AND a.processing_status = 'APPROVED'
			ORDER BY a.checker_at DESC NULLS LAST, a.audit_id DESC
			LIMIT 1
		) ia ON true
		WHERE COALESCE(i.is_deleted, false) = false
		  AND i.closure_status = 'CONFIRM'
		  AND i.closure_type IN ('PAYOUT', 'ROLLOVER')
		  AND COALESCE(m.is_deleted, false) = false
		  AND m.fd_status = 'ACTIVE'
		  AND m.maturity_date <= CURRENT_DATE
		  AND NOT EXISTS (
			SELECT 1 FROM cimplr.fd_closure_confirm c
			WHERE c.closure_initiate_id = i.closure_initiate_id
			  AND COALESCE(c.is_deleted, false) = false
			  AND c.closure_status NOT IN ('REJECTED')
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM cimplr.fd_closure_execution_log l
			WHERE l.closure_initiate_id = i.closure_initiate_id
			  AND l.execution_source = 'AUTO_MATURITY'
			  AND l.status IN ('SUCCESS', 'SKIPPED')
		  )
		  AND NOT (
			i.closure_type = 'ROLLOVER'
			AND EXISTS (
				SELECT 1 FROM cimplr.fd_closure_execution_log l2
				WHERE l2.fd_id = i.fd_id
				  AND l2.closure_type = 'ROLLOVER'
				  AND l2.execution_source = 'AUTO_MATURITY'
				  AND l2.status = 'SUCCESS'
			)
		  )
		ORDER BY m.maturity_date ASC, i.closure_initiate_id ASC
		LIMIT 200`)
	if err != nil {
		return 0, 0, 0
	}
	defer rows.Close()

	for rows.Next() {
		var initiateID, fdID, closureType, reqDate, rolloverBasis, actorEmail, actorUserID string
		var newTenor int
		var newRate float64
		if err := rows.Scan(&initiateID, &fdID, &closureType, &reqDate, &rolloverBasis, &newTenor, &newRate, &actorEmail, &actorUserID); err != nil {
			failed++
			continue
		}
		if strings.TrimSpace(actorEmail) == "" {
			actorEmail = "system@cimplr.auto"
		}
		if strings.TrimSpace(actorUserID) == "" {
			actorUserID = actorEmail
		}
		if err := processCimplrAutoMaturityInitiate(ctx, pool, initiateID, fdID, closureType, reqDate, rolloverBasis, newTenor, newRate, actorUserID, actorEmail); err != nil {
			_ = insertCimplrExecutionLog(ctx, pool, initiateID, fdID, closureType, "", "AUTO_MATURITY", "FAILED", err.Error())
			failed++
		} else {
			processed++
		}
	}
	return processed, skipped, failed
}

func processCimplrAutoMaturityInitiate(
	ctx context.Context,
	pool *pgxpool.Pool,
	initiateID, fdID, closureType, reqDate, rolloverBasis string,
	newTenor int,
	newRate float64,
	actorUserID, actorEmail string,
) error {
	initiate, err := loadCimplrInitiateOld(ctx, pool, initiateID)
	if err != nil {
		return err
	}
	src, err := loadCimplrFDSource(ctx, pool, fdID)
	if err != nil {
		return err
	}
	calc, err := calculateCimplrClosure(ctx, pool, src, closureType, firstNonEmpty(reqDate, fmt.Sprint(initiate["requested_closure_date"])), true)
	if err != nil {
		return err
	}

	req := cimplrClosureConfirmRequest{
		UserID:             actorUserID,
		ClosureInitiateID: initiateID,
		RequestedClosureDate: firstNonEmpty(reqDate, src.MaturityDate.Format(constants.DateFormat)),
		PrincipalExpected:  src.Principal,
		InterestExpected:   calc.AccruedInterest,
		TDSExpected:        calc.TDSAmount,
		NetExpected:        calc.NetPayout,
		PrincipalReceived:  src.Principal,
		InterestReceived:   calc.RevisedInterestAmount,
		TDSDeducted:        calc.TDSAmount,
		NetAmountReceived:  calc.NetPayout,
		NewTenorDays:       newTenor,
		NewInterestRate:    newRate,
		RolloverAmountBasis: rolloverBasis,
		Remarks:            "Auto maturity confirm (system calculation)",
		Reason:             "AUTO_MATURITY_JOB",
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var closureConfirmID string
	err = tx.QueryRow(ctx, `
		INSERT INTO cimplr.fd_closure_confirm (
			closure_initiate_id, fd_id, booking_id, confirmation_id, entity_id, entity_name,
			bank_id, bank_name, fd_ref_no, bank_fd_ref_no, closure_type,
			confirmation_mode, bank_reference_no, actual_payout_date, requested_closure_date,
			premature_reason, principal_expected, interest_expected, tds_expected, net_expected,
			principal_received, interest_received, tds_deducted, net_amount_received,
			variance_type, resolution_action, remarks, closure_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'AUTO',NULL,$12::date,$13::date,
			NULL,$14,$15,$16,$17,$18,$19,$20,$21,NULL,NULL,$22,'CONFIRM'
		) RETURNING closure_confirm_id`,
		initiateID, src.FDID, nullStrOrNil(src.BookingID), nullStrOrNil(src.ConfirmationID), nullStrOrNil(src.EntityID), nullStrOrNil(src.EntityName),
		nullStrOrNil(src.BankID), nullStrOrNil(src.BankName), nullStrOrNil(src.FDRefNo), nullStrOrNil(src.BankFDRefNo), closureType,
		nullDateArg(src.MaturityDate.Format(constants.DateFormat)), nullDateArg(req.RequestedClosureDate),
		req.PrincipalExpected, req.InterestExpected, req.TDSExpected, req.NetExpected,
		req.PrincipalReceived, req.InterestReceived, req.TDSDeducted, req.NetAmountReceived, req.Remarks,
	).Scan(&closureConfirmID)
	if err != nil {
		return fmt.Errorf("auto confirm insert: %w", err)
	}

	if closureType == "ROLLOVER" {
		if err := upsertCimplrRolloverConfirm(ctx, tx, closureConfirmID, src, req, calc); err != nil {
			return err
		}
	}
	if err := insertCimplrCalculation(ctx, tx, initiateID, closureConfirmID, src, calc); err != nil {
		return err
	}
	if err := insertCimplrConfirmAudit(ctx, tx, closureConfirmID, initiateID, "CREATE", "APPROVED", req.Reason, actorUserID, nil); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE cimplr.fd_closure_confirm_audit
		SET checker_by=$1, checker_at=NOW(), checker_comment=$2, processing_status='APPROVED'
		WHERE closure_confirm_id=$3 AND processing_status LIKE 'PENDING%'`,
		actorEmail, "Auto-approved on maturity date", closureConfirmID)
	if err != nil {
		return err
	}
	varianceSummary, _ := persistCimplrConfirmVariances(ctx, tx, closureConfirmID, req, src, calc)
	open := cimplrVarianceOpenCount(varianceSummary)
	if open > 0 {
		_ = tx.Rollback(ctx)
		_ = insertCimplrExecutionLog(ctx, pool, initiateID, fdID, closureType, closureConfirmID, "AUTO_MATURITY", "SKIPPED", fmt.Sprintf("%d open variance(s) after system calc", open))
		return fmt.Errorf("open variance count %d — manual confirm required", open)
	}
	if err := cimplrAssertConfirmApprovable(ctx, tx, closureConfirmID); err != nil {
		_ = tx.Rollback(ctx)
		_ = insertCimplrExecutionLog(ctx, pool, initiateID, fdID, closureType, closureConfirmID, "AUTO_MATURITY", "SKIPPED", err.Error())
		return err
	}
	if err := finalizeCimplrConfirmApprovalTx(ctx, tx, closureConfirmID, actorEmail, "Auto maturity posting"); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return insertCimplrExecutionLog(ctx, pool, initiateID, fdID, closureType, closureConfirmID, "AUTO_MATURITY", "SUCCESS", "")
}

func ensureCimplrExecutionLogTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS cimplr.fd_closure_execution_log (
			log_id TEXT PRIMARY KEY DEFAULT ('FCEL-' || substr(md5(random()::text || clock_timestamp()::text), 1, 12)),
			closure_initiate_id TEXT,
			fd_id TEXT NOT NULL,
			closure_type TEXT NOT NULL,
			closure_confirm_id TEXT,
			execution_source TEXT NOT NULL DEFAULT 'AUTO_MATURITY',
			status TEXT NOT NULL,
			message TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_fd_closure_execution_log_initiate
			ON cimplr.fd_closure_execution_log (closure_initiate_id);
		CREATE INDEX IF NOT EXISTS idx_fd_closure_execution_log_fd_type
			ON cimplr.fd_closure_execution_log (fd_id, closure_type, status);
		CREATE INDEX IF NOT EXISTS idx_fd_closure_execution_log_created
			ON cimplr.fd_closure_execution_log (created_at DESC);
	`)
	return err
}

func insertCimplrExecutionLog(ctx context.Context, pool *pgxpool.Pool, initiateID, fdID, closureType, confirmID, source, status, message string) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_execution_log (
			closure_initiate_id, fd_id, closure_type, closure_confirm_id, execution_source, status, message
		) VALUES (NULLIF($1,''),$2,$3,NULLIF($4,''),$5,$6,NULLIF($7,''))`,
		initiateID, fdID, closureType, confirmID, source, status, message)
	return err
}

func generateAutoRenewalInitiates(ctx context.Context, pool *pgxpool.Pool) {
	// Auto-create initiate records for FDs with auto_renewal = true.
	// We set closure_type = 'ROLLOVER', rollover_type = 'PRINCIPAL_PLUS_INTEREST',
	// and closure_status = 'CONFIRM'.
	query := `
		INSERT INTO cimplr.fd_closure_initiate (
			fd_id, booking_id, confirmation_id, entity_id, entity_name,
			bank_id, bank_name, fd_ref_no, bank_fd_ref_no,
			closure_type, action_at_maturity, maturity_date, requested_closure_date,
			principal_amount, interest_type_code, interest_rate,
			auto_renewal_flag, maturity_status, action_required,
			rollover_type, rollover_bank_type,
			tentative_new_tenor_days, remarks, closure_status
		)
		SELECT 
			m.fd_id, m.booking_id, m.confirmation_id, b.entity_id, b.entity_name,
			m.bank_id, m.bank_name, m.fd_ref_no, b.bank_fd_ref_no,
			'ROLLOVER', 'ROLLOVER', m.maturity_date, m.maturity_date,
			m.principal_amount, m.interest_type_code, m.interest_rate,
			true, 'MATURED', false,
			'PRINCIPAL_PLUS_INTEREST', 'SAME_BANK',
			m.tenure_days, 'Auto-renewal initiated by system', 'CONFIRM'
		FROM investment.fd_master m
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
		WHERE m.fd_status = 'ACTIVE'
		  AND m.auto_renewal = true
		  AND m.maturity_date <= CURRENT_DATE
		  AND COALESCE(m.is_deleted, false) = false
		  AND NOT EXISTS (
			SELECT 1 FROM cimplr.fd_closure_initiate i 
			WHERE i.fd_id = m.fd_id 
			  AND COALESCE(i.is_deleted, false) = false 
			  AND i.closure_status != 'REJECTED'
		  )
	`
	_, _ = pool.Exec(ctx, query)
}
