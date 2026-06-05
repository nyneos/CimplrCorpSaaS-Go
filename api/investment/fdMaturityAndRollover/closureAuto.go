package fdMaturityAndRollover

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CimplrAutoMaturityRunNow exposes the auto-maturity sweep as an on-demand HTTP
// endpoint. Same logic the 1h background worker runs, but synchronous so QA /
// ops can trigger it without restarting or waiting for the next tick.
//
// Request body (all fields optional, used only for logging):
//
//	{ "user_id": "...", "user_email": "..." }
//
// Response:
//
//	{ "success": true, "rows": { "processed": N, "skipped": N, "failed": N, "elapsed_ms": N } }
func CimplrAutoMaturityRunNow(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string `json:"user_id"`
			UserEmail string `json:"user_email"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		actor := strings.TrimSpace(req.UserEmail)
		if actor == "" {
			actor = strings.TrimSpace(req.UserID)
		}
		if actor == "" {
			actor = "manual-trigger"
		}

		started := time.Now()
		api.LogInfo("[CimplrAutoMaturity] manual run triggered by actor=%s at %s",
			actor, started.Format(time.RFC3339))

		processed, skipped, failed := RunCimplrAutoMaturityDue(r.Context(), pool)
		elapsedMs := time.Since(started).Milliseconds()

		api.LogInfo("[CimplrAutoMaturity] manual run done actor=%s processed=%d skipped=%d failed=%d elapsed_ms=%d",
			actor, processed, skipped, failed, elapsedMs)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"processed":  processed,
			"skipped":    skipped,
			"failed":     failed,
			"elapsed_ms": elapsedMs,
			"actor":      actor,
		})
	}
}

// autoMaturityOutcome classifies a per-initiate result so callers can
// increment the right counter and emit a consistent debug line.
type autoMaturityOutcome string

const (
	autoMaturityOutcomeSuccess autoMaturityOutcome = "SUCCESS"
	autoMaturityOutcomeSkipped autoMaturityOutcome = "SKIPPED"
	autoMaturityOutcomeFailed  autoMaturityOutcome = "FAILED"
)

// RunCimplrAutoMaturityDue creates and finalizes payout/rollover confirms for approved
// initiates whose FD maturity date has arrived. Each initiate is processed at most once.
func RunCimplrAutoMaturityDue(ctx context.Context, pool *pgxpool.Pool) (processed, skipped, failed int) {
	if err := ensureCimplrExecutionLogTable(ctx, pool); err != nil {
		api.LogError("[CimplrAutoMaturity] ensureCimplrExecutionLogTable failed: %v", err)
	}
	generateAutoRenewalInitiates(ctx, pool)

	// cimplr.fd_closure_initiate has NO rollover_amount_basis column — that
	// preference lives only on the rollover_confirm detail row. For
	// auto-maturity we default to PRINCIPAL_PLUS_INTEREST (the standard
	// rollover semantic everywhere else in this module).
	rows, err := pool.Query(ctx, `
		SELECT
			i.closure_initiate_id,
			i.fd_id, 
			i.closure_type,
			COALESCE(i.requested_closure_date::text, '') AS requested_closure_date,
			'PRINCIPAL_PLUS_INTEREST'::text AS rollover_amount_basis,
			COALESCE(i.tentative_new_tenor_days, m.tenure_days, 365) AS new_tenor_days,
			COALESCE(m.interest_rate, 0) AS new_interest_rate,
			-- cimplr.fd_closure_initiate has no created_by column — actor
			-- attribution lives only on the audit table; fall back to '' so
			-- the Go layer applies its system@cimplr.auto default.
			COALESCE(ia.checker_by, ia.requested_by, '') AS actor_email,
			COALESCE(ia.requested_by, '')                AS actor_user_id
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
			  AND l.status = 'SUCCESS'
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
		api.LogError("[CimplrAutoMaturity] due-initiates query failed: %v", err)
		return 0, 0, 0
	}
	defer rows.Close()

	candidateCount := 0
	for rows.Next() {
		candidateCount++
		var initiateID, fdID, closureType, reqDate, rolloverBasis, actorEmail, actorUserID string
		var newTenor int
		var newRate float64
		if err := rows.Scan(&initiateID, &fdID, &closureType, &reqDate, &rolloverBasis, &newTenor, &newRate, &actorEmail, &actorUserID); err != nil {
			failed++
			api.LogError("[CimplrAutoMaturity] row scan failed candidate#=%d: %v", candidateCount, err)
			continue
		}
		if strings.TrimSpace(actorEmail) == "" {
			actorEmail = "system@cimplr.auto"
		}
		if strings.TrimSpace(actorUserID) == "" {
			actorUserID = actorEmail
		}
		api.LogInfo("[CimplrAutoMaturity] processing initiate_id=%s fd_id=%s type=%s tenor_days=%d rate=%v actor=%s",
			initiateID, fdID, closureType, newTenor, newRate, actorEmail)

		outcome, confirmID, err := processCimplrAutoMaturityInitiate(ctx, pool, autoMaturityInitiateParams{InitiateID: initiateID, FDID: fdID, ClosureType: closureType, ReqDate: reqDate, RolloverBasis: rolloverBasis, NewTenor: newTenor, NewRate: newRate, ActorUserID: actorUserID, ActorEmail: actorEmail})
		switch outcome {
		case autoMaturityOutcomeSuccess:
			processed++
			api.LogInfo("[CimplrAutoMaturity] SUCCESS initiate_id=%s fd_id=%s type=%s confirm_id=%s",
				initiateID, fdID, closureType, confirmID)
		case autoMaturityOutcomeSkipped:
			skipped++
			api.LogInfo("[CimplrAutoMaturity] SKIPPED initiate_id=%s fd_id=%s type=%s confirm_id=%s reason=%v",
				initiateID, fdID, closureType, confirmID, err)
		default:
			failed++
			if logErr := insertCimplrExecutionLog(ctx, pool, executionLogEntry{InitiateID: initiateID, FDID: fdID, ClosureType: closureType, ConfirmID: confirmID, Source: "AUTO_MATURITY", Status: "FAILED", Message: err.Error()}); logErr != nil {
				api.LogError("[CimplrAutoMaturity] execution log insert failed initiate_id=%s fd_id=%s: %v (original error: %v)",
					initiateID, fdID, logErr, err)
			}
			api.LogError("[CimplrAutoMaturity] FAILED initiate_id=%s fd_id=%s type=%s confirm_id=%s reason=%v",
				initiateID, fdID, closureType, confirmID, err)
		}
	}
	if err := rows.Err(); err != nil {
		api.LogError("[CimplrAutoMaturity] due-initiates row iteration failed: %v", err)
	}
	api.LogInfo("[CimplrAutoMaturity] sweep summary candidates=%d processed=%d skipped=%d failed=%d",
		candidateCount, processed, skipped, failed)
	go func(candidates, okCount, skipCount, failCount int) {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[CimplrAutoMaturity] notification panic: %v", rec)
			}
		}()
		notifcatalog.TriggerNotification(context.Background(), pool,
			"/investment/fd/closure/auto-maturity/run",
			fmt.Sprintf("FD-CLOSURE-AUTO-%d", time.Now().UnixNano()),
			map[string]interface{}{
				"record_id":   "AUTO_MATURITY",
				"event":       "FD_CLOSURE_AUTO_MATURITY_RUN",
				"actor_email": "system@cimplr.auto",
				"candidates":  candidates,
				"processed":   okCount,
				"skipped":     skipCount,
				"failed":      failCount,
			})
	}(candidateCount, processed, skipped, failed)
	return processed, skipped, failed
}

type autoMaturityInitiateParams struct {
	InitiateID    string
	FDID          string
	ClosureType   string
	ReqDate       string
	RolloverBasis string
	NewTenor      int
	NewRate       float64
	ActorUserID   string
	ActorEmail    string
}

func processCimplrAutoMaturityInitiate(ctx context.Context, pool *pgxpool.Pool, p autoMaturityInitiateParams) (autoMaturityOutcome, string, error) {
	initiateID := p.InitiateID
	fdID := p.FDID
	closureType := p.ClosureType
	reqDate := p.ReqDate
	rolloverBasis := p.RolloverBasis
	newTenor := p.NewTenor
	newRate := p.NewRate
	actorUserID := p.ActorUserID
	actorEmail := p.ActorEmail
	initiate, err := loadCimplrInitiateOld(ctx, pool, initiateID)
	if err != nil {
		return autoMaturityOutcomeFailed, "", fmt.Errorf("load initiate: %w", err)
	}
	src, err := loadCimplrFDSource(ctx, pool, fdID)
	if err != nil {
		return autoMaturityOutcomeFailed, "", fmt.Errorf("load fd source fd_id=%s: %w", fdID, err)
	}
	calc, err := calculateCimplrClosure(ctx, pool, src, closureType, firstNonEmpty(reqDate, fmt.Sprint(initiate["requested_closure_date"])), true)
	if err != nil {
		return autoMaturityOutcomeFailed, "", fmt.Errorf("calculate closure fd_id=%s type=%s: %w", fdID, closureType, err)
	}

	expectedNewFD := cimplrExpectedRolloverNewFD(src, calc, rolloverBasis)
	req := cimplrClosureConfirmRequest{
		UserID:               actorUserID,
		ClosureInitiateID:    initiateID,
		RequestedClosureDate: firstNonEmpty(reqDate, src.MaturityDate.Format(constants.DateFormat)),
		PrincipalExpected:    src.Principal,
		InterestExpected:     calc.AccruedInterest,
		TDSExpected:          calc.TDSAmount,
		NetExpected:          calc.NetPayout,
		PrincipalReceived:    src.Principal,
		InterestReceived:     calc.AccruedInterest,
		TDSDeducted:          calc.TDSAmount,
		NetAmountReceived:    calc.NetPayout,
		NewTenorDays:         newTenor,
		NewInterestRate:      newRate,
		RolloverAmountBasis:  rolloverBasis,
		NewFDAmount:          expectedNewFD,
		ClosureAmount:        calc.NetPayout,
		Remarks:              "Auto maturity confirm (system calculation)",
		Reason:               "AUTO_MATURITY_JOB",
	}
	if closureType == "PREMATURE" {
		req.InterestReceived = calc.RevisedInterestAmount
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return autoMaturityOutcomeFailed, "", fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var closureConfirmID string
	// confirmation_mode must be NULL, 'API', 'EMAIL', or 'UPLOAD' per
	// fd_closure_confirm_mode_check — there is no 'AUTO' value. System runs
	// are identified via remarks + audit reason (AUTO_MATURITY_JOB).
	err = tx.QueryRow(ctx, `
		INSERT INTO cimplr.fd_closure_confirm (
			closure_initiate_id, fd_id, booking_id, confirmation_id, entity_id, entity_name,
			bank_id, bank_name, fd_ref_no, bank_fd_ref_no, closure_type,
			confirmation_mode, bank_reference_no, actual_payout_date, requested_closure_date,
			premature_reason, principal_expected, interest_expected, tds_expected, net_expected,
			principal_received, interest_received, tds_deducted, net_amount_received,
			variance_type, resolution_action, remarks, closure_status
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULL,NULL,$12::date,$13::date,
			NULL,$14,$15,$16,$17,$18,$19,$20,$21,NULL,NULL,$22,'CONFIRM'
		) RETURNING closure_confirm_id`,
		initiateID, src.FDID, nullStrOrNil(src.BookingID), nullStrOrNil(src.ConfirmationID), nullStrOrNil(src.EntityID), nullStrOrNil(src.EntityName),
		nullStrOrNil(src.BankID), nullStrOrNil(src.BankName), nullStrOrNil(src.FDRefNo), nullStrOrNil(src.BankFDRefNo), closureType,
		nullDateArg(src.MaturityDate.Format(constants.DateFormat)), nullDateArg(req.RequestedClosureDate),
		req.PrincipalExpected, req.InterestExpected, req.TDSExpected, req.NetExpected,
		req.PrincipalReceived, req.InterestReceived, req.TDSDeducted, req.NetAmountReceived, req.Remarks,
	).Scan(&closureConfirmID)
	if err != nil {
		return autoMaturityOutcomeFailed, "", fmt.Errorf("auto confirm insert: %w", err)
	}

	if closureType == "ROLLOVER" {
		if err := upsertCimplrRolloverConfirm(ctx, tx, closureConfirmID, src, req, calc); err != nil {
			return autoMaturityOutcomeFailed, closureConfirmID, fmt.Errorf("upsert rollover confirm: %w", err)
		}
	}
	if err := insertCimplrCalculation(ctx, tx, initiateID, closureConfirmID, src, calc); err != nil {
		return autoMaturityOutcomeFailed, closureConfirmID, fmt.Errorf("insert calculation: %w", err)
	}
	if err := insertCimplrConfirmAudit(ctx, tx, confirmAuditEntry{ConfirmID: closureConfirmID, InitiateID: initiateID, Action: "CREATE", Status: constants.StatusApproved, Reason: req.Reason, RequestedBy: actorUserID, Old: nil}); err != nil {
		return autoMaturityOutcomeFailed, closureConfirmID, fmt.Errorf("insert confirm audit: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE cimplr.fd_closure_confirm_audit
		SET checker_by=$1, checker_at=NOW(), checker_comment=$2, processing_status='APPROVED'
		WHERE closure_confirm_id=$3 AND processing_status LIKE 'PENDING%'`,
		actorEmail, "Auto-approved on maturity date", closureConfirmID)
	if err != nil {
		return autoMaturityOutcomeFailed, closureConfirmID, fmt.Errorf("stamp approved audit: %w", err)
	}
	varianceSummary, varErr := persistCimplrConfirmVariances(ctx, tx, closureConfirmID, req, src, calc)
	if varErr != nil {
		return autoMaturityOutcomeFailed, closureConfirmID, fmt.Errorf("persist variances: %w", varErr)
	}
	open := cimplrVarianceOpenCount(varianceSummary)
	if open > 0 {
		_ = tx.Rollback(ctx)
		varDebug := cimplrFormatOpenVarianceDebug(varianceSummary)
		msg := fmt.Sprintf("%d open variance(s) after system calc — %s", open, varDebug)
		api.LogInfo("[CimplrAutoMaturity] variance skip initiate_id=%s fd_id=%s confirm_id=%s %s",
			initiateID, fdID, closureConfirmID, varDebug)
		if logErr := insertCimplrExecutionLog(ctx, pool, executionLogEntry{InitiateID: initiateID, FDID: fdID, ClosureType: closureType, ConfirmID: closureConfirmID, Source: "AUTO_MATURITY", Status: "SKIPPED", Message: msg}); logErr != nil {
			api.LogError("[CimplrAutoMaturity] execution log insert failed (variance skip) initiate_id=%s: %v", initiateID, logErr)
		}
		return autoMaturityOutcomeSkipped, closureConfirmID, fmt.Errorf("open variance count %d — manual confirm required (%s)", open, varDebug)
	}
	if err := cimplrAssertConfirmApprovable(ctx, tx, closureConfirmID); err != nil {
		_ = tx.Rollback(ctx)
		outcome := autoMaturityOutcomeFailed
		if strings.Contains(err.Error(), "cannot approve:") || strings.Contains(err.Error(), "open variance") || strings.Contains(err.Error(), "manual confirm") {
			outcome = autoMaturityOutcomeSkipped
		}
		if logErr := insertCimplrExecutionLog(ctx, pool, executionLogEntry{InitiateID: initiateID, FDID: fdID, ClosureType: closureType, ConfirmID: closureConfirmID, Source: "AUTO_MATURITY", Status: string(outcome), Message: err.Error()}); logErr != nil {
			api.LogError("[CimplrAutoMaturity] execution log insert failed (approvable check) initiate_id=%s: %v", initiateID, logErr)
		}
		return outcome, closureConfirmID, err
	}
	if err := finalizeCimplrConfirmApprovalTx(ctx, tx, closureConfirmID, actorEmail, "Auto maturity posting"); err != nil {
		_ = tx.Rollback(ctx)
		return autoMaturityOutcomeFailed, closureConfirmID, fmt.Errorf("finalize approval/post: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return autoMaturityOutcomeFailed, closureConfirmID, fmt.Errorf("commit tx: %w", err)
	}
	if err := insertCimplrExecutionLog(ctx, pool, executionLogEntry{InitiateID: initiateID, FDID: fdID, ClosureType: closureType, ConfirmID: closureConfirmID, Source: "AUTO_MATURITY", Status: "SUCCESS", Message: ""}); err != nil {
		api.LogError("[CimplrAutoMaturity] execution log insert failed (success) initiate_id=%s confirm_id=%s: %v",
			initiateID, closureConfirmID, err)
	}
	return autoMaturityOutcomeSuccess, closureConfirmID, nil
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

type executionLogEntry struct {
	InitiateID  string
	FDID        string
	ClosureType string
	ConfirmID   string
	Source      string
	Status      string
	Message     string
}

func insertCimplrExecutionLog(ctx context.Context, pool *pgxpool.Pool, e executionLogEntry) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO cimplr.fd_closure_execution_log (
			closure_initiate_id, fd_id, closure_type, closure_confirm_id, execution_source, status, message
		) VALUES (NULLIF($1,''),$2,$3,NULLIF($4,''),$5,$6,NULLIF($7,''))`,
		e.InitiateID, e.FDID, e.ClosureType, e.ConfirmID, e.Source, e.Status, e.Message)
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
		-- investment.fd_master has no fd_ref_no column — the externally-
		-- recognisable reference is bank_fd_ref_no; fd_id is a safe fallback.
		-- Mirrors what loadCimplrFDSource uses for src.FDRefNo.
		--
		-- fd_closure_initiate_maturity_status_check only allows
		-- 'OVERDUE', 'DUE', 'UPCOMING' (see deriveCimplrMaturityStatus); we
		-- derive the right value from the maturity_date instead of using the
		-- (illegal) string 'MATURED'.
		SELECT
			m.fd_id, m.booking_id, m.confirmation_id, b.entity_id, b.entity_name,
			m.bank_id, m.bank_name,
			COALESCE(m.bank_fd_ref_no, m.fd_id) AS fd_ref_no,
			COALESCE(m.bank_fd_ref_no, '')      AS bank_fd_ref_no,
			'ROLLOVER', 'ROLLOVER', m.maturity_date, m.maturity_date,
			m.principal_amount, m.interest_type_code, m.interest_rate,
			true,
			CASE WHEN m.maturity_date < CURRENT_DATE THEN 'OVERDUE'
			     WHEN m.maturity_date = CURRENT_DATE THEN 'DUE'
			     ELSE 'UPCOMING' END,
			false,
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
	if tag, err := pool.Exec(ctx, query); err != nil {
		api.LogError("[CimplrAutoMaturity] generateAutoRenewalInitiates failed: %v", err)
	} else {
		api.LogInfo("[CimplrAutoMaturity] generateAutoRenewalInitiates inserted=%d auto-renewal initiate(s)", tag.RowsAffected())
	}
}
