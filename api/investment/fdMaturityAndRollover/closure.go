package fdMaturityAndRollover

// closure.go — FD Closure Lifecycle Handlers
//
// Handlers:
//   GetFDsNearMaturity        POST /investment/fd/closure/maturity-dashboard
//   InitiateClosure           POST /investment/fd/closure/initiate
//   UpdateClosure             POST /investment/fd/closure/update
//   GetAllClosureRequests     POST /investment/fd/closure/all
//   GetClosureDetail          POST /investment/fd/closure/detail
//   BulkApproveClosureRequest POST /investment/fd/closure/bulk-approve
//   BulkRejectClosureRequest  POST /investment/fd/closure/bulk-reject
//   DeleteClosureRequest      POST /investment/fd/closure/delete

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dbExec is an interface satisfied by both pgx.Tx and pgxpool.Pool.
type dbExec interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, arguments ...interface{}) pgx.Row
	Query(ctx context.Context, sql string, arguments ...interface{}) (pgx.Rows, error)
}

type closureIDsRequest struct {
	UserID            string   `json:"user_id"`
	ClosureRequestIDs []string `json:"closure_request_ids"`
	Comment           string   `json:"comment"`
}

func getUserEmail(userID string) string {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			return s.Email
		}
	}
	return ""
}

func nullStrOrNil(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func roundToFour(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// Handler 1 — GetFDsNearMaturity

type maturityDashboardRequest struct {
	UserID      string `json:"user_id"`
	EntityID    string `json:"entity_id"`
	BankID      string `json:"bank_id"`
	DaysAhead   int    `json:"days_ahead"`
	IncludePast bool   `json:"include_past"`
	Page        int    `json:"page"`
	PageSize    int    `json:"page_size"`
}

func GetFDsNearMaturity(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req maturityDashboardRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.DaysAhead <= 0 {
			req.DaysAhead = 30
		}
		if req.Page <= 0 {
			req.Page = 1
		}
		if req.PageSize <= 0 || req.PageSize > 200 {
			req.PageSize = 50
		}
		offset := (req.Page - 1) * req.PageSize
		ctx := r.Context()

		conditions := []string{
			"m.is_deleted = false",
			"m.fd_status IN ('ACTIVE','MATURED')",
		}
		args := []interface{}{}
		argIdx := 1

		if req.IncludePast {
			conditions = append(conditions,
				fmt.Sprintf("m.maturity_date <= CURRENT_DATE + INTERVAL '%d days'", req.DaysAhead))
		} else {
			conditions = append(conditions, fmt.Sprintf(
				"m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '%d days'", req.DaysAhead))
		}
		if req.EntityID != "" {
			args = append(args, req.EntityID)
			conditions = append(conditions, fmt.Sprintf("b.entity_id = $%d", argIdx))
			argIdx++
		}
		if req.BankID != "" {
			args = append(args, req.BankID)
			conditions = append(conditions, fmt.Sprintf("m.bank_id = $%d", argIdx))
			argIdx++
		}
		where := strings.Join(conditions, " AND ")

		var totalCount int
		_ = pool.QueryRow(ctx,
			fmt.Sprintf("SELECT COUNT(DISTINCT m.fd_id) FROM investment.fd_master m LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id WHERE %s", where),
			args...).Scan(&totalCount)

		limitArgs := append(args, req.PageSize, offset)

		dataSQL := fmt.Sprintf(`
			SELECT
			  m.fd_id, m.booking_id, m.confirmation_id,
			  COALESCE(m.bank_id,'') AS bank_id,
			  COALESCE(m.bank_name,'') AS bank_name,
			  COALESCE(m.fd_reference_number,'') AS fd_reference_number,
			  m.fd_status, m.principal_amount, m.interest_rate, m.interest_type_code,
			  m.tenor_days, m.start_date, m.maturity_date,
			  COALESCE(m.maturity_amount, 0) AS maturity_amount,
			  COALESCE(m.maturity_instructions,'PENDING') AS maturity_instructions,
			  COALESCE(m.auto_renewal, false) AS auto_renewal,
			  COALESCE(m.day_count_convention,'') AS day_count_convention,
			  (m.maturity_date - CURRENT_DATE) AS days_to_maturity,
			  COALESCE(b.entity_id,'') AS entity_id,
			  COALESCE(b.entity_name,'') AS entity_name,
			  COALESCE(b.frequency_id,'') AS frequency_id,
			  COALESCE(b.bank_account_id,'') AS source_account_id,
			  COALESCE(b.tds_plan_id,'') AS tds_plan_id,
			  COALESCE(c.confirmation_status,'') AS confirmation_status,
			  COALESCE(c.confirmation_date::text,'') AS confirmation_date,
			  COALESCE(c.bank_fd_reference,'') AS bank_fd_reference,
			  COALESCE(al.total_interest_accrued, 0) AS total_interest_accrued,
			  COALESCE(al.total_tds_accrued, 0) AS total_tds_accrued,
			  COALESCE(p.penalty_value, 0) AS penalty_value,
			  COALESCE(p.penalty_type,'NONE') AS penalty_type,
			  COALESCE(p.no_interest_if_withdrawn_before, 0) AS no_interest_min_days,
			  COALESCE(cr.closure_request_id,'') AS closure_request_id,
			  COALESCE(cr.closure_type,'') AS closure_type,
			  COALESCE(cr.closure_status,'') AS closure_status,
			  COALESCE(cf.total_periods,0) AS total_cashflow_periods,
			  COALESCE(cf.last_event_date::text,'') AS last_cashflow_event
			FROM investment.fd_master m
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
			LEFT JOIN investment.fd_confirmation c ON c.confirmation_id = m.confirmation_id
			LEFT JOIN LATERAL (
			  SELECT SUM(COALESCE(interest_accrued,0)) AS total_interest_accrued,
			         SUM(COALESCE(tds_amount,0)) AS total_tds_accrued
			  FROM investment.fd_accrual_ledger
			  WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false
			) al ON true
			LEFT JOIN LATERAL (
			  SELECT penalty_value, penalty_type, no_interest_if_withdrawn_before
			  FROM investment.fd_penalty_structure_master ps
			  JOIN investment.fd_bank_config_master bc ON bc.bank_code = ps.bank_code
			  WHERE bc.bank_id = m.bank_id AND COALESCE(ps.is_deleted,false)=false
			  ORDER BY ps.penalty_value DESC LIMIT 1
			) p ON true
			LEFT JOIN LATERAL (
			  SELECT closure_request_id, closure_type, closure_status
			  FROM investment.fd_closure_request
			  WHERE fd_id = m.fd_id AND is_deleted=false
			  ORDER BY created_at DESC LIMIT 1
			) cr ON true
			LEFT JOIN LATERAL (
			  SELECT COUNT(*) AS total_periods, MAX(event_date) AS last_event_date
			  FROM investment.fd_cashflow_schedule WHERE fd_id = m.fd_id
			) cf ON true
			WHERE %s
			ORDER BY m.maturity_date ASC
			LIMIT $%d OFFSET $%d`, where, argIdx, argIdx+1)

		rows, err := pool.Query(ctx, dataSQL, limitArgs...)
		if err != nil {
			api.LogError("[FDClosure] GetFDsNearMaturity query error: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch maturity dashboard")
			return
		}
		defer rows.Close()

		var records []map[string]interface{}
		for rows.Next() {
			vals, _ := rows.Values()
			colDescs := rows.FieldDescriptions()
			row := make(map[string]interface{}, len(colDescs))
			for i, col := range colDescs {
				row[string(col.Name)] = vals[i]
			}
			records = append(records, row)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"records": records, "total": totalCount, "page": req.Page,
			"page_size": req.PageSize, "days_ahead": req.DaysAhead,
		})
		api.LogInfo("[FDClosure] GetFDsNearMaturity: %d/%d by %s", len(records), totalCount, userEmail)
	}
}

// Handler 2 — InitiateClosure

type initiateClosureRequest struct {
	UserID               string  `json:"user_id"`
	FDID                 string  `json:"fd_id"`
	ClosureType          string  `json:"closure_type"`
	EffectiveClosureDate string  `json:"effective_closure_date"`
	MaturityInstructions string  `json:"maturity_instructions"`
	SettlementAccountID  string  `json:"settlement_account_id"`
	RolloverAmount       float64 `json:"rollover_amount"`
	RolloverTenorDays    int     `json:"rollover_tenor_days"`
	ClosureReason        string  `json:"closure_reason"`
	ClosureNotes         string  `json:"closure_notes"`
}

func InitiateClosure(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req initiateClosureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
			return
		}
		validClosureTypes := map[string]bool{"MATURITY": true, "PREMATURE": true, "ROLLOVER": true}
		if !validClosureTypes[req.ClosureType] {
			api.RespondWithError(w, http.StatusBadRequest, "closure_type must be MATURITY, PREMATURE, or ROLLOVER")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		var (
			bookingID, confirmationID string
			entityID, entityName      string
			bankID                    string
			principalAmount           float64
			maturityDate              time.Time
			currentStatus             string
		)
		err := pool.QueryRow(ctx, `
			SELECT COALESCE(m.booking_id,''), COALESCE(m.confirmation_id,''),
			       COALESCE(b.entity_id,''), COALESCE(b.entity_name,''),
			       COALESCE(m.bank_id,''), m.principal_amount,
			       m.maturity_date, m.fd_status
			FROM investment.fd_master m
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
			WHERE m.fd_id = $1 AND m.is_deleted = false`, req.FDID,
		).Scan(&bookingID, &confirmationID, &entityID, &entityName, &bankID,
			&principalAmount, &maturityDate, &currentStatus)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, "FD not found")
			} else {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to load FD")
			}
			return
		}
		if currentStatus != "ACTIVE" && currentStatus != "MATURED" {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("FD is in status %s — closure requires ACTIVE or MATURED", currentStatus))
			return
		}

		var existingOpenCount int
		_ = pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM investment.fd_closure_request
			WHERE fd_id=$1 AND is_deleted=false
			  AND closure_status NOT IN ('REJECTED','CANCELLED')`, req.FDID,
		).Scan(&existingOpenCount)
		if existingOpenCount > 0 {
			api.RespondWithError(w, http.StatusConflict, "An open closure request already exists for this FD")
			return
		}

		var accruedInterest, tdsDeducted float64
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(interest_accrued),0), COALESCE(SUM(tds_amount),0)
			FROM investment.fd_accrual_ledger
			WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false`, req.FDID,
		).Scan(&accruedInterest, &tdsDeducted)

		var penaltyAmount, penaltyRate float64
		var noInterestFlag bool
		if req.ClosureType == "PREMATURE" {
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(ps.penalty_value,0),
				       COALESCE(ps.no_interest_if_withdrawn_before,0) > 0
				FROM investment.fd_penalty_structure_master ps
				JOIN investment.fd_bank_config_master bc ON bc.bank_code = ps.bank_code
				WHERE bc.bank_id=$1 AND COALESCE(ps.is_deleted,false)=false
				ORDER BY ps.penalty_value DESC LIMIT 1`, bankID,
			).Scan(&penaltyRate, &noInterestFlag)

			if noInterestFlag {
				var daysHeld, minDays int
				_ = pool.QueryRow(ctx, `SELECT COALESCE((CURRENT_DATE - start_date)::int,0) FROM investment.fd_master WHERE fd_id=$1`, req.FDID).Scan(&daysHeld)
				_ = pool.QueryRow(ctx, `SELECT COALESCE(no_interest_if_withdrawn_before,0) FROM investment.fd_penalty_structure_master ps JOIN investment.fd_bank_config_master bc ON bc.bank_code=ps.bank_code WHERE bc.bank_id=$1 ORDER BY penalty_value DESC LIMIT 1`, bankID).Scan(&minDays)
				if daysHeld < minDays {
					accruedInterest = 0
					tdsDeducted = 0
				}
			}
			penaltyAmount = roundToFour(accruedInterest * penaltyRate / 100.0)
		}

		netPayout := roundToFour(principalAmount + accruedInterest - tdsDeducted - penaltyAmount)
		if req.ClosureType == "ROLLOVER" {
			if req.RolloverAmount > 0 {
				netPayout = roundToFour(req.RolloverAmount)
			} else {
				netPayout = roundToFour(principalAmount)
			}
		}

		effectiveDateStr := req.EffectiveClosureDate
		if effectiveDateStr == "" {
			effectiveDateStr = time.Now().Format("2006-01-02")
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var closureRequestID string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_closure_request (
			  fd_id, booking_id, confirmation_id, entity_id, entity_name,
			  closure_type, closure_status,
			  initiation_date, effective_closure_date, maturity_date,
			  principal_amount, accrued_interest, tds_deducted,
			  penalty_amount, net_payout_amount,
			  settlement_account_id, maturity_instructions,
			  rollover_amount, rollover_tenor_days,
			  closure_reason, closure_notes,
			  submitted_by, submitted_by_email,
			  accounting_posted, is_deleted,
			  created_by, created_at, updated_by, updated_at
			) VALUES (
			  $1,$2,$3,$4,$5,
			  $6,'PENDING_APPROVAL',
			  CURRENT_DATE,$7::date,$8::date,
			  $9,$10,$11,$12,$13,
			  $14,$15,$16,$17,$18,$19,
			  $20,$21,false,false,
			  $21,NOW(),$21,NOW()
			) RETURNING closure_request_id`,
			req.FDID, nullStrOrNil(bookingID), nullStrOrNil(confirmationID),
			nullStrOrNil(entityID), nullStrOrNil(entityName),
			req.ClosureType,
			effectiveDateStr, maturityDate.Format("2006-01-02"),
			principalAmount, roundToFour(accruedInterest), roundToFour(tdsDeducted),
			roundToFour(penaltyAmount), netPayout,
			nullStrOrNil(req.SettlementAccountID), nullStrOrNil(req.MaturityInstructions),
			req.RolloverAmount, req.RolloverTenorDays,
			nullStrOrNil(req.ClosureReason), nullStrOrNil(req.ClosureNotes),
			req.UserID, userEmail,
		).Scan(&closureRequestID)
		if err != nil {
			api.LogError("[FDClosure] InitiateClosure insert error: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to create closure request")
			return
		}

		_ = insertClosureAudit(ctx, tx, closureRequestID, req.UserID, userEmail, "CREATE", "PENDING_APPROVAL", "Closure initiated", nil)
		_, _ = tx.Exec(ctx, `UPDATE investment.fd_master SET closure_request_id=$1, updated_at=NOW() WHERE fd_id=$2`, closureRequestID, req.FDID)

		instID, instErr := approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
			ModuleCode: "FIXED_DEPOSIT", EntityCode: firstNonEmpty(entityID, "DEFAULT"),
			TransactionType: "FD_CLOSURE_" + req.ClosureType,
			RecordID: closureRequestID, RecordTable: "investment.fd_closure_request",
			AuditTable: "investment.fd_audit_closure_request", AuditIDColumn: "closure_request_id",
			ActionType: "CREATE", Amount: principalAmount,
			SubmittedBy: req.UserID, SubmittedByEmail: userEmail,
		})
		if instErr != nil {
			api.LogError("[FDClosure] InitiateClosure approval engine: %v", instErr)
		}
		if instID != "" {
			_, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_request SET approval_instance_id=$1 WHERE closure_request_id=$2`, instID, closureRequestID)
		}

		switch req.ClosureType {
		case "MATURITY":
			_, _ = tx.Exec(ctx, `INSERT INTO investment.fd_closure_maturity_payout (closure_request_id,fd_id,payout_date,principal_returned,gross_interest,tds_deducted,net_payout,settlement_account_id,payment_status,created_by) VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,'PENDING',$9)`,
				closureRequestID, req.FDID, effectiveDateStr, principalAmount, roundToFour(accruedInterest), roundToFour(tdsDeducted), netPayout, nullStrOrNil(req.SettlementAccountID), userEmail)
		case "PREMATURE":
			var daysHeld int
			_ = tx.QueryRow(ctx, `SELECT COALESCE((CURRENT_DATE - start_date)::int,0) FROM investment.fd_master WHERE fd_id=$1`, req.FDID).Scan(&daysHeld)
			var contractedRate float64
			_ = tx.QueryRow(ctx, `SELECT interest_rate FROM investment.fd_master WHERE fd_id=$1`, req.FDID).Scan(&contractedRate)
			_, _ = tx.Exec(ctx, `INSERT INTO investment.fd_closure_premature (closure_request_id,fd_id,premature_closure_date,days_held,contracted_rate,applicable_rate,gross_interest_earned,penalty_rate,penalty_amount,no_interest_flag,tds_on_interest,net_payout,created_by) VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
				closureRequestID, req.FDID, effectiveDateStr, daysHeld, contractedRate, roundToFour(contractedRate-penaltyRate),
				roundToFour(accruedInterest), penaltyRate, roundToFour(penaltyAmount), noInterestFlag, roundToFour(tdsDeducted), netPayout, userEmail)
		case "ROLLOVER":
			_, _ = tx.Exec(ctx, `INSERT INTO investment.fd_closure_rollover (closure_request_id,source_fd_id,rollover_date,rollover_amount,rollover_principal,interest_credited,tds_deducted,new_tenor_days,rollover_type,created_by) VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,'FULL',$9)`,
				closureRequestID, req.FDID, effectiveDateStr, netPayout, principalAmount, roundToFour(accruedInterest), roundToFour(tdsDeducted), req.RolloverTenorDays, userEmail)
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction commit failed")
			return
		}

		go func() {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/closure/initiate", closureRequestID, map[string]interface{}{
				"record_id": closureRequestID, "fd_id": req.FDID, "closure_type": req.ClosureType,
				"event": "FD_CLOSURE_INITIATED", "actor_email": userEmail,
			})
		}()

		api.LogInfo("[FDClosure] InitiateClosure: closureID=%s fd=%s type=%s by=%s", closureRequestID, req.FDID, req.ClosureType, userEmail)
		api.RespondWithPayload(w, true, "Closure request created and submitted for approval", map[string]interface{}{
			"closure_request_id": closureRequestID, "fd_id": req.FDID,
			"closure_type": req.ClosureType, "closure_status": "PENDING_APPROVAL",
			"principal_amount": principalAmount, "accrued_interest": roundToFour(accruedInterest),
			"tds_deducted": roundToFour(tdsDeducted), "penalty_amount": roundToFour(penaltyAmount),
			"net_payout_amount": netPayout, "approval_instance_id": instID,
		})
	}
}

// Handler 3 — UpdateClosure

type updateClosureRequest struct {
	UserID               string  `json:"user_id"`
	ClosureRequestID     string  `json:"closure_request_id"`
	EffectiveClosureDate string  `json:"effective_closure_date"`
	SettlementAccountID  string  `json:"settlement_account_id"`
	MaturityInstructions string  `json:"maturity_instructions"`
	RolloverAmount       float64 `json:"rollover_amount"`
	RolloverTenorDays    int     `json:"rollover_tenor_days"`
	ClosureReason        string  `json:"closure_reason"`
	ClosureNotes         string  `json:"closure_notes"`
	UpdateReason         string  `json:"update_reason"`
}

func UpdateClosure(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req updateClosureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ClosureRequestID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "closure_request_id is required")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		var (
			oldEffDate, oldSettlAcct, oldMatInstr, oldReason, oldNotes string
			oldRolloverAmt                                              float64
			oldRolloverDays                                             int
			oldStatus                                                   string
		)
		err := pool.QueryRow(ctx, `
			SELECT COALESCE(effective_closure_date::text,''), COALESCE(settlement_account_id,''),
			       COALESCE(maturity_instructions,''), COALESCE(closure_reason,''),
			       COALESCE(closure_notes,''), COALESCE(rollover_amount,0),
			       COALESCE(rollover_tenor_days,0), closure_status
			FROM investment.fd_closure_request
			WHERE closure_request_id=$1 AND is_deleted=false`, req.ClosureRequestID,
		).Scan(&oldEffDate, &oldSettlAcct, &oldMatInstr, &oldReason, &oldNotes, &oldRolloverAmt, &oldRolloverDays, &oldStatus)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, "Closure request not found")
			} else {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to load closure request")
			}
			return
		}
		if oldStatus != "PENDING_APPROVAL" {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Cannot update closure in status %s", oldStatus))
			return
		}

		oldRow := map[string]interface{}{
			"effective_closure_date": oldEffDate, "settlement_account_id": oldSettlAcct,
			"maturity_instructions": oldMatInstr, "closure_reason": oldReason,
			"closure_notes": oldNotes, "rollover_amount": oldRolloverAmt,
			"rollover_tenor_days": oldRolloverDays, "closure_status": oldStatus,
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		setClauses := []string{"updated_by=$1", "updated_at=NOW()"}
		updateArgs := []interface{}{userEmail}
		argN := 2

		if req.EffectiveClosureDate != "" {
			setClauses = append(setClauses, fmt.Sprintf("effective_closure_date=$%d::date", argN))
			updateArgs = append(updateArgs, req.EffectiveClosureDate)
			argN++
		}
		if req.SettlementAccountID != "" {
			setClauses = append(setClauses, fmt.Sprintf("settlement_account_id=$%d", argN))
			updateArgs = append(updateArgs, req.SettlementAccountID)
			argN++
		}
		if req.MaturityInstructions != "" {
			setClauses = append(setClauses, fmt.Sprintf("maturity_instructions=$%d", argN))
			updateArgs = append(updateArgs, req.MaturityInstructions)
			argN++
		}
		if req.RolloverAmount > 0 {
			setClauses = append(setClauses, fmt.Sprintf("rollover_amount=$%d", argN))
			updateArgs = append(updateArgs, req.RolloverAmount)
			argN++
		}
		if req.RolloverTenorDays > 0 {
			setClauses = append(setClauses, fmt.Sprintf("rollover_tenor_days=$%d", argN))
			updateArgs = append(updateArgs, req.RolloverTenorDays)
			argN++
		}
		if req.ClosureReason != "" {
			setClauses = append(setClauses, fmt.Sprintf("closure_reason=$%d", argN))
			updateArgs = append(updateArgs, req.ClosureReason)
			argN++
		}
		if req.ClosureNotes != "" {
			setClauses = append(setClauses, fmt.Sprintf("closure_notes=$%d", argN))
			updateArgs = append(updateArgs, req.ClosureNotes)
			argN++
		}

		updateArgs = append(updateArgs, req.ClosureRequestID)
		updateSQL := fmt.Sprintf("UPDATE investment.fd_closure_request SET %s WHERE closure_request_id=$%d",
			strings.Join(setClauses, ", "), argN)

		if _, err := tx.Exec(ctx, updateSQL, updateArgs...); err != nil {
			api.LogError("[FDClosure] UpdateClosure exec error: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to update closure request")
			return
		}

		snapshotJSON, _ := json.Marshal(oldRow)
		_ = insertClosureAudit(ctx, tx, req.ClosureRequestID, req.UserID, userEmail, "UPDATE", oldStatus, req.UpdateReason, snapshotJSON)

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction commit failed")
			return
		}
		api.LogInfo("[FDClosure] UpdateClosure: closureID=%s by=%s", req.ClosureRequestID, userEmail)
		api.RespondWithPayload(w, true, "Closure request updated", map[string]interface{}{
			"closure_request_id": req.ClosureRequestID, "updated_by": userEmail,
		})
	}
}

// Handler 4 — GetAllClosureRequests

type getAllClosureRequest struct {
	UserID        string `json:"user_id"`
	EntityID      string `json:"entity_id"`
	ClosureStatus string `json:"closure_status"`
	ClosureType   string `json:"closure_type"`
	Page          int    `json:"page"`
	PageSize      int    `json:"page_size"`
}

func GetAllClosureRequests(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req getAllClosureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if req.Page <= 0 {
			req.Page = 1
		}
		if req.PageSize <= 0 || req.PageSize > 200 {
			req.PageSize = 50
		}
		offset := (req.Page - 1) * req.PageSize
		ctx := r.Context()

		conditions := []string{"cr.is_deleted = false"}
		args := []interface{}{}
		argIdx := 1

		if req.EntityID != "" {
			args = append(args, req.EntityID)
			conditions = append(conditions, fmt.Sprintf("cr.entity_id = $%d", argIdx))
			argIdx++
		}
		if req.ClosureStatus != "" {
			args = append(args, req.ClosureStatus)
			conditions = append(conditions, fmt.Sprintf("cr.closure_status = $%d", argIdx))
			argIdx++
		}
		if req.ClosureType != "" {
			args = append(args, req.ClosureType)
			conditions = append(conditions, fmt.Sprintf("cr.closure_type = $%d", argIdx))
			argIdx++
		}
		where := strings.Join(conditions, " AND ")

		var totalCount int
		_ = pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM investment.fd_closure_request cr WHERE %s", where), args...).Scan(&totalCount)

		listArgs := append(args, req.PageSize, offset)
		dataSQL := fmt.Sprintf(`
			SELECT cr.closure_request_id, cr.fd_id, cr.booking_id, cr.confirmation_id,
			  cr.entity_id, cr.entity_name, cr.closure_type, cr.closure_status,
			  cr.initiation_date, cr.effective_closure_date, cr.maturity_date,
			  cr.principal_amount, cr.accrued_interest, cr.tds_deducted,
			  cr.penalty_amount, cr.net_payout_amount,
			  COALESCE(cr.settlement_account_id,'') AS settlement_account_id,
			  COALESCE(cr.maturity_instructions,'') AS maturity_instructions,
			  COALESCE(cr.closure_reason,'') AS closure_reason,
			  cr.submitted_by_email, cr.approved_by_email,
			  cr.approved_at, cr.rejected_at,
			  COALESCE(cr.rejection_reason,'') AS rejection_reason,
			  cr.accounting_posted, cr.created_at, cr.updated_at,
			  COALESCE(m.fd_reference_number,'') AS fd_reference_number,
			  COALESCE(m.bank_name,'') AS bank_name,
			  m.interest_rate, m.tenor_days, m.fd_status,
			  COALESCE(ai.status,'') AS approval_status
			FROM investment.fd_closure_request cr
			LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id
			LEFT JOIN uam.approval_instance ai ON ai.instance_id = cr.approval_instance_id
			WHERE %s ORDER BY cr.created_at DESC
			LIMIT $%d OFFSET $%d`, where, argIdx, argIdx+1)

		rows, err := pool.Query(ctx, dataSQL, listArgs...)
		if err != nil {
			api.LogError("[FDClosure] GetAllClosureRequests query error: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch closure requests")
			return
		}
		defer rows.Close()

		var records []map[string]interface{}
		for rows.Next() {
			vals, _ := rows.Values()
			colDescs := rows.FieldDescriptions()
			row := make(map[string]interface{}, len(colDescs))
			for i, col := range colDescs {
				row[string(col.Name)] = vals[i]
			}
			records = append(records, row)
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"records": records, "total": totalCount, "page": req.Page, "page_size": req.PageSize,
		})
	}
}

// Handler 5 — GetClosureDetail

type closureDetailRequest struct {
	UserID           string `json:"user_id"`
	ClosureRequestID string `json:"closure_request_id"`
}

func GetClosureDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req closureDetailRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ClosureRequestID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "closure_request_id is required")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		closureRow := map[string]interface{}{}

		rows, err := pool.Query(ctx, `
			SELECT cr.*, COALESCE(m.fd_reference_number,'') AS fd_reference_number,
			  COALESCE(m.bank_name,'') AS bank_name, COALESCE(m.bank_id,'') AS bank_id_enriched,
			  m.interest_rate, m.interest_type_code, m.tenor_days, m.start_date,
			  m.principal_amount AS original_principal, m.fd_status,
			  COALESCE(b.entity_name,'') AS booking_entity_name, COALESCE(b.entity_id,'') AS booking_entity_id,
			  COALESCE(c.bank_fd_reference,'') AS bank_fd_reference, COALESCE(c.confirmation_status,'') AS confirmation_status,
			  COALESCE(ai.status,'') AS approval_engine_status, COALESCE(ai.submitted_at::text,'') AS approval_submitted_at
			FROM investment.fd_closure_request cr
			LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = cr.booking_id
			LEFT JOIN investment.fd_confirmation c ON c.confirmation_id = cr.confirmation_id
			LEFT JOIN uam.approval_instance ai ON ai.instance_id = cr.approval_instance_id
			WHERE cr.closure_request_id=$1 AND cr.is_deleted=false LIMIT 1`, req.ClosureRequestID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to load closure detail")
			return
		}
		defer rows.Close()
		if rows.Next() {
			vals, _ := rows.Values()
			colDescs := rows.FieldDescriptions()
			for i, col := range colDescs {
				closureRow[string(col.Name)] = vals[i]
			}
		} else {
			api.RespondWithError(w, http.StatusNotFound, "Closure request not found")
			return
		}
		rows.Close()

		fetchSub := func(sql string) []map[string]interface{} {
			var out []map[string]interface{}
			sr, qErr := pool.Query(ctx, sql, req.ClosureRequestID)
			if qErr != nil { return out }
			defer sr.Close()
			for sr.Next() {
				v, _ := sr.Values()
				cd := sr.FieldDescriptions()
				row := make(map[string]interface{}, len(cd))
				for i, c := range cd { row[string(c.Name)] = v[i] }
				out = append(out, row)
			}
			return out
		}

		var auditTrail []map[string]interface{}
		ar, qErr := pool.Query(ctx, `SELECT audit_id,action_type,processing_status,performed_by_email,action_reason,snapshot_data,created_at FROM investment.fd_audit_closure_request WHERE closure_request_id=$1 ORDER BY created_at ASC`, req.ClosureRequestID)
		if qErr == nil {
			defer ar.Close()
			for ar.Next() {
				v, _ := ar.Values()
				cd := ar.FieldDescriptions()
				row := make(map[string]interface{}, len(cd))
				for i, c := range cd { row[string(c.Name)] = v[i] }
				auditTrail = append(auditTrail, row)
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_request":  closureRow,
			"maturity_payouts": fetchSub("SELECT * FROM investment.fd_closure_maturity_payout WHERE closure_request_id=$1"),
			"premature_detail": fetchSub("SELECT * FROM investment.fd_closure_premature WHERE closure_request_id=$1"),
			"rollover_detail":  fetchSub("SELECT * FROM investment.fd_closure_rollover WHERE closure_request_id=$1"),
			"audit_trail":      auditTrail,
		})
		_ = userEmail
	}
}

// Handler 6 — BulkApproveClosureRequest

func BulkApproveClosureRequest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req closureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ClosureRequestIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "closure_request_ids are required")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		engineActed, directActed := 0, 0
		var errors []string

		for _, crID := range req.ClosureRequestIDs {
			var fdID, closureType, closureStatus string
			var principalAmt, accruedInt, tdsAmt, penaltyAmt, netPayout float64
			var entityID, entityName string
			loadErr := pool.QueryRow(ctx, `
				SELECT fd_id,closure_type,closure_status,
				       principal_amount,accrued_interest,tds_deducted,penalty_amount,net_payout_amount,
				       COALESCE(entity_id,''),COALESCE(entity_name,'')
				FROM investment.fd_closure_request
				WHERE closure_request_id=$1 AND is_deleted=false`, crID,
			).Scan(&fdID,&closureType,&closureStatus,&principalAmt,&accruedInt,&tdsAmt,&penaltyAmt,&netPayout,&entityID,&entityName)
			if loadErr != nil { errors = append(errors, crID+": not found"); continue }
			if closureStatus != "PENDING_APPROVAL" { errors = append(errors, crID+": status is "+closureStatus); continue }

			var instanceEyeID string
			engineErr := pool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id AND ie.status='ACTIVE'
				JOIN uam.approval_matrix_eye_member m ON m.eye_id=ie.matrix_eye_id
				  AND m.member_type='APPROVER' AND m.is_active=true AND m.is_deleted=false
				  AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id=$2
				WHERE i.record_id=$1 AND i.module_code='FIXED_DEPOSIT' AND i.status='PENDING'
				ORDER BY ie.position ASC LIMIT 1`, crID, req.UserID,
			).Scan(&instanceEyeID)

			if engineErr == nil && instanceEyeID != "" {
				if err := approvalengine.RecordAction(ctx, pool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID, ActorUserID: req.UserID, ActorEmail: userEmail,
					ActionType: approvalengine.ActionApproved,
					Comment: firstNonEmpty(req.Comment, "Bulk approved FD closure"),
				}); err != nil {
					errors = append(errors, crID+": "+err.Error())
					continue
				}
				var instStatus string
				_ = pool.QueryRow(ctx, `SELECT i.status FROM uam.approval_instance i JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id WHERE ie.instance_eye_id=$1`, instanceEyeID).Scan(&instStatus)
				if instStatus == "APPROVED" {
					if postErr := postClosureJournals(ctx, pool, crID, fdID, closureType, entityID, entityName, principalAmt, accruedInt, tdsAmt, penaltyAmt, netPayout, req.UserID, userEmail); postErr != nil {
						api.LogError("[FDClosure] postClosureJournals failed for %s: %v", crID, postErr)
						errors = append(errors, crID+": journal posting failed: "+postErr.Error())
					}
				}
				engineActed++
			} else {
				var anyInstance int
				_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM uam.approval_instance WHERE record_id=$1 AND module_code='FIXED_DEPOSIT' AND status='PENDING'`, crID).Scan(&anyInstance)
				if anyInstance > 0 { errors = append(errors, crID+": not your turn in approval sequence"); continue }
				if postErr := postClosureJournals(ctx, pool, crID, fdID, closureType, entityID, entityName, principalAmt, accruedInt, tdsAmt, penaltyAmt, netPayout, req.UserID, userEmail); postErr != nil {
					errors = append(errors, crID+": "+postErr.Error())
					continue
				}
				directActed++
			}
		}

		for _, crID := range req.ClosureRequestIDs {
			go func(id, uEmail string) {
				defer func() { recover() }() //nolint:errcheck
				notifcatalog.TriggerNotification(context.Background(), pool,
					"/investment/fd/closure/bulk-approve", id, map[string]interface{}{
						"record_id": id, "event": "FD_CLOSURE_APPROVED", "actor_email": uEmail,
					})
			}(crID, userEmail)
		}

		api.LogInfo("[FDClosure] BulkApproveClosureRequest: engine=%d direct=%d errors=%d by=%s", engineActed, directActed, len(errors), userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed, "errors": errors, "checker": userEmail,
		})
	}
}

// Handler 7 — BulkRejectClosureRequest

func BulkRejectClosureRequest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req closureIDsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ClosureRequestIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "closure_request_ids are required")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		acted := 0
		var errors []string

		for _, crID := range req.ClosureRequestIDs {
			var curStatus string
			if err := pool.QueryRow(ctx, `SELECT closure_status FROM investment.fd_closure_request WHERE closure_request_id=$1 AND is_deleted=false`, crID).Scan(&curStatus); err != nil {
				errors = append(errors, crID+": not found"); continue
			}
			if curStatus != "PENDING_APPROVAL" { errors = append(errors, crID+": status is "+curStatus); continue }

			var instanceEyeID string
			engineErr := pool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id AND ie.status='ACTIVE'
				JOIN uam.approval_matrix_eye_member m ON m.eye_id=ie.matrix_eye_id
				  AND m.member_type='APPROVER' AND m.is_active=true AND m.is_deleted=false
				  AND m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id=$2
				WHERE i.record_id=$1 AND i.module_code='FIXED_DEPOSIT' AND i.status='PENDING'
				ORDER BY ie.position ASC LIMIT 1`, crID, req.UserID,
			).Scan(&instanceEyeID)

			if engineErr == nil && instanceEyeID != "" {
				if err := approvalengine.RecordAction(ctx, pool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID, ActorUserID: req.UserID, ActorEmail: userEmail,
					ActionType: approvalengine.ActionRejected,
					Comment: firstNonEmpty(req.Comment, "Bulk rejected FD closure"),
				}); err != nil {
					errors = append(errors, crID+": "+err.Error()); continue
				}
			}

			tx, txErr := pool.Begin(ctx)
			if txErr != nil { errors = append(errors, crID+": tx begin failed"); continue }

			_, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_request SET closure_status='REJECTED',rejected_at=NOW(),rejected_by=$1,rejection_reason=$2,updated_by=$1,updated_at=NOW() WHERE closure_request_id=$3`, userEmail, req.Comment, crID)
			_, _ = tx.Exec(ctx, `UPDATE investment.fd_master SET closure_request_id=NULL,updated_at=NOW() WHERE closure_request_id=$1`, crID)

			snapshotJSON, _ := json.Marshal(map[string]interface{}{"closure_status": "PENDING_APPROVAL", "rejected_by": userEmail, "rejection_reason": req.Comment})
			_ = insertClosureAudit(ctx, tx, crID, req.UserID, userEmail, "REJECT", "REJECTED", req.Comment, snapshotJSON)

			if cerr := tx.Commit(ctx); cerr != nil {
				_ = tx.Rollback(ctx); errors = append(errors, crID+": commit failed"); continue
			}
			acted++
		}

		for _, crID := range req.ClosureRequestIDs {
			go func(id, uEmail string) {
				defer func() { recover() }() //nolint:errcheck
				notifcatalog.TriggerNotification(context.Background(), pool,
					"/investment/fd/closure/bulk-reject", id, map[string]interface{}{
						"record_id": id, "event": "FD_CLOSURE_REJECTED", "actor_email": uEmail,
					})
			}(crID, userEmail)
		}

		api.LogInfo("[FDClosure] BulkRejectClosureRequest: acted=%d errors=%d by=%s", acted, len(errors), userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"acted": acted, "errors": errors, "checker": userEmail,
		})
	}
}

// Handler 8 — DeleteClosureRequest

type deleteClosureRequest struct {
	UserID           string `json:"user_id"`
	ClosureRequestID string `json:"closure_request_id"`
	Reason           string `json:"reason"`
}

func DeleteClosureRequest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req deleteClosureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ClosureRequestID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "closure_request_id is required")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		var curStatus, fdID string
		err := pool.QueryRow(ctx, `SELECT closure_status, fd_id FROM investment.fd_closure_request WHERE closure_request_id=$1 AND is_deleted=false`, req.ClosureRequestID).Scan(&curStatus, &fdID)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, "Closure request not found")
			} else {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to load closure request")
			}
			return
		}
		if curStatus != "PENDING_APPROVAL" && curStatus != "REJECTED" {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Cannot delete closure request in status %s", curStatus))
			return
		}

		tx, txErr := pool.Begin(ctx)
		if txErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction")
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		_, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_request SET is_deleted=true,deleted_by=$1,deleted_at=NOW(),closure_status='CANCELLED',updated_by=$1,updated_at=NOW() WHERE closure_request_id=$2`, userEmail, req.ClosureRequestID)
		_, _ = tx.Exec(ctx, `UPDATE investment.fd_master SET closure_request_id=NULL,updated_at=NOW() WHERE closure_request_id=$1`, req.ClosureRequestID)

		snapshotJSON, _ := json.Marshal(map[string]interface{}{"closure_status": curStatus, "fd_id": fdID, "delete_reason": req.Reason})
		_ = insertClosureAudit(ctx, tx, req.ClosureRequestID, req.UserID, userEmail, "DELETE", "CANCELLED", req.Reason, snapshotJSON)

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction commit failed")
			return
		}

		api.LogInfo("[FDClosure] DeleteClosureRequest: closureID=%s by=%s", req.ClosureRequestID, userEmail)
		api.RespondWithPayload(w, true, "Closure request deleted", map[string]interface{}{
			"closure_request_id": req.ClosureRequestID, "deleted_by": userEmail,
		})
	}
}

// Internal helpers

func insertClosureAudit(ctx context.Context, exec dbExec, closureRequestID, performedBy, performedByEmail, actionType, processingStatus, reason string, snapshot []byte) error {
	if closureRequestID == "" {
		return nil
	}
	var snapshotArg interface{}
	if len(snapshot) > 0 {
		snapshotArg = snapshot
	}
	_, err := exec.Exec(ctx, `
		INSERT INTO investment.fd_audit_closure_request (
		  closure_request_id, action_type, processing_status,
		  performed_by, performed_by_email, action_reason,
		  snapshot_data, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())`,
		closureRequestID, actionType, processingStatus,
		nullStrOrNil(performedBy), nullStrOrNil(performedByEmail),
		nullStrOrNil(reason), snapshotArg,
	)
	return err
}

func postClosureJournals(ctx context.Context, pool *pgxpool.Pool, closureRequestID, fdID, closureType, entityID, entityName string, principalAmt, accruedInterest, tdsAmt, penaltyAmt, netPayout float64, approvedBy, approvedByEmail string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postClosureJournals begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	now := time.Now()
	accountingPeriod := fmt.Sprintf("%d-%02d", now.Year(), now.Month())

	var settleAcctID, bankName string
	_ = tx.QueryRow(ctx, `SELECT COALESCE(settlement_account_id,''), COALESCE(settlement_bank_name,'') FROM investment.fd_closure_request WHERE closure_request_id=$1`, closureRequestID).Scan(&settleAcctID, &bankName)

	var bankAccountNumber, bankAccountName string
	if settleAcctID != "" {
		_ = tx.QueryRow(ctx, `SELECT COALESCE(account_number,''), COALESCE(account_nickname,'') FROM public.masterbankaccount WHERE account_id=$1 OR account_number=$1 LIMIT 1`, settleAcctID).Scan(&bankAccountNumber, &bankAccountName)
	}
	if bankAccountName == "" { bankAccountName = firstNonEmpty(bankName, "Settlement Account") }
	if bankAccountNumber == "" { bankAccountNumber = firstNonEmpty(settleAcctID, "SETTLEMENT") }

	activitySubtype := "FD_MATURITY_PAYOUT"
	switch closureType {
	case "PREMATURE": activitySubtype = "FD_PREMATURE_CLOSURE"
	case "ROLLOVER":  activitySubtype = "FD_ROLLOVER"
	}

	var activityID string
	err = tx.QueryRow(ctx, `INSERT INTO investment.accounting_activity (activity_type,activity_subtype,effective_date,accounting_period,data_source,status) VALUES ('FIXED_DEPOSIT',$1,CURRENT_DATE,$2,'FD_CLOSURE','APPROVED') RETURNING activity_id`, activitySubtype, accountingPeriod).Scan(&activityID)
	if err != nil { return fmt.Errorf("postClosureJournals create activity: %w", err) }

	var entryID string
	description := fmt.Sprintf("FD %s closure — %s", closureType, fdID)
	totalAmt := roundToFour(principalAmt)

	err = tx.QueryRow(ctx, `
		INSERT INTO investment.accounting_journal_entry (
		  activity_id,entity_id,entity_name,entry_date,accounting_period,entry_type,description,
		  total_debit,total_credit,status,fd_id,closure_request_id,is_reversal,created_by
		) VALUES ($1,$2,$3,CURRENT_DATE,$4,'CLOSURE',$5,$6,$6,'APPROVED',$7,$8,false,$9)
		RETURNING entry_id`,
		activityID, nullStrOrNil(entityID), nullStrOrNil(entityName),
		accountingPeriod, description, totalAmt, fdID, closureRequestID, approvedByEmail,
	).Scan(&entryID)
	if err != nil { return fmt.Errorf("postClosureJournals insert journal entry: %w", err) }

	type jLine struct{ num int; acctNum, acctName, acctType string; debitAmt, creditAmt float64; narration string }
	lines := []jLine{
		{1, "FD-INVEST-" + fdID, "FD Investment — " + fdID, "ASSET", roundToFour(principalAmt), 0, "Return of principal — " + closureType},
		{2, bankAccountNumber, bankAccountName, "ASSET", 0, roundToFour(netPayout), "Net payout to settlement account"},
	}
	if accruedInterest > 0 {
		lines = append(lines, jLine{3, "FD-INT-INC-" + fdID, "Interest Income — FD", "INCOME", 0, roundToFour(accruedInterest), "Interest income on closure"})
	}
	if tdsAmt > 0 {
		lines = append(lines, jLine{4, "TDS-PAYABLE", "TDS Payable", "LIABILITY", roundToFour(tdsAmt), 0, "TDS on interest income"})
	}
	if penaltyAmt > 0 {
		lines = append(lines, jLine{5, "PENALTY-CHG", "Premature Withdrawal Penalty", "EXPENSE", roundToFour(penaltyAmt), 0, "Penalty for premature withdrawal"})
	}

	for _, l := range lines {
		_, err = tx.Exec(ctx, `INSERT INTO investment.accounting_journal_entry_line (entry_id,line_number,account_number,account_name,account_type,debit_amount,credit_amount,narration,fd_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			entryID, l.num, l.acctNum, l.acctName, l.acctType, l.debitAmt, l.creditAmt, l.narration, fdID)
		if err != nil { return fmt.Errorf("postClosureJournals insert journal line %d: %w", l.num, err) }
	}

	newFDStatus := "MATURED"
	switch closureType {
	case "PREMATURE": newFDStatus = "PREMATURELY_CLOSED"
	case "ROLLOVER":  newFDStatus = "ROLLED_OVER"
	}

	_, err = tx.Exec(ctx, `UPDATE investment.fd_master SET fd_status=$1,closed_at=NOW(),closed_by=$2,accounting_posted=true,closure_request_id=$3,updated_by=$2,updated_at=NOW() WHERE fd_id=$4`, newFDStatus, approvedByEmail, closureRequestID, fdID)
	if err != nil { return fmt.Errorf("postClosureJournals update fd_master: %w", err) }

	_, err = tx.Exec(ctx, `UPDATE investment.fd_closure_request SET closure_status='POSTED',approved_by=$1,approved_by_email=$2,approved_at=NOW(),accounting_posted=true,accounting_posted_at=NOW(),updated_by=$2,updated_at=NOW() WHERE closure_request_id=$3`, approvedBy, approvedByEmail, closureRequestID)
	if err != nil { return fmt.Errorf("postClosureJournals update closure request: %w", err) }

	switch closureType {
	case "MATURITY":  _, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_maturity_payout SET journal_entry_id=$1,payment_status='COMPLETED' WHERE closure_request_id=$2`, entryID, closureRequestID)
	case "PREMATURE": _, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_premature SET journal_entry_id=$1 WHERE closure_request_id=$2`, entryID, closureRequestID)
	case "ROLLOVER":  _, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_rollover SET journal_entry_id=$1 WHERE closure_request_id=$2`, entryID, closureRequestID)
	}

	snapshotJSON, _ := json.Marshal(map[string]interface{}{
		"journal_entry_id": entryID, "activity_id": activityID, "new_fd_status": newFDStatus,
		"accounting_period": accountingPeriod, "total_debit": totalAmt, "approved_by_email": approvedByEmail,
	})
	_ = insertClosureAudit(ctx, tx, closureRequestID, approvedBy, approvedByEmail, "POST_ACCOUNTING", "POSTED", "Journals posted on approval", snapshotJSON)

	if err := tx.Commit(ctx); err != nil { return fmt.Errorf("postClosureJournals commit: %w", err) }

	api.LogInfo("[FDClosure] postClosureJournals: closureID=%s fdID=%s type=%s entryID=%s by=%s", closureRequestID, fdID, closureType, entryID, approvedByEmail)
	return nil
}
