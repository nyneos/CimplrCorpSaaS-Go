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
	"CimplrCorpSaas/api/varianceengine"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
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
			  COALESCE(m.bank_fd_ref_no,'') AS fd_reference_number,
			  m.fd_status, m.principal_amount, m.interest_rate, m.interest_type_code,
			  m.tenure_days, m.start_date, m.maturity_date,
			  COALESCE(m.maturity_instructions,'PENDING') AS maturity_instructions,
			  COALESCE(m.auto_renewal, false) AS auto_renewal,
			  COALESCE(m.day_count_code,'') AS day_count_convention,
			  (m.maturity_date - CURRENT_DATE) AS days_to_maturity,
			  COALESCE(b.entity_id,'') AS entity_id,
			  COALESCE(b.entity_name,'') AS entity_name,
			  COALESCE(b.frequency_id,'') AS frequency_id,
			  COALESCE(b.source_account_id,'') AS source_account_id,
			  COALESCE(b.tds_plan_id,'') AS tds_plan_id,
			  COALESCE(c.confirmation_status,'') AS confirmation_status,
			  COALESCE(c.confirmation_received_date::text,'') AS confirmation_date,
			  COALESCE(c.bank_fd_ref_no,'') AS bank_fd_reference,
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
			  SELECT SUM(COALESCE(period_interest_accrued,0)) AS total_interest_accrued,
			         SUM(COALESCE(tds_deducted_in_period,0)) AS total_tds_accrued
			  FROM investment.fd_accrual_ledger
			  WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false
			) al ON true
			LEFT JOIN LATERAL (
			  SELECT penalty_value, penalty_type, no_interest_if_withdrawn_before
			  FROM investment.fd_penalty_structure_master ps
			  WHERE ps.bank_code = m.bank_id AND COALESCE(ps.is_deleted,false)=false
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

		// If no records found, and no entity/bank filter was provided, return all ACTIVE FDs (ignore date window).
		if len(records) == 0 && req.EntityID == "" && req.BankID == "" {
			// Query without the date condition to return all active FDs
			allWhere := "m.is_deleted = false AND m.fd_status IN ('ACTIVE','MATURED')"
			var totalAll int
			_ = pool.QueryRow(ctx,
				"SELECT COUNT(DISTINCT m.fd_id) FROM investment.fd_master m LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id WHERE "+allWhere).Scan(&totalAll)

			allSQL := `
				SELECT
				  m.fd_id, m.booking_id, m.confirmation_id,
				  COALESCE(m.bank_id,'') AS bank_id,
				  COALESCE(m.bank_name,'') AS bank_name,
				  COALESCE(m.bank_fd_ref_no,'') AS fd_reference_number,
				  m.fd_status, m.principal_amount, m.interest_rate, m.interest_type_code,
				  m.tenure_days, m.start_date, m.maturity_date,
				  COALESCE(m.maturity_instructions,'PENDING') AS maturity_instructions,
				  COALESCE(m.auto_renewal, false) AS auto_renewal,
				  COALESCE(m.day_count_code,'') AS day_count_convention,
				  (m.maturity_date - CURRENT_DATE) AS days_to_maturity,
				  COALESCE(b.entity_id,'') AS entity_id,
				  COALESCE(b.entity_name,'') AS entity_name,
				  COALESCE(b.frequency_id,'') AS frequency_id,
				  COALESCE(b.source_account_id,'') AS source_account_id,
				  COALESCE(b.tds_plan_id,'') AS tds_plan_id,
				  COALESCE(c.confirmation_status,'') AS confirmation_status,
				  COALESCE(c.confirmation_received_date::text,'') AS confirmation_date,
				  COALESCE(c.bank_fd_ref_no,'') AS bank_fd_reference,
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
				  SELECT SUM(COALESCE(period_interest_accrued,0)) AS total_interest_accrued,
						 SUM(COALESCE(tds_deducted_in_period,0)) AS total_tds_accrued
				  FROM investment.fd_accrual_ledger
				  WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false
				) al ON true
				LEFT JOIN LATERAL (
				  SELECT penalty_value, penalty_type, no_interest_if_withdrawn_before
				  FROM investment.fd_penalty_structure_master ps
				  WHERE ps.bank_code = m.bank_id AND COALESCE(ps.is_deleted,false)=false
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
				WHERE ` + allWhere + `
				ORDER BY m.maturity_date ASC
				LIMIT $1 OFFSET $2`

			rowsAll, err := pool.Query(ctx, allSQL, req.PageSize, offset)
			if err != nil {
				api.LogError("[FDClosure] GetFDsNearMaturity (all) query error: %v", err)
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch maturity dashboard")
				return
			}
			defer rowsAll.Close()

			records = []map[string]interface{}{}
			for rowsAll.Next() {
				vals, _ := rowsAll.Values()
				colDescs := rowsAll.FieldDescriptions()
				row := make(map[string]interface{}, len(colDescs))
				for i, col := range colDescs {
					row[string(col.Name)] = vals[i]
				}
				records = append(records, row)
			}

			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"records": records, "total": totalAll, "page": req.Page,
				"page_size": req.PageSize, "days_ahead": req.DaysAhead,
			})
			api.LogInfo("[FDClosure] GetFDsNearMaturity (all active) returned %d/%d by %s", len(records), totalAll, userEmail)
			return
		}

		if len(records) == 0 {
			// No results for the provided filters / window — return clear human-readable message
			msg := fmt.Sprintf("No FDs found in the maturity window of %d day(s) for entity=%s bank=%s", req.DaysAhead, firstNonEmpty(req.EntityID, "ALL"), firstNonEmpty(req.BankID, "ALL"))
			api.RespondWithPayload(w, false, msg, map[string]interface{}{
				"records": records, "total": totalCount, "page": req.Page,
				"page_size": req.PageSize, "days_ahead": req.DaysAhead,
			})
			api.LogInfo("[FDClosure] GetFDsNearMaturity: no records for %s — by %s", msg, userEmail)
			return
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
	// Core fields
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

	// Overrides from UI pre-population (used by variance engine when exception=true)
	// If user changed a value vs what the system computed, these carry the user value.
	OverridePrincipalAmount  float64 `json:"override_principal_amount"`
	OverrideAccruedInterest  float64 `json:"override_accrued_interest"`
	OverrideTDSDeducted      float64 `json:"override_tds_deducted"`
	OverridePenaltyAmount    float64 `json:"override_penalty_amount"`
	OverrideNetPayout        float64 `json:"override_net_payout"`
	OverrideMaturityDate     string  `json:"override_maturity_date"`
	OverrideTenureDays       int     `json:"override_tenure_days"`
	OverrideInterestRate     float64 `json:"override_interest_rate"`
	OverrideBankName         string  `json:"override_bank_name"`
	OverrideBankID           string  `json:"override_bank_id"`

	// Variance / exception controls
	// HasVariance: UI tells us variance was detected on last validate call
	HasVariance          bool   `json:"has_variance"`
	// ProceedWithException: true = user chose to treat all open variances as EXCEPTION and proceed
	ProceedWithException bool   `json:"proceed_with_exception"`
	// LastValidatedRunID: run_id from the last /validate call (must be present if HasVariance=true)
	LastValidatedRunID   string `json:"last_validated_run_id"`
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

		// ── Variance guard ─────────────────────────────────────────────────
		// If the last /validate run detected open variances, block initiation
		// unless the user explicitly passes proceed_with_exception=true.
		if req.HasVariance && req.LastValidatedRunID != "" {
			// Count OPEN variances for this FD from the last validate run
			var openCount int
			_ = pool.QueryRow(ctx,
				`SELECT COUNT(*) FROM public.variance_log
				 WHERE record_id=$1 AND run_id=$2 AND status='OPEN'`,
				req.FDID, req.LastValidatedRunID).Scan(&openCount)
			// Also check any OPEN row regardless of run — covers re-use of old run IDs
			if openCount == 0 {
				_ = pool.QueryRow(ctx,
					`SELECT COUNT(*) FROM public.variance_log
					 WHERE record_id=$1 AND status='OPEN'`,
					req.FDID).Scan(&openCount)
			}
			if openCount > 0 {
				if !req.ProceedWithException {
					// Block — tell the UI exactly how many are still open
					api.RespondWithError(w, http.StatusConflict,
						fmt.Sprintf("Cannot initiate: %d open variance(s) detected. Correct values via /validate or set proceed_with_exception=true to proceed with exception.", openCount))
					return
				}
				// proceed_with_exception=true → mark all OPEN as EXCEPTION and continue
				_, _ = pool.Exec(ctx, `
					UPDATE public.variance_log SET
					  status='EXCEPTION', is_exception=true,
					  resolution_reason='Proceeded with exception on initiation',
					  resolved_by=$1, resolved_by_email=$2,
					  resolved_at=NOW(), updated_at=NOW()
					WHERE record_id=$3 AND status='OPEN'`,
					req.UserID, userEmail, req.FDID)
				api.LogInfo("[FDClosure] InitiateClosure: proceed_with_exception — %d variances marked EXCEPTION by=%s fd=%s",
					openCount, userEmail, req.FDID)
			}
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
			SELECT COALESCE(SUM(period_interest_accrued),0), COALESCE(SUM(tds_deducted_in_period),0)
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
				WHERE ps.bank_code=$1 AND COALESCE(ps.is_deleted,false)=false
				ORDER BY ps.penalty_value DESC LIMIT 1`, bankID,
			).Scan(&penaltyRate, &noInterestFlag)

			if noInterestFlag {
				var daysHeld, minDays int
				_ = pool.QueryRow(ctx, `SELECT COALESCE((CURRENT_DATE - start_date)::int,0) FROM investment.fd_master WHERE fd_id=$1`, req.FDID).Scan(&daysHeld)
				_ = pool.QueryRow(ctx, `SELECT COALESCE(no_interest_if_withdrawn_before,0) FROM investment.fd_penalty_structure_master ps WHERE ps.bank_code=$1 ORDER BY penalty_value DESC LIMIT 1`, bankID).Scan(&minDays)
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
			_ = tx.QueryRow(ctx, `SELECT COALESCE(interest_rate,0) FROM investment.fd_master WHERE fd_id=$1`, req.FDID).Scan(&contractedRate)
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
			  COALESCE(m.bank_fd_ref_no,'') AS fd_reference_number,
			  COALESCE(m.bank_name,'') AS bank_name,
			  m.interest_rate, m.tenure_days, m.fd_status,
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
			SELECT cr.*, COALESCE(m.bank_fd_ref_no,'') AS fd_reference_number,
			  COALESCE(m.bank_name,'') AS bank_name, COALESCE(m.bank_id,'') AS bank_id_enriched,
			  m.interest_rate, m.interest_type_code, m.tenure_days, m.start_date,
			  m.principal_amount AS original_principal, m.fd_status,
			  COALESCE(b.entity_name,'') AS booking_entity_name, COALESCE(b.entity_id,'') AS booking_entity_id,
			  COALESCE(c.bank_fd_ref_no,'') AS bank_fd_reference, COALESCE(c.confirmation_status,'') AS confirmation_status,
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
			"variances":        fetchVariances(ctx, pool, req.ClosureRequestID),
		})
		_ = userEmail
	}
}

// fetchVariances loads variance_log rows for a closure and returns open_count + rows.
func fetchVariances(ctx context.Context, pool *pgxpool.Pool, closureRequestID string) map[string]interface{} {
	rows, err := varianceengine.GetVariances(ctx, pool, closureRequestID)
	if err != nil || rows == nil {
		rows = []map[string]interface{}{}
	}
	openCount := 0
	for _, v := range rows {
		if s, ok := v["status"].(string); ok && s == "OPEN" {
			openCount++
		}
	}
	return map[string]interface{}{
		"items":      rows,
		"total":      len(rows),
		"open_count": openCount,
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
			var hasUnresolved bool
			loadErr := pool.QueryRow(ctx, `
				SELECT fd_id,closure_type,closure_status,
				       principal_amount,accrued_interest,tds_deducted,penalty_amount,net_payout_amount,
				       COALESCE(entity_id,''),COALESCE(entity_name,''),
				       COALESCE(has_unresolved_variance,false)
				FROM investment.fd_closure_request
				WHERE closure_request_id=$1 AND is_deleted=false`, crID,
			).Scan(&fdID,&closureType,&closureStatus,&principalAmt,&accruedInt,&tdsAmt,&penaltyAmt,&netPayout,&entityID,&entityName,&hasUnresolved)
			if loadErr != nil { errors = append(errors, crID+": not found"); continue }
			if closureStatus != "PENDING_APPROVAL" { errors = append(errors, crID+": status is "+closureStatus); continue }
			if hasUnresolved {
				errors = append(errors, crID+": has unresolved variances — resolve or raise as exception before approving")
				continue
			}

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
				engineActed++ // action recorded; count regardless of whether this is the final eye
				var instStatus string
				_ = pool.QueryRow(ctx, `SELECT i.status FROM uam.approval_instance i JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id WHERE ie.instance_eye_id=$1`, instanceEyeID).Scan(&instStatus)
				if instStatus == "APPROVED" {
					if postErr := postClosureJournals(ctx, pool, crID, fdID, closureType, entityID, entityName, principalAmt, accruedInt, tdsAmt, penaltyAmt, netPayout, req.UserID, userEmail); postErr != nil {
						api.LogError("[FDClosure] postClosureJournals failed for %s: %v", crID, postErr)
						errors = append(errors, crID+": journal posting failed: "+postErr.Error())
					}
				}
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

		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errors) == 0
		msg := ""
		if !success {
			msg = "No closures were approved"
		}
		api.LogInfo("[FDClosure] BulkApproveClosureRequest: engine=%d direct=%d errors=%d by=%s", engineActed, directActed, len(errors), userEmail)
		api.RespondWithPayload(w, success, msg, map[string]interface{}{
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

		totalActed := acted
		success := totalActed > 0 || len(errors) == 0
		msg := ""
		if !success {
			msg = "No closures were rejected"
		}
		api.LogInfo("[FDClosure] BulkRejectClosureRequest: acted=%d errors=%d by=%s", acted, len(errors), userEmail)
		api.RespondWithPayload(w, success, msg, map[string]interface{}{
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
		_ = tx.QueryRow(ctx, `SELECT COALESCE(account_number,''), COALESCE(account_nickname,'') FROM public.masterbankaccount WHERE account_id=$1 LIMIT 1`, settleAcctID).Scan(&bankAccountNumber, &bankAccountName)
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

	_, err = tx.Exec(ctx, `UPDATE investment.fd_master SET fd_status=$1,closed_at=NOW(),closed_by=$2,accounting_posted=true,closure_request_id=$3,updated_by=$4,updated_at=NOW() WHERE fd_id=$5`, newFDStatus, approvedByEmail, closureRequestID, approvedByEmail, fdID)
	if err != nil { return fmt.Errorf("postClosureJournals update fd_master: %w", err) }

	_, err = tx.Exec(ctx, `UPDATE investment.fd_closure_request SET closure_status='POSTED',approved_by=$1,approved_by_email=$2,approved_at=NOW(),accounting_posted=true,accounting_posted_at=NOW(),updated_by=$3,updated_at=NOW() WHERE closure_request_id=$4`, approvedBy, approvedByEmail, approvedByEmail, closureRequestID)
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

// ─── Handler: ValidateClosure ─────────────────────────────────────────────────
//
// POST /investment/fd/closure/validate
//
// Single validation entrypoint for ALL closure types (MATURITY, PREMATURE, ROLLOVER).
// Hit repeatedly — each call re-runs the variance engine.
//
// Behaviour:
//   1. Loads DB source-of-truth for the FD.
//   2. Computes system-expected values (interest, penalty, payout …).
//   3. Compares EVERY user-submitted field against system values.
//   4. If closure_request_id is given (re-validate): persists / updates variance rows.
//      Any field whose delta is now 0 gets auto-marked RESOLVED (AutoResolveCleared).
//   5. Returns system_values for UI pre-population + full variances[] array.
//
// Only two endpoints:
//   POST /validate  — call until all variances resolved (no separate resolve API)
//   POST /initiate  — blocked if OPEN variances exist (unless proceed_with_exception=true)

// validateClosureRequest covers ALL fields for MATURITY, PREMATURE and ROLLOVER.
// The UI pre-populates from maturity-dashboard; user edits; we compare every field.
type validateClosureRequest struct {
	UserID           string `json:"user_id"`
	ClosureRequestID string `json:"closure_request_id"` // empty on first call; set on re-validate

	// ── FD identity ────────────────────────────────────────────────────────
	FDID             string `json:"fd_id"`
	ClosureType      string `json:"closure_type"` // MATURITY | PREMATURE | ROLLOVER
	BankID           string `json:"bank_id"`
	BankName         string `json:"bank_name"`
	BankFDRefNo      string `json:"bank_fd_ref_no"`
	EntityID         string `json:"entity_id"`
	EntityName       string `json:"entity_name"`

	// ── Core FD economics ──────────────────────────────────────────────────
	PrincipalAmount  float64 `json:"principal_amount"`
	InterestRate     float64 `json:"interest_rate"`
	TenureDays       int     `json:"tenure_days"`
	StartDate        string  `json:"start_date"`
	MaturityDate     string  `json:"maturity_date"`
	InterestTypeCode string  `json:"interest_type_code"` // SIMPLE | COMPOUND

	// ── Accrual & TDS ──────────────────────────────────────────────────────
	AccruedInterest float64 `json:"accrued_interest"`
	TDSDeducted     float64 `json:"tds_deducted"`
	TDSRate         float64 `json:"tds_rate"`

	// ── Penalty (PREMATURE) ────────────────────────────────────────────────
	PenaltyRate    float64 `json:"penalty_rate"`
	PenaltyAmount  float64 `json:"penalty_amount"`
	NoInterestFlag bool    `json:"no_interest_flag"`
	DaysHeld       int     `json:"days_held"`

	// ── Net payout / settlement ────────────────────────────────────────────
	NetPayoutAmount      float64 `json:"net_payout_amount"`
	SettlementAccountID  string  `json:"settlement_account_id"`
	SettlementBankName   string  `json:"settlement_bank_name"`
	SettlementAccountNo  string  `json:"settlement_account_number"`
	PaymentMode          string  `json:"payment_mode"`
	EffectiveClosureDate string  `json:"effective_closure_date"`
	MaturityInstructions string  `json:"maturity_instructions"` // LIQUIDATE | ROLLOVER | TRANSFER

	// ── Maturity payout specific ───────────────────────────────────────────
	PrincipalReturned float64 `json:"principal_returned"`
	GrossInterest     float64 `json:"gross_interest"`
	PayoutDate        string  `json:"payout_date"`

	// ── Premature specific ─────────────────────────────────────────────────
	ContractedRate       float64 `json:"contracted_rate"`
	ApplicableRate       float64 `json:"applicable_rate"` // contracted_rate - penalty_rate
	GrossInterestEarned  float64 `json:"gross_interest_earned"`
	TDSOnInterest        float64 `json:"tds_on_interest"`
	PenaltyStructureID   string  `json:"penalty_structure_id"`
	PrematureClosureDate string  `json:"premature_closure_date"`

	// ── Rollover specific ──────────────────────────────────────────────────
	RolloverType         string  `json:"rollover_type"`        // FULL | PARTIAL | PRINCIPAL_ONLY
	RolloverAmount       float64 `json:"rollover_amount"`
	RolloverPrincipal    float64 `json:"rollover_principal"`
	InterestCredited     float64 `json:"interest_credited"`
	RolloverTenorDays    int     `json:"rollover_tenor_days"`
	RolloverInterestRate float64 `json:"rollover_interest_rate"`
	NewMaturityDate      string  `json:"new_maturity_date"`
	PartialWithdrawal    float64 `json:"partial_withdrawal"`
	NewFDID              string  `json:"new_fd_id"`

	// ── Closure metadata ──────────────────────────────────────────────────
	ClosureReason string `json:"closure_reason"`
	ClosureNotes  string `json:"closure_notes"`
}

func ValidateClosure(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req validateClosureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id is required")
			return
		}
		validTypes := map[string]bool{"MATURITY": true, "PREMATURE": true, "ROLLOVER": true}
		if req.ClosureType != "" && !validTypes[req.ClosureType] {
			api.RespondWithError(w, http.StatusBadRequest, "closure_type must be MATURITY, PREMATURE or ROLLOVER")
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		// ── 1. Load DB source-of-truth ─────────────────────────────────────
		var (
			dbBankID, dbBankName, dbBankFDRef string
			dbEntityID, dbEntityName          string
			dbPrincipal, dbInterestRate        float64
			dbMaturityDate, dbStartDate        time.Time
			dbTenureDays                       int
			dbFDStatus, dbInterestTypeCode     string
		)
		err := pool.QueryRow(ctx, `
			SELECT COALESCE(m.bank_id,''), COALESCE(m.bank_name,''), COALESCE(m.bank_fd_ref_no,''),
			       m.principal_amount, m.interest_rate,
			       m.maturity_date, m.start_date, m.tenure_days,
			       m.fd_status, COALESCE(m.interest_type_code,'SIMPLE'),
			       COALESCE(b.entity_id,''), COALESCE(b.entity_name,'')
			FROM investment.fd_master m
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
			WHERE m.fd_id=$1 AND m.is_deleted=false`, req.FDID,
		).Scan(&dbBankID, &dbBankName, &dbBankFDRef, &dbPrincipal, &dbInterestRate,
			&dbMaturityDate, &dbStartDate, &dbTenureDays, &dbFDStatus, &dbInterestTypeCode,
			&dbEntityID, &dbEntityName)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "FD not found")
			return
		}

		fmtDate := func(t time.Time) string { return t.Format("2006-01-02") }
		fmtF := func(v float64) string { return strconv.FormatFloat(v, 'f', 4, 64) }
		fmtI := func(v int) string { return strconv.Itoa(v) }

		// ── 2. System accrual ──────────────────────────────────────────────
		var dbAccruedInterest, dbTDSDeducted float64
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(period_interest_accrued),0), COALESCE(SUM(tds_deducted_in_period),0)
			FROM investment.fd_accrual_ledger
			WHERE fd_id=$1 AND COALESCE(is_deleted,false)=false`, req.FDID,
		).Scan(&dbAccruedInterest, &dbTDSDeducted)

		// ── 3. Penalty (PREMATURE) ─────────────────────────────────────────
		var dbPenaltyRate, dbPenaltyAmount float64
		var dbNoInterestFlag bool
		var dbDaysHeld, dbMinDays int
		var dbPenaltyStructureID string
		if req.ClosureType == "PREMATURE" {
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(ps.penalty_structure_id,''),
				       COALESCE(ps.penalty_value,0),
				       COALESCE(ps.no_interest_if_withdrawn_before,0) > 0,
				       COALESCE(ps.no_interest_if_withdrawn_before,0)
				FROM investment.fd_penalty_structure_master ps
				WHERE ps.bank_code=$1 AND COALESCE(ps.is_deleted,false)=false
				ORDER BY ps.penalty_value DESC LIMIT 1`, dbBankID,
			).Scan(&dbPenaltyStructureID, &dbPenaltyRate, &dbNoInterestFlag, &dbMinDays)
			_ = pool.QueryRow(ctx,
				`SELECT COALESCE((CURRENT_DATE - start_date)::int,0) FROM investment.fd_master WHERE fd_id=$1`,
				req.FDID).Scan(&dbDaysHeld)
			intForPenalty := dbAccruedInterest
			if dbNoInterestFlag && dbDaysHeld < dbMinDays {
				intForPenalty = 0
			}
			dbPenaltyAmount = roundToFour(intForPenalty * dbPenaltyRate / 100.0)
		}
		dbApplicableRate := roundToFour(dbInterestRate - dbPenaltyRate)

		// ── 4. System net payout per type ──────────────────────────────────
		var dbNetPayout float64
		switch req.ClosureType {
		case "MATURITY":
			dbNetPayout = roundToFour(dbPrincipal + dbAccruedInterest - dbTDSDeducted)
		case "PREMATURE":
			dbNetPayout = roundToFour(dbPrincipal + dbAccruedInterest - dbTDSDeducted - dbPenaltyAmount)
		case "ROLLOVER":
			if req.RolloverType == "PARTIAL" && req.PartialWithdrawal > 0 {
				dbNetPayout = roundToFour(dbPrincipal - req.PartialWithdrawal)
			} else {
				dbNetPayout = roundToFour(dbPrincipal)
			}
		default:
			dbNetPayout = roundToFour(dbPrincipal + dbAccruedInterest - dbTDSDeducted)
		}

		// ── 5. Build variance rules ────────────────────────────────────────
		runID := varianceengine.NewRunID()
		entityID := firstNonEmpty(req.EntityID, dbEntityID)

		// Common rules — apply to ALL closure types
		rules := []varianceengine.Rule{
			{FieldName: "bank_id",            VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbBankID,                       ActualValue: req.BankID,              Priority: varianceengine.PriorityHigh,   SystemComment: "Bank does not match FD master"},
			{FieldName: "bank_name",          VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbBankName,                     ActualValue: req.BankName,            Priority: varianceengine.PriorityLow},
			{FieldName: "bank_fd_ref_no",     VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbBankFDRef,                    ActualValue: req.BankFDRefNo,         Priority: varianceengine.PriorityMedium},
			{FieldName: "entity_id",          VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbEntityID,                     ActualValue: req.EntityID,            Priority: varianceengine.PriorityHigh,   SystemComment: "Entity mismatch"},
			{FieldName: "entity_name",        VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbEntityName,                   ActualValue: req.EntityName,          Priority: varianceengine.PriorityLow},
			{FieldName: "principal_amount",   VarianceType: varianceengine.TypeAmount,   ExpectedValue: fmtF(dbPrincipal),              ActualValue: fmtF(req.PrincipalAmount), Priority: varianceengine.PriorityHigh,   Tolerance: 0.01},
			{FieldName: "interest_rate",      VarianceType: varianceengine.TypeRate,     ExpectedValue: fmtF(dbInterestRate),           ActualValue: fmtF(req.InterestRate),  Priority: varianceengine.PriorityHigh,   Tolerance: 0.001},
			{FieldName: "interest_type_code", VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbInterestTypeCode,             ActualValue: req.InterestTypeCode,    Priority: varianceengine.PriorityMedium},
			{FieldName: "tenure_days",        VarianceType: varianceengine.TypeDays,     ExpectedValue: fmtI(dbTenureDays),             ActualValue: fmtI(req.TenureDays),    Priority: varianceengine.PriorityHigh,   Tolerance: 0},
			{FieldName: "start_date",         VarianceType: varianceengine.TypeDate,     ExpectedValue: fmtDate(dbStartDate),           ActualValue: req.StartDate,           Priority: varianceengine.PriorityHigh,   Tolerance: 0},
			{FieldName: "maturity_date",      VarianceType: varianceengine.TypeDate,     ExpectedValue: fmtDate(dbMaturityDate),        ActualValue: req.MaturityDate,        Priority: varianceengine.PriorityHigh,   Tolerance: 0},
			{FieldName: "accrued_interest",   VarianceType: varianceengine.TypeAmount,   ExpectedValue: fmtF(dbAccruedInterest),        ActualValue: fmtF(req.AccruedInterest), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
			{FieldName: "tds_deducted",       VarianceType: varianceengine.TypeAmount,   ExpectedValue: fmtF(dbTDSDeducted),            ActualValue: fmtF(req.TDSDeducted),   Priority: varianceengine.PriorityMedium, Tolerance: 0.5},
			{FieldName: "net_payout_amount",  VarianceType: varianceengine.TypeAmount,   ExpectedValue: fmtF(dbNetPayout),              ActualValue: fmtF(req.NetPayoutAmount), Priority: varianceengine.PriorityHigh,   Tolerance: 1.0},
		}

		// MATURITY-specific rules
		if req.ClosureType == "MATURITY" || req.ClosureType == "" {
			rules = append(rules,
				varianceengine.Rule{FieldName: "principal_returned", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbPrincipal),       ActualValue: fmtF(req.PrincipalReturned), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
				varianceengine.Rule{FieldName: "gross_interest",      VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbAccruedInterest), ActualValue: fmtF(req.GrossInterest),     Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
			)
		}

		// PREMATURE-specific rules
		if req.ClosureType == "PREMATURE" {
			rules = append(rules,
				varianceengine.Rule{FieldName: "days_held",             VarianceType: varianceengine.TypeDays,     ExpectedValue: fmtI(dbDaysHeld),              ActualValue: fmtI(req.DaysHeld),             Priority: varianceengine.PriorityHigh, Tolerance: 0},
				varianceengine.Rule{FieldName: "contracted_rate",       VarianceType: varianceengine.TypeRate,     ExpectedValue: fmtF(dbInterestRate),          ActualValue: fmtF(req.ContractedRate),       Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
				varianceengine.Rule{FieldName: "applicable_rate",       VarianceType: varianceengine.TypeRate,     ExpectedValue: fmtF(dbApplicableRate),        ActualValue: fmtF(req.ApplicableRate),       Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
				varianceengine.Rule{FieldName: "gross_interest_earned", VarianceType: varianceengine.TypeAmount,   ExpectedValue: fmtF(dbAccruedInterest),       ActualValue: fmtF(req.GrossInterestEarned),  Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
				varianceengine.Rule{FieldName: "penalty_rate",          VarianceType: varianceengine.TypeRate,     ExpectedValue: fmtF(dbPenaltyRate),           ActualValue: fmtF(req.PenaltyRate),          Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
				varianceengine.Rule{FieldName: "penalty_amount",        VarianceType: varianceengine.TypeAmount,   ExpectedValue: fmtF(dbPenaltyAmount),         ActualValue: fmtF(req.PenaltyAmount),        Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
				varianceengine.Rule{FieldName: "no_interest_flag",      VarianceType: varianceengine.TypeIdentity, ExpectedValue: strconv.FormatBool(dbNoInterestFlag), ActualValue: strconv.FormatBool(req.NoInterestFlag), Priority: varianceengine.PriorityMedium},
				varianceengine.Rule{FieldName: "tds_on_interest",       VarianceType: varianceengine.TypeAmount,   ExpectedValue: fmtF(dbTDSDeducted),           ActualValue: fmtF(req.TDSOnInterest),        Priority: varianceengine.PriorityMedium, Tolerance: 0.5},
			)
		}

		// ROLLOVER-specific rules
		if req.ClosureType == "ROLLOVER" {
			rules = append(rules,
				varianceengine.Rule{FieldName: "rollover_principal",     VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbPrincipal),       ActualValue: fmtF(req.RolloverPrincipal),    Priority: varianceengine.PriorityHigh,   Tolerance: 0.01},
				varianceengine.Rule{FieldName: "rollover_amount",        VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbNetPayout),       ActualValue: fmtF(req.RolloverAmount),       Priority: varianceengine.PriorityHigh,   Tolerance: 1.0},
				varianceengine.Rule{FieldName: "interest_credited",      VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbAccruedInterest), ActualValue: fmtF(req.InterestCredited),     Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
				varianceengine.Rule{FieldName: "rollover_interest_rate", VarianceType: varianceengine.TypeRate,   ExpectedValue: fmtF(dbInterestRate),    ActualValue: fmtF(req.RolloverInterestRate), Priority: varianceengine.PriorityHigh,   Tolerance: 0.001, SystemComment: "Rollover rate vs current contracted rate"},
				varianceengine.Rule{FieldName: "partial_withdrawal",     VarianceType: varianceengine.TypeAmount, ExpectedValue: "0.0000",                ActualValue: fmtF(req.PartialWithdrawal),    Priority: varianceengine.PriorityHigh,   Tolerance: 0.01,  SystemComment: "Non-zero only for PARTIAL rollover"},
			)
		}

		// ── 6. Run engine ──────────────────────────────────────────────────
		items := varianceengine.Compare("FD_CLOSURE", req.ClosureRequestID, entityID, runID, rules)

		// ── 7. Persist if re-validating ────────────────────────────────────
		if req.ClosureRequestID != "" {
			// Auto-resolve any field whose value now matches system expectation
			_ = varianceengine.AutoResolveCleared(ctx, pool, req.ClosureRequestID, items, req.UserID, userEmail)
			// Persist / update rows that still have a variance
			_ = varianceengine.PersistVariances(ctx, pool, items)
			// Stamp flags on the closure request record
			_ = varianceengine.UpdateRecordFlags(ctx, pool,
				"investment.fd_closure_request", "closure_request_id",
				req.ClosureRequestID, runID, items)
		}

		// ── 8. Build variance response ─────────────────────────────────────
		openCount := 0
		variances := make([]map[string]interface{}, 0, len(items))
		for _, item := range items {
			if item.HasVariance && item.Status == varianceengine.StatusOpen {
				openCount++
			}
			variances = append(variances, map[string]interface{}{
				"field_name":     item.FieldName,
				"variance_type":  item.VarianceType,
				"expected_value": item.ExpectedValue,
				"actual_value":   item.ActualValue,
				"delta":          item.VarianceDelta,
				"priority":       item.Priority,
				"has_variance":   item.HasVariance,
				"status":         item.Status,
				"system_comment": item.SystemComment,
				"variance_id":    item.VarianceID,
			})
		}

		api.LogInfo("[FDClosure] ValidateClosure: fd=%s type=%s run=%s total_variances=%d open=%d by=%s",
			req.FDID, req.ClosureType, runID, countVariances(items), openCount, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"fd_id":          req.FDID,
			"closure_type":   req.ClosureType,
			"run_id":         runID,
			"has_variance":   openCount > 0,
			"variance_count": countVariances(items),
			"open_count":     openCount,
			"variances":      variances,
			// System-computed values — for UI pre-population / confirmation
			"system_values": map[string]interface{}{
				// Identity
				"bank_id":            dbBankID,
				"bank_name":          dbBankName,
				"bank_fd_ref_no":     dbBankFDRef,
				"entity_id":          dbEntityID,
				"entity_name":        dbEntityName,
				// Core FD
				"principal_amount":    dbPrincipal,
				"interest_rate":       dbInterestRate,
				"interest_type_code":  dbInterestTypeCode,
				"tenure_days":         dbTenureDays,
				"start_date":          fmtDate(dbStartDate),
				"maturity_date":       fmtDate(dbMaturityDate),
				"fd_status":           dbFDStatus,
				// Accrual
				"accrued_interest":    roundToFour(dbAccruedInterest),
				"tds_deducted":        roundToFour(dbTDSDeducted),
				// Net payout
				"net_payout_amount":   dbNetPayout,
				// Maturity-specific system values
				"principal_returned":  dbPrincipal,
				"gross_interest":      roundToFour(dbAccruedInterest),
				// Premature-specific system values
				"days_held":               dbDaysHeld,
				"contracted_rate":         dbInterestRate,
				"applicable_rate":         dbApplicableRate,
				"penalty_rate":            dbPenaltyRate,
				"penalty_amount":          dbPenaltyAmount,
				"no_interest_flag":        dbNoInterestFlag,
				"min_days_for_interest":   dbMinDays,
				"penalty_structure_id":    dbPenaltyStructureID,
				// Rollover-specific system values
				"rollover_principal":  dbPrincipal,
				"rollover_amount":     dbNetPayout,
				"interest_credited":   roundToFour(dbAccruedInterest),
			},
		})
	}
}

func countVariances(items []varianceengine.VarianceItem) int {
	n := 0
	for _, item := range items {
		if item.HasVariance {
			n++
		}
	}
	return n
}

// ─── Approval post-finalize hook ──────────────────────────────────────────────
//
// Registered at startup: when the approval engine finalises any FD_CLOSURE_*
// instance, this hook updates fd_closure_request.closure_status to match the
// approval outcome (APPROVED → triggers journal posting; REJECTED → resets FD).

func init() {
	for _, txType := range []string{"FD_CLOSURE_MATURITY", "FD_CLOSURE_PREMATURE", "FD_CLOSURE_ROLLOVER"} {
		t := txType
		approvalengine.RegisterPostFinalizeHook(t, func(ctx context.Context, pool *pgxpool.Pool, recordID, transactionType, finalStatus, actorEmail, comment string) {
			if finalStatus == "APPROVED" {
				// Load closure data and post journals.
				var fdID, closureType, entityID, entityName string
				var principalAmt, accruedInterest, tdsAmt, penaltyAmt, netPayout float64
				err := pool.QueryRow(ctx, `
					SELECT fd_id, closure_type,
					       COALESCE(entity_id,''), COALESCE(entity_name,''),
					       principal_amount, accrued_interest, tds_deducted,
					       penalty_amount, net_payout_amount
					FROM investment.fd_closure_request
					WHERE closure_request_id = $1`, recordID,
				).Scan(&fdID, &closureType, &entityID, &entityName,
					&principalAmt, &accruedInterest, &tdsAmt, &penaltyAmt, &netPayout)
				if err != nil {
					api.LogError("[FDClosure] postFinalizeHook load error for %s: %v", recordID, err)
					return
				}
				// Derive approvedBy from email (best-effort userID lookup).
				approvedBy := ""
				for _, s := range auth.GetActiveSessions() {
					if s.Email == actorEmail {
						approvedBy = s.UserID
						break
					}
				}
				if postErr := postClosureJournals(ctx, pool, recordID, fdID, closureType,
					entityID, entityName, principalAmt, accruedInterest, tdsAmt, penaltyAmt, netPayout,
					approvedBy, actorEmail); postErr != nil {
					api.LogError("[FDClosure] postFinalizeHook journal posting failed for %s: %v", recordID, postErr)
				}
			} else if finalStatus == "REJECTED" {
				_, _ = pool.Exec(ctx,
					`UPDATE investment.fd_closure_request
					 SET closure_status='REJECTED', rejected_at=NOW(),
					     rejected_by=$1, rejection_reason=$2,
					     updated_by=$1, updated_at=NOW()
					 WHERE closure_request_id=$3`,
					actorEmail, comment, recordID)
				_, _ = pool.Exec(ctx,
					`UPDATE investment.fd_master
					 SET closure_request_id=NULL, updated_at=NOW()
					 WHERE closure_request_id=$1`, recordID)
			}
			api.LogInfo("[FDClosure] postFinalizeHook: closureID=%s type=%s status=%s by=%s", recordID, t, finalStatus, actorEmail)
		})
	}
}

// ─── Handler 9 — GetClosureApprovalList ──────────────────────────────────────
//
// POST /investment/fd/closure/approval-list
//
// Returns closure requests enriched with approval instance status, current
// active eye, and pending approvers — for building an approval-queue screen.

type closureApprovalListRequest struct {
	UserID      string `json:"user_id"`
	EntityID    string `json:"entity_id"`
	ClosureType string `json:"closure_type"` // optional filter
	Status      string `json:"status"`       // optional: PENDING_APPROVAL | APPROVED | REJECTED | POSTED
	Page        int    `json:"page"`
	PageSize    int    `json:"page_size"`
}

func GetClosureApprovalList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req closureApprovalListRequest
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

		// Build WHERE clause
		conditions := []string{"cr.is_deleted = false"}
		args := []interface{}{}
		argIdx := 1

		if req.EntityID != "" {
			args = append(args, req.EntityID)
			conditions = append(conditions, fmt.Sprintf("cr.entity_id = $%d", argIdx))
			argIdx++
		}
		if req.ClosureType != "" {
			args = append(args, req.ClosureType)
			conditions = append(conditions, fmt.Sprintf("cr.closure_type = $%d", argIdx))
			argIdx++
		}
		if req.Status != "" {
			args = append(args, req.Status)
			conditions = append(conditions, fmt.Sprintf("cr.closure_status = $%d", argIdx))
			argIdx++
		}
		where := strings.Join(conditions, " AND ")

		// Count
		var totalCount int
		_ = pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT COUNT(*) FROM investment.fd_closure_request cr WHERE %s", where), args...).Scan(&totalCount)

		// Paginated list with approval engine enrichment
		listArgs := append(args, req.PageSize, offset)
		dataSQL := fmt.Sprintf(`
			SELECT
			  cr.closure_request_id,
			  cr.fd_id,
			  COALESCE(cr.entity_id,'')           AS entity_id,
			  COALESCE(cr.entity_name,'')          AS entity_name,
			  cr.closure_type,
			  cr.closure_status,
			  cr.principal_amount,
			  cr.accrued_interest,
			  cr.tds_deducted,
			  cr.penalty_amount,
			  cr.net_payout_amount,
			  cr.initiation_date,
			  cr.effective_closure_date,
			  cr.maturity_date,
			  cr.submitted_by_email,
			  cr.created_at,
			  cr.updated_at,
			  COALESCE(m.bank_name,'')             AS bank_name,
			  COALESCE(m.bank_fd_ref_no,'')        AS bank_fd_ref_no,
			  m.interest_rate,
			  m.tenure_days,
			  COALESCE(ai.instance_id,'')          AS instance_id,
			  COALESCE(ai.status,'')               AS approval_instance_status,
			  COALESCE(ai.submitted_at::text,'')   AS approval_submitted_at,
			  COALESCE(ai.resolved_at::text,'')    AS approval_resolved_at,
			  COALESCE(ai.resolved_by_email,'')    AS approval_resolved_by,
			  COALESCE(
			    (SELECT json_agg(json_build_object(
			        'instance_eye_id', ie.instance_eye_id,
			        'position',        ie.position,
			        'status',          ie.status,
			        'approvals_required', ie.approvals_required,
			        'approvals_received', ie.approvals_received,
			        'activated_at',    ie.activated_at
			    ) ORDER BY ie.position)
			     FROM uam.approval_instance_eye ie
			     WHERE ie.instance_id = ai.instance_id
			    )::text,
			  '[]') AS eye_summary,
			  COALESCE(
			    (SELECT json_agg(json_build_object(
			        'user_id',    m2.user_id,
			        'eye_id',     ie2.matrix_eye_id,
			        'eye_status', ie2.status
			    ))
			     FROM uam.approval_instance_eye ie2
			     JOIN uam.approval_matrix_eye_member m2
			       ON m2.eye_id = ie2.matrix_eye_id
			      AND m2.member_type = 'APPROVER'
			      AND m2.is_active = true
			      AND m2.is_deleted = false
			     WHERE ie2.instance_id = ai.instance_id
			       AND ie2.status = 'ACTIVE'
			    )::text,
			  '[]') AS pending_approvers,
			  COALESCE(
			    (SELECT COUNT(*) FROM public.variance_log vl
			     WHERE vl.record_id = cr.fd_id AND vl.status = 'OPEN')
			  , 0) AS open_variance_count
			FROM investment.fd_closure_request cr
			LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id
			LEFT JOIN uam.approval_instance ai ON ai.instance_id = cr.approval_instance_id
			WHERE %s
			ORDER BY cr.created_at DESC
			LIMIT $%d OFFSET $%d`, where, argIdx, argIdx+1)

		rows, err := pool.Query(ctx, dataSQL, listArgs...)
		if err != nil {
			api.LogError("[FDClosure] GetClosureApprovalList query error: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch approval list")
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

		api.LogInfo("[FDClosure] GetClosureApprovalList: %d/%d page=%d by=%s", len(records), totalCount, req.Page, userEmail)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"records":   records,
			"total":     totalCount,
			"page":      req.Page,
			"page_size": req.PageSize,
		})
	}
}
