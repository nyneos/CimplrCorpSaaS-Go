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

		// Maturity-dashboard eligibility rules:
		//   SHOW  — FD has no closure entry at all
		//   SHOW  — FD's only closure entries are is_deleted=true (operator deleted → re-admitted)
		//   HIDE  — FD has ANY non-deleted closure entry regardless of status
		//           (PENDING_APPROVAL, APPROVED, POSTED, REJECTED — all hide the FD)
		//
		// Only a hard delete (is_deleted=true) on every closure row re-admits the FD.
		conditions := []string{
			"m.is_deleted = false",
			"m.fd_status IN ('ACTIVE','MATURED')",
			`NOT EXISTS (
			  SELECT 1 FROM investment.fd_closure_request xcr
			  WHERE xcr.fd_id = m.fd_id
			    AND xcr.is_deleted = false
			)`,
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
			  COALESCE(cr.closure_is_deleted, false) AS closure_is_deleted,
			  COALESCE(ai.status,'') AS approval_status,
			  COALESCE((SELECT aud.processing_status FROM investment.fd_audit_closure_request aud WHERE aud.closure_request_id = cr.closure_request_id ORDER BY aud.created_at DESC LIMIT 1),'') AS latest_processing_status,
			  COALESCE(cf.total_periods,0) AS total_cashflow_periods,
			  COALESCE(cf.last_event_date::text,'') AS last_cashflow_event,
			  ROUND((
			    m.principal_amount
			    + COALESCE(al.total_interest_accrued, 0)
			    - COALESCE(al.total_tds_accrued, 0)
			  )::numeric, 4) AS expected_maturity_amount
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
			  SELECT closure_request_id, closure_type, closure_status,
			         is_deleted AS closure_is_deleted,
			         approval_instance_id
			  FROM investment.fd_closure_request
			  WHERE fd_id = m.fd_id
			  ORDER BY created_at DESC LIMIT 1
			) cr ON true
			LEFT JOIN uam.approval_instance ai ON ai.instance_id = cr.approval_instance_id
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
			allWhere := `m.is_deleted = false AND m.fd_status IN ('ACTIVE','MATURED')
			  AND NOT EXISTS (
			    SELECT 1 FROM investment.fd_closure_request xcr
			    WHERE xcr.fd_id = m.fd_id
			      AND xcr.is_deleted = false
			      AND xcr.closure_status IN ('PENDING_APPROVAL','APPROVED','POSTED')
			  )`
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
			  COALESCE(cr.closure_is_deleted, false) AS closure_is_deleted,
			  COALESCE(ai.status,'') AS approval_status,
			  COALESCE((SELECT aud.processing_status FROM investment.fd_audit_closure_request aud WHERE aud.closure_request_id = cr.closure_request_id ORDER BY aud.created_at DESC LIMIT 1),'') AS latest_processing_status,
			  COALESCE(cf.total_periods,0) AS total_cashflow_periods,
			  COALESCE(cf.last_event_date::text,'') AS last_cashflow_event,
			  ROUND((
			    m.principal_amount
			    + COALESCE(al.total_interest_accrued, 0)
			    - COALESCE(al.total_tds_accrued, 0)
			  )::numeric, 4) AS expected_maturity_amount
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
				  SELECT closure_request_id, closure_type, closure_status,
				         is_deleted AS closure_is_deleted,
				         approval_instance_id
				  FROM investment.fd_closure_request
				  WHERE fd_id = m.fd_id
				  ORDER BY created_at DESC LIMIT 1
				) cr ON true
				LEFT JOIN uam.approval_instance ai ON ai.instance_id = cr.approval_instance_id
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

// initiateClosureRequest mirrors validateClosureRequest exactly so the UI sends
// the same payload to both endpoints.  Every field here is the final user-confirmed
// (possibly overridden) value.  The handler uses these to persist the closure row;
// any field that differs from the DB source-of-truth must have been resolved / accepted
// via proceed_with_exception before calling /initiate.
type initiateClosureRequest struct {
	// ── Session ────────────────────────────────────────────────────────────
	UserID string `json:"user_id"`

	// ── FD identity ────────────────────────────────────────────────────────
	FDID        string `json:"fd_id"`
	ClosureType string `json:"closure_type"` // MATURITY | PREMATURE | ROLLOVER
	BankID      string `json:"bank_id"`
	BankName    string `json:"bank_name"`
	BankFDRefNo string `json:"bank_fd_ref_no"`
	EntityID    string `json:"entity_id"`
	EntityName  string `json:"entity_name"`

	// ── Core FD economics ──────────────────────────────────────────────────
	PrincipalAmount  float64 `json:"principal_amount"`
	InterestRate     float64 `json:"interest_rate"`
	TenureDays       int     `json:"tenure_days"`
	StartDate        string  `json:"start_date"`
	MaturityDate     string  `json:"maturity_date"`
	InterestTypeCode string  `json:"interest_type_code"`

	// ── Accrual & TDS ──────────────────────────────────────────────────────
	AccruedInterest float64 `json:"accrued_interest"`
	TDSDeducted     float64 `json:"tds_deducted"`
	TDSRate         float64 `json:"tds_rate"`

	// ── Penalty (PREMATURE) ────────────────────────────────────────────────
	PenaltyRate          float64 `json:"penalty_rate"`
	PenaltyAmount        float64 `json:"penalty_amount"`
	NoInterestFlag       bool    `json:"no_interest_flag"`
	DaysHeld             int     `json:"days_held"`
	ContractedRate       float64 `json:"contracted_rate"`
	ApplicableRate       float64 `json:"applicable_rate"`
	GrossInterestEarned  float64 `json:"gross_interest_earned"`
	TDSOnInterest        float64 `json:"tds_on_interest"`
	PenaltyStructureID   string  `json:"penalty_structure_id"`
	PenaltyType          string  `json:"penalty_type"`          // snapshot: PERCENTAGE | FLAT_AMOUNT | RATE_REDUCTION
	PenaltyValue         float64 `json:"penalty_value"`         // snapshot of penalty master value
	PenaltyCalcMethod    string  `json:"penalty_calc_method"`   // snapshot of calculation_method
	PrematureClosureDate string  `json:"premature_closure_date"`

	// ── Net payout / settlement ────────────────────────────────────────────
	NetPayoutAmount      float64 `json:"net_payout_amount"`
	SettlementAccountID  string  `json:"settlement_account_id"`
	SettlementBankName   string  `json:"settlement_bank_name"`
	SettlementAccountNo  string  `json:"settlement_account_number"`
	PaymentMode          string  `json:"payment_mode"`
	EffectiveClosureDate string  `json:"effective_closure_date"`
	MaturityInstructions string  `json:"maturity_instructions"`

	// ── Maturity payout specific ───────────────────────────────────────────
	PrincipalReturned float64 `json:"principal_returned"`
	GrossInterest     float64 `json:"gross_interest"`
	PayoutDate        string  `json:"payout_date"`
	BankReference     string  `json:"bank_reference"` // bank's UTR / transaction ref for payout

	// ── Rollover specific ──────────────────────────────────────────────────
	RolloverType         string  `json:"rollover_type"` // FULL | PARTIAL | PRINCIPAL_ONLY
	RolloverAmount       float64 `json:"rollover_amount"`
	RolloverPrincipal    float64 `json:"rollover_principal"`
	InterestCredited     float64 `json:"interest_credited"`
	RolloverTenorDays    int     `json:"rollover_tenor_days"`
	RolloverInterestRate float64 `json:"rollover_interest_rate"`
	NewMaturityDate      string  `json:"new_maturity_date"`
	PartialWithdrawal    float64 `json:"partial_withdrawal"`
	NewFDID              string  `json:"new_fd_id"`
	// New bank for rollover (may differ from the source FD's bank)
	NewBankID    string `json:"new_bank_id"`
	NewBankName  string `json:"new_bank_name"`
	NewAccountID string `json:"new_account_id"` // settlement/source account at new bank

	// ── Closure metadata ──────────────────────────────────────────────────
	ClosureReason string `json:"closure_reason"`
	ClosureNotes  string `json:"closure_notes"`

	// ── Variance / exception controls ─────────────────────────────────────
	// HasVariance: UI carries forward from the last /validate response.
	HasVariance bool `json:"has_variance"`
	// ProceedWithException: true = user accepted remaining open variances as exceptions.
	ProceedWithException bool `json:"proceed_with_exception"`
	// LastValidatedRunID: run_id from the last /validate call.
	LastValidatedRunID string `json:"last_validated_run_id"`
	// VarianceRemark: free-text reason the user provides when proceeding with exceptions.
	VarianceRemark string `json:"variance_remark"`
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
			// Compute days_held so we can pick the correct tenor-bracket from the penalty table.
			var daysHeld int
			_ = pool.QueryRow(ctx, `SELECT COALESCE((CURRENT_DATE - start_date)::int,0) FROM investment.fd_master WHERE fd_id=$1`, req.FDID).Scan(&daysHeld)

			// Select the penalty row whose holding-period bracket best matches days_held.
			// Brackets are expressed via min_holding_days / max_holding_days when present;
			// fall back to the highest-value row if no bracket columns exist.
			var noInterestMinDays int
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(ps.penalty_value,0),
				       COALESCE(ps.no_interest_if_withdrawn_before,0) > 0,
				       COALESCE(ps.no_interest_if_withdrawn_before,0)
				FROM investment.fd_penalty_structure_master ps
				WHERE ps.bank_code = $1
				  AND COALESCE(ps.is_deleted, false) = false
				  AND (
				        -- bracket match: days_held falls within [min, max)
				        (ps.min_holding_days IS NOT NULL AND $2 >= ps.min_holding_days
				         AND (ps.max_holding_days IS NULL OR $2 < ps.max_holding_days))
				        -- or no bracket columns defined: pick the one highest-penalty row
				        OR (ps.min_holding_days IS NULL AND ps.max_holding_days IS NULL)
				      )
				ORDER BY
				  -- prefer bracket rows over non-bracket rows
				  (CASE WHEN ps.min_holding_days IS NOT NULL THEN 0 ELSE 1 END),
				  ps.penalty_value DESC
				LIMIT 1`, bankID, daysHeld,
			).Scan(&penaltyRate, &noInterestFlag, &noInterestMinDays)

			if noInterestFlag && daysHeld < noInterestMinDays {
				accruedInterest = 0
				tdsDeducted = 0
			}
			penaltyAmount = roundToFour(accruedInterest * penaltyRate / 100.0)
		}

		// If the user supplied explicit values (from /validate system_values), use those;
		// otherwise fall back to DB-computed values loaded above.
		if req.AccruedInterest > 0 {
			accruedInterest = req.AccruedInterest
		}
		if req.TDSDeducted > 0 {
			tdsDeducted = req.TDSDeducted
		}
		if req.PenaltyAmount > 0 {
			penaltyAmount = req.PenaltyAmount
		}
		if req.PenaltyRate > 0 {
			penaltyRate = req.PenaltyRate
		}

		netPayout := roundToFour(principalAmount + accruedInterest - tdsDeducted - penaltyAmount)
		// If user explicitly sent net_payout_amount (post-exception), honour it.
		if req.NetPayoutAmount > 0 {
			netPayout = roundToFour(req.NetPayoutAmount)
		}
		if req.ClosureType == "ROLLOVER" {
			switch req.RolloverType {
			case "PRINCIPAL_ONLY":
				// Interest is paid out separately; only principal is reinvested.
				netPayout = roundToFour(principalAmount)
			case "PARTIAL":
				// partial_withdrawal is taken out; rest (principal+net_interest) rolls over.
				if req.RolloverAmount > 0 {
					netPayout = roundToFour(req.RolloverAmount)
				} else {
					netPayout = roundToFour(principalAmount + accruedInterest - tdsDeducted - req.PartialWithdrawal)
				}
			default: // FULL or empty
				// Entire principal + net interest rolls over.
				if req.RolloverAmount > 0 {
					netPayout = roundToFour(req.RolloverAmount)
				} else {
					netPayout = roundToFour(principalAmount + accruedInterest - tdsDeducted)
				}
			}
		}

		effectiveDateStr := req.EffectiveClosureDate
		if effectiveDateStr == "" {
			effectiveDateStr = time.Now().Format(constants.DateFormat)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
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
			  variance_remark,
			  submitted_by, submitted_by_email,
			  accounting_posted, is_deleted,
			  created_by, created_at, updated_by, updated_at
			) VALUES (
			  $1,$2,$3,$4,$5,
			  $6,'PENDING_APPROVAL',
			  CURRENT_DATE,$7::date,$8::date,
			  $9,$10,$11,$12,$13,
			  $14,$15,$16,$17,$18,$19,
			  $20,
			  $21,$22,false,false,
			  $22,NOW(),$22,NOW()
			) RETURNING closure_request_id`,
			req.FDID, nullStrOrNil(bookingID), nullStrOrNil(confirmationID),
			nullStrOrNil(firstNonEmpty(req.EntityID, entityID)),
			nullStrOrNil(firstNonEmpty(req.EntityName, entityName)),
			req.ClosureType,
			effectiveDateStr, maturityDate.Format(constants.DateFormat),
			principalAmount, roundToFour(accruedInterest), roundToFour(tdsDeducted),
			roundToFour(penaltyAmount), netPayout,
			nullStrOrNil(req.SettlementAccountID), nullStrOrNil(req.MaturityInstructions),
			req.RolloverAmount, req.RolloverTenorDays,
			nullStrOrNil(req.ClosureReason), nullStrOrNil(req.ClosureNotes),
			nullStrOrNil(req.VarianceRemark),
			req.UserID, userEmail,
		).Scan(&closureRequestID)
		if err != nil {
			api.LogError("[FDClosure] InitiateClosure insert error: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to create closure request")
			return
		}

		// Build initial audit snapshot with ALL submitted fields for full traceability.
		initialSnapshot := map[string]interface{}{
			"bank_id": req.BankID, "bank_name": req.BankName, "bank_fd_ref_no": req.BankFDRefNo,
			"entity_id": req.EntityID, "entity_name": req.EntityName,
			"principal_amount": principalAmount, "interest_rate": req.InterestRate,
			"interest_type_code": req.InterestTypeCode, "tenure_days": req.TenureDays,
			"start_date": req.StartDate, "maturity_date": req.MaturityDate,
			"accrued_interest": roundToFour(accruedInterest), "tds_deducted": roundToFour(tdsDeducted),
			"penalty_rate": penaltyRate, "penalty_amount": roundToFour(penaltyAmount),
			"no_interest_flag": noInterestFlag, "days_held": req.DaysHeld,
			"contracted_rate": req.ContractedRate, "applicable_rate": req.ApplicableRate,
			"net_payout_amount": netPayout, "settlement_account_id": req.SettlementAccountID,
			"maturity_instructions": req.MaturityInstructions,
			"principal_returned":    req.PrincipalReturned, "gross_interest": req.GrossInterest,
			"rollover_type": req.RolloverType, "rollover_amount": req.RolloverAmount,
			"rollover_principal": req.RolloverPrincipal, "interest_credited": req.InterestCredited,
			"rollover_tenor_days": req.RolloverTenorDays, "rollover_interest_rate": req.RolloverInterestRate,
			"partial_withdrawal": req.PartialWithdrawal, "new_maturity_date": req.NewMaturityDate,
			"closure_reason": req.ClosureReason, "closure_notes": req.ClosureNotes,
			"variance_remark": req.VarianceRemark, "proceed_with_exception": req.ProceedWithException,
			"closure_status": "PENDING_APPROVAL",
		}
		initialSnapJSON, _ := json.Marshal(initialSnapshot)
		_ = insertClosureAudit(ctx, ClosureAuditParams{Exec: tx, ClosureRequestID: closureRequestID, PerformedBy: req.UserID, PerformedByEmail: userEmail, ActionType: "CREATE", ProcessingStatus: "PENDING_APPROVAL", Reason: "Closure initiated", Snapshot: initialSnapJSON, OldValues: initialSnapshot})
		_, _ = tx.Exec(ctx, `UPDATE investment.fd_master SET closure_request_id=$1, updated_at=NOW() WHERE fd_id=$2`, closureRequestID, req.FDID)

		instID, instErr := approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
			ModuleCode: "FIXED_DEPOSIT", EntityCode: firstNonEmpty(entityID, "DEFAULT"),
			TransactionType: "FD_CLOSURE_" + req.ClosureType,
			RecordID:        closureRequestID, RecordTable: "investment.fd_closure_request",
			AuditTable: constants.QuerryAuditClosureRequest, AuditIDColumn: "closure_request_id",
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
			_, _ = tx.Exec(ctx, `
				INSERT INTO investment.fd_closure_maturity_payout
				  (closure_request_id,fd_id,payout_date,principal_returned,gross_interest,
				   tds_deducted,net_payout,settlement_account_id,bank_reference,payment_status,created_by)
				VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,'PENDING',$10)`,
				closureRequestID, req.FDID, effectiveDateStr, principalAmount,
				roundToFour(accruedInterest), roundToFour(tdsDeducted), netPayout,
				nullStrOrNil(req.SettlementAccountID), nullStrOrNil(req.BankReference), userEmail)
		case "PREMATURE":
			var daysHeld int
			_ = tx.QueryRow(ctx, `SELECT COALESCE((CURRENT_DATE - start_date)::int,0) FROM investment.fd_master WHERE fd_id=$1`, req.FDID).Scan(&daysHeld)
			var contractedRate float64
			_ = tx.QueryRow(ctx, `SELECT COALESCE(interest_rate,0) FROM investment.fd_master WHERE fd_id=$1`, req.FDID).Scan(&contractedRate)
			// Use penalty details from request (populated via /validate system_values) or fall back to computed values
			penaltyType := req.PenaltyType
			penaltyValue := req.PenaltyValue
			penaltyCalcMethod := req.PenaltyCalcMethod
			penaltyStructureID := req.PenaltyStructureID
			_, _ = tx.Exec(ctx, `
				INSERT INTO investment.fd_closure_premature
				  (closure_request_id,fd_id,premature_closure_date,days_held,contracted_rate,
				   applicable_rate,gross_interest_earned,penalty_rate,penalty_amount,no_interest_flag,
				   tds_on_interest,net_payout,penalty_id,penalty_type,penalty_value,calculation_method,
				   bank_reference,created_by)
				VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
				closureRequestID, req.FDID, effectiveDateStr, daysHeld, contractedRate,
				roundToFour(contractedRate-penaltyRate),
				roundToFour(accruedInterest), penaltyRate, roundToFour(penaltyAmount), noInterestFlag,
				roundToFour(tdsDeducted), netPayout,
				nullStrOrNil(penaltyStructureID), nullStrOrNil(penaltyType),
				penaltyValue, nullStrOrNil(penaltyCalcMethod),
				nullStrOrNil(req.BankReference), userEmail)
		case "ROLLOVER":
			rollType := req.RolloverType
			if rollType == "" {
				rollType = "FULL"
			}
			_, _ = tx.Exec(ctx, `
				INSERT INTO investment.fd_closure_rollover
				  (closure_request_id,source_fd_id,rollover_date,
				   rollover_amount,rollover_principal,interest_credited,tds_deducted,
				   new_tenor_days,new_interest_rate,rollover_type,
				   bank_code,new_bank_id,new_bank_name,new_account_id,created_by)
				VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
				closureRequestID, req.FDID, effectiveDateStr,
				netPayout, principalAmount, roundToFour(accruedInterest), roundToFour(tdsDeducted),
				req.RolloverTenorDays, req.RolloverInterestRate, rollType,
				nullStrOrNil(req.NewBankID), nullStrOrNil(req.NewBankID),
				nullStrOrNil(req.NewBankName), nullStrOrNil(req.NewAccountID), userEmail)
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed)
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

// updateClosureRequest carries the same rich field set as initiateClosureRequest
// so a single payload can update any aspect of the closure row and its sub-tables.
type updateClosureRequest struct {
	// ── Session ────────────────────────────────────────────────────────────
	UserID           string `json:"user_id"`
	ClosureRequestID string `json:"closure_request_id"`
	UpdateReason     string `json:"update_reason"`

	// ── FD identity ─────────────────────────────────────────────────────────
	BankID      string `json:"bank_id"`
	BankName    string `json:"bank_name"`
	BankFDRefNo string `json:"bank_fd_ref_no"`
	EntityID    string `json:"entity_id"`
	EntityName  string `json:"entity_name"`

	// ── Core FD economics ───────────────────────────────────────────────────
	PrincipalAmount  float64 `json:"principal_amount"`
	InterestRate     float64 `json:"interest_rate"`
	TenureDays       int     `json:"tenure_days"`
	StartDate        string  `json:"start_date"`
	MaturityDate     string  `json:"maturity_date"`
	InterestTypeCode string  `json:"interest_type_code"`

	// ── Accrual & TDS ───────────────────────────────────────────────────────
	AccruedInterest float64 `json:"accrued_interest"`
	TDSDeducted     float64 `json:"tds_deducted"`
	TDSRate         float64 `json:"tds_rate"`

	// ── Penalty (PREMATURE) ─────────────────────────────────────────────────
	PenaltyRate          float64 `json:"penalty_rate"`
	PenaltyAmount        float64 `json:"penalty_amount"`
	NoInterestFlag       bool    `json:"no_interest_flag"`
	DaysHeld             int     `json:"days_held"`
	ContractedRate       float64 `json:"contracted_rate"`
	ApplicableRate       float64 `json:"applicable_rate"`
	GrossInterestEarned  float64 `json:"gross_interest_earned"`
	TDSOnInterest        float64 `json:"tds_on_interest"`
	PenaltyStructureID   string  `json:"penalty_structure_id"`
	PenaltyType          string  `json:"penalty_type"`
	PenaltyValue         float64 `json:"penalty_value"`
	PenaltyCalcMethod    string  `json:"penalty_calc_method"`
	PrematureClosureDate string  `json:"premature_closure_date"`

	// ── Net payout / settlement ──────────────────────────────────────────────
	NetPayoutAmount      float64 `json:"net_payout_amount"`
	SettlementAccountID  string  `json:"settlement_account_id"`
	SettlementBankName   string  `json:"settlement_bank_name"`
	SettlementAccountNo  string  `json:"settlement_account_number"`
	PaymentMode          string  `json:"payment_mode"`
	EffectiveClosureDate string  `json:"effective_closure_date"`
	MaturityInstructions string  `json:"maturity_instructions"`

	// ── Maturity payout specific ─────────────────────────────────────────────
	PrincipalReturned float64 `json:"principal_returned"`
	GrossInterest     float64 `json:"gross_interest"`
	PayoutDate        string  `json:"payout_date"`
	BankReference     string  `json:"bank_reference"`

	// ── Rollover specific ───────────────────────────────────────────────────
	RolloverType         string  `json:"rollover_type"`
	RolloverAmount       float64 `json:"rollover_amount"`
	RolloverPrincipal    float64 `json:"rollover_principal"`
	InterestCredited     float64 `json:"interest_credited"`
	RolloverTenorDays    int     `json:"rollover_tenor_days"`
	RolloverInterestRate float64 `json:"rollover_interest_rate"`
	NewMaturityDate      string  `json:"new_maturity_date"`
	PartialWithdrawal    float64 `json:"partial_withdrawal"`
	NewFDID              string  `json:"new_fd_id"`
	// New bank for rollover (may differ from source FD’s bank)
	NewBankID    string `json:"new_bank_id"`
	NewBankName  string `json:"new_bank_name"`
	NewAccountID string `json:"new_account_id"`

	// ── Closure metadata ────────────────────────────────────────────────────
	ClosureReason  string `json:"closure_reason"`
	ClosureNotes   string `json:"closure_notes"`
	VarianceRemark string `json:"variance_remark"`
}

func UpdateClosure(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req updateClosureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ClosureRequestID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureRequestIDRequired)
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		var (
			oldEffDate, oldSettlAcct, oldMatInstr, oldReason, oldNotes, oldVarianceRemark string
			oldBankID, oldBankName, oldBankFDRef, oldEntityID, oldEntityName              string
			oldInterestTypeCode, oldStartDate, oldMaturityDate                            string
			oldRolloverAmt, oldRolloverInterestRate, oldPartialWithdrawal                 float64
			oldPrincipalAmt, oldInterestRate, oldAccruedInterest, oldTDSDeducted          float64
			oldPenaltyRate, oldPenaltyAmount, oldNetPayout                                float64
			oldRolloverDays, oldTenureDays                                                int
			oldStatus, oldClosureType, oldFDID                                            string
		)
		err := pool.QueryRow(ctx, `
			SELECT
			  COALESCE(effective_closure_date::text,''), COALESCE(settlement_account_id,''),
			  COALESCE(maturity_instructions,''), COALESCE(closure_reason,''),
			  COALESCE(closure_notes,''), COALESCE(variance_remark,''),
			  COALESCE(rollover_amount,0), COALESCE(rollover_tenor_days,0),
			  closure_status, closure_type, fd_id,
			  COALESCE(principal_amount,0), COALESCE(accrued_interest,0),
			  COALESCE(tds_deducted,0), COALESCE(penalty_amount,0), COALESCE(net_payout_amount,0),
			  COALESCE(m.bank_id,''), COALESCE(m.bank_name,''), COALESCE(m.bank_fd_ref_no,''),
			  COALESCE(b.entity_id,''), COALESCE(b.entity_name,''),
			  COALESCE(m.interest_rate,0), COALESCE(m.interest_type_code,''),
			  COALESCE(m.tenure_days,0),
			  COALESCE(m.start_date::text,''), COALESCE(m.maturity_date::text,''),
			  COALESCE(cr.penalty_rate,0),
			  COALESCE(cr.rollover_interest_rate,0), COALESCE(cr.partial_withdrawal,0)
			FROM investment.fd_closure_request cr
			LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
			WHERE cr.closure_request_id=$1 AND cr.is_deleted=false`, req.ClosureRequestID,
		).Scan(
			&oldEffDate, &oldSettlAcct, &oldMatInstr, &oldReason, &oldNotes, &oldVarianceRemark,
			&oldRolloverAmt, &oldRolloverDays,
			&oldStatus, &oldClosureType, &oldFDID,
			&oldPrincipalAmt, &oldAccruedInterest, &oldTDSDeducted, &oldPenaltyAmount, &oldNetPayout,
			&oldBankID, &oldBankName, &oldBankFDRef,
			&oldEntityID, &oldEntityName,
			&oldInterestRate, &oldInterestTypeCode, &oldTenureDays,
			&oldStartDate, &oldMaturityDate,
			&oldPenaltyRate, &oldRolloverInterestRate, &oldPartialWithdrawal,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrClosureRequestNotFound)
			} else {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to load closure request")
			}
			return
		}
		if oldStatus != "PENDING_APPROVAL" {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Cannot update closure in status %s", oldStatus))
			return
		}
		_ = oldClosureType
		_ = oldFDID // used in audit only

		oldRow := map[string]interface{}{
			"closure_status":         oldStatus,
			"bank_id":                oldBankID,
			"bank_name":              oldBankName,
			"bank_fd_ref_no":         oldBankFDRef,
			"entity_id":              oldEntityID,
			"entity_name":            oldEntityName,
			"principal_amount":       oldPrincipalAmt,
			"interest_rate":          oldInterestRate,
			"interest_type_code":     oldInterestTypeCode,
			"tenure_days":            oldTenureDays,
			"start_date":             oldStartDate,
			"maturity_date":          oldMaturityDate,
			"accrued_interest":       oldAccruedInterest,
			"tds_deducted":           oldTDSDeducted,
			"penalty_rate":           oldPenaltyRate,
			"penalty_amount":         oldPenaltyAmount,
			"net_payout_amount":      oldNetPayout,
			"effective_closure_date": oldEffDate,
			"settlement_account_id":  oldSettlAcct,
			"maturity_instructions":  oldMatInstr,
			"rollover_amount":        oldRolloverAmt,
			"rollover_tenor_days":    oldRolloverDays,
			"rollover_interest_rate": oldRolloverInterestRate,
			"partial_withdrawal":     oldPartialWithdrawal,
			"closure_reason":         oldReason,
			"closure_notes":          oldNotes,
			"variance_remark":        oldVarianceRemark,
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// ── Build SET clause for fd_closure_request ─────────────────────────────
		setClauses := []string{"updated_by=$1", "updated_at=NOW()"}
		updateArgs := []interface{}{userEmail}
		argN := 2

		// identity
		if req.BankID != "" {
			setClauses = append(setClauses, fmt.Sprintf("bank_id=$%d", argN))
			updateArgs = append(updateArgs, req.BankID)
			argN++
		}
		if req.BankName != "" {
			setClauses = append(setClauses, fmt.Sprintf("bank_name=$%d", argN))
			updateArgs = append(updateArgs, req.BankName)
			argN++
		}
		if req.BankFDRefNo != "" {
			setClauses = append(setClauses, fmt.Sprintf("bank_fd_ref_no=$%d", argN))
			updateArgs = append(updateArgs, req.BankFDRefNo)
			argN++
		}
		if req.EntityID != "" {
			setClauses = append(setClauses, fmt.Sprintf("entity_id=$%d", argN))
			updateArgs = append(updateArgs, req.EntityID)
			argN++
		}
		if req.EntityName != "" {
			setClauses = append(setClauses, fmt.Sprintf("entity_name=$%d", argN))
			updateArgs = append(updateArgs, req.EntityName)
			argN++
		}
		// economics
		if req.PrincipalAmount > 0 {
			setClauses = append(setClauses, fmt.Sprintf("principal_amount=$%d", argN))
			updateArgs = append(updateArgs, req.PrincipalAmount)
			argN++
		}
		if req.InterestRate > 0 {
			setClauses = append(setClauses, fmt.Sprintf("interest_rate=$%d", argN))
			updateArgs = append(updateArgs, req.InterestRate)
			argN++
		}
		if req.TenureDays > 0 {
			setClauses = append(setClauses, fmt.Sprintf("tenure_days=$%d", argN))
			updateArgs = append(updateArgs, req.TenureDays)
			argN++
		}
		if req.StartDate != "" {
			setClauses = append(setClauses, fmt.Sprintf("start_date=$%d::date", argN))
			updateArgs = append(updateArgs, req.StartDate)
			argN++
		}
		if req.MaturityDate != "" {
			setClauses = append(setClauses, fmt.Sprintf("maturity_date=$%d::date", argN))
			updateArgs = append(updateArgs, req.MaturityDate)
			argN++
		}
		if req.InterestTypeCode != "" {
			setClauses = append(setClauses, fmt.Sprintf("interest_type_code=$%d", argN))
			updateArgs = append(updateArgs, req.InterestTypeCode)
			argN++
		}
		// accrual & TDS
		if req.AccruedInterest > 0 {
			setClauses = append(setClauses, fmt.Sprintf("accrued_interest=$%d", argN))
			updateArgs = append(updateArgs, req.AccruedInterest)
			argN++
		}
		if req.TDSDeducted > 0 {
			setClauses = append(setClauses, fmt.Sprintf("tds_deducted=$%d", argN))
			updateArgs = append(updateArgs, req.TDSDeducted)
			argN++
		}
		// penalty
		if req.PenaltyRate > 0 {
			setClauses = append(setClauses, fmt.Sprintf("penalty_rate=$%d", argN))
			updateArgs = append(updateArgs, req.PenaltyRate)
			argN++
		}
		if req.PenaltyAmount > 0 {
			setClauses = append(setClauses, fmt.Sprintf("penalty_amount=$%d", argN))
			updateArgs = append(updateArgs, req.PenaltyAmount)
			argN++
		}
		// payout / settlement
		if req.NetPayoutAmount > 0 {
			setClauses = append(setClauses, fmt.Sprintf("net_payout_amount=$%d", argN))
			updateArgs = append(updateArgs, req.NetPayoutAmount)
			argN++
		}
		if req.SettlementAccountID != "" {
			setClauses = append(setClauses, fmt.Sprintf("settlement_account_id=$%d", argN))
			updateArgs = append(updateArgs, req.SettlementAccountID)
			argN++
		}
		if req.EffectiveClosureDate != "" {
			setClauses = append(setClauses, fmt.Sprintf("effective_closure_date=$%d::date", argN))
			updateArgs = append(updateArgs, req.EffectiveClosureDate)
			argN++
		}
		if req.MaturityInstructions != "" {
			setClauses = append(setClauses, fmt.Sprintf("maturity_instructions=$%d", argN))
			updateArgs = append(updateArgs, req.MaturityInstructions)
			argN++
		}
		// rollover
		if req.RolloverType != "" {
			setClauses = append(setClauses, fmt.Sprintf("rollover_type=$%d", argN))
			updateArgs = append(updateArgs, req.RolloverType)
			argN++
		}
		if req.RolloverAmount > 0 {
			setClauses = append(setClauses, fmt.Sprintf("rollover_amount=$%d", argN))
			updateArgs = append(updateArgs, req.RolloverAmount)
			argN++
		}
		if req.RolloverPrincipal > 0 {
			setClauses = append(setClauses, fmt.Sprintf("rollover_principal=$%d", argN))
			updateArgs = append(updateArgs, req.RolloverPrincipal)
			argN++
		}
		if req.InterestCredited > 0 {
			setClauses = append(setClauses, fmt.Sprintf("interest_credited=$%d", argN))
			updateArgs = append(updateArgs, req.InterestCredited)
			argN++
		}
		if req.RolloverTenorDays > 0 {
			setClauses = append(setClauses, fmt.Sprintf("rollover_tenor_days=$%d", argN))
			updateArgs = append(updateArgs, req.RolloverTenorDays)
			argN++
		}
		if req.RolloverInterestRate > 0 {
			setClauses = append(setClauses, fmt.Sprintf("rollover_interest_rate=$%d", argN))
			updateArgs = append(updateArgs, req.RolloverInterestRate)
			argN++
		}
		if req.NewMaturityDate != "" {
			setClauses = append(setClauses, fmt.Sprintf("new_maturity_date=$%d::date", argN))
			updateArgs = append(updateArgs, req.NewMaturityDate)
			argN++
		}
		if req.PartialWithdrawal > 0 {
			setClauses = append(setClauses, fmt.Sprintf("partial_withdrawal=$%d", argN))
			updateArgs = append(updateArgs, req.PartialWithdrawal)
			argN++
		}
		if req.NewFDID != "" {
			setClauses = append(setClauses, fmt.Sprintf("new_fd_id=$%d", argN))
			updateArgs = append(updateArgs, req.NewFDID)
			argN++
		}
		// metadata
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
		if req.VarianceRemark != "" {
			setClauses = append(setClauses, fmt.Sprintf("variance_remark=$%d", argN))
			updateArgs = append(updateArgs, req.VarianceRemark)
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

		// ── Upsert type-specific sub-table ───────────────────────────────────────
		switch oldClosureType {
		case "MATURITY":
			effDate := firstNonEmpty(req.EffectiveClosureDate, req.PayoutDate, oldEffDate)
			princ := req.PrincipalAmount
			if princ <= 0 {
				princ = oldPrincipalAmt
			}
			netP := req.NetPayoutAmount
			if netP <= 0 {
				netP = oldNetPayout
			}
			grInt := req.GrossInterest
			if grInt <= 0 {
				grInt = oldAccruedInterest
			}
			tds := req.TDSDeducted
			if tds <= 0 {
				tds = oldTDSDeducted
			}
			settleAcct := firstNonEmpty(req.SettlementAccountID, oldSettlAcct)
			_, _ = tx.Exec(ctx, `
				INSERT INTO investment.fd_closure_maturity_payout
				  (closure_request_id,fd_id,payout_date,principal_returned,gross_interest,tds_deducted,net_payout,settlement_account_id,bank_reference,payment_status,created_by)
				VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,'PENDING',$10)
				ON CONFLICT (closure_request_id) DO UPDATE SET
				  payout_date=EXCLUDED.payout_date, principal_returned=EXCLUDED.principal_returned,
				  gross_interest=EXCLUDED.gross_interest, tds_deducted=EXCLUDED.tds_deducted,
				  net_payout=EXCLUDED.net_payout, settlement_account_id=EXCLUDED.settlement_account_id,
				  bank_reference=EXCLUDED.bank_reference`,
				req.ClosureRequestID, oldFDID, effDate, princ, roundToFour(grInt), roundToFour(tds), roundToFour(netP), nullStrOrNil(settleAcct), nullStrOrNil(req.BankReference), userEmail)

		case "PREMATURE":
			effDate := firstNonEmpty(req.PrematureClosureDate, req.EffectiveClosureDate, oldEffDate)
			dH := req.DaysHeld
			if dH <= 0 {
				_ = pool.QueryRow(ctx, `SELECT COALESCE((CURRENT_DATE-start_date)::int,0) FROM investment.fd_master WHERE fd_id=$1`, oldFDID).Scan(&dH)
			}
			contrRate := req.ContractedRate
			if contrRate <= 0 {
				contrRate = oldInterestRate
			}
			appRate := req.ApplicableRate
			if appRate <= 0 {
				appRate = roundToFour(contrRate - oldPenaltyRate)
			}
			grInt := req.GrossInterestEarned
			if grInt <= 0 {
				grInt = oldAccruedInterest
			}
			pRate := req.PenaltyRate
			if pRate <= 0 {
				pRate = oldPenaltyRate
			}
			pAmt := req.PenaltyAmount
			if pAmt <= 0 {
				pAmt = oldPenaltyAmount
			}
			tdsOI := req.TDSOnInterest
			if tdsOI <= 0 {
				tdsOI = oldTDSDeducted
			}
			netP := req.NetPayoutAmount
			if netP <= 0 {
				netP = oldNetPayout
			}
			penType := req.PenaltyType
			penVal := req.PenaltyValue
			penCalc := req.PenaltyCalcMethod
			penID := req.PenaltyStructureID
			_, _ = tx.Exec(ctx, `
				INSERT INTO investment.fd_closure_premature
				  (closure_request_id,fd_id,premature_closure_date,days_held,contracted_rate,applicable_rate,
				   gross_interest_earned,penalty_rate,penalty_amount,no_interest_flag,tds_on_interest,net_payout,
				   penalty_id,penalty_type,penalty_value,calculation_method,bank_reference,created_by)
				VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
				ON CONFLICT (closure_request_id) DO UPDATE SET
				  premature_closure_date=EXCLUDED.premature_closure_date,
				  days_held=EXCLUDED.days_held, contracted_rate=EXCLUDED.contracted_rate,
				  applicable_rate=EXCLUDED.applicable_rate, gross_interest_earned=EXCLUDED.gross_interest_earned,
				  penalty_rate=EXCLUDED.penalty_rate, penalty_amount=EXCLUDED.penalty_amount,
				  no_interest_flag=EXCLUDED.no_interest_flag, tds_on_interest=EXCLUDED.tds_on_interest,
				  net_payout=EXCLUDED.net_payout,
				  penalty_id=EXCLUDED.penalty_id, penalty_type=EXCLUDED.penalty_type,
				  penalty_value=EXCLUDED.penalty_value, calculation_method=EXCLUDED.calculation_method,
				  bank_reference=EXCLUDED.bank_reference`,
				req.ClosureRequestID, oldFDID, effDate, dH, contrRate, appRate,
				roundToFour(grInt), pRate, roundToFour(pAmt), req.NoInterestFlag, roundToFour(tdsOI), roundToFour(netP),
				nullStrOrNil(penID), nullStrOrNil(penType), penVal, nullStrOrNil(penCalc),
				nullStrOrNil(req.BankReference), userEmail)

		case "ROLLOVER":
			effDate := firstNonEmpty(req.EffectiveClosureDate, oldEffDate)
			rollAmt := req.RolloverAmount
			if rollAmt <= 0 {
				rollAmt = oldRolloverAmt
			}
			rollPrinc := req.RolloverPrincipal
			if rollPrinc <= 0 {
				rollPrinc = oldPrincipalAmt
			}
			intCred := req.InterestCredited
			if intCred <= 0 {
				intCred = oldAccruedInterest
			}
			tds := req.TDSDeducted
			if tds <= 0 {
				tds = oldTDSDeducted
			}
			rollDays := req.RolloverTenorDays
			if rollDays <= 0 {
				rollDays = oldRolloverDays
			}
			rollType := req.RolloverType
			if rollType == "" {
				rollType = "FULL"
			}
			newBankCode := req.NewBankID
			rollRate := req.RolloverInterestRate
			_, _ = tx.Exec(ctx, `
				INSERT INTO investment.fd_closure_rollover
				  (closure_request_id,source_fd_id,rollover_date,rollover_amount,rollover_principal,
				   interest_credited,tds_deducted,new_tenor_days,new_interest_rate,rollover_type,
				   bank_code,new_bank_id,new_bank_name,new_account_id,created_by)
				VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
				ON CONFLICT (closure_request_id) DO UPDATE SET
				  rollover_date=EXCLUDED.rollover_date, rollover_amount=EXCLUDED.rollover_amount,
				  rollover_principal=EXCLUDED.rollover_principal, interest_credited=EXCLUDED.interest_credited,
				  tds_deducted=EXCLUDED.tds_deducted, new_tenor_days=EXCLUDED.new_tenor_days,
				  new_interest_rate=EXCLUDED.new_interest_rate, rollover_type=EXCLUDED.rollover_type,
				  bank_code=EXCLUDED.bank_code,
				  new_bank_id=EXCLUDED.new_bank_id, new_bank_name=EXCLUDED.new_bank_name,
				  new_account_id=EXCLUDED.new_account_id`,
				req.ClosureRequestID, oldFDID, effDate, roundToFour(rollAmt), roundToFour(rollPrinc),
				roundToFour(intCred), roundToFour(tds), rollDays, rollRate, rollType,
				nullStrOrNil(newBankCode),
				nullStrOrNil(req.NewBankID), nullStrOrNil(req.NewBankName), nullStrOrNil(req.NewAccountID),
				userEmail)
		}

		snapshotJSON, _ := json.Marshal(oldRow)
		// Pass all old values to audit as individual columns.
		// Use PENDING_EDIT_APPROVAL so the audit trail shows this was an edit on a pending record.
		_ = insertClosureAudit(ctx, ClosureAuditParams{Exec: tx, ClosureRequestID: req.ClosureRequestID, PerformedBy: req.UserID, PerformedByEmail: userEmail, ActionType: "UPDATE", ProcessingStatus: "PENDING_EDIT_APPROVAL", Reason: req.UpdateReason, Snapshot: snapshotJSON, OldValues: oldRow})

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed)
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

		// Full-detail SELECT — mirrors GetClosureDetail but in list form.
		// Includes every field from closure_request + all JOINed stage data so
		// the caller has everything needed to pre-fill UpdateClosure without a
		// separate detail call.
		dataSQL := fmt.Sprintf(`
			SELECT
			  -- ── closure_request (columns that actually exist) ────────────
			  cr.closure_request_id,
			  cr.fd_id,
			  COALESCE(cr.booking_id,'')            AS booking_id,
			  COALESCE(cr.confirmation_id,'')       AS confirmation_id,
			  COALESCE(cr.entity_id,'')             AS entity_id,
			  COALESCE(cr.entity_name,'')           AS entity_name,
			  cr.closure_type,
			  cr.closure_status,
			  COALESCE(cr.initiation_date::text,'')          AS initiation_date,
			  COALESCE(cr.effective_closure_date::text,'')   AS effective_closure_date,
			  COALESCE(cr.maturity_date::text,'')            AS maturity_date,
			  COALESCE(cr.principal_amount,0)                AS principal_amount,
			  COALESCE(cr.accrued_interest,0)                AS accrued_interest,
			  COALESCE(cr.tds_deducted,0)                    AS tds_deducted,
			  COALESCE(cr.penalty_amount,0)                  AS penalty_amount,
			  COALESCE(cr.net_payout_amount,0)               AS net_payout_amount,
			  COALESCE(cr.rollover_amount,0)                 AS rollover_amount,
			  COALESCE(cr.rollover_tenor_days,0)             AS rollover_tenor_days,
			  COALESCE(cr.settlement_account_id,'')          AS settlement_account_id,
			  COALESCE(cr.maturity_instructions,'')          AS maturity_instructions,
			  COALESCE(cr.closure_reason,'')                 AS closure_reason,
			  COALESCE(cr.closure_notes,'')                  AS closure_notes,
			  COALESCE(cr.variance_remark,'')                AS variance_remark,
			  COALESCE(cr.approval_instance_id,'')           AS approval_instance_id,
			  COALESCE(cr.submitted_by,'')                   AS submitted_by,
			  COALESCE(cr.submitted_by_email,'')             AS submitted_by_email,
			  COALESCE(cr.accounting_posted,false)           AS accounting_posted,
			  COALESCE(cr.created_at::text,'')               AS created_at,
			  COALESCE(cr.updated_at::text,'')               AS updated_at,
			  -- ── fd_master (original FD at time of booking/activation) ────
			  COALESCE(m.bank_fd_ref_no,'')          AS fd_reference_number,
			  COALESCE(m.bank_id,'')                 AS bank_id,
			  COALESCE(m.bank_name,'')               AS bank_name,
			  COALESCE(m.interest_rate,0)            AS fd_interest_rate,
			  COALESCE(m.interest_type_code,'')      AS interest_type_code,
			  COALESCE(m.tenure_days,0)              AS tenure_days,
			  COALESCE(m.start_date::text,'')        AS fd_start_date,
			  COALESCE(m.maturity_date::text,'')     AS fd_maturity_date,
			  COALESCE(m.principal_amount,0)         AS original_principal,
			  COALESCE(m.fd_status,'')               AS fd_status,
			  COALESCE(m.maturity_instructions,'')   AS fd_maturity_instructions,
			  COALESCE(m.day_count_code,'')          AS day_count_code,
			  COALESCE(m.frequency_id,'')            AS frequency_id,
			  COALESCE(m.auto_renewal,false)         AS auto_renewal,
			  -- ── fd_booking_request (at time of initiation) ───────────────
			  COALESCE(b.entity_id,'')               AS booking_entity_id,
			  COALESCE(b.entity_name,'')             AS booking_entity_name,
			  COALESCE(b.source_account_id,'')       AS source_account_id,
			  COALESCE(b.tds_plan_id,'')             AS tds_plan_id,
			  COALESCE(b.frequency_id,'')            AS booking_frequency_id,
			  COALESCE(b.bank_config_id,'')          AS bank_config_id,
			  -- ── fd_confirmation (as confirmed by bank) ───────────────────
			  COALESCE(c.bank_fd_ref_no,'')          AS bank_fd_reference,
			  COALESCE(c.confirmation_status,'')     AS confirmation_status,
			  COALESCE(c.confirmation_received_date::text,'') AS confirmation_date,
			  -- ── accrual ledger totals (live) ─────────────────────────────
			  COALESCE(al.total_interest_accrued,0)  AS total_interest_accrued,
			  COALESCE(al.total_tds_accrued,0)       AS total_tds_accrued,
			  -- ── penalty structure (for the bank) ─────────────────────────
			  COALESCE(ps.penalty_value,0)           AS penalty_structure_value,
			  COALESCE(ps.penalty_type,'NONE')       AS penalty_structure_type,
			  COALESCE(ps.no_interest_if_withdrawn_before,0) AS no_interest_min_days,
			  -- ── maturity payout sub-table (LATERAL, latest row) ──────────
			  COALESCE(mp.payout_date::text,'')      AS mp_payout_date,
			  COALESCE(mp.principal_returned,0)      AS mp_principal_returned,
			  COALESCE(mp.gross_interest,0)          AS mp_gross_interest,
			  COALESCE(mp.tds_deducted,0)            AS mp_tds_deducted,
			  COALESCE(mp.net_payout,0)              AS mp_net_payout,
			  COALESCE(mp.settlement_account_id,'')  AS mp_settlement_account_id,
			  COALESCE(mp.payment_status,'')         AS mp_payment_status,
			  -- ── premature closure sub-table (LATERAL, latest row) ────────
			  COALESCE(prem.premature_closure_date::text,'') AS prem_closure_date,
			  COALESCE(prem.days_held,0)             AS prem_days_held,
			  COALESCE(prem.contracted_rate,0)       AS prem_contracted_rate,
			  COALESCE(prem.applicable_rate,0)       AS prem_applicable_rate,
			  COALESCE(prem.gross_interest_earned,0) AS prem_gross_interest_earned,
			  COALESCE(prem.penalty_rate,0)          AS prem_penalty_rate,
			  COALESCE(prem.penalty_amount,0)        AS prem_penalty_amount,
			  COALESCE(prem.no_interest_flag,false)  AS prem_no_interest_flag,
			  COALESCE(prem.tds_on_interest,0)       AS prem_tds_on_interest,
			  COALESCE(prem.net_payout,0)            AS prem_net_payout,
			  -- ── rollover sub-table (LATERAL, latest row) ─────────────────
			  COALESCE(rol.rollover_date::text,'')   AS rol_rollover_date,
			  COALESCE(rol.rollover_amount,0)        AS rol_rollover_amount,
			  COALESCE(rol.rollover_principal,0)     AS rol_rollover_principal,
			  COALESCE(rol.interest_credited,0)      AS rol_interest_credited,
			  COALESCE(rol.tds_deducted,0)           AS rol_tds_deducted,
			  COALESCE(rol.new_tenor_days,0)         AS rol_new_tenor_days,
			  COALESCE(rol.rollover_type,'')         AS rol_rollover_type,
			  -- ── approval engine status ───────────────────────────────────
			  COALESCE(ai.status,'')                 AS approval_status,
			  COALESCE(ai.submitted_at::text,'')     AS approval_submitted_at,
			  -- ── latest audit processing status ───────────────────────────
			  COALESCE((
			    SELECT aud.processing_status
			    FROM investment.fd_audit_closure_request aud
			    WHERE aud.closure_request_id = cr.closure_request_id
			    ORDER BY aud.created_at DESC LIMIT 1
			  ),'') AS latest_processing_status,
			  -- ── computed maturity amount ─────────────────────────────────
			  ROUND((
			    COALESCE(cr.principal_amount,0)
			    + COALESCE(al.total_interest_accrued,0)
			    - COALESCE(al.total_tds_accrued,0)
			  )::numeric, 4) AS expected_maturity_amount
			FROM investment.fd_closure_request cr
			LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = cr.booking_id
			LEFT JOIN investment.fd_confirmation c ON c.confirmation_id = cr.confirmation_id
			LEFT JOIN uam.approval_instance ai ON ai.instance_id = cr.approval_instance_id
			LEFT JOIN LATERAL (
			  SELECT SUM(COALESCE(period_interest_accrued,0)) AS total_interest_accrued,
			         SUM(COALESCE(tds_deducted_in_period,0))  AS total_tds_accrued
			  FROM investment.fd_accrual_ledger
			  WHERE fd_id = cr.fd_id AND COALESCE(is_deleted,false) = false
			) al ON true
			LEFT JOIN LATERAL (
			  SELECT penalty_value, penalty_type, no_interest_if_withdrawn_before
			  FROM investment.fd_penalty_structure_master ps
			  WHERE ps.bank_code = m.bank_id AND COALESCE(ps.is_deleted,false) = false
			  ORDER BY ps.penalty_value DESC LIMIT 1
			) ps ON true
			LEFT JOIN LATERAL (
			  SELECT payout_date, principal_returned, gross_interest, tds_deducted, net_payout,
			         settlement_account_id, payment_status
			  FROM investment.fd_closure_maturity_payout
			  WHERE closure_request_id = cr.closure_request_id
			  ORDER BY created_at DESC LIMIT 1
			) mp ON true
			LEFT JOIN LATERAL (
			  SELECT premature_closure_date, days_held, contracted_rate, applicable_rate,
			         gross_interest_earned, penalty_rate, penalty_amount, no_interest_flag,
			         tds_on_interest, net_payout
			  FROM investment.fd_closure_premature
			  WHERE closure_request_id = cr.closure_request_id
			  ORDER BY created_at DESC LIMIT 1
			) prem ON true
			LEFT JOIN LATERAL (
			  SELECT rollover_date, rollover_amount, rollover_principal, interest_credited,
			         tds_deducted, new_tenor_days, rollover_type
			  FROM investment.fd_closure_rollover
			  WHERE closure_request_id = cr.closure_request_id
			  ORDER BY created_at DESC LIMIT 1
			) rol ON true
			WHERE %s
			ORDER BY cr.created_at DESC
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
		if records == nil {
			records = []map[string]interface{}{}
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"records": records, "total": totalCount, "page": req.Page, "page_size": req.PageSize,
		})
		api.LogInfo("[FDClosure] GetAllClosureRequests: %d/%d by=%s", len(records), totalCount, userEmail)
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureRequestIDRequired)
			return
		}
		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()

		// Explicit typed scan — no SELECT cr.* / rows.Values() generic pgx mapping.
		var (
			closureRequestID, fdID, closureType, closureStatus                   string
			bookingID, confirmationID, entityID, entityName                      string
			initiationDate, effectiveClosureDate, maturityDate                   string
			settlementAccountID, settlementBankName, closureReason, closureNotes string
			varianceRemark, approvalInstanceID, submittedBy, submittedByEmail    string
			rolloverFDID                                                         string
			createdAt, updatedAt                                                 string
			principalAmount, accruedInterest, tdsDeducted, penaltyAmount         float64
			netPayoutAmount, rolloverAmount, rolloverInterestRate                float64
			rolloverTenorDays                                                    int
			hasUnresolvedVariance                                                bool
			// enriched from fd_master
			fdReferenceNumber, bankName, bankIDEnriched, interestTypeCode string
			fdStartDate, fdMaturityDate, fdStatus, fdMaturityInstructions string
			originalPrincipal, fdInterestRate                             float64
			tenureDays                                                    int
			// enriched from booking / confirmation / approval engine
			bookingEntityName, bookingEntityID        string
			bankFDReference, confirmationStatus       string
			approvalEngineStatus, approvalSubmittedAt string
		)

		err := pool.QueryRow(ctx, `
			SELECT
			  cr.closure_request_id,
			  cr.fd_id,
			  COALESCE(cr.booking_id,''),
			  COALESCE(cr.confirmation_id,''),
			  COALESCE(cr.entity_id,''),
			  COALESCE(cr.entity_name,''),
			  cr.closure_type,
			  cr.closure_status,
			  COALESCE(cr.initiation_date::text,''),
			  COALESCE(cr.effective_closure_date::text,''),
			  COALESCE(cr.maturity_date::text,''),
			  COALESCE(cr.principal_amount,0),
			  COALESCE(cr.accrued_interest,0),
			  COALESCE(cr.tds_deducted,0),
			  COALESCE(cr.penalty_amount,0),
			  COALESCE(cr.net_payout_amount,0),
			  COALESCE(cr.rollover_amount,0),
			  COALESCE(cr.rollover_tenor_days,0),
			  COALESCE(cr.rollover_interest_rate,0),
			  COALESCE(cr.rollover_fd_id,''),
			  COALESCE(cr.settlement_account_id,''),
			  COALESCE(cr.settlement_bank_name,''),
			  COALESCE(cr.closure_reason,''),
			  COALESCE(cr.closure_notes,''),
			  COALESCE(cr.variance_remark,''),
			  COALESCE(cr.approval_instance_id,''),
			  COALESCE(cr.has_unresolved_variance,false),
			  COALESCE(cr.submitted_by,''),
			  COALESCE(cr.submitted_by_email,''),
			  COALESCE(cr.created_at::text,''),
			  COALESCE(cr.updated_at::text,''),
			  -- enriched: fd_master
			  COALESCE(m.bank_fd_ref_no,''),
			  COALESCE(m.bank_name,''),
			  COALESCE(m.bank_id,''),
			  COALESCE(m.interest_rate,0),
			  COALESCE(m.interest_type_code,''),
			  COALESCE(m.tenure_days,0),
			  COALESCE(m.start_date::text,''),
			  COALESCE(m.maturity_date::text,''),
			  COALESCE(m.principal_amount,0),
			  COALESCE(m.fd_status,''),
			  COALESCE(m.maturity_instructions,''),
			  -- enriched: booking
			  COALESCE(b.entity_name,''),
			  COALESCE(b.entity_id,''),
			  -- enriched: confirmation
			  COALESCE(c.bank_fd_ref_no,''),
			  COALESCE(c.confirmation_status,''),
			  -- enriched: approval engine
			  COALESCE(ai.status,''),
			  COALESCE(ai.submitted_at::text,'')
			FROM investment.fd_closure_request cr
			LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = cr.booking_id
			LEFT JOIN investment.fd_confirmation c ON c.confirmation_id = cr.confirmation_id
			LEFT JOIN uam.approval_instance ai ON ai.instance_id = cr.approval_instance_id
			WHERE cr.closure_request_id=$1 AND cr.is_deleted=false LIMIT 1`, req.ClosureRequestID,
		).Scan(
			&closureRequestID, &fdID, &bookingID, &confirmationID, &entityID, &entityName,
			&closureType, &closureStatus,
			&initiationDate, &effectiveClosureDate, &maturityDate,
			&principalAmount, &accruedInterest, &tdsDeducted, &penaltyAmount, &netPayoutAmount,
			&rolloverAmount, &rolloverTenorDays, &rolloverInterestRate, &rolloverFDID,
			&settlementAccountID, &settlementBankName, &closureReason, &closureNotes,
			&varianceRemark, &approvalInstanceID, &hasUnresolvedVariance,
			&submittedBy, &submittedByEmail, &createdAt, &updatedAt,
			&fdReferenceNumber, &bankName, &bankIDEnriched,
			&fdInterestRate, &interestTypeCode, &tenureDays, &fdStartDate, &fdMaturityDate,
			&originalPrincipal, &fdStatus, &fdMaturityInstructions,
			&bookingEntityName, &bookingEntityID,
			&bankFDReference, &confirmationStatus,
			&approvalEngineStatus, &approvalSubmittedAt,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrClosureRequestNotFound)
			} else {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to load closure detail")
			}
			return
		}

		closureRow := map[string]interface{}{
			"closure_request_id":      closureRequestID,
			"fd_id":                   fdID,
			"booking_id":              bookingID,
			"confirmation_id":         confirmationID,
			"entity_id":               entityID,
			"entity_name":             entityName,
			"closure_type":            closureType,
			"closure_status":          closureStatus,
			"initiation_date":         initiationDate,
			"effective_closure_date":  effectiveClosureDate,
			"maturity_date":           maturityDate,
			"principal_amount":        principalAmount,
			"accrued_interest":        accruedInterest,
			"tds_deducted":            tdsDeducted,
			"penalty_amount":          penaltyAmount,
			"net_payout_amount":       netPayoutAmount,
			"rollover_amount":         rolloverAmount,
			"rollover_tenor_days":     rolloverTenorDays,
			"rollover_interest_rate":  rolloverInterestRate,
			"rollover_fd_id":          rolloverFDID,
			"settlement_account_id":   settlementAccountID,
			"settlement_bank_name":    settlementBankName,
			"closure_reason":          closureReason,
			"closure_notes":           closureNotes,
			"variance_remark":         varianceRemark,
			"approval_instance_id":    approvalInstanceID,
			"has_unresolved_variance": hasUnresolvedVariance,
			"submitted_by":            submittedBy,
			"submitted_by_email":      submittedByEmail,
			"created_at":              createdAt,
			"updated_at":              updatedAt,
			// enriched fields
			"fd_reference_number":    fdReferenceNumber,
			"bank_name":              bankName,
			"bank_id":                bankIDEnriched,
			"interest_rate":          fdInterestRate,
			"interest_type_code":     interestTypeCode,
			"tenure_days":            tenureDays,
			"start_date":             fdStartDate,
			"fd_maturity_date":       fdMaturityDate,
			"original_principal":     originalPrincipal,
			"fd_status":              fdStatus,
			"maturity_instructions":  fdMaturityInstructions,
			"booking_entity_name":    bookingEntityName,
			"booking_entity_id":      bookingEntityID,
			"bank_fd_reference":      bankFDReference,
			"confirmation_status":    confirmationStatus,
			"approval_engine_status": approvalEngineStatus,
			"approval_submitted_at":  approvalSubmittedAt,
		}

		fetchSub := func(sql string) []map[string]interface{} {
			var out []map[string]interface{}
			sr, qErr := pool.Query(ctx, sql, req.ClosureRequestID)
			if qErr != nil {
				return out
			}
			defer sr.Close()
			for sr.Next() {
				v, _ := sr.Values()
				cd := sr.FieldDescriptions()
				row := make(map[string]interface{}, len(cd))
				for i, c := range cd {
					row[string(c.Name)] = v[i]
				}
				out = append(out, row)
			}
			return out
		}

		var auditTrail []map[string]interface{}
		ar, qErr := pool.Query(ctx, `
			SELECT
			  audit_id, action_type, processing_status,
			  performed_by_email, action_reason, snapshot_data, created_at,
			  -- individual old-value columns
			  old_closure_status,
			  old_bank_id, old_bank_name, old_bank_fd_ref_no,
			  old_entity_id, old_entity_name,
			  old_principal_amount, old_interest_rate, old_interest_type_code,
			  old_tenure_days, old_start_date, old_maturity_date,
			  old_accrued_interest, old_tds_deducted,
			  old_penalty_rate, old_penalty_amount, old_no_interest_flag, old_days_held,
			  old_contracted_rate, old_applicable_rate,
			  old_net_payout_amount, old_settlement_account_id, old_maturity_instructions,
			  old_principal_returned, old_gross_interest,
			  old_rollover_type, old_rollover_amount, old_rollover_principal,
			  old_interest_credited, old_rollover_tenor_days, old_rollover_interest_rate,
			  old_partial_withdrawal, old_new_maturity_date,
			  old_closure_reason, old_closure_notes, old_variance_remark
			FROM investment.fd_audit_closure_request
			WHERE closure_request_id=$1
			ORDER BY created_at ASC`, req.ClosureRequestID)
		if qErr == nil {
			defer ar.Close()
			for ar.Next() {
				v, _ := ar.Values()
				cd := ar.FieldDescriptions()
				row := make(map[string]interface{}, len(cd))
				for i, c := range cd {
					row[string(c.Name)] = v[i]
				}
				auditTrail = append(auditTrail, row)
			}
		}

		// ── Approval workflow (rich — same shape as booking/confirmation detail) ─
		viewerUserID := req.UserID
		var approvalWorkflow interface{}
		if approvalInstanceID != "" {
			richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pool, approvalInstanceID, viewerUserID)
			if richErr != nil {
				api.LogError("[FDClosure] GetRichInstanceDetail failed instance=%s closure=%s: %v", approvalInstanceID, req.ClosureRequestID, richErr)
			} else {
				approvalWorkflow = richDetail
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"closure_request":   closureRow,
			"maturity_payouts":  fetchSub("SELECT * FROM investment.fd_closure_maturity_payout WHERE closure_request_id=$1"),
			"premature_detail":  fetchSub("SELECT * FROM investment.fd_closure_premature WHERE closure_request_id=$1"),
			"rollover_detail":   fetchSub("SELECT * FROM investment.fd_closure_rollover WHERE closure_request_id=$1"),
			"audit_trail":       auditTrail,
			"variances":         fetchVariances(ctx, pool, req.ClosureRequestID),
			"approval_workflow": approvalWorkflow,
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
			).Scan(&fdID, &closureType, &closureStatus, &principalAmt, &accruedInt, &tdsAmt, &penaltyAmt, &netPayout, &entityID, &entityName, &hasUnresolved)
			if loadErr != nil {
				errors = append(errors, crID+": not found")
				continue
			}
			if closureStatus != "PENDING_APPROVAL" {
				errors = append(errors, crID+": status is "+closureStatus)
				continue
			}
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
					Comment:    firstNonEmpty(req.Comment, "Bulk approved FD closure"),
				}); err != nil {
					errors = append(errors, crID+": "+err.Error())
					continue
				}
				engineActed++ // action recorded; count regardless of whether this is the final eye
				var instStatus string
				_ = pool.QueryRow(ctx, `SELECT i.status FROM uam.approval_instance i JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id WHERE ie.instance_eye_id=$1`, instanceEyeID).Scan(&instStatus)
				if instStatus == "APPROVED" {
					if postErr := postClosureJournals(ctx, PostClosureJournalsParams{Pool: pool, ClosureRequestID: crID, FDID: fdID, ClosureType: closureType, EntityID: entityID, EntityName: entityName, PrincipalAmt: principalAmt, AccruedInterest: accruedInt, TDSAmt: tdsAmt, PenaltyAmt: penaltyAmt, NetPayout: netPayout, ApprovedBy: req.UserID, ApprovedByEmail: userEmail}); postErr != nil {
						api.LogError("[FDClosure] postClosureJournals failed for %s: %v", crID, postErr)
						errors = append(errors, crID+": journal posting failed: "+postErr.Error())
					}
				}
			} else {
				var anyInstance int
				_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM uam.approval_instance WHERE record_id=$1 AND module_code='FIXED_DEPOSIT' AND status='PENDING'`, crID).Scan(&anyInstance)
				if anyInstance > 0 {
					errors = append(errors, crID+": not your turn in approval sequence")
					continue
				}
				// Idempotency guard: re-check status under the same connection before posting journals
				// to avoid double-posting if another goroutine or request raced us.
				var currentStatusCheck string
				_ = pool.QueryRow(ctx, `SELECT closure_status FROM investment.fd_closure_request WHERE closure_request_id=$1 AND is_deleted=false FOR UPDATE`, crID).Scan(&currentStatusCheck)
				if currentStatusCheck != "PENDING_APPROVAL" {
					errors = append(errors, crID+": status changed to "+currentStatusCheck+" — skipped")
					continue
				}
				if postErr := postClosureJournals(ctx, PostClosureJournalsParams{Pool: pool, ClosureRequestID: crID, FDID: fdID, ClosureType: closureType, EntityID: entityID, EntityName: entityName, PrincipalAmt: principalAmt, AccruedInterest: accruedInt, TDSAmt: tdsAmt, PenaltyAmt: penaltyAmt, NetPayout: netPayout, ApprovedBy: req.UserID, ApprovedByEmail: userEmail}); postErr != nil {
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
				errors = append(errors, crID+": not found")
				continue
			}
			if curStatus != "PENDING_APPROVAL" {
				errors = append(errors, crID+": status is "+curStatus)
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
				// Engine instance found — RecordAction will trigger finalizeRecord which:
				//   1. Updates processing_status → REJECTED on the audit table
				//   2. Fires postFinalizeHook which sets closure_status='REJECTED' + clears fd_master
				if err := approvalengine.RecordAction(ctx, pool, approvalengine.ActionRequest{
					InstanceEyeID: instanceEyeID, ActorUserID: req.UserID, ActorEmail: userEmail,
					ActionType: approvalengine.ActionRejected,
					Comment:    firstNonEmpty(req.Comment, "Bulk rejected FD closure"),
				}); err != nil {
					errors = append(errors, crID+": "+err.Error())
					continue
				}
				acted++
			} else {
				// No engine instance — handle rejection directly with audit row.
				tx, txErr := pool.Begin(ctx)
				if txErr != nil {
					errors = append(errors, crID+": tx begin failed")
					continue
				}

				_, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_request SET closure_status='REJECTED',rejected_at=NOW(),rejected_by=$1,rejection_reason=$2,updated_by=$1,updated_at=NOW() WHERE closure_request_id=$3`, userEmail, req.Comment, crID)
				_, _ = tx.Exec(ctx, `UPDATE investment.fd_master SET closure_request_id=NULL,updated_at=NOW() WHERE closure_request_id=$1`, crID)

				snapshotJSON, _ := json.Marshal(map[string]interface{}{"closure_status": "PENDING_APPROVAL", "rejected_by": userEmail, "rejection_reason": req.Comment})
				_ = insertClosureAudit(ctx, ClosureAuditParams{Exec: tx, ClosureRequestID: crID, PerformedBy: req.UserID, PerformedByEmail: userEmail, ActionType: "REJECT", ProcessingStatus: "REJECTED", Reason: req.Comment, Snapshot: snapshotJSON, OldValues: map[string]interface{}{"closure_status": "PENDING_APPROVAL"}})

				if cerr := tx.Commit(ctx); cerr != nil {
					_ = tx.Rollback(ctx)
					errors = append(errors, crID+constants.ErrCommitFailed)
					continue
				}
				acted++
			}
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrClosureRequestIDRequired)
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
				api.RespondWithError(w, http.StatusNotFound, constants.ErrClosureRequestNotFound)
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
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		_, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_request SET is_deleted=true,deleted_by=$1,deleted_at=NOW(),closure_status='CANCELLED',updated_by=$1,updated_at=NOW() WHERE closure_request_id=$2`, userEmail, req.ClosureRequestID)
		_, _ = tx.Exec(ctx, `UPDATE investment.fd_master SET closure_request_id=NULL,updated_at=NOW() WHERE closure_request_id=$1`, req.ClosureRequestID)

		snapshotJSON, _ := json.Marshal(map[string]interface{}{"closure_status": curStatus, "fd_id": fdID, "delete_reason": req.Reason})
		_ = insertClosureAudit(ctx, ClosureAuditParams{Exec: tx, ClosureRequestID: req.ClosureRequestID, PerformedBy: req.UserID, PerformedByEmail: userEmail, ActionType: "DELETE", ProcessingStatus: "CANCELLED", Reason: req.Reason, Snapshot: snapshotJSON, OldValues: map[string]interface{}{"closure_status": curStatus}})

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed)
			return
		}

		api.LogInfo("[FDClosure] DeleteClosureRequest: closureID=%s by=%s", req.ClosureRequestID, userEmail)
		api.RespondWithPayload(w, true, "Closure request deleted", map[string]interface{}{
			"closure_request_id": req.ClosureRequestID, "deleted_by": userEmail,
		})
	}
}

// Internal helpers

// insertClosureAudit writes one audit row.  It stores both a full snapshot_data JSONB
// blob (for quick reads) AND individual old_* columns for every field so the audit
// table is queryable without unpacking JSON.
//
// oldValues keys must match the column names in fd_audit_closure_request prefixed with "old_".
// Unrecognised keys are silently ignored.
// ClosureAuditParams holds all inputs for insertClosureAudit.
type ClosureAuditParams struct {
	Exec             dbExec
	ClosureRequestID string
	PerformedBy      string
	PerformedByEmail string
	ActionType       string
	ProcessingStatus string
	Reason           string
	Snapshot         []byte
	OldValues        map[string]interface{}
}

func insertClosureAudit(ctx context.Context, p ClosureAuditParams) error {
	exec := p.Exec
	closureRequestID := p.ClosureRequestID
	performedBy := p.PerformedBy
	performedByEmail := p.PerformedByEmail
	actionType := p.ActionType
	processingStatus := p.ProcessingStatus
	reason := p.Reason
	snapshot := p.Snapshot
	oldValues := p.OldValues
	if closureRequestID == "" {
		return nil
	}
	var snapshotArg interface{}
	if len(snapshot) > 0 {
		snapshotArg = snapshot
	}

	// Helper to extract a value from the oldValues map or return nil.
	ov := func(k string) interface{} {
		if oldValues == nil {
			return nil
		}
		v, ok := oldValues[k]
		if !ok || v == nil {
			return nil
		}
		if s, ok := v.(string); ok && s == "" {
			return nil
		}
		return v
	}

	_, err := exec.Exec(ctx, `
		INSERT INTO investment.fd_audit_closure_request (
		  closure_request_id, action_type, processing_status,
		  performed_by, performed_by_email, action_reason,
		  snapshot_data,
		  -- individual old-value columns (one per tracked field)
		  old_closure_status,
		  old_bank_id, old_bank_name, old_bank_fd_ref_no,
		  old_entity_id, old_entity_name,
		  old_principal_amount, old_interest_rate, old_interest_type_code,
		  old_tenure_days, old_start_date, old_maturity_date,
		  old_accrued_interest, old_tds_deducted,
		  old_penalty_rate, old_penalty_amount, old_no_interest_flag, old_days_held,
		  old_contracted_rate, old_applicable_rate,
		  old_net_payout_amount, old_settlement_account_id, old_maturity_instructions,
		  old_principal_returned, old_gross_interest,
		  old_rollover_type, old_rollover_amount, old_rollover_principal,
		  old_interest_credited, old_rollover_tenor_days, old_rollover_interest_rate,
		  old_partial_withdrawal, old_new_maturity_date,
		  old_closure_reason, old_closure_notes, old_variance_remark,
		  created_at
		) VALUES (
		  $1,$2,$3,$4,$5,$6,$7,
		  $8,$9,$10,$11,$12,$13,
		  $14,$15,$16,$17,$18,$19,
		  $20,$21,$22,$23,$24,$25,
		  $26,$27,$28,$29,$30,$31,
		  $32,$33,$34,$35,$36,$37,
		  $38,$39,$40,$41,$42,$43,
		  NOW()
		)`,
		closureRequestID, actionType, processingStatus,
		nullStrOrNil(performedBy), nullStrOrNil(performedByEmail),
		nullStrOrNil(reason), snapshotArg,
		// individual old values
		ov("closure_status"),
		ov("bank_id"), ov("bank_name"), ov("bank_fd_ref_no"),
		ov("entity_id"), ov("entity_name"),
		ov("principal_amount"), ov("interest_rate"), ov("interest_type_code"),
		ov("tenure_days"), ov("start_date"), ov("maturity_date"),
		ov("accrued_interest"), ov("tds_deducted"),
		ov("penalty_rate"), ov("penalty_amount"), ov("no_interest_flag"), ov("days_held"),
		ov("contracted_rate"), ov("applicable_rate"),
		ov("net_payout_amount"), ov("settlement_account_id"), ov("maturity_instructions"),
		ov("principal_returned"), ov("gross_interest"),
		ov("rollover_type"), ov("rollover_amount"), ov("rollover_principal"),
		ov("interest_credited"), ov("rollover_tenor_days"), ov("rollover_interest_rate"),
		ov("partial_withdrawal"), ov("new_maturity_date"),
		ov("closure_reason"), ov("closure_notes"), ov("variance_remark"),
	)
	return err
}

// PostClosureJournalsParams holds all inputs for postClosureJournals.
type PostClosureJournalsParams struct {
	Pool             *pgxpool.Pool
	ClosureRequestID string
	FDID             string
	ClosureType      string
	EntityID         string
	EntityName       string
	PrincipalAmt     float64
	AccruedInterest  float64
	TDSAmt           float64
	PenaltyAmt       float64
	NetPayout        float64
	ApprovedBy       string
	ApprovedByEmail  string
}

func postClosureJournals(ctx context.Context, p PostClosureJournalsParams) error {
	pool := p.Pool
	closureRequestID := p.ClosureRequestID
	fdID := p.FDID
	closureType := p.ClosureType
	entityID := p.EntityID
	entityName := p.EntityName
	principalAmt := p.PrincipalAmt
	accruedInterest := p.AccruedInterest
	tdsAmt := p.TDSAmt
	penaltyAmt := p.PenaltyAmt
	netPayout := p.NetPayout
	approvedBy := p.ApprovedBy
	approvedByEmail := p.ApprovedByEmail
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
	if bankAccountName == "" {
		bankAccountName = firstNonEmpty(bankName, "Settlement Account")
	}
	if bankAccountNumber == "" {
		bankAccountNumber = firstNonEmpty(settleAcctID, "SETTLEMENT")
	}

	activitySubtype := "FD_MATURITY_PAYOUT"
	switch closureType {
	case "PREMATURE":
		activitySubtype = "FD_PREMATURE_CLOSURE"
	case "ROLLOVER":
		activitySubtype = "FD_ROLLOVER"
	}

	var activityID string
	err = tx.QueryRow(ctx, `INSERT INTO investment.accounting_activity (activity_type,activity_subtype,effective_date,accounting_period,data_source,status) VALUES ('FIXED_DEPOSIT',$1,CURRENT_DATE,$2,'FD_CLOSURE','APPROVED') RETURNING activity_id`, activitySubtype, accountingPeriod).Scan(&activityID)
	if err != nil {
		return fmt.Errorf("postClosureJournals create activity: %w", err)
	}

	var entryID string
	description := fmt.Sprintf("FD %s closure — %s", closureType, fdID)

	// ── Journal double-entry structure ──────────────────────────────────────
	//
	// The FD Investment account (ASSET) held the principal.  At closure we:
	//   DR  FD Investment          principal        (remove the asset)
	//   CR  Settlement Account     netPayout        (cash paid out)
	//   CR  Interest Income        netInterest      (interest earned, net of TDS)
	//   CR  TDS Payable            tdsAmt           (TDS liability created)
	//   CR  Penalty Income / DR    penaltyAmt       (for PREMATURE: reduce interest CR)
	//
	// Balance check:
	//   DR total = principal
	//   CR total = netPayout + netInterest + tdsAmt
	//            = netPayout + (accruedInterest - tdsAmt) + tdsAmt
	//            = netPayout + accruedInterest
	//            = (principal + accruedInterest - tdsAmt - penaltyAmt) + accruedInterest  [MATURITY/PREMATURE]
	//   → need separate penalty DR to keep it balanced for PREMATURE.
	//
	// Correct balanced structure:
	//   DR FD Investment     = principal
	//   DR Penalty Expense   = penaltyAmt          (PREMATURE only)
	//   CR Settlement Acct   = netPayout            (= principal + interest - TDS - penalty)
	//   CR Interest Income   = accruedInterest - tdsAmt  (net interest recognised)
	//   CR TDS Payable       = tdsAmt
	//
	// Verify: DR = principal + penalty
	//         CR = netPayout + (interest - TDS) + TDS
	//            = (principal + interest - TDS - penalty) + interest - TDS + TDS
	//            = principal + 2×interest - TDS - penalty   ← NOT balanced without further thought.
	//
	// Simplest correct model (used below):
	//   DR FD Investment     = principal                       always
	//   DR Penalty Expense   = penaltyAmt                      if > 0
	//   CR Settlement Acct   = netPayout                       always
	//   CR Interest Income   = accruedInterest                 if > 0
	//   CR TDS Payable       = tdsAmt                          if > 0
	//   CR Penalty Income    = penaltyAmt                      if > 0  (offsets DR above, net = 0 on P&L)
	// → DR = principal + penalty  ;  CR = netPayout + interest + tdsAmt + penalty
	//      = (principal+interest-TDS-penalty) + interest + TDS + penalty  ← still wrong.
	//
	// The standard accounting treatment for FD closure is:
	//   DR FD Investment (principal)               ... reduce asset
	//   DR TDS Payable   (tdsAmt)                  ... TDS was already a liability — settle it
	//   DR Penalty Expense (penaltyAmt)             ... recognise penalty cost
	//   CR Interest Income (accruedInterest)        ... recognise full interest earned
	//   CR Bank/Settlement (netPayout)              ... actual cash paid
	//
	// Balance: DR = principal + tds + penalty
	//          CR = interest + netPayout
	//             = interest + (principal + interest - tds - penalty)
	//             = principal + 2×interest - tds - penalty  ← still doesn't balance.
	//
	// Root cause: accrued TDS was already booked as a liability when accrued.
	// At closure the TDS liability is SETTLED (DR TDS Payable, CR Bank/Govt).
	// The "CR Interest Income" was already recognised in prior accrual runs.
	// At closure we only need to record the CASH MOVEMENT and close the FD asset:
	//
	//   DR FD Investment   = principal              (close asset)
	//   CR Bank Account    = netPayout              (cash out)
	//   CR Int Receivable  = accruedInterest-tds    (realise net interest; if not yet received)
	//   DR TDS Payable     = tdsAmt                 (settle TDS liability)
	//   DR Penalty Expense = penaltyAmt             (PREMATURE cost)
	//
	// For simplicity this system books interest income at closure (single-entry accrual model).
	// Balanced closure entry (interest-at-closure model):
	//   Line 1  DR FD Investment   principal
	//   Line 2  DR Penalty Expense penaltyAmt          (PREMATURE only)
	//   Line 3  CR Bank Account    netPayout
	//   Line 4  CR Interest Income accruedInterest      (gross)
	//   Line 5  DR TDS Payable     tdsAmt               (TDS on interest)
	//
	// DR = principal + penalty + tds
	// CR = netPayout + interest
	//    = (principal + interest - tds - penalty) + interest
	//    = principal + 2×interest - tds - penalty  ← STILL not balanced.
	//
	// The only balanced approach: treat "Interest Income" as NET (after TDS & penalty).
	//   Line 1  DR FD Investment   principal
	//   Line 2  CR Bank Account    netPayout           = principal + interest - tds - penalty
	//   Line 3  CR Interest Income netInterest         = interest - tds - penalty  (net recognised)
	//   DR = principal  ;  CR = netPayout + netInterest
	//      = (principal+interest-tds-penalty) + (interest-tds-penalty) ← wrong.
	//
	// ── Correct minimal balanced entry ────────────────────────────────────
	// Treat this as a CASH SETTLEMENT entry only (accruals already posted):
	//   Line 1  DR FD Investment   principal          (close principal asset)
	//   Line 2  DR TDS Payable     tdsAmt             (extinguish TDS liability)
	//   Line 3  DR Penalty Expense penaltyAmt         (PREMATURE cost)
	//   Line 4  CR Bank Account    netPayout          (cash to entity)
	//   Line 5  CR Accrued Int     accruedInterest    (reverse accrual receivable)
	//
	// DR = principal + tds + penalty
	// CR = netPayout + interest
	//    = (principal + interest - tds - penalty) + interest = principal + 2×interest - tds - penalty
	//    ← still unbalanced if interest ≠ 0.
	//
	// ── Final resolution ──────────────────────────────────────────────────
	// The bank pays: principal + interest - TDS - penalty  (= netPayout).
	// The only cash-balanced single journal is:
	//   DR FD Investment    principal
	//   CR Bank Account     netPayout
	//   CR Interest Income  accruedInterest - tdsAmt - penaltyAmt  (net P&L gain)
	//
	// DR = principal
	// CR = netPayout + (interest - tds - penalty)
	//    = (principal + interest - tds - penalty) + (interest - tds - penalty)  ← still off.
	//
	// CORRECT:
	//   DR = principal
	//   CR = netPayout + netInterest  where netInterest = interest - tds - penalty
	//   netPayout = principal + interest - tds - penalty
	//   CR = (principal + interest - tds - penalty) + (interest - tds - penalty)
	//      ≠ principal unless interest=tds+penalty.
	//
	// The ONLY truly balanced closure entry:
	//   DR FD Investment  principal
	//   DR TDS Expense    tdsAmt          (TDS is a COST to entity, not a liability here)
	//   DR Penalty Exp    penaltyAmt
	//   CR Bank Account   netPayout       = principal + interest - tds - penalty
	//   CR Interest Inc   interest        (gross)
	//
	// DR = principal + tds + penalty
	// CR = netPayout + interest
	//    = principal + interest - tds - penalty + interest
	//    = principal + 2*interest - tds - penalty  ← still wrong.
	//
	// THE REAL CORRECT ENTRY (standard FD accounting):
	//   At initiation:  DR FD Investment  /  CR Bank
	//   At closure:     DR Bank (netPayout+tds+penalty)  /  CR FD Investment (principal)
	//                              + CR Interest Income (interest)
	//                   DR TDS Expense / CR Tax Payable (tds)
	//                   DR Penalty Exp / CR Bank (penalty already deducted)
	// Simplified single entry:
	//   DR Bank           netPayout      (cash received)
	//   DR TDS Receivable tdsAmt         (amount withheld, owed back)
	//   DR Penalty Exp    penaltyAmt     (cost)
	//   CR FD Investment  principal      (close asset)
	//   CR Interest Inc   accruedInterest (income)
	//
	// DR = netPayout + tds + penalty = principal + interest
	// CR = principal + interest  ✓  BALANCED
	netInterest := roundToFour(accruedInterest - tdsAmt - penaltyAmt)
	if netInterest < 0 {
		netInterest = 0
	}
	totalDebitAmt := roundToFour(netPayout + tdsAmt + penaltyAmt) // = principal + accruedInterest
	totalCreditAmt := roundToFour(principalAmt + accruedInterest)

	err = tx.QueryRow(ctx, `
		INSERT INTO investment.accounting_journal_entry (
		  activity_id,entity_id,entity_name,entry_date,accounting_period,entry_type,description,
		  total_debit,total_credit,status,fd_id,closure_request_id,is_reversal,created_by
		) VALUES ($1,$2,$3,CURRENT_DATE,$4,'CLOSURE',$5,$6,$7,'APPROVED',$8,$9,false,$10)
		RETURNING entry_id`,
		activityID, nullStrOrNil(entityID), nullStrOrNil(entityName),
		accountingPeriod, description, totalDebitAmt, totalCreditAmt, fdID, closureRequestID, approvedByEmail,
	).Scan(&entryID)
	if err != nil {
		return fmt.Errorf("postClosureJournals insert journal entry: %w", err)
	}

	// Balanced journal lines:
	//   DR Bank/Settlement  netPayout      (cash received from bank)
	//   DR TDS Receivable   tdsAmt         (TDS withheld — recoverable)
	//   DR Penalty Expense  penaltyAmt     (premature withdrawal cost)
	//   CR FD Investment    principal      (close investment asset)
	//   CR Interest Income  interest       (gross income recognised)
	// Total DR = netPayout + tds + penalty = principal + interest  ✓
	// Total CR = principal + interest                              ✓
	type jLine struct {
		num                         int
		acctNum, acctName, acctType string
		debitAmt, creditAmt         float64
		narration                   string
	}
	lineNum := 1
	lines := []jLine{}
	lines = append(lines, jLine{lineNum, bankAccountNumber, bankAccountName, "ASSET",
		roundToFour(netPayout), 0, "Cash received on FD closure — " + closureType})
	lineNum++
	if tdsAmt > 0 {
		lines = append(lines, jLine{lineNum, "TDS-RECEIVABLE", "TDS Receivable", "ASSET",
			roundToFour(tdsAmt), 0, "TDS withheld at source — recoverable"})
		lineNum++
	}
	if penaltyAmt > 0 {
		lines = append(lines, jLine{lineNum, "PENALTY-EXP", "Premature Withdrawal Penalty", "EXPENSE",
			roundToFour(penaltyAmt), 0, "Penalty for premature withdrawal"})
		lineNum++
	}
	lines = append(lines, jLine{lineNum, "FD-INVEST-" + fdID, "FD Investment — " + fdID, "ASSET",
		0, roundToFour(principalAmt), "Close FD investment asset — " + closureType})
	lineNum++
	if accruedInterest > 0 {
		lines = append(lines, jLine{lineNum, "FD-INT-INC-" + fdID, "Interest Income — FD", "INCOME",
			0, roundToFour(accruedInterest), "Gross interest income recognised on closure"})
		lineNum++ //nolint:ineffassign
	}

	for _, l := range lines {
		_, err = tx.Exec(ctx, `INSERT INTO investment.accounting_journal_entry_line (entry_id,line_number,account_number,account_name,account_type,debit_amount,credit_amount,narration,fd_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			entryID, l.num, l.acctNum, l.acctName, l.acctType, l.debitAmt, l.creditAmt, l.narration, fdID)
		if err != nil {
			return fmt.Errorf("postClosureJournals insert journal line %d: %w", l.num, err)
		}
	}

	newFDStatus := "MATURED"
	switch closureType {
	case "PREMATURE":
		newFDStatus = "PREMATURELY_CLOSED"
	case "ROLLOVER":
		newFDStatus = "ROLLED_OVER"
	}

	_, err = tx.Exec(ctx, `UPDATE investment.fd_master SET fd_status=$1,closed_at=NOW(),closed_by=$2,accounting_posted=true,closure_request_id=$3,updated_by=$4,updated_at=NOW() WHERE fd_id=$5`, newFDStatus, approvedByEmail, closureRequestID, approvedByEmail, fdID)
	if err != nil {
		return fmt.Errorf("postClosureJournals update fd_master: %w", err)
	}

	_, err = tx.Exec(ctx, `UPDATE investment.fd_closure_request SET closure_status='POSTED',approved_by=$1,approved_by_email=$2,approved_at=NOW(),accounting_posted=true,accounting_posted_at=NOW(),updated_by=$3,updated_at=NOW() WHERE closure_request_id=$4`, approvedBy, approvedByEmail, approvedByEmail, closureRequestID)
	if err != nil {
		return fmt.Errorf("postClosureJournals update closure request: %w", err)
	}

	switch closureType {
	case "MATURITY":
		_, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_maturity_payout SET journal_entry_id=$1,payment_status='COMPLETED' WHERE closure_request_id=$2`, entryID, closureRequestID)
	case "PREMATURE":
		_, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_premature SET journal_entry_id=$1 WHERE closure_request_id=$2`, entryID, closureRequestID)
	case "ROLLOVER":
		_, _ = tx.Exec(ctx, `UPDATE investment.fd_closure_rollover SET journal_entry_id=$1 WHERE closure_request_id=$2`, entryID, closureRequestID)

		// ── ROLLOVER: create the new fd_booking_request only.
		//
		// We do NOT create fd_master here. The normal pipeline is:
		//   fd_booking_request (SENT_TO_BANK)
		//       → ops captures confirmation  (CaptureConfirmation)
		//       → confirmation approved      (BulkApproveConfirmation)
		//       → ActivateFD inserts fd_master + cashflows
		//
		// The 6pm fd_rollover_reconcile_worker watches for confirmed-but-
		// not-yet-activated rollover bookings and auto-activates them so
		// nothing is orphaned if ops misses the manual step.

		// 1. Fetch source FD details to mirror onto the new booking.
		var (
			srcBankID, srcBankName, srcEntityID, srcEntityName       string
			srcFrequencyID, srcTDSPlanID, srcDayCountCode            string
			srcBankConfigID, srcSourceAccountID, srcInterestTypeCode string
			srcInterestRate                                          float64
			srcTenureDays                                            int
		)
		_ = tx.QueryRow(ctx, `
			SELECT
			  COALESCE(m.bank_id,''),   COALESCE(m.bank_name,''),
			  COALESCE(b.entity_id,''), COALESCE(b.entity_name,''),
			  COALESCE(b.frequency_id,''), COALESCE(b.tds_plan_id,''),
			  COALESCE(m.day_count_code,''), COALESCE(b.bank_config_id,''),
			  COALESCE(b.source_account_id,''), COALESCE(m.interest_type_code,''),
			  COALESCE(m.interest_rate,0), COALESCE(m.tenure_days,0)
			FROM investment.fd_master m
			LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
			WHERE m.fd_id = $1 LIMIT 1`, fdID,
		).Scan(
			&srcBankID, &srcBankName,
			&srcEntityID, &srcEntityName,
			&srcFrequencyID, &srcTDSPlanID,
			&srcDayCountCode, &srcBankConfigID,
			&srcSourceAccountID, &srcInterestTypeCode,
			&srcInterestRate, &srcTenureDays,
		)
		if entityID != "" {
			srcEntityID = entityID
		}
		if entityName != "" {
			srcEntityName = entityName
		}

		// 2. Read the confirmed rollover amounts persisted on fd_closure_rollover.
		var rolloverAmt, rolloverInterestRate float64
		var newTenorDays int
		var rvNewBankID, rvNewBankName, rvNewAccountID string
		_ = tx.QueryRow(ctx, `
			SELECT COALESCE(rollover_amount,0), COALESCE(new_tenor_days,0), COALESCE(rollover_interest_rate,0),
			       COALESCE(new_bank_id,''), COALESCE(new_bank_name,''), COALESCE(new_account_id,'')
			FROM investment.fd_closure_rollover WHERE closure_request_id=$1 LIMIT 1`, closureRequestID,
		).Scan(&rolloverAmt, &newTenorDays, &rolloverInterestRate, &rvNewBankID, &rvNewBankName, &rvNewAccountID)
		if rolloverAmt <= 0 {
			rolloverAmt = roundToFour(principalAmt + accruedInterest - tdsAmt)
		}
		if newTenorDays <= 0 {
			newTenorDays = srcTenureDays
		}
		if rolloverInterestRate <= 0 {
			rolloverInterestRate = srcInterestRate
		}
		// Prefer new bank details if specified; fall back to source FD's bank
		if rvNewBankID != "" {
			srcBankID = rvNewBankID
		}
		if rvNewBankName != "" {
			srcBankName = rvNewBankName
		}
		if rvNewAccountID != "" {
			srcSourceAccountID = rvNewAccountID
		}

		// 3. INSERT fd_booking_request with status SENT_TO_BANK.
		//    The bank already has the instruction; we just need ops to
		//    capture the confirmation slip and activate.
		var newBookingID string
		bookingErr := tx.QueryRow(ctx, `
			INSERT INTO investment.fd_booking_request (
			  entity_id, entity_name,
			  bank_id, bank_name,
			  bank_config_id, source_account_id,
			  principal_amount, interest_rate, tenure_days,
			  interest_type_code, frequency_id, day_count_code, tds_plan_id,
			  booking_status, booking_remarks, source_closure_request_id, created_by
			) VALUES (
			  $1,$2,$3,$4,
			  NULLIF($5,''), NULLIF($6,''),
			  $7,$8,$9,
			  NULLIF($10,''), NULLIF($11,''), NULLIF($12,''), NULLIF($13,''),
			  'SENT_TO_BANK',
			  $14, $15, $16
			) RETURNING booking_id`,
			srcEntityID, srcEntityName,
			srcBankID, srcBankName,
			srcBankConfigID, srcSourceAccountID,
			rolloverAmt, rolloverInterestRate, newTenorDays,
			srcInterestTypeCode, srcFrequencyID, srcDayCountCode, srcTDSPlanID,
			fmt.Sprintf("Rollover booking — source closure %s source FD %s", closureRequestID, fdID),
			closureRequestID, approvedByEmail,
		).Scan(&newBookingID)
		if bookingErr != nil {
			// Non-fatal: journals are posted; log and let the reconcile worker retry.
			api.LogError("[FDClosure] ROLLOVER: failed to create booking for closure %s: %v", closureRequestID, bookingErr)
		}

		// 4. Link new booking back onto fd_closure_rollover so the
		//    reconcile worker and the UI can find it.
		if newBookingID != "" {
			if _, linkErr := tx.Exec(ctx,
				`UPDATE investment.fd_closure_rollover SET new_fd_booking_id=$1 WHERE closure_request_id=$2`,
				newBookingID, closureRequestID); linkErr != nil {
				api.LogError("[FDClosure] ROLLOVER: failed to link booking_id=%s on fd_closure_rollover for closure=%s: %v",
					newBookingID, closureRequestID, linkErr)
			} else {
				api.LogInfo("[FDClosure] ROLLOVER: new booking=%s linked on fd_closure_rollover for closure=%s (fd_master will be created after confirmation)", newBookingID, closureRequestID)
			}
		}
	}

	postSnap := map[string]interface{}{
		"journal_entry_id": entryID, "activity_id": activityID, "new_fd_status": newFDStatus,
		"accounting_period": accountingPeriod, "total_debit": totalDebitAmt, "total_credit": totalCreditAmt, "approved_by_email": approvedByEmail,
	}
	snapshotJSON, _ := json.Marshal(postSnap)
	_ = insertClosureAudit(ctx, ClosureAuditParams{Exec: tx, ClosureRequestID: closureRequestID, PerformedBy: approvedBy, PerformedByEmail: approvedByEmail, ActionType: "POST_ACCOUNTING", ProcessingStatus: "POSTED", Reason: "Journals posted on approval", Snapshot: snapshotJSON, OldValues: postSnap})

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postClosureJournals commit: %w", err)
	}

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
	FDID        string `json:"fd_id"`
	ClosureType string `json:"closure_type"` // MATURITY | PREMATURE | ROLLOVER
	BankID      string `json:"bank_id"`
	BankName    string `json:"bank_name"`
	BankFDRefNo string `json:"bank_fd_ref_no"`
	EntityID    string `json:"entity_id"`
	EntityName  string `json:"entity_name"`

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
	RolloverType         string  `json:"rollover_type"` // FULL | PARTIAL | PRINCIPAL_ONLY
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
			dbPrincipal, dbInterestRate       float64
			dbMaturityDate, dbStartDate       time.Time
			dbTenureDays                      int
			dbFDStatus, dbInterestTypeCode    string
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

		fmtDate := func(t time.Time) string { return t.Format(constants.DateFormat) }
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
		var dbPenaltyStructureID, dbPenaltyType, dbPenaltyCalcMethod string
		if req.ClosureType == "PREMATURE" {
			_ = pool.QueryRow(ctx,
				`SELECT COALESCE((CURRENT_DATE - start_date)::int,0) FROM investment.fd_master WHERE fd_id=$1`,
				req.FDID).Scan(&dbDaysHeld)
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(ps.penalty_structure_id,''),
				       COALESCE(ps.penalty_value,0),
				       COALESCE(ps.no_interest_if_withdrawn_before,0) > 0,
				       COALESCE(ps.no_interest_if_withdrawn_before,0),
				       COALESCE(ps.penalty_type,''),
				       COALESCE(ps.calculation_method,'')
				FROM investment.fd_penalty_structure_master ps
				WHERE ps.bank_code = $1
				  AND COALESCE(ps.is_deleted, false) = false
				  AND (
				        (ps.min_holding_days IS NOT NULL AND $2 >= ps.min_holding_days
				         AND (ps.max_holding_days IS NULL OR $2 < ps.max_holding_days))
				        OR (ps.min_holding_days IS NULL AND ps.max_holding_days IS NULL)
				      )
				ORDER BY
				  (CASE WHEN ps.min_holding_days IS NOT NULL THEN 0 ELSE 1 END),
				  ps.penalty_value DESC
				LIMIT 1`, dbBankID, dbDaysHeld,
			).Scan(&dbPenaltyStructureID, &dbPenaltyRate, &dbNoInterestFlag, &dbMinDays, &dbPenaltyType, &dbPenaltyCalcMethod)
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
			switch req.RolloverType {
			case "PRINCIPAL_ONLY":
				// Only principal is reinvested; net interest is paid out separately.
				dbNetPayout = roundToFour(dbPrincipal)
			case "PARTIAL":
				// partial_withdrawal is taken as cash; remainder (principal+net_interest) rolls over.
				dbNetPayout = roundToFour(dbPrincipal + dbAccruedInterest - dbTDSDeducted - req.PartialWithdrawal)
			default: // FULL or empty — entire principal + net interest reinvested
				dbNetPayout = roundToFour(dbPrincipal + dbAccruedInterest - dbTDSDeducted)
			}
		default:
			dbNetPayout = roundToFour(dbPrincipal + dbAccruedInterest - dbTDSDeducted)
		}

		// ── 5. Build variance rules ────────────────────────────────────────
		runID := varianceengine.NewRunID()
		entityID := firstNonEmpty(req.EntityID, dbEntityID)

		// Common rules — apply to ALL closure types
		rules := []varianceengine.Rule{
			{FieldName: "bank_id", VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbBankID, ActualValue: req.BankID, Priority: varianceengine.PriorityHigh, SystemComment: "Bank does not match FD master"},
			{FieldName: "bank_name", VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbBankName, ActualValue: req.BankName, Priority: varianceengine.PriorityLow},
			{FieldName: "bank_fd_ref_no", VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbBankFDRef, ActualValue: req.BankFDRefNo, Priority: varianceengine.PriorityMedium},
			{FieldName: "entity_id", VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbEntityID, ActualValue: req.EntityID, Priority: varianceengine.PriorityHigh, SystemComment: "Entity mismatch"},
			{FieldName: "entity_name", VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbEntityName, ActualValue: req.EntityName, Priority: varianceengine.PriorityLow},
			{FieldName: "principal_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbPrincipal), ActualValue: fmtF(req.PrincipalAmount), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
			{FieldName: "interest_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: fmtF(dbInterestRate), ActualValue: fmtF(req.InterestRate), Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
			{FieldName: "interest_type_code", VarianceType: varianceengine.TypeIdentity, ExpectedValue: dbInterestTypeCode, ActualValue: req.InterestTypeCode, Priority: varianceengine.PriorityMedium},
			{FieldName: "tenure_days", VarianceType: varianceengine.TypeDays, ExpectedValue: fmtI(dbTenureDays), ActualValue: fmtI(req.TenureDays), Priority: varianceengine.PriorityHigh, Tolerance: 0},
			{FieldName: "start_date", VarianceType: varianceengine.TypeDate, ExpectedValue: fmtDate(dbStartDate), ActualValue: req.StartDate, Priority: varianceengine.PriorityHigh, Tolerance: 0},
			{FieldName: "maturity_date", VarianceType: varianceengine.TypeDate, ExpectedValue: fmtDate(dbMaturityDate), ActualValue: req.MaturityDate, Priority: varianceengine.PriorityHigh, Tolerance: 0},
			{FieldName: "accrued_interest", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbAccruedInterest), ActualValue: fmtF(req.AccruedInterest), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
			{FieldName: "tds_deducted", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbTDSDeducted), ActualValue: fmtF(req.TDSDeducted), Priority: varianceengine.PriorityMedium, Tolerance: 0.5},
			{FieldName: "net_payout_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbNetPayout), ActualValue: fmtF(req.NetPayoutAmount), Priority: varianceengine.PriorityHigh, Tolerance: 1.0},
		}

		// MATURITY-specific rules
		if req.ClosureType == "MATURITY" || req.ClosureType == "" {
			rules = append(rules,
				varianceengine.Rule{FieldName: "principal_returned", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbPrincipal), ActualValue: fmtF(req.PrincipalReturned), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
				varianceengine.Rule{FieldName: "gross_interest", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbAccruedInterest), ActualValue: fmtF(req.GrossInterest), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
			)
		}

		// PREMATURE-specific rules
		if req.ClosureType == "PREMATURE" {
			rules = append(rules,
				varianceengine.Rule{FieldName: "days_held", VarianceType: varianceengine.TypeDays, ExpectedValue: fmtI(dbDaysHeld), ActualValue: fmtI(req.DaysHeld), Priority: varianceengine.PriorityHigh, Tolerance: 0},
				varianceengine.Rule{FieldName: "contracted_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: fmtF(dbInterestRate), ActualValue: fmtF(req.ContractedRate), Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
				varianceengine.Rule{FieldName: "applicable_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: fmtF(dbApplicableRate), ActualValue: fmtF(req.ApplicableRate), Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
				varianceengine.Rule{FieldName: "gross_interest_earned", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbAccruedInterest), ActualValue: fmtF(req.GrossInterestEarned), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
				varianceengine.Rule{FieldName: "penalty_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: fmtF(dbPenaltyRate), ActualValue: fmtF(req.PenaltyRate), Priority: varianceengine.PriorityHigh, Tolerance: 0.001},
				varianceengine.Rule{FieldName: "penalty_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbPenaltyAmount), ActualValue: fmtF(req.PenaltyAmount), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
				varianceengine.Rule{FieldName: "no_interest_flag", VarianceType: varianceengine.TypeIdentity, ExpectedValue: strconv.FormatBool(dbNoInterestFlag), ActualValue: strconv.FormatBool(req.NoInterestFlag), Priority: varianceengine.PriorityMedium},
				varianceengine.Rule{FieldName: "tds_on_interest", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbTDSDeducted), ActualValue: fmtF(req.TDSOnInterest), Priority: varianceengine.PriorityMedium, Tolerance: 0.5},
			)
		}

		// ROLLOVER-specific rules
		if req.ClosureType == "ROLLOVER" {
			rules = append(rules,
				varianceengine.Rule{FieldName: "rollover_principal", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbPrincipal), ActualValue: fmtF(req.RolloverPrincipal), Priority: varianceengine.PriorityHigh, Tolerance: 0.01},
				varianceengine.Rule{FieldName: "rollover_amount", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbNetPayout), ActualValue: fmtF(req.RolloverAmount), Priority: varianceengine.PriorityHigh, Tolerance: 1.0},
				varianceengine.Rule{FieldName: "interest_credited", VarianceType: varianceengine.TypeAmount, ExpectedValue: fmtF(dbAccruedInterest), ActualValue: fmtF(req.InterestCredited), Priority: varianceengine.PriorityMedium, Tolerance: 1.0},
				varianceengine.Rule{FieldName: "rollover_interest_rate", VarianceType: varianceengine.TypeRate, ExpectedValue: fmtF(dbInterestRate), ActualValue: fmtF(req.RolloverInterestRate), Priority: varianceengine.PriorityHigh, Tolerance: 0.001, SystemComment: "Rollover rate vs current contracted rate"},
				varianceengine.Rule{FieldName: "partial_withdrawal", VarianceType: varianceengine.TypeAmount, ExpectedValue: "0.0000", ActualValue: fmtF(req.PartialWithdrawal), Priority: varianceengine.PriorityHigh, Tolerance: 0.01, SystemComment: "Non-zero only for PARTIAL rollover"},
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
				"bank_id":        dbBankID,
				"bank_name":      dbBankName,
				"bank_fd_ref_no": dbBankFDRef,
				"entity_id":      dbEntityID,
				"entity_name":    dbEntityName,
				// Core FD
				"principal_amount":   dbPrincipal,
				"interest_rate":      dbInterestRate,
				"interest_type_code": dbInterestTypeCode,
				"tenure_days":        dbTenureDays,
				"start_date":         fmtDate(dbStartDate),
				"maturity_date":      fmtDate(dbMaturityDate),
				"fd_status":          dbFDStatus,
				// Accrual
				"accrued_interest": roundToFour(dbAccruedInterest),
				"tds_deducted":     roundToFour(dbTDSDeducted),
				// Net payout
				"net_payout_amount": dbNetPayout,
				// Maturity-specific system values
				"principal_returned": dbPrincipal,
				"gross_interest":     roundToFour(dbAccruedInterest),
				// Premature-specific system values
				"days_held":             dbDaysHeld,
				"contracted_rate":       dbInterestRate,
				"applicable_rate":       dbApplicableRate,
				"penalty_rate":          dbPenaltyRate,
				"penalty_amount":        dbPenaltyAmount,
				"no_interest_flag":      dbNoInterestFlag,
				"min_days_for_interest": dbMinDays,
				"penalty_structure_id":  dbPenaltyStructureID,
				"penalty_type":          dbPenaltyType,
				"penalty_value":         dbPenaltyRate,
				"calculation_method":    dbPenaltyCalcMethod,
				// Rollover-specific system values
				"rollover_principal": dbPrincipal,
				"rollover_amount":    dbNetPayout,
				"interest_credited":  roundToFour(dbAccruedInterest),
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
	for _, txType := range []string{"FD_CLOSURE_MATURITY", "FD_CLOSURE_PREMATURE", "FD_CLOSURE_ROLLOVER", "FD_CLOSURE_AUTO_RENEWAL"} {
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
				if postErr := postClosureJournals(ctx, PostClosureJournalsParams{Pool: pool, ClosureRequestID: recordID, FDID: fdID, ClosureType: closureType, EntityID: entityID, EntityName: entityName, PrincipalAmt: principalAmt, AccruedInterest: accruedInterest, TDSAmt: tdsAmt, PenaltyAmt: penaltyAmt, NetPayout: netPayout, ApprovedBy: approvedBy, ApprovedByEmail: actorEmail}); postErr != nil {
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
