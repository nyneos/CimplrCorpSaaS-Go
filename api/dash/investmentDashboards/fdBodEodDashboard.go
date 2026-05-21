// Package investmentdashboards — FD BOD / EOD Control Dashboard
//
// POST /dash/investment/fd/bod-eod-dashboard
//
// Returns the Begin-of-Day or End-of-Day control room payload for FD operations:
//
// BOD sub-computations:
//   - maturities_today     — FDs maturing today (count, total principal+interest)
//   - maturities_3days     — FDs maturing in the next 1-3 days
//   - confirmations_due    — booking requests awaiting bank confirmation (SLA aging)
//   - accrual_scheduled    — accrual runs scheduled today
//   - expected_interest    — cashflow INTEREST_RECEIPT rows expected today
//   - action_list          — booking requests + closure requests as action items
//   - bank_contacts        — confirmed variance / pending confirmation FDs to contact
//   - sla_breach_yesterday — items that breached SLA in the prior day
//
// EOD sub-computations:
//   - bookings_today       — FD bookings created today
//   - confirmations_today  — confirmations captured today
//   - receipts_today       — interest receipts ingested + matched today
//   - exceptions_today     — exceptions opened vs closed today
//   - posting_today        — cashflow GL postings success/failure today
//   - accrual_run_latest   — most recent accrual run status
//   - eod_checklist        — driven by real data states
//   - handover_notes       — latest accrual exception notes as context
//
// All sub-computations run concurrently via sync.WaitGroup.
package investmentdashboards

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── request type ─────────────────────────────────────────────────────────────

type fdBodEodDashRequest struct {
	UserID    string `json:"user_id"`
	EntityID  string `json:"entity_id"`
	Currency  string `json:"currency"`
	Mode      string `json:"mode"`       // "BOD" | "EOD" — default "BOD"
	Period    string `json:"period"`     // "Today" | "This Week" | "This Month"
	StartDate string `json:"start_date"` // for custom range
	EndDate   string `json:"end_date"`
}

// ─── handler ──────────────────────────────────────────────────────────────────

// GetFDBodEodDashboard returns the full BOD/EOD Control Room payload.
func GetFDBodEodDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req fdBodEodDashRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Currency == "" {
			req.Currency = "INR"
		}
		if req.Mode == "" {
			req.Mode = "BOD"
		}

		now := time.Now().UTC()
		periodBounds := resolveFDPeriodBounds(req.Period, req.StartDate, req.EndDate, now)
		today := now.Format(constants.DateFormat)
		threeDaysOut := now.AddDate(0, 0, 3).Format(constants.DateFormat)
		ctx := r.Context()
		entityFilter := req.EntityID

		type subResult struct {
			data interface{}
			err  error
		}
		results := make(map[string]subResult, 16)
		var mu sync.Mutex
		var wg sync.WaitGroup

		run := func(key string, fn func(context.Context) (interface{}, error)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, e := fn(ctx)
				mu.Lock()
				results[key] = subResult{d, e}
				mu.Unlock()
			}()
		}

		// ── BOD: 1. FDs maturing today ────────────────────────────────────────
		run("maturities_today", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  m.fd_id,
				  m.bank_name,
				  m.entity_name,
				  COALESCE(m.entity_id,'') AS entity_id,
				  COALESCE(m.principal_amount,0) AS principal_amount,
				  COALESCE(m.interest_rate,0) AS interest_rate,
				  COALESCE(m.fd_status,'') AS fd_status,
				  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(cf.interest_accrued,0) AS expected_interest,
				  COALESCE(NULLIF(cimplr_ci.closure_type, ''), NULLIF(cr.closure_type, ''), m.maturity_instructions, '') AS closure_instruction,
				  COALESCE(NULLIF(cimplr_cc.closure_status, ''), NULLIF(cimplr_ci.closure_status, ''), NULLIF(cr.closure_status, ''), '') AS closure_status
				FROM investment.fd_master m
				LEFT JOIN investment.fd_cashflow_schedule cf
				  ON cf.fd_id = m.fd_id AND cf.event_type = 'MATURITY' AND cf.is_deleted=false
				`+sqlLatestCimplrInitiate+`
				`+sqlLatestCimplrConfirm+`
				LEFT JOIN LATERAL (
				  SELECT cr.closure_type, cr.closure_status
				  FROM investment.fd_closure_request cr
				  WHERE cr.fd_id = m.fd_id AND COALESCE(cr.is_deleted, false) = false
				  ORDER BY cr.created_at DESC
				  LIMIT 1
				) cr ON true
				WHERE m.is_deleted=false
				  AND m.maturity_date = $1::date
				  AND ($2::text='' OR m.entity_id=$2)
				ORDER BY m.principal_amount DESC NULLS LAST
				LIMIT 200`, today, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] maturities_today query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_principal": 0, "total_interest": 0}, nil
			}
			defer rows.Close()

			type matRow struct {
				FDID               string  `json:"fd_id"`
				Bank               string  `json:"bank"`
				Entity             string  `json:"entity"`
				EntityID           string  `json:"entity_id"`
				Principal          float64 `json:"principal"`
				InterestRate       float64 `json:"interest_rate"`
				FDStatus           string  `json:"fd_status"`
				MaturityDate       string  `json:"maturity_date"`
				ExpectedInterest   float64 `json:"expected_interest"`
				ClosureInstruction string  `json:"closure_instruction"`
				ClosureStatus      string  `json:"closure_status"`
				DaysToMaturity     int     `json:"days_to_maturity"`
			}
			out := []matRow{}
			totalPrincipal, totalInterest := 0.0, 0.0
			for rows.Next() {
				var mr matRow
				if err2 := rows.Scan(
					&mr.FDID, &mr.Bank, &mr.Entity, &mr.EntityID,
					&mr.Principal, &mr.InterestRate, &mr.FDStatus, &mr.MaturityDate,
					&mr.ExpectedInterest, &mr.ClosureInstruction, &mr.ClosureStatus,
				); err2 != nil {
					api.LogError("[BodEodDash] maturities_today scan error: %v", err2)
					continue
				}
				mr.Principal = fdRound(mr.Principal, 2)
				mr.ExpectedInterest = fdRound(mr.ExpectedInterest, 2)
				mr.InterestRate = fdRound(mr.InterestRate, 4)
				totalPrincipal += mr.Principal
				totalInterest += mr.ExpectedInterest
				out = append(out, mr)
			}
			return map[string]interface{}{
				"rows":            out,
				"count":           len(out),
				"total_principal": fdRound(totalPrincipal, 2),
				"total_interest":  fdRound(totalInterest, 2),
			}, nil
		})

		// ── BOD: 2. FDs maturing in next 1-3 days ────────────────────────────
		run("maturities_3days", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  m.fd_id,
				  m.bank_name,
				  m.entity_name,
				  COALESCE(m.principal_amount,0) AS principal_amount,
				  COALESCE(m.interest_rate,0) AS interest_rate,
				  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  (m.maturity_date - CURRENT_DATE)::int AS days_to_maturity,
				  COALESCE(m.fd_status,'') AS fd_status
				FROM investment.fd_master m
				WHERE m.is_deleted=false
				  AND m.maturity_date > $1::date
				  AND m.maturity_date <= $2::date
				  AND ($3::text='' OR m.entity_id=$3)
				ORDER BY m.maturity_date ASC, m.principal_amount DESC NULLS LAST
				LIMIT 200`, today, threeDaysOut, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] maturities_3days query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_principal": 0}, nil
			}
			defer rows.Close()

			type mat3Row struct {
				FDID           string  `json:"fd_id"`
				Bank           string  `json:"bank"`
				Entity         string  `json:"entity"`
				Principal      float64 `json:"principal"`
				InterestRate   float64 `json:"interest_rate"`
				MaturityDate   string  `json:"maturity_date"`
				DaysToMaturity int     `json:"days_to_maturity"`
				FDStatus       string  `json:"fd_status"`
			}
			out := []mat3Row{}
			totalPrincipal := 0.0
			for rows.Next() {
				var mr mat3Row
				if err2 := rows.Scan(&mr.FDID, &mr.Bank, &mr.Entity, &mr.Principal,
					&mr.InterestRate, &mr.MaturityDate, &mr.DaysToMaturity, &mr.FDStatus); err2 != nil {
					api.LogError("[BodEodDash] maturities_3days scan error: %v", err2)
					continue
				}
				mr.Principal = fdRound(mr.Principal, 2)
				mr.InterestRate = fdRound(mr.InterestRate, 4)
				totalPrincipal += mr.Principal
				out = append(out, mr)
			}
			return map[string]interface{}{
				"rows":            out,
				"count":           len(out),
				"total_principal": fdRound(totalPrincipal, 2),
			}, nil
		})

		// ── BOD: 3. Confirmations pending (SLA aging) ─────────────────────────
		run("confirmations_due", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  b.booking_id,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0) AS principal,
				  b.booking_status,
				  COALESCE(EXTRACT(DAY FROM NOW()-b.created_at)::int, 0) AS aging_days,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD'),'') AS booking_date
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('SENT_TO_BANK','APPROVAL_PENDING')
				  AND ($1::text='' OR b.entity_id=$1)
				ORDER BY b.created_at ASC
				LIMIT 100`, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] confirmations_due query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "overdue_count": 0}, nil
			}
			defer rows.Close()

			type confRow struct {
				BookingID   string  `json:"booking_id"`
				Entity      string  `json:"entity"`
				Bank        string  `json:"bank"`
				Principal   float64 `json:"principal"`
				Status      string  `json:"status"`
				AgingDays   int     `json:"aging_days"`
				BookingDate string  `json:"booking_date"`
				SLAStatus   string  `json:"sla_status"`
			}
			out := []confRow{}
			overdue := 0
			for rows.Next() {
				var cr confRow
				if err2 := rows.Scan(&cr.BookingID, &cr.Entity, &cr.Bank, &cr.Principal,
					&cr.Status, &cr.AgingDays, &cr.BookingDate); err2 != nil {
					api.LogError("[BodEodDash] confirmations_due scan error: %v", err2)
					continue
				}
				cr.Principal = fdRound(cr.Principal, 2)
				switch {
				case cr.AgingDays >= 3:
					cr.SLAStatus = "Breached"
					overdue++
				case cr.AgingDays >= 2:
					cr.SLAStatus = "Warning"
				default:
					cr.SLAStatus = "OK"
				}
				out = append(out, cr)
			}
			return map[string]interface{}{
				"rows":          out,
				"count":         len(out),
				"overdue_count": overdue,
			}, nil
		})

		// ── BOD: 4. Accrual runs scheduled / active today ─────────────────────
		run("accrual_scheduled", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  run_id,
				  run_type,
				  run_mode,
				  run_status,
				  entity_name,
				  COALESCE(TO_CHAR(accrual_period_start,'YYYY-MM-DD'),'') AS period_start,
				  COALESCE(TO_CHAR(accrual_period_end,'YYYY-MM-DD'),'') AS period_end,
				  fds_in_scope,
				  fds_calculated,
				  fds_failed,
				  COALESCE(total_interest_accrued,0) AS total_interest_accrued,
				  posting_status,
				  COALESCE(TO_CHAR(run_date,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS run_date
				FROM investment.fd_accrual_run
				WHERE is_deleted=false
				  AND DATE(run_date) = $1::date
				  AND ($2::text='' OR entity_id=$2)
				ORDER BY run_date DESC
				LIMIT 20`, today, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] accrual_scheduled query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type runRow struct {
				RunID                string  `json:"run_id"`
				RunType              string  `json:"run_type"`
				RunMode              string  `json:"run_mode"`
				RunStatus            string  `json:"run_status"`
				Entity               string  `json:"entity"`
				PeriodStart          string  `json:"period_start"`
				PeriodEnd            string  `json:"period_end"`
				FDsInScope           int64   `json:"fds_in_scope"`
				FDsCalculated        int64   `json:"fds_calculated"`
				FDsFailed            int64   `json:"fds_failed"`
				TotalInterestAccrued float64 `json:"total_interest_accrued"`
				PostingStatus        string  `json:"posting_status"`
				RunDate              string  `json:"run_date"`
			}
			out := []runRow{}
			for rows.Next() {
				var rr runRow
				if err2 := rows.Scan(
					&rr.RunID, &rr.RunType, &rr.RunMode, &rr.RunStatus, &rr.Entity,
					&rr.PeriodStart, &rr.PeriodEnd,
					&rr.FDsInScope, &rr.FDsCalculated, &rr.FDsFailed,
					&rr.TotalInterestAccrued, &rr.PostingStatus, &rr.RunDate,
				); err2 != nil {
					api.LogError("[BodEodDash] accrual_scheduled scan error: %v", err2)
					continue
				}
				rr.TotalInterestAccrued = fdRound(rr.TotalInterestAccrued, 2)
				out = append(out, rr)
			}
			return out, nil
		})

		// ── BOD: 5. Expected interest credits today (cashflow schedule) ───────
		run("expected_interest", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  cf.cashflow_id,
				  m.fd_id,
				  m.bank_name,
				  m.entity_name,
				  COALESCE(cf.net_cash_flow, cf.interest_accrued, 0) AS expected_amount,
				  COALESCE(cf.tds_amount,0) AS tds_amount,
				  cf.event_type,
				  cf.posting_status,
				  cf.bank_confirmed,
				  cf.receipt_cleared
				FROM investment.fd_cashflow_schedule cf
				JOIN investment.fd_master m ON m.fd_id = cf.fd_id AND m.is_deleted=false
				WHERE cf.is_deleted=false
				  AND cf.event_date = $1::date
				  AND cf.event_type IN ('INTEREST_RECEIPT','MATURITY')
				  AND ($2::text='' OR m.entity_id=$2)
				ORDER BY cf.net_cash_flow DESC NULLS LAST
				LIMIT 200`, today, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] expected_interest query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_expected": 0}, nil
			}
			defer rows.Close()

			type cfRow struct {
				CashflowID     string  `json:"cashflow_id"`
				FDID           string  `json:"fd_id"`
				Bank           string  `json:"bank"`
				Entity         string  `json:"entity"`
				ExpectedAmount float64 `json:"expected_amount"`
				TDSAmount      float64 `json:"tds_amount"`
				EventType      string  `json:"event_type"`
				PostingStatus  string  `json:"posting_status"`
				BankConfirmed  bool    `json:"bank_confirmed"`
				ReceiptCleared bool    `json:"receipt_cleared"`
			}
			out := []cfRow{}
			totalExpected := 0.0
			for rows.Next() {
				var cr cfRow
				if err2 := rows.Scan(
					&cr.CashflowID, &cr.FDID, &cr.Bank, &cr.Entity,
					&cr.ExpectedAmount, &cr.TDSAmount, &cr.EventType,
					&cr.PostingStatus, &cr.BankConfirmed, &cr.ReceiptCleared,
				); err2 != nil {
					api.LogError("[BodEodDash] expected_interest scan error: %v", err2)
					continue
				}
				cr.ExpectedAmount = fdRound(cr.ExpectedAmount, 2)
				cr.TDSAmount = fdRound(cr.TDSAmount, 2)
				totalExpected += cr.ExpectedAmount
				out = append(out, cr)
			}
			return map[string]interface{}{
				"rows":           out,
				"count":          len(out),
				"total_expected": fdRound(totalExpected, 2),
			}, nil
		})

		// ── BOD: 6. Today's action list (booking + closure tasks) ─────────────
		run("action_list", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  b.booking_id AS ref_id,
				  'BOOKING' AS task_type,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0) AS amount,
				  b.booking_status AS status,
				  COALESCE(EXTRACT(DAY FROM NOW()-b.created_at)::int, 0) AS aging_days,
				  b.created_by AS owner
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('DRAFT','APPROVAL_PENDING','SENT_TO_BANK')
				  AND ($1::text='' OR b.entity_id=$1)
				UNION ALL
				SELECT
				  ci.closure_initiate_id AS ref_id,
				  'CLOSURE' AS task_type,
				  COALESCE(ci.entity_name, m.entity_name, '') AS entity,
				  COALESCE(ci.bank_name, m.bank_name, '') AS bank,
				  COALESCE(ci.principal_amount, m.principal_amount, 0)::float8 AS amount,
				  COALESCE(la.processing_status, ci.closure_status, '') AS status,
				  -- cimplr.fd_closure_initiate has no created_at column; age the
				  -- task off the latest audit requested_at instead.
				  COALESCE(EXTRACT(DAY FROM NOW() - la.requested_at)::int, 0) AS aging_days,
				  COALESCE(la.requested_by, '') AS owner
				FROM cimplr.fd_closure_initiate ci
				JOIN investment.fd_master m ON m.fd_id = ci.fd_id AND m.is_deleted = false
				LEFT JOIN LATERAL (
				  SELECT processing_status, requested_by, requested_at
				  FROM cimplr.fd_closure_initiate_audit a
				  WHERE a.closure_initiate_id = ci.closure_initiate_id
				  ORDER BY a.requested_at DESC
				  LIMIT 1
				) la ON true
				WHERE COALESCE(ci.is_deleted, false) = false
				  AND COALESCE(la.processing_status, ci.closure_status, '') NOT IN ('POSTED', 'REJECTED', 'DELETED')
				  AND NOT (`+sqlCimplrClosureProcessed+`)
				  AND ($1::text = '' OR COALESCE(ci.entity_id, m.entity_id) = $1)
				UNION ALL
				SELECT
				  cr.closure_request_id AS ref_id,
				  'CLOSURE' AS task_type,
				  COALESCE(cr.entity_name,'') AS entity,
				  COALESCE(m.bank_name,'') AS bank,
				  COALESCE(cr.principal_amount,0)::float8 AS amount,
				  cr.closure_status AS status,
				  COALESCE(EXTRACT(DAY FROM NOW()-cr.created_at)::int, 0) AS aging_days,
				  COALESCE(cr.submitted_by,'') AS owner
				FROM investment.fd_closure_request cr
				JOIN investment.fd_master m ON m.fd_id = cr.fd_id AND m.is_deleted=false
				WHERE cr.is_deleted=false
				  AND cr.closure_status IN ('PENDING_APPROVAL','APPROVED')
				  AND ($1::text='' OR cr.entity_id=$1)
				ORDER BY aging_days DESC
				LIMIT 100`, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] action_list query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type actionRow struct {
				RefID     string  `json:"ref_id"`
				TaskType  string  `json:"task_type"`
				Entity    string  `json:"entity"`
				Bank      string  `json:"bank"`
				Amount    float64 `json:"amount"`
				Status    string  `json:"status"`
				AgingDays int     `json:"aging_days"`
				Owner     string  `json:"owner"`
				Priority  string  `json:"priority"`
			}
			out := []actionRow{}
			for rows.Next() {
				var ar actionRow
				if err2 := rows.Scan(&ar.RefID, &ar.TaskType, &ar.Entity, &ar.Bank,
					&ar.Amount, &ar.Status, &ar.AgingDays, &ar.Owner); err2 != nil {
					api.LogError("[BodEodDash] action_list scan error: %v", err2)
					continue
				}
				ar.Amount = fdRound(ar.Amount, 2)
				switch {
				case ar.AgingDays >= 3:
					ar.Priority = "High"
				case ar.AgingDays >= 1:
					ar.Priority = "Medium"
				default:
					ar.Priority = "Low"
				}
				out = append(out, ar)
			}
			return out, nil
		})

		// ── BOD: 7. Yesterday SLA breaches ────────────────────────────────────
		run("sla_breach_yesterday", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  b.booking_id,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, b.bank_name, b.bank_id,'') AS bank,
				  b.booking_status,
				  COALESCE(EXTRACT(DAY FROM NOW()-b.created_at)::int, 0) AS aging_days
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND b.booking_status NOT IN ('APPROVED','REJECTED')
				  AND EXTRACT(DAY FROM NOW()-b.created_at) >= 2
				  AND ($1::text='' OR b.entity_id=$1)
				ORDER BY aging_days DESC
				LIMIT 50`, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] sla_breach_yesterday query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type slaRow struct {
				BookingID string `json:"booking_id"`
				Entity    string `json:"entity"`
				Bank      string `json:"bank"`
				Status    string `json:"status"`
				AgingDays int    `json:"aging_days"`
			}
			out := []slaRow{}
			for rows.Next() {
				var sr slaRow
				if err2 := rows.Scan(&sr.BookingID, &sr.Entity, &sr.Bank, &sr.Status, &sr.AgingDays); err2 != nil {
					api.LogError("[BodEodDash] sla_breach scan error: %v", err2)
					continue
				}
				out = append(out, sr)
			}
			return out, nil
		})

		// ── EOD: 8. Bookings created today ────────────────────────────────────
		run("bookings_today", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  b.booking_id,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0) AS principal,
				  b.booking_status,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS created_at,
				  COALESCE(b.created_by,'') AS created_by
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND DATE(b.created_at) = $1::date
				  AND ($2::text='' OR b.entity_id=$2)
				ORDER BY b.created_at DESC
				LIMIT 100`, today, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] bookings_today query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_amount": 0}, nil
			}
			defer rows.Close()

			type bkRow struct {
				BookingID string  `json:"booking_id"`
				Entity    string  `json:"entity"`
				Bank      string  `json:"bank"`
				Principal float64 `json:"principal"`
				Status    string  `json:"status"`
				CreatedAt string  `json:"created_at"`
				CreatedBy string  `json:"created_by"`
			}
			out := []bkRow{}
			totalAmt := 0.0
			for rows.Next() {
				var br bkRow
				if err2 := rows.Scan(&br.BookingID, &br.Entity, &br.Bank, &br.Principal,
					&br.Status, &br.CreatedAt, &br.CreatedBy); err2 != nil {
					api.LogError("[BodEodDash] bookings_today scan error: %v", err2)
					continue
				}
				br.Principal = fdRound(br.Principal, 2)
				totalAmt += br.Principal
				out = append(out, br)
			}
			return map[string]interface{}{
				"rows":         out,
				"count":        len(out),
				"total_amount": fdRound(totalAmt, 2),
			}, nil
		})

		// ── EOD: 9. Confirmations captured today ──────────────────────────────
		run("confirmations_today", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  cf.confirmation_id,
				  cf.booking_id,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, m.bank_id,'') AS bank,
				  cf.actual_principal,
				  cf.confirmed_rate,
				  cf.confirmation_status,
				  COALESCE(TO_CHAR(cf.actual_maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  cf.variance_flag,
				  COALESCE(cf.variance_type,'') AS variance_type
				FROM investment.fd_confirmation cf
				JOIN investment.fd_booking_request b ON b.booking_id = cf.booking_id
				LEFT JOIN investment.fd_master m ON m.booking_id = cf.booking_id AND m.is_deleted=false
				WHERE cf.is_deleted=false
				  AND DATE(cf.created_at) = $1::date
				  AND ($2::text='' OR b.entity_id=$2)
				ORDER BY cf.created_at DESC
				LIMIT 100`, today, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] confirmations_today query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "variance_count": 0}, nil
			}
			defer rows.Close()

			type confRow struct {
				ConfirmationID string  `json:"confirmation_id"`
				BookingID      string  `json:"booking_id"`
				Entity         string  `json:"entity"`
				Bank           string  `json:"bank"`
				Principal      float64 `json:"principal"`
				ConfirmedRate  float64 `json:"confirmed_rate"`
				Status         string  `json:"status"`
				MaturityDate   string  `json:"maturity_date"`
				VarianceFlag   bool    `json:"variance_flag"`
				VarianceType   string  `json:"variance_type"`
			}
			out := []confRow{}
			varianceCount := 0
			for rows.Next() {
				var cr confRow
				if err2 := rows.Scan(
					&cr.ConfirmationID, &cr.BookingID, &cr.Entity, &cr.Bank,
					&cr.Principal, &cr.ConfirmedRate, &cr.Status, &cr.MaturityDate,
					&cr.VarianceFlag, &cr.VarianceType,
				); err2 != nil {
					api.LogError("[BodEodDash] confirmations_today scan error: %v", err2)
					continue
				}
				cr.Principal = fdRound(cr.Principal, 2)
				cr.ConfirmedRate = fdRound(cr.ConfirmedRate, 4)
				if cr.VarianceFlag {
					varianceCount++
				}
				out = append(out, cr)
			}
			return map[string]interface{}{
				"rows":           out,
				"count":          len(out),
				"variance_count": varianceCount,
			}, nil
		})

		// ── EOD: 10. Receipts ingested and matched today ───────────────────────
		run("receipts_today", func(ctx context.Context) (interface{}, error) {
			var ingested, matched, unmatched int64
			var totalIngested float64
			err := pool.QueryRow(ctx, `
				SELECT
				  COUNT(*) AS ingested,
				  SUM(CASE WHEN COALESCE(reconciliation_status,'') IN ('MATCHED','RECONCILED') THEN 1 ELSE 0 END) AS matched,
				  SUM(CASE WHEN COALESCE(reconciliation_status,'UNMATCHED') IN ('UNMATCHED','PENDING','') THEN 1 ELSE 0 END) AS unmatched,
				  COALESCE(SUM(COALESCE(gross_interest_received,0)),0) AS total_ingested
				FROM investment.fd_interest_receipt
				WHERE is_deleted=false
				  AND DATE(created_at) = $1::date
				  AND ($2::text='' OR entity_id=$2)`, today, entityFilter).Scan(&ingested, &matched, &unmatched, &totalIngested)
			if err != nil {
				api.LogError("[BodEodDash] receipts_today query error: %v", err)
				return map[string]interface{}{"ingested": 0, "matched": 0, "unmatched": 0, "total_amount": 0, "match_rate_pct": 0}, nil
			}
			matchPct := 0.0
			if ingested > 0 {
				matchPct = fdRound(float64(matched)/float64(ingested)*100, 1)
			}
			return map[string]interface{}{
				"ingested":       ingested,
				"matched":        matched,
				"unmatched":      unmatched,
				"total_amount":   fdRound(totalIngested, 2),
				"match_rate_pct": matchPct,
			}, nil
		})

		// ── EOD: 11. Exceptions opened vs closed today ────────────────────────
		run("exceptions_today", func(ctx context.Context) (interface{}, error) {
			var opened, closed, escalated int64
			err := pool.QueryRow(ctx, `
				SELECT
				  SUM(CASE WHEN exception_status NOT IN ('RESOLVED','CLOSED') THEN 1 ELSE 0 END) AS opened,
				  SUM(CASE WHEN exception_status IN ('RESOLVED','CLOSED') THEN 1 ELSE 0 END) AS closed,
				  SUM(CASE WHEN exception_status = 'ESCALATED' THEN 1 ELSE 0 END) AS escalated
				FROM investment.fd_accrual_exception ae
				WHERE COALESCE(ae.is_deleted,false)=false
				  AND DATE(ae.created_at) = $1::date
				  AND ($2::text='' OR EXISTS (
				      SELECT 1 FROM investment.fd_master m
				      WHERE m.fd_id = ae.fd_id AND m.entity_id=$2
				  ))`, today, entityFilter).Scan(&opened, &closed, &escalated)
			if err != nil {
				api.LogError("[BodEodDash] exceptions_today query error: %v", err)
				return map[string]interface{}{"opened": 0, "closed": 0, "escalated": 0}, nil
			}
			return map[string]interface{}{"opened": opened, "closed": closed, "escalated": escalated}, nil
		})

		// ── EOD: 12. GL postings today ────────────────────────────────────────
		run("posting_today", func(ctx context.Context) (interface{}, error) {
			var posted, failed, notPosted int64
			var totalPosted float64
			err := pool.QueryRow(ctx, `
				SELECT
				  SUM(CASE WHEN posting_status = 'POSTED' THEN 1 ELSE 0 END) AS posted,
				  SUM(CASE WHEN posting_status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
				  SUM(CASE WHEN posting_status = 'NOT_POSTED' THEN 1 ELSE 0 END) AS not_posted,
				  COALESCE(SUM(CASE WHEN posting_status = 'POSTED' THEN COALESCE(net_cash_flow,interest_accrued,0) ELSE 0 END),0) AS total_posted
				FROM investment.fd_cashflow_schedule cf
				JOIN investment.fd_master m ON m.fd_id = cf.fd_id AND m.is_deleted=false
				WHERE cf.is_deleted=false
				  AND DATE(cf.last_modified_at) = $1::date
				  AND ($2::text='' OR m.entity_id=$2)`, today, entityFilter).Scan(&posted, &failed, &notPosted, &totalPosted)
			if err != nil {
				api.LogError("[BodEodDash] posting_today query error: %v", err)
				return map[string]interface{}{"posted": 0, "failed": 0, "not_posted": 0, "total_posted_amount": 0}, nil
			}
			return map[string]interface{}{
				"posted":              posted,
				"failed":              failed,
				"not_posted":          notPosted,
				"total_posted_amount": fdRound(totalPosted, 2),
			}, nil
		})

		// ── EOD: 13. Latest accrual run ───────────────────────────────────────
		run("accrual_run_latest", func(ctx context.Context) (interface{}, error) {
			var runID, runType, runStatus, entity, postingStatus string
			var fdsInScope, fdsFailed int64
			var totalInterest float64
			var runDate, completedAt string
			err := pool.QueryRow(ctx, `
				SELECT
				  run_id,
				  run_type,
				  run_status,
				  entity_name,
				  COALESCE(fds_in_scope,0) AS fds_in_scope,
				  COALESCE(fds_failed,0) AS fds_failed,
				  COALESCE(total_interest_accrued,0) AS total_interest_accrued,
				  posting_status,
				  COALESCE(TO_CHAR(run_date,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS run_date,
				  COALESCE(TO_CHAR(completed_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS completed_at
				FROM investment.fd_accrual_run
				WHERE is_deleted=false
				  AND ($1::text='' OR entity_id=$1)
				ORDER BY run_date DESC
				LIMIT 1`, entityFilter).Scan(
				&runID, &runType, &runStatus, &entity,
				&fdsInScope, &fdsFailed, &totalInterest,
				&postingStatus, &runDate, &completedAt,
			)
			if err != nil {
				api.LogError("[BodEodDash] accrual_run_latest query error: %v", err)
				return map[string]interface{}{
					"run_id": "", "run_type": "", "run_status": "Unknown",
					"entity": "", "fds_in_scope": 0, "fds_failed": 0,
					"total_interest_accrued": 0, "posting_status": "", "run_date": "", "completed_at": "",
				}, nil
			}
			return map[string]interface{}{
				"run_id":                 runID,
				"run_type":               runType,
				"run_status":             runStatus,
				"entity":                 entity,
				"fds_in_scope":           fdsInScope,
				"fds_failed":             fdsFailed,
				"total_interest_accrued": fdRound(totalInterest, 2),
				"posting_status":         postingStatus,
				"run_date":               runDate,
				"completed_at":           completedAt,
			}, nil
		})

		// ── EOD: 14. EOD Checklist — driven by real data states ───────────────
		run("eod_checklist", func(ctx context.Context) (interface{}, error) {
			// Run 5 small count queries concurrently inside this goroutine
			type checkItem struct {
				ID          string `json:"id"`
				Label       string `json:"label"`
				Done        bool   `json:"done"`
				Critical    bool   `json:"critical"`
				DetailValue string `json:"detail_value,omitempty"`
			}

			// C1: statement ingestion done (no unmatched today)
			var unmatchedCount int64
			pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_interest_receipt
				WHERE is_deleted=false AND DATE(created_at)=$1::date
				AND COALESCE(reconciliation_status,'UNMATCHED') IN ('UNMATCHED','PENDING','')
				AND ($2::text='' OR entity_id=$2)`, today, entityFilter).Scan(&unmatchedCount)

			// C2: high variance cases addressed
			var openVariance int64
			pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_confirmation
				WHERE is_deleted=false AND variance_flag=true
				AND COALESCE(variance_action,'PENDING') = 'PENDING'
				AND ($1::text='' OR EXISTS(
				    SELECT 1 FROM investment.fd_booking_request b WHERE b.booking_id=fd_confirmation.booking_id AND ($1::text='' OR b.entity_id=$1)
				))`, entityFilter).Scan(&openVariance)

			// C3: GL postings clear
			var failedPostings int64
			pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_cashflow_schedule cf
				JOIN investment.fd_master m ON m.fd_id=cf.fd_id AND m.is_deleted=false
				WHERE cf.is_deleted=false AND cf.posting_status='FAILED'
				AND ($1::text='' OR m.entity_id=$1)`, entityFilter).Scan(&failedPostings)

			// C4: no critical exceptions open
			var criticalExceptions int64
			pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_accrual_exception ae
				WHERE COALESCE(ae.is_deleted,false)=false
				AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				AND ($1::text='' OR EXISTS (
				    SELECT 1 FROM investment.fd_master m WHERE m.fd_id=ae.fd_id AND m.entity_id=$1
				))`, entityFilter).Scan(&criticalExceptions)

			// C5: latest accrual run completed
			var latestRunStatus string
			pool.QueryRow(ctx, `SELECT COALESCE(run_status,'') FROM investment.fd_accrual_run
				WHERE is_deleted=false AND ($1::text='' OR entity_id=$1)
				ORDER BY run_date DESC LIMIT 1`, entityFilter).Scan(&latestRunStatus)

			// C6: maturities processed (closure requests completed for today maturities)
			var pendingMaturities int64
			pool.QueryRow(ctx, `SELECT COUNT(*) FROM investment.fd_master m
				WHERE m.is_deleted=false AND m.maturity_date=$1::date
				AND m.fd_status IN ('ACTIVE','MATURED')
				AND NOT (`+sqlAnyClosureProcessed+`)
				AND ($2::text='' OR m.entity_id=$2)`, today, entityFilter).Scan(&pendingMaturities)

			accrualDone := latestRunStatus == "COMPLETED" || latestRunStatus == "POSTED"

			items := []checkItem{
				{ID: "C1", Label: "Bank statement ingestion completed — unmatched receipts below threshold", Done: unmatchedCount == 0, Critical: true, DetailValue: formatInt64(unmatchedCount) + " unmatched"},
				{ID: "C2", Label: "High variance cases reviewed and addressed", Done: openVariance == 0, Critical: true, DetailValue: formatInt64(openVariance) + " pending"},
				{ID: "C3", Label: "All GL postings successful — no failed postings", Done: failedPostings == 0, Critical: true, DetailValue: formatInt64(failedPostings) + " failed"},
				{ID: "C4", Label: "No critical exceptions open", Done: criticalExceptions == 0, Critical: false, DetailValue: formatInt64(criticalExceptions) + " open"},
				{ID: "C5", Label: "Accrual batch run completed successfully", Done: accrualDone, Critical: false, DetailValue: "Status: " + latestRunStatus},
				{ID: "C6", Label: "Maturity proceeds verified — closure requests processed", Done: pendingMaturities == 0, Critical: true, DetailValue: formatInt64(pendingMaturities) + " pending"},
			}

			completedCount := 0
			for _, it := range items {
				if it.Done {
					completedCount++
				}
			}
			return map[string]interface{}{
				"items":          items,
				"completed":      completedCount,
				"total":          len(items),
				"completion_pct": fdRound(float64(completedCount)/float64(len(items))*100, 1),
			}, nil
		})

		// ── 15. Bank concentration KPIs ───────────────────────────────────────
		run("bank_concentration", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  m.bank_name,
				  m.bank_id,
				  COUNT(*) AS fd_count,
				  COALESCE(SUM(m.principal_amount),0) AS total_principal,
				  COALESCE(AVG(m.interest_rate),0) AS avg_rate,
				  COALESCE(MIN(m.interest_rate),0) AS min_rate,
				  COALESCE(MAX(m.interest_rate),0) AS max_rate,
				  COUNT(CASE WHEN m.maturity_date <= (CURRENT_DATE + 30) THEN 1 END) AS maturing_30d,
				  COALESCE(SUM(CASE WHEN m.maturity_date <= (CURRENT_DATE + 30) THEN m.principal_amount ELSE 0 END),0) AS maturing_30d_amount
				FROM investment.fd_master m
				WHERE m.is_deleted=false
				  AND m.fd_status IN ('ACTIVE','PENDING_ACTIVATION')
				  AND ($1::text='' OR m.entity_id=$1)
				GROUP BY m.bank_name, m.bank_id
				ORDER BY total_principal DESC
				LIMIT 20`, entityFilter)
			if err != nil {
				api.LogError("[BodEodDash] bank_concentration query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type bankRow struct {
				BankName          string  `json:"bank_name"`
				BankID            string  `json:"bank_id"`
				FDCount           int64   `json:"fd_count"`
				TotalPrincipal    float64 `json:"total_principal"`
				AvgRate           float64 `json:"avg_rate"`
				MinRate           float64 `json:"min_rate"`
				MaxRate           float64 `json:"max_rate"`
				Maturing30D       int64   `json:"maturing_30d"`
				Maturing30DAmount float64 `json:"maturing_30d_amount"`
				ConcentrationPct  float64 `json:"concentration_pct"`
			}
			out := []bankRow{}
			grandTotal := 0.0
			for rows.Next() {
				var br bankRow
				if err2 := rows.Scan(
					&br.BankName, &br.BankID, &br.FDCount, &br.TotalPrincipal,
					&br.AvgRate, &br.MinRate, &br.MaxRate,
					&br.Maturing30D, &br.Maturing30DAmount,
				); err2 != nil {
					api.LogError("[BodEodDash] bank_concentration scan error: %v", err2)
					continue
				}
				br.TotalPrincipal = fdRound(br.TotalPrincipal, 2)
				br.AvgRate = fdRound(br.AvgRate, 4)
				br.MinRate = fdRound(br.MinRate, 4)
				br.MaxRate = fdRound(br.MaxRate, 4)
				br.Maturing30DAmount = fdRound(br.Maturing30DAmount, 2)
				grandTotal += br.TotalPrincipal
				out = append(out, br)
			}
			// compute concentration pct
			if grandTotal > 0 {
				for i := range out {
					out[i].ConcentrationPct = fdRound(out[i].TotalPrincipal/grandTotal*100, 2)
				}
			}
			return map[string]interface{}{
				"rows":        out,
				"grand_total": fdRound(grandTotal, 2),
			}, nil
		})

		// ── wait ──────────────────────────────────────────────────────────────
		wg.Wait()

		get := func(key string) interface{} {
			if r, ok := results[key]; ok && r.err == nil {
				return r.data
			}
			return nil
		}

		// BOD KPIs
		todayMaturityCount := 0
		todayMaturityTotal := 0.0
		if v := get("maturities_today"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					todayMaturityCount = c
				}
				if t, ok2 := m["total_principal"].(float64); ok2 {
					todayMaturityTotal = t
				}
			}
		}
		next3Count := 0
		if v := get("maturities_3days"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					next3Count = c
				}
			}
		}
		confDueCount, confOverdue := 0, 0
		if v := get("confirmations_due"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					confDueCount = c
				}
				if c, ok2 := m["overdue_count"].(int); ok2 {
					confOverdue = c
				}
			}
		}
		expectedInterestTotal := 0.0
		if v := get("expected_interest"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if t, ok2 := m["total_expected"].(float64); ok2 {
					expectedInterestTotal = t
				}
			}
		}

		// EOD KPIs
		bookingsTodayCount := 0
		bookingsTodayAmount := 0.0
		if v := get("bookings_today"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					bookingsTodayCount = c
				}
				if t, ok2 := m["total_amount"].(float64); ok2 {
					bookingsTodayAmount = t
				}
			}
		}
		confTodayCount := 0
		if v := get("confirmations_today"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					confTodayCount = c
				}
			}
		}

		payload := map[string]interface{}{
			"generated_at": now.Format(time.RFC3339),
			"as_of_date":   today,
			"filters": map[string]interface{}{
				"entity_id":  entityFilter,
				"currency":   req.Currency,
				"mode":       req.Mode,
				"period":     periodBounds.Period,
				"start_date": periodBounds.StartStr,
				"end_date":   periodBounds.EndStr,
			},
			// ── BOD KPIs ──────────────────────────────────────────────────────
			"bod_kpis": map[string]interface{}{
				"maturities_today_count":   todayMaturityCount,
				"maturities_today_total":   todayMaturityTotal,
				"maturities_next_3d_count": next3Count,
				"confirmations_due":        confDueCount,
				"confirmations_overdue":    confOverdue,
				"expected_interest_today":  expectedInterestTotal,
				"accrual_runs_today":       countSlice(get("accrual_scheduled")),
				"sla_breaches_yesterday":   countSlice(get("sla_breach_yesterday")),
				"action_items_total":       countSlice(get("action_list")),
			},
			// ── EOD KPIs ──────────────────────────────────────────────────────
			"eod_kpis": map[string]interface{}{
				"bookings_today_count":   bookingsTodayCount,
				"bookings_today_amount":  bookingsTodayAmount,
				"confirmations_today":    confTodayCount,
				"receipts_ingested":      getNestedInt64(get("receipts_today"), "ingested"),
				"receipts_matched":       getNestedInt64(get("receipts_today"), "matched"),
				"receipts_unmatched":     getNestedInt64(get("receipts_today"), "unmatched"),
				"receipt_match_rate_pct": getNestedFloat(get("receipts_today"), "match_rate_pct"),
				"exceptions_opened":      getNestedInt64(get("exceptions_today"), "opened"),
				"exceptions_closed":      getNestedInt64(get("exceptions_today"), "closed"),
				"gl_postings_success":    getNestedInt64(get("posting_today"), "posted"),
				"gl_postings_failed":     getNestedInt64(get("posting_today"), "failed"),
			},
			// ── Detail tables ─────────────────────────────────────────────────
			"maturities_today":     get("maturities_today"),
			"maturities_3days":     get("maturities_3days"),
			"confirmations_due":    get("confirmations_due"),
			"accrual_scheduled":    get("accrual_scheduled"),
			"expected_interest":    get("expected_interest"),
			"action_list":          get("action_list"),
			"sla_breach_yesterday": get("sla_breach_yesterday"),
			"bookings_today":       get("bookings_today"),
			"confirmations_today":  get("confirmations_today"),
			"receipts_today":       get("receipts_today"),
			"exceptions_today":     get("exceptions_today"),
			"posting_today":        get("posting_today"),
			"accrual_run_latest":   get("accrual_run_latest"),
			"eod_checklist":        get("eod_checklist"),
			"bank_concentration":   get("bank_concentration"),
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func countSlice(v interface{}) int {
	if v == nil {
		return 0
	}
	// try []interface{}
	if s, ok := v.([]interface{}); ok {
		return len(s)
	}
	// try map with "count"
	if m, ok := v.(map[string]interface{}); ok {
		if c, ok2 := m["count"].(int); ok2 {
			return c
		}
	}
	return 0
}

func getNestedInt64(v interface{}, key string) int64 {
	if v == nil {
		return 0
	}
	if m, ok := v.(map[string]interface{}); ok {
		if val, ok2 := m[key]; ok2 {
			switch t := val.(type) {
			case int64:
				return t
			case int:
				return int64(t)
			case float64:
				return int64(t)
			}
		}
	}
	return 0
}

func formatInt64(n int64) string {
	if n == 0 {
		return "0"
	}
	s := ""
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}
