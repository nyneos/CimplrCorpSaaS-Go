// Package investmentdashboards — FD Treasury Manager Dashboard
//
// POST /dash/investment/fd/treasury-dashboard
//
// Returns aggregated data for the Treasury Manager view:
//   - Surplus & deployment KPIs
//   - Rate negotiations (open/pending/offers)
//   - FDs near maturity (next 30 days)
//   - Rollover decisions pending
//   - Rate distribution heatmap by bank × tenor
//   - Weighted average yield by bank
//   - Deployment distribution by bank
//   - Maturity ladder (weekly/monthly buckets)
//   - Booking confirmations awaiting (SLA tracking)
//
// All sub-computations run concurrently via sync.WaitGroup.
package investmentdashboards

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── request type ────────────────────────────────────────────────────────────

type fdTreasuryDashRequest struct {
	UserID    string `json:"user_id"`
	EntityID  string `json:"entity_id"`
	Currency  string `json:"currency"`
	Period    string `json:"period"`     // MTD | QTD | YTD | CUSTOM
	StartDate string `json:"start_date"` // YYYY-MM-DD when Period==CUSTOM
	EndDate   string `json:"end_date"`   // YYYY-MM-DD when Period==CUSTOM
	AsOnDate  string `json:"as_on_date"`
	Bank      string `json:"bank"`    // bank_id filter
	FDType    string `json:"fd_type"` // SIMPLE | COMPOUND — filters by interest_type_code
}

// ─── handler ─────────────────────────────────────────────────────────────────

// GetFDTreasuryDashboard returns the full Treasury Manager FD dashboard payload.
func GetFDTreasuryDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req fdTreasuryDashRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Currency == "" {
			req.Currency = "INR"
		}
		if req.Period == "" {
			req.Period = "MTD"
		}

		now := time.Now().UTC()
		if asOn, ok := parseFDDate(req.AsOnDate); ok {
			now = asOn
		}
		snapshotDate := now.Format(constants.DateFormat)

		ctx := r.Context()
		entityFilter := req.EntityID
		bankFilter    := req.Bank
		fdTypeFilter  := req.FDType

		// snapshotFilter restricts every fd_master query to FDs that started
		// on or before as_on_date — enforces the strict snapshot semantics.
		snapshotFilter := " AND m.start_date <= '" + snapshotDate + "'::date"
		snapBookingFilter := " AND b.created_at <= '" + snapshotDate + "'::date + INTERVAL '1 day'"
		snapConfirmationFilter := " AND c.created_at <= '" + snapshotDate + "'::date + INTERVAL '1 day'"
		snapNegotiationFilter := " AND n.created_at <= '" + snapshotDate + "'::date + INTERVAL '1 day'"

		periodBounds := resolveFDPeriodBounds(req.Period, req.StartDate, req.EndDate, now)
		startDateStr := periodBounds.StartStr
		endDateStr := periodBounds.EndStr

		type subResult struct {
			data interface{}
			err  error
		}
		results := make(map[string]subResult, 12)
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

		// ── 1. surplus & deployment KPI ──────────────────────────────────────────────────────────────────────────────────────
		// Surplus "available" = fd_master rows in ACTIVE or PENDING_ACTIVATION.
		// Includes:
		//   • ACTIVE FDs that have NOT been fully closed via the cimplr workflow.
		//   • PENDING_ACTIVATION FDs awaiting activation approval.
		// Excludes:
		//   • MATURED / CLOSED / other fd_status values.
		//   • ACTIVE FDs already POSTED via cimplr.fd_closure_confirm (accounting_posted=true).
		run("surplus_deployment", func(ctx context.Context) (interface{}, error) {
			var activeAmt, pendingAmt float64
			var activeCnt, pendingCnt int64

			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(SUM(m.principal_amount),0), COUNT(*)
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted = false
				  AND m.fd_status = 'ACTIVE'
				  AND NOT EXISTS (
				    SELECT 1 FROM cimplr.fd_closure_confirm cc
				    WHERE cc.fd_id = m.fd_id
				      AND COALESCE(cc.is_deleted, false) = false
				      AND cc.closure_status = 'POSTED'
				      AND COALESCE(cc.accounting_posted, false) = true
				  )
				  AND NOT EXISTS (
				    SELECT 1 FROM investment.fd_closure_request cr
				    WHERE cr.fd_id = m.fd_id
				      AND COALESCE(cr.is_deleted, false) = false
				      AND cr.closure_status IN ('COMPLETED','POSTED','CLOSED')
				  )
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` + snapshotFilter,
				entityFilter, fdTypeFilter, bankFilter).Scan(&activeAmt, &activeCnt)

			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(SUM(m.principal_amount),0), COUNT(*)
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted = false
				  AND m.fd_status = 'PENDING_ACTIVATION'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` + snapshotFilter,
				entityFilter, fdTypeFilter, bankFilter).Scan(&pendingAmt, &pendingCnt)

			total := activeAmt + pendingAmt
			return map[string]interface{}{
				"deployed":         fdRound(total, 2),
				"deployment_count": activeCnt + pendingCnt,
				"active_amount":    fdRound(activeAmt, 2),
				"active_count":     activeCnt,
				"pending_amount":   fdRound(pendingAmt, 2),
				"pending_count":    pendingCnt,
			}, nil
		})

		// ── 2. surplus deployment count (fd count) ─────────────────────────────────────
		run("fd_count_active", func(ctx context.Context) (interface{}, error) {
			var cnt int64
			err := pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` + snapshotFilter,
				entityFilter, fdTypeFilter, bankFilter).Scan(&cnt)
			if err != nil {
				api.LogError("[TreasuryDash] fd_count_active query error: %v", err)
				return int64(0), nil
			}
			return cnt, nil
		})

		// ── 3. rate negotiations (open/sent/received) ─────────────────────────
		run("negotiations", func(ctx context.Context) (interface{}, error) {
			// Try fd_bank_rate_negotiation if it exists; graceful fallback to empty
			rows, err := pool.Query(ctx, `
				SELECT
				  n.negotiation_id,
				  COALESCE(n.bank_name, n.bank_id, '') AS bank,
				  COALESCE(b.entity_name, n.entity_id, '') AS entity,
				  COALESCE(n.amount, 0) AS amount,
				  COALESCE(n.requested_rate, 0) AS requested_rate,
				  COALESCE(n.offered_rate, 0) AS offered_rate,
				  COALESCE(n.tenor, '') AS tenor,
				  COALESCE(n.negotiation_status, '') AS status,
				  COALESCE(n.aging_days, 0) AS aging_days,
				  COALESCE(TO_CHAR(n.offer_expiry_at, 'YYYY-MM-DD"T"HH24:MI:SS'), '') AS expires_at
				FROM investment.fd_bank_rate_negotiation n
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = n.booking_id
				WHERE COALESCE(n.is_deleted,false)=false
				  AND ($1::text='' OR n.entity_id=$1)` + snapNegotiationFilter + `
				ORDER BY n.created_at DESC
				LIMIT 50`, entityFilter)
			if err != nil {
				// Table may not exist - return empty gracefully
				return map[string]interface{}{
					"rows":         []interface{}{},
					"open_count":   0,
					"avg_aging":    0,
					"best_rate":    0,
					"offers_today": []interface{}{},
				}, nil
			}
			defer rows.Close()

			type negRow struct {
				ID            string  `json:"id"`
				Bank          string  `json:"bank"`
				Entity        string  `json:"entity"`
				Amount        float64 `json:"amount"`
				RequestedRate float64 `json:"requested_rate"`
				OfferedRate   float64 `json:"offered_rate"`
				Tenor         string  `json:"tenor"`
				Status        string  `json:"status"`
				AgingDays     int     `json:"aging_days"`
				ExpiresAt     string  `json:"expires_at"`
			}
			var negRows []negRow
			for rows.Next() {
				var nr negRow
				if err2 := rows.Scan(&nr.ID, &nr.Bank, &nr.Entity, &nr.Amount,
					&nr.RequestedRate, &nr.OfferedRate, &nr.Tenor, &nr.Status,
					&nr.AgingDays, &nr.ExpiresAt); err2 == nil {
					negRows = append(negRows, nr)
				}
			}
			if negRows == nil {
				negRows = []negRow{}
			}

			openCount := 0
			agingSum := 0
			bestRate := 0.0
			var offersToday []negRow
			for _, nr := range negRows {
				if nr.Status == "SENT" || nr.Status == "DRAFT" || nr.Status == "PENDING" {
					openCount++
					agingSum += nr.AgingDays
				}
				if nr.OfferedRate > bestRate {
					bestRate = nr.OfferedRate
				}
				if nr.ExpiresAt != "" && nr.ExpiresAt[:10] == snapshotDate {
					offersToday = append(offersToday, nr)
				}
			}
			avgAging := 0.0
			if openCount > 0 {
				avgAging = fdRound(float64(agingSum)/float64(openCount), 1)
			}
			if offersToday == nil {
				offersToday = []negRow{}
			}
			return map[string]interface{}{
				"rows":         negRows,
				"open_count":   openCount,
				"avg_aging":    avgAging,
				"best_rate":    fdRound(bestRate, 2),
				"offers_today": offersToday,
			}, nil
		})

		// ── 4. FDs near maturity (≤30 days ahead + overdue pending redemption) ─────
		// Eligibility aligned with closure.go GetFDsNearMaturity / closureV2:
		//   SHOW  — ACTIVE/MATURED with no non-deleted closure request / initiate / confirm
		//   SHOW  — calendar-matured ACTIVE rows still awaiting payout (maturity_date < today)
		//   WINDOW — maturity_date <= CURRENT_DATE + 30 days (no lower bound)
		run("near_maturity", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  m.fd_id,
				  COALESCE(b.entity_name, m.entity_name, '') AS entity,
				  COALESCE(m.bank_name, m.bank_id,'') AS bank,
				  m.principal_amount,
				  m.interest_rate,
				  TO_CHAR(m.maturity_date,'YYYY-MM-DD') AS maturity_date,
				  TO_CHAR(m.start_date,'YYYY-MM-DD') AS start_date,
				  COALESCE(m.maturity_instructions,'') AS maturity_instructions,
				  m.fd_status,
				  (m.maturity_date - ('` + snapshotDate + `'::date)) AS days_to_maturity,
				  COALESCE(al.total_interest_accrued, 0) AS interest_accrued,
				  COALESCE(ci.closure_initiate_id, '') AS closure_initiate_id,
				  COALESCE(ci.closure_type, '') AS workflow_closure_type,
				  COALESCE(ci.closure_status, '') AS workflow_closure_status,
				  COALESCE(ci.action_at_maturity, '') AS workflow_action_at_maturity
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				LEFT JOIN LATERAL (
				  SELECT SUM(COALESCE(period_interest_accrued,0)) AS total_interest_accrued
				  FROM investment.fd_accrual_ledger
				  WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false
				) al ON true
				LEFT JOIN LATERAL (
				  SELECT closure_initiate_id, closure_type, closure_status, action_at_maturity
				  FROM cimplr.fd_closure_initiate ci2
				  WHERE ci2.fd_id = m.fd_id AND COALESCE(ci2.is_deleted,false)=false
				  ORDER BY ci2.created_at DESC LIMIT 1
				) ci ON true
				WHERE m.is_deleted = false
				  AND m.fd_status IN ('ACTIVE','MATURED')
				  AND m.maturity_date <= ('` + snapshotDate + `'::date) + INTERVAL '30 days'
				  AND NOT EXISTS (
				    SELECT 1 FROM investment.fd_closure_request xcr
				    WHERE xcr.fd_id = m.fd_id AND COALESCE(xcr.is_deleted,false) = false
				  )
				  AND NOT EXISTS (
				    SELECT 1 FROM cimplr.fd_closure_initiate ncr
				    WHERE ncr.fd_id = m.fd_id AND COALESCE(ncr.is_deleted,false) = false
				  )
				  AND NOT EXISTS (
				    SELECT 1 FROM cimplr.fd_closure_confirm ncc
				    WHERE ncc.fd_id = m.fd_id AND COALESCE(ncc.is_deleted,false) = false
				  )
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` +
				snapshotFilter + `
				ORDER BY m.maturity_date ASC`, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				api.LogError("[TreasuryDash] near_maturity query error: %v", err)
				return map[string]interface{}{
					"rows": []interface{}{}, "count": 0, "total_amount": 0.0, "rollover_pending": 0,
				}, nil
			}
			defer rows.Close()

			type matRow struct {
				FDID                  string  `json:"fd_id"`
				Entity                string  `json:"entity"`
				Bank                  string  `json:"bank"`
				Principal             float64 `json:"principal"`
				Rate                  float64 `json:"rate"`
				MaturityDate          string  `json:"maturity_date"`
				StartDate             string  `json:"start_date"`
				MaturityInstructions  string  `json:"maturity_instructions"`
				Status                string  `json:"status"`
				DaysToMaturity        int     `json:"days_to_maturity"`
				InterestAccrued       float64 `json:"interest_accrued"`
				ClosureInitiateID     string  `json:"closure_initiate_id,omitempty"`
				WorkflowClosureType   string  `json:"workflow_closure_type,omitempty"`
				WorkflowClosureStatus string  `json:"workflow_closure_status,omitempty"`
				WorkflowActionAtMat   string  `json:"workflow_action_at_maturity,omitempty"`
				NeedsRolloverDecision bool    `json:"needs_rollover_decision"`
			}
			out := []matRow{}
			totalAmt := 0.0
			rolloverPending := 0
			maturingToday := 0
			for rows.Next() {
				var mr matRow
				if err2 := rows.Scan(&mr.FDID, &mr.Entity, &mr.Bank, &mr.Principal,
					&mr.Rate, &mr.MaturityDate, &mr.StartDate, &mr.MaturityInstructions,
					&mr.Status, &mr.DaysToMaturity, &mr.InterestAccrued,
					&mr.ClosureInitiateID, &mr.WorkflowClosureType,
					&mr.WorkflowClosureStatus, &mr.WorkflowActionAtMat); err2 != nil {
					continue
				}
				mr.Principal = fdRound(mr.Principal, 2)
				mr.Rate = fdRound(mr.Rate, 4)
				mr.InterestAccrued = fdRound(mr.InterestAccrued, 2)

				// No closure workflow started yet → rollover decision still required.
				if mr.ClosureInitiateID == "" {
					mr.NeedsRolloverDecision = true
					rolloverPending++
				}
				if mr.DaysToMaturity == 0 {
					maturingToday++
				}
				totalAmt += mr.Principal
				out = append(out, mr)
			}
			return map[string]interface{}{
				"rows":             out,
				"count":            len(out),
				"total_amount":     fdRound(totalAmt, 2),
				"rollover_pending": rolloverPending,
				"maturing_today":   maturingToday,
			}, nil
		})

		// ── 5. deployment distribution by bank ────────────────────────────────
		run("deployment_by_bank", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  COALESCE(m.bank_name, m.bank_id) AS bank,
				  COALESCE(SUM(m.principal_amount),0) AS exposure,
				  COUNT(*) AS fd_count
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR b.entity_id=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` +
				snapshotFilter + `
				GROUP BY COALESCE(m.bank_name, m.bank_id)
				ORDER BY exposure DESC`, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				return []interface{}{}, nil
			}
			defer rows.Close()

			type depRow struct {
				Bank     string  `json:"bank"`
				Exposure float64 `json:"exposure"`
				FDCount  int64   `json:"fd_count"`
			}
			out := []depRow{}
			for rows.Next() {
				var dr depRow
				if err2 := rows.Scan(&dr.Bank, &dr.Exposure, &dr.FDCount); err2 == nil {
					dr.Exposure = fdRound(dr.Exposure, 2)
					out = append(out, dr)
				}
			}
			return out, nil
		})

		// ── 6. weighted avg yield by bank ─────────────────────────────────────
		run("yield_by_bank", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  COALESCE(m.bank_name, m.bank_id) AS bank,
				  COALESCE(
				    SUM(m.principal_amount * m.interest_rate) / NULLIF(SUM(m.principal_amount),0),
				    0
				  ) AS weighted_avg_yield
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR b.entity_id=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` +
				snapshotFilter + `
				GROUP BY COALESCE(m.bank_name, m.bank_id)
				ORDER BY weighted_avg_yield DESC`, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				return []interface{}{}, nil
			}
			defer rows.Close()

			type yieldRow struct {
				Bank             string  `json:"bank"`
				WeightedAvgYield float64 `json:"weighted_avg_yield"`
			}
			out := []yieldRow{}
			for rows.Next() {
				var yr yieldRow
				if err2 := rows.Scan(&yr.Bank, &yr.WeightedAvgYield); err2 == nil {
					yr.WeightedAvgYield = fdRound(yr.WeightedAvgYield, 2)
					out = append(out, yr)
				}
			}
			return out, nil
		})

		// ── 7. maturity ladder ────────────────────────────────────────────────
		// Show only ACTIVE FDs whose maturity is still upcoming and that have not
		// been processed through the cimplr closure workflow (POSTED/CONFIRM)
		// or legacy fd_closure_request (COMPLETED/POSTED/CLOSED).
		run("maturity_ladder_treasury", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  CASE
				    WHEN m.maturity_date BETWEEN ('` + snapshotDate + `'::date) AND ('` + snapshotDate + `'::date)+6   THEN 'Wk 1'
				    WHEN m.maturity_date BETWEEN ('` + snapshotDate + `'::date)+7  AND ('` + snapshotDate + `'::date)+13  THEN 'Wk 2'
				    WHEN m.maturity_date BETWEEN ('` + snapshotDate + `'::date)+14 AND ('` + snapshotDate + `'::date)+20  THEN 'Wk 3'
				    WHEN m.maturity_date BETWEEN ('` + snapshotDate + `'::date)+21 AND ('` + snapshotDate + `'::date)+27  THEN 'Wk 4'
				    WHEN m.maturity_date BETWEEN ('` + snapshotDate + `'::date)+28 AND ('` + snapshotDate + `'::date)+59  THEN 'M 2'
				    WHEN m.maturity_date BETWEEN ('` + snapshotDate + `'::date)+60 AND ('` + snapshotDate + `'::date)+89  THEN 'M 3'
				    WHEN m.maturity_date BETWEEN ('` + snapshotDate + `'::date)+90 AND ('` + snapshotDate + `'::date)+179 THEN 'Q3'
				    ELSE 'Q4+'
				  END AS period,
				  COALESCE(SUM(m.principal_amount),0) AS amount,
				  COUNT(*) AS count
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted = false
				  AND m.fd_status = 'ACTIVE'
				  AND m.maturity_date >= ('` + snapshotDate + `'::date)
				  AND NOT EXISTS (
				    SELECT 1 FROM cimplr.fd_closure_confirm cc
				    WHERE cc.fd_id = m.fd_id
				      AND COALESCE(cc.is_deleted,false) = false
				      AND cc.closure_status = 'POSTED'
				      AND COALESCE(cc.accounting_posted,false) = true
				  )
				  AND NOT EXISTS (
				    SELECT 1 FROM investment.fd_closure_request cr
				    WHERE cr.fd_id = m.fd_id
				      AND COALESCE(cr.is_deleted,false) = false
				      AND cr.closure_status IN ('COMPLETED','POSTED','CLOSED')
				  )
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` +
				snapshotFilter + `
				GROUP BY 1
				ORDER BY MIN(m.maturity_date)`, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				return []interface{}{}, nil
			}
			defer rows.Close()

			type ladderRow struct {
				Period string  `json:"period"`
				Amount float64 `json:"amount"`
				Count  int64   `json:"count"`
			}
			out := []ladderRow{}
			for rows.Next() {
				var lr ladderRow
				if err2 := rows.Scan(&lr.Period, &lr.Amount, &lr.Count); err2 == nil {
					lr.Amount = fdRound(lr.Amount, 2)
					out = append(out, lr)
				}
			}
			return out, nil
		})

		// ── 8. rate distribution (bank × rate bucket) for heatmap ────────────
		run("rate_by_bank", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  COALESCE(m.bank_name, m.bank_id) AS bank,
				  CASE
				    WHEN m.interest_rate < 5  THEN '<5%'
				    WHEN m.interest_rate < 6  THEN '5-6%'
				    WHEN m.interest_rate < 7  THEN '6-7%'
				    WHEN m.interest_rate < 8  THEN '7-8%'
				    WHEN m.interest_rate < 9  THEN '8-9%'
				    ELSE '9%+'
				  END AS rate_bucket,
				  COUNT(*) AS fd_count,
				  COALESCE(SUM(m.principal_amount),0) AS exposure,
				  COALESCE(
				    SUM(m.principal_amount * m.interest_rate) / NULLIF(SUM(m.principal_amount),0),
				    0
				  ) AS avg_rate
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR b.entity_id=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` +
				snapshotFilter + `
				GROUP BY COALESCE(m.bank_name, m.bank_id), 2
				ORDER BY COALESCE(m.bank_name, m.bank_id), MIN(m.interest_rate)`, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				return []interface{}{}, nil
			}
			defer rows.Close()

			type rateRow struct {
				Bank       string  `json:"bank"`
				RateBucket string  `json:"rate_bucket"`
				FDCount    int64   `json:"fd_count"`
				Exposure   float64 `json:"exposure"`
				AvgRate    float64 `json:"avg_rate"`
			}
			out := []rateRow{}
			for rows.Next() {
				var rr rateRow
				if err2 := rows.Scan(&rr.Bank, &rr.RateBucket, &rr.FDCount, &rr.Exposure, &rr.AvgRate); err2 == nil {
					rr.Exposure = fdRound(rr.Exposure, 2)
					rr.AvgRate = fdRound(rr.AvgRate, 2)
					out = append(out, rr)
				}
			}
			return out, nil
		})

		// ── 9. booking confirmation SLA tracking (TC-105) ─────────────────────
		// Returns the full confirmation pipeline so users can clearly see WHY
		// each booking is "pending":
		//   • booking_status (raw)            — DRAFT / APPROVAL_PENDING / APPROVED / SENT_TO_BANK
		//   • confirmation_state              — Pending Maker / Pending Approval / Sent to Bank /
		//                                       Confirmed / Activated / Rejected (human readable)
		//   • approval_progress               — "2/3 approvals" derived from approval_instance_eye
		//   • last_actor_email + last_action_at — for accountability
		//   • fd_id / activation_date         — present only after the bank has confirmed
		run("booking_confirmations", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  b.booking_id,
				  COALESCE(b.entity_name,'')                                                       AS entity,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'')                      AS bank,
				  COALESCE(b.principal_amount,0)                                                   AS amount,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')                     AS sent_at,
				  b.booking_status                                                                 AS booking_status,
				  COALESCE(b.created_by,'')                                                        AS created_by,
				  COALESCE(EXTRACT(EPOCH FROM (('` + snapshotDate + `'::timestamp)-b.created_at))/3600,0)                        AS elapsed_hours,
				  COALESCE(m.fd_id,'')                                                             AS fd_id,
				  COALESCE(m.fd_status,'')                                                         AS fd_status,
				  COALESCE(TO_CHAR(m.created_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')                     AS activation_date,
				  COALESCE(ai.instance_status,'')                                                  AS approval_status,
				  COALESCE(ai.approvals_received,0)                                                AS approvals_received,
				  COALESCE(ai.approvals_required,0)                                                AS approvals_required,
				  COALESCE(ai.last_actor,'')                                                       AS last_actor,
				  COALESCE(TO_CHAR(ai.last_action_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')                AS last_action_at
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				LEFT JOIN LATERAL (
				  SELECT
				    i.status AS instance_status,
				    COALESCE(tot.approvals_received, 0) AS approvals_received,
				    COALESCE(tot.approvals_required, 0) AS approvals_required,
				    (SELECT ia.actor_email FROM uam.approval_instance_action ia
				     JOIN uam.approval_instance_eye ie2 ON ie2.instance_eye_id = ia.instance_eye_id
				     WHERE ie2.instance_id = i.instance_id ORDER BY ia.acted_at DESC NULLS LAST LIMIT 1) AS last_actor,
				    (SELECT ia.acted_at FROM uam.approval_instance_action ia
				     JOIN uam.approval_instance_eye ie2 ON ie2.instance_eye_id = ia.instance_eye_id
				     WHERE ie2.instance_id = i.instance_id ORDER BY ia.acted_at DESC NULLS LAST LIMIT 1) AS last_action_at
				  FROM uam.approval_instance i
				  LEFT JOIN (
				    SELECT instance_id,
				           SUM(COALESCE(approvals_received,0))::int AS approvals_received,
				           SUM(COALESCE(approvals_required,0))::int AS approvals_required
				    FROM uam.approval_instance_eye
				    GROUP BY instance_id
				  ) tot ON tot.instance_id = i.instance_id
				  WHERE i.is_deleted=false
				    AND i.module_code = 'FIXED_DEPOSIT'
				    AND i.record_id = b.booking_id
				  ORDER BY i.submitted_at DESC NULLS LAST
				  LIMIT 1
				) ai ON true
				WHERE b.is_deleted=false
				  AND `+sqlExcludeTerminalFdOnBooking+`
				  AND b.booking_status IN ('APPROVAL_PENDING','SENT_TO_BANK','APPROVED','DRAFT','CONFIRMED','ACTIVE')
				  AND ($1::text='' OR b.entity_id=$1)` + snapBookingFilter + `
				ORDER BY b.created_at ASC
				LIMIT 100`, entityFilter)
			if err != nil {
				api.LogError("[TreasuryDash] booking_confirmations query error: %v", err)
				return map[string]interface{}{
					"rows": []interface{}{}, "total": 0,
					"overdue": 0, "at_risk": 0, "confirmed": 0, "pending_approval": 0, "sent_to_bank": 0,
				}, nil
			}
			defer rows.Close()

			type confRow struct {
				BookingID         string  `json:"booking_id"`
				Entity            string  `json:"entity"`
				Bank              string  `json:"bank"`
				Amount            float64 `json:"amount"`
				SentAt            string  `json:"sent_at"`
				Status            string  `json:"status"` // raw booking_status
				CreatedBy         string  `json:"created_by"`
				ConfirmationState string  `json:"confirmation_state"` // human-readable lifecycle stage
				ConfirmationStage int     `json:"confirmation_stage"` // 1..6 (Maker..Activated)
				FDID              string  `json:"fd_id,omitempty"`
				FDStatus          string  `json:"fd_status,omitempty"`
				ActivationDate    string  `json:"activation_date,omitempty"`
				ApprovalStatus    string  `json:"approval_status,omitempty"`
				ApprovalsReceived int     `json:"approvals_received"`
				ApprovalsRequired int     `json:"approvals_required"`
				ApprovalProgress  string  `json:"approval_progress"`
				LastActor         string  `json:"last_actor,omitempty"`
				LastActionAt      string  `json:"last_action_at,omitempty"`
				ElapsedHours      float64 `json:"elapsed_hours"`
				SLAHours          float64 `json:"sla_hours"`
				SLAStatus         string  `json:"sla_status"`
			}
			out := []confRow{}
			confirmed, pendingApproval, sentToBank, drafts := 0, 0, 0, 0
			for rows.Next() {
				var cr confRow
				if err2 := rows.Scan(
					&cr.BookingID, &cr.Entity, &cr.Bank, &cr.Amount,
					&cr.SentAt, &cr.Status, &cr.CreatedBy, &cr.ElapsedHours,
					&cr.FDID, &cr.FDStatus, &cr.ActivationDate,
					&cr.ApprovalStatus, &cr.ApprovalsReceived, &cr.ApprovalsRequired,
					&cr.LastActor, &cr.LastActionAt,
				); err2 != nil {
					api.LogError("[TreasuryDash] booking_confirmations scan error: %v", err2)
					continue
				}
				cr.Amount = fdRound(cr.Amount, 2)
				cr.ElapsedHours = fdRound(cr.ElapsedHours, 1)
				// Stage-aware SLA targets (hours) so draft / bank-send rows are not all "overdue" at 24h.
				switch cr.Status {
				case "DRAFT":
					cr.SLAHours = 120
				case "APPROVAL_PENDING":
					cr.SLAHours = 72
				case constants.StatusApproved:
					cr.SLAHours = 72
				case "SENT_TO_BANK":
					cr.SLAHours = 336
				default:
					cr.SLAHours = 168
				}
				if cr.SLAHours < 1 {
					cr.SLAHours = 1
				}

				// Derive a clear confirmation lifecycle state.
				switch {
				case cr.FDStatus == constants.StatusActive:
					cr.ConfirmationState = "Activated"
					cr.ConfirmationStage = 6
				case cr.FDID != "":
					cr.ConfirmationState = "Confirmed by Bank"
					cr.ConfirmationStage = 5
				case cr.Status == "SENT_TO_BANK":
					cr.ConfirmationState = "Sent to Bank — Awaiting Confirmation"
					cr.ConfirmationStage = 4
					sentToBank++
				case cr.Status == constants.StatusApproved:
					cr.ConfirmationState = "Approved — Ready to Send"
					cr.ConfirmationStage = 3
				case cr.Status == "APPROVAL_PENDING":
					cr.ConfirmationState = "Pending Approval"
					cr.ConfirmationStage = 2
					pendingApproval++
				case cr.Status == "DRAFT":
					cr.ConfirmationState = "Draft — Pending Maker"
					cr.ConfirmationStage = 1
					drafts++
				default:
					cr.ConfirmationState = cr.Status
				}
				if cr.FDID != "" {
					confirmed++
				}

				if cr.ApprovalsRequired > 0 {
					cr.ApprovalProgress = fmtApprovalProgress(cr.ApprovalsReceived, cr.ApprovalsRequired)
				} else if cr.Status == "APPROVAL_PENDING" || cr.Status == "DRAFT" {
					cr.ApprovalProgress = "Not started"
				}

				ratio := cr.ElapsedHours / cr.SLAHours
				switch {
				case ratio >= 1:
					cr.SLAStatus = "Overdue"
				case ratio >= 0.75:
					cr.SLAStatus = "At Risk"
				default:
					cr.SLAStatus = "On Track"
				}
				out = append(out, cr)
			}
			return map[string]interface{}{
				"rows":             out,
				"total":            len(out),
				"overdue":          countSLAStatus(out, "Overdue"),
				"at_risk":          countSLAStatus(out, "At Risk"),
				"confirmed":        confirmed,
				"pending_approval": pendingApproval,
				"sent_to_bank":     sentToBank,
				"drafts":           drafts,
			}, nil
		})

		// ── 9b. fd_confirmation rows still awaiting bank acknowledgement ──────
		// Show every confirmation that is NOT yet finalised — i.e. status not in
		// (CONFIRMED / APPROVED / VARIANCE_ACCEPTED / REJECTED). This includes
		// PENDING_APPROVAL, APPROVAL_PENDING, DRAFT, and any other interim state.
		run("pending_confirmations", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  COALESCE(c.confirmation_id,''),
				  COALESCE(c.booking_id,''),
				  COALESCE(m.fd_id,''),
				  COALESCE(b.entity_name,''),
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,''),
				  COALESCE(c.confirmed_principal_amount, b.principal_amount, 0),
				  COALESCE(c.confirmed_interest_rate, b.interest_rate, m.interest_rate, 0),
				  COALESCE(TO_CHAR(c.confirmed_maturity_date,'YYYY-MM-DD'), TO_CHAR(m.maturity_date,'YYYY-MM-DD'),''),
				  COALESCE(c.confirmation_status,''),
				  COALESCE(c.bank_fd_ref_no,''),
				  COALESCE(c.created_by,''),
				  COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
				  COALESCE(EXTRACT(EPOCH FROM (('` + snapshotDate + `'::timestamp)-c.created_at))/3600, 0),
				  COALESCE(b.booking_status,'')
				FROM investment.fd_confirmation c
				JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id AND b.is_deleted=false
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE COALESCE(c.is_deleted,false)=false
				  AND `+sqlConfirmationAwaitingApproval+`
				  AND `+sqlExcludeTerminalFdOnBooking+`
				  AND ($1::text='' OR b.entity_id=$1)` + snapConfirmationFilter + `
				ORDER BY c.created_at ASC
				LIMIT 100`, entityFilter)
			if err != nil {
				api.LogError("[TreasuryDash] pending_confirmations query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "total": 0}, nil
			}
			defer rows.Close()

			type pendConfRow struct {
				ConfirmationID     string  `json:"confirmation_id"`
				BookingID          string  `json:"booking_id"`
				FDID               string  `json:"fd_id"`
				Entity             string  `json:"entity"`
				Bank               string  `json:"bank"`
				Amount             float64 `json:"amount"`
				Rate               float64 `json:"rate"`
				MaturityDate       string  `json:"maturity_date"`
				ConfirmationStatus string  `json:"confirmation_status"`
				BankFDReference    string  `json:"bank_fd_reference"`
				CreatedBy          string  `json:"created_by"`
				CreatedAt          string  `json:"created_at"`
				ElapsedHours       float64 `json:"elapsed_hours"`
				BookingStatus      string  `json:"booking_status"`
			}
			out := []pendConfRow{}
			for rows.Next() {
				var r pendConfRow
				if err2 := rows.Scan(
					&r.ConfirmationID, &r.BookingID, &r.FDID, &r.Entity, &r.Bank,
					&r.Amount, &r.Rate, &r.MaturityDate, &r.ConfirmationStatus,
					&r.BankFDReference, &r.CreatedBy, &r.CreatedAt, &r.ElapsedHours,
					&r.BookingStatus,
				); err2 != nil {
					continue
				}
				r.Amount = fdRound(r.Amount, 2)
				r.Rate = fdRound(r.Rate, 4)
				r.ElapsedHours = fdRound(r.ElapsedHours, 1)
				out = append(out, r)
			}
			return map[string]interface{}{"rows": out, "total": len(out)}, nil
		})

		// ── 9b2. fd_confirmation approved / confirmed (confirmation.go lifecycle) ──
		run("confirmed_confirmations", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  COALESCE(c.confirmation_id,''),
				  COALESCE(c.booking_id,''),
				  COALESCE(m.fd_id,''),
				  COALESCE(b.entity_name,''),
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,''),
				  COALESCE(c.confirmed_principal_amount, b.principal_amount, 0),
				  COALESCE(c.confirmed_interest_rate, b.interest_rate, m.interest_rate, 0),
				  COALESCE(TO_CHAR(c.confirmed_maturity_date,'YYYY-MM-DD'), TO_CHAR(m.maturity_date,'YYYY-MM-DD'),''),
				  COALESCE(c.confirmation_status,''),
				  COALESCE(c.bank_fd_reference,''),
				  COALESCE(TO_CHAR(c.confirmation_received_date,'YYYY-MM-DD'), TO_CHAR(c.receipt_date,'YYYY-MM-DD'),''),
				  COALESCE(b.booking_status,''),
				  COALESCE(c.created_by,''),
				  COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
				  EXISTS (
				    SELECT 1 FROM investment.fd_master fm
				    WHERE fm.confirmation_id = c.confirmation_id
				      AND COALESCE(fm.is_deleted,false) = false
				  ) AS fd_activated
				FROM investment.fd_confirmation c
				JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id AND b.is_deleted=false
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE COALESCE(c.is_deleted,false)=false
				  AND c.confirmation_status IN ('CONFIRMED','APPROVED','VARIANCE_ACCEPTED')
				  AND `+sqlExcludeTerminalFdOnBooking+`
				  AND ($1::text='' OR b.entity_id=$1)
				ORDER BY c.created_at DESC
				LIMIT 100`, entityFilter)
			if err != nil {
				api.LogError("[TreasuryDash] confirmed_confirmations query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "total": 0}, nil
			}
			defer rows.Close()

			type confDoneRow struct {
				ConfirmationID     string  `json:"confirmation_id"`
				BookingID          string  `json:"booking_id"`
				FDID               string  `json:"fd_id"`
				Entity             string  `json:"entity"`
				Bank               string  `json:"bank"`
				Amount             float64 `json:"amount"`
				Rate               float64 `json:"rate"`
				MaturityDate       string  `json:"maturity_date"`
				ConfirmationStatus string  `json:"confirmation_status"`
				BankFDReference    string  `json:"bank_fd_reference"`
				ReceiptDate        string  `json:"receipt_date"`
				BookingStatus      string  `json:"booking_status"`
				CreatedBy          string  `json:"created_by"`
				CreatedAt          string  `json:"created_at"`
				FDActivated        bool    `json:"fd_activated"`
			}
			out := []confDoneRow{}
			for rows.Next() {
				var r confDoneRow
				if err2 := rows.Scan(
					&r.ConfirmationID, &r.BookingID, &r.FDID, &r.Entity, &r.Bank,
					&r.Amount, &r.Rate, &r.MaturityDate, &r.ConfirmationStatus,
					&r.BankFDReference, &r.ReceiptDate, &r.BookingStatus,
					&r.CreatedBy, &r.CreatedAt, &r.FDActivated,
				); err2 != nil {
					continue
				}
				r.Amount = fdRound(r.Amount, 2)
				r.Rate = fdRound(r.Rate, 4)
				out = append(out, r)
			}
			return map[string]interface{}{"rows": out, "total": len(out)}, nil
		})

		// ── 9c. rollover / closure decisions pending (cimplr initiate/confirm + legacy request) ──
		// Sources, in priority order:
		//   1. cimplr.fd_closure_initiate rows still in INITIATE / CONFIRM stage
		//   2. ACTIVE FDs maturing within 30 days that have NO closure entry at all
		//      (operator must still pick PAYOUT vs ROLLOVER vs PREMATURE).
		//   3. Legacy investment.fd_closure_request rows still in PENDING_APPROVAL.
		run("rollover_pending", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT ref_id, fd_id, entity, bank, principal, rate, maturity_date,
				       closure_status, closure_type, days_to_maturity, maturity_instructions
				FROM (
				  SELECT
				    COALESCE(ci.closure_initiate_id, '') AS ref_id,
				    COALESCE(ci.fd_id, '') AS fd_id,
				    COALESCE(ci.entity_name, b.entity_name, m.entity_name, '') AS entity,
				    COALESCE(ci.bank_name, m.bank_id, b.bank_name, b.bank_id, '') AS bank,
				    COALESCE(ci.principal_amount, m.principal_amount, 0) AS principal,
				    COALESCE(ci.interest_rate, m.interest_rate, 0) AS rate,
				    COALESCE(TO_CHAR(ci.maturity_date, 'YYYY-MM-DD'), TO_CHAR(m.maturity_date, 'YYYY-MM-DD'), '') AS maturity_date,
				    COALESCE(cc.closure_status, ci.closure_status, '') AS closure_status,
				    COALESCE(ci.closure_type, '') AS closure_type,
				    COALESCE((m.maturity_date - ('` + snapshotDate + `'::date))::int, 0) AS days_to_maturity,
				    COALESCE(m.maturity_instructions, '') AS maturity_instructions,
				    -- cimplr.fd_closure_initiate has no created_at column; pull
				    -- the timestamp from the latest audit row instead so newest
				    -- pending decisions surface at the top.
				    COALESCE(ia.requested_at, ('` + snapshotDate + `'::timestamp)) AS sort_ts
				  FROM cimplr.fd_closure_initiate ci
				  JOIN investment.fd_master m ON m.fd_id = ci.fd_id AND m.is_deleted = false
				  LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				  LEFT JOIN LATERAL (
				    SELECT requested_at
				    FROM cimplr.fd_closure_initiate_audit ia
				    WHERE ia.closure_initiate_id = ci.closure_initiate_id
				    ORDER BY ia.requested_at DESC
				    LIMIT 1
				  ) ia ON true
				  LEFT JOIN LATERAL (
				    SELECT cc.closure_status
				    FROM cimplr.fd_closure_confirm cc
				    WHERE cc.closure_initiate_id = ci.closure_initiate_id
				      AND COALESCE(cc.is_deleted, false) = false
				    ORDER BY cc.closure_confirm_id DESC
				    LIMIT 1
				  ) cc ON true
				  WHERE COALESCE(ci.is_deleted, false) = false
				    AND UPPER(COALESCE(ci.closure_type, '')) IN ('ROLLOVER', 'PAYOUT')
				    AND COALESCE(cc.closure_status, ci.closure_status, '') NOT IN ('POSTED', 'REJECTED', 'DELETED')
				    AND NOT (`+sqlCimplrClosureProcessed+`)
				    AND ($1::text = '' OR COALESCE(ci.entity_id, m.entity_id, b.entity_id) = $1)
				    AND (ia.requested_at IS NULL OR ia.requested_at <= ('` + snapshotDate + `'::date + INTERVAL '1 day'))` + snapshotFilter + `
				  UNION ALL
				  -- ACTIVE FDs maturing soon with no closure initiate yet (needs decision)
				  SELECT
				    '' AS ref_id,
				    m.fd_id,
				    COALESCE(b.entity_name, m.entity_name, '') AS entity,
				    COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id, '') AS bank,
				    COALESCE(m.principal_amount, 0) AS principal,
				    COALESCE(m.interest_rate, 0) AS rate,
				    COALESCE(TO_CHAR(m.maturity_date, 'YYYY-MM-DD'), '') AS maturity_date,
				    'PENDING_DECISION' AS closure_status,
				    '' AS closure_type,
				    COALESCE((m.maturity_date - ('` + snapshotDate + `'::date))::int, 0) AS days_to_maturity,
				    COALESCE(m.maturity_instructions, '') AS maturity_instructions,
				    m.maturity_date::timestamptz AS sort_ts
				  FROM investment.fd_master m
				  LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				  WHERE m.is_deleted = false
				    AND m.fd_status IN ('ACTIVE','MATURED')
				    AND m.maturity_date <= ('` + snapshotDate + `'::date) + INTERVAL '30 days'
				    AND NOT EXISTS (
				      SELECT 1 FROM cimplr.fd_closure_initiate ci2
				      WHERE ci2.fd_id = m.fd_id AND COALESCE(ci2.is_deleted,false) = false
				    )
				    AND NOT EXISTS (
				      SELECT 1 FROM cimplr.fd_closure_confirm cc2
				      WHERE cc2.fd_id = m.fd_id AND COALESCE(cc2.is_deleted,false) = false
				    )
				    AND NOT EXISTS (
				      SELECT 1 FROM investment.fd_closure_request cr2
				      WHERE cr2.fd_id = m.fd_id AND COALESCE(cr2.is_deleted,false) = false
				    )
				    AND ($1::text = '' OR COALESCE(m.entity_id, b.entity_id) = $1)` + snapshotFilter + `
				  UNION ALL
				  SELECT
				    COALESCE(cr.closure_request_id::text, ''),
				    COALESCE(cr.fd_id, ''),
				    COALESCE(b.entity_name, m.entity_name, ''),
				    COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id, ''),
				    COALESCE(m.principal_amount, 0),
				    COALESCE(m.interest_rate, 0),
				    COALESCE(TO_CHAR(m.maturity_date, 'YYYY-MM-DD'), ''),
				    COALESCE(cr.closure_status, ''),
				    COALESCE(cr.closure_type, ''),
				    COALESCE((m.maturity_date - ('` + snapshotDate + `'::date))::int, 0),
				    COALESCE(m.maturity_instructions, ''),
				    COALESCE(cr.created_at, ('` + snapshotDate + `'::timestamp))
				  FROM investment.fd_closure_request cr
				  LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id AND m.is_deleted = false
				  LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				  WHERE COALESCE(cr.is_deleted, false) = false
				    AND cr.closure_status = 'PENDING_APPROVAL'
				    AND cr.created_at <= ('` + snapshotDate + `'::date + INTERVAL '1 day')
				    AND ($1::text = '' OR COALESCE(m.entity_id, cr.entity_id, b.entity_id) = $1)` + snapshotFilter + `
				) pending
				ORDER BY sort_ts ASC, maturity_date ASC NULLS LAST
				LIMIT 100`, entityFilter)
			if err != nil {
				api.LogError("[TreasuryDash] rollover_pending query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "total": 0}, nil
			}
			defer rows.Close()

			type rollRow struct {
				ClosureRequestID     string  `json:"closure_request_id"`
				FDID                 string  `json:"fd_id"`
				Entity               string  `json:"entity"`
				Bank                 string  `json:"bank"`
				Principal            float64 `json:"principal"`
				Rate                 float64 `json:"rate"`
				MaturityDate         string  `json:"maturity_date"`
				ClosureStatus        string  `json:"closure_status"`
				ClosureType          string  `json:"closure_type"`
				DaysToMaturity       int     `json:"days_to_maturity"`
				MaturityInstructions string  `json:"maturity_instructions"`
			}
			out := []rollRow{}
			for rows.Next() {
				var r rollRow
				if err2 := rows.Scan(
					&r.ClosureRequestID, &r.FDID, &r.Entity, &r.Bank,
					&r.Principal, &r.Rate, &r.MaturityDate, &r.ClosureStatus,
					&r.ClosureType, &r.DaysToMaturity, &r.MaturityInstructions,
				); err2 != nil {
					continue
				}
				r.Principal = fdRound(r.Principal, 2)
				r.Rate = fdRound(r.Rate, 4)
				out = append(out, r)
			}
			return map[string]interface{}{"rows": out, "total": len(out)}, nil
		})

		// ── 10a. full FD list with all info ─────────────────────────────────────
		// Treasury portfolio view: ACTIVE + PENDING_ACTIVATION rows from fd_master.
		// ACTIVE rows exclude FDs already closed via cimplr / legacy closure workflow.
		// MATURED rows are intentionally excluded — surplus & ladder calcs must
		// not double-count capital that is already released.
		run("fd_list", func(ctx context.Context) (interface{}, error) {
			fdRows, err := pool.Query(ctx, `
				SELECT
				  m.fd_id,
				  COALESCE(m.bank_name, m.bank_id,'') AS bank,
				  COALESCE(b.entity_name, m.entity_name, '') AS entity,
				  COALESCE(m.entity_id, b.entity_id, '') AS entity_id,
				  COALESCE(m.principal_amount,0) AS principal_amount,
				  COALESCE(m.interest_rate,0) AS interest_rate,
				  COALESCE(m.interest_type_code,'') AS interest_type,
				  COALESCE(TO_CHAR(m.start_date,'YYYY-MM-DD'),'') AS start_date,
				  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(m.fd_status,'') AS fd_status,
				  COALESCE(m.maturity_instructions,'') AS maturity_instructions,
				  COALESCE(b.booking_id,'') AS booking_id,
				  COALESCE(b.booking_status,'') AS booking_status,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD'),'') AS booking_date,
				  COALESCE(b.created_by, m.created_by,'') AS created_by,
				  COALESCE((m.maturity_date - ('` + snapshotDate + `'::date))::int, 0) AS days_to_maturity,
				  COALESCE(al.total_interest_accrued, 0) AS interest_accrued,
				  EXISTS (
				    SELECT 1 FROM investment.fd_accrual_ledger al2
				    WHERE al2.fd_id = m.fd_id AND COALESCE(al2.is_deleted, false) = false
				  ) AS has_accrual
				FROM investment.fd_master m
				LEFT JOIN LATERAL (
				  SELECT SUM(COALESCE(period_interest_accrued,0)) AS total_interest_accrued
				  FROM investment.fd_accrual_ledger
				  WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false
				) al ON true
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted = false
				  AND (
				    m.fd_status = 'PENDING_ACTIVATION'
				    OR (
				      m.fd_status = 'ACTIVE'
				      AND NOT EXISTS (
				        SELECT 1 FROM cimplr.fd_closure_confirm cc
				        WHERE cc.fd_id = m.fd_id
				          AND COALESCE(cc.is_deleted,false) = false
				          AND cc.closure_status = 'POSTED'
				          AND COALESCE(cc.accounting_posted,false) = true
				      )
				      AND NOT EXISTS (
				        SELECT 1 FROM investment.fd_closure_request cr
				        WHERE cr.fd_id = m.fd_id
				          AND COALESCE(cr.is_deleted,false) = false
				          AND cr.closure_status IN ('COMPLETED','POSTED','CLOSED')
				      )
				    )
				  )
				  AND ($1::text='' OR COALESCE(m.entity_id, b.entity_id)=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` +
				snapshotFilter + `
				ORDER BY m.maturity_date ASC NULLS LAST
				LIMIT 500`, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				api.LogError("[TreasuryDash] fd_list query error: %v", err)
				return []interface{}{}, nil
			}
			defer fdRows.Close()

			type fdRow struct {
				FDID                 string  `json:"fd_id"`
				Bank                 string  `json:"bank"`
				Entity               string  `json:"entity"`
				EntityID             string  `json:"entity_id"`
				PrincipalAmount      float64 `json:"principal_amount"`
				InterestRate         float64 `json:"interest_rate"`
				InterestType         string  `json:"interest_type"`
				StartDate            string  `json:"start_date"`
				MaturityDate         string  `json:"maturity_date"`
				FDStatus             string  `json:"fd_status"`
				MaturityInstructions string  `json:"maturity_instructions"`
				BookingID            string  `json:"booking_id"`
				BookingStatus        string  `json:"booking_status"`
				BookingDate          string  `json:"booking_date"`
				CreatedBy            string  `json:"created_by"`
				DaysToMaturity       int     `json:"days_to_maturity"`
				InterestAccrued      float64 `json:"interest_accrued"`
				HasAccrual           bool    `json:"has_accrual"`
			}
			out := []fdRow{}
			for fdRows.Next() {
				var fr fdRow
				if err2 := fdRows.Scan(
					&fr.FDID, &fr.Bank, &fr.Entity, &fr.EntityID,
					&fr.PrincipalAmount, &fr.InterestRate, &fr.InterestType,
					&fr.StartDate, &fr.MaturityDate, &fr.FDStatus, &fr.MaturityInstructions,
					&fr.BookingID, &fr.BookingStatus, &fr.BookingDate,
					&fr.CreatedBy, &fr.DaysToMaturity, &fr.InterestAccrued, &fr.HasAccrual,
				); err2 != nil {
					api.LogError("[TreasuryDash] fd_list scan error: %v", err2)
					continue
				}
				fr.PrincipalAmount = fdRound(fr.PrincipalAmount, 2)
				fr.InterestRate = fdRound(fr.InterestRate, 4)
				fr.InterestAccrued = fdRound(fr.InterestAccrued, 2)
				out = append(out, fr)
			}
			return out, nil
		})

		// ── 10. yield curve (rate by interest rate bucket across all banks) ───
		run("yield_curve", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  CASE
				    WHEN m.interest_rate < 5  THEN '<5%'
				    WHEN m.interest_rate < 6  THEN '5-6%'
				    WHEN m.interest_rate < 7  THEN '6-7%'
				    WHEN m.interest_rate < 8  THEN '7-8%'
				    WHEN m.interest_rate < 9  THEN '8-9%'
				    ELSE '9%+'
				  END AS bucket,
				  COALESCE(
				    SUM(m.principal_amount * m.interest_rate) / NULLIF(SUM(m.principal_amount),0),
				    0
				  ) AS avg_rate,
				  COALESCE(SUM(m.principal_amount),0) AS exposure
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR b.entity_id=$1)
				  AND ($2::text='' OR m.interest_type_code=$2)
				  AND ($3::text='' OR m.bank_id=$3)` +
				snapshotFilter + `
				GROUP BY 1
				ORDER BY MIN(m.interest_rate)`, entityFilter, fdTypeFilter, bankFilter)
			if err != nil {
				return []interface{}{}, nil
			}
			defer rows.Close()

			type ycRow struct {
				Bucket   string  `json:"bucket"`
				AvgRate  float64 `json:"avg_rate"`
				Exposure float64 `json:"exposure"`
			}
			out := []ycRow{}
			for rows.Next() {
				var yr ycRow
				if err2 := rows.Scan(&yr.Bucket, &yr.AvgRate, &yr.Exposure); err2 == nil {
					yr.AvgRate = fdRound(yr.AvgRate, 2)
					yr.Exposure = fdRound(yr.Exposure, 2)
					out = append(out, yr)
				}
			}
			return out, nil
		})

		// ── 10b. interest_trend (same logic as CFO dashboard — cashflow + ledger fallback)
		run("interest_trend", func(ctx context.Context) (interface{}, error) {
			daily, dErr := buildInterestTrendSeries(ctx, pool, entityFilter, "DAY", snapshotDate)
			if dErr != nil {
				daily = []interestTrendRow{}
			}
			monthly, mErr := buildInterestTrendSeries(ctx, pool, entityFilter, "MONTH", snapshotDate)
			if mErr != nil {
				monthly = []interestTrendRow{}
			}
			yearly, yErr := buildInterestTrendSeries(ctx, pool, entityFilter, "YEAR", snapshotDate)
			if yErr != nil {
				yearly = []interestTrendRow{}
			}
			return map[string]interface{}{
				"daily":   daily,
				"monthly": monthly,
				"yearly":  yearly,
				"weekly":  daily,
			}, nil
		})

		// ── 11. approval_workflow (TC-117) ────────────────────────────────────
		// Concise per-instance approval status for FD bookings/confirmations
		// so the Treasury dashboard can render the multi-step workflow
		// (Maker → Checker → Approver → Resolved) and highlight what is stuck.
		run("approval_workflow", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  i.instance_id,
				  i.transaction_type,
				  i.record_id,
				  i.status                                                         AS instance_status,
				  COALESCE(i.submitted_by_email,'')                                AS submitted_by,
				  COALESCE(TO_CHAR(i.submitted_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')   AS submitted_at,
				  COALESCE(i.resolved_by_email,'')                                 AS resolved_by,
				  COALESCE(TO_CHAR(i.resolved_at,'YYYY-MM-DD"T"HH24:MI:SS'),'')    AS resolved_at,
				  COALESCE(
				    (SELECT bx.principal_amount FROM investment.fd_booking_request bx
				     WHERE bx.booking_id = i.record_id AND COALESCE(bx.is_deleted,false)=false LIMIT 1),
				    (SELECT crx.principal_amount FROM investment.fd_closure_request crx
				     WHERE crx.closure_request_id = i.record_id LIMIT 1),
				    (SELECT b3.principal_amount FROM investment.fd_confirmation c3
				     JOIN investment.fd_booking_request b3 ON b3.booking_id = c3.booking_id
				     WHERE c3.confirmation_id = i.record_id AND COALESCE(c3.is_deleted,false)=false LIMIT 1),
				    (SELECT m2.principal_amount FROM investment.fd_master m2
				     WHERE m2.fd_id = i.record_id AND COALESCE(m2.is_deleted,false)=false LIMIT 1),
				    0
				  )                                                                  AS amount,
				  COALESCE(
				    NULLIF(TRIM(COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id, '')),''),
				    (SELECT COALESCE(mx.bank_name, mx.bank_id,'')
				     FROM investment.fd_closure_request crx
				     JOIN investment.fd_master mx ON mx.fd_id = crx.fd_id AND COALESCE(mx.is_deleted,false)=false
				     WHERE crx.closure_request_id = i.record_id LIMIT 1),
				    (SELECT COALESCE(b3.bank_name, b3.bank_id,'')
				     FROM investment.fd_confirmation c3
				     JOIN investment.fd_booking_request b3 ON b3.booking_id = c3.booking_id
				     WHERE c3.confirmation_id = i.record_id AND COALESCE(c3.is_deleted,false)=false LIMIT 1),
				    (SELECT COALESCE(mx.bank_name, mx.bank_id,'')
				     FROM investment.fd_master mx
				     WHERE mx.fd_id = i.record_id AND COALESCE(mx.is_deleted,false)=false LIMIT 1),
				    ''
				  )                                                                  AS bank,
				  COALESCE(
				    NULLIF(TRIM(COALESCE(b.entity_name,'')),''),
				    (SELECT COALESCE(mx.entity_name,'')
				     FROM investment.fd_closure_request crx
				     JOIN investment.fd_master mx ON mx.fd_id = crx.fd_id AND COALESCE(mx.is_deleted,false)=false
				     WHERE crx.closure_request_id = i.record_id LIMIT 1),
				    (SELECT COALESCE(b3.entity_name,'')
				     FROM investment.fd_confirmation c3
				     JOIN investment.fd_booking_request b3 ON b3.booking_id = c3.booking_id
				     WHERE c3.confirmation_id = i.record_id AND COALESCE(c3.is_deleted,false)=false LIMIT 1),
				    (SELECT COALESCE(mx.entity_name,'')
				     FROM investment.fd_master mx
				     WHERE mx.fd_id = i.record_id AND COALESCE(mx.is_deleted,false)=false LIMIT 1),
				    ''
				  )                                                                  AS entity,
				  COALESCE(
				    b.booking_status,
				    (SELECT b2.booking_status FROM investment.fd_closure_request crx
				     JOIN investment.fd_booking_request b2 ON b2.booking_id = crx.booking_id
				     WHERE crx.closure_request_id = i.record_id LIMIT 1),
				    (SELECT b3.booking_status FROM investment.fd_confirmation c3
				     JOIN investment.fd_booking_request b3 ON b3.booking_id = c3.booking_id
				     WHERE c3.confirmation_id = i.record_id AND COALESCE(c3.is_deleted,false)=false LIMIT 1),
				    ''
				  )                                                                  AS booking_status,
				  COALESCE(SUM(ie.approvals_received)::int, 0)                     AS approvals_received,
				  COALESCE(SUM(ie.approvals_required)::int, 0)                     AS approvals_required,
				  COALESCE(MAX(ie.position), 1)                                    AS total_steps,
				  COALESCE((
				     SELECT MIN(ie2.position) FROM uam.approval_instance_eye ie2
				     WHERE ie2.instance_id = i.instance_id AND ie2.status='ACTIVE'
				  ), 0)                                                            AS current_step,
				  COALESCE(BOOL_OR(ie.is_escalated), false)                        AS is_escalated,
				  COALESCE(MIN(ie.sla_deadline), NULL)                             AS sla_deadline
				FROM uam.approval_instance i
				LEFT JOIN uam.approval_instance_eye ie ON ie.instance_id = i.instance_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = i.record_id AND COALESCE(b.is_deleted,false)=false
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE i.is_deleted = false
				  AND i.module_code = 'FIXED_DEPOSIT'
				  AND i.submitted_at >= $1::date
				  AND i.submitted_at <= ('` + snapshotDate + `'::date + INTERVAL '1 day')
				  AND ($2::text='' OR i.entity_code=$2 OR b.entity_id=$2)
				GROUP BY i.instance_id, i.transaction_type, i.record_id, i.status,
				         i.submitted_by_email, i.submitted_at, i.resolved_by_email, i.resolved_at
				ORDER BY i.submitted_at DESC
				LIMIT 60`, startDateStr, entityFilter)
			if err != nil {
				api.LogError("[TreasuryDash] approval_workflow query error: %v", err)
				return map[string]interface{}{
					"rows": []interface{}{}, "total": 0,
					"pending": 0, "approved": 0, "rejected": 0, "escalated": 0,
				}, nil
			}
			defer rows.Close()

			type wfRow struct {
				InstanceID        string     `json:"instance_id"`
				TransactionType   string     `json:"transaction_type"`
				RecordID          string     `json:"record_id"`
				InstanceStatus    string     `json:"instance_status"`
				SubmittedBy       string     `json:"submitted_by"`
				SubmittedAt       string     `json:"submitted_at"`
				ResolvedBy        string     `json:"resolved_by,omitempty"`
				ResolvedAt        string     `json:"resolved_at,omitempty"`
				Amount            float64    `json:"amount"`
				Bank              string     `json:"bank"`
				Entity            string     `json:"entity"`
				BookingStatus     string     `json:"booking_status"`
				ApprovalsReceived int        `json:"approvals_received"`
				ApprovalsRequired int        `json:"approvals_required"`
				CurrentStep       int        `json:"current_step"`
				TotalSteps        int        `json:"total_steps"`
				IsEscalated       bool       `json:"is_escalated"`
				SlaDeadline       *time.Time `json:"sla_deadline,omitempty"`
				ProgressPct       int        `json:"progress_pct"`
				StatusLabel       string     `json:"status_label"`
			}
			out := []wfRow{}
			pending, approved, rejected, escalated := 0, 0, 0, 0
			for rows.Next() {
				var w wfRow
				if err2 := rows.Scan(
					&w.InstanceID, &w.TransactionType, &w.RecordID, &w.InstanceStatus,
					&w.SubmittedBy, &w.SubmittedAt, &w.ResolvedBy, &w.ResolvedAt,
					&w.Amount, &w.Bank, &w.Entity, &w.BookingStatus,
					&w.ApprovalsReceived, &w.ApprovalsRequired,
					&w.TotalSteps, &w.CurrentStep, &w.IsEscalated, &w.SlaDeadline,
				); err2 != nil {
					api.LogError("[TreasuryDash] approval_workflow scan error: %v", err2)
					continue
				}
				w.Amount = fdRound(w.Amount, 2)
				if w.ApprovalsRequired > 0 {
					w.ProgressPct = int(float64(w.ApprovalsReceived) / float64(w.ApprovalsRequired) * 100)
				}
				switch w.InstanceStatus {
				case "PENDING", constants.StatusActive, "SUBMITTED":
					pending++
					w.StatusLabel = "In Progress"
				case constants.StatusApproved, "RESOLVED", "COMPLETED":
					approved++
					w.StatusLabel = "Approved"
				case constants.StatusRejected, "DECLINED":
					rejected++
					w.StatusLabel = "Rejected"
				default:
					w.StatusLabel = w.InstanceStatus
				}
				if w.IsEscalated {
					escalated++
				}
				out = append(out, w)
			}
			return map[string]interface{}{
				"rows":      out,
				"total":     len(out),
				"pending":   pending,
				"approved":  approved,
				"rejected":  rejected,
				"escalated": escalated,
			}, nil
		})

		// ── 12. lifecycle_summary (TC-120) ────────────────────────────────────
		// Single source of truth for the end-to-end FD workflow on the Treasury
		// dashboard so that figures across the page never disagree:
		//   Booking → Approval → Sent to Bank → Confirmed → Activated → Matured
		run("lifecycle_summary", func(ctx context.Context) (interface{}, error) {
			type stage struct {
				ID         string  `json:"id"`
				Label      string  `json:"label"`
				Count      int64   `json:"count"`
				Amount     float64 `json:"amount"`
				Stuck      int64   `json:"stuck"`
				StuckLabel string  `json:"stuck_label,omitempty"`
			}

			var bkTotal, bkApprovalPending int64
			var bkAmt float64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*),
				       COUNT(*) FILTER (WHERE `+sqlBookingAwaitingApproval+`),
				       COALESCE(SUM(principal_amount),0)
				FROM investment.fd_booking_request b
				WHERE b.is_deleted=false
				  AND `+sqlExcludeTerminalFdOnBooking+`
				  AND b.created_at >= $2::date AND b.created_at <= ($3::date + INTERVAL '1 day')
				  AND ($1::text='' OR b.entity_id=$1)`,
				entityFilter, startDateStr, endDateStr).Scan(&bkTotal, &bkApprovalPending, &bkAmt)

			var apTotal int64
			var apAmt float64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*), COALESCE(SUM(principal_amount),0)
				FROM investment.fd_booking_request
				WHERE is_deleted=false
				  AND booking_status='APPROVED'
				  AND created_at >= $2::date AND created_at <= ($3::date + INTERVAL '1 day')
				  AND ($1::text='' OR entity_id=$1)`,
				entityFilter, startDateStr, endDateStr).Scan(&apTotal, &apAmt)

			var sentTotal, sentStuck int64
			var sentAmt float64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*),
				       COUNT(*) FILTER (WHERE EXTRACT(DAY FROM (('` + snapshotDate + `'::timestamp)-created_at)) >= 2),
				       COALESCE(SUM(principal_amount),0)
				FROM investment.fd_booking_request
				WHERE is_deleted=false
				  AND booking_status='SENT_TO_BANK'
				  AND created_at >= $2::date AND created_at <= ($3::date + INTERVAL '1 day')
				  AND ($1::text='' OR entity_id=$1)`,
				entityFilter, startDateStr, endDateStr).Scan(&sentTotal, &sentStuck, &sentAmt)

			var confTotal int64
			var confAmt float64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*), COALESCE(SUM(c.actual_principal),0)
				FROM investment.fd_confirmation c
				JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id AND b.is_deleted=false
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE COALESCE(c.is_deleted,false)=false
				  AND `+sqlExcludeTerminalFdOnBooking+`
				  AND COALESCE(b.created_at, c.created_at) >= $2::date
				  AND COALESCE(b.created_at, c.created_at) <= ($3::date + INTERVAL '1 day')
				  AND ($1::text='' OR COALESCE(b.entity_id,m.entity_id)=$1)`,
				entityFilter, startDateStr, endDateStr).Scan(&confTotal, &confAmt)

			var activeTotal int64
			var activeAmt float64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*), COALESCE(SUM(principal_amount),0)
				FROM investment.fd_master
				WHERE is_deleted=false
				  AND fd_status='ACTIVE'
				  AND ($1::text='' OR entity_id=$1)`, entityFilter).Scan(&activeTotal, &activeAmt)

			var maturedTotal int64
			var maturedAmt float64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*), COALESCE(SUM(principal_amount),0)
				FROM investment.fd_master
				WHERE is_deleted=false
				  AND fd_status IN ('MATURED','CLOSED')
				  AND ($1::text='' OR entity_id=$1)`, entityFilter).Scan(&maturedTotal, &maturedAmt)

			stages := []stage{
				{ID: "booking", Label: "Booking Created", Count: bkTotal, Amount: fdRound(bkAmt, 2), Stuck: bkApprovalPending, StuckLabel: "awaiting approval"},
				{ID: "approved", Label: "Approved", Count: apTotal, Amount: fdRound(apAmt, 2)},
				{ID: "sent", Label: "Sent to Bank", Count: sentTotal, Amount: fdRound(sentAmt, 2), Stuck: sentStuck, StuckLabel: "≥2 days unconfirmed"},
				{ID: "confirmed", Label: "Confirmed by Bank", Count: confTotal, Amount: fdRound(confAmt, 2)},
				{ID: "active", Label: "Active FDs", Count: activeTotal, Amount: fdRound(activeAmt, 2)},
				{ID: "matured", Label: "Matured / Closed", Count: maturedTotal, Amount: fdRound(maturedAmt, 2)},
			}

			conversion := 0.0
			if bkTotal > 0 {
				conversion = fdRound(float64(confTotal)/float64(bkTotal)*100, 1)
			}
			bottleneck := ""
			maxDrop := int64(0)
			for i := 1; i < len(stages); i++ {
				drop := stages[i-1].Count - stages[i].Count
				if drop > maxDrop {
					maxDrop = drop
					bottleneck = stages[i-1].Label + " → " + stages[i].Label
				}
			}
			return map[string]interface{}{
				"stages":         stages,
				"conversion_pct": conversion,
				"bottleneck":     bottleneck,
			}, nil
		})

		// ── interest_income — from fd_interest_receipt (MATCHED + POSTED + not deleted) ──
		run("interest_income", func(ctx context.Context) (interface{}, error) {
			var mtd, qtd, ytd, received float64
			err := pool.QueryRow(ctx, `
				SELECT
				  COALESCE(SUM(CASE
				    WHEN ir.receipt_date >= DATE_TRUNC('month', ('` + snapshotDate + `'::date))
				    THEN ir.gross_interest_received ELSE 0 END), 0) AS mtd,
				  COALESCE(SUM(CASE
				    WHEN ir.receipt_date >= DATE_TRUNC('quarter', ('` + snapshotDate + `'::date))
				    THEN ir.gross_interest_received ELSE 0 END), 0) AS qtd,
				  COALESCE(SUM(CASE
				    WHEN ir.receipt_date >= (
				      CASE WHEN EXTRACT(MONTH FROM ('` + snapshotDate + `'::date)) >= 4
				           THEN DATE_TRUNC('year', ('` + snapshotDate + `'::date)) + INTERVAL '3 months'
				           ELSE DATE_TRUNC('year', ('` + snapshotDate + `'::date)) - INTERVAL '9 months'
				      END)
				    THEN ir.gross_interest_received ELSE 0 END), 0) AS ytd,
				  COALESCE(SUM(CASE
				    WHEN ir.receipt_status = 'POSTED'
				    THEN ir.gross_interest_received ELSE 0 END), 0) AS received
				FROM investment.fd_interest_receipt ir
				WHERE ir.is_deleted = false
				  AND ir.reconcile_status = 'MATCHED'
				  AND ir.receipt_date <= ('` + snapshotDate + `'::date)
				  AND ($1::text='' OR ir.entity_id=$1)
				  AND ($2::text='' OR ir.bank_id=$2)`,
				entityFilter, bankFilter).Scan(&mtd, &qtd, &ytd, &received)
			if err != nil {
				api.LogError("[TreasuryDash] interest_income query error: %v", err)
				return map[string]interface{}{
					"mtd_accrued": 0.0, "qtd_accrued": 0.0,
					"ytd_accrued": 0.0, "received": 0.0,
				}, nil
			}
			return map[string]interface{}{
				"mtd_accrued": fdRound(mtd, 2),
				"qtd_accrued": fdRound(qtd, 2),
				"ytd_accrued": fdRound(ytd, 2),
				"received":    fdRound(received, 2),
			}, nil
		})

		// wait for all
		wg.Wait()

		get := func(key string) interface{} {
			if r, ok := results[key]; ok && r.err == nil {
				return r.data
			}
			return nil
		}

		payload := map[string]interface{}{
			"generated_at": now.Format(time.RFC3339),
			"filters": map[string]interface{}{
				"entity_id":  entityFilter,
				"currency":   req.Currency,
				"period":     periodBounds.Period,
				"start_date": startDateStr,
				"end_date":   endDateStr,
			},
			"kpis": map[string]interface{}{
				"surplus_deployment": get("surplus_deployment"),
				"fd_count":           get("fd_count_active"),
				"near_maturity":      get("near_maturity"),
				"negotiations":       get("negotiations"),
				"interest_income":    get("interest_income"),
			},
			"charts": map[string]interface{}{
				"maturity_ladder":       get("maturity_ladder_treasury"),
				"interest_trend":        get("interest_trend"),
				"deployment_by_bank":    get("deployment_by_bank"),
				"yield_by_bank":         get("yield_by_bank"),
				"rate_by_bank":          get("rate_by_bank"),
				"yield_curve":           get("yield_curve"),
				"interest_income_trend": get("interest_trend"),
			},
			"tables": map[string]interface{}{
				"near_maturity_fds":       get("near_maturity"),
				"booking_confirmations":   get("booking_confirmations"),
				"pending_confirmations":   get("pending_confirmations"),
				"confirmed_confirmations": get("confirmed_confirmations"),
				"rollover_pending":        get("rollover_pending"),
				"negotiations":            get("negotiations"),
				"fd_list":                 get("fd_list"),
				"approval_workflow":       get("approval_workflow"),
			},
			"fd_list":                 get("fd_list"),
			"pending_confirmations":   get("pending_confirmations"),
			"confirmed_confirmations": get("confirmed_confirmations"),
			"rollover_pending":        get("rollover_pending"),
			"approval_workflow":       get("approval_workflow"),
			"lifecycle_summary":       get("lifecycle_summary"),
			"period_start":            periodBounds.StartStr,
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}

// fmtApprovalProgress renders an "n/N approvals" label.
func fmtApprovalProgress(received, required int) string {
	if required <= 0 {
		return ""
	}
	return strconv.Itoa(received) + "/" + strconv.Itoa(required) + " approvals"
}

// countSLAStatus counts rows of a specific SLA status in the booking confirmations result.
// Uses interface{} slice to avoid importing reflect.
func countSLAStatus(rows interface{}, status string) int {
	type confRow struct {
		SLAStatus string `json:"sla_status"`
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return 0
	}
	var items []confRow
	if err := json.Unmarshal(b, &items); err != nil {
		return 0
	}
	count := 0
	for _, item := range items {
		if item.SLAStatus == status {
			count++
		}
	}
	return count
}
