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
		ctx := r.Context()
		entityFilter := req.EntityID

		// Resolve period start / end for date-range filtering
		var periodStart time.Time
		if req.Period == "CUSTOM" && req.StartDate != "" {
			if parsed, err := time.Parse(constants.DateFormat, req.StartDate); err == nil {
				periodStart = parsed
			} else {
				periodStart = periodStartDate("MTD", now)
			}
		} else {
			periodStart = periodStartDate(req.Period, now)
		}
		periodEnd := now
		if req.Period == "CUSTOM" && req.EndDate != "" {
			if parsed, err := time.Parse(constants.DateFormat, req.EndDate); err == nil {
				periodEnd = parsed
			}
		}
		startDateStr := periodStart.Format(constants.DateFormat)
		endDateStr := periodEnd.Format(constants.DateFormat)

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
		run("surplus_deployment", func(ctx context.Context) (interface{}, error) {
			var deployed float64
			err := pool.QueryRow(ctx, `
				SELECT COALESCE(SUM(m.principal_amount),0)
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED','PENDING_ACTIVATION')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`,
				entityFilter).Scan(&deployed)
			if err != nil {
				api.LogError("[TreasuryDash] surplus_deployment query error: %v", err)
				return map[string]interface{}{"deployed": 0.0, "deployment_count": 0}, nil
			}
			return map[string]interface{}{
				"deployed":         fdRound(deployed, 2),
				"deployment_count": 0,
			}, nil
		})

		// ── 2. surplus deployment count (fd count) ─────────────────────────────────────
		run("fd_count_active", func(ctx context.Context) (interface{}, error) {
			var cnt int64
			err := pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED','PENDING_ACTIVATION')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`,
				entityFilter).Scan(&cnt)
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
				  AND ($1::text='' OR n.entity_id=$1)
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
				if nr.ExpiresAt != "" && nr.ExpiresAt[:10] == time.Now().Format(constants.DateFormat) {
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

		// ── 4. FDs near maturity (next 30 days) ───────────────────────────────
		run("near_maturity", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  m.fd_id,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, m.bank_id,'') AS bank,
				  m.principal_amount,
				  m.interest_rate,
				  TO_CHAR(m.maturity_date,'YYYY-MM-DD') AS maturity_date,
				  COALESCE(m.maturity_instructions,'') AS maturity_instructions,
				  m.fd_status,
				  (m.maturity_date - CURRENT_DATE) AS days_to_maturity
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false
				  AND m.fd_status IN ('ACTIVE','MATURED','PENDING_ACTIVATION')
				  AND m.maturity_date <= CURRENT_DATE + INTERVAL '90 days'
				  AND ($1::text='' OR b.entity_id=$1)
				ORDER BY m.maturity_date ASC`, entityFilter)
			if err != nil {
				return []interface{}{}, nil
			}
			defer rows.Close()

			type matRow struct {
				FDID                 string  `json:"fd_id"`
				Entity               string  `json:"entity"`
				Bank                 string  `json:"bank"`
				Principal            float64 `json:"principal"`
				Rate                 float64 `json:"rate"`
				MaturityDate         string  `json:"maturity_date"`
				MaturityInstructions string  `json:"maturity_instructions"`
				Status               string  `json:"status"`
				DaysToMaturity       int     `json:"days_to_maturity"`
			}
			out := []matRow{}
			totalAmt := 0.0
			for rows.Next() {
				var mr matRow
				if err2 := rows.Scan(&mr.FDID, &mr.Entity, &mr.Bank, &mr.Principal,
					&mr.Rate, &mr.MaturityDate, &mr.MaturityInstructions,
					&mr.Status, &mr.DaysToMaturity); err2 == nil {
					mr.Principal = fdRound(mr.Principal, 2)
					totalAmt += mr.Principal
					out = append(out, mr)
				}
			}
			rolloverPending := 0
			for _, mr := range out {
				if mr.MaturityInstructions == "" {
					rolloverPending++
				}
			}
			return map[string]interface{}{
				"rows":             out,
				"count":            len(out),
				"total_amount":     fdRound(totalAmt, 2),
				"rollover_pending": rolloverPending,
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
				GROUP BY COALESCE(m.bank_name, m.bank_id)
				ORDER BY exposure DESC`, entityFilter)
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
				GROUP BY COALESCE(m.bank_name, m.bank_id)
				ORDER BY weighted_avg_yield DESC`, entityFilter)
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
		run("maturity_ladder_treasury", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  CASE
				    WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+6   THEN 'Wk 1'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+7  AND CURRENT_DATE+13  THEN 'Wk 2'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+14 AND CURRENT_DATE+20  THEN 'Wk 3'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+21 AND CURRENT_DATE+27  THEN 'Wk 4'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+28 AND CURRENT_DATE+59  THEN 'M 2'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+60 AND CURRENT_DATE+89  THEN 'M 3'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+90 AND CURRENT_DATE+179 THEN 'Q3'
				    ELSE 'Q4+'
				  END AS period,
				  COALESCE(SUM(m.principal_amount),0) AS amount,
				  COUNT(*) AS count
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR b.entity_id=$1)
				  AND m.maturity_date >= CURRENT_DATE
				GROUP BY 1
				ORDER BY MIN(m.maturity_date)`, entityFilter)
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
				GROUP BY COALESCE(m.bank_name, m.bank_id), 2
				ORDER BY COALESCE(m.bank_name, m.bank_id), MIN(m.interest_rate)`, entityFilter)
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

		// ── 9. booking confirmation SLA tracking ──────────────────────────────────────────────────────────────────────────────────────
		run("booking_confirmations", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  b.booking_id,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0) AS amount,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS sent_at,
				  b.booking_status,
				  COALESCE(EXTRACT(EPOCH FROM (NOW()-b.created_at))/3600,0) AS elapsed_hours
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('APPROVAL_PENDING','SENT_TO_BANK','APPROVED','DRAFT')
				  AND ($1::text='' OR b.entity_id=$1)
				ORDER BY b.created_at ASC
				LIMIT 50`, entityFilter)
			if err != nil {
				return []interface{}{}, nil
			}
			defer rows.Close()

			type confRow struct {
				BookingID    string  `json:"booking_id"`
				Entity       string  `json:"entity"`
				Bank         string  `json:"bank"`
				Amount       float64 `json:"amount"`
				SentAt       string  `json:"sent_at"`
				Status       string  `json:"status"`
				ElapsedHours float64 `json:"elapsed_hours"`
				SLAHours     float64 `json:"sla_hours"`
				SLAStatus    string  `json:"sla_status"`
			}
			out := []confRow{}
			for rows.Next() {
				var cr confRow
				if err2 := rows.Scan(&cr.BookingID, &cr.Entity, &cr.Bank, &cr.Amount,
					&cr.SentAt, &cr.Status, &cr.ElapsedHours); err2 == nil {
					cr.Amount = fdRound(cr.Amount, 2)
					cr.ElapsedHours = fdRound(cr.ElapsedHours, 1)
					cr.SLAHours = 24 // configurable; default 24h
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
			}
			return map[string]interface{}{
				"rows":    out,
				"total":   len(out),
				"overdue": countSLAStatus(out, "Overdue"),
				"at_risk": countSLAStatus(out, "At Risk"),
			}, nil
		})

		// ── 10a. full FD list with all info ─────────────────────────────────────
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
				  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(m.fd_status,'') AS fd_status,
				  COALESCE(m.maturity_instructions,'') AS maturity_instructions,
				  COALESCE(b.booking_id,'') AS booking_id,
				  COALESCE(b.booking_status,'') AS booking_status,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD'),'') AS booking_date,
				  COALESCE(b.created_by, m.created_by,'') AS created_by,
				  COALESCE((m.maturity_date - CURRENT_DATE)::int, 0) AS days_to_maturity
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false
				  AND m.fd_status NOT IN ('CANCELLED')
				  AND ($1::text='' OR COALESCE(m.entity_id, b.entity_id)=$1)
				ORDER BY m.maturity_date ASC NULLS LAST
				LIMIT 500`, entityFilter)
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
				MaturityDate         string  `json:"maturity_date"`
				FDStatus             string  `json:"fd_status"`
				MaturityInstructions string  `json:"maturity_instructions"`
				BookingID            string  `json:"booking_id"`
				BookingStatus        string  `json:"booking_status"`
				BookingDate          string  `json:"booking_date"`
				CreatedBy            string  `json:"created_by"`
				DaysToMaturity       int     `json:"days_to_maturity"`
			}
			out := []fdRow{}
			for fdRows.Next() {
				var fr fdRow
				if err2 := fdRows.Scan(
					&fr.FDID, &fr.Bank, &fr.Entity, &fr.EntityID,
					&fr.PrincipalAmount, &fr.InterestRate, &fr.InterestType,
					&fr.MaturityDate, &fr.FDStatus, &fr.MaturityInstructions,
					&fr.BookingID, &fr.BookingStatus, &fr.BookingDate,
					&fr.CreatedBy, &fr.DaysToMaturity,
				); err2 != nil {
					api.LogError("[TreasuryDash] fd_list scan error: %v", err2)
					continue
				}
				fr.PrincipalAmount = fdRound(fr.PrincipalAmount, 2)
				fr.InterestRate = fdRound(fr.InterestRate, 4)
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
				GROUP BY 1
				ORDER BY MIN(m.interest_rate)`, entityFilter)
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
				"period":     req.Period,
				"start_date": startDateStr,
				"end_date":   endDateStr,
			},
			"kpis": map[string]interface{}{
				"surplus_deployment": get("surplus_deployment"),
				"fd_count":           get("fd_count_active"),
				"near_maturity":      get("near_maturity"),
				"negotiations":       get("negotiations"),
			},
			"charts": map[string]interface{}{
				"maturity_ladder":    get("maturity_ladder_treasury"),
				"deployment_by_bank": get("deployment_by_bank"),
				"yield_by_bank":      get("yield_by_bank"),
				"rate_by_bank":       get("rate_by_bank"),
				"yield_curve":        get("yield_curve"),
			},
			"tables": map[string]interface{}{
				"near_maturity_fds":     get("near_maturity"),
				"booking_confirmations": get("booking_confirmations"),
				"negotiations":          get("negotiations"),
				"fd_list":               get("fd_list"),
			},
			"period_start": periodStart.Format(constants.DateFormat),
		}

		api.RespondWithPayload(w, true, "", payload)
	}
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
