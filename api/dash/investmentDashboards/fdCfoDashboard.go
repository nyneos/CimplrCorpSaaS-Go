// Package investmentdashboards — FD CFO Dashboard
//
// POST /dash/investment/fd/cfo-dashboard
//
// Returns a single aggregated JSON payload covering every KPI, chart, governance
// and FD-list panel required by the CFO's Fixed Deposit dashboard.
// All sub-computations run concurrently via sync.WaitGroup + goroutines so
// latency is bounded by the single slowest query, not their sum.
//
// Request:
//
//	{
//	  "user_id":   "...",          // optional — for session scoping
//	  "entity_id": "",             // "" = all entities
//	  "currency":  "INR",         // default INR
//	  "period":    "MTD"          // MTD | QTD | YTD — controls interest roll-up
//	}
//
// Response shape mirrors the spec exactly:
//
//	{
//	  "success": true,
//	  "generated_at": "<RFC3339>",
//	  "filters": { "entity_id":"", "currency":"INR", "period":"MTD" },
//	  "kpis": { total_exposure, bank_concentration, maturity, interest, exceptions },
//	  "charts": { maturity_ladder, interest_trend, rate_distribution, bank_concentration },
//	  "governance": { approvals, closing_status },
//	  "fd_list": [ ... ]
//	}
package investmentdashboards

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── request / response types ────────────────────────────────────────────────

type fdCfoDashRequest struct {
	UserID            string `json:"user_id"`
	EntityID          string `json:"entity_id"`
	Currency          string `json:"currency"`
	Period            string `json:"period"`     // MTD | QTD | YTD | CUSTOM
	StartDate         string `json:"start_date"` // YYYY-MM-DD — used when Period=="CUSTOM"
	EndDate           string `json:"end_date"`   // YYYY-MM-DD — used when Period=="CUSTOM"
	AsOnDate          string `json:"as_on_date"` // optional snapshot date (default = today)
	Bank              string `json:"bank"`
	FDStatus          string `json:"fd_status"`           // ACTIVE | NEAR_MATURITY | MATURED | CLOSED
	FDType            string `json:"fd_type"`             // SIMPLE | COMPOUNDING
	InterestFrequency string `json:"interest_frequency"`  // PAYOUT | COMPOUNDING
	LadderView        string `json:"ladder_view"`         // WEEK | MONTH | YEAR — Maturity Ladder bucket size (default WEEK)
}

// roundN rounds v to n decimal places.
func fdRound(v float64, n int) float64 {
	p := math.Pow(10, float64(n))
	return math.Round(v*p) / p
}

// periodStartDate returns the start of the requested period (MTD / QTD / YTD).
func periodStartDate(period string, now time.Time) time.Time {
	switch period {
	case "QTD":
		// Financial quarter: Apr–Jun, Jul–Sep, Oct–Dec, Jan–Mar
		m := int(now.Month())
		var qStart int
		switch {
		case m >= 4 && m <= 6:
			qStart = 4
		case m >= 7 && m <= 9:
			qStart = 7
		case m >= 10 && m <= 12:
			qStart = 10
		default:
			qStart = 1
		}
		return time.Date(now.Year(), time.Month(qStart), 1, 0, 0, 0, 0, now.Location())
	case "YTD":
		fy := now.Year()
		if now.Month() < time.April {
			fy--
		}
		return time.Date(fy, time.April, 1, 0, 0, 0, 0, now.Location())
	default: // MTD
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	}
}

// ─── handler ─────────────────────────────────────────────────────────────────

// GetFDCfoDashboard returns the full FD CFO dashboard payload in a single call.
func GetFDCfoDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req fdCfoDashRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Currency == "" {
			req.Currency = "INR"
		}
		if req.Period == "" {
			req.Period = "MTD"
		}

		now := time.Now().UTC()
		var periodStart time.Time
		if req.Period == "CUSTOM" && req.StartDate != "" {
			if parsed, err2 := time.Parse(constants.DateFormat, req.StartDate); err2 == nil {
				periodStart = parsed
			} else {
				periodStart = periodStartDate("MTD", now)
			}
		} else {
			periodStart = periodStartDate(req.Period, now)
		}
		ctx := r.Context()

		// Build optional entity filter SQL fragment (used across many queries)
		entityFilter := req.EntityID
		// Surface the fd_status filter early — `total_exposure` (and other
		// early-registered widgets) need access to it via closure.
		fdStatusFilter := req.FDStatus

		// ── concurrent sub-computations ──────────────────────────────────────
		type subResult struct {
			data interface{}
			err  error
		}
		results := make(map[string]subResult, 8)
		var mu sync.Mutex
		var wg sync.WaitGroup

		run := func(key string, fn func(context.Context) (interface{}, error)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data, err := fn(ctx)
				mu.Lock()
				results[key] = subResult{data, err}
				mu.Unlock()
			}()
		}

		// ── 1. total_exposure ─────────────────────────────────────────────────
		// Sum principal across every non-deleted FD in scope. We deliberately
		// do *not* filter by fd_status here — the FD register tile shows
		// "X instruments in scope" using the same population, and any
		// status-narrowing the user wants is already applied through the
		// dedicated fd_status filter ($2). Keeping these consistent prevents
		// the tile showing ₹0 when the register clearly has live FDs.
		run("total_exposure", func(ctx context.Context) (interface{}, error) {
			sqlStr := `
				SELECT
				  COALESCE(SUM(m.principal_amount), 0) AS value,
				  COUNT(*)                              AS fd_count
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted = false
				  AND ($1::text = '' OR COALESCE(m.entity_id,b.entity_id) = $1)
				  AND ($2::text = '' OR m.fd_status = $2)`
			var value float64
			var count int64
			err := pool.QueryRow(ctx, sqlStr, entityFilter, fdStatusFilter).Scan(&value, &count)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"value":     fdRound(value, 2),
				"fd_count":  count,
				"trend_pct": 0,
			}, nil
		})

		// ── 2. bank_concentration ─────────────────────────────────────────────
		// Per-bank exposure vs derived bank limit.
		//
		// Bank limit derivation (no dedicated `credit_limit` column exists in
		// fd_bank_config_master, so we derive the cap):
		//   1. Configured cap = SUM(maximum_amount) across that bank's active
		//      product configs (each product's max-per-FD aggregates into a
		//      coarse bank-level cap).
		//   2. Fallback policy cap = 30% of total portfolio exposure
		//      (industry-standard single-counterparty concentration limit).
		// The larger of the two is taken so we never under-state a bank's room
		// but always have a non-zero number to compare against.
		run("bank_concentration", func(ctx context.Context) (interface{}, error) {
			var totalAmt float64
			_ = pool.QueryRow(ctx,
				`SELECT COALESCE(SUM(m.principal_amount),0)
				 FROM investment.fd_master m
				 LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				 WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				   AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`,
				entityFilter).Scan(&totalAmt)

			// 30% of total portfolio = default per-counterparty concentration cap
			policyCap := totalAmt * 0.30

			sqlStr := `
				SELECT
				  COALESCE(m.bank_name, m.bank_id, '') AS bank,
				  COALESCE(SUM(m.principal_amount), 0) AS exposure,
				  COALESCE(lim.bank_cap, 0)            AS bank_cap
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				LEFT JOIN LATERAL (
				  SELECT COALESCE(SUM(COALESCE(bc.maximum_amount, 0)), 0) AS bank_cap
				  FROM investment.fd_bank_config_master bc
				  WHERE (bc.bank_code = m.bank_id OR bc.bank_code = m.bank_name)
				    AND COALESCE(bc.is_deleted, false) = false
				    AND COALESCE(bc.effective_to, '9999-12-31'::date) >= CURRENT_DATE
				) lim ON true
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				GROUP BY COALESCE(m.bank_name, m.bank_id, ''), lim.bank_cap
				ORDER BY exposure DESC`

			type bcRow struct {
				Bank             string  `json:"bank"`
				Exposure         float64 `json:"exposure"`
				Limit            float64 `json:"limit"`
				Pct              float64 `json:"pct"`
				UtilizationPct   float64 `json:"utilization_pct"`
				RemainingLimit   float64 `json:"remaining_limit"`
				LimitSource      string  `json:"limit_source"`
				Breach           bool    `json:"breach"`
			}

			rows, err := pool.Query(ctx, sqlStr, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			var data []bcRow
			for rows.Next() {
				var br bcRow
				var bankCap float64
				if err := rows.Scan(&br.Bank, &br.Exposure, &bankCap); err != nil {
					continue
				}
				if bankCap > 0 {
					br.Limit = bankCap
					br.LimitSource = "bank_config"
				} else {
					br.Limit = policyCap
					br.LimitSource = "policy_30pct"
				}
				if totalAmt > 0 {
					br.Pct = fdRound(br.Exposure/totalAmt*100, 2)
				}
				if br.Limit > 0 {
					br.UtilizationPct = fdRound(br.Exposure/br.Limit*100, 2)
					br.RemainingLimit = math.Max(0, br.Limit-br.Exposure)
				}
				br.Breach = br.Limit > 0 && br.Exposure > br.Limit
				data = append(data, br)
			}
			if len(data) == 0 {
				return map[string]interface{}{
					"top_bank":   "",
					"top_pct":    0,
					"breach":     false,
					"data":       []bcRow{},
					"policy_cap": fdRound(policyCap, 2),
				}, nil
			}
			top := data[0]
			return map[string]interface{}{
				"top_bank":   top.Bank,
				"top_pct":    top.Pct,
				"breach":     top.Breach,
				"data":       data,
				"policy_cap": fdRound(policyCap, 2),
			}, nil
		})

		// ── 3. maturity buckets ───────────────────────────────────────────────
		run("maturity", func(ctx context.Context) (interface{}, error) {
			sqlStr := `
				SELECT
				  COALESCE(SUM(CASE WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+7   THEN m.principal_amount ELSE 0 END),0) AS amt7,
				  COUNT(CASE  WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+7   THEN 1 END) AS cnt7,
				  COALESCE(SUM(CASE WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+15  THEN m.principal_amount ELSE 0 END),0) AS amt15,
				  COUNT(CASE  WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+15  THEN 1 END) AS cnt15,
				  COALESCE(SUM(CASE WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+30  THEN m.principal_amount ELSE 0 END),0) AS amt30,
				  COUNT(CASE  WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+30  THEN 1 END) AS cnt30
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`
			var a7, a15, a30 float64
			var c7, c15, c30 int64
			err := pool.QueryRow(ctx, sqlStr, entityFilter).Scan(&a7, &c7, &a15, &c15, &a30, &c30)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"next_7_days":  map[string]interface{}{"amount": fdRound(a7, 2), "count": c7},
				"next_15_days": map[string]interface{}{"amount": fdRound(a15, 2), "count": c15},
				"next_30_days": map[string]interface{}{"amount": fdRound(a30, 2), "count": c30},
			}, nil
		})

		// ── 4. interest KPIs ──────────────────────────────────────────────────
		run("interest", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  -- YTD accrued
				  COALESCE(SUM(CASE WHEN al.accrual_period_end >= $2::date THEN al.period_interest_accrued ELSE 0 END),0) AS ytd_accrued,
				  -- Period (MTD/QTD/YTD) accrued
				  COALESCE(SUM(CASE WHEN al.accrual_period_end >= $3::date THEN al.period_interest_accrued ELSE 0 END),0) AS period_accrued,
				  -- QTD accrued (Apr 1 of current FY quarter start)
				  COALESCE(SUM(CASE WHEN al.accrual_period_end >= $4::date THEN al.period_interest_accrued ELSE 0 END),0) AS qtd_accrued,
				  -- Interest received (receipts)
				  COALESCE((SELECT SUM(ir.gross_interest_received)
				            FROM investment.fd_interest_receipt ir
				            WHERE ir.is_deleted=false
				              AND ir.receipt_date >= $2::date
				              AND ($1::text='' OR ir.entity_id=$1)),0) AS received
				FROM investment.fd_accrual_ledger al
				LEFT JOIN investment.fd_master m ON m.fd_id = al.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE COALESCE(al.is_deleted,false)=false
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`

			fyStart := periodStartDate("YTD", now)
			qtdStart := periodStartDate("QTD", now)
			var ytd, periodAcc, qtd, received float64
			err := pool.QueryRow(ctx, sql, entityFilter, fyStart.Format(constants.DateFormat),
				periodStart.Format(constants.DateFormat), qtdStart.Format(constants.DateFormat)).
				Scan(&ytd, &periodAcc, &qtd, &received)
			if err != nil {
				return nil, err
			}

			key := "mtd_accrued"
			if req.Period == "QTD" {
				key = "qtd_accrued"
			} else if req.Period == "YTD" {
				key = "ytd_accrued"
			}
			return map[string]interface{}{
				"ytd_accrued": fdRound(ytd, 2),
				"mtd_accrued": fdRound(periodAcc, 2),
				"qtd_accrued": fdRound(qtd, 2),
				"received":    fdRound(received, 2),
				"trend_pct":   0,
				"period_key":  key,
			}, nil
		})

		// ── 5. exceptions (Policy Exceptions Summary — TC-74) ─────────────────
		// Aggregates two real exception sources so the CFO sees a true "policy
		// exception" picture (count + value at risk):
		//
		//   a) Operational exceptions raised by the accrual engine
		//      (investment.fd_accrual_exception, status NOT IN RESOLVED/CLOSED)
		//
		//   b) Policy variance exceptions raised by the variance engine
		//      (public.variance_log, module_code LIKE 'FD_%' AND status='OPEN')
		//      — these capture rate / amount / tenor / date breaches the user
		//      hasn't yet resolved.
		//
		// "value" = principal at risk across distinct FDs that have at least
		// one open exception (regardless of source) so a single FD with
		// multiple exceptions is only counted once.
		run("exceptions", func(ctx context.Context) (interface{}, error) {
			// (a) accrual-engine breakdown
			accSQL := `
				SELECT
				  ae.exception_type,
				  COUNT(DISTINCT ae.exception_id) AS cnt
				FROM investment.fd_accrual_exception ae
				LEFT JOIN investment.fd_master m ON m.fd_id = ae.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE COALESCE(ae.is_deleted,false)=false
				  AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				GROUP BY ae.exception_type
				ORDER BY cnt DESC`
			rows, err := pool.Query(ctx, accSQL, entityFilter)
			if err != nil {
				return nil, err
			}
			type bkRow struct {
				Type  string `json:"type"`
				Count int64  `json:"count"`
			}
			breakup := []bkRow{}
			for rows.Next() {
				var br bkRow
				if err := rows.Scan(&br.Type, &br.Count); err != nil {
					continue
				}
				if br.Type == "" {
					br.Type = "Accrual Exception"
				}
				breakup = append(breakup, br)
			}
			rows.Close()

			// (b) variance-engine breakdown (variance_type → count)
			varSQL := `
				SELECT
				  COALESCE(NULLIF(vl.variance_type,''),'OTHER') AS variance_type,
				  COUNT(*) AS cnt
				FROM public.variance_log vl
				WHERE vl.module_code LIKE 'FD_%'
				  AND vl.status='OPEN'
				  AND ($1::text='' OR vl.entity_id=$1)
				GROUP BY 1
				ORDER BY cnt DESC`
			if vrows, verr := pool.Query(ctx, varSQL, entityFilter); verr == nil {
				for vrows.Next() {
					var t string
					var c int64
					if scanErr := vrows.Scan(&t, &c); scanErr != nil {
						continue
					}
					breakup = append(breakup, bkRow{Type: "Variance: " + t, Count: c})
				}
				vrows.Close()
			}

			// (c) total distinct FDs at risk + principal sum (de-duplicated).
			// The variance_log.record_id can be a closure_request_id /
			// booking_id depending on module_code; resolve back to fd_master
			// through those tables before summing the principal-at-risk so
			// the alert tile is not stuck at ₹0.
			var distinctCount int64
			var totalVal float64
			_ = pool.QueryRow(ctx, `
				WITH at_risk AS (
				  -- accrual exceptions hold fd_id directly
				  SELECT DISTINCT ae.fd_id AS fd_id
				  FROM investment.fd_accrual_exception ae
				  WHERE COALESCE(ae.is_deleted,false)=false
				    AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  UNION
				  -- variance on closure → fd_id via fd_closure_request
				  SELECT DISTINCT cr.fd_id
				  FROM public.variance_log vl
				  JOIN investment.fd_closure_request cr
				    ON cr.closure_request_id = vl.record_id
				  WHERE vl.module_code='FD_CLOSURE' AND vl.status='OPEN'
				  UNION
				  -- variance on booking/confirmation → fd_id via fd_master.booking_id
				  SELECT DISTINCT m2.fd_id
				  FROM public.variance_log vl
				  JOIN investment.fd_master m2 ON m2.booking_id = vl.record_id
				  WHERE vl.module_code IN ('FD_BOOKING','FD_CONFIRMATION') AND vl.status='OPEN'
				)
				SELECT
				  COUNT(DISTINCT m.fd_id),
				  COALESCE(SUM(m.principal_amount),0)
				FROM at_risk a
				JOIN investment.fd_master m ON m.fd_id = a.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`,
				entityFilter).Scan(&distinctCount, &totalVal)

			// Sum of breakup counts is a reasonable proxy when the join above
			// returns 0 (e.g. variance_log holds non-fd_id record_ids).
			var sumCounts int64
			for _, b := range breakup {
				sumCounts += b.Count
			}
			finalCount := distinctCount
			if finalCount == 0 {
				finalCount = sumCounts
			}

			return map[string]interface{}{
				"count":   finalCount,
				"value":   fdRound(totalVal, 2),
				"breakup": breakup,
			}, nil
		})

		// ── 5b. variance_impact (TC-83) ───────────────────────────────────────
		// Real numbers from public.variance_log scoped to FD modules.
		run("variance_impact", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  COUNT(*) FILTER (WHERE vl.status='OPEN')                                                         AS open_count,
				  COUNT(*) FILTER (WHERE vl.status='OPEN' AND vl.priority='HIGH')                                  AS high_count,
				  COUNT(*) FILTER (WHERE vl.status='OPEN' AND vl.is_exception=true)                                AS exception_count,
				  COALESCE(SUM(ABS(COALESCE(vl.variance_delta,0))) FILTER (WHERE vl.status='OPEN' AND vl.variance_type='AMOUNT'),0) AS amount_impact,
				  COALESCE(AVG(ABS(COALESCE(vl.variance_delta,0))) FILTER (WHERE vl.status='OPEN' AND vl.variance_type='RATE'),0)   AS avg_rate_delta,
				  COALESCE(AVG(ABS(COALESCE(vl.variance_delta,0))) FILTER (WHERE vl.status='OPEN' AND vl.variance_type='DAYS'),0)   AS avg_day_delta
				FROM public.variance_log vl
				WHERE vl.module_code LIKE 'FD_%'
				  AND ($1::text='' OR vl.entity_id=$1)`
			var openCnt, highCnt, excCnt int64
			var amtImpact, avgRateDelta, avgDayDelta float64
			if err := pool.QueryRow(ctx, sql, entityFilter).Scan(
				&openCnt, &highCnt, &excCnt, &amtImpact, &avgRateDelta, &avgDayDelta,
			); err != nil {
				// Empty result if variance_log not available
				return map[string]interface{}{
					"open_count":      0,
					"high_count":      0,
					"exception_count": 0,
					"amount_impact":   0,
					"avg_rate_delta":  0,
					"avg_day_delta":   0,
					"breakup":         []interface{}{},
				}, nil
			}

			// per-type breakup for the tile sub-list
			type vrRow struct {
				Type  string  `json:"type"`
				Count int64   `json:"count"`
				Delta float64 `json:"delta"`
			}
			breakup := []vrRow{}
			brSQL := `
				SELECT COALESCE(NULLIF(variance_type,''),'OTHER') AS type,
				       COUNT(*)                                   AS cnt,
				       COALESCE(SUM(ABS(COALESCE(variance_delta,0))),0) AS delta
				FROM public.variance_log
				WHERE module_code LIKE 'FD_%' AND status='OPEN'
				  AND ($1::text='' OR entity_id=$1)
				GROUP BY 1
				ORDER BY cnt DESC`
			if brRows, berr := pool.Query(ctx, brSQL, entityFilter); berr == nil {
				for brRows.Next() {
					var br vrRow
					if scanErr := brRows.Scan(&br.Type, &br.Count, &br.Delta); scanErr != nil {
						continue
					}
					br.Delta = fdRound(br.Delta, 2)
					breakup = append(breakup, br)
				}
				brRows.Close()
			}

			// per-FD items (HIGH first) — joined through closure/booking so the
			// drill-down drawer can populate real FDs instead of an empty list.
			type vrItem struct {
				FDID         string  `json:"fd_id"`
				BookingID    string  `json:"booking_id"`
				Bank         string  `json:"bank"`
				Entity       string  `json:"entity"`
				EntityID     string  `json:"entity_id"`
				Principal    float64 `json:"principal"`
				Rate         float64 `json:"rate"`
				MaturityDate string  `json:"maturity_date"`
				FieldName    string  `json:"field_name"`
				VarianceType string  `json:"variance_type"`
				Priority     string  `json:"priority"`
				Delta        float64 `json:"delta"`
				IsException  bool    `json:"is_exception"`
				ModuleCode   string  `json:"module_code"`
				Status       string  `json:"status"`
			}
			items := []vrItem{}
			itemsSQL := `
				SELECT
				  COALESCE(m.fd_id,'')                                      AS fd_id,
				  COALESCE(b.booking_id, m.booking_id,'')                   AS booking_id,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(m.entity_name, b.entity_name,'')                 AS entity,
				  COALESCE(m.entity_id, b.entity_id, vl.entity_id,'')       AS entity_id,
				  COALESCE(m.principal_amount, b.principal_amount, 0)       AS principal,
				  COALESCE(m.interest_rate, b.interest_rate, 0)             AS rate,
				  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),
				           TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(vl.field_name,'')                                AS field_name,
				  COALESCE(vl.variance_type,'')                             AS variance_type,
				  COALESCE(vl.priority,'')                                  AS priority,
				  COALESCE(ABS(vl.variance_delta),0)                        AS delta,
				  COALESCE(vl.is_exception,false)                           AS is_exception,
				  COALESCE(vl.module_code,'')                               AS module_code,
				  COALESCE(vl.status,'')                                    AS status
				FROM public.variance_log vl
				LEFT JOIN investment.fd_closure_request cr
				  ON vl.module_code='FD_CLOSURE' AND cr.closure_request_id = vl.record_id
				LEFT JOIN investment.fd_booking_request b
				  ON (vl.module_code IN ('FD_CONFIRMATION','FD_BOOKING') AND b.booking_id = vl.record_id)
				LEFT JOIN investment.fd_master m
				  ON  ( cr.fd_id IS NOT NULL AND m.fd_id = cr.fd_id )
				   OR ( b.booking_id IS NOT NULL AND m.booking_id = b.booking_id )
				WHERE vl.module_code LIKE 'FD_%'
				  AND vl.status='OPEN'
				  AND ($1::text='' OR vl.entity_id=$1)
				ORDER BY (vl.priority='HIGH') DESC, ABS(vl.variance_delta) DESC
				LIMIT 500`
			if itRows, ierr := pool.Query(ctx, itemsSQL, entityFilter); ierr == nil {
				defer itRows.Close()
				for itRows.Next() {
					var it vrItem
					if scanErr := itRows.Scan(
						&it.FDID, &it.BookingID, &it.Bank, &it.Entity, &it.EntityID,
						&it.Principal, &it.Rate, &it.MaturityDate,
						&it.FieldName, &it.VarianceType, &it.Priority,
						&it.Delta, &it.IsException, &it.ModuleCode, &it.Status,
					); scanErr != nil {
						api.LogError("[CfoDash] variance_impact items scan: %v", scanErr)
						continue
					}
					it.Principal = fdRound(it.Principal, 2)
					it.Rate = fdRound(it.Rate, 4)
					it.Delta = fdRound(it.Delta, 4)
					items = append(items, it)
				}
			} else {
				api.LogError("[CfoDash] variance_impact items query: %v", ierr)
			}

			// Distinct FD count + principal-at-risk derived from the items
			// (so the alert tile shows a real "at risk" rupee number even
			// when variance_log holds non-fd_id record IDs).
			seen := map[string]bool{}
			distinctFDValue := 0.0
			for _, it := range items {
				if it.FDID == "" || seen[it.FDID] {
					continue
				}
				seen[it.FDID] = true
				distinctFDValue += it.Principal
			}

			return map[string]interface{}{
				"open_count":         openCnt,
				"high_count":         highCnt,
				"exception_count":    excCnt,
				"amount_impact":      fdRound(amtImpact, 2),
				"avg_rate_delta":     fdRound(avgRateDelta, 4),
				"avg_day_delta":      fdRound(avgDayDelta, 1),
				"distinct_fd_count":  len(seen),
				"distinct_fd_value":  fdRound(distinctFDValue, 2),
				"breakup":            breakup,
				"items":              items,
			}, nil
		})

		// ── 6. maturity_ladder (chart) ────────────────────────────────────────
		// Bucket size is controlled by req.LadderView:
		//   WEEK  → 8 weekly buckets covering the next 8 weeks (default)
		//   MONTH → 12 monthly buckets covering the next 12 months
		//   YEAR  → 5 yearly buckets covering the next 5 years
		// The bucket column is built deterministically so labels sort in order.
		ladderView := strings.ToUpper(strings.TrimSpace(req.LadderView))
		if ladderView == "" {
			ladderView = "WEEK"
		}
		run("maturity_ladder", func(ctx context.Context) (interface{}, error) {
			var bucketExpr, sortExpr string
			switch ladderView {
			case "MONTH":
				// Months from today: 0..11, label as 'Mon YY'
				bucketExpr = `to_char(date_trunc('month', m.maturity_date), 'Mon YY')`
				sortExpr = `date_trunc('month', m.maturity_date)`
			case "YEAR":
				bucketExpr = `to_char(date_trunc('year', m.maturity_date), 'YYYY')`
				sortExpr = `date_trunc('year', m.maturity_date)`
			default: // WEEK
				bucketExpr = `'Wk ' || to_char(((m.maturity_date - CURRENT_DATE)::int / 7) + 1, 'FM00') ||
				              ' (' || to_char(date_trunc('week', m.maturity_date), 'DD Mon') || ')'`
				sortExpr = `date_trunc('week', m.maturity_date)`
			}

			horizonClause := "AND m.maturity_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + 56)"
			if ladderView == "MONTH" {
				horizonClause = "AND m.maturity_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + INTERVAL '12 months')"
			} else if ladderView == "YEAR" {
				horizonClause = "AND m.maturity_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + INTERVAL '5 years')"
			}

			sql := `
				SELECT
				  ` + bucketExpr + ` AS period,
				  COALESCE(SUM(m.principal_amount),0) AS amount,
				  COUNT(*) AS count,
				  MIN(m.maturity_date) AS first_maturity
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status='ACTIVE'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  ` + horizonClause + `
				GROUP BY ` + bucketExpr + `, ` + sortExpr + `
				ORDER BY ` + sortExpr
			rows, err := pool.Query(ctx, sql, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type ladderRow struct {
				Period        string  `json:"period"`
				Amount        float64 `json:"amount"`
				Count         int64   `json:"count"`
				FirstMaturity string  `json:"first_maturity"`
			}
			var out []ladderRow
			for rows.Next() {
				var lr ladderRow
				var firstMat interface{}
				if err := rows.Scan(&lr.Period, &lr.Amount, &lr.Count, &firstMat); err != nil {
					continue
				}
				lr.Amount = fdRound(lr.Amount, 2)
				if t, ok := firstMat.(time.Time); ok {
					lr.FirstMaturity = t.Format(constants.DateFormat)
				}
				out = append(out, lr)
			}
			if out == nil {
				out = []ladderRow{}
			}
			return map[string]interface{}{
				"view":    ladderView,
				"buckets": out,
			}, nil
		})

		// ── 7. interest_trend (chart — last 6 months) ─────────────────────────
		run("interest_trend", func(ctx context.Context) (interface{}, error) {
			// Pull monthly accruals
			accrualSQL := `
				SELECT
				  TO_CHAR(DATE_TRUNC('month', al.accrual_period_end),'Mon') AS month,
				  TO_CHAR(DATE_TRUNC('month', al.accrual_period_end),'YYYY-MM') AS month_sort,
				  COALESCE(SUM(al.period_interest_accrued),0) AS accrued
				FROM investment.fd_accrual_ledger al
				LEFT JOIN investment.fd_master m ON m.fd_id = al.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE COALESCE(al.is_deleted,false)=false
				  AND al.accrual_period_end >= CURRENT_DATE - INTERVAL '6 months'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				GROUP BY 1, 2
				ORDER BY 2`
			accrRows, err := pool.Query(ctx, accrualSQL, entityFilter)
			if err != nil {
				return []interface{}{}, nil // graceful fallback
			}
			defer accrRows.Close()

			type trendRow struct {
				Month    string  `json:"month"`
				SortKey  string  `json:"-"`
				Accrued  float64 `json:"accrued"`
				Received float64 `json:"received"`
			}
			out := []trendRow{}
			for accrRows.Next() {
				var tr trendRow
				if err := accrRows.Scan(&tr.Month, &tr.SortKey, &tr.Accrued); err != nil {
					continue
				}
				tr.Accrued = fdRound(tr.Accrued, 2)
				out = append(out, tr)
			}
			accrRows.Close()

			// Pull monthly interest receipts and merge
			recSQL := `
				SELECT
				  TO_CHAR(DATE_TRUNC('month', ir.receipt_date),'Mon') AS month,
				  TO_CHAR(DATE_TRUNC('month', ir.receipt_date),'YYYY-MM') AS month_sort,
				  COALESCE(SUM(ir.gross_interest_received),0) AS received
				FROM investment.fd_interest_receipt ir
				WHERE ir.is_deleted=false
				  AND ir.receipt_date >= CURRENT_DATE - INTERVAL '6 months'
				  AND ($1::text='' OR ir.entity_id=$1)
				GROUP BY 1, 2
				ORDER BY 2`
			recvMap := map[string]float64{}
			if recRows, rerr := pool.Query(ctx, recSQL, entityFilter); rerr == nil {
				defer recRows.Close()
				for recRows.Next() {
					var mon, sortK string
					var recv float64
					if err2 := recRows.Scan(&mon, &sortK, &recv); err2 == nil {
						recvMap[sortK] = fdRound(recv, 2)
					}
				}
			}
			for i := range out {
				out[i].Received = recvMap[out[i].SortKey]
			}
			return out, nil
		})

		// ── 8. rate_distribution (chart) ──────────────────────────────────────
		run("rate_distribution", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  CASE
				    WHEN m.interest_rate < 5  THEN '<5%'
				    WHEN m.interest_rate < 6  THEN '5–6%'
				    WHEN m.interest_rate < 7  THEN '6–7%'
				    WHEN m.interest_rate < 8  THEN '7–8%'
				    WHEN m.interest_rate < 9  THEN '8–9%'
				    ELSE '9%+'
				  END AS bucket,
				  COUNT(*) AS count,
				  COALESCE(SUM(m.principal_amount),0) AS value
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status='ACTIVE'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				GROUP BY 1
				ORDER BY MIN(m.interest_rate)`
			rows, err := pool.Query(ctx, sql, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type rateRow struct {
				Bucket string  `json:"bucket"`
				Count  int64   `json:"count"`
				Value  float64 `json:"value"`
			}
			var out []rateRow
			for rows.Next() {
				var rr rateRow
				if err := rows.Scan(&rr.Bucket, &rr.Count, &rr.Value); err != nil {
					continue
				}
				rr.Value = fdRound(rr.Value, 2)
				out = append(out, rr)
			}
			if out == nil {
				out = []rateRow{}
			}
			return out, nil
		})

		// ── 9. governance — approvals pending ─────────────────────────────────
		// Returns the three pending approval categories the CFO actually needs
		// to action against FDs:
		//   - Pending Approval        (new bookings awaiting approval)
		//   - Pending Edit Approval   (booking edits awaiting approval)
		//   - Pending Delete Approval (booking deletes awaiting approval)
		// Each category includes its own FD detail rows so the frontend can
		// open a drawer populated with the right records.
		run("governance_approvals", func(ctx context.Context) (interface{}, error) {
			// fdItem now also carries `Source` (which FD page the approval
			// originated from — Booking, Confirmation, Closure, Accrual…).
			type fdItem struct {
				FDID          string  `json:"fd_id"`
				BookingID     string  `json:"booking_id"`
				Entity        string  `json:"entity"`
				EntityID      string  `json:"entity_id"`
				Bank          string  `json:"bank"`
				Principal     float64 `json:"principal"`
				Rate          float64 `json:"rate"`
				MaturityDate  string  `json:"maturity_date"`
				Status        string  `json:"status"`
				Action        string  `json:"action"`
				Source        string  `json:"source"` // FD page label
				SourcePage    string  `json:"source_page"` // url-safe page id
				RequestedBy   string  `json:"requested_by"`
				RequestedAt   string  `json:"requested_at"`
				Currency      string  `json:"currency"`
			}

			// 1. Pending new bookings (booking_status reflects pending approval)
			pendingBookingSQL := `
				SELECT
				  COALESCE(b.booking_id,'')                                AS booking_id,
				  COALESCE(m.fd_id,'')                                     AS fd_id,
				  COALESCE(b.entity_name, m.entity_name,'')                AS entity,
				  COALESCE(b.entity_id, m.entity_id,'')                    AS entity_id,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0)                           AS principal,
				  COALESCE(b.interest_rate, m.interest_rate, 0)            AS rate,
				  COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'), TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(b.booking_status,'')                            AS status,
				  COALESCE(b.created_by,'')                                AS requested_by,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('PENDING_APPROVAL','APPROVAL_PENDING','SUBMITTED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				ORDER BY b.created_at DESC
				LIMIT 200`

			// 2. Pending edits — most recent EDIT audit row per booking still pending
			pendingEditSQL := `
				WITH latest_edit AS (
				  SELECT DISTINCT ON (a.booking_id)
				    a.booking_id, a.processing_status,
				    a.requested_by, a.requested_at
				  FROM investment.fd_audit_booking_request a
				  WHERE a.action_type='EDIT'
				  ORDER BY a.booking_id, a.requested_at DESC
				)
				SELECT
				  COALESCE(b.booking_id,'')                                AS booking_id,
				  COALESCE(m.fd_id,'')                                     AS fd_id,
				  COALESCE(b.entity_name, m.entity_name,'')                AS entity,
				  COALESCE(b.entity_id, m.entity_id,'')                    AS entity_id,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0)                           AS principal,
				  COALESCE(b.interest_rate, m.interest_rate, 0)            AS rate,
				  COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'), TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(le.processing_status,'PENDING_EDIT_APPROVAL')   AS status,
				  COALESCE(le.requested_by,'')                             AS requested_by,
				  COALESCE(TO_CHAR(le.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at
				FROM latest_edit le
				JOIN investment.fd_booking_request b ON b.booking_id = le.booking_id
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND le.processing_status='PENDING_EDIT_APPROVAL'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				ORDER BY le.requested_at DESC
				LIMIT 200`

			// 3. Pending deletes
			pendingDeleteSQL := `
				WITH latest_del AS (
				  SELECT DISTINCT ON (a.booking_id)
				    a.booking_id, a.processing_status,
				    a.requested_by, a.requested_at
				  FROM investment.fd_audit_booking_request a
				  WHERE a.action_type='DELETE'
				  ORDER BY a.booking_id, a.requested_at DESC
				)
				SELECT
				  COALESCE(b.booking_id,'')                                AS booking_id,
				  COALESCE(m.fd_id,'')                                     AS fd_id,
				  COALESCE(b.entity_name, m.entity_name,'')                AS entity,
				  COALESCE(b.entity_id, m.entity_id,'')                    AS entity_id,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0)                           AS principal,
				  COALESCE(b.interest_rate, m.interest_rate, 0)            AS rate,
				  COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'), TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(ld.processing_status,'PENDING_DELETE_APPROVAL') AS status,
				  COALESCE(ld.requested_by,'')                             AS requested_by,
				  COALESCE(TO_CHAR(ld.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at
				FROM latest_del ld
				JOIN investment.fd_booking_request b ON b.booking_id = ld.booking_id
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND ld.processing_status='PENDING_DELETE_APPROVAL'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				ORDER BY ld.requested_at DESC
				LIMIT 200`

			fetch := func(sql, action, source, sourcePage string) []fdItem {
				rows, err := pool.Query(ctx, sql, entityFilter)
				if err != nil {
					api.LogError("[CfoDash] approvals %s query error: %v", action, err)
					return []fdItem{}
				}
				defer rows.Close()
				out := []fdItem{}
				for rows.Next() {
					var f fdItem
					if scanErr := rows.Scan(
						&f.BookingID, &f.FDID, &f.Entity, &f.EntityID, &f.Bank,
						&f.Principal, &f.Rate, &f.MaturityDate, &f.Status,
						&f.RequestedBy, &f.RequestedAt,
					); scanErr != nil {
						api.LogError("[CfoDash] approvals %s scan error: %v", action, scanErr)
						continue
					}
					f.Principal = fdRound(f.Principal, 2)
					f.Rate = fdRound(f.Rate, 4)
					f.Action = action
					f.Source = source
					f.SourcePage = sourcePage
					f.Currency = "INR"
					out = append(out, f)
				}
				return out
			}

			// 4. Pending bank confirmation captures (Bank Confirmation page)
			pendingConfirmSQL := `
				SELECT
				  COALESCE(b.booking_id,'')                                AS booking_id,
				  COALESCE(m.fd_id,'')                                     AS fd_id,
				  COALESCE(b.entity_name, m.entity_name,'')                AS entity,
				  COALESCE(b.entity_id, m.entity_id,'')                    AS entity_id,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0)                           AS principal,
				  COALESCE(b.interest_rate, m.interest_rate, 0)            AS rate,
				  COALESCE(TO_CHAR(b.expected_maturity_date,'YYYY-MM-DD'), TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(c.confirmation_status,'')                       AS status,
				  COALESCE(c.created_by,'')                                AS requested_by,
				  COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at
				FROM investment.fd_confirmation c
				JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE COALESCE(c.is_deleted,false)=false
				  AND c.confirmation_status='PENDING_APPROVAL'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				ORDER BY c.created_at DESC
				LIMIT 200`

			// 5. Pending closures (FD Maturity / Closure page)
			pendingClosureSQL := `
				SELECT
				  COALESCE(b.booking_id,'')                                AS booking_id,
				  COALESCE(m.fd_id, cr.fd_id,'')                           AS fd_id,
				  COALESCE(m.entity_name, b.entity_name,'')                AS entity,
				  COALESCE(m.entity_id, b.entity_id,'')                    AS entity_id,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(m.principal_amount, b.principal_amount,0)       AS principal,
				  COALESCE(m.interest_rate, b.interest_rate, 0)            AS rate,
				  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'')       AS maturity_date,
				  COALESCE(cr.closure_status,'')                           AS status,
				  COALESCE(cr.created_by,'')                               AS requested_by,
				  COALESCE(TO_CHAR(cr.created_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at
				FROM investment.fd_closure_request cr
				LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id AND m.is_deleted=false
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE COALESCE(cr.is_deleted,false)=false
				  AND cr.closure_status='PENDING_APPROVAL'
				  AND ($1::text='' OR COALESCE(m.entity_id, b.entity_id)=$1)
				ORDER BY cr.created_at DESC
				LIMIT 200`

			// 6. Pending accrual run approvals (Accrual Engine page) — these
			// don't map to a single FD so we expose a single aggregate row.
			var accrualRunCount int64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM investment.fd_accrual_run
				WHERE COALESCE(is_deleted,false)=false
				  AND run_status='PENDING_APPROVAL'
				  AND ($1::text='' OR entity_id=$1)`,
				entityFilter).Scan(&accrualRunCount)

			pendingNew := fetch(pendingBookingSQL, "PENDING_APPROVAL",
				"FD Booking", "fd-booking")
			pendingEdit := fetch(pendingEditSQL, "PENDING_EDIT_APPROVAL",
				"FD Master (Edit)", "fd-master")
			pendingDelete := fetch(pendingDeleteSQL, "PENDING_DELETE_APPROVAL",
				"FD Master (Delete)", "fd-master")
			pendingConfirm := fetch(pendingConfirmSQL, "PENDING_CONFIRMATION_APPROVAL",
				"Bank Confirmation", "fd-confirmation")
			pendingClosure := fetch(pendingClosureSQL, "PENDING_CLOSURE_APPROVAL",
				"FD Maturity / Closure", "fd-maturity")

			sumValue := func(items []fdItem) float64 {
				s := 0.0
				for _, x := range items {
					s += x.Principal
				}
				return fdRound(s, 2)
			}

			type approvalRow struct {
				Type       string   `json:"type"`
				Status     string   `json:"status"`
				Source     string   `json:"source"`
				SourcePage string   `json:"source_page"`
				Count      int      `json:"count"`
				Value      float64  `json:"value"`
				Priority   string   `json:"priority"`
				Items      []fdItem `json:"items"`
			}

			out := []approvalRow{
				{Type: "FD Booking — New", Status: "PENDING_APPROVAL",
					Source: "FD Booking", SourcePage: "fd-booking",
					Count: len(pendingNew), Value: sumValue(pendingNew),
					Priority: "High", Items: pendingNew},
				{Type: "FD Booking — Edit", Status: "PENDING_EDIT_APPROVAL",
					Source: "FD Master", SourcePage: "fd-master",
					Count: len(pendingEdit), Value: sumValue(pendingEdit),
					Priority: "Medium", Items: pendingEdit},
				{Type: "FD Booking — Delete", Status: "PENDING_DELETE_APPROVAL",
					Source: "FD Master", SourcePage: "fd-master",
					Count: len(pendingDelete), Value: sumValue(pendingDelete),
					Priority: "High", Items: pendingDelete},
				{Type: "Bank Confirmation", Status: "PENDING_APPROVAL",
					Source: "Bank Confirmation", SourcePage: "fd-confirmation",
					Count: len(pendingConfirm), Value: sumValue(pendingConfirm),
					Priority: "Medium", Items: pendingConfirm},
				{Type: "FD Closure / Maturity", Status: "PENDING_APPROVAL",
					Source: "FD Maturity", SourcePage: "fd-maturity",
					Count: len(pendingClosure), Value: sumValue(pendingClosure),
					Priority: "High", Items: pendingClosure},
				{Type: "Accrual Run", Status: "PENDING_APPROVAL",
					Source: "Accrual Engine", SourcePage: "fd-accrual-engine",
					Count: int(accrualRunCount), Value: 0,
					Priority: "Medium", Items: []fdItem{}},
			}
			return out, nil
		})

		// ── 10. governance — closing_status + closing_checklist (TC-79) ───────
		// Period Closing Readiness: real, deterministic checklist derived from
		// the operational state — not just a count of fd_closure_request rows.
		// `completed/total/pct` reflects how many checklist items are done.
		run("closing_status", func(ctx context.Context) (interface{}, error) {
			type ckItem struct {
				ID       string `json:"id"`
				Label    string `json:"label"`
				Done     bool   `json:"done"`
				Blocker  bool   `json:"blocker"`
				Detail   string `json:"detail,omitempty"`
			}

			// 1. Latest accrual run for the period is COMPLETED/POSTED
			var latestRunStatus string
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(run_status,'')
				FROM investment.fd_accrual_run
				WHERE COALESCE(is_deleted,false)=false
				  AND ($1::text='' OR entity_id=$1)
				ORDER BY created_at DESC LIMIT 1`,
				entityFilter).Scan(&latestRunStatus)
			accrualDone := latestRunStatus == "POSTED" || latestRunStatus == "APPROVED"

			// 2. All bookings approved (no PENDING_APPROVAL/SUBMITTED in period)
			var pendingBookings int64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM investment.fd_booking_request b
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('PENDING_APPROVAL','SUBMITTED')
				  AND b.created_at >= $2::date
				  AND ($1::text='' OR b.entity_id=$1)`,
				entityFilter, periodStart.Format(constants.DateFormat)).Scan(&pendingBookings)

			// 3. All matured FDs in period have closure completed
			var unprocessedMaturities int64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false
				  AND m.maturity_date <= CURRENT_DATE
				  AND m.maturity_date >= $2::date
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND NOT EXISTS (
				    SELECT 1 FROM investment.fd_closure_request cr
				    WHERE cr.fd_id=m.fd_id
				      AND cr.is_deleted=false
				      AND cr.closure_status IN ('COMPLETED','POSTED','CLOSED')
				  )`,
				entityFilter, periodStart.Format(constants.DateFormat)).Scan(&unprocessedMaturities)

			// 4. Confirmations received for active FDs
			var pendingConfirmations int64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM investment.fd_booking_request b
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('APPROVED','PENDING_CONFIRMATION')
				  AND b.created_at >= $2::date
				  AND ($1::text='' OR b.entity_id=$1)
				  AND NOT EXISTS (
				    SELECT 1 FROM investment.fd_master m
				    WHERE m.booking_id=b.booking_id AND m.is_deleted=false
				  )`,
				entityFilter, periodStart.Format(constants.DateFormat)).Scan(&pendingConfirmations)

			// 5. Open accrual exceptions
			var openExceptions int64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM investment.fd_accrual_exception ae
				LEFT JOIN investment.fd_master m ON m.fd_id = ae.fd_id
				WHERE COALESCE(ae.is_deleted,false)=false
				  AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  AND ($1::text='' OR m.entity_id=$1)`,
				entityFilter).Scan(&openExceptions)

			// 6. Open variance exceptions
			var openVariances int64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM public.variance_log
				WHERE module_code LIKE 'FD_%' AND status='OPEN'
				  AND ($1::text='' OR entity_id=$1)`,
				entityFilter).Scan(&openVariances)

			// 7. Interest receipts reconciled (no UNMATCHED for the period)
			var unmatchedReceipts int64
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM investment.fd_interest_receipt ir
				WHERE ir.is_deleted=false
				  AND ir.receipt_date >= $2::date
				  AND COALESCE(ir.reconciliation_status,'UNMATCHED') IN ('UNMATCHED','PENDING','')
				  AND ($1::text='' OR ir.entity_id=$1)`,
				entityFilter, periodStart.Format(constants.DateFormat)).Scan(&unmatchedReceipts)

			items := []ckItem{
				{ID: "C1", Label: "Latest accrual run posted", Done: accrualDone, Blocker: !accrualDone, Detail: "Status: " + latestRunStatus},
				{ID: "C2", Label: "All booking approvals processed", Done: pendingBookings == 0, Blocker: pendingBookings > 0, Detail: formatInt64(pendingBookings) + " pending"},
				{ID: "C3", Label: "Bank confirmations captured for new bookings", Done: pendingConfirmations == 0, Blocker: false, Detail: formatInt64(pendingConfirmations) + " pending"},
				{ID: "C4", Label: "Matured FDs closed in period", Done: unprocessedMaturities == 0, Blocker: unprocessedMaturities > 0, Detail: formatInt64(unprocessedMaturities) + " unprocessed"},
				{ID: "C5", Label: "Accrual exceptions resolved", Done: openExceptions == 0, Blocker: openExceptions > 0, Detail: formatInt64(openExceptions) + " open"},
				{ID: "C6", Label: "Policy variances cleared", Done: openVariances == 0, Blocker: false, Detail: formatInt64(openVariances) + " open"},
				{ID: "C7", Label: "Interest receipts reconciled", Done: unmatchedReceipts == 0, Blocker: false, Detail: formatInt64(unmatchedReceipts) + " unmatched"},
			}

			completed := int64(0)
			blockers := int64(0)
			for _, it := range items {
				if it.Done {
					completed++
				} else if it.Blocker {
					blockers++
				}
			}
			total := int64(len(items))
			pct := 0.0
			if total > 0 {
				pct = fdRound(float64(completed)/float64(total)*100, 1)
			}

			return map[string]interface{}{
				"completed": completed,
				"total":     total,
				"pct":       pct,
				"blockers":  blockers,
				"checklist": items,
			}, nil
		})

		// ── 11. fd_list ───────────────────────────────────────────────────────
		// Build dynamic WHERE for optional secondary filters (bank, fd_status,
		// fd_type, interest_frequency). Status defaults remain ACTIVE/MATURED
		// when no explicit fdStatusFilter was supplied.
		bankFilter := req.Bank
		fdTypeFilter := req.FDType
		interestFreqFilter := req.InterestFrequency
		// fdStatusFilter is declared earlier (above the run() loop)

		// Map UI status alias → DB column value
		dbStatusFilter := ""
		switch fdStatusFilter {
		case "ACTIVE":
			dbStatusFilter = "ACTIVE"
		case "MATURED":
			dbStatusFilter = "MATURED"
		case "CLOSED":
			dbStatusFilter = "CLOSED"
		}

		// Tenor years between 0 and ~1 = "NEAR_MATURITY" interpreted at SQL
		// time via maturity_date <= CURRENT_DATE + 30. Handle that flag below.
		nearMaturity := fdStatusFilter == "NEAR_MATURITY"

		run("fd_list", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  m.fd_id,
				  COALESCE(b.entity_name, m.entity_name,'') AS entity,
				  COALESCE(m.entity_id, b.entity_id,'') AS entity_id,
				  COALESCE(m.bank_name, m.bank_id,'') AS bank,
				  COALESCE(m.bank_fd_ref_no, m.fd_id,'') AS bank_fd_ref_no,
				  m.principal_amount AS principal,
				  m.interest_rate AS rate,
				  COALESCE(m.interest_type_code,'') AS interest_type_code,
				  COALESCE(m.frequency_id,'') AS frequency_id,
				  COALESCE(TO_CHAR(m.start_date,'YYYY-MM-DD'),'') AS start_date,
				  COALESCE(m.tenure_days, 0) AS tenure_days,
				  COALESCE(m.tenure_months, 0) AS tenure_months,
				  COALESCE(m.tenure_years, 0) AS tenure_years,
				  TO_CHAR(m.maturity_date,'YYYY-MM-DD') AS maturity_date,
				  COALESCE(al.total_interest_accrued, 0) AS interest_accrued,
				  m.fd_status AS status,
				  COALESCE(b.booking_id,'') AS booking_id,
				  COALESCE(b.booking_status,'') AS booking_status,
				  COALESCE(cr.closure_type,'') AS closure_type,
				  COALESCE(cr.closure_status,'') AS closure_status
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				LEFT JOIN LATERAL (
				  SELECT COALESCE(SUM(period_interest_accrued),0) AS total_interest_accrued
				  FROM investment.fd_accrual_ledger
				  WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false
				) al ON true
				LEFT JOIN LATERAL (
				  SELECT closure_type, COALESCE(closure_status,'') AS closure_status
				  FROM investment.fd_closure_request
				  WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false
				  ORDER BY created_at DESC LIMIT 1
				) cr ON true
				WHERE m.is_deleted=false
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND ($2::text='' OR COALESCE(m.bank_name, m.bank_id,'')=$2)
				  AND (
				        ($3::text='' AND m.fd_status IN ('ACTIVE','MATURED'))
				    OR  ($3::text<>'' AND m.fd_status=$3)
				  )
				  AND ($4::boolean=false OR (m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+30))
				  AND ($5::text='' OR COALESCE(m.interest_type_code,'')=$5)
				  AND ($6::text='' OR COALESCE(m.frequency_id,'')=$6)
				ORDER BY m.maturity_date ASC
				LIMIT 500`
			rows, err := pool.Query(ctx, sql,
				entityFilter,
				bankFilter,
				dbStatusFilter,
				nearMaturity,
				fdTypeFilter,
				interestFreqFilter,
			)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type fdRow struct {
				FDID             string  `json:"fd_id"`
				Entity           string  `json:"entity"`
				EntityID         string  `json:"entity_id"`
				Bank             string  `json:"bank"`
				BankFDRefNo      string  `json:"bank_fd_ref_no"`
				Principal        float64 `json:"principal"`
				Rate             float64 `json:"rate"`
				InterestTypeCode string  `json:"interest_type_code"`
				FrequencyID      string  `json:"frequency_id"`
				StartDate        string  `json:"start_date"`
				TenureDays       int     `json:"tenure_days"`
				TenureMonths     int     `json:"tenure_months"`
				TenureYears      int     `json:"tenure_years"`
				MaturityDate     string  `json:"maturity_date"`
				InterestAccrued  float64 `json:"interest_accrued"`
				Status           string  `json:"status"`
				BookingID        string  `json:"booking_id"`
				BookingStatus    string  `json:"booking_status"`
				ClosureType      string  `json:"closure_type"`
				ClosureStatus    string  `json:"closure_status"`
			}
			var out []fdRow
			for rows.Next() {
				var fr fdRow
				if err := rows.Scan(
					&fr.FDID, &fr.Entity, &fr.EntityID, &fr.Bank, &fr.BankFDRefNo,
					&fr.Principal, &fr.Rate, &fr.InterestTypeCode, &fr.FrequencyID,
					&fr.StartDate, &fr.TenureDays, &fr.TenureMonths, &fr.TenureYears,
					&fr.MaturityDate,
					&fr.InterestAccrued, &fr.Status,
					&fr.BookingID, &fr.BookingStatus,
					&fr.ClosureType, &fr.ClosureStatus,
				); err != nil {
					continue
				}
				fr.Principal = fdRound(fr.Principal, 2)
				fr.InterestAccrued = fdRound(fr.InterestAccrued, 2)
				fr.Rate = fdRound(fr.Rate, 4)
				out = append(out, fr)
			}
			if out == nil {
				out = []fdRow{}
			}
			return out, nil
		})

		// ── wait for all goroutines ───────────────────────────────────────────
		wg.Wait()

		// ── build bank_concentration chart + limit_utilization KPI ───────────
		// Both derived from the bank_concentration sub-result so the chart and
		// KPI agree on the same per-bank limits.
		type bcChartRow struct {
			Bank           string  `json:"bank"`
			Exposure       float64 `json:"exposure"`
			Limit          float64 `json:"limit"`
			Pct            float64 `json:"pct"`
			UtilizationPct float64 `json:"utilization_pct"`
			RemainingLimit float64 `json:"remaining_limit"`
			Breach         bool    `json:"breach"`
		}
		var bankConcChart interface{} = []bcChartRow{}
		var limitUtilization interface{} = map[string]interface{}{
			"total_exposure":  0.0,
			"total_limit":     0.0,
			"utilization_pct": 0.0,
			"banks_breached":  0,
			"banks_critical":  0,
			"banks_total":     0,
		}
		if bcRes, ok := results["bank_concentration"]; ok && bcRes.err == nil {
			if bcMap, ok2 := bcRes.data.(map[string]interface{}); ok2 {
				if rawData, ok3 := bcMap["data"]; ok3 {
					if jsonB, jerr := json.Marshal(rawData); jerr == nil {
						type wireRow struct {
							Bank           string  `json:"bank"`
							Exposure       float64 `json:"exposure"`
							Limit          float64 `json:"limit"`
							Pct            float64 `json:"pct"`
							UtilizationPct float64 `json:"utilization_pct"`
							RemainingLimit float64 `json:"remaining_limit"`
							Breach         bool    `json:"breach"`
						}
						var wireRows []wireRow
						if json.Unmarshal(jsonB, &wireRows) == nil && len(wireRows) > 0 {
							out := make([]bcChartRow, len(wireRows))
							var totExp, totLim float64
							var breached, critical int
							for i, r := range wireRows {
								out[i] = bcChartRow{
									Bank:           r.Bank,
									Exposure:       r.Exposure,
									Limit:          r.Limit,
									Pct:            r.Pct,
									UtilizationPct: r.UtilizationPct,
									RemainingLimit: r.RemainingLimit,
									Breach:         r.Breach,
								}
								totExp += r.Exposure
								totLim += r.Limit
								if r.Breach {
									breached++
								} else if r.UtilizationPct >= 90 {
									critical++
								}
							}
							bankConcChart = out
							utilPct := 0.0
							if totLim > 0 {
								utilPct = fdRound(totExp/totLim*100, 2)
							}
							limitUtilization = map[string]interface{}{
								"total_exposure":  fdRound(totExp, 2),
								"total_limit":     fdRound(totLim, 2),
								"utilization_pct": utilPct,
								"banks_breached":  breached,
								"banks_critical":  critical,
								"banks_total":     len(wireRows),
							}
						}
					}
				}
			}
		}

		// ── extract helper ─────────────────────────────────────────────────────
		get := func(key string) interface{} {
			if r, ok := results[key]; ok && r.err == nil {
				return r.data
			}
			return nil
		}

		// ── extract closing_checklist from closing_status sub-result ────────
		// The closing_status goroutine returns the checklist embedded in the
		// same map; surface it under governance for the frontend.
		var closingChecklist interface{} = []interface{}{}
		if csRes, ok := results["closing_status"]; ok && csRes.err == nil {
			if csMap, ok2 := csRes.data.(map[string]interface{}); ok2 {
				if cl, ok3 := csMap["checklist"]; ok3 {
					closingChecklist = cl
				}
			}
		}

		// ── assemble final payload ────────────────────────────────────────────
		payload := map[string]interface{}{
			"generated_at": now.Format(time.RFC3339),
			"filters": map[string]interface{}{
				"entity_id": entityFilter,
				"currency":  req.Currency,
				"period":    req.Period,
			},
			"kpis": map[string]interface{}{
				"total_exposure":     get("total_exposure"),
				"bank_concentration": get("bank_concentration"),
				"maturity":           get("maturity"),
				"interest":           get("interest"),
				"exceptions":         get("exceptions"),
				"variance_impact":    get("variance_impact"),
				"limit_utilization":  limitUtilization,
			},
			"charts": map[string]interface{}{
				"maturity_ladder":    get("maturity_ladder"),
				"interest_trend":     get("interest_trend"),
				"rate_distribution":  get("rate_distribution"),
				"bank_concentration": bankConcChart,
			},
			"governance": map[string]interface{}{
				"approvals":         get("governance_approvals"),
				"closing_status":    get("closing_status"),
				"closing_checklist": closingChecklist,
			},
			"fd_list": get("fd_list"),
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}
