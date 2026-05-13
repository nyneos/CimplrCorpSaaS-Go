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
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── request / response types ────────────────────────────────────────────────

type fdCfoDashRequest struct {
	UserID    string `json:"user_id"`
	EntityID  string `json:"entity_id"`
	Currency  string `json:"currency"`
	Period    string `json:"period"`     // MTD | QTD | YTD | CUSTOM
	StartDate string `json:"start_date"` // YYYY-MM-DD — used when Period=="CUSTOM"
	EndDate   string `json:"end_date"`   // YYYY-MM-DD — used when Period=="CUSTOM"
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
		run("total_exposure", func(ctx context.Context) (interface{}, error) {
			sqlStr := `
				SELECT
				  COALESCE(SUM(m.principal_amount), 0) AS value,
				  COUNT(*)                              AS fd_count
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted = false
				  AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text = '' OR COALESCE(m.entity_id,b.entity_id) = $1)`
			var value float64
			var count int64
			err := pool.QueryRow(ctx, sqlStr, entityFilter).Scan(&value, &count)
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

			// (c) total distinct FDs at risk + principal sum (de-duplicated)
			var distinctCount int64
			var totalVal float64
			_ = pool.QueryRow(ctx, `
				WITH at_risk AS (
				  SELECT DISTINCT ae.fd_id
				  FROM investment.fd_accrual_exception ae
				  WHERE COALESCE(ae.is_deleted,false)=false
				    AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  UNION
				  SELECT DISTINCT vl.record_id::text AS fd_id
				  FROM public.variance_log vl
				  WHERE vl.module_code LIKE 'FD_%'
				    AND vl.status='OPEN'
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

			return map[string]interface{}{
				"open_count":      openCnt,
				"high_count":      highCnt,
				"exception_count": excCnt,
				"amount_impact":   fdRound(amtImpact, 2),
				"avg_rate_delta":  fdRound(avgRateDelta, 4),
				"avg_day_delta":   fdRound(avgDayDelta, 1),
				"breakup":         breakup,
			}, nil
		})

		// ── 6. maturity_ladder (chart) ────────────────────────────────────────
		run("maturity_ladder", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  CASE
				    WHEN m.maturity_date BETWEEN CURRENT_DATE AND CURRENT_DATE+6   THEN 'Wk 1'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+7  AND CURRENT_DATE+13  THEN 'Wk 2'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+14 AND CURRENT_DATE+20  THEN 'Wk 3'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+21 AND CURRENT_DATE+30  THEN 'Wk 4'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+31 AND CURRENT_DATE+60  THEN '1–2 Mo'
				    WHEN m.maturity_date BETWEEN CURRENT_DATE+61 AND CURRENT_DATE+90  THEN '2–3 Mo'
				    ELSE '3+ Mo'
				  END AS period,
				  COALESCE(SUM(m.principal_amount),0) AS amount,
				  COUNT(*) AS count
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status='ACTIVE'
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				  AND m.maturity_date >= CURRENT_DATE
				GROUP BY 1
				ORDER BY MIN(m.maturity_date)`
			rows, err := pool.Query(ctx, sql, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type ladderRow struct {
				Period string  `json:"period"`
				Amount float64 `json:"amount"`
				Count  int64   `json:"count"`
			}
			var out []ladderRow
			for rows.Next() {
				var lr ladderRow
				if err := rows.Scan(&lr.Period, &lr.Amount, &lr.Count); err != nil {
					continue
				}
				lr.Amount = fdRound(lr.Amount, 2)
				out = append(out, lr)
			}
			if out == nil {
				out = []ladderRow{}
			}
			return out, nil
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
		run("governance_approvals", func(ctx context.Context) (interface{}, error) {
			// Booking approvals pending
			sql := `
				SELECT 'Booking Approvals' AS type,
				       COUNT(*) AS count,
				       COALESCE(SUM(b.principal_amount),0) AS value,
				       'High' AS priority
				FROM investment.fd_booking_request b
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('PENDING_APPROVAL','SUBMITTED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				UNION ALL
				SELECT 'Maturity Decisions' AS type,
				       COUNT(*) AS count,
				       COALESCE(SUM(m.principal_amount),0) AS value,
				       'Medium' AS priority
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false AND m.fd_status='ACTIVE'
				  AND m.maturity_instructions IS NULL
				  AND m.maturity_date <= CURRENT_DATE + 30
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				UNION ALL
				SELECT 'Closure Approvals' AS type,
				       COUNT(*) AS count,
				       COALESCE(SUM(m.principal_amount),0) AS value,
				       'High' AS priority
				FROM investment.fd_closure_request cr
				LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE cr.is_deleted=false
				  AND cr.closure_status IN ('PENDING_APPROVAL','SUBMITTED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`
			rows, err := pool.Query(ctx, sql, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type approvalRow struct {
				Type     string  `json:"type"`
				Count    int64   `json:"count"`
				Value    float64 `json:"value"`
				Priority string  `json:"priority"`
			}
			var out []approvalRow
			for rows.Next() {
				var ar approvalRow
				if err := rows.Scan(&ar.Type, &ar.Count, &ar.Value, &ar.Priority); err != nil {
					continue
				}
				ar.Value = fdRound(ar.Value, 2)
				out = append(out, ar)
			}
			if out == nil {
				out = []approvalRow{}
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
				  COALESCE(TO_CHAR(m.start_date,'YYYY-MM-DD'),'') AS start_date,
				  COALESCE(m.tenure_days, 0) AS tenure_days,
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
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				ORDER BY m.maturity_date ASC
				LIMIT 200`
			rows, err := pool.Query(ctx, sql, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			type fdRow struct {
				FDID            string  `json:"fd_id"`
				Entity          string  `json:"entity"`
				EntityID        string  `json:"entity_id"`
				Bank            string  `json:"bank"`
				BankFDRefNo     string  `json:"bank_fd_ref_no"`
				Principal       float64 `json:"principal"`
				Rate            float64 `json:"rate"`
				InterestTypeCode string `json:"interest_type_code"`
				StartDate       string  `json:"start_date"`
				TenureDays      int     `json:"tenure_days"`
				MaturityDate    string  `json:"maturity_date"`
				InterestAccrued float64 `json:"interest_accrued"`
				Status          string  `json:"status"`
				BookingID       string  `json:"booking_id"`
				BookingStatus   string  `json:"booking_status"`
				ClosureType     string  `json:"closure_type"`
				ClosureStatus   string  `json:"closure_status"`
			}
			var out []fdRow
			for rows.Next() {
				var fr fdRow
				if err := rows.Scan(
					&fr.FDID, &fr.Entity, &fr.EntityID, &fr.Bank, &fr.BankFDRefNo,
					&fr.Principal, &fr.Rate, &fr.InterestTypeCode,
					&fr.StartDate, &fr.TenureDays, &fr.MaturityDate,
					&fr.InterestAccrued, &fr.Status,
					&fr.BookingID, &fr.BookingStatus,
					&fr.ClosureType, &fr.ClosureStatus,
				); err != nil {
					continue
				}
				fr.Principal = fdRound(fr.Principal, 2)
				fr.InterestAccrued = fdRound(fr.InterestAccrued, 2)
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
