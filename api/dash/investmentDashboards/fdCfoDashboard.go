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
		run("bank_concentration", func(ctx context.Context) (interface{}, error) {
			// total for pct calc (separate simple query to avoid correlated sub-query issues)
			var totalAmt float64
			_ = pool.QueryRow(ctx,
				`SELECT COALESCE(SUM(m.principal_amount),0)
				 FROM investment.fd_master m
				 LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				 WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				   AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`,
				entityFilter).Scan(&totalAmt)

			sqlStr := `
				SELECT
				  COALESCE(m.bank_name, m.bank_id) AS bank,
				  COALESCE(SUM(m.principal_amount), 0) AS exposure,
				  COALESCE(lim.credit_limit, 0) AS lim
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				LEFT JOIN LATERAL (
				  SELECT COALESCE(credit_limit, 0) AS credit_limit
				  FROM investment.fd_bank_config_master bc
				  WHERE bc.bank_id = m.bank_id
				    AND COALESCE(bc.is_deleted, false)=false
				  ORDER BY bc.created_at DESC LIMIT 1
				) lim ON true
				WHERE m.is_deleted=false AND m.fd_status IN ('ACTIVE','MATURED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				GROUP BY COALESCE(m.bank_name, m.bank_id), lim.credit_limit
				ORDER BY exposure DESC`

			type bcRow struct {
				Bank     string  `json:"bank"`
				Exposure float64 `json:"exposure"`
				Limit    float64 `json:"limit"`
				Pct      float64 `json:"pct"`
			}

			rows, err := pool.Query(ctx, sqlStr, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			var data []bcRow
			for rows.Next() {
				var br bcRow
				if err := rows.Scan(&br.Bank, &br.Exposure, &br.Limit); err != nil {
					continue
				}
				if totalAmt > 0 {
					br.Pct = fdRound(br.Exposure/totalAmt*100, 2)
				}
				data = append(data, br)
			}
			if len(data) == 0 {
				return map[string]interface{}{
					"top_bank": "", "top_pct": 0, "breach": false, "data": []bcRow{},
				}, nil
			}
			top := data[0]
			breach := top.Limit > 0 && top.Exposure > top.Limit
			return map[string]interface{}{
				"top_bank": top.Bank,
				"top_pct":  top.Pct,
				"breach":   breach,
				"data":     data,
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

		// ── 5. exceptions ─────────────────────────────────────────────────────
		run("exceptions", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  COUNT(DISTINCT ae.exception_id) AS cnt,
				  COALESCE(SUM(m.principal_amount),0) AS val,
				  ae.exception_type
				FROM investment.fd_accrual_exception ae
				LEFT JOIN investment.fd_master m ON m.fd_id = ae.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE COALESCE(ae.is_deleted,false)=false
				  AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				GROUP BY ae.exception_type
				ORDER BY cnt DESC`
			rows, err := pool.Query(ctx, sql, entityFilter)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			type bkRow struct {
				Type  string `json:"type"`
				Count int64  `json:"count"`
			}
			var breakup []bkRow
			var totalCnt int64
			var totalVal float64
			for rows.Next() {
				var br bkRow
				var val float64
				if err := rows.Scan(&br.Count, &val, &br.Type); err != nil {
					continue
				}
				totalCnt += br.Count
				totalVal += val
				breakup = append(breakup, br)
			}
			if breakup == nil {
				breakup = []bkRow{}
			}
			return map[string]interface{}{
				"count":   totalCnt,
				"value":   fdRound(totalVal, 2),
				"breakup": breakup,
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

		// ── 10. governance — closing_status ───────────────────────────────────
		run("closing_status", func(ctx context.Context) (interface{}, error) {
			sql := `
				SELECT
				  COUNT(*) FILTER (WHERE cr.closure_status IN ('COMPLETED','CLOSED'))   AS completed,
				  COUNT(*) AS total,
				  COUNT(*) FILTER (WHERE cr.closure_status IN ('PENDING_APPROVAL','SUBMITTED','PROCESSING'))
				    AS pending,
				  COUNT(*) FILTER (WHERE cr.closure_status = 'BLOCKED')                AS blockers
				FROM investment.fd_closure_request cr
				LEFT JOIN investment.fd_master m ON m.fd_id = cr.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE cr.is_deleted=false
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)`
			var completed, total, pending, blockers int64
			err := pool.QueryRow(ctx, sql, entityFilter).Scan(&completed, &total, &pending, &blockers)
			if err != nil {
				return nil, err
			}
			pct := 0.0
			if total > 0 {
				pct = fdRound(float64(completed)/float64(total)*100, 1)
			}
			return map[string]interface{}{
				"completed": completed,
				"total":     total,
				"pct":       pct,
				"blockers":  blockers,
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

		// ── build bank_concentration chart (reuse KPI data) ──────────────────
		type bcChartRow struct {
			Bank           string  `json:"bank"`
			Exposure       float64 `json:"exposure"`
			Pct            float64 `json:"pct"`
			RemainingLimit float64 `json:"remaining_limit"`
		}
		var bankConcChart interface{} = []bcChartRow{}
		if bcRes, ok := results["bank_concentration"]; ok && bcRes.err == nil {
			if bcMap, ok2 := bcRes.data.(map[string]interface{}); ok2 {
				if rawData, ok3 := bcMap["data"]; ok3 {
					// data is []bcRow from the closure above — use reflect-free JSON round-trip
					if jsonB, jerr := json.Marshal(rawData); jerr == nil {
						type wireRow struct {
							Bank     string  `json:"bank"`
							Exposure float64 `json:"exposure"`
							Limit    float64 `json:"limit"`
							Pct      float64 `json:"pct"`
						}
						var wireRows []wireRow
						if json.Unmarshal(jsonB, &wireRows) == nil && len(wireRows) > 0 {
							out := make([]bcChartRow, len(wireRows))
							for i, r := range wireRows {
								out[i] = bcChartRow{
									Bank:           r.Bank,
									Exposure:       r.Exposure,
									Pct:            r.Pct,
									RemainingLimit: math.Max(0, r.Limit-r.Exposure),
								}
							}
							bankConcChart = out
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
			},
			"charts": map[string]interface{}{
				"maturity_ladder":    get("maturity_ladder"),
				"interest_trend":     get("interest_trend"),
				"rate_distribution":  get("rate_distribution"),
				"bank_concentration": bankConcChart,
			},
			"governance": map[string]interface{}{
				"approvals":      get("governance_approvals"),
				"closing_status": get("closing_status"),
			},
			"fd_list": get("fd_list"),
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}
