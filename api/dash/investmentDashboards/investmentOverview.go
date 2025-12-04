package investmentdashboards

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"fmt"
	"io"
	"strconv"

	// "CimplrCorpSaas/api/dash/liqsnap"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type KPICard struct {
	Title     string  `json:"title"`
	Value     float64 `json:"value"`
	LastValue float64 `json:"lastValue"`
	Change    float64 `json:"change"`
}

// GetInvestmentOverviewKPIs computes the 4 KPI cards: Total AUM, YTD P&L, Portfolio XIRR, Liquidity Position
// Request JSON: { "entity_name": "Optional entity filter" }
func GetInvestmentOverviewKPIs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := context.Background()

		// financial year boundaries
		today := time.Now()
		fyStart := getFinancialYearStart(today)
		lastFyEnd := fyStart.Add(-time.Nanosecond)
		// lastFyStart := getFinancialYearStart(fyStart.AddDate(-1,0,0))

		// 1) Total AUM (current): sum current_value from portfolio_snapshot
		var totalAUM float64
		aumQ := `SELECT COALESCE(SUM(current_value),0)::float8 FROM investment.portfolio_snapshot WHERE ($1::text IS NULL OR entity_name = $1)`
		_ = pgxPool.QueryRow(ctx, aumQ, nullIfEmpty(req.EntityName)).Scan(&totalAUM)

		// 1b) Last financial year AUM approximation: aggregate units up to lastFyEnd and multiply by latest NAV as of that date
		var lastAUM float64
		lastAumQ := `
            WITH tx AS (
                SELECT COALESCE(ot.scheme_id, ot.scheme_internal_code, ms.scheme_id::text) AS scheme_ref,
                       SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription') THEN COALESCE(ot.units,0) ELSE -COALESCE(ot.units,0) END) AS units
                FROM investment.onboard_transaction ot
                LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
                WHERE ot.transaction_date <= $2
                  AND ($1::text IS NULL OR COALESCE(ot.entity_name, '') = $1)
                GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code, ms.scheme_id::text)
            ), navs AS (
                SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_ref, nav_value
                FROM investment.amfi_nav_staging
                WHERE nav_date <= $2
                ORDER BY scheme_code, nav_date DESC
            )
            SELECT COALESCE(SUM(tx.units * COALESCE(n.nav_value,0)),0)::float8 FROM tx LEFT JOIN navs n ON n.scheme_ref = tx.scheme_ref
        `
		_ = pgxPool.QueryRow(ctx, lastAumQ, nullIfEmpty(req.EntityName), lastFyEnd.Format("2006-01-02")).Scan(&lastAUM)

		// 2) YTD P&L approx: (current AUM) - (sum buys since FY start) + (sum sell proceeds since FY start)
		var buysSinceFY, sellsSinceFY float64
		_ = pgxPool.QueryRow(ctx, `SELECT COALESCE(SUM(amount),0)::float8 FROM investment.onboard_transaction WHERE transaction_date >= $1 AND LOWER(transaction_type) IN ('buy','purchase','subscription') AND ($2::text IS NULL OR COALESCE(entity_name,'') = $2)`, fyStart.Format("2006-01-02"), nullIfEmpty(req.EntityName)).Scan(&buysSinceFY)
		_ = pgxPool.QueryRow(ctx, `SELECT COALESCE(SUM(amount),0)::float8 FROM investment.onboard_transaction WHERE transaction_date >= $1 AND LOWER(transaction_type) IN ('sell','redemption') AND ($2::text IS NULL OR COALESCE(entity_name,'') = $2)`, fyStart.Format("2006-01-02"), nullIfEmpty(req.EntityName)).Scan(&sellsSinceFY)
		ytdPL := (totalAUM - buysSinceFY) + sellsSinceFY

		// 3) Portfolio XIRR: build cash flows from onboard_transaction (buys negative, sells positive) + terminal value
		flowsQ := `SELECT transaction_date, CASE WHEN LOWER(transaction_type) IN ('buy','purchase','subscription') THEN -amount WHEN LOWER(transaction_type) IN ('sell','redemption') THEN amount ELSE 0 END AS flow_amount FROM investment.onboard_transaction WHERE ($1::text IS NULL OR COALESCE(entity_name,'') = $1) AND LOWER(transaction_type) IN ('buy','purchase','subscription','sell','redemption')`
		rows, err := pgxPool.Query(ctx, flowsQ, nullIfEmpty(req.EntityName))
		flows := make([]CashFlow, 0)
		if err == nil {
			for rows.Next() {
				var d time.Time
				var amt float64
				if err := rows.Scan(&d, &amt); err == nil {
					flows = append(flows, CashFlow{Date: d, Amount: amt})
				}
			}
			rows.Close()
		}
		// add terminal positive flow as current AUM today
		flows = append(flows, CashFlow{Date: today, Amount: totalAUM})
		xirrVal, xirrErr := ComputeXIRR(flows)

		// 4) Liquidity Position: cash (bank_statement + manual approved balances) + sellable MF holdings
		// Use latest-per-account up to 'yesterday' when aggregating balances to avoid double-counting.
		yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")

		// cash from approved bank_statement: pick latest record per account_no (as_of_date <= yesterday)
		var cashFromStatements float64
		_ = pgxPool.QueryRow(ctx, `
						SELECT COALESCE(SUM(t.balance),0)::float8 FROM (
							SELECT DISTINCT ON (COALESCE(bs.account_no, '')) COALESCE(bs.account_no, '') as acct, bs.closingbalance as balance
							FROM bank_statement bs
							JOIN masterbankaccount mba ON bs.account_no = mba.account_no
							JOIN masterentity me ON mba.entity_id = me.entity_id
							WHERE bs.status = 'Approved' AND bs.as_of_date <= $2 AND ($1::text IS NULL OR me.entity_name = $1)
							ORDER BY COALESCE(bs.account_no, ''), bs.as_of_date DESC, bs.as_of_time DESC
						) t
				`, nullIfEmpty(req.EntityName), yesterday).Scan(&cashFromStatements)

		// cash from approved bank_balances_manual: pick latest per account identifier where latest audit processing_status = 'APPROVED' and as_of_date <= yesterday
		var cashFromManual float64
		_ = pgxPool.QueryRow(ctx, `
						SELECT COALESCE(SUM(t.balance),0)::float8 FROM (
							SELECT DISTINCT ON (COALESCE(mb.account_no, mb.iban, mb.nickname)) COALESCE(mb.account_no, mb.iban, mb.nickname) AS acct, mb.balance_amount AS balance
							FROM bank_balances_manual mb
							JOIN masterbankaccount mba ON mb.account_no = mba.account_number
							JOIN masterentitycash me ON mba.entity_id = me.entity_id
							JOIN (
								SELECT DISTINCT ON (balance_id) balance_id, processing_status
								FROM auditactionbankbalances
								ORDER BY balance_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
							) a ON a.balance_id = mb.balance_id AND a.processing_status = 'APPROVED'
							WHERE mb.as_of_date <= $2 AND ($1::text IS NULL OR me.entity_name = $1)
							ORDER BY COALESCE(mb.account_no, mb.iban, mb.nickname), mb.as_of_date DESC, mb.as_of_time DESC
						) t
				`, nullIfEmpty(req.EntityName), yesterday).Scan(&cashFromManual)

		totalCash := cashFromStatements + cashFromManual

		// sellable MFs: portfolio_snapshot rows with positive units and positive current_value
		var sellableMFTotal float64
		_ = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(SUM(current_value),0)::float8
			FROM investment.portfolio_snapshot
			WHERE ($1::text IS NULL OR entity_name = $1)
			  AND COALESCE(total_units,0) > 0
			  AND COALESCE(current_value,0) > 0
		`, nullIfEmpty(req.EntityName)).Scan(&sellableMFTotal)

		// also fetch top 5 sellable holdings for breakdown (scheme_name, current_value)
		topHoldings := make([]map[string]interface{}, 0)
		thRows, thErr := pgxPool.Query(ctx, `
			SELECT scheme_name, current_value
			FROM investment.portfolio_snapshot
			WHERE ($1::text IS NULL OR entity_name = $1)
			  AND COALESCE(total_units,0) > 0
			  AND COALESCE(current_value,0) > 0
			ORDER BY current_value DESC
			LIMIT 5
		`, nullIfEmpty(req.EntityName))
		if thErr == nil {
			for thRows.Next() {
				var sname string
				var val float64
				if err := thRows.Scan(&sname, &val); err == nil {
					topHoldings = append(topHoldings, map[string]interface{}{"scheme_name": sname, "current_value": val})
				}
			}
			thRows.Close()
		}

		// Keep previous text-match near-term assets as diagnostic (optional)
		var nearTermAssets float64
		_ = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(SUM(current_value),0)::float8
			FROM investment.portfolio_snapshot
			WHERE ($1::text IS NULL OR entity_name = $1)
			  AND (
				LOWER(scheme_name) LIKE '%liquid%'
				OR LOWER(scheme_name) LIKE '%arbitrage%'
				OR LOWER(scheme_name) LIKE '%ultra%'
				OR LOWER(scheme_name) LIKE '%overnight%'
				OR LOWER(scheme_name) LIKE '%ultrashort%'
			  )
		`, nullIfEmpty(req.EntityName)).Scan(&nearTermAssets)

		liquidityTotal := totalCash + sellableMFTotal

		// Prepare cards and compute change percentages
		cards := make([]KPICard, 0, 4)
		cards = append(cards, KPIChartFromValues("Total AUM", totalAUM, lastAUM))
		cards = append(cards, KPIChartFromValues("YTD P&L", ytdPL, 0))
		if xirrErr != nil || math.IsNaN(xirrVal) {
			cards = append(cards, KPICard{Title: "Portfolio XIRR", Value: 0, LastValue: 0, Change: 0})
		} else {
			cards = append(cards, KPIChartFromValues("Portfolio XIRR", xirrVal*100, 0))
		}
		cards = append(cards, KPIChartFromValues("Liquidity Position", liquidityTotal, 0))

		resp := map[string]interface{}{
			"cards":        cards,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
			"liquidity_breakdown": map[string]interface{}{
				"cash_from_statements": cashFromStatements,
				"cash_from_manual":     cashFromManual,
				"cash_total":           totalCash,
				"sellable_mf_total":    sellableMFTotal,
				"near_term_text_match": nearTermAssets,
				"top_liquid_holdings":  topHoldings,
			},
			"notes": map[string]interface{}{
				"approximation": "YTD P&L and last-FY AUM are computed with best-effort aggregates from onboard_transaction and latest NAV. If you need exact historical snapshots, schedule periodic snapshot batches.",
				"xirr_error":    xirrErrString(xirrErr),
			},
		}
		api.RespondWithPayload(w, true, "", resp)
	}
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func getFinancialYearStart(t time.Time) time.Time {
	y := t.Year()
	// FY starts on April 1st
	fy := time.Date(y, time.April, 1, 0, 0, 0, 0, time.UTC)
	if t.Before(fy) {
		fy = fy.AddDate(-1, 0, 0)
	}
	return fy
}

// CashFlow used for XIRR
type CashFlow struct {
	Date   time.Time
	Amount float64
}

// ComputeXIRR uses Newton-Raphson to solve for rate r where NPV = 0
func ComputeXIRR(flows []CashFlow) (float64, error) {
	if len(flows) < 2 {
		return 0, nil
	}
	// convert to days from first date
	base := flows[0].Date
	days := func(d time.Time) float64 { return d.Sub(base).Hours() / 24.0 / 365.0 }
	npv := func(r float64) float64 {
		s := 0.0
		for _, f := range flows {
			s += f.Amount / math.Pow(1.0+r, days(f.Date))
		}
		return s
	}
	// derivative approximate
	deriv := func(r float64) float64 {
		h := 1e-6
		return (npv(r+h) - npv(r-h)) / (2 * h)
	}
	r := 0.1
	for i := 0; i < 100; i++ {
		f := npv(r)
		df := deriv(r)
		if math.Abs(df) < 1e-12 {
			break
		}
		nr := r - f/df
		if math.IsNaN(nr) {
			break
		}
		if math.Abs(nr-r) < 1e-9 {
			r = nr
			break
		}
		r = nr
	}
	return r, nil
}

func KPIChartFromValues(title string, value, last float64) KPICard {
	var change float64
	if last == 0 {
		change = 0
	} else {
		change = ((value - last) / math.Abs(last)) * 100
	}
	return KPICard{Title: title, Value: value, LastValue: last, Change: math.Round(change*100) / 100}
}

func xirrErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// package investmentdashboards

// EntityPerformanceRow matches the frontend sample: label is scheme name, YTD P&L value
type EntityPerformanceRow struct {
	Label string  `json:"label"`
	YTD   float64 `json:"YTD P&L"`
}

// GetEntityPerformance returns per-scheme YTD P&L (since FY start) for an entity with optional filters
// Request JSON: { "entity_name": "", "amc_name": "", "scheme_category": "", "scheme_sub_category": "", "limit": 10 }
func GetEntityPerformance(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}
		var req struct {
			EntityName     string `json:"entity_name,omitempty"`
			AMCName        string `json:"amc_name,omitempty"`
			SchemeCategory string `json:"scheme_category,omitempty"`
			SchemeSubCat   string `json:"scheme_sub_category,omitempty"`
			Limit          int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := context.Background()
		if req.Limit <= 0 {
			req.Limit = 50
		}

		fyStart := getFinancialYearStart(time.Now()).Format("2006-01-02")

		// per-scheme aggregated YTD P&L approximation: (current_value) - buys_since + sells_since
		q := `
        WITH tx AS (
            SELECT COALESCE(ot.scheme_id, ot.scheme_internal_code, ms.scheme_id::text) AS scheme_ref,
                   COALESCE(ms.scheme_name, ot.scheme_internal_code, ot.scheme_id::text) AS scheme_name,
                   COALESCE(ms.amc_name,'') AS amc_name,
                   SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription') THEN COALESCE(ot.amount,0) ELSE 0 END) AS buys_since,
                   SUM(CASE WHEN LOWER(ot.transaction_type) IN ('sell','redemption') THEN COALESCE(ot.amount,0) ELSE 0 END) AS sells_since
            FROM investment.onboard_transaction ot
            LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
            WHERE ot.transaction_date >= $1
              AND ($2::text IS NULL OR COALESCE(ot.entity_name,'') = $2)
            GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code, ms.scheme_id::text), COALESCE(ms.scheme_name, ot.scheme_internal_code, ot.scheme_id::text), ms.amc_name
        ), pv AS (
            SELECT COALESCE(COALESCE(ps.scheme_id,ps.isin,ps.scheme_name::text), '') AS scheme_ref, SUM(COALESCE(ps.current_value,0)) AS current_value
            FROM investment.portfolio_snapshot ps
            WHERE ($2::text IS NULL OR ps.entity_name = $2)
            GROUP BY COALESCE(ps.scheme_id,ps.isin,ps.scheme_name::text)
        )
        SELECT COALESCE(t.scheme_name,'Unknown') AS scheme_name, COALESCE(pv.current_value,0) - COALESCE(t.buys_since,0) + COALESCE(t.sells_since,0) AS ytd_pl, t.amc_name
        FROM tx t
        LEFT JOIN pv ON pv.scheme_ref = t.scheme_ref
        WHERE ($3::text IS NULL OR t.amc_name = $3)
        ORDER BY ytd_pl DESC
        LIMIT $4
        `

		rows, err := pgxPool.Query(ctx, q, fyStart, nullIfEmpty(req.EntityName), nullIfEmpty(req.AMCName), req.Limit)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]EntityPerformanceRow, 0)
		for rows.Next() {
			var name string
			var ytd float64
			var amc string
			if err := rows.Scan(&name, &ytd, &amc); err != nil {
				continue
			}
			out = append(out, EntityPerformanceRow{Label: name, YTD: ytd})
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"rows": out, "generated_at": time.Now().UTC().Format(time.RFC3339)})
	}
}

// ConsolidatedRiskRow is the response structure for consolidated risk
type ConsolidatedRiskRow struct {
	LCR         float64 `json:"lcr"`
	TotalValue  float64 `json:"total_value"`
	LowValue    float64 `json:"low_value"`
	MediumValue float64 `json:"medium_value"`
	HighValue   float64 `json:"high_value"`
	GeneratedAt string  `json:"generated_at"`
}

// GetConsolidatedRisk computes a weighted internal risk score (0-100) for an entity
// based on scheme internal risk rating (Low/Medium/High) and current investment values.
func GetConsolidatedRisk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := context.Background()

		// Mapping: Low -> 15, Medium -> 50, High -> 85 (gives values within the gauge zones)
		// Use current_value if present, else total_units * current_nav as value
		q := `
        SELECT
          COALESCE(
            SUM(val * (
                CASE LOWER(COALESCE(ms.internal_risk_rating, 'medium'))
                  WHEN 'low' THEN 15
                  WHEN 'medium' THEN 50
                  WHEN 'high' THEN 85
                  ELSE 50
                END
            )) / NULLIF(SUM(val), 0),
            0
          ) AS lcr,
          COALESCE(SUM(val),0) AS total_value,
          COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium')) = 'low' THEN val ELSE 0 END),0) AS low_value,
          COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium')) = 'medium' THEN val ELSE 0 END),0) AS medium_value,
          COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium')) = 'high' THEN val ELSE 0 END),0) AS high_value
        FROM (
          SELECT COALESCE(ps.current_value::numeric, (ps.total_units::numeric * ps.current_nav::numeric), 0) AS val,
                 ps.scheme_id, ps.isin, ps.entity_name
          FROM investment.portfolio_snapshot ps
          WHERE ($1::text IS NULL OR ps.entity_name = $1::text)
        ) ps
        LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ps.scheme_id OR ms.internal_scheme_code = ps.scheme_id OR ms.isin = ps.isin)
        `

		row := pgxPool.QueryRow(ctx, q, nullIfEmpty(req.EntityName))

		var out ConsolidatedRiskRow
		var lcrNull, total, lowv, medv, highv float64
		if err := row.Scan(&lcrNull, &total, &lowv, &medv, &highv); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		out.LCR = lcrNull
		out.TotalValue = total
		out.LowValue = lowv
		out.MediumValue = medv
		out.HighValue = highv
		out.GeneratedAt = time.Now().UTC().Format(time.RFC3339)

		api.RespondWithPayload(w, true, "", out)
	}
}

type WaterfallRow struct {
	Label        string   `json:"label"`
	Contribution *float64 `json:"contribution,omitempty"`
	OpeningAUM   *float64 `json:"opening_aum,omitempty"`
	ClosingAUM   *float64 `json:"closing_aum,omitempty"`
}

// GetAMCWaterfall computes opening/closing AUM per AMC and returns waterfall-style rows
func GetAMCWaterfall(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName  string `json:"entity_name,omitempty"`
			PeriodStart string `json:"period_start,omitempty"` // YYYY-MM-DD
			PeriodEnd   string `json:"period_end,omitempty"`   // YYYY-MM-DD
			Limit       int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		// default date range: start = financial year start (1 Apr), end = today
		now := time.Now().UTC()
		if req.PeriodEnd == "" {
			req.PeriodEnd = now.Format("2006-01-02")
		}
		if req.PeriodStart == "" {
			fyStart := getFinancialYearStart(now)
			req.PeriodStart = fyStart.Format("2006-01-02")
		}

		ctx := context.Background()

		q := `
        WITH start_snap AS (
          SELECT DISTINCT ON (COALESCE(ps.scheme_id, ps.isin)) COALESCE(ps.scheme_id, ps.isin)::text AS scheme_ref,
                 ps.total_units::numeric AS total_units, ps.avg_nav::numeric AS avg_nav, ps.current_nav::numeric AS current_nav,
                 ps.current_value::numeric AS current_value, ps.entity_name
          FROM investment.portfolio_snapshot ps
          WHERE ps.created_at <= $2::date
          ORDER BY COALESCE(ps.scheme_id, ps.isin), ps.created_at DESC
        ), end_snap AS (
          SELECT DISTINCT ON (COALESCE(ps.scheme_id, ps.isin)) COALESCE(ps.scheme_id, ps.isin)::text AS scheme_ref,
                 ps.total_units::numeric AS total_units, ps.avg_nav::numeric AS avg_nav, ps.current_nav::numeric AS current_nav,
                 ps.current_value::numeric AS current_value, ps.entity_name
          FROM investment.portfolio_snapshot ps
          WHERE ps.created_at <= $3::date
          ORDER BY COALESCE(ps.scheme_id, ps.isin), ps.created_at DESC
        ), start_amc AS (
          SELECT COALESCE(ms.amc_name,'') AS amc_name,
                 SUM(COALESCE(s.total_units,0) * COALESCE(s.avg_nav,0))::numeric AS start_value
          FROM start_snap s
          LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = s.scheme_ref OR ms.internal_scheme_code = s.scheme_ref OR ms.isin = s.scheme_ref)
          WHERE ($1::text IS NULL OR s.entity_name = $1::text)
          GROUP BY COALESCE(ms.amc_name,'')
        ), end_amc AS (
          SELECT COALESCE(ms.amc_name,'') AS amc_name,
                 SUM(COALESCE(e.total_units,0) * COALESCE(e.current_nav,0))::numeric AS end_value
          FROM end_snap e
          LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = e.scheme_ref OR ms.internal_scheme_code = e.scheme_ref OR ms.isin = e.scheme_ref)
          WHERE ($1::text IS NULL OR e.entity_name = $1::text)
          GROUP BY COALESCE(ms.amc_name,'')
        ), amc_delta AS (
          SELECT COALESCE(e.amc_name, s.amc_name) AS amc_name,
                 COALESCE(s.start_value,0)::float8 AS start_value,
                 COALESCE(e.end_value,0)::float8 AS end_value,
                 (COALESCE(e.end_value,0) - COALESCE(s.start_value,0))::float8 AS delta
          FROM start_amc s
          FULL OUTER JOIN end_amc e ON e.amc_name = s.amc_name
        ), totals AS (
          SELECT SUM(start_value)::float8 AS opening_total, SUM(end_value)::float8 AS closing_total FROM amc_delta
        )
        SELECT ad.amc_name, ad.start_value, ad.end_value, ad.delta, t.opening_total, t.closing_total
        FROM amc_delta ad, totals t
        ORDER BY ad.delta DESC
        `

		rows, err := pgxPool.Query(ctx, q, nullIfEmpty(req.EntityName), req.PeriodStart, req.PeriodEnd)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		type rowOut struct {
			AMCName      string  `json:"amc_name"`
			StartValue   float64 `json:"start_value"`
			EndValue     float64 `json:"end_value"`
			Delta        float64 `json:"delta"`
			OpeningTotal float64 `json:"opening_total"`
			ClosingTotal float64 `json:"closing_total"`
		}

		amcRows := make([]rowOut, 0)
		var openingTotal, closingTotal float64
		for rows.Next() {
			var r rowOut
			if err := rows.Scan(&r.AMCName, &r.StartValue, &r.EndValue, &r.Delta, &r.OpeningTotal, &r.ClosingTotal); err != nil {
				continue
			}
			openingTotal = r.OpeningTotal
			closingTotal = r.ClosingTotal
			amcRows = append(amcRows, r)
		}

		// Build waterfall rows: Opening, each AMC delta, Closing
		out := make([]WaterfallRow, 0)
		ot := openingTotal
		out = append(out, WaterfallRow{Label: "Opening AUM", OpeningAUM: &ot})

		for _, r := range amcRows {
			d := r.Delta
			out = append(out, WaterfallRow{Label: r.AMCName, Contribution: &d})
		}

		ct := closingTotal
		out = append(out, WaterfallRow{Label: "Closing AUM", ClosingAUM: &ct})

		api.RespondWithPayload(w, true, "", map[string]interface{}{"rows": out, "opening_total": openingTotal, "closing_total": closingTotal, "generated_at": time.Now().UTC().Format(time.RFC3339)})
	}
}

type AMCPerfRow struct {
	AMCName      string   `json:"amc_name"`
	StartValue   float64  `json:"start_value"`
	CurrentValue float64  `json:"current_value"`
	PnL          float64  `json:"pnl"`
	PnLPercent   *float64 `json:"pnl_percent"`
}

// GetAMCPerformance returns per-AMC AUM at FY start (1 Apr) and now, with P&L
func GetAMCPerformance(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}
		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Limit      int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := context.Background()
		if req.Limit <= 0 {
			req.Limit = 100
		}

		// use only portfolio_snapshot to compute start/current per your formula

		q := `
                SELECT
                    COALESCE(ms.amc_name, '') AS amc_name,
                    SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0))::float8 AS start_value,
                    SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.current_nav::numeric,0))::float8 AS current_value,
                    (SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.current_nav::numeric,0)) - SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0)))::float8 AS pnl,
                    CASE WHEN SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0)) = 0 THEN NULL
                             ELSE ((SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.current_nav::numeric,0)) - SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0))) / SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0))) * 100
                    END AS pnl_percent
                FROM investment.portfolio_snapshot ps
                LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ps.scheme_id OR ms.internal_scheme_code = ps.scheme_id OR ms.isin = ps.isin)
                WHERE ($1::text IS NULL OR ps.entity_name = $1::text)
                GROUP BY COALESCE(ms.amc_name, '')
                ORDER BY pnl DESC
                LIMIT $2::int
                `

		rows, err := pgxPool.Query(ctx, q, nullIfEmpty(req.EntityName), req.Limit)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]AMCPerfRow, 0)
		for rows.Next() {
			var (
				name     string
				startVal float64
				currVal  float64
				pnl      float64
				pct      sql.NullFloat64
			)

			if err := rows.Scan(&name, &startVal, &currVal, &pnl, &pct); err != nil {
				continue
			}

			var pctPtr *float64
			if pct.Valid {
				v := pct.Float64
				pctPtr = &v
			}

			out = append(out, AMCPerfRow{AMCName: name, StartValue: startVal, CurrentValue: currVal, PnL: pnl, PnLPercent: pctPtr})
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"rows": out, "generated_at": time.Now().UTC().Format(time.RFC3339)})
	}
}

type TopAssetRow struct {
	Title    string  `json:"title"`
	Subtitle string  `json:"subtitle"`
	Pct      string  `json:"pct"`
	Value    float64 `json:"value"`
}

// GetTopPerformingAssets returns top N performing assets using snapshot only
func GetTopPerformingAssets(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Limit      int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		// default limit
		if req.Limit <= 0 {
			req.Limit = 3
		}

		ctx := context.Background()

		// Clean, simple SQL:
		// Just calculate start_value (units * avg_nav)
		// and end_value (units * current_nav)
		q := `
			SELECT
				ps.scheme_name,
				COALESCE(ms.amc_name, '') AS amc_name,
				(ps.total_units::numeric * ps.avg_nav::numeric)::float8 AS start_value,
				(ps.total_units::numeric * ps.current_nav::numeric)::float8 AS end_value,
				CASE 
					WHEN ps.avg_nav = 0 THEN NULL
					ELSE (((ps.total_units * ps.current_nav) - (ps.total_units * ps.avg_nav)) / NULLIF((ps.total_units * ps.avg_nav), 0)) * 100
				END AS pct
			FROM investment.portfolio_snapshot ps
			LEFT JOIN investment.masterscheme ms 
			ON (
				ms.scheme_id = ps.scheme_id OR 
				ms.internal_scheme_code = ps.scheme_id OR 
				ms.isin = ps.isin
			)
			WHERE ($1::text IS NULL OR ps.entity_name = $1::text)
			ORDER BY pct DESC NULLS LAST
			LIMIT $2;
		`

		rows, err := pgxPool.Query(ctx, q, nullIfEmpty(req.EntityName), req.Limit)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]TopAssetRow, 0)

		for rows.Next() {
			var (
				name   string
				amc    string
				startV float64
				endV   float64
				pct    sql.NullFloat64
			)

			if err := rows.Scan(&name, &amc, &startV, &endV, &pct); err != nil {
				continue
			}

			pctStr := "0%"

			if pct.Valid {
				pctStr = formatPercent(pct.Float64)
			}

			out = append(out, TopAssetRow{
				Title:    name,
				Subtitle: amc,
				Pct:      pctStr,
				Value:    endV,
			})
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows":         out,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// Format percentage nicely
func formatPercent(v float64) string {
	sign := ""
	if v > 0 {
		sign = "+"
	}
	return fmt.Sprintf("%s%.1f%% YTD", sign, v)
}

// GetAUMCompositionTrend returns monthly AUM breakdown by AMC for stacked area chart (Financial Year: Apr-Mar)
// Request JSON: { "entity_name": "optional", "year": 2025 } (year = FY start year, e.g., 2025 means FY 2025-26: Apr 2025 to Mar 2026)
// Response: { rows: [{ month: "Apr", "AMC1": 1000, "AMC2": 2000, ... }, ...], amc_names: [...] }
//
// NOTE: Uses portfolio_snapshot for current AUM (authoritative) and transaction history + NAV for historical months.
func GetAUMCompositionTrend(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Year       int    `json:"year,omitempty"` // FY start year (e.g., 2025 = Apr 2025 to Mar 2026)
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		// Default to current financial year
		now := time.Now().UTC()
		if req.Year <= 0 {
			// Determine current FY start year
			if now.Month() >= time.April {
				req.Year = now.Year()
			} else {
				req.Year = now.Year() - 1
			}
		}

		ctx := context.Background()

		// Step 1: Get all unique AMC names from portfolio_snapshot (current holdings)
		amcQuery := `
			SELECT DISTINCT COALESCE(ms.amc_name, 'Unknown') AS amc_name
			FROM investment.portfolio_snapshot ps
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ps.scheme_id OR 
				ms.internal_scheme_code = ps.scheme_id OR 
				ms.isin = ps.isin
			)
			WHERE ($1::text IS NULL OR ps.entity_name = $1::text)
			  AND COALESCE(ms.amc_name, '') != ''
			ORDER BY amc_name
		`
		amcRows, err := pgxPool.Query(ctx, amcQuery, nullIfEmpty(req.EntityName))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		amcNames := make([]string, 0)
		for amcRows.Next() {
			var amc string
			if err := amcRows.Scan(&amc); err == nil && amc != "" {
				amcNames = append(amcNames, amc)
			}
		}
		amcRows.Close()

		// Step 2: Generate 12 months for FY (April to March)
		type monthInfo struct {
			Label     string
			EndDate   time.Time
			IsCurrent bool
		}
		fyMonths := []time.Month{
			time.April, time.May, time.June, time.July, time.August, time.September,
			time.October, time.November, time.December, time.January, time.February, time.March,
		}
		months := make([]monthInfo, 0, 12)
		for i, m := range fyMonths {
			year := req.Year
			if i >= 9 { // Jan, Feb, Mar are in next calendar year
				year = req.Year + 1
			}
			// Last day of month
			firstOfMonth := time.Date(year, m, 1, 0, 0, 0, 0, time.UTC)
			lastOfMonth := firstOfMonth.AddDate(0, 1, -1)

			// Don't include future months
			if lastOfMonth.After(now) {
				// For current month, use today as end date
				if firstOfMonth.Before(now) || firstOfMonth.Equal(now) {
					months = append(months, monthInfo{
						Label:     m.String()[:3],
						EndDate:   now,
						IsCurrent: true,
					})
				}
				continue
			}

			months = append(months, monthInfo{
				Label:     m.String()[:3],
				EndDate:   lastOfMonth,
				IsCurrent: false,
			})
		}

		// Query for current month: use portfolio_snapshot (authoritative current value)
		currentMonthQuery := `
			SELECT COALESCE(ms.amc_name, 'Unknown') AS amc_name,
			       SUM(COALESCE(ps.current_value::numeric, 0))::float8 AS aum_value
			FROM investment.portfolio_snapshot ps
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ps.scheme_id OR 
				ms.internal_scheme_code = ps.scheme_id OR 
				ms.isin = ps.isin
			)
			WHERE ($1::text IS NULL OR ps.entity_name = $1::text)
			GROUP BY COALESCE(ms.amc_name, 'Unknown')
		`

		// Query for historical months: use transactions + NAV
		historicalQuery := `
			WITH units_by_scheme AS (
				SELECT 
					COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref,
					COALESCE(ms.amc_name, 'Unknown') AS amc_name,
					SUM(
						CASE 
							WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in') 
							THEN COALESCE(ot.units, 0)
							WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') 
							THEN -COALESCE(ot.units, 0)
							ELSE 0
						END
					) AS total_units,
					ms.amfi_scheme_code
				FROM investment.onboard_transaction ot
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ot.scheme_id OR 
					ms.internal_scheme_code = ot.scheme_internal_code OR 
					ms.isin = ot.scheme_id
				)
				WHERE ot.transaction_date <= $2::date
				  AND ($1::text IS NULL OR COALESCE(ot.entity_name, '') = $1::text)
				GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code), COALESCE(ms.amc_name, 'Unknown'), ms.amfi_scheme_code
				HAVING SUM(
					CASE 
						WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in') 
						THEN COALESCE(ot.units, 0)
						WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') 
						THEN -COALESCE(ot.units, 0)
						ELSE 0
					END
				) > 0
			),
			navs AS (
				SELECT DISTINCT ON (scheme_code) 
					scheme_code::text AS scheme_code, 
					nav_value
				FROM investment.amfi_nav_staging
				WHERE nav_date <= $2::date
				ORDER BY scheme_code, nav_date DESC
			)
			SELECT 
				u.amc_name,
				SUM(u.total_units * COALESCE(n.nav_value, 0))::float8 AS aum_value
			FROM units_by_scheme u
			LEFT JOIN navs n ON n.scheme_code = u.amfi_scheme_code::text
			GROUP BY u.amc_name
		`

		// Build output rows for each month
		outRows := make([]map[string]interface{}, 0, len(months))

		for _, m := range months {
			row := map[string]interface{}{
				"month": m.Label,
			}
			// Initialize all AMCs to 0
			for _, amc := range amcNames {
				row[amc] = 0.0
			}

			var rows pgxRows
			var err error

			if m.IsCurrent {
				// Use portfolio_snapshot for current month
				rows, err = pgxPool.Query(ctx, currentMonthQuery, nullIfEmpty(req.EntityName))
			} else {
				// Use transaction history for historical months
				rows, err = pgxPool.Query(ctx, historicalQuery, nullIfEmpty(req.EntityName), m.EndDate.Format("2006-01-02"))
			}

			if err != nil {
				outRows = append(outRows, row)
				continue
			}

			for rows.Next() {
				var amcName string
				var aumValue float64
				if err := rows.Scan(&amcName, &aumValue); err == nil {
					row[amcName] = aumValue
				}
			}
			rows.Close()

			outRows = append(outRows, row)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows":         outRows,
			"amc_names":    amcNames,
			"fy_label":     fmt.Sprintf("FY %d-%d", req.Year, (req.Year+1)%100),
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// pgxRows interface to handle both query result types
type pgxRows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close()
}

// AUMMovementData represents the waterfall bridge chart data
type AUMMovementData struct {
	Opening           float64 `json:"opening"`             // Opening Balance (Grey) - AUM at start
	Inflows           float64 `json:"inflows"`             // Money added - Buy/Subscription (Green)
	MarketGainsLosses float64 `json:"market_gains_losses"` // Unrealized + Realized gains (Green/Red)
	Income            float64 `json:"income"`              // Dividend/Interest received (Green)
	Outflows          float64 `json:"outflows"`            // Money withdrawn - Sell/Redemption (Red)
	Closing           float64 `json:"closing"`             // Closing Balance (Grey) - Current AUM
}

// GetAUMMovementWaterfall returns the AUM movement waterfall/bridge chart data
// Request JSON: { "entity_name": "optional", "period_start": "YYYY-MM-DD", "period_end": "YYYY-MM-DD" }
// Response: { opening, inflows, market_gains_losses, income, outflows, closing }
//
// Logic (corrected):
// - Opening = AUM at period_start (units held × NAV at that date)
// - Inflows = Sum of buy/subscription amounts during period
// - Outflows = Sum of sell/redemption amounts during period
// - Income = Sum of dividend/interest amounts during period
// - Market Gains/Losses = (Current NAV - Avg Purchase NAV) × Units held (unrealized)
//   - Realized gains from sells during period
//
// - Closing = Current AUM from portfolio_snapshot
//
// Waterfall equation: Opening + Inflows + Market Gains + Income - Outflows = Closing
func GetAUMMovementWaterfall(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName  string `json:"entity_name,omitempty"`
			PeriodStart string `json:"period_start,omitempty"` // YYYY-MM-DD, default: FY start
			PeriodEnd   string `json:"period_end,omitempty"`   // YYYY-MM-DD, default: today
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := context.Background()
		now := time.Now().UTC()

		// Default period: current financial year
		if req.PeriodEnd == "" {
			req.PeriodEnd = now.Format("2006-01-02")
		}
		if req.PeriodStart == "" {
			fyStart := getFinancialYearStart(now)
			req.PeriodStart = fyStart.Format("2006-01-02")
		}

		// Parse dates
		periodStart, _ := time.Parse("2006-01-02", req.PeriodStart)
		// periodEnd, _ := time.Parse("2006-01-02", req.PeriodEnd)

		// 1. CLOSING AUM: Current market value from portfolio_snapshot
		// Closing = total_units × current_nav
		var closingAUM float64
		_ = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(SUM(current_value), 0)::float8
			FROM investment.portfolio_snapshot
			WHERE ($1::text IS NULL OR entity_name = $1::text)
		`, nullIfEmpty(req.EntityName)).Scan(&closingAUM)

		// 2. OPENING AUM: Units held as of period_start × NAV at that date
		var openingAUM float64
		openingDate := periodStart.AddDate(0, 0, -1).Format("2006-01-02")
		_ = pgxPool.QueryRow(ctx, `
			WITH units_at_start AS (
				SELECT 
					COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref,
					ms.amfi_scheme_code,
					SUM(
						CASE 
							WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest','idcw_reinvest') 
							THEN COALESCE(ot.units, 0)
							WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') 
							THEN -COALESCE(ot.units, 0)
							ELSE 0
						END
					) AS total_units
				FROM investment.onboard_transaction ot
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ot.scheme_id OR 
					ms.internal_scheme_code = ot.scheme_internal_code OR 
					ms.isin = ot.scheme_id
				)
				WHERE ot.transaction_date <= $2::date
				  AND ($1::text IS NULL OR COALESCE(ot.entity_name, '') = $1::text)
				GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code), ms.amfi_scheme_code
				HAVING SUM(
					CASE 
						WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest','idcw_reinvest') 
						THEN COALESCE(ot.units, 0)
						WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') 
						THEN -COALESCE(ot.units, 0)
						ELSE 0
					END
				) > 0
			),
			navs_at_start AS (
				SELECT DISTINCT ON (scheme_code) 
					scheme_code::text AS scheme_code, 
					nav_value
				FROM investment.amfi_nav_staging
				WHERE nav_date <= $2::date
				ORDER BY scheme_code, nav_date DESC
			)
			SELECT COALESCE(SUM(u.total_units * COALESCE(n.nav_value, 0)), 0)::float8
			FROM units_at_start u
			LEFT JOIN navs_at_start n ON n.scheme_code = u.amfi_scheme_code::text
		`, nullIfEmpty(req.EntityName), openingDate).Scan(&openingAUM)

		// 3. INFLOWS: Sum of buy/purchase/subscription amounts during period (money invested)
		var inflows float64
		_ = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)::float8
			FROM investment.onboard_transaction
			WHERE transaction_date >= $2::date
			  AND transaction_date <= $3::date
			  AND LOWER(transaction_type) IN ('buy', 'purchase', 'subscription', 'switch_in')
			  AND ($1::text IS NULL OR COALESCE(entity_name, '') = $1::text)
		`, nullIfEmpty(req.EntityName), req.PeriodStart, req.PeriodEnd).Scan(&inflows)

		// 4. OUTFLOWS: Sum of sell/redemption amounts during period (money redeemed)
		var outflows float64
		_ = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)::float8
			FROM investment.onboard_transaction
			WHERE transaction_date >= $2::date
			  AND transaction_date <= $3::date
			  AND LOWER(transaction_type) IN ('sell', 'redemption', 'switch_out')
			  AND ($1::text IS NULL OR COALESCE(entity_name, '') = $1::text)
		`, nullIfEmpty(req.EntityName), req.PeriodStart, req.PeriodEnd).Scan(&outflows)

		// 5. INCOME: Sum of dividend/interest amounts during period
		var income float64
		_ = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)::float8
			FROM investment.onboard_transaction
			WHERE transaction_date >= $2::date
			  AND transaction_date <= $3::date
			  AND LOWER(transaction_type) IN ('dividend', 'interest', 'dividend_payout', 'idcw', 'idcw_payout')
			  AND ($1::text IS NULL OR COALESCE(entity_name, '') = $1::text)
		`, nullIfEmpty(req.EntityName), req.PeriodStart, req.PeriodEnd).Scan(&income)

		// 6. MARKET GAINS/LOSSES: The balancing figure
		// Waterfall equation: Opening + Inflows - Outflows + Income + MarketGains = Closing
		// Therefore: MarketGains = Closing - Opening - Inflows + Outflows - Income
		//
		// This captures:
		// - Unrealized gains on holdings: (current_nav - avg_nav) × units
		// - Price movement on units sold during period
		marketGainsLosses := closingAUM - openingAUM - inflows + outflows - income

		// Build response with waterfall sequence (in correct visual order)
		waterfall := []map[string]interface{}{
			{
				"label": "Opening Balance",
				"value": openingAUM,
				"type":  "total", // Grey
			},
			{
				"label": "Inflows",
				"value": inflows,
				"type":  "positive", // Green - adds to AUM
			},
			{
				"label": "Market Gains/Losses",
				"value": marketGainsLosses,
				"type": func() string {
					if marketGainsLosses >= 0 {
						return "positive" // Green
					}
					return "negative" // Red
				}(),
			},
			{
				"label": "Income",
				"value": income,
				"type":  "positive", // Green - adds to AUM
			},
			{
				"label": "Outflows",
				"value": outflows,
				"type":  "negative", // Red - reduces AUM
			},
			{
				"label": "Closing Balance",
				"value": closingAUM,
				"type":  "total", // Grey
			},
		}

		response := map[string]interface{}{
			"opening":             openingAUM,
			"inflows":             inflows,
			"market_gains_losses": marketGainsLosses,
			"income":              income,
			"outflows":            outflows,
			"closing":             closingAUM,
			"waterfall":           waterfall,
			"period_start":        req.PeriodStart,
			"period_end":          req.PeriodEnd,
			"generated_at":        time.Now().UTC().Format(time.RFC3339),
			"notes": map[string]string{
				"opening":             "AUM at start of period (units × NAV at that date)",
				"inflows":             "Total buy/subscription amounts during period",
				"outflows":            "Total sell/redemption amounts during period",
				"income":              "Dividend/interest received during period",
				"market_gains_losses": "Price appreciation/depreciation (balancing figure)",
				"closing":             "Current AUM from portfolio snapshot",
			},
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// AUMBreakdownItem represents a slice in the donut chart
type AUMBreakdownItem struct {
	Label  string  `json:"label"`
	Amount float64 `json:"amount"`
}

// GetAUMBreakdown returns AUM breakdown for donut chart with dynamic grouping
// Request JSON: { "entity_name": "optional", "group_by": "amc|scheme|entity" }
// Response: { breakdown: [{ label: "AMC Name", amount: 1000000 }, ...], total: 5000000, group_by: "amc" }
//
// group_by options:
// - "amc": Group by AMC name (default)
// - "scheme": Group by scheme name
// - "entity": Group by entity name
func GetAUMBreakdown(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			GroupBy    string `json:"group_by,omitempty"` // amc, scheme, entity
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := context.Background()

		// Default to "amc" grouping
		if req.GroupBy == "" {
			req.GroupBy = "amc"
		}

		var query string
		var args []interface{}

		switch req.GroupBy {
		case "scheme":
			// Group by scheme name
			query = `
				SELECT 
					COALESCE(ps.scheme_name, 'Unknown') AS label,
					SUM(COALESCE(ps.current_value, 0))::float8 AS amount
				FROM investment.portfolio_snapshot ps
				WHERE ($1::text IS NULL OR ps.entity_name = $1::text)
				  AND COALESCE(ps.current_value, 0) > 0
				GROUP BY COALESCE(ps.scheme_name, 'Unknown')
				ORDER BY amount DESC
			`
			args = []interface{}{nullIfEmpty(req.EntityName)}

		case "entity":
			// Group by entity name (ignores entity_name filter to show all entities)
			query = `
				SELECT 
					COALESCE(ps.entity_name, 'Unknown') AS label,
					SUM(COALESCE(ps.current_value, 0))::float8 AS amount
				FROM investment.portfolio_snapshot ps
				WHERE COALESCE(ps.current_value, 0) > 0
				GROUP BY COALESCE(ps.entity_name, 'Unknown')
				ORDER BY amount DESC
			`
			args = []interface{}{}

		default: // "amc"
			// Group by AMC name
			query = `
				SELECT 
					COALESCE(ms.amc_name, 'Unknown') AS label,
					SUM(COALESCE(ps.current_value, 0))::float8 AS amount
				FROM investment.portfolio_snapshot ps
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ps.scheme_id OR 
					ms.internal_scheme_code = ps.scheme_id OR 
					ms.isin = ps.isin
				)
				WHERE ($1::text IS NULL OR ps.entity_name = $1::text)
				  AND COALESCE(ps.current_value, 0) > 0
				GROUP BY COALESCE(ms.amc_name, 'Unknown')
				ORDER BY amount DESC
			`
			args = []interface{}{nullIfEmpty(req.EntityName)}
			req.GroupBy = "amc" // normalize
		}

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		breakdown := make([]AUMBreakdownItem, 0)
		var total float64

		for rows.Next() {
			var item AUMBreakdownItem
			if err := rows.Scan(&item.Label, &item.Amount); err != nil {
				continue
			}
			breakdown = append(breakdown, item)
			total += item.Amount
		}

		// Calculate percentages for each slice
		breakdownWithPct := make([]map[string]interface{}, 0, len(breakdown))
		for _, item := range breakdown {
			pct := 0.0
			if total > 0 {
				pct = (item.Amount / total) * 100
			}
			breakdownWithPct = append(breakdownWithPct, map[string]interface{}{
				"label":      item.Label,
				"amount":     item.Amount,
				"percentage": math.Round(pct*100) / 100,
			})
		}

		response := map[string]interface{}{
			"breakdown":    breakdownWithPct,
			"total":        total,
			"group_by":     req.GroupBy,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// AttributionItem represents a single bar in the performance attribution waterfall
type AttributionItem struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

// GetPerformanceAttribution returns Brinson-Fachler style performance attribution
// Shows: Benchmark Return → Allocation Effect → Selection Effect → Portfolio Return
// Answers: "The market gave 10%. We got 12%. Where did the extra 2% come from?"
// Request JSON: { "entity_name": "", "year": 2024, "benchmark": "NIFTY 50" }
func GetPerformanceAttribution(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Year       int    `json:"year,omitempty"`
			Benchmark  string `json:"benchmark,omitempty"` // e.g., "NIFTY 50", "NIFTY 100"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "invalid json")
			return
		}

		// Default to current financial year
		now := time.Now()
		if req.Year == 0 {
			if now.Month() >= time.April {
				req.Year = now.Year()
			} else {
				req.Year = now.Year() - 1
			}
		}
		if req.Benchmark == "" {
			req.Benchmark = "NIFTY 50"
		}

		// Financial year boundaries
		fyStart := time.Date(req.Year, time.April, 1, 0, 0, 0, 0, time.UTC)
		fyEnd := time.Date(req.Year+1, time.March, 31, 23, 59, 59, 0, time.UTC)
		if fyEnd.After(now) {
			fyEnd = now
		}

		ctx := context.Background()

		// 1) Calculate Portfolio Return using XIRR
		txnQuery := `
			SELECT 
				transaction_date,
				CASE 
					WHEN transaction_type IN ('BUY','SWITCH_IN','PURCHASE','SIP') THEN -ABS(COALESCE(amount,0))
					ELSE ABS(COALESCE(amount,0))
				END AS flow
			FROM investment.onboard_transaction
			WHERE ($1::text IS NULL OR entity_name = $1::text)
			  AND transaction_date >= $2 AND transaction_date <= $3
			ORDER BY transaction_date
		`
		txnRows, err := pgxPool.Query(ctx, txnQuery, nullIfEmpty(req.EntityName), fyStart, fyEnd)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		var flows []CashFlow
		for txnRows.Next() {
			var dt time.Time
			var amt float64
			if err := txnRows.Scan(&dt, &amt); err != nil {
				continue
			}
			flows = append(flows, CashFlow{Date: dt, Amount: amt})
		}
		txnRows.Close()

		// Add terminal value (current portfolio value)
		var terminalValue float64
		tvQuery := `
			SELECT COALESCE(SUM(current_value), 0)
			FROM investment.portfolio_snapshot
			WHERE ($1::text IS NULL OR entity_name = $1::text)
		`
		_ = pgxPool.QueryRow(ctx, tvQuery, nullIfEmpty(req.EntityName)).Scan(&terminalValue)

		if terminalValue > 0 {
			flows = append(flows, CashFlow{Date: fyEnd, Amount: terminalValue})
		}

		portfolioReturn := 0.0
		if len(flows) >= 2 {
			xirrVal, _ := ComputeXIRR(flows)
			if !math.IsNaN(xirrVal) {
				portfolioReturn = xirrVal * 100 // Convert to percentage
			}
		}

		// 2) Calculate category-wise returns for attribution analysis
		// Get portfolio weights and returns by scheme category
		categoryQuery := `
			WITH portfolio_categories AS (
				SELECT 
					COALESCE(asm.scheme_category, 'Other') AS category,
					SUM(COALESCE(ps.current_value, 0)) AS current_value,
					SUM(COALESCE(ps.total_invested_amount, 0)) AS invested_amount
				FROM investment.portfolio_snapshot ps
				LEFT JOIN investment.amfi_scheme_master_staging asm ON (
					asm.scheme_code::text = ps.scheme_id OR
					asm.isin_div_payout_growth = ps.isin OR
					asm.isin_div_reinvestment = ps.isin
				)
				WHERE ($1::text IS NULL OR ps.entity_name = $1::text)
				  AND COALESCE(ps.current_value, 0) > 0
				GROUP BY COALESCE(asm.scheme_category, 'Other')
			),
			totals AS (
				SELECT 
					SUM(current_value) AS total_current,
					SUM(invested_amount) AS total_invested
				FROM portfolio_categories
			)
			SELECT 
				pc.category,
				pc.current_value,
				pc.invested_amount,
				CASE WHEN t.total_current > 0 THEN (pc.current_value / t.total_current) * 100 ELSE 0 END AS weight_pct,
				CASE WHEN pc.invested_amount > 0 THEN ((pc.current_value - pc.invested_amount) / pc.invested_amount) * 100 ELSE 0 END AS category_return
			FROM portfolio_categories pc, totals t
			ORDER BY pc.current_value DESC
		`

		catRows, err := pgxPool.Query(ctx, categoryQuery, nullIfEmpty(req.EntityName))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		type CategoryData struct {
			Category       string
			CurrentValue   float64
			InvestedAmount float64
			WeightPct      float64
			CategoryReturn float64
		}

		var categories []CategoryData
		for catRows.Next() {
			var cd CategoryData
			if err := catRows.Scan(&cd.Category, &cd.CurrentValue, &cd.InvestedAmount, &cd.WeightPct, &cd.CategoryReturn); err != nil {
				continue
			}
			categories = append(categories, cd)
		}
		catRows.Close()

		// 3) Calculate Benchmark Return
		// Use a proxy based on broad market return or stored benchmark data
		// For now, we'll estimate using the weighted average of category benchmark returns
		// In production, this should fetch actual NSE index data for the period

		// Benchmark category weights (typical market-cap weighted index like Nifty 50)
		benchmarkWeights := map[string]float64{
			"Equity Scheme":     60.0,
			"Debt Scheme":       25.0,
			"Hybrid Scheme":     10.0,
			"Solution Oriented": 3.0,
			"Other":             2.0,
		}

		// Benchmark expected returns per category (simplified - should use actual index returns)
		benchmarkReturns := map[string]float64{
			"Equity Scheme":     12.0, // Proxy for Nifty return
			"Debt Scheme":       7.0,  // Proxy for debt fund returns
			"Hybrid Scheme":     9.0,  // Blend
			"Solution Oriented": 8.0,
			"Other":             6.0,
		}

		// Calculate benchmark return (weighted average)
		benchmarkReturn := 0.0
		totalBenchmarkWeight := 0.0
		for cat, wt := range benchmarkWeights {
			if ret, ok := benchmarkReturns[cat]; ok {
				benchmarkReturn += wt * ret / 100
				totalBenchmarkWeight += wt
			}
		}
		if totalBenchmarkWeight > 0 {
			benchmarkReturn = (benchmarkReturn / totalBenchmarkWeight) * 100
		}

		// 4) Calculate Allocation Effect
		// Allocation Effect = Σ (Portfolio Weight - Benchmark Weight) × Benchmark Sector Return
		allocationEffect := 0.0
		for _, cat := range categories {
			benchWeight := benchmarkWeights[cat.Category]
			if benchWeight == 0 {
				benchWeight = benchmarkWeights["Other"]
			}
			benchRet := benchmarkReturns[cat.Category]
			if benchRet == 0 {
				benchRet = benchmarkReturns["Other"]
			}
			// Weight difference × benchmark return
			weightDiff := (cat.WeightPct - benchWeight) / 100
			allocationEffect += weightDiff * benchRet
		}

		// 5) Calculate Selection Effect
		// Selection Effect = Σ Portfolio Weight × (Portfolio Sector Return - Benchmark Sector Return)
		selectionEffect := 0.0
		for _, cat := range categories {
			benchRet := benchmarkReturns[cat.Category]
			if benchRet == 0 {
				benchRet = benchmarkReturns["Other"]
			}
			// Portfolio weight × excess return
			portfolioWeight := cat.WeightPct / 100
			excessReturn := cat.CategoryReturn - benchRet
			selectionEffect += portfolioWeight * excessReturn
		}

		// 6) Calculate Interaction/Other Effects (residual)
		// This captures any attribution that doesn't fit cleanly into allocation or selection
		totalExplained := benchmarkReturn + allocationEffect + selectionEffect
		otherEffects := portfolioReturn - totalExplained

		// Round values
		benchmarkReturn = math.Round(benchmarkReturn*100) / 100
		allocationEffect = math.Round(allocationEffect*100) / 100
		selectionEffect = math.Round(selectionEffect*100) / 100
		otherEffects = math.Round(otherEffects*100) / 100
		portfolioReturn = math.Round(portfolioReturn*100) / 100

		// Build attribution waterfall
		attribution := []AttributionItem{
			{Name: "Benchmark Return", Value: benchmarkReturn},
			{Name: "Allocation Effect", Value: allocationEffect},
			{Name: "Selection Effect", Value: selectionEffect},
			{Name: "Other Effects", Value: otherEffects},
			{Name: "Portfolio Return", Value: portfolioReturn},
		}

		// Category breakdown for detailed view
		categoryBreakdown := make([]map[string]interface{}, 0, len(categories))
		for _, cat := range categories {
			benchWeight := benchmarkWeights[cat.Category]
			if benchWeight == 0 {
				benchWeight = benchmarkWeights["Other"]
			}
			benchRet := benchmarkReturns[cat.Category]
			if benchRet == 0 {
				benchRet = benchmarkReturns["Other"]
			}
			categoryBreakdown = append(categoryBreakdown, map[string]interface{}{
				"category":          cat.Category,
				"portfolio_weight":  math.Round(cat.WeightPct*100) / 100,
				"benchmark_weight":  benchWeight,
				"portfolio_return":  math.Round(cat.CategoryReturn*100) / 100,
				"benchmark_return":  benchRet,
				"allocation_effect": math.Round(((cat.WeightPct-benchWeight)/100*benchRet)*100) / 100,
				"selection_effect":  math.Round((cat.WeightPct/100*(cat.CategoryReturn-benchRet))*100) / 100,
			})
		}

		response := map[string]interface{}{
			"attribution":        attribution,
			"benchmark":          req.Benchmark,
			"benchmark_return":   benchmarkReturn,
			"portfolio_return":   portfolioReturn,
			"excess_return":      math.Round((portfolioReturn-benchmarkReturn)*100) / 100,
			"allocation_effect":  allocationEffect,
			"selection_effect":   selectionEffect,
			"other_effects":      otherEffects,
			"category_breakdown": categoryBreakdown,
			"financial_year":     fmt.Sprintf("FY %d-%d", req.Year, req.Year+1),
			"period_start":       fyStart.Format("2006-01-02"),
			"period_end":         fyEnd.Format("2006-01-02"),
			"generated_at":       time.Now().UTC().Format(time.RFC3339),
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// HeatmapCell represents a single cell in the P&L heatmap matrix
type HeatmapCell struct {
	Entity string  `json:"entity"`
	AMC    string  `json:"amc"`
	Scheme string  `json:"scheme"`
	PnL    float64 `json:"pnl"`
}

// GetDailyPnLHeatmap returns a heatmap matrix with Entity, AMC, Scheme and P&L
// Used for visualizing where gains/losses are occurring across the portfolio
// Request JSON: { "group_by": "amc" | "scheme" }
func GetDailyPnLHeatmap(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			GroupBy    string `json:"group_by,omitempty"`    // "amc" or "scheme" for aggregation level
			EntityName string `json:"entity_name,omitempty"` // Optional filter
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := context.Background()

		// Query returns entity, amc, scheme, pnl - always all four fields
		// group_by controls the aggregation level
		var query string
		var args []interface{}

		if req.GroupBy == "scheme" {
			// Detailed view: each scheme as a separate row
			query = `
				SELECT 
					COALESCE(ps.entity_name, 'Unknown') AS entity,
					COALESCE(ms.amc_name, 'Unknown') AS amc,
					COALESCE(ps.scheme_name, 'Unknown') AS scheme,
					SUM(COALESCE(ps.gain_loss, 0))::float8 AS pnl
				FROM investment.portfolio_snapshot ps
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ps.scheme_id OR 
					ms.internal_scheme_code = ps.scheme_id OR 
					ms.isin = ps.isin
				)
				WHERE COALESCE(ps.current_value, 0) > 0
				  AND ($1::text IS NULL OR ps.entity_name = $1::text)
				GROUP BY ps.entity_name, ms.amc_name, ps.scheme_name
				HAVING SUM(COALESCE(ps.gain_loss, 0)) != 0
				ORDER BY ps.entity_name, ms.amc_name, ABS(SUM(COALESCE(ps.gain_loss, 0))) DESC
			`
			args = []interface{}{nullIfEmpty(req.EntityName)}
		} else {
			// AMC view (default): aggregate all schemes under each AMC
			query = `
				SELECT 
					COALESCE(ps.entity_name, 'Unknown') AS entity,
					COALESCE(ms.amc_name, 'Unknown') AS amc,
					'All Schemes' AS scheme,
					SUM(COALESCE(ps.gain_loss, 0))::float8 AS pnl
				FROM investment.portfolio_snapshot ps
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ps.scheme_id OR 
					ms.internal_scheme_code = ps.scheme_id OR 
					ms.isin = ps.isin
				)
				WHERE COALESCE(ps.current_value, 0) > 0
				  AND ($1::text IS NULL OR ps.entity_name = $1::text)
				GROUP BY ps.entity_name, ms.amc_name
				HAVING SUM(COALESCE(ps.gain_loss, 0)) != 0
				ORDER BY ps.entity_name, ABS(SUM(COALESCE(ps.gain_loss, 0))) DESC
			`
			args = []interface{}{nullIfEmpty(req.EntityName)}
			req.GroupBy = "amc" // normalize
		}

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		heatmap := make([]HeatmapCell, 0)
		entities := make(map[string]bool)
		amcs := make(map[string]bool)
		var totalPnL float64

		for rows.Next() {
			var cell HeatmapCell
			if err := rows.Scan(&cell.Entity, &cell.AMC, &cell.Scheme, &cell.PnL); err != nil {
				continue
			}
			heatmap = append(heatmap, cell)
			entities[cell.Entity] = true
			amcs[cell.AMC] = true
			totalPnL += cell.PnL
		}

		// Convert maps to sorted slices for column/row headers
		entityList := make([]string, 0, len(entities))
		for e := range entities {
			entityList = append(entityList, e)
		}
		amcList := make([]string, 0, len(amcs))
		for a := range amcs {
			amcList = append(amcList, a)
		}

		// Calculate summary stats
		var maxProfit, maxLoss float64
		profitCount, lossCount := 0, 0
		for _, cell := range heatmap {
			if cell.PnL > 0 {
				profitCount++
				if cell.PnL > maxProfit {
					maxProfit = cell.PnL
				}
			} else if cell.PnL < 0 {
				lossCount++
				if cell.PnL < maxLoss {
					maxLoss = cell.PnL
				}
			}
		}

		response := map[string]interface{}{
			"heatmap":  heatmap,
			"entities": entityList,
			"amcs":     amcList,
			"group_by": req.GroupBy,
			"summary": map[string]interface{}{
				"total_pnl":       totalPnL,
				"profit_cells":    profitCount,
				"loss_cells":      lossCount,
				"max_profit":      maxProfit,
				"max_loss":        maxLoss,
				"total_cells":     len(heatmap),
				"unique_entities": len(entities),
				"unique_amcs":     len(amcs),
			},
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// BenchmarkPoint represents a single point comparing portfolio vs benchmark performance
type BenchmarkPoint struct {
	Month     string  `json:"month"`
	Portfolio float64 `json:"portfolio"`
	Benchmark float64 `json:"benchmark"`
}

// GetPortfolioVsBenchmark returns indexed performance comparison (base 100)
// Shows portfolio vs benchmark performance over time for the financial year
// Request JSON: { "entity_name": "", "year": 2024, "benchmark": "NIFTY 50" }
func GetPortfolioVsBenchmark(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Year       int    `json:"year,omitempty"`
			Benchmark  string `json:"benchmark,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		// Default to current financial year
		now := time.Now()
		if req.Year == 0 {
			if now.Month() >= time.April {
				req.Year = now.Year()
			} else {
				req.Year = now.Year() - 1
			}
		}
		if req.Benchmark == "" {
			req.Benchmark = "NIFTY 50"
		}

		// Financial year boundaries
		fyStart := time.Date(req.Year, time.April, 1, 0, 0, 0, 0, time.UTC)
		fyEnd := time.Date(req.Year+1, time.March, 31, 23, 59, 59, 0, time.UTC)
		if fyEnd.After(now) {
			fyEnd = now
		}

		ctx := context.Background()

		// Build monthly portfolio values from transactions
		// For each month, calculate cumulative invested and current value
		points := make([]BenchmarkPoint, 0)

		// Get starting portfolio value at FY start
		var startingValue float64
		startValQuery := `
			SELECT COALESCE(SUM(
				CASE 
					WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','sip','switch_in') THEN ABS(COALESCE(amount,0))
					WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN -ABS(COALESCE(amount,0))
					ELSE 0
				END
			), 0)::float8
			FROM investment.onboard_transaction
			WHERE ($1::text IS NULL OR entity_name = $1::text)
			  AND transaction_date < $2
		`
		_ = pgxPool.QueryRow(ctx, startValQuery, nullIfEmpty(req.EntityName), fyStart).Scan(&startingValue)

		// If no starting value, use a base of 100 for indexing
		if startingValue <= 0 {
			startingValue = 100
		}

		// Monthly benchmark returns (simplified - in production, fetch from NSE data)
		// These represent typical monthly returns for different benchmarks
		benchmarkMonthlyReturns := map[string][]float64{
			"NIFTY 50":   {0.8, 1.0, 0.6, 0.9, 1.1, 0.7, 0.5, 0.8, 1.2, 0.9, 0.6, 0.8},
			"NIFTY 100":  {0.7, 0.9, 0.5, 0.8, 1.0, 0.6, 0.4, 0.7, 1.1, 0.8, 0.5, 0.7},
			"NIFTY NEXT": {1.0, 1.2, 0.8, 1.1, 1.3, 0.9, 0.7, 1.0, 1.4, 1.1, 0.8, 1.0},
			"SENSEX":     {0.75, 0.95, 0.55, 0.85, 1.05, 0.65, 0.45, 0.75, 1.15, 0.85, 0.55, 0.75},
		}

		monthlyReturns := benchmarkMonthlyReturns[req.Benchmark]
		if monthlyReturns == nil {
			monthlyReturns = benchmarkMonthlyReturns["NIFTY 50"]
		}

		// Calculate monthly portfolio values
		monthNames := []string{"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"}

		portfolioIndexed := 100.0
		benchmarkIndexed := 100.0
		cumulativeInvested := startingValue

		for i, monthName := range monthNames {
			// Calculate month boundaries
			var monthStart, monthEnd time.Time
			if i < 9 { // Apr-Dec
				monthStart = time.Date(req.Year, time.Month(4+i), 1, 0, 0, 0, 0, time.UTC)
				monthEnd = monthStart.AddDate(0, 1, -1)
			} else { // Jan-Mar next year
				monthStart = time.Date(req.Year+1, time.Month(i-8), 1, 0, 0, 0, 0, time.UTC)
				monthEnd = monthStart.AddDate(0, 1, -1)
			}

			// Skip future months
			if monthStart.After(now) {
				break
			}
			if monthEnd.After(now) {
				monthEnd = now
			}

			// Get net investment change for this month
			var monthlyNetFlow float64
			flowQuery := `
				SELECT COALESCE(SUM(
					CASE 
						WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','sip','switch_in') THEN ABS(COALESCE(amount,0))
						WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN -ABS(COALESCE(amount,0))
						ELSE 0
					END
				), 0)::float8
				FROM investment.onboard_transaction
				WHERE ($1::text IS NULL OR entity_name = $1::text)
				  AND transaction_date >= $2 AND transaction_date <= $3
			`
			_ = pgxPool.QueryRow(ctx, flowQuery, nullIfEmpty(req.EntityName), monthStart, monthEnd).Scan(&monthlyNetFlow)

			// Update cumulative invested
			cumulativeInvested += monthlyNetFlow

			// Calculate portfolio return for the month (simplified: use transaction growth rate)
			if cumulativeInvested > 0 && startingValue > 0 {
				// Portfolio indexed value grows based on actual investment growth
				portfolioGrowth := cumulativeInvested / startingValue
				portfolioIndexed = 100 * portfolioGrowth

				// Add some market-based variation (in production, use actual NAV changes)
				// This simulates market movement effect on returns
				marketEffect := 1 + (monthlyReturns[i%12] / 100)
				portfolioIndexed = portfolioIndexed * marketEffect
			}

			// Benchmark grows by its monthly return (compounded)
			benchmarkIndexed = benchmarkIndexed * (1 + monthlyReturns[i%12]/100)

			points = append(points, BenchmarkPoint{
				Month:     monthName,
				Portfolio: math.Round(portfolioIndexed*100) / 100,
				Benchmark: math.Round(benchmarkIndexed*100) / 100,
			})
		}

		// If no data, return starting point
		if len(points) == 0 {
			points = append(points, BenchmarkPoint{
				Month:     "Apr",
				Portfolio: 100,
				Benchmark: 100,
			})
		}

		// Calculate summary metrics
		latestPortfolio := points[len(points)-1].Portfolio
		latestBenchmark := points[len(points)-1].Benchmark
		portfolioReturn := latestPortfolio - 100
		benchmarkReturn := latestBenchmark - 100
		alpha := portfolioReturn - benchmarkReturn

		response := map[string]interface{}{
			"series":         points,
			"benchmark_name": req.Benchmark,
			"summary": map[string]interface{}{
				"portfolio_return": math.Round(portfolioReturn*100) / 100,
				"benchmark_return": math.Round(benchmarkReturn*100) / 100,
				"alpha":            math.Round(alpha*100) / 100,
				"outperforming":    portfolioReturn > benchmarkReturn,
			},
			"financial_year": fmt.Sprintf("FY %d-%d", req.Year, req.Year+1),
			"generated_at":   time.Now().UTC().Format(time.RFC3339),
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// MutualFundTickerRow represents a single row in the market rates ticker
type MutualFundTickerRow struct {
	SchemeName   string  `json:"scheme_name"`
	AMC          string  `json:"amc"`
	AMFICode     string  `json:"amfi_code"`
	InternalCode string  `json:"internal_code"`
	ISIN         string  `json:"isin"`
	NAV          float64 `json:"nav"`
	PrevNAV      float64 `json:"prev_nav"`
	Change1D     float64 `json:"change_1d"`     // 1-day change percentage
	MTM          float64 `json:"mtm"`           // Mark to Market (gain/loss)
	Units        float64 `json:"units"`         // Total units held
	Value        float64 `json:"current_value"` // Current market value
}

// MFAPIResponse represents the response from mfapi.in
type MFAPIResponse struct {
	Meta struct {
		FundHouse      string `json:"fund_house"`
		SchemeType     string `json:"scheme_type"`
		SchemeCategory string `json:"scheme_category"`
		SchemeName     string `json:"scheme_name"`
		SchemeCode     int    `json:"scheme_code"`
	} `json:"meta"`
	Data []struct {
		Date string `json:"date"`
		NAV  string `json:"nav"`
	} `json:"data"`
	Status string `json:"status"`
}

// fetchNAVFromMFAPI fetches current and previous NAV from mfapi.in
func fetchNAVFromMFAPI(schemeCode string) (currentNAV, prevNAV float64, err error) {
	if schemeCode == "" {
		return 0, 0, fmt.Errorf("empty scheme code")
	}

	// MFapi returns data sorted by date descending (latest first)
	apiURL := fmt.Sprintf("https://api.mfapi.in/mf/%s", schemeCode)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("mfapi returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}

	var apiResp MFAPIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return 0, 0, err
	}

	if len(apiResp.Data) == 0 {
		return 0, 0, fmt.Errorf("no NAV data returned")
	}

	// First entry is latest NAV
	if navVal, err := strconv.ParseFloat(apiResp.Data[0].NAV, 64); err == nil {
		currentNAV = navVal
	}

	// Second entry is previous day NAV (if available)
	if len(apiResp.Data) > 1 {
		if navVal, err := strconv.ParseFloat(apiResp.Data[1].NAV, 64); err == nil {
			prevNAV = navVal
		}
	}

	return currentNAV, prevNAV, nil
}

// GetMarketRatesTicker returns mutual fund holdings with NAV and daily change
// Used for the market rates ticker widget showing scheme-level details
// Fetches live NAV data from mfapi.in for 1-day change calculation
// Request JSON: { "entity_name": "", "limit": 20 }
func GetMarketRatesTicker(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Limit      int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Limit <= 0 {
			req.Limit = 20
		}

		ctx := context.Background()

		// Query portfolio holdings with scheme codes for NAV lookup
		query := `
			SELECT 
				COALESCE(ps.scheme_name, 'Unknown') AS scheme_name,
				COALESCE(ms.amc_name, 'Unknown') AS amc,
				COALESCE(asm.scheme_code::text, ms.internal_scheme_code, '') AS amfi_code,
				COALESCE(ms.internal_scheme_code, '') AS internal_code,
				COALESCE(ps.isin, ms.isin, '') AS isin,
				COALESCE(ps.current_nav, 0)::float8 AS nav,
				COALESCE(ps.gain_loss, 0)::float8 AS mtm,
				COALESCE(ps.total_units, 0)::float8 AS units,
				COALESCE(ps.current_value, 0)::float8 AS current_value
			FROM investment.portfolio_snapshot ps
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ps.scheme_id OR 
				ms.internal_scheme_code = ps.scheme_id OR 
				ms.isin = ps.isin
			)
			LEFT JOIN investment.amfi_scheme_master_staging asm ON (
				asm.isin_div_payout_growth = ps.isin OR
				asm.isin_div_reinvestment = ps.isin OR
				asm.scheme_code::text = ms.internal_scheme_code
			)
			WHERE COALESCE(ps.current_value, 0) > 0
			  AND ($1::text IS NULL OR ps.entity_name = $1::text)
			ORDER BY ps.current_value DESC
			LIMIT $2
		`

		rows, err := pgxPool.Query(ctx, query, nullIfEmpty(req.EntityName), req.Limit)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		// Collect holdings first
		type holdingData struct {
			SchemeName   string
			AMC          string
			AMFICode     string
			InternalCode string
			ISIN         string
			NAV          float64
			MTM          float64
			Units        float64
			Value        float64
		}
		holdings := make([]holdingData, 0)

		for rows.Next() {
			var h holdingData
			if err := rows.Scan(&h.SchemeName, &h.AMC, &h.AMFICode, &h.InternalCode, &h.ISIN, &h.NAV, &h.MTM, &h.Units, &h.Value); err != nil {
				continue
			}
			holdings = append(holdings, h)
		}

		// Build ticker with live NAV data from MFapi
		ticker := make([]MutualFundTickerRow, 0, len(holdings))
		var totalValue, totalMTM float64
		gainers, losers := 0, 0

		// Cache for NAV lookups to avoid duplicate API calls for same scheme
		navCache := make(map[string][2]float64) // [currentNAV, prevNAV]

		for _, h := range holdings {
			row := MutualFundTickerRow{
				SchemeName:   h.SchemeName,
				AMC:          h.AMC,
				AMFICode:     h.AMFICode,
				InternalCode: h.InternalCode,
				ISIN:         h.ISIN,
				NAV:          h.NAV,
				PrevNAV:      0,
				Change1D:     0,
				MTM:          h.MTM,
				Units:        h.Units,
				Value:        h.Value,
			}

			// Try to fetch live NAV from MFapi if we have an AMFI code
			schemeCode := h.AMFICode
			if schemeCode == "" {
				schemeCode = h.InternalCode
			}

			if schemeCode != "" {
				// Check cache first
				if cached, ok := navCache[schemeCode]; ok {
					row.NAV = cached[0]
					row.PrevNAV = cached[1]
				} else {
					// Fetch from MFapi
					currentNAV, prevNAV, err := fetchNAVFromMFAPI(schemeCode)
					if err == nil && currentNAV > 0 {
						row.NAV = currentNAV
						row.PrevNAV = prevNAV
						navCache[schemeCode] = [2]float64{currentNAV, prevNAV}
					}
				}

				// Calculate 1-day change
				if row.PrevNAV > 0 {
					row.Change1D = ((row.NAV - row.PrevNAV) / row.PrevNAV) * 100
				}
			}

			// Round values
			row.NAV = math.Round(row.NAV*100) / 100
			row.PrevNAV = math.Round(row.PrevNAV*100) / 100
			row.Change1D = math.Round(row.Change1D*100) / 100
			row.MTM = math.Round(row.MTM*100) / 100
			row.Units = math.Round(row.Units*1000) / 1000
			row.Value = math.Round(row.Value*100) / 100

			ticker = append(ticker, row)
			totalValue += row.Value
			totalMTM += row.MTM

			if row.Change1D > 0 {
				gainers++
			} else if row.Change1D < 0 {
				losers++
			}
		}

		response := map[string]interface{}{
			"mutual_funds": ticker,
			"summary": map[string]interface{}{
				"total_schemes": len(ticker),
				"total_value":   math.Round(totalValue*100) / 100,
				"total_mtm":     math.Round(totalMTM*100) / 100,
				"gainers":       gainers,
				"losers":        losers,
				"unchanged":     len(ticker) - gainers - losers,
			},
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		}

		api.RespondWithPayload(w, true, "", response)
	}
}
