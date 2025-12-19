package forecastVsActual

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

var rates = map[string]float64{
	"USD": 1.0,
	"AUD": 0.68,
	"CAD": 0.75,
	"CHF": 1.1,
	"CNY": 0.14,
	"RMB": 0.14,
	"EUR": 1.09,
	"GBP": 1.28,
	"JPY": 0.0067,
	"SEK": 0.095,
	"INR": 0.0117,
}

type CatRow struct {
	Category string  `json:"category"`
	Type     string  `json:"type"`
	Actual   float64 `json:"actual"`
	Forecast float64 `json:"forecast"`
	Variance float64 `json:"variance"`
}

type DateCatRow struct {
	Date     string  `json:"date"`
	Category string  `json:"category"`
	Type     string  `json:"type"`
	Actual   float64 `json:"actual"`
	Forecast float64 `json:"forecast"`
	Variance float64 `json:"variance"`
}

type KPIs struct {
	ForecastNet float64 `json:"forecast_net"`
	ActualNet   float64 `json:"actual_net"`
	NetVariance float64 `json:"net_variance"`
	NetPct      float64 `json:"net_variance_pct"`
}

func GetForecastVsActualRowsHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 30
		}
		rows, err := GetForecastVsActualRows(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{constants.ValueSuccess: true, "rows": rows}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

func GetForecastVsActualKPIHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 30
		}
		k, err := GetForecastVsActualKPIs(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{constants.ValueSuccess: true, "kpis": k}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}
func GetForecastVsActualByDateHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 30
		}
		rows, err := GetForecastVsActualByDateRows(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{constants.ValueSuccess: true, "rows": rows}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

func GetForecastVsActualByMonthHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
		AccountID  string `json:"account_id,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 90
		}
		rows, err := GetForecastVsActualByMonthRows(pgxPool, req.Horizon, req.EntityName, req.Currency, req.AccountID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{constants.ValueSuccess: true, "rows": rows}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

func GetForecastVsActualRows(pgxPool *pgxpool.Pool, horizon int, entityName, currency string) ([]CatRow, error) {

	today := time.Now().UTC()
	end := today.AddDate(0, 0, horizon-1)
	actualQ := `
        SELECT fpl.category, fpl.source_ref, fpl.amount::float8, fpl.currency, fg.primary_key, fg.primary_value
        FROM fund_plan_lines fpl
        JOIN fund_plan_groups fg ON fpl.group_id = fg.group_id
        LEFT JOIN LATERAL (
            SELECT processing_status FROM auditaction_fund_plan_groups a WHERE a.group_id = fg.group_id ORDER BY requested_at DESC LIMIT 1
        ) aa ON TRUE
        WHERE aa.processing_status = 'APPROVED'
          AND (UPPER(fpl.source_ref) LIKE 'TR-PAY-%' OR UPPER(fpl.source_ref) LIKE 'TR-REC-%')
    `
	aargs := []interface{}{}
	if currency != "" {
		actualQ += fmt.Sprintf(constants.QuerryFplCurrency, len(aargs)+1)
		aargs = append(aargs, currency)
	}
	if entityName != "" {
		actualQ += fmt.Sprintf(constants.QuerryFilterGroup, len(aargs)+1)
		aargs = append(aargs, entityName)
	}

	arows, err := pgxPool.Query(context.Background(), actualQ, aargs...)
	if err != nil {
		return nil, fmt.Errorf("actuals (fund plan) query: %w", err)
	}
	defer arows.Close()
	actuals := map[string]float64{}
	for arows.Next() {
		var cat, srcRef, cur, pk, pv string
		var amt float64
		if err := arows.Scan(&cat, &srcRef, &amt, &cur, &pk, &pv); err != nil {
			continue
		}
		up := strings.ToUpper(srcRef)
		typ := ""
		if strings.HasPrefix(up, constants.ErrPrefixPayable) {
			typ = "Outflow"
		} else if strings.HasPrefix(up, constants.ErrPrefixReceivable) {
			typ = "Inflow"
		} else {
			if amt < 0 {
				typ = "Outflow"
			} else {
				typ = "Inflow"
			}
		}
		include := true
		if pv != "" {
			var d time.Time
			var parseErr error
			layouts := []string{constants.DateFormat, time.RFC3339}
			for _, L := range layouts {
				d, parseErr = time.Parse(L, pv)
				if parseErr == nil {
					break
				}
			}
			if parseErr == nil {
				if d.Before(today) || d.After(end) {
					include = false
				}
			}
		}
		if !include {
			continue
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		key := fmt.Sprintf("%s-(%s)", cat, typ)
		actuals[key] += amt * rate
	}

	startKey := today.Year()*12 + int(today.Month())
	endKey := end.Year()*12 + int(end.Month())
	forecastQ := `
        SELECT COALESCE(cpi.category_id, 'Uncategorized') AS category, cpi.cashflow_type AS type, COALESCE(SUM(cpm.projected_amount),0)::float8 AS amount, COALESCE(cp.currency_code,'USD')
        FROM cashflow_projection_monthly cpm
        JOIN cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
        JOIN cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
        LEFT JOIN LATERAL (
            SELECT processing_status FROM audit_action_cashflow_proposal a2 WHERE a2.proposal_id = cp.proposal_id ORDER BY requested_at DESC LIMIT 1
        ) aa ON TRUE
        WHERE aa.processing_status = 'APPROVED'
          AND (cpm.year * 12 + cpm.month) BETWEEN $1 AND $2
    `
	fargs := []interface{}{startKey, endKey}
	if entityName != "" {
		forecastQ += fmt.Sprintf(constants.QuerryEntityName, len(fargs)+1)
		fargs = append(fargs, entityName)
	}
	if currency != "" {
		forecastQ += fmt.Sprintf(constants.QuerryCurrencyCode, len(fargs)+1)
		fargs = append(fargs, currency)
	}
	forecastQ += " GROUP BY cpi.category_id, cpi.cashflow_type, cp.currency_code"

	frows, err := pgxPool.Query(context.Background(), forecastQ, fargs...)
	if err != nil {
		return nil, fmt.Errorf("forecast query: %w", err)
	}
	defer frows.Close()
	forecasts := map[string]float64{}
	for frows.Next() {
		var cat, typ, cur string
		var amt float64
		if err := frows.Scan(&cat, &typ, &amt, &cur); err != nil {
			continue
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		key := fmt.Sprintf("%s-(%s)", cat, typ)
		forecasts[key] += amt * rate
	}

	combined := map[string]CatRow{}
	for k, v := range actuals {
		combined[k] = CatRow{Category: k, Type: "", Actual: v, Forecast: 0, Variance: 0}
	}
	for k, v := range forecasts {
		if cr, ok := combined[k]; ok {
			cr.Forecast = v
			cr.Variance = cr.Actual - v
			combined[k] = cr
		} else {
			combined[k] = CatRow{Category: k, Type: "", Actual: 0, Forecast: v, Variance: 0 - v}
		}
	}
	out := make([]CatRow, 0, len(combined))
	for k, v := range combined {
		var cat, typ string
		if idx := len(k) - 1; idx > 0 {
			p := "-("
			if pos := findLast(k, p); pos >= 0 {
				cat = k[:pos]
				typ = k[pos+2 : len(k)-1]
			} else {
				cat = k
				typ = ""
			}
		} else {
			cat = k
			typ = ""
		}
		v.Category = cat
		v.Type = typ
		v.Variance = v.Forecast - v.Actual
		out = append(out, v)
	}

	return out, nil
}

func GetForecastVsActualByDateRows(pgxPool *pgxpool.Pool, horizon int, entityName, currency string) ([]DateCatRow, error) {
	today := time.Now().UTC()
	end := today.AddDate(0, 0, horizon-1)

	actualQ := `
        SELECT fpl.category, fpl.source_ref, fpl.amount::float8, fpl.currency, fg.primary_key, fg.primary_value
        FROM fund_plan_lines fpl
        JOIN fund_plan_groups fg ON fpl.group_id = fg.group_id
        LEFT JOIN LATERAL (
            SELECT processing_status FROM auditaction_fund_plan_groups a WHERE a.group_id = fg.group_id ORDER BY requested_at DESC LIMIT 1
        ) aa ON TRUE
        WHERE aa.processing_status = 'APPROVED'
          AND (UPPER(fpl.source_ref) LIKE 'TR-PAY-%' OR UPPER(fpl.source_ref) LIKE 'TR-REC-%')
    `
	aargs := []interface{}{}
	if currency != "" {
		actualQ += fmt.Sprintf(constants.QuerryFplCurrency, len(aargs)+1)
		aargs = append(aargs, currency)
	}
	if entityName != "" {
		actualQ += fmt.Sprintf(constants.QuerryFilterGroup, len(aargs)+1)
		aargs = append(aargs, entityName)
	}

	arows, err := pgxPool.Query(context.Background(), actualQ, aargs...)
	if err != nil {
		return nil, fmt.Errorf("actuals (fund plan) query: %w", err)
	}
	defer arows.Close()

	combined := map[string]*DateCatRow{}

	layouts := []string{constants.DateFormat, time.RFC3339, constants.DateTimeFormat}
	for arows.Next() {
		var cat, srcRef, cur, pk, pv string
		var amt float64
		if err := arows.Scan(&cat, &srcRef, &amt, &cur, &pk, &pv); err != nil {
			continue
		}
		up := strings.ToUpper(srcRef)
		typ := ""
		if strings.HasPrefix(up, constants.ErrPrefixPayable) {
			typ = "Outflow"
		} else if strings.HasPrefix(up, constants.ErrPrefixReceivable) {
			typ = "Inflow"
		} else {
			if amt < 0 {
				typ = "Outflow"
			} else {
				typ = "Inflow"
			}
		}

		var d time.Time
		parsed := false
		if strings.TrimSpace(pv) != "" {
			for _, L := range layouts {
				if td, e := time.Parse(L, pv); e == nil {
					d = td
					parsed = true
					break
				}
			}
		}
		if !parsed {
			continue
		}
		if d.Before(today) || d.After(end) {
			continue
		}
		dateKey := d.Format(constants.DateFormat)
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		key := fmt.Sprintf(constants.FormatPipelineTriple, dateKey, cat, typ)
		if _, ok := combined[key]; !ok {
			combined[key] = &DateCatRow{Date: dateKey, Category: cat, Type: typ}
		}
		combined[key].Actual += amt * rate
	}

	startKey := today.Year()*12 + int(today.Month())
	endKey := end.Year()*12 + int(end.Month())
	forecastQ := `
        SELECT cpm.year, cpm.month, cpi.category_id, cpi.cashflow_type, COALESCE(cpi.start_date, NULL) as start_date, COALESCE(SUM(cpm.projected_amount),0)::float8 AS amount, COALESCE(cp.currency_code,'USD')
        FROM cashflow_projection_monthly cpm
        JOIN cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
        JOIN cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
        LEFT JOIN LATERAL (
            SELECT processing_status FROM audit_action_cashflow_proposal a2 WHERE a2.proposal_id = cp.proposal_id ORDER BY requested_at DESC LIMIT 1
        ) aa ON TRUE
        WHERE aa.processing_status = 'APPROVED'
          AND (cpm.year * 12 + cpm.month) BETWEEN $1 AND $2
    `
	fargs := []interface{}{startKey, endKey}
	if entityName != "" {
		forecastQ += fmt.Sprintf(constants.QuerryEntityName, len(fargs)+1)
		fargs = append(fargs, entityName)
	}
	if currency != "" {
		forecastQ += fmt.Sprintf(constants.QuerryCurrencyCode, len(fargs)+1)
		fargs = append(fargs, currency)
	}
	forecastQ += " GROUP BY cpm.year, cpm.month, cpi.category_id, cpi.cashflow_type, cpi.start_date, cp.currency_code ORDER BY cpm.year, cpm.month"

	frows, err := pgxPool.Query(context.Background(), forecastQ, fargs...)
	if err != nil {
		return nil, fmt.Errorf("forecast query: %w", err)
	}
	defer frows.Close()

	for frows.Next() {
		var year, month int
		var cat, typ, cur string
		var startDate *time.Time
		var amt float64
		if err := frows.Scan(&year, &month, &cat, &typ, &startDate, &amt, &cur); err != nil {
			continue
		}
		firstOfMonth := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		daysInMonth := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		if startDate != nil {
			effDay := startDate.Day()
			if effDay > daysInMonth {
				effDay = daysInMonth
			}
			d := time.Date(year, time.Month(month), effDay, 0, 0, 0, 0, time.UTC)
			if !d.Before(today) && !d.After(end) {
				key := fmt.Sprintf(constants.FormatPipelineTriple, d.Format(constants.DateFormat), cat, typ)
				if _, ok := combined[key]; !ok {
					combined[key] = &DateCatRow{Date: d.Format(constants.DateFormat), Category: cat, Type: typ}
				}
				combined[key].Forecast += amt * rate
			}
		} else {
			// distribute evenly across month
			daily := (amt * rate) / float64(daysInMonth)
			for d := firstOfMonth; d.Month() == time.Month(month); d = d.AddDate(0, 0, 1) {
				if d.Before(today) || d.After(end) {
					continue
				}
				key := fmt.Sprintf(constants.FormatPipelineTriple, d.Format(constants.DateFormat), cat, typ)
				if _, ok := combined[key]; !ok {
					combined[key] = &DateCatRow{Date: d.Format(constants.DateFormat), Category: cat, Type: typ}
				}
				combined[key].Forecast += daily
			}
		}
	}
	out := make([]DateCatRow, 0, len(combined))
	for _, v := range combined {
		v.Variance = v.Forecast - v.Actual
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Date == out[j].Date {
			if out[i].Category == out[j].Category {
				return out[i].Type < out[j].Type
			}
			return out[i].Category < out[j].Category
		}
		return out[i].Date < out[j].Date
	})
	return out, nil
}

func GetForecastVsActualKPIs(pgxPool *pgxpool.Pool, horizon int, entityName, currency string) (KPIs, error) {
	var k KPIs
	rows, err := GetForecastVsActualRows(pgxPool, horizon, entityName, currency)
	if err != nil {
		return k, err
	}
	var totalForecastSigned, totalActualSigned float64
	for _, r := range rows {
		f := math.Abs(r.Forecast)
		a := math.Abs(r.Actual)
		switch r.Type {
		case "Inflow":
			totalForecastSigned += f
			totalActualSigned += a
		case "Outflow":
			totalForecastSigned -= f
			totalActualSigned -= a
		default:
			totalForecastSigned += r.Forecast
			totalActualSigned += r.Actual
		}
	}
	k.ForecastNet = totalForecastSigned
	k.ActualNet = totalActualSigned
	k.NetVariance = totalForecastSigned - totalActualSigned
	denom := math.Abs(totalActualSigned)
	if denom == 0 {
		denom = math.Abs(totalForecastSigned)
	}
	if denom == 0 {
		k.NetPct = 0
	} else {
		k.NetPct = (k.NetVariance / denom) * 100
	}
	return k, nil
}

func findLast(s, sub string) int {
	last := -1
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			last = i
		}
	}
	return last
}

func GetForecastVsActualByMonthRows(
	pgxPool *pgxpool.Pool,
	horizon int,
	entityName, currency, accountID string,
) ([]DateCatRow, error) {

	if horizon <= 0 {
		horizon = 3
	}

	today := time.Now().UTC()
	anchors := []time.Time{}
	for i := 0; i < horizon; i++ {
		anchors = append(anchors, today.AddDate(0, i, 0)) // same day-of-month
	}

	// Query lines with APPROVED status
	q := `
        SELECT fpl.category, fpl.source_ref, fpl.amount::float8, fpl.currency,
               fg.direction, fg.primary_key, fg.primary_value
        FROM fund_plan_lines fpl
        JOIN fund_plan_groups fg ON fpl.group_id = fg.group_id
        LEFT JOIN LATERAL (
            SELECT processing_status
            FROM auditaction_fund_plan_groups a
            WHERE a.group_id = fg.group_id
            ORDER BY requested_at DESC LIMIT 1
        ) aa ON TRUE
        WHERE aa.processing_status = 'APPROVED'
          AND (UPPER(fpl.source_ref) LIKE 'TR-PAY-%'
            OR UPPER(fpl.source_ref) LIKE 'TR-REC-%'
            OR UPPER(fpl.source_ref) LIKE 'PROP-%')
    `
	args := []interface{}{}
	if currency != "" {
		q += fmt.Sprintf(constants.QuerryFplCurrency, len(args)+1)
		args = append(args, currency)
	}
	if entityName != "" {
		q += fmt.Sprintf(constants.QuerryFilterGroup, len(args)+1)
		args = append(args, entityName)
	}
	if accountID != "" {
		q += fmt.Sprintf(" AND COALESCE(fpl.allocated_account_id::text,'') = $%d", len(args)+1)
		args = append(args, accountID)
	}

	rows, err := pgxPool.Query(context.Background(), q, args...)
	if err != nil {
		return nil, fmt.Errorf("query fund plan lines: %w", err)
	}
	defer rows.Close()

	// Aggregation: key = anchorDate|category
	combined := map[string]*DateCatRow{}
	for rows.Next() {
		var cat, srcRef, cur, direction, pk, pv string
		var amt float64
		if err := rows.Scan(&cat, &srcRef, &amt, &cur, &direction, &pk, &pv); err != nil {
			continue
		}

		up := strings.ToUpper(srcRef)
		isForecast := false
		typ := ""
		if strings.HasPrefix(up, constants.ErrPrefixPayable) {
			typ = "Outflow"
		} else if strings.HasPrefix(up, constants.ErrPrefixReceivable) {
			typ = "Inflow"
		} else if strings.HasPrefix(up, "PROP-") {
			isForecast = true
			if strings.EqualFold(direction, "Inflow") {
				typ = "Inflow"
			} else {
				typ = "Outflow"
			}
		}

		// Normalize by FX rate
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		amtNorm := amt * rate
		if typ == "Outflow" {
			amtNorm = -math.Abs(amtNorm)
		} else {
			amtNorm = math.Abs(amtNorm)
		}

		for _, anchor := range anchors {
			monthKey := anchor.Format(constants.DateFormat)
			key := fmt.Sprintf("%s|%s", monthKey, cat)
			// choose a sensible Type value: prefer computed typ, otherwise fall back to group's direction
			rowType := typ
			if rowType == "" && strings.TrimSpace(direction) != "" {
				rowType = direction
			}
			if _, ok := combined[key]; !ok {
				combined[key] = &DateCatRow{Date: monthKey, Category: cat, Type: rowType}
			}
			if isForecast {
				combined[key].Forecast += amtNorm
			} else {
				combined[key].Actual += amtNorm
			}
		}
	}

	// Build output
	out := make([]DateCatRow, 0, len(combined))
	for _, v := range combined {
		v.Variance = v.Forecast - v.Actual
		out = append(out, *v)
	}

	// Sort by date then category
	sort.Slice(out, func(i, j int) bool {
		if out[i].Date == out[j].Date {
			return out[i].Category < out[j].Category
		}
		return out[i].Date < out[j].Date
	})

	return out, nil
}
