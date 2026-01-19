package landingpage

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Request structure for FX Ops Dashboard
type FXOpsDashboardRequest struct {
	UserID     string   `json:"user_id"`
	Entities   []string `json:"entities,omitempty"`
	Currencies []string `json:"currencies,omitempty"`
	TimePeriod int      `json:"time_period,omitempty"` // Number of days (e.g., 7, 30, 90)
}

// Response structures matching TypeScript interfaces
type Alert struct {
	Title       string      `json:"title"`
	Value       interface{} `json:"value"` // can be string or number
	Description string      `json:"description"`
	Action      string      `json:"action"`
	Color       string      `json:"color"`
	BG          string      `json:"bg"`
}

type KPICard struct {
	Title    string      `json:"title"`
	Value    interface{} `json:"value"` // can be string or number
	SubTitle string      `json:"subTitle"`
}

type ExposureBlock struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type SettlementSummary struct {
	Confirmed     int     `json:"confirmed"`
	Pending       int     `json:"pending"`
	Upcoming      int     `json:"upcoming"`
	ReconciledPct float64 `json:"reconciledPct"`
}

type NetPosition struct {
	Currency string `json:"currency"`
	Amount   string `json:"amount"`
}

type BankLimit struct {
	Bank  string `json:"bank"`
	Used  string `json:"used"`
	Limit string `json:"limit"`
}

type FXOpsDashboardResponse struct {
	Alerts            []Alert           `json:"alerts"`
	KPIs              []KPICard         `json:"kpis"`
	Exposures         []ExposureBlock   `json:"exposures"`
	SettlementSummary SettlementSummary `json:"settlementSummary"`
	NetPositions      []NetPosition     `json:"netPositions"`
	SpotRates         []NetPosition     `json:"spotRates"`
	BankLimits        []BankLimit       `json:"bankLimits"`
}

// GetFXOpsDashboard returns comprehensive FX Ops dashboard data with filters
func GetFXOpsDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req FXOpsDashboardRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid request format")
			return
		}

		// Use entities from request if provided, otherwise get all data (no entity filter)
		entities := req.Entities

		// Log for debugging
		log.Printf("Entities from request: %v", entities)
		if len(entities) == 0 {
			log.Printf("No entities filter provided - fetching all data")
		} else {
			log.Printf("Using entity filter: %v", entities)
		}

		// Parse time period (0 or not provided means fetch all data, no time filtering)
		days := 0 // Default to all data
		if req.TimePeriod > 0 {
			days = req.TimePeriod
		}

		// 1. ALERTS SECTION - Always return exactly 3 alerts
		alerts := []Alert{}

		// Alert 1: Maturing in 3 days
		maturingAlert := getMaturingExposuresAlert(r.Context(), db, entities, req.Currencies)
		alerts = append(alerts, maturingAlert)

		// Alert 2: Settlement Failed
		settlementAlert := getSettlementFailedAlert(r.Context(), db, entities)
		alerts = append(alerts, settlementAlert)

		// Alert 3: High Unhedged Exposure (always show)
		highExposureAlert := getHighUnhedgedAlert(r.Context(), db, entities, req.Currencies)
		alerts = append(alerts, highExposureAlert)

		// 2. KPI CARDS SECTION
		// KPI 1: Exposures Requiring Attention (Unhedged & Approaching)
		unhedgedCount, unhedgedAmount := getUnhedgedExposures(r.Context(), db, entities, req.Currencies, days)

		kpis := []KPICard{
			{
				Title:    "Exposures Requiring Attention",
				Value:    unhedgedCount,
				SubTitle: formatAmount(unhedgedAmount),
			},
		}

		// KPI 2: Trades Maturing Today
		tradesCount := getTradesMaturingToday(r.Context(), db, entities, req.Currencies)
		kpis = append(kpis, KPICard{
			Title:    "Trades Maturing Today",
			Value:    tradesCount,
			SubTitle: "Forward bookings",
		})

		// KPI 3: Overall Unhedged (Spot exposure)
		_, overallUnhedgedUSD := getUnhedgedExposures(r.Context(), db, entities, req.Currencies, 0) // All time
		kpis = append(kpis, KPICard{
			Title:    "Overall Unhedged",
			Value:    formatAmount(overallUnhedgedUSD),
			SubTitle: "Spot exposure",
		})

		// KPI 4: Pending Settlement Today
		pendingCount := getPendingSettlementToday(r.Context(), db, entities)
		kpis = append(kpis, KPICard{
			Title:    "Pending Settlement Today",
			Value:    pendingCount,
			SubTitle: "Requires approval",
		})

		// 3. EXPOSURE MATURITIES
		exposures := getExposureMaturities(r.Context(), db, entities, req.Currencies)

		// 4. SETTLEMENT PERFORMANCE (Daily)
		settlementSummary := getSettlementPerformance(r.Context(), db, entities)

		// 5. TOP 5 CURRENCY NET POSITIONS
		netPositions := getTopCurrencyPositions(r.Context(), db, entities, 5)

		// 6. BANK LIMITS USAGE (placeholder - no table yet)
		bankLimits := []BankLimit{} // Empty array for now

		// Spot rates for top trade currencies (INR based)
		spotRates := getTopTradeCurrencies()

		response := FXOpsDashboardResponse{
			Alerts:            alerts,
			KPIs:              kpis,
			Exposures:         exposures,
			SettlementSummary: settlementSummary,
			NetPositions:      netPositions,
			SpotRates:         spotRates,
			BankLimits:        bankLimits,
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// Helper: Get unhedged exposures maturing in 3 days for alerts
// Helper: Get maturing exposures alert - always returns exactly one alert
func getMaturingExposuresAlert(ctx context.Context, db *sql.DB, entities []string, currencies []string) Alert {
	now := time.Now()
	threeDaysLater := now.AddDate(0, 0, 3).Format(constants.DateFormat)

	entityFilter := ""
	currencyFilter := ""
	args := []interface{}{threeDaysLater}
	argPos := 2

	if len(entities) > 0 {
		entityFilter = fmt.Sprintf(" AND eh.entity = ANY($%d)", argPos)
		args = append(args, pq.Array(entities))
		argPos++
	}

	if len(currencies) > 0 {
		currencyFilter = fmt.Sprintf(" AND eh.currency = ANY($%d)", argPos)
		args = append(args, pq.Array(currencies))
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) as exposure_count
		FROM exposure_headers eh
		WHERE eh.value_date <= $1
			AND eh.value_date >= CURRENT_DATE
			%s
			AND LOWER(eh.approval_status) = 'approved'
			AND eh.exposure_header_id NOT IN (
				SELECT DISTINCT exposure_header_id 
				FROM exposure_hedge_links 
				WHERE is_active = true
			)
			%s
	`, entityFilter, currencyFilter)

	var count int
	err := db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		log.Printf("Error in getMaturingExposuresAlert: %v", err)
		count = 0
	}

	if count > 0 {
		return Alert{
			Title:       "Maturing in 3 days",
			Value:       count,
			Description: fmt.Sprintf("%d unhedged exposures maturing in next 3 days", count),
			Action:      "Review hedging strategy",
			Color:       "#ff6b6b",
			BG:          "#fff5f5",
		}
	} else {
		return Alert{
			Title:       "Maturing in 3 days",
			Value:       0,
			Description: "0 exposures maturing in next 3 days",
			Action:      "Continue monitoring",
			Color:       "#2196f3",
			BG:          "#e3f2fd",
		}
	}
}

// Helper: Get settlement failures alert
// Helper: Get settlement failed alert - always returns exactly one alert
func getSettlementFailedAlert(ctx context.Context, db *sql.DB, entities []string) Alert {
	entityFilter := ""
	args := []interface{}{}
	if len(entities) > 0 {
		entityFilter = "WHERE eh.entity = ANY($1) AND"
		args = append(args, pq.Array(entities))
	} else {
		entityFilter = "WHERE"
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM settlements s
		JOIN exposure_headers eh ON s.exposure_header_id = eh.exposure_header_id
		%s LOWER(s.ssettlement_status) = 'failed'
			AND s.settlement_date >= CURRENT_DATE - INTERVAL '7 days'
	`, entityFilter)

	var count int
	err := db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		log.Printf("Error in getSettlementFailedAlert: %v", err)
		count = 0
	}

	if count > 0 {
		return Alert{
			Title:       "Settlement Failed",
			Value:       count,
			Description: fmt.Sprintf("%d settlements failed in last 7 days", count),
			Action:      "Investigate failure cause",
			Color:       "#f44336",
			BG:          "#ffebee",
		}
	} else {
		return Alert{
			Title:       "Settlement Failed",
			Value:       0,
			Description: "0 settlement failures in last 7 days",
			Action:      "Continue monitoring",
			Color:       "#4caf50",
			BG:          "#f1f8e9",
		}
	}
}

// Helper: Get unhedged exposures count and amount
func getUnhedgedExposures(ctx context.Context, db *sql.DB, entities []string, currencies []string, days int) (int, float64) {
	entityFilter := ""
	currencyFilter := ""
	dateFilter := ""
	args := []interface{}{}
	argPos := 1

	// Add entity filter only if entities are provided
	if len(entities) > 0 {
		entityFilter = fmt.Sprintf(" AND eh.entity = ANY($%d)", argPos)
		args = append(args, pq.Array(entities))
		argPos++
	}

	// Add date filter only if days > 0
	if days > 0 {
		dateFilter = fmt.Sprintf(" AND eh.value_date <= $%d", argPos)
		args = append(args, time.Now().AddDate(0, 0, days).Format(constants.DateFormat))
		argPos++
	}

	if len(currencies) > 0 {
		currencyFilter = fmt.Sprintf(" AND eh.currency = ANY($%d)", argPos)
		args = append(args, pq.Array(currencies))
	}

	query := fmt.Sprintf(`
		SELECT 
			COUNT(*) as count,
			COALESCE(SUM(ABS(eh.total_open_amount)), 0) as total_amount,
			eh.currency
		FROM exposure_headers eh
		WHERE LOWER(eh.approval_status) = 'approved'
			%s
			%s
			AND eh.exposure_header_id NOT IN (
				SELECT DISTINCT exposure_header_id 
				FROM exposure_hedge_links 
				WHERE is_active = true
			)
			%s
		GROUP BY eh.currency
	`, entityFilter, dateFilter, currencyFilter)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		log.Printf("Error in getUnhedgedExposures: %v", err)
		return 0, 0
	}
	defer rows.Close()

	totalCount := 0
	totalUSD := 0.0

	for rows.Next() {
		var count int
		var totalAmount float64
		var currency string

		if err := rows.Scan(&count, &totalAmount, &currency); err != nil {
			log.Printf("Error scanning unhedged exposures: %v", err)
			continue
		}

		totalCount += count
		// Convert to INR (simplified conversion)
		usdAmount := convertToINR(totalAmount, currency)
		totalUSD += usdAmount
	}

	return totalCount, totalUSD
}

// Helper: Get trades maturing today from forward bookings
func getTradesMaturingToday(ctx context.Context, db *sql.DB, entities []string, currencies []string) int {
	today := time.Now().Format(constants.DateFormat)

	entityFilter := ""
	currencyFilter := ""
	args := []interface{}{today}
	argPos := 2

	if len(entities) > 0 {
		entityFilter = fmt.Sprintf(" AND fb.entity_level_0 = ANY($%d)", argPos)
		args = append(args, pq.Array(entities))
		argPos++
	}

	if len(currencies) > 0 {
		currencyFilter = fmt.Sprintf(" AND fb.base_currency = ANY($%d)", argPos)
		args = append(args, pq.Array(currencies))
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM forward_bookings fb
		WHERE fb.maturity_date = $1
			AND LOWER(fb.processing_status) = 'approved'
			%s
			%s
	`, entityFilter, currencyFilter)

	var count int
	err := db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		log.Printf("Error in getTradesMaturingToday: %v", err)
		return 0
	}

	return count
}

// Helper: Get pending settlements today
func getPendingSettlementToday(ctx context.Context, db *sql.DB, entities []string) int {
	today := time.Now().Format(constants.DateFormat)

	entityFilter := ""
	args := []interface{}{today}
	if len(entities) > 0 {
		entityFilter = "WHERE eh.entity = ANY($2) AND"
		args = append(args, pq.Array(entities))
	} else {
		entityFilter = "WHERE"
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM settlements s
		JOIN exposure_headers eh ON s.exposure_header_id = eh.exposure_header_id
		%s s.settlement_date = $1
			AND LOWER(s.apprval_status) = 'pending'
	`, entityFilter)

	var count int
	err := db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		log.Printf("Error in getPendingSettlementToday: %v", err)
		return 0
	}

	return count
}

// Helper: Get exposure maturities (Next 7 days, Next 30 days, Total)
func getExposureMaturities(ctx context.Context, db *sql.DB, entities []string, currencies []string) []ExposureBlock {
	now := time.Now()

	entityFilter := ""
	currencyFilter := ""
	args := []interface{}{}
	argPos := 1

	if len(entities) > 0 {
		entityFilter = fmt.Sprintf(" AND eh.entity = ANY($%d)", argPos)
		args = append(args, pq.Array(entities))
		argPos++
	}

	if len(currencies) > 0 {
		currencyFilter = fmt.Sprintf(" AND eh.currency = ANY($%d)", argPos)
		args = append(args, pq.Array(currencies))
	}

	query := fmt.Sprintf(`
		SELECT 
			eh.total_open_amount,
			eh.currency,
			eh.value_date,
			eh.exposure_header_id
		FROM exposure_headers eh
		WHERE eh.value_date IS NOT NULL
			AND LOWER(eh.approval_status) = 'approved'
			%s
			%s
	`, entityFilter, currencyFilter)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		log.Printf("Error in getExposureMaturities: %v", err)
		return []ExposureBlock{}
	}
	defer rows.Close()

	var sum7, sum30, sumTotal float64
	var unhedged7, unhedged30 int

	// Check if exposure is hedged
	hedgedMap := make(map[string]bool)
	hedgeQuery := `SELECT DISTINCT exposure_header_id FROM exposure_hedge_links WHERE is_active = true`
	hedgeRows, err := db.QueryContext(ctx, hedgeQuery)
	if err != nil {
		log.Printf("[WARN] failed to fetch exposure hedges: %v", err)
	} else {
		defer hedgeRows.Close()
		for hedgeRows.Next() {
			var expID string
			hedgeRows.Scan(&expID)
			hedgedMap[expID] = true
		}
	}

	for rows.Next() {
		var amount float64
		var currency, valueDate, expID string

		if err := rows.Scan(&amount, &currency, &valueDate, &expID); err != nil {
			continue
		}

		maturityDate, err := time.Parse(constants.DateFormat, valueDate)
		if err != nil {
			continue
		}

		usdAmount := convertToINR(math.Abs(amount), currency)
		sumTotal += usdAmount

		daysDiff := int(maturityDate.Sub(now).Hours() / 24)

		if daysDiff <= 7 {
			sum7 += usdAmount
			if !hedgedMap[expID] {
				unhedged7++
			}
		}
		if daysDiff <= 30 {
			sum30 += usdAmount
			if !hedgedMap[expID] {
				unhedged30++
			}
		}
	}

	return []ExposureBlock{
		{
			Label: "Next 7 days",
			Value: fmt.Sprintf("%s (%d unhedged)", formatAmount(sum7), unhedged7),
		},
		{
			Label: "Next 30 days",
			Value: fmt.Sprintf("%s (%d unhedged)", formatAmount(sum30), unhedged30),
		},
		{
			Label: "Total Upcoming",
			Value: formatAmount(sumTotal),
		},
	}
}

// Helper: Get settlement performance summary - FIXED SQL INTERVAL SYNTAX
func getSettlementPerformance(ctx context.Context, db *sql.DB, entities []string) SettlementSummary {
	today := time.Now().Format(constants.DateFormat)

	summary := SettlementSummary{
		Confirmed:     0,
		Pending:       0,
		Upcoming:      0,
		ReconciledPct: 0.0,
	}

	entityFilter := ""
	args := []interface{}{today}
	if len(entities) > 0 {
		entityFilter = "WHERE eh.entity = ANY($2) AND"
		args = append(args, pq.Array(entities))
	} else {
		entityFilter = "WHERE"
	}

	// Count by status - FIXED: Use proper date arithmetic
	query := fmt.Sprintf(`
		SELECT 
			LOWER(s.apprval_status) as status,
			COUNT(*) as count
		FROM settlements s
		JOIN exposure_headers eh ON s.exposure_header_id = eh.exposure_header_id
		%s s.settlement_date >= $1::date - INTERVAL '1 day'
			AND s.settlement_date <= $1::date
		GROUP BY LOWER(s.apprval_status)
	`, entityFilter)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		log.Printf("Error in getSettlementPerformance: %v", err)
		return summary
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int

		if err := rows.Scan(&status, &count); err != nil {
			log.Printf("Error scanning settlement performance: %v", err)
			continue
		}

		switch status {
		case "confirmed":
			summary.Confirmed = count
		case "pending":
			summary.Pending = count
		}
	}

	// Upcoming (future dates)
	upcomingEntityFilter := ""
	upcomingArgs := []interface{}{today}
	if len(entities) > 0 {
		upcomingEntityFilter = "WHERE eh.entity = ANY($2) AND"
		upcomingArgs = append(upcomingArgs, pq.Array(entities))
	} else {
		upcomingEntityFilter = "WHERE"
	}

	upcomingQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM settlements s
		JOIN exposure_headers eh ON s.exposure_header_id = eh.exposure_header_id
		%s s.settlement_date > $1::date
	`, upcomingEntityFilter)
	db.QueryRowContext(ctx, upcomingQuery, upcomingArgs...).Scan(&summary.Upcoming)

	// Auto-reconciled percentage (placeholder logic)
	total := summary.Confirmed + summary.Pending
	if total > 0 {
		summary.ReconciledPct = float64(summary.Confirmed) / float64(total) * 100
	}

	return summary
}

// Helper: Get top N currency net positions
func getTopCurrencyPositions(ctx context.Context, db *sql.DB, entities []string, topN int) []NetPosition {
	entityFilter := ""
	args := []interface{}{}
	argPos := 1

	if len(entities) > 0 {
		entityFilter = fmt.Sprintf("WHERE eh.entity = ANY($%d) AND", argPos)
		args = append(args, pq.Array(entities))
		argPos++
	} else {
		entityFilter = "WHERE"
	}

	query := fmt.Sprintf(`
		SELECT 
			eh.currency,
			SUM(eh.total_open_amount) as net_position
		FROM exposure_headers eh
		%s LOWER(eh.approval_status) = 'approved'
		GROUP BY eh.currency
		ORDER BY ABS(SUM(eh.total_open_amount)) DESC
		LIMIT $%d
	`, entityFilter, argPos)

	args = append(args, topN)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		log.Printf("Error in getTopCurrencyPositions: %v", err)
		return []NetPosition{}
	}
	defer rows.Close()

	positions := []NetPosition{}
	for rows.Next() {
		var currency string
		var netPosition float64

		if err := rows.Scan(&currency, &netPosition); err != nil {
			log.Printf("Error scanning net positions: %v", err)
			continue
		}

		usdAmount := convertToINR(math.Abs(netPosition), currency)
		sign := ""
		if netPosition < 0 {
			sign = "-"
		}

		positions = append(positions, NetPosition{
			Currency: currency,
			Amount:   sign + formatAmount(usdAmount),
		})
	}

	return positions
}

// Helper: Format amount in INR with M/K suffix and rupee symbol
func formatAmount(val float64) string {
	if val >= 10000000 { // 1 crore
		return fmt.Sprintf("₹%.2fCr", val/10000000)
	} else if val >= 100000 {
		return fmt.Sprintf("₹%.1fL", val/100000) // lakhs
	} else if val >= 1000000 {
		// fallback million style
		return fmt.Sprintf("₹%.1fM", val/1000000)
	} else if val >= 1000 {
		return fmt.Sprintf("₹%.1fK", val/1000)
	}
	return fmt.Sprintf("₹%.0f", val)
}

// Helper: Convert currency amount to INR using simplified spot rates
func convertToINR(amount float64, currency string) float64 {
	rates := map[string]float64{
		// INR-based spot rates (example/fallback values)
		"INR": 1.0,
		"USD": 90.0,
		"EUR": 98.0,
		"GBP": 118.0,
		"JPY": 0.67,
		"AUD": 56.0,
		"CAD": 67.0,
		"CHF": 98.0,
		"SGD": 67.0,
		"HKD": 11.5,
		"NZD": 50.0,
		"AED": 24.5,
		"SAR": 24.0,
		"CNY": 12.8,
		"KRW": 0.067,
		"SEK": 8.5,
		"NOK": 8.3,
		"DKK": 13.1,
		"RUB": 1.1,
		"ZAR": 4.8,
	}

	if rate, exists := rates[strings.ToUpper(currency)]; exists {
		return amount * rate
	}
	// Default: assume amount already in INR
	return amount
}

// Helper: Return top 20 currencies India commonly trades with example spot rates
func getTopTradeCurrencies() []NetPosition {
	// fixed list - can be replaced with live rates later
	top := []struct {
		c string
		r float64
	}{
		{"USD", 90.0}, {"EUR", 98.0}, {"GBP", 118.0}, {"JPY", 0.67}, {"AUD", 56.0},
		{"CAD", 67.0}, {"CHF", 98.0}, {"SGD", 67.0}, {"HKD", 11.5}, {"NZD", 50.0},
		{"AED", 24.5}, {"SAR", 24.0}, {"CNY", 12.8}, {"KRW", 0.067}, {"SEK", 8.5},
		{"NOK", 8.3}, {"DKK", 13.1}, {"RUB", 1.1}, {"ZAR", 4.8}, {"INR", 1.0},
	}

	res := make([]NetPosition, 0, len(top))
	for _, t := range top {
		res = append(res, NetPosition{
			Currency: t.c,
			Amount:   fmt.Sprintf("₹%.4f", t.r),
		})
	}
	return res
}

// Helper: Get high unhedged exposure alert - always returns exactly one alert
func getHighUnhedgedAlert(ctx context.Context, db *sql.DB, entities []string, currencies []string) Alert {
	_, totalUnhedged := getUnhedgedExposures(ctx, db, entities, currencies, 0)

	if totalUnhedged > 1000000 { // > $1M
		return Alert{
			Title:       "High Unhedged Exposure",
			Value:       formatAmount(totalUnhedged),
			Description: fmt.Sprintf("Total unhedged exposure of %s requires attention", formatAmount(totalUnhedged)),
			Action:      "Review hedging strategy",
			Color:       "#ff9800",
			BG:          "#fff3e0",
		}
	} else if totalUnhedged > 0 {
		return Alert{
			Title:       "High Unhedged Exposure",
			Value:       formatAmount(totalUnhedged),
			Description: fmt.Sprintf("Total unhedged exposure: %s", formatAmount(totalUnhedged)),
			Action:      "Monitor exposure levels",
			Color:       "#2196f3",
			BG:          "#e3f2fd",
		}
	} else {
		return Alert{
			Title:       "High Unhedged Exposure",
			Value:       "$0",
			Description: "No unhedged exposures detected",
			Action:      "Maintain current strategy",
			Color:       "#4caf50",
			BG:          "#f1f8e9",
		}
	}
}
