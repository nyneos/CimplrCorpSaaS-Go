package fundavailibilty

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetFundAvailability returns combined actuals and projections data
// with flexible period aggregation (daily/weekly/monthly/quarterly/yearly)
func GetFundAvailability(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID    string `json:"user_id"`
			AsOfDate  string `json:"as_of_date"`  // YYYY-MM-DD format
			ViewType  string `json:"view_type"`   // daily, weekly, monthly, quarterly, yearly
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, "Missing user_id")
			return
		}

		// Validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// Default as_of_date to today
		asOfDate := time.Now()
		if req.AsOfDate != "" {
			parsed, err := time.Parse(constants.DateFormat, req.AsOfDate)
			if err != nil {
				api.RespondWithResult(w, false, "Invalid as_of_date format. Use YYYY-MM-DD")
				return
			}
			asOfDate = parsed
		}

		// Default view_type to monthly
		viewType := strings.ToLower(strings.TrimSpace(req.ViewType))
		if viewType == "" {
			viewType = "monthly"
		}
		if viewType != "daily" && viewType != "weekly" && viewType != "monthly" && viewType != "quarterly" && viewType != "yearly" {
			api.RespondWithResult(w, false, "Invalid view_type. Must be: daily, weekly, monthly, quarterly, or yearly")
			return
		}

		// Calculate date range based on view_type
		var endDate time.Time
		var periodCount int
		switch viewType {
		case "daily":
			endDate = asOfDate.AddDate(0, 0, 31)
			periodCount = 31
		case "weekly":
			endDate = asOfDate.AddDate(0, 0, 364) // 52 weeks
			periodCount = 52
		case "monthly":
			endDate = asOfDate.AddDate(0, 24, 0) // 24 months
			periodCount = 24
		case "quarterly":
			endDate = asOfDate.AddDate(1, 0, 0) // 1 year = 4 quarters
			periodCount = 4
		case "yearly":
			endDate = asOfDate.AddDate(1, 0, 0) // 1 year
			periodCount = 1
		}

		// Get middleware context filters
		entityIDs := api.GetEntityIDsFromCtx(ctx)
		bankNames := api.GetBankNamesFromCtx(ctx)

		// Fetch actuals from bank statements
		actuals, actualsErr := fetchActuals(ctx, pgxPool, asOfDate, endDate, viewType, entityIDs, bankNames)
		if actualsErr != nil {
			api.RespondWithResult(w, false, "Failed to fetch actuals: "+actualsErr.Error())
			return
		}

		// Fetch projections from cashflow proposals
		projections, projectionsErr := fetchProjections(ctx, pgxPool, asOfDate, endDate, viewType, entityIDs, bankNames)
		if projectionsErr != nil {
			api.RespondWithResult(w, false, "Failed to fetch projections: "+projectionsErr.Error())
			return
		}

		// Calculate summaries
		actualsSummary := calculateSummary(actuals)
		projectionsSummary := calculateSummary(projections)

		response := map[string]interface{}{
			"success":     true,
			"as_of_date":  asOfDate.Format(constants.DateFormat),
			"view_type":   viewType,
			"date_range": map[string]string{
				"start": asOfDate.Format(constants.DateFormat),
				"end":   endDate.Format(constants.DateFormat),
			},
			"period_count": periodCount,
			"actuals":      actuals,
			"projections":  projections,
			"summary": map[string]interface{}{
				"actuals":     actualsSummary,
				"projections": projectionsSummary,
				"combined": map[string]interface{}{
					"total_inflow":  actualsSummary["total_inflow"].(float64) + projectionsSummary["total_inflow"].(float64),
					"total_outflow": actualsSummary["total_outflow"].(float64) + projectionsSummary["total_outflow"].(float64),
					"net_amount":    actualsSummary["net_amount"].(float64) + projectionsSummary["net_amount"].(float64),
				},
			},
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// fetchActuals retrieves bank statement transactions grouped by entity/bank/account/currency/category
func fetchActuals(ctx context.Context, pgxPool *pgxpool.Pool, asOfDate, endDate time.Time, viewType string, entityIDs, bankNames []string) ([]map[string]interface{}, error) {
	// Build entity filter
	entityFilter := ""
	if len(entityIDs) > 0 {
		quotedEntities := make([]string, len(entityIDs))
		for i, e := range entityIDs {
			quotedEntities[i] = "'" + strings.ReplaceAll(e, "'", "''") + "'"
		}
		entityFilter = " AND mba.entity_id IN (" + strings.Join(quotedEntities, ",") + ")"
	}

	// Build bank filter
	bankFilter := ""
	if len(bankNames) > 0 {
		quotedBanks := make([]string, len(bankNames))
		for i, b := range bankNames {
			quotedBanks[i] = "'" + strings.ReplaceAll(strings.ToLower(b), "'", "''") + "'"
		}
		bankFilter = " AND LOWER(TRIM(mba.bank_name)) IN (" + strings.Join(quotedBanks, ",") + ")"
	}

	query := fmt.Sprintf(`
		WITH approved_statements AS (
			SELECT DISTINCT ON (bs.bank_statement_id)
				bs.bank_statement_id,
				bs.entity_id,
				bs.account_number
			FROM cimplrcorpsaas.bank_statements bs
			JOIN cimplrcorpsaas.auditactionbankstatement a 
				ON a.bankstatementid = bs.bank_statement_id
			WHERE a.processing_status = 'APPROVED'
			ORDER BY bs.bank_statement_id, a.requested_at DESC
		)
		SELECT 
			mba.entity_id,
			COALESCE(me.entity_name, '') AS entity_name,
			mba.bank_name,
			mba.account_number,
			mba.currency AS currency_code,
			mba.usage,
			CASE 
				WHEN mcc.category_type IS NOT NULL THEN mcc.category_type
				WHEN t.deposit_amount > 0 THEN 'Inflow'
				WHEN t.withdrawal_amount > 0 THEN 'Outflow'
				ELSE 'Uncategorized'
			END AS flow,
			COALESCE(t.category_id, '') AS category_id,
			COALESCE(mcc.category_name, 'Uncategorized') AS category_name,
			t.description,
			t.transaction_date,
			COALESCE(t.withdrawal_amount, 0) AS withdrawal_amount,
			COALESCE(t.deposit_amount, 0) AS deposit_amount
		FROM cimplrcorpsaas.bank_statement_transactions t
		JOIN approved_statements ast ON ast.bank_statement_id = t.bank_statement_id
		JOIN public.masterbankaccount mba ON mba.account_number = t.account_number
		LEFT JOIN public.masterentitycash me ON me.entity_id = mba.entity_id
		LEFT JOIN public.mastercashflowcategory mcc ON mcc.category_id = t.category_id
		WHERE t.transaction_date >= $1 
			AND t.transaction_date <= $2
			AND mba.is_deleted = false
			%s
			%s
		ORDER BY mba.entity_id, mba.bank_name, mba.account_number, t.transaction_date
	`, entityFilter, bankFilter)

	rows, err := pgxPool.Query(ctx, query, asOfDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Group data by entity/bank/account/currency/category/description
	type groupKey struct {
		EntityID     string
		EntityName   string
		BankName     string
		BankAccount  string
		CurrencyCode string
		Usage        string
		Flow         string
		CategoryID   string
		CategoryName string
		Description  string
	}

	groups := make(map[groupKey][]struct {
		Date       time.Time
		Withdrawal float64
		Deposit    float64
	})

	for rows.Next() {
		var entityID, entityName, bankName, accountNumber, currencyCode, usage, flow, categoryID, categoryName, description string
		var transactionDate time.Time
		var withdrawal, deposit float64

		if err := rows.Scan(&entityID, &entityName, &bankName, &accountNumber, &currencyCode, &usage, &flow, &categoryID, &categoryName, &description, &transactionDate, &withdrawal, &deposit); err != nil {
			return nil, err
		}

		key := groupKey{
			EntityID:     entityID,
			EntityName:   entityName,
			BankName:     bankName,
			BankAccount:  accountNumber,
			CurrencyCode: currencyCode,
			Usage:        usage,
			Flow:         flow,
			CategoryID:   categoryID,
			CategoryName: categoryName,
			Description:  description,
		}

		groups[key] = append(groups[key], struct {
			Date       time.Time
			Withdrawal float64
			Deposit    float64
		}{
			Date:       transactionDate,
			Withdrawal: withdrawal,
			Deposit:    deposit,
		})
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	// Convert to output format with period aggregation
	result := make([]map[string]interface{}, 0, len(groups))
	for key, transactions := range groups {
		periods := aggregateByPeriod(transactions, asOfDate, viewType)
		
		// Zero-fill all periods in range
		allPeriods := generateAllPeriods(asOfDate, endDate, viewType)
		for periodKey := range allPeriods {
			if _, exists := periods[periodKey]; !exists {
				periods[periodKey] = 0.0
			}
		}
		
		totalAmount := 0.0
		for _, amount := range periods {
			totalAmount += amount
		}

		result = append(result, map[string]interface{}{
			"entity_id":     key.EntityID,
			"entity_name":   key.EntityName,
			"bank_name":     key.BankName,
			"bank_account":  key.BankAccount,
			"currency_code": key.CurrencyCode,
			"usage":         key.Usage,
			"flow":          key.Flow,
			"category_id":   key.CategoryID,
			"category_name": key.CategoryName,
			"description":   key.Description,
			"periods":       periods,
			"total_amount":  totalAmount,
		})
	}

	return result, nil
}

// fetchProjections retrieves cashflow projections grouped by entity/bank/account/currency/category
func fetchProjections(ctx context.Context, pgxPool *pgxpool.Pool, asOfDate, endDate time.Time, viewType string, entityIDs, bankNames []string) ([]map[string]interface{}, error) {
	// NOTE: For projections, we don't filter by entity_id or bank_name from middleware
	// because proposal items use free text fields which may not match master data
	// Users create projections with their own entity/bank names, so we show all approved projections
	// filtered only by date range

	log.Printf("[fetchProjections] Date range: %s to %s (showing ALL approved projections)", 
		asOfDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	// First, check if there are ANY approved proposals
	var approvedCount int
	countQ := `SELECT COUNT(DISTINCT p.proposal_id) FROM cimplrcorpsaas.cashflow_proposal p
		JOIN cimplrcorpsaas.audit_action_cashflow_proposal a ON a.proposal_id = p.proposal_id
		WHERE a.processing_status = 'APPROVED'`
	pgxPool.QueryRow(ctx, countQ).Scan(&approvedCount)
	log.Printf("[fetchProjections] Total approved proposals in DB: %d", approvedCount)

	// Check how many projection records exist
	var projCount int
	pgxPool.QueryRow(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.cashflow_projection_monthly`).Scan(&projCount)
	log.Printf("[fetchProjections] Total projection records in DB: %d", projCount)

	// Check how many would match WITHOUT entity/bank filters (just date range)
	var dateMatchCount int
	dateOnlyQ := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM cimplrcorpsaas.cashflow_projection_monthly pm
		JOIN cimplrcorpsaas.cashflow_proposal_item i ON i.item_id = pm.item_id
		JOIN (
			SELECT DISTINCT ON (p.proposal_id) p.proposal_id
			FROM cimplrcorpsaas.cashflow_proposal p
			JOIN cimplrcorpsaas.audit_action_cashflow_proposal a ON a.proposal_id = p.proposal_id
			WHERE a.processing_status = 'APPROVED'
			ORDER BY p.proposal_id, a.requested_at DESC
		) ap ON ap.proposal_id = i.proposal_id
		WHERE (
			pm.year > EXTRACT(YEAR FROM $1::timestamp)::int 
			OR (pm.year = EXTRACT(YEAR FROM $1::timestamp)::int AND pm.month >= EXTRACT(MONTH FROM $1::timestamp)::int)
		)
		AND (
			pm.year < EXTRACT(YEAR FROM $2::timestamp)::int
			OR (pm.year = EXTRACT(YEAR FROM $2::timestamp)::int AND pm.month <= EXTRACT(MONTH FROM $2::timestamp)::int)
		)
	`)
	pgxPool.QueryRow(ctx, dateOnlyQ, asOfDate, endDate).Scan(&dateMatchCount)
	log.Printf("[fetchProjections] Records matching date range (no entity/bank filter): %d", dateMatchCount)

	// Check sample entity names in proposal items
	sampleQ := `SELECT DISTINCT i.entity_name FROM cimplrcorpsaas.cashflow_proposal_item i LIMIT 5`
	sampleRows, _ := pgxPool.Query(ctx, sampleQ)
	var sampleEntities []string
	for sampleRows.Next() {
		var entName string
		sampleRows.Scan(&entName)
		sampleEntities = append(sampleEntities, entName)
	}
	sampleRows.Close()
	log.Printf("[fetchProjections] Sample entity_name values in proposal_item: %v", sampleEntities)

	query := fmt.Sprintf(`
		WITH approved_proposals AS (
			SELECT DISTINCT ON (p.proposal_id)
				p.proposal_id,
				p.base_currency_code,
				p.effective_date
			FROM cimplrcorpsaas.cashflow_proposal p
			JOIN cimplrcorpsaas.audit_action_cashflow_proposal a 
				ON a.proposal_id = p.proposal_id
			WHERE a.processing_status = 'APPROVED'
			ORDER BY p.proposal_id, a.requested_at DESC
		)
		SELECT 
			COALESCE(me.entity_id, i.entity_name, '') AS entity_id,
			COALESCE(me.entity_name, i.entity_name, '') AS entity_name,
			COALESCE(i.bank_name, '') AS bank_name,
			COALESCE(i.bank_account_number, '') AS bank_account,
			i.currency_code,
			COALESCE(mba.usage, '') AS usage,
			CASE 
				WHEN mcc.category_type IS NOT NULL THEN mcc.category_type
				ELSE i.cashflow_type
			END AS flow,
			i.category_id,
			COALESCE(mcc.category_name, 'Unknown') AS category_name,
			i.description,
			COALESCE(i.maturity_date, ap.effective_date) AS maturity_date,
			i.is_recurring,
			COALESCE(i.recurrence_frequency, 'Yearly') AS recurrence_frequency,
			pm.year,
			pm.month,
			pm.projected_amount
		FROM cimplrcorpsaas.cashflow_projection_monthly pm
		JOIN cimplrcorpsaas.cashflow_proposal_item i ON i.item_id = pm.item_id
		JOIN approved_proposals ap ON ap.proposal_id = i.proposal_id
		LEFT JOIN public.masterentitycash me ON LOWER(TRIM(me.entity_name)) = LOWER(TRIM(i.entity_name))
		LEFT JOIN public.masterbankaccount mba ON mba.account_number = i.bank_account_number AND mba.entity_id = me.entity_id
		LEFT JOIN public.mastercashflowcategory mcc ON mcc.category_id = i.category_id
		WHERE (
			pm.year > EXTRACT(YEAR FROM $1::timestamp)::int 
			OR (pm.year = EXTRACT(YEAR FROM $1::timestamp)::int AND pm.month >= EXTRACT(MONTH FROM $1::timestamp)::int)
		)
		AND (
			pm.year < EXTRACT(YEAR FROM $2::timestamp)::int
			OR (pm.year = EXTRACT(YEAR FROM $2::timestamp)::int AND pm.month <= EXTRACT(MONTH FROM $2::timestamp)::int)
		)
		ORDER BY me.entity_id, i.bank_name, i.bank_account_number, pm.year, pm.month
	`)

	log.Printf("[fetchProjections] Executing query...")
	rows, err := pgxPool.Query(ctx, query, asOfDate, endDate)
	if err != nil {
		log.Printf("[fetchProjections] Query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	log.Printf("[fetchProjections] Query executed, reading rows...")
	rowCount := 0

	// Group data
	type groupKey struct {
		EntityID          string
		EntityName        string
		BankName          string
		BankAccount       string
		CurrencyCode      string
		Usage             string
		Flow              string
		CategoryID        string
		CategoryName      string
		Description       string
		MaturityDate      time.Time
		IsRecurring       bool
		RecurrenceFreq    string
	}

	groups := make(map[groupKey][]struct {
		Year   int
		Month  int
		Amount float64
	})

	for rows.Next() {
		var entityID, entityName, bankName, bankAccount, currencyCode, usage, flow, categoryID, categoryName, description string
		var maturityDate time.Time
		var isRecurring bool
		var recurrenceFreq string
		var year, month int
		var amount float64

		if err := rows.Scan(&entityID, &entityName, &bankName, &bankAccount, &currencyCode, &usage, &flow, &categoryID, &categoryName, &description, &maturityDate, &isRecurring, &recurrenceFreq, &year, &month, &amount); err != nil {
			log.Printf("[fetchProjections] Row scan error: %v", err)
			return nil, err
		}

		rowCount++
		if rowCount <= 3 {
			log.Printf("[fetchProjections] Sample row %d: entity=%s/%s bank=%s account=%s year=%d month=%d amount=%.2f", 
				rowCount, entityID, entityName, bankName, bankAccount, year, month, amount)
		}

		key := groupKey{
			EntityID:       entityID,
			EntityName:     entityName,
			BankName:       bankName,
			BankAccount:    bankAccount,
			CurrencyCode:   currencyCode,
			Usage:          usage,
			Flow:           flow,
			CategoryID:     categoryID,
			CategoryName:   categoryName,
			Description:    description,
			MaturityDate:   maturityDate,
			IsRecurring:    isRecurring,
			RecurrenceFreq: recurrenceFreq,
		}

		groups[key] = append(groups[key], struct {
			Year   int
			Month  int
			Amount float64
		}{
			Year:   year,
			Month:  month,
			Amount: amount,
		})
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	log.Printf("[fetchProjections] Total rows fetched: %d, Found %d unique projection groups", rowCount, len(groups))

	// Convert to output format
	result := make([]map[string]interface{}, 0, len(groups))
	for key, monthlyData := range groups {
		periods := aggregateProjectionsByPeriod(monthlyData, asOfDate, viewType)
		
		// Zero-fill all periods in range
		allPeriods := generateAllPeriods(asOfDate, endDate, viewType)
		for periodKey := range allPeriods {
			if _, exists := periods[periodKey]; !exists {
				periods[periodKey] = 0.0
			}
		}
		
		totalAmount := 0.0
		for _, amount := range periods {
			totalAmount += amount
		}

		result = append(result, map[string]interface{}{
			"entity_id":            key.EntityID,
			"entity_name":          key.EntityName,
			"bank_name":            key.BankName,
			"bank_account":         key.BankAccount,
			"currency_code":        key.CurrencyCode,
			"usage":                key.Usage,
			"flow":                 key.Flow,
			"category_id":          key.CategoryID,
			"category_name":        key.CategoryName,
			"description":          key.Description,
			"maturity_date":        key.MaturityDate.Format("2006-01-02"),
			"periods":              periods,
			"total_amount":         totalAmount,
			"is_recurring":         key.IsRecurring,
			"recurrence_frequency": key.RecurrenceFreq,
		})
	}

	return result, nil
}

// aggregateByPeriod aggregates transaction data by the specified view type
func aggregateByPeriod(transactions []struct {
	Date       time.Time
	Withdrawal float64
	Deposit    float64
}, asOfDate time.Time, viewType string) map[string]float64 {
	periods := make(map[string]float64)

	for _, txn := range transactions {
		var periodKey string
		
		switch viewType {
		case "daily":
			periodKey = txn.Date.Format("2006-01-02")
		case "weekly":
			year, week := txn.Date.ISOWeek()
			periodKey = fmt.Sprintf("%d-W%02d", year, week)
		case "monthly":
			periodKey = txn.Date.Format("2006-01")
		case "quarterly":
			quarter := (int(txn.Date.Month())-1)/3 + 1
			periodKey = fmt.Sprintf("%d-Q%d", txn.Date.Year(), quarter)
		case "yearly":
			periodKey = fmt.Sprintf("%d", txn.Date.Year())
		}

		// Net amount: deposits are positive, withdrawals are negative
		netAmount := txn.Deposit - txn.Withdrawal
		periods[periodKey] += netAmount
	}

	return periods
}

// aggregateProjectionsByPeriod aggregates monthly projection data by the specified view type
func aggregateProjectionsByPeriod(monthlyData []struct {
	Year   int
	Month  int
	Amount float64
}, asOfDate time.Time, viewType string) map[string]float64 {
	periods := make(map[string]float64)

	for _, data := range monthlyData {
		date := time.Date(data.Year, time.Month(data.Month), 1, 0, 0, 0, 0, time.UTC)
		var periodKey string

		switch viewType {
		case "daily":
			// Distribute monthly amount evenly across days in month
			daysInMonth := time.Date(data.Year, time.Month(data.Month+1), 0, 0, 0, 0, 0, time.UTC).Day()
			dailyAmount := data.Amount / float64(daysInMonth)
			for day := 1; day <= daysInMonth; day++ {
				dayDate := time.Date(data.Year, time.Month(data.Month), day, 0, 0, 0, 0, time.UTC)
				if dayDate.After(asOfDate) || dayDate.Equal(asOfDate) {
					periodKey = dayDate.Format("2006-01-02")
					periods[periodKey] += dailyAmount
				}
			}
			continue
		case "weekly":
			// Distribute monthly amount across weeks in month
			firstDay := time.Date(data.Year, time.Month(data.Month), 1, 0, 0, 0, 0, time.UTC)
			lastDay := time.Date(data.Year, time.Month(data.Month+1), 0, 0, 0, 0, 0, time.UTC)
			
			weeksInMonth := make(map[string]bool)
			for d := firstDay; !d.After(lastDay); d = d.AddDate(0, 0, 1) {
				year, week := d.ISOWeek()
				weeksInMonth[fmt.Sprintf("%d-W%02d", year, week)] = true
			}
			
			weeklyAmount := data.Amount / float64(len(weeksInMonth))
			for week := range weeksInMonth {
				periods[week] += weeklyAmount
			}
			continue
		case "monthly":
			periodKey = date.Format("2006-01")
		case "quarterly":
			quarter := (data.Month-1)/3 + 1
			periodKey = fmt.Sprintf("%d-Q%d", data.Year, quarter)
		case "yearly":
			periodKey = fmt.Sprintf("%d", data.Year)
		}

		periods[periodKey] += data.Amount
	}

	return periods
}

// generateAllPeriods creates a map with all period keys in the range filled with 0
func generateAllPeriods(startDate, endDate time.Time, viewType string) map[string]float64 {
	allPeriods := make(map[string]float64)
	
	switch viewType {
	case "daily":
		for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
			allPeriods[d.Format("2006-01-02")] = 0.0
		}
	case "weekly":
		for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 7) {
			year, week := d.ISOWeek()
			allPeriods[fmt.Sprintf("%d-W%02d", year, week)] = 0.0
		}
	case "monthly":
		for d := startDate; !d.After(endDate); d = d.AddDate(0, 1, 0) {
			allPeriods[d.Format("2006-01")] = 0.0
		}
	case "quarterly":
		for d := startDate; !d.After(endDate); d = d.AddDate(0, 3, 0) {
			quarter := (int(d.Month())-1)/3 + 1
			allPeriods[fmt.Sprintf("%d-Q%d", d.Year(), quarter)] = 0.0
		}
	case "yearly":
		for y := startDate.Year(); y <= endDate.Year(); y++ {
			allPeriods[fmt.Sprintf("%d", y)] = 0.0
		}
	}
	
	return allPeriods
}

// calculateSummary calculates total inflow, outflow, and net amount
func calculateSummary(data []map[string]interface{}) map[string]interface{} {
	totalInflow := 0.0
	totalOutflow := 0.0
	totalRecords := len(data)

	for _, item := range data {
		flow := item["flow"].(string)
		totalAmount := item["total_amount"].(float64)

		if flow == "Inflow" {
			totalInflow += totalAmount
		} else if flow == "Outflow" {
			totalOutflow += totalAmount
		}
	}

	return map[string]interface{}{
		"total_records": totalRecords,
		"total_inflow":  totalInflow,
		"total_outflow": totalOutflow,
		"net_amount":    totalInflow - totalOutflow,
	}
}
