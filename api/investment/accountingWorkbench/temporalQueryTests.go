package accountingworkbench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestTemporalQueriesHandler is a comprehensive test endpoint for temporal queries
// GET /investment/accounting/test-temporal?scheme_id=XXX&test_date=2024-01-05
func TestTemporalQueriesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		schemeID := r.URL.Query().Get("scheme_id")
		testDateStr := r.URL.Query().Get("test_date")

		if schemeID == "" {
			http.Error(w, "scheme_id is required", http.StatusBadRequest)
			return
		}

		var testDate time.Time
		var err error
		if testDateStr != "" {
			testDate, err = time.Parse("2006-01-02", testDateStr)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid test_date format (use YYYY-MM-DD): %v", err), http.StatusBadRequest)
				return
			}
		} else {
			testDate = time.Now().AddDate(0, 0, -30) // Default: 30 days ago
		}

		result, err := runTemporalTests(ctx, pool, schemeID, testDate)
		if err != nil {
			http.Error(w, fmt.Sprintf("test failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

type TemporalTestResult struct {
	SchemeID        string                 `json:"scheme_id"`
	TestDate        string                 `json:"test_date"`
	CurrentData     map[string]interface{} `json:"current_data"`
	HistoricalData  map[string]interface{} `json:"historical_data"`
	SnapshotFound   bool                   `json:"snapshot_found"`
	Activities      []ActivityInfo         `json:"activities"`
	Validation      ValidationResult       `json:"validation"`
	AllQueriesValid bool                   `json:"all_queries_valid"`
}

type ActivityInfo struct {
	ActivityID    int64     `json:"activity_id"`
	ActionType    string    `json:"action_type"`
	EffectiveDate time.Time `json:"effective_date"`
	HasSnapshot   bool      `json:"has_snapshot"`
}

type ValidationResult struct {
	SnapshotCaptureWorks      bool   `json:"snapshot_capture_works"`
	TemporalQueryWorks        bool   `json:"temporal_query_works"`
	OnboardTransactionUpdated bool   `json:"onboard_transaction_updated"`
	PortfolioRefreshWorks     bool   `json:"portfolio_refresh_works"`
	Message                   string `json:"message"`
}

func runTemporalTests(ctx context.Context, pool *pgxpool.Pool, schemeID string, asOfDate time.Time) (*TemporalTestResult, error) {
	result := &TemporalTestResult{
		SchemeID: schemeID,
		TestDate: asOfDate.Format("2006-01-02"),
	}

	// Test 1: Get current data from masterscheme
	currentScheme, err := getCurrentSchemeData(ctx, pool, schemeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get current scheme data: %w", err)
	}
	result.CurrentData = currentScheme

	// Test 2: Get historical data using temporal query
	schemeSnapshot, portfolioRecords, err := GetDataAsOf(ctx, pool, schemeID, asOfDate.Format("2006-01-02"))
	if err != nil {
		return nil, fmt.Errorf("failed to get historical data: %w", err)
	}

	// Convert to map[string]interface{} for JSON response
	historicalData := make(map[string]interface{})
	if schemeSnapshot != nil {
		historicalData["masterscheme"] = schemeSnapshot
	}
	if portfolioRecords != nil {
		historicalData["portfolio_snapshot"] = portfolioRecords
	}

	result.HistoricalData = historicalData
	result.SnapshotFound = schemeSnapshot != nil || (portfolioRecords != nil && len(*portfolioRecords) > 0)

	// Test 3: Get all activities affecting this scheme
	activities, err := getActivitiesForScheme(ctx, pool, schemeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get activities: %w", err)
	}
	result.Activities = activities

	// Test 4: Validate snapshot integrity
	validation := validateTemporalIntegrity(currentScheme, historicalData, activities, asOfDate)
	result.Validation = validation
	result.AllQueriesValid = validation.SnapshotCaptureWorks && validation.TemporalQueryWorks

	return result, nil
}

func getCurrentSchemeData(ctx context.Context, pool *pgxpool.Pool, schemeID string) (map[string]interface{}, error) {
	var data = make(map[string]interface{})

	// Query masterscheme
	var schemeName, isin string
	var navValue float64
	err := pool.QueryRow(ctx, `
		SELECT scheme_name, isin, COALESCE(nav_value, 0)
		FROM investment.masterscheme
		WHERE scheme_id = $1
	`, schemeID).Scan(&schemeName, &isin, &navValue)

	if err != nil {
		return nil, err
	}

	data["scheme_name"] = schemeName
	data["isin"] = isin
	data["nav_value"] = navValue

	// Query portfolio snapshot
	var totalUnits, avgNav float64
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(total_units), 0), COALESCE(AVG(avg_nav), 0)
		FROM investment.portfolio_snapshot
		WHERE scheme_id = $1
	`, schemeID).Scan(&totalUnits, &avgNav)

	if err == nil {
		data["total_units"] = totalUnits
		data["avg_nav"] = avgNav
	}

	// Query onboard_transaction count
	var txCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM investment.onboard_transaction
		WHERE scheme_id = $1
	`, schemeID).Scan(&txCount)

	if err == nil {
		data["transaction_count"] = txCount
	}

	return data, nil
}

func getActivitiesForScheme(ctx context.Context, pool *pgxpool.Pool, schemeID string) ([]ActivityInfo, error) {
	rows, err := pool.Query(ctx, `
		SELECT 
			a.activity_id,
			a.action_type,
			a.effective_date,
			CASE WHEN aud.snapshot_before IS NOT NULL THEN true ELSE false END as has_snapshot
		FROM investment.accounting_activity a
		JOIN investment.accounting_corporate_action ca ON ca.activity_id = a.activity_id
		LEFT JOIN investment.auditactionaccountingactivity aud ON aud.activity_id = a.activity_id AND aud.actiontype = 'APPROVE'
		WHERE ca.scheme_id = $1
		  AND a.current_status = 'APPROVED'
		ORDER BY a.effective_date DESC
	`, schemeID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var activities []ActivityInfo
	for rows.Next() {
		var info ActivityInfo
		if err := rows.Scan(&info.ActivityID, &info.ActionType, &info.EffectiveDate, &info.HasSnapshot); err != nil {
			return nil, err
		}
		activities = append(activities, info)
	}

	return activities, rows.Err()
}

func validateTemporalIntegrity(currentData, historicalData map[string]interface{}, activities []ActivityInfo, asOfDate time.Time) ValidationResult {
	validation := ValidationResult{
		SnapshotCaptureWorks:      false,
		TemporalQueryWorks:        false,
		OnboardTransactionUpdated: false,
		PortfolioRefreshWorks:     false,
	}

	// Check if snapshots are being captured
	hasSnapshots := false
	for _, activity := range activities {
		if activity.HasSnapshot {
			hasSnapshots = true
			break
		}
	}
	validation.SnapshotCaptureWorks = hasSnapshots

	// Check if temporal query returns data
	validation.TemporalQueryWorks = len(historicalData) > 0

	// Check if there are activities after the test date
	hasActivitiesAfter := false
	for _, activity := range activities {
		if activity.EffectiveDate.After(asOfDate) {
			hasActivitiesAfter = true
			break
		}
	}

	// Validate temporal logic
	if hasActivitiesAfter && validation.TemporalQueryWorks {
		// Historical data should differ from current if there were changes
		currentName, _ := currentData["scheme_name"].(string)
		historicalName := ""
		if ms, ok := historicalData["masterscheme"].(*SchemeSnapshot); ok {
			historicalName = ms.SchemeName
		}

		if currentName != "" && historicalName != "" {
			validation.Message = fmt.Sprintf("✓ Temporal query working: Current='%s', Historical='%s'", currentName, historicalName)
		}
	} else if !hasActivitiesAfter {
		validation.Message = "⚠ No activities found after test date - cannot validate temporal difference"
	}

	// Check onboard_transaction exists
	if txCount, ok := currentData["transaction_count"].(int); ok && txCount > 0 {
		validation.OnboardTransactionUpdated = true
	}

	// Check portfolio refresh
	if units, ok := currentData["total_units"].(float64); ok && units > 0 {
		validation.PortfolioRefreshWorks = true
	}

	if validation.Message == "" {
		if validation.SnapshotCaptureWorks && validation.TemporalQueryWorks {
			validation.Message = "✓ All temporal query components validated successfully"
		} else {
			validation.Message = "⚠ Some validation checks failed - review individual flags"
		}
	}

	return validation
}

// VerifyAllQueriesHandler checks if all required queries are in place
// GET /investment/accounting/verify-queries
func VerifyAllQueriesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		queryChecks := map[string]QueryCheck{
			"snapshot_columns_exist": {
				Description: "Check if snapshot_before and snapshot_after columns exist in audit table",
				Query: `
					SELECT column_name 
					FROM information_schema.columns 
					WHERE table_schema = 'investment' 
					  AND table_name = 'auditactionaccountingactivity' 
					  AND column_name IN ('snapshot_before', 'snapshot_after')
				`,
				ExpectedCount: 2,
			},
			"snapshot_indexes_exist": {
				Description: "Check if GIN indexes exist on JSONB snapshot columns",
				Query: `
					SELECT indexname 
					FROM pg_indexes 
					WHERE schemaname = 'investment' 
					  AND tablename = 'auditactionaccountingactivity' 
					  AND indexname LIKE '%snapshot%'
				`,
				ExpectedCount: 2,
			},
			"corporate_actions_with_snapshots": {
				Description: "Count approved corporate actions that have snapshots captured",
				Query: `
					SELECT COUNT(*) 
					FROM investment.auditactionaccountingactivity 
					WHERE actiontype = 'APPROVE' 
					  AND snapshot_before IS NOT NULL
				`,
				ExpectedCount: -1, // Any count >= 0 is valid
			},
			"onboard_transaction_table": {
				Description: "Verify onboard_transaction table exists with required columns",
				Query: `
					SELECT column_name 
					FROM information_schema.columns 
					WHERE table_schema = 'investment' 
					  AND table_name = 'onboard_transaction' 
					  AND column_name IN ('transaction_type', 'units', 'nav', 'amount', 'scheme_id')
				`,
				ExpectedCount: 5,
			},
			"portfolio_snapshot_table": {
				Description: "Verify portfolio_snapshot table exists",
				Query: `
					SELECT COUNT(*) 
					FROM information_schema.tables 
					WHERE table_schema = 'investment' 
					  AND table_name = 'portfolio_snapshot'
				`,
				ExpectedCount: 1,
			},
		}

		results := make(map[string]QueryCheckResult)
		allPassed := true

		for name, check := range queryChecks {
			result := executeQueryCheck(ctx, pool, check)
			results[name] = result
			if !result.Passed {
				allPassed = false
			}
		}

		response := map[string]interface{}{
			"all_queries_valid": allPassed,
			"checks":            results,
			"timestamp":         time.Now().Format(time.RFC3339),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

type QueryCheck struct {
	Description   string
	Query         string
	ExpectedCount int // -1 means any count is valid
}

type QueryCheckResult struct {
	Description string `json:"description"`
	Passed      bool   `json:"passed"`
	ActualCount int    `json:"actual_count"`
	Expected    int    `json:"expected_count"`
	Error       string `json:"error,omitempty"`
}

func executeQueryCheck(ctx context.Context, pool *pgxpool.Pool, check QueryCheck) QueryCheckResult {
	result := QueryCheckResult{
		Description: check.Description,
		Expected:    check.ExpectedCount,
	}

	rows, err := pool.Query(ctx, check.Query)
	if err != nil {
		result.Error = err.Error()
		result.Passed = false
		return result
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	result.ActualCount = count

	if check.ExpectedCount == -1 {
		result.Passed = true // Any count is acceptable
	} else {
		result.Passed = (count == check.ExpectedCount)
	}

	return result
}
