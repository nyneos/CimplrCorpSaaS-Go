package sweepconfig

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ===================================================================================
// SWEEP SIMULATION & ANALYSIS SYSTEM V2
// Comprehensive pre-execution sweep analyzer for CFO-level decision making
// ===================================================================================

// SimulationRequest holds input parameters for sweep simulation
type SimulationRequest struct {
	UserID             string   `json:"user_id"`                       // Required: User ID for session validation
	EntityIDs          []string `json:"entity_ids,omitempty"`          // Optional filter: Specific entities
	BankNames          []string `json:"bank_names,omitempty"`          // Optional filter: Specific banks
	Frequency          string   `json:"frequency,omitempty"`           // Optional filter: DAILY, MONTHLY, SPECIFIC_DATE
	EffectiveDate      string   `json:"effective_date,omitempty"`      // Optional: Date to simulate (defaults to today)
	ExecutionTime      string   `json:"execution_time,omitempty"`      // Optional: Time of day (HH:MM format)
	SweepIDs           []string `json:"sweep_ids,omitempty"`           // Optional: Specific sweep IDs to simulate
	IncludeChains      bool     `json:"include_chains,omitempty"`      // Optional: Detect multi-hop sweep chains
	IncludeSuggestions bool     `json:"include_suggestions,omitempty"` // Optional: AI-powered optimization suggestions
}

// SweepExecutionStep represents a single sweep in the execution sequence
type SweepExecutionStep struct {
	Step             int                    `json:"step"`
	Time             string                 `json:"time"`
	SweepID          string                 `json:"sweep_id"`
	InitiationID     string                 `json:"initiation_id,omitempty"`
	FromAccount      string                 `json:"from_account"`
	FromBank         string                 `json:"from_bank"`
	FromEntity       string                 `json:"from_entity"`
	ToAccount        string                 `json:"to_account"`
	ToBank           string                 `json:"to_bank"`
	ToEntity         string                 `json:"to_entity"`
	SweepType        string                 `json:"sweep_type"`
	BufferAmount     float64                `json:"buffer_amount"`
	AvailableBalance float64                `json:"available_balance"`
	RequestedAmount  float64                `json:"requested_amount"`
	ValidatedAmount  float64                `json:"validated_amount"`
	Violations       []string               `json:"violations"`
	Penalties        []string               `json:"penalties"`
	ExecutionStatus  string                 `json:"execution_status"` // APPROVED, BLOCKED, PARTIAL
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// KPIAccountBreakdown provides drill-down data per account
type KPIAccountBreakdown struct {
	AccountNumber  string  `json:"account_number"`
	AccountName    string  `json:"account_name,omitempty"`
	BankName       string  `json:"bank_name"`
	EntityName     string  `json:"entity_name"`
	Role           string  `json:"role"`            // SOURCE, DESTINATION, BOTH
	OpeningBalance float64 `json:"opening_balance"` // Initial balance
	ClosingBalance float64 `json:"closing_balance"` // Balance after simulation
	NetChange      float64 `json:"net_change"`      // Closing - Opening
}

// KPIMetrics holds key performance indicators
type KPIMetrics struct {
	GrossOpeningBalance        float64               `json:"gross_opening_balance"`        // Sum of balances ONLY for accounts in sweeps
	TotalPlannedOutflow        float64               `json:"total_planned_outflow"`        // Total amount that would sweep if no violations
	MasterConcentrationInitial float64               `json:"master_concentration_initial"` // Initial balance of end-node accounts
	MasterConcentrationFinal   float64               `json:"master_concentration_final"`   // Final balance of end-node accounts after simulation
	ActualPosition             float64               `json:"actual_position"`              // Same as master_concentration_final
	ProjectedPosition          float64               `json:"projected_position"`           // master_concentration_initial + total_planned_outflow
	ConcentrationEfficiency    float64               `json:"concentration_efficiency"`     // (actual_transferred / total_planned_outflow) * 100
	IdleCashReduction          float64               `json:"idle_cash_reduction"`          // % reduction in idle cash in source accounts
	BufferUtilizationRate      float64               `json:"buffer_utilization_rate"`      // Average buffer usage
	SuccessRate                float64               `json:"success_rate"`                 // % of sweeps executed
	AccountBreakdown           []KPIAccountBreakdown `json:"account_breakdown"`            // Drillable account-level data
}

// BalanceImpact shows before/after balance positions
type BalanceImpact struct {
	Before              []AccountBalance `json:"before"`
	After               []AccountBalance `json:"after"`
	MasterAccountGrowth float64          `json:"master_account_growth"`
}

type AccountBalance struct {
	AccountNumber string  `json:"account_number"`
	BankName      string  `json:"bank_name"`
	EntityName    string  `json:"entity_name"`
	Balance       float64 `json:"balance"`
	Currency      string  `json:"currency"`
}

// Violation represents a rule violation
type Violation struct {
	SweepID     string `json:"sweep_id"`
	Account     string `json:"account"`
	Type        string `json:"type"`     // BUFFER_BREACH, INSUFFICIENT_FUNDS, TIMING_CONFLICT, CIRCULAR_DEPENDENCY
	Severity    string `json:"severity"` // HIGH, MEDIUM, LOW
	Message     string `json:"message"`
	PenaltyRisk string `json:"penalty_risk,omitempty"`
}

// Suggestion represents an optimization recommendation
type Suggestion struct {
	Type             string                 `json:"type"`     // OPTIMIZE_SEQUENCE, CREATE_NEW_SWEEP, MODIFY_BUFFER, CHANGE_TIMING
	Priority         string                 `json:"priority"` // HIGH, MEDIUM, LOW
	Description      string                 `json:"description"`
	EstimatedBenefit string                 `json:"estimated_benefit"`
	SuggestedConfig  map[string]interface{} `json:"suggested_config,omitempty"`
}

// TimingAnalysis provides execution timing insights
type TimingAnalysis struct {
	TotalExecutionWindow    string   `json:"total_execution_window"`    // Time from earliest to latest sweep (now - last_datetime)
	EarliestExecution       string   `json:"earliest_execution"`        // First sweep datetime
	LatestExecution         string   `json:"latest_execution"`          // Last sweep datetime
	TotalExecutionTime      string   `json:"total_execution_time"`      // Deprecated: use total_execution_window
	CriticalPath            []string `json:"critical_path"`             // Sweep IDs on critical path
	ParallelOpportunities   int      `json:"parallel_opportunities"`    // Number of parallel execution opportunities
	EstimatedCompletionTime string   `json:"estimated_completion_time"` // Deprecated: use latest_execution
}

// SimulationResponse is the complete simulation output
type SimulationResponse struct {
	SimulationID   string               `json:"simulation_id"`
	ExecutionPlan  []SweepExecutionStep `json:"execution_plan"`
	KPIs           KPIMetrics           `json:"kpis"`
	BalanceImpact  BalanceImpact        `json:"balance_impact"`
	Violations     []Violation          `json:"violations"`
	Suggestions    []Suggestion         `json:"suggestions,omitempty"`
	TimingAnalysis TimingAnalysis       `json:"timing_analysis"`
	SweepChains    []SweepChain         `json:"sweep_chains,omitempty"`
	GeneratedAt    string               `json:"generated_at"`
}

// SweepChain represents a multi-hop sweep flow (A→B→C→D)
type SweepChain struct {
	ChainID     string   `json:"chain_id"`
	Hops        []string `json:"hops"`         // Account numbers in sequence
	TotalAmount float64  `json:"total_amount"` // Amount flowing through chain
	IsCircular  bool     `json:"is_circular"`  // Detects A→B→C→A circular flows
}

// ===================================================================================
// HANDLER 1: CORE SWEEP SIMULATION
// ===================================================================================

// SimulateSweepExecution is the main simulation engine
func SimulateSweepExecution(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req SimulationRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		// Validate session
		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIIsRequired)
			return
		}

		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrUnauthorized)
			return
		}

		// Set defaults for optional fields
		if req.EffectiveDate == "" {
			req.EffectiveDate = time.Now().Format(constants.DateFormat) // Default to today
		}
		if req.ExecutionTime == "" {
			req.ExecutionTime = "18:00" // Default to 6 PM
		}

		log.Printf("[SWEEP SIMULATION] User: %s, Date: %s, Time: %s, Frequency: %s, SweepIDs: %v",
			req.UserID, req.EffectiveDate, req.ExecutionTime, req.Frequency, req.SweepIDs)

		// Get middleware-filtered entities and banks
		entityNames := api.GetEntityNamesFromCtx(ctx)
		bankNames := api.GetBankNamesFromCtx(ctx)

		// Override if user provided specific filters
		if len(req.EntityIDs) > 0 {
			entityNames = req.EntityIDs
		}
		if len(req.BankNames) > 0 {
			bankNames = req.BankNames
		}

		// Fetch approved sweeps
		sweeps, err := fetchApprovedSweepsForSimulation(ctx, pgxPool, req, entityNames, bankNames)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch sweeps: "+err.Error())
			return
		}

		if len(sweeps) == 0 {
			api.RespondWithResult(w, false, "No sweeps found for simulation (check is_deleted status or filters)")
			return
		}

		log.Printf("[SWEEP SIMULATION] Found %d approved sweeps to simulate", len(sweeps))

		// Fetch current balances
		balances, err := fetchCurrentBalances(ctx, pgxPool, entityNames, bankNames)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch balances: "+err.Error())
			return
		}

		// Run simulation
		simulation := runSweepSimulation(sweeps, balances, req)

		// Detect sweep chains if requested
		if req.IncludeChains {
			simulation.SweepChains = detectSweepChains(sweeps)
		}

		// Generate suggestions if requested
		if req.IncludeSuggestions {
			simulation.Suggestions = generateOptimizationSuggestions(sweeps, balances, simulation)
		}

		simulation.SimulationID = fmt.Sprintf("SIM-%s-%d", time.Now().Format("20060102"), time.Now().Unix())
		simulation.GeneratedAt = time.Now().Format(time.RFC3339)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    simulation,
		})
	}
}

// ===================================================================================
// HANDLER 2: BALANCE SNAPSHOT
// ===================================================================================

// GetBalanceSnapshot returns real-time balances for simulation
func GetBalanceSnapshot(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIIsRequired)
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
			api.RespondWithResult(w, false, constants.ErrUnauthorized)
			return
		}

		entityNames := api.GetEntityNamesFromCtx(ctx)
		bankNames := api.GetBankNamesFromCtx(ctx)

		balances, err := fetchCurrentBalances(ctx, pgxPool, entityNames, bankNames)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch balances: "+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"balances": balances,
			"count":    len(balances),
		})
	}
}

// ===================================================================================
// HANDLER 3: VALIDATION ENGINE
// ===================================================================================

// ValidateSweepConfiguration validates sweeps without executing
func ValidateSweepConfiguration(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			SweepIDs []string `json:"sweep_ids"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIIsRequired)
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
			api.RespondWithResult(w, false, constants.ErrUnauthorized)
			return
		}

		// Get middleware-filtered context (for future enhancements)
		_ = api.GetEntityNamesFromCtx(ctx)
		_ = api.GetBankNamesFromCtx(ctx)

		// Build sweep ID filter
		sweepFilter := ""
		sweepArgs := []interface{}{}
		argPos := 1

		if len(req.SweepIDs) > 0 {
			sweepFilter = fmt.Sprintf(" AND sc.sweep_id = ANY($%d)", argPos)
			sweepArgs = append(sweepArgs, req.SweepIDs)
		}

		// Fetch sweeps to validate
		query := fmt.Sprintf(`
			SELECT DISTINCT ON (sc.sweep_id)
				sc.sweep_id,
				sc.entity_name,
				sc.source_bank_account,
				sc.source_bank_name,
				sc.target_bank_account,
				sc.target_bank_name,
				sc.sweep_type,
				sc.buffer_amount,
				sc.sweep_amount
			FROM cimplrcorpsaas.sweepconfiguration sc
			JOIN cimplrcorpsaas.auditactionsweepconfiguration a 
				ON a.sweep_id = sc.sweep_id
			WHERE a.processing_status = 'APPROVED'
				AND COALESCE(sc.is_deleted, false) = false
				%s
			ORDER BY sc.sweep_id, a.requested_at DESC
		`, sweepFilter)

		rows, err := pgxPool.Query(ctx, query, sweepArgs...)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch sweeps: "+err.Error())
			return
		}
		defer rows.Close()

		violations := []Violation{}
		sweepCount := 0

		for rows.Next() {
			var sweepID, entityName, sourceAcc, sourceBank, targetAcc, targetBank, sweepType string
			var bufferAmt, sweepAmt *float64

			if err := rows.Scan(&sweepID, &entityName, &sourceAcc, &sourceBank, &targetAcc, &targetBank, &sweepType, &bufferAmt, &sweepAmt); err != nil {
				continue
			}

			sweepCount++

			// Validation 1: Check buffer amount
			if bufferAmt != nil && *bufferAmt < 0 {
				violations = append(violations, Violation{
					SweepID:     sweepID,
					Account:     sourceAcc,
					Type:        "INVALID_BUFFER",
					Severity:    "HIGH",
					Message:     fmt.Sprintf("Buffer amount cannot be negative: %.2f", *bufferAmt),
					PenaltyRisk: "Configuration error",
				})
			}

			// Validation 2: Check circular dependency (same source and target)
			if sourceAcc == targetAcc {
				violations = append(violations, Violation{
					SweepID:     sweepID,
					Account:     sourceAcc,
					Type:        "CIRCULAR_DEPENDENCY",
					Severity:    "HIGH",
					Message:     "Source and target accounts are identical",
					PenaltyRisk: "Sweep will fail",
				})
			}

			// Validation 3: Check sweep type validity
			sweepTypeUpper := strings.ToUpper(strings.TrimSpace(sweepType))
			if sweepTypeUpper != "ZBA" && sweepTypeUpper != "CONCENTRATION" && sweepTypeUpper != "TARGET_BALANCE" {
				violations = append(violations, Violation{
					SweepID:     sweepID,
					Account:     sourceAcc,
					Type:        "INVALID_SWEEP_TYPE",
					Severity:    "HIGH",
					Message:     fmt.Sprintf("Invalid sweep type: %s", sweepType),
					PenaltyRisk: "Configuration error",
				})
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"sweeps_checked": sweepCount,
			"violations":     violations,
			"is_valid":       len(violations) == 0,
		})
	}
}

// ===================================================================================
// HANDLER 4: KPI ANALYTICS
// ===================================================================================

// GetSweepAnalytics provides deep CFO-level insights
func GetSweepAnalytics(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string `json:"user_id"`
			DateFrom string `json:"date_from,omitempty"`
			DateTo   string `json:"date_to,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIIsRequired)
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
			api.RespondWithResult(w, false, constants.ErrUnauthorized)
			return
		}

		// Default to last 30 days if not provided
		if req.DateFrom == "" {
			req.DateFrom = time.Now().AddDate(0, 0, -30).Format(constants.DateFormat)
		}
		if req.DateTo == "" {
			req.DateTo = time.Now().Format(constants.DateFormat)
		}

		entityNames := api.GetEntityNamesFromCtx(ctx)

		// Build entity filter
		entityFilter := ""
		entityArgs := []interface{}{req.DateFrom, req.DateTo}
		if len(entityNames) > 0 {
			normEntities := make([]string, 0, len(entityNames))
			for _, n := range entityNames {
				if s := strings.TrimSpace(n); s != "" {
					normEntities = append(normEntities, strings.ToLower(s))
				}
			}
			if len(normEntities) > 0 {
				entityFilter = " AND LOWER(TRIM(sc.entity_name)) = ANY($3)"
				entityArgs = append(entityArgs, normEntities)
			}
		}

		// Fetch execution history
		query := fmt.Sprintf(`
			SELECT 
				COUNT(*) as total_executions,
				COUNT(CASE WHEN l.status = 'SUCCESS' THEN 1 END) as successful,
				COUNT(CASE WHEN l.status = 'FAILED' THEN 1 END) as failed,
				COALESCE(SUM(l.amount_swept), 0) as total_swept,
				COALESCE(AVG(l.amount_swept), 0) as avg_swept
			FROM cimplrcorpsaas.sweep_execution_log l
			JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = l.sweep_id
			WHERE l.execution_date >= $1 AND l.execution_date <= $2
				%s
		`, entityFilter)

		var totalExec, successful, failed int
		var totalSwept, avgSwept float64

		err := pgxPool.QueryRow(ctx, query, entityArgs...).Scan(&totalExec, &successful, &failed, &totalSwept, &avgSwept)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch analytics: "+err.Error())
			return
		}

		successRate := 0.0
		if totalExec > 0 {
			successRate = (float64(successful) / float64(totalExec)) * 100
		}

		analytics := map[string]interface{}{
			"concentration_metrics": map[string]interface{}{
				"total_swept":           totalSwept,
				"avg_sweep_amount":      avgSwept,
				"total_executions":      totalExec,
				"successful_executions": successful,
				"failed_executions":     failed,
			},
			"efficiency_scores": map[string]interface{}{
				"sweep_success_rate":    successRate,
				"execution_reliability": successRate,
			},
			"period": map[string]interface{}{
				"from": req.DateFrom,
				"to":   req.DateTo,
			},
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    analytics,
		})
	}
}

// ===================================================================================
// HANDLER 5: SUGGESTION ENGINE
// ===================================================================================

// GetSweepSuggestions provides AI-powered optimization recommendations
func GetSweepSuggestions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIIsRequired)
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
			api.RespondWithResult(w, false, constants.ErrUnauthorized)
			return
		}

		entityNames := api.GetEntityNamesFromCtx(ctx)
		bankNames := api.GetBankNamesFromCtx(ctx)

		// Fetch current sweeps and balances
		balances, _ := fetchCurrentBalances(ctx, pgxPool, entityNames, bankNames)

		// Generate basic suggestions
		suggestions := []Suggestion{}

		// Suggestion 1: Identify accounts with high idle cash
		for _, bal := range balances {
			if bal.Balance > 5000000 { // > 5M idle
				suggestions = append(suggestions, Suggestion{
					Type:             "CREATE_NEW_SWEEP",
					Priority:         "HIGH",
					Description:      fmt.Sprintf("High idle cash detected in %s (%s)", bal.AccountNumber, bal.BankName),
					EstimatedBenefit: "Potential interest savings: ₹50K-100K monthly",
					SuggestedConfig: map[string]interface{}{
						"from_account": bal.AccountNumber,
						"sweep_type":   "CONCENTRATION",
						"buffer":       1000000,
						"frequency":    "DAILY",
					},
				})
			}
		}

		// Suggestion 2: Buffer optimization
		suggestions = append(suggestions, Suggestion{
			Type:             "OPTIMIZE_BUFFER",
			Priority:         "MEDIUM",
			Description:      "Review buffer amounts across all sweeps for optimal liquidity",
			EstimatedBenefit: "Unlock up to 15% more working capital",
		})

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":     true,
			"suggestions": suggestions,
			"count":       len(suggestions),
		})
	}
}

// ===================================================================================
// HANDLER 6: EXECUTION GRAPH DATA
// ===================================================================================

// GetSweepExecutionGraph returns visual flow diagram data
func GetSweepExecutionGraph(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIIsRequired)
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
			api.RespondWithResult(w, false, constants.ErrUnauthorized)
			return
		}

		entityNames := api.GetEntityNamesFromCtx(ctx)

		// Build entity filter
		entityFilter := ""
		entityArgs := []interface{}{}
		if len(entityNames) > 0 {
			normEntities := make([]string, 0, len(entityNames))
			for _, n := range entityNames {
				if s := strings.TrimSpace(n); s != "" {
					normEntities = append(normEntities, strings.ToLower(s))
				}
			}
			if len(normEntities) > 0 {
				entityFilter = " AND LOWER(TRIM(sc.entity_name)) = ANY($1)"
				entityArgs = append(entityArgs, normEntities)
			}
		}

		// Fetch sweep relationships
		query := fmt.Sprintf(`
			SELECT DISTINCT
				sc.sweep_id,
				sc.source_bank_account,
				sc.source_bank_name,
				sc.target_bank_account,
				sc.target_bank_name,
				sc.sweep_type,
				sc.sweep_amount
			FROM cimplrcorpsaas.sweepconfiguration sc
			JOIN cimplrcorpsaas.auditactionsweepconfiguration a 
				ON a.sweep_id = sc.sweep_id
			WHERE a.processing_status = 'APPROVED'
				AND COALESCE(sc.is_deleted, false) = false
				%s
			ORDER BY sc.sweep_id
		`, entityFilter)

		rows, err := pgxPool.Query(ctx, query, entityArgs...)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch graph data: "+err.Error())
			return
		}
		defer rows.Close()

		nodes := make(map[string]map[string]interface{})
		edges := []map[string]interface{}{}

		for rows.Next() {
			var sweepID, sourceAcc, sourceBank, targetAcc, targetBank, sweepType string
			var sweepAmt *float64

			if err := rows.Scan(&sweepID, &sourceAcc, &sourceBank, &targetAcc, &targetBank, &sweepType, &sweepAmt); err != nil {
				continue
			}

			// Add source node
			if _, exists := nodes[sourceAcc]; !exists {
				nodes[sourceAcc] = map[string]interface{}{
					"id":        sourceAcc,
					"label":     sourceAcc,
					"bank":      sourceBank,
					"node_type": "source",
				}
			}

			// Add target node
			if _, exists := nodes[targetAcc]; !exists {
				nodes[targetAcc] = map[string]interface{}{
					"id":        targetAcc,
					"label":     targetAcc,
					"bank":      targetBank,
					"node_type": "target",
				}
			}

			// Add edge
			amount := 0.0
			if sweepAmt != nil {
				amount = *sweepAmt
			}

			edges = append(edges, map[string]interface{}{
				"from":       sourceAcc,
				"to":         targetAcc,
				"sweep_id":   sweepID,
				"sweep_type": sweepType,
				"amount":     amount,
			})
		}

		// Convert nodes map to array
		nodeArray := make([]map[string]interface{}, 0, len(nodes))
		for _, node := range nodes {
			nodeArray = append(nodeArray, node)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"graph": map[string]interface{}{
				"nodes": nodeArray,
				"edges": edges,
			},
		})
	}
}

// ===================================================================================
// HELPER FUNCTIONS
// ===================================================================================

// fetchApprovedSweepsForSimulation retrieves approved sweeps for simulation
func fetchApprovedSweepsForSimulation(ctx context.Context, pgxPool *pgxpool.Pool, req SimulationRequest, entityNames, bankNames []string) ([]map[string]interface{}, error) {
	// Build filters
	sweepFilter := ""
	args := []interface{}{}
	argPos := 1

	if len(req.SweepIDs) > 0 {
		sweepFilter += fmt.Sprintf(" AND sc.sweep_id = ANY($%d)", argPos)
		args = append(args, req.SweepIDs)
		argPos++
	}

	if len(entityNames) > 0 {
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}
		if len(normEntities) > 0 {
			sweepFilter += fmt.Sprintf(" AND LOWER(TRIM(sc.entity_name)) = ANY($%d)", argPos)
			args = append(args, normEntities)
			argPos++
		}
	}

	if len(bankNames) > 0 {
		normBanks := make([]string, 0, len(bankNames))
		for _, b := range bankNames {
			if s := strings.TrimSpace(b); s != "" {
				normBanks = append(normBanks, strings.ToLower(s))
			}
		}
		if len(normBanks) > 0 {
			sweepFilter += fmt.Sprintf(" AND (LOWER(TRIM(sc.source_bank_name)) = ANY($%d) OR LOWER(TRIM(sc.target_bank_name)) = ANY($%d))", argPos, argPos)
			args = append(args, normBanks)
			argPos++
		}
	}

	// Frequency filter: only if provided
	frequencyFilter := ""
	if req.Frequency != "" {
		frequencyFilter = fmt.Sprintf(" AND UPPER(TRIM(sc.frequency)) = $%d", argPos)
		args = append(args, strings.ToUpper(strings.TrimSpace(req.Frequency)))
		argPos++
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT ON (sc.sweep_id)
			sc.sweep_id,
			sc.entity_name,
			sc.source_bank_account,
			sc.source_bank_name,
			sc.target_bank_account,
			sc.target_bank_name,
			sc.sweep_type,
			sc.frequency,
			sc.effective_date,
			sc.execution_time,
			sc.buffer_amount,
			sc.sweep_amount,
			sc.requires_initiation
		FROM cimplrcorpsaas.sweepconfiguration sc
		LEFT JOIN cimplrcorpsaas.auditactionsweepconfiguration a ON a.sweep_id = sc.sweep_id
		WHERE COALESCE(sc.is_deleted, false) = false
			%s
			%s
		ORDER BY sc.sweep_id, COALESCE(a.requested_at, now()) DESC
	`, frequencyFilter, sweepFilter)

	rows, err := pgxPool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sweeps := []map[string]interface{}{}
	for rows.Next() {
		var sweepID, entityName, sourceAcc, sourceBank, targetAcc, targetBank, sweepType, frequency, executionTime string
		var effectiveDate sql.NullTime
		var bufferAmt, sweepAmt *float64
		var requiresInitiation bool

		if err := rows.Scan(&sweepID, &entityName, &sourceAcc, &sourceBank, &targetAcc, &targetBank, &sweepType, &frequency, &effectiveDate, &executionTime, &bufferAmt, &sweepAmt, &requiresInitiation); err != nil {
			log.Printf("[ERROR] Sweep scan error: %v", err)
			continue
		}

		// Format effective_date as string
		var effectiveDateStr *string
		if effectiveDate.Valid {
			formatted := effectiveDate.Time.Format(constants.DateFormat)
			effectiveDateStr = &formatted
		}

		sweeps = append(sweeps, map[string]interface{}{
			"sweep_id":            sweepID,
			"entity_name":         entityName,
			"source_account":      sourceAcc,
			"source_bank":         sourceBank,
			"target_account":      targetAcc,
			"target_bank":         targetBank,
			"sweep_type":          sweepType,
			"frequency":           frequency,
			"effective_date":      effectiveDateStr,
			"execution_time":      executionTime,
			"buffer_amount":       bufferAmt,
			"sweep_amount":        sweepAmt,
			"requires_initiation": requiresInitiation,
		})
	}

	return sweeps, nil
}

// fetchCurrentBalances retrieves latest balances
func fetchCurrentBalances(ctx context.Context, pgxPool *pgxpool.Pool, entityNames, bankNames []string) ([]AccountBalance, error) {
	// Build filters
	filters := ""
	args := []interface{}{}
	argPos := 1

	if len(entityNames) > 0 {
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}
		// if len(normEntities) > 0 {
		// 	filters += fmt.Sprintf(" AND LOWER(TRIM(mba.entity_id)) = ANY($%d)", argPos)
		// 	args = append(args, normEntities)
		// 	argPos++
		// }
	}

	if len(bankNames) > 0 {
		normBanks := make([]string, 0, len(bankNames))
		for _, b := range bankNames {
			if s := strings.TrimSpace(b); s != "" {
				normBanks = append(normBanks, strings.ToLower(s))
			}
		}
		if len(normBanks) > 0 {
			filters += fmt.Sprintf(" AND LOWER(TRIM(mba.bank_name)) = ANY($%d)", argPos)
			args = append(args, normBanks)
		}
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT ON (bb.account_no)
			bb.account_no,
			mba.bank_name,
			mba.entity_id,
			COALESCE(bb.closing_balance, 0) as balance,
			COALESCE(mba.currency, 'INR') as currency
		FROM public.bank_balances_manual bb
		JOIN public.masterbankaccount mba ON mba.account_number = bb.account_no
		LEFT JOIN public.auditactionbankbalances audit ON audit.balance_id = bb.balance_id
		WHERE COALESCE(mba.is_deleted, false) = false
			AND (audit.processing_status = 'APPROVED' OR audit.processing_status IS NULL)
			%s
		ORDER BY bb.account_no, bb.as_of_date DESC, bb.as_of_time DESC
	`, filters)

	log.Printf("[BALANCE FETCH] Query: %s", query)
	log.Printf("[BALANCE FETCH] Args: %v", args)
	log.Printf("[BALANCE FETCH] EntityNames: %v", entityNames)
	log.Printf("[BALANCE FETCH] BankNames: %v", bankNames)

	rows, err := pgxPool.Query(ctx, query, args...)
	if err != nil {
		log.Printf("[BALANCE FETCH ERROR] %v", err)
		return nil, err
	}
	defer rows.Close()

	balances := []AccountBalance{}
	rowCount := 0
	for rows.Next() {
		rowCount++
		var accountNo, bankName, entityID, currency string
		var balance float64

		if err := rows.Scan(&accountNo, &bankName, &entityID, &balance, &currency); err != nil {
			log.Printf("[BALANCE FETCH] Scan error on row %d: %v", rowCount, err)
			continue
		}

		log.Printf("[BALANCE FETCH] Found balance: Account=%s, Bank=%s, Entity=%s, Balance=%.2f", accountNo, bankName, entityID, balance)

		balances = append(balances, AccountBalance{
			AccountNumber: accountNo,
			BankName:      bankName,
			EntityName:    entityID,
			Balance:       balance,
			Currency:      currency,
		})
	}

	log.Printf("[BALANCE FETCH] Total rows: %d, Balances collected: %d", rowCount, len(balances))

	return balances, nil
}

// runSweepSimulation executes the core simulation logic
func runSweepSimulation(sweeps []map[string]interface{}, balances []AccountBalance, req SimulationRequest) SimulationResponse {
	// Create balance lookup map and track initial balances
	balanceMap := make(map[string]float64)
	initialBalanceMap := make(map[string]float64)
	accountInfoMap := make(map[string]AccountBalance)
	for _, bal := range balances {
		balanceMap[bal.AccountNumber] = bal.Balance
		initialBalanceMap[bal.AccountNumber] = bal.Balance
		accountInfoMap[bal.AccountNumber] = bal
	}

	// Track accounts involved in sweeps and their roles
	accountsInvolved := make(map[string]string) // account -> role (SOURCE, DESTINATION, BOTH)

	// Sort sweeps by effective_date + execution_time (FIFO queue - earliest first)
	sort.Slice(sweeps, func(i, j int) bool {
		dateI := ""
		timeI := "18:00" // default
		if sweeps[i]["effective_date"] != nil {
			if datePtr, ok := sweeps[i]["effective_date"].(*string); ok && datePtr != nil {
				dateI = *datePtr
			}
		}
		if sweeps[i]["execution_time"] != nil {
			if timeStr, ok := sweeps[i]["execution_time"].(string); ok {
				timeI = timeStr
			}
		}
		datetimeI := dateI + " " + timeI

		dateJ := ""
		timeJ := "18:00"
		if sweeps[j]["effective_date"] != nil {
			if datePtr, ok := sweeps[j]["effective_date"].(*string); ok && datePtr != nil {
				dateJ = *datePtr
			}
		}
		if sweeps[j]["execution_time"] != nil {
			if timeStr, ok := sweeps[j]["execution_time"].(string); ok {
				timeJ = timeStr
			}
		}
		datetimeJ := dateJ + " " + timeJ

		return datetimeI < datetimeJ // FIFO: earliest datetime first
	})

	executionPlan := []SweepExecutionStep{}
	violations := []Violation{}

	totalPlannedOutflow := 0.0 // Amount that would transfer if no violations
	actualTransferred := 0.0   // Amount actually transferred in simulation
	masterConcentration := 0.0
	successCount := 0

	// Track destination accounts (end nodes)
	destinationAccounts := make(map[string]bool)
	sourceAccounts := make(map[string]bool)

	// First pass: identify all source and destination accounts
	for _, sweep := range sweeps {
		sourceAcc := fmt.Sprint(sweep["source_account"])
		targetAcc := fmt.Sprint(sweep["target_account"])
		sourceAccounts[sourceAcc] = true
		destinationAccounts[targetAcc] = true

		// Mark roles
		if _, exists := accountsInvolved[sourceAcc]; exists {
			if accountsInvolved[sourceAcc] == "DESTINATION" {
				accountsInvolved[sourceAcc] = "BOTH"
			}
		} else {
			accountsInvolved[sourceAcc] = "SOURCE"
		}

		if _, exists := accountsInvolved[targetAcc]; exists {
			if accountsInvolved[targetAcc] == "SOURCE" {
				accountsInvolved[targetAcc] = "BOTH"
			}
		} else {
			accountsInvolved[targetAcc] = "DESTINATION"
		}
	}

	// Calculate gross opening balance (ONLY accounts involved in sweeps)
	grossOpening := 0.0
	for acc := range accountsInvolved {
		if bal, exists := initialBalanceMap[acc]; exists {
			grossOpening += bal
		}
	}

	// Calculate master concentration initial (ALL destination accounts)
	masterConcentrationInitial := 0.0
	for acc := range destinationAccounts {
		if bal, exists := initialBalanceMap[acc]; exists {
			masterConcentrationInitial += bal
		}
	}

	// Simulate each sweep in chronological order
	for i, sweep := range sweeps {
		sweepID := fmt.Sprint(sweep["sweep_id"])
		sourceAcc := fmt.Sprint(sweep["source_account"])
		targetAcc := fmt.Sprint(sweep["target_account"])
		sweepType := fmt.Sprint(sweep["sweep_type"])

		bufferAmt := 0.0
		if sweep["buffer_amount"] != nil {
			if val, ok := sweep["buffer_amount"].(*float64); ok && val != nil {
				bufferAmt = *val
			}
		}

		sweepAmt := 0.0
		if sweep["sweep_amount"] != nil {
			if val, ok := sweep["sweep_amount"].(*float64); ok && val != nil {
				sweepAmt = *val
			}
		}

		availableBalance := balanceMap[sourceAcc]

		// If sweep_amount is not defined (NULL or 0), calculate based on sweep type
		requestedAmount := sweepAmt
		if sweepAmt == 0 {
			sweepTypeUpper := strings.ToUpper(strings.TrimSpace(sweepType))
			if sweepTypeUpper == "ZBA" || sweepTypeUpper == "CONCENTRATION" {
				// ZBA/CONCENTRATION: Sweep everything above buffer
				if availableBalance > bufferAmt {
					requestedAmount = availableBalance - bufferAmt
				} else {
					requestedAmount = 0
				}
			}
			// For TARGET_BALANCE, we'd need target amount (not implemented yet)
		}

		validatedAmount := requestedAmount
		status := "APPROVED"
		stepViolations := []string{}

		// This is the planned amount (what SHOULD transfer if no violations)
		totalPlannedOutflow += requestedAmount

		// Validation: Buffer check
		if availableBalance <= bufferAmt {
			violations = append(violations, Violation{
				SweepID:     sweepID,
				Account:     sourceAcc,
				Type:        "BUFFER_BREACH",
				Severity:    "HIGH",
				Message:     fmt.Sprintf("Balance (%.2f) below buffer (%.2f)", availableBalance, bufferAmt),
				PenaltyRisk: "Potential NSF charges",
			})
			stepViolations = append(stepViolations, "BUFFER_BREACH")
			status = "BLOCKED"
			validatedAmount = 0
		} else if requestedAmount > (availableBalance - bufferAmt) {
			// Partial sweep
			validatedAmount = availableBalance - bufferAmt
			stepViolations = append(stepViolations, "PARTIAL_SWEEP")
			status = "PARTIAL"
		}

		if status == "APPROVED" || status == "PARTIAL" {
			if status == "APPROVED" {
				successCount++
			}
			actualTransferred += validatedAmount

			// Update simulated balances
			balanceMap[sourceAcc] -= validatedAmount
			balanceMap[targetAcc] += validatedAmount
		}

		// Use actual sweep execution time from database
		execTime := "18:00" // default
		if sweep["execution_time"] != nil {
			if timeStr, ok := sweep["execution_time"].(string); ok && timeStr != "" {
				execTime = timeStr
			}
		}

		// Use actual effective date from sweep
		execDate := req.EffectiveDate // fallback
		if sweep["effective_date"] != nil {
			if datePtr, ok := sweep["effective_date"].(*string); ok && datePtr != nil && *datePtr != "" {
				execDate = *datePtr
			}
		}

		executionPlan = append(executionPlan, SweepExecutionStep{
			Step:             i + 1,
			Time:             fmt.Sprintf("%s %s", execDate, execTime),
			SweepID:          sweepID,
			FromAccount:      sourceAcc,
			FromBank:         fmt.Sprint(sweep["source_bank"]),
			FromEntity:       fmt.Sprint(sweep["entity_name"]),
			ToAccount:        targetAcc,
			ToBank:           fmt.Sprint(sweep["target_bank"]),
			ToEntity:         fmt.Sprint(sweep["entity_name"]),
			SweepType:        sweepType,
			BufferAmount:     bufferAmt,
			AvailableBalance: availableBalance,
			RequestedAmount:  requestedAmount,
			ValidatedAmount:  validatedAmount,
			Violations:       stepViolations,
			ExecutionStatus:  status,
		})
	}

	// Calculate master concentration final (ALL destination accounts after simulation)
	masterConcentrationFinal := 0.0
	for acc := range destinationAccounts {
		if bal, exists := balanceMap[acc]; exists {
			masterConcentrationFinal += bal
		}
	}

	// Calculate KPIs
	successRate := 0.0
	if len(sweeps) > 0 {
		successRate = (float64(successCount) / float64(len(sweeps))) * 100
	}

	// Concentration efficiency: actual transferred / planned outflow
	concentrationEfficiency := 0.0
	if totalPlannedOutflow > 0 {
		concentrationEfficiency = (actualTransferred / totalPlannedOutflow) * 100
	}

	// Projected position: what master concentration WOULD BE if all sweeps succeeded
	projectedPosition := masterConcentrationInitial + totalPlannedOutflow

	// Actual position = master concentration final
	actualPosition := masterConcentrationFinal

	// Idle cash reduction: % reduction in pure source account balances (not destinations)
	idleCashReduction := 0.0
	initialSourceBalance := 0.0
	finalSourceBalance := 0.0
	for acc := range accountsInvolved {
		// Only count accounts that are sources (includes BOTH)
		if sourceAccounts[acc] {
			if initial, exists := initialBalanceMap[acc]; exists {
				initialSourceBalance += initial
			}
			if final, exists := balanceMap[acc]; exists {
				finalSourceBalance += final
			}
		}
	}
	if initialSourceBalance > 0 {
		idleCashReduction = ((initialSourceBalance - finalSourceBalance) / initialSourceBalance) * 100
	}

	// Buffer utilization rate: average % of buffer used across all sweeps
	bufferUtilizationRate := 0.0
	bufferCount := 0
	totalBufferUsage := 0.0
	for _, sweep := range sweeps {
		sourceAcc := fmt.Sprint(sweep["source_account"])
		var bufferAmt float64 = 0
		if sweep["buffer_amount"] != nil {
			if val, ok := sweep["buffer_amount"].(*float64); ok && val != nil {
				bufferAmt = *val
			}
		}
		if bufferAmt > 0 {
			openingBal := initialBalanceMap[sourceAcc]
			if openingBal > 0 {
				usage := (bufferAmt / openingBal) * 100
				totalBufferUsage += usage
				bufferCount++
			}
		}
	}
	if bufferCount > 0 {
		bufferUtilizationRate = totalBufferUsage / float64(bufferCount)
	}

	// Build account breakdown for drillability
	accountBreakdown := []KPIAccountBreakdown{}
	for acc, role := range accountsInvolved {
		info, exists := accountInfoMap[acc]
		if !exists {
			continue
		}
		openingBal := initialBalanceMap[acc]
		closingBal := balanceMap[acc]
		accountBreakdown = append(accountBreakdown, KPIAccountBreakdown{
			AccountNumber:  acc,
			BankName:       info.BankName,
			EntityName:     info.EntityName,
			Role:           role,
			OpeningBalance: openingBal,
			ClosingBalance: closingBal,
			NetChange:      closingBal - openingBal,
		})
	}

	kpis := KPIMetrics{
		GrossOpeningBalance:        grossOpening,
		TotalPlannedOutflow:        totalPlannedOutflow,
		MasterConcentrationInitial: masterConcentrationInitial,
		MasterConcentrationFinal:   masterConcentrationFinal,
		ActualPosition:             actualPosition,
		ProjectedPosition:          projectedPosition,
		ConcentrationEfficiency:    concentrationEfficiency,
		IdleCashReduction:          idleCashReduction,
		BufferUtilizationRate:      bufferUtilizationRate,
		SuccessRate:                successRate,
		AccountBreakdown:           accountBreakdown,
	}

	// Build balance impact
	beforeBalances := []AccountBalance{}
	afterBalances := []AccountBalance{}

	for _, bal := range balances {
		beforeBalances = append(beforeBalances, bal)
		afterBalances = append(afterBalances, AccountBalance{
			AccountNumber: bal.AccountNumber,
			BankName:      bal.BankName,
			EntityName:    bal.EntityName,
			Balance:       balanceMap[bal.AccountNumber],
			Currency:      bal.Currency,
		})
	}

	balanceImpact := BalanceImpact{
		Before:              beforeBalances,
		After:               afterBalances,
		MasterAccountGrowth: masterConcentration,
	}

	// Timing analysis: calculate execution window from earliest to latest sweep
	var earliestTime, latestTime time.Time
	if len(executionPlan) > 0 {
		// Parse first and last execution times
		earliestTime, _ = time.Parse("2006-01-02 15:04", executionPlan[0].Time)
		latestTime, _ = time.Parse("2006-01-02 15:04", executionPlan[len(executionPlan)-1].Time)
	}

	executionWindow := ""
	if !earliestTime.IsZero() && !latestTime.IsZero() {
		duration := latestTime.Sub(earliestTime)
		if duration < 24*time.Hour {
			executionWindow = fmt.Sprintf("%.0f hours %.0f minutes", duration.Hours(), duration.Minutes()-duration.Hours()*60)
		} else {
			executionWindow = fmt.Sprintf("%.0f days %.0f hours", duration.Hours()/24, duration.Hours()-float64(int(duration.Hours()/24))*24)
		}
	}

	timingAnalysis := TimingAnalysis{
		TotalExecutionWindow:    executionWindow,
		EarliestExecution:       executionPlan[0].Time,
		LatestExecution:         executionPlan[len(executionPlan)-1].Time,
		TotalExecutionTime:      executionWindow, // deprecated field
		CriticalPath:            extractCriticalPath(executionPlan),
		ParallelOpportunities:   0,
		EstimatedCompletionTime: executionPlan[len(executionPlan)-1].Time, // deprecated field
	}

	return SimulationResponse{
		ExecutionPlan:  executionPlan,
		KPIs:           kpis,
		BalanceImpact:  balanceImpact,
		Violations:     violations,
		TimingAnalysis: timingAnalysis,
	}
}

// detectSweepChains identifies multi-hop sweep flows
func detectSweepChains(sweeps []map[string]interface{}) []SweepChain {
	// Build adjacency map
	graph := make(map[string][]string)
	for _, sweep := range sweeps {
		source := fmt.Sprint(sweep["source_account"])
		target := fmt.Sprint(sweep["target_account"])
		graph[source] = append(graph[source], target)
	}

	chains := []SweepChain{}
	visited := make(map[string]bool)

	// DFS to find chains
	var dfs func(string, []string) []string
	dfs = func(node string, path []string) []string {
		if visited[node] {
			// Circular dependency detected
			return path
		}
		visited[node] = true
		path = append(path, node)

		if targets, exists := graph[node]; exists {
			for _, target := range targets {
				return dfs(target, path)
			}
		}
		return path
	}

	chainID := 1
	for source := range graph {
		if !visited[source] {
			path := dfs(source, []string{})
			if len(path) > 2 { // Multi-hop chain
				isCircular := path[0] == path[len(path)-1]
				chains = append(chains, SweepChain{
					ChainID:    fmt.Sprintf("CHAIN-%d", chainID),
					Hops:       path,
					IsCircular: isCircular,
				})
				chainID++
			}
		}
	}

	return chains
}

// generateOptimizationSuggestions creates AI-powered recommendations
func generateOptimizationSuggestions(sweeps []map[string]interface{}, balances []AccountBalance, simulation SimulationResponse) []Suggestion {
	suggestions := []Suggestion{}

	// Suggestion 1: Low success rate
	if simulation.KPIs.SuccessRate < 80 {
		suggestions = append(suggestions, Suggestion{
			Type:             "OPTIMIZE_SEQUENCE",
			Priority:         "HIGH",
			Description:      fmt.Sprintf("Low success rate (%.1f%%). Consider adjusting buffer amounts or sweep sequence", simulation.KPIs.SuccessRate),
			EstimatedBenefit: "+15-20% execution success",
		})
	}

	// Suggestion 2: Low concentration efficiency
	if simulation.KPIs.ConcentrationEfficiency < 70 {
		suggestions = append(suggestions, Suggestion{
			Type:             "MODIFY_BUFFER",
			Priority:         "MEDIUM",
			Description:      "Buffer amounts may be too high, limiting sweep efficiency",
			EstimatedBenefit: "+10% concentration efficiency",
		})
	}

	// Suggestion 3: Circular dependencies
	if len(simulation.SweepChains) > 0 {
		for _, chain := range simulation.SweepChains {
			if chain.IsCircular {
				suggestions = append(suggestions, Suggestion{
					Type:        "OPTIMIZE_SEQUENCE",
					Priority:    "HIGH",
					Description: fmt.Sprintf("Circular sweep detected in chain %s - this may cause execution issues", chain.ChainID),
				})
			}
		}
	}

	return suggestions
}

// extractCriticalPath identifies the critical execution path
func extractCriticalPath(steps []SweepExecutionStep) []string {
	// Simple implementation: return sweep IDs in execution order
	criticalPath := []string{}
	for _, step := range steps {
		if step.ExecutionStatus == "APPROVED" {
			criticalPath = append(criticalPath, step.SweepID)
		}
	}
	return criticalPath
}
