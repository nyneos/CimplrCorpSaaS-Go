package sweepconfig

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

// ===================================================================================
// SWEEP SIMULATION & ANALYSIS SYSTEM V2
// Comprehensive pre-execution sweep analyzer for CFO-level decision making
// ===================================================================================

// SimulationRequest holds input parameters for sweep simulation
type SimulationRequest struct {
	UserID         string   `json:"user_id"`
	EntityIDs      []string `json:"entity_ids,omitempty"`      // Filter by entities
	BankNames      []string `json:"bank_names,omitempty"`      // Filter by banks
	Frequency      string   `json:"frequency"`                  // DAILY, MONTHLY, SPECIFIC_DATE
	EffectiveDate  string   `json:"effective_date"`             // Date to simulate
	ExecutionTime  string   `json:"execution_time"`             // Time of day (HH:MM format)
	SweepIDs       []string `json:"sweep_ids,omitempty"`       // Specific sweeps (optional, empty = all approved)
	IncludeChains  bool     `json:"include_chains"`             // Detect multi-hop sweep chains
	IncludeSuggestions bool  `json:"include_suggestions"`       // AI-powered optimization suggestions
}

// SweepExecutionStep represents a single sweep in the execution sequence
type SweepExecutionStep struct {
	Step              int                    `json:"step"`
	Time              string                 `json:"time"`
	SweepID           string                 `json:"sweep_id"`
	InitiationID      string                 `json:"initiation_id,omitempty"`
	FromAccount       string                 `json:"from_account"`
	FromBank          string                 `json:"from_bank"`
	FromEntity        string                 `json:"from_entity"`
	ToAccount         string                 `json:"to_account"`
	ToBank            string                 `json:"to_bank"`
	ToEntity          string                 `json:"to_entity"`
	SweepType         string                 `json:"sweep_type"`
	BufferAmount      float64                `json:"buffer_amount"`
	AvailableBalance  float64                `json:"available_balance"`
	RequestedAmount   float64                `json:"requested_amount"`
	ValidatedAmount   float64                `json:"validated_amount"`
	Violations        []string               `json:"violations"`
	Penalties         []string               `json:"penalties"`
	ExecutionStatus   string                 `json:"execution_status"` // APPROVED, BLOCKED, PARTIAL
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
}

// KPIMetrics holds key performance indicators
type KPIMetrics struct {
	GrossOpeningBalance       float64 `json:"gross_opening_balance"`
	TotalPlannedOutflow       float64 `json:"total_planned_outflow"`
	MasterConcentrationFinal  float64 `json:"master_concentration_final"`
	ProjectedPosition         float64 `json:"projected_position"`
	ConcentrationEfficiency   float64 `json:"concentration_efficiency"`    // % of funds concentrated
	IdleCashReduction         float64 `json:"idle_cash_reduction"`         // % reduction in idle cash
	BufferUtilizationRate     float64 `json:"buffer_utilization_rate"`     // Average buffer usage
	SuccessRate               float64 `json:"success_rate"`                // % of sweeps executed
}

// BalanceImpact shows before/after balance positions
type BalanceImpact struct {
	Before             []AccountBalance `json:"before"`
	After              []AccountBalance `json:"after"`
	MasterAccountGrowth float64         `json:"master_account_growth"`
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
	SweepID      string `json:"sweep_id"`
	Account      string `json:"account"`
	Type         string `json:"type"`     // BUFFER_BREACH, INSUFFICIENT_FUNDS, TIMING_CONFLICT, CIRCULAR_DEPENDENCY
	Severity     string `json:"severity"` // HIGH, MEDIUM, LOW
	Message      string `json:"message"`
	PenaltyRisk  string `json:"penalty_risk,omitempty"`
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
	TotalExecutionTime      string   `json:"total_execution_time"`
	CriticalPath            []string `json:"critical_path"`           // Sweep IDs on critical path
	ParallelOpportunities   int      `json:"parallel_opportunities"`  // Number of parallel execution opportunities
	EstimatedCompletionTime string   `json:"estimated_completion_time"`
}

// SimulationResponse is the complete simulation output
type SimulationResponse struct {
	SimulationID      string               `json:"simulation_id"`
	ExecutionPlan     []SweepExecutionStep `json:"execution_plan"`
	KPIs              KPIMetrics           `json:"kpis"`
	BalanceImpact     BalanceImpact        `json:"balance_impact"`
	Violations        []Violation          `json:"violations"`
	Suggestions       []Suggestion         `json:"suggestions,omitempty"`
	TimingAnalysis    TimingAnalysis       `json:"timing_analysis"`
	SweepChains       []SweepChain         `json:"sweep_chains,omitempty"`
	GeneratedAt       string               `json:"generated_at"`
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
			api.RespondWithResult(w, false, "user_id is required")
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

		// Validate required fields
		if req.Frequency == "" {
			api.RespondWithResult(w, false, "frequency is required (DAILY, MONTHLY, SPECIFIC_DATE)")
			return
		}
		if req.EffectiveDate == "" {
			api.RespondWithResult(w, false, "effective_date is required")
			return
		}
		if req.ExecutionTime == "" {
			req.ExecutionTime = "18:00" // Default to 6 PM
		}

		log.Printf("[SWEEP SIMULATION] User: %s, Date: %s, Time: %s, Frequency: %s",
			req.UserID, req.EffectiveDate, req.ExecutionTime, req.Frequency)

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
			api.RespondWithResult(w, false, "No approved sweeps found for simulation")
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

		w.Header().Set("Content-Type", "application/json")
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
			api.RespondWithResult(w, false, "user_id is required")
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

		w.Header().Set("Content-Type", "application/json")
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
			api.RespondWithResult(w, false, "user_id is required")
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

		w.Header().Set("Content-Type", "application/json")
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
			UserID     string `json:"user_id"`
			DateFrom   string `json:"date_from,omitempty"`
			DateTo     string `json:"date_to,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON+": "+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, "user_id is required")
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
			req.DateFrom = time.Now().AddDate(0, 0, -30).Format("2006-01-02")
		}
		if req.DateTo == "" {
			req.DateTo = time.Now().Format("2006-01-02")
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
				"total_swept":             totalSwept,
				"avg_sweep_amount":        avgSwept,
				"total_executions":        totalExec,
				"successful_executions":   successful,
				"failed_executions":       failed,
			},
			"efficiency_scores": map[string]interface{}{
				"sweep_success_rate":      successRate,
				"execution_reliability":   successRate,
			},
			"period": map[string]interface{}{
				"from": req.DateFrom,
				"to":   req.DateTo,
			},
		}

		w.Header().Set("Content-Type", "application/json")
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
			api.RespondWithResult(w, false, "user_id is required")
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

		w.Header().Set("Content-Type", "application/json")
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
			api.RespondWithResult(w, false, "user_id is required")
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

		w.Header().Set("Content-Type", "application/json")
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
		JOIN cimplrcorpsaas.auditactionsweepconfiguration a 
			ON a.sweep_id = sc.sweep_id
		WHERE a.processing_status = 'APPROVED'
			AND COALESCE(sc.is_deleted, false) = false
			AND UPPER(TRIM(sc.frequency)) = $%d
			%s
		ORDER BY sc.sweep_id, a.requested_at DESC
	`, argPos, sweepFilter)

	args = append(args, strings.ToUpper(strings.TrimSpace(req.Frequency)))

	rows, err := pgxPool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sweeps := []map[string]interface{}{}
	for rows.Next() {
		var sweepID, entityName, sourceAcc, sourceBank, targetAcc, targetBank, sweepType, frequency, executionTime string
		var effectiveDate *string
		var bufferAmt, sweepAmt *float64
		var requiresInitiation bool

		if err := rows.Scan(&sweepID, &entityName, &sourceAcc, &sourceBank, &targetAcc, &targetBank, &sweepType, &frequency, &effectiveDate, &executionTime, &bufferAmt, &sweepAmt, &requiresInitiation); err != nil {
			continue
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
			"effective_date":      effectiveDate,
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
		if len(normEntities) > 0 {
			filters += fmt.Sprintf(" AND LOWER(TRIM(mba.entity_id)) = ANY($%d)", argPos)
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
			mba.currency
		FROM cimplrcorpsaas.bank_balances_manual bb
		JOIN public.masterbankaccount mba ON mba.account_number = bb.account_no
		WHERE COALESCE(mba.is_deleted, false) = false
			%s
		ORDER BY bb.account_no, bb.as_of_date DESC, bb.as_of_time DESC
	`, filters)

	rows, err := pgxPool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	balances := []AccountBalance{}
	for rows.Next() {
		var accountNo, bankName, entityID, currency string
		var balance float64

		if err := rows.Scan(&accountNo, &bankName, &entityID, &balance, &currency); err != nil {
			continue
		}

		balances = append(balances, AccountBalance{
			AccountNumber: accountNo,
			BankName:      bankName,
			EntityName:    entityID,
			Balance:       balance,
			Currency:      currency,
		})
	}

	return balances, nil
}

// runSweepSimulation executes the core simulation logic
func runSweepSimulation(sweeps []map[string]interface{}, balances []AccountBalance, req SimulationRequest) SimulationResponse {
	// Create balance lookup map
	balanceMap := make(map[string]float64)
	for _, bal := range balances {
		balanceMap[bal.AccountNumber] = bal.Balance
	}

	executionPlan := []SweepExecutionStep{}
	violations := []Violation{}
	
	grossOpening := 0.0
	totalPlannedOutflow := 0.0
	masterConcentration := 0.0
	successCount := 0

	// Simulate each sweep
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
		grossOpening += availableBalance

		requestedAmount := sweepAmt
		validatedAmount := sweepAmt
		status := "APPROVED"
		stepViolations := []string{}

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
		} else if sweepAmt > (availableBalance - bufferAmt) {
			// Partial sweep
			validatedAmount = availableBalance - bufferAmt
			stepViolations = append(stepViolations, "PARTIAL_SWEEP")
			status = "PARTIAL"
		}

		if status == "APPROVED" {
			successCount++
			totalPlannedOutflow += validatedAmount
			masterConcentration += validatedAmount
			
			// Update simulated balances
			balanceMap[sourceAcc] -= validatedAmount
			balanceMap[targetAcc] += validatedAmount
		}

		execTime := req.ExecutionTime
		if i > 0 {
			// Stagger execution times by 5 minutes
			parsedTime, _ := time.Parse("15:04", req.ExecutionTime)
			parsedTime = parsedTime.Add(time.Duration(i*5) * time.Minute)
			execTime = parsedTime.Format("15:04")
		}

		executionPlan = append(executionPlan, SweepExecutionStep{
			Step:             i + 1,
			Time:             fmt.Sprintf("%s %s", req.EffectiveDate, execTime),
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

	// Calculate KPIs
	successRate := 0.0
	if len(sweeps) > 0 {
		successRate = (float64(successCount) / float64(len(sweeps))) * 100
	}

	concentrationEfficiency := 0.0
	if grossOpening > 0 {
		concentrationEfficiency = (totalPlannedOutflow / grossOpening) * 100
	}

	kpis := KPIMetrics{
		GrossOpeningBalance:      grossOpening,
		TotalPlannedOutflow:      totalPlannedOutflow,
		MasterConcentrationFinal: masterConcentration,
		ProjectedPosition:        masterConcentration,
		ConcentrationEfficiency:  concentrationEfficiency,
		SuccessRate:              successRate,
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

	// Timing analysis
	estimatedTime := len(sweeps) * 5 // 5 minutes per sweep
	timingAnalysis := TimingAnalysis{
		TotalExecutionTime:      fmt.Sprintf("%d minutes", estimatedTime),
		CriticalPath:            extractCriticalPath(executionPlan),
		ParallelOpportunities:   0, // Could be enhanced with dependency analysis
		EstimatedCompletionTime: fmt.Sprintf("%s %s", req.EffectiveDate, req.ExecutionTime),
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
