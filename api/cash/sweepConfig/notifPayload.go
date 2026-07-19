package sweepconfig

// notif_payload.go — Rich notification payload builders for sweep configuration, initiation, and execution events
//
// DESIGN PHILOSOPHY
// ─────────────────
// Notification templates need FULL sweep record data to:
//   • Display rich tables with TABLE_HTML(SweepConfigs, 'entity_name', 'source_bank_name', 'target_bank_name', ...)
//   • Create KPI cards with KPI_CARDS_HTML showing totals by entity/bank/sweep_type
//   • Filter/sort/group sweeps with FILTER, ORDER_BY, GROUP_BY
//   • Calculate aggregates with SUM_OF_FIELD, COUNT_OF, AVG_OF_FIELD
//
// IMPLEMENTATION STRATEGY
// ───────────────────────
// We REUSE the existing GET endpoint queries which already return complete joined views:
//   • GetSweepConfigurationsV2 — returns sweep configs with bank balance data
//   • GetSweepInitiations — returns initiations with config data
//   • Sweep execution logs — execution results with full context
//
// This ensures:
//   1. Consistency — template variables match GET response structure exactly
//   2. Zero duplication — we don't re-implement complex JOIN logic
//   3. Maintainability — changes to GET endpoint auto-flow to notifications

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ═══════════════════════════════════════════════════════════════════════════
// SWEEP CONFIGURATION NOTIFICATION PAYLOAD
// ═══════════════════════════════════════════════════════════════════════════

// SweepConfigRow represents a single sweep configuration with all fields
type SweepConfigRow map[string]interface{}

// KPIRow is a grouped aggregate (entity/bank/sweep_type summary)
type KPIRow struct {
	GroupName   string  `json:"group_name"`
	Count       int     `json:"count"`
	TotalAmount float64 `json:"total_amount"` // sum of sweep_amount or buffer_amount
}

// SweepConfigNotifPayload holds ALL sweep config data for rich template rendering
type SweepConfigNotifPayload struct {
	// Scalars
	Action            string  // CREATE | UPDATE | DELETE | APPROVE | REJECT
	RequestedBy       string  // user who triggered action
	Count             int     // number of sweep configs affected
	TotalSweepAmount  float64 // sum of sweep_amount
	TotalBufferAmount float64 // sum of buffer_amount
	ActionAt          string  // ISO timestamp
	Reason            string  // for UPDATE/DELETE actions
	CheckerComment    string  // for APPROVE/REJECT actions

	// Lists (for TABLE_HTML, KPI_CARDS_HTML, FILTER, etc.)
	SweepConfigs     []SweepConfigRow // full records from GET response
	SweepConfigIDs   []string         // simple ID array for COUNT_OF() template function
	ByEntityKPIs     []KPIRow         // grouped by entity_name
	BySweepTypeKPIs  []KPIRow         // grouped by sweep_type
	BySourceBankKPIs []KPIRow         // grouped by source_bank_name
	ByTargetBankKPIs []KPIRow         // grouped by target_bank_name
}

// ToMap converts payload to map[string]interface{} for template engine
func (p SweepConfigNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":            p.Action,
		"RequestedBy":       p.RequestedBy,
		"Count":             p.Count,
		"TotalSweepAmount":  p.TotalSweepAmount,
		"TotalBufferAmount": p.TotalBufferAmount,
		"ActionAt":          p.ActionAt,
		"Reason":            p.Reason,
		"CheckerComment":    p.CheckerComment,
		"SweepConfigs":      p.SweepConfigs,
		"SweepConfigIDs":    p.SweepConfigIDs,
		"ByEntityKPIs":      p.ByEntityKPIs,
		"BySweepTypeKPIs":   p.BySweepTypeKPIs,
		"BySourceBankKPIs":  p.BySourceBankKPIs,
		"ByTargetBankKPIs":  p.ByTargetBankKPIs,
	}
}

// BuildSweepConfigNotifPayload fetches FULL sweep config records using GetSweepConfigurationsV2 query
func BuildSweepConfigNotifPayload(ctx context.Context, pool *pgxpool.Pool, sweepIDs []string, action, requestedBy string) SweepConfigNotifPayload {
	p := SweepConfigNotifPayload{
		Action:       action,
		RequestedBy:  requestedBy,
		Count:        len(sweepIDs),
		ActionAt:     time.Now().Format(time.RFC3339),
		SweepConfigs: make([]SweepConfigRow, 0),
	}

	if len(sweepIDs) == 0 {
		return p
	}

	// Reuse GetSweepConfigurationsV2 query with WHERE filter for specific IDs
	query := buildGetSweepConfigurationsQuery()
	rows, err := pool.Query(ctx, query, sweepIDs)
	if err != nil {
		return p // return empty on error (notifications should not crash the main flow)
	}
	defer rows.Close()

	var totalSweepAmt, totalBufferAmt float64
	for rows.Next() {
		vals, _ := rows.Values()
		cols := rows.FieldDescriptions()
		row := make(SweepConfigRow)
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}

		// Extract sweep_amount and buffer_amount for sums
		if amt, ok := row["sweep_amount"].(float64); ok {
			totalSweepAmt += amt
		}
		if amt, ok := row["buffer_amount"].(float64); ok {
			totalBufferAmt += amt
		}

		p.SweepConfigs = append(p.SweepConfigs, row)
	}

	p.TotalSweepAmount = totalSweepAmt
	p.TotalBufferAmount = totalBufferAmt

	// Auto-compute KPIs (group by entity, sweep_type, source_bank, target_bank)
	p.ByEntityKPIs = computeSweepConfigKPIs(p.SweepConfigs, "entity_name")
	p.BySweepTypeKPIs = computeSweepConfigKPIs(p.SweepConfigs, "sweep_type")
	p.BySourceBankKPIs = computeSweepConfigKPIs(p.SweepConfigs, "source_bank_name")
	p.ByTargetBankKPIs = computeSweepConfigKPIs(p.SweepConfigs, "target_bank_name")

	// Extract ID array for COUNT_OF() template function
	p.SweepConfigIDs = sweepConfigIDsFromRows(p.SweepConfigs)

	return p
}

// buildGetSweepConfigurationsQuery returns the query from GetSweepConfigurationsV2 with ID filter
func buildGetSweepConfigurationsQuery() string {
	return `
		SELECT 
			sweep_id, entity_name, 
			source_bank_name, source_bank_account, 
			target_bank_name, target_bank_account, 
			sweep_type, frequency,
			COALESCE(bbal1.current_balance,0) AS source_current_balance,
			COALESCE(bbal2.current_balance,0) AS target_current_balance,
			effective_date, execution_time, 
			buffer_amount, sweep_amount, 
			requires_initiation,
			old_entity_name, 
			old_source_bank_name, old_source_bank_account, 
			old_target_bank_name, old_target_bank_account, 
			old_sweep_type, old_frequency, 
			old_effective_date, old_execution_time, 
			old_buffer_amount, old_sweep_amount
		FROM cimplrcorpsaas.sweepconfiguration 
		LEFT JOIN LATERAL (
			SELECT COALESCE(bbm.closing_balance,0) AS current_balance
			FROM public.bank_balances_manual bbm
			JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
			WHERE a.processing_status = 'APPROVED'
				AND bbm.account_no = COALESCE(sweepconfiguration.source_bank_account, sweepconfiguration.source_bank_account)
			ORDER BY bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
			LIMIT 1
		) bbal1 ON true
		LEFT JOIN LATERAL (
			SELECT COALESCE(bbm.closing_balance,0) AS current_balance
			FROM public.bank_balances_manual bbm
			JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
			WHERE a.processing_status = 'APPROVED'
				AND bbm.account_no = COALESCE(sweepconfiguration.target_bank_account, sweepconfiguration.target_bank_account)
			ORDER BY bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
			LIMIT 1
		) bbal2 ON true
		WHERE is_deleted != TRUE 
		  AND sweep_id = ANY($1)
		ORDER BY GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp)) DESC, sweep_id`
}

// computeSweepConfigKPIs groups sweep configs by given field and computes counts and sums
func computeSweepConfigKPIs(configs []SweepConfigRow, groupField string) []KPIRow {
	groups := make(map[string]*KPIRow)

	for _, c := range configs {
		groupVal := ""
		if v, ok := c[groupField]; ok && v != nil {
			groupVal = fmt.Sprintf("%v", v)
		}
		if groupVal == "" {
			groupVal = constants.Unknown
		}

		if _, exists := groups[groupVal]; !exists {
			groups[groupVal] = &KPIRow{GroupName: groupVal}
		}

		kpi := groups[groupVal]
		kpi.Count++
		if amt, ok := c["sweep_amount"].(float64); ok {
			kpi.TotalAmount += amt
		}
	}

	kpis := make([]KPIRow, 0, len(groups))
	for _, kpi := range groups {
		kpis = append(kpis, *kpi)
	}
	return kpis
}

// sweepConfigIDsFromRows extracts sweep_id values from SweepConfigRow maps for COUNT_OF() template function.
func sweepConfigIDsFromRows(rows []SweepConfigRow) []string {
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		if id, ok := r["sweep_id"].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// ═══════════════════════════════════════════════════════════════════════════
// SWEEP INITIATION NOTIFICATION PAYLOAD
// ═══════════════════════════════════════════════════════════════════════════

// SweepInitiationRow represents a single sweep initiation with config data
type SweepInitiationRow map[string]interface{}

// SweepInitiationNotifPayload holds ALL sweep initiation data for rich template rendering
type SweepInitiationNotifPayload struct {
	// Scalars
	Action               string  // CREATE | APPROVE | REJECT
	RequestedBy          string  // user who triggered action
	Count                int     // number of initiations affected
	TotalInitiatedAmount float64 // sum of initiated amounts
	ActionAt             string  // ISO timestamp
	CheckerComment       string  // for APPROVE/REJECT actions

	// Lists
	Initiations     []SweepInitiationRow // full initiation records
	InitiationIDs   []string             // simple ID array for COUNT_OF() template function
	ByEntityKPIs    []KPIRow             // grouped by entity_name
	BySweepTypeKPIs []KPIRow             // grouped by sweep_type
}

// ToMap converts payload to map[string]interface{} for template engine
func (p SweepInitiationNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":               p.Action,
		"RequestedBy":          p.RequestedBy,
		"Count":                p.Count,
		"TotalInitiatedAmount": p.TotalInitiatedAmount,
		"ActionAt":             p.ActionAt,
		"CheckerComment":       p.CheckerComment,
		"Initiations":          p.Initiations,
		"InitiationIDs":        p.InitiationIDs,
		"ByEntityKPIs":         p.ByEntityKPIs,
		"BySweepTypeKPIs":      p.BySweepTypeKPIs,
	}
}

// BuildSweepInitiationNotifPayload fetches FULL sweep initiation records
func BuildSweepInitiationNotifPayload(ctx context.Context, pool *pgxpool.Pool, initiationIDs []string, action, requestedBy string) SweepInitiationNotifPayload {
	p := SweepInitiationNotifPayload{
		Action:      action,
		RequestedBy: requestedBy,
		Count:       len(initiationIDs),
		ActionAt:    time.Now().Format(time.RFC3339),
		Initiations: make([]SweepInitiationRow, 0),
	}

	if len(initiationIDs) == 0 {
		return p
	}

	// Query sweep initiations with config data
	query := `
		SELECT 
			si.initiation_id, si.sweep_id, si.initiated_by, si.initiated_at,
			si.source_bank_account, si.target_bank_account, si.amount, si.remarks,
			si.processing_status,
			sc.entity_name, sc.source_bank_name, sc.target_bank_name, 
			sc.sweep_type, sc.frequency, sc.buffer_amount
		FROM cimplrcorpsaas.sweep_initiation si
		LEFT JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = si.sweep_id
		WHERE si.initiation_id = ANY($1)
		ORDER BY si.initiated_at DESC`

	rows, err := pool.Query(ctx, query, initiationIDs)
	if err != nil {
		return p
	}
	defer rows.Close()

	var totalAmount float64
	for rows.Next() {
		vals, _ := rows.Values()
		cols := rows.FieldDescriptions()
		row := make(SweepInitiationRow)
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}

		if amt, ok := row["amount"].(float64); ok {
			totalAmount += amt
		}

		p.Initiations = append(p.Initiations, row)
	}

	p.TotalInitiatedAmount = totalAmount
	p.ByEntityKPIs = computeSweepInitiationKPIs(p.Initiations, "entity_name")
	p.BySweepTypeKPIs = computeSweepInitiationKPIs(p.Initiations, "sweep_type")

	// Extract ID array for COUNT_OF() template function
	p.InitiationIDs = sweepInitiationIDsFromRows(p.Initiations)

	return p
}

// computeSweepInitiationKPIs groups initiations by given field
func computeSweepInitiationKPIs(initiations []SweepInitiationRow, groupField string) []KPIRow {
	groups := make(map[string]*KPIRow)

	for _, init := range initiations {
		groupVal := ""
		if v, ok := init[groupField]; ok && v != nil {
			groupVal = fmt.Sprintf("%v", v)
		}
		if groupVal == "" {
			groupVal = constants.Unknown
		}

		if _, exists := groups[groupVal]; !exists {
			groups[groupVal] = &KPIRow{GroupName: groupVal}
		}

		kpi := groups[groupVal]
		kpi.Count++
		if amt, ok := init["amount"].(float64); ok {
			kpi.TotalAmount += amt
		}
	}

	kpis := make([]KPIRow, 0, len(groups))
	for _, kpi := range groups {
		kpis = append(kpis, *kpi)
	}
	return kpis
}

// sweepInitiationIDsFromRows extracts initiation_id values from SweepInitiationRow maps for COUNT_OF() template function.
func sweepInitiationIDsFromRows(rows []SweepInitiationRow) []string {
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		if id, ok := r["initiation_id"].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// ═══════════════════════════════════════════════════════════════════════════
// SWEEP EXECUTION NOTIFICATION PAYLOAD
// ═══════════════════════════════════════════════════════════════════════════

// SweepExecutionRow represents a single sweep execution result
type SweepExecutionRow map[string]interface{}

// SweepExecutionNotifPayload holds sweep execution data for template rendering
type SweepExecutionNotifPayload struct {
	// Scalars
	Action              string  // MANUAL_TRIGGER | SCHEDULED_TRIGGER
	RequestedBy         string  // user who triggered (for manual) or "SYSTEM" for scheduled
	SweepID             string  // sweep_id that was executed
	ExecutionStatus     string  // SUCCESS | FAILED
	TotalExecutedAmount float64 // amount swept
	ActionAt            string  // ISO timestamp

	// Execution details
	ExecutionResult map[string]interface{} // full execution result object

	// Config context (for reference)
	EntityName     string
	SourceBankName string
	TargetBankName string
	SweepType      string
}

// ToMap converts payload to map[string]interface{} for template engine
func (p SweepExecutionNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":              p.Action,
		"RequestedBy":         p.RequestedBy,
		"SweepID":             p.SweepID,
		"ExecutionStatus":     p.ExecutionStatus,
		"TotalExecutedAmount": p.TotalExecutedAmount,
		"ActionAt":            p.ActionAt,
		"ExecutionResult":     p.ExecutionResult,
		"EntityName":          p.EntityName,
		"SourceBankName":      p.SourceBankName,
		"TargetBankName":      p.TargetBankName,
		"SweepType":           p.SweepType,
	}
}

// BuildSweepExecutionNotifPayload fetches sweep execution context
func BuildSweepExecutionNotifPayload(ctx context.Context, pool *pgxpool.Pool, sweepID string, executionResult map[string]interface{}, action, requestedBy string) SweepExecutionNotifPayload {
	p := SweepExecutionNotifPayload{
		Action:          action,
		RequestedBy:     requestedBy,
		SweepID:         sweepID,
		ActionAt:        time.Now().Format(time.RFC3339),
		ExecutionResult: executionResult,
	}

	// Extract status and amount from execution result
	if status, ok := executionResult["status"].(string); ok {
		p.ExecutionStatus = status
	}
	if amt, ok := executionResult["amount_swept"].(float64); ok {
		p.TotalExecutedAmount = amt
	}

	// Fetch sweep config context for enrichment
	query := `
		SELECT entity_name, source_bank_name, target_bank_name, sweep_type
		FROM cimplrcorpsaas.sweepconfiguration
		WHERE sweep_id = $1 AND is_deleted != TRUE`

	row := pool.QueryRow(ctx, query, sweepID)
	var entity, sourceBank, targetBank, sweepType string
	if err := row.Scan(&entity, &sourceBank, &targetBank, &sweepType); err == nil {
		p.EntityName = entity
		p.SourceBankName = sourceBank
		p.TargetBankName = targetBank
		p.SweepType = sweepType
	}

	return p
}
