package limit

// notif_payload.go — Rich notification payload builder for bank limit events
//
// DESIGN PHILOSOPHY
// ─────────────────
// Notification templates need the FULL record data (not just IDs/counts) to:
//   • Display rich tables with TABLE_HTML(Limits, 'entity_name', 'bank_name', 'sanctioned_amount', ...)
//   • Create KPI cards with KPI_CARDS_HTML showing totals by entity/bank/currency
//   • Filter/sort/group limits with FILTER, ORDER_BY, GROUP_BY
//   • Calculate aggregates with SUM_OF_FIELD, COUNT_OF, AVG_OF_FIELD
//
// IMPLEMENTATION STRATEGY
// ───────────────────────
// We REUSE the existing GET endpoint query (GetBankLimitUtilizationV2) which already
// returns the complete joined view with ALL fields from:
//   • bank_limit (core limit master data)
//   • auditactionbanklimit (workflow audit trail)
//   • limit_utilization (actual utilization records)
//   • auditactionlimitutilization (utilization workflow)
//
// This ensures:
//   1. Consistency — template variables match GET response structure exactly
//   2. Zero duplication — we don't re-implement complex JOIN logic
//   3. Maintainability — changes to GET endpoint auto-flow to notifications
//
// USAGE PATTERN
// ─────────────
// Instead of:
//   go catalog.TriggerNotification(ctx, pool, route, correlationID, map[string]interface{}{
//       "Count": 3,
//       "Action": "CREATE",
//   })
//
// Do this:
//   payload := BuildLimitNotifPayload(ctx, pool, limitIDs, "CREATE", requestedBy)
//   go catalog.TriggerNotification(ctx, pool, route, correlationID, payload.ToMap())
//
// TEMPLATE AUTHOR REFERENCE
// ─────────────────────────
// Top-level scalar variables:
//   Action               — "CREATE" | "UPDATE" | "DELETE" | "APPROVE" | "REJECT"
//   RequestedBy          — user who triggered the action
//   Count                — number of limits affected
//   TotalSanctioned      — sum of all sanctioned_amount (in base currency or mixed)
//   ActionAt             — ISO timestamp
//
// List variables (use with TABLE_HTML, GROUP_BY, KPI_CARDS_HTML, etc.):
//   Limits               — []LimitRow (full records from GET response)
//     Fields: limit_id, entity_name, bank_name, core_limit_type, limit_type, limit_sub_type,
//             sanctioned_amount, currency_code, sanction_date, effective_date,
//             fungibility_type, fungibility_pct, security_type, remarks,
//             limit_available, limit_utilization_pct, processing_status, requested_by, requested_at,
//             checker_by, checker_at, checker_comment, reason,
//             ... (ALL fields from GetBankLimitUtilizationV2 response)
//
//   ByEntityKPIs         — []KPIRow grouped by entity_name
//     Fields: group_name, count, total_sanctioned
//
//   ByBankKPIs           — []KPIRow grouped by bank_name
//
//   ByCurrencyKPIs       — []KPIRow grouped by currency_code
//
// EXAMPLE TEMPLATE SNIPPETS
// ─────────────────────────
// 1. Simple counts/totals:
//    {{COUNT_OF(Limits)}} limit(s) {{Action}} by {{RequestedBy}}
//    Total Sanctioned: {{FORMAT_CURRENCY(TotalSanctioned, 'INR')}}
//
// 2. Full limit table:
//    {{TABLE_HTML(Limits,
//      ['entity_name','bank_name','limit_type','sanctioned_amount','currency_code','processing_status'],
//      ['Entity','Bank','Type','Sanctioned Amt','Currency','Status']
//    )}}
//
// 3. KPI cards by entity:
//    {{KPI_CARDS_HTML([
//      {"label":"Total Limits","value":"{{COUNT_OF(Limits)}}"},
//      {"label":"Total Sanctioned","value":"{{FORMAT_NUMBER(TotalSanctioned)}}"},
//      {"label":"Entities","value":"{{COUNT_OF(ByEntityKPIs)}}"}
//    ])}}
//
// 4. Filtered view (only FUND BASED limits):
//    {{FILTER(Limits, 'core_limit_type', 'FUND BASED')}}
//    {{TABLE_HTML(__filter_core_limit_type_FUND_BASED, ['entity_name','sanctioned_amount'])}}
//
// 5. Sorted by sanctioned amount (high to low):
//    {{ORDER_BY(Limits, 'sanctioned_amount', 'DESC')}}
//    {{TABLE_HTML(__ordered_Limits_sanctioned_amount_DESC, ['bank_name','sanctioned_amount'], ['Bank','Amount'], 5)}}

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LimitRow represents one limit record with ALL fields from GetBankLimitUtilizationV2.
// Field names match the GET response exactly so template authors can reference them directly.
type LimitRow map[string]interface{}

// KPIRow is a grouped aggregate (entity/bank/currency summary).
type KPIRow struct {
	GroupName       string  `json:"group_name"`
	Count           int     `json:"count"`
	TotalSanctioned float64 `json:"total_sanctioned"`
}

// LimitNotifPayload is the top-level notification payload for limit events.
type LimitNotifPayload struct {
	// ── Scalar metadata ────────────────────────────────────────────────────────
	Action          string    `json:"Action"`           // CREATE | UPDATE | DELETE | APPROVE | REJECT
	RequestedBy     string    `json:"RequestedBy"`      // user who triggered
	Count           int       `json:"Count"`            // number of limits
	TotalSanctioned float64   `json:"TotalSanctioned"`  // sum of sanctioned_amount (mixed currency warning: sums INR+USD+CNY)
	ActionAt        string    `json:"ActionAt"`         // ISO timestamp

	// ── List fields ────────────────────────────────────────────────────────────
	Limits         []LimitRow `json:"Limits"`         // full records from GET
	LimitIDs       []string   `json:"LimitIDs"`       // simple ID array for COUNT_OF() template function
	ByEntityKPIs   []KPIRow   `json:"ByEntityKPIs"`   // grouped by entity_name
	ByBankKPIs     []KPIRow   `json:"ByBankKPIs"`     // grouped by bank_name
	ByCurrencyKPIs []KPIRow   `json:"ByCurrencyKPIs"` // grouped by currency_code
}

// ToMap converts LimitNotifPayload to map[string]interface{} for TriggerNotification.
func (p *LimitNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":          p.Action,
		"RequestedBy":     p.RequestedBy,
		"Count":           p.Count,
		"TotalSanctioned": p.TotalSanctioned,
		"ActionAt":        p.ActionAt,
		"Limits":          limitRowsToMaps(p.Limits),
		"LimitIDs":        p.LimitIDs,
		"ByEntityKPIs":    kpiRowsToMaps(p.ByEntityKPIs),
		"ByBankKPIs":      kpiRowsToMaps(p.ByBankKPIs),
		"ByCurrencyKPIs":  kpiRowsToMaps(p.ByCurrencyKPIs),
	}
}

// limitRowsToMaps converts []LimitRow → []map[string]interface{} for template engine.
func limitRowsToMaps(rows []LimitRow) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = r // LimitRow is already map[string]interface{}
	}
	return out
}

// kpiRowsToMaps converts []KPIRow → []map[string]interface{}.
func kpiRowsToMaps(rows []KPIRow) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = map[string]interface{}{
			"group_name":       r.GroupName,
			"count":            r.Count,
			"total_sanctioned": r.TotalSanctioned,
		}
	}
	return out
}

// limitIDsFromRows extracts limit_id values from LimitRow maps for COUNT_OF() template function.
func limitIDsFromRows(rows []LimitRow) []string {
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		if id, ok := r["limit_id"].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// BuildLimitNotifPayload constructs a rich notification payload by REUSING the
// GetBankLimitUtilizationV2 query logic to fetch full limit records.
//
// Parameters:
//   ctx         — request context
//   pool        — pgx connection pool
//   limitIDs    — slice of limit_id strings to include
//   action      — "CREATE" | "UPDATE" | "DELETE" | "APPROVE" | "REJECT"
//   requestedBy — user who triggered the action
//
// Returns:
//   *LimitNotifPayload with Limits[] populated from DB + computed KPIs
func BuildLimitNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	limitIDs []string,
	action string,
	requestedBy string,
) *LimitNotifPayload {
	p := &LimitNotifPayload{
		Action:      action,
		RequestedBy: requestedBy,
		Count:       len(limitIDs),
		ActionAt:    time.Now().Format(time.RFC3339),
		Limits:      []LimitRow{},
	}

	if len(limitIDs) == 0 {
		return p
	}

	// REUSE GetBankLimitUtilizationV2 query — this is the SAME query that powers
	// the GET /cash/limit/utilization/v2/get endpoint, ensuring consistency.
	//
	// The query returns ALL fields from:
	//   • bank_limit (limit master)
	//   • auditactionbanklimit (limit workflow)
	//   • limit_utilization (utilization records)
	//   • auditactionlimitutilization (utilization workflow)
	//
	// We filter by limit_id IN (...) to get only the affected records.
	query := buildGetLimitUtilizationQuery(limitIDs)

	rows, err := pool.Query(ctx, query, limitIDs)
	if err != nil {
		// Log error but don't fail notification — send with empty Limits list
		fmt.Printf("[ERROR] BuildLimitNotifPayload query failed: %v\n", err)
		return p
	}
	defer rows.Close()

	// Parse rows into LimitRow maps
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			continue
		}
		cols := rows.FieldDescriptions()
		row := make(LimitRow)
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}
		p.Limits = append(p.Limits, row)
	}

	// Compute KPIs by grouping
	p.ByEntityKPIs = computeKPIs(p.Limits, "limit_entity_name")
	p.ByBankKPIs = computeKPIs(p.Limits, "limit_bank_name")
	p.ByCurrencyKPIs = computeKPIs(p.Limits, "limit_currency_code")

	// Extract ID array for COUNT_OF() template function
	p.LimitIDs = limitIDsFromRows(p.Limits)

	// Compute total sanctioned (WARNING: mixed currency — this sums INR+USD+CNY naively)
	for _, lim := range p.Limits {
		if amt, ok := lim["limit_sanctioned_amount"].(float64); ok {
			p.TotalSanctioned += amt
		}
	}

	return p
}

// buildGetLimitUtilizationQuery returns the exact query used by GetBankLimitUtilizationV2,
// filtered to only the specified limit_ids.
//
// This query performs a complex LEFT JOIN across:
//   • bank_limit
//   • auditactionbanklimit (latest audit per limit)
//   • limit_utilization (utilizations for this limit)
//   • auditactionlimitutilization (latest audit per utilization)
//
// The result set has ALL fields flattened with prefixes:
//   limit_*       — from bank_limit + auditactionbanklimit
//   (no prefix)   — from limit_utilization + auditactionlimitutilization
//
// NOTE: This is a COPY of the query from GetBankLimitUtilizationV2.
// If that query changes, update this copy to match.
func buildGetLimitUtilizationQuery(limitIDs []string) string {
	// Build IN clause placeholders
	placeholders := "("
	for i := range limitIDs {
		if i > 0 {
			placeholders += ","
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}
	placeholders += ")"

	return `
		WITH latest_limit_audit AS (
			SELECT DISTINCT ON (limit_id)
				limit_id,
				action_type AS limit_action_type,
				processing_status AS limit_processing_status,
				reason AS limit_reason,
				requested_by AS limit_requested_by,
				TO_CHAR(requested_at, 'YYYY-MM-DD HH24:MI:SS') AS limit_requested_at,
				checker_by AS limit_checker_by,
				TO_CHAR(checker_at, 'YYYY-MM-DD HH24:MI:SS') AS limit_checker_at,
				checker_comment AS limit_checker_comment,
				old_entity_name AS limit_old_entity_name,
				old_bank_name AS limit_old_bank_name,
				old_core_limit_type AS limit_old_core_limit_type,
				old_limit_type AS limit_old_limit_type,
				old_limit_sub_type AS limit_old_limit_sub_type,
				old_sanction_date AS limit_old_sanction_date,
				old_effective_date AS limit_old_effective_date,
				old_currency_code AS limit_old_currency_code,
				old_sanctioned_amount AS limit_old_sanctioned_amount,
				old_fungibility_type AS limit_old_fungibility_type,
				old_fungibility_pct AS limit_old_fungibility_pct,
				old_security_type AS limit_old_security_type,
				old_remarks AS limit_old_remarks,
				old_initial_utilization AS limit_old_initial_utilization
			FROM cimplrcorpsaas.auditactionbanklimit
			ORDER BY limit_id, requested_at DESC
		),
		latest_utilization_audit AS (
			SELECT DISTINCT ON (utilization_id)
				utilization_id,
				action_type,
				processing_status,
				reason,
				requested_by,
				TO_CHAR(requested_at, 'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				checker_by,
				TO_CHAR(checker_at, 'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				checker_comment,
				old_utilization_date,
				old_utilized_amount,
				old_currency_code,
				old_remarks,
				old_reference_doc
			FROM cimplrcorpsaas.auditactionlimitutilization
			ORDER BY utilization_id, requested_at DESC
		)
		SELECT
			l.limit_id AS limit_limit_id,
			l.entity_name AS limit_entity_name,
			l.bank_name AS limit_bank_name,
			l.core_limit_type AS limit_core_limit_type,
			l.limit_type AS limit_limit_type,
			COALESCE(l.limit_sub_type, '') AS limit_limit_sub_type,
			TO_CHAR(l.sanction_date, 'YYYY-MM-DD HH24:MI:SS') AS limit_sanction_date,
			TO_CHAR(l.effective_date, 'YYYY-MM-DD HH24:MI:SS') AS limit_effective_date,
			l.currency_code AS limit_currency_code,
			l.sanctioned_amount AS limit_sanctioned_amount,
			l.fungibility_type AS limit_fungibility_type,
			COALESCE(l.fungibility_pct, 0) AS limit_fungibility_pct,
			l.security_type AS limit_security_type,
			COALESCE(l.remarks, '') AS limit_remarks,
			COALESCE(l.initial_utilization, 0) AS limit_initial_utilization,
			COALESCE(l.available, 0) AS limit_available,
			COALESCE(l.utilization_pct, 0) AS limit_utilization_pct,
			COALESCE(la.limit_action_type, '') AS limit_action_type,
			COALESCE(la.limit_processing_status, '') AS limit_processing_status,
			COALESCE(la.limit_reason, '') AS limit_reason,
			COALESCE(la.limit_requested_by, '') AS limit_requested_by,
			COALESCE(la.limit_requested_at, '') AS limit_requested_at,
			COALESCE(la.limit_checker_by, '') AS limit_checker_by,
			COALESCE(la.limit_checker_at, '') AS limit_checker_at,
			COALESCE(la.limit_checker_comment, '') AS limit_checker_comment,
			COALESCE(la.limit_old_entity_name, '') AS limit_old_entity_name,
			COALESCE(la.limit_old_bank_name, '') AS limit_old_bank_name,
			COALESCE(la.limit_old_core_limit_type, '') AS limit_old_core_limit_type,
			COALESCE(la.limit_old_limit_type, '') AS limit_old_limit_type,
			COALESCE(la.limit_old_limit_sub_type, '') AS limit_old_limit_sub_type,
			COALESCE(la.limit_old_sanction_date, '') AS limit_old_sanction_date,
			COALESCE(la.limit_old_effective_date, '') AS limit_old_effective_date,
			COALESCE(la.limit_old_currency_code, '') AS limit_old_currency_code,
			COALESCE(la.limit_old_sanctioned_amount, 0) AS limit_old_sanctioned_amount,
			COALESCE(la.limit_old_fungibility_type, '') AS limit_old_fungibility_type,
			COALESCE(la.limit_old_fungibility_pct, 0) AS limit_old_fungibility_pct,
			COALESCE(la.limit_old_security_type, '') AS limit_old_security_type,
			COALESCE(la.limit_old_remarks, '') AS limit_old_remarks,
			COALESCE(la.limit_old_initial_utilization, 0) AS limit_old_initial_utilization,
			COALESCE(u.utilization_id, '') AS utilization_id,
			COALESCE(u.limit_id, '') AS limit_id,
			COALESCE(TO_CHAR(u.utilization_date, 'YYYY-MM-DD HH24:MI:SS'), '') AS utilization_date,
			COALESCE(u.utilized_amount, 0) AS utilized_amount,
			COALESCE(u.currency_code, '') AS currency_code,
			COALESCE(u.remarks, '') AS remarks,
			COALESCE(u.reference_doc, '') AS reference_doc,
			COALESCE(u.entry_mode, '') AS entry_mode,
			COALESCE(u.status, '') AS status,
			COALESCE(ua.action_type, '') AS action_type,
			COALESCE(ua.processing_status, '') AS processing_status,
			COALESCE(ua.reason, '') AS reason,
			COALESCE(ua.requested_by, '') AS requested_by,
			COALESCE(ua.requested_at, '') AS requested_at,
			COALESCE(ua.checker_by, '') AS checker_by,
			COALESCE(ua.checker_at, '') AS checker_at,
			COALESCE(ua.checker_comment, '') AS checker_comment,
			COALESCE(ua.old_utilization_date, '') AS old_utilization_date,
			COALESCE(ua.old_utilized_amount, 0) AS old_utilized_amount,
			COALESCE(ua.old_currency_code, '') AS old_currency_code,
			COALESCE(ua.old_remarks, '') AS old_remarks,
			COALESCE(ua.old_reference_doc, '') AS old_reference_doc
		FROM cimplrcorpsaas.bank_limit l
		LEFT JOIN latest_limit_audit la ON la.limit_id = l.limit_id
		LEFT JOIN cimplrcorpsaas.limit_utilization u ON u.limit_id = l.limit_id
		LEFT JOIN latest_utilization_audit ua ON ua.utilization_id = u.utilization_id
		WHERE l.limit_id = ANY(` + placeholders + `)
		ORDER BY l.entity_name, l.bank_name, l.limit_id, u.utilization_date DESC
	`
}

// computeKPIs groups LimitRows by a field and computes count + total_sanctioned.
func computeKPIs(limits []LimitRow, groupField string) []KPIRow {
	groups := map[string]*KPIRow{}
	for _, lim := range limits {
		key, _ := lim[groupField].(string)
		if key == "" {
			key = "(unknown)"
		}
		if _, ok := groups[key]; !ok {
			groups[key] = &KPIRow{GroupName: key}
		}
		groups[key].Count++
		if amt, ok := lim["limit_sanctioned_amount"].(float64); ok {
			groups[key].TotalSanctioned += amt
		}
	}
	out := []KPIRow{}
	for _, kpi := range groups {
		out = append(out, *kpi)
	}
	return out
}

// BuildLimitNotifPayloadFromRequest is a convenience wrapper for bulk operations
// where you have the full request struct and need to extract limit_id values
// from the DB after insertion.
//
// Use this pattern in handlers like BulkCreateBankLimit:
//
//	var createdLimitIDs []string
//	// ... insert limits, collect IDs ...
//	payload := BuildLimitNotifPayloadFromIDs(ctx, pool, createdLimitIDs, "CREATE", requestedBy)
//	go catalog.TriggerNotification(ctx, pool, route, correlationID, payload.ToMap())
func BuildLimitNotifPayloadFromIDs(
	ctx context.Context,
	pool *pgxpool.Pool,
	limitIDs []string,
	action string,
	requestedBy string,
) *LimitNotifPayload {
	return BuildLimitNotifPayload(ctx, pool, limitIDs, action, requestedBy)
}

// MarshalJSON for debugging/logging
func (p *LimitNotifPayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.ToMap())
}

// ═══════════════════════════════════════════════════════════════════════════
// LIMIT UTILIZATION NOTIFICATION PAYLOAD
// ═══════════════════════════════════════════════════════════════════════════

// UtilizationRow represents a single utilization record with joined limit data
type UtilizationRow map[string]interface{}

// UtilizationNotifPayload holds ALL utilization data for rich template rendering
type UtilizationNotifPayload struct {
	// Scalars
	Action             string  // CREATE | UPDATE | DELETE | APPROVE | REJECT | UPLOAD
	RequestedBy        string  // user who triggered action
	Count              int     // number of utilizations affected
	TotalUtilized      float64 // sum of utilized_amount
	ActionAt           string  // ISO timestamp
	FileName           string  // for UPLOAD action only
	RowsUploaded       int     // for UPLOAD action only

	// Lists (for TABLE_HTML, KPI_CARDS_HTML, FILTER, etc.)
	Utilizations       []UtilizationRow // full records from GET response
	UtilizationIDs     []string         // simple ID array for COUNT_OF() template function
	ByEntityKPIs       []KPIRow         // grouped by limit_entity_name
	ByBankKPIs         []KPIRow         // grouped by limit_bank_name
	ByCurrencyKPIs     []KPIRow         // grouped by currency_code
}

// ToMap converts payload to map[string]interface{} for template engine
func (p UtilizationNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":           p.Action,
		"RequestedBy":      p.RequestedBy,
		"Count":            p.Count,
		"TotalUtilized":    p.TotalUtilized,
		"ActionAt":         p.ActionAt,
		"FileName":         p.FileName,
		"RowsUploaded":     p.RowsUploaded,
		"Utilizations":     p.Utilizations,
		"UtilizationIDs":   p.UtilizationIDs,
		"ByEntityKPIs":     p.ByEntityKPIs,
		"ByBankKPIs":       p.ByBankKPIs,
		"ByCurrencyKPIs":   p.ByCurrencyKPIs,
	}
}

// BuildUtilizationNotifPayload fetches FULL utilization records using the GetAllUtilizations query
func BuildUtilizationNotifPayload(ctx context.Context, pool *pgxpool.Pool, utilizationIDs []string, action, requestedBy string) UtilizationNotifPayload {
	p := UtilizationNotifPayload{
		Action:      action,
		RequestedBy: requestedBy,
		Count:       len(utilizationIDs),
		ActionAt:    time.Now().Format(time.RFC3339),
		Utilizations: make([]UtilizationRow, 0),
	}

	if len(utilizationIDs) == 0 {
		return p
	}

	// Reuse GetAllUtilizations query with WHERE filter for specific IDs
	query := buildGetAllUtilizationsQuery(utilizationIDs)
	rows, err := pool.Query(ctx, query, utilizationIDs)
	if err != nil {
		return p // return empty on error (notifications should not crash the main flow)
	}
	defer rows.Close()

	var totalUtilized float64
	for rows.Next() {
		vals, _ := rows.Values()
		cols := rows.FieldDescriptions()
		row := make(UtilizationRow)
		for i, col := range cols {
			row[string(col.Name)] = vals[i]
		}

		// Extract utilized_amount for sum
		if amt, ok := row["utilized_amount"].(float64); ok {
			totalUtilized += amt
		}

		p.Utilizations = append(p.Utilizations, row)
	}

	p.TotalUtilized = totalUtilized

	// Auto-compute KPIs (group by entity, bank, currency)
	p.ByEntityKPIs = computeUtilizationKPIs(p.Utilizations, "limit_entity_name")
	p.ByBankKPIs = computeUtilizationKPIs(p.Utilizations, "limit_bank_name")
	p.ByCurrencyKPIs = computeUtilizationKPIs(p.Utilizations, "currency_code")

	// Extract ID array for COUNT_OF() template function
	p.UtilizationIDs = utilizationIDsFromRows(p.Utilizations)

	return p
}

// buildGetAllUtilizationsQuery returns the exact query from GetAllUtilizations with ID filter
func buildGetAllUtilizationsQuery(utilizationIDs []string) string {
	return `
		SELECT 
			u.utilization_id, u.limit_id, u.utilization_date, u.currency_code, u.utilized_amount,
			u.remarks, u.reference_doc, u.entry_mode, u.status,
			u.old_utilization_date, u.old_currency_code, u.old_utilized_amount, u.old_remarks, u.old_reference_doc,
			a.action_type, a.processing_status, a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason,

			-- limit fields (flattened JOIN)
			l.limit_id as limit_limit_id, l.entity_name as limit_entity_name, l.bank_name as limit_bank_name, 
			l.core_limit_type as limit_core_limit_type, l.limit_type as limit_limit_type, l.limit_sub_type as limit_limit_sub_type,
			l.sanction_date as limit_sanction_date, l.effective_date as limit_effective_date, 
			l.currency_code as limit_currency_code, l.sanctioned_amount as limit_sanctioned_amount,
			l.fungibility_type as limit_fungibility_type, l.fungibility_pct as limit_fungibility_pct, 
			l.security_type as limit_security_type, l.remarks as limit_remarks, l.initial_utilization as limit_initial_utilization,
			l.old_entity_name as limit_old_entity_name, l.old_bank_name as limit_old_bank_name, 
			l.old_core_limit_type as limit_old_core_limit_type, l.old_limit_type as limit_old_limit_type, 
			l.old_limit_sub_type as limit_old_limit_sub_type,
			l.old_sanction_date as limit_old_sanction_date, l.old_effective_date as limit_old_effective_date, 
			l.old_currency_code as limit_old_currency_code, l.old_sanctioned_amount as limit_old_sanctioned_amount,
			l.old_fungibility_type as limit_old_fungibility_type, l.old_fungibility_pct as limit_old_fungibility_pct, 
			l.old_security_type as limit_old_security_type, l.old_remarks as limit_old_remarks, 
			l.old_initial_utilization as limit_old_initial_utilization,
			la.action_type as limit_action_type, la.processing_status as limit_processing_status, 
			la.requested_by as limit_requested_by, la.requested_at as limit_requested_at, 
			la.checker_by as limit_checker_by, la.checker_at as limit_checker_at, 
			la.checker_comment as limit_checker_comment, la.reason as limit_reason

		FROM cimplrcorpsaas.bank_limit_utilization u
		LEFT JOIN LATERAL (
			SELECT action_type, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment, reason
			FROM cimplrcorpsaas.auditactionbanklimitutilization
			WHERE utilization_id = u.utilization_id
			ORDER BY requested_at DESC
			LIMIT 1
		) a ON TRUE
		LEFT JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		LEFT JOIN LATERAL (
			SELECT action_type, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment, reason
			FROM cimplrcorpsaas.auditactionbanklimit
			WHERE limit_id = l.limit_id
			ORDER BY requested_at DESC
			LIMIT 1
		) la ON TRUE
		WHERE COALESCE(u.is_deleted, false) = false
		  AND u.utilization_id = ANY($1)
		ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC`
}

// computeUtilizationKPIs groups utilizations by given field and computes counts and sums
func computeUtilizationKPIs(utilizations []UtilizationRow, groupField string) []KPIRow {
	groups := make(map[string]*KPIRow)

	for _, u := range utilizations {
		groupVal := ""
		if v, ok := u[groupField]; ok && v != nil {
			groupVal = fmt.Sprintf("%v", v)
		}
		if groupVal == "" {
			groupVal = "(unknown)"
		}

		if _, exists := groups[groupVal]; !exists {
			groups[groupVal] = &KPIRow{GroupName: groupVal}
		}

		kpi := groups[groupVal]
		kpi.Count++
		if amt, ok := u["utilized_amount"].(float64); ok {
			kpi.TotalSanctioned += amt // reuse TotalSanctioned field for utilized amount sum
		}
	}

	kpis := make([]KPIRow, 0, len(groups))
	for _, kpi := range groups {
		kpis = append(kpis, *kpi)
	}
	return kpis
}

// utilizationIDsFromRows extracts utilization_id values from UtilizationRow maps for COUNT_OF() template function.
func utilizationIDsFromRows(rows []UtilizationRow) []string {
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		if id, ok := r["utilization_id"].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}
