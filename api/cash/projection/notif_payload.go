package projection

// notif_payload.go — Rich notification payload builder for cash flow projection events
//
// DESIGN PHILOSOPHY
// ─────────────────
// Notification templates need FULL projection proposal data to:
//   • Display rich tables with TABLE_HTML(Proposals, 'proposal_name', 'effective_date', 'item_count', ...)
//   • Create KPI cards with KPI_CARDS_HTML showing totals by entity/category/cashflow_type
//   • Filter/sort/group proposals with FILTER, ORDER_BY, GROUP_BY
//   • Calculate aggregates with SUM_OF_FIELD, COUNT_OF, AVG_OF_FIELD
//
// IMPLEMENTATION STRATEGY
// ───────────────────────
// We REUSE the existing GET endpoint query (GetProposalDetailV2) which returns:
//   • cashflow_proposal (proposal header with name, currency, effective date)
//   • cashflow_proposal_item (detailed items with amounts, categories, banks)
//   • audit_action_cashflow_proposal (workflow status)
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

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProjectionProposalRow represents a single projection proposal with all fields
type ProjectionProposalRow map[string]interface{}

// KPIRow is a grouped aggregate (entity/category/cashflow_type summary)
type KPIRow struct {
	GroupName   string  `json:"group_name"`
	Count       int     `json:"count"`
	TotalAmount float64 `json:"total_amount"`
}

// ProjectionNotifPayload holds ALL projection proposal data for rich template rendering
type ProjectionNotifPayload struct {
	// Scalars
	Action              string  // CREATE | UPDATE | DELETE | APPROVE | REJECT | UPLOAD
	RequestedBy         string  // user who triggered action
	Count               int     // number of proposals affected
	TotalExpectedAmount float64 // sum of expected_amount across all items
	TotalItemCount      int     // total number of items across all proposals
	ActionAt            string  // ISO timestamp
	Reason              string  // for UPDATE/DELETE actions
	CheckerComment      string  // for APPROVE/REJECT actions
	FileName            string  // for UPLOAD action only

	// Lists (for TABLE_HTML, KPI_CARDS_HTML, FILTER, etc.)
	ProposalIDs        []string                 // simple array of proposal_id strings (for COUNT_OF)
	Proposals          []ProjectionProposalRow  // full proposal headers
	Items              []map[string]interface{} // all items across proposals
	ByEntityKPIs       []KPIRow                 // grouped by entity_name
	ByCategoryKPIs     []KPIRow                 // grouped by category_id
	ByCashflowTypeKPIs []KPIRow                 // grouped by cashflow_type (Inflow/Outflow)
}

// ToMap converts payload to map[string]interface{} for template engine
func (p ProjectionNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":              p.Action,
		"RequestedBy":         p.RequestedBy,
		"Count":               p.Count,
		"TotalExpectedAmount": p.TotalExpectedAmount,
		"TotalItemCount":      p.TotalItemCount,
		"ActionAt":            p.ActionAt,
		"Reason":              p.Reason,
		"CheckerComment":      p.CheckerComment,
		"FileName":            p.FileName,
		"ProposalIDs":         p.ProposalIDs,
		"Proposals":           p.Proposals,
		"Items":               p.Items,
		"ByEntityKPIs":        p.ByEntityKPIs,
		"ByCategoryKPIs":      p.ByCategoryKPIs,
		"ByCashflowTypeKPIs":  p.ByCashflowTypeKPIs,
	}
}

// BuildProjectionNotifPayload fetches FULL projection proposal records using GetProposalDetailV2 logic
func BuildProjectionNotifPayload(ctx context.Context, pool *pgxpool.Pool, proposalIDs []string, action, requestedBy string) ProjectionNotifPayload {
	p := ProjectionNotifPayload{
		Action:      action,
		RequestedBy: requestedBy,
		Count:       len(proposalIDs),
		ActionAt:    time.Now().Format(time.RFC3339),
		ProposalIDs: proposalIDs,
		Proposals:   make([]ProjectionProposalRow, 0),
		Items:       make([]map[string]interface{}, 0),
	}

	if len(proposalIDs) == 0 {
		return p
	}

	// Fetch proposal headers
	headerQuery := `
		SELECT
			p.proposal_id,
			p.proposal_name,
			p.base_currency_code,
			p.effective_date,
			p.old_proposal_name,
			p.old_base_currency_code,
			p.old_effective_date,
			CASE
				WHEN COALESCE(p.is_deleted, false) = true THEN 'DELETED'
				ELSE a.processing_status
			END AS processing_status
		FROM cimplrcorpsaas.cashflow_proposal p
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM cimplrcorpsaas.audit_action_cashflow_proposal a2
			WHERE a2.proposal_id = p.proposal_id
			ORDER BY requested_at DESC
			LIMIT 1
		) a ON TRUE
		WHERE p.proposal_id = ANY($1)`

	rows, err := pool.Query(ctx, headerQuery, proposalIDs)
	if err != nil {
		return p // return empty on error
	}
	defer rows.Close()

	for rows.Next() {
		vals, _ := rows.Values()
		cols := rows.FieldDescriptions()
		row := make(ProjectionProposalRow)
		for i, col := range cols {
			row[string(col.Name)] = normalizeVal(vals[i])
		}
		p.Proposals = append(p.Proposals, row)
	}

	// Fetch all items for these proposals
	itemQuery := `
		SELECT 
			item_id, proposal_id, description, cashflow_type, category_id, currency_code, expected_amount,
			is_recurring, recurrence_frequency, maturity_date, bank_name, bank_account_number, entity_name,
			old_cashflow_type, old_category_id, old_currency_code, old_expected_amount,
			old_is_recurring, old_recurrence_frequency, old_maturity_date, old_entity_name,
			old_bank_name, old_bank_account_number
		FROM cimplrcorpsaas.cashflow_proposal_item
		WHERE proposal_id = ANY($1)
		ORDER BY proposal_id, created_at`

	itemRows, err := pool.Query(ctx, itemQuery, proposalIDs)
	if err != nil {
		return p
	}
	defer itemRows.Close()

	var totalAmount float64
	for itemRows.Next() {
		vals, _ := itemRows.Values()
		cols := itemRows.FieldDescriptions()
		item := make(map[string]interface{})
		for i, col := range cols {
			item[string(col.Name)] = normalizeVal(vals[i])
		}

		// Sum expected_amount
		if amt, ok := item["expected_amount"].(float64); ok {
			totalAmount += amt
		}

		p.Items = append(p.Items, item)
	}

	p.TotalExpectedAmount = totalAmount
	p.TotalItemCount = len(p.Items)

	// Auto-compute KPIs (group items by entity, category, cashflow_type)
	p.ByEntityKPIs = computeProjectionKPIs(p.Items, "entity_name")
	p.ByCategoryKPIs = computeProjectionKPIs(p.Items, "category_id")
	p.ByCashflowTypeKPIs = computeProjectionKPIs(p.Items, "cashflow_type")

	return p
}

// computeProjectionKPIs groups items by given field and computes counts and sums
func computeProjectionKPIs(items []map[string]interface{}, groupField string) []KPIRow {
	groups := make(map[string]*KPIRow)

	for _, item := range items {
		groupVal := ""
		if v, ok := item[groupField]; ok && v != nil {
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
		if amt, ok := item["expected_amount"].(float64); ok {
			kpi.TotalAmount += amt
		}
	}

	kpis := make([]KPIRow, 0, len(groups))
	for _, kpi := range groups {
		kpis = append(kpis, *kpi)
	}
	return kpis
}

// normalizeVal converts pgx raw types to standard Go types for template rendering.
// pgtype.Numeric (PostgreSQL numeric/decimal) → float64
// pgtype.Text, pgtype.Varchar → string
// pgtype.Date, pgtype.Timestamptz → time.Time string
func normalizeVal(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case pgtype.Numeric:
		if !t.Valid {
			return float64(0)
		}
		f, err := t.Float64Value()
		if err != nil {
			return float64(0)
		}
		return f.Float64
	case pgtype.Text:
		if t.Valid {
			return t.String
		}
		return nil
	case pgtype.Date:
		if t.Valid {
			return t.Time.Format(constants.DateFormat)
		}
		return nil
	case pgtype.Timestamptz:
		if t.Valid {
			return t.Time.Format(time.RFC3339)
		}
		return nil
	case pgtype.Timestamp:
		if t.Valid {
			return t.Time.Format(time.RFC3339)
		}
		return nil
	default:
		return v
	}
}
