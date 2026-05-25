// Package investmentdashboards — Template-driven BOD/EOD checklist resolver.
//
// Joins cimplr.fd_bod_eod_checklist_template (per persona × mode) with the live
// counts already computed by the dashboard sub-queries (so we never query twice)
// and persisted per-item sign-off rows in cimplr.fd_bod_eod_sign_off.
//
// Each template row carries a `source_metric` key. We look that key up in a
// metrics bag (built from already-computed sub-results) to decide is_done. This
// keeps the "what counts as done" rule in SQL/seed data rather than scattered
// across Go.
package investmentdashboards

import (
	"context"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// checklistItem is the wire shape consumed by the UI.
type checklistItem struct {
	ItemCode     string `json:"item_code"`
	Label        string `json:"label"`
	Category     string `json:"category"`
	IsBlocker    bool   `json:"blocker"`
	Done         bool   `json:"done"`
	DetailValue  string `json:"detail_value,omitempty"`
	SourceMetric string `json:"source_metric,omitempty"`
	DoneBy       string `json:"done_by,omitempty"`
	DoneAt       string `json:"done_at,omitempty"`
	Remarks      string `json:"remarks,omitempty"`
	DisplayOrder int    `json:"display_order"`
}

type checklistTemplateRow struct {
	ItemCode     string
	Label        string
	Category     string
	SourceMetric string
	IsBlocker    bool
	DisplayOrder int
	IsDone       *bool
	DoneBy       *string
	DoneAt       *string
	Remarks      *string
}

// loadChecklistTemplate fetches active template rows + any persisted sign-off
// for the given (persona, mode) and dashboard run coordinates.
//
// Falls back to a hardcoded OPERATOR template if the table is missing or no
// rows exist for the persona — guarantees the UI always renders something.
func loadChecklistTemplate(
	ctx context.Context,
	pool *pgxpool.Pool,
	persona, mode, entityID, businessDate string,
) []checklistTemplateRow {
	const q = `
		SELECT
		  t.item_code,
		  t.label,
		  t.category,
		  COALESCE(t.source_metric, '') AS source_metric,
		  COALESCE(t.is_blocker, false) AS is_blocker,
		  COALESCE(t.display_order, 0)  AS display_order,
		  s.is_done,
		  s.done_by,
		  CASE WHEN s.done_at IS NOT NULL
		    THEN TO_CHAR(s.done_at, 'YYYY-MM-DD"T"HH24:MI:SS')
		    ELSE NULL
		  END AS done_at,
		  s.remarks
		FROM cimplr.fd_bod_eod_checklist_template t
		LEFT JOIN cimplr.fd_bod_eod_run r
		  ON  r.entity_id     = $3
		  AND r.business_date = $4::date
		  AND r.mode          = $2
		LEFT JOIN cimplr.fd_bod_eod_sign_off s
		  ON  s.run_id    = r.run_id
		  AND s.persona   = t.persona
		  AND s.item_code = t.item_code
		WHERE t.persona = $1
		  AND t.mode    = $2
		  AND COALESCE(t.is_active, true) = true
		ORDER BY t.display_order ASC, t.item_code ASC`

	rows, err := pool.Query(ctx, q, persona, mode, entityID, businessDate)
	if err != nil {
		api.LogInfo("[BodEodDashV2] checklist template query failed (%v) — using fallback for %s/%s",
			err, persona, mode)
		return fallbackChecklistTemplate(persona, mode)
	}
	defer rows.Close()

	out := []checklistTemplateRow{}
	for rows.Next() {
		var t checklistTemplateRow
		if err := rows.Scan(
			&t.ItemCode, &t.Label, &t.Category, &t.SourceMetric,
			&t.IsBlocker, &t.DisplayOrder,
			&t.IsDone, &t.DoneBy, &t.DoneAt, &t.Remarks,
		); err != nil {
			api.LogError("[BodEodDashV2] checklist scan: %v", err)
			continue
		}
		out = append(out, t)
	}
	if len(out) == 0 {
		return fallbackChecklistTemplate(persona, mode)
	}
	return out
}

// fallbackChecklistTemplate mirrors the v1 hardcoded checklist so the dashboard
// still works on environments that have not run the V2 seed migration.
func fallbackChecklistTemplate(persona, mode string) []checklistTemplateRow {
	if mode == "BOD" {
		return []checklistTemplateRow{
			{ItemCode: "B1", Label: "Maturities for today reviewed", Category: "Maturity", SourceMetric: "maturities_today_count", DisplayOrder: 10},
			{ItemCode: "B2", Label: "Confirmations SLA reviewed", Category: "Confirmation", SourceMetric: "confirmations_overdue", IsBlocker: true, DisplayOrder: 20},
			{ItemCode: "B3", Label: "Accrual jobs scheduled", Category: "Accrual", SourceMetric: "accrual_runs_today", DisplayOrder: 30},
			{ItemCode: "B4", Label: "Expected interest credits planned", Category: "Cashflow", SourceMetric: "expected_interest_today", DisplayOrder: 40},
			{ItemCode: "B5", Label: "Yesterday SLA breaches actioned", Category: "SLA", SourceMetric: "sla_breaches_yesterday", IsBlocker: true, DisplayOrder: 50},
		}
	}
	return []checklistTemplateRow{
		{ItemCode: "C1", Label: "Bank statement ingestion completed", Category: "Reconciliation", SourceMetric: "receipts_unmatched", IsBlocker: true, DisplayOrder: 10},
		{ItemCode: "C2", Label: "High variance cases reviewed", Category: "Reconciliation", SourceMetric: "open_variance", IsBlocker: true, DisplayOrder: 20},
		{ItemCode: "C3", Label: "All GL postings successful", Category: "Posting", SourceMetric: "failed_postings", IsBlocker: true, DisplayOrder: 30},
		{ItemCode: "C4", Label: "No critical exceptions open", Category: "Exceptions", SourceMetric: "critical_exceptions", DisplayOrder: 40},
		{ItemCode: "C5", Label: "Accrual run completed", Category: "Accrual", SourceMetric: "accrual_run_status", DisplayOrder: 50},
		{ItemCode: "C6", Label: "Maturity proceeds verified", Category: "Maturity", SourceMetric: "pending_maturities", IsBlocker: true, DisplayOrder: 60},
	}
}

// resolveItemDone applies the source_metric semantics: 0/empty/"COMPLETED" → done.
// Detail text is a short human label, e.g. "3 unmatched", that the UI shows
// next to the checkbox without doing arithmetic itself.
func resolveItemDone(sourceMetric string, metrics map[string]interface{}) (bool, string) {
	if strings.TrimSpace(sourceMetric) == "" {
		return false, ""
	}
	raw, ok := metrics[sourceMetric]
	if !ok {
		return false, ""
	}
	switch v := raw.(type) {
	case int:
		return v == 0, formatChecklistCount(int64(v), sourceMetric)
	case int64:
		return v == 0, formatChecklistCount(v, sourceMetric)
	case float64:
		return v == 0, formatChecklistCount(int64(v), sourceMetric)
	case string:
		up := strings.ToUpper(strings.TrimSpace(v))
		done := up == "COMPLETED" || up == "POSTED" || up == "DONE" || up == "SUCCESS"
		return done, "Status: " + v
	case bool:
		return v, ""
	}
	return false, ""
}

func formatChecklistCount(n int64, metric string) string {
	suffix := "items"
	switch metric {
	case "receipts_unmatched":
		suffix = "unmatched"
	case "open_variance":
		suffix = "pending"
	case "failed_postings":
		suffix = "failed"
	case "critical_exceptions", "pending_maturities", "sla_breaches_yesterday":
		suffix = "pending"
	case "confirmations_overdue":
		suffix = "overdue"
	case "maturities_today_count", "accrual_runs_today":
		suffix = "today"
	}
	return formatInt64(n) + " " + suffix
}

// buildChecklistPayload glues template + sign-off + live metrics into the wire
// shape used by the UI. It also returns aggregate counters for the header card.
func buildChecklistPayload(
	template []checklistTemplateRow,
	metrics map[string]interface{},
) map[string]interface{} {
	items := make([]checklistItem, 0, len(template))
	completed := 0
	blockers := 0
	for _, t := range template {
		derivedDone, detail := resolveItemDone(t.SourceMetric, metrics)
		// Persisted sign-off wins over derived state. Allows ops to mark items
		// done manually (e.g. after a manual recon outside the system).
		done := derivedDone
		if t.IsDone != nil {
			done = *t.IsDone
		}
		it := checklistItem{
			ItemCode:     t.ItemCode,
			Label:        t.Label,
			Category:     t.Category,
			IsBlocker:    t.IsBlocker,
			Done:         done,
			DetailValue:  detail,
			SourceMetric: t.SourceMetric,
			DisplayOrder: t.DisplayOrder,
		}
		if t.DoneBy != nil {
			it.DoneBy = *t.DoneBy
		}
		if t.DoneAt != nil {
			it.DoneAt = *t.DoneAt
		}
		if t.Remarks != nil {
			it.Remarks = *t.Remarks
		}
		if done {
			completed++
		} else if t.IsBlocker {
			blockers++
		}
		items = append(items, it)
	}
	total := len(items)
	pct := 0.0
	if total > 0 {
		pct = fdRound(float64(completed)/float64(total)*100, 1)
	}
	return map[string]interface{}{
		"items":          items,
		"completed":      completed,
		"total":          total,
		"completion_pct": pct,
		"blockers":       blockers,
	}
}
