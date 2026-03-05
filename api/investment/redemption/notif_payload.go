package redemption

// notif_payload.go — Rich notification payload builders for redemption events
//
// DESIGN PHILOSOPHY
// ─────────────────
// Each Build<Domain>NotifPayload function calls the shared fetch*Rows helper
// (defined in the respective GET handler file) with an ID filter instead of
// duplicating SQL here. All SQL lives in exactly ONE place per domain.
//
// USAGE PATTERN
// ─────────────
//   payload := BuildRedemptionInitiationNotifPayload(ctx, pool, ids, "CREATE", requestedBy)
//   go catalog.TriggerNotification(ctx, pool, route, correlationID, payload.ToMap())
//
// ─────────────────────────────────────────────────────────────────────────────────
// REDEMPTION INITIATION TEMPLATE VARIABLES
// ─────────────────────────────────────────────────────────────────────────────────
// Scalars: Action, RequestedBy, Count, TotalAmount, ActionAt
// Lists:
//   Initiations     — []map[string]interface{} — full records
//   InitiationIDs   — []string
//   ByEntityKPIs    — []map{group_name, count, total_amount}  grouped by entity_name
//   BySchemeKPIs    — []map{group_name, count, total_amount}  grouped by scheme_name
//
// ─────────────────────────────────────────────────────────────────────────────────
// REDEMPTION CONFIRMATION TEMPLATE VARIABLES
// ─────────────────────────────────────────────────────────────────────────────────
// Scalars: Action, RequestedBy, Count, TotalNetCredited, ActionAt
// Lists:
//   Confirmations      — []map[string]interface{} — full records
//   ConfirmationIDs    — []string
//   ByEntityKPIs       — []map{group_name, count, total_net_credited}  grouped by initiation_entity_name
//   BySchemeKPIs       — []map{group_name, count, total_net_credited}  grouped by initiation_scheme_name

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

// redemptionAnyToFloat64 converts common numeric types returned by pgx to float64.
// Handles pgtype.Numeric via JSON round-trip fallback.
func redemptionAnyToFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return f
		}
	}
	// pgtype.Numeric and other types — JSON round-trip fallback
	if b, err := json.Marshal(v); err == nil {
		var f float64
		if err2 := json.Unmarshal(b, &f); err2 == nil {
			return f
		}
	}
	return 0
}

// redemptionStrField safely extracts a string field from a row map.
func redemptionStrField(row map[string]interface{}, key string) string {
	if v, ok := row[key].(string); ok {
		return v
	}
	return ""
}

// redemptionRowsToMaps normalises a nil slice to an empty slice for JSON.
func redemptionRowsToMaps(rows []map[string]interface{}) []map[string]interface{} {
	if rows == nil {
		return []map[string]interface{}{}
	}
	return rows
}

// ─────────────────────────────────────────────────────────────────────────────
// REDEMPTION INITIATION
// ─────────────────────────────────────────────────────────────────────────────

// RedemptionInitiationRow is one row from fetchRedemptionInitiationRows.
type RedemptionInitiationRow = map[string]interface{}

// RedemptionInitiationKPIRow is a grouped aggregate for redemption initiations.
type RedemptionInitiationKPIRow struct {
	GroupName   string  `json:"group_name"`
	Count       int     `json:"count"`
	TotalAmount float64 `json:"total_amount"`
}

// RedemptionInitiationNotifPayload is the top-level notification payload for redemption initiation events.
type RedemptionInitiationNotifPayload struct {
	Action      string  `json:"Action"`
	RequestedBy string  `json:"RequestedBy"`
	Count       int     `json:"Count"`
	TotalAmount float64 `json:"TotalAmount"`
	ActionAt    string  `json:"ActionAt"`

	Initiations   []RedemptionInitiationRow    `json:"Initiations"`
	InitiationIDs []string                     `json:"InitiationIDs"`
	ByEntityKPIs  []RedemptionInitiationKPIRow `json:"ByEntityKPIs"`
	BySchemeKPIs  []RedemptionInitiationKPIRow `json:"BySchemeKPIs"`
}

// ToMap converts RedemptionInitiationNotifPayload to map[string]interface{} for TriggerNotification.
func (p *RedemptionInitiationNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":        p.Action,
		"RequestedBy":   p.RequestedBy,
		"Count":         p.Count,
		"TotalAmount":   p.TotalAmount,
		"ActionAt":      p.ActionAt,
		"Initiations":   redemptionRowsToMaps(p.Initiations),
		"InitiationIDs": p.InitiationIDs,
		"ByEntityKPIs":  redemptionInitiationKPIToMaps(p.ByEntityKPIs),
		"BySchemeKPIs":  redemptionInitiationKPIToMaps(p.BySchemeKPIs),
	}
}

func redemptionInitiationKPIToMaps(rows []RedemptionInitiationKPIRow) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = map[string]interface{}{
			"group_name":   r.GroupName,
			"count":        r.Count,
			"total_amount": r.TotalAmount,
		}
	}
	return out
}

func computeRedemptionInitiationKPIs(rows []RedemptionInitiationRow, groupField string) []RedemptionInitiationKPIRow {
	groups := map[string]*RedemptionInitiationKPIRow{}
	for _, row := range rows {
		key := redemptionStrField(row, groupField)
		if key == "" {
			key = "(unknown)"
		}
		if _, ok := groups[key]; !ok {
			groups[key] = &RedemptionInitiationKPIRow{GroupName: key}
		}
		groups[key].Count++
		// by_amount is the primary amount for redemption initiations
		groups[key].TotalAmount += redemptionAnyToFloat64(row["by_amount"])
	}
	out := make([]RedemptionInitiationKPIRow, 0, len(groups))
	for _, kpi := range groups {
		out = append(out, *kpi)
	}
	return out
}

// BuildRedemptionInitiationNotifPayload constructs a rich notification payload for redemption initiation events.
// Calls fetchRedemptionInitiationRows (redemptionInitiation.go) with an ID filter — no SQL here.
func BuildRedemptionInitiationNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	initiationIDs []string,
	action string,
	requestedBy string,
) *RedemptionInitiationNotifPayload {
	p := &RedemptionInitiationNotifPayload{
		Action:        action,
		RequestedBy:   requestedBy,
		Count:         len(initiationIDs),
		ActionAt:      time.Now().Format(time.RFC3339),
		Initiations:   []RedemptionInitiationRow{},
		InitiationIDs: initiationIDs,
		ByEntityKPIs:  []RedemptionInitiationKPIRow{},
		BySchemeKPIs:  []RedemptionInitiationKPIRow{},
	}
	if len(initiationIDs) == 0 {
		return p
	}
	rows, err := fetchRedemptionInitiationRows(ctx, pool, initiationIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildRedemptionInitiationNotifPayload fetchRedemptionInitiationRows: %v\n", err)
		return p
	}
	p.Initiations = rows
	p.ByEntityKPIs = computeRedemptionInitiationKPIs(rows, "entity_name")
	p.BySchemeKPIs = computeRedemptionInitiationKPIs(rows, "scheme_name")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := redemptionStrField(row, "redemption_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	p.InitiationIDs = ids
	for _, row := range rows {
		p.TotalAmount += redemptionAnyToFloat64(row["by_amount"])
	}
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// REDEMPTION CONFIRMATION
// ─────────────────────────────────────────────────────────────────────────────

// RedemptionConfirmationRow is one row from fetchRedemptionConfirmationRows.
type RedemptionConfirmationRow = map[string]interface{}

// RedemptionConfirmationKPIRow is a grouped aggregate for redemption confirmations.
type RedemptionConfirmationKPIRow struct {
	GroupName        string  `json:"group_name"`
	Count            int     `json:"count"`
	TotalNetCredited float64 `json:"total_net_credited"`
}

// RedemptionConfirmationNotifPayload is the top-level notification payload for redemption confirmation events.
type RedemptionConfirmationNotifPayload struct {
	Action           string  `json:"Action"`
	RequestedBy      string  `json:"RequestedBy"`
	Count            int     `json:"Count"`
	TotalNetCredited float64 `json:"TotalNetCredited"`
	ActionAt         string  `json:"ActionAt"`

	Confirmations   []RedemptionConfirmationRow    `json:"Confirmations"`
	ConfirmationIDs []string                       `json:"ConfirmationIDs"`
	ByEntityKPIs    []RedemptionConfirmationKPIRow `json:"ByEntityKPIs"`
	BySchemeKPIs    []RedemptionConfirmationKPIRow `json:"BySchemeKPIs"`
}

// ToMap converts RedemptionConfirmationNotifPayload to map[string]interface{} for TriggerNotification.
func (p *RedemptionConfirmationNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":           p.Action,
		"RequestedBy":      p.RequestedBy,
		"Count":            p.Count,
		"TotalNetCredited": p.TotalNetCredited,
		"ActionAt":         p.ActionAt,
		"Confirmations":    redemptionRowsToMaps(p.Confirmations),
		"ConfirmationIDs":  p.ConfirmationIDs,
		"ByEntityKPIs":     redemptionConfirmationKPIToMaps(p.ByEntityKPIs),
		"BySchemeKPIs":     redemptionConfirmationKPIToMaps(p.BySchemeKPIs),
	}
}

func redemptionConfirmationKPIToMaps(rows []RedemptionConfirmationKPIRow) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = map[string]interface{}{
			"group_name":         r.GroupName,
			"count":              r.Count,
			"total_net_credited": r.TotalNetCredited,
		}
	}
	return out
}

func computeRedemptionConfirmationKPIs(rows []RedemptionConfirmationRow, groupField string) []RedemptionConfirmationKPIRow {
	groups := map[string]*RedemptionConfirmationKPIRow{}
	for _, row := range rows {
		key := redemptionStrField(row, groupField)
		if key == "" {
			key = "(unknown)"
		}
		if _, ok := groups[key]; !ok {
			groups[key] = &RedemptionConfirmationKPIRow{GroupName: key}
		}
		groups[key].Count++
		// net_credited is the primary amount for redemption confirmations
		groups[key].TotalNetCredited += redemptionAnyToFloat64(row["net_credited"])
	}
	out := make([]RedemptionConfirmationKPIRow, 0, len(groups))
	for _, kpi := range groups {
		out = append(out, *kpi)
	}
	return out
}

// BuildRedemptionConfirmationNotifPayload constructs a rich notification payload for redemption confirmation events.
// Calls fetchRedemptionConfirmationRows (redemptionConfirmation.go) with an ID filter — no SQL here.
func BuildRedemptionConfirmationNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	confirmationIDs []string,
	action string,
	requestedBy string,
) *RedemptionConfirmationNotifPayload {
	p := &RedemptionConfirmationNotifPayload{
		Action:           action,
		RequestedBy:      requestedBy,
		Count:            len(confirmationIDs),
		ActionAt:         time.Now().Format(time.RFC3339),
		Confirmations:    []RedemptionConfirmationRow{},
		ConfirmationIDs:  confirmationIDs,
		ByEntityKPIs:     []RedemptionConfirmationKPIRow{},
		BySchemeKPIs:     []RedemptionConfirmationKPIRow{},
	}
	if len(confirmationIDs) == 0 {
		return p
	}
	rows, err := fetchRedemptionConfirmationRows(ctx, pool, confirmationIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildRedemptionConfirmationNotifPayload fetchRedemptionConfirmationRows: %v\n", err)
		return p
	}
	p.Confirmations = rows
	p.ByEntityKPIs = computeRedemptionConfirmationKPIs(rows, "initiation_entity_name")
	p.BySchemeKPIs = computeRedemptionConfirmationKPIs(rows, "initiation_scheme_name")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := redemptionStrField(row, "redemption_confirm_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	p.ConfirmationIDs = ids
	for _, row := range rows {
		p.TotalNetCredited += redemptionAnyToFloat64(row["net_credited"])
	}
	return p
}
