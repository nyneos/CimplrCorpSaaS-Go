package investmentsuite

// notif_payload.go — Rich notification payload builders for investment suite events
//
// DESIGN PHILOSOPHY
// ─────────────────
// Each Build<Domain>NotifPayload function calls the shared fetch*Rows helper
// (defined in the respective GET handler file) with an ID filter instead of
// duplicating SQL here. All SQL lives in exactly ONE place per domain.
//
// USAGE PATTERN
// ─────────────
//   payload := BuildProposalNotifPayload(ctx, pool, ids, "CREATE", requestedBy)
//   go catalog.TriggerNotification(ctx, pool, route, correlationID, payload.ToMap())
//
// ─────────────────────────────────────────────────────────────────────────────────
// PROPOSAL TEMPLATE VARIABLES
// ─────────────────────────────────────────────────────────────────────────────────
// Scalars: Action, RequestedBy, Count, TotalAmount, ActionAt
// Lists:
//   Proposals       — []map[string]interface{} — full records
//   ProposalIDs     — []string
//   ByEntityKPIs    — []map{group_name, count, total_amount}  grouped by entity_name
//   BySourceKPIs    — []map{group_name, count, total_amount}  grouped by source
//
// ─────────────────────────────────────────────────────────────────────────────────
// INITIATION TEMPLATE VARIABLES
// ─────────────────────────────────────────────────────────────────────────────────
// Scalars: Action, RequestedBy, Count, TotalAmount, ActionAt
// Lists:
//   Initiations     — []map[string]interface{} — full records
//   InitiationIDs   — []string
//   ByEntityKPIs    — []map{group_name, count, total_amount}  grouped by entity_name
//   BySchemeKPIs    — []map{group_name, count, total_amount}  grouped by scheme_name
//
// ─────────────────────────────────────────────────────────────────────────────────
// CONFIRMATION TEMPLATE VARIABLES
// ─────────────────────────────────────────────────────────────────────────────────
// Scalars: Action, RequestedBy, Count, TotalNetAmount, ActionAt
// Lists:
//   Confirmations      — []map[string]interface{} — full records
//   ConfirmationIDs    — []string
//   ByEntityKPIs       — []map{group_name, count, total_net_amount}  grouped by initiation_entity_name
//   BySchemeKPIs       — []map{group_name, count, total_net_amount}  grouped by initiation_scheme_name

import (
	"CimplrCorpSaas/api/constants"
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

// investAnyToFloat64 converts common numeric types returned by pgx to float64.
// Handles pgtype.Numeric via JSON round-trip fallback.
func investAnyToFloat64(v interface{}) float64 {
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

// strField safely extracts a string field from a row map.
func strField(row map[string]interface{}, key string) string {
	if v, ok := row[key].(string); ok {
		return v
	}
	return ""
}

// ─────────────────────────────────────────────────────────────────────────────
// PROPOSALS
// ─────────────────────────────────────────────────────────────────────────────

// ProposalRow represents one proposal record with all fields from fetchProposalRows.
type ProposalRow = map[string]interface{}

// ProposalKPIRow is a grouped aggregate for proposals.
type ProposalKPIRow struct {
	GroupName   string  `json:"group_name"`
	Count       int     `json:"count"`
	TotalAmount float64 `json:"total_amount"`
}

// ProposalNotifPayload is the top-level notification payload for proposal events.
type ProposalNotifPayload struct {
	Action      string  `json:"Action"`
	RequestedBy string  `json:"RequestedBy"`
	Count       int     `json:"Count"`
	TotalAmount float64 `json:"TotalAmount"`
	ActionAt    string  `json:"ActionAt"`

	Proposals    []ProposalRow    `json:"Proposals"`
	ProposalIDs  []string         `json:"ProposalIDs"`
	ByEntityKPIs []ProposalKPIRow `json:"ByEntityKPIs"`
	BySourceKPIs []ProposalKPIRow `json:"BySourceKPIs"`
}

// ToMap converts ProposalNotifPayload to map[string]interface{} for TriggerNotification.
func (p *ProposalNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":       p.Action,
		"RequestedBy":  p.RequestedBy,
		"Count":        p.Count,
		"TotalAmount":  p.TotalAmount,
		"ActionAt":     p.ActionAt,
		"Proposals":    rowsToMaps(p.Proposals),
		"ProposalIDs":  p.ProposalIDs,
		"ByEntityKPIs": proposalKPIToMaps(p.ByEntityKPIs),
		"BySourceKPIs": proposalKPIToMaps(p.BySourceKPIs),
	}
}

func rowsToMaps(rows []map[string]interface{}) []map[string]interface{} {
	if rows == nil {
		return []map[string]interface{}{}
	}
	return rows
}

func proposalKPIToMaps(rows []ProposalKPIRow) []map[string]interface{} {
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

func computeProposalKPIs(rows []ProposalRow, groupField string) []ProposalKPIRow {
	groups := map[string]*ProposalKPIRow{}
	for _, row := range rows {
		key := strField(row, groupField)
		if key == "" {
			key = constants.Unknown
		}
		if _, ok := groups[key]; !ok {
			groups[key] = &ProposalKPIRow{GroupName: key}
		}
		groups[key].Count++
		groups[key].TotalAmount += investAnyToFloat64(row["total_amount"])
	}
	out := make([]ProposalKPIRow, 0, len(groups))
	for _, kpi := range groups {
		out = append(out, *kpi)
	}
	return out
}

// BuildProposalNotifPayload constructs a rich notification payload for proposal events.
// Calls fetchProposalRows (investmentProposalCreation.go) with an ID filter — no SQL here.
func BuildProposalNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	proposalIDs []string,
	action string,
	requestedBy string,
) *ProposalNotifPayload {
	p := &ProposalNotifPayload{
		Action:       action,
		RequestedBy:  requestedBy,
		Count:        len(proposalIDs),
		ActionAt:     time.Now().Format(time.RFC3339),
		Proposals:    []ProposalRow{},
		ProposalIDs:  proposalIDs,
		ByEntityKPIs: []ProposalKPIRow{},
		BySourceKPIs: []ProposalKPIRow{},
	}
	if len(proposalIDs) == 0 {
		return p
	}
	rows, err := fetchProposalRows(ctx, pool, proposalIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildProposalNotifPayload fetchProposalRows: %v\n", err)
		return p
	}
	p.Proposals = rows
	p.ByEntityKPIs = computeProposalKPIs(rows, "entity_name")
	p.BySourceKPIs = computeProposalKPIs(rows, "source")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := strField(row, "proposal_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	p.ProposalIDs = ids
	for _, row := range rows {
		p.TotalAmount += investAnyToFloat64(row["total_amount"])
	}
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// INITIATIONS
// ─────────────────────────────────────────────────────────────────────────────

// InitiationRow represents one initiation record with all fields from fetchInitiationRows.
type InitiationRow = map[string]interface{}

// InitiationKPIRow is a grouped aggregate for initiations.
type InitiationKPIRow struct {
	GroupName   string  `json:"group_name"`
	Count       int     `json:"count"`
	TotalAmount float64 `json:"total_amount"`
}

// InitiationNotifPayload is the top-level notification payload for initiation events.
type InitiationNotifPayload struct {
	Action      string  `json:"Action"`
	RequestedBy string  `json:"RequestedBy"`
	Count       int     `json:"Count"`
	TotalAmount float64 `json:"TotalAmount"`
	ActionAt    string  `json:"ActionAt"`

	Initiations   []InitiationRow    `json:"Initiations"`
	InitiationIDs []string           `json:"InitiationIDs"`
	ByEntityKPIs  []InitiationKPIRow `json:"ByEntityKPIs"`
	BySchemeKPIs  []InitiationKPIRow `json:"BySchemeKPIs"`
}

// ToMap converts InitiationNotifPayload to map[string]interface{} for TriggerNotification.
func (p *InitiationNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":        p.Action,
		"RequestedBy":   p.RequestedBy,
		"Count":         p.Count,
		"TotalAmount":   p.TotalAmount,
		"ActionAt":      p.ActionAt,
		"Initiations":   rowsToMaps(p.Initiations),
		"InitiationIDs": p.InitiationIDs,
		"ByEntityKPIs":  initiationKPIToMaps(p.ByEntityKPIs),
		"BySchemeKPIs":  initiationKPIToMaps(p.BySchemeKPIs),
	}
}

func initiationKPIToMaps(rows []InitiationKPIRow) []map[string]interface{} {
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

func computeInitiationKPIs(rows []InitiationRow, groupField string) []InitiationKPIRow {
	groups := map[string]*InitiationKPIRow{}
	for _, row := range rows {
		key := strField(row, groupField)
		if key == "" {
			key = constants.Unknown
		}
		if _, ok := groups[key]; !ok {
			groups[key] = &InitiationKPIRow{GroupName: key}
		}
		groups[key].Count++
		groups[key].TotalAmount += investAnyToFloat64(row["amount"])
	}
	out := make([]InitiationKPIRow, 0, len(groups))
	for _, kpi := range groups {
		out = append(out, *kpi)
	}
	return out
}

// BuildInitiationNotifPayload constructs a rich notification payload for initiation events.
// Calls fetchInitiationRows (investmentInitiation.go) with an ID filter — no SQL here.
func BuildInitiationNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	initiationIDs []string,
	action string,
	requestedBy string,
) *InitiationNotifPayload {
	p := &InitiationNotifPayload{
		Action:        action,
		RequestedBy:   requestedBy,
		Count:         len(initiationIDs),
		ActionAt:      time.Now().Format(time.RFC3339),
		Initiations:   []InitiationRow{},
		InitiationIDs: initiationIDs,
		ByEntityKPIs:  []InitiationKPIRow{},
		BySchemeKPIs:  []InitiationKPIRow{},
	}
	if len(initiationIDs) == 0 {
		return p
	}
	rows, err := fetchInitiationRows(ctx, pool, initiationIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildInitiationNotifPayload fetchInitiationRows: %v\n", err)
		return p
	}
	p.Initiations = rows
	p.ByEntityKPIs = computeInitiationKPIs(rows, "entity_name")
	p.BySchemeKPIs = computeInitiationKPIs(rows, "scheme_name")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := strField(row, "initiation_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	p.InitiationIDs = ids
	for _, row := range rows {
		p.TotalAmount += investAnyToFloat64(row["amount"])
	}
	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// CONFIRMATIONS
// ─────────────────────────────────────────────────────────────────────────────

// ConfirmationRow represents one confirmation record with all fields from fetchConfirmationRows.
type ConfirmationRow = map[string]interface{}

// ConfirmationKPIRow is a grouped aggregate for confirmations.
type ConfirmationKPIRow struct {
	GroupName      string  `json:"group_name"`
	Count          int     `json:"count"`
	TotalNetAmount float64 `json:"total_net_amount"`
}

// ConfirmationNotifPayload is the top-level notification payload for confirmation events.
type ConfirmationNotifPayload struct {
	Action         string  `json:"Action"`
	RequestedBy    string  `json:"RequestedBy"`
	Count          int     `json:"Count"`
	TotalNetAmount float64 `json:"TotalNetAmount"`
	ActionAt       string  `json:"ActionAt"`

	Confirmations   []ConfirmationRow    `json:"Confirmations"`
	ConfirmationIDs []string             `json:"ConfirmationIDs"`
	ByEntityKPIs    []ConfirmationKPIRow `json:"ByEntityKPIs"`
	BySchemeKPIs    []ConfirmationKPIRow `json:"BySchemeKPIs"`
}

// ToMap converts ConfirmationNotifPayload to map[string]interface{} for TriggerNotification.
func (p *ConfirmationNotifPayload) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"Action":          p.Action,
		"RequestedBy":     p.RequestedBy,
		"Count":           p.Count,
		"TotalNetAmount":  p.TotalNetAmount,
		"ActionAt":        p.ActionAt,
		"Confirmations":   rowsToMaps(p.Confirmations),
		"ConfirmationIDs": p.ConfirmationIDs,
		"ByEntityKPIs":    confirmationKPIToMaps(p.ByEntityKPIs),
		"BySchemeKPIs":    confirmationKPIToMaps(p.BySchemeKPIs),
	}
}

func confirmationKPIToMaps(rows []ConfirmationKPIRow) []map[string]interface{} {
	out := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		out[i] = map[string]interface{}{
			"group_name":       r.GroupName,
			"count":            r.Count,
			"total_net_amount": r.TotalNetAmount,
		}
	}
	return out
}

func computeConfirmationKPIs(rows []ConfirmationRow, groupField string) []ConfirmationKPIRow {
	groups := map[string]*ConfirmationKPIRow{}
	for _, row := range rows {
		key := strField(row, groupField)
		if key == "" {
			key = constants.Unknown
		}
		if _, ok := groups[key]; !ok {
			groups[key] = &ConfirmationKPIRow{GroupName: key}
		}
		groups[key].Count++
		groups[key].TotalNetAmount += investAnyToFloat64(row["net_amount"])
	}
	out := make([]ConfirmationKPIRow, 0, len(groups))
	for _, kpi := range groups {
		out = append(out, *kpi)
	}
	return out
}

// BuildConfirmationNotifPayload constructs a rich notification payload for confirmation events.
// Calls fetchConfirmationRows (investmentConfirmation.go) with an ID filter — no SQL here.
func BuildConfirmationNotifPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	confirmationIDs []string,
	action string,
	requestedBy string,
) *ConfirmationNotifPayload {
	p := &ConfirmationNotifPayload{
		Action:          action,
		RequestedBy:     requestedBy,
		Count:           len(confirmationIDs),
		ActionAt:        time.Now().Format(time.RFC3339),
		Confirmations:   []ConfirmationRow{},
		ConfirmationIDs: confirmationIDs,
		ByEntityKPIs:    []ConfirmationKPIRow{},
		BySchemeKPIs:    []ConfirmationKPIRow{},
	}
	if len(confirmationIDs) == 0 {
		return p
	}
	rows, err := fetchConfirmationRows(ctx, pool, confirmationIDs)
	if err != nil {
		fmt.Printf("[ERROR] BuildConfirmationNotifPayload fetchConfirmationRows: %v\n", err)
		return p
	}
	p.Confirmations = rows
	p.ByEntityKPIs = computeConfirmationKPIs(rows, "initiation_entity_name")
	p.BySchemeKPIs = computeConfirmationKPIs(rows, "initiation_scheme_name")
	seen := map[string]bool{}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := strField(row, "confirmation_id"); id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	p.ConfirmationIDs = ids
	for _, row := range rows {
		p.TotalNetAmount += investAnyToFloat64(row["net_amount"])
	}
	return p
}
