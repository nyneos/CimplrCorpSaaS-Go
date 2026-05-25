// Package investmentdashboards — Persona resolution helpers for BOD/EOD V2.
//
// A "persona" is the operational role lens the dashboard renders in. It is
// resolved from the caller's user_id by joining public.user_roles → public.roles
// against cimplr.fd_dashboard_role_map. Admins (is_admin_override) may force a
// persona by sending role_override in the request body.
package investmentdashboards

import (
	"context"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Persona enum values stored in cimplr.fd_dashboard_role_map.persona.
const (
	PersonaCFO        = "CFO"
	PersonaTreasury   = "TREASURY"
	PersonaOperations = "OPERATIONS"
	PersonaAudit      = "AUDIT"
	PersonaOperator   = "OPERATOR"
)

var validPersonas = map[string]struct{}{
	PersonaCFO:        {},
	PersonaTreasury:   {},
	PersonaOperations: {},
	PersonaAudit:      {},
	PersonaOperator:   {},
}

// normalizePersona uppercases input and falls back to OPERATOR when unrecognised.
func normalizePersona(p string) string {
	p = strings.ToUpper(strings.TrimSpace(p))
	if _, ok := validPersonas[p]; ok {
		return p
	}
	return PersonaOperator
}

// resolvePersonaForUser maps user_id → persona via cimplr.fd_dashboard_role_map.
// Returns PersonaOperator when:
//   - userID is empty
//   - the user has no role mapping
//   - the role_map table is missing (graceful, never blocks the dashboard)
//
// `override` is honoured only when the caller is an admin override session.
func resolvePersonaForUser(
	ctx context.Context,
	pool *pgxpool.Pool,
	userID string,
	override string,
	isAdminOverride bool,
) string {
	if isAdminOverride && strings.TrimSpace(override) != "" {
		return normalizePersona(override)
	}
	if strings.TrimSpace(userID) == "" {
		return PersonaOperator
	}

	const q = `
		SELECT m.persona
		FROM cimplr.fd_dashboard_role_map m
		JOIN public.roles r
		  ON  (r.role_code = m.role_code OR r.rolecode = m.role_code OR r.name = m.role_code)
		JOIN public.user_roles ur
		  ON  ur.role_id = r.id
		WHERE ur.user_id = $1
		  AND COALESCE(ur.is_deleted, false) = false
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.priority ASC
		LIMIT 1`

	var persona string
	if err := pool.QueryRow(ctx, q, userID).Scan(&persona); err != nil {
		// Includes pgx.ErrNoRows and "relation does not exist" cases. Both
		// degrade silently to the default persona so the dashboard never
		// fails just because role mapping is missing.
		api.LogInfo("[BodEodDashV2] persona resolve: defaulting to OPERATOR (user=%s err=%v)", userID, err)
		return PersonaOperator
	}
	return normalizePersona(persona)
}

// personaShowsTable encodes which sub-computations are surfaced for each persona.
// Aggregate KPIs (kpis.common) are always returned; this gates the bigger tables
// so we never ship payload a persona will not render.
func personaShowsTable(persona, key string) bool {
	switch persona {
	case PersonaCFO:
		switch key {
		case "kpis", "checklist", "closing_readiness", "governance_summary",
			"bank_concentration", "expected_interest", "audit_today",
			"handover_notes", "maturities_today", "posting_today":
			return true
		}
	case PersonaTreasury:
		switch key {
		case "kpis", "checklist", "maturities_today", "maturities_3days",
			"confirmations_due", "bank_contacts", "offers_expiring_today",
			"expected_interest", "bank_concentration", "handover_notes":
			return true
		}
	case PersonaOperations:
		switch key {
		case "kpis", "checklist", "action_list", "sla_breach_yesterday",
			"confirmations_due", "bank_contacts", "exceptions_today",
			"posting_today", "receipts_today", "bookings_today",
			"confirmations_today", "accrual_run_latest", "handover_notes":
			return true
		}
	case PersonaAudit:
		switch key {
		case "kpis", "checklist", "audit_today", "overrides_today",
			"posting_today", "exceptions_today", "accrual_run_latest",
			"governance_summary":
			return true
		}
	case PersonaOperator:
		// Operator sees the full dashboard (parity with v1).
		return true
	}
	return false
}
