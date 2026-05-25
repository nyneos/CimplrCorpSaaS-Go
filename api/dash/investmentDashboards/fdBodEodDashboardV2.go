// Package investmentdashboards — Role-based BOD/EOD Control Dashboard (V2).
//
// POST /dash/investment/fd/bod-eod-dashboard/v2
//
// Layer on top of v1 (fdBodEodDashboard.go) that:
//   - Resolves persona (CFO/TREASURY/OPERATIONS/AUDIT/OPERATOR) from user_id
//     via cimplr.fd_dashboard_role_map (admin override supported)
//   - Reuses common aggregates from v1-style SQL (maturities, confirmations,
//     accrual, postings, receipts, exceptions, action_list, sla_breach, bank
//     concentration)
//   - Adds NEW sub-queries:
//       * bank_contacts          (cimplr.fd_bod_eod_bank_contact + auto-derived)
//       * offers_expiring_today  (investment.fd_bank_rate_negotiation; safe fallback)
//       * handover_notes         (cimplr.fd_bod_eod_handover_note)
//       * audit_today            (latest audit rows across FD modules)
//       * overrides_today        (investment.fd_accrual_ledger override entries)
//       * closing_readiness      (lightweight period-close summary)
//       * sign_off_status        (cimplr.fd_bod_eod_run header)
//   - Drives a template-aware checklist via cimplr.fd_bod_eod_checklist_template
//     + cimplr.fd_bod_eod_sign_off (with graceful fallback to v1 hardcoded list)
//   - Optionally narrows action_list / bank_contacts / handover_notes to the
//     caller when scope=MINE (user-aware "My items only" toggle)
//
// All sub-computations run concurrently. Persona only decides which slices to
// surface in the response — every persona always gets the common KPI bag so
// the top of the page never feels empty.
package investmentdashboards

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── request type ─────────────────────────────────────────────────────────────

type fdBodEodDashV2Request struct {
	UserID       string `json:"user_id"`
	EntityID     string `json:"entity_id"`
	Currency     string `json:"currency"`
	Mode         string `json:"mode"`       // BOD | EOD (default BOD)
	Period       string `json:"period"`     // Today | This Month | ...
	StartDate    string `json:"start_date"` // optional custom range
	EndDate      string `json:"end_date"`
	RoleOverride string `json:"role_override"` // admin-only persona override
	Scope        string `json:"scope"`         // ENTITY (default) | MINE
}

// ─── handler ──────────────────────────────────────────────────────────────────

// GetFDBodEodDashboardV2 returns the persona-aware BOD/EOD payload.
func GetFDBodEodDashboardV2(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req fdBodEodDashV2Request
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Currency == "" {
			req.Currency = "INR"
		}
		req.Mode = normalizeMode(req.Mode)
		if strings.ToUpper(strings.TrimSpace(req.Scope)) == "MINE" {
			req.Scope = "MINE"
		} else {
			req.Scope = "ENTITY"
		}

		ctx := r.Context()
		now := time.Now().UTC()
		periodBounds := resolveFDPeriodBounds(req.Period, req.StartDate, req.EndDate, now)
		today := now.Format(constants.DateFormat)
		threeDaysOut := now.AddDate(0, 0, 3).Format(constants.DateFormat)
		entityFilter := req.EntityID

		// Resolve caller. User_id can come from body (nos auto-attaches) or ctx.
		userID := strings.TrimSpace(req.UserID)
		if userID == "" {
			userID = middlewares.GetUserIDFromContext(ctx)
		}
		isAdmin, _ := ctx.Value("is_admin_override").(bool)
		persona := resolvePersonaForUser(ctx, pool, userID, req.RoleOverride, isAdmin)

		// MINE scope only narrows owner-bearing tables; aggregates remain entity-wide.
		ownerFilter := ""
		if req.Scope == "MINE" {
			ownerFilter = userID
		}

		// ─── concurrent sub-computations ────────────────────────────────────
		type subResult struct {
			data interface{}
			err  error
		}
		results := make(map[string]subResult, 20)
		var mu sync.Mutex
		var wg sync.WaitGroup
		run := func(key string, fn func(context.Context) (interface{}, error)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, e := fn(ctx)
				mu.Lock()
				results[key] = subResult{d, e}
				mu.Unlock()
			}()
		}

		// 1. Maturities today (kept lean — full row schema in v1).
		run("maturities_today", func(ctx context.Context) (interface{}, error) {
			return queryMaturitiesToday(ctx, pool, today, entityFilter)
		})
		// 2. Maturities in next 3 days.
		run("maturities_3days", func(ctx context.Context) (interface{}, error) {
			return queryMaturities3Days(ctx, pool, today, threeDaysOut, entityFilter)
		})
		// 3. Confirmations due (SLA aging).
		run("confirmations_due", func(ctx context.Context) (interface{}, error) {
			return queryConfirmationsDue(ctx, pool, entityFilter)
		})
		// 4. Accrual scheduled today.
		run("accrual_scheduled", func(ctx context.Context) (interface{}, error) {
			return queryAccrualScheduled(ctx, pool, today, entityFilter)
		})
		// 5. Expected interest today.
		run("expected_interest", func(ctx context.Context) (interface{}, error) {
			return queryExpectedInterest(ctx, pool, today, entityFilter)
		})
		// 6. Action list (optionally narrowed to caller).
		run("action_list", func(ctx context.Context) (interface{}, error) {
			return queryActionList(ctx, pool, entityFilter, ownerFilter)
		})
		// 7. Yesterday SLA breaches.
		run("sla_breach_yesterday", func(ctx context.Context) (interface{}, error) {
			return querySlaBreachYesterday(ctx, pool, entityFilter)
		})
		// 8. Bookings today.
		run("bookings_today", func(ctx context.Context) (interface{}, error) {
			return queryBookingsToday(ctx, pool, today, entityFilter)
		})
		// 9. Confirmations captured today.
		run("confirmations_today", func(ctx context.Context) (interface{}, error) {
			return queryConfirmationsToday(ctx, pool, today, entityFilter)
		})
		// 10. Receipts ingested today.
		run("receipts_today", func(ctx context.Context) (interface{}, error) {
			return queryReceiptsToday(ctx, pool, today, entityFilter)
		})
		// 11. Exceptions opened/closed today.
		run("exceptions_today", func(ctx context.Context) (interface{}, error) {
			return queryExceptionsToday(ctx, pool, today, entityFilter)
		})
		// 12. GL postings today.
		run("posting_today", func(ctx context.Context) (interface{}, error) {
			return queryPostingToday(ctx, pool, today, entityFilter)
		})
		// 13. Latest accrual run.
		run("accrual_run_latest", func(ctx context.Context) (interface{}, error) {
			return queryAccrualRunLatest(ctx, pool, entityFilter)
		})
		// 14. Bank concentration.
		run("bank_concentration", func(ctx context.Context) (interface{}, error) {
			return queryBankConcentration(ctx, pool, entityFilter)
		})

		// ── NEW: manual + derived bank-contact targets ───────────────────────
		run("bank_contacts", func(ctx context.Context) (interface{}, error) {
			return queryBankContacts(ctx, pool, entityFilter, ownerFilter)
		})
		// ── NEW: rate-offer expiry watch ─────────────────────────────────────
		run("offers_expiring_today", func(ctx context.Context) (interface{}, error) {
			return queryOffersExpiringToday(ctx, pool, today, entityFilter)
		})
		// ── NEW: handover notes ──────────────────────────────────────────────
		run("handover_notes", func(ctx context.Context) (interface{}, error) {
			return queryHandoverNotes(ctx, pool, entityFilter, today, req.Mode, ownerFilter)
		})
		// ── NEW: audit log for today ─────────────────────────────────────────
		run("audit_today", func(ctx context.Context) (interface{}, error) {
			return queryAuditToday(ctx, pool, today, entityFilter)
		})
		// ── NEW: accrual overrides today ─────────────────────────────────────
		run("overrides_today", func(ctx context.Context) (interface{}, error) {
			return queryOverridesToday(ctx, pool, today, entityFilter)
		})
		// ── NEW: closing readiness summary (lightweight) ─────────────────────
		run("closing_readiness", func(ctx context.Context) (interface{}, error) {
			return queryClosingReadiness(ctx, pool, entityFilter)
		})
		// ── NEW: sign-off run header ─────────────────────────────────────────
		run("sign_off_status", func(ctx context.Context) (interface{}, error) {
			return queryRunHeader(ctx, pool, entityFilter, today, req.Mode)
		})

		wg.Wait()

		get := func(key string) interface{} {
			if r, ok := results[key]; ok && r.err == nil {
				return r.data
			}
			return nil
		}

		// ─── common KPI bag (always present) ────────────────────────────────
		commonKPIs := buildBodEodCommonKPIs(get)

		// ─── persona-specific KPI overlay ───────────────────────────────────
		personaKPIs := buildBodEodPersonaKPIs(persona, get)

		// ─── checklist (template-driven, falls back gracefully) ─────────────
		metrics := buildChecklistMetricsBag(get)
		template := loadChecklistTemplate(ctx, pool, persona, req.Mode, entityFilter, today)
		checklist := buildChecklistPayload(template, metrics)

		// ─── persona-scoped table bundle ────────────────────────────────────
		tables := map[string]interface{}{}
		addIfShown := func(key string, value interface{}) {
			if personaShowsTable(persona, key) {
				tables[key] = value
			}
		}
		addIfShown("maturities_today", get("maturities_today"))
		addIfShown("maturities_3days", get("maturities_3days"))
		addIfShown("confirmations_due", get("confirmations_due"))
		addIfShown("accrual_scheduled", get("accrual_scheduled"))
		addIfShown("expected_interest", get("expected_interest"))
		addIfShown("action_list", get("action_list"))
		addIfShown("sla_breach_yesterday", get("sla_breach_yesterday"))
		addIfShown("bookings_today", get("bookings_today"))
		addIfShown("confirmations_today", get("confirmations_today"))
		addIfShown("receipts_today", get("receipts_today"))
		addIfShown("exceptions_today", get("exceptions_today"))
		addIfShown("posting_today", get("posting_today"))
		addIfShown("accrual_run_latest", get("accrual_run_latest"))
		addIfShown("bank_concentration", get("bank_concentration"))
		addIfShown("bank_contacts", get("bank_contacts"))
		addIfShown("offers_expiring_today", get("offers_expiring_today"))
		addIfShown("handover_notes", get("handover_notes"))
		addIfShown("audit_today", get("audit_today"))
		addIfShown("overrides_today", get("overrides_today"))
		addIfShown("closing_readiness", get("closing_readiness"))

		payload := map[string]interface{}{
			"generated_at": now.Format(time.RFC3339),
			"as_of_date":   today,
			"persona":      persona,
			"scope":        req.Scope,
			"mode":         req.Mode,
			"filters": map[string]interface{}{
				"entity_id":  entityFilter,
				"currency":   req.Currency,
				"mode":       req.Mode,
				"period":     periodBounds.Period,
				"start_date": periodBounds.StartStr,
				"end_date":   periodBounds.EndStr,
			},
			"kpis": map[string]interface{}{
				"common":  commonKPIs,
				"persona": personaKPIs,
			},
			"checklist":       checklist,
			"sign_off_status": get("sign_off_status"),
			"tables":          tables,
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}

// ─── KPI bag builders ─────────────────────────────────────────────────────────

// buildBodEodCommonKPIs returns the always-present KPI block (BOD + EOD).
// Every persona sees these to keep the top of the page consistent.
func buildBodEodCommonKPIs(get func(string) interface{}) map[string]interface{} {
	matTodayCount, matTodayTotal := 0, 0.0
	if v := get("maturities_today"); v != nil {
		if m, ok := v.(map[string]interface{}); ok {
			if c, ok := m["count"].(int); ok {
				matTodayCount = c
			}
			if t, ok := m["total_principal"].(float64); ok {
				matTodayTotal = t
			}
		}
	}
	next3 := 0
	if v := get("maturities_3days"); v != nil {
		if m, ok := v.(map[string]interface{}); ok {
			if c, ok := m["count"].(int); ok {
				next3 = c
			}
		}
	}
	confDue, confOverdue := 0, 0
	if v := get("confirmations_due"); v != nil {
		if m, ok := v.(map[string]interface{}); ok {
			if c, ok := m["count"].(int); ok {
				confDue = c
			}
			if c, ok := m["overdue_count"].(int); ok {
				confOverdue = c
			}
		}
	}
	expectedInterest := 0.0
	if v := get("expected_interest"); v != nil {
		if m, ok := v.(map[string]interface{}); ok {
			if t, ok := m["total_expected"].(float64); ok {
				expectedInterest = t
			}
		}
	}
	return map[string]interface{}{
		"maturities_today_count":   matTodayCount,
		"maturities_today_total":   matTodayTotal,
		"maturities_next_3d_count": next3,
		"confirmations_due":        confDue,
		"confirmations_overdue":    confOverdue,
		"expected_interest_today":  expectedInterest,
		"accrual_runs_today":       countSlice(get("accrual_scheduled")),
		"sla_breaches_yesterday":   countSlice(get("sla_breach_yesterday")),
		"action_items_total":       countSlice(get("action_list")),
		// EOD half
		"bookings_today":         getNestedInt(get("bookings_today"), "count"),
		"bookings_today_amount":  getNestedFloat(get("bookings_today"), "total_amount"),
		"confirmations_today":    getNestedInt(get("confirmations_today"), "count"),
		"receipts_ingested":      getNestedInt64(get("receipts_today"), "ingested"),
		"receipts_matched":       getNestedInt64(get("receipts_today"), "matched"),
		"receipts_unmatched":     getNestedInt64(get("receipts_today"), "unmatched"),
		"receipt_match_rate_pct": getNestedFloat(get("receipts_today"), "match_rate_pct"),
		"exceptions_opened":      getNestedInt64(get("exceptions_today"), "opened"),
		"exceptions_closed":      getNestedInt64(get("exceptions_today"), "closed"),
		"gl_postings_success":    getNestedInt64(get("posting_today"), "posted"),
		"gl_postings_failed":     getNestedInt64(get("posting_today"), "failed"),
	}
}

// buildBodEodPersonaKPIs returns a small KPI overlay tuned to the persona.
func buildBodEodPersonaKPIs(persona string, get func(string) interface{}) map[string]interface{} {
	switch persona {
	case PersonaCFO:
		return map[string]interface{}{
			"closing_readiness_pct": getNestedFloat(get("closing_readiness"), "pct"),
			"closing_blockers":      getNestedInt(get("closing_readiness"), "blockers"),
			"bank_concentration":    getNestedFloat(get("bank_concentration"), "grand_total"),
			"audit_today":           countSlice(get("audit_today")),
		}
	case PersonaTreasury:
		return map[string]interface{}{
			"offers_expiring_today": countSlice(get("offers_expiring_today")),
			"bank_contacts_open":    countSlice(get("bank_contacts")),
			"maturities_3d":         getNestedInt(get("maturities_3days"), "count"),
		}
	case PersonaOperations:
		return map[string]interface{}{
			"action_items_total": countSlice(get("action_list")),
			"posting_failed":     getNestedInt64(get("posting_today"), "failed"),
			"receipts_unmatched": getNestedInt64(get("receipts_today"), "unmatched"),
			"sla_breached_y":     countSlice(get("sla_breach_yesterday")),
		}
	case PersonaAudit:
		return map[string]interface{}{
			"audit_records_today": countSlice(get("audit_today")),
			"overrides_today":     countSlice(get("overrides_today")),
			"failed_postings":     getNestedInt64(get("posting_today"), "failed"),
			"open_exceptions":     getNestedInt64(get("exceptions_today"), "opened"),
		}
	default:
		return map[string]interface{}{
			"handover_notes": countSlice(get("handover_notes")),
		}
	}
}

// buildChecklistMetricsBag converts the sub-results into a flat metric map keyed
// by the same `source_metric` strings stored in cimplr.fd_bod_eod_checklist_template.
// This is the bridge that lets non-developers edit checklist behaviour by row.
func buildChecklistMetricsBag(get func(string) interface{}) map[string]interface{} {
	return map[string]interface{}{
		"receipts_unmatched":      getNestedInt64(get("receipts_today"), "unmatched"),
		"open_variance":           getNestedInt64(get("exceptions_today"), "opened"),
		"failed_postings":         getNestedInt64(get("posting_today"), "failed"),
		"critical_exceptions":     getNestedInt64(get("exceptions_today"), "opened"),
		"accrual_run_status":      getNestedString(get("accrual_run_latest"), "run_status"),
		"pending_maturities":      getNestedInt(get("maturities_today"), "count"),
		"maturities_today_count":  getNestedInt(get("maturities_today"), "count"),
		"confirmations_overdue":   getNestedInt(get("confirmations_due"), "overdue_count"),
		"accrual_runs_today":      countSlice(get("accrual_scheduled")),
		"expected_interest_today": getNestedFloat(get("expected_interest"), "total_expected"),
		"sla_breaches_yesterday":  countSlice(get("sla_breach_yesterday")),
	}
}

// ─── small typed helpers (missing in v1 helpers.go) ───────────────────────────

func getNestedInt(v interface{}, key string) int {
	if v == nil {
		return 0
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return 0
	}
	switch val := m[key].(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	}
	return 0
}

func getNestedString(v interface{}, key string) string {
	if v == nil {
		return ""
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return ""
	}
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}
