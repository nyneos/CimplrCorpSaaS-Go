package bankstatement

// ═══════════════════════════════════════════════════════════════════════════════
// AI Rule Manager Endpoints
//
// POST /cash/smart-cat/rules/ai-suggested  — list AI_SUGGESTED rules
// POST /cash/smart-cat/rule/approve        — activate an AI_SUGGESTED rule
// POST /cash/smart-cat/rule/reject         — reject (soft-delete) an AI_SUGGESTED rule
//
// Only AI_SUGGESTED rules are managed here. ACTIVE/INACTIVE rules are managed
// via the existing category rules admin screens.
// ═══════════════════════════════════════════════════════════════════════════════

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────
// List AI-Suggested Rules
// POST /cash/smart-cat/rules/ai-suggested
// ─────────────────────────────────────────────────────────────

func ListAISuggestedRulesHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		type ruleRow struct {
			RuleID       int64      `json:"rule_id"`
			RuleName     string     `json:"rule_name"`
			CategoryID   string     `json:"category_id"`
			CategoryName string     `json:"category_name"`
			AIReasoning  string     `json:"ai_reasoning"`
			ScopeType    string     `json:"scope_type"`
			SuggestedAt  *time.Time `json:"suggested_at"`
			RuleStatus   string     `json:"rule_status"`
		}

		rows, err := pool.Query(r.Context(), `
			SELECT r.rule_id,
			       r.rule_name,
			       r.category_id::text,
			       COALESCE(mc.category_name, ''),
			       COALESCE(r.ai_reasoning, ''),
			       COALESCE(rs.scope_type, 'GLOBAL'),
			       r.suggested_at,
			       r.rule_status
			FROM cimplrcorpsaas.category_rules r
			LEFT JOIN public.mastercashflowcategory mc ON mc.category_id::text = r.category_id::text
			LEFT JOIN cimplrcorpsaas.rule_scope rs ON rs.scope_id = r.scope_id
			WHERE r.rule_status = 'AI_SUGGESTED'
			  AND r.is_active = FALSE
			  AND COALESCE(r.is_deleted, false) = false
			ORDER BY r.suggested_at DESC NULLS LAST
			LIMIT 200
		`)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, err.Error(), "")
			return
		}
		defer rows.Close()

		var result []ruleRow
		for rows.Next() {
			var rr ruleRow
			if err := rows.Scan(
				&rr.RuleID, &rr.RuleName, &rr.CategoryID, &rr.CategoryName,
				&rr.AIReasoning, &rr.ScopeType, &rr.SuggestedAt, &rr.RuleStatus,
			); err != nil {
				continue
			}
			result = append(result, rr)
		}
		if result == nil {
			result = []ruleRow{}
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"rules": result,
		})
	})
}

// ─────────────────────────────────────────────────────────────
// Approve AI-Suggested Rule
// POST /cash/smart-cat/rule/approve
// ─────────────────────────────────────────────────────────────

func ApproveAIRuleHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			RuleID int64  `json:"rule_id"`
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RuleID == 0 {
			writeErrJSON(w, "rule_id is required", http.StatusBadRequest)
			return
		}

		tag, err := pool.Exec(r.Context(), `
			UPDATE cimplrcorpsaas.category_rules
			SET is_active   = TRUE,
			    rule_status = 'ACTIVE'
			WHERE rule_id   = $1
			  AND rule_status = 'AI_SUGGESTED'
		`, req.RuleID)
		if err != nil {
			writeErrJSON(w, "db error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if tag.RowsAffected() == 0 {
			writeErrJSON(w, "rule not found or already processed", http.StatusNotFound)
			return
		}

		// Also activate all components of this rule
		if _, err := pool.Exec(r.Context(), `
			UPDATE cimplrcorpsaas.category_rule_components
			SET is_active = TRUE
			WHERE rule_id = $1
		`, req.RuleID); err != nil {
			writeErrJSON(w, "failed to activate rule components: "+err.Error(), http.StatusInternalServerError)
			return
		}

		api.RespondEnvelopeSuccess(w, "Rule approved and activated", nil)
	})
}

// ─────────────────────────────────────────────────────────────
// Reject AI-Suggested Rule
// POST /cash/smart-cat/rule/reject
// ─────────────────────────────────────────────────────────────

func RejectAIRuleHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			RuleID int64  `json:"rule_id"`
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RuleID == 0 {
			writeErrJSON(w, "rule_id is required", http.StatusBadRequest)
			return
		}

		tag, err := pool.Exec(r.Context(), `
			UPDATE cimplrcorpsaas.category_rules
			SET rule_status = 'REJECTED',
			    is_active   = FALSE
			WHERE rule_id   = $1
			  AND rule_status = 'AI_SUGGESTED'
		`, req.RuleID)
		if err != nil {
			writeErrJSON(w, "db error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if tag.RowsAffected() == 0 {
			writeErrJSON(w, "rule not found or already processed", http.StatusNotFound)
			return
		}

		api.RespondEnvelopeSuccess(w, "Rule rejected", nil)
	})
}
