package policy

import (
	"encoding/json"
	"net/http"
	"strconv"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ListItem is the full policy list view: policy_master header columns +
// array_agg'd trigger event codes / module codes + processing_status.
type ListItem struct {
	PolicyID         string   `json:"policy_id"`
	Code             string   `json:"code"`
	Name             string   `json:"name"`
	Category         string   `json:"category"`
	ValidationLevel  string   `json:"validation_level"`
	Criticality      string   `json:"criticality"`
	ActionOnBreach   string   `json:"action_on_breach"`
	RuleType         string   `json:"rule_type"`
	Status           string   `json:"status"`
	ProcessingStatus string   `json:"processing_status"`
	Version          int      `json:"version"`
	EffectiveStart   string   `json:"effective_start"`
	Source           string   `json:"source"`
	TriggerEvents    []string `json:"trigger_events"`
	Modules          []string `json:"modules"`
	SubModules       []string `json:"sub_modules"`
}

const listFrom = `
	FROM policyengine_svc.policy_master p
	LEFT JOIN LATERAL (
		SELECT array_agg(pt.event_code ORDER BY pt.event_code) AS trigger_events
		FROM policyengine_svc.policy_trigger pt
		WHERE pt.policy_id = p.policy_id AND pt.is_deleted = false
	) t ON true
	LEFT JOIN LATERAL (
		SELECT array_agg(pmod.module_code ORDER BY pmod.module_code) AS modules
		FROM policyengine_svc.policy_module pmod
		WHERE pmod.policy_id = p.policy_id AND pmod.is_deleted = false
	) m ON true
	LEFT JOIN LATERAL (
		SELECT array_agg(psm.sub_module_code ORDER BY psm.sub_module_code) AS sub_modules
		FROM policyengine_svc.policy_sub_module psm
		WHERE psm.policy_id = p.policy_id AND psm.is_deleted = false
	) sm ON true
	WHERE p.is_deleted = false`

func HandleList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req common.PageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			// empty body → first page
			req = common.PageRequest{}
		}
		page, pageSize, offset := common.NormalizePage(req)
		search := common.SearchPattern(req.Search)

		ctx := r.Context()
		countQ := `SELECT COUNT(*) ` + listFrom
		countArgs := []interface{}{}
		if search != "" {
			countQ += ` AND (p.code ILIKE $1 OR p.name ILIKE $1 OR p.category ILIKE $1)`
			countArgs = append(countArgs, search)
		}
		var total int
		if err := pool.QueryRow(ctx, countQ, countArgs...).Scan(&total); err != nil {
			api.LogErrorForResponse(w, "policy list count: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list policies", "POLICY_LIST_FAILED")
			return
		}

		listQ := `
			SELECT p.policy_id::text, p.code, p.name, p.category, p.validation_level, p.criticality,
			       p.action_on_breach, p.rule_type, p.status, p.processing_status, p.version,
			       p.effective_start::text, p.source,
			       COALESCE(t.trigger_events, ARRAY[]::varchar[]) AS trigger_events,
			       COALESCE(m.modules, ARRAY[]::varchar[]) AS modules,
			       COALESCE(sm.sub_modules, ARRAY[]::varchar[]) AS sub_modules` + listFrom
		listArgs := []interface{}{}
		argN := 1
		if search != "" {
			listQ += ` AND (p.code ILIKE $1 OR p.name ILIKE $1 OR p.category ILIKE $1)`
			listArgs = append(listArgs, search)
			argN = 2
		}
		listQ += ` ` + common.PolicyListOrderBy + ` LIMIT $` + strconv.Itoa(argN) + ` OFFSET $` + strconv.Itoa(argN+1)
		listArgs = append(listArgs, pageSize, offset)

		rows, err := pool.Query(ctx, listQ, listArgs...)
		if err != nil {
			api.LogErrorForResponse(w, "policy list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list policies", "POLICY_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]ListItem, 0)
		for rows.Next() {
			var it ListItem
			if err := rows.Scan(&it.PolicyID, &it.Code, &it.Name, &it.Category, &it.ValidationLevel, &it.Criticality,
				&it.ActionOnBreach, &it.RuleType, &it.Status, &it.ProcessingStatus, &it.Version,
				&it.EffectiveStart, &it.Source, &it.TriggerEvents, &it.Modules, &it.SubModules); err != nil {
				api.LogErrorForResponse(w, "policy list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list policies", "POLICY_LIST_FAILED")
				return
			}
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Policies fetched", map[string]interface{}{
			"rows":      out,
			"total":     total,
			"page":      page,
			"page_size": pageSize,
		})
	}
}
