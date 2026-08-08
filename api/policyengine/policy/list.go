package policy

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const errMsgListPolicies = "failed to list policies"

// ListItem is the full policy list view: policy_master header columns +
// array_agg'd trigger event codes / module codes + processing_status +
// audit metadata (Requested / Edited / Approved) for expand panels.
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
	// Audit metadata resolved per action type from policy_master_audit
	// (CREATE → created_*, EDIT → edited_*, DELETE → deleted_*), so a policy
	// that was never edited reports an empty edited_at instead of created_at.
	CreatedBy      string `json:"created_by"`
	CreatedAt      string `json:"created_at"`
	EditedBy       string `json:"edited_by"`
	EditedAt       string `json:"edited_at"`
	DeletedBy      string `json:"deleted_by"`
	DeletedAt      string `json:"deleted_at"`
	ApprovedBy     string `json:"approved_by"`
	ApprovedAt     string `json:"approved_at"`
	RequestedBy    string `json:"requested_by"`
	RequestedAt    string `json:"requested_at"`
	CheckerBy      string `json:"checker_by"`
	CheckerAt      string `json:"checker_at"`
	CheckerComment string `json:"checker_comment"`
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
`

// Latest audit row per action type, so Requested / Edited / Deleted stay
// distinct instead of all collapsing onto the master's last_modified_at.
const listAuditFrom = `
	LEFT JOIN LATERAL (
		SELECT a.requested_by, a.requested_at
		FROM policyengine_svc.policy_master_audit a
		WHERE a.policy_id = p.policy_id AND a.action_type = 'CREATE'
		ORDER BY a.requested_at DESC LIMIT 1
	) ac ON true
	LEFT JOIN LATERAL (
		SELECT a.requested_by, a.requested_at
		FROM policyengine_svc.policy_master_audit a
		WHERE a.policy_id = p.policy_id AND a.action_type = 'EDIT'
		ORDER BY a.requested_at DESC LIMIT 1
	) ae ON true
	LEFT JOIN LATERAL (
		SELECT a.requested_by, a.requested_at
		FROM policyengine_svc.policy_master_audit a
		WHERE a.policy_id = p.policy_id AND a.action_type = 'DELETE'
		ORDER BY a.requested_at DESC LIMIT 1
	) ad ON true
	LEFT JOIN LATERAL (
		SELECT a.requested_by, a.requested_at, a.checker_by, a.checker_at,
		       COALESCE(a.checker_comment, '') AS checker_comment
		FROM policyengine_svc.policy_master_audit a
		WHERE a.policy_id = p.policy_id
		  AND a.action_type IN ('CREATE', 'EDIT', 'DELETE')
		ORDER BY a.requested_at DESC
		LIMIT 1
	) ar ON true
`

const listWhere = `
	WHERE p.is_deleted = false`

const listSearchWhere = `
	AND (p.code ILIKE $1 OR p.name ILIKE $1 OR p.category ILIKE $1
	     OR EXISTS (
	         SELECT 1 FROM policyengine_svc.policy_module pm
	         WHERE pm.policy_id = p.policy_id AND pm.is_deleted = false AND pm.module_code ILIKE $1
	     )
	     OR EXISTS (
	         SELECT 1 FROM policyengine_svc.policy_sub_module psm
	         WHERE psm.policy_id = p.policy_id AND psm.is_deleted = false AND psm.sub_module_code ILIKE $1
	     ))`

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
		countQ := `SELECT COUNT(*) ` + listFrom + listWhere
		countArgs := []interface{}{}
		if search != "" {
			countQ += listSearchWhere
			countArgs = append(countArgs, search)
		}
		var total int
		if err := pool.QueryRow(ctx, countQ, countArgs...).Scan(&total); err != nil {
			api.LogErrorForResponse(w, "policy list count: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errMsgListPolicies, "POLICY_LIST_FAILED")
			return
		}

		listQ := `
			SELECT p.policy_id::text, p.code, p.name, p.category, p.validation_level, p.criticality,
			       p.action_on_breach, p.rule_type, p.status, p.processing_status, p.version,
			       p.effective_start::text, p.source,
			       COALESCE(t.trigger_events, ARRAY[]::varchar[]) AS trigger_events,
			       COALESCE(m.modules, ARRAY[]::varchar[]) AS modules,
			       COALESCE(sm.sub_modules, ARRAY[]::varchar[]) AS sub_modules,
			       COALESCE(ac.requested_by, COALESCE(p.created_by, '')), COALESCE(ac.requested_at, p.created_at),
			       COALESCE(ae.requested_by, ''), ae.requested_at,
			       COALESCE(ad.requested_by, ''), ad.requested_at,
			       COALESCE(p.approved_by, ''), p.approved_at,
			       COALESCE(ar.requested_by, COALESCE(p.created_by, '')),
			       COALESCE(ar.requested_at, p.created_at),
			       COALESCE(ar.checker_by, ''), ar.checker_at,
			       COALESCE(ar.checker_comment, '')` + listFrom + listAuditFrom + listWhere
		listArgs := []interface{}{}
		argN := 1
		if search != "" {
			listQ += listSearchWhere
			listArgs = append(listArgs, search)
			argN = 2
		}
		listQ += ` ` + common.PolicyListOrderBy + ` LIMIT $` + strconv.Itoa(argN) + ` OFFSET $` + strconv.Itoa(argN+1)
		listArgs = append(listArgs, pageSize, offset)

		rows, err := pool.Query(ctx, listQ, listArgs...)
		if err != nil {
			api.LogErrorForResponse(w, "policy list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errMsgListPolicies, "POLICY_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]ListItem, 0)
		for rows.Next() {
			var it ListItem
			var createdAt, editedAt, deletedAt, approvedAt, requestedAt, checkerAt *time.Time
			if err := rows.Scan(&it.PolicyID, &it.Code, &it.Name, &it.Category, &it.ValidationLevel, &it.Criticality,
				&it.ActionOnBreach, &it.RuleType, &it.Status, &it.ProcessingStatus, &it.Version,
				&it.EffectiveStart, &it.Source, &it.TriggerEvents, &it.Modules, &it.SubModules,
				&it.CreatedBy, &createdAt, &it.EditedBy, &editedAt,
				&it.DeletedBy, &deletedAt, &it.ApprovedBy, &approvedAt,
				&it.RequestedBy, &requestedAt, &it.CheckerBy, &checkerAt,
				&it.CheckerComment); err != nil {
				api.LogErrorForResponse(w, "policy list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, errMsgListPolicies, "POLICY_LIST_FAILED")
				return
			}
			it.CreatedAt = common.FormatAuditTime(createdAt)
			it.EditedAt = common.FormatAuditTime(editedAt)
			it.DeletedAt = common.FormatAuditTime(deletedAt)
			it.ApprovedAt = common.FormatAuditTime(approvedAt)
			it.RequestedAt = common.FormatAuditTime(requestedAt)
			it.CheckerAt = common.FormatAuditTime(checkerAt)
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
