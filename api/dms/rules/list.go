package rules

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type listReq struct {
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
	EntityID      string `json:"entity_id"`
	PendingOnly   bool   `json:"pending_only"`
}

// HandleList lists non-deleted rules, newest-edited first.
func HandleList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req listReq
		if r.ContentLength != 0 {
			if err := common.DecodeJSON(r, &req); err != nil {
				api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
				return
			}
		}
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.EntityID = strings.TrimSpace(req.EntityID)

		query := `
			SELECT rule_id::text, name, description, module_code, sub_module_code,
			       entity_id, entity_name,
			       status, processing_status, current_version_id::text,
			       COALESCE(created_by,''), created_at, COALESCE(last_modified_by,''), last_modified_at
			FROM dms_svc.generation_rule
			WHERE is_deleted = false`
		args := []interface{}{}
		if req.ModuleCode != "" {
			args = append(args, req.ModuleCode)
			query += " AND module_code = $" + strconv.Itoa(len(args))
		}
		if req.SubModuleCode != "" {
			args = append(args, req.SubModuleCode)
			query += " AND sub_module_code = $" + strconv.Itoa(len(args))
		}
		if req.EntityID != "" {
			args = append(args, req.EntityID)
			query += " AND (entity_id IS NULL OR entity_id = $" + strconv.Itoa(len(args)) + ")"
		}
		if req.PendingOnly {
			query += ` AND processing_status = ANY('{PENDING_APPROVAL,PENDING_EDIT_APPROVAL,PENDING_DELETE_APPROVAL}')`
		}
		query += " ORDER BY last_modified_at DESC"

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			api.LogErrorForResponse(w, "dms rule list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list rules", "DMS_RULE_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]ruleListItem, 0)
		for rows.Next() {
			var it ruleListItem
			var createdAt, lastModifiedAt time.Time
			var currentVersionID *string
			if err := rows.Scan(&it.RuleID, &it.Name, &it.Description, &it.ModuleCode, &it.SubModuleCode,
				&it.EntityID, &it.EntityName,
				&it.Status, &it.ProcessingStatus, &currentVersionID, &it.CreatedBy, &createdAt, &it.LastModifiedBy, &lastModifiedAt); err != nil {
				continue
			}
			it.CurrentVersionID = currentVersionID
			it.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			it.LastModifiedAt = lastModifiedAt.UTC().Format(time.RFC3339)
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Rules fetched", out)
	}
}
