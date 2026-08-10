package rules

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

type createReq struct {
	Name                    string                `json:"name"`
	Description             string                `json:"description"`
	ModuleCode              string                `json:"module_code"`
	SubModuleCode           string                `json:"sub_module_code"`
	EntityID                string                `json:"entity_id"`
	EntityName              string                `json:"entity_name"`
	TimeWindowType          string                `json:"time_window_type"`
	TimeWindowValue         *int                  `json:"time_window_value"`
	TimeWindowUnit          string                `json:"time_window_unit"`
	CustomStart             string                `json:"custom_start"`
	CustomEnd               string                `json:"custom_end"`
	ScheduleType            string                `json:"schedule_type"`
	CronExpr                string                `json:"cron_expr"`
	RepeatKind              string                `json:"repeat_kind"`
	ScheduleTime            string                `json:"schedule_time"`
	ScheduleWeekday         *int                  `json:"schedule_weekday"`
	ScheduleMonthDay        *int                  `json:"schedule_month_day"`
	ScheduleTimezone        string                `json:"schedule_timezone"`
	RowExpandMode           string                `json:"row_expand_mode"` // FIRST_ROW | PER_ROW
	DataRowFrom             *int                  `json:"data_row_from"` // 1-based inclusive; default 1
	DataRowTo               *int                  `json:"data_row_to"`   // 1-based inclusive; default 500
	Filters                 []filterReq           `json:"filters"`
	Attachments             []attachmentReq       `json:"attachments"`
	Destinations            []destinationReq      `json:"destinations"`
	EmailRecipients         []emailRecipientReq   `json:"email_recipients"`
	BankAccountScope        []bankAccountScopeReq `json:"bank_account_scope"`
	NotificationTemplateIDs []string              `json:"notification_template_ids"`
	Triggers                []triggerReq          `json:"triggers"`
	ActorID                 string                `json:"actor_id"`
}

// HandleCreate raises a new rule (+ its first version) for approval.
// Stage-then-apply, same pattern as templates/create.go.
func HandleCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req createReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.EntityID = strings.TrimSpace(req.EntityID)
		req.EntityName = strings.TrimSpace(req.EntityName)
		req.TimeWindowType = strings.TrimSpace(strings.ToUpper(req.TimeWindowType))
		req.ScheduleType = strings.TrimSpace(strings.ToUpper(req.ScheduleType))
		if req.ScheduleType == "" {
			req.ScheduleType = "MANUAL"
		}
		req.RepeatKind = strings.TrimSpace(strings.ToUpper(req.RepeatKind))
		if req.RepeatKind == "" {
			if req.ScheduleType == "MANUAL" {
				req.RepeatKind = "MANUAL"
			} else {
				req.RepeatKind = "CUSTOM"
			}
		}
		if req.ScheduleTimezone == "" {
			req.ScheduleTimezone = "Asia/Kolkata"
		}
		req.RowExpandMode = strings.TrimSpace(strings.ToUpper(req.RowExpandMode))
		if req.RowExpandMode == "" {
			req.RowExpandMode = "FIRST_ROW"
		}
		if req.RowExpandMode != "FIRST_ROW" && req.RowExpandMode != "PER_ROW" && req.RowExpandMode != "PER_PAGE_IN_ONE_DOC" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "row_expand_mode must be FIRST_ROW, PER_ROW, or PER_PAGE_IN_ONE_DOC", "VALIDATION_ERROR")
			return
		}
		rowFrom, rowTo, ok := normalizeDataRowRangePtr(req.DataRowFrom, req.DataRowTo)
		if !ok {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "data_row_from/to must be 1–2000 with from ≤ to", "VALIDATION_ERROR")
			return
		}
		if req.Name == "" || req.ModuleCode == "" || req.SubModuleCode == "" || req.TimeWindowType == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "name, module_code, sub_module_code and time_window_type are required", "VALIDATION_ERROR")
			return
		}
		if (req.EntityID == "") != (req.EntityName == "") {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "entity_id and entity_name must both be set or both empty", "VALIDATION_ERROR")
			return
		}
		scope := ctxutil.FromContext(r.Context())
		if !common.RequireEntityAccess(w, scope, req.EntityID) {
			return
		}
		if len(req.Attachments) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "at least one document attachment is required", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms rule create begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create rule", "DMS_RULE_CREATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var ruleID string
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO dms_svc.generation_rule (name, description, module_code, sub_module_code, entity_id, entity_name, status, processing_status, created_by)
			VALUES ($1, $2, $3, $4, NULLIF($5,''), NULLIF($6,''), 'PendingApproval', 'PENDING_APPROVAL', $7)
			RETURNING rule_id::text`,
			req.Name, req.Description, req.ModuleCode, req.SubModuleCode, req.EntityID, req.EntityName, actor,
		).Scan(&ruleID); err != nil {
			api.LogErrorForResponse(w, "dms rule create insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusConflict, "failed to create rule (duplicate name, or unknown module/sub_module?)", "DMS_RULE_CREATE_FAILED")
			return
		}

		var versionID string
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO dms_svc.generation_rule_version
				(rule_id, version_no, time_window_type, time_window_value, time_window_unit,
				 custom_start, custom_end, schedule_type, cron_expr, row_expand_mode,
				 data_row_from, data_row_to,
				 repeat_kind, schedule_time, schedule_weekday, schedule_month_day, schedule_timezone,
				 status, created_by)
			VALUES ($1::uuid, 1, $2, $3, NULLIF($4,''), NULLIF($5,'')::date, NULLIF($6,'')::date,
			        $7, NULLIF($8,''), $9, $10, $11, $12, NULLIF($13,'')::time, $14, $15, $16,
			        'PENDING_APPROVAL', $17)
			RETURNING version_id::text`,
			ruleID, req.TimeWindowType, req.TimeWindowValue, req.TimeWindowUnit,
			req.CustomStart, req.CustomEnd, req.ScheduleType, req.CronExpr, req.RowExpandMode,
			rowFrom, rowTo,
			req.RepeatKind, req.ScheduleTime, req.ScheduleWeekday, req.ScheduleMonthDay,
			req.ScheduleTimezone, actor,
		).Scan(&versionID); err != nil {
			api.LogErrorForResponse(w, "dms rule create version: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create rule version", "DMS_RULE_CREATE_FAILED")
			return
		}

		if err := insertVersionChildren(r.Context(), tx, ruleID, versionID, actor, "CREATE", req.Filters, req.Attachments, req.Destinations, req.EmailRecipients, req.BankAccountScope, req.NotificationTemplateIDs, req.Triggers); err != nil {
			api.LogErrorForResponse(w, "dms rule create children: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "failed to attach filters/documents/destinations/recipients/scope/notification templates (unknown id?)", "DMS_RULE_CREATE_FAILED")
			return
		}

		a := &auditRow{}
		a.set("rule_id", ruleID)
		a.set("version_id", versionID)
		a.set("action_type", "CREATE")
		a.set("processing_status", "PENDING_APPROVAL")
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		a.set("new_name", req.Name)
		a.set("new_module_code", req.ModuleCode)
		a.set("new_sub_module_code", req.SubModuleCode)
		a.set("new_entity_id", common.NullIfEmpty(req.EntityID))
		a.set("new_entity_name", common.NullIfEmpty(req.EntityName))
		a.set("new_status", "PendingApproval")
		a.set("new_is_deleted", false)
		a.set("new_data_row_from", rowFrom)
		a.set("new_data_row_to", rowTo)
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms rule create audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit rule creation", "DMS_RULE_CREATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms rule create commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create rule", "DMS_RULE_CREATE_FAILED")
			return
		}

		api.RespondEnvelopeSuccess(w, "Rule submitted for approval", map[string]interface{}{
			"rule_id":    ruleID,
			"version_id": versionID,
		})
	}
}
