package rules

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

type detailReq struct {
	RuleID    string `json:"rule_id"`
	VersionID string `json:"version_id"`
}

// HandleDetail returns a rule's header, every version summary, and the full
// config (filters/attachments/notification templates) of one selected
// version — defaults to current_version_id, or the latest version.
func HandleDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req detailReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.RuleID = strings.TrimSpace(req.RuleID)
		if req.RuleID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rule_id is required", "VALIDATION_ERROR")
			return
		}

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms rule detail begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch rule", "DMS_RULE_DETAIL_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var d ruleDetail
		var createdAt, lastModifiedAt time.Time
		var currentVersionID *string
		if err := tx.QueryRow(r.Context(), `
			SELECT rule_id::text, name, description, module_code, sub_module_code,
			       entity_id, entity_name,
			       status, processing_status, current_version_id::text,
			       COALESCE(created_by,''), created_at, COALESCE(last_modified_by,''), last_modified_at
			FROM dms_svc.generation_rule
			WHERE rule_id = $1::uuid AND is_deleted = false`, req.RuleID,
		).Scan(&d.RuleID, &d.Name, &d.Description, &d.ModuleCode, &d.SubModuleCode,
			&d.EntityID, &d.EntityName,
			&d.Status, &d.ProcessingStatus, &currentVersionID, &d.CreatedBy, &createdAt, &d.LastModifiedBy, &lastModifiedAt,
		); err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "rule not found", "NOT_FOUND")
			return
		}
		scope := ctxutil.FromContext(r.Context())
		entityID := ""
		if d.EntityID != nil {
			entityID = *d.EntityID
		}
		if !common.RequireEntityAccess(w, scope, entityID) {
			return
		}
		d.CurrentVersionID = currentVersionID
		d.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		d.LastModifiedAt = lastModifiedAt.UTC().Format(time.RFC3339)

		versionRows, err := tx.Query(r.Context(), `
			SELECT version_id::text, version_no, status, time_window_type, time_window_value, time_window_unit,
			       custom_start::text, custom_end::text, schedule_type, cron_expr,
			       COALESCE(row_expand_mode, 'FIRST_ROW'),
			       COALESCE(data_row_from, 1), COALESCE(data_row_to, 500),
			       COALESCE(repeat_kind,'MANUAL'), schedule_time::text,
			       schedule_weekday, schedule_month_day, COALESCE(schedule_timezone,'Asia/Kolkata'),
			       COALESCE(created_by,''), created_at, approved_by, approved_at
			FROM dms_svc.generation_rule_version
			WHERE rule_id = $1::uuid AND is_deleted = false
			ORDER BY version_no DESC`, req.RuleID)
		if err != nil {
			api.LogErrorForResponse(w, "dms rule detail versions: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch rule", "DMS_RULE_DETAIL_FAILED")
			return
		}
		d.Versions = make([]versionSummary, 0)
		for versionRows.Next() {
			var v versionSummary
			var vCreatedAt time.Time
			var vApprovedAt *time.Time
			if err := versionRows.Scan(&v.VersionID, &v.VersionNo, &v.Status, &v.TimeWindowType, &v.TimeWindowValue, &v.TimeWindowUnit,
				&v.CustomStart, &v.CustomEnd, &v.ScheduleType, &v.CronExpr, &v.RowExpandMode,
				&v.DataRowFrom, &v.DataRowTo,
				&v.RepeatKind, &v.ScheduleTime, &v.ScheduleWeekday, &v.ScheduleMonthDay, &v.ScheduleTimezone,
				&v.CreatedBy, &vCreatedAt, &v.ApprovedBy, &vApprovedAt); err != nil {
				continue
			}
			v.CreatedAt = vCreatedAt.UTC().Format(time.RFC3339)
			if vApprovedAt != nil {
				s := vApprovedAt.UTC().Format(time.RFC3339)
				v.ApprovedAt = &s
			}
			v.IsCurrent = currentVersionID != nil && *currentVersionID == v.VersionID
			d.Versions = append(d.Versions, v)
		}
		versionRows.Close()

		selectedVersionID := strings.TrimSpace(req.VersionID)
		if selectedVersionID == "" && currentVersionID != nil {
			selectedVersionID = *currentVersionID
		}
		if selectedVersionID == "" && len(d.Versions) > 0 {
			selectedVersionID = d.Versions[0].VersionID
		}
		if selectedVersionID != "" {
			for _, v := range d.Versions {
				if v.VersionID == selectedVersionID {
					d.TimeWindowType = v.TimeWindowType
					d.TimeWindowValue = v.TimeWindowValue
					d.TimeWindowUnit = v.TimeWindowUnit
					d.CustomStart = v.CustomStart
					d.CustomEnd = v.CustomEnd
					d.ScheduleType = v.ScheduleType
					d.CronExpr = v.CronExpr
					d.RepeatKind = v.RepeatKind
					d.ScheduleTime = v.ScheduleTime
					d.ScheduleWeekday = v.ScheduleWeekday
					d.ScheduleMonthDay = v.ScheduleMonthDay
					d.ScheduleTimezone = v.ScheduleTimezone
					d.RowExpandMode = v.RowExpandMode
					d.DataRowFrom = v.DataRowFrom
					d.DataRowTo = v.DataRowTo
					break
				}
			}
			children, err := loadVersionChildren(r.Context(), tx, selectedVersionID)
			if err == nil {
				d.Filters = children.Filters
				d.Attachments = children.Attachments
				d.Destinations = children.Destinations
				d.EmailRecipients = children.EmailRecipients
				d.BankAccountScope = children.BankAccountScope
				d.NotificationTemplateIDs = children.NotificationTemplateIDs
				d.Triggers = children.Triggers
			}
		}

		api.RespondEnvelopeSuccess(w, "Rule fetched", d)
	}
}
