package rules

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const errFailedCreateRuleVersion = "failed to create rule version"

type createVersionReq struct {
	RuleID                  string                `json:"rule_id"`
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
	RowExpandMode           string                `json:"row_expand_mode"`
	DataRowFrom             *int                  `json:"data_row_from"`
	DataRowTo               *int                  `json:"data_row_to"`
	Filters                 []filterReq           `json:"filters"`
	Attachments             []attachmentReq       `json:"attachments"`
	Destinations            []destinationReq      `json:"destinations"`
	EmailRecipients         []emailRecipientReq   `json:"email_recipients"`
	BankAccountScope        []bankAccountScopeReq `json:"bank_account_scope"`
	NotificationTemplateIDs []string              `json:"notification_template_ids"`
	Triggers                []triggerReq          `json:"triggers"`
	Reason                  string                `json:"reason"`
	ActorID                 string                `json:"actor_id"`
}

// HandleCreateVersion raises a new configuration version for an existing
// rule — same version-controlled edit pattern as templates/versions.go.
func HandleCreateVersion(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req createVersionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.RuleID = strings.TrimSpace(req.RuleID)
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
		if req.RuleID == "" || req.TimeWindowType == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rule_id and time_window_type are required", "VALIDATION_ERROR")
			return
		}
		if len(req.Attachments) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "at least one document attachment is required", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms rule version create begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedCreateRuleVersion, "DMS_RULE_VERSION_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		amend, err := pendingAmendableAudit(r.Context(), tx, req.RuleID)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_RULE_PENDING_EXISTS")
			return
		}

		var nextVersionNo int
		if err := tx.QueryRow(r.Context(), `
			SELECT COALESCE(MAX(version_no), 0) + 1 FROM dms_svc.generation_rule_version
			WHERE rule_id = $1::uuid`, req.RuleID,
		).Scan(&nextVersionNo); err != nil {
			api.LogErrorForResponse(w, "dms rule version seq: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedCreateRuleVersion, "DMS_RULE_VERSION_FAILED")
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
			VALUES ($1::uuid, $2, $3, $4, NULLIF($5,''), NULLIF($6,'')::date, NULLIF($7,'')::date,
			        $8, NULLIF($9,''), $10, $11, $12, $13, NULLIF($14,'')::time, $15, $16, $17,
			        'PENDING_APPROVAL', $18)
			RETURNING version_id::text`,
			req.RuleID, nextVersionNo, req.TimeWindowType, req.TimeWindowValue, req.TimeWindowUnit,
			req.CustomStart, req.CustomEnd, req.ScheduleType, req.CronExpr, req.RowExpandMode,
			rowFrom, rowTo,
			req.RepeatKind, req.ScheduleTime, req.ScheduleWeekday, req.ScheduleMonthDay,
			req.ScheduleTimezone, actor,
		).Scan(&versionID); err != nil {
			api.LogErrorForResponse(w, "dms rule version insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedCreateRuleVersion, "DMS_RULE_VERSION_FAILED")
			return
		}

		if err := insertVersionChildren(r.Context(), tx, insertVersionChildrenParams{
			RuleID:                  req.RuleID,
			VersionID:               versionID,
			Actor:                   actor,
			ActionType:              "CREATE_VERSION",
			Filters:                 req.Filters,
			Attachments:             req.Attachments,
			Destinations:            req.Destinations,
			EmailRecipients:         req.EmailRecipients,
			BankAccountScope:        req.BankAccountScope,
			NotificationTemplateIDs: req.NotificationTemplateIDs,
			Triggers:                req.Triggers,
		}); err != nil {
			api.LogErrorForResponse(w, "dms rule version children: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "failed to attach filters/documents/destinations/notification templates (unknown id?)", "DMS_RULE_VERSION_FAILED")
			return
		}

		if amend != nil {
			if amend.VersionID != nil {
				if _, err := tx.Exec(r.Context(), `
					UPDATE dms_svc.generation_rule_version SET is_deleted = true
					WHERE version_id = $1::uuid`, *amend.VersionID); err != nil {
					api.LogErrorForResponse(w, "dms rule version supersede: %v", err)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedCreateRuleVersion, "DMS_RULE_VERSION_FAILED")
					return
				}
			}
			if _, err := tx.Exec(r.Context(), `
				UPDATE dms_svc.generation_rule_audit
				SET version_id = $1::uuid, reason = COALESCE($2, reason), requested_by = $3,
					requested_ip = $4, requested_at = now()
				WHERE audit_id = $5::uuid`,
				versionID, common.NullIfEmpty(req.Reason), actor, common.NullIfEmpty(ip), amend.AuditID); err != nil {
				api.LogErrorForResponse(w, "dms rule version amend audit: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit rule version", "DMS_RULE_VERSION_FAILED")
				return
			}
			if err := tx.Commit(r.Context()); err != nil {
				api.LogErrorForResponse(w, "dms rule version commit: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedCreateRuleVersion, "DMS_RULE_VERSION_FAILED")
				return
			}
			api.RespondEnvelopeSuccess(w, "Rule version submitted for approval", map[string]interface{}{
				"rule_id":    req.RuleID,
				"version_id": versionID,
				"version_no": nextVersionNo,
			})
			return
		}

		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.generation_rule SET processing_status = 'PENDING_EDIT_APPROVAL'
			WHERE rule_id = $1::uuid`, req.RuleID); err != nil {
			api.LogErrorForResponse(w, "dms rule version flag: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedCreateRuleVersion, "DMS_RULE_VERSION_FAILED")
			return
		}

		a := &auditRow{}
		a.set("rule_id", req.RuleID)
		a.set("version_id", versionID)
		a.set("action_type", "CREATE_VERSION")
		a.set("processing_status", "PENDING_EDIT_APPROVAL")
		a.set("reason", common.NullIfEmpty(req.Reason))
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		var oldFrom, oldTo *int
		_ = tx.QueryRow(r.Context(), `
			SELECT v.data_row_from, v.data_row_to
			FROM dms_svc.generation_rule r
			JOIN dms_svc.generation_rule_version v ON v.version_id = r.current_version_id
			WHERE r.rule_id = $1::uuid`, req.RuleID,
		).Scan(&oldFrom, &oldTo)
		if oldFrom != nil {
			a.set("old_data_row_from", *oldFrom)
		}
		if oldTo != nil {
			a.set("old_data_row_to", *oldTo)
		}
		a.set("new_data_row_from", rowFrom)
		a.set("new_data_row_to", rowTo)
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms rule version audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit rule version", "DMS_RULE_VERSION_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms rule version commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedCreateRuleVersion, "DMS_RULE_VERSION_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Rule version submitted for approval", map[string]interface{}{
			"rule_id":    req.RuleID,
			"version_id": versionID,
			"version_no": nextVersionNo,
		})
	}
}

type listVersionsReq struct {
	RuleID string `json:"rule_id"`
}

// HandleListVersions returns every version of a rule, newest first.
func HandleListVersions(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req listVersionsReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.RuleID = strings.TrimSpace(req.RuleID)
		if req.RuleID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rule_id is required", "VALIDATION_ERROR")
			return
		}

		var currentVersionID *string
		if err := pool.QueryRow(r.Context(), `
			SELECT current_version_id::text FROM dms_svc.generation_rule WHERE rule_id = $1::uuid`, req.RuleID,
		).Scan(&currentVersionID); err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "rule not found", "NOT_FOUND")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT version_id::text, version_no, status, time_window_type, time_window_value, time_window_unit,
			       custom_start::text, custom_end::text, schedule_type, cron_expr,
			       COALESCE(row_expand_mode, 'FIRST_ROW'),
			       COALESCE(data_row_from, 1), COALESCE(data_row_to, 500),
			       COALESCE(created_by,''), created_at, approved_by, approved_at
			FROM dms_svc.generation_rule_version
			WHERE rule_id = $1::uuid AND is_deleted = false
			ORDER BY version_no DESC`, req.RuleID)
		if err != nil {
			api.LogErrorForResponse(w, "dms rule versions list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list rule versions", "DMS_RULE_VERSIONS_FAILED")
			return
		}
		defer rows.Close()

		out := make([]versionSummary, 0)
		for rows.Next() {
			var v versionSummary
			var createdAt time.Time
			var approvedAt *time.Time
			if err := rows.Scan(&v.VersionID, &v.VersionNo, &v.Status, &v.TimeWindowType, &v.TimeWindowValue, &v.TimeWindowUnit,
				&v.CustomStart, &v.CustomEnd, &v.ScheduleType, &v.CronExpr, &v.RowExpandMode,
				&v.DataRowFrom, &v.DataRowTo,
				&v.CreatedBy, &createdAt, &v.ApprovedBy, &approvedAt); err != nil {
				continue
			}
			v.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			if approvedAt != nil {
				s := approvedAt.UTC().Format(time.RFC3339)
				v.ApprovedAt = &s
			}
			v.IsCurrent = currentVersionID != nil && *currentVersionID == v.VersionID
			out = append(out, v)
		}
		api.RespondEnvelopeSuccess(w, "Rule versions fetched", out)
	}
}
