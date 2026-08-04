package rules

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type createVersionReq struct {
	RuleID                   string          `json:"rule_id"`
	TimeWindowType           string          `json:"time_window_type"`
	TimeWindowValue          *int            `json:"time_window_value"`
	TimeWindowUnit           string          `json:"time_window_unit"`
	CustomStart              string          `json:"custom_start"`
	CustomEnd                string          `json:"custom_end"`
	ScheduleType             string          `json:"schedule_type"`
	CronExpr                 string          `json:"cron_expr"`
	Filters                 []filterReq           `json:"filters"`
	Attachments             []attachmentReq       `json:"attachments"`
	Destinations            []destinationReq      `json:"destinations"`
	EmailRecipients         []emailRecipientReq   `json:"email_recipients"`
	BankAccountScope        []bankAccountScopeReq `json:"bank_account_scope"`
	NotificationTemplateIDs []string              `json:"notification_template_ids"`
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
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create rule version", "DMS_RULE_VERSION_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		if err := requirePendingFree(r.Context(), tx, req.RuleID); err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_RULE_PENDING_EXISTS")
			return
		}

		var nextVersionNo int
		if err := tx.QueryRow(r.Context(), `
			SELECT COALESCE(MAX(version_no), 0) + 1 FROM dms_svc.generation_rule_version
			WHERE rule_id = $1::uuid`, req.RuleID,
		).Scan(&nextVersionNo); err != nil {
			api.LogErrorForResponse(w, "dms rule version seq: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create rule version", "DMS_RULE_VERSION_FAILED")
			return
		}

		var versionID string
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO dms_svc.generation_rule_version
				(rule_id, version_no, time_window_type, time_window_value, time_window_unit,
				 custom_start, custom_end, schedule_type, cron_expr, status, created_by)
			VALUES ($1::uuid, $2, $3, $4, NULLIF($5,''), NULLIF($6,'')::date, NULLIF($7,'')::date, $8, NULLIF($9,''), 'PENDING_APPROVAL', $10)
			RETURNING version_id::text`,
			req.RuleID, nextVersionNo, req.TimeWindowType, req.TimeWindowValue, req.TimeWindowUnit,
			req.CustomStart, req.CustomEnd, req.ScheduleType, req.CronExpr, actor,
		).Scan(&versionID); err != nil {
			api.LogErrorForResponse(w, "dms rule version insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create rule version", "DMS_RULE_VERSION_FAILED")
			return
		}

		if err := insertVersionChildren(r.Context(), tx, versionID, actor, req.Filters, req.Attachments, req.Destinations, req.EmailRecipients, req.BankAccountScope, req.NotificationTemplateIDs); err != nil {
			api.LogErrorForResponse(w, "dms rule version children: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "failed to attach filters/documents/destinations/notification templates (unknown id?)", "DMS_RULE_VERSION_FAILED")
			return
		}

		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.generation_rule SET processing_status = 'PENDING_EDIT_APPROVAL'
			WHERE rule_id = $1::uuid`, req.RuleID); err != nil {
			api.LogErrorForResponse(w, "dms rule version flag: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create rule version", "DMS_RULE_VERSION_FAILED")
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
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms rule version audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit rule version", "DMS_RULE_VERSION_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms rule version commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create rule version", "DMS_RULE_VERSION_FAILED")
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
			       COALESCE(created_by,''), created_at, approved_by, approved_at
			FROM dms_svc.generation_rule_version
			WHERE rule_id = $1::uuid
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
				&v.CustomStart, &v.CustomEnd, &v.ScheduleType, &v.CronExpr,
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
