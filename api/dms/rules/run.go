package rules

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

type runReq struct {
	RuleID    string   `json:"rule_id"`
	SourceIDs []string `json:"source_ids"`
	ActorID   string   `json:"actor_id"`
}

// HandleRun triggers a rule. When DMS_GENERATION_QUEUE_ENABLED=true, enqueues via
// Document-Service and returns immediately (job_id). Otherwise runs inline (legacy).
func HandleRun(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		if !common.RequireDMSEnabled(w) {
			return
		}
		var req runReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		if !common.RequireRecordsEntityAccess(w, r, pool, common.RuleEntityQuery, []string{req.RuleID}) {
			return
		}
		req.RuleID = strings.TrimSpace(req.RuleID)
		if req.RuleID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rule_id is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)

		sourceIDField := ""
		if len(req.SourceIDs) > 0 {
			queryErr := pool.QueryRow(r.Context(), `
				SELECT t.source_id_field
				FROM dms_svc.generation_rule r
				JOIN dms_svc.generation_rule_trigger t
				  ON t.version_id = r.current_version_id
				WHERE r.rule_id = $1::uuid
				  AND t.trigger_type = 'MANUAL'
				  AND t.is_enabled = true
				ORDER BY t.sort_order
				LIMIT 1`, req.RuleID).Scan(&sourceIDField)
			if queryErr != nil || strings.TrimSpace(sourceIDField) == "" {
				api.RespondEnvelopeError(w, http.StatusBadRequest,
					"selected-row run needs an enabled MANUAL trigger with source_id_field",
					"DMS_RULE_SOURCE_FIELD_REQUIRED")
				return
			}
		}

		if docsvc.QueueEnabled() {
			jobID, err := dmsjobs.EnqueueRuleGeneration(
				r.Context(), pool, req.RuleID, "MANUAL", actor, sourceIDField, req.SourceIDs,
			)
			if err != nil {
				api.LogErrorForResponse(w, "dms rule enqueue: %v", err)
				status, code, msg := mapGenerationGateError(err)
				api.RespondEnvelopeError(w, status, msg, code)
				return
			}
			api.RespondEnvelopeSuccess(w, "Rule run queued", map[string]interface{}{
				"job_id": jobID,
				"queued": true,
				"status": "STARTED",
			})
			return
		}

		var runID string
		var err error
		if len(req.SourceIDs) > 0 {
			runID, err = dmsjobs.RunGenerationForSourceIDs(
				r.Context(), pool, req.RuleID, "MANUAL", actor,
				sourceIDField, req.SourceIDs,
			)
		} else {
			runID, err = dmsjobs.RunGeneration(r.Context(), pool, req.RuleID, "MANUAL", actor)
		}
		if err != nil {
			api.LogErrorForResponse(w, "dms rule run: %v", err)
			status, code, msg := mapGenerationGateError(err)
			if runID != "" {
				api.RespondEnvelopeFailureWithData(w, status, msg, code, map[string]interface{}{"run_id": runID})
				return
			}
			api.RespondEnvelopeError(w, status, msg, code)
			return
		}
		api.RespondEnvelopeSuccess(w, "Rule run completed", map[string]interface{}{"run_id": runID})
	}
}

func mapGenerationGateError(err error) (status int, code, msg string) {
	raw := err.Error()
	code = "DMS_RULE_RUN_FAILED"
	msg = "failed to run the rule"
	status = http.StatusUnprocessableEntity
	switch {
	case strings.Contains(raw, "DOCUMENT_SERVICE_UNAUTHORIZED"):
		code = "DOCUMENT_SERVICE_UNAUTHORIZED"
		status = http.StatusUnauthorized
		msg = "document service rejected the request"
	case strings.Contains(raw, "DOCUMENT_SERVICE_KEY not configured"):
		code = "DOCUMENT_SERVICE_MISCONFIGURED"
		status = http.StatusServiceUnavailable
		msg = "document service is not configured"
	case strings.Contains(raw, "document-service unavailable"), strings.Contains(raw, "document-service unreachable"), strings.Contains(raw, "document-service render"):
		code = "DOCUMENT_SERVICE_UNAVAILABLE"
		status = http.StatusServiceUnavailable
		msg = "document service is unavailable"
	case strings.Contains(raw, "DMS_DISABLED"):
		code = "DMS_DISABLED"
		status = http.StatusForbidden
		msg = "DMS is disabled"
	case strings.Contains(raw, "DMS_GENERATION_DISABLED"):
		code = "DMS_GENERATION_DISABLED"
		status = http.StatusServiceUnavailable
		msg = "document generation is disabled"
	case strings.Contains(raw, "DMS_GENERATION_QUOTA_EXCEEDED"), strings.Contains(raw, "QUOTA_EXCEEDED"):
		code = "DMS_GENERATION_QUOTA_EXCEEDED"
		status = http.StatusTooManyRequests
		msg = "document generation quota is exceeded"
	case strings.Contains(raw, "DMS_RULE_ENQUEUE_FAILED"):
		code = "DMS_RULE_ENQUEUE_FAILED"
		msg = "failed to queue the rule run"
	}
	return status, code, msg
}
