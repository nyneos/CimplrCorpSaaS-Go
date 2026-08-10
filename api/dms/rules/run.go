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
		var req runReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
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
	msg = err.Error()
	code = "DMS_RULE_RUN_FAILED"
	status = http.StatusUnprocessableEntity
	switch {
	case strings.Contains(msg, "DOCUMENT_SERVICE_UNAUTHORIZED"):
		code = "DOCUMENT_SERVICE_UNAUTHORIZED"
		status = http.StatusUnauthorized
	case strings.Contains(msg, "DOCUMENT_SERVICE_KEY not configured"):
		code = "DOCUMENT_SERVICE_MISCONFIGURED"
		status = http.StatusServiceUnavailable
	case strings.Contains(msg, "document-service unavailable"), strings.Contains(msg, "document-service unreachable"), strings.Contains(msg, "document-service render"):
		code = "DOCUMENT_SERVICE_UNAVAILABLE"
		status = http.StatusServiceUnavailable
	case strings.Contains(msg, "DMS_GENERATION_DISABLED"):
		code = "DMS_GENERATION_DISABLED"
		status = http.StatusServiceUnavailable
	case strings.Contains(msg, "DMS_GENERATION_QUOTA_EXCEEDED"), strings.Contains(msg, "QUOTA_EXCEEDED"):
		code = "DMS_GENERATION_QUOTA_EXCEEDED"
		status = http.StatusTooManyRequests
	case strings.Contains(msg, "DMS_RULE_ENQUEUE_FAILED"):
		code = "DMS_RULE_ENQUEUE_FAILED"
	}
	return status, code, msg
}
