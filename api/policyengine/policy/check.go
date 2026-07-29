package policy

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

type checkReq struct {
	EventCode          string            `json:"event_code"`
	ModuleCode         string            `json:"module_code"`
	SubModule          string            `json:"sub_module"`
	FormID             string            `json:"form_id"`
	EntityCode         string            `json:"entity_code"`
	ActorUserID        string            `json:"actor_user_id"`
	HandlerName        string            `json:"handler_name"`
	APIPath            string            `json:"api_path"`
	CorrelationID      string            `json:"correlation_id"`
	TraceID            string            `json:"trace_id"`
	BusinessRecordType string            `json:"business_record_type"`
	BusinessRecordID   string            `json:"business_record_id"`
	SourceFileName     string            `json:"source_file_name"`
	SourceFileID       string            `json:"source_file_id"`
	BatchID            string            `json:"batch_id"`
	Variables          map[string]string `json:"variables"`
	// Optional explicit policy payloads; if empty, Active policies for event+module are loaded.
	Policies []map[string]interface{} `json:"policies"`
}

// HandleCheck loads applicable policies, calls CIMPLR-Policy-Service (parallel-safe client),
// writes execution_log rows, returns aggregated action.
func HandleCheck(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req checkReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.EventCode = strings.TrimSpace(req.EventCode)
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModule = strings.TrimSpace(req.SubModule)
		if req.EventCode == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "event_code is required", "VALIDATION_ERROR")
			return
		}
		// Without sub_module, loadActivePolicies skips the sub-module filter and evaluates
		// every Active policy for the module+trigger (e.g. FD_CONFIRMATION on booking).
		if req.ModuleCode != "" && req.SubModule == "" && len(req.Policies) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest,
				"sub_module is required when module_code is set (e.g. FD_BOOKING, FD_CONFIRMATION)",
				"VALIDATION_ERROR")
			return
		}
		if req.Variables == nil {
			req.Variables = map[string]string{}
		}

		traceID := common.ResolveTraceID(w, r, req.TraceID)
		correlationID := common.ResolveCorrelationID(r, req.CorrelationID)

		result, err := runtime.RunCheck(r.Context(), pool, runtime.CheckRequest{
			EventCode:          req.EventCode,
			ModuleCode:         req.ModuleCode,
			SubModule:          req.SubModule,
			FormID:             req.FormID,
			EntityCode:         req.EntityCode,
			ActorUserID:        req.ActorUserID,
			HandlerName:        req.HandlerName,
			APIPath:            req.APIPath,
			CorrelationID:      correlationID,
			TraceID:            traceID,
			BusinessRecordType: req.BusinessRecordType,
			BusinessRecordID:   req.BusinessRecordID,
			SourceFileName:     req.SourceFileName,
			SourceFileID:       req.SourceFileID,
			BatchID:            req.BatchID,
			Variables:          req.Variables,
			Policies:           req.Policies,
		})
		if err != nil {
			api.LogErrorForResponse(w, "policy check evaluate: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadGateway, "policy check service failed", "POLICY_SERVICE_ERROR")
			return
		}

		passed, failed := result.CountPassedFailed()
		msg := "Policy check completed"
		if line := result.SummaryLine(); line != "" {
			msg = line
		}
		if result.BlocksSubmit() {
			msg = result.ClientMessage("Blocked by policy")
		}
		api.RespondEnvelopeSuccess(w, msg, map[string]interface{}{
			"aggregated_action": result.AggregatedAction,
			"results":           result.Results,
			"duration_ms":       result.DurationMS,
			"passed_count":      passed,
			"failed_count":      failed,
			"summary":           result.SummaryLine(),
			"conflict_findings": conflictFindingsPayload(result.ConflictReport),
			"conflict_warnings": conflictWarningsPayload(result.ConflictReport),
		})
	}
}

func conflictFindingsPayload(report runtime.ConflictReport) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(report.Findings))
	for _, f := range report.Findings {
		out = append(out, map[string]interface{}{
			"severity":     f.Severity,
			"variable":     f.Variable,
			"message":      f.Message,
			"policy_codes": f.PolicyCodes,
		})
	}
	return out
}

func conflictWarningsPayload(report runtime.ConflictReport) []map[string]interface{} {
	warns := report.Warnings()
	out := make([]map[string]interface{}, 0, len(warns))
	for _, f := range warns {
		out = append(out, map[string]interface{}{
			"severity":     f.Severity,
			"variable":     f.Variable,
			"message":      f.Message,
			"policy_codes": f.PolicyCodes,
		})
	}
	return out
}
