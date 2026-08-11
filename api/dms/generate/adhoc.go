package generate

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"

	"github.com/jackc/pgx/v5/pgxpool"
)

const errInvalidRequestBody = "invalid request body"

type adhocReq struct {
	DocumentTemplateID string            `json:"document_template_id"`
	OutputFormat       string            `json:"output_format"`
	ModuleCode         string            `json:"module_code"`
	SubModuleCode      string            `json:"sub_module_code"`
	SourceIDs          []string          `json:"source_ids"`
	MergeOverrides     map[string]string `json:"merge_overrides"`
	StoreS3            *bool             `json:"store_s3"`
	StoreLocal         bool              `json:"store_local"`
	SendEmail          bool              `json:"send_email"`
	PackageMode        string            `json:"package_mode"`
	EmailTemplateID    string            `json:"email_template_id"`
	EmailTo            []string          `json:"email_to"`
	EmailCc            []string          `json:"email_cc"`
	EmailSubject       string            `json:"email_subject"`
	EmailBodyHTML      string            `json:"email_body_html"`
	RequireApproval    *bool             `json:"require_approval"`
	ActorID            string            `json:"actor_id"`
}

// HandleAdhoc generates documents from selected rows + an approved template (no rule).
// By default stages dispatch for maker-checker (PENDING_APPROVAL).
func HandleAdhoc(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		if !common.RequireDMSEnabled(w) {
			return
		}
		var req adhocReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, errInvalidRequestBody, "BAD_REQUEST")
			return
		}
		tplID := strings.TrimSpace(req.DocumentTemplateID)
		if tplID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "document_template_id is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		storeS3 := true
		if req.StoreS3 != nil {
			storeS3 = *req.StoreS3
		}
		if !storeS3 && !req.StoreLocal && !req.SendEmail {
			storeS3 = true
		}

		result, err := dmsjobs.RunAdhocGeneration(r.Context(), pool, dmsjobs.AdhocRequest{
			ModuleCode:         strings.TrimSpace(req.ModuleCode),
			SubModuleCode:      strings.TrimSpace(req.SubModuleCode),
			DocumentTemplateID: tplID,
			OutputFormat:       req.OutputFormat,
			SourceIDs:          req.SourceIDs,
			TriggeredBy:        actor,
			MergeOverrides:     req.MergeOverrides,
			StoreS3:            storeS3,
			StoreLocal:         req.StoreLocal,
			SendEmail:          req.SendEmail,
			PackageMode:        req.PackageMode,
			EmailTemplateID:    strings.TrimSpace(req.EmailTemplateID),
			EmailTo:            req.EmailTo,
			EmailCc:            req.EmailCc,
			EmailSubject:       req.EmailSubject,
			EmailBodyHTML:      req.EmailBodyHTML,
			RequireApproval:    req.RequireApproval,
		})
		if err != nil {
			api.LogErrorForResponse(w, "dms adhoc: %v", err)
			api.RespondEnvelopeFailureWithData(w, http.StatusUnprocessableEntity, err.Error(), "DMS_ADHOC_FAILED",
				map[string]interface{}{"run_id": result.RunID, "request_id": result.RequestID})
			return
		}
		msg := "Ad-hoc generation completed"
		if result.PendingApproval {
			msg = "Documents generated — pending dispatch approval (email / finalize)"
		}
		api.RespondEnvelopeSuccess(w, msg, map[string]interface{}{
			"run_id":            result.RunID,
			"request_id":        result.RequestID,
			"doc_ids":           result.DocIDs,
			"filenames":         result.Filenames,
			"html_preview":      result.HTMLPreview,
			"pending_approval":  result.PendingApproval,
			"processing_status": result.ProcessingStatus,
		})
	}
}

type adhocDecisionReq struct {
	RequestID      string `json:"request_id"`
	CheckerComment string `json:"checker_comment"`
	ActorID        string `json:"actor_id"`
}

// HandleAdhocApprove applies pending adhoc dispatch (email and/or archive finalize).
func HandleAdhocApprove(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req adhocDecisionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, errInvalidRequestBody, "BAD_REQUEST")
			return
		}
		if strings.TrimSpace(req.RequestID) == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "request_id is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		if err := dmsjobs.ApproveAdhocDispatch(r.Context(), pool, req.RequestID, actor, req.CheckerComment); err != nil {
			api.LogErrorForResponse(w, "dms adhoc approve: %v", err)
			api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, err.Error(), "DMS_ADHOC_APPROVE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Ad-hoc dispatch approved", map[string]interface{}{
			"request_id": req.RequestID,
		})
	}
}

// HandleAdhocReject rejects a pending adhoc dispatch request.
func HandleAdhocReject(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req adhocDecisionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, errInvalidRequestBody, "BAD_REQUEST")
			return
		}
		if strings.TrimSpace(req.RequestID) == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "request_id is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		if err := dmsjobs.RejectAdhocDispatch(r.Context(), pool, req.RequestID, actor, req.CheckerComment); err != nil {
			api.LogErrorForResponse(w, "dms adhoc reject: %v", err)
			api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, err.Error(), "DMS_ADHOC_REJECT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Ad-hoc dispatch rejected", map[string]interface{}{
			"request_id": req.RequestID,
		})
	}
}

// HandleAdhocListPending lists pending adhoc dispatch requests.
func HandleAdhocListPending(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var body struct {
			ModuleCode    string `json:"module_code"`
			SubModuleCode string `json:"sub_module_code"`
			Limit         int    `json:"limit"`
		}
		_ = common.DecodeJSON(r, &body)
		rows, err := dmsjobs.ListPendingAdhocRequests(
			r.Context(), pool, body.Limit, body.ModuleCode, body.SubModuleCode,
		)
		if err != nil {
			api.LogErrorForResponse(w, "dms adhoc list-pending: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list pending adhoc requests", "DMS_ADHOC_LIST_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Pending adhoc requests", map[string]interface{}{
			"rows": rows,
		})
	}
}

// HandleAdhocAuditLog returns maker-checker audit rows for adhoc dispatch requests.
func HandleAdhocAuditLog(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var body struct {
			RequestID     string `json:"request_id"`
			ModuleCode    string `json:"module_code"`
			SubModuleCode string `json:"sub_module_code"`
			Limit         int    `json:"limit"`
		}
		_ = common.DecodeJSON(r, &body)
		rows, err := dmsjobs.ListAdhocAuditLog(
			r.Context(), pool, body.RequestID, body.ModuleCode, body.SubModuleCode, body.Limit,
		)
		if err != nil {
			api.LogErrorForResponse(w, "dms adhoc audit-log: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load adhoc audit", "DMS_ADHOC_AUDIT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Adhoc audit fetched", rows)
	}
}
