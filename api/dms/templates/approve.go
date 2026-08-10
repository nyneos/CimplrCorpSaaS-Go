package templates

import (
	"context"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type decisionReq struct {
	IDs            []string `json:"ids"`
	CheckerComment string   `json:"checker_comment"`
	ActorID        string   `json:"actor_id"`
}

// HandleApprove bulk-approves pending CREATE/EDIT/DELETE/CREATE_VERSION
// requests. Because create/update/versions all stage-then-apply, approve is
// the only place new_* values (or a new version) actually go live.
func HandleApprove(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req decisionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		if len(req.IDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "ids is required", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template approve begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to approve templates", "DMS_TEMPLATE_APPROVE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		approved := make([]string, 0, len(req.IDs))
		errs := make([]string, 0)
		for _, raw := range req.IDs {
			id := strings.TrimSpace(raw)
			if id == "" {
				continue
			}
			if err := applyApproval(r.Context(), tx, id, actor, ip, req.CheckerComment); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			approved = append(approved, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template approve commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to approve templates", "DMS_TEMPLATE_APPROVE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Templates approved", map[string]interface{}{
			"approved": approved,
			"errors":   errs,
		})
	}
}

func applyApproval(ctx context.Context, tx pgx.Tx, templateID, actor, ip, checkerComment string) error {
	pa, err := findPendingAudit(ctx, tx, templateID)
	if err != nil {
		return err
	}

	switch pa.ActionType {
	case "CREATE":
		// CREATE always carries its version 1 alongside it (see create.go) —
		// approving the template must also approve and activate that version,
		// or the template goes Active while current_version_id stays unset.
		if pa.VersionID == nil {
			return errNoVersionOnAudit
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template_version SET status = 'APPROVED', approved_by = $1, approved_at = now()
			WHERE version_id = $2::uuid`, actor, *pa.VersionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template SET processing_status = 'APPROVED', status = 'Active',
				current_version_id = $1::uuid, last_modified_by = $2, last_modified_at = now()
			WHERE template_id = $3::uuid`, *pa.VersionID, actor, templateID); err != nil {
			return err
		}
	case "EDIT":
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template SET
				name = COALESCE($1, name),
				module_code = COALESCE($2, module_code),
				sub_module_code = COALESCE($3, sub_module_code),
				status = COALESCE($4, status),
				processing_status = 'APPROVED',
				last_modified_by = $5, last_modified_at = now()
			WHERE template_id = $6::uuid`,
			pa.NewName, pa.NewModuleCode, pa.NewSubModuleCode, pa.NewStatus, actor, templateID); err != nil {
			return err
		}
	case "DELETE":
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template SET is_deleted = true, processing_status = 'APPROVED',
				last_modified_by = $1, last_modified_at = now()
			WHERE template_id = $2::uuid`, actor, templateID); err != nil {
			return err
		}
	case "CREATE_VERSION":
		if pa.VersionID == nil {
			return errNoVersionOnAudit
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template_version SET status = 'APPROVED', approved_by = $1, approved_at = now()
			WHERE version_id = $2::uuid`, actor, *pa.VersionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template SET current_version_id = $1::uuid, processing_status = 'APPROVED',
				status = CASE WHEN status = 'PendingApproval' THEN 'Active' ELSE status END,
				last_modified_by = $2, last_modified_at = now()
			WHERE template_id = $3::uuid`, *pa.VersionID, actor, templateID); err != nil {
			return err
		}
	default:
		return errUnknownActionType
	}

	_, err = tx.Exec(ctx, `
		UPDATE dms_svc.template_audit
		SET processing_status = 'APPROVED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
		WHERE audit_id = $4::uuid`,
		actor, common.NullIfEmpty(ip), common.NullIfEmpty(checkerComment), pa.AuditID)
	return err
}
