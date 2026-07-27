package cdm

import (
	"context"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type updateReq struct {
	VariableID   string `json:"variable_id"`
	Name         string `json:"name"`
	DataType     string `json:"data_type"`
	Unit         string `json:"unit"`
	Label        string `json:"label"`
	Description  string `json:"description"`
	Domain       string `json:"domain"`
	SourceSystem string `json:"source_system"`
	CanonicalRef string `json:"canonical_ref"`
	UserAlias    string `json:"user_alias"`
	Nullable     bool   `json:"nullable"`
	Status       string `json:"status"`
	ActorID      string `json:"actor_id"`
	Reason       string `json:"reason"`
}

// HandleUpdate always stages a fresh EDIT request:
// master → PENDING_EDIT_APPROVAL + new EDIT audit row (never revises CREATE in place).
func HandleUpdate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req updateReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.VariableID = strings.TrimSpace(req.VariableID)
		req.Name = strings.TrimSpace(req.Name)
		req.Label = strings.TrimSpace(req.Label)
		req.Domain = strings.TrimSpace(req.Domain)
		req.DataType = strings.TrimSpace(req.DataType)
		req.Description = strings.TrimSpace(req.Description)
		req.CanonicalRef = strings.TrimSpace(req.CanonicalRef)
		req.UserAlias = strings.TrimSpace(req.UserAlias)
		req.Status = strings.TrimSpace(req.Status)
		if req.VariableID == "" || req.Name == "" || req.Label == "" || req.Domain == "" || req.DataType == "" || req.Description == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "variable_id, name, label, domain, data_type, description are required", "VALIDATION_ERROR")
			return
		}
		if req.Status == "" {
			req.Status = "Active"
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "cdm update begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update CDM variable", "CDM_UPDATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var old Item
		err = tx.QueryRow(r.Context(), `
			SELECT name, data_type, unit, label, description, domain, COALESCE(source_system, ''),
			       COALESCE(canonical_ref, ''), COALESCE(user_alias, ''), nullable, status, processing_status
			FROM policyengine_svc.cdm_variable
			WHERE variable_id = $1::uuid AND is_deleted = false
			FOR UPDATE`, req.VariableID,
		).Scan(&old.Name, &old.DataType, &old.Unit, &old.Label, &old.Description, &old.Domain,
			&old.SourceSystem, &old.CanonicalRef, &old.UserAlias, &old.Nullable, &old.Status, &old.ProcessingStatus)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "CDM variable not found", "NOT_FOUND")
			return
		}

		// Close any open CREATE/EDIT/DELETE pending so approve picks this new EDIT row.
		if common.IsPendingStatus(old.ProcessingStatus) {
			if err := supersedePendingCDMAudit(r.Context(), tx, req.VariableID, actor, ip, "superseded by edit"); err != nil {
				respondCDMUpdateError(w, "audit", err)
				return
			}
		}

		if err := applyCDMMasterWrite(r.Context(), tx, req, actor, "PENDING_EDIT_APPROVAL"); err != nil {
			respondCDMUpdateError(w, "exec", err)
			return
		}
		if err := insertCDMEditAudit(r.Context(), tx, req, old, actor, ip); err != nil {
			respondCDMUpdateError(w, "audit", err)
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "cdm update commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update CDM variable", "CDM_UPDATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "CDM variable edit submitted for approval", map[string]string{
			"variable_id": req.VariableID,
		})
	}
}

func applyCDMMasterWrite(ctx context.Context, tx pgx.Tx, req updateReq, actor, processingStatus string) error {
	_, err := tx.Exec(ctx, `
		UPDATE policyengine_svc.cdm_variable
		SET name = $1, data_type = $2, unit = $3, label = $4, description = $5, domain = $6,
		    source_system = NULLIF($7,''), canonical_ref = COALESCE($8,''), user_alias = NULLIF($9,''),
		    nullable = $10, status = $11,
		    processing_status = $12, last_modified_by = $13, last_modified_at = now()
		WHERE variable_id = $14::uuid`,
		req.Name, req.DataType, req.Unit, req.Label, req.Description, req.Domain,
		req.SourceSystem, req.CanonicalRef, req.UserAlias, req.Nullable, req.Status,
		processingStatus, actor, req.VariableID,
	)
	return err
}

func insertCDMEditAudit(ctx context.Context, tx pgx.Tx, req updateReq, old Item, actor, ip string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO policyengine_svc.cdm_variable_audit (
			variable_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
			old_name, new_name, old_data_type, new_data_type, old_unit, new_unit,
			old_label, new_label, old_description, new_description, old_domain, new_domain,
			old_source_system, new_source_system,
			old_canonical_ref, new_canonical_ref, old_user_alias, new_user_alias,
			old_nullable, new_nullable, old_status, new_status,
			old_is_deleted, new_is_deleted
		) VALUES ($1::uuid, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now(), $4,
			$5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22, $23, $24, $25, $26, false, false)`,
		req.VariableID, common.NullIfEmpty(req.Reason), actor, common.NullIfEmpty(ip),
		old.Name, req.Name, old.DataType, req.DataType, old.Unit, req.Unit,
		old.Label, req.Label, old.Description, req.Description, old.Domain, req.Domain,
		old.SourceSystem, req.SourceSystem,
		old.CanonicalRef, req.CanonicalRef,
		common.NullIfEmpty(old.UserAlias), common.NullIfEmpty(req.UserAlias),
		old.Nullable, req.Nullable, old.Status, req.Status,
	)
	return err
}

func supersedePendingCDMAudit(ctx context.Context, tx pgx.Tx, variableID, actor, ip, comment string) error {
	// Close ONLY the latest open pending audit — never bulk-reject the trail.
	// SUPERSEDED ≠ REJECTED (checker decision).
	_, err := tx.Exec(ctx, `
		UPDATE policyengine_svc.cdm_variable_audit
		SET processing_status = 'SUPERSEDED',
		    checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
		WHERE audit_id = (
			SELECT audit_id
			FROM policyengine_svc.cdm_variable_audit
			WHERE variable_id = $4::uuid
			  AND processing_status = ANY($5::text[])
			ORDER BY requested_at DESC
			LIMIT 1
		)`,
		actor, common.NullIfEmpty(ip), comment, variableID, common.PendingProcessingStatuses,
	)
	return err
}
