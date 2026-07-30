package policy

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ExportFormatVersion is the envelope version stamped on every export file.
// Import rejects payloads with a different version.
const ExportFormatVersion = 1

type exportReq struct {
	// At least one filter is required — empty body is rejected to avoid
	// accidental full-table dumps.
	Module    string   `json:"module"`
	SubModule string   `json:"sub_module"`
	Entity    string   `json:"entity"`
	Status    string   `json:"status"`
	PolicyIDs []string `json:"policy_ids"`
}

// ExportEnvelope wraps the canonical DetailItem definitions so a future
// import can detect an incompatible file before applying anything.
type ExportEnvelope struct {
	FormatVersion int          `json:"format_version"`
	ExportedAt    string       `json:"exported_at"`
	Policies      []DetailItem `json:"policies"`
}

// HandleExport returns the full canonical definition of policies matching the
// request filters. Each item is the same shape as /policies/detail, with
// instance/lifecycle fields stripped (see stripForExport).
func HandleExport(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req exportReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.Module = strings.TrimSpace(req.Module)
		req.SubModule = strings.TrimSpace(req.SubModule)
		req.Entity = strings.TrimSpace(req.Entity)
		req.Status = strings.TrimSpace(req.Status)
		ids := make([]string, 0, len(req.PolicyIDs))
		for _, id := range req.PolicyIDs {
			id = strings.TrimSpace(id)
			if id != "" {
				ids = append(ids, id)
			}
		}
		if req.Module == "" && req.SubModule == "" && req.Entity == "" && req.Status == "" && len(ids) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest,
				"at least one filter is required (module, sub_module, entity, status, or policy_ids)",
				"VALIDATION_ERROR")
			return
		}

		policyIDs, err := resolveExportPolicyIDs(r, pool, req, ids)
		if err != nil {
			api.LogErrorForResponse(w, "policy export resolve: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to resolve policies for export", "POLICY_EXPORT_FAILED")
			return
		}

		out := make([]DetailItem, 0, len(policyIDs))
		for _, id := range policyIDs {
			it, err := loadPolicyDetail(r, pool, id)
			if err != nil {
				api.LogErrorForResponse(w, "policy export load policy_id=%s: %v", id, err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load policy for export", "POLICY_EXPORT_FAILED")
				return
			}
			out = append(out, stripForExport(it))
		}

		api.RespondEnvelopeSuccess(w, "Policies exported", ExportEnvelope{
			FormatVersion: ExportFormatVersion,
			ExportedAt:    time.Now().UTC().Format(time.RFC3339),
			Policies:      out,
		})
	}
}

func resolveExportPolicyIDs(r *http.Request, pool *pgxpool.Pool, req exportReq, ids []string) ([]string, error) {
	ctx := r.Context()
	args := make([]interface{}, 0, 8)
	where := []string{"p.is_deleted = false"}
	argN := 1

	if len(ids) > 0 {
		where = append(where, fmt.Sprintf("p.policy_id = ANY($%d::uuid[])", argN))
		args = append(args, ids)
		argN++
	}
	if req.Status != "" {
		where = append(where, fmt.Sprintf("p.status = $%d", argN))
		args = append(args, req.Status)
		argN++
	}
	if req.Module != "" {
		where = append(where, fmt.Sprintf(`EXISTS (
			SELECT 1 FROM policyengine_svc.policy_module pm
			WHERE pm.policy_id = p.policy_id AND pm.is_deleted = false AND pm.module_code = $%d)`, argN))
		args = append(args, req.Module)
		argN++
	}
	if req.SubModule != "" {
		where = append(where, fmt.Sprintf(`EXISTS (
			SELECT 1 FROM policyengine_svc.policy_sub_module psm
			WHERE psm.policy_id = p.policy_id AND psm.is_deleted = false AND psm.sub_module_code = $%d)`, argN))
		args = append(args, req.SubModule)
		argN++
	}
	if req.Entity != "" {
		where = append(where, fmt.Sprintf(`EXISTS (
			SELECT 1 FROM policyengine_svc.policy_entity pe
			WHERE pe.policy_id = p.policy_id AND pe.is_deleted = false AND pe.entity_code = $%d)`, argN))
		args = append(args, req.Entity)
		argN++
	}

	q := `SELECT p.policy_id::text FROM policyengine_svc.policy_master p WHERE ` +
		strings.Join(where, " AND ") +
		` ORDER BY p.code`
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// stripForExport clears instance-specific and lifecycle fields so the artifact
// is a portable definition, not a DB record. Cleared fields:
//   - policy_id (internal UUID)
//   - status, processing_status (approval / lifecycle state)
//   - version
//   - approval_matrix_id, approval_workflow (env-bound approval wiring)
//   - created_by, created_at, last_modified_by, last_modified_at
//
// approved_by / approved_at are not on DetailItem (never loaded) and therefore
// never appear in the export.
func stripForExport(it *DetailItem) DetailItem {
	out := *it
	out.PolicyID = ""
	out.Status = ""
	out.ProcessingStatus = ""
	out.Version = 0
	out.ApprovalMatrixID = ""
	out.ApprovalWorkflow = ""
	out.CreatedBy = ""
	out.CreatedAt = ""
	out.LastModifiedBy = ""
	out.LastModifiedAt = ""
	return out
}
