package policy

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const breachTriggerApproval = "TriggerApproval"

const approvedActiveMatrixWhere = `
	m.is_active  = true
	AND m.is_deleted = false
	AND ma.processing_status = 'APPROVED'
	AND ma.action_type <> 'DELETE'`

const latestMatrixAudit = `
	JOIN LATERAL (
		SELECT action_type, processing_status
		FROM uam.audit_approval_matrix_master a
		WHERE a.matrix_id = m.matrix_id
		ORDER BY a.requested_at DESC, a.audit_id DESC
		LIMIT 1
	) ma ON true`

// MatrixOption is one selectable approval matrix for the policy form.
type MatrixOption struct {
	MatrixID        string `json:"matrix_id"`
	ModuleCode      string `json:"module_code"`
	TransactionType string `json:"transaction_type"`
	ApprovalOrder   string `json:"approval_order"`
	EyeCount        int    `json:"eye_count"`
}

// HandleApprovalMatrixOptions lists approved + active approval matrices for the
// TriggerApproval picker on the policy form.
func HandleApprovalMatrixOptions(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT m.matrix_id, m.module_code, m.transaction_type, m.approval_order,
			       (SELECT COUNT(*) FROM uam.approval_matrix_eye e
			         WHERE e.matrix_id = m.matrix_id AND e.is_active = true AND e.is_deleted = false)
			FROM uam.approval_matrix_master m`+latestMatrixAudit+`
			WHERE`+approvedActiveMatrixWhere+`
			ORDER BY m.module_code, m.transaction_type`)
		if err != nil {
			api.LogErrorForResponse(w, "approval matrix options: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load approval matrices", "MATRIX_LOAD_FAILED")
			return
		}
		defer rows.Close()

		out := make([]MatrixOption, 0)
		for rows.Next() {
			var o MatrixOption
			if err := rows.Scan(&o.MatrixID, &o.ModuleCode, &o.TransactionType, &o.ApprovalOrder, &o.EyeCount); err != nil {
				api.LogErrorForResponse(w, "approval matrix options scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load approval matrices", "MATRIX_LOAD_FAILED")
				return
			}
			out = append(out, o)
		}
		api.RespondEnvelopeSuccess(w, "Approval matrices fetched", out)
	}
}

// validateTriggerApprovalMatrix enforces the TriggerApproval wiring rules:
// when no approved matrix exists at all the selection is optional (TriggerApproval
// then behaves as before), otherwise a valid approved+active matrix must be chosen.
func validateTriggerApprovalMatrix(ctx context.Context, pool *pgxpool.Pool, actionOnBreach, matrixID string) error {
	if !strings.EqualFold(strings.TrimSpace(actionOnBreach), breachTriggerApproval) {
		return nil
	}
	matrixID = strings.TrimSpace(matrixID)

	var available int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM uam.approval_matrix_master m`+latestMatrixAudit+`
		WHERE`+approvedActiveMatrixWhere).Scan(&available); err != nil {
		return errors.New("could not verify approval matrices")
	}
	if available == 0 {
		return nil
	}
	if matrixID == "" {
		return errors.New("approval_matrix_id is required when action_on_breach is TriggerApproval")
	}

	var one int
	if err := pool.QueryRow(ctx, `
		SELECT 1 FROM uam.approval_matrix_master m`+latestMatrixAudit+`
		WHERE m.matrix_id = $1 AND`+approvedActiveMatrixWhere, matrixID).Scan(&one); err != nil {
		return errors.New("approval_matrix_id does not match an approved, active approval matrix")
	}
	return nil
}
