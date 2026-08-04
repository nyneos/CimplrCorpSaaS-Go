package approvalMatrix

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PolicyEngineModuleCode marks matrices owned by the policy engine. They are
// stored in the same uam.approval_matrix_master table as module matrices but
// carry no entity scope and no amount slab.
const PolicyEngineModuleCode = "POLICY_ENGINE"

// policyEngineEntityCode is the fixed entity_code for policy engine matrices —
// they apply regardless of which entity the underlying transaction belongs to.
const policyEngineEntityCode = "DEFAULT"

// CreatePolicyEngineMatrix creates a policy engine approval matrix.
//
// It deliberately does NOT reuse CreateApprovalMatrix: module matrices are
// scoped by entity and amount slab and validate both, whereas a policy engine
// matrix is keyed on transaction_type alone. Only one may exist per
// transaction type.
func CreatePolicyEngineMatrix(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string     `json:"user_id"`
			TransactionType string     `json:"transaction_type"`
			Description     *string    `json:"description"`
			ApprovalOrder   string     `json:"approval_order"`
			SlaHours        *int       `json:"sla_hours"`
			IsActive        *bool      `json:"is_active"`
			Eyes            []EyeInput `json:"eyes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.TransactionType == "" {
			api.RespondWithError(w, http.StatusBadRequest, "transaction_type is required")
			return
		}
		if req.ApprovalOrder != "PARALLEL" && req.ApprovalOrder != "SEQUENTIAL" {
			api.RespondWithError(w, http.StatusBadRequest, "approval_order must be PARALLEL or SEQUENTIAL")
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		effectiveSla := req.SlaHours
		for i := range req.Eyes {
			if req.Eyes[i].SlaHours == nil {
				continue
			}
			if effectiveSla == nil || *req.Eyes[i].SlaHours > *effectiveSla {
				v := *req.Eyes[i].SlaHours
				effectiveSla = &v
			}
		}
		if effectiveSla != nil && *effectiveSla <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "sla_hours must be > 0")
			return
		}

		isActive := true
		if req.IsActive != nil {
			isActive = *req.IsActive
		}

		ctx := r.Context()
		var duplicates int
		if err := pgxPool.QueryRow(ctx, `
			SELECT COUNT(*) FROM uam.approval_matrix_master
			WHERE module_code = $1 AND transaction_type = $2 AND is_deleted = false`,
			PolicyEngineModuleCode, req.TransactionType,
		).Scan(&duplicates); err != nil {
			logDBError(err, "policy engine matrix duplicate check")
			api.RespondWithError(w, http.StatusInternalServerError, "Could not verify existing policy engine matrices")
			return
		}
		if duplicates > 0 {
			api.RespondWithError(w, http.StatusConflict,
				fmt.Sprintf("A policy engine approval matrix already exists for %s.", req.TransactionType))
			return
		}

		allUserIDs, errMsg := validatePolicyEngineEyes(req.Eyes)
		if errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}
		if err := validateUserIDsExist(ctx, pgxPool, allUserIDs); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)

		var matrixID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO uam.approval_matrix_master
				(module_code,entity_code,transaction_type,min_amount,max_amount,description,approval_order,sla_hours,is_active)
			VALUES ($1,$2,$3,NULL,NULL,$4,$5,$6,$7) RETURNING matrix_id`,
			PolicyEngineModuleCode, policyEngineEntityCode, req.TransactionType,
			req.Description, req.ApprovalOrder, effectiveSla, isActive,
		).Scan(&matrixID); err != nil {
			msg, status := getUserFriendlyApprovalMatrixError(err, "Create policy engine matrix failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_master
				(matrix_id,action_type,processing_status,requested_by,requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, matrixID, userEmail,
		); err != nil {
			logDBError(err, "policy engine master audit insert")
			msg, status := getUserFriendlyApprovalMatrixError(err, "Audit insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		totalMembers, err := insertPolicyEngineEyes(ctx, tx, matrixID, userEmail, req.Eyes)
		if err != nil {
			msg, status := getUserFriendlyApprovalMatrixError(err, "Create policy engine eyes failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if !isActive {
			if _, err := tx.Exec(ctx, `
				UPDATE uam.approval_matrix_eye SET is_active = false
				WHERE matrix_id = $1 AND is_deleted = false`, matrixID); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Deactivate eyes failed")
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE uam.approval_matrix_eye_member SET is_active = false
				WHERE matrix_id = $1 AND is_deleted = false`, matrixID); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Deactivate members failed")
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "CreatePolicyEngineMatrix commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"matrix_id": matrixID, "eyes_created": len(req.Eyes),
			"members_created": totalMembers, "requested_by": userEmail,
		})
		api.LogInfo("PolicyEngineMatrix created: matrix_id=%s tx_type=%s eyes=%d members=%d by=%s",
			matrixID, req.TransactionType, len(req.Eyes), totalMembers, userEmail)
	}
}

// validatePolicyEngineEyes applies the same eye/member rules as the module
// matrix path and returns every user_id referenced, plus a user-facing error.
func validatePolicyEngineEyes(eyes []EyeInput) ([]string, string) {
	if len(eyes) == 0 {
		return nil, "at least one eye with approvers is required"
	}
	var allUserIDs []string
	for i := range eyes {
		eye := &eyes[i]
		if err := validateEyeFields(eye.EyeCount, eye.Position, eye.SlaHours); err != nil {
			return nil, fmt.Sprintf("eye[%d]: %s", i, err.Error())
		}
		if len(eye.Members) == 0 {
			return nil, fmt.Sprintf("eye[%d] (position %d): at least 1 member is required", i, eye.Position)
		}
		if len(eye.Members) > 5 {
			return nil, fmt.Sprintf("eye[%d] (position %d): a maximum of 5 members is allowed (got %d)",
				i, eye.Position, len(eye.Members))
		}
		hasApprover, hasEscalation := false, false
		for j := range eye.Members {
			m := &eye.Members[j]
			if m.SlotOrder == 0 {
				m.SlotOrder = 1
			}
			if err := validateMemberFields(m.MemberType, m.AssignmentType, m.RoleID, m.UserID, m.SlotOrder); err != nil {
				return nil, fmt.Sprintf("eye[%d].member[%d]: %s", i, j, err.Error())
			}
			switch m.MemberType {
			case "APPROVER":
				hasApprover = true
			case "ESCALATION":
				hasEscalation = true
			}
			if (m.AssignmentType == "USER_ONLY" || m.AssignmentType == "ROLE_USER") && m.UserID != nil && *m.UserID != "" {
				allUserIDs = append(allUserIDs, *m.UserID)
			}
		}
		if hasEscalation && !hasApprover {
			return nil, fmt.Sprintf("eye[%d] (position %d): an ESCALATION eye must contain at least one APPROVER member",
				i, eye.Position)
		}
		eye.EyeCount = normaliseEyeCount(eye.Position)
	}
	return allUserIDs, ""
}

// insertPolicyEngineEyes writes eye + member rows and their audit rows for a
// policy engine matrix, returning the total member count.
func insertPolicyEngineEyes(ctx context.Context, tx pgx.Tx, matrixID, userEmail string, eyes []EyeInput) (int, error) {
	totalMembers := 0
	for _, eye := range eyes {
		var eyeID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO uam.approval_matrix_eye (matrix_id,eye_count,position,sla_hours)
			VALUES ($1,$2,$3,$4) RETURNING eye_id`,
			matrixID, eye.EyeCount, eye.Position, eye.SlaHours,
		).Scan(&eyeID); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_eye
				(eye_id,matrix_id,action_type,processing_status,requested_by,requested_at)
			VALUES ($1,$2,'CREATE','PENDING_APPROVAL',$3,now())`, eyeID, matrixID, userEmail,
		); err != nil {
			return 0, err
		}
		for _, m := range eye.Members {
			var memberID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO uam.approval_matrix_eye_member
					(eye_id,matrix_id,member_type,assignment_type,role_id,user_id,slot_order)
				VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING member_id`,
				eyeID, matrixID, m.MemberType, m.AssignmentType, m.RoleID, m.UserID, m.SlotOrder,
			).Scan(&memberID); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO uam.audit_approval_matrix_eye_member
					(member_id,eye_id,matrix_id,action_type,processing_status,requested_by,requested_at)
				VALUES ($1,$2,$3,'CREATE','PENDING_APPROVAL',$4,now())`,
				memberID, eyeID, matrixID, userEmail,
			); err != nil {
				return 0, err
			}
			totalMembers++
		}
	}
	return totalMembers, nil
}
