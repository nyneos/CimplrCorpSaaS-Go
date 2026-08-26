package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const policyEngineModuleCode = "POLICY_ENGINE"

const matchingMatrixSQL = `
		SELECT matrix_id, approval_order, sla_hours
		FROM uam.approval_matrix_master m
		JOIN LATERAL (
			SELECT action_type, processing_status
			FROM uam.audit_approval_matrix_master a
			WHERE a.matrix_id = m.matrix_id
			ORDER BY a.requested_at DESC, a.audit_id DESC
			LIMIT 1
		) ma ON true
		WHERE m.module_code      = $1
		  AND m.entity_code      = $2
		  AND m.transaction_type = $3
		  AND m.is_active        = true
		  AND m.is_deleted       = false
		  AND ma.processing_status = 'APPROVED'
		  AND ma.action_type <> 'DELETE'
		  AND (m.min_amount IS NULL OR m.min_amount <= $4)
		  AND (m.max_amount IS NULL OR m.max_amount >= $4)
		  AND EXISTS (
			SELECT 1
			FROM uam.approval_matrix_eye e
			JOIN LATERAL (
				SELECT action_type, processing_status
				FROM uam.audit_approval_matrix_eye ea
				WHERE ea.eye_id=e.eye_id
				ORDER BY ea.requested_at DESC, ea.audit_id DESC
				LIMIT 1
			) ela ON true
			WHERE e.matrix_id=m.matrix_id
			  AND e.is_active=true
			  AND e.is_deleted=false
			  AND ela.processing_status='APPROVED'
			  AND ela.action_type <> 'DELETE'
			  AND EXISTS (
				SELECT 1
				FROM uam.approval_matrix_eye_member mem
				JOIN LATERAL (
					SELECT action_type, processing_status
					FROM uam.audit_approval_matrix_eye_member ma2
					WHERE ma2.member_id=mem.member_id
					ORDER BY ma2.requested_at DESC, ma2.audit_id DESC
					LIMIT 1
				) mla ON true
				WHERE mem.eye_id=e.eye_id
				  AND mem.member_type='APPROVER'
				  AND mem.is_active=true
				  AND mem.is_deleted=false
				  AND mla.processing_status='APPROVED'
				  AND mla.action_type <> 'DELETE'
				  AND (
					(mem.assignment_type IN ('USER_ONLY','ROLE_USER') AND NULLIF(mem.user_id,'') IS NOT NULL)
					OR (mem.assignment_type IN ('ROLE_ONLY','ROLE_USER') AND NULLIF(mem.role_id,'') IS NOT NULL)
				  )
			  )
		  )
		ORDER BY
		  COALESCE(m.min_amount, -1)         DESC,
		  COALESCE(m.max_amount, 999999999)  ASC
		LIMIT 1`

// IsEngineEnabled checks whether the multi-eye approval engine is active for
// the given module. Defaults to false on any error so the legacy code path runs.
func IsEngineEnabled(ctx context.Context, pool *pgxpool.Pool, moduleCode string) bool {
	var enabled bool
	err := pool.QueryRow(ctx,
		`SELECT enabled FROM uam.engine_enabled_modules WHERE module_code = $1`,
		moduleCode,
	).Scan(&enabled)
	if err != nil {
		return false
	}
	return enabled
}

func lookupMatchingMatrix(
	ctx context.Context,
	pool *pgxpool.Pool,
	moduleCode, entityCode, transactionType string,
	amount float64,
) (matrixID, approvalOrder string, slaHours *int, err error) {
	err = pool.QueryRow(ctx, matchingMatrixSQL, moduleCode, entityCode, transactionType, amount).
		Scan(&matrixID, &approvalOrder, &slaHours)
	if err == pgx.ErrNoRows {
		return "", "", nil, nil
	}
	return matrixID, approvalOrder, slaHours, err
}

// resolvedMatrixInfo bundles the fields needed to finish resolving a matched
// approval matrix into a MatrixResult.
type resolvedMatrixInfo struct {
	MatrixID        string
	ApprovalOrder   string
	ModuleCode      string
	EntityCode      string
	TransactionType string
	SlaHours        *int
}

func finishResolvedMatrix(ctx context.Context, pool *pgxpool.Pool, info resolvedMatrixInfo) (*MatrixResult, error) {
	matrixID, approvalOrder, moduleCode, entityCode, transactionType, slaHours :=
		info.MatrixID, info.ApprovalOrder, info.ModuleCode, info.EntityCode, info.TransactionType, info.SlaHours
	eyes, err := loadMatrixEyes(ctx, pool, matrixID, approvalOrder)
	if err != nil {
		return nil, err
	}
	if len(eyes) == 0 {
		return nil, nil
	}
	api.LogInfo("[ApprovalEngine] Resolved matrix=%s order=%s eyes=%d for %s/%s/%s",
		matrixID, approvalOrder, len(eyes), moduleCode, entityCode, transactionType)
	return &MatrixResult{
		MatrixID:      matrixID,
		ApprovalOrder: approvalOrder,
		SlaHours:      slaHours,
		Eyes:          eyes,
	}, nil
}

// ResolveMatrix finds the best-matching approval matrix for the given parameters.
// A live Policy Engine matrix for the same transaction_type wins over the
// module matrix (CASH/FD/…) so sequential eyes configured on that screen
// actually gate the business transaction.
// Returns (nil, nil) when no matrix is configured — callers must treat this as
// "engine skips" rather than an error.
func ResolveMatrix(
	ctx context.Context,
	pool *pgxpool.Pool,
	moduleCode, entityCode, transactionType string,
	amount float64,
) (*MatrixResult, error) {

	if !strings.EqualFold(strings.TrimSpace(moduleCode), policyEngineModuleCode) {
		peID, peOrder, peSla, err := lookupMatchingMatrix(ctx, pool, policyEngineModuleCode, "DEFAULT", transactionType, amount)
		if err != nil {
			return nil, fmt.Errorf("ResolveMatrix POLICY_ENGINE lookup: %w", err)
		}
		if peID != "" {
			api.LogInfo("[ApprovalEngine] Using POLICY_ENGINE matrix %s for %s/%s (overrides %s module matrix)",
				peID, transactionType, entityCode, moduleCode)
			return finishResolvedMatrix(ctx, pool, resolvedMatrixInfo{
				MatrixID:        peID,
				ApprovalOrder:   peOrder,
				ModuleCode:      policyEngineModuleCode,
				EntityCode:      "DEFAULT",
				TransactionType: transactionType,
				SlaHours:        peSla,
			})
		}
	}

	matrixID, approvalOrder, slaHours, err := lookupMatchingMatrix(ctx, pool, moduleCode, entityCode, transactionType, amount)
	if err != nil {
		return nil, fmt.Errorf("ResolveMatrix query: %w", err)
	}
	if matrixID == "" && entityCode != "DEFAULT" {
		matrixID, approvalOrder, slaHours, err = lookupMatchingMatrix(ctx, pool, moduleCode, "DEFAULT", transactionType, amount)
		if err != nil {
			return nil, fmt.Errorf("ResolveMatrix DEFAULT fallback query: %w", err)
		}
		if matrixID != "" {
			api.LogInfo("[ApprovalEngine] Using DEFAULT matrix for %s/%s/%s", moduleCode, entityCode, transactionType)
		}
	}
	if matrixID == "" {
		api.LogInfo("[ApprovalEngine] No matrix for %s/%s/%s amount=%.2f",
			moduleCode, entityCode, transactionType, amount)
		return nil, nil
	}

	return finishResolvedMatrix(ctx, pool, resolvedMatrixInfo{
		MatrixID:        matrixID,
		ApprovalOrder:   approvalOrder,
		ModuleCode:      moduleCode,
		EntityCode:      entityCode,
		TransactionType: transactionType,
		SlaHours:        slaHours,
	})
}

// LoadMatrixByID returns a specific matrix by id, applying the same
// approved/active guards as ResolveMatrix but skipping entity/amount matching.
// Returns (nil, nil) when the matrix is missing, unapproved, deleted, inactive,
// or has no usable eyes — callers treat that as "engine skips".
func LoadMatrixByID(ctx context.Context, pool *pgxpool.Pool, matrixID string) (*MatrixResult, error) {
	var approvalOrder string
	var slaHours *int
	err := pool.QueryRow(ctx, `
		SELECT approval_order, sla_hours
		FROM uam.approval_matrix_master m
		JOIN LATERAL (
			SELECT action_type, processing_status
			FROM uam.audit_approval_matrix_master a
			WHERE a.matrix_id = m.matrix_id
			ORDER BY a.requested_at DESC, a.audit_id DESC
			LIMIT 1
		) ma ON true
		WHERE m.matrix_id  = $1
		  AND m.is_active  = true
		  AND m.is_deleted = false
		  AND ma.processing_status = 'APPROVED'
		  AND ma.action_type <> 'DELETE'`,
		matrixID,
	).Scan(&approvalOrder, &slaHours)
	if err != nil {
		if err == pgx.ErrNoRows {
			api.LogInfo("[ApprovalEngine] Matrix %s not found/approved/active — skipping", matrixID)
			return nil, nil
		}
		return nil, fmt.Errorf("LoadMatrixByID query: %w", err)
	}

	eyes, err := loadMatrixEyes(ctx, pool, matrixID, approvalOrder)
	if err != nil {
		return nil, err
	}
	if len(eyes) == 0 {
		return nil, nil
	}

	api.LogInfo("[ApprovalEngine] Pinned matrix=%s order=%s eyes=%d", matrixID, approvalOrder, len(eyes))
	return &MatrixResult{
		MatrixID:      matrixID,
		ApprovalOrder: approvalOrder,
		SlaHours:      slaHours,
		Eyes:          eyes,
	}, nil
}

// loadMatrixEyes loads every approved+active eye of a matrix along with its
// approver count and escalation target. Returns an empty slice when the matrix
// has no usable eyes.
func loadMatrixEyes(ctx context.Context, pool *pgxpool.Pool, matrixID, approvalOrder string) ([]MatrixEye, error) {
	rows, err := pool.Query(ctx, `
		SELECT e.eye_id, e.position, e.eye_count, e.sla_hours,
		       (
		           SELECT COUNT(*)
		           FROM uam.approval_matrix_eye_member mem
		           JOIN LATERAL (
		               SELECT processing_status, action_type
		               FROM uam.audit_approval_matrix_eye_member ma2
		               WHERE ma2.member_id = mem.member_id
		               ORDER BY requested_at DESC, audit_id DESC
		               LIMIT 1
		           ) mla ON true
		           WHERE mem.eye_id = e.eye_id
		             AND mem.member_type = 'APPROVER'
		             AND mem.is_active = true
		             AND mem.is_deleted = false
		             AND mla.processing_status = 'APPROVED'
		             AND mla.action_type <> 'DELETE'
		             AND (
		                 (mem.assignment_type IN ('USER_ONLY','ROLE_USER') AND NULLIF(mem.user_id,'') IS NOT NULL)
		                 OR (mem.assignment_type IN ('ROLE_ONLY','ROLE_USER') AND NULLIF(mem.role_id,'') IS NOT NULL)
		             )
		       ) AS approver_count
		FROM uam.approval_matrix_eye e
		JOIN LATERAL (
			SELECT action_type, processing_status
			FROM uam.audit_approval_matrix_eye ea
			WHERE ea.eye_id=e.eye_id
			ORDER BY ea.requested_at DESC, ea.audit_id DESC
			LIMIT 1
		) ela ON true
		WHERE e.matrix_id  = $1
		  AND e.is_deleted = false
		  AND e.is_active  = true
		  AND ela.processing_status='APPROVED'
		  AND ela.action_type <> 'DELETE'
		  AND EXISTS (
			SELECT 1
			FROM uam.approval_matrix_eye_member mem
			JOIN LATERAL (
				SELECT action_type, processing_status
				FROM uam.audit_approval_matrix_eye_member ma2
				WHERE ma2.member_id=mem.member_id
				ORDER BY ma2.requested_at DESC, ma2.audit_id DESC
				LIMIT 1
			) mla ON true
			WHERE mem.eye_id=e.eye_id
			  AND mem.member_type='APPROVER'
			  AND mem.is_active=true
			  AND mem.is_deleted=false
			  AND mla.processing_status='APPROVED'
			  AND mla.action_type <> 'DELETE'
			  AND (
				(mem.assignment_type IN ('USER_ONLY','ROLE_USER') AND NULLIF(mem.user_id,'') IS NOT NULL)
				OR (mem.assignment_type IN ('ROLE_ONLY','ROLE_USER') AND NULLIF(mem.role_id,'') IS NOT NULL)
			  )
		  )
		ORDER BY position ASC`,
		matrixID,
	)
	if err != nil {
		return nil, fmt.Errorf("ResolveMatrix eyes query: %w", err)
	}
	defer rows.Close()

	var eyes []MatrixEye
	for rows.Next() {
		var e MatrixEye
		var approverCount int
		if err := rows.Scan(&e.EyeID, &e.Position, &e.EyeCount, &e.SlaHours, &approverCount); err != nil {
			return nil, fmt.Errorf("ResolveMatrix eye scan: %w", err)
		}
		if approvalOrder == "PARALLEL" {
			e.ApprovalsRequired = 1
		} else {
			e.ApprovalsRequired = approverCount
		}
		if e.ApprovalsRequired < 1 {
			e.ApprovalsRequired = 1
		}
		eyes = append(eyes, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ResolveMatrix eyes iteration: %w", err)
	}
	if len(eyes) == 0 {
		api.LogInfo("[ApprovalEngine] Matrix %s has no approved active eyes with approved approver members — skipping", matrixID)
		return nil, nil
	}

	// Attach escalation member info to each eye.
	for i := range eyes {
		var escRoleID, escUserID *string
		err := pool.QueryRow(ctx, `
			SELECT role_id, user_id
			FROM uam.approval_matrix_eye_member
			WHERE eye_id      = $1
			  AND member_type = 'ESCALATION'
			  AND is_deleted  = false
			LIMIT 1`,
			eyes[i].EyeID,
		).Scan(&escRoleID, &escUserID)
		if err != nil && err != pgx.ErrNoRows {
			return nil, fmt.Errorf("ResolveMatrix escalation query eye=%s: %w", eyes[i].EyeID, err)
		}
		// err == pgx.ErrNoRows is fine — no escalation configured for this eye.
		eyes[i].EscalationRoleID = escRoleID
		eyes[i].EscalationUserID = escUserID
	}

	return eyes, nil
}
