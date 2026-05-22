package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

// ResolveMatrix finds the best-matching approval matrix for the given parameters.
// Returns (nil, nil) when no matrix is configured — callers must treat this as
// "engine skips" rather than an error.
func ResolveMatrix(
	ctx context.Context,
	pool *pgxpool.Pool,
	moduleCode, entityCode, transactionType string,
	amount float64,
) (*MatrixResult, error) {

	// Step 1: Find the most-specific matching matrix.
	var (
		matrixID      string
		approvalOrder string
		slaHours      *int
	)
	err := pool.QueryRow(ctx, `
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
		LIMIT 1`,
		moduleCode, entityCode, transactionType, amount,
	).Scan(&matrixID, &approvalOrder, &slaHours)

	if err != nil {
		if err == pgx.ErrNoRows {
			// No entity-specific matrix — try DEFAULT fallback.
			if entityCode != "DEFAULT" {
				err2 := pool.QueryRow(ctx, `
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
					  AND m.entity_code      = 'DEFAULT'
					  AND m.transaction_type = $2
					  AND m.is_active        = true
					  AND m.is_deleted       = false
					  AND ma.processing_status = 'APPROVED'
					  AND ma.action_type <> 'DELETE'
					  AND (m.min_amount IS NULL OR m.min_amount <= $3)
					  AND (m.max_amount IS NULL OR m.max_amount >= $3)
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
					ORDER BY COALESCE(m.min_amount,-1) DESC, COALESCE(m.max_amount,999999999) ASC
					LIMIT 1`,
					moduleCode, transactionType, amount,
				).Scan(&matrixID, &approvalOrder, &slaHours)
				if err2 != nil {
					if err2 == pgx.ErrNoRows {
						api.LogInfo("[ApprovalEngine] No matrix (entity or DEFAULT) for %s/%s/%s amount=%.2f",
							moduleCode, entityCode, transactionType, amount)
						return nil, nil
					}
					return nil, fmt.Errorf("ResolveMatrix DEFAULT fallback query: %w", err2)
				}
				api.LogInfo("[ApprovalEngine] Using DEFAULT matrix for %s/%s/%s", moduleCode, entityCode, transactionType)
			} else {
				api.LogInfo("[ApprovalEngine] No matrix for %s/%s/%s amount=%.2f",
					moduleCode, entityCode, transactionType, amount)
				return nil, nil
			}
		} else {
			return nil, fmt.Errorf("ResolveMatrix query: %w", err)
		}
	}

	// Step 2: Load all active eyes for this matrix.
	rows, err := pool.Query(ctx, `
		SELECT eye_id, position, eye_count, sla_hours
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
		if err := rows.Scan(&e.EyeID, &e.Position, &e.EyeCount, &e.SlaHours); err != nil {
			return nil, fmt.Errorf("ResolveMatrix eye scan: %w", err)
		}
		e.ApprovalsRequired = e.EyeCount / 2
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

	// Step 3: Attach escalation member info to each eye.
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

	result := &MatrixResult{
		MatrixID:      matrixID,
		ApprovalOrder: approvalOrder,
		SlaHours:      slaHours,
		Eyes:          eyes,
	}

	api.LogInfo("[ApprovalEngine] Resolved matrix=%s order=%s eyes=%d for %s/%s/%s",
		matrixID, approvalOrder, len(eyes), moduleCode, entityCode, transactionType)

	return result, nil
}
