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
		FROM uam.approval_matrix_master
		WHERE module_code      = $1
		  AND entity_code      = $2
		  AND transaction_type = $3
		  AND is_active        = true
		  AND is_deleted       = false
		  AND (min_amount IS NULL OR min_amount <= $4)
		  AND (max_amount IS NULL OR max_amount >= $4)
		ORDER BY
		  COALESCE(min_amount, -1)         DESC,
		  COALESCE(max_amount, 999999999)  ASC
		LIMIT 1`,
		moduleCode, entityCode, transactionType, amount,
	).Scan(&matrixID, &approvalOrder, &slaHours)

	if err != nil {
		if err == pgx.ErrNoRows {
			api.LogInfo("[ApprovalEngine] No matrix for %s/%s/%s amount=%.2f",
				moduleCode, entityCode, transactionType, amount)
			return nil, nil
		}
		return nil, fmt.Errorf("ResolveMatrix query: %w", err)
	}

	// Step 2: Load all active eyes for this matrix.
	rows, err := pool.Query(ctx, `
		SELECT eye_id, position, eye_count, sla_hours
		FROM uam.approval_matrix_eye
		WHERE matrix_id  = $1
		  AND is_deleted = false
		  AND is_active  = true
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
		eyes = append(eyes, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ResolveMatrix eyes iteration: %w", err)
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
