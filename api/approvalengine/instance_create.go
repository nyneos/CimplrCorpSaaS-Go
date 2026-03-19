package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateInstance creates a new approval instance with all eyes for the given
// submission. Returns ("", nil) — not an error — when the engine is disabled
// for the module or when no matrix is configured for the given parameters.
// Callers must never treat an empty instanceID + nil error as a failure.
func CreateInstance(ctx context.Context, pool *pgxpool.Pool, req InstanceRequest) (string, error) {

	// Step 1: Feature-flag check.
	if !IsEngineEnabled(ctx, pool, req.ModuleCode) {
		api.LogInfo("[ApprovalEngine] Engine disabled for %s — skipping", req.ModuleCode)
		return "", nil
	}

	// Step 2: Resolve matrix.
	matrix, err := ResolveMatrix(ctx, pool, req.ModuleCode, req.EntityCode, req.TransactionType, req.Amount)
	if err != nil {
		return "", err
	}
	if matrix == nil {
		api.LogInfo("[ApprovalEngine] No matrix configured for %s/%s/%s — skipping",
			req.ModuleCode, req.EntityCode, req.TransactionType)
		return "", nil
	}

	// Step 3: Begin transaction.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("CreateInstance begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Step 4: INSERT approval_instance.
	// NOTE: audit_table IS a real column on uam.approval_instance.
	// audit_id_column does NOT exist — it is looked up at runtime via
	// LookupTxTableConfig(transactionType) wherever finalizeRecord needs it.
	var instanceID string
	err = tx.QueryRow(ctx, `
		INSERT INTO uam.approval_instance (
			matrix_id,        module_code,      entity_code,       transaction_type,
			record_id,        record_table,     audit_table,
			action_type,      status,           submitted_by,      submitted_by_email,
			submitted_at,     overall_sla_hours,
			overall_sla_deadline
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, 'PENDING', $9, $10,
			now(), $11,
			CASE
				WHEN $11::smallint IS NOT NULL
				THEN now() + ($11::smallint * interval '1 hour')
				ELSE NULL
			END
		) RETURNING instance_id`,
		matrix.MatrixID, req.ModuleCode, req.EntityCode, req.TransactionType,
		req.RecordID, req.RecordTable, req.AuditTable,
		req.ActionType, req.SubmittedBy, req.SubmittedByEmail, matrix.SlaHours,
	).Scan(&instanceID)
	if err != nil {
		return "", fmt.Errorf("CreateInstance insert instance: %w", err)
	}

	// Step 5: INSERT one approval_instance_eye row per eye.
	for _, eye := range matrix.Eyes {
		isActive := (matrix.ApprovalOrder == OrderParallel) || (eye.Position == 1)

		eyeStatus := EyeStatusWaiting
		var activatedAt *time.Time
		var slaDeadline *time.Time

		if isActive {
			eyeStatus = EyeStatusActive
			now := time.Now()
			activatedAt = &now
			if eye.SlaHours != nil {
				deadline := now.Add(time.Duration(*eye.SlaHours) * time.Hour)
				slaDeadline = &deadline
			}
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO uam.approval_instance_eye (
				instance_id,        matrix_eye_id,       position,          eye_count,
				approvals_required, approvals_received,  status,
				activated_at,       sla_hours,           sla_deadline,      is_escalated,
				escalated_to_user_id, escalated_to_role_id
			) VALUES (
				$1, $2, $3, $4,
				$5, 0, $6,
				$7, $8, $9, false,
				$10, $11
			)`,
			instanceID, eye.EyeID, eye.Position, eye.EyeCount,
			eye.ApprovalsRequired, eyeStatus,
			activatedAt, eye.SlaHours, slaDeadline,
			eye.EscalationUserID, eye.EscalationRoleID,
		)
		if err != nil {
			return "", fmt.Errorf("CreateInstance insert eye position=%d: %w", eye.Position, err)
		}
	}

	// Step 6: Commit.
	if err = tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("CreateInstance commit: %w", err)
	}

	api.LogInfo("[ApprovalEngine] Instance %s created: module=%s record=%s action=%s eyes=%d",
		instanceID, req.ModuleCode, req.RecordID, req.ActionType, len(matrix.Eyes))

	return instanceID, nil
}

// CancelPendingInstances cancels all PENDING approval instances for a given
// record_id + module_code combination, marking all their WAITING/ACTIVE eyes
// as SKIPPED. Call this BEFORE CreateInstance whenever an edit or re-submission
// should reset the approval chain (e.g. UpdateBooking, ResolveVariance).
func CancelPendingInstances(ctx context.Context, pool *pgxpool.Pool, moduleCode, recordID, cancelledByEmail string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("CancelPendingInstances begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Fetch all PENDING instance IDs for this record.
	rows, err := tx.Query(ctx, `
		SELECT instance_id
		FROM uam.approval_instance
		WHERE record_id   = $1
		  AND module_code = $2
		  AND status      = 'PENDING'
		  AND is_deleted  = false`,
		recordID, moduleCode,
	)
	if err != nil {
		return fmt.Errorf("CancelPendingInstances query: %w", err)
	}
	var instanceIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("CancelPendingInstances scan: %w", err)
		}
		instanceIDs = append(instanceIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("CancelPendingInstances rows.Err: %w", err)
	}

	if len(instanceIDs) == 0 {
		return nil // nothing to cancel
	}

	// Cancel all their non-terminal eyes.
	if _, err := tx.Exec(ctx, `
		UPDATE uam.approval_instance_eye
		SET status = 'SKIPPED', resolved_at = now()
		WHERE instance_id = ANY($1::text[])
		  AND status NOT IN ('APPROVED','REJECTED','SKIPPED')`,
		instanceIDs,
	); err != nil {
		return fmt.Errorf("CancelPendingInstances skip eyes: %w", err)
	}

	// Cancel the instances themselves.
	if _, err := tx.Exec(ctx, `
		UPDATE uam.approval_instance
		SET status           = 'CANCELLED',
		    resolved_at      = now(),
		    resolved_by_email = $1
		WHERE instance_id = ANY($2::text[])`,
		cancelledByEmail, instanceIDs,
	); err != nil {
		return fmt.Errorf("CancelPendingInstances cancel instances: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("CancelPendingInstances commit: %w", err)
	}
	api.LogInfo("[ApprovalEngine] Cancelled %d pending instance(s) for record=%s module=%s by=%s",
		len(instanceIDs), recordID, moduleCode, cancelledByEmail)
	return nil
}
