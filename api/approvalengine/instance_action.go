package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// canUserActOnEye returns true if the given user is eligible to act on the
// specified eye, either via direct/role membership or via the escalation path.
func canUserActOnEye(ctx context.Context, tx pgx.Tx, userID, matrixEyeID, instanceEyeID string) (bool, error) {
	// Query 1: Direct or role-based APPROVER membership.
	var directCount int
	err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM uam.approval_matrix_eye_member m
		WHERE m.eye_id      = $1
		  AND m.member_type = 'APPROVER'
		  AND m.is_deleted  = false
		  AND m.is_active   = true
		  AND (
		    (m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2)
		    OR
		    (m.assignment_type = 'ROLE_ONLY' AND EXISTS (
		      SELECT 1 FROM public.user_roles ur
		      WHERE ur.role_id = m.role_id AND ur.user_id = $2
		    ))
		    OR
		    (m.assignment_type = 'ROLE_USER' AND EXISTS (
		      SELECT 1 FROM public.user_roles ur
		      WHERE ur.role_id = m.role_id AND ur.user_id = $2
		    ))
		  )`,
		matrixEyeID, userID,
	).Scan(&directCount)
	if err != nil {
		return false, fmt.Errorf("canUserActOnEye direct check: %w", err)
	}
	if directCount > 0 {
		return true, nil
	}

	// Query 2: Escalation path (only if this eye has been escalated).
	var isEscalated bool
	var escUserID, escRoleID *string
	err = tx.QueryRow(ctx, `
		SELECT is_escalated, escalated_to_user_id, escalated_to_role_id
		FROM uam.approval_instance_eye
		WHERE instance_eye_id = $1`,
		instanceEyeID,
	).Scan(&isEscalated, &escUserID, &escRoleID)
	if err != nil {
		return false, fmt.Errorf("canUserActOnEye escalation lookup: %w", err)
	}

	if isEscalated {
		if escUserID != nil && *escUserID == userID {
			return true, nil
		}
		if escRoleID != nil && *escRoleID != "" {
			var roleCount int
			if err := tx.QueryRow(ctx,
				`SELECT COUNT(*) FROM public.user_roles WHERE role_id = $1 AND user_id = $2`,
				*escRoleID, userID,
			).Scan(&roleCount); err != nil {
				return false, fmt.Errorf("canUserActOnEye escalation role check: %w", err)
			}
			if roleCount > 0 {
				return true, nil
			}
		}
	}

	return false, nil
}

// RecordAction processes one APPROVED or REJECTED action from an approver.
// It locks the eye row, validates the actor, records the action, and advances
// or finalises the instance as needed.
func RecordAction(ctx context.Context, pool *pgxpool.Pool, req ActionRequest) error {

	// Step 1: Begin transaction.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("RecordAction begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Step 2: Fetch eye + instance — lock the eye row for update.
	// NOTE: approval_order lives on approval_matrix_master, not approval_instance.
	// audit_id_column does NOT exist on approval_instance; it is derived at
	// runtime from LookupTxTableConfig(transactionType). audit_table IS a real column.
	var (
		instanceID      string
		matrixEyeID     string
		eyeStatus       string
		approvalsReq    int
		approvalsRcvd   int
		currentPos      int
		approvalOrder   string
		recordID        string
		recordTable     string
		auditTable      string
		transactionType string
		actionType      string
		instanceStatus  string
	)
	err = tx.QueryRow(ctx, `
		SELECT
		  ie.instance_id,        ie.matrix_eye_id,       ie.status,
		  ie.approvals_required, ie.approvals_received,  ie.position,
		  mm.approval_order,     inst.record_id,         inst.record_table,
		  inst.audit_table,      inst.transaction_type,  inst.action_type,
		  inst.status
		FROM uam.approval_instance_eye ie
		JOIN uam.approval_instance      inst ON inst.instance_id = ie.instance_id
		JOIN uam.approval_matrix_master mm   ON mm.matrix_id     = inst.matrix_id
		WHERE ie.instance_eye_id = $1
		FOR UPDATE OF ie`,
		req.InstanceEyeID,
	).Scan(
		&instanceID, &matrixEyeID, &eyeStatus,
		&approvalsReq, &approvalsRcvd, &currentPos,
		&approvalOrder, &recordID, &recordTable,
		&auditTable, &transactionType, &actionType,
		&instanceStatus,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("eye not found: %s", req.InstanceEyeID)
		}
		return fmt.Errorf("RecordAction fetch eye: %w", err)
	}
	// audit_id_column is not stored in the DB — derive it from the transaction type.
	_, auditIDColumn := LookupTxTableConfig(transactionType)

	if eyeStatus != EyeStatusActive {
		return fmt.Errorf("eye is not active (current status: %s)", eyeStatus)
	}
	if instanceStatus != InstStatusPending {
		return fmt.Errorf("instance is not pending (current status: %s)", instanceStatus)
	}

	// Step 3: Validate actor eligibility.
	allowed, err := canUserActOnEye(ctx, tx, req.ActorUserID, matrixEyeID, req.InstanceEyeID)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("unauthorized: user %s cannot act on eye %s", req.ActorUserID, req.InstanceEyeID)
	}

	// Step 4: Check for duplicate action from same user on this eye.
	var dupCount int
	err = tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM uam.approval_instance_action
		WHERE instance_eye_id = $1
		  AND actor_user_id   = $2
		  AND action_type IN ('APPROVED','REJECTED')`,
		req.InstanceEyeID, req.ActorUserID,
	).Scan(&dupCount)
	if err != nil {
		return fmt.Errorf("RecordAction duplicate check: %w", err)
	}
	if dupCount > 0 {
		return fmt.Errorf("user %s has already acted on this eye", req.ActorUserID)
	}

	// Step 5: INSERT action record.
	var actorRoleID *string
	if req.ActorRoleID != "" {
		actorRoleID = &req.ActorRoleID
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO uam.approval_instance_action (
			instance_id, instance_eye_id, actor_user_id, actor_email,
			actor_role_id, action_type, comment, acted_at, is_system_action
		) VALUES ($1,$2,$3,$4,$5,$6,$7,now(),false)`,
		instanceID, req.InstanceEyeID, req.ActorUserID, req.ActorEmail,
		actorRoleID, req.ActionType, req.Comment,
	)
	if err != nil {
		return fmt.Errorf("RecordAction insert action: %w", err)
	}

	// Step 6: Handle REJECTED — immediately close eye and instance.
	if req.ActionType == ActionRejected {
		if _, err = tx.Exec(ctx, `
			UPDATE uam.approval_instance_eye
			SET status = 'REJECTED', resolved_at = now()
			WHERE instance_eye_id = $1`, req.InstanceEyeID); err != nil {
			return fmt.Errorf("RecordAction reject eye: %w", err)
		}
		if _, err = tx.Exec(ctx, `
			UPDATE uam.approval_instance
			SET status = 'REJECTED', resolved_at = now(), resolved_by_email = $1
			WHERE instance_id = $2`, req.ActorEmail, instanceID); err != nil {
			return fmt.Errorf("RecordAction reject instance: %w", err)
		}
		if err = finalizeRecord(ctx, tx, recordID, auditTable, auditIDColumn, recordTable,
			actionType, InstStatusRejected, req.ActorEmail, req.Comment); err != nil {
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("RecordAction reject commit: %w", err)
		}
		api.LogInfo("[ApprovalEngine] Instance %s REJECTED at eye=%s by %s",
			instanceID, req.InstanceEyeID, req.ActorEmail)
		return nil
	}

	// Step 7: Increment approvals_received for APPROVED action.
	var newCount int
	err = tx.QueryRow(ctx, `
		UPDATE uam.approval_instance_eye
		SET approvals_received = approvals_received + 1
		WHERE instance_eye_id = $1
		RETURNING approvals_received`,
		req.InstanceEyeID,
	).Scan(&newCount)
	if err != nil {
		return fmt.Errorf("RecordAction increment approvals: %w", err)
	}

	// Step 8: Eye still needs more approvals.
	if newCount < approvalsReq {
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("RecordAction partial commit: %w", err)
		}
		api.LogInfo("[ApprovalEngine] Eye %s: %d/%d approvals",
			req.InstanceEyeID, newCount, approvalsReq)
		return nil
	}

	// Step 9: Eye has enough approvals — mark it approved.
	if _, err = tx.Exec(ctx, `
		UPDATE uam.approval_instance_eye
		SET status = 'APPROVED', resolved_at = now()
		WHERE instance_eye_id = $1`, req.InstanceEyeID); err != nil {
		return fmt.Errorf("RecordAction approve eye: %w", err)
	}

	// Step 10: For SEQUENTIAL order, activate the next eye if one exists.
	if approvalOrder == OrderSequential {
		nextPosition := currentPos + 1
		var nextEyeID string
		var nextSlaHours *int
		nextErr := tx.QueryRow(ctx, `
			SELECT instance_eye_id, sla_hours
			FROM uam.approval_instance_eye
			WHERE instance_id = $1
			  AND position    = $2
			  AND status      = 'WAITING'`,
			instanceID, nextPosition,
		).Scan(&nextEyeID, &nextSlaHours)

		if nextErr == nil {
			// Next eye found — activate it.
			var slaDeadline *time.Time
			if nextSlaHours != nil {
				d := time.Now().Add(time.Duration(*nextSlaHours) * time.Hour)
				slaDeadline = &d
			}
			if _, err = tx.Exec(ctx, `
				UPDATE uam.approval_instance_eye
				SET status = 'ACTIVE', activated_at = now(), sla_deadline = $1
				WHERE instance_eye_id = $2`,
				slaDeadline, nextEyeID); err != nil {
				return fmt.Errorf("RecordAction activate next eye: %w", err)
			}
			if err = tx.Commit(ctx); err != nil {
				return fmt.Errorf("RecordAction sequential commit: %w", err)
			}
			api.LogInfo("[ApprovalEngine] Sequential: eye %s approved, activated next eye %s",
				req.InstanceEyeID, nextEyeID)
			return nil
		} else if nextErr != pgx.ErrNoRows {
			return fmt.Errorf("RecordAction next eye query: %w", nextErr)
		}
		// nextErr == pgx.ErrNoRows means this was the last eye — fall through to finalize.
	}

	// Step 11: Check whether any eyes are still non-terminal (covers PARALLEL + last SEQUENTIAL).
	var remaining int
	err = tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM uam.approval_instance_eye
		WHERE instance_id = $1
		  AND status NOT IN ('APPROVED','SKIPPED')`,
		instanceID,
	).Scan(&remaining)
	if err != nil {
		return fmt.Errorf("RecordAction remaining check: %w", err)
	}

	if remaining > 0 {
		// Parallel mode — other eyes still need approvals.
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("RecordAction parallel partial commit: %w", err)
		}
		return nil
	}

	// Step 12: All eyes done — finalize the instance as APPROVED.
	if _, err = tx.Exec(ctx, `
		UPDATE uam.approval_instance
		SET status = 'APPROVED', resolved_at = now(), resolved_by_email = $1
		WHERE instance_id = $2`, req.ActorEmail, instanceID); err != nil {
		return fmt.Errorf("RecordAction finalize instance: %w", err)
	}

	if err = finalizeRecord(ctx, tx, recordID, auditTable, auditIDColumn, recordTable,
		actionType, InstStatusApproved, req.ActorEmail, req.Comment); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("RecordAction finalize commit: %w", err)
	}

	api.LogInfo("[ApprovalEngine] Instance %s FULLY APPROVED: module=%s record=%s by=%s",
		instanceID, actionType, recordID, req.ActorEmail)
	return nil
}
