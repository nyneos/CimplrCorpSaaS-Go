package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartSLAWorker runs as a long-lived goroutine. It polls for SLA-breached
// approval eyes every 10 minutes and escalates or skips them as configured.
// It never panics — all errors inside the cycle are logged and swallowed.
func StartSLAWorker(ctx context.Context, pool *pgxpool.Pool) {
	api.LogInfo("[SLA WORKER] Started — polling every 10 minutes")
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			api.LogInfo("[SLA WORKER] Context cancelled — stopping")
			return
		case <-ticker.C:
			func() {
				// Recover from any panic so the worker never dies.
				defer func() {
					if r := recover(); r != nil {
						api.LogError("[SLA WORKER] Panic recovered: %v", r)
					}
				}()
				cycleCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
				defer cancel()
				runSLACycle(cycleCtx, pool)
			}()
		}
	}
}

// breachedEye holds the data for one SLA-breached eye row from the DB.
// Note: audit_id_column is NOT a column on uam.approval_instance;
// it is derived at runtime via LookupTxTableConfig(transactionType).
// audit_table IS a real column and is read directly from the DB.
type breachedEye struct {
	eyeID             string
	instanceID        string
	matrixEyeID       string
	position          int
	escalationUserID  *string
	escalationRoleID  *string
	approvalsRequired int
	approvalsReceived int
	approvalOrder     string
	recordID          string
	recordTable       string
	auditTable        string // read from DB
	auditIDColumn     string // derived from LookupTxTableConfig, not read from DB
	actionType        string
	submittedBy       string
}

// runSLACycle finds all SLA-breached active eyes and handles each one in its
// own transaction. All errors are logged; none are returned.
func runSLACycle(ctx context.Context, pool *pgxpool.Pool) {
	// Step 1: Find breached eyes.
	// NOTE: approval_order lives on approval_matrix_master, not approval_instance.
	// audit_table IS a real column on approval_instance and is read directly.
	// audit_id_column does NOT exist in the DB — derived via LookupTxTableConfig.
	rows, err := pool.Query(ctx, `
		SELECT
		  ie.instance_eye_id,
		  ie.instance_id,
		  ie.matrix_eye_id,
		  ie.position,
		  ie.escalated_to_user_id,
		  ie.escalated_to_role_id,
		  ie.approvals_required,
		  ie.approvals_received,
		  mm.approval_order,
		  inst.record_id,
		  inst.record_table,
		  inst.audit_table,
		  inst.transaction_type,
		  inst.action_type,
		  inst.submitted_by
		FROM uam.approval_instance_eye ie
		JOIN uam.approval_instance      inst ON inst.instance_id = ie.instance_id
		JOIN uam.approval_matrix_master mm   ON mm.matrix_id     = inst.matrix_id
		WHERE ie.status       = 'ACTIVE'
		  AND ie.sla_deadline IS NOT NULL
		  AND ie.sla_deadline  < now()
		  AND ie.is_escalated  = false
		  AND inst.status      = 'PENDING'`)
	if err != nil {
		api.LogError("[SLA WORKER] Breached eyes query failed: %v", err)
		return
	}
	defer rows.Close()

	var eyes []breachedEye
	for rows.Next() {
		var e breachedEye
		var txType string
		if err := rows.Scan(
			&e.eyeID, &e.instanceID, &e.matrixEyeID, &e.position,
			&e.escalationUserID, &e.escalationRoleID,
			&e.approvalsRequired, &e.approvalsReceived,
			&e.approvalOrder,
			&e.recordID, &e.recordTable, &e.auditTable, &txType, &e.actionType,
			&e.submittedBy,
		); err != nil {
			api.LogError("[SLA WORKER] Scan breached eye: %v", err)
			continue
		}
		// audit_id_column is not stored in the DB — derive it from the transaction type.
		_, e.auditIDColumn = LookupTxTableConfig(txType)
		eyes = append(eyes, e)
	}
	if err := rows.Err(); err != nil {
		api.LogError("[SLA WORKER] Breached eyes iteration: %v", err)
	}

	// Step 2: Handle each eye in its own transaction.
	for _, eye := range eyes {
		processBreachedEye(ctx, pool, eye)
	}

	api.LogInfo("[SLA WORKER] Cycle done: %d breached eyes processed", len(eyes))
}

// processBreachedEye handles one SLA-breached eye in its own transaction.
func processBreachedEye(ctx context.Context, pool *pgxpool.Pool, eye breachedEye) {
	// Determine system actor user ID.
	systemActorID := eye.submittedBy // fallback
	var sysID string
	lookupErr := pool.QueryRow(ctx,
		`SELECT id FROM public.users WHERE email ILIKE '%system%' LIMIT 1`,
	).Scan(&sysID)
	if lookupErr == nil && sysID != "" {
		systemActorID = sysID
	}

	hasEscalation := (eye.escalationUserID != nil && *eye.escalationUserID != "") ||
		(eye.escalationRoleID != nil && *eye.escalationRoleID != "")

	if hasEscalation {
		// CASE A: Mark as escalated so the escalation target can approve.
		handleEscalation(ctx, pool, eye, systemActorID)
		return
	}

	if eye.approvalOrder == OrderSequential {
		// CASE B: Sequential without escalation — skip this eye, advance to next.
		handleSequentialSkip(ctx, pool, eye, systemActorID)
		return
	}

	// CASE C: Parallel without escalation — skip this eye.
	handleParallelSkip(ctx, pool, eye, systemActorID)
}

func handleEscalation(ctx context.Context, pool *pgxpool.Pool, eye breachedEye, systemActorID string) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		api.LogError("[SLA WORKER] Escalation begin tx eye=%s: %v", eye.eyeID, err)
		return
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err = tx.Exec(ctx, `
		UPDATE uam.approval_instance_eye
		SET is_escalated = true, escalated_at = now()
		WHERE instance_eye_id = $1`,
		eye.eyeID); err != nil {
		api.LogError("[SLA WORKER] Escalation eye update eye=%s: %v", eye.eyeID, err)
		return
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO uam.approval_instance_action (
			instance_id, instance_eye_id, actor_user_id, actor_email,
			action_type, comment, acted_at, is_system_action
		) VALUES ($1,$2,$3,'SYSTEM','AUTO_ESCALATED',
		  'SLA breached. Automatically escalated by system.',now(),true)`,
		eye.instanceID, eye.eyeID, systemActorID); err != nil {
		api.LogError("[SLA WORKER] Escalation action insert eye=%s: %v", eye.eyeID, err)
		return
	}

	if err = tx.Commit(ctx); err != nil {
		api.LogError("[SLA WORKER] Escalation commit eye=%s: %v", eye.eyeID, err)
		return
	}
	api.LogInfo("[SLA WORKER] Escalated eye=%s instance=%s", eye.eyeID, eye.instanceID)
}

func handleSequentialSkip(ctx context.Context, pool *pgxpool.Pool, eye breachedEye, systemActorID string) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		api.LogError("[SLA WORKER] SeqSkip begin tx eye=%s: %v", eye.eyeID, err)
		return
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err = tx.Exec(ctx, `
		UPDATE uam.approval_instance_eye
		SET status = 'SKIPPED', resolved_at = now()
		WHERE instance_eye_id = $1`,
		eye.eyeID); err != nil {
		api.LogError("[SLA WORKER] SeqSkip eye update eye=%s: %v", eye.eyeID, err)
		return
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO uam.approval_instance_action (
			instance_id, instance_eye_id, actor_user_id, actor_email,
			action_type, comment, acted_at, is_system_action
		) VALUES ($1,$2,$3,'SYSTEM','AUTO_ESCALATED',
		  'SLA breached, no escalation configured. Eye auto-skipped.',now(),true)`,
		eye.instanceID, eye.eyeID, systemActorID); err != nil {
		api.LogError("[SLA WORKER] SeqSkip action insert eye=%s: %v", eye.eyeID, err)
		return
	}

	// Find the next WAITING eye.
	nextPosition := eye.position + 1
	var nextEyeID string
	var nextSlaHours *int
	nextErr := tx.QueryRow(ctx, `
		SELECT instance_eye_id, sla_hours
		FROM uam.approval_instance_eye
		WHERE instance_id = $1 AND position = $2 AND status = 'WAITING'`,
		eye.instanceID, nextPosition,
	).Scan(&nextEyeID, &nextSlaHours)

	if nextErr == nil {
		// Activate next eye.
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
			api.LogError("[SLA WORKER] SeqSkip activate next eye=%s: %v", nextEyeID, err)
			return
		}
		if err = tx.Commit(ctx); err != nil {
			api.LogError("[SLA WORKER] SeqSkip commit eye=%s: %v", eye.eyeID, err)
			return
		}
		api.LogInfo("[SLA WORKER] Eye %s skipped (no escalation), activated next eye %s",
			eye.eyeID, nextEyeID)
		return
	}

	// No next eye — check if all eyes are now resolved.
	var remaining int
	if err = tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM uam.approval_instance_eye
		WHERE instance_id = $1 AND status NOT IN ('APPROVED','SKIPPED')`,
		eye.instanceID,
	).Scan(&remaining); err != nil {
		api.LogError("[SLA WORKER] SeqSkip remaining check eye=%s: %v", eye.eyeID, err)
		return
	}

	if remaining == 0 {
		if _, err = tx.Exec(ctx, `
			UPDATE uam.approval_instance
			SET status = 'APPROVED', resolved_at = now(), resolved_by_email = 'SYSTEM'
			WHERE instance_id = $1`, eye.instanceID); err != nil {
			api.LogError("[SLA WORKER] SeqSkip finalize instance=%s: %v", eye.instanceID, err)
			return
		}
		if err = finalizeRecord(ctx, tx, eye.recordID, eye.auditTable, eye.auditIDColumn,
			eye.recordTable, eye.actionType, InstStatusApproved,
			"system@auto", "Auto-approved: all eyes resolved"); err != nil {
			api.LogError("[SLA WORKER] SeqSkip finalizeRecord instance=%s: %v", eye.instanceID, err)
			return
		}
		if err = tx.Commit(ctx); err != nil {
			api.LogError("[SLA WORKER] SeqSkip finalize commit instance=%s: %v", eye.instanceID, err)
			return
		}
		api.LogInfo("[SLA WORKER] Instance %s auto-finalized after last eye skipped", eye.instanceID)
		return
	}

	if err = tx.Commit(ctx); err != nil {
		api.LogError("[SLA WORKER] SeqSkip final commit eye=%s: %v", eye.eyeID, err)
	}
}

func handleParallelSkip(ctx context.Context, pool *pgxpool.Pool, eye breachedEye, systemActorID string) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		api.LogError("[SLA WORKER] ParSkip begin tx eye=%s: %v", eye.eyeID, err)
		return
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err = tx.Exec(ctx, `
		UPDATE uam.approval_instance_eye
		SET status = 'SKIPPED', resolved_at = now()
		WHERE instance_eye_id = $1`,
		eye.eyeID); err != nil {
		api.LogError("[SLA WORKER] ParSkip eye update eye=%s: %v", eye.eyeID, err)
		return
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO uam.approval_instance_action (
			instance_id, instance_eye_id, actor_user_id, actor_email,
			action_type, comment, acted_at, is_system_action
		) VALUES ($1,$2,$3,'SYSTEM','AUTO_ESCALATED',
		  'SLA breached, parallel eye skipped.',now(),true)`,
		eye.instanceID, eye.eyeID, systemActorID); err != nil {
		api.LogError("[SLA WORKER] ParSkip action insert eye=%s: %v", eye.eyeID, err)
		return
	}

	// Check if all parallel eyes are now resolved.
	var remaining int
	if err = tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM uam.approval_instance_eye
		WHERE instance_id = $1 AND status NOT IN ('APPROVED','SKIPPED')`,
		eye.instanceID,
	).Scan(&remaining); err != nil {
		api.LogError("[SLA WORKER] ParSkip remaining check eye=%s: %v", eye.eyeID, err)
		return
	}

	if remaining == 0 {
		if _, err = tx.Exec(ctx, `
			UPDATE uam.approval_instance
			SET status = 'APPROVED', resolved_at = now(), resolved_by_email = 'SYSTEM'
			WHERE instance_id = $1`, eye.instanceID); err != nil {
			api.LogError("[SLA WORKER] ParSkip finalize instance=%s: %v", eye.instanceID, err)
			return
		}
		if err = finalizeRecord(ctx, tx, eye.recordID, eye.auditTable, eye.auditIDColumn,
			eye.recordTable, eye.actionType, InstStatusApproved,
			"system@auto", "Auto-approved: all parallel eyes resolved or skipped"); err != nil {
			api.LogError("[SLA WORKER] ParSkip finalizeRecord instance=%s: %v", eye.instanceID, err)
			return
		}
	}

	if err = tx.Commit(ctx); err != nil {
		api.LogError("[SLA WORKER] ParSkip commit eye=%s: %v", eye.eyeID, err)
		return
	}
	api.LogInfo("[SLA WORKER] Parallel eye %s skipped", eye.eyeID)
}
