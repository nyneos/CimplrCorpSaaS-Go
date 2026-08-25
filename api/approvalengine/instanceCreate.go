package approvalengine

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"strings"
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

	if existing, existErr := LookupLatestInstanceID(ctx, pool, req.ModuleCode, req.RecordID); existErr == nil && existing != "" {
		var st string
		_ = pool.QueryRow(ctx,
			`SELECT status FROM uam.approval_instance WHERE instance_id=$1 AND is_deleted=false`,
			existing,
		).Scan(&st)
		if st == InstStatusPending {
			api.LogInfo("[ApprovalEngine] Reusing pending instance %s for %s/%s", existing, req.ModuleCode, req.RecordID)
			return existing, nil
		}
	}

	// Step 2: Matrix is attached only when a TriggerApproval policy pinned one.
	// Empty / dead pin must not ResolveMatrix — that is what made rows look
	// "stuck" on a default matrix when the policy never fired.
	var matrix *MatrixResult
	var err error
	if pin := strings.TrimSpace(req.MatrixID); pin != "" {
		matrix, err = LoadMatrixByID(ctx, pool, pin)
		if err != nil {
			return "", err
		}
		if matrix == nil {
			api.LogInfo("[ApprovalEngine] Pinned matrix %s is dead/unusable for %s/%s/%s — skipping approval",
				pin, req.ModuleCode, req.EntityCode, req.TransactionType)
		}
	}
	if matrix == nil && isAmountRoutedModule(req.ModuleCode) {
		matrix, err = ResolveMatrix(ctx, pool, req.ModuleCode, req.EntityCode, req.TransactionType, req.Amount)
		if err != nil {
			return "", err
		}
		if matrix != nil {
			api.LogInfo("[ApprovalEngine] FD amount matrix %s matched %s/%s amount=%.2f",
				matrix.MatrixID, req.ModuleCode, req.TransactionType, req.Amount)
		}
	}
	if matrix == nil {
		api.LogInfo("[ApprovalEngine] No policy-pinned matrix for %s/%s/%s — skipping approval",
			req.ModuleCode, req.EntityCode, req.TransactionType)
		if req.AutoApplyIfUnpinned {
			if applyErr := autoApplyUnpinned(ctx, pool, req); applyErr != nil {
				api.LogInfo("[ApprovalEngine] Auto-apply failed for %s/%s: %v",
					req.ModuleCode, req.RecordID, applyErr)
				return "", applyErr
			}
		}
		return "", nil
	}

	// Backfill audit metadata from the registry when callers omit it (common on FX paths).
	if strings.TrimSpace(req.AuditTable) == "" || strings.TrimSpace(req.AuditIDColumn) == "" {
		regAuditTable, regAuditIDColumn := LookupTxTableConfig(req.TransactionType)
		if strings.TrimSpace(req.AuditTable) == "" {
			req.AuditTable = regAuditTable
		}
		if strings.TrimSpace(req.AuditIDColumn) == "" {
			req.AuditIDColumn = regAuditIDColumn
		}
	}

	submittedByID, submittedByEmail := resolveSubmitter(ctx, pool, req.SubmittedBy, req.SubmittedByEmail)

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
	// submitted_by has FK to public.users(id): never insert email/blank.
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
		req.ActionType, submittedByID, submittedByEmail, matrix.SlaHours,
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

func resolveSubmitter(ctx context.Context, pool *pgxpool.Pool, submittedBy, submittedByEmail string) (*string, string) {
	submittedBy = strings.TrimSpace(submittedBy)
	submittedByEmail = strings.TrimSpace(submittedByEmail)
	email := submittedByEmail

	var id, em string
	err := pool.QueryRow(ctx, `
		SELECT u.id::text, COALESCE(NULLIF(u.email, ''), $2)
		FROM public.users u
		WHERE (
			($1 <> '' AND u.id::text = $1)
			OR ($1 <> '' AND lower(u.email) = lower($1))
			OR ($2 <> '' AND lower(u.email) = lower($2))
			OR (
				$1 <> '' AND position('@' in $1) = 0
				AND lower(u.employee_name) = lower($1)
				AND (SELECT COUNT(*) FROM public.users u2 WHERE lower(u2.employee_name) = lower($1)) = 1
			)
		)
		ORDER BY
			CASE
				WHEN $1 <> '' AND u.id::text = $1 THEN 0
				WHEN $1 <> '' AND lower(u.email) = lower($1) THEN 1
				WHEN $2 <> '' AND lower(u.email) = lower($2) THEN 2
				ELSE 3
			END
		LIMIT 1`,
		submittedBy, submittedByEmail,
	).Scan(&id, &em)
	if err != nil || strings.TrimSpace(id) == "" {
		if submittedBy != "" || submittedByEmail != "" {
			api.LogInfo("[ApprovalEngine] submitted_by %q / email %q did not match public.users — storing NULL", submittedBy, submittedByEmail)
		}
		if email == "" {
			email = submittedBy
		}
		return nil, email
	}
	if strings.TrimSpace(em) != "" {
		email = em
	}
	return &id, email
}

type PendingInstanceDiagnosis struct {
	HasPending            bool
	InstanceID            string
	MatrixID              string
	TransactionType       string
	MatrixStatus          string
	MatrixAction          string
	ActiveEyeID           string
	ActiveEyeStatus       string
	ActiveEyeAction       string
	ApprovedApproverCount int
	Eligible              bool
	Reason                string
	Approvers             []string
}

func (d PendingInstanceDiagnosis) IsStale() bool {
	if !d.HasPending {
		return false
	}
	if d.MatrixStatus != constants.StatusApproved || d.MatrixAction == "DELETE" {
		return true
	}
	if d.ActiveEyeID == "" {
		return true
	}
	if d.ActiveEyeStatus != constants.StatusApproved || d.ActiveEyeAction == "DELETE" {
		return true
	}
	if d.ApprovedApproverCount == 0 {
		return true
	}
	return false
}

type ActOnPendingResult struct {
	Acted          bool
	CancelledStale bool
	InstanceID     string
	InstanceStatus string
	Reason         string
	Diagnosis      PendingInstanceDiagnosis
}

func DiagnosePendingInstance(ctx context.Context, pool *pgxpool.Pool, moduleCode, recordID, userID string) (PendingInstanceDiagnosis, error) {
	var d PendingInstanceDiagnosis
	err := pool.QueryRow(ctx, `
		SELECT i.instance_id, COALESCE(i.matrix_id,''), COALESCE(i.transaction_type,''),
		       COALESCE(ma.processing_status,''), COALESCE(ma.action_type,''),
		       COALESCE(ie.instance_eye_id,''),
		       COALESCE(ea.processing_status,''), COALESCE(ea.action_type,'')
		FROM uam.approval_instance i
		LEFT JOIN LATERAL (
			SELECT action_type, processing_status
			FROM uam.audit_approval_matrix_master a
			WHERE a.matrix_id=i.matrix_id
			ORDER BY a.requested_at DESC, a.audit_id DESC
			LIMIT 1
		) ma ON true
		LEFT JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id AND ie.status='ACTIVE'
		LEFT JOIN LATERAL (
			SELECT action_type, processing_status
			FROM uam.audit_approval_matrix_eye a
			WHERE a.eye_id=ie.matrix_eye_id
			ORDER BY a.requested_at DESC, a.audit_id DESC
			LIMIT 1
		) ea ON true
		WHERE i.record_id=$1 AND i.module_code=$2 AND i.status='PENDING' AND COALESCE(i.is_deleted,false)=false
		ORDER BY i.submitted_at DESC
		LIMIT 1`,
		recordID, moduleCode,
	).Scan(&d.InstanceID, &d.MatrixID, &d.TransactionType, &d.MatrixStatus, &d.MatrixAction, &d.ActiveEyeID, &d.ActiveEyeStatus, &d.ActiveEyeAction)
	if err != nil {
		return d, nil
	}
	d.HasPending = true
	if d.MatrixStatus != constants.StatusApproved || d.MatrixAction == "DELETE" {
		d.Reason = fmt.Sprintf("pending approval instance %s uses matrix %s whose latest status is %s/%s; matrix is not approved-active", d.InstanceID, d.MatrixID, d.MatrixAction, d.MatrixStatus)
		return d, nil
	}
	if d.ActiveEyeID == "" {
		d.Reason = fmt.Sprintf("pending approval instance %s has no active eye", d.InstanceID)
		return d, nil
	}
	if d.ActiveEyeStatus != constants.StatusApproved || d.ActiveEyeAction == "DELETE" {
		d.Reason = fmt.Sprintf("pending approval instance %s uses active eye %s whose latest status is %s/%s; eye is not approved-active", d.InstanceID, d.ActiveEyeID, d.ActiveEyeAction, d.ActiveEyeStatus)
		return d, nil
	}
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM uam.approval_instance_eye ie
		JOIN uam.approval_matrix_eye_member m ON m.eye_id=ie.matrix_eye_id
		LEFT JOIN LATERAL (
			SELECT action_type, processing_status
			FROM uam.audit_approval_matrix_eye_member ma
			WHERE ma.member_id=m.member_id
			ORDER BY ma.requested_at DESC, ma.audit_id DESC
			LIMIT 1
		) mla ON true
		WHERE ie.instance_eye_id=$1
		  AND m.member_type='APPROVER'
		  AND m.is_active=true
		  AND m.is_deleted=false
		  AND COALESCE(mla.processing_status,'APPROVED')='APPROVED'
		  AND COALESCE(mla.action_type,'') <> 'DELETE'
		  AND (
			(m.assignment_type IN ('USER_ONLY','ROLE_USER') AND NULLIF(m.user_id,'') IS NOT NULL)
			OR (m.assignment_type IN ('ROLE_ONLY','ROLE_USER') AND NULLIF(m.role_id,'') IS NOT NULL)
		  )`,
		d.ActiveEyeID,
	).Scan(&d.ApprovedApproverCount)
	if d.ApprovedApproverCount == 0 {
		d.Reason = fmt.Sprintf("pending approval instance %s uses active eye %s with no valid approved approver members", d.InstanceID, d.ActiveEyeID)
		return d, nil
	}
	var eligible int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM uam.approval_instance_eye ie
		JOIN uam.approval_matrix_eye_member m ON m.eye_id=ie.matrix_eye_id
		LEFT JOIN LATERAL (
			SELECT action_type, processing_status
			FROM uam.audit_approval_matrix_eye_member ma
			WHERE ma.member_id=m.member_id
			ORDER BY ma.requested_at DESC, ma.audit_id DESC
			LIMIT 1
		) mla ON true
		WHERE ie.instance_eye_id=$1
		  AND m.member_type='APPROVER'
		  AND m.is_active=true
		  AND m.is_deleted=false
		  AND COALESCE(mla.processing_status,'APPROVED')='APPROVED'
		  AND COALESCE(mla.action_type,'') <> 'DELETE'
		  AND `+sqlUserOnEyeMember("$2"),
		d.ActiveEyeID, userID,
	).Scan(&eligible)
	d.Eligible = eligible > 0

	rows, err := pool.Query(ctx, `
		SELECT DISTINCT COALESCE(u.email, u.employee_name, r.name, r.rolecode, m.role_id, m.user_id, m.assignment_type, '')
		FROM uam.approval_instance_eye ie
		JOIN uam.approval_matrix_eye_member m ON m.eye_id=ie.matrix_eye_id
		LEFT JOIN public.users u ON u.id::text=m.user_id
		LEFT JOIN public.roles r ON r.id::text=m.role_id
		WHERE ie.instance_eye_id=$1
		  AND m.member_type='APPROVER'
		  AND m.is_active=true
		  AND m.is_deleted=false`,
		d.ActiveEyeID,
	)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var approver string
			if scanErr := rows.Scan(&approver); scanErr == nil && strings.TrimSpace(approver) != "" {
				d.Approvers = append(d.Approvers, approver)
			}
		}
	}
	if !d.Eligible {
		if len(d.Approvers) > 0 {
			d.Reason = fmt.Sprintf("not your turn in approval sequence; active eye %s can be approved by: %s", d.ActiveEyeID, strings.Join(d.Approvers, ", "))
		} else {
			d.Reason = fmt.Sprintf("not your turn in approval sequence; active eye %s has no valid approved approver members", d.ActiveEyeID)
		}
	}
	return d, nil
}

// ActOnPendingRequest groups the parameters for ActOnPendingOrDiagnose.
type ActOnPendingRequest struct {
	ModuleCode string
	RecordID   string
	UserID     string
	UserEmail  string
	RoleID     string
	Action     string
	Comment    string
}

func ActOnPendingOrDiagnose(ctx context.Context, pool *pgxpool.Pool, req ActOnPendingRequest) (ActOnPendingResult, error) {
	return actOnPendingOrDiagnose(ctx, pool, req, false)
}

func actOnPendingOrDiagnose(ctx context.Context, pool *pgxpool.Pool, req ActOnPendingRequest, healed bool) (ActOnPendingResult, error) {
	moduleCode := req.ModuleCode
	recordID := req.RecordID
	userID := req.UserID
	userEmail := req.UserEmail
	roleID := req.RoleID
	action := req.Action
	comment := req.Comment
	var result ActOnPendingResult
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT ie.instance_eye_id
		FROM uam.approval_instance i
		JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id AND ie.status='ACTIVE'
		LEFT JOIN uam.approval_matrix_eye_member m ON m.eye_id=ie.matrix_eye_id
		  AND m.member_type='APPROVER'
		  AND m.is_active=true
		  AND m.is_deleted=false
		LEFT JOIN LATERAL (
			SELECT action_type, processing_status
			FROM uam.audit_approval_matrix_eye_member ma
			WHERE ma.member_id=m.member_id
			ORDER BY ma.requested_at DESC, ma.audit_id DESC
			LIMIT 1
		) mla ON true
		WHERE i.record_id=$1
		  AND i.module_code=$2
		  AND i.status='PENDING'
		  AND COALESCE(i.is_deleted,false)=false
		  AND (
			(
			  COALESCE(mla.processing_status,'APPROVED')='APPROVED'
			  AND COALESCE(mla.action_type,'') <> 'DELETE'
			  AND `+sqlUserOnEyeMember("$3")+`
			)
			OR (
			  ie.is_escalated=true
			  AND (
				ie.escalated_to_user_id=$3
				OR EXISTS (
					SELECT 1 FROM public.user_roles ur
					WHERE ur.role_id::text=ie.escalated_to_role_id::text AND ur.user_id::text=$3
				)
			  )
			)
		  )
		ORDER BY ie.instance_eye_id`,
		recordID, moduleCode, userID,
	)
	if err != nil {
		return result, fmt.Errorf("ActOnPendingOrDiagnose eligible eye query: %w", err)
	}
	defer rows.Close()

	var recordErr error
	for rows.Next() {
		var eyeID string
		if err := rows.Scan(&eyeID); err != nil {
			return result, fmt.Errorf("ActOnPendingOrDiagnose eye scan: %w", err)
		}
		recordErr = RecordAction(ctx, pool, ActionRequest{
			InstanceEyeID: eyeID,
			ActorUserID:   userID,
			ActorEmail:    userEmail,
			ActorRoleID:   roleID,
			ActionType:    action,
			Comment:       comment,
		})
		if recordErr == nil {
			result.Acted = true
			_ = pool.QueryRow(ctx, `
				SELECT i.instance_id, i.status
				FROM uam.approval_instance i
				JOIN uam.approval_instance_eye ie ON ie.instance_id=i.instance_id
				WHERE ie.instance_eye_id=$1`, eyeID).Scan(&result.InstanceID, &result.InstanceStatus)
			return result, nil
		}
		if !strings.Contains(recordErr.Error(), "unauthorized") && !strings.Contains(recordErr.Error(), "already acted") {
			return result, recordErr
		}
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("ActOnPendingOrDiagnose eye iteration: %w", err)
	}

	diag, diagErr := DiagnosePendingInstance(ctx, pool, moduleCode, recordID, userID)
	if diagErr != nil {
		return result, diagErr
	}
	result.Diagnosis = diag
	if !diag.HasPending {
		return applyNoPendingMatrixGate(ctx, pool, req, &result, healed)
	}
	result.InstanceID = diag.InstanceID
	if diag.IsStale() {
		if err := CancelPendingInstances(ctx, pool, moduleCode, recordID, userEmail); err != nil {
			return result, err
		}
		result.CancelledStale = true
		result.Reason = diag.Reason
		return result, nil
	}
	if diag.Reason != "" {
		result.Reason = diag.Reason
	} else if recordErr != nil {
		result.Reason = recordErr.Error()
	} else {
		result.Reason = "not your turn in approval sequence"
	}
	return result, nil
}

func PendingInstanceUsesUnapprovedMatrix(ctx context.Context, pool *pgxpool.Pool, moduleCode, recordID string) (bool, string) {
	d, _ := DiagnosePendingInstance(ctx, pool, moduleCode, recordID, "")
	if !d.HasPending {
		return false, ""
	}
	if d.MatrixStatus != constants.StatusApproved || d.MatrixAction == "DELETE" {
		return true, d.Reason
	}
	return false, ""
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
