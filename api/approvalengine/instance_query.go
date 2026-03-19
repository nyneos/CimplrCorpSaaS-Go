package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GetPendingForUser returns all active approval instances where the given user
// is eligible to act (either as a direct member, role member, or escalation target).
func GetPendingForUser(ctx context.Context, pool *pgxpool.Pool, userID string) ([]InstanceSummary, error) {
	rows, err := pool.Query(ctx, `
		SELECT
		  i.instance_id,
		  i.module_code,
		  i.transaction_type,
		  i.record_id,
		  i.action_type,
		  i.status,
		  i.submitted_by_email,
		  i.submitted_at,
		  ie.instance_eye_id   AS current_eye_id,
		  ie.position          AS current_position,
		  ie.approvals_required,
		  ie.approvals_received,
		  ie.sla_deadline,
		  ie.is_escalated
		FROM uam.approval_instance i
		JOIN uam.approval_instance_eye ie
		  ON ie.instance_id = i.instance_id
		  AND ie.status = 'ACTIVE'
		WHERE i.status     = 'PENDING'
		  AND i.is_deleted = false
		  AND (
		    EXISTS (
		      SELECT 1 FROM uam.approval_matrix_eye_member m
		      WHERE m.eye_id      = ie.matrix_eye_id
		        AND m.member_type = 'APPROVER'
		        AND m.is_deleted  = false
		        AND m.is_active   = true
		        AND (
		          (m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $1)
		          OR
		          (m.assignment_type = 'ROLE_ONLY' AND EXISTS (
		            SELECT 1 FROM public.user_roles ur
		            WHERE ur.role_id = m.role_id AND ur.user_id = $1
		          ))
		          OR
		          (m.assignment_type = 'ROLE_USER' AND EXISTS (
		            SELECT 1 FROM public.user_roles ur
		            WHERE ur.role_id = m.role_id AND ur.user_id = $1
		          ))
		        )
		    )
		    OR (
		      ie.is_escalated = true
		      AND (
		        ie.escalated_to_user_id = $1
		        OR EXISTS (
		          SELECT 1 FROM public.user_roles ur2
		          WHERE ur2.role_id = ie.escalated_to_role_id AND ur2.user_id = $1
		        )
		      )
		    )
		  )
		ORDER BY ie.sla_deadline ASC NULLS LAST, i.submitted_at ASC`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("GetPendingForUser query: %w", err)
	}
	defer rows.Close()

	summaries := make([]InstanceSummary, 0)
	for rows.Next() {
		var s InstanceSummary
		if err := rows.Scan(
			&s.InstanceID,
			&s.ModuleCode,
			&s.TransactionType,
			&s.RecordID,
			&s.ActionType,
			&s.Status,
			&s.SubmittedByEmail,
			&s.SubmittedAt,
			&s.CurrentEyeID,
			&s.CurrentPosition,
			&s.ApprovalsRequired,
			&s.ApprovalsReceived,
			&s.SlaDeadline,
			&s.IsEscalated,
		); err != nil {
			return nil, fmt.Errorf("GetPendingForUser scan: %w", err)
		}
		summaries = append(summaries, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetPendingForUser iteration: %w", err)
	}

	api.LogInfo("[ApprovalEngine] GetPendingForUser: user=%s count=%d", userID, len(summaries))
	return summaries, nil
}

// GetInstanceDetail returns the full state of one approval instance including
// all eyes and all actions taken within each eye.
func GetInstanceDetail(ctx context.Context, pool *pgxpool.Pool, instanceID string) (*InstanceDetail, error) {

	// Query 1: The instance row.
	var d InstanceDetail
	var slaH16 *int16
	err := pool.QueryRow(ctx, `
		SELECT
		  instance_id, matrix_id, module_code, entity_code, transaction_type,
		  record_id, record_table, action_type, status, submitted_by_email,
		  submitted_at, overall_sla_hours, overall_sla_deadline,
		  resolved_at, COALESCE(resolved_by_email,'')
		FROM uam.approval_instance
		WHERE instance_id = $1
		  AND is_deleted  = false`,
		instanceID,
	).Scan(
		&d.InstanceID, &d.MatrixID, &d.ModuleCode, &d.EntityCode, &d.TransactionType,
		&d.RecordID, &d.RecordTable, &d.ActionType, &d.Status, &d.SubmittedByEmail,
		&d.SubmittedAt, &slaH16, &d.OverallSlaDeadline,
		&d.ResolvedAt, &d.ResolvedByEmail,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("instance not found: %s", instanceID)
		}
		return nil, fmt.Errorf("GetInstanceDetail instance query: %w", err)
	}
	if slaH16 != nil {
		v := int(*slaH16)
		d.OverallSlaHours = &v
	}

	// Query 2: All eyes with their actions via LEFT JOIN.
	// acted_at can be NULL (no actions on an eye yet), so we scan into *time.Time.
	rows, err := pool.Query(ctx, `
		SELECT
		  ie.instance_eye_id,    ie.matrix_eye_id,    ie.position,     ie.eye_count,
		  ie.approvals_required, ie.approvals_received, ie.status,
		  ie.activated_at,       ie.sla_deadline,     ie.resolved_at,
		  ie.is_escalated,       ie.escalated_at,
		  COALESCE(ia.action_id,''),
		  COALESCE(ia.actor_email,''),
		  COALESCE(ia.actor_role_id,''),
		  COALESCE(ia.action_type,''),
		  COALESCE(ia.comment,''),
		  ia.acted_at,
		  COALESCE(ia.is_system_action, false)
		FROM uam.approval_instance_eye ie
		LEFT JOIN uam.approval_instance_action ia
		  ON ia.instance_eye_id = ie.instance_eye_id
		WHERE ie.instance_id = $1
		ORDER BY ie.position ASC, ia.acted_at ASC`,
		instanceID,
	)
	if err != nil {
		return nil, fmt.Errorf("GetInstanceDetail eyes query: %w", err)
	}
	defer rows.Close()

	// Build nested structure using a map to deduplicate eye rows from the join.
	eyeMap := make(map[string]*EyeDetail)
	eyeOrder := make([]string, 0)

	for rows.Next() {
		var (
			eyeID             string
			matrixEyeID       string
			position          int
			eyeCount          int
			approvalsRequired int
			approvalsReceived int
			eyeStatus         string
			activatedAt       *time.Time
			slaDeadline       *time.Time
			resolvedAt        *time.Time
			isEscalated       bool
			escalatedAt       *time.Time
			actionID          string
			actorEmail        string
			actorRoleID       string
			actionType        string
			comment           string
			actedAt           *time.Time
			isSystem          bool
		)
		if err := rows.Scan(
			&eyeID, &matrixEyeID, &position, &eyeCount,
			&approvalsRequired, &approvalsReceived, &eyeStatus,
			&activatedAt, &slaDeadline, &resolvedAt,
			&isEscalated, &escalatedAt,
			&actionID, &actorEmail, &actorRoleID, &actionType, &comment,
			&actedAt, &isSystem,
		); err != nil {
			return nil, fmt.Errorf("GetInstanceDetail eye scan: %w", err)
		}

		if _, seen := eyeMap[eyeID]; !seen {
			eyeMap[eyeID] = &EyeDetail{
				InstanceEyeID:     eyeID,
				MatrixEyeID:       matrixEyeID,
				Position:          position,
				EyeCount:          eyeCount,
				ApprovalsRequired: approvalsRequired,
				ApprovalsReceived: approvalsReceived,
				Status:            eyeStatus,
				ActivatedAt:       activatedAt,
				SlaDeadline:       slaDeadline,
				ResolvedAt:        resolvedAt,
				IsEscalated:       isEscalated,
				EscalatedAt:       escalatedAt,
				Actions:           []EyeActionDetail{},
			}
			eyeOrder = append(eyeOrder, eyeID)
		}

		if actionID != "" && actedAt != nil {
			eyeMap[eyeID].Actions = append(eyeMap[eyeID].Actions, EyeActionDetail{
				ActionID:    actionID,
				ActorEmail:  actorEmail,
				ActorRoleID: actorRoleID,
				ActionType:  actionType,
				Comment:     comment,
				ActedAt:     *actedAt,
				IsSystem:    isSystem,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetInstanceDetail eyes iteration: %w", err)
	}

	// Flatten the map in position order.
	d.Eyes = make([]EyeDetail, 0, len(eyeOrder))
	for _, id := range eyeOrder {
		d.Eyes = append(d.Eyes, *eyeMap[id])
	}

	return &d, nil
}

// ─── GetRichInstanceDetail ────────────────────────────────────────────────────

// GetRichInstanceDetail returns a RichInstanceDetail for one approval instance.
// It enriches every eye with numbered approver identities from the matrix
// configuration, the action each approver has taken, and computes whether the
// supplied viewerUserID can still act on any active eye.
//
// viewerUserID may be empty — in that case ViewerCanAct will be false.
func GetRichInstanceDetail(ctx context.Context, pool *pgxpool.Pool, instanceID, viewerUserID string) (*RichInstanceDetail, error) {

	// ── Step 1: Fetch the instance row ─────────────────────────────────────
	var d RichInstanceDetail
	var matrixID string
	var slaHours16 *int16  // DB column is smallint — pgx v5 returns int16
	err := pool.QueryRow(ctx, `
		SELECT
		  instance_id, matrix_id, module_code, entity_code, transaction_type,
		  record_id, record_table, action_type, status, submitted_by_email,
		  submitted_at, overall_sla_hours, overall_sla_deadline,
		  resolved_at, COALESCE(resolved_by_email,'')
		FROM uam.approval_instance
		WHERE instance_id = $1
		  AND is_deleted  = false`,
		instanceID,
	).Scan(
		&d.InstanceID, &matrixID, &d.ModuleCode, &d.EntityCode, &d.TransactionType,
		&d.RecordID, &d.RecordTable, &d.ActionType, &d.Status, &d.SubmittedByEmail,
		&d.SubmittedAt, &slaHours16, &d.OverallSlaDeadline,
		&d.ResolvedAt, &d.ResolvedByEmail,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("instance not found: %s", instanceID)
		}
		return nil, fmt.Errorf("GetRichInstanceDetail instance query: %w", err)
	}
	if slaHours16 != nil {
		v := int(*slaHours16)
		d.OverallSlaHours = &v
	}
	d.MatrixID = matrixID

	// ── Step 2: Fetch eyes + actions ───────────────────────────────────────
	eyeRows, err := pool.Query(ctx, `
		SELECT
		  ie.instance_eye_id, ie.matrix_eye_id, ie.position, ie.eye_count,
		  ie.approvals_required, ie.approvals_received, ie.status,
		  ie.activated_at, ie.sla_deadline, ie.resolved_at,
		  ie.is_escalated, ie.escalated_at,
		  COALESCE(ia.action_id,''),
		  COALESCE(ia.actor_email,''),
		  COALESCE(ia.actor_role_id,''),
		  COALESCE(ia.action_type,''),
		  COALESCE(ia.comment,''),
		  ia.acted_at,
		  COALESCE(ia.is_system_action, false),
		  COALESCE(ia.actor_user_id,'')
		FROM uam.approval_instance_eye ie
		LEFT JOIN uam.approval_instance_action ia
		  ON ia.instance_eye_id = ie.instance_eye_id
		WHERE ie.instance_id = $1
		ORDER BY ie.position ASC, ia.acted_at ASC`,
		instanceID,
	)
	if err != nil {
		return nil, fmt.Errorf("GetRichInstanceDetail eyes query: %w", err)
	}
	defer eyeRows.Close()

	type eyeMeta struct {
		matrixEyeID string
		status      string
	}
	eyeMetaMap := make(map[string]eyeMeta)   // instance_eye_id → meta
	richEyeMap := make(map[string]*RichEyeDetail)
	eyeOrder   := make([]string, 0)

	for eyeRows.Next() {
		var (
			eyeID, matrixEyeID, eyeStatus string
			position, eyeCount, appReq, appRcvd int
			activatedAt, slaDeadline, resolvedAt, escalatedAt *time.Time
			isEscalated bool
			actionID, actorEmail, actorRoleID, actionType, comment string
			actedAt *time.Time
			isSystem bool
			actorUserID string
		)
		if err := eyeRows.Scan(
			&eyeID, &matrixEyeID, &position, &eyeCount,
			&appReq, &appRcvd, &eyeStatus,
			&activatedAt, &slaDeadline, &resolvedAt,
			&isEscalated, &escalatedAt,
			&actionID, &actorEmail, &actorRoleID, &actionType, &comment,
			&actedAt, &isSystem, &actorUserID,
		); err != nil {
			return nil, fmt.Errorf("GetRichInstanceDetail eye scan: %w", err)
		}

		if _, seen := richEyeMap[eyeID]; !seen {
			richEyeMap[eyeID] = &RichEyeDetail{
				InstanceEyeID:     eyeID,
				Position:          position,
				EyeCount:          eyeCount,
				ApprovalsRequired: appReq,
				ApprovalsReceived: appRcvd,
				Status:            eyeStatus,
				ActivatedAt:       activatedAt,
				SlaDeadline:       slaDeadline,
				ResolvedAt:        resolvedAt,
				IsEscalated:       isEscalated,
				EscalatedAt:       escalatedAt,
				Approvers:         make(map[string]ApproverInfo),
				ActionLog:         []EyeActionDetail{},
			}
			eyeOrder = append(eyeOrder, eyeID)
			eyeMetaMap[eyeID] = eyeMeta{matrixEyeID: matrixEyeID, status: eyeStatus}
		}

		if actionID != "" && actedAt != nil {
			richEyeMap[eyeID].ActionLog = append(richEyeMap[eyeID].ActionLog, EyeActionDetail{
				ActionID:    actionID,
				ActorEmail:  actorEmail,
				ActorRoleID: actorRoleID,
				ActionType:  actionType,
				Comment:     comment,
				ActedAt:     *actedAt,
				IsSystem:    isSystem,
			})
		}
	}
	if err := eyeRows.Err(); err != nil {
		return nil, fmt.Errorf("GetRichInstanceDetail eyes iteration: %w", err)
	}

	// ── Step 3: For each eye, load approver members from matrix config ─────
	// Build a map: actorEmail → {actionType, actedAt, comment} from the action log
	// so we can annotate each approver with what they did.
	for _, eyeID := range eyeOrder {
		meta := eyeMetaMap[eyeID]
		re   := richEyeMap[eyeID]

		// actionByEmail: last non-system action per actor email on this eye.
		type actRecord struct {
			actionType string
			actedAt    time.Time
			comment    string
		}
		actionByEmail := make(map[string]actRecord)
		for _, act := range re.ActionLog {
			if !act.IsSystem && act.ActionType != "" {
				// keep the most recent action per email
				if prev, ok := actionByEmail[act.ActorEmail]; !ok || act.ActedAt.After(prev.actedAt) {
					actionByEmail[act.ActorEmail] = actRecord{act.ActionType, act.ActedAt, act.Comment}
				}
			}
		}

		// Query approver members for this eye from the matrix config.
		mRows, err := pool.Query(ctx, `
			SELECT
			  m.slot_order,
			  m.assignment_type,
			  COALESCE(m.user_id,''),
			  COALESCE(u.email,''),
			  COALESCE(u.employee_name,''),
			  COALESCE(m.role_id,''),
			  COALESCE(r.name,'')
			FROM uam.approval_matrix_eye_member m
			LEFT JOIN public.users  u ON u.id    = m.user_id
			LEFT JOIN public.roles  r ON r.id    = m.role_id
			WHERE m.eye_id      = $1
			  AND m.member_type = 'APPROVER'
			  AND m.is_deleted  = false
			  AND m.is_active   = true
			ORDER BY m.slot_order ASC`,
			meta.matrixEyeID,
		)
		if err != nil {
			return nil, fmt.Errorf("GetRichInstanceDetail members query eye=%s: %w", eyeID, err)
		}

		slot := 0
		for mRows.Next() {
			slot++
			var (
				slotOrder      int
				assignType     string
				userID, userEmail, userName string
				roleID, roleName            string
			)
			if err := mRows.Scan(&slotOrder, &assignType, &userID, &userEmail, &userName, &roleID, &roleName); err != nil {
				mRows.Close()
				return nil, fmt.Errorf("GetRichInstanceDetail member scan eye=%s: %w", eyeID, err)
			}

			info := ApproverInfo{
				SlotOrder:      slotOrder,
				AssignmentType: assignType,
				UserID:         userID,
				UserEmail:      userEmail,
				UserName:       userName,
				RoleID:         roleID,
				RoleName:       roleName,
			}

			// Annotate with action taken — match by user email.
			if userEmail != "" {
				if act, ok := actionByEmail[userEmail]; ok {
					info.ActionTaken  = act.actionType
					info.ActionAt     = &act.actedAt
					info.ActionComment = act.comment
				}
			}

			key := fmt.Sprintf("approver_%d", slot)
			re.Approvers[key] = info
		}
		mRows.Close()
	}

	// ── Step 4: Determine viewer context ──────────────────────────────────
	if viewerUserID != "" {
		for _, eyeID := range eyeOrder {
			re := richEyeMap[eyeID]
			if re.Status != EyeStatusActive {
				continue
			}
			meta := eyeMetaMap[eyeID]
			// Quick eligibility check (same logic as canUserActOnEye but read-only).
			var cnt int
			_ = pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM uam.approval_matrix_eye_member m
				WHERE m.eye_id      = $1
				  AND m.member_type = 'APPROVER'
				  AND m.is_deleted  = false
				  AND m.is_active   = true
				  AND (
				    (m.assignment_type IN ('USER_ONLY','ROLE_USER') AND m.user_id = $2)
				    OR
				    (m.assignment_type IN ('ROLE_ONLY','ROLE_USER') AND EXISTS (
				      SELECT 1 FROM public.user_roles ur WHERE ur.role_id = m.role_id AND ur.user_id = $2
				    ))
				  )`,
				meta.matrixEyeID, viewerUserID,
			).Scan(&cnt)
			if cnt > 0 {
				d.ViewerCanAct      = true
				d.ViewerActiveEyeID = eyeID
				break
			}
		}
	}

	// ── Step 5: Flatten in order ───────────────────────────────────────────
	d.Eyes = make([]RichEyeDetail, 0, len(eyeOrder))
	for _, id := range eyeOrder {
		d.Eyes = append(d.Eyes, *richEyeMap[id])
	}

	return &d, nil
}

// ─── GetMySubmissions ─────────────────────────────────────────────────────────

// GetMySubmissions returns all approval instances submitted by the given user,
// ordered newest-first. Includes a summary of the current active eye per instance.
func GetMySubmissions(ctx context.Context, pool *pgxpool.Pool, userID string) ([]InstanceSummary, error) {
	rows, err := pool.Query(ctx, `
		SELECT
		  i.instance_id,
		  i.module_code,
		  i.transaction_type,
		  i.record_id,
		  i.action_type,
		  i.status,
		  i.submitted_by_email,
		  i.submitted_at,
		  COALESCE(ie.instance_eye_id,'')  AS current_eye_id,
		  COALESCE(ie.position, 0)          AS current_position,
		  COALESCE(ie.approvals_required,0) AS approvals_required,
		  COALESCE(ie.approvals_received,0) AS approvals_received,
		  ie.sla_deadline,
		  COALESCE(ie.is_escalated, false)
		FROM uam.approval_instance i
		LEFT JOIN uam.approval_instance_eye ie
		  ON ie.instance_id = i.instance_id
		  AND ie.status = 'ACTIVE'
		WHERE i.submitted_by = $1
		  AND i.is_deleted   = false
		ORDER BY i.submitted_at DESC`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("GetMySubmissions query: %w", err)
	}
	defer rows.Close()

	summaries := make([]InstanceSummary, 0)
	for rows.Next() {
		var s InstanceSummary
		if err := rows.Scan(
			&s.InstanceID, &s.ModuleCode, &s.TransactionType,
			&s.RecordID, &s.ActionType, &s.Status,
			&s.SubmittedByEmail, &s.SubmittedAt,
			&s.CurrentEyeID, &s.CurrentPosition,
			&s.ApprovalsRequired, &s.ApprovalsReceived,
			&s.SlaDeadline, &s.IsEscalated,
		); err != nil {
			return nil, fmt.Errorf("GetMySubmissions scan: %w", err)
		}
		summaries = append(summaries, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetMySubmissions iteration: %w", err)
	}

	api.LogInfo("[ApprovalEngine] GetMySubmissions: user=%s count=%d", userID, len(summaries))
	return summaries, nil
}
