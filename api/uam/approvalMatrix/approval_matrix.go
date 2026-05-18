package approvalMatrix

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Error helpers ────────────────────────────────────────────────────────────

func getUserFriendlyApprovalMatrixError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		logDBError(err, context)
		switch pgErr.Code {
		case "23505":
			switch pgErr.ConstraintName {
			case "uniq_uam_master_slab":
				return "An approval matrix already exists for this module, entity, transaction type and amount range.", http.StatusConflict
			case "uniq_uam_eye_position":
				return "A round at this position already exists for this matrix.", http.StatusConflict
			case "uniq_uam_eye_count":
				return "A round with this eye count already exists for this matrix.", http.StatusConflict
			case "uniq_uam_member_user_per_eye":
				return "This user is already assigned to this eye-round.", http.StatusConflict
			case "uniq_uam_member_role_only_per_eye":
				return "This role is already assigned to this eye-round.", http.StatusConflict
			default:
				return "Duplicate entry detected.", http.StatusConflict
			}
		case "23514":
			switch pgErr.ConstraintName {
			case "uam_appr_eye_count_chk":
				return "Eye count must be 2, 4, or 6.", http.StatusBadRequest
			case "uam_appr_master_order_chk":
				return "Approval order must be PARALLEL or SEQUENTIAL.", http.StatusBadRequest
			case "uam_appr_member_role_only_chk":
				return "ROLE_ONLY assignment requires a role and no user.", http.StatusBadRequest
			case "uam_appr_member_user_only_chk":
				return "USER_ONLY assignment requires a user and no role.", http.StatusBadRequest
			case "uam_appr_member_role_user_chk":
				return "ROLE_USER assignment requires both a role and a user.", http.StatusBadRequest
			case "uam_appr_member_escalation_slot_chk":
				return "Escalation members must be at slot order 1.", http.StatusBadRequest
			default:
				return fmt.Sprintf("Check constraint failed: %s", pgErr.ConstraintName), http.StatusBadRequest
			}
		case "23503":
			if strings.Contains(pgErr.ConstraintName, "role") {
				return "Role not found.", http.StatusBadRequest
			}
			if strings.Contains(pgErr.ConstraintName, "user") {
				return "User not found.", http.StatusBadRequest
			}
			return "Referenced record not found.", http.StatusBadRequest
		}
		return fmt.Sprintf("Database error [%s]: %s", pgErr.Code, pgErr.Message), http.StatusInternalServerError
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "Record not found.", http.StatusNotFound
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue.", http.StatusServiceUnavailable
	}
	return "Internal server error: " + context, http.StatusInternalServerError
}

func logDBError(err error, context string) {
	if err == nil {
		return
	}
	api.LogError("%s: %v", context, err)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		api.LogError("PG: Code=%s Msg=%s Detail=%s Constraint=%s Table=%s",
			pgErr.Code, pgErr.Message, pgErr.Detail, pgErr.ConstraintName, pgErr.TableName)
	}
}

// ─── Validation helpers ───────────────────────────────────────────────────────

var validModuleCodes = map[string]bool{
	"FIXED_DEPOSIT": true, "PAYMENTS": true, "RECONCILIATION": true,
	"VENDOR": true, "GENERAL": true,
}

func validateMasterFields(moduleCode, approvalOrder string, minAmount, maxAmount *float64, slaHours *int) error {
	if !validModuleCodes[moduleCode] {
		return errors.New("module_code must be one of FIXED_DEPOSIT, PAYMENTS, RECONCILIATION, VENDOR, GENERAL")
	}
	if approvalOrder != "PARALLEL" && approvalOrder != "SEQUENTIAL" {
		return errors.New("approval_order must be PARALLEL or SEQUENTIAL")
	}
	if minAmount != nil && *minAmount < 0 {
		return errors.New("min_amount must be >= 0")
	}
	if minAmount != nil && maxAmount != nil && *maxAmount <= *minAmount {
		return errors.New("max_amount must be > min_amount")
	}
	if slaHours != nil && *slaHours <= 0 {
		return errors.New("sla_hours must be > 0")
	}
	return nil
}

func validateEyeFields(eyeCount, position int, slaHours *int) error {
	// eye_count is auto-normalised by CreateApprovalMatrix to match the DB check
	// constraint (2/4/6). We no longer reject odd member counts up-front, the
	// only hard rules are: 1 ≤ members ≤ 5 and escalation requires an approver.
	if position < 1 {
		return errors.New("position must be >= 1")
	}
	if slaHours != nil && *slaHours <= 0 {
		return errors.New("sla_hours must be > 0")
	}
	return nil
}

// normaliseEyeCount rounds an actual member count (1..5) up to the smallest
// value the uam_appr_eye_count_chk check constraint accepts (2, 4, or 6).
func normaliseEyeCount(memberCount int) int {
	switch {
	case memberCount <= 2:
		return 2
	case memberCount <= 4:
		return 4
	default:
		return 6
	}
}

func validateMemberFields(memberType, assignmentType string, roleID, userID *string, slotOrder int) error {
	if memberType != "APPROVER" && memberType != "ESCALATION" {
		return errors.New("member_type must be APPROVER or ESCALATION")
	}
	if assignmentType != "ROLE_ONLY" && assignmentType != "USER_ONLY" && assignmentType != "ROLE_USER" {
		return errors.New("assignment_type must be ROLE_ONLY, USER_ONLY, or ROLE_USER")
	}
	if slotOrder < 1 || slotOrder > 10 {
		return errors.New("slot_order must be between 1 and 10")
	}
	if memberType == "ESCALATION" && slotOrder != 1 {
		return errors.New("escalation members must have slot_order = 1")
	}
	switch assignmentType {
	case "ROLE_ONLY":
		if roleID == nil || *roleID == "" {
			return errors.New("ROLE_ONLY requires role_id")
		}
		if userID != nil && *userID != "" {
			return errors.New("ROLE_ONLY must not have user_id")
		}
	case "USER_ONLY":
		if userID == nil || *userID == "" {
			return errors.New("USER_ONLY requires user_id")
		}
		if roleID != nil && *roleID != "" {
			return errors.New("USER_ONLY must not have role_id")
		}
	case "ROLE_USER":
		if roleID == nil || *roleID == "" {
			return errors.New("ROLE_USER requires role_id")
		}
		if userID == nil || *userID == "" {
			return errors.New("ROLE_USER requires user_id")
		}
	}
	return nil
}

// ─── Shared types ─────────────────────────────────────────────────────────────

type MemberInput struct {
	MemberType     string  `json:"member_type"`
	AssignmentType string  `json:"assignment_type"`
	RoleID         *string `json:"role_id"`
	UserID         *string `json:"user_id"`
	SlotOrder      int     `json:"slot_order"`
}

type EyeInput struct {
	EyeCount int           `json:"eye_count"`
	Position int           `json:"position"`
	SlaHours *int          `json:"sla_hours"`
	Members  []MemberInput `json:"members"`
}

type MatrixDetail struct {
	MatrixID        string   `json:"matrix_id"`
	ModuleCode      string   `json:"module_code"`
	EntityCode      string   `json:"entity_code"`
	EntityName      string   `json:"entity_name"`
	TransactionType string   `json:"transaction_type"`
	MinAmount       *float64 `json:"min_amount"`
	MaxAmount       *float64 `json:"max_amount"`
	Description     *string  `json:"description"`
	ApprovalOrder   string   `json:"approval_order"`
	SlaHours        *int     `json:"sla_hours"`
	IsActive        bool     `json:"is_active"`
	// Audit trail
	ProcessingStatus string `json:"processing_status"`
	CreatedBy        string `json:"created_by"`
	CreatedAt        string `json:"created_at"`
	EditedBy         string `json:"edited_by"`
	EditedAt         string `json:"edited_at"`
	DeletedBy        string `json:"deleted_by"`
	DeletedAt        string `json:"deleted_at"`
	CheckerBy        string `json:"checker_by"`
	CheckerAt        string `json:"checker_at"`
	CheckerComment   string `json:"checker_comment"`
	// Old values (pre-edit snapshot)
	OldModuleCode      *string     `json:"old_module_code"`
	OldEntityCode      *string     `json:"old_entity_code"`
	OldTransactionType *string     `json:"old_transaction_type"`
	OldMinAmount       *float64    `json:"old_min_amount"`
	OldMaxAmount       *float64    `json:"old_max_amount"`
	OldDescription     *string     `json:"old_description"`
	OldApprovalOrder   *string     `json:"old_approval_order"`
	OldSlaHours        *int        `json:"old_sla_hours"`
	OldIsActive        *bool       `json:"old_is_active"`
	Eyes               []EyeDetail `json:"eyes"`
}

type EyeDetail struct {
	EyeID    string `json:"eye_id"`
	EyeCount int    `json:"eye_count"`
	Position int    `json:"position"`
	SlaHours *int   `json:"sla_hours"`
	IsActive bool   `json:"is_active"`
	// Audit trail
	ProcessingStatus string `json:"processing_status"`
	CreatedBy        string `json:"created_by"`
	CreatedAt        string `json:"created_at"`
	EditedBy         string `json:"edited_by"`
	EditedAt         string `json:"edited_at"`
	CheckerBy        string `json:"checker_by"`
	CheckerAt        string `json:"checker_at"`
	// Old values (pre-edit snapshot)
	OldEyeCount *int           `json:"old_eye_count"`
	OldPosition *int           `json:"old_position"`
	OldSlaHours *int           `json:"old_sla_hours"`
	OldIsActive *bool          `json:"old_is_active"`
	Members     []MemberDetail `json:"members"`
}

type MemberDetail struct {
	MemberID       string  `json:"member_id"`
	MemberType     string  `json:"member_type"`
	AssignmentType string  `json:"assignment_type"`
	SlotOrder      int     `json:"slot_order"`
	IsActive       bool    `json:"is_active"`
	RoleID         *string `json:"role_id"`
	RoleName       *string `json:"role_name"`
	RoleCode       *string `json:"rolecode"`
	UserID         *string `json:"user_id"`
	EmployeeName   *string `json:"employee_name"`
	Email          *string `json:"email"`
	Mobile         *string `json:"mobile"`
	// Audit trail
	ProcessingStatus string `json:"processing_status"`
	CreatedBy        string `json:"created_by"`
	CreatedAt        string `json:"created_at"`
	EditedBy         string `json:"edited_by"`
	EditedAt         string `json:"edited_at"`
	CheckerBy        string `json:"checker_by"`
	CheckerAt        string `json:"checker_at"`
	// Old values (pre-edit snapshot)
	OldMemberType     *string `json:"old_member_type"`
	OldAssignmentType *string `json:"old_assignment_type"`
	OldRoleID         *string `json:"old_role_id"`
	OldUserID         *string `json:"old_user_id"`
	OldSlotOrder      *int    `json:"old_slot_order"`
	OldIsActive       *bool   `json:"old_is_active"`
}

func resolveUserEmail(userID string) string {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			return s.Email
		}
	}
	return ""
}

// validateUserIDsExist checks that every user_id in the supplied list actually
// exists in public.users. Returns an error listing all unknown IDs.
func validateUserIDsExist(ctx context.Context, pool *pgxpool.Pool, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	// Deduplicate.
	seen := make(map[string]struct{}, len(userIDs))
	uniq := userIDs[:0]
	for _, id := range userIDs {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			uniq = append(uniq, id)
		}
	}
	rows, err := pool.Query(ctx,
		`SELECT id FROM public.users WHERE id = ANY($1)`,
		uniq,
	)
	if err != nil {
		return fmt.Errorf("user_id existence check failed: %w", err)
	}
	defer rows.Close()
	existing := make(map[string]struct{}, len(uniq))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("user_id existence scan: %w", err)
		}
		existing[id] = struct{}{}
	}
	var missing []string
	for _, id := range uniq {
		if _, ok := existing[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("unknown user_id(s): %s — use the real DB user id, not a display name",
			strings.Join(missing, ", "))
	}
	return nil
}

// ─── 1. CreateApprovalMatrix ──────────────────────────────────────────────────
// Creates matrix master + audit, all eyes + eye audits, all members + member audits
// All 6 tables in one atomic transaction.

func CreateApprovalMatrix(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string     `json:"user_id"`
			ModuleCode      string     `json:"module_code"`
			EntityCode      string     `json:"entity_code"`
			TransactionType string     `json:"transaction_type"`
			MinAmount       *float64   `json:"min_amount"`
			MaxAmount       *float64   `json:"max_amount"`
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
		if req.EntityCode == "" || req.TransactionType == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_code and transaction_type are required")
			return
		}
		// Master SLA column is used by list/grid views; if omitted, mirror the highest per-eye SLA (when any).
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
		isActive := true
		if req.IsActive != nil {
			isActive = *req.IsActive
		}
		if err := validateMasterFields(req.ModuleCode, req.ApprovalOrder, req.MinAmount, req.MaxAmount, effectiveSla); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		var allUserIDs []string
		for i, eye := range req.Eyes {
			if err := validateEyeFields(eye.EyeCount, eye.Position, eye.SlaHours); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("eye[%d]: %s", i, err.Error()))
				return
			}
			// New rules:
			//   - Each eye must have between 1 and 5 members
			//   - If the eye contains ESCALATION member(s) it must also contain at least 1 APPROVER member
			//   - eye_count is auto-normalised below to a value the DB check constraint accepts (2/4/6)
			if len(eye.Members) == 0 {
				api.RespondWithError(w, http.StatusBadRequest,
					fmt.Sprintf("eye[%d] (position %d): at least 1 member is required",
						i, eye.Position))
				return
			}
			if len(eye.Members) > 5 {
				api.RespondWithError(w, http.StatusBadRequest,
					fmt.Sprintf("eye[%d] (position %d): a maximum of 5 members is allowed (got %d)",
						i, eye.Position, len(eye.Members)))
				return
			}
			hasApprover, hasEscalation := false, false
			for j, m := range eye.Members {
				if m.SlotOrder == 0 {
					req.Eyes[i].Members[j].SlotOrder = 1
				}
				if err := validateMemberFields(m.MemberType, m.AssignmentType, m.RoleID, m.UserID, req.Eyes[i].Members[j].SlotOrder); err != nil {
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("eye[%d].member[%d]: %s", i, j, err.Error()))
					return
				}
				switch m.MemberType {
				case "APPROVER":
					hasApprover = true
				case "ESCALATION":
					hasEscalation = true
				}
				// Collect user_ids for existence check
				if (m.AssignmentType == "USER_ONLY" || m.AssignmentType == "ROLE_USER") && m.UserID != nil && *m.UserID != "" {
					allUserIDs = append(allUserIDs, *m.UserID)
				}
			}
			if hasEscalation && !hasApprover {
				api.RespondWithError(w, http.StatusBadRequest,
					fmt.Sprintf("eye[%d] (position %d): an ESCALATION eye must contain at least one APPROVER member (role/user) in addition to its ESCALATION member(s)",
						i, eye.Position))
				return
			}
			// Normalise eye_count so it satisfies uam_appr_eye_count_chk (2/4/6).
			req.Eyes[i].EyeCount = normaliseEyeCount(len(eye.Members))
		}

		ctx := r.Context()
		// Validate all user_ids exist in public.users BEFORE opening the DB transaction.
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

		// Table 1: approval_matrix_master
		var matrixID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO uam.approval_matrix_master
				(module_code,entity_code,transaction_type,min_amount,max_amount,description,approval_order,sla_hours,is_active)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING matrix_id`,
			req.ModuleCode, req.EntityCode, req.TransactionType,
			req.MinAmount, req.MaxAmount, req.Description, req.ApprovalOrder, effectiveSla, isActive,
		).Scan(&matrixID); err != nil {
			msg, status := getUserFriendlyApprovalMatrixError(err, "Create master failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Table 4: audit_approval_matrix_master
		if _, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_master
				(matrix_id,action_type,processing_status,requested_by,requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, matrixID, userEmail,
		); err != nil {
			logDBError(err, "master audit insert")
			msg, status := getUserFriendlyApprovalMatrixError(err, "Audit insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		totalMembers := 0
		for _, eye := range req.Eyes {
			// Table 2: approval_matrix_eye
			var eyeID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO uam.approval_matrix_eye (matrix_id,eye_count,position,sla_hours)
				VALUES ($1,$2,$3,$4) RETURNING eye_id`,
				matrixID, eye.EyeCount, eye.Position, eye.SlaHours,
			).Scan(&eyeID); err != nil {
				logDBError(err, "Create eye failed")
				msg, status := getUserFriendlyApprovalMatrixError(err, "Create eye failed")
				api.RespondWithError(w, status, msg)
				return
			}

			// Table 5: audit_approval_matrix_eye
			if _, err := tx.Exec(ctx, `
				INSERT INTO uam.audit_approval_matrix_eye
					(eye_id,matrix_id,action_type,processing_status,requested_by,requested_at)
				VALUES ($1,$2,'CREATE','PENDING_APPROVAL',$3,now())`, eyeID, matrixID, userEmail,
			); err != nil {
				logDBError(err, "eye audit insert")
				msg, status := getUserFriendlyApprovalMatrixError(err, constants.ErrEyeAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}

			for _, m := range eye.Members {
				// Table 3: approval_matrix_eye_member
				var memberID string
				if err := tx.QueryRow(ctx, `
					INSERT INTO uam.approval_matrix_eye_member
						(eye_id,matrix_id,member_type,assignment_type,role_id,user_id,slot_order)
					VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING member_id`,
					eyeID, matrixID, m.MemberType, m.AssignmentType, m.RoleID, m.UserID, m.SlotOrder,
				).Scan(&memberID); err != nil {
					msg, status := getUserFriendlyApprovalMatrixError(err, "Create member failed")
					api.RespondWithError(w, status, msg)
					return
				}

				// Table 6: audit_approval_matrix_eye_member
				if _, err := tx.Exec(ctx, `
					INSERT INTO uam.audit_approval_matrix_eye_member
						(member_id,eye_id,matrix_id,action_type,processing_status,requested_by,requested_at)
					VALUES ($1,$2,$3,'CREATE','PENDING_APPROVAL',$4,now())`,
					memberID, eyeID, matrixID, userEmail,
				); err != nil {
					logDBError(err, "member audit insert")
					msg, status := getUserFriendlyApprovalMatrixError(err, constants.ErrMemberAuditFailed)
					api.RespondWithError(w, status, msg)
					return
				}
				totalMembers++
			}
		}

		// Eye/member INSERT paths omit is_active; DB defaults are typically true.
		// When the master is inactive, align child rows so list "active eyes" and UI stay consistent.
		if !isActive {
			if _, err := tx.Exec(ctx, `
				UPDATE uam.approval_matrix_eye SET is_active = false
				WHERE matrix_id = $1 AND is_deleted = false`, matrixID); err != nil {
				logDBError(err, "CreateApprovalMatrix deactivate eyes")
				msg, status := getUserFriendlyApprovalMatrixError(err, "Deactivate eyes after inactive create failed")
				api.RespondWithError(w, status, msg)
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE uam.approval_matrix_eye_member SET is_active = false
				WHERE matrix_id = $1 AND is_deleted = false`, matrixID); err != nil {
				logDBError(err, "CreateApprovalMatrix deactivate members")
				msg, status := getUserFriendlyApprovalMatrixError(err, "Deactivate members after inactive create failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "CreateApprovalMatrix commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"matrix_id": matrixID, "eyes_created": len(req.Eyes),
			"members_created": totalMembers, "requested_by": userEmail,
		})
		api.LogInfo("ApprovalMatrix created: matrix_id=%s eyes=%d members=%d by=%s", matrixID, len(req.Eyes), totalMembers, userEmail)
	}
}

// ─── 2. UpdateApprovalMatrix ──────────────────────────────────────────────────
// One call updates matrix-level fields + any number of eyes + any number of members.
// The whole payload is one atomic tx — one fail = full rollback.
// Any change to any level puts the entire matrix into PENDING_EDIT_APPROVAL.
// Approving the matrix_id cascades approval to all child eyes and members.
//
// Complexity: O(1) DB round-trips regardless of how many eyes/members are sent:
//   - 1 SELECT FOR UPDATE on master
//   - 1 bulk UPDATE on master (optional)
//   - 1 bulk UPDATE on eyes via unnest (optional)
//   - 1 bulk UPDATE on members via unnest (optional)
//   - 3 bulk INSERTs into audit tables via unnest
//   - 1 COMMIT

func UpdateApprovalMatrix(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		type EyeUpdate struct {
			EyeID     string                 `json:"eye_id"`
			EyeFields map[string]interface{} `json:"eye_fields"`
		}
		type MemberUpdate struct {
			MemberID     string                 `json:"member_id"`
			MemberFields map[string]interface{} `json:"member_fields"`
		}
		var req struct {
			UserID       string                 `json:"user_id"`
			Reason       string                 `json:"reason"`
			MatrixID     string                 `json:"matrix_id"`
			MatrixFields map[string]interface{} `json:"matrix_fields"`
			Eyes         []EyeUpdate            `json:"eyes"`
			Members      []MemberUpdate         `json:"members"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.MatrixID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "matrix_id is required")
			return
		}
		hasMatrix := len(req.MatrixFields) > 0
		hasEyes := len(req.Eyes) > 0
		hasMembers := len(req.Members) > 0
		if !hasMatrix && !hasEyes && !hasMembers {
			api.RespondWithError(w, http.StatusBadRequest,
				"Provide at least one of: matrix_fields, eyes[], members[]")
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)

		// ── Lock and snapshot the matrix master row ─────────────────────────────
		var oldModuleCode, oldEntityCode, oldTxType, oldApprovalOrder string
		var oldMinAmount, oldMaxAmount *float64
		var oldDescription *string
		var oldSlaHours *int
		var oldIsActive bool
		if err := tx.QueryRow(ctx, `
			SELECT module_code,entity_code,transaction_type,min_amount,max_amount,
			       description,approval_order,sla_hours,is_active
			FROM uam.approval_matrix_master
			WHERE matrix_id=$1 AND is_deleted=false FOR UPDATE`, req.MatrixID,
		).Scan(&oldModuleCode, &oldEntityCode, &oldTxType,
			&oldMinAmount, &oldMaxAmount, &oldDescription,
			&oldApprovalOrder, &oldSlaHours, &oldIsActive,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrApprovalMatrixNotFound)
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "Fetch matrix failed: "+err.Error())
			return
		}

		// ── Update matrix master fields ──────────────────────────────────────────
		if hasMatrix {
			allowed := map[string]bool{
				"module_code": true, "description": true, "approval_order": true,
				"sla_hours": true, "min_amount": true, "max_amount": true, "is_active": true,
			}
			var sets []string
			var args []interface{}
			pos := 1
			for k, v := range req.MatrixFields {
				if allowed[strings.ToLower(k)] {
					sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, strings.ToLower(k), pos))
					args = append(args, v)
					pos++
				}
			}
			if len(sets) > 0 {
				args = append(args, req.MatrixID)
				q := fmt.Sprintf("UPDATE uam.approval_matrix_master SET %s WHERE matrix_id=$%d",
					strings.Join(sets, ", "), pos)
				if _, err := tx.Exec(ctx, q, args...); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Update matrix fields failed: "+err.Error())
					return
				}
			}
		}
		// Always write a master audit row (the whole matrix is now PENDING_EDIT_APPROVAL)
		if _, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_master
				(matrix_id,action_type,processing_status,reason,requested_by,requested_at,
				 old_module_code,old_entity_code,old_transaction_type,
				 old_min_amount,old_max_amount,old_description,
				 old_approval_order,old_sla_hours,old_is_active)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			req.MatrixID, req.Reason, userEmail,
			oldModuleCode, oldEntityCode, oldTxType,
			oldMinAmount, oldMaxAmount, oldDescription,
			oldApprovalOrder, oldSlaHours, oldIsActive,
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Matrix audit insert failed: "+err.Error())
			return
		}

		// ── Update eyes (bulk via unnest) ────────────────────────────────────────
		eyesUpdated := 0
		if hasEyes {
			allowedEye := map[string]bool{"sla_hours": true, "position": true, "is_active": true, "eye_count": true}

			// Snapshot old values for all eyes in one query
			eyeIDs := make([]string, len(req.Eyes))
			for i, e := range req.Eyes {
				eyeIDs[i] = e.EyeID
			}
			type eyeSnap struct {
				eyeID    string
				eyeCount int
				position int
				slaHours *int
				isActive bool
			}
			eSnapRows, err := tx.Query(ctx,
				`SELECT eye_id, eye_count, position, sla_hours, is_active
				 FROM uam.approval_matrix_eye
				 WHERE eye_id=ANY($1) AND matrix_id=$2 AND is_deleted=false FOR UPDATE`,
				eyeIDs, req.MatrixID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Eye snapshot failed: "+err.Error())
				return
			}
			eyeSnapMap := map[string]eyeSnap{}
			for eSnapRows.Next() {
				var s eyeSnap
				if err := eSnapRows.Scan(&s.eyeID, &s.eyeCount, &s.position, &s.slaHours, &s.isActive); err != nil {
					eSnapRows.Close()
					api.RespondWithError(w, http.StatusInternalServerError, "Eye snapshot scan failed: "+err.Error())
					return
				}
				eyeSnapMap[s.eyeID] = s
			}
			eSnapRows.Close()

			for _, eu := range req.Eyes {
				snap, ok := eyeSnapMap[eu.EyeID]
				if !ok {
					api.RespondWithError(w, http.StatusNotFound, "Eye not found or does not belong to this matrix: "+eu.EyeID)
					return
				}
				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range eu.EyeFields {
					if allowedEye[strings.ToLower(k)] {
						sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, strings.ToLower(k), pos))
						args = append(args, v)
						pos++
					}
				}
				if len(sets) > 0 {
					args = append(args, eu.EyeID)
					q := fmt.Sprintf("UPDATE uam.approval_matrix_eye SET %s WHERE eye_id=$%d",
						strings.Join(sets, ", "), pos)
					if _, err := tx.Exec(ctx, q, args...); err != nil {
						api.RespondWithError(w, http.StatusInternalServerError, "Update eye "+eu.EyeID+" failed: "+err.Error())
						return
					}
				}
				// Bulk audit insert for this eye
				if _, err := tx.Exec(ctx, `
					INSERT INTO uam.audit_approval_matrix_eye
						(eye_id,matrix_id,action_type,processing_status,reason,requested_by,requested_at,
						 old_eye_count,old_position,old_sla_hours,old_is_active)
					VALUES ($1,$2,'EDIT','PENDING_EDIT_APPROVAL',$3,$4,now(),$5,$6,$7,$8)`,
					eu.EyeID, req.MatrixID, req.Reason, userEmail,
					snap.eyeCount, snap.position, snap.slaHours, snap.isActive,
				); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Eye audit insert failed for "+eu.EyeID+": "+err.Error())
					return
				}
				eyesUpdated++
			}
		}

		// ── Update members (bulk via unnest) ─────────────────────────────────────
		membersUpdated := 0
		if hasMembers {
			allowedMember := map[string]bool{
				"member_type": true, "assignment_type": true, "role_id": true,
				"user_id": true, "slot_order": true, "is_active": true,
			}
			memberIDs := make([]string, len(req.Members))
			for i, m := range req.Members {
				memberIDs[i] = m.MemberID
			}
			type memberSnap struct {
				memberID       string
				eyeID          string
				memberType     string
				assignmentType string
				roleID         *string
				userID         *string
				slotOrder      int
				isActive       bool
			}
			mSnapRows, err := tx.Query(ctx,
				`SELECT member_id, eye_id, member_type, assignment_type, role_id, user_id, slot_order, is_active
				 FROM uam.approval_matrix_eye_member
				 WHERE member_id=ANY($1) AND matrix_id=$2 AND is_deleted=false FOR UPDATE`,
				memberIDs, req.MatrixID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Member snapshot failed: "+err.Error())
				return
			}
			mSnapMap := map[string]memberSnap{}
			for mSnapRows.Next() {
				var s memberSnap
				if err := mSnapRows.Scan(&s.memberID, &s.eyeID, &s.memberType, &s.assignmentType,
					&s.roleID, &s.userID, &s.slotOrder, &s.isActive); err != nil {
					mSnapRows.Close()
					api.RespondWithError(w, http.StatusInternalServerError, "Member snapshot scan failed: "+err.Error())
					return
				}
				mSnapMap[s.memberID] = s
			}
			mSnapRows.Close()

			for _, mu := range req.Members {
				snap, ok := mSnapMap[mu.MemberID]
				if !ok {
					api.RespondWithError(w, http.StatusNotFound, "Member not found or does not belong to this matrix: "+mu.MemberID)
					return
				}
				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range mu.MemberFields {
					if allowedMember[strings.ToLower(k)] {
						sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, strings.ToLower(k), pos))
						args = append(args, v)
						pos++
					}
				}
				if len(sets) > 0 {
					args = append(args, mu.MemberID)
					q := fmt.Sprintf("UPDATE uam.approval_matrix_eye_member SET %s WHERE member_id=$%d",
						strings.Join(sets, ", "), pos)
					if _, err := tx.Exec(ctx, q, args...); err != nil {
						api.RespondWithError(w, http.StatusInternalServerError, "Update member "+mu.MemberID+" failed: "+err.Error())
						return
					}
				}
				if _, err := tx.Exec(ctx, `
					INSERT INTO uam.audit_approval_matrix_eye_member
						(member_id,eye_id,matrix_id,action_type,processing_status,reason,requested_by,requested_at,
						 old_member_type,old_assignment_type,old_role_id,old_user_id,old_slot_order,old_is_active)
					VALUES ($1,$2,$3,'EDIT','PENDING_EDIT_APPROVAL',$4,$5,now(),$6,$7,$8,$9,$10,$11)`,
					mu.MemberID, snap.eyeID, req.MatrixID, req.Reason, userEmail,
					snap.memberType, snap.assignmentType, snap.roleID, snap.userID, snap.slotOrder, snap.isActive,
				); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Member audit insert failed for "+mu.MemberID+": "+err.Error())
					return
				}
				membersUpdated++
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"matrix_id":       req.MatrixID,
			"eyes_updated":    eyesUpdated,
			"members_updated": membersUpdated,
			"requested_by":    userEmail,
			"reason":          req.Reason,
		})
		api.LogInfo("ApprovalMatrix update: matrix=%s eyes=%d members=%d by=%s",
			req.MatrixID, eyesUpdated, membersUpdated, userEmail)
	}
}

// ─── 3. DeleteApprovalMatrix ──────────────────────────────────────────────────
// Queues matrix_ids (+ all child eyes + all child members) for delete approval
// in a single atomic tx using 3 bulk INSERT … SELECT statements.
//
// Complexity: O(1) DB round-trips regardless of matrix/eye/member count:
//   - 1 SELECT to verify IDs
//   - 1 INSERT … SELECT for master audit rows
//   - 1 INSERT … SELECT for all eye audit rows under those matrices
//   - 1 INSERT … SELECT for all member audit rows under those eyes
//   - 1 COMMIT
// One fail = full rollback, simple error string returned.

func DeleteApprovalMatrix(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			MatrixIDs []string `json:"matrix_ids"`
			Reason    string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.MatrixIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMatrixIDsCannotBeEmpty)
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)

		// Verify all provided IDs exist and are not already deleted
		var foundCount int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM uam.approval_matrix_master WHERE matrix_id=ANY($1) AND is_deleted=false`,
			req.MatrixIDs,
		).Scan(&foundCount); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Lookup failed: "+err.Error())
			return
		}
		if foundCount != len(req.MatrixIDs) {
			api.RespondWithError(w, http.StatusNotFound,
				fmt.Sprintf("%d of %d matrix_ids not found or already deleted — aborting all",
					len(req.MatrixIDs)-foundCount, len(req.MatrixIDs)))
			return
		}

		// Bulk audit: master — one INSERT per matrix in the input list
		masterTag, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_master
				(matrix_id, action_type, processing_status, reason, requested_by, requested_at)
			SELECT matrix_id, 'DELETE', 'PENDING_DELETE_APPROVAL', $1, $2, now()
			FROM uam.approval_matrix_master
			WHERE matrix_id=ANY($3) AND is_deleted=false`,
			req.Reason, userEmail, req.MatrixIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Master audit insert failed: "+err.Error())
			return
		}

		// Bulk audit: all eyes under those matrices
		eyeTag, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_eye
				(eye_id, matrix_id, action_type, processing_status, reason, requested_by, requested_at)
			SELECT eye_id, matrix_id, 'DELETE', 'PENDING_DELETE_APPROVAL', $1, $2, now()
			FROM uam.approval_matrix_eye
			WHERE matrix_id=ANY($3) AND is_deleted=false`,
			req.Reason, userEmail, req.MatrixIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Eye audit insert failed: "+err.Error())
			return
		}

		// Bulk audit: all members under those matrices
		memberTag, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_eye_member
				(member_id, eye_id, matrix_id, action_type, processing_status, reason, requested_by, requested_at)
			SELECT member_id, eye_id, matrix_id, 'DELETE', 'PENDING_DELETE_APPROVAL', $1, $2, now()
			FROM uam.approval_matrix_eye_member
			WHERE matrix_id=ANY($3) AND is_deleted=false`,
			req.Reason, userEmail, req.MatrixIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Member audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"matrices_queued": int(masterTag.RowsAffected()),
			"eyes_queued":     int(eyeTag.RowsAffected()),
			"members_queued":  int(memberTag.RowsAffected()),
			"requested_by":    userEmail,
		})
		api.LogInfo("DeleteApprovalMatrix: matrices=%d eyes=%d members=%d by=%s",
			masterTag.RowsAffected(), eyeTag.RowsAffected(), memberTag.RowsAffected(), userEmail)
	}
}

// ─── 4. BulkApproveMatrix ─────────────────────────────────────────────────────
// Approves all PENDING audit rows for the given matrix_ids in one tx.
// Cascade: master → eyes → members, all in 3 bulk UPDATEs.
// For DELETE approvals: flips is_deleted=true on master + all child eyes + members.
// Complexity: O(1) DB round-trips. One fail = full rollback.
//
// CTE trick: we capture DELETE matrix_ids from the audit table BEFORE updating
// status (inside the same tx) so the subquery sees the old PENDING status.

func BulkApproveMatrix(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			MatrixIDs []string `json:"matrix_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.MatrixIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMatrixIDsCannotBeEmpty)
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)

		// Step 1: Capture DELETE-pending matrix IDs BEFORE approving (still PENDING here)
		deleteRows, err := tx.Query(ctx, `
			SELECT DISTINCT matrix_id FROM uam.audit_approval_matrix_master
			WHERE matrix_id=ANY($1) AND action_type='DELETE' AND processing_status LIKE 'PENDING%'`,
			req.MatrixIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Delete detection failed: "+err.Error())
			return
		}
		var deleteMatrixIDs []string
		for deleteRows.Next() {
			var id string
			if err := deleteRows.Scan(&id); err != nil {
				deleteRows.Close()
				api.RespondWithError(w, http.StatusInternalServerError, "Delete detection scan failed: "+err.Error())
				return
			}
			deleteMatrixIDs = append(deleteMatrixIDs, id)
		}
		deleteRows.Close()

		// Step 2: Approve all pending master audit rows
		masterTag, err := tx.Exec(ctx, `
			UPDATE uam.audit_approval_matrix_master
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE matrix_id=ANY($3) AND processing_status LIKE 'PENDING%'`,
			userEmail, req.Comment, req.MatrixIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Approve master audit failed: "+err.Error())
			return
		}

		// Step 3: Cascade approve eyes
		eyeTag, err := tx.Exec(ctx, `
			UPDATE uam.audit_approval_matrix_eye
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE matrix_id=ANY($3) AND processing_status LIKE 'PENDING%'`,
			userEmail, req.Comment, req.MatrixIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Approve eye cascade failed: "+err.Error())
			return
		}

		// Step 4: Cascade approve members
		memberTag, err := tx.Exec(ctx, `
			UPDATE uam.audit_approval_matrix_eye_member
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE matrix_id=ANY($3) AND processing_status LIKE 'PENDING%'`,
			userEmail, req.Comment, req.MatrixIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Approve member cascade failed: "+err.Error())
			return
		}

		// Step 5: For DELETE approvals — flip is_deleted=true on all 3 live tables
		if len(deleteMatrixIDs) > 0 {
			if _, err := tx.Exec(ctx,
				`UPDATE uam.approval_matrix_master SET is_deleted=true WHERE matrix_id=ANY($1)`,
				deleteMatrixIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Delete flip master failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx,
				`UPDATE uam.approval_matrix_eye SET is_deleted=true WHERE matrix_id=ANY($1)`,
				deleteMatrixIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Delete flip eyes failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx,
				`UPDATE uam.approval_matrix_eye_member SET is_deleted=true WHERE matrix_id=ANY($1)`,
				deleteMatrixIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Delete flip members failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"matrices_approved": int(masterTag.RowsAffected()),
			"eyes_approved":     int(eyeTag.RowsAffected()),
			"members_approved":  int(memberTag.RowsAffected()),
			"deleted_matrices":  len(deleteMatrixIDs),
			"checker_by":        userEmail,
		})
		api.LogInfo("BulkApprove: matrices=%d eyes=%d members=%d deleted=%d by=%s",
			masterTag.RowsAffected(), eyeTag.RowsAffected(), memberTag.RowsAffected(), len(deleteMatrixIDs), userEmail)
	}
}

// ─── 5. BulkRejectMatrix ──────────────────────────────────────────────────────
// Rejecting matrix_ids cascades: reject all pending eye and member audit rows under those matrices too.
// No is_deleted flip on reject.

func BulkRejectMatrix(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			MatrixIDs []string `json:"matrix_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.MatrixIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMatrixIDsCannotBeEmpty)
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)

		// Reject master audit rows
		masterTag, err := tx.Exec(ctx, `
			UPDATE uam.audit_approval_matrix_master
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE matrix_id=ANY($3) AND processing_status LIKE 'PENDING%'`,
			userEmail, req.Comment, req.MatrixIDs,
		)
		if err != nil {
			logDBError(err, "BulkReject master")
			msg, status := getUserFriendlyApprovalMatrixError(err, "Reject matrices failed")
			api.RespondWithError(w, status, msg)
			return
		}
		matricesRejected := int(masterTag.RowsAffected())

		// Cascade: reject all pending eye audit rows under these matrices
		eyeTag, err := tx.Exec(ctx, `
			UPDATE uam.audit_approval_matrix_eye
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE matrix_id=ANY($3) AND processing_status LIKE 'PENDING%'`,
			userEmail, req.Comment, req.MatrixIDs,
		)
		if err != nil {
			logDBError(err, "BulkReject eye cascade")
			msg, status := getUserFriendlyApprovalMatrixError(err, "Reject eyes failed")
			api.RespondWithError(w, status, msg)
			return
		}
		eyesRejected := int(eyeTag.RowsAffected())

		// Cascade: reject all pending member audit rows under these matrices
		memberTag, err := tx.Exec(ctx, `
			UPDATE uam.audit_approval_matrix_eye_member
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE matrix_id=ANY($3) AND processing_status LIKE 'PENDING%'`,
			userEmail, req.Comment, req.MatrixIDs,
		)
		if err != nil {
			logDBError(err, "BulkReject member cascade")
			msg, status := getUserFriendlyApprovalMatrixError(err, "Reject members failed")
			api.RespondWithError(w, status, msg)
			return
		}
		membersRejected := int(memberTag.RowsAffected())

		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "BulkRejectMatrix commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"matrices_rejected": matricesRejected,
			"eyes_rejected":     eyesRejected,
			"members_rejected":  membersRejected,
			"checker_by":        userEmail,
		})
		api.LogInfo("BulkReject: matrices=%d eyes=%d members=%d by=%s",
			matricesRejected, eyesRejected, membersRejected, userEmail)
	}
}

// ─── 6. GetApprovalMatrixAll ──────────────────────────────────────────────────
// Returns all matrices (not deleted) with rich audit trail:
// created_by, created_at, edited_by, edited_at, deleted_by, deleted_at,
// current processing_status, checker info, eye count summary.

func GetApprovalMatrixAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Optional filters
		moduleCode := r.URL.Query().Get("module_code")
		entityCode := r.URL.Query().Get("entity_code")
		txType := r.URL.Query().Get("transaction_type")
		status := r.URL.Query().Get("status") // filter by processing_status

		q := `
		WITH audit_history AS (
			SELECT
				matrix_id,
				MAX(CASE WHEN action_type='CREATE' THEN requested_by  END) AS created_by,
				MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
				MAX(CASE WHEN action_type='EDIT'   THEN requested_by  END) AS edited_by,
				MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
				MAX(CASE WHEN action_type='DELETE' THEN requested_by  END) AS deleted_by,
				MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
			FROM uam.audit_approval_matrix_master
			GROUP BY matrix_id
		),
		latest_audit AS (
			SELECT DISTINCT ON (matrix_id)
				matrix_id, processing_status, action_type,
				audit_id::text AS audit_id,
				requested_by, TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(checker_by,'')    AS checker_by,
				COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
				COALESCE(checker_comment,'') AS checker_comment,
				COALESCE(reason,'')        AS reason,
				old_module_code, old_entity_code, old_transaction_type,
				old_min_amount, old_max_amount, old_description,
				old_approval_order, old_sla_hours, old_is_active
			FROM uam.audit_approval_matrix_master
			ORDER BY matrix_id,
				GREATEST(
					COALESCE(requested_at, '1970-01-01'::timestamptz),
					COALESCE(checker_at,   '1970-01-01'::timestamptz)
				) DESC
		),
		eye_summary AS (
			SELECT
				matrix_id,
				COUNT(*)                               AS total_eyes,
				SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_eyes
			FROM uam.approval_matrix_eye
			WHERE is_deleted=false
			GROUP BY matrix_id
		)
		SELECT
			m.matrix_id, m.module_code, m.entity_code,
			COALESCE(me.entity_name,'')       AS entity_name,
			m.transaction_type,
			m.min_amount, m.max_amount, m.description, m.approval_order,
			COALESCE(m.sla_hours, (
				SELECT MAX(e.sla_hours)::int
				FROM uam.approval_matrix_eye e
				WHERE e.matrix_id = m.matrix_id
				  AND e.is_deleted = false
				  AND e.sla_hours IS NOT NULL
			)) AS sla_hours,
			m.is_active,
			COALESCE(l.processing_status,'')  AS processing_status,
			COALESCE(l.action_type,'')        AS latest_action,
			COALESCE(l.audit_id,'')           AS audit_id,
			COALESCE(l.requested_by,'')       AS requested_by,
			COALESCE(l.requested_at,'')       AS requested_at,
			COALESCE(l.checker_by,'')         AS checker_by,
			COALESCE(l.checker_at,'')         AS checker_at,
			COALESCE(l.checker_comment,'')    AS checker_comment,
			COALESCE(l.reason,'')             AS reason,
			COALESCE(h.created_by,'')         AS created_by,
			COALESCE(h.created_at,'')         AS created_at,
			COALESCE(h.edited_by,'')          AS edited_by,
			COALESCE(h.edited_at,'')          AS edited_at,
			COALESCE(h.deleted_by,'')         AS deleted_by,
			COALESCE(h.deleted_at,'')         AS deleted_at,
			COALESCE(e.total_eyes,0)          AS total_eyes,
			COALESCE(e.active_eyes,0)         AS active_eyes,
			l.old_module_code, l.old_entity_code, l.old_transaction_type,
			l.old_min_amount, l.old_max_amount, l.old_description,
			l.old_approval_order, l.old_sla_hours, l.old_is_active
		FROM uam.approval_matrix_master m
		LEFT JOIN latest_audit l ON l.matrix_id = m.matrix_id
		LEFT JOIN audit_history h ON h.matrix_id = m.matrix_id
		LEFT JOIN eye_summary e   ON e.matrix_id = m.matrix_id
		-- entity_name is fetched by matching the approval matrix's entity_code
		-- against masterentitycash.entity_id. We do NOT filter by is_deleted
		-- here, so historical/soft-deleted entities still resolve to a name.
		LEFT JOIN public.masterentitycash me ON me.entity_id = m.entity_code
		WHERE m.is_deleted=false`

		var whereParts []string
		var args []interface{}
		pos := 1
		if moduleCode != "" {
			whereParts = append(whereParts, fmt.Sprintf("AND m.module_code=$%d", pos))
			args = append(args, moduleCode)
			pos++
		}
		if entityCode != "" {
			whereParts = append(whereParts, fmt.Sprintf("AND m.entity_code=$%d", pos))
			args = append(args, entityCode)
			pos++
		}
		if txType != "" {
			whereParts = append(whereParts, fmt.Sprintf("AND m.transaction_type=$%d", pos))
			args = append(args, txType)
			pos++
		}
		if status != "" {
			whereParts = append(whereParts, fmt.Sprintf("AND l.processing_status=$%d", pos))
			args = append(args, status)
			pos++
		}
		if len(whereParts) > 0 {
			q += " " + strings.Join(whereParts, " ")
		}
		q += `
		ORDER BY GREATEST(
			COALESCE(l.requested_at, ''),
			COALESCE(l.checker_at,   '')
		) DESC`

		pgRows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer pgRows.Close()

		var out []map[string]interface{}
		fds := pgRows.FieldDescriptions()
		for pgRows.Next() {
			vals, err := pgRows.Values()
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed")
				return
			}
			row := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				row[string(fd.Name)] = vals[i]
			}
			out = append(out, row)
		}
		if out == nil {
			out = []map[string]interface{}{}
		}
		api.RespondWithPayload(w, true, "", map[string]any{"rows": out, "count": len(out)})
		api.LogInfo("GetApprovalMatrixAll: returned %d rows", len(out))
	}
}

// ─── 7. GetApprovalMatrixDetail ───────────────────────────────────────────────
// Full nested detail: matrix → eyes → members.
// Each level includes its own audit trail: created_by, edited_by, deleted_by,
// processing_status, checker info.

func GetApprovalMatrixDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		matrixID := r.URL.Query().Get("matrix_id")
		if matrixID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "matrix_id query param is required")
			return
		}
		ctx := r.Context()

		// ── Fetch matrix with its audit summary ─────────────────────────────────
		var detail MatrixDetail
		var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
		var processingStatus, checkerBy, checkerAt, checkerComment string
		err := pgxPool.QueryRow(ctx, `
			WITH audit_history AS (
				SELECT
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM uam.audit_approval_matrix_master WHERE matrix_id=$1
			),
			latest_audit AS (
				SELECT processing_status,
					COALESCE(checker_by,'') AS checker_by,
					COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
					COALESCE(checker_comment,'') AS checker_comment,
					old_module_code, old_entity_code, old_transaction_type,
					old_min_amount, old_max_amount, old_description,
					old_approval_order, old_sla_hours, old_is_active
				FROM uam.audit_approval_matrix_master WHERE matrix_id=$1
				ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz),COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC
				LIMIT 1
			)
			SELECT m.matrix_id, m.module_code, m.entity_code,
				COALESCE(me.entity_name,'') AS entity_name,
				m.transaction_type,
				m.min_amount, m.max_amount, m.description, m.approval_order, m.sla_hours, m.is_active,
				COALESCE(la.processing_status,''),
				COALESCE(la.checker_by,''), COALESCE(la.checker_at,''), COALESCE(la.checker_comment,''),
				COALESCE(h.created_by,''), COALESCE(h.created_at,''),
				COALESCE(h.edited_by,''),  COALESCE(h.edited_at,''),
				COALESCE(h.deleted_by,''), COALESCE(h.deleted_at,''),
				la.old_module_code, la.old_entity_code, la.old_transaction_type,
				la.old_min_amount, la.old_max_amount, la.old_description,
				la.old_approval_order, la.old_sla_hours, la.old_is_active
			FROM uam.approval_matrix_master m
			CROSS JOIN audit_history h
			LEFT JOIN latest_audit la ON true
			LEFT JOIN public.masterentitycash me ON me.entity_id = m.entity_code
			WHERE m.matrix_id=$1 AND m.is_deleted=false`, matrixID,
		).Scan(
			&detail.MatrixID, &detail.ModuleCode, &detail.EntityCode, &detail.EntityName, &detail.TransactionType,
			&detail.MinAmount, &detail.MaxAmount, &detail.Description, &detail.ApprovalOrder,
			&detail.SlaHours, &detail.IsActive,
			&processingStatus, &checkerBy, &checkerAt, &checkerComment,
			&createdBy, &createdAt, &editedBy, &editedAt, &deletedBy, &deletedAt,
			&detail.OldModuleCode, &detail.OldEntityCode, &detail.OldTransactionType,
			&detail.OldMinAmount, &detail.OldMaxAmount, &detail.OldDescription,
			&detail.OldApprovalOrder, &detail.OldSlaHours, &detail.OldIsActive,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrApprovalMatrixNotFound)
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		detail.ProcessingStatus = processingStatus
		detail.CheckerBy = checkerBy
		detail.CheckerAt = checkerAt
		detail.CheckerComment = checkerComment
		detail.CreatedBy = createdBy
		detail.CreatedAt = createdAt
		detail.EditedBy = editedBy
		detail.EditedAt = editedAt
		detail.DeletedBy = deletedBy
		detail.DeletedAt = deletedAt
		detail.Eyes = []EyeDetail{}

		// ── Fetch eyes with their audit summary ─────────────────────────────────
		eyeRows, err := pgxPool.Query(ctx, `
			WITH eye_history AS (
				SELECT eye_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at
				FROM uam.audit_approval_matrix_eye WHERE matrix_id=$1
				GROUP BY eye_id
			),
			latest_eye_audit AS (
				SELECT DISTINCT ON (eye_id)
					eye_id, processing_status,
					COALESCE(checker_by,'') AS checker_by,
					COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
					old_eye_count, old_position, old_sla_hours, old_is_active
				FROM uam.audit_approval_matrix_eye WHERE matrix_id=$1
				ORDER BY eye_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz),COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC
			)
			SELECT e.eye_id, e.eye_count, e.position, e.sla_hours, e.is_active,
				COALESCE(la.processing_status,''),
				COALESCE(la.checker_by,''), COALESCE(la.checker_at,''),
				COALESCE(h.created_by,''),  COALESCE(h.created_at,''),
				COALESCE(h.edited_by,''),   COALESCE(h.edited_at,''),
				la.old_eye_count, la.old_position, la.old_sla_hours, la.old_is_active
			FROM uam.approval_matrix_eye e
			LEFT JOIN eye_history h       ON h.eye_id = e.eye_id
			LEFT JOIN latest_eye_audit la ON la.eye_id = e.eye_id
			WHERE e.matrix_id=$1 AND e.is_deleted=false
			ORDER BY e.position`, matrixID,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Eye query failed: "+err.Error())
			return
		}
		defer eyeRows.Close()

		eyeMap := map[string]*EyeDetail{}
		var eyeOrder []string
		for eyeRows.Next() {
			var ed EyeDetail
			var eidStr string
			if err := eyeRows.Scan(
				&eidStr, &ed.EyeCount, &ed.Position, &ed.SlaHours, &ed.IsActive,
				&ed.ProcessingStatus, &ed.CheckerBy, &ed.CheckerAt,
				&ed.CreatedBy, &ed.CreatedAt, &ed.EditedBy, &ed.EditedAt,
				&ed.OldEyeCount, &ed.OldPosition, &ed.OldSlaHours, &ed.OldIsActive,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Eye scan failed: "+err.Error())
				return
			}
			ed.EyeID = eidStr
			ed.Members = []MemberDetail{}
			eyeMap[eidStr] = &ed
			eyeOrder = append(eyeOrder, eidStr)
		}
		eyeRows.Close()

		// ── Fetch members with their audit summary ──────────────────────────────
		if len(eyeOrder) > 0 {
			memberRows, err := pgxPool.Query(ctx, `
				WITH member_history AS (
					SELECT member_id,
						MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
						MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
						MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
						MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at
					FROM uam.audit_approval_matrix_eye_member WHERE matrix_id=$1
					GROUP BY member_id
				),
				latest_member_audit AS (
					SELECT DISTINCT ON (member_id)
						member_id, processing_status,
						COALESCE(checker_by,'') AS checker_by,
						COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
						old_member_type, old_assignment_type, old_role_id, old_user_id, old_slot_order, old_is_active
					FROM uam.audit_approval_matrix_eye_member WHERE matrix_id=$1
					ORDER BY member_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz),COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC
				)
				SELECT mb.member_id, mb.eye_id, mb.member_type, mb.assignment_type,
					mb.slot_order, mb.is_active,
					r.id::text AS role_id, r.name AS role_name, r.rolecode,
					u.id::text AS user_id, u.employee_name, u.email, u.mobile,
					COALESCE(la.processing_status,''),
					COALESCE(la.checker_by,''),       COALESCE(la.checker_at,''),
					COALESCE(h.created_by,''),        COALESCE(h.created_at,''),
					COALESCE(h.edited_by,''),         COALESCE(h.edited_at,''),
					la.old_member_type, la.old_assignment_type,
					la.old_role_id::text, la.old_user_id::text,
					la.old_slot_order, la.old_is_active
				FROM uam.approval_matrix_eye_member mb
				LEFT JOIN member_history h        ON h.member_id = mb.member_id
				LEFT JOIN latest_member_audit la  ON la.member_id = mb.member_id
				LEFT JOIN public.roles r ON r.id::text = mb.role_id
				LEFT JOIN public.users u ON u.id::text = mb.user_id
				WHERE mb.matrix_id=$1 AND mb.is_deleted=false
				ORDER BY mb.eye_id, mb.slot_order`, matrixID,
			)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Member query failed: "+err.Error())
				return
			}
			defer memberRows.Close()

			for memberRows.Next() {
				var md MemberDetail
				var midStr, eidStr string
				if err := memberRows.Scan(
					&midStr, &eidStr, &md.MemberType, &md.AssignmentType,
					&md.SlotOrder, &md.IsActive,
					&md.RoleID, &md.RoleName, &md.RoleCode,
					&md.UserID, &md.EmployeeName, &md.Email, &md.Mobile,
					&md.ProcessingStatus, &md.CheckerBy, &md.CheckerAt,
					&md.CreatedBy, &md.CreatedAt, &md.EditedBy, &md.EditedAt,
					&md.OldMemberType, &md.OldAssignmentType,
					&md.OldRoleID, &md.OldUserID,
					&md.OldSlotOrder, &md.OldIsActive,
				); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Member scan failed: "+err.Error())
					return
				}
				md.MemberID = midStr
				if eye, ok := eyeMap[eidStr]; ok {
					eye.Members = append(eye.Members, md)
				}
			}
			memberRows.Close()
		}

		for _, eid := range eyeOrder {
			detail.Eyes = append(detail.Eyes, *eyeMap[eid])
		}
		api.RespondWithPayload(w, true, "", detail)
	}
}

// ─── 8. GetApprovalMatrixAuditHistory ─────────────────────────────────────────
// Returns audit history grouped by logical event (action_type + requested_by +
// requested_at). Each event has a master snapshot plus nested eyes → members,
// exactly mirroring the detail response structure.

func GetApprovalMatrixAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		matrixID := r.URL.Query().Get("matrix_id")
		if matrixID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "matrix_id query param is required")
			return
		}
		ctx := r.Context()

		// ── internal types ──────────────────────────────────────────────────────
		type auditMasterRow struct {
			auditID          string
			actionType       string
			processingStatus string
			reason           string
			requestedBy      string
			requestedAt      string
			checkerBy        string
			checkerAt        string
			checkerComment   string
			oldModuleCode    *string
			oldEntityCode    *string
			oldTxType        *string
			oldMinAmount     *float64
			oldMaxAmount     *float64
			oldDescription   *string
			oldApprovalOrder *string
			oldSlaHours      *int
			oldIsActive      *bool
		}
		type auditEyeRow struct {
			auditID          string
			eyeID            string
			actionType       string
			processingStatus string
			requestedBy      string
			requestedAt      string
			checkerBy        string
			checkerAt        string
			oldEyeCount      *int
			oldPosition      *int
			oldSlaHours      *int
			oldIsActive      *bool
		}
		type auditMemberRow struct {
			auditID          string
			eyeID            string
			memberID         string
			actionType       string
			processingStatus string
			requestedBy      string
			requestedAt      string
			checkerBy        string
			checkerAt        string
			oldMemberType    *string
			oldAssignType    *string
			oldRoleID        *string
			oldUserID        *string
			oldSlotOrder     *int
			oldIsActive      *bool
		}

		// ── 1. fetch master audit rows ──────────────────────────────────────────
		masterRows, err := pgxPool.Query(ctx, `
			SELECT audit_id::text, action_type, processing_status,
				COALESCE(reason,''),
				COALESCE(requested_by,''),
				COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS'),''),
				COALESCE(checker_by,''),
				COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),''),
				COALESCE(checker_comment,''),
				old_module_code, old_entity_code, old_transaction_type,
				old_min_amount, old_max_amount, old_description,
				old_approval_order, old_sla_hours, old_is_active
			FROM uam.audit_approval_matrix_master
			WHERE matrix_id=$1
			ORDER BY requested_at DESC`, matrixID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Master audit query failed: "+err.Error())
			return
		}

		// eventKey → auditMasterRow, preserving order
		type eventKey struct{ actionType, requestedBy, requestedAt string }
		var eventOrder []eventKey
		eventMaster := map[eventKey]*auditMasterRow{}

		for masterRows.Next() {
			var m auditMasterRow
			if err := masterRows.Scan(
				&m.auditID, &m.actionType, &m.processingStatus,
				&m.reason, &m.requestedBy, &m.requestedAt,
				&m.checkerBy, &m.checkerAt, &m.checkerComment,
				&m.oldModuleCode, &m.oldEntityCode, &m.oldTxType,
				&m.oldMinAmount, &m.oldMaxAmount, &m.oldDescription,
				&m.oldApprovalOrder, &m.oldSlaHours, &m.oldIsActive,
			); err != nil {
				masterRows.Close()
				api.RespondWithError(w, http.StatusInternalServerError, "Master audit scan failed: "+err.Error())
				return
			}
			k := eventKey{m.actionType, m.requestedBy, m.requestedAt}
			if _, exists := eventMaster[k]; !exists {
				eventOrder = append(eventOrder, k)
				cp := m
				eventMaster[k] = &cp
			}
		}
		masterRows.Close()

		// ── 2. fetch eye audit rows ─────────────────────────────────────────────
		eyeAuditRows, err := pgxPool.Query(ctx, `
			SELECT audit_id::text, eye_id::text, action_type, processing_status,
				COALESCE(requested_by,''),
				COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS'),''),
				COALESCE(checker_by,''),
				COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),''),
				old_eye_count, old_position, old_sla_hours, old_is_active
			FROM uam.audit_approval_matrix_eye
			WHERE matrix_id=$1
			ORDER BY requested_at DESC, eye_id`, matrixID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Eye audit query failed: "+err.Error())
			return
		}

		// eventKey → eyeID → auditEyeRow
		type eyeEventKey struct {
			eventKey
			eyeID string
		}
		eventEyes := map[eventKey]map[string]*auditEyeRow{}
		eventEyeOrder := map[eventKey][]string{}

		for eyeAuditRows.Next() {
			var e auditEyeRow
			if err := eyeAuditRows.Scan(
				&e.auditID, &e.eyeID, &e.actionType, &e.processingStatus,
				&e.requestedBy, &e.requestedAt,
				&e.checkerBy, &e.checkerAt,
				&e.oldEyeCount, &e.oldPosition, &e.oldSlaHours, &e.oldIsActive,
			); err != nil {
				eyeAuditRows.Close()
				api.RespondWithError(w, http.StatusInternalServerError, "Eye audit scan failed: "+err.Error())
				return
			}
			k := eventKey{e.actionType, e.requestedBy, e.requestedAt}
			if eventEyes[k] == nil {
				eventEyes[k] = map[string]*auditEyeRow{}
			}
			if _, exists := eventEyes[k][e.eyeID]; !exists {
				eventEyeOrder[k] = append(eventEyeOrder[k], e.eyeID)
				cp := e
				eventEyes[k][e.eyeID] = &cp
			}
		}
		eyeAuditRows.Close()

		// ── 3. fetch member audit rows ──────────────────────────────────────────
		memAuditRows, err := pgxPool.Query(ctx, `
			SELECT audit_id::text, eye_id::text, member_id::text,
				action_type, processing_status,
				COALESCE(requested_by,''),
				COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS'),''),
				COALESCE(checker_by,''),
				COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),''),
				old_member_type, old_assignment_type,
				old_role_id::text, old_user_id::text,
				old_slot_order, old_is_active
			FROM uam.audit_approval_matrix_eye_member
			WHERE matrix_id=$1
			ORDER BY requested_at DESC, eye_id, member_id`, matrixID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Member audit query failed: "+err.Error())
			return
		}

		// eventKey → eyeID → memberID → auditMemberRow
		eventMembers := map[eventKey]map[string]map[string]*auditMemberRow{}
		eventMemberOrder := map[eyeEventKey][]string{}

		for memAuditRows.Next() {
			var m auditMemberRow
			if err := memAuditRows.Scan(
				&m.auditID, &m.eyeID, &m.memberID,
				&m.actionType, &m.processingStatus,
				&m.requestedBy, &m.requestedAt,
				&m.checkerBy, &m.checkerAt,
				&m.oldMemberType, &m.oldAssignType,
				&m.oldRoleID, &m.oldUserID,
				&m.oldSlotOrder, &m.oldIsActive,
			); err != nil {
				memAuditRows.Close()
				api.RespondWithError(w, http.StatusInternalServerError, "Member audit scan failed: "+err.Error())
				return
			}
			k := eventKey{m.actionType, m.requestedBy, m.requestedAt}
			if eventMembers[k] == nil {
				eventMembers[k] = map[string]map[string]*auditMemberRow{}
			}
			if eventMembers[k][m.eyeID] == nil {
				eventMembers[k][m.eyeID] = map[string]*auditMemberRow{}
			}
			ek := eyeEventKey{k, m.eyeID}
			if _, exists := eventMembers[k][m.eyeID][m.memberID]; !exists {
				eventMemberOrder[ek] = append(eventMemberOrder[ek], m.memberID)
				cp := m
				eventMembers[k][m.eyeID][m.memberID] = &cp
			}
		}
		memAuditRows.Close()

		// ── 4. assemble nested response ─────────────────────────────────────────
		type memberSnap struct {
			MemberID         string  `json:"member_id"`
			AuditID          string  `json:"audit_id"`
			ActionType       string  `json:"action_type"`
			ProcessingStatus string  `json:"processing_status"`
			CheckerBy        string  `json:"checker_by"`
			CheckerAt        string  `json:"checker_at"`
			OldMemberType    *string `json:"old_member_type"`
			OldAssignType    *string `json:"old_assignment_type"`
			OldRoleID        *string `json:"old_role_id"`
			OldUserID        *string `json:"old_user_id"`
			OldSlotOrder     *int    `json:"old_slot_order"`
			OldIsActive      *bool   `json:"old_is_active"`
		}
		type eyeSnap struct {
			EyeID            string       `json:"eye_id"`
			AuditID          string       `json:"audit_id"`
			ActionType       string       `json:"action_type"`
			ProcessingStatus string       `json:"processing_status"`
			CheckerBy        string       `json:"checker_by"`
			CheckerAt        string       `json:"checker_at"`
			OldEyeCount      *int         `json:"old_eye_count"`
			OldPosition      *int         `json:"old_position"`
			OldSlaHours      *int         `json:"old_sla_hours"`
			OldIsActive      *bool        `json:"old_is_active"`
			Members          []memberSnap `json:"members"`
		}
		type eventSnap struct {
			AuditID          string    `json:"audit_id"`
			ActionType       string    `json:"action_type"`
			ProcessingStatus string    `json:"processing_status"`
			Reason           string    `json:"reason"`
			RequestedBy      string    `json:"requested_by"`
			RequestedAt      string    `json:"requested_at"`
			CheckerBy        string    `json:"checker_by"`
			CheckerAt        string    `json:"checker_at"`
			CheckerComment   string    `json:"checker_comment"`
			OldModuleCode    *string   `json:"old_module_code"`
			OldEntityCode    *string   `json:"old_entity_code"`
			OldTxType        *string   `json:"old_transaction_type"`
			OldMinAmount     *float64  `json:"old_min_amount"`
			OldMaxAmount     *float64  `json:"old_max_amount"`
			OldDescription   *string   `json:"old_description"`
			OldApprovalOrder *string   `json:"old_approval_order"`
			OldSlaHours      *int      `json:"old_sla_hours"`
			OldIsActive      *bool     `json:"old_is_active"`
			Eyes             []eyeSnap `json:"eyes"`
		}

		var events []eventSnap
		for _, k := range eventOrder {
			m := eventMaster[k]
			ev := eventSnap{
				AuditID:          m.auditID,
				ActionType:       m.actionType,
				ProcessingStatus: m.processingStatus,
				Reason:           m.reason,
				RequestedBy:      m.requestedBy,
				RequestedAt:      m.requestedAt,
				CheckerBy:        m.checkerBy,
				CheckerAt:        m.checkerAt,
				CheckerComment:   m.checkerComment,
				OldModuleCode:    m.oldModuleCode,
				OldEntityCode:    m.oldEntityCode,
				OldTxType:        m.oldTxType,
				OldMinAmount:     m.oldMinAmount,
				OldMaxAmount:     m.oldMaxAmount,
				OldDescription:   m.oldDescription,
				OldApprovalOrder: m.oldApprovalOrder,
				OldSlaHours:      m.oldSlaHours,
				OldIsActive:      m.oldIsActive,
				Eyes:             []eyeSnap{},
			}

			eyesForEvent := eventEyes[k]
			for _, eid := range eventEyeOrder[k] {
				e := eyesForEvent[eid]
				es := eyeSnap{
					EyeID:            e.eyeID,
					AuditID:          e.auditID,
					ActionType:       e.actionType,
					ProcessingStatus: e.processingStatus,
					CheckerBy:        e.checkerBy,
					CheckerAt:        e.checkerAt,
					OldEyeCount:      e.oldEyeCount,
					OldPosition:      e.oldPosition,
					OldSlaHours:      e.oldSlaHours,
					OldIsActive:      e.oldIsActive,
					Members:          []memberSnap{},
				}
				ek := eyeEventKey{k, eid}
				membersForEye := eventMembers[k][eid]
				for _, mid := range eventMemberOrder[ek] {
					mem := membersForEye[mid]
					es.Members = append(es.Members, memberSnap{
						MemberID:         mem.memberID,
						AuditID:          mem.auditID,
						ActionType:       mem.actionType,
						ProcessingStatus: mem.processingStatus,
						CheckerBy:        mem.checkerBy,
						CheckerAt:        mem.checkerAt,
						OldMemberType:    mem.oldMemberType,
						OldAssignType:    mem.oldAssignType,
						OldRoleID:        mem.oldRoleID,
						OldUserID:        mem.oldUserID,
						OldSlotOrder:     mem.oldSlotOrder,
						OldIsActive:      mem.oldIsActive,
					})
				}
				ev.Eyes = append(ev.Eyes, es)
			}
			events = append(events, ev)
		}
		if events == nil {
			events = []eventSnap{}
		}
		api.RespondWithPayload(w, true, "", map[string]any{"rows": events, "count": len(events)})
	}
}

// ─── 9. GetApprovedActiveMatrices ─────────────────────────────────────────────

func GetApprovedActiveMatrices(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		baseQ := `
			SELECT m.matrix_id, m.module_code, m.entity_code,
				COALESCE(me.entity_name,'') AS entity_name,
				m.transaction_type,
				m.min_amount, m.max_amount, m.approval_order,
				COALESCE(m.sla_hours, (
					SELECT MAX(e.sla_hours)::int
					FROM uam.approval_matrix_eye e
					WHERE e.matrix_id = m.matrix_id
					  AND e.is_deleted = false
					  AND e.sla_hours IS NOT NULL
				)) AS sla_hours, m.is_active,
				MAX(CASE WHEN a.action_type='CREATE' THEN a.requested_by END) AS created_by,
				MAX(CASE WHEN a.action_type='CREATE' THEN TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
				MAX(CASE WHEN a.action_type='EDIT'   THEN a.requested_by END) AS edited_by,
				MAX(CASE WHEN a.action_type='EDIT'   THEN TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at
			FROM uam.approval_matrix_master m
			INNER JOIN uam.audit_approval_matrix_master a ON a.matrix_id=m.matrix_id
			LEFT JOIN public.masterentitycash me ON me.entity_id = m.entity_code
			WHERE a.processing_status='APPROVED' AND m.is_active=true AND m.is_deleted=false`

		var whereParts []string
		var args []interface{}
		pos := 1

		if mc := r.URL.Query().Get("module_code"); mc != "" {
			whereParts = append(whereParts, fmt.Sprintf("AND m.module_code=$%d", pos))
			args = append(args, mc)
			pos++
		}
		if ec := r.URL.Query().Get("entity_code"); ec != "" {
			whereParts = append(whereParts, fmt.Sprintf("AND m.entity_code=$%d", pos))
			args = append(args, ec)
			pos++
		}
		if tt := r.URL.Query().Get("transaction_type"); tt != "" {
			whereParts = append(whereParts, fmt.Sprintf("AND m.transaction_type=$%d", pos))
			args = append(args, tt)
			pos++
		}

		fullQ := baseQ
		if len(whereParts) > 0 {
			fullQ += " " + strings.Join(whereParts, " ")
		}
		fullQ += " GROUP BY m.matrix_id, m.module_code, m.entity_code, me.entity_name, m.transaction_type, m.min_amount, m.max_amount, m.approval_order, m.is_active, COALESCE(m.sla_hours, (SELECT MAX(e.sla_hours)::int FROM uam.approval_matrix_eye e WHERE e.matrix_id = m.matrix_id AND e.is_deleted = false AND e.sla_hours IS NOT NULL))"
		fullQ += " ORDER BY m.entity_code, m.transaction_type, m.min_amount"

		rows, err := pgxPool.Query(ctx, fullQ, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		fds := rows.FieldDescriptions()
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed")
				return
			}
			row := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				row[string(fd.Name)] = vals[i]
			}
			out = append(out, row)
		}
		if out == nil {
			out = []map[string]interface{}{}
		}
		api.RespondWithPayload(w, true, "", map[string]any{"rows": out})
	}
}

// ─── Granular eye/member helpers (kept for fine-grained ops) ──────────────────

func AddEyeToMatrix(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string        `json:"user_id"`
			MatrixID string        `json:"matrix_id"`
			EyeCount int           `json:"eye_count"`
			Position int           `json:"position"`
			SlaHours *int          `json:"sla_hours"`
			Members  []MemberInput `json:"members"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.MatrixID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "matrix_id is required")
			return
		}
		if err := validateEyeFields(req.EyeCount, req.Position, req.SlaHours); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		if len(req.Members) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "at least 1 member is required")
			return
		}
		if len(req.Members) > 5 {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("a maximum of 5 members is allowed (got %d)", len(req.Members)))
			return
		}
		hasApprover, hasEscalation := false, false
		for i, m := range req.Members {
			if m.SlotOrder == 0 {
				req.Members[i].SlotOrder = 1
			}
			if err := validateMemberFields(m.MemberType, m.AssignmentType, m.RoleID, m.UserID, req.Members[i].SlotOrder); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("member[%d]: %s", i, err.Error()))
				return
			}
			switch m.MemberType {
			case "APPROVER":
				hasApprover = true
			case "ESCALATION":
				hasEscalation = true
			}
		}
		if hasEscalation && !hasApprover {
			api.RespondWithError(w, http.StatusBadRequest,
				"an ESCALATION eye must contain at least one APPROVER member (role/user) in addition to its ESCALATION member(s)")
			return
		}
		req.EyeCount = normaliseEyeCount(len(req.Members))
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)

		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT true FROM uam.approval_matrix_master WHERE matrix_id=$1 AND is_deleted=false`,
			req.MatrixID,
		).Scan(&exists); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrApprovalMatrixNotFound)
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrLookupFailed)
			return
		}

		var eyeID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO uam.approval_matrix_eye (matrix_id,eye_count,position,sla_hours)
			VALUES ($1,$2,$3,$4) RETURNING eye_id`,
			req.MatrixID, req.EyeCount, req.Position, req.SlaHours,
		).Scan(&eyeID); err != nil {
			msg, status := getUserFriendlyApprovalMatrixError(err, "Add eye failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_eye
				(eye_id,matrix_id,action_type,processing_status,requested_by,requested_at)
			VALUES ($1,$2,'CREATE','PENDING_APPROVAL',$3,now())`, eyeID, req.MatrixID, userEmail,
		); err != nil {
			logDBError(err, "AddEyeToMatrix audit")
			msg, status := getUserFriendlyApprovalMatrixError(err, constants.ErrEyeAuditFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		membersCreated := 0
		for _, m := range req.Members {
			var memberID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO uam.approval_matrix_eye_member
					(eye_id,matrix_id,member_type,assignment_type,role_id,user_id,slot_order)
				VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING member_id`,
				eyeID, req.MatrixID, m.MemberType, m.AssignmentType, m.RoleID, m.UserID, m.SlotOrder,
			).Scan(&memberID); err != nil {
				msg, status := getUserFriendlyApprovalMatrixError(err, "Add member failed")
				api.RespondWithError(w, status, msg)
				return
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO uam.audit_approval_matrix_eye_member
					(member_id,eye_id,matrix_id,action_type,processing_status,requested_by,requested_at)
				VALUES ($1,$2,$3,'CREATE','PENDING_APPROVAL',$4,now())`,
				memberID, eyeID, req.MatrixID, userEmail,
			); err != nil {
				logDBError(err, "AddEyeToMatrix member audit")
				msg, status := getUserFriendlyApprovalMatrixError(err, constants.ErrMemberAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
			membersCreated++
		}

		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "AddEyeToMatrix commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"eye_id": eyeID, "matrix_id": req.MatrixID,
			"members_created": membersCreated, "requested_by": userEmail,
		})
		api.LogInfo("Eye added: eye_id=%s matrix_id=%s members=%d", eyeID, req.MatrixID, membersCreated)
	}
}

func UpdateEye(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                 `json:"user_id"`
			EyeID  string                 `json:"eye_id"`
			Fields map[string]interface{} `json:"fields"`
			Reason string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.EyeID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "eye_id is required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No fields provided for update")
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)
		var matrixID string
		var oldEyeCount, oldPosition int
		var oldSlaHours *int
		var oldIsActive bool
		if err := tx.QueryRow(ctx, `
			SELECT matrix_id,eye_count,position,sla_hours,is_active
			FROM uam.approval_matrix_eye WHERE eye_id=$1 AND is_deleted=false FOR UPDATE`, req.EyeID,
		).Scan(&matrixID, &oldEyeCount, &oldPosition, &oldSlaHours, &oldIsActive); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Eye not found")
				return
			}
			msg, status := getUserFriendlyApprovalMatrixError(err, "Fetch eye failed")
			api.RespondWithError(w, status, msg)
			return
		}
		allowed := map[string]bool{"sla_hours": true, "position": true, "is_active": true}
		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			if allowed[strings.ToLower(k)] {
				sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, strings.ToLower(k), pos))
				args = append(args, v)
				pos++
			}
		}
		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}
		args = append(args, req.EyeID)
		q := fmt.Sprintf("UPDATE uam.approval_matrix_eye SET %s WHERE eye_id=$%d", strings.Join(sets, ", "), pos)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			logDBError(err, "UpdateEye exec")
			msg, status := getUserFriendlyApprovalMatrixError(err, "Update eye failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_eye
				(eye_id,matrix_id,action_type,processing_status,reason,requested_by,requested_at,
				 old_eye_count,old_position,old_sla_hours,old_is_active)
			VALUES ($1,$2,'EDIT','PENDING_EDIT_APPROVAL',$3,$4,now(),$5,$6,$7,$8)`,
			req.EyeID, matrixID, req.Reason, userEmail, oldEyeCount, oldPosition, oldSlaHours, oldIsActive,
		); err != nil {
			logDBError(err, "UpdateEye audit")
			msg, status := getUserFriendlyApprovalMatrixError(err, constants.ErrEyeAuditFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "UpdateEye commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"eye_id": req.EyeID, "requested_by": userEmail})
		api.LogInfo("Eye updated: eye_id=%s by=%s", req.EyeID, userEmail)
	}
}

func DeleteEye(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			EyeIDs []string `json:"eye_ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.EyeIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "eye_ids cannot be empty")
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)
		rows, err := tx.Query(ctx,
			`SELECT eye_id,matrix_id FROM uam.approval_matrix_eye WHERE eye_id=ANY($1) AND is_deleted=false`,
			req.EyeIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrLookupFailed)
			return
		}
		type eyeRec struct{ matrixID string }
		existing := map[string]eyeRec{}
		for rows.Next() {
			var eID, mID string
			if err := rows.Scan(&eID, &mID); err == nil {
				existing[eID] = eyeRec{mID}
			}
		}
		rows.Close()
		var results []map[string]any
		for _, id := range req.EyeIDs {
			rec, ok := existing[id]
			if !ok {
				results = append(results, map[string]any{"success": false, "eye_id": id, "error": "Not found or already deleted"})
				continue
			}
			// Audit Table 5: eye delete pending
			if _, err := tx.Exec(ctx, `
				INSERT INTO uam.audit_approval_matrix_eye
					(eye_id,matrix_id,action_type,processing_status,reason,requested_by,requested_at)
				VALUES ($1,$2,'DELETE','PENDING_DELETE_APPROVAL',$3,$4,now())`,
				id, rec.matrixID, req.Reason, userEmail,
			); err != nil {
				logDBError(err, "DeleteEye audit")
				results = append(results, map[string]any{"success": false, "eye_id": id, "error": err.Error()})
				continue
			}
			// Cascade: audit all members under this eye
			mbRows, err := tx.Query(ctx,
				`SELECT member_id FROM uam.approval_matrix_eye_member WHERE eye_id=$1 AND is_deleted=false`, id,
			)
			if err != nil {
				logDBError(err, "DeleteEye member lookup")
			} else {
				for mbRows.Next() {
					var mID string
					if scanErr := mbRows.Scan(&mID); scanErr == nil {
						if _, execErr := tx.Exec(ctx, `
							INSERT INTO uam.audit_approval_matrix_eye_member
								(member_id,eye_id,matrix_id,action_type,processing_status,reason,requested_by,requested_at)
							VALUES ($1,$2,$3,'DELETE','PENDING_DELETE_APPROVAL',$4,$5,now())`,
							mID, id, rec.matrixID, req.Reason, userEmail,
						); execErr != nil {
							logDBError(execErr, "DeleteEye member audit")
						}
					}
				}
				mbRows.Close()
			}
			results = append(results, map[string]any{"success": true, "eye_id": id})
		}
		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "DeleteEye commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", results)
		api.LogInfo("DeleteEye: %d IDs by=%s", len(req.EyeIDs), userEmail)
	}
}

func AddMemberToEye(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string        `json:"user_id"`
			EyeID    string        `json:"eye_id"`
			MatrixID string        `json:"matrix_id"`
			Members  []MemberInput `json:"members"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.EyeID == "" || req.MatrixID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "eye_id and matrix_id are required")
			return
		}
		if len(req.Members) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "members array cannot be empty")
			return
		}
		for i, m := range req.Members {
			if m.SlotOrder == 0 {
				req.Members[i].SlotOrder = 1
			}
			if err := validateMemberFields(m.MemberType, m.AssignmentType, m.RoleID, m.UserID, req.Members[i].SlotOrder); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("member[%d]: %s", i, err.Error()))
				return
			}
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT true FROM uam.approval_matrix_eye WHERE eye_id=$1 AND is_deleted=false`, req.EyeID,
		).Scan(&exists); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Eye-round not found")
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrLookupFailed)
			return
		}
		membersCreated := 0
		for _, m := range req.Members {
			var memberID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO uam.approval_matrix_eye_member
					(eye_id,matrix_id,member_type,assignment_type,role_id,user_id,slot_order)
				VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING member_id`,
				req.EyeID, req.MatrixID, m.MemberType, m.AssignmentType, m.RoleID, m.UserID, m.SlotOrder,
			).Scan(&memberID); err != nil {
				msg, status := getUserFriendlyApprovalMatrixError(err, "Add member failed")
				api.RespondWithError(w, status, msg)
				return
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO uam.audit_approval_matrix_eye_member
					(member_id,eye_id,matrix_id,action_type,processing_status,requested_by,requested_at)
				VALUES ($1,$2,$3,'CREATE','PENDING_APPROVAL',$4,now())`,
				memberID, req.EyeID, req.MatrixID, userEmail,
			); err != nil {
				logDBError(err, "AddMemberToEye audit")
				msg, status := getUserFriendlyApprovalMatrixError(err, constants.ErrMemberAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
			membersCreated++
		}
		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "AddMemberToEye commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{
			"members_created": membersCreated, "eye_id": req.EyeID, "requested_by": userEmail,
		})
		api.LogInfo("Members added: eye_id=%s count=%d by=%s", req.EyeID, membersCreated, userEmail)
	}
}

func UpdateMember(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                 `json:"user_id"`
			MemberID string                 `json:"member_id"`
			Fields   map[string]interface{} `json:"fields"`
			Reason   string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.MemberID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "member_id is required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No fields provided for update")
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)
		var eyeID, matrixID, oldMemberType, oldAssignmentType string
		var oldRoleID, oldUserID *string
		var oldSlotOrder int
		var oldIsActive bool
		if err := tx.QueryRow(ctx, `
			SELECT eye_id,matrix_id,member_type,assignment_type,role_id,user_id,slot_order,is_active
			FROM uam.approval_matrix_eye_member
			WHERE member_id=$1 AND is_deleted=false FOR UPDATE`, req.MemberID,
		).Scan(&eyeID, &matrixID, &oldMemberType, &oldAssignmentType,
			&oldRoleID, &oldUserID, &oldSlotOrder, &oldIsActive,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Member not found")
				return
			}
			msg, status := getUserFriendlyApprovalMatrixError(err, "Fetch member failed")
			api.RespondWithError(w, status, msg)
			return
		}
		allowed := map[string]bool{
			"member_type": true, "assignment_type": true, "role_id": true,
			"user_id": true, "slot_order": true, "is_active": true,
		}
		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			if allowed[strings.ToLower(k)] {
				sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, strings.ToLower(k), pos))
				args = append(args, v)
				pos++
			}
		}
		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}
		args = append(args, req.MemberID)
		q := fmt.Sprintf("UPDATE uam.approval_matrix_eye_member SET %s WHERE member_id=$%d", strings.Join(sets, ", "), pos)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			logDBError(err, "UpdateMember exec")
			msg, status := getUserFriendlyApprovalMatrixError(err, "Update member failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO uam.audit_approval_matrix_eye_member
				(member_id,eye_id,matrix_id,action_type,processing_status,reason,requested_by,requested_at,
				 old_member_type,old_assignment_type,old_role_id,old_user_id,old_slot_order,old_is_active)
			VALUES ($1,$2,$3,'EDIT','PENDING_EDIT_APPROVAL',$4,$5,now(),$6,$7,$8,$9,$10,$11)`,
			req.MemberID, eyeID, matrixID, req.Reason, userEmail,
			oldMemberType, oldAssignmentType, oldRoleID, oldUserID, oldSlotOrder, oldIsActive,
		); err != nil {
			logDBError(err, "UpdateMember audit")
			msg, status := getUserFriendlyApprovalMatrixError(err, constants.ErrMemberAuditFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "UpdateMember commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"member_id": req.MemberID, "requested_by": userEmail})
		api.LogInfo("Member updated: member_id=%s by=%s", req.MemberID, userEmail)
	}
}

func DeleteMember(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			MemberIDs []string `json:"member_ids"`
			Reason    string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.MemberIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "member_ids cannot be empty")
			return
		}
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx)
		rows, err := tx.Query(ctx,
			`SELECT member_id,eye_id,matrix_id FROM uam.approval_matrix_eye_member WHERE member_id=ANY($1) AND is_deleted=false`,
			req.MemberIDs,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrLookupFailed)
			return
		}
		type memberRec struct{ eyeID, matrixID string }
		existing := map[string]memberRec{}
		for rows.Next() {
			var mID, eID, matID string
			if err := rows.Scan(&mID, &eID, &matID); err == nil {
				existing[mID] = memberRec{eID, matID}
			}
		}
		rows.Close()
		var results []map[string]any
		for _, id := range req.MemberIDs {
			rec, ok := existing[id]
			if !ok {
				results = append(results, map[string]any{"success": false, "member_id": id, "error": "Not found or already deleted"})
				continue
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO uam.audit_approval_matrix_eye_member
					(member_id,eye_id,matrix_id,action_type,processing_status,reason,requested_by,requested_at)
				VALUES ($1,$2,$3,'DELETE','PENDING_DELETE_APPROVAL',$4,$5,now())`,
				id, rec.eyeID, rec.matrixID, req.Reason, userEmail,
			); err != nil {
				logDBError(err, "DeleteMember audit")
				results = append(results, map[string]any{"success": false, "member_id": id, "error": err.Error()})
				continue
			}
			results = append(results, map[string]any{"success": true, "member_id": id})
		}
		if err := tx.Commit(ctx); err != nil {
			logDBError(err, "DeleteMember commit")
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedUser)
			return
		}
		api.RespondWithPayload(w, true, "", results)
		api.LogInfo("DeleteMember: %d IDs by=%s", len(req.MemberIDs), userEmail)
	}
}
