package transformrules

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	emailcommon "CimplrCorpSaas/api/email/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TransformationRule struct {
	RuleID           string          `json:"rule_id,omitempty"`
	InboxID          string          `json:"inbox_id"`
	RuleName         string          `json:"rule_name"`
	ConditionType    string          `json:"condition_type"`
	ConditionValue   string          `json:"condition_value"`
	MatchMode        string          `json:"match_mode"` // PREFIX | SUFFIX | EXACT | CONTAINS | GLOB (for SUBJECT_CONTAINS / ATTACHMENT_NAME)
	MappingID        string          `json:"mapping_id"`
	MappingName      string          `json:"mapping_name"`
	ActionType       string          `json:"action_type"`
	IsActive         bool            `json:"is_active"`
	ProcessingStatus string          `json:"processing_status"`
	PendingEditJSON  json.RawMessage `json:"pending_edit_json,omitempty"`
	SubmittedBy      string          `json:"submitted_by,omitempty"`
	ApprovedBy       string          `json:"approved_by,omitempty"`
	CheckerComment   string          `json:"checker_comment,omitempty"`
	IsDeleted        bool            `json:"is_deleted"`
	CreatedAt        *time.Time      `json:"created_at,omitempty"`
	UpdatedAt        *time.Time      `json:"updated_at,omitempty"`
	UserID           string          `json:"user_id,omitempty"` // Injected by frontend nos.post
}

// normalizeMatchMode returns a canonical match mode for the given condition type.
// Pattern conditions (SUBJECT_CONTAINS, ATTACHMENT_NAME) require PREFIX|SUFFIX|EXACT|CONTAINS|GLOB.
// Other condition types ignore match_mode at evaluation time and store EXACT.
func normalizeMatchMode(conditionType, matchMode string) (string, error) {
	ct := strings.ToUpper(strings.TrimSpace(conditionType))
	mm := strings.ToUpper(strings.TrimSpace(matchMode))
	needsMode := ct == "SUBJECT_CONTAINS" || ct == "ATTACHMENT_NAME" || ct == "SUBJECT"
	if !needsMode {
		return "EXACT", nil
	}
	if mm == "" {
		if ct == "SUBJECT_CONTAINS" || ct == "SUBJECT" {
			return "CONTAINS", nil
		}
		return "EXACT", nil
	}
	switch mm {
	case "PREFIX", "SUFFIX", "EXACT", "CONTAINS", "GLOB":
		return mm, nil
	default:
		return "", fmt.Errorf("invalid match_mode %q (use PREFIX, SUFFIX, EXACT, CONTAINS, or GLOB)", matchMode)
	}
}

func logAudit(r *http.Request, pool *pgxpool.Pool, ruleID, action, actorID string, newState interface{}) {
	newJSON, _ := json.Marshal(newState)
	query := `INSERT INTO email_svc.transformation_rules_audit (rule_id, audit_action, actor_id, new_state_json) VALUES ($1, $2, $3, $4)`
	_, _ = pool.Exec(r.Context(), query, ruleID, action, actorID, newJSON)
}

type listReq struct {
	InboxID string `json:"inbox_id"`
}

func handleList(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost {
		emailcommon.RespondMethodNotAllowed(w)
		return
	}

	var req listReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		emailcommon.RespondBadRequest(w, "Invalid JSON body")
		return
	}

	inboxID := strings.TrimSpace(req.InboxID)

	query := `
		SELECT rule_id, inbox_id, rule_name, condition_type, condition_value,
		       COALESCE(NULLIF(match_mode, ''), 'CONTAINS'),
		       mapping_id, mapping_name, action_type, is_active, 
		       processing_status, COALESCE(pending_edit_json, '{}'), submitted_by, approved_by, checker_comment, is_deleted,
		       created_at, updated_at
		FROM email_svc.transformation_rules
		WHERE ($1 = '' OR inbox_id::text = $1)
		  AND (is_deleted = false OR is_deleted IS NULL)
		ORDER BY created_at DESC
	`

	rows, err := pool.Query(r.Context(), query, inboxID)
	if err != nil {
		emailcommon.RespondInternal(w, "Failed to list rules")
		return
	}
	defer rows.Close()

	var rules []TransformationRule
	for rows.Next() {
		var rule TransformationRule
		var submittedBy, approvedBy, checkerComment *string
		if err := rows.Scan(
			&rule.RuleID, &rule.InboxID, &rule.RuleName,
			&rule.ConditionType, &rule.ConditionValue, &rule.MatchMode,
			&rule.MappingID, &rule.MappingName, &rule.ActionType,
			&rule.IsActive, &rule.ProcessingStatus, &rule.PendingEditJSON,
			&submittedBy, &approvedBy, &checkerComment, &rule.IsDeleted,
			&rule.CreatedAt, &rule.UpdatedAt,
		); err != nil {
			emailcommon.RespondInternal(w, "Failed to parse rules")
			return
		}
		if submittedBy != nil {
			rule.SubmittedBy = *submittedBy
		}
		if approvedBy != nil {
			rule.ApprovedBy = *approvedBy
		}
		if checkerComment != nil {
			rule.CheckerComment = *checkerComment
		}
		rules = append(rules, rule)
	}

	emailcommon.RespondList(w, "transform-rules-list", rules, len(rules))
}

func handleCreate(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost {
		emailcommon.RespondMethodNotAllowed(w)
		return
	}

	var req TransformationRule
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		emailcommon.RespondBadRequest(w, "Invalid JSON body")
		return
	}
	if req.InboxID == "" || req.RuleName == "" || req.ConditionType == "" || req.MappingID == "" {
		emailcommon.RespondBadRequest(w, "Missing required fields")
		return
	}
	mode, modeErr := normalizeMatchMode(req.ConditionType, req.MatchMode)
	if modeErr != nil {
		emailcommon.RespondBadRequest(w, modeErr.Error())
		return
	}
	req.MatchMode = mode
	req.ProcessingStatus = "PENDING_APPROVAL"

	query := `
		INSERT INTO email_svc.transformation_rules 
		(inbox_id, rule_name, condition_type, condition_value, match_mode, mapping_id, mapping_name, action_type, is_active, processing_status, submitted_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING rule_id, created_at, updated_at
	`
	err := pool.QueryRow(r.Context(), query,
		req.InboxID, req.RuleName, req.ConditionType, req.ConditionValue, req.MatchMode,
		req.MappingID, req.MappingName, req.ActionType, req.IsActive, req.ProcessingStatus, req.UserID,
	).Scan(&req.RuleID, &req.CreatedAt, &req.UpdatedAt)

	if err != nil {
		if err.Error() == "ERROR: duplicate key value violates unique constraint \"unique_active_rule\" (SQLSTATE 23505)" { emailcommon.RespondBadRequest(w, "An active rule with this condition and mapping already exists for this inbox.") } else { emailcommon.RespondInternal(w, "Failed to create rule: " + err.Error()) }
		return
	}

	logAudit(r, pool, req.RuleID, "CREATE_PENDING", req.UserID, req)
	emailcommon.RespondPayload(w, "transform-rules-create", req)
}

func handleUpdate(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost {
		emailcommon.RespondMethodNotAllowed(w)
		return
	}

	var req TransformationRule
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		emailcommon.RespondBadRequest(w, "Invalid JSON body")
		return
	}
	if req.RuleID == "" {
		emailcommon.RespondBadRequest(w, "rule_id is required")
		return
	}
	mode, modeErr := normalizeMatchMode(req.ConditionType, req.MatchMode)
	if modeErr != nil {
		emailcommon.RespondBadRequest(w, modeErr.Error())
		return
	}
	req.MatchMode = mode

	pendingJSON, _ := json.Marshal(req)

	query := `
		UPDATE email_svc.transformation_rules 
		SET processing_status = 'PENDING_APPROVAL',
		    pending_edit_json = $1,
		    submitted_by = $2,
		    updated_at = now()
		WHERE rule_id = $3
		RETURNING updated_at
	`
	err := pool.QueryRow(r.Context(), query, pendingJSON, req.UserID, req.RuleID).Scan(&req.UpdatedAt)
	if err != nil {
		emailcommon.RespondInternal(w, "Failed to update rule")
		return
	}

	logAudit(r, pool, req.RuleID, "UPDATE_PENDING", req.UserID, req)
	emailcommon.RespondPayload(w, "transform-rules-update", req)
}

type deleteReq struct {
	RuleID         string `json:"rule_id"`
	UserID         string `json:"user_id"`
	CheckerComment string `json:"checker_comment"`
	Reason         string `json:"reason"` // alias used by UI (same as FD delete)
}

func handleDelete(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost {
		emailcommon.RespondMethodNotAllowed(w)
		return
	}

	var req deleteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		emailcommon.RespondBadRequest(w, "Invalid JSON body")
		return
	}
	if req.RuleID == "" {
		emailcommon.RespondBadRequest(w, "rule_id is required")
		return
	}
	comment := strings.TrimSpace(req.CheckerComment)
	if comment == "" {
		comment = strings.TrimSpace(req.Reason)
	}

	query := `
		UPDATE email_svc.transformation_rules 
		SET processing_status = 'PENDING_DELETE_APPROVAL',
		    submitted_by = $1,
		    checker_comment = $2,
		    updated_at = now()
		WHERE rule_id = $3
	`
	_, err := pool.Exec(r.Context(), query, req.UserID, comment, req.RuleID)
	if err != nil {
		emailcommon.RespondInternal(w, "Failed to delete rule")
		return
	}

	logAudit(r, pool, req.RuleID, "DELETE_PENDING", req.UserID, map[string]string{"reason": comment})
	emailcommon.RespondPayload(w, "transform-rules-delete", map[string]bool{"success": true})
}

type approveRejectReq struct {
	RuleID         string `json:"rule_id"`
	UserID         string `json:"user_id"`
	CheckerComment string `json:"checker_comment"`
}

func handleApprove(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost {
		emailcommon.RespondMethodNotAllowed(w)
		return
	}

	var req approveRejectReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		emailcommon.RespondBadRequest(w, "Invalid JSON body")
		return
	}
	if req.RuleID == "" {
		emailcommon.RespondBadRequest(w, "rule_id is required")
		return
	}

	// Fetch current rule to apply pending edits
	var pendingJSON json.RawMessage
	var isDeleted bool
	var processingStatus string
	err := pool.QueryRow(r.Context(), `SELECT pending_edit_json, is_deleted, processing_status FROM email_svc.transformation_rules WHERE rule_id = $1`, req.RuleID).Scan(&pendingJSON, &isDeleted, &processingStatus)
	if err != nil {
		emailcommon.RespondInternal(w, "Failed to fetch rule")
		return
	}

	if isDeleted || processingStatus == "PENDING_DELETE_APPROVAL" {
		// Permanently delete or mark deleted
		_, err = pool.Exec(r.Context(), `
			UPDATE email_svc.transformation_rules 
			SET processing_status = 'APPROVED', is_deleted = true, approved_by = $1, checker_comment = $2, deleted_at = now(), deleted_by = $1 
			WHERE rule_id = $3
		`, req.UserID, req.CheckerComment, req.RuleID)
	} else if pendingJSON != nil && len(pendingJSON) > 2 {
		// Apply pending edit
		var edits TransformationRule
		_ = json.Unmarshal(pendingJSON, &edits)
		editMode, _ := normalizeMatchMode(edits.ConditionType, edits.MatchMode)
		_, err = pool.Exec(r.Context(), `
			UPDATE email_svc.transformation_rules 
			SET rule_name = COALESCE(NULLIF($1, ''), rule_name),
			    condition_type = COALESCE(NULLIF($2, ''), condition_type),
			    condition_value = COALESCE(NULLIF($3, ''), condition_value),
			    match_mode = COALESCE(NULLIF($4, ''), match_mode),
			    mapping_id = COALESCE(NULLIF($5, ''), mapping_id),
			    mapping_name = COALESCE(NULLIF($6, ''), mapping_name),
			    action_type = COALESCE(NULLIF($7, ''), action_type),
			    is_active = $8,
			    processing_status = 'APPROVED',
			    approved_by = $9,
			    checker_comment = $10,
			    pending_edit_json = NULL,
			    updated_at = now()
			WHERE rule_id = $11
		`, edits.RuleName, edits.ConditionType, edits.ConditionValue, editMode, edits.MappingID, edits.MappingName, edits.ActionType, edits.IsActive, req.UserID, req.CheckerComment, req.RuleID)
	} else {
		// Just approve (like from initial create)
		_, err = pool.Exec(r.Context(), `
			UPDATE email_svc.transformation_rules 
			SET processing_status = 'APPROVED', approved_by = $1, checker_comment = $2, updated_at = now() 
			WHERE rule_id = $3
		`, req.UserID, req.CheckerComment, req.RuleID)
	}

	if err != nil {
		emailcommon.RespondInternal(w, "Failed to approve rule")
		return
	}

	logAudit(r, pool, req.RuleID, "APPROVED", req.UserID, req)
	emailcommon.RespondPayload(w, "transform-rules-approve", map[string]bool{"success": true})
}

func handleReject(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost {
		emailcommon.RespondMethodNotAllowed(w)
		return
	}

	var req approveRejectReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		emailcommon.RespondBadRequest(w, "Invalid JSON body")
		return
	}
	if req.RuleID == "" {
		emailcommon.RespondBadRequest(w, "rule_id is required")
		return
	}

	_, err := pool.Exec(r.Context(), `
		UPDATE email_svc.transformation_rules 
		SET processing_status = 'REJECTED',
		    approved_by = $1,
		    checker_comment = $2,
		    pending_edit_json = NULL,
		    is_deleted = false,
		    updated_at = now()
		WHERE rule_id = $3
	`, req.UserID, req.CheckerComment, req.RuleID)
	
	if err != nil {
		emailcommon.RespondInternal(w, "Failed to reject rule")
		return
	}

	logAudit(r, pool, req.RuleID, "REJECTED", req.UserID, req)
	emailcommon.RespondPayload(w, "transform-rules-reject", map[string]bool{"success": true})
}

