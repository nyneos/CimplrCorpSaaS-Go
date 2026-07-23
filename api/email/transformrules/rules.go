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

// Destination types for transformed output storage.
const (
	DestS3    = "S3"
	DestLocal = "LOCAL"
	DestSFTP  = "SFTP"
	DestAPI   = "API"
)

type TransformationRule struct {
	RuleID           string          `json:"rule_id,omitempty"`
	InboxID          string          `json:"inbox_id"`
	RuleName         string          `json:"rule_name"`
	ConditionType    string          `json:"condition_type"`
	ConditionValue   string          `json:"condition_value"`
	MatchMode        string          `json:"match_mode"` // PREFIX | SUFFIX | EXACT | CONTAINS | GLOB
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
	UserID           string          `json:"user_id,omitempty"`

	// Output destination + naming (1:1 on the rule master — no JSONB).
	DestinationType  string `json:"destination_type"`            // S3 | LOCAL | SFTP | API
	OutputNamePrefix string `json:"output_name_prefix"`          // user base name
	AppendDatetime   *bool  `json:"append_datetime"`             // nil → true; append _YYYYMMDD_HHMMSS
	S3Prefix         string `json:"s3_prefix,omitempty"`         // optional key prefix
	LocalFolder      string `json:"local_folder,omitempty"`      // subfolder under local base
	SftpHost         string `json:"sftp_host,omitempty"`
	SftpPort         int    `json:"sftp_port,omitempty"`
	SftpUser         string `json:"sftp_user,omitempty"`
	SftpPassword     string `json:"sftp_password"`
	SftpFolder       string `json:"sftp_folder,omitempty"`
	APIURL           string `json:"api_url,omitempty"`
	APIAuthToken     string `json:"api_auth_token"`
}

func appendDatetimeValue(v *bool) bool {
	if v == nil {
		return true
	}
	return *v
}

func boolPtr(v bool) *bool { return &v }

const ruleSelectCols = `
		r.rule_id, r.inbox_id, r.rule_name, r.condition_type, r.condition_value,
		COALESCE(NULLIF(r.match_mode, ''), 'CONTAINS'),
		r.mapping_id, r.mapping_name, r.action_type, r.is_active,
		r.processing_status, COALESCE(r.pending_edit_json, '{}'), r.submitted_by, r.approved_by, r.checker_comment, r.is_deleted,
		r.created_at, r.updated_at,
		COALESCE(NULLIF(r.destination_type, ''), 'S3'),
		COALESCE(r.output_name_prefix, ''),
		COALESCE(r.append_datetime, true),
		COALESCE(r.s3_prefix, ''),
		COALESCE(r.local_folder, ''),
		COALESCE(r.sftp_host, ''),
		COALESCE(r.sftp_port, 22),
		COALESCE(r.sftp_user, ''),
		COALESCE(r.sftp_password, ''),
		COALESCE(r.sftp_folder, ''),
		COALESCE(r.api_url, ''),
		COALESCE(r.api_auth_token, '')
`

func scanRule(rows interface {
	Scan(dest ...any) error
}, rule *TransformationRule) error {
	var submittedBy, approvedBy, checkerComment *string
	var appendDT bool
	err := rows.Scan(
		&rule.RuleID, &rule.InboxID, &rule.RuleName,
		&rule.ConditionType, &rule.ConditionValue, &rule.MatchMode,
		&rule.MappingID, &rule.MappingName, &rule.ActionType,
		&rule.IsActive, &rule.ProcessingStatus, &rule.PendingEditJSON,
		&submittedBy, &approvedBy, &checkerComment, &rule.IsDeleted,
		&rule.CreatedAt, &rule.UpdatedAt,
		&rule.DestinationType, &rule.OutputNamePrefix, &appendDT,
		&rule.S3Prefix, &rule.LocalFolder,
		&rule.SftpHost, &rule.SftpPort, &rule.SftpUser, &rule.SftpPassword, &rule.SftpFolder,
		&rule.APIURL, &rule.APIAuthToken,
	)
	if err != nil {
		return err
	}
	rule.AppendDatetime = boolPtr(appendDT)
	if submittedBy != nil {
		rule.SubmittedBy = *submittedBy
	}
	if approvedBy != nil {
		rule.ApprovedBy = *approvedBy
	}
	if checkerComment != nil {
		rule.CheckerComment = *checkerComment
	}
	return nil
}

// normalizeMatchMode returns a canonical match mode for the given condition type.
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

func normalizeDestination(req *TransformationRule) error {
	dt := strings.ToUpper(strings.TrimSpace(req.DestinationType))
	if dt == "" {
		dt = DestS3
	}
	switch dt {
	case DestS3, DestLocal, DestSFTP, DestAPI:
		req.DestinationType = dt
	default:
		return fmt.Errorf("invalid destination_type %q (use S3, LOCAL, SFTP, or API)", req.DestinationType)
	}

	req.OutputNamePrefix = strings.TrimSpace(req.OutputNamePrefix)
	req.S3Prefix = strings.Trim(strings.TrimSpace(req.S3Prefix), "/")
	req.LocalFolder = strings.Trim(strings.TrimSpace(req.LocalFolder), "/")
	req.SftpHost = strings.TrimSpace(req.SftpHost)
	req.SftpUser = strings.TrimSpace(req.SftpUser)
	req.SftpPassword = strings.TrimSpace(req.SftpPassword)
	req.SftpFolder = strings.Trim(strings.TrimSpace(req.SftpFolder), "/")
	req.APIURL = strings.TrimSpace(req.APIURL)
	req.APIAuthToken = strings.TrimSpace(req.APIAuthToken)

	if req.SftpPort <= 0 {
		req.SftpPort = 22
	}

	switch req.DestinationType {
	case DestSFTP:
		if req.SftpHost == "" || req.SftpUser == "" {
			return fmt.Errorf("sftp_host and sftp_user are required for SFTP destination")
		}
	case DestAPI:
		if req.APIURL == "" {
			return fmt.Errorf("api_url is required for API destination")
		}
		if !strings.HasPrefix(strings.ToLower(req.APIURL), "http://") &&
			!strings.HasPrefix(strings.ToLower(req.APIURL), "https://") {
			return fmt.Errorf("api_url must start with http:// or https://")
		}
	}
	return nil
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
		SELECT ` + ruleSelectCols + `
		FROM email_svc.transformation_rules r
		LEFT JOIN LATERAL (
			SELECT
				MAX(a.created_at) FILTER (
					WHERE a.audit_action IN ('CREATE_PENDING', 'UPDATE_PENDING', 'DELETE_PENDING')
				) AS requested_at,
				MAX(a.created_at) FILTER (
					WHERE a.audit_action IN ('APPROVED', 'REJECTED')
				) AS checker_at
			FROM email_svc.transformation_rules_audit a
			WHERE a.rule_id = r.rule_id
		) ra ON true
		WHERE ($1 = '' OR r.inbox_id::text = $1)
		  AND (r.is_deleted = false OR r.is_deleted IS NULL)
		ORDER BY GREATEST(
			COALESCE(ra.requested_at, '-infinity'::timestamptz),
			COALESCE(ra.checker_at, '-infinity'::timestamptz),
			COALESCE(r.updated_at, r.created_at, '-infinity'::timestamptz)
		) DESC
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
		if err := scanRule(rows, &rule); err != nil {
			emailcommon.RespondInternal(w, "Failed to parse rules")
			return
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
	if destErr := normalizeDestination(&req); destErr != nil {
		emailcommon.RespondBadRequest(w, destErr.Error())
		return
	}
	appendDT := appendDatetimeValue(req.AppendDatetime)
	req.AppendDatetime = boolPtr(appendDT)
	req.ProcessingStatus = "PENDING_APPROVAL"

	query := `
		INSERT INTO email_svc.transformation_rules
		(inbox_id, rule_name, condition_type, condition_value, match_mode, mapping_id, mapping_name, action_type, is_active, processing_status, submitted_by,
		 destination_type, output_name_prefix, append_datetime, s3_prefix, local_folder,
		 sftp_host, sftp_port, sftp_user, sftp_password, sftp_folder, api_url, api_auth_token)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
		        $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
		RETURNING rule_id, created_at, updated_at
	`
	err := pool.QueryRow(r.Context(), query,
		req.InboxID, req.RuleName, req.ConditionType, req.ConditionValue, req.MatchMode,
		req.MappingID, req.MappingName, req.ActionType, req.IsActive, req.ProcessingStatus, req.UserID,
		req.DestinationType, req.OutputNamePrefix, appendDT, req.S3Prefix, req.LocalFolder,
		req.SftpHost, req.SftpPort, req.SftpUser, req.SftpPassword, req.SftpFolder, req.APIURL, req.APIAuthToken,
	).Scan(&req.RuleID, &req.CreatedAt, &req.UpdatedAt)

	if err != nil {
		if strings.Contains(err.Error(), "unique_active_rule") {
			emailcommon.RespondBadRequest(w, "An active rule with this condition and mapping already exists for this inbox.")
		} else {
			emailcommon.RespondInternal(w, "Failed to create rule: "+err.Error())
		}
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

	var currentStatus string
	if err := pool.QueryRow(r.Context(), `
		SELECT COALESCE(processing_status, '')
		FROM email_svc.transformation_rules
		WHERE rule_id = $1 AND (is_deleted = false OR is_deleted IS NULL)
	`, req.RuleID).Scan(&currentStatus); err != nil {
		emailcommon.RespondBadRequest(w, "rule not found")
		return
	}
	statusNorm := strings.ToUpper(strings.TrimSpace(currentStatus))
	switch statusNorm {
	case "PENDING_APPROVAL", "PENDING_EDIT_APPROVAL", "REJECTED":
	default:
		emailcommon.RespondBadRequest(w, "cannot edit rule in status "+statusNorm)
		return
	}

	mode, modeErr := normalizeMatchMode(req.ConditionType, req.MatchMode)
	if modeErr != nil {
		emailcommon.RespondBadRequest(w, modeErr.Error())
		return
	}
	req.MatchMode = mode
	if destErr := normalizeDestination(&req); destErr != nil {
		emailcommon.RespondBadRequest(w, destErr.Error())
		return
	}

	pendingJSON, _ := json.Marshal(req)

	query := `
		UPDATE email_svc.transformation_rules
		SET processing_status = 'PENDING_EDIT_APPROVAL',
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
	req.ProcessingStatus = "PENDING_EDIT_APPROVAL"
	emailcommon.RespondPayload(w, "transform-rules-update", req)
}

type deleteReq struct {
	RuleID         string `json:"rule_id"`
	UserID         string `json:"user_id"`
	CheckerComment string `json:"checker_comment"`
	Reason         string `json:"reason"`
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

	var pendingJSON json.RawMessage
	var isDeleted bool
	var processingStatus string
	err := pool.QueryRow(r.Context(), `SELECT pending_edit_json, is_deleted, processing_status FROM email_svc.transformation_rules WHERE rule_id = $1`, req.RuleID).Scan(&pendingJSON, &isDeleted, &processingStatus)
	if err != nil {
		emailcommon.RespondInternal(w, "Failed to fetch rule")
		return
	}

	if isDeleted || processingStatus == "PENDING_DELETE_APPROVAL" {
		_, err = pool.Exec(r.Context(), `
			UPDATE email_svc.transformation_rules
			SET processing_status = 'APPROVED', is_deleted = true, approved_by = $1, checker_comment = $2, deleted_at = now(), deleted_by = $1
			WHERE rule_id = $3
		`, req.UserID, req.CheckerComment, req.RuleID)
	} else if pendingJSON != nil && len(pendingJSON) > 2 {
		var edits TransformationRule
		_ = json.Unmarshal(pendingJSON, &edits)
		editMode, _ := normalizeMatchMode(edits.ConditionType, edits.MatchMode)
		_ = normalizeDestination(&edits)
		editAppendDT := appendDatetimeValue(edits.AppendDatetime)
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
			    destination_type = COALESCE(NULLIF($9, ''), destination_type),
			    output_name_prefix = $10,
			    append_datetime = $11,
			    s3_prefix = $12,
			    local_folder = $13,
			    sftp_host = $14,
			    sftp_port = $15,
			    sftp_user = $16,
			    sftp_password = CASE WHEN $17 <> '' THEN $17 ELSE sftp_password END,
			    sftp_folder = $18,
			    api_url = $19,
			    api_auth_token = CASE WHEN $20 <> '' THEN $20 ELSE api_auth_token END,
			    processing_status = 'APPROVED',
			    approved_by = $21,
			    checker_comment = $22,
			    pending_edit_json = NULL,
			    updated_at = now()
			WHERE rule_id = $23
		`, edits.RuleName, edits.ConditionType, edits.ConditionValue, editMode, edits.MappingID, edits.MappingName, edits.ActionType, edits.IsActive,
			edits.DestinationType, edits.OutputNamePrefix, editAppendDT, edits.S3Prefix, edits.LocalFolder,
			edits.SftpHost, edits.SftpPort, edits.SftpUser, edits.SftpPassword, edits.SftpFolder,
			edits.APIURL, edits.APIAuthToken,
			req.UserID, req.CheckerComment, req.RuleID)
	} else {
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

	var submittedBy string
	_ = pool.QueryRow(r.Context(), `
		SELECT COALESCE(submitted_by, '') FROM email_svc.transformation_rules WHERE rule_id = $1
	`, req.RuleID).Scan(&submittedBy)
	logAudit(r, pool, req.RuleID, "APPROVED", req.UserID, map[string]interface{}{
		"rule_id":            req.RuleID,
		"submitted_by":       submittedBy,
		"approved_by":        req.UserID,
		"checker_comment":    req.CheckerComment,
		"processing_status":  "APPROVED",
	})
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

	var submittedBy string
	_ = pool.QueryRow(r.Context(), `
		SELECT COALESCE(submitted_by, '') FROM email_svc.transformation_rules WHERE rule_id = $1
	`, req.RuleID).Scan(&submittedBy)
	logAudit(r, pool, req.RuleID, "REJECTED", req.UserID, map[string]interface{}{
		"rule_id":           req.RuleID,
		"submitted_by":      submittedBy,
		"rejected_by":       req.UserID,
		"checker_comment":   req.CheckerComment,
		"processing_status": "REJECTED",
	})
	emailcommon.RespondPayload(w, "transform-rules-reject", map[string]bool{"success": true})
}

func sanitizeTransformRuleAuditState(raw map[string]interface{}) map[string]interface{} {
	if raw == nil {
		return map[string]interface{}{}
	}
	out := make(map[string]interface{}, len(raw))
	for k, v := range raw {
		switch strings.ToLower(k) {
		case "sftp_password", "api_auth_token":
			continue
		default:
			out[k] = v
		}
	}
	return out
}

func handleAuditLog(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if r.Method != http.MethodPost {
		emailcommon.RespondMethodNotAllowed(w)
		return
	}

	var req struct {
		RuleID string `json:"rule_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		emailcommon.RespondBadRequest(w, "Invalid JSON body")
		return
	}
	ruleID := strings.TrimSpace(req.RuleID)
	if ruleID == "" {
		emailcommon.RespondBadRequest(w, "rule_id is required")
		return
	}

	rows, err := pool.Query(r.Context(), `
		SELECT rule_id::text, audit_action, COALESCE(actor_id, ''), new_state_json, created_at
		FROM email_svc.transformation_rules_audit
		WHERE rule_id = $1::uuid
		ORDER BY created_at DESC
	`, ruleID)
	if err != nil {
		emailcommon.RespondInternal(w, "Failed to load transform rule audit log")
		return
	}
	defer rows.Close()

	type auditRow struct {
		AuditID     string                 `json:"audit_id"`
		RuleID      string                 `json:"rule_id"`
		AuditAction string                 `json:"audit_action"`
		ActorID     string                 `json:"actor_id"`
		NewState    map[string]interface{} `json:"new_state_json"`
		CreatedAt   string                 `json:"created_at"`
	}
	var items []auditRow
	for rows.Next() {
		var row auditRow
		var rawState []byte
		var createdAt time.Time
		if err := rows.Scan(&row.RuleID, &row.AuditAction, &row.ActorID, &rawState, &createdAt); err != nil {
			emailcommon.RespondInternal(w, "Failed to parse transform rule audit log")
			return
		}
		row.AuditID = fmt.Sprintf("%s-%d", row.RuleID, createdAt.UnixNano())
		row.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
		row.NewState = map[string]interface{}{}
		if len(rawState) > 0 {
			_ = json.Unmarshal(rawState, &row.NewState)
		}
		row.NewState = sanitizeTransformRuleAuditState(row.NewState)
		items = append(items, row)
	}
	if items == nil {
		items = []auditRow{}
	}

	emailcommon.RespondList(w, "transform-rules/audit-log", items, len(items))
}
