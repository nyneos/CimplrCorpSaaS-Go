package emailworkflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"
	emailmailbox "CimplrCorpSaas/api/email/mailbox"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	emailInboxModuleCode  = "EMAIL_INBOX"
	emailInboxRecordTable = "email_svc.inbox_config"
	emailInboxAuditTable  = "email_svc.inbox_audit"
)

type workflowInbox struct {
	InboxID          string          `json:"inbox_id"`
	MailboxAddress   string          `json:"mailbox_address"`
	DisplayName      string          `json:"display_name"`
	Domain           string          `json:"domain"`
	FiltersJSON      json.RawMessage `json:"filters_json"`
	PollIntervalSecs int             `json:"poll_interval_secs"`
	Module           string          `json:"module"`
	EntityID         string          `json:"entity_id"`
	ProcessingStatus string          `json:"processing_status"`
	SESSyncStatus    string          `json:"ses_sync_status"`
	SESSyncedAt      *time.Time      `json:"ses_synced_at"`
	SESLastError     string          `json:"ses_last_error"`
	SubmittedBy      string          `json:"submitted_by"`
	ApprovedBy       string          `json:"approved_by"`
	SourceType       string          `json:"source_type"`
	emailmailbox.MailboxGraphFields
	emailmailbox.MailboxIMAPFields
	emailmailbox.MailboxOAuthFields
	emailmailbox.MailboxGoogleWorkspaceFields
	GraphLastSyncAt     *time.Time      `json:"graph_last_sync_at"`
	GraphSentLastSyncAt *time.Time      `json:"graph_sent_last_sync_at"`
	CheckerComment      string          `json:"checker_comment"`
	IsActive            bool            `json:"is_active"`
	IsDeleted           bool            `json:"is_deleted"`
	GraphSecretSet      bool            `json:"graph_client_secret_set,omitempty"`
	GooglePrivateKeySet bool            `json:"google_workspace_private_key_set,omitempty"`
	IMAPPasswordSet     bool            `json:"imap_password_set,omitempty"`
	SharedUserIDs       []string        `json:"shared_user_ids,omitempty"`
	PendingEditJSON     json.RawMessage `json:"pending_edit_json,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
	// Audit metadata resolved per action type from inbox_audit (CREATE →
	// requested_*, EDIT → edited_*, DELETE → deleted_*), so a mailbox that was
	// never edited reports an empty edited_at instead of its creation time.
	RequestedAt *time.Time `json:"requested_at,omitempty"`
	RequestedBy string     `json:"requested_by,omitempty"`
	EditedAt    *time.Time `json:"edited_at,omitempty"`
	EditedBy    string     `json:"edited_by,omitempty"`
	DeletedAt   *time.Time `json:"deleted_at,omitempty"`
	DeletedBy   string     `json:"deleted_by,omitempty"`
	CheckerAt   *time.Time `json:"checker_at,omitempty"`
	CheckerBy   string     `json:"checker_by,omitempty"`
}

func init() {
	approvalengine.RegisterPostFinalizeHook("EMAIL_INBOX_CREATE", finalizeEmailInboxApproval)
	approvalengine.RegisterPostFinalizeHook("EMAIL_INBOX_EDIT", finalizeEmailInboxApproval)
	approvalengine.RegisterPostFinalizeHook("EMAIL_INBOX_DELETE", finalizeEmailInboxApproval)
}

func sesRuleNameForInbox(inboxID string) string {
	short := strings.ReplaceAll(inboxID, "-", "")
	if len(short) > 12 {
		short = short[:12]
	}
	return "cimplr-inbox-" + short
}

func HandleWorkflowMeta(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		domains := emailmailbox.AllowedMailboxDomains()
		defaultDomain := ""
		if len(domains) > 0 {
			defaultDomain = domains[0]
		}
		graphLabel := strings.TrimSpace(os.Getenv("AZURE_GRAPH_TENANT_LABEL"))
		googleLabel := strings.TrimSpace(os.Getenv("GOOGLE_WORKSPACE_TENANT_LABEL"))
		if row, err := emailjobs.LoadDefaultGraphTenant(r.Context(), pool); err == nil {
			if graphLabel == "" {
				graphLabel = strings.TrimSpace(row.TenantLabel)
			}
		}
		if row, err := emailjobs.LoadDefaultGoogleWorkspaceTenant(r.Context(), pool); err == nil {
			if googleLabel == "" {
				googleLabel = strings.TrimSpace(row.TenantLabel)
			}
		}
		if graphLabel == "" {
			graphLabel = "default"
		}
		if googleLabel == "" {
			googleLabel = "default"
		}
		emailcommon.RespondPayload(w, "inbox/workflow/meta", map[string]interface{}{
			"allowed_domains":                       domains,
			"default_mailbox_domain":                defaultDomain,
			"default_graph_tenant_label":            graphLabel,
			"default_google_workspace_tenant_label": googleLabel,
		})
	}
}

// inboxAuditEntry groups the fields logged by logInboxAudit, keeping the
// function signature under the project's parameter-count limit.
type inboxAuditEntry struct {
	InboxID string
	Action  string
	Status  string
	UserID  string
	Comment string
	Detail  map[string]interface{}
}

func logInboxAudit(ctx context.Context, pool *pgxpool.Pool, entry inboxAuditEntry) {
	detail := entry.Detail
	if detail == nil {
		detail = map[string]interface{}{}
	}
	b, _ := json.Marshal(detail)
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.inbox_audit (inbox_id, action_type, status, processing_status, performed_by, comment, detail)
		VALUES ($1::uuid, $2, $3, $3, NULLIF($4,''), NULLIF($5,''), $6::jsonb)
	`, entry.InboxID, entry.Action, entry.Status, entry.UserID, entry.Comment, string(b))
}

// inboxApprovalRequest groups the fields submitted to the approval engine by
// submitInboxApproval, keeping the function signature under the project's
// parameter-count limit.
type inboxApprovalRequest struct {
	InboxID    string
	EntityID   string
	TxType     string
	ActionType string
	UserID     string
	UserEmail  string
	MatrixID   string
}

func inboxEnforceMatrix(ctx context.Context, r *http.Request, pool *pgxpool.Pool, event, handler, path, entity, actor string) (bool, string, string) {
	return runtime.EnforceInlineWithMatrix(ctx, r, pool, runtime.EnforceInput{
		EventCode:        event,
		ModuleCode:       common.ModuleEmail,
		SubModule:        "EMAIL_INBOX",
		EntityCode:       entity,
		ActorUserID:      actor,
		HandlerName:      handler,
		APIPath:          path,
		RequireVariables: false,
	})
}

func submitInboxApproval(ctx context.Context, pool *pgxpool.Pool, req inboxApprovalRequest) (string, error) {
	userEmail := req.UserEmail
	if userEmail == "" {
		userEmail = req.UserID
	}
	if err := approvalengine.CancelPendingInstances(ctx, pool, emailInboxModuleCode, req.InboxID, userEmail); err != nil {
		return "", err
	}
	return approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
		ModuleCode:       emailInboxModuleCode,
		EntityCode:       req.EntityID,
		TransactionType:  req.TxType,
		RecordID:         req.InboxID,
		RecordTable:      emailInboxRecordTable,
		AuditTable:       emailInboxAuditTable,
		AuditIDColumn:    "inbox_id",
		ActionType:       req.ActionType,
		Amount:           0,
		SubmittedBy:      req.UserID,
		SubmittedByEmail: userEmail,
		MatrixID:         req.MatrixID,
	})
}

func finalizeEmailInboxApproval(ctx context.Context, pool *pgxpool.Pool, recordID, transactionType, finalStatus, actorEmail, comment string) {
	_ = applyInboxDecision(ctx, pool, recordID, transactionType, finalStatus, actorEmail, comment)
	// SES sync is for create/edit approvals only — delete must not re-sync rules.
	if finalStatus == approvalengine.InstStatusApproved && transactionType != "EMAIL_INBOX_DELETE" {
		result, err := SyncApprovedInboxesToSES(ctx, pool)
		ApplyInboundRuleSyncResult(ctx, pool, result, err)
	}
}

func applyInboxDecision(ctx context.Context, pool *pgxpool.Pool, inboxID, transactionType, finalStatus, actorID, comment string) error {
	if finalStatus == approvalengine.InstStatusRejected {
		switch transactionType {
		case "EMAIL_INBOX_CREATE", "EMAIL_INBOX_EDIT", "EMAIL_INBOX_DELETE":
			// Reject create, edit, or delete → REJECTED (never restored to the prior
			// status, never soft-deleted). The row is kept (is_deleted stays false) so
			// it stays visible.
			_, err := pool.Exec(ctx, `
				UPDATE email_svc.inbox_config
				SET processing_status = $2,
				    pending_edit_json = NULL,
				    checker_comment = $3,
				    is_active = false,
				    is_deleted = false,
				    updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, constants.StatusRejected, comment)
			return err
		}
		return nil
	}

	switch transactionType {
	case "EMAIL_INBOX_CREATE":
		_, err := pool.Exec(ctx, `
			UPDATE email_svc.inbox_config
			SET processing_status = $2, approved_by = $3, checker_comment = $4,
			    is_active = true, updated_at = now()
			WHERE inbox_id = $1::uuid
		`, inboxID, constants.StatusApproved, actorID, comment)
		return err
	case "EMAIL_INBOX_EDIT":
		var pendingEdit string
		if err := pool.QueryRow(ctx, `
			SELECT COALESCE(pending_edit_json::text,'') FROM email_svc.inbox_config WHERE inbox_id = $1::uuid
		`, inboxID).Scan(&pendingEdit); err != nil {
			return err
		}
		if pendingEdit == "" || pendingEdit == "null" {
			_, err := pool.Exec(ctx, `
				UPDATE email_svc.inbox_config
				SET processing_status = $2, pending_edit_json = NULL,
				    approved_by = $3, checker_comment = $4,
				    is_active = true, updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, constants.StatusApproved, actorID, comment)
			return err
		}
		var edit workflowInbox
		if err := json.Unmarshal([]byte(pendingEdit), &edit); err != nil {
			return err
		}
		mailbox, existingIMAP, _ := emailmailbox.LoadMailboxIMAPFields(ctx, pool, inboxID)
		existingGraph, _ := emailmailbox.LoadMailboxGraphFields(ctx, pool, inboxID)
		existingGoogle, _ := emailmailbox.LoadMailboxGoogleWorkspaceFields(ctx, pool, inboxID)
		imapFinal := existingIMAP
		graphFinal := existingGraph
		googleFinal := existingGoogle
		if strings.EqualFold(edit.SourceType, "IMAP") && emailmailbox.IMAPPatchPresent(edit.MailboxIMAPFields) {
			if merged, mergeErr := emailmailbox.MergeIMAPFields(existingIMAP, edit.MailboxIMAPFields, mailbox); mergeErr == nil {
				imapFinal = merged
			}
		}
		if emailmailbox.GraphPatchPresent(edit.MailboxGraphFields) {
			if merged, mergeErr := emailmailbox.MergeGraphFields(existingGraph, edit.MailboxGraphFields); mergeErr == nil {
				graphFinal = merged
			}
		}
		if emailmailbox.GoogleWorkspacePatchPresent(edit.MailboxGoogleWorkspaceFields) {
			if merged, mergeErr := emailmailbox.MergeGoogleWorkspaceFields(existingGoogle, edit.MailboxGoogleWorkspaceFields); mergeErr == nil {
				googleFinal = merged
			}
		}
		_, err := pool.Exec(ctx, `
			UPDATE email_svc.inbox_config
			SET display_name = COALESCE(NULLIF($2,''), display_name),
			    filters_json = COALESCE($3::jsonb, filters_json),
			    poll_interval_secs = CASE WHEN $4 > 0 THEN $4 ELSE poll_interval_secs END,
			    module = COALESCE(NULLIF($5,''), module),
			    graph_tenant_key = $6,
			    graph_tenant_label = $7, graph_tenant_id = $8,
			    graph_client_id = $9, graph_client_secret = $10,
			    google_workspace_tenant_key = $11,
			    google_workspace_tenant_label = $12,
			    google_workspace_service_account_email = $13,
			    google_workspace_client_id = $14,
			    google_workspace_private_key = $15,
			    imap_provider = $16, imap_host = $17, imap_port = $18,
			    imap_username = $19, imap_password = $20,
			    imap_inbox_folder = $21, imap_sent_folder = $22, imap_use_tls = $23,
			    processing_status = $24,
			    pending_edit_json = NULL,
			    approved_by = $25,
			    checker_comment = $26,
			    is_active = true,
			    updated_at = now()
			WHERE inbox_id = $1::uuid
		`, inboxID, edit.DisplayName, emailcommon.NullableJSON(edit.FiltersJSON), edit.PollIntervalSecs,
			edit.Module,
			graphFinal.TenantKey, graphFinal.TenantLabel, graphFinal.TenantID, graphFinal.ClientID, graphFinal.ClientSecret,
			googleFinal.WorkspaceTenantKey, googleFinal.WorkspaceTenantLabel, googleFinal.WorkspaceServiceAccountEmail,
			googleFinal.WorkspaceClientID, googleFinal.WorkspacePrivateKey,
			imapFinal.Provider, imapFinal.Host, imapFinal.Port,
			imapFinal.Username, imapFinal.Password,
			imapFinal.InboxFolder, imapFinal.SentFolder, imapFinal.UseTLS,
			constants.StatusApproved, actorID, comment)
		return err
	case "EMAIL_INBOX_DELETE":
		return applyApprovedInboxDelete(ctx, pool, inboxID, actorID, comment)
	}
	return nil
}

// applyApprovedInboxDelete soft-deletes the mailbox (row kept for audit), removes
// its messages, and drops any SES inbound rule. Reject of a delete request must
// NOT call this — reject sets REJECTED without touching is_deleted.
func applyApprovedInboxDelete(ctx context.Context, pool *pgxpool.Pool, inboxID, actorID, comment string) error {
	var ruleName string
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(ses_rule_name,'') FROM email_svc.inbox_config WHERE inbox_id = $1::uuid
	`, inboxID).Scan(&ruleName)

	if err := emailcommon.DeleteMessagesForInbox(ctx, pool, inboxID); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET processing_status = 'DELETED', is_deleted = true, is_active = false,
		    deleted_at = now(), deleted_by = $2, approved_by = $2,
		    pending_edit_json = NULL,
		    checker_comment = $3, updated_at = now()
		WHERE inbox_id = $1::uuid
		  AND is_deleted = false
	`, inboxID, actorID, comment)
	if err != nil {
		return err
	}
	if strings.TrimSpace(ruleName) != "" {
		client := mailruntime.NewRuntime()
		if client.Ready() {
			ruleSet := strings.TrimSpace(os.Getenv("EMAIL_SES_RULE_SET_NAME"))
			_ = client.RemoveInboundRule(ctx, ruleSet, ruleName)
		}
	}
	return nil
}

func SyncApprovedInboxesToSES(ctx context.Context, pool *pgxpool.Pool) (*mailruntime.InboundRuleSyncResult, error) {
	client := mailruntime.NewRuntime()
	if !client.Ready() {
		return nil, fmt.Errorf("mail processing not configured")
	}

	rows, err := pool.Query(ctx, `
		SELECT inbox_id::text, mailbox_address, COALESCE(ses_rule_name,'')
		FROM email_svc.inbox_config
		WHERE processing_status = 'APPROVED'
		  AND is_deleted = false
		  AND is_active = true
		  AND COALESCE(source_type, 'OUTLOOK_GRAPH') = 'SES'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []mailruntime.InboundRuleSpec
	for rows.Next() {
		var id, addr, ruleName string
		if err := rows.Scan(&id, &addr, &ruleName); err != nil {
			return nil, err
		}
		if ruleName == "" {
			ruleName = sesRuleNameForInbox(id)
		}
		rules = append(rules, mailruntime.InboundRuleSpec{RuleName: ruleName, Recipient: strings.ToLower(addr)})
	}

	ruleSet := strings.TrimSpace(os.Getenv("EMAIL_SES_RULE_SET_NAME"))
	bucket := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_BUCKET"))
	prefix := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_PREFIX"))

	return client.ApplyInboundRules(ctx, ruleSet, bucket, prefix, rules)
}

func ApplyInboundRuleSyncResult(ctx context.Context, pool *pgxpool.Pool, result *mailruntime.InboundRuleSyncResult, syncErr error) {
	status := "SYNCED"
	errMsg := ""
	if syncErr != nil {
		status = "FAILED"
		errMsg = syncErr.Error()
	} else if result != nil && len(result.Errors) > 0 {
		status = "PARTIAL"
		errMsg = strings.Join(result.Errors, "; ")
	}
	_, _ = pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET ses_sync_status = $1,
		    ses_synced_at = CASE WHEN $1 = 'SYNCED' THEN now() ELSE ses_synced_at END,
		    ses_last_error = NULLIF($2,''),
		    updated_at = now()
		WHERE processing_status = 'APPROVED'
		  AND is_deleted = false
		  AND COALESCE(source_type, 'OUTLOOK_GRAPH') = 'SES'
	`, status, errMsg)
}

func userCanAccessInbox(ctx context.Context, pool *pgxpool.Pool, inboxID, userID, userEmail string, entityIDs []string, admin bool) bool {
	if admin {
		return true
	}
	var owner, ent, mailbox string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(owner_user_id,''), COALESCE(entity_id,''), COALESCE(mailbox_address,'')
		FROM email_svc.inbox_config WHERE inbox_id = $1::uuid
	`, inboxID).Scan(&owner, &ent, &mailbox)
	if err != nil {
		return false
	}
	if userEmail != "" && strings.EqualFold(strings.TrimSpace(mailbox), strings.TrimSpace(userEmail)) {
		return true
	}
	if ent != "" && emailcommon.EntityInScope(ent, entityIDs) {
		return true
	}
	if owner == userID {
		return true
	}
	var member bool
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.inbox_members WHERE inbox_id = $1::uuid AND user_id = $2)
	`, inboxID, userID).Scan(&member)
	return member
}

func HandleWorkflowInboxList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
			Status   string `json:"status"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		userID, userEmail, entityID, entityIDs := emailcommon.RequestIdentity(r, req.UserID, req.EntityID)
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)

		emailjobs.SyncEmailServiceStatus(r.Context(), pool)

		query := fmt.Sprintf(`
			SELECT i.inbox_id::text, i.mailbox_address, COALESCE(i.display_name,''),
			       COALESCE(i.domain,''), i.filters_json, i.poll_interval_secs,
			       COALESCE(i.module,''), COALESCE(i.entity_id,''),
			       i.processing_status, COALESCE(i.ses_sync_status,''),
			       i.ses_synced_at, COALESCE(i.ses_last_error,''),
			       COALESCE(i.submitted_by,''), COALESCE(i.approved_by,''),
			       COALESCE(i.source_type,'OUTLOOK_GRAPH'),
			       %s, %s, %s, %s, %s,
			       %s, %s, %s, %s, %s,
			       %s, %s, %s,
			       %s, %s,
			       %s, %s,
			       %s,
			       COALESCE(i.oauth_provider,''), COALESCE(i.oauth_mail_transport,'api'),
			       (COALESCE(i.oauth_refresh_token,'') <> ''),
			       i.graph_last_sync_at, i.graph_sent_last_sync_at,
			       COALESCE(i.checker_comment,''), i.is_active, i.is_deleted,
			       COALESCE(i.pending_edit_json::text, 'null'),
			       i.created_at, i.updated_at,
			       COALESCE(ia.requested_at, i.created_at), COALESCE(ia.requested_by, ''),
			       ia.edited_at, COALESCE(ia.edited_by, ''),
			       ia.deleted_at, COALESCE(ia.deleted_by, ''),
			       ia.checker_at, COALESCE(ia.checker_by, '')
			FROM email_svc.inbox_config i
			LEFT JOIN LATERAL (
				SELECT
					MAX(a.created_at) FILTER (
						WHERE a.action_type IN ('CREATE', 'EDIT', 'DELETE')
					) AS requested_at,
					(array_agg(a.performed_by ORDER BY a.created_at DESC)
						FILTER (WHERE a.action_type IN ('CREATE', 'EDIT', 'DELETE')))[1] AS requested_by,
					MAX(a.created_at) FILTER (WHERE a.action_type = 'EDIT') AS edited_at,
					(array_agg(a.performed_by ORDER BY a.created_at DESC)
						FILTER (WHERE a.action_type = 'EDIT'))[1] AS edited_by,
					MAX(a.created_at) FILTER (WHERE a.action_type = 'DELETE') AS deleted_at,
					(array_agg(a.performed_by ORDER BY a.created_at DESC)
						FILTER (WHERE a.action_type = 'DELETE'))[1] AS deleted_by,
					MAX(COALESCE(a.checker_at, a.created_at)) FILTER (
						WHERE a.action_type LIKE 'APPROVE%%' OR a.action_type = 'REJECT'
					) AS checker_at,
					(array_agg(COALESCE(a.checker_by, a.performed_by)
						ORDER BY COALESCE(a.checker_at, a.created_at) DESC)
						FILTER (WHERE a.action_type LIKE 'APPROVE%%' OR a.action_type = 'REJECT'))[1] AS checker_by,
					MAX(a.created_at) AS last_activity_at
				FROM email_svc.inbox_audit a
				WHERE a.inbox_id = i.inbox_id
			) ia ON true
			WHERE i.is_deleted = false
		`, emailjobs.SQLCoalesceGraphTenantKey, emailjobs.SQLCoalesceGraphTenantLabel, emailjobs.SQLCoalesceGraphTenantID,
			emailjobs.SQLCoalesceGraphClientID, emailjobs.SQLCoalesceGraphSecret,
			emailjobs.SQLCoalesceGoogleWorkspaceTenantKey, emailjobs.SQLCoalesceGoogleWorkspaceTenantLabel,
			emailjobs.SQLCoalesceGoogleWorkspaceServiceAccountEmail, emailjobs.SQLCoalesceGoogleWorkspaceClientID,
			emailjobs.SQLCoalesceGoogleWorkspacePrivateKey,
			emailjobs.SQLCoalesceIMAPProvider, emailjobs.SQLCoalesceIMAPHost, emailjobs.SQLCoalesceIMAPPort,
			emailjobs.SQLCoalesceIMAPUsername, emailjobs.SQLCoalesceIMAPPassword,
			emailjobs.SQLCoalesceIMAPInboxFolder, emailjobs.SQLCoalesceIMAPSentFolder,
			emailjobs.SQLCoalesceIMAPUseTLS)
		args := []interface{}{}
		argN := 1
		if !admin {
			query += fmt.Sprintf(` AND (
				i.owner_user_id = $%d
				OR ($%d <> '' AND LOWER(i.mailbox_address) = LOWER($%d))
				OR EXISTS (SELECT 1 FROM email_svc.inbox_members m WHERE m.inbox_id = i.inbox_id AND m.user_id = $%d)
				OR (
					i.processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL', 'PENDING_DELETE_APPROVAL')
					AND i.entity_id = ANY($%d::text[])
				)
			)`, argN, argN+1, argN+1, argN, argN+2)
			if len(entityIDs) == 0 && entityID != "" {
				entityIDs = []string{entityID}
			}
			args = append(args, userID, userEmail, entityIDs)
			argN += 3
		}
		if s := strings.TrimSpace(req.Status); s != "" {
			query += fmt.Sprintf(" AND i.processing_status = $%d", argN)
			args = append(args, s)
		}
		query += ` ORDER BY GREATEST(
			COALESCE(ia.last_activity_at, '-infinity'::timestamptz),
			COALESCE(i.updated_at, i.created_at, '-infinity'::timestamptz)
		) DESC`

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		defer rows.Close()

		items := make([]workflowInbox, 0)
		for rows.Next() {
			var item workflowInbox
			var pendingText string
			var graphSecret string
			var googlePrivateKey string
			var imapPassword string
			var oauthProvider string
			var oauthMailTransport string
			var oauthConnected bool
			if err := rows.Scan(
				&item.InboxID, &item.MailboxAddress, &item.DisplayName, &item.Domain,
				&item.FiltersJSON, &item.PollIntervalSecs, &item.Module, &item.EntityID,
				&item.ProcessingStatus, &item.SESSyncStatus, &item.SESSyncedAt, &item.SESLastError,
				&item.SubmittedBy, &item.ApprovedBy, &item.SourceType,
				&item.TenantKey, &item.TenantLabel, &item.TenantID, &item.ClientID, &graphSecret,
				&item.MailboxGoogleWorkspaceFields.WorkspaceTenantKey, &item.MailboxGoogleWorkspaceFields.WorkspaceTenantLabel,
				&item.MailboxGoogleWorkspaceFields.WorkspaceServiceAccountEmail, &item.MailboxGoogleWorkspaceFields.WorkspaceClientID, &googlePrivateKey,
				&item.Provider, &item.Host, &item.Port,
				&item.Username, &imapPassword, &item.InboxFolder,
				&item.SentFolder, &item.UseTLS,
				&oauthProvider, &oauthMailTransport, &oauthConnected,
				&item.GraphLastSyncAt, &item.GraphSentLastSyncAt,
				&item.CheckerComment,
				&item.IsActive, &item.IsDeleted, &pendingText,
				&item.CreatedAt, &item.UpdatedAt,
				&item.RequestedAt, &item.RequestedBy,
				&item.EditedAt, &item.EditedBy,
				&item.DeletedAt, &item.DeletedBy,
				&item.CheckerAt, &item.CheckerBy,
			); err != nil {
				emailcommon.RespondInternal(w, err.Error())
				return
			}
			if pendingText != "" && pendingText != "null" {
				item.PendingEditJSON = json.RawMessage(pendingText)
			}
			if graphSecret != "" {
				item.ClientSecret = graphSecret
				item.GraphSecretSet = true
			}
			if googlePrivateKey != "" {
				item.MailboxGoogleWorkspaceFields.WorkspacePrivateKey = googlePrivateKey
				item.GooglePrivateKeySet = true
			}
			if imapPassword != "" {
				item.Password = imapPassword
				item.IMAPPasswordSet = true
			}
			item.OAuthProvider = oauthProvider
			item.OAuthMailTransport = oauthMailTransport
			item.OAuthConnected = oauthConnected
			items = append(items, item)
		}
		emailcommon.RespondList(w, "inbox/workflow/list", items, len(items))
	}
}

func HandleWorkflowInboxCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID        string          `json:"user_id"`
			EntityID      string          `json:"entity_id"`
			Items         []workflowInbox `json:"items"`
			SharedUserIDs []string        `json:"shared_user_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		if len(req.Items) == 0 {
			emailcommon.RespondBadRequest(w, "items is required")
			return
		}
		userID, userEmail, defaultEntityID, _ := emailcommon.RequestIdentity(r, req.UserID, req.EntityID)
		if userID == "" {
			emailcommon.RespondUnauthorized(w, "user context missing")
			return
		}

		var created []string
		var errors []string
		for _, item := range req.Items {
			addr := strings.ToLower(strings.TrimSpace(item.MailboxAddress))
			if addr == "" {
				errors = append(errors, "mailbox_address required")
				continue
			}
			sourceType := strings.TrimSpace(item.SourceType)
			if sourceType == "" {
				sourceType = "OUTLOOK_GRAPH"
			}
			graphFields := item.MailboxGraphFields
			googleFields := item.MailboxGoogleWorkspaceFields
			customCreds := emailmailbox.GraphFieldsConfigured(graphFields) || emailmailbox.GoogleWorkspaceFieldsConfigured(googleFields)
			if !emailmailbox.MailboxAddressAllowed(addr, sourceType, customCreds) {
				errors = append(errors, addr+": domain not allowed for source "+sourceType)
				continue
			}
			if strings.EqualFold(sourceType, "OAUTH") {
				errors = append(errors, addr+": OAuth mail source is not available yet")
				continue
			}
			domain := strings.TrimPrefix(addr[strings.LastIndex(addr, "@"):], "@")
			if len(item.FiltersJSON) == 0 {
				item.FiltersJSON = json.RawMessage(`{}`)
			}
			item.FiltersJSON = emailjobs.NormalizeMailboxFiltersJSON(item.FiltersJSON)
			if item.PollIntervalSecs <= 0 {
				item.PollIntervalSecs = 60
			}
			entityID := strings.TrimSpace(defaultEntityID)
			if strings.TrimSpace(item.EntityID) != "" {
				entityID = strings.TrimSpace(item.EntityID)
			}
			okPolicy, pmsg, matrixID := inboxEnforceMatrix(r.Context(), r, pool, common.TriggerPreCreate,
				"HandleWorkflowInboxCreate", "/email/inbox/workflow/create", entityID, userEmail)
			if !okPolicy {
				errors = append(errors, addr+": "+pmsg)
				continue
			}

			imapFields := item.MailboxIMAPFields
			if strings.EqualFold(sourceType, "IMAP") {
				merged, mergeErr := emailmailbox.MergeIMAPFields(emailmailbox.MailboxIMAPFields{}, imapFields, addr)
				if mergeErr != nil {
					errors = append(errors, addr+constants.ErrImapPrefix+mergeErr.Error())
					continue
				}
				imapFields = merged
			}
			if strings.EqualFold(sourceType, "OAUTH") && strings.TrimSpace(item.OAuthProvider) == "" {
				errors = append(errors, addr+": oauth provider required (microsoft or google)")
				continue
			}
			if strings.EqualFold(sourceType, "OAUTH") {
				if strings.TrimSpace(item.OAuthMailTransport) == "" {
					item.OAuthMailTransport = "api"
				}
				if strings.EqualFold(item.OAuthMailTransport, "imap") {
					merged, mergeErr := emailmailbox.ResolveIMAPOAuthFields(imapFields, addr)
					if mergeErr != nil {
						errors = append(errors, addr+": oauth imap: "+mergeErr.Error())
						continue
					}
					imapFields = merged
				}
			}
			if strings.EqualFold(sourceType, "OUTLOOK_GRAPH") && emailmailbox.GraphFieldsConfigured(graphFields) {
				merged, mergeErr := emailmailbox.MergeGraphFields(emailmailbox.MailboxGraphFields{}, graphFields)
				if mergeErr != nil {
					errors = append(errors, addr+constants.ErrGraphPrefix+mergeErr.Error())
					continue
				}
				graphFields = merged
			}
			if strings.EqualFold(sourceType, "GOOGLE_WORKSPACE") && emailmailbox.GoogleWorkspaceFieldsConfigured(googleFields) {
				merged, mergeErr := emailmailbox.MergeGoogleWorkspaceFields(emailmailbox.MailboxGoogleWorkspaceFields{}, googleFields)
				if mergeErr != nil {
					errors = append(errors, addr+constants.ErrGoogleWorkspacePrefix+mergeErr.Error())
					continue
				}
				googleFields = merged
			}
			graphTenantKey := strings.TrimSpace(graphFields.TenantKey)
			if strings.EqualFold(sourceType, "OUTLOOK_GRAPH") && graphTenantKey == "" && !emailmailbox.GraphFieldsConfigured(graphFields) {
				graphTenantKey = "default"
			}
			googleTenantKey := strings.TrimSpace(googleFields.WorkspaceTenantKey)
			if strings.EqualFold(sourceType, "GOOGLE_WORKSPACE") && googleTenantKey == "" && !emailmailbox.GoogleWorkspaceFieldsConfigured(googleFields) {
				googleTenantKey = "default"
			}

			var activeExists bool
			if err := pool.QueryRow(r.Context(), `
				SELECT EXISTS(
					SELECT 1 FROM email_svc.inbox_config
					WHERE LOWER(mailbox_address) = LOWER($1)
					  AND COALESCE(is_deleted, false) = false
				)
			`, addr).Scan(&activeExists); err != nil {
				errors = append(errors, addr+": lookup failed")
				continue
			}
			if activeExists {
				errors = append(errors, addr+": mailbox already exists")
				continue
			}

			var inboxID string
			err := pool.QueryRow(r.Context(), `
				INSERT INTO email_svc.inbox_config (
					mailbox_address, display_name, domain, filters_json, poll_interval_secs,
					module, entity_id, is_active, processing_status, source_type,
					graph_tenant_key, graph_tenant_label, graph_tenant_id, graph_client_id, graph_client_secret,
					google_workspace_tenant_key, google_workspace_tenant_label,
					google_workspace_service_account_email, google_workspace_client_id, google_workspace_private_key,
					imap_provider, imap_host, imap_port, imap_username, imap_password,
					imap_inbox_folder, imap_sent_folder, imap_use_tls,
					oauth_provider, oauth_mail_transport,
					owner_user_id, submitted_by, ses_rule_name
				) VALUES ($1, $2, $3, $4::jsonb, $5, NULLIF($6,''), NULLIF($7,''), false,
				          $8, $9,
				          $10, $11, $12, $13, $14,
				          $15, $16, $17, $18, $19,
				          $20, $21, $22, $23, $24,
				          $25, $26, $27,
				          COALESCE($28,''), COALESCE(NULLIF($29,''),'api'),
				          $30, $31, $32)
				RETURNING inbox_id::text
			`, addr, item.DisplayName, domain, string(item.FiltersJSON), item.PollIntervalSecs,
				item.Module, entityID, constants.StatusPendingApproval, sourceType,
				graphTenantKey, graphFields.TenantLabel, graphFields.TenantID, graphFields.ClientID, graphFields.ClientSecret,
				googleTenantKey, googleFields.WorkspaceTenantLabel, googleFields.WorkspaceServiceAccountEmail, googleFields.WorkspaceClientID, googleFields.WorkspacePrivateKey,
				imapFields.Provider, imapFields.Host, imapFields.Port, imapFields.Username, imapFields.Password,
				imapFields.InboxFolder, imapFields.SentFolder, imapFields.UseTLS,
				strings.TrimSpace(item.OAuthProvider), strings.TrimSpace(item.OAuthMailTransport),
				userID, userID, "",
			).Scan(&inboxID)
			if err != nil {
				errors = append(errors, addr+": "+err.Error())
				continue
			}

			ruleName := sesRuleNameForInbox(inboxID)
			if sourceType == "SES" {
				_, _ = pool.Exec(r.Context(), `UPDATE email_svc.inbox_config SET ses_rule_name = $2 WHERE inbox_id = $1::uuid`, inboxID, ruleName)
			}

			shared := req.SharedUserIDs
			if len(item.SharedUserIDs) > 0 {
				shared = item.SharedUserIDs
			}
			for _, uid := range shared {
				uid = strings.TrimSpace(uid)
				if uid == "" {
					continue
				}
				_, _ = pool.Exec(r.Context(), `
					INSERT INTO email_svc.inbox_members (inbox_id, user_id, role)
					VALUES ($1::uuid, $2, 'VIEWER')
					ON CONFLICT DO NOTHING
				`, inboxID, uid)
			}
			logInboxAudit(r.Context(), pool, inboxAuditEntry{
				InboxID: inboxID, Action: "CREATE", Status: constants.StatusPendingApproval, UserID: userID, Comment: "", Detail: map[string]interface{}{"mailbox": addr},
			})
			if _, err := submitInboxApproval(r.Context(), pool, inboxApprovalRequest{
				InboxID: inboxID, EntityID: entityID, TxType: "EMAIL_INBOX_CREATE", ActionType: "CREATE", UserID: userID, UserEmail: userEmail, MatrixID: matrixID,
			}); err != nil {
				errors = append(errors, addr+constants.ErrApprovalInstancePrefix+err.Error())
			}
			created = append(created, inboxID)
		}

		emailcommon.RespondBulk(w, "inbox/workflow/create", "created", created, errors)
	}
}

func HandleWorkflowInboxUpdate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID   string          `json:"user_id"`
			EntityID string          `json:"entity_id"`
			Items    []workflowInbox `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}

		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, req.UserID, req.EntityID)
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)
		var updated, errors []string

		for _, item := range req.Items {
			inboxID := strings.TrimSpace(item.InboxID)
			if inboxID == "" {
				errors = append(errors, "inbox_id required")
				continue
			}
			if !userCanAccessInbox(r.Context(), pool, inboxID, userID, userEmail, entityIDs, admin) {
				errors = append(errors, inboxID+": access denied")
				continue
			}

			var status, inboxEntityID string
			if err := pool.QueryRow(r.Context(), `
				SELECT processing_status, COALESCE(entity_id,'') FROM email_svc.inbox_config WHERE inbox_id = $1::uuid AND is_deleted = false
			`, inboxID).Scan(&status, &inboxEntityID); err != nil {
				errors = append(errors, inboxID+constants.ErrNotFoundSuffix)
				continue
			}

			// Every edit is staged, never applied in place: the live config keeps running
			// on its current values until a checker approves. The staged payload lands in
			// pending_edit_json and applyInboxDecision merges it in on approve.
			statusNorm := strings.ToUpper(strings.TrimSpace(status))
			switch statusNorm {
			case constants.StatusPendingApproval, constants.StatusApproved,
				constants.StatusPendingEditApproval, constants.StatusRejected,
				constants.StatusPendingDeleteApproval:
			default:
				errors = append(errors, inboxID+": cannot edit in status "+status)
				continue
			}

			okPolicy, pmsg, matrixID := inboxEnforceMatrix(r.Context(), r, pool, common.TriggerPreEdit,
				"HandleWorkflowInboxUpdate", "/email/inbox/workflow/update", inboxEntityID, userEmail)
			if !okPolicy {
				errors = append(errors, inboxID+": "+pmsg)
				continue
			}

			pending, _ := json.Marshal(item)
			_, err := pool.Exec(r.Context(), `
				UPDATE email_svc.inbox_config
				SET processing_status = $2,
				    pending_edit_json = $3::jsonb,
				    submitted_by = $4,
				    checker_comment = '',
				    updated_at = now()
				WHERE inbox_id = $1::uuid
				  AND is_deleted = false
			`, inboxID, constants.StatusPendingEditApproval, string(pending), userID)
			if err != nil {
				errors = append(errors, inboxID+": "+err.Error())
				continue
			}
			logInboxAudit(r.Context(), pool, inboxAuditEntry{
				InboxID: inboxID, Action: "EDIT", Status: constants.StatusPendingEditApproval, UserID: userID, Comment: "", Detail: map[string]interface{}{
					"pending":      item,
					"prior_status": statusNorm,
				},
			})
			if _, instErr := submitInboxApproval(r.Context(), pool, inboxApprovalRequest{
				InboxID: inboxID, EntityID: inboxEntityID, TxType: "EMAIL_INBOX_EDIT", ActionType: "EDIT", UserID: userID, UserEmail: userEmail, MatrixID: matrixID,
			}); instErr != nil {
				errors = append(errors, inboxID+constants.ErrApprovalInstancePrefix+instErr.Error())
			}
			updated = append(updated, inboxID)
		}
		emailcommon.RespondBulk(w, "inbox/workflow/update", "updated", updated, errors)
	}
}

func HandleWorkflowInboxDeleteRequest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID   string   `json:"user_id"`
			EntityID string   `json:"entity_id"`
			InboxIDs []string `json:"inbox_ids"`
			Comment  string   `json:"comment"`
			Reason   string   `json:"reason"` // alias (FD-style delete reason)
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		deleteReason := strings.TrimSpace(req.Comment)
		if deleteReason == "" {
			deleteReason = strings.TrimSpace(req.Reason)
		}
		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, req.UserID, req.EntityID)
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)
		var updated, errors []string

		for _, inboxID := range req.InboxIDs {
			inboxID = strings.TrimSpace(inboxID)
			if inboxID == "" {
				continue
			}
			if !userCanAccessInbox(r.Context(), pool, inboxID, userID, userEmail, entityIDs, admin) {
				errors = append(errors, inboxID+": access denied")
				continue
			}
			var status, inboxEntityID string
			if err := pool.QueryRow(r.Context(), `
				SELECT processing_status, COALESCE(entity_id,'') FROM email_svc.inbox_config WHERE inbox_id = $1::uuid AND is_deleted = false
			`, inboxID).Scan(&status, &inboxEntityID); err != nil {
				errors = append(errors, inboxID+constants.ErrNotFoundSuffix)
				continue
			}

			statusNorm := strings.ToUpper(strings.TrimSpace(status))

			// Maker delete request — stay visible as PENDING_DELETE_APPROVAL until checker approves.
			// Soft-delete (is_deleted=true) happens only on approve-delete, never here.
			// APPROVED (live) mailboxes cannot be deleted at all.
			switch statusNorm {
			case constants.StatusPendingApproval, constants.StatusPendingEditApproval, constants.StatusRejected:
				okPolicy, pmsg, matrixID := inboxEnforceMatrix(r.Context(), r, pool, common.TriggerPreDelete,
					"HandleWorkflowInboxDeleteRequest", "/email/inbox/workflow/delete", inboxEntityID, userEmail)
				if !okPolicy {
					errors = append(errors, inboxID+": "+pmsg)
					continue
				}
				_, err := pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = 'PENDING_DELETE_APPROVAL',
					    submitted_by = $2,
					    pending_edit_json = NULL,
					    checker_comment = $3,
					    updated_at = now()
					WHERE inbox_id = $1::uuid
					  AND is_deleted = false
				`, inboxID, userID, deleteReason)
				if err != nil {
					errors = append(errors, inboxID+": "+err.Error())
					continue
				}
				// Cancel any open create/edit approval instances; submit a delete instance.
				_ = approvalengine.CancelPendingInstances(r.Context(), pool, emailInboxModuleCode, inboxID, userEmail)
				logInboxAudit(r.Context(), pool, inboxAuditEntry{
					InboxID: inboxID, Action: "DELETE", Status: constants.StatusPendingDeleteApproval, UserID: userID, Comment: deleteReason, Detail: map[string]interface{}{
						"prior_status": statusNorm,
						"reason":       deleteReason,
					},
				})
				if _, instErr := submitInboxApproval(r.Context(), pool, inboxApprovalRequest{
					InboxID: inboxID, EntityID: inboxEntityID, TxType: "EMAIL_INBOX_DELETE", ActionType: "DELETE", UserID: userID, UserEmail: userEmail, MatrixID: matrixID,
				}); instErr != nil {
					errors = append(errors, inboxID+constants.ErrApprovalInstancePrefix+instErr.Error())
				}
				updated = append(updated, inboxID)
			case constants.StatusPendingDeleteApproval:
				errors = append(errors, inboxID+": already pending delete approval")
			case constants.StatusApproved:
				errors = append(errors, inboxID+": approved mailboxes cannot be deleted")
			default:
				errors = append(errors, inboxID+": delete only from PENDING_APPROVAL, PENDING_EDIT_APPROVAL, or REJECTED")
			}
		}
		emailcommon.RespondBulk(w, "inbox/workflow/update", "updated", updated, errors)
	}
}

func checkerCanAct(submittedBy, checkerUserID, inboxEntityID string, entityIDs []string) bool {
	if submittedBy == checkerUserID {
		return false
	}
	if inboxEntityID == "" {
		return true
	}
	return emailcommon.EntityInScope(inboxEntityID, entityIDs)
}

func HandleWorkflowInboxApprove(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID   string   `json:"user_id"`
			EntityID string   `json:"entity_id"`
			InboxIDs []string `json:"inbox_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, req.UserID, req.EntityID)
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)
		var approved, errors []string

		for _, inboxID := range req.InboxIDs {
			inboxID = strings.TrimSpace(inboxID)
			var status, submittedBy, entityID, pendingEdit string
			err := pool.QueryRow(r.Context(), `
				SELECT processing_status, COALESCE(submitted_by,''), COALESCE(entity_id,''),
				       COALESCE(pending_edit_json::text,'')
				FROM email_svc.inbox_config WHERE inbox_id = $1::uuid AND is_deleted = false
			`, inboxID).Scan(&status, &submittedBy, &entityID, &pendingEdit)
			if err != nil {
				errors = append(errors, inboxID+constants.ErrNotFoundSuffix)
				continue
			}
			if !admin && !checkerCanAct(submittedBy, userID, entityID, entityIDs) {
				errors = append(errors, inboxID+": checker must be in same entity and not the submitter")
				continue
			}

			txType := map[string]string{
				constants.StatusPendingApproval:       "EMAIL_INBOX_CREATE",
				constants.StatusPendingEditApproval:   "EMAIL_INBOX_EDIT",
				constants.StatusPendingDeleteApproval: "EMAIL_INBOX_DELETE",
			}[status]
			if txType != "" {
				if txType == "EMAIL_INBOX_CREATE" || txType == "EMAIL_INBOX_EDIT" {
					if err := emailmailbox.ValidateMailboxReady(r.Context(), pool, inboxID); err != nil {
						errors = append(errors, inboxID+": "+err.Error())
						continue
					}
				}
				actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(r.Context(), pool, approvalengine.ActOnPendingRequest{
					ModuleCode: emailInboxModuleCode,
					RecordID:   inboxID,
					UserID:     userID,
					UserEmail:  userEmail,
					Action:     approvalengine.ActionApproved,
					Comment:    req.Comment,
				})
				if actionErr != nil {
					errors = append(errors, inboxID+": approval engine: "+actionErr.Error())
					continue
				}
				if actionRes.Acted {
					if actionRes.InstanceStatus == constants.StatusApproved {
						if err := applyInboxDecision(r.Context(), pool, inboxID, txType, approvalengine.InstStatusApproved, userID, req.Comment); err != nil {
							errors = append(errors, inboxID+": "+err.Error())
							continue
						}
						auditAction, auditStatus := "APPROVE", constants.StatusApproved
						if txType == "EMAIL_INBOX_DELETE" {
							auditAction, auditStatus = "APPROVE_DELETE", "DELETED"
						} else if txType == "EMAIL_INBOX_EDIT" {
							auditAction = "APPROVE_EDIT"
						}
						logInboxAudit(r.Context(), pool, inboxAuditEntry{
							InboxID: inboxID, Action: auditAction, Status: auditStatus, UserID: userID, Comment: req.Comment, Detail: map[string]interface{}{"engine_instance": actionRes.InstanceID},
						})
						approved = append(approved, inboxID)
						if txType != "EMAIL_INBOX_DELETE" {
							OnInboxApproved(pool, inboxID)
						}
					}
					continue
				}
			}

			switch status {
			case constants.StatusPendingApproval:
				if err := emailmailbox.ValidateMailboxReady(r.Context(), pool, inboxID); err != nil {
					errors = append(errors, inboxID+": "+err.Error())
					continue
				}
				_, err = pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = $2, approved_by = $3, checker_comment = $4,
					    is_active = true, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, constants.StatusApproved, userID, req.Comment)
				logInboxAudit(r.Context(), pool, inboxAuditEntry{
					InboxID: inboxID, Action: "APPROVE", Status: constants.StatusApproved, UserID: userID, Comment: req.Comment, Detail: nil,
				})

			case constants.StatusPendingEditApproval:
				if err := emailmailbox.ValidateMailboxReady(r.Context(), pool, inboxID); err != nil {
					errors = append(errors, inboxID+": "+err.Error())
					continue
				}
				if pendingEdit != "" && pendingEdit != "null" {
					var edit workflowInbox
					if json.Unmarshal([]byte(pendingEdit), &edit) == nil {
						mailbox, existingIMAP, _ := emailmailbox.LoadMailboxIMAPFields(r.Context(), pool, inboxID)
						existingGraph, _ := emailmailbox.LoadMailboxGraphFields(r.Context(), pool, inboxID)
						existingGoogle, _ := emailmailbox.LoadMailboxGoogleWorkspaceFields(r.Context(), pool, inboxID)
						imapFinal := existingIMAP
						graphFinal := existingGraph
						googleFinal := existingGoogle
						if strings.EqualFold(edit.SourceType, "IMAP") && emailmailbox.IMAPPatchPresent(edit.MailboxIMAPFields) {
							merged, mergeErr := emailmailbox.MergeIMAPFields(existingIMAP, edit.MailboxIMAPFields, mailbox)
							if mergeErr != nil {
								errors = append(errors, inboxID+constants.ErrImapPrefix+mergeErr.Error())
								continue
							}
							imapFinal = merged
						}
						if emailmailbox.GraphPatchPresent(edit.MailboxGraphFields) {
							merged, mergeErr := emailmailbox.MergeGraphFields(existingGraph, edit.MailboxGraphFields)
							if mergeErr != nil {
								errors = append(errors, inboxID+constants.ErrGraphPrefix+mergeErr.Error())
								continue
							}
							graphFinal = merged
						}
						if emailmailbox.GoogleWorkspacePatchPresent(edit.MailboxGoogleWorkspaceFields) {
							merged, mergeErr := emailmailbox.MergeGoogleWorkspaceFields(existingGoogle, edit.MailboxGoogleWorkspaceFields)
							if mergeErr != nil {
								errors = append(errors, inboxID+constants.ErrGoogleWorkspacePrefix+mergeErr.Error())
								continue
							}
							googleFinal = merged
						}
						_, err = pool.Exec(r.Context(), `
							UPDATE email_svc.inbox_config
							SET display_name = COALESCE(NULLIF($2,''), display_name),
							    filters_json = COALESCE($3::jsonb, filters_json),
							    poll_interval_secs = CASE WHEN $4 > 0 THEN $4 ELSE poll_interval_secs END,
							    module = COALESCE(NULLIF($5,''), module),
							    graph_tenant_key = $6,
							    graph_tenant_label = $7, graph_tenant_id = $8,
							    graph_client_id = $9, graph_client_secret = $10,
							    google_workspace_tenant_key = $11,
							    google_workspace_tenant_label = $12,
							    google_workspace_service_account_email = $13,
							    google_workspace_client_id = $14,
							    google_workspace_private_key = $15,
							    imap_provider = $16, imap_host = $17, imap_port = $18,
							    imap_username = $19, imap_password = $20,
							    imap_inbox_folder = $21, imap_sent_folder = $22, imap_use_tls = $23,
							    processing_status = $24,
							    pending_edit_json = NULL,
							    approved_by = $25,
							    checker_comment = $26,
							    is_active = true,
							    updated_at = now()
							WHERE inbox_id = $1::uuid
						`, inboxID, edit.DisplayName, emailcommon.NullableJSON(edit.FiltersJSON), edit.PollIntervalSecs,
							edit.Module,
							graphFinal.TenantKey, graphFinal.TenantLabel, graphFinal.TenantID, graphFinal.ClientID, graphFinal.ClientSecret,
							googleFinal.WorkspaceTenantKey, googleFinal.WorkspaceTenantLabel, googleFinal.WorkspaceServiceAccountEmail,
							googleFinal.WorkspaceClientID, googleFinal.WorkspacePrivateKey,
							imapFinal.Provider, imapFinal.Host, imapFinal.Port,
							imapFinal.Username, imapFinal.Password,
							imapFinal.InboxFolder, imapFinal.SentFolder, imapFinal.UseTLS,
							constants.StatusApproved, userID, req.Comment)
					}
				} else {
					_, err = pool.Exec(r.Context(), `
						UPDATE email_svc.inbox_config
						SET processing_status = $2, pending_edit_json = NULL,
						    approved_by = $3, checker_comment = $4,
						    is_active = true, updated_at = now()
						WHERE inbox_id = $1::uuid
					`, inboxID, constants.StatusApproved, userID, req.Comment)
				}
				logInboxAudit(r.Context(), pool, inboxAuditEntry{
					InboxID: inboxID, Action: "APPROVE_EDIT", Status: constants.StatusApproved, UserID: userID, Comment: req.Comment, Detail: nil,
				})

			case constants.StatusPendingDeleteApproval:
				var mailbox string
				_ = pool.QueryRow(r.Context(), `SELECT mailbox_address FROM email_svc.inbox_config WHERE inbox_id = $1::uuid`, inboxID).Scan(&mailbox)
				if err = applyApprovedInboxDelete(r.Context(), pool, inboxID, userID, req.Comment); err != nil {
					errors = append(errors, inboxID+": "+err.Error())
					continue
				}
				logInboxAudit(r.Context(), pool, inboxAuditEntry{
					InboxID: inboxID, Action: "APPROVE_DELETE", Status: "DELETED", UserID: userID, Comment: req.Comment, Detail: map[string]interface{}{"mailbox": mailbox},
				})
				approved = append(approved, inboxID)
				continue

			default:
				errors = append(errors, inboxID+": not pending approval")
				continue
			}
			if err != nil {
				errors = append(errors, inboxID+": "+err.Error())
				continue
			}
			approved = append(approved, inboxID)
			OnInboxApproved(pool, inboxID)
		}

		emailcommon.RespondBulk(w, "inbox/workflow/approve", "approved", approved, errors)
	}
}

func HandleWorkflowInboxReject(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID   string   `json:"user_id"`
			EntityID string   `json:"entity_id"`
			InboxIDs []string `json:"inbox_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, req.UserID, req.EntityID)
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)
		var rejected, errors []string

		for _, inboxID := range req.InboxIDs {
			inboxID = strings.TrimSpace(inboxID)
			var status, submittedBy, entityID string
			err := pool.QueryRow(r.Context(), `
				SELECT processing_status, COALESCE(submitted_by,''), COALESCE(entity_id,'')
				FROM email_svc.inbox_config WHERE inbox_id = $1::uuid AND is_deleted = false
			`, inboxID).Scan(&status, &submittedBy, &entityID)
			if err != nil {
				errors = append(errors, inboxID+constants.ErrNotFoundSuffix)
				continue
			}
			if !admin && !checkerCanAct(submittedBy, userID, entityID, entityIDs) {
				errors = append(errors, inboxID+": checker must be in same entity and not the submitter")
				continue
			}

			txType := map[string]string{
				constants.StatusPendingApproval:       "EMAIL_INBOX_CREATE",
				constants.StatusPendingEditApproval:   "EMAIL_INBOX_EDIT",
				constants.StatusPendingDeleteApproval: "EMAIL_INBOX_DELETE",
			}[status]
			if txType != "" {
				actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(r.Context(), pool, approvalengine.ActOnPendingRequest{
					ModuleCode: emailInboxModuleCode,
					RecordID:   inboxID,
					UserID:     userID,
					UserEmail:  userEmail,
					Action:     approvalengine.ActionRejected,
					Comment:    req.Comment,
				})
				if actionErr != nil {
					errors = append(errors, inboxID+": approval engine: "+actionErr.Error())
					continue
				}
				if actionRes.Acted {
					// Rejecting a create, edit, or delete request always lands on REJECTED —
					// never restored to the pre-request status.
					if err := applyInboxDecision(r.Context(), pool, inboxID, txType, approvalengine.InstStatusRejected, userID, req.Comment); err != nil {
						errors = append(errors, inboxID+": "+err.Error())
						continue
					}
					logInboxAudit(r.Context(), pool, inboxAuditEntry{
						InboxID: inboxID, Action: "REJECT", Status: constants.StatusRejected, UserID: userID, Comment: req.Comment, Detail: map[string]interface{}{
							"engine_instance": actionRes.InstanceID,
							"rejected_tx":     txType,
							"prior_status":    status,
						},
					})
					rejected = append(rejected, inboxID)
					continue
				}
			}

			switch status {
			case constants.StatusPendingApproval:
				// Reject create → REJECTED (row kept, is_deleted stays false)
				_, err = pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = 'REJECTED',
					    checker_comment = $2,
					    is_active = false,
					    is_deleted = false,
					    updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, req.Comment)
			case constants.StatusPendingEditApproval, constants.StatusPendingDeleteApproval:
				// Reject edit or delete request → REJECTED, clear pending_edit_json.
				// Row is kept (is_deleted stays false) so it stays visible.
				_, err = pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = $2,
					    pending_edit_json = NULL,
					    checker_comment = $3,
					    is_active = false,
					    is_deleted = false,
					    updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, constants.StatusRejected, req.Comment)
			default:
				errors = append(errors, inboxID+": not pending approval")
				continue
			}
			if err != nil {
				errors = append(errors, inboxID+": "+err.Error())
				continue
			}
			logInboxAudit(r.Context(), pool, inboxAuditEntry{
				InboxID: inboxID, Action: "REJECT", Status: constants.StatusRejected, UserID: userID, Comment: req.Comment, Detail: map[string]interface{}{
					"prior_status": status,
				},
			})
			rejected = append(rejected, inboxID)
		}
		emailcommon.RespondBulk(w, "inbox/workflow/reject", "rejected", rejected, errors)
	}
}

func HandleWorkflowInboxAuditLog(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			InboxID string `json:"inbox_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		inboxID := strings.TrimSpace(req.InboxID)
		if inboxID == "" {
			emailcommon.RespondBadRequest(w, "inbox_id is required")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT audit_id::text, inbox_id::text, action_type, status,
			       COALESCE(performed_by, ''), COALESCE(comment, ''), detail, created_at
			FROM email_svc.inbox_audit
			WHERE inbox_id = $1::uuid
			ORDER BY created_at DESC
		`, inboxID)
		if err != nil {
			emailcommon.RespondInternal(w, "Failed to load inbox audit log")
			return
		}
		defer rows.Close()

		type auditRow struct {
			AuditID     string                 `json:"audit_id"`
			InboxID     string                 `json:"inbox_id"`
			ActionType  string                 `json:"action_type"`
			Status      string                 `json:"status"`
			PerformedBy string                 `json:"performed_by"`
			Comment     string                 `json:"comment"`
			Detail      map[string]interface{} `json:"detail"`
			CreatedAt   string                 `json:"created_at"`
		}
		var items []auditRow
		for rows.Next() {
			var row auditRow
			var rawDetail []byte
			var createdAt time.Time
			if err := rows.Scan(
				&row.AuditID, &row.InboxID, &row.ActionType, &row.Status,
				&row.PerformedBy, &row.Comment, &rawDetail, &createdAt,
			); err != nil {
				emailcommon.RespondInternal(w, "Failed to parse inbox audit log")
				return
			}
			row.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
			row.Detail = map[string]interface{}{}
			if len(rawDetail) > 0 {
				_ = json.Unmarshal(rawDetail, &row.Detail)
			}
			items = append(items, row)
		}
		if items == nil {
			items = []auditRow{}
		}

		emailcommon.RespondList(w, "inbox/workflow/audit-log", items, len(items))
	}
}

func HandleWorkflowInboxSync(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		result, err := SyncApprovedInboxesToSES(r.Context(), pool)
		if err != nil {
			emailcommon.RespondBadRequest(w, err.Error())
			return
		}
		ApplyInboundRuleSyncResult(r.Context(), pool, result, nil)
		msg := "SES rules synced"
		if result != nil {
			if len(result.Errors) > 0 {
				msg = strings.Join(result.Errors, "; ")
				emailcommon.RespondFailPayload(w, "inbox/workflow/sync", msg, map[string]interface{}{
					"message":  msg,
					"ses_sync": result,
				})
				return
			}
			if result.Synced == 0 {
				msg = "No approved SES mailboxes to sync. Create and approve a mailbox first."
			}
		}
		emailcommon.RespondPayload(w, "inbox/workflow/sync", map[string]interface{}{
			"message":  msg,
			"ses_sync": result,
		})
	}
}

func HandleEmailServiceHealth(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		healthy := emailjobs.SyncEmailServiceStatus(r.Context(), pool)
		msg := ""
		if !healthy {
			msg = emailjobs.EmailSubscriptionExpiredMsg
		}
		emailcommon.RespondPayload(w, "service/health", map[string]interface{}{
			"healthy": healthy,
			"message": msg,
		})
	}
}

func HandlePollTrigger(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		userID, _, _, _ := emailcommon.RequestIdentity(r, req.UserID, "")
		if !emailcommon.IsEmailAdmin(r.Context(), userID) {
			emailcommon.RespondForbidden(w, constants.ErrManualPollAdminOnly)
			return
		}
		started := emailjobs.StartInboundPollAsync(pool)
		graphStarted := emailjobs.StartGraphPollAsync(pool)
		googleStarted := emailjobs.StartGoogleWorkspacePollAsync(pool)
		imapStarted := emailjobs.StartIMAPPollAsync(pool)
		oauthStarted := emailjobs.StartOAuthPollAsync(pool)
		emailcommon.RespondPayload(w, "poll/trigger", map[string]interface{}{
			"started":         started || graphStarted || googleStarted || imapStarted || oauthStarted,
			"inbound_started": started,
			"graph_started":   graphStarted,
			"google_started":  googleStarted,
			"imap_started":    imapStarted,
			"oauth_started":   oauthStarted,
			"message":         "Poll started in background; mail arrives within ~60s via cron if already running",
		})
	}
}

func HandleGraphPollTrigger(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		userID, _, _, _ := emailcommon.RequestIdentity(r, req.UserID, "")
		if !emailcommon.IsEmailAdmin(r.Context(), userID) {
			emailcommon.RespondForbidden(w, constants.ErrManualPollAdminOnly)
			return
		}
		started := emailjobs.StartGraphPollAsync(pool)
		msg := "Outlook poll started in background"
		if !started {
			msg = "Outlook poll already running"
		}
		emailcommon.RespondPayload(w, "poll/graph/trigger", map[string]interface{}{
			"graph_polled": started,
			"started":      started,
			"message":      msg,
		})
	}
}

func HandleIMAPPollTrigger(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		userID, _, _, _ := emailcommon.RequestIdentity(r, req.UserID, "")
		if !emailcommon.IsEmailAdmin(r.Context(), userID) {
			emailcommon.RespondForbidden(w, constants.ErrManualPollAdminOnly)
			return
		}
		started := emailjobs.StartIMAPPollAsync(pool)
		msg := "IMAP poll started in background"
		if !started {
			msg = "IMAP poll already running"
		}
		emailcommon.RespondPayload(w, "poll/imap/trigger", map[string]interface{}{
			"imap_polled": started,
			"started":     started,
			"message":     msg,
		})
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
