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
	SourceType       string `json:"source_type"`
	emailmailbox.MailboxGraphFields
	emailmailbox.MailboxIMAPFields
	GraphLastSyncAt     *time.Time `json:"graph_last_sync_at"`
	GraphSentLastSyncAt *time.Time      `json:"graph_sent_last_sync_at"`
	CheckerComment      string          `json:"checker_comment"`
	IsActive         bool            `json:"is_active"`
	IsDeleted        bool            `json:"is_deleted"`
	GraphSecretSet      bool `json:"graph_client_secret_set,omitempty"`
	IMAPPasswordSet     bool `json:"imap_password_set,omitempty"`
	SharedUserIDs    []string        `json:"shared_user_ids,omitempty"`
	PendingEditJSON  json.RawMessage `json:"pending_edit_json,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
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

func HandleWorkflowMeta() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		emailcommon.RespondPayload(w, "inbox/workflow/meta", map[string]interface{}{
			"allowed_domains": emailmailbox.AllowedMailboxDomains(),
		})
	}
}

func logInboxAudit(ctx context.Context, pool *pgxpool.Pool, inboxID, action, status, userID, comment string, detail map[string]interface{}) {
	if detail == nil {
		detail = map[string]interface{}{}
	}
	b, _ := json.Marshal(detail)
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.inbox_audit (inbox_id, action_type, status, processing_status, performed_by, comment, detail)
		VALUES ($1::uuid, $2, $3, $3, NULLIF($4,''), NULLIF($5,''), $6::jsonb)
	`, inboxID, action, status, userID, comment, string(b))
}

func submitInboxApproval(ctx context.Context, pool *pgxpool.Pool, inboxID, entityID, txType, actionType, userID, userEmail string) (string, error) {
	if userEmail == "" {
		userEmail = userID
	}
	if err := approvalengine.CancelPendingInstances(ctx, pool, emailInboxModuleCode, inboxID, userEmail); err != nil {
		return "", err
	}
	return approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
		ModuleCode:       emailInboxModuleCode,
		EntityCode:       entityID,
		TransactionType:  txType,
		RecordID:         inboxID,
		RecordTable:      emailInboxRecordTable,
		AuditTable:       emailInboxAuditTable,
		AuditIDColumn:    "inbox_id",
		ActionType:       actionType,
		Amount:           0,
		SubmittedBy:      userID,
		SubmittedByEmail: userEmail,
	})
}

func finalizeEmailInboxApproval(ctx context.Context, pool *pgxpool.Pool, recordID, transactionType, finalStatus, actorEmail, comment string) {
	_ = applyInboxDecision(ctx, pool, recordID, transactionType, finalStatus, actorEmail, comment)
	if finalStatus == approvalengine.InstStatusApproved {
		result, err := SyncApprovedInboxesToSES(ctx, pool)
		ApplyInboundRuleSyncResult(ctx, pool, result, err)
	}
}

func applyInboxDecision(ctx context.Context, pool *pgxpool.Pool, inboxID, transactionType, finalStatus, actorID, comment string) error {
	if finalStatus == approvalengine.InstStatusRejected {
		switch transactionType {
		case "EMAIL_INBOX_CREATE":
			_, err := pool.Exec(ctx, `
				UPDATE email_svc.inbox_config
				SET processing_status = $2, checker_comment = $3, is_active = false, updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, constants.StatusRejected, comment)
			return err
		case "EMAIL_INBOX_EDIT", "EMAIL_INBOX_DELETE":
			_, err := pool.Exec(ctx, `
				UPDATE email_svc.inbox_config
				SET processing_status = $2, pending_edit_json = NULL, checker_comment = $3, updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, constants.StatusApproved, comment)
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
				    approved_by = $3, checker_comment = $4, updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, constants.StatusApproved, actorID, comment)
			return err
		}
		var edit workflowInbox
		if err := json.Unmarshal([]byte(pendingEdit), &edit); err != nil {
			return err
		}
		_, err := pool.Exec(ctx, `
			UPDATE email_svc.inbox_config
			SET display_name = COALESCE(NULLIF($2,''), display_name),
			    filters_json = COALESCE($3::jsonb, filters_json),
			    poll_interval_secs = CASE WHEN $4 > 0 THEN $4 ELSE poll_interval_secs END,
			    module = COALESCE(NULLIF($5,''), module),
			    processing_status = $6,
			    pending_edit_json = NULL,
			    approved_by = $7,
			    checker_comment = $8,
			    updated_at = now()
			WHERE inbox_id = $1::uuid
		`, inboxID, edit.DisplayName, emailcommon.NullableJSON(edit.FiltersJSON), edit.PollIntervalSecs,
			edit.Module, constants.StatusApproved, actorID, comment)
		return err
	case "EMAIL_INBOX_DELETE":
		_, err := pool.Exec(ctx, `
			UPDATE email_svc.inbox_config
			SET processing_status = 'DELETED', is_deleted = true, is_active = false,
			    deleted_at = now(), deleted_by = $2, approved_by = $2,
			    checker_comment = $3, updated_at = now()
			WHERE inbox_id = $1::uuid
		`, inboxID, actorID, comment)
		return err
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

		query := fmt.Sprintf(`
			SELECT i.inbox_id::text, i.mailbox_address, COALESCE(i.display_name,''),
			       COALESCE(i.domain,''), i.filters_json, i.poll_interval_secs,
			       COALESCE(i.module,''), COALESCE(i.entity_id,''),
			       i.processing_status, COALESCE(i.ses_sync_status,''),
			       i.ses_synced_at, COALESCE(i.ses_last_error,''),
			       COALESCE(i.submitted_by,''), COALESCE(i.approved_by,''),
			       COALESCE(i.source_type,'OUTLOOK_GRAPH'),
			       %s, %s, %s, %s, %s,
			       %s, %s, %s,
			       %s, %s,
			       %s, %s,
			       %s,
			       i.graph_last_sync_at, i.graph_sent_last_sync_at,
			       COALESCE(i.checker_comment,''), i.is_active, i.is_deleted,
			       COALESCE(i.pending_edit_json::text, 'null'),
			       i.created_at, i.updated_at
			FROM email_svc.inbox_config i
			WHERE i.is_deleted = false
		`, emailjobs.SQLCoalesceGraphTenantKey, emailjobs.SQLCoalesceGraphTenantLabel, emailjobs.SQLCoalesceGraphTenantID,
			emailjobs.SQLCoalesceGraphClientID, emailjobs.SQLCoalesceGraphSecret,
			emailjobs.SQLCoalesceIMAPProvider, emailjobs.SQLCoalesceIMAPHost, emailjobs.SQLCoalesceIMAPPort,
			emailjobs.SQLCoalesceIMAPUsername, emailjobs.SQLCoalesceIMAPPassword,
			emailjobs.SQLCoalesceIMAPInboxFolder, emailjobs.SQLCoalesceIMAPSentFolder,
			emailjobs.SQLCoalesceIMAPUseTLS)
		args := []interface{}{}
		argN := 1
		if !admin {
			query += fmt.Sprintf(` AND (
				i.entity_id = ANY($%d::text[])
				OR i.owner_user_id = $%d
				OR ($%d <> '' AND LOWER(i.mailbox_address) = LOWER($%d))
				OR EXISTS (SELECT 1 FROM email_svc.inbox_members m WHERE m.inbox_id = i.inbox_id AND m.user_id = $%d)
			)`, argN, argN+1, argN+2, argN+2, argN+1)
			if len(entityIDs) == 0 && entityID != "" {
				entityIDs = []string{entityID}
			}
			args = append(args, entityIDs, userID, userEmail)
			argN += 3
		}
		if s := strings.TrimSpace(req.Status); s != "" {
			query += fmt.Sprintf(" AND i.processing_status = $%d", argN)
			args = append(args, s)
		}
		query += " ORDER BY i.created_at DESC"

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
			var imapPassword string
			if err := rows.Scan(
				&item.InboxID, &item.MailboxAddress, &item.DisplayName, &item.Domain,
				&item.FiltersJSON, &item.PollIntervalSecs, &item.Module, &item.EntityID,
				&item.ProcessingStatus, &item.SESSyncStatus, &item.SESSyncedAt, &item.SESLastError,
				&item.SubmittedBy, &item.ApprovedBy, &item.SourceType,
				&item.TenantKey, &item.TenantLabel, &item.TenantID, &item.ClientID, &graphSecret,
				&item.Provider, &item.Host, &item.Port,
				&item.Username, &imapPassword, &item.InboxFolder,
				&item.SentFolder, &item.UseTLS,
				&item.GraphLastSyncAt, &item.GraphSentLastSyncAt,
				&item.CheckerComment,
				&item.IsActive, &item.IsDeleted, &pendingText,
				&item.CreatedAt, &item.UpdatedAt,
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
			if imapPassword != "" {
				item.Password = imapPassword
				item.IMAPPasswordSet = true
			}
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
			emailcommon.RespondBadRequest(w, "invalid body")
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
			customGraph := emailmailbox.GraphFieldsConfigured(graphFields)
			if !emailmailbox.MailboxAddressAllowed(addr, sourceType, customGraph) {
				errors = append(errors, addr+": domain not allowed for source "+sourceType)
				continue
			}
			domain := strings.TrimPrefix(addr[strings.LastIndex(addr, "@"):], "@")
			if len(item.FiltersJSON) == 0 {
				item.FiltersJSON = json.RawMessage(`{}`)
			}
			if item.PollIntervalSecs <= 0 {
				item.PollIntervalSecs = 60
			}
			entityID := strings.TrimSpace(defaultEntityID)
			if strings.TrimSpace(item.EntityID) != "" {
				entityID = strings.TrimSpace(item.EntityID)
			}

			imapFields := item.MailboxIMAPFields
			if strings.EqualFold(sourceType, "IMAP") {
				merged, mergeErr := emailmailbox.MergeIMAPFields(emailmailbox.MailboxIMAPFields{}, imapFields, addr)
				if mergeErr != nil {
					errors = append(errors, addr+": imap: "+mergeErr.Error())
					continue
				}
				imapFields = merged
			}
			if strings.EqualFold(sourceType, "OUTLOOK_GRAPH") && emailmailbox.GraphFieldsConfigured(graphFields) {
				merged, mergeErr := emailmailbox.MergeGraphFields(emailmailbox.MailboxGraphFields{}, graphFields)
				if mergeErr != nil {
					errors = append(errors, addr+": graph: "+mergeErr.Error())
					continue
				}
				graphFields = merged
			}
			graphTenantKey := strings.TrimSpace(graphFields.TenantKey)
			if strings.EqualFold(sourceType, "OUTLOOK_GRAPH") && graphTenantKey == "" && !emailmailbox.GraphFieldsConfigured(graphFields) {
				graphTenantKey = "default"
			}

			var inboxID string
			err := pool.QueryRow(r.Context(), `
				INSERT INTO email_svc.inbox_config (
					mailbox_address, display_name, domain, filters_json, poll_interval_secs,
					module, entity_id, is_active, processing_status, source_type,
					graph_tenant_key, graph_tenant_label, graph_tenant_id, graph_client_id, graph_client_secret,
					imap_provider, imap_host, imap_port, imap_username, imap_password,
					imap_inbox_folder, imap_sent_folder, imap_use_tls,
					owner_user_id, submitted_by, ses_rule_name
				) VALUES ($1, $2, $3, $4::jsonb, $5, NULLIF($6,''), NULLIF($7,''), false,
				          $8, $9,
				          $10, $11, $12, $13, $14,
				          $15, $16, $17, $18, $19,
				          $20, $21, $22,
				          $23, $24, $25)
				RETURNING inbox_id::text
			`, addr, item.DisplayName, domain, string(item.FiltersJSON), item.PollIntervalSecs,
				item.Module, entityID, constants.StatusPendingApproval, sourceType,
				graphTenantKey, graphFields.TenantLabel, graphFields.TenantID, graphFields.ClientID, graphFields.ClientSecret,
				imapFields.Provider, imapFields.Host, imapFields.Port, imapFields.Username, imapFields.Password,
				imapFields.InboxFolder, imapFields.SentFolder, imapFields.UseTLS,
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
			logInboxAudit(r.Context(), pool, inboxID, "CREATE", constants.StatusPendingApproval, userID, "", map[string]interface{}{"mailbox": addr})
			if _, err := submitInboxApproval(r.Context(), pool, inboxID, entityID, "EMAIL_INBOX_CREATE", "CREATE", userID, userEmail); err != nil {
				errors = append(errors, addr+": approval instance: "+err.Error())
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
			emailcommon.RespondBadRequest(w, "invalid body")
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
				errors = append(errors, inboxID+": not found")
				continue
			}

			pending, _ := json.Marshal(item)
			switch status {
			case constants.StatusPendingApproval:
				extraSQL := ""
				extraArgs := []interface{}{}
				argN := 6
				if strings.EqualFold(item.SourceType, "IMAP") && emailmailbox.IMAPPatchPresent(item.MailboxIMAPFields) {
					mailbox, existing, loadErr := emailmailbox.LoadMailboxIMAPFields(r.Context(), pool, inboxID)
					if loadErr != nil {
						errors = append(errors, inboxID+": "+loadErr.Error())
						continue
					}
					merged, mergeErr := emailmailbox.MergeIMAPFields(existing, item.MailboxIMAPFields, mailbox)
					if mergeErr != nil {
						errors = append(errors, inboxID+": imap: "+mergeErr.Error())
						continue
					}
					extraSQL = fmt.Sprintf(`,
					    imap_provider = $%d, imap_host = $%d, imap_port = $%d,
					    imap_username = $%d, imap_password = $%d,
					    imap_inbox_folder = $%d, imap_sent_folder = $%d, imap_use_tls = $%d`,
						argN, argN+1, argN+2, argN+3, argN+4, argN+5, argN+6, argN+7)
					extraArgs = append(extraArgs,
						merged.Provider, merged.Host, merged.Port,
						merged.Username, merged.Password,
						merged.InboxFolder, merged.SentFolder, merged.UseTLS)
					argN += 8
				}
				if strings.EqualFold(item.SourceType, "OUTLOOK_GRAPH") && emailmailbox.GraphPatchPresent(item.MailboxGraphFields) {
					existing, loadErr := emailmailbox.LoadMailboxGraphFields(r.Context(), pool, inboxID)
					if loadErr != nil {
						errors = append(errors, inboxID+": "+loadErr.Error())
						continue
					}
					merged, mergeErr := emailmailbox.MergeGraphFields(existing, item.MailboxGraphFields)
					if mergeErr != nil {
						errors = append(errors, inboxID+": graph: "+mergeErr.Error())
						continue
					}
					extraSQL += fmt.Sprintf(`,
					    graph_tenant_key = $%d,
					    graph_tenant_label = $%d, graph_tenant_id = $%d,
					    graph_client_id = $%d, graph_client_secret = $%d`,
						argN, argN+1, argN+2, argN+3, argN+4)
					extraArgs = append(extraArgs,
						merged.TenantKey, merged.TenantLabel, merged.TenantID, merged.ClientID, merged.ClientSecret)
					argN += 5
				}
				_, err := pool.Exec(r.Context(), fmt.Sprintf(`
					UPDATE email_svc.inbox_config
					SET display_name = COALESCE(NULLIF($2,''), display_name),
					    filters_json = COALESCE($3::jsonb, filters_json),
					    poll_interval_secs = CASE WHEN $4 > 0 THEN $4 ELSE poll_interval_secs END,
					    module = COALESCE(NULLIF($5,''), module),
					    updated_at = now()%s
					WHERE inbox_id = $1::uuid
				`, extraSQL), append([]interface{}{inboxID, item.DisplayName, emailcommon.NullableJSON(item.FiltersJSON), item.PollIntervalSecs, item.Module}, extraArgs...)...)
				if err != nil {
					errors = append(errors, inboxID+": "+err.Error())
					continue
				}
				if _, instErr := submitInboxApproval(r.Context(), pool, inboxID, inboxEntityID, "EMAIL_INBOX_CREATE", "CREATE", userID, userEmail); instErr != nil {
					errors = append(errors, inboxID+": approval instance: "+instErr.Error())
				}
			case constants.StatusApproved:
				_, err := pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = $2,
					    pending_edit_json = $3::jsonb,
					    submitted_by = $4,
					    checker_comment = '',
					    updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, constants.StatusPendingEditApproval, string(pending), userID)
				if err != nil {
					errors = append(errors, inboxID+": "+err.Error())
					continue
				}
				logInboxAudit(r.Context(), pool, inboxID, "EDIT", constants.StatusPendingEditApproval, userID, "", map[string]interface{}{"pending": item})
				if _, instErr := submitInboxApproval(r.Context(), pool, inboxID, inboxEntityID, "EMAIL_INBOX_EDIT", "EDIT", userID, userEmail); instErr != nil {
					errors = append(errors, inboxID+": approval instance: "+instErr.Error())
				}
			default:
				errors = append(errors, inboxID+": cannot edit in status "+status)
				continue
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
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
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
				errors = append(errors, inboxID+": not found")
				continue
			}
			if status == constants.StatusPendingApproval {
				_, _ = pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config SET is_deleted = true, deleted_at = now(), deleted_by = $2, processing_status = 'REJECTED'
					WHERE inbox_id = $1::uuid
				`, inboxID, userID)
				_ = approvalengine.CancelPendingInstances(r.Context(), pool, emailInboxModuleCode, inboxID, userEmail)
				logInboxAudit(r.Context(), pool, inboxID, "DELETE", "CANCELLED", userID, "", nil)
				updated = append(updated, inboxID)
				continue
			}
			if status != constants.StatusApproved {
				errors = append(errors, inboxID+": delete only from APPROVED or PENDING_APPROVAL")
				continue
			}
			_, err := pool.Exec(r.Context(), `
				UPDATE email_svc.inbox_config
				SET processing_status = 'PENDING_DELETE_APPROVAL',
				    submitted_by = $2,
				    checker_comment = '',
				    updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, userID)
			if err != nil {
				errors = append(errors, inboxID+": "+err.Error())
				continue
			}
			logInboxAudit(r.Context(), pool, inboxID, "DELETE", constants.StatusPendingDeleteApproval, userID, "", nil)
			if _, instErr := submitInboxApproval(r.Context(), pool, inboxID, inboxEntityID, "EMAIL_INBOX_DELETE", "DELETE", userID, userEmail); instErr != nil {
				errors = append(errors, inboxID+": approval instance: "+instErr.Error())
			}
			updated = append(updated, inboxID)
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
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}
		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, req.UserID, req.EntityID)
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)
		var approved, errors []string

		for _, inboxID := range req.InboxIDs {
			inboxID = strings.TrimSpace(inboxID)
			var status, submittedBy, entityID, ruleName, pendingEdit string
			err := pool.QueryRow(r.Context(), `
				SELECT processing_status, COALESCE(submitted_by,''), COALESCE(entity_id,''),
				       COALESCE(ses_rule_name,''), COALESCE(pending_edit_json::text,'')
				FROM email_svc.inbox_config WHERE inbox_id = $1::uuid AND is_deleted = false
			`, inboxID).Scan(&status, &submittedBy, &entityID, &ruleName, &pendingEdit)
			if err != nil {
				errors = append(errors, inboxID+": not found")
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
						logInboxAudit(r.Context(), pool, inboxID, "APPROVE", constants.StatusApproved, userID, req.Comment, map[string]interface{}{"engine_instance": actionRes.InstanceID})
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
				logInboxAudit(r.Context(), pool, inboxID, "APPROVE", constants.StatusApproved, userID, req.Comment, nil)

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
						imapFinal := existingIMAP
						graphFinal := existingGraph
						if strings.EqualFold(edit.SourceType, "IMAP") && emailmailbox.IMAPPatchPresent(edit.MailboxIMAPFields) {
							merged, mergeErr := emailmailbox.MergeIMAPFields(existingIMAP, edit.MailboxIMAPFields, mailbox)
							if mergeErr != nil {
								errors = append(errors, inboxID+": imap: "+mergeErr.Error())
								continue
							}
							imapFinal = merged
						}
						if emailmailbox.GraphPatchPresent(edit.MailboxGraphFields) {
							merged, mergeErr := emailmailbox.MergeGraphFields(existingGraph, edit.MailboxGraphFields)
							if mergeErr != nil {
								errors = append(errors, inboxID+": graph: "+mergeErr.Error())
								continue
							}
							graphFinal = merged
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
							    imap_provider = $11, imap_host = $12, imap_port = $13,
							    imap_username = $14, imap_password = $15,
							    imap_inbox_folder = $16, imap_sent_folder = $17, imap_use_tls = $18,
							    processing_status = $19,
							    pending_edit_json = NULL,
							    approved_by = $20,
							    checker_comment = $21,
							    updated_at = now()
							WHERE inbox_id = $1::uuid
						`, inboxID, edit.DisplayName, emailcommon.NullableJSON(edit.FiltersJSON), edit.PollIntervalSecs,
							edit.Module,
							graphFinal.TenantKey, graphFinal.TenantLabel, graphFinal.TenantID, graphFinal.ClientID, graphFinal.ClientSecret,
							imapFinal.Provider, imapFinal.Host, imapFinal.Port,
							imapFinal.Username, imapFinal.Password,
							imapFinal.InboxFolder, imapFinal.SentFolder, imapFinal.UseTLS,
							constants.StatusApproved, userID, req.Comment)
					}
				} else {
					_, err = pool.Exec(r.Context(), `
						UPDATE email_svc.inbox_config
						SET processing_status = $2, pending_edit_json = NULL,
						    approved_by = $3, checker_comment = $4, updated_at = now()
						WHERE inbox_id = $1::uuid
					`, inboxID, constants.StatusApproved, userID, req.Comment)
				}
				logInboxAudit(r.Context(), pool, inboxID, "APPROVE_EDIT", constants.StatusApproved, userID, req.Comment, nil)

			case constants.StatusPendingDeleteApproval:
				var mailbox string
				_ = pool.QueryRow(r.Context(), `SELECT mailbox_address FROM email_svc.inbox_config WHERE inbox_id = $1::uuid`, inboxID).Scan(&mailbox)
				_, err = pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = 'DELETED', is_deleted = true, is_active = false,
					    deleted_at = now(), deleted_by = $2, approved_by = $2,
					    checker_comment = $3, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, userID, req.Comment)
				if ruleName != "" {
					client := mailruntime.NewRuntime()
					if client.Ready() {
						ruleSet := strings.TrimSpace(os.Getenv("EMAIL_SES_RULE_SET_NAME"))
						_ = client.RemoveInboundRule(r.Context(), ruleSet, ruleName)
					}
				}
				logInboxAudit(r.Context(), pool, inboxID, "APPROVE_DELETE", "DELETED", userID, req.Comment, map[string]interface{}{"mailbox": mailbox})
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
			emailcommon.RespondBadRequest(w, "invalid body")
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
				errors = append(errors, inboxID+": not found")
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
					if err := applyInboxDecision(r.Context(), pool, inboxID, txType, approvalengine.InstStatusRejected, userID, req.Comment); err != nil {
						errors = append(errors, inboxID+": "+err.Error())
						continue
					}
					logInboxAudit(r.Context(), pool, inboxID, "REJECT", status, userID, req.Comment, map[string]interface{}{"engine_instance": actionRes.InstanceID})
					rejected = append(rejected, inboxID)
					continue
				}
			}

			switch status {
			case constants.StatusPendingApproval:
				_, err = pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = 'REJECTED', checker_comment = $2, is_active = false, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, req.Comment)
			case constants.StatusPendingEditApproval:
				_, err = pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = $2, pending_edit_json = NULL, checker_comment = $3, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, constants.StatusApproved, req.Comment)
			case constants.StatusPendingDeleteApproval:
				_, err = pool.Exec(r.Context(), `
					UPDATE email_svc.inbox_config
					SET processing_status = $2, checker_comment = $3, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, constants.StatusApproved, req.Comment)
			default:
				errors = append(errors, inboxID+": not pending approval")
				continue
			}
			if err != nil {
				errors = append(errors, inboxID+": "+err.Error())
				continue
			}
			logInboxAudit(r.Context(), pool, inboxID, "REJECT", status, userID, req.Comment, nil)
			rejected = append(rejected, inboxID)
		}
		emailcommon.RespondBulk(w, "inbox/workflow/reject", "rejected", rejected, errors)
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
			emailcommon.RespondForbidden(w, "manual poll is admin/QA only — mailboxes poll automatically after approval")
			return
		}
		started := emailjobs.StartInboundPollAsync(pool)
		graphStarted := emailjobs.StartGraphPollAsync(pool)
		imapStarted := emailjobs.StartIMAPPollAsync(pool)
		emailcommon.RespondPayload(w, "poll/trigger", map[string]interface{}{
			"started":         started || graphStarted || imapStarted,
			"inbound_started": started,
			"graph_started":   graphStarted,
			"imap_started":    imapStarted,
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
			emailcommon.RespondForbidden(w, "manual poll is admin/QA only — mailboxes poll automatically after approval")
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
			emailcommon.RespondForbidden(w, "manual poll is admin/QA only — mailboxes poll automatically after approval")
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
