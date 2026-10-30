package emailcommon

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	middlewares "CimplrCorpSaas/api/middlewares"
	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

func IsEmailAdmin(ctx context.Context, userID string) bool {
	if v, ok := ctx.Value("is_admin_override").(bool); ok && v {
		return true
	}
	return middlewares.IsAdminOverrideEnabled() && middlewares.IsAdminUser(userID)
}

func normalizeEmail(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

type approvedInbox struct {
	InboxID        string
	MailboxAddress string
	FiltersJSON    []byte
	Module         string
	EntityID       string
	OwnerUserID    string
}

// ListMessageScopedToUserSQL — after filters $1–$6: $7 filterMatchedOnly, $8 admin, $9 userID, $10 userEmail.
const ListMessageScopedToUserSQL = `
AND (
	$8::boolean
	OR EXISTS (
		SELECT 1
		FROM email_svc.inbox_config i
		WHERE i.is_deleted = false
		  AND i.processing_status = 'APPROVED'
		  AND i.is_active = true
		  AND (
		      i.owner_user_id = $9
		      OR ($10 <> '' AND LOWER(i.mailbox_address) = LOWER($10))
		      OR EXISTS (
		          SELECT 1 FROM email_svc.inbox_members im
		          WHERE im.inbox_id = i.inbox_id AND im.user_id = $9
		      )
		  )
		  AND (
		      (m.inbox_id IS NOT NULL AND i.inbox_id = m.inbox_id)
		      OR LOWER(COALESCE(m.envelope_from, '')) = LOWER(i.mailbox_address)
		      OR LOWER(i.mailbox_address) = ANY (
		          SELECT LOWER(unnest(COALESCE(m.envelope_to, ARRAY[]::text[])))
		      )
		  )
	)
	OR (
		m.processing_status = 'MANUAL_UPLOAD'
		AND EXISTS (
			SELECT 1 FROM email_svc.processing_log upl_self
			WHERE upl_self.message_id = m.message_id
			  AND upl_self.step = 'UPLOAD_EML'
			  AND (
			      upl_self.detail->>'uploaded_by' = $9
			      OR ($10 <> '' AND upl_self.detail->>'uploaded_by' = $10)
			  )
		)
	)
)`

// MessageScopedToUserSQL is kept for legacy callers ($6 admin, $7 userID, $8 userEmail).
const MessageScopedToUserSQL = `
AND (
	$6::boolean
	OR EXISTS (
		SELECT 1
		FROM email_svc.inbox_config i
		WHERE i.is_deleted = false
		  AND i.processing_status = 'APPROVED'
		  AND i.is_active = true
		  AND (
		      i.owner_user_id = $7
		      OR ($8 <> '' AND LOWER(i.mailbox_address) = LOWER($8))
		      OR EXISTS (
		          SELECT 1 FROM email_svc.inbox_members im
		          WHERE im.inbox_id = i.inbox_id AND im.user_id = $7
		      )
		  )
		  AND (
		      (m.inbox_id IS NOT NULL AND i.inbox_id = m.inbox_id)
		      OR LOWER(COALESCE(m.envelope_from, '')) = LOWER(i.mailbox_address)
		      OR LOWER(i.mailbox_address) = ANY (
		          SELECT LOWER(unnest(COALESCE(m.envelope_to, ARRAY[]::text[])))
		      )
		  )
	)
	OR (
		m.processing_status = 'MANUAL_UPLOAD'
		AND EXISTS (
			SELECT 1 FROM email_svc.processing_log upl_self
			WHERE upl_self.message_id = m.message_id
			  AND upl_self.step = 'UPLOAD_EML'
			  AND (
			      upl_self.detail->>'uploaded_by' = $7
			      OR ($8 <> '' AND upl_self.detail->>'uploaded_by' = $8)
			  )
		)
	)
)`

// joinedMessageScopedToUserSQL for attachment download ($2 admin, $3 userID, $4 userEmail; alias m)
const JoinedMessageScopedToUserSQL = `
AND (
	$2::boolean
	OR EXISTS (
		SELECT 1
		FROM email_svc.inbox_config i
		WHERE i.is_deleted = false
		  AND i.processing_status = 'APPROVED'
		  AND i.is_active = true
		  AND (
		      i.owner_user_id = $3
		      OR ($4 <> '' AND LOWER(i.mailbox_address) = LOWER($4))
		      OR EXISTS (
		          SELECT 1 FROM email_svc.inbox_members im
		          WHERE im.inbox_id = i.inbox_id AND im.user_id = $3
		      )
		  )
		  AND (
		      (m.inbox_id IS NOT NULL AND i.inbox_id = m.inbox_id)
		      OR LOWER(COALESCE(m.envelope_from, '')) = LOWER(i.mailbox_address)
		      OR LOWER(i.mailbox_address) = ANY (
		          SELECT LOWER(unnest(COALESCE(m.envelope_to, ARRAY[]::text[])))
		      )
		  )
	)
)`

// singleMessageScopedToUserSQL for handleMessageGet ($2 admin, $3 userID, $4 userEmail)
const SingleMessageScopedToUserSQL = `
AND (
	$2::boolean
	OR EXISTS (
		SELECT 1
		FROM email_svc.inbox_config i
		WHERE i.is_deleted = false
		  AND i.processing_status = 'APPROVED'
		  AND i.is_active = true
		  AND (
		      i.owner_user_id = $3
		      OR ($4 <> '' AND LOWER(i.mailbox_address) = LOWER($4))
		      OR EXISTS (
		          SELECT 1 FROM email_svc.inbox_members im
		          WHERE im.inbox_id = i.inbox_id AND im.user_id = $3
		      )
		  )
		  AND (
		      (email_svc.message.inbox_id IS NOT NULL AND i.inbox_id = email_svc.message.inbox_id)
		      OR LOWER(COALESCE(email_svc.message.envelope_from, '')) = LOWER(i.mailbox_address)
		      OR LOWER(i.mailbox_address) = ANY (
		          SELECT LOWER(unnest(COALESCE(email_svc.message.envelope_to, ARRAY[]::text[])))
		      )
		  )
	)
	OR (
		email_svc.message.processing_status = 'MANUAL_UPLOAD'
		AND EXISTS (
			SELECT 1 FROM email_svc.processing_log upl_self
			WHERE upl_self.message_id = email_svc.message.message_id
			  AND upl_self.step = 'UPLOAD_EML'
			  AND (
			      upl_self.detail->>'uploaded_by' = $3
			      OR ($4 <> '' AND upl_self.detail->>'uploaded_by' = $4)
			  )
		)
	)
)`

func userCanAccessInboxRecord(inbox approvedInbox, userID, userEmail string, entityIDs []string, admin bool) bool {
	if admin {
		return true
	}
	if inbox.OwnerUserID != "" && inbox.OwnerUserID == userID {
		return true
	}
	if userEmail != "" && normalizeEmail(inbox.MailboxAddress) == normalizeEmail(userEmail) {
		return true
	}
	return false
}

func loadApprovedInboxes(ctx context.Context, pool *pgxpool.Pool) ([]approvedInbox, error) {
	rows, err := pool.Query(ctx, `
		SELECT inbox_id::text, mailbox_address, filters_json, COALESCE(module,''), COALESCE(entity_id,''),
		       COALESCE(owner_user_id,'')
		FROM email_svc.inbox_config
		WHERE is_deleted = false
		  AND processing_status = 'APPROVED'
		  AND is_active = true
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]approvedInbox, 0)
	for rows.Next() {
		var item approvedInbox
		if err := rows.Scan(&item.InboxID, &item.MailboxAddress, &item.FiltersJSON, &item.Module, &item.EntityID, &item.OwnerUserID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func FilterMatchInput(msg mailruntime.ParsedEmail) emailjobs.FilterMatchInput {
	names := make([]string, 0, len(msg.Attachments))
	for _, att := range msg.Attachments {
		names = append(names, att.Filename)
	}
	return emailjobs.FilterMatchInput{
		From:            msg.Envelope.From,
		To:              msg.Envelope.To,
		Subject:         msg.Envelope.Subject,
		HasAttachments:  len(msg.Attachments) > 0,
		AttachmentNames: names,
	}
}

// ManualUploadIdentity carries the requesting user's identity/scope for
// ResolveManualUploadInbox, keeping the function signature under the
// project's parameter-count limit.
type ManualUploadIdentity struct {
	UserID           string
	UserEmail        string
	EntityIDs        []string
	Admin            bool
	SkipInboxFilters bool
}

func ResolveManualUploadInbox(
	ctx context.Context,
	pool *pgxpool.Pool,
	msg mailruntime.ParsedEmail,
	identity ManualUploadIdentity,
) (*approvedInbox, string, error) {
	userID, userEmail, entityIDs, admin, skipInboxFilters :=
		identity.UserID, identity.UserEmail, identity.EntityIDs, identity.Admin, identity.SkipInboxFilters
	inboxes, err := loadApprovedInboxes(ctx, pool)
	if err != nil {
		return nil, "", err
	}
	if len(inboxes) == 0 {
		return nil, "", fmt.Errorf("no approved mailboxes configured — add and approve a mailbox first")
	}

	matchInput := FilterMatchInput(msg)
	from := normalizeEmail(msg.Envelope.From)

	tryInbox := func(inbox approvedInbox, direction string) (*approvedInbox, string, bool) {
		mailbox := normalizeEmail(inbox.MailboxAddress)
		switch direction {
		case "SENT":
			if from != mailbox {
				return nil, "", false
			}
			if !skipInboxFilters && !emailjobs.MatchSentFilters(inbox.FiltersJSON, matchInput) {
				return nil, "", false
			}
		default:
			if !emailjobs.MailboxMatchesRecipient(inbox.MailboxAddress, msg.Envelope.To) {
				return nil, "", false
			}
			if !skipInboxFilters && !emailjobs.MatchInboundFilters(inbox.FiltersJSON, matchInput) {
				return nil, "", false
			}
		}

		canAccess := userCanAccessInboxRecord(inbox, userID, userEmail, entityIDs, admin)
		if !canAccess {
			var shared bool
			_ = pool.QueryRow(ctx, `
				SELECT EXISTS(SELECT 1 FROM email_svc.inbox_members WHERE inbox_id = $1::uuid AND user_id = $2)
			`, inbox.InboxID, userID).Scan(&shared)
			canAccess = shared
		}
		if !canAccess {
			return nil, "", false
		}
		return &inbox, direction, true
	}

	for _, inbox := range inboxes {
		if hit, dir, ok := tryInbox(inbox, "RECEIVED"); ok {
			return hit, dir, nil
		}
	}
	for _, inbox := range inboxes {
		if hit, dir, ok := tryInbox(inbox, "SENT"); ok {
			return hit, dir, nil
		}
	}

	return nil, "", fmt.Errorf(
		"upload rejected: from/to must match an approved mailbox you can access (from=%s to=%v)",
		strings.TrimSpace(msg.Envelope.From), msg.Envelope.To,
	)
}

// CanManageInbox reports whether the user may act on the inbox: admin, owner,
// submitter, member, or an inbox entity inside the caller's entity scope.
func CanManageInbox(ctx context.Context, pool *pgxpool.Pool, inboxID, userID string) bool {
	inboxID = strings.TrimSpace(inboxID)
	if inboxID == "" || userID == "" {
		return false
	}
	if IsEmailAdmin(ctx, userID) {
		return true
	}
	var owner, submitter, entityID string
	var member bool
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(i.owner_user_id,''), COALESCE(i.submitted_by,''), COALESCE(i.entity_id,''),
		       EXISTS(SELECT 1 FROM email_svc.inbox_members im WHERE im.inbox_id = i.inbox_id AND im.user_id = $2)
		FROM email_svc.inbox_config i
		WHERE i.inbox_id::text = $1 AND i.is_deleted = false
	`, inboxID, userID).Scan(&owner, &submitter, &entityID, &member)
	if err != nil {
		return false
	}
	if owner == userID || submitter == userID || member {
		return true
	}
	return entityID != "" && EntityInScope(entityID, api.GetEntityIDsFromCtx(ctx))
}

// RequireMessageAccess writes a 403 (or 500) unless the caller can see every
// message the query selects (one message_id per row).
func RequireMessageAccess(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, messageQuery string, args ...any) bool {
	ctx := r.Context()
	userID, userEmail, _, _ := RequestIdentity(r, "", "")
	admin := IsEmailAdmin(ctx, userID)
	if admin {
		return true
	}
	n := len(args)
	q := fmt.Sprintf(`
		SELECT COUNT(*) FILTER (WHERE NOT EXISTS (
			SELECT 1 FROM email_svc.message m
			WHERE m.message_id = t.message_id %s
		))
		FROM (%s) t(message_id)`, ListMessageScopedToUserSQLAt(n+1, n+2, n+3), messageQuery)
	var denied int
	if err := pool.QueryRow(ctx, q, append(args, admin, userID, userEmail)...).Scan(&denied); err != nil {
		api.LogErrorForResponse(w, "email message access lookup: %v", err)
		RespondInternal(w, "failed to verify message access")
		return false
	}
	if denied > 0 {
		RespondForbidden(w, "message not accessible")
		return false
	}
	return true
}

const (
	MessageByIDQuery      = `SELECT $1::uuid`
	MessageByResultQuery  = `SELECT ma.message_id FROM email_svc.transformation_results tr JOIN email_svc.message_attachment ma ON ma.attachment_id = tr.attachment_id WHERE tr.result_id = $1::uuid`
	MessageByResultsQuery = `SELECT ma.message_id FROM email_svc.transformation_results tr JOIN email_svc.message_attachment ma ON ma.attachment_id = tr.attachment_id WHERE tr.result_id::text = ANY($1)`
)
