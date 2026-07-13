package emailcommon

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ActiveInboxMessageSQL excludes messages linked to a deleted or inactive mailbox.
// Bind after list filters; no extra parameters.
const ActiveInboxMessageSQL = `
AND (
	m.inbox_id IS NULL
	OR m.processing_status = 'MANUAL_UPLOAD'
	OR EXISTS (
		SELECT 1
		FROM email_svc.inbox_config active_inbox
		WHERE active_inbox.inbox_id = m.inbox_id
		  AND COALESCE(active_inbox.is_deleted, false) = false
		  AND active_inbox.processing_status = 'APPROVED'
		  AND active_inbox.is_active = true
	)
)`

// DeleteMessagesForInbox removes all messages (and child rows) for a mailbox.
func DeleteMessagesForInbox(ctx context.Context, pool *pgxpool.Pool, inboxID string) error {
	if inboxID == "" {
		return nil
	}
	_, err := pool.Exec(ctx, `
		WITH to_delete AS (
			SELECT message_id FROM email_svc.message WHERE inbox_id = $1::uuid
		)
		DELETE FROM email_svc.processing_log pl
		WHERE pl.message_id IN (SELECT message_id FROM to_delete)
	`, inboxID)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
		WITH to_delete AS (
			SELECT message_id FROM email_svc.message WHERE inbox_id = $1::uuid
		)
		DELETE FROM email_svc.message_attachment ma
		WHERE ma.message_id IN (SELECT message_id FROM to_delete)
	`, inboxID)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `DELETE FROM email_svc.message WHERE inbox_id = $1::uuid`, inboxID)
	return err
}
