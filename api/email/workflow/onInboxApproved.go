package emailworkflow

import (
	"context"
	"strings"
	"time"

	emailjobs "CimplrCorpSaas/internal/jobs/email"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OnInboxApproved resets sync cursor to now (no history) and starts background poll/sync.
func OnInboxApproved(pool *pgxpool.Pool, inboxID string) {
	go func() {
		bgCtx := context.Background()
		var sourceType string
		if err := pool.QueryRow(bgCtx, `
			SELECT COALESCE(source_type, 'OUTLOOK_GRAPH')
			FROM email_svc.inbox_config WHERE inbox_id = $1::uuid
		`, inboxID).Scan(&sourceType); err != nil {
			return
		}

		now := time.Now().UTC()
		switch strings.ToUpper(strings.TrimSpace(sourceType)) {
		case "SES":
			result, err := SyncApprovedInboxesToSES(bgCtx, pool)
			ApplyInboundRuleSyncResult(bgCtx, pool, result, err)
			return
		case "IMAP":
			_, _ = pool.Exec(bgCtx, `
				UPDATE email_svc.inbox_config
				SET imap_inbox_last_uid = 0,
				    imap_sent_last_uid = 0,
				    graph_last_sync_at = $2,
				    graph_sent_last_sync_at = $2,
				    ses_sync_status = 'IMAP',
				    ses_last_error = NULL,
				    updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, now)
			if err := emailjobs.TriggerIMAPPoll(bgCtx, pool); err != nil {
				_, _ = pool.Exec(bgCtx, `
					UPDATE email_svc.inbox_config
					SET ses_last_error = $2, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, "imap poll after approve: "+err.Error())
			}
			return
		case "GOOGLE_WORKSPACE":
			_, _ = pool.Exec(bgCtx, `
				UPDATE email_svc.inbox_config
				SET graph_last_sync_at = $2,
				    graph_sent_last_sync_at = $2,
				    ses_sync_status = 'GOOGLE_WORKSPACE',
				    ses_last_error = NULL,
				    updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, now)
			if err := emailjobs.TriggerGoogleWorkspacePoll(bgCtx, pool); err != nil {
				_, _ = pool.Exec(bgCtx, `
					UPDATE email_svc.inbox_config
					SET ses_last_error = $2, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, "google workspace poll after approve: "+err.Error())
			}
			return
		case "OAUTH":
			_, _ = pool.Exec(bgCtx, `
				UPDATE email_svc.inbox_config
				SET imap_inbox_last_uid = 0,
				    imap_sent_last_uid = 0,
				    graph_last_sync_at = $2,
				    graph_sent_last_sync_at = $2,
				    ses_sync_status = 'OAUTH',
				    ses_last_error = NULL,
				    updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, now)
			if err := emailjobs.TriggerOAuthPoll(bgCtx, pool); err != nil {
				_, _ = pool.Exec(bgCtx, `
					UPDATE email_svc.inbox_config
					SET ses_last_error = $2, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, "oauth poll after approve: "+err.Error())
			}
			return
		default:
			_, _ = pool.Exec(bgCtx, `
				UPDATE email_svc.inbox_config
				SET graph_last_sync_at = $2,
				    graph_sent_last_sync_at = $2,
				    ses_sync_status = 'GRAPH',
				    ses_last_error = NULL,
				    updated_at = now()
				WHERE inbox_id = $1::uuid
			`, inboxID, now)
			if err := emailjobs.TriggerGraphPoll(bgCtx, pool); err != nil {
				_, _ = pool.Exec(bgCtx, `
					UPDATE email_svc.inbox_config
					SET ses_last_error = $2, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, inboxID, "graph poll after approve: "+err.Error())
			}
		}
	}()
}
