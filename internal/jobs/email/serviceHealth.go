package emailjobs

import (
	"context"
	"fmt"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// mailEngineUnavailablePrefix tags every ses_last_error value written by the
// mail-engine-wide health check below (as opposed to a per-mailbox poll
// error a specific poller writes on its own), so clearSubscriptionExpiredErrors
// can find-and-clear exactly the rows it previously set once health recovers,
// without touching unrelated per-mailbox errors (bad IMAP creds, revoked
// OAuth token, etc.) that have nothing to do with overall mail-engine health.
const mailEngineUnavailablePrefix = "mail engine unavailable: "

func mailEngineUnavailableMsg(err error) string {
	return mailEngineUnavailablePrefix + err.Error()
}

// EmailServiceHealthy reports whether the mail engine (internal/mailengine,
// reached via mailruntime) is actually usable right now — AWS config
// resolves and the inbound S3 bucket is reachable.
func EmailServiceHealthy(ctx context.Context) bool {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return false
	}
	return rt.HealthCheck(ctx) == nil
}

// SyncEmailServiceStatus updates mailbox ses_last_error from a live mail-engine
// health check (list refresh). The returned string is the real failure detail
// (empty when healthy) — e.g. "mail engine unavailable: s3 bucket cimplr
// unreachable: ...", not a generic "service is down" message, since there is
// no longer a separate process that can simply be "not running": a failure
// here means AWS/S3 configuration, not a stopped service.
func SyncEmailServiceStatus(ctx context.Context, pool *pgxpool.Pool) (bool, string) {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return false, "mail processing not configured"
	}
	if err := rt.HealthCheck(ctx); err != nil {
		detail := mailEngineUnavailableMsg(err)
		logger.LogError("[email-service] health check failed: %v", err)
		markPollingMailboxesSubscriptionExpired(ctx, pool, detail)
		return false, detail
	}
	clearSubscriptionExpiredErrors(ctx, pool)
	return true, ""
}

// RequireEmailService blocks poll/ingest when the mail engine is unavailable.
func RequireEmailService(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime) error {
	if rt == nil || !rt.Ready() {
		return fmt.Errorf("mail processing not configured")
	}
	if err := rt.HealthCheck(ctx); err != nil {
		detail := mailEngineUnavailableMsg(err)
		logger.LogError("[email-service] poll blocked — health check failed: %v", err)
		markPollingMailboxesSubscriptionExpired(ctx, pool, detail)
		return fmt.Errorf("%s", detail)
	}
	return nil
}

func markPollingMailboxesSubscriptionExpired(ctx context.Context, pool *pgxpool.Pool, detail string) {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET ses_last_error = $1, updated_at = now()
		WHERE is_deleted = false
		  AND processing_status = 'APPROVED'
		  AND COALESCE(source_type, 'OUTLOOK_GRAPH') IN (
		    'OUTLOOK_GRAPH', 'GOOGLE_WORKSPACE', 'IMAP', 'OAUTH', 'SES'
		  )
	`, detail)
	if err != nil {
		logger.LogError("[email-service] mark subscription expired: %v", err)
	}
}

func clearSubscriptionExpiredErrors(ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET ses_last_error = NULL, updated_at = now()
		WHERE is_deleted = false
		  AND ses_last_error LIKE $1
	`, mailEngineUnavailablePrefix+"%")
	if err != nil {
		logger.LogError("[email-service] clear subscription expired: %v", err)
	}
	// Legacy rows written before this change still carry the old generic
	// constant verbatim — clear those too so they don't linger forever.
	_, err = pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET ses_last_error = NULL, updated_at = now()
		WHERE is_deleted = false
		  AND ses_last_error = 'Email subscription expired'
	`)
	if err != nil {
		logger.LogError("[email-service] clear legacy subscription expired: %v", err)
	}
}

func clearMailboxPollError(ctx context.Context, pool *pgxpool.Pool, inboxID string) {
	_, _ = pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET ses_last_error = NULL, updated_at = now()
		WHERE inbox_id = $1::uuid
		  AND COALESCE(ses_last_error, '') <> ''
	`, inboxID)
}
