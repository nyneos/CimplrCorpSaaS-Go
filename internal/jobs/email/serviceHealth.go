package emailjobs

import (
	"context"
	"fmt"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EmailSubscriptionExpiredMsg is shown in mailbox sync column when email service is down.
const EmailSubscriptionExpiredMsg = "Email subscription expired"

// EmailServiceHealthy reports whether the standalone email service responds on /v1/health.
func EmailServiceHealthy(ctx context.Context) bool {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return false
	}
	return rt.HealthCheck(ctx) == nil
}

// SyncEmailServiceStatus updates mailbox ses_last_error from live email-service health (list refresh).
func SyncEmailServiceStatus(ctx context.Context, pool *pgxpool.Pool) bool {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return false
	}
	if err := rt.HealthCheck(ctx); err != nil {
		logger.LogError("[email-service] health check failed: %v", err)
		markPollingMailboxesSubscriptionExpired(ctx, pool)
		return false
	}
	clearSubscriptionExpiredErrors(ctx, pool)
	return true
}

// RequireEmailService blocks poll/ingest when the standalone email service is unavailable.
func RequireEmailService(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime) error {
	if rt == nil || !rt.Ready() {
		return fmt.Errorf("mail processing not configured")
	}
	if err := rt.HealthCheck(ctx); err != nil {
		logger.LogError("[email-service] poll blocked — health check failed: %v", err)
		markPollingMailboxesSubscriptionExpired(ctx, pool)
		return fmt.Errorf("%s", EmailSubscriptionExpiredMsg)
	}
	return nil
}

func markPollingMailboxesSubscriptionExpired(ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET ses_last_error = $1, updated_at = now()
		WHERE is_deleted = false
		  AND processing_status = 'APPROVED'
		  AND COALESCE(source_type, 'OUTLOOK_GRAPH') IN (
		    'OUTLOOK_GRAPH', 'GOOGLE_WORKSPACE', 'IMAP', 'OAUTH', 'SES'
		  )
	`, EmailSubscriptionExpiredMsg)
	if err != nil {
		logger.LogError("[email-service] mark subscription expired: %v", err)
	}
}

func clearSubscriptionExpiredErrors(ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET ses_last_error = NULL, updated_at = now()
		WHERE is_deleted = false
		  AND ses_last_error = $1
	`, EmailSubscriptionExpiredMsg)
	if err != nil {
		logger.LogError("[email-service] clear subscription expired: %v", err)
	}
}

func clearMailboxPollError(ctx context.Context, pool *pgxpool.Pool, inboxID string) {
	_, _ = pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET ses_last_error = NULL, updated_at = now()
		WHERE inbox_id = $1::uuid AND ses_last_error = $2
	`, inboxID, EmailSubscriptionExpiredMsg)
}
