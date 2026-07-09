package emailjobs

import (
	"context"
	"sync"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	inboundPollMu sync.Mutex
	graphPollMu   sync.Mutex
	imapPollMu    sync.Mutex
)

// StartInboundPollAsync runs one S3 poll cycle in the background. Returns false if already running.
func StartInboundPollAsync(pool *pgxpool.Pool) bool {
	if !inboundPollMu.TryLock() {
		return false
	}
	go func() {
		defer inboundPollMu.Unlock()
		if err := TriggerInboundPoll(context.Background(), pool); err != nil {
			logger.LogError("[email-poller] manual trigger: %v", err)
		}
	}()
	return true
}

// StartGraphPollAsync runs one Graph poll cycle in the background. Returns false if already running.
func StartGraphPollAsync(pool *pgxpool.Pool) bool {
	if !graphPollMu.TryLock() {
		return false
	}
	go func() {
		defer graphPollMu.Unlock()
		if err := TriggerGraphPoll(context.Background(), pool); err != nil {
			logger.LogError("[graph-poller] manual trigger: %v", err)
		}
	}()
	return true
}

// StartIMAPPollAsync runs one IMAP poll cycle in the background. Returns false if already running.
func StartIMAPPollAsync(pool *pgxpool.Pool) bool {
	if !imapPollMu.TryLock() {
		return false
	}
	go func() {
		defer imapPollMu.Unlock()
		if err := TriggerIMAPPoll(context.Background(), pool); err != nil {
			logger.LogError("[imap-poller] manual trigger: %v", err)
		}
	}()
	return true
}
