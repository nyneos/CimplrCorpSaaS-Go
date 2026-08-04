package jobs

import (
	"context"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NudgeDeliveryAfterEnqueue runs one cycle of the same work as the background
// workers' poll ticks: EMAIL outbox → send endpoint, PUSH outbox → VAPID, and
// in-app SSE inbox for connected users. Without this, new rows wait up to
// OUTBOX_WORKER_POLL_SECS (default 10s), BROWSER_PUSH_POLL_SECS (default 15s),
// or IN_APP_WORKER_POLL_SECS before the first delivery — so back-to-back actions
// often arrive in a single batch and feel "delayed" or merged.
//
// Set NOTIF_NUDGE_AFTER_ENQUEUE=false to disable and rely on periodic polling only.
func NudgeDeliveryAfterEnqueue(ctx context.Context, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	switch strings.TrimSpace(strings.ToLower(os.Getenv("NOTIF_NUDGE_AFTER_ENQUEUE"))) {
	case "false", "0", "no", "off":
		return
	}

	if owGetenvBool("OUTBOX_WORKER_ENABLED", true) {
		target := strings.TrimSpace(os.Getenv("SEND_ENDPOINT_URL"))
		if target == "" {
			target = resolveRoute()
		}
		if target != "" {
			batch := owGetenvInt("OUTBOX_WORKER_BATCH_SIZE", 50)
			owProcessBatch(ctx, pool, target, batch)
		}
	}

	if bpwGetenvBool("BROWSER_PUSH_ENABLED", true) {
		pub := strings.TrimSpace(os.Getenv("VAPID_PUBLIC_KEY"))
		priv := strings.TrimSpace(os.Getenv("VAPID_PRIVATE_KEY"))
		subj := strings.TrimSpace(os.Getenv("VAPID_SUBJECT"))
		if pub != "" && priv != "" && subj != "" {
			cfg := &vapidConfig{public: pub, private: priv, subject: subj}
			b := bpwGetenvInt("BROWSER_PUSH_BATCH_SIZE", 50)
			bpwProcessOutbox(ctx, pool, cfg, b)
		}
	}

	if iawGetenvBool("IN_APP_WORKER_ENABLED", true) {
		b := iawGetenvInt("IN_APP_WORKER_BATCH", 50)
		_ = iawProcessBatch(ctx, pool, b)
	}
}
