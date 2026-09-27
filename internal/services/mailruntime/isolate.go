package mailruntime

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// runIsolated runs fn on its own goroutine so the mail engine (IMAP/Graph/
// Gmail/SES/S3 libraries, MIME parsing) can never take the shared server
// process down with it: a panic anywhere inside fn is recovered and returned
// as an error instead of crashing the process, and a call that hangs past
// timeout returns a timeout error instead of blocking its caller forever.
//
// This does not reclaim the goroutine if fn ignores ctx cancellation — fn
// should respect ctx for the timeout to actually free the underlying
// network call, which every mail-engine call here already does (they all
// thread ctx into their HTTP/IMAP round trips).
func runIsolated(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	if timeout <= 0 {
		timeout = defaultTimeout()
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				done <- fmt.Errorf("mail engine panic: %v", rec)
			}
		}()
		done <- fn(runCtx)
	}()

	select {
	case err := <-done:
		return err
	case <-runCtx.Done():
		return fmt.Errorf("mail engine operation timed out: %w", runCtx.Err())
	}
}

func defaultTimeout() time.Duration {
	return 5 * time.Minute
}

func pullTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv("MAIL_RUNTIME_PULL_TIMEOUT_SECS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 10 * time.Minute
}

func healthCheckTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv("MAIL_RUNTIME_HEALTH_TIMEOUT_SECS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 5 * time.Second
}
