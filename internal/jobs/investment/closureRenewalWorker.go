package jobs

import (
	"context"
	"os"
	"strconv"
	"time"

	fdclosure "CimplrCorpSaas/api/investment/fdMaturityAndRollover"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartCimplrAutoMaturityWorker runs the auto-maturity sweep (and once at startup)
// to auto-create and approve payout/rollover confirms for approved initiates
// on or after FD maturity date.
//
// Tick interval is configurable via env FD_AUTO_MATURITY_INTERVAL_MINUTES
// (default 60). The previous 24h default made the worker effectively invisible
// in dev — at 60 minutes you still avoid load while getting per-hour visibility.
func StartCimplrAutoMaturityWorker(db *pgxpool.Pool) {
	intervalMinutes := 60
	if raw := os.Getenv("FD_AUTO_MATURITY_INTERVAL_MINUTES"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			intervalMinutes = v
		}
	}
	interval := time.Duration(intervalMinutes) * time.Minute

	logger.LogInfo("[CimplrAutoMaturity] worker started — interval=%s, first run NOW", interval)
	runCimplrAutoMaturity(db)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		runCimplrAutoMaturity(db)
	}
}

func runCimplrAutoMaturity(db *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	logger.LogInfo("[CimplrAutoMaturity] run started at %s", time.Now().Format(time.RFC3339))
	ok, skip, fail := fdclosure.RunCimplrAutoMaturityDue(ctx, db)
	logger.LogInfo("[CimplrAutoMaturity] run done processed=%d skipped=%d failed=%d at %s",
		ok, skip, fail, time.Now().Format(time.RFC3339))
}
