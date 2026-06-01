package jobs

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	approvalengine "CimplrCorpSaas/api/approvalengine"
	fdAccrual "CimplrCorpSaas/api/investment/fdAccrual"
	cashjobs "CimplrCorpSaas/internal/jobs/cash"
	dinojobs "CimplrCorpSaas/internal/jobs/dino"
	investmentjobs "CimplrCorpSaas/internal/jobs/investment"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/serviceiface"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CronService struct {
	config map[string]interface{}
	db     *pgxpool.Pool
}

func NewCronService(cfg map[string]interface{}, db *pgxpool.Pool) serviceiface.Service {
	return &CronService{
		config: cfg,
		db:     db,
	}
}

func (s *CronService) Name() string {
	return "cron"
}

func (s *CronService) Start() error {
	logger.LogInfo("Starting cron service...")

	// ---------------- AMFI ----------------
	amfiConfig := investmentjobs.NewDefaultConfig()
	if s.config != nil {
		if batchSize, ok := s.config["batch_size"].(int); ok && batchSize > 0 {
			amfiConfig.BatchSize = batchSize
		}
	}

	if err := investmentjobs.RunAMFIDataDownloader(amfiConfig, s.db); err != nil {
		return fmt.Errorf("failed to start AMFI data downloader: %v", err)
	}
	logger.GlobalLogger.LogAudit("Cron service started with AMFI downloader")

	// // ---------------- Sweep V1 ----------------
	// sweepConfig := cashjobs.NewDefaultSweepConfig()
	// if s.config != nil {
	// 	if val, ok := s.config["sweep_schedule"].(string); ok && val != "" {
	// 		sweepConfig.Schedule = val
	// 	}
	// 	if val, ok := s.config["sweep_batch_size"].(int); ok && val > 0 {
	// 		sweepConfig.BatchSize = val
	// 	}
	// }

	// if err := cashjobs.RunSweepScheduler(sweepConfig, s.db); err != nil {
	// 	return fmt.Errorf("failed to start sweep scheduler: %v", err)
	// }
	// logger.GlobalLogger.LogAudit("Sweep scheduler started")

	// ---------------- Sweep V2 ----------------
	sweepConfigV2 := cashjobs.NewDefaultSweepConfigV2()
	if s.config != nil {
		if val, ok := s.config["sweep_schedule_v2"].(string); ok && val != "" {
			sweepConfigV2.Schedule = val
		}
		if val, ok := s.config["sweep_batch_size_v2"].(int); ok && val > 0 {
			sweepConfigV2.BatchSize = val
		}
	}

	if err := cashjobs.RunSweepSchedulerV2(sweepConfigV2, s.db); err != nil {
		return fmt.Errorf("failed to start sweep V2 scheduler: %v", err)
	}
	logger.GlobalLogger.LogAudit("Sweep V2 scheduler started")

	// ---------------- Categorization ----------------
	categorizationConfig := cashjobs.NewDefaultCategorizationConfig()
	if s.config != nil {
		if val, ok := s.config["categorization_schedule"].(string); ok && val != "" {
			categorizationConfig.Schedule = val
		}
		if val, ok := s.config["categorization_batch_size"].(int); ok && val > 0 {
			categorizationConfig.BatchSize = val
		}
	}

	if err := cashjobs.RunCategorizationScheduler(categorizationConfig, s.db); err != nil {
		return fmt.Errorf("failed to start categorization scheduler: %v", err)
	}
	logger.GlobalLogger.LogAudit("Auto-categorization scheduler started")

	// ---------------- Background Workers ----------------
	ctx := context.Background()

	go dinojobs.StartOutboxWorker(ctx, s.db)
	go dinojobs.StartInboxWorker(ctx, s.db)
	go dinojobs.StartBrowserPushWorker(ctx, s.db)
	go approvalengine.StartSLAWorker(ctx, s.db)
	go fdAccrual.StartAccrualSchedulerWorker(s.db)
	go investmentjobs.StartReceiptReconcileWorker(s.db)
	// Legacy auto-renewal worker (StartAutoRenewalWorker) is intentionally
	// disabled. It writes ROLLOVER closures into investment.fd_closure_request,
	// which no Cimplr-tab UI reads from, and would create duplicate parallel
	// closures alongside StartCimplrAutoMaturityWorker. The Cimplr worker
	// below is the single source of truth for auto-maturity now.
	//
	// go investmentjobs.StartAutoRenewalWorker(s.db)
	// go investmentjobs.StartAutoRenewalWorker(s.db) // Deprecated: Replaced by new CimplrAutoMaturity worker
	go investmentjobs.StartCimplrAutoMaturityWorker(s.db)

	logger.GlobalLogger.LogAudit("All background workers started")

	// ---------------- DB Cleanup Worker ----------------
	go s.startDBCleanupWorker()
	logger.GlobalLogger.LogAudit("DB cleanup worker started")
	logger.LogInfo("Cron service started — DB Cleanup Worker running")

	// ---------------- DB Health Monitor Worker ----------------
	go s.startDBHealthMonitorWorker()
	logger.GlobalLogger.LogAudit("DB health monitor worker started")
	log.Println("Cron service started — DB Health Monitor Worker running")

	return nil
}

func (s *CronService) Stop() error {
	logger.LogInfo("Cron service stopped.")
	return nil
}

//
// ---------------- DB CLEANUP WORKER ----------------
//

func (s *CronService) startDBCleanupWorker() {
	interval := 100 // default seconds (safe)
	enabled := false

	// config override
	if s.config != nil {
		if val, ok := s.config["db_cleanup_interval"].(int); ok && val > 0 {
			interval = val
		}
		if val, ok := asBool(s.config["db_cleanup_enabled"]); ok {
			enabled = val
		}
	}

	if !enabled {
		log.Println("DB Cleanup Worker disabled (set db_cleanup_enabled=true to enable)")
		return
	}

	logger.DBLogf("DB Cleanup Worker running every %d seconds", interval)

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.cleanupIdleConnections()
	}
}

func (s *CronService) cleanupIdleConnections() {
	query := `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE usename = current_user
		  AND pid <> pg_backend_pid()
		  AND state = 'idle'
		  AND now() - state_change > interval '5 minutes';
	`

	tag, err := s.db.Exec(context.Background(), query)
	if err != nil {
		logger.LogError("DB cleanup error: %v", err)
		if logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit("DB cleanup failed: " + err.Error())
		}
		return
	}

	logger.DBLogf("DB cleanup executed. Rows affected: %d", tag.RowsAffected())
}

func (s *CronService) startDBHealthMonitorWorker() {
	interval := 30
	connUtilizationPct := 85
	longRunningSeconds := 20
	blockedThreshold := 1
	maxConnFallback := 100

	if s.config != nil {
		if val, ok := asInt(s.config["db_health_interval_seconds"]); ok && val > 0 {
			interval = val
		}
		if val, ok := asInt(s.config["db_health_conn_utilization_pct"]); ok && val > 0 && val <= 100 {
			connUtilizationPct = val
		}
		if val, ok := asInt(s.config["db_health_long_running_seconds"]); ok && val > 0 {
			longRunningSeconds = val
		}
		if val, ok := asInt(s.config["db_health_blocked_threshold"]); ok && val >= 0 {
			blockedThreshold = val
		}
	}

	logger.DBLogf(
		"DB Health Monitor running every %ds (conn_util>=%d%%, long_query>=%ds, blocked>=%d)",
		interval, connUtilizationPct, longRunningSeconds, blockedThreshold,
	)

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.checkDBHealth(connUtilizationPct, longRunningSeconds, blockedThreshold, maxConnFallback)
	}
}

func (s *CronService) checkDBHealth(connUtilizationPct, longRunningSeconds, blockedThreshold, maxConnFallback int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const metricsQ = `
		SELECT
		  (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()) AS total_connections,
		  (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'active') AS active_connections,
		  (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock') AS blocked_connections,
		  COALESCE(current_setting('max_connections', true), '') AS max_connections_setting
	`
	var totalConns, activeConns, blockedConns int
	var maxConnSetting string
	if err := s.db.QueryRow(ctx, metricsQ).Scan(&totalConns, &activeConns, &blockedConns, &maxConnSetting); err != nil {
		logger.DBLogf("DB health monitor: metrics query failed: %v", err)
		return
	}

	maxConns := maxConnFallback
	if parsed, err := strconv.Atoi(strings.TrimSpace(maxConnSetting)); err == nil && parsed > 0 {
		maxConns = parsed
	}
	utilPct := 0
	if maxConns > 0 {
		utilPct = (totalConns * 100) / maxConns
	}

	healthy := utilPct < connUtilizationPct && blockedConns <= blockedThreshold
	if healthy {
		return
	}

	logger.DBLogf(
		"[DB_UNHEALTHY] total=%d active=%d blocked=%d max=%d util=%d%%",
		totalConns, activeConns, blockedConns, maxConns, utilPct,
	)

	s.logTopLongRunningQueries(longRunningSeconds)
	s.logBlockedQueryPairs()
}

func (s *CronService) logTopLongRunningQueries(longRunningSeconds int) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	const longQ = `
		SELECT
		  pid,
		  usename,
		  application_name,
		  state,
		  COALESCE(wait_event_type, '') AS wait_type,
		  now() - query_start AS duration,
		  regexp_replace(COALESCE(query, ''), '\s+', ' ', 'g') AS query_text
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND query_start IS NOT NULL
		  AND now() - query_start > ($1::int * interval '1 second')
		  AND pid <> pg_backend_pid()
		ORDER BY duration DESC
		LIMIT 5
	`
	rows, err := s.db.Query(ctx, longQ, longRunningSeconds)
	if err != nil {
		logger.DBLogf("DB health monitor: long-running query check failed: %v", err)
		return
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		found = true
		var pid int
		var userName, appName, state, waitType, duration, queryText string
		if err := rows.Scan(&pid, &userName, &appName, &state, &waitType, &duration, &queryText); err != nil {
			continue
		}
		logger.DBLogf(
			"[DB_CULPRIT_QUERY] pid=%d user=%s app=%s state=%s wait=%s duration=%s query=%q",
			pid, userName, appName, state, waitType, duration, truncate(queryText, 400),
		)
	}
	if !found {
		logger.DBLogf("[DB_CULPRIT_QUERY] none over %ds", longRunningSeconds)
	}
}

func (s *CronService) logBlockedQueryPairs() {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	const blockedQ = `
		SELECT
		  blocked.pid AS blocked_pid,
		  COALESCE(blocked.usename, '') AS blocked_user,
		  now() - blocked.query_start AS blocked_for,
		  regexp_replace(COALESCE(blocked.query, ''), '\s+', ' ', 'g') AS blocked_query,
		  blocker.pid AS blocker_pid,
		  COALESCE(blocker.usename, '') AS blocker_user,
		  now() - blocker.query_start AS blocker_running_for,
		  regexp_replace(COALESCE(blocker.query, ''), '\s+', ' ', 'g') AS blocker_query
		FROM pg_stat_activity blocked
		CROSS JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS blk(blocker_pid)
		JOIN pg_stat_activity blocker ON blocker.pid = blk.blocker_pid
		WHERE blocked.datname = current_database()
		LIMIT 5
	`
	rows, err := s.db.Query(ctx, blockedQ)
	if err != nil {
		logger.DBLogf("DB health monitor: blocked query check failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var blockedPID, blockerPID int
		var blockedUser, blockedFor, blockedQuery string
		var blockerUser, blockerFor, blockerQuery string
		if err := rows.Scan(
			&blockedPID, &blockedUser, &blockedFor, &blockedQuery,
			&blockerPID, &blockerUser, &blockerFor, &blockerQuery,
		); err != nil {
			continue
		}
		logger.DBLogf(
			"[DB_BLOCKING] blocked_pid=%d blocked_user=%s blocked_for=%s blocked_query=%q blocker_pid=%d blocker_user=%s blocker_for=%s blocker_query=%q",
			blockedPID, blockedUser, blockedFor, truncate(blockedQuery, 260), blockerPID, blockerUser, blockerFor, truncate(blockerQuery, 260),
		)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func asBool(v interface{}) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(t))
		if err == nil {
			return b, true
		}
	}
	return false, false
}

func asInt(v interface{}) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int32:
		return int(t), true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case string:
		p, err := strconv.Atoi(strings.TrimSpace(t))
		if err == nil {
			return p, true
		}
	}
	return 0, false
}
