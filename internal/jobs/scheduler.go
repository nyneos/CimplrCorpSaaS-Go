package jobs

import (
	"context"
	"fmt"
	"time"

	approvalengine "CimplrCorpSaas/api/approvalengine"
	fdAccrual "CimplrCorpSaas/api/investment/fdAccrual"
	cashjobs "CimplrCorpSaas/internal/jobs/cash"
	dinojobs "CimplrCorpSaas/internal/jobs/dino"
	emailjobs "CimplrCorpSaas/internal/jobs/email"
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
	emailjobs.EnsureInboxCredentialSchema(ctx, s.db)
	go emailjobs.StartInboundPoller(ctx, s.db)
	go emailjobs.StartGraphPoller(ctx, s.db)
	go emailjobs.StartGoogleWorkspacePoller(ctx, s.db)
	go emailjobs.StartIMAPPoller(ctx, s.db)
	go emailjobs.StartOAuthPoller(ctx, s.db)
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
	go investmentjobs.StartCimplrAutoMaturityWorker(s.db)

	// ---------------- Portfolio Refresh ----------------
	if err := investmentjobs.RunPortfolioRefreshWorker(s.db); err != nil {
		return fmt.Errorf("failed to start portfolio refresh worker: %v", err)
	}
	logger.GlobalLogger.LogAudit("Portfolio refresh worker started")

	logger.GlobalLogger.LogAudit("All background workers started")

	// ---------------- DB Cleanup Worker ----------------
	go s.startDBCleanupWorker()
	logger.GlobalLogger.LogAudit("DB cleanup worker started")
	logger.LogInfo("Cron service started — DB Cleanup Worker running")

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

	// config override
	if s.config != nil {
		if val, ok := s.config["db_cleanup_interval"].(int); ok && val > 0 {
			interval = val
		}
	}

	logger.LogInfo("DB Cleanup Worker running every %d seconds\n", interval)

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanupIdleConnections()
		}
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

	logger.LogInfo("DB cleanup executed. Rows affected: %d\n", tag.RowsAffected())
}
