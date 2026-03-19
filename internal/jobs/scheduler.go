package jobs

import (
	"context"
	"fmt"
	"log"

	approvalengine "CimplrCorpSaas/api/approvalengine"
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
	log.Println("Starting cron service...")

	// Create default AMFI config from config file
	amfiConfig := investmentjobs.NewDefaultConfig()

	// Override batch size from services.yaml if provided
	if s.config != nil {
		if batchSize, ok := s.config["batch_size"].(int); ok && batchSize > 0 {
			amfiConfig.BatchSize = batchSize
		}
	}

	// Start the AMFI data downloader
	err := investmentjobs.RunAMFIDataDownloader(amfiConfig, s.db)
	if err != nil {
		return fmt.Errorf("failed to start AMFI data downloader: %v", err)
	}

	logger.GlobalLogger.LogAudit("Cron service started with AMFI downloader")
	log.Println("Cron service started — AMFI Downloader scheduled")

	// Start the Sweep Scheduler
	sweepConfig := cashjobs.NewDefaultSweepConfig()

	// Override sweep config from services.yaml if provided
	if s.config != nil {
		if sweepSchedule, ok := s.config["sweep_schedule"].(string); ok && sweepSchedule != "" {
			sweepConfig.Schedule = sweepSchedule
		}
		if sweepBatchSize, ok := s.config["sweep_batch_size"].(int); ok && sweepBatchSize > 0 {
			sweepConfig.BatchSize = sweepBatchSize
		}
	}

	err = cashjobs.RunSweepScheduler(sweepConfig, s.db)
	if err != nil {
		return fmt.Errorf("failed to start sweep scheduler: %v", err)
	}

	logger.GlobalLogger.LogAudit("Sweep scheduler started")
	log.Println("Cron service started — Sweep Scheduler scheduled")

	// Start the Sweep V2 Scheduler
	sweepConfigV2 := cashjobs.NewDefaultSweepConfigV2()

	// Override sweep V2 config from services.yaml if provided
	if s.config != nil {
		if sweepSchedule, ok := s.config["sweep_schedule_v2"].(string); ok && sweepSchedule != "" {
			sweepConfigV2.Schedule = sweepSchedule
		}
		if sweepBatchSize, ok := s.config["sweep_batch_size_v2"].(int); ok && sweepBatchSize > 0 {
			sweepConfigV2.BatchSize = sweepBatchSize
		}
	}

	err = cashjobs.RunSweepSchedulerV2(sweepConfigV2, s.db)
	if err != nil {
		return fmt.Errorf("failed to start sweep V2 scheduler: %v", err)
	}

	logger.GlobalLogger.LogAudit("Sweep V2 scheduler started")
	log.Println("Cron service started — Sweep V2 Scheduler scheduled")

	// Start the Auto-Categorization Scheduler
	categorizationConfig := cashjobs.NewDefaultCategorizationConfig()

	// Override categorization config from services.yaml if provided
	if s.config != nil {
		if catSchedule, ok := s.config["categorization_schedule"].(string); ok && catSchedule != "" {
			categorizationConfig.Schedule = catSchedule
		}
		if catBatchSize, ok := s.config["categorization_batch_size"].(int); ok && catBatchSize > 0 {
			categorizationConfig.BatchSize = catBatchSize
		}
	}

	err = cashjobs.RunCategorizationScheduler(categorizationConfig, s.db)
	if err != nil {
		return fmt.Errorf("failed to start categorization scheduler: %v", err)
	}

	logger.GlobalLogger.LogAudit("Auto-categorization scheduler started")
	log.Println("Cron service started — Auto-Categorization Scheduler scheduled")

	// Start Outbox Worker — polls notification_svc.outbox and delivers emails
	// via SEND_ENDPOINT_URL. Controlled by OUTBOX_WORKER_ENABLED env var.
	ctx := context.Background()
	go dinojobs.StartOutboxWorker(ctx, s.db)
	logger.GlobalLogger.LogAudit("Outbox worker goroutine launched")
	log.Println("Cron service started — Outbox Worker goroutine launched")

	go dinojobs.StartInboxWorker(ctx, s.db)
	logger.GlobalLogger.LogAudit("In-app inbox worker goroutine launched")
	log.Println("Cron service started — In-App Inbox Worker goroutine launched")

	go dinojobs.StartBrowserPushWorker(ctx, s.db)
	logger.GlobalLogger.LogAudit("Browser push worker goroutine launched")
	log.Println("Cron service started — Browser Push Worker goroutine launched")

	go approvalengine.StartSLAWorker(ctx, s.db)
	logger.GlobalLogger.LogAudit("Approval engine SLA worker goroutine launched")
	log.Println("Cron service started — Approval Engine SLA Worker goroutine launched")

	return nil
}

func (s *CronService) Stop() error {
	// The cron jobs are managed internally by RunAMFIDataDownloader
	// We could add a way to stop them if needed in the future
	log.Println("Cron service stopped.")
	return nil
}
