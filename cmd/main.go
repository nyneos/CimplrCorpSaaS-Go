package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	catalog "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/internal/appmanager"
	"CimplrCorpSaas/internal/dbutil"
	// "CimplrCorpSaas/internal/logger"
)

func main() {
	_ = godotenv.Overload("../.env") // Overload forces .env to win over stale shell exports

	fmt.Println("ENV CHECK:")
	fmt.Println("  DB_USER:", os.Getenv("DB_USER"))
	fmt.Println("  DB_HOST:", os.Getenv("DB_HOST"))
	fmt.Println("  DB_PORT:", os.Getenv("DB_PORT"))
	fmt.Println("  DB_NAME:", os.Getenv("DB_NAME"))

	if os.Getenv("DB_PASSWORD") != "" {
		fmt.Println("  DB_PASSWORD: [SET]")
	} else {
		fmt.Println("  DB_PASSWORD: [NOT SET!]")
	}

	// Initialize pgx pool for better performance
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	// connect_timeout is a libpq client-side dial timeout. Do not put
	// statement_timeout in the DSN: pgx sends it as a startup GUC and
	// Supabase PgBouncer (port 6543) rejects it with
	// FATAL: unsupported startup parameter: statement_timeout.
	sslMode := os.Getenv("DB_SSLMODE")
	if sslMode == "" {
		sslMode = "require"
	}
	pgxConnStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s&connect_timeout=10",
		user, pass, host, port, name, sslMode,
	)

	ctx := context.Background()
	pgxConfig, err := pgxpool.ParseConfig(pgxConnStr)
	if err != nil {
		log.Fatal("failed to parse pgx config:", err)
	}
	// Keep the shared core pool small: each module also opens its own pool
	// (see dbutil.NewTracedPool). Together they must fit under Supabase
	// max_connections (~90 on small plans, with slots reserved for SUPERUSER).
	pgxConfig.MaxConns = 8
	pgxConfig.MinConns = 1
	pgxConfig.MaxConnIdleTime = 2 * time.Minute
	pgxConfig.MaxConnLifetime = 30 * time.Minute
	pgxConfig.HealthCheckPeriod = 1 * time.Minute
	if pgxConfig.ConnConfig.RuntimeParams == nil {
		pgxConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	pgxConfig.ConnConfig.RuntimeParams["application_name"] = "cimplr-core"
	dbutil.ApplyPoolerSafeQueryMode(pgxConfig)
	// pgxConfig.ConnConfig.Tracer = logger.NewDBTracer("core")

	pgxPool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		log.Fatal("failed to create pgx pool:", err)
	}
	defer pgxPool.Close()

	// Fail fast: verify the pool can actually reach the DB before starting services.
	if pingErr := pgxPool.Ping(ctx); pingErr != nil {
		log.Fatalf("pgx pool ping failed — check DB_HOST/DB_PORT/DB_SSLMODE in .env: %v", pingErr)
	}
	log.Printf("DB connected: host=%s port=%s db=%s sslmode=%s", host, port, name, sslMode)

	appmanager.SetPgxPool(pgxPool)

	manager := appmanager.NewAppManager()

	// Load service configs from YAML
	servicesCfg, err := appmanager.LoadServiceSequence("../services.yaml")
	if err != nil {
		log.Fatal("failed to load service sequence:", err)
	}

	// Automatically register all services
	manager.AutoRegisterServices(servicesCfg)

	// Start all services
	if err := manager.StartAll(); err != nil {
		log.Fatal("failed to start:", err)
	}

	// --- Wire AuthService to Gateway ---
	authSvcIface := manager.GetServiceByName("auth")
	if authSvcIface == nil {
		log.Fatal("Auth service not found in manager")
	}
	realAuthSvc, ok := authSvcIface.(*auth.AuthService)
	if !ok {
		log.Fatal("Auth service type assertion failed")
	}
	api.SetAuthService(realAuthSvc)

	auth.OnLogoutHook = catalog.ClearSystemNotifications

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	
	if err := manager.StopAll(); err != nil {
		log.Fatal("failed to stop:", err)
	}

	// Close pgx pool if initialized
	if appmanager.GetPgxPool() != nil {
		appmanager.GetPgxPool().Close()
	}
}
