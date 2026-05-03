package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv" // Add this import
	_ "github.com/lib/pq"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	catalog "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/internal/appmanager"

	"CimplrCorpSaas/internal/logger")

// InitDB loads DB config from env vars
func InitDB() (*sql.DB, error) {
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	sslMode := os.Getenv("DB_SSLMODE")
	if sslMode == "" {
		sslMode = "disable"
	}
	connStr := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s sslmode=%s",
		user, pass, host, port, name, sslMode,
	)
	return sql.Open("postgres", connStr)
}

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

	db, err := InitDB()
	if err != nil {

		logger.LogError("failed to connect to DB:", err)
	}
	appmanager.SetDB(db)

	// Initialize pgx pool for better performance
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	// Use sslmode=disable when connecting directly to RDS (no SSL terminator).
	// Switch to sslmode=require only when routing through a Supabase/pgBouncer pooler.
	// connect_timeout and statement_timeout guard against hung connections.
	sslMode := os.Getenv("DB_SSLMODE")
	if sslMode == "" {
		sslMode = "disable"
	}
	pgxConnStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s&connect_timeout=10&pool_max_conns=30&pool_min_conns=2&statement_timeout=30000",
		user, pass, host, port, name, sslMode,
	)

	ctx := context.Background()
	pgxPool, err := pgxpool.New(ctx, pgxConnStr)
	if err != nil {
		logger.LogError("failed to create pgx pool:", err)
	}
	defer pgxPool.Close()

	// Fail fast: verify the pool can actually reach the DB before starting services.
	if pingErr := pgxPool.Ping(ctx); pingErr != nil {
		logger.LogError("pgx pool ping failed — check DB_HOST/DB_PORT/DB_SSLMODE in .env: %v", pingErr)
	}
	logger.LogInfo("DB connected: host=%s port=%s db=%s sslmode=%s", host, port, name, sslMode)

	appmanager.SetPgxPool(pgxPool)

	manager := appmanager.NewAppManager()

	// Load service configs from YAML
	servicesCfg, err := appmanager.LoadServiceSequence("./services.yaml")
	if err != nil {
		logger.LogError("failed to load service sequence:", err)
	}

	// Automatically register all services
	manager.AutoRegisterServices(servicesCfg)

	// Start all services
	if err := manager.StartAll(); err != nil {
		logger.LogError("failed to start:", err)
	}

	// --- Wire AuthService to Gateway ---
	authSvcIface := manager.GetServiceByName("auth")
	if authSvcIface == nil {
		logger.LogError("Auth service not found in manager")
	}
	realAuthSvc, ok := authSvcIface.(*auth.AuthService)
	if !ok {
		logger.LogError("Auth service type assertion failed")
	}
	api.SetAuthService(realAuthSvc)

	// Register logout hook: clear in-memory system notifications when a user logs out
	// so stale pipeline-error alerts don't persist across sessions.
	auth.OnLogoutHook = catalog.ClearSystemNotifications

	// Graceful shutdown handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// Stop all services
	if err := manager.StopAll(); err != nil {
		logger.LogError("failed to stop:", err)
	}

	// Close pgx pool if initialized
	if appmanager.GetPgxPool() != nil {
		appmanager.GetPgxPool().Close()
	}
}
