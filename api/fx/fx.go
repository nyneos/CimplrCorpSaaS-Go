package fx

import (
	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/observability"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func NewFXServer(port string) (*http.Server, *pgxpool.Pool, error) {
	const serviceName = "fx"
	mux := http.NewServeMux()

	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	if user == "" || pass == "" || host == "" || dbPort == "" || name == "" {
		return nil, nil, fmt.Errorf("FX DB environment is incomplete")
	}
	sslMode := dbutil.EffectiveSSLMode(host)
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)

	// create a shared pgx pool once for all middleware and handlers
	pgxPool, err := dbutil.NewTracedPool(context.Background(), dsn, serviceName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to pgxpool DB: %w", err)
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := pgxPool.Ping(pingCtx); err != nil {
		logger.LogError("FX: failed to verify pgxpool DB connectivity at startup: %v", err)
	}

	RegisterFXRoutes(mux, serviceName, pgxPool)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
	return server, pgxPool, nil
}
func StartFXService(port string) {
	server, pool, err := NewFXServer(port)
	if err != nil {
		logger.LogError("FX Service failed: %v", err)
		return
	}
	defer pool.Close()

	logger.LogInfo("FX Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("FX Service failed: %v", err)
	}
}
