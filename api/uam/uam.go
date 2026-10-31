package uam

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

func NewUAMServer(port string) (*http.Server, *pgxpool.Pool, error) {
	const serviceName = "uam"
	mux := http.NewServeMux()

	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	dsn := dbutil.BuildPostgresDSN(user, pass, host, dbPort, name, dbutil.EffectiveSSLMode(host))
	pgxPool, err := dbutil.NewTracedPool(context.Background(), dsn, serviceName)
	if err != nil {
		return nil, nil, fmt.Errorf("UAM: failed to initialize pgxpool DB: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pgxPool.Ping(pingCtx); err != nil {
		pgxPool.Close()
		return nil, nil, fmt.Errorf("uam database ping failed: %w", err)
	}
	RegisterUAMRoutes(mux, serviceName, pgxPool)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
	return server, pgxPool, nil
}

func StartUAMService(port string) {
	server, pool, err := NewUAMServer(port)
	if err != nil {
		logger.LogError("UAM Service failed: %v", err)
		return
	}
	defer pool.Close()

	logger.LogInfo("UAM Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("UAM Service failed: %v", err)
	}
}
