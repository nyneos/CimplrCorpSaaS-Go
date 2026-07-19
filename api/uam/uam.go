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

	// Build pgx pool for approval matrix handlers (PreValidationMiddleware pattern)
	pgxPool := func() *pgxpool.Pool {
		user := os.Getenv("DB_USER")
		pass := os.Getenv("DB_PASSWORD")
		host := os.Getenv("DB_HOST")
		port := os.Getenv("DB_PORT")
		name := os.Getenv("DB_NAME")
		sslMode := dbutil.EffectiveSSLMode(host)
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, port, name, sslMode)
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			return nil
		}
		pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := pool.Ping(pingCtx); err != nil {
			pool.Close()
			return nil
		}
		return pool
	}()
	if pgxPool == nil {
		return nil, nil, fmt.Errorf("UAM: failed to initialize pgxpool DB")
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
