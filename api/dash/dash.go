package dash

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

func NewDashServer(port string) (*http.Server, *pgxpool.Pool, error) {
	const serviceName = "dash"
	mux := http.NewServeMux()
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	dsn := dbutil.BuildPostgresDSN(user, pass, host, dbPort, name, dbutil.EffectiveSSLMode(host))
	pgxPool, err := dbutil.NewTracedPool(context.Background(), dsn, serviceName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to pgxpool DB: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pgxPool.Ping(pingCtx); err != nil {
		pgxPool.Close()
		return nil, nil, fmt.Errorf("dash database ping failed: %w", err)
	}

	RegisterDashRoutes(mux, serviceName, pgxPool)

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           observability.WrapHTTP(serviceName, withDashQueryTimeout(mux)),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       120 * time.Second,
	}
	return server, pgxPool, nil
}

func StartDashService(port string) {
	server, pool, err := NewDashServer(port)
	if err != nil {
		logger.LogError("Dashboard Service failed: %v", err)
		return
	}
	defer pool.Close()

	logger.LogInfo("Dashboard Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("Dashboard Service failed: %v", err)
	}
}
