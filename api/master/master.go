package master

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

func NewMasterServer(port string) (*http.Server, *pgxpool.Pool, error) {
	const serviceName = "master"
	mux := http.NewServeMux()
	registerMasterAdditionalFileAliasRoutes(mux)
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	sslMode := dbutil.EffectiveSSLMode(host)
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)
	pgxPool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to pgxpool DB: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pgxPool.Ping(pingCtx); err != nil {
		logger.LogError("Master: failed to verify pgxpool DB connectivity at startup: %v", err)
	}

	RegisterMasterRoutes(mux, serviceName, pgxPool)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
	return server, pgxPool, nil
}

func StartMasterService(port string) {
	server, pool, err := NewMasterServer(port)
	if err != nil {
		logger.LogError("Master Service failed: %v", err)
		return
	}
	defer pool.Close()

	logger.LogInfo("Master Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("Master Service failed: %v", err)
	}
}
