package notification

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func NewNotificationServer(pool *pgxpool.Pool, port string) (*http.Server, *pgxpool.Pool, bool, error) {
	const serviceName = "notification"
	mux := http.NewServeMux()
	ownsPool := false

	if pool == nil {
		user := os.Getenv("DB_USER")
		pass := os.Getenv("DB_PASSWORD")
		host := os.Getenv("DB_HOST")
		port := os.Getenv("DB_PORT")
		name := os.Getenv("DB_NAME")
		if user != "" && pass != "" && host != "" && port != "" && name != "" {
			sslMode := dbutil.EffectiveSSLMode(host)
			dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, port, name, sslMode)
			var err error
			pool, err = dbutil.NewTracedPool(context.Background(), dsn, serviceName)
			if err != nil {
				return nil, nil, false, fmt.Errorf("failed to connect to pgxpool DB: %w", err)
			}
			ownsPool = true
			pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := pool.Ping(pingCtx); err != nil {
				logger.LogError("Notification: failed to verify pgxpool DB connectivity at startup: %v", err)
			}
		}
	}
	if pool == nil {
		return nil, nil, false, fmt.Errorf("notification pgxpool is not configured")
	}

	RegisterNotificationRoutes(mux, serviceName, pool)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
	return server, pool, ownsPool, nil
}

func StartNotificationService(pool *pgxpool.Pool, port string) {
	server, ownedPool, ownsPool, err := NewNotificationServer(pool, port)
	if err != nil {
		logger.LogError("Notification Service failed: %v", err)
		return
	}
	if ownsPool {
		defer ownedPool.Close()
	}

	logger.LogInfo("Notification Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("Notification Service failed: %v", err)
	}
}
