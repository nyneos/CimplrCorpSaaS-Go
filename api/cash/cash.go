package cash

import (
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/observability"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func NewCashServer(port string) (*http.Server, *pgxpool.Pool, error) {
	const serviceName = "cash"
	mux := http.NewServeMux()
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	sslMode := strings.TrimSpace(os.Getenv("DB_SSLMODE"))
	if sslMode == "" {
		sslMode = "require"
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)
	pgxPool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {

		return nil, nil, fmt.Errorf("failed to connect to pgxpool DB: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pgxPool.Ping(pingCtx); err != nil {
		logger.LogError("failed to verify pgxpool DB connectivity at startup: %v", err)
	}
	RegisterCashRoutes(mux, serviceName, pgxPool)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, cashJSONActionResponses(mux)),
	}
	return server, pgxPool, nil
}

func StartCashService(port string) {
	server, pool, err := NewCashServer(port)
	if err != nil {
		logger.LogError("Cash Service failed: %v", err)
		return
	}
	defer pool.Close()

	logger.LogInfo("Cash Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("Cash Service failed: %v", err)
	}
}

func cashJSONActionResponses(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isCashJSONActionRoute(r.URL.Path) {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		}
		next.ServeHTTP(w, r)
	})
}

func isCashJSONActionRoute(path string) bool {
	if !strings.HasPrefix(path, "/cash/") {
		return false
	}

	for _, segment := range strings.Split(strings.Trim(path, "/"), "/") {
		if strings.Contains(segment, "audit") ||
			strings.Contains(segment, "approve") ||
			strings.Contains(segment, "delete") {
			return true
		}
	}
	return false
}
