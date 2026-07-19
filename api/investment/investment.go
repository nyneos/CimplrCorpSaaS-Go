package investment

import (
	"net/http"

	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func NewInvestmentServer(pool *pgxpool.Pool, port string) *http.Server {
	const serviceName = "investment"
	mux := http.NewServeMux()

	RegisterInvestmentRoutes(mux, serviceName, pool)

	return &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
}

func StartInvestmentService(pool *pgxpool.Pool, port string) {
	server := NewInvestmentServer(pool, port)

	logger.LogInfo("Investment Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("Investment service failed: %v", err)
	}
}
