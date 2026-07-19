package policyengine

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/api/policyengine/cdm"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/execution"
	"CimplrCorpSaas/api/policyengine/policy"
	"CimplrCorpSaas/api/policyengine/trigger"
	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPolicyEngineServer(pool *pgxpool.Pool, port string) (*http.Server, *pgxpool.Pool, bool, error) {
	const serviceName = "policyengine"
	mux := http.NewServeMux()
	ownsPool := false

	if pool == nil {
		user := os.Getenv("DB_USER")
		pass := os.Getenv("DB_PASSWORD")
		host := os.Getenv("DB_HOST")
		dbPort := os.Getenv("DB_PORT")
		name := os.Getenv("DB_NAME")
		if user != "" && pass != "" && host != "" && dbPort != "" && name != "" {
			sslMode := dbutil.EffectiveSSLMode(host)
			dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)
			var err error
			pool, err = pgxpool.New(context.Background(), dsn)
			if err != nil {
				return nil, nil, false, fmt.Errorf("policyengine pgxpool connect: %w", err)
			}
			ownsPool = true
		}
	}
	if pool == nil {
		return nil, nil, false, fmt.Errorf("policyengine pgxpool is not configured")
	}

	chain := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(h),
			),
		)
	}

	mux.HandleFunc("/policy-engine/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			common.RespondMethodNotAllowed(w)
			return
		}
		_, _ = w.Write([]byte("Policy Engine Service is healthy"))
	})

	cdm.RegisterRoutes(mux, pool, chain)
	trigger.RegisterRoutes(mux, pool, chain)
	policy.RegisterRoutes(mux, pool, chain)
	execution.RegisterRoutes(mux, pool, chain)

	mux.Handle("/policy-engine/metrics", observability.MetricsHandler(serviceName))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           observability.WrapHTTP(serviceName, mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return server, pool, ownsPool, nil
}
