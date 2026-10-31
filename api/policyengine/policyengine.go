package policyengine

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"CimplrCorpSaas/api/domaincatalog"
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
			dsn := dbutil.BuildPostgresDSN(user, pass, host, dbPort, name, dbutil.EffectiveSSLMode(host))
			var err error
			pool, err = dbutil.NewTracedPool(context.Background(), dsn, serviceName)
			if err != nil {
				return nil, nil, false, fmt.Errorf("policyengine pgxpool connect: %w", err)
			}
			pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := pool.Ping(pingCtx); err != nil {
				pool.Close()
				return nil, nil, false, fmt.Errorf("policyengine database ping failed: %w", err)
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
	// Shared domain catalog (FD booking first vertical) — also exposed at /domain-catalog/*
	domaincatalog.RegisterRoutes(mux, pool, chain)

	mux.Handle("/policy-engine/metrics", observability.MetricsHandler(serviceName))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           observability.WrapHTTP(serviceName, mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return server, pool, ownsPool, nil
}
