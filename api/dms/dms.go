package dms

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	dmscommon "CimplrCorpSaas/api/dms/common"
	dmsgenerate "CimplrCorpSaas/api/dms/generate"
	dmsrules "CimplrCorpSaas/api/dms/rules"
	dmstemplates "CimplrCorpSaas/api/dms/templates"
	dmscontrol "CimplrCorpSaas/api/dms/control"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewDmsServer bootstraps the DMS (document generation/management) module:
// DB pool + middleware chain + delegate to each feature's RegisterRoutes.
// No inline route tables, no handler logic, no response writing here — see
// api/dms/templates/routes.go for the actual route table.
func NewDmsServer(pool *pgxpool.Pool, port string) (*http.Server, *pgxpool.Pool, bool, error) {
	const serviceName = "dms"
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
			pool, err = dbutil.NewTracedPool(context.Background(), dsn, serviceName)
			if err != nil {
				return nil, nil, false, fmt.Errorf("dms pgxpool connect: %w", err)
			}
			ownsPool = true
		}
	}
	if pool == nil {
		return nil, nil, false, fmt.Errorf("dms pgxpool is not configured")
	}

	chain := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(h),
			),
		)
	}

	mux.HandleFunc("/dms/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			dmscommon.RespondMethodNotAllowed(w)
			return
		}
		_, _ = w.Write([]byte("DMS Service is healthy"))
	})

	dmstemplates.RegisterRoutes(mux, pool, chain)
	dmsrules.RegisterRoutes(mux, pool, chain)
	dmsgenerate.RegisterRoutes(mux, pool, chain)
	dmscontrol.RegisterRoutes(mux, chain)

	mux.Handle("/dms/metrics", observability.MetricsHandler(serviceName))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           observability.WrapHTTP(serviceName, mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return server, pool, ownsPool, nil
}
