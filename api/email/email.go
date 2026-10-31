package email

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	emailcommon "CimplrCorpSaas/api/email/common"
	emailinbox "CimplrCorpSaas/api/email/inbox"
	emailmailbox "CimplrCorpSaas/api/email/mailbox"
	emailmessages "CimplrCorpSaas/api/email/messages"
	emailtransformrules "CimplrCorpSaas/api/email/transformrules"
	emailupload "CimplrCorpSaas/api/email/upload"
	emailworkflow "CimplrCorpSaas/api/email/workflow"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/dbutil"
	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewEmailServer(pool *pgxpool.Pool, port string) (*http.Server, *pgxpool.Pool, bool, error) {
	const serviceName = "email"
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
				return nil, nil, false, fmt.Errorf("email pgxpool connect: %w", err)
			}
			pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := pool.Ping(pingCtx); err != nil {
				pool.Close()
				return nil, nil, false, fmt.Errorf("email database ping failed: %w", err)
			}
			ownsPool = true
		}
	}
	if pool == nil {
		return nil, nil, false, fmt.Errorf("email pgxpool is not configured")
	}

	emailjobs.EnsureInboxCredentialSchema(context.Background(), pool)
	chain := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(h),
			),
		)
	}

	mux.HandleFunc("/email/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		w.Write([]byte("Email Service is healthy"))
	})

	emailinbox.RegisterInboxRoutes(mux, pool, chain)
	emailmessages.RegisterMessageRoutes(mux, pool, chain)
	emailworkflow.RegisterWorkflowRoutes(mux, pool, chain)
	emailmailbox.RegisterMailboxRoutes(mux, pool, chain)
	emailupload.RegisterUploadRoutes(mux, pool, chain)
	emailtransformrules.RegisterTransformRuleRoutes(mux, pool, chain)

	mux.Handle("/email/metrics", observability.MetricsHandler(serviceName))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           observability.WrapHTTP(serviceName, mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       120 * time.Second,
	}
	return server, pool, ownsPool, nil
}
