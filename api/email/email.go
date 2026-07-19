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
			sslMode := dbutil.EffectiveSSLMode(host)
			dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)
			var err error
			pool, err = pgxpool.New(context.Background(), dsn)
			if err != nil {
				return nil, nil, false, fmt.Errorf("email pgxpool connect: %w", err)
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
	}
	return server, pool, ownsPool, nil
}
