package notification

import (
	"CimplrCorpSaas/internal/telemetry"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	middlewares "CimplrCorpSaas/api/middlewares"
	catalog "CimplrCorpSaas/api/notification/catalog"
	push "CimplrCorpSaas/api/notification/push"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var notificationServer *http.Server
var notificationServerStop context.CancelFunc
var notificationTracerShutdown func(context.Context) error

func StartNotificationService(pool *pgxpool.Pool, db *sql.DB, port string) {
	mux := http.NewServeMux()
	ownsPool := false
	tracerProvider, tracerShutdown, err := telemetry.InitTracerProvider("notification")
	if err != nil {
		log.Fatalf("failed to initialize OpenTelemetry tracer for notification service: %v", err)
	}
	notificationTracerShutdown = tracerShutdown

	if pool == nil {
		user := os.Getenv("DB_USER")
		pass := os.Getenv("DB_PASSWORD")
		host := os.Getenv("DB_HOST")
		port := os.Getenv("DB_PORT")
		name := os.Getenv("DB_NAME")
		if user != "" && pass != "" && host != "" && port != "" && name != "" {
			dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, name)
			var err error
			pool, err = pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Fatalf("failed to connect to pgxpool DB: %v", err)
			}
			ownsPool = true
		}
	}
	if ownsPool {
		defer pool.Close()
	}

	// Register routes for event catalog
	mux.Handle("/notification/event/create", middlewares.PreValidationMiddleware(pool)(catalog.CreateEventSingle(pool)))
	mux.Handle("/notification/event/bulk-create", middlewares.PreValidationMiddleware(pool)(catalog.CreateEvent(pool)))
	mux.Handle("/notification/event/update", middlewares.PreValidationMiddleware(pool)(catalog.UpdateEvent(pool)))
	mux.Handle("/notification/event/bulk-update", middlewares.PreValidationMiddleware(pool)(catalog.BulkUpdateEvent(pool)))
	mux.Handle("/notification/event/bulk-delete", middlewares.PreValidationMiddleware(pool)(catalog.DeleteEvent(pool)))
	mux.Handle("/notification/event/bulk-approve", middlewares.PreValidationMiddleware(pool)(catalog.BulkApproveEvent(pool)))
	mux.Handle("/notification/event/bulk-reject", middlewares.PreValidationMiddleware(pool)(catalog.BulkRejectEvent(pool)))
	mux.Handle("/notification/event/approved-active", middlewares.PreValidationMiddleware(pool)(catalog.GetEventsApprovedActive(pool)))
	mux.Handle("/notification/event/all", middlewares.PreValidationMiddleware(pool)(catalog.GetEventsWithAudit(pool)))
	mux.Handle("/notification/event/audit-history", middlewares.PreValidationMiddleware(pool)(catalog.GetEventAuditHistory(pool)))
	mux.Handle("/notification/event/upload", middlewares.PreValidationMiddleware(pool)(catalog.UploadEventSimple(pool)))
	mux.Handle("/notification/event/get", middlewares.PreValidationMiddleware(pool)(catalog.GetEvent(pool)))

	// Register routes for template catalog
	mux.Handle("/notification/template/create", middlewares.PreValidationMiddleware(pool)(catalog.CreateTemplateSingle(pool)))
	mux.Handle("/notification/template/create-with-recipients", middlewares.PreValidationMiddleware(pool)(catalog.CreateTemplateWithRecipients(pool)))
	mux.Handle("/notification/template/bulk-create-with-recipients", middlewares.PreValidationMiddleware(pool)(catalog.CreateTemplateWithRecipientsBulk(pool)))
	mux.Handle("/notification/template/bulk-create", middlewares.PreValidationMiddleware(pool)(catalog.CreateTemplate(pool)))
	mux.Handle("/notification/template/edit", middlewares.PreValidationMiddleware(pool)(catalog.EditTemplateSingle(pool)))
	mux.Handle("/notification/template/all", middlewares.PreValidationMiddleware(pool)(catalog.GetTemplatesWithAudit(pool)))
	mux.Handle("/notification/template/versions", middlewares.PreValidationMiddleware(pool)(catalog.GetTemplateVersions(pool)))
	mux.Handle("/notification/template/approved-active", middlewares.PreValidationMiddleware(pool)(catalog.GetTemplatesApprovedActive(pool)))
	mux.Handle("/notification/template/get", middlewares.PreValidationMiddleware(pool)(catalog.GetTemplate(pool)))
	mux.Handle("/notification/template/audit-history", middlewares.PreValidationMiddleware(pool)(catalog.GetTemplateAuditHistory(pool)))
	mux.Handle("/notification/template/bulk-approve", middlewares.PreValidationMiddleware(pool)(catalog.BulkApproveTemplate(pool)))
	mux.Handle("/notification/template/bulk-reject", middlewares.PreValidationMiddleware(pool)(catalog.BulkRejectTemplate(pool)))
	mux.Handle("/notification/template/bulk-delete", middlewares.PreValidationMiddleware(pool)(catalog.DeleteTemplateVersion(pool)))

	// Template recipients
	mux.Handle("/notification/template/recipient/create", middlewares.PreValidationMiddleware(pool)(catalog.CreateTemplateRecipient(pool)))
	mux.Handle("/notification/template/recipient/bulk-create", middlewares.PreValidationMiddleware(pool)(catalog.BulkCreateRecipients(pool)))
	mux.Handle("/notification/template/recipient/update", middlewares.PreValidationMiddleware(pool)(catalog.UpdateTemplateRecipient(pool)))
	mux.Handle("/notification/template/recipient/list", middlewares.PreValidationMiddleware(pool)(catalog.GetRecipientsByTemplate(pool)))
	mux.Handle("/notification/template/recipient/delete", middlewares.PreValidationMiddleware(pool)(catalog.DeleteTemplateRecipient(pool)))

	// Notification config (enable/disable channels per event)
	mux.Handle("/notification/config/all", middlewares.PreValidationMiddleware(pool)(catalog.GetNotifConfig(pool)))
	mux.Handle("/notification/config/toggle", middlewares.PreValidationMiddleware(pool)(catalog.ToggleNotifConfig(pool)))
	mux.Handle("/notification/config/upsert", middlewares.PreValidationMiddleware(pool)(catalog.UpsertNotifConfig(pool)))
	mux.Handle("/notification/config/bulk-approve", middlewares.PreValidationMiddleware(pool)(catalog.BulkApproveNotifConfig(pool)))
	mux.Handle("/notification/config/bulk-reject", middlewares.PreValidationMiddleware(pool)(catalog.BulkRejectNotifConfig(pool)))

	// Register in-app push inbox routes
	push.RegisterPushInboxRoutes(mux, pool)

	// Register browser push subscription routes (VAPID public key, register, unregister)
	push.RegisterSubscriptionRoutes(mux, pool)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: otelhttp.NewHandler(mux, "notification", otelhttp.WithTracerProvider(tracerProvider)),
	}
	notificationServer = server

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	notificationServerStop = stop
	defer stop()

	go func() {
		log.Printf("Notification Service started on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Notification Service failed: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func shutdownNotificationService() error {
	if notificationServerStop != nil {
		notificationServerStop()
	}
	if notificationServer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := notificationServer.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
		return err
	}
	if notificationTracerShutdown != nil {
		if err := notificationTracerShutdown(ctx); err != nil {
			return err
		}
	}
	return nil
}
