package notification

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	middlewares "CimplrCorpSaas/api/middlewares"
	catalog "CimplrCorpSaas/api/notification/catalog"
	push "CimplrCorpSaas/api/notification/push"
	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func NewNotificationServer(pool *pgxpool.Pool, db *sql.DB, port string) (*http.Server, *pgxpool.Pool, bool, error) {
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
			pool, err = pgxpool.New(context.Background(), dsn)
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

	mux.HandleFunc("/notification/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Notification Service is healthy"))
	})

	midNotification := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(h)
	}

	// Register routes for event catalog
	mux.Handle("/notification/event/create", midNotification(catalog.CreateEventSingle(pool)))
	mux.Handle("/notification/event/bulk-create", midNotification(catalog.CreateEvent(pool)))
	mux.Handle("/notification/event/update", midNotification(catalog.UpdateEvent(pool)))
	mux.Handle("/notification/event/bulk-update", midNotification(catalog.BulkUpdateEvent(pool)))
	mux.Handle("/notification/event/bulk-delete", midNotification(catalog.DeleteEvent(pool)))
	mux.Handle("/notification/event/bulk-approve", midNotification(catalog.BulkApproveEvent(pool)))
	mux.Handle("/notification/event/bulk-reject", midNotification(catalog.BulkRejectEvent(pool)))
	mux.Handle("/notification/event/approved-active", midNotification(catalog.GetEventsApprovedActive(pool)))
	mux.Handle("/notification/event/all", midNotification(catalog.GetEventsWithAudit(pool)))
	mux.Handle("/notification/event/audit-history", midNotification(catalog.GetEventAuditHistory(pool)))
	mux.Handle("/notification/event/upload", midNotification(catalog.UploadEventSimple(pool)))
	mux.Handle("/notification/event/get", midNotification(catalog.GetEvent(pool)))

	// Register routes for template catalog
	mux.Handle("/notification/template/create", midNotification(catalog.CreateTemplateSingle(pool)))
	mux.Handle("/notification/template/create-with-recipients", midNotification(catalog.CreateTemplateWithRecipients(pool)))
	mux.Handle("/notification/template/bulk-create-with-recipients", midNotification(catalog.CreateTemplateWithRecipientsBulk(pool)))
	mux.Handle("/notification/template/bulk-create", midNotification(catalog.CreateTemplate(pool)))
	mux.Handle("/notification/template/edit", midNotification(catalog.EditTemplateSingle(pool)))
	mux.Handle("/notification/template/all", midNotification(catalog.GetTemplatesWithAudit(pool)))
	mux.Handle("/notification/template/versions", midNotification(catalog.GetTemplateVersions(pool)))
	mux.Handle("/notification/template/approved-active", midNotification(catalog.GetTemplatesApprovedActive(pool)))
	mux.Handle("/notification/template/get", midNotification(catalog.GetTemplate(pool)))
	mux.Handle("/notification/template/audit-history", midNotification(catalog.GetTemplateAuditHistory(pool)))
	mux.Handle("/notification/template/bulk-approve", midNotification(catalog.BulkApproveTemplate(pool)))
	mux.Handle("/notification/template/bulk-reject", midNotification(catalog.BulkRejectTemplate(pool)))
	mux.Handle("/notification/template/bulk-delete", midNotification(catalog.DeleteTemplateVersion(pool)))

	// Template recipients
	mux.Handle("/notification/template/recipient/create", midNotification(catalog.CreateTemplateRecipient(pool)))
	mux.Handle("/notification/template/recipient/bulk-create", midNotification(catalog.BulkCreateRecipients(pool)))
	mux.Handle("/notification/template/recipient/update", midNotification(catalog.UpdateTemplateRecipient(pool)))
	mux.Handle("/notification/template/recipient/list", midNotification(catalog.GetRecipientsByTemplate(pool)))
	mux.Handle("/notification/template/recipient/delete", midNotification(catalog.DeleteTemplateRecipient(pool)))

	// Notification config (enable/disable channels per event)
	mux.Handle("/notification/config/all", midNotification(catalog.GetNotifConfig(pool)))
	mux.Handle("/notification/config/toggle", midNotification(catalog.ToggleNotifConfig(pool)))
	mux.Handle("/notification/config/upsert", midNotification(catalog.UpsertNotifConfig(pool)))
	mux.Handle("/notification/config/bulk-approve", midNotification(catalog.BulkApproveNotifConfig(pool)))
	mux.Handle("/notification/config/bulk-reject", midNotification(catalog.BulkRejectNotifConfig(pool)))

	// Register in-app push inbox routes
	push.RegisterPushInboxRoutes(mux, pool)

	// Register browser push subscription routes (VAPID public key, register, unregister)
	push.RegisterSubscriptionRoutes(mux, pool)
	mux.Handle("/notification/metrics", observability.MetricsHandler(serviceName))

	server := &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
	return server, pool, ownsPool, nil
}

func StartNotificationService(pool *pgxpool.Pool, db *sql.DB, port string) {
	server, ownedPool, ownsPool, err := NewNotificationServer(pool, db, port)
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
