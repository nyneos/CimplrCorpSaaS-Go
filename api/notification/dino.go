package notification

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	middlewares "CimplrCorpSaas/api/middlewares"
	catalog "CimplrCorpSaas/api/notification/catalog"

	"github.com/jackc/pgx/v5/pgxpool"
)

const notificationPort = "9111"

func StartNotificationService(pool *pgxpool.Pool, db *sql.DB) {
	mux := http.NewServeMux()

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
		}
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

	// Template recipients
	mux.Handle("/notification/template/recipient/create", middlewares.PreValidationMiddleware(pool)(catalog.CreateTemplateRecipient(pool)))
	mux.Handle("/notification/template/recipient/list", middlewares.PreValidationMiddleware(pool)(catalog.GetRecipientsByTemplate(pool)))
	mux.Handle("/notification/template/recipient/delete", middlewares.PreValidationMiddleware(pool)(catalog.DeleteTemplateRecipient(pool)))

	// Notification config (enable/disable channels per event)
	mux.Handle("/notification/config/all", middlewares.PreValidationMiddleware(pool)(catalog.GetNotifConfig(pool)))
	mux.Handle("/notification/config/toggle", middlewares.PreValidationMiddleware(pool)(catalog.ToggleNotifConfig(pool)))
	mux.Handle("/notification/config/upsert", middlewares.PreValidationMiddleware(pool)(catalog.UpsertNotifConfig(pool)))

	log.Printf("Notification Service started on :%s", notificationPort)
	if err := http.ListenAndServe(":"+notificationPort, mux); err != nil {
		log.Fatalf("Notification Service failed: %v", err)
	}
}
