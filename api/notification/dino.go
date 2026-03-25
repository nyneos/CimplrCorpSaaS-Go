package notification

import (
	"database/sql"
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"
	catalog "CimplrCorpSaas/api/notification/catalog"
	push "CimplrCorpSaas/api/notification/push"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterNotificationRoutes(mux *http.ServeMux, db *sql.DB, pgxPool *pgxpool.Pool) {
	// mux := http.NewServeMux()

	/*
		old local pgx pool fallback (replaced by shared pool from cmd/main.go):
		if pgxPool == nil {
			user := os.Getenv("DB_USER")
			pass := os.Getenv("DB_PASSWORD")
			host := os.Getenv("DB_HOST")
			port := os.Getenv("DB_PORT")
			name := os.Getenv("DB_NAME")
			if user != "" && pass != "" && host != "" && port != "" && name != "" {
				dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, name)
				var err error
				pgxPool, err = pgxpool .New(context.Background(), dsn)
				if err != nil {
					log.Fatalf("failed to connect to pgxpool DB: %v", err)
				}
			}
		}
	*/

	// Register routes for event catalog
	mux.Handle("/notification/event/create", middlewares.PreValidationMiddleware(pgxPool)(catalog.CreateEventSingle(pgxPool)))
	mux.Handle("/notification/event/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(catalog.CreateEvent(pgxPool)))
	mux.Handle("/notification/event/update", middlewares.PreValidationMiddleware(pgxPool)(catalog.UpdateEvent(pgxPool)))
	mux.Handle("/notification/event/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(catalog.BulkUpdateEvent(pgxPool)))
	mux.Handle("/notification/event/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(catalog.DeleteEvent(pgxPool)))
	mux.Handle("/notification/event/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(catalog.BulkApproveEvent(pgxPool)))
	mux.Handle("/notification/event/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(catalog.BulkRejectEvent(pgxPool)))
	mux.Handle("/notification/event/approved-active", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetEventsApprovedActive(pgxPool)))
	mux.Handle("/notification/event/all", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetEventsWithAudit(pgxPool)))
	mux.Handle("/notification/event/audit-history", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetEventAuditHistory(pgxPool)))
	mux.Handle("/notification/event/upload", middlewares.PreValidationMiddleware(pgxPool)(catalog.UploadEventSimple(pgxPool)))
	mux.Handle("/notification/event/get", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetEvent(pgxPool)))

	// Register routes for template catalog
	mux.Handle("/notification/template/create", middlewares.PreValidationMiddleware(pgxPool)(catalog.CreateTemplateSingle(pgxPool)))
	mux.Handle("/notification/template/create-with-recipients", middlewares.PreValidationMiddleware(pgxPool)(catalog.CreateTemplateWithRecipients(pgxPool)))
	mux.Handle("/notification/template/bulk-create-with-recipients", middlewares.PreValidationMiddleware(pgxPool)(catalog.CreateTemplateWithRecipientsBulk(pgxPool)))
	mux.Handle("/notification/template/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(catalog.CreateTemplate(pgxPool)))
	mux.Handle("/notification/template/edit", middlewares.PreValidationMiddleware(pgxPool)(catalog.EditTemplateSingle(pgxPool)))
	mux.Handle("/notification/template/all", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetTemplatesWithAudit(pgxPool)))
	mux.Handle("/notification/template/versions", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetTemplateVersions(pgxPool)))
	mux.Handle("/notification/template/approved-active", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetTemplatesApprovedActive(pgxPool)))
	mux.Handle("/notification/template/get", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetTemplate(pgxPool)))
	mux.Handle("/notification/template/audit-history", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetTemplateAuditHistory(pgxPool)))
	mux.Handle("/notification/template/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(catalog.BulkApproveTemplate(pgxPool)))
	mux.Handle("/notification/template/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(catalog.BulkRejectTemplate(pgxPool)))
	mux.Handle("/notification/template/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(catalog.DeleteTemplateVersion(pgxPool)))

	// Template recipients
	mux.Handle("/notification/template/recipient/create", middlewares.PreValidationMiddleware(pgxPool)(catalog.CreateTemplateRecipient(pgxPool)))
	mux.Handle("/notification/template/recipient/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(catalog.BulkCreateRecipients(pgxPool)))
	mux.Handle("/notification/template/recipient/update", middlewares.PreValidationMiddleware(pgxPool)(catalog.UpdateTemplateRecipient(pgxPool)))
	mux.Handle("/notification/template/recipient/list", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetRecipientsByTemplate(pgxPool)))
	mux.Handle("/notification/template/recipient/delete", middlewares.PreValidationMiddleware(pgxPool)(catalog.DeleteTemplateRecipient(pgxPool)))

	// Notification config (enable/disable channels per event)
	mux.Handle("/notification/config/all", middlewares.PreValidationMiddleware(pgxPool)(catalog.GetNotifConfig(pgxPool)))
	mux.Handle("/notification/config/toggle", middlewares.PreValidationMiddleware(pgxPool)(catalog.ToggleNotifConfig(pgxPool)))
	mux.Handle("/notification/config/upsert", middlewares.PreValidationMiddleware(pgxPool)(catalog.UpsertNotifConfig(pgxPool)))
	mux.Handle("/notification/config/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(catalog.BulkApproveNotifConfig(pgxPool)))
	mux.Handle("/notification/config/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(catalog.BulkRejectNotifConfig(pgxPool)))

	// Register in-app push inbox routes
	push.RegisterPushInboxRoutes(mux, pgxPool)

	// Register browser push subscription routes (VAPID public key, register, unregister)
	push.RegisterSubscriptionRoutes(mux, pgxPool)

	// port := os.Getenv("NOTIFICATION_PORT")
	// if port == "" {
	// 	port = "9111"
	// }
	// log.Printf("Notification Service starting on :%s", port)
	// if err := http.ListenAndServe(":"+port, mux); err != nil {
	// 	log.Fatalf("Notification Service failed: %v", err)
	// }
}

/*
func StartNotificationService(pool *pgxpool.Pool, db *sql.DB) {
	mux := http.NewServeMux()
	RegisterNotificationRoutes(mux, db, pool)
}
*/
