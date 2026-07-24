package notification

import (
	middlewares "CimplrCorpSaas/api/middlewares"
	catalog "CimplrCorpSaas/api/notification/catalog"
	push "CimplrCorpSaas/api/notification/push"
	"CimplrCorpSaas/internal/observability"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterNotificationRoutes wires every /notification/* route onto mux. Route
// registration only — handler logic lives in the catalog/push packages.
func RegisterNotificationRoutes(mux *http.ServeMux, serviceName string, pool *pgxpool.Pool) {
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
	mux.Handle("/notification/template/approved-active-lite", midNotification(catalog.GetTemplatesApprovedActiveLite(pool)))
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
}
