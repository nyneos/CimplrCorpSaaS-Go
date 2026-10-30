package domaincatalog

import (
	"CimplrCorpSaas/api"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterRoutes mounts shared domain catalog APIs on the policyengine server
// (gateway path /domain-catalog/ → :8185). All methods are POST-only.
func RegisterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/domain-catalog/modules/list", chain(http.HandlerFunc(HandleModulesList(pool))))
	mux.Handle("/domain-catalog/sub-modules/list", chain(http.HandlerFunc(HandleSubModulesList(pool))))
	mux.Handle("/domain-catalog/parts/list", chain(http.HandlerFunc(HandlePartsList(pool))))
	mux.Handle("/domain-catalog/fields/list", chain(http.HandlerFunc(HandleFieldsList(pool))))
	mux.Handle("/domain-catalog/tree", chain(http.HandlerFunc(HandleTree(pool))))
	mux.Handle("/domain-catalog/notification-schema", chain(http.HandlerFunc(HandleNotificationSchema(pool))))
	mux.Handle("/domain-catalog/resolve", chain(http.HandlerFunc(HandleResolve(pool))))
	mux.Handle("/domain-catalog/compatibility", chain(http.HandlerFunc(HandleCompatibility(pool))))
	mux.Handle("/domain-catalog/dashboard-schema", chain(http.HandlerFunc(HandleDashboardSchema(pool))))
	mux.Handle("/domain-catalog/dms-binding", chain(http.HandlerFunc(HandleDmsBinding(pool))))

	// Write side — seeding/admin, no maker-checker (see upsert.go).
	mux.Handle("/domain-catalog/module/upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandleModuleUpsert(pool)))))
	mux.Handle("/domain-catalog/module-alias/upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandleModuleAliasUpsert(pool)))))
	mux.Handle("/domain-catalog/sub-module/upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandleSubModuleUpsert(pool)))))
	mux.Handle("/domain-catalog/sub-module-alias/upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandleSubModuleAliasUpsert(pool)))))
	mux.Handle("/domain-catalog/part/upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandlePartUpsert(pool)))))
	mux.Handle("/domain-catalog/part-alias/upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandlePartAliasUpsert(pool)))))
	mux.Handle("/domain-catalog/field/upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandleFieldUpsert(pool)))))
	mux.Handle("/domain-catalog/field-alias/upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandleFieldAliasUpsert(pool)))))
	mux.Handle("/domain-catalog/fields/bulk-upsert", chain(api.RequireAdminOverride(http.HandlerFunc(HandleBulkFieldsUpsert(pool)))))
}
