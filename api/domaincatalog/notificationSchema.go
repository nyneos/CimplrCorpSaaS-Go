package domaincatalog

import (
	"net/http"
	"sort"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

type notifScalar struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type notifListField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type notifList struct {
	Name   string           `json:"name"`
	Fields []notifListField `json:"fields"`
}

type notifSchema struct {
	SubModuleCode string         `json:"sub_module_code"`
	Scalars       []notifScalar  `json:"scalars"`
	Lists         []notifList    `json:"lists"`
}

func HandleNotificationSchema(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req struct {
			SubModuleCode string `json:"sub_module_code"`
			Alias         string `json:"alias"`
		}
		_ = decodeJSON(r, &req)

		sm, err := resolveSubModule(r, pool, strings.TrimSpace(req.SubModuleCode), strings.TrimSpace(req.Alias), "NOTIFICATION")
		if err != nil || sm == "" {
			api.RespondEnvelopeError(w, http.StatusNotFound, "notification catalog sub-module not found", "CATALOG_NOTIF_SUB_MODULE_NOT_FOUND")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT a.alias_key, a.container_name, a.usage_kind, a.sort_order, f.data_type
			FROM domain_catalog.field_alias a
			JOIN domain_catalog.field f ON f.field_id = a.field_id
			WHERE a.is_deleted = false AND f.is_deleted = false
			  AND f.sub_module_code = $1 AND a.consumer_system = 'NOTIFICATION'
			ORDER BY a.container_name, a.sort_order, a.alias_key`, sm)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog notification-schema: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to build notification schema", "CATALOG_NOTIF_SCHEMA_FAILED")
			return
		}
		defer rows.Close()

		schema := notifSchema{
			SubModuleCode: sm,
			Scalars:       make([]notifScalar, 0),
			Lists:         make([]notifList, 0),
		}
		listMap := map[string]*notifList{}

		for rows.Next() {
			var aliasKey, container, usage, dataType string
			var sortOrder int
			if err := rows.Scan(&aliasKey, &container, &usage, &sortOrder, &dataType); err != nil {
				api.LogErrorForResponse(w, "domain-catalog notification-schema scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to build notification schema", "CATALOG_NOTIF_SCHEMA_FAILED")
				return
			}
			switch usage {
			case "scalar":
				schema.Scalars = append(schema.Scalars, notifScalar{Name: aliasKey, Type: dataType})
			case "list_field":
				lst, ok := listMap[container]
				if !ok {
					lst = &notifList{Name: container, Fields: make([]notifListField, 0)}
					listMap[container] = lst
				}
				lst.Fields = append(lst.Fields, notifListField{Name: aliasKey, Type: dataType})
			}
		}

		names := make([]string, 0, len(listMap))
		for n := range listMap {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			schema.Lists = append(schema.Lists, *listMap[n])
		}

		api.RespondEnvelopeSuccess(w, "Notification schema fetched", schema)
	}
}

func resolveSubModule(r *http.Request, pool *pgxpool.Pool, subModuleCode, alias, consumer string) (string, error) {
	if subModuleCode != "" {
		var sm string
		err := pool.QueryRow(r.Context(), `
			SELECT sub_module_code FROM domain_catalog.sub_module
			WHERE is_deleted = false AND sub_module_code = $1`, subModuleCode).Scan(&sm)
		return sm, err
	}
	if alias == "" {
		return "", nil
	}
	consumer = normalizeConsumer(consumer)
	if consumer == "" {
		consumer = "NOTIFICATION"
	}

	var sm string
	err := pool.QueryRow(r.Context(), `
		SELECT sub_module_code FROM domain_catalog.sub_module_alias
		WHERE is_deleted = false AND consumer_system = $1 AND alias_code = $2`, consumer, alias).Scan(&sm)
	if err == nil {
		return sm, nil
	}

	// Sanitized match (e.g. FDBOOKING from "FD BOOKING")
	san := sanitizeNotifKey(alias)
	err = pool.QueryRow(r.Context(), `
		SELECT sub_module_code FROM domain_catalog.sub_module_alias
		WHERE is_deleted = false AND consumer_system = $1
		  AND regexp_replace(upper(alias_code), '[^A-Z0-9]', '', 'g') = $2
		LIMIT 1`, consumer, san).Scan(&sm)
	return sm, err
}

func HandleResolve(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req struct {
			ConsumerSystem string `json:"consumer_system"`
			AliasCode      string `json:"alias_code"`
			Level          string `json:"level"` // module | sub_module | part
		}
		_ = decodeJSON(r, &req)
		consumer := normalizeConsumer(req.ConsumerSystem)
		alias := strings.TrimSpace(req.AliasCode)
		level := strings.ToLower(strings.TrimSpace(req.Level))
		if consumer == "" || alias == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "consumer_system and alias_code are required", "CATALOG_RESOLVE_REQUIRED")
			return
		}
		if level == "" {
			level = "sub_module"
		}

		switch level {
		case "module":
			var code, name string
			err := pool.QueryRow(r.Context(), `
				SELECT m.module_code, m.module_name
				FROM domain_catalog.module_alias a
				JOIN domain_catalog.module m ON m.module_code = a.module_code
				WHERE a.is_deleted = false AND m.is_deleted = false
				  AND a.consumer_system = $1 AND a.alias_code = $2`, consumer, alias).Scan(&code, &name)
			if err != nil {
				api.RespondEnvelopeError(w, http.StatusNotFound, "module alias not found", "CATALOG_RESOLVE_NOT_FOUND")
				return
			}
			api.RespondEnvelopeSuccess(w, "Resolved", map[string]string{"module_code": code, "module_name": name})
		case "part":
			var partCode, partName, subCode string
			err := pool.QueryRow(r.Context(), `
				SELECT p.part_code, p.part_name, p.sub_module_code
				FROM domain_catalog.part_alias a
				JOIN domain_catalog.part p ON p.part_id = a.part_id
				WHERE a.is_deleted = false AND p.is_deleted = false
				  AND a.consumer_system = $1 AND a.alias_code = $2`, consumer, alias).Scan(&partCode, &partName, &subCode)
			if err != nil {
				api.RespondEnvelopeError(w, http.StatusNotFound, "part alias not found", "CATALOG_RESOLVE_NOT_FOUND")
				return
			}
			api.RespondEnvelopeSuccess(w, "Resolved", map[string]string{
				"part_code": partCode, "part_name": partName, "sub_module_code": subCode,
			})
		default:
			var code, name, moduleCode string
			err := pool.QueryRow(r.Context(), `
				SELECT s.sub_module_code, s.sub_module_name, s.module_code
				FROM domain_catalog.sub_module_alias a
				JOIN domain_catalog.sub_module s ON s.sub_module_code = a.sub_module_code
				WHERE a.is_deleted = false AND s.is_deleted = false
				  AND a.consumer_system = $1 AND a.alias_code = $2`, consumer, alias).Scan(&code, &name, &moduleCode)
			if err != nil {
				// try sanitized
				san := sanitizeNotifKey(alias)
				err = pool.QueryRow(r.Context(), `
					SELECT s.sub_module_code, s.sub_module_name, s.module_code
					FROM domain_catalog.sub_module_alias a
					JOIN domain_catalog.sub_module s ON s.sub_module_code = a.sub_module_code
					WHERE a.is_deleted = false AND s.is_deleted = false
					  AND a.consumer_system = $1
					  AND regexp_replace(upper(a.alias_code), '[^A-Z0-9]', '', 'g') = $2
					LIMIT 1`, consumer, san).Scan(&code, &name, &moduleCode)
			}
			if err != nil {
				api.RespondEnvelopeError(w, http.StatusNotFound, "sub-module alias not found", "CATALOG_RESOLVE_NOT_FOUND")
				return
			}
			api.RespondEnvelopeSuccess(w, "Resolved", map[string]string{
				"sub_module_code": code, "sub_module_name": name, "module_code": moduleCode,
			})
		}
	}
}
