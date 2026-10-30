package common

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/ctxutil"
)

// AppendEntityScopeFilter keeps org-wide rows (NULL entity_id) plus rows whose
// entity_id is in the caller's prevalidated entity list. Admin override and an
// empty entity list (legacy "all entities") skip the filter.
func AppendEntityScopeFilter(
	query string,
	args []interface{},
	entityCol string,
	scope ctxutil.RequestScope,
) (string, []interface{}) {
	if scope.IsAdminOverride || len(scope.EntityIDs) == 0 {
		return query, args
	}
	args = append(args, scope.EntityIDs)
	n := len(args)
	query += fmt.Sprintf(
		" AND (%s IS NULL OR %s = ANY($%d))",
		entityCol, entityCol, n,
	)
	return query, args
}

// RequireEntityAccess rejects when the caller cannot see entityID.
// Empty entityID (org-wide) is always allowed.
func RequireEntityAccess(w http.ResponseWriter, scope ctxutil.RequestScope, entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return true
	}
	if scope.HasEntityAccess(entityID) {
		return true
	}
	api.RespondEnvelopeError(w, http.StatusForbidden, "entity is outside your access scope", "DMS_ENTITY_FORBIDDEN")
	return false
}

// RecordsEntityAllowed runs query (one nullable entity id per row) and reports
// whether every row is org-wide or inside the caller's entity scope.
func RecordsEntityAllowed(ctx context.Context, db api.RowQuerier, query string, args ...any) (bool, error) {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return true, nil
	}
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var entityID *string
		if err := rows.Scan(&entityID); err != nil {
			return false, err
		}
		if entityID != nil && strings.TrimSpace(*entityID) != "" && !scope.HasEntityAccess(*entityID) {
			return false, nil
		}
	}
	return true, rows.Err()
}

// RequireRecordsEntityAccess writes a 403 (or 500) unless RecordsEntityAllowed passes.
func RequireRecordsEntityAccess(w http.ResponseWriter, r *http.Request, db api.RowQuerier, query string, args ...any) bool {
	ok, err := RecordsEntityAllowed(r.Context(), db, query, args...)
	if err != nil {
		api.LogErrorForResponse(w, "dms entity scope lookup: %v", err)
		api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to verify record access", "DMS_SCOPE_LOOKUP_FAILED")
		return false
	}
	if !ok {
		api.RespondEnvelopeError(w, http.StatusForbidden, "entity is outside your access scope", "DMS_ENTITY_FORBIDDEN")
		return false
	}
	return true
}

const (
	RuleEntityQuery     = `SELECT entity_id::text FROM dms_svc.generation_rule WHERE rule_id::text = ANY($1)`
	TemplateEntityQuery = `SELECT entity_id::text FROM dms_svc.template WHERE template_id::text = ANY($1)`
	RunEntityQuery      = `SELECT r.entity_id::text FROM dms_svc.generation_run gr LEFT JOIN dms_svc.generation_rule r ON r.rule_id = gr.rule_id WHERE gr.run_id::text = ANY($1)`
	DocEntityQuery      = `SELECT r.entity_id::text FROM dms_svc.generated_document gd JOIN dms_svc.generation_run gr ON gr.run_id = gd.run_id LEFT JOIN dms_svc.generation_rule r ON r.rule_id = gr.rule_id WHERE gd.doc_id::text = ANY($1)`
)
