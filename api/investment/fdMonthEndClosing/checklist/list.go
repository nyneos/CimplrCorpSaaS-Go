package checklist

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// checklistItemSelect is the shared SELECT (no WHERE/ORDER BY) for a
// checklist item row — used by both ListChecklistItems (the "N steps x N FDs"
// grid) and DetailChecklistItem's single-row lookup, so the two never drift
// out of sync on column shape (same pairing convention as
// cycle/list.go+cycle/detail.go). entity_id is included so callers can run
// ctxutil scope checks without a second round trip.
const checklistItemSelect = `
	SELECT
		i.item_id, i.cycle_id, i.fd_id, i.scope_id,
		i.step_code, i.step_name, i.owner_role, i.sequence, i.is_critical,
		COALESCE(i.depends_on_step_code,'') AS depends_on_step_code,
		i.status,
		COALESCE(i.evidence_ref,'') AS evidence_ref,
		COALESCE(i.evidence_type,'') AS evidence_type,
		i.exception_count,
		COALESCE(i.blocked_comment,'') AS blocked_comment,
		COALESCE(i.last_updated_by,'') AS last_updated_by,
		COALESCE(TO_CHAR((i.last_updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS last_updated_at,
		TO_CHAR((i.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS') AS created_at,
		c.entity_id AS entity_id
	FROM investment.fd_closing_checklist_item i
	JOIN investment.fd_closing_cycle c ON c.cycle_id = i.cycle_id`

// ListChecklistItems handles POST /investment/fd-closing/checklist/list —
// this is what the per-cycle "5 steps x N FDs" grid renders from. fd_id is an
// optional filter (single-FD drill-down); omitted, every FD in the cycle's
// scope is returned.
func ListChecklistItems(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID string `json:"cycle_id"`
			FDID    string `json:"fd_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.CycleID = strings.TrimSpace(req.CycleID)
		if req.CycleID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "cycle_id is required")
			return
		}

		ctx := r.Context()
		q := checklistItemSelect + " WHERE i.cycle_id = $1"
		args := []interface{}{req.CycleID}

		if fdID := strings.TrimSpace(req.FDID); fdID != "" {
			args = append(args, fdID)
			q += " AND i.fd_id = $" + strconv.Itoa(len(args))
		}

		scope := ctxutil.FromContext(ctx)
		if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
			args = append(args, scope.EntityIDs)
			q += " AND c.entity_id = ANY($" + strconv.Itoa(len(args)) + "::text[])"
		}
		q += " ORDER BY i.sequence ASC, i.fd_id ASC"

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] ListChecklistItems query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] ListChecklistItems row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
		api.LogInfo("[FDClosingChecklist] ListChecklistItems: cycle=%s rows=%d", req.CycleID, len(out))
	}
}

// scanRowsToMaps converts a pgx.Rows result into []map[string]interface{} —
// same shape as fdMonthEndClosing/cycle's own copy of this helper. It is
// package-local by design (not shared across feature packages), matching that
// package's existing precedent rather than introducing a new grab-bag utils
// file.
func scanRowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0, 50)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[string(f.Name)] = ""
			} else {
				row[string(f.Name)] = vals[i]
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
