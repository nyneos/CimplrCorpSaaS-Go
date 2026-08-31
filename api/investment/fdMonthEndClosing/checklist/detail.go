package checklist

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DetailChecklistItem handles POST /investment/fd-closing/checklist/detail —
// single item + its audit history + its attached files, per the handler
// spec's Section 3 Detail row.
func DetailChecklistItem(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ItemID string `json:"item_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.ItemID = strings.TrimSpace(req.ItemID)
		if req.ItemID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "item_id is required")
			return
		}

		ctx := r.Context()

		var item map[string]interface{}
		var entityID string
		{
			rows, err := pool.Query(ctx, checklistItemSelect+" WHERE i.item_id = $1", req.ItemID)
			if err != nil {
				api.LogErrorForResponse(w, "[FDClosingChecklist] DetailChecklistItem query: %v", err)
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
				return
			}
			rowsOut, scanErr := scanRowsToMaps(rows)
			rows.Close()
			if scanErr != nil {
				api.LogErrorForResponse(w, "[FDClosingChecklist] DetailChecklistItem row error: %v", scanErr)
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
				return
			}
			if len(rowsOut) == 0 {
				fdclosingcommon.RespondError(w, http.StatusNotFound, "Checklist item not found")
				return
			}
			item = rowsOut[0]
			entityID, _ = item["entity_id"].(string)
		}

		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+entityID+"' is not within your authorized access scope.")
			return
		}

		auditRows, err := pool.Query(ctx, checklistAuditSelect+`
			WHERE a.item_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`,
			req.ItemID,
		)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] DetailChecklistItem audit query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		auditHistory, err := scanRowsToMaps(auditRows)
		auditRows.Close()
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] DetailChecklistItem audit row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fileRows, err := pool.Query(ctx, `
			SELECT file_id, stored_file_name, COALESCE(content_type,'') AS content_type,
			       COALESCE(file_size,0) AS file_size, upload_s3_key, COALESCE(uploaded_by,'') AS uploaded_by,
			       TO_CHAR((uploaded_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS') AS uploaded_at
			FROM investment.fd_closing_checklist_item_files
			WHERE item_id = $1 AND is_deleted = false
			ORDER BY uploaded_at DESC`,
			req.ItemID,
		)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] DetailChecklistItem files query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		files, err := scanRowsToMaps(fileRows)
		fileRows.Close()
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] DetailChecklistItem files row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{
			"item":          item,
			"audit_history": auditHistory,
			"files":         files,
		})
		api.LogInfo("[FDClosingChecklist] DetailChecklistItem: item_id=%s", req.ItemID)
	}
}
