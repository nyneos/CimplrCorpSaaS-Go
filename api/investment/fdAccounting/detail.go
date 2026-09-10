package fdAccounting

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

const auditHistorySelect = `
	SELECT
		a.action_id::text                       AS action_id,
		a.entry_id,
		COALESCE(a.actiontype,'')               AS action_type,
		COALESCE(a.processing_status,'')        AS processing_status,
		COALESCE(a.reason,'')                   AS reason,
		COALESCE(a.requested_by,'')             AS requested_by,
		COALESCE(TO_CHAR(a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(a.requested_ip,'')             AS requested_ip,
		COALESCE(a.checker_by,'')               AS checker_by,
		COALESCE(TO_CHAR(a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(a.checker_ip,'')               AS checker_ip,
		COALESCE(a.checker_comment,'')          AS checker_comment,
		COALESCE(je.entry_type,'')              AS entry_type,
		COALESCE(je.status,'')                  AS entry_status,
		COALESCE(je.fd_id,'')                   AS fd_id
	FROM ` + journalAuditTable + ` a
	JOIN ` + journalTable + ` je ON je.entry_id = a.entry_id
	WHERE a.entry_id = $1
	ORDER BY a.requested_at DESC, a.action_id DESC`

// DetailJournal handles POST /investment/fd/accounting/journal/detail (AP-02/03).
func DetailJournal(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntryID string `json:"entry_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.EntryID) == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "entry_id is required")
			return
		}
		entryID := strings.TrimSpace(req.EntryID)
		ctx := r.Context()

		args := []interface{}{}
		q := journalSelect + scopeClause(ctxutil.FromContext(ctx), &args)
		args = append(args, entryID)
		q += " AND je.entry_id = $" + itoa(len(args))

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDAccounting] DetailJournal query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		entries, err := scanRowsToMaps(rows)
		rows.Close()
		if err != nil || len(entries) == 0 {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "journal entry not found")
			return
		}
		entry := entries[0]

		// Original entry when this is a reversal.
		var original map[string]interface{}
		if orig, _ := entry["reversal_of_entry_id"].(string); orig != "" {
			oRows, oErr := pool.Query(ctx, journalSelect+" AND je.entry_id = $1", orig)
			if oErr == nil {
				if o, _ := scanRowsToMaps(oRows); len(o) > 0 {
					original = o[0]
				}
				oRows.Close()
			}
		}

		aRows, err := pool.Query(ctx, auditHistorySelect, entryID)
		if err != nil {
			api.LogErrorForResponse(w, "[FDAccounting] DetailJournal audit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		audit, _ := scanRowsToMaps(aRows)
		aRows.Close()

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{
			"entry":          entry,
			"original_entry": original,
			"audit_history":  audit,
			"posting_mode":   postingModeValue,
		})
	}
}

// JournalAudit handles POST /investment/fd/accounting/journal/audit — rows for
// AuditTrailSection.
func JournalAudit(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntryID string `json:"entry_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.EntryID) == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "entry_id is required")
			return
		}
		rows, err := pool.Query(r.Context(), auditHistorySelect, strings.TrimSpace(req.EntryID))
		if err != nil {
			api.LogErrorForResponse(w, "[FDAccounting] JournalAudit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()
		out, err := scanRowsToMaps(rows)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}
		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
	}
}

func itoa(i int) string { return strconv.Itoa(i) }
