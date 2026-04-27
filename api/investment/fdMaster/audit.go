package fdMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── GetFDMasterAuditHistory ──────────────────────────────────────────────────
// GET /investment/fd/master/audit?fd_id=
// Response: { success: true, data: [...] }

func GetFDMasterAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFDIDRequired)
			return
		}

		auditTable := resolveFDAuditTable(ctx, pgxPool)
		if auditTable == "" {
			api.RespondWithPayload(w, true, "", []map[string]interface{}{})
			return
		}

		schemaName, tableName := splitQualifiedTable(auditTable)
		cols, err := loadTableColumns(ctx, pgxPool, schemaName, tableName)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		refCol := pickFirstExistingColumn(cols, "fd_id", "master_id", "confirmation_id")
		if refCol == "" {
			api.RespondWithPayload(w, true, "", []map[string]interface{}{})
			return
		}

		rows, err := pgxPool.Query(ctx, fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 ORDER BY requested_at DESC", auditTable, refCol), fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		defer rows.Close()

		payload, err := rowsToMaps(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", payload)
	}
}

// ─── GetCashflowAuditHistory ──────────────────────────────────────────────────
// GET /investment/fd/master/cashflow/audit?fd_id=&cashflow_id=
// Response: { success: true, data: [...] }

func GetCashflowAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		fdID := strings.TrimSpace(r.URL.Query().Get("fd_id"))
		cashflowID := strings.TrimSpace(r.URL.Query().Get("cashflow_id"))
		if fdID == "" && cashflowID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id or cashflow_id is required")
			return
		}

		query := `SELECT * FROM investment.fd_audit_cashflow_schedule WHERE 1=1`
		var args []interface{}
		pos := 1
		if fdID != "" {
			query += fmt.Sprintf(" AND fd_id = $%d", pos)
			args = append(args, fdID)
			pos++
		}
		if cashflowID != "" {
			query += fmt.Sprintf(" AND cashflow_id = $%d", pos)
			args = append(args, cashflowID)
			pos++
		}
		query += " ORDER BY requested_at DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToQuery+": "+err.Error())
			return
		}
		defer rows.Close()

		payload, err := rowsToMaps(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+err.Error())
			return
		}
		if payload == nil {
			payload = []map[string]interface{}{}
		}
		api.RespondWithPayload(w, true, "", payload)
	}
}
