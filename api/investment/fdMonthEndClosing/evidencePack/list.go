package evidencePack

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

// packWithDmsJoinQuery is shared by ListEvidencePacks and DownloadEvidencePack.
// It LEFT JOINs the DMS generation chain (dms_svc.generation_run_source_row ->
// dms_svc.generated_document, matched on source_id = pack_id::text) so a
// caller can surface s3_key/status/file_format from the DMS side whenever
// fd_closing_evidence_pack's own s3_key/checksum/file_size are still NULL
// (i.e. generation fired but hasn't completed/been polled yet). See
// generate.go's header comment for why these columns start out NULL.
const packWithDmsJoinQuery = `
	SELECT
		p.pack_id, p.cycle_id, p.format,
		p.include_accrual_ledger, p.include_reconciliation_report, p.include_exceptions_register,
		p.include_posting_summary, p.include_approval_logs, p.include_period_lock_certificate,
		p.include_audit_trail, p.include_supporting_documents,
		COALESCE(p.s3_key, gd.s3_key, '') AS s3_key,
		COALESCE(p.file_size, gd.file_size, 0) AS file_size,
		p.report_count,
		COALESCE(p.page_count, 0) AS page_count,
		COALESCE(p.document_count, 0) AS document_count,
		COALESCE(p.checksum, gd.checksum, '') AS checksum,
		p.download_count,
		p.generated_by,
		TO_CHAR((p.generated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS') AS generated_at,
		COALESCE(gd.status, '') AS dms_status,
		COALESCE(gd.file_format, '') AS dms_file_format,
		p.is_deleted
	FROM investment.fd_closing_evidence_pack p
	LEFT JOIN LATERAL (
		SELECT sr.run_id
		FROM dms_svc.generation_run_source_row sr
		WHERE sr.source_id = p.pack_id::text
		ORDER BY sr.run_id DESC
		LIMIT 1
	) src ON true
	LEFT JOIN dms_svc.generated_document gd ON gd.run_id = src.run_id`

// ListEvidencePacks handles POST /investment/fd-closing/evidence/list.
// cycle_id is optional: when supplied it scopes to that one cycle (the
// original single-cycle history view); when omitted it lists every pack
// across every cycle the caller's entity scope can see (the "All Evidence
// Packs" tab), entity-filtered the same way cycle/list.go filters cycles.
func ListEvidencePacks(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID string `json:"cycle_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req) // body is optional for the all-packs listing

		req.CycleID = strings.TrimSpace(req.CycleID)
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		q := packWithDmsJoinQuery + ` WHERE p.is_deleted = false`
		args := []interface{}{}
		argIdx := 1

		if req.CycleID != "" {
			var entityID string
			if err := pool.QueryRow(ctx, `SELECT entity_id FROM `+cycleTable+` WHERE cycle_id = $1`, req.CycleID).Scan(&entityID); err != nil {
				fdclosingcommon.RespondError(w, http.StatusNotFound, "Cycle not found")
				return
			}
			if !scope.HasEntityAccess(entityID) {
				fdclosingcommon.RespondError(w, http.StatusForbidden,
					"Entity ID '"+entityID+"' is not within your authorized access scope.")
				return
			}
			q += " AND p.cycle_id = $" + strconv.Itoa(argIdx)
			args = append(args, req.CycleID)
			argIdx++
		} else if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
			q += ` AND EXISTS (
				SELECT 1 FROM ` + cycleTable + ` c
				WHERE c.cycle_id = p.cycle_id AND c.entity_id = ANY($` + strconv.Itoa(argIdx) + `::text[])
			)`
			args = append(args, scope.EntityIDs)
			argIdx++
		}
		q += ` ORDER BY p.generated_at DESC`

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingEvidencePack] ListEvidencePacks query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingEvidencePack] ListEvidencePacks row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
		api.LogInfo("[FDClosingEvidencePack] ListEvidencePacks: cycle=%q rows=%d", req.CycleID, len(out))
	}
}

// scanRowsToMaps converts a pgx.Rows result into []map[string]interface{},
// same pattern as cycle package's helper of the same name (kept as its own
// copy here since it is unexported in the cycle package).
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
