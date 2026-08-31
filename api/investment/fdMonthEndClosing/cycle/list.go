package cycle

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

// listWithAuditQuery is shared by ListCycles (all non-deleted cycles) and
// ListApprovedActiveCycles (a status-filtered subset) — same latest_audit +
// approval-engine LATERAL join shape as fdBookingWorkbench's
// GetBookingsWithAudit, per the handler spec's Section 9 query conventions.
const listWithAuditQuery = `
	WITH latest_audit AS (
		SELECT DISTINCT ON (a.cycle_id)
			a.cycle_id, a.audit_id, a.action_type, a.processing_status,
			a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason,
			a.old_bank_id, a.old_currency_code, a.old_include_matured,
			a.new_bank_id, a.new_currency_code, a.new_include_matured
		FROM investment.fd_closing_cycle_audit a
		ORDER BY a.cycle_id,
		         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
		                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
	)
	SELECT
		m.cycle_id, m.close_type, m.entity_id, m.entity_name,
		COALESCE(m.bank_id,'') AS bank_id, COALESCE(m.bank_name,'') AS bank_name,
		COALESCE(m.currency_code,'') AS currency_code,
		m.financial_period,
		TO_CHAR(m.period_start,'YYYY-MM-DD') AS period_start,
		TO_CHAR(m.period_end,'YYYY-MM-DD') AS period_end,
		m.include_matured, m.source, m.status,
		m.fd_count, m.readiness_score, m.blocker_count, m.eligibility,
		m.initiated_by, TO_CHAR(m.initiated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS initiated_at,
		m.is_deleted, m.created_by, TO_CHAR(m.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,

		COALESCE(l.audit_id::text,'') AS audit_id,
		COALESCE(l.action_type,'') AS action_type,
		COALESCE(l.processing_status,'') AS processing_status,
		COALESCE(l.requested_by,'') AS requested_by,
		COALESCE(TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(l.checker_by,'') AS checker_by,
		COALESCE(TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(l.checker_comment,'') AS checker_comment,
		COALESCE(l.reason,'') AS reason,
		COALESCE(l.old_bank_id,'') AS old_bank_id,
		COALESCE(l.old_currency_code,'') AS old_currency_code,
		COALESCE(l.old_include_matured,false) AS old_include_matured,
		COALESCE(l.new_bank_id,'') AS new_bank_id,
		COALESCE(l.new_currency_code,'') AS new_currency_code,
		COALESCE(l.new_include_matured,false) AS new_include_matured,

		COALESCE(ai.instance_id,'') AS approval_instance_id,
		COALESCE(ai.status,'') AS approval_engine_status,
		COALESCE(aie.instance_eye_id,'') AS current_eye_id,
		COALESCE(aie.position::text,'') AS current_eye_position,
		COALESCE(aie.approvals_required,0) AS approvals_required,
		COALESCE(aie.approvals_received,0) AS approvals_received,
		aie.sla_deadline AS sla_deadline,
		COALESCE(aie.is_escalated,false) AS is_escalated
	FROM investment.fd_closing_cycle m
	LEFT JOIN latest_audit l ON l.cycle_id = m.cycle_id
	LEFT JOIN LATERAL (
		SELECT ai.* FROM uam.approval_instance ai
		WHERE ai.record_id = m.cycle_id AND ai.module_code = $moduleCode$
		  AND ai.status = 'PENDING' AND ai.is_deleted = false
		ORDER BY ai.submitted_at DESC, ai.instance_id DESC
		LIMIT 1
	) ai ON true
	LEFT JOIN LATERAL (
		SELECT aie.* FROM uam.approval_instance_eye aie
		WHERE aie.instance_id = ai.instance_id AND aie.status = 'ACTIVE'
		ORDER BY aie.position ASC, aie.instance_eye_id ASC
		LIMIT 1
	) aie ON true
	WHERE m.is_deleted = false`

// ListCycles handles POST /investment/fd-closing/cycle/list.
func ListCycles(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntityID string `json:"entity_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req) // body is optional for list

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		q := strings.Replace(listWithAuditQuery, "$moduleCode$", "$1", 1)
		args := []interface{}{moduleCode}
		argIdx := 2

		if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
			q += " AND m.entity_id = ANY($" + strconv.Itoa(argIdx) + "::text[])"
			args = append(args, scope.EntityIDs)
			argIdx++
		}
		if strings.TrimSpace(req.EntityID) != "" {
			q += " AND m.entity_id = $" + strconv.Itoa(argIdx)
			args = append(args, strings.TrimSpace(req.EntityID))
			argIdx++
		}
		q += ` ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp)
		) DESC`

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] ListCycles query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] ListCycles row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
		api.LogInfo("[FDClosingCycle] ListCycles: %d rows", len(out))
	}
}

// scanRowsToMaps converts a pgx.Rows result into []map[string]interface{},
// same pattern as fdBookingWorkbench's GetBookingsWithAudit/GetApprovedActiveBookings.
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
