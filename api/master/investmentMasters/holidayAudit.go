package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"net/http"
	"strings"
	"time"

	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// ─────────────────────────────────────────────────────────────────────────────
// Holiday Master — dedicated maker-checker pipeline.
//
// Holidays keep their audit trail in investment.auditactionmasterholiday, keyed
// by holiday_id and carrying calendar_id so approval can still be done at the
// calendar level (preserving today's UX) while the audit stream is fully
// decoupled from investment.auditactioncalendar.
// ─────────────────────────────────────────────────────────────────────────────

// BulkApproveHolidayActions approves the latest pending audit row per holiday.
// Accepts calendar_ids (primary — approves every pending holiday under those
// calendars, matching the current calendar-scoped approval UX) and/or
// holiday_ids (finer control). A PENDING_DELETE_APPROVAL row that gets approved
// soft-deletes the holiday.
// POST /master/holiday/bulk-approve
func BulkApproveHolidayActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID      string   `json:"user_id"`
			CalendarIDs []string `json:"calendar_ids"`
			HolidayIDs  []string `json:"holiday_ids"`
			Comment     string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.CalendarIDs) == 0 && len(req.HolidayIDs) == 0 {
			api.RespondWithError(w, 400, "calendar_ids or holiday_ids required")
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, 401, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed")
			return
		}
		defer tx.Rollback(ctx)

		// Select the latest audit row per holiday, scoped by holiday_ids if given,
		// else by calendar_ids.
		var sel string
		var selArg interface{}
		if len(req.HolidayIDs) > 0 {
			sel = `
				SELECT DISTINCT ON (holiday_id)
					action_id::text, holiday_id::text, actiontype, processing_status
				FROM investment.auditactionmasterholiday
				WHERE holiday_id = ANY($1)
				ORDER BY holiday_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz), COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC, action_id DESC
			`
			selArg = req.HolidayIDs
		} else {
			sel = `
				SELECT DISTINCT ON (holiday_id)
					action_id::text, holiday_id::text, actiontype, processing_status
				FROM investment.auditactionmasterholiday
				WHERE calendar_id = ANY($1)
				ORDER BY holiday_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz), COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC, action_id DESC
			`
			selArg = req.CalendarIDs
		}

		rows, err := tx.Query(ctx, sel, selArg)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		toApprove := []string{}
		toDeleteActionIDs := []string{}
		deleteHolidayIDs := []string{}

		for rows.Next() {
			var aid, hid, atype, pstatus string
			if err := rows.Scan(&aid, &hid, &atype, &pstatus); err != nil {
				logger.LogError("BulkApproveHolidayActions: scan failed: %v", err)
				continue
			}

			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == constants.StatusApproved {
				continue
			}
			if ps == constants.StatusPendingDeleteApproval {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteHolidayIDs = append(deleteHolidayIDs, hid)
				continue
			}
			if ps == constants.StatusPendingApproval || ps == constants.StatusPendingEditApproval {
				toApprove = append(toApprove, aid)
			}
		}
		rows.Close()

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions,
				map[string]any{"approved_action_ids": []string{}, "deleted_holidays": []string{}})
			return
		}

		if len(toApprove) > 0 {
			_, err := tx.Exec(ctx, `
				UPDATE investment.auditactionmasterholiday
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove)
			if err != nil {
				api.RespondWithError(w, 500, "approve update failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			_, err := tx.Exec(ctx, `
				UPDATE investment.auditactionmasterholiday
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs)
			if err != nil {
				api.RespondWithError(w, 500, "mark deleted failed: "+err.Error())
				return
			}

			_, err = tx.Exec(ctx, `
				UPDATE investment.masterholiday
				SET is_deleted=true, status='Inactive'
				WHERE holiday_id = ANY($1)
			`, deleteHolidayIDs)
			if err != nil {
				api.RespondWithError(w, 500, "holiday soft-delete failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "",
			map[string]any{"approved_action_ids": toApprove, "deleted_holidays": deleteHolidayIDs})
	}
}

// BulkRejectHolidayActions rejects the latest pending audit row per holiday.
// Scoped by holiday_ids if given, else by calendar_ids.
// POST /master/holiday/bulk-reject
func BulkRejectHolidayActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID      string   `json:"user_id"`
			CalendarIDs []string `json:"calendar_ids"`
			HolidayIDs  []string `json:"holiday_ids"`
			Comment     string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.CalendarIDs) == 0 && len(req.HolidayIDs) == 0 {
			api.RespondWithError(w, 400, "calendar_ids or holiday_ids required")
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, 401, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed")
			return
		}
		defer tx.Rollback(ctx)

		var sel string
		var selArg interface{}
		if len(req.HolidayIDs) > 0 {
			sel = `
				SELECT DISTINCT ON (holiday_id)
					action_id::text, holiday_id::text, processing_status
				FROM investment.auditactionmasterholiday
				WHERE holiday_id = ANY($1)
				ORDER BY holiday_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz), COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC, action_id DESC
			`
			selArg = req.HolidayIDs
		} else {
			sel = `
				SELECT DISTINCT ON (holiday_id)
					action_id::text, holiday_id::text, processing_status
				FROM investment.auditactionmasterholiday
				WHERE calendar_id = ANY($1)
				ORDER BY holiday_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz), COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC, action_id DESC
			`
			selArg = req.CalendarIDs
		}

		rows, err := tx.Query(ctx, sel, selArg)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var actionIDs, cannotReject []string
		for rows.Next() {
			var aid, hid, ps string
			if err := rows.Scan(&aid, &hid, &ps); err != nil {
				logger.LogError("BulkRejectHolidayActions: scan failed: %v", err)
				continue
			}
			if strings.ToUpper(strings.TrimSpace(ps)) == constants.StatusApproved {
				cannotReject = append(cannotReject, hid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}
		rows.Close()

		if len(actionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions,
				map[string]any{"rejected_action_ids": []string{}, "cannot_reject": cannotReject})
			return
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.auditactionmasterholiday
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrUpdateFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrCommitFailedUser)
			return
		}

		api.RespondWithPayload(w, true, "",
			map[string]any{"rejected_action_ids": actionIDs, "cannot_reject": cannotReject})
	}
}

// GetHolidaysWithAudit returns every holiday joined with its latest audit row and
// created/edited/deleted history — the maker-checker management grid. Optionally
// filtered to a single calendar via {calendar_id}.
// POST /master/holiday/all
func GetHolidaysWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CalendarID string `json:"calendar_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		baseQ := `
WITH latest_audit AS (
    SELECT DISTINCT ON (a.holiday_id)
        a.holiday_id, a.actiontype, a.processing_status, a.action_id,
        a.requested_by, a.requested_at, a.checker_by, a.checker_at,
        a.checker_comment, a.reason
    FROM investment.auditactionmasterholiday a
    WHERE a.actiontype IN ('CREATE','EDIT','DELETE')
    ORDER BY a.holiday_id, GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamptz), COALESCE(a.checker_at,'1970-01-01'::timestamptz)) DESC, a.action_id DESC
),
history AS (
    SELECT
        holiday_id,
        MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
        MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS created_at,
        MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
        MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS edited_at,
        MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
        MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS deleted_at
    FROM investment.auditactionmasterholiday
    GROUP BY holiday_id
)
SELECT
    mh.holiday_id,
    mh.calendar_id,
    TO_CHAR(mh.holiday_date,'YYYY-MM-DD') AS holiday_date,
    TO_CHAR(mh.old_holiday_date,'YYYY-MM-DD') AS old_holiday_date,
    mh.holiday_name,
    mh.old_holiday_name,
    mh.holiday_type,
    mh.old_holiday_type,
    mh.recurrence_rule,
    mh.old_recurrence_rule,
    mh.notes,
    mh.old_notes,
    mh.status,
    mh.old_status,
    mh.ingestion_source,
    mh.is_deleted,

    COALESCE(l.actiontype,'')        AS action_type,
    COALESCE(l.processing_status,'') AS processing_status,
    COALESCE(l.action_id::text,'')   AS action_id,
    COALESCE(l.requested_by,'')      AS requested_by,
    TO_CHAR(l.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
    COALESCE(l.checker_by,'')        AS checker_by,
    TO_CHAR(l.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
    COALESCE(l.checker_comment,'')   AS checker_comment,
    COALESCE(l.reason,'')            AS reason,

    COALESCE(h.created_by,'') AS created_by,
    COALESCE(h.created_at,'') AS created_at,
    COALESCE(h.edited_by,'')  AS edited_by,
    COALESCE(h.edited_at,'')  AS edited_at,
    COALESCE(h.deleted_by,'') AS deleted_by,
    COALESCE(h.deleted_at,'') AS deleted_at

FROM investment.masterholiday mh
LEFT JOIN latest_audit l ON l.holiday_id::text = mh.holiday_id::text
LEFT JOIN history h ON h.holiday_id::text = mh.holiday_id::text
WHERE COALESCE(mh.is_deleted,false) = false`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.CalendarID) != "" {
			q = baseQ + `
  AND mh.calendar_id = $1
ORDER BY mh.holiday_date, mh.holiday_name`
			args = append(args, req.CalendarID)
		} else {
			q = baseQ + `
ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamptz), COALESCE(l.checker_at,'1970-01-01'::timestamptz)) DESC, mh.holiday_date`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				name := string(f.Name)
				if vals[i] == nil {
					rec[name] = ""
				} else if t, ok := vals[i].(time.Time); ok {
					rec[name] = t.Format(constants.DateTimeFormat)
				} else {
					rec[name] = vals[i]
				}
			}
			out = append(out, rec)
		}
		if rows.Err() != nil {
			api.RespondWithError(w, 500, constants.ErrScanFailedPrefix+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// GetApprovedActiveHolidays returns holidays whose latest holiday-audit row is
// APPROVED and that are active/not-deleted. Optionally scoped to {calendar_id}.
// POST /master/holiday/approved-active
func GetApprovedActiveHolidays(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CalendarID string `json:"calendar_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		baseQ := `
WITH latest AS (
    SELECT DISTINCT ON (holiday_id)
        holiday_id, processing_status
    FROM investment.auditactionmasterholiday
    ORDER BY holiday_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamptz), COALESCE(checker_at,'1970-01-01'::timestamptz)) DESC, action_id DESC
)
SELECT
    mh.holiday_id,
    mh.calendar_id,
    TO_CHAR(mh.holiday_date,'YYYY-MM-DD') AS holiday_date,
    mh.holiday_name,
    mh.holiday_type,
    COALESCE(mh.recurrence_rule,'') AS recurrence_rule,
    COALESCE(mh.notes,'') AS notes
FROM investment.masterholiday mh
JOIN latest l ON l.holiday_id::text = mh.holiday_id::text
WHERE UPPER(l.processing_status) = 'APPROVED'
  AND UPPER(mh.status) = 'ACTIVE'
  AND COALESCE(mh.is_deleted,false) = false`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.CalendarID) != "" {
			q = baseQ + `
  AND mh.calendar_id = $1
ORDER BY mh.holiday_date, mh.holiday_name`
			args = append(args, req.CalendarID)
		} else {
			q = baseQ + `
ORDER BY mh.holiday_date, mh.holiday_name`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			vals, _ := rows.Values()
			rec := map[string]interface{}{
				"holiday_id":      vals[0],
				"calendar_id":     vals[1],
				"holiday_date":    vals[2],
				"holiday_name":    vals[3],
				"holiday_type":    vals[4],
				"recurrence_rule": vals[5],
				"notes":           vals[6],
			}
			out = append(out, rec)
		}
		if rows.Err() != nil {
			api.RespondWithError(w, 500, constants.ErrScanFailedPrefix+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
