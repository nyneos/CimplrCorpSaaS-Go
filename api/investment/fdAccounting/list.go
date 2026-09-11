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

// journalSelect is the shared projection for list + detail. One row per
// journal entry; lines come back as a JSON array in line_items; the newest
// maker-checker audit row is joined LATERAL, same shape as GetFVOsWithAudit.
const journalSelect = `
	SELECT
		je.entry_id,
		COALESCE(je.activity_id::text,'')            AS activity_id,
		COALESCE(je.entity_id,'')                    AS entity_id,
		COALESCE(je.entity_name,'')                  AS entity_name,
		COALESCE(je.fd_id,'')                        AS fd_id,
		COALESCE(je.receipt_id,'')                   AS receipt_id,
		COALESCE(je.accrual_run_id,'')               AS accrual_run_id,
		COALESCE(je.accrual_ledger_id::text,'')      AS accrual_ledger_id,
		COALESCE(je.closure_request_id::text,'')     AS closure_request_id,
		COALESCE(je.is_reversal,false)               AS is_reversal,
		COALESCE(je.reversal_of_entry_id,'')         AS reversal_of_entry_id,
		TO_CHAR(je.entry_date,'YYYY-MM-DD')          AS entry_date,
		COALESCE(je.accounting_period,'')            AS accounting_period,
		COALESCE(je.entry_type,'')                   AS entry_type,
		COALESCE(je.description,'')                  AS description,
		COALESCE(je.total_debit,0)                   AS total_debit,
		COALESCE(je.total_credit,0)                  AS total_credit,
		COALESCE(je.status,'')                       AS status,
		COALESCE(je.reason_code,'')                  AS reason_code,
		COALESCE(je.remarks,'')                      AS remarks,
		COALESCE(je.posted_by,'')                    AS posted_by,
		COALESCE(TO_CHAR(je.posted_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS posted_at,
		COALESCE(je.posting_reference,'')            AS posting_reference,
		COALESCE(je.failure_reason,'')               AS failure_reason,
		COALESCE(je.gl_mapping_version,'')           AS gl_mapping_version,
		COALESCE(TO_CHAR(je.created_at,'YYYY-MM-DD HH24:MI:SS'),'') AS created_at,
		COALESCE(je.created_by,'')                   AS created_by,
		(SELECT entry_id FROM ` + journalTable + ` r
		   WHERE r.reversal_of_entry_id = je.entry_id AND COALESCE(r.is_deleted,false) = false
		     AND COALESCE(r.status,'') <> 'REJECTED' ORDER BY r.created_at DESC LIMIT 1) AS reversed_by_entry_id,
		(SELECT COUNT(*) FROM ` + journalLineTable + ` jl WHERE jl.entry_id = je.entry_id) AS line_count,
		(SELECT COALESCE(json_agg(json_build_object(
			'line_id', jl.line_id, 'line_number', jl.line_number,
			'account_number', COALESCE(jl.account_number,''), 'account_name', COALESCE(jl.account_name,''),
			'account_type', COALESCE(jl.account_type,''),
			'debit_amount', COALESCE(jl.debit_amount,0), 'credit_amount', COALESCE(jl.credit_amount,0),
			'narration', COALESCE(jl.narration,'')) ORDER BY jl.line_number), '[]'::json)
		 FROM ` + journalLineTable + ` jl WHERE jl.entry_id = je.entry_id) AS line_items,
		COALESCE(l.actiontype,'')                    AS action_type,
		COALESCE(l.processing_status,'')             AS processing_status,
		COALESCE(l.action_id::text,'')               AS action_id,
		COALESCE(l.requested_by,'')                  AS requested_by,
		COALESCE(TO_CHAR(l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(l.checker_by,'')                    AS checker_by,
		COALESCE(TO_CHAR(l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(l.checker_comment,'')               AS checker_comment,
		COALESCE(l.reason,'')                        AS audit_reason
	FROM ` + journalTable + ` je
	LEFT JOIN LATERAL (
		SELECT a.* FROM ` + journalAuditTable + ` a
		WHERE a.entry_id = je.entry_id
		  AND UPPER(COALESCE(a.actiontype,'')) NOT IN ('UPLOAD_FILE','DOWNLOAD')
		ORDER BY a.requested_at DESC LIMIT 1
	) l ON true
	WHERE COALESCE(je.is_deleted,false) = false` + fdJournalPredicate

type listRequest struct {
	EntityID         string `json:"entity_id"`
	AccountingPeriod string `json:"accounting_period"`
	EntryType        string `json:"entry_type"`
	Status           string `json:"status"`
	ProcessingStatus string `json:"processing_status"`
	FDID             string `json:"fd_id"`
	FromDate         string `json:"from_date"`
	ToDate           string `json:"to_date"`
	OnlyPostable     bool   `json:"only_postable"` // status = POSTED and not reversed — for the reversal picker
}

func buildFilters(req listRequest, args *[]interface{}) string {
	q := ""
	add := func(clause string, v interface{}) {
		*args = append(*args, v)
		q += " AND " + strings.ReplaceAll(clause, "?", "$"+strconv.Itoa(len(*args)))
	}
	if s := strings.TrimSpace(req.EntityID); s != "" {
		add("(je.entity_id = ? OR je.entity_name = ?)", s)
	}
	if s := strings.TrimSpace(req.AccountingPeriod); s != "" {
		add("je.accounting_period = ?", s)
	}
	if s := strings.TrimSpace(req.EntryType); s != "" {
		add("je.entry_type = ?", s)
	}
	if s := strings.TrimSpace(req.Status); s != "" {
		add("je.status = ?", s)
	}
	if s := strings.TrimSpace(req.FDID); s != "" {
		add("je.fd_id = ?", s)
	}
	if s := strings.TrimSpace(req.FromDate); s != "" {
		add("je.entry_date >= ?::date", s)
	}
	if s := strings.TrimSpace(req.ToDate); s != "" {
		add("je.entry_date <= ?::date", s)
	}
	if req.OnlyPostable {
		q += ` AND je.status = 'POSTED' AND COALESCE(je.is_reversal,false) = false
		       AND NOT EXISTS (SELECT 1 FROM ` + journalTable + ` r
		           WHERE r.reversal_of_entry_id = je.entry_id AND COALESCE(r.is_deleted,false) = false
		             AND COALESCE(r.status,'') NOT IN ('REJECTED'))`
	}
	return q
}

// ListJournals handles POST /investment/fd/accounting/journal/list (AP-01).
func ListJournals(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req listRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		args := []interface{}{}
		q := journalSelect + scopeClause(ctxutil.FromContext(ctx), &args) + buildFilters(req, &args)
		if strings.TrimSpace(req.ProcessingStatus) != "" {
			args = append(args, strings.TrimSpace(req.ProcessingStatus))
			q += " AND COALESCE(l.processing_status,'') = $" + strconv.Itoa(len(args))
		}
		q += " ORDER BY je.entry_date DESC, je.created_at DESC, je.entry_id DESC"

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDAccounting] ListJournals query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()
		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDAccounting] ListJournals rows: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}
		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
	}
}

// JournalKpis handles POST /investment/fd/accounting/journal/kpis (AP-01 tiles).
func JournalKpis(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req listRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		ctx := r.Context()
		args := []interface{}{}
		where := `WHERE COALESCE(je.is_deleted,false) = false` + fdJournalPredicate +
			scopeClause(ctxutil.FromContext(ctx), &args) + buildFilters(req, &args)

		q := `
			SELECT
				COUNT(*)                                                       AS total,
				COUNT(*) FILTER (WHERE je.status = 'PENDING_APPROVAL')          AS pending_approval,
				COUNT(*) FILTER (WHERE je.status = 'APPROVED')                  AS ready_to_post,
				COUNT(*) FILTER (WHERE je.status = 'POSTED')                    AS posted,
				COUNT(*) FILTER (WHERE je.status = 'FAILED')                    AS failed,
				COUNT(*) FILTER (WHERE je.status = 'REVERSED')                  AS reversed,
				COUNT(*) FILTER (WHERE ABS(COALESCE(je.total_debit,0) - COALESCE(je.total_credit,0)) > 0.005) AS unbalanced,
				COALESCE(SUM(je.total_debit) FILTER (WHERE je.status = 'POSTED'),0) AS posted_amount
			FROM ` + journalTable + ` je ` + where

		var total, pending, ready, posted, failed, reversed, unbalanced int64
		var postedAmount float64
		if err := pool.QueryRow(ctx, q, args...).Scan(&total, &pending, &ready, &posted, &failed, &reversed, &unbalanced, &postedAmount); err != nil {
			api.LogErrorForResponse(w, "[FDAccounting] JournalKpis: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{
			"total": total, "pending_approval": pending, "ready_to_post": ready, "posted": posted,
			"failed": failed, "reversed": reversed, "unbalanced": unbalanced, "posted_amount": postedAmount,
			"posting_mode": postingModeValue,
		})
	}
}
