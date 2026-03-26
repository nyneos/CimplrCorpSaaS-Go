// Package investmentdashboards — FD Audit Dashboard
//
// POST /dash/investment/fd/audit-dashboard
//
// Returns a comprehensive audit and governance payload covering:
//   - audit_summary       — record counts per audit table + aggregate stats
//   - maker_checker_rate  — % of requests that passed 2nd-level checker
//   - audit_log           — unified trail from all fd_audit_* tables (latest 200 rows)
//   - overrides           — fd_accrual_ledger rows where is_overridden=true
//   - missing_evidence    — overrides that have no attachment
//   - period_reopens      — fd_accrual_period_lock rows where unlocked_at IS NOT NULL
//   - approvals_register  — checker decisions (booking + master audit)
//   - evidence_packs      — period lock list as evidence archive
//   - policy_exceptions   — open variance confirmations + unresolved accrual exceptions
//   - transaction_trace   — full lifecycle for a specific fd_id (optional)
//
// All sub-computations run concurrently via sync.WaitGroup.
package investmentdashboards

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── request type ─────────────────────────────────────────────────────────────

type fdAuditDashRequest struct {
	UserID     string `json:"user_id"`
	EntityID   string `json:"entity_id"`
	Currency   string `json:"currency"`
	Period     string `json:"period"`     // "Today" | "This Week" | "This Month" | "CUSTOM"
	StartDate  string `json:"start_date"` // ISO date for CUSTOM
	EndDate    string `json:"end_date"`
	FDID       string `json:"fd_id"`      // optional — activates transaction_trace
	ActionType string `json:"action_type"`// optional filter: CREATE/EDIT/DELETE/ACTIVATE/STATUS_CHANGE
}

// ─── handler ──────────────────────────────────────────────────────────────────

// GetFDAuditDashboard returns the full FD Audit & Governance payload.
func GetFDAuditDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req fdAuditDashRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Currency == "" {
			req.Currency = "INR"
		}

		now := time.Now().UTC()
		entityFilter := req.EntityID
		fdFilter := req.FDID
		actionFilter := req.ActionType

		// ── resolve period start/end for audit date range ─────────────────────
		var startDate, endDate string
		switch req.Period {
		case "CUSTOM", "Custom":
			startDate = req.StartDate
			endDate = req.EndDate
	case "This Week":
		startDate = periodStartDate("This Week", now).Format("2006-01-02")
		endDate = now.Format("2006-01-02")
	case "This Month":
		startDate = periodStartDate("This Month", now).Format("2006-01-02")
		endDate = now.Format("2006-01-02")
		default: // "Today" + anything else
			startDate = now.Format("2006-01-02")
			endDate = now.AddDate(0, 0, 1).Format("2006-01-02")
		}
		if startDate == "" {
			startDate = now.AddDate(-1, 0, 0).Format("2006-01-02") // default 1 year back
		}
		if endDate == "" {
			endDate = now.AddDate(0, 0, 1).Format("2006-01-02")
		}

		ctx := r.Context()

		type subResult struct {
			data interface{}
			err  error
		}
		results := make(map[string]subResult, 12)
		var mu sync.Mutex
		var wg sync.WaitGroup

		run := func(key string, fn func(context.Context) (interface{}, error)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				d, e := fn(ctx)
				mu.Lock()
				results[key] = subResult{d, e}
				mu.Unlock()
			}()
		}

		// ── 1. Audit summary counts ───────────────────────────────────────────
		run("audit_summary", func(ctx context.Context) (interface{}, error) {
			type summaryRow struct {
				Source          string `json:"source"`
				Total           int64  `json:"total"`
				Pending         int64  `json:"pending"`
				Approved        int64  `json:"approved"`
				Rejected        int64  `json:"rejected"`
				Checked         int64  `json:"checked"` // checker_by IS NOT NULL
			}

			queries := []struct {
				Source string
				Query  string
			}{
				{"MASTER", `
					SELECT 'MASTER',
					  COUNT(*),
					  SUM(CASE WHEN processing_status IN ('PENDING_APPROVAL','PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL') THEN 1 ELSE 0 END),
					  SUM(CASE WHEN processing_status = 'APPROVED' THEN 1 ELSE 0 END),
					  SUM(CASE WHEN processing_status = 'REJECTED' THEN 1 ELSE 0 END),
					  SUM(CASE WHEN checker_by IS NOT NULL THEN 1 ELSE 0 END)
					FROM investment.fd_audit_master
					WHERE requested_at >= $1::date AND requested_at < $2::date
					  AND ($3::text='' OR EXISTS(SELECT 1 FROM investment.fd_master fm WHERE fm.fd_id=fd_audit_master.fd_id AND fm.entity_id=$3))`},
				{"BOOKING", `
					SELECT 'BOOKING',
					  COUNT(*),
					  SUM(CASE WHEN processing_status IN ('PENDING_APPROVAL','PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL') THEN 1 ELSE 0 END),
					  SUM(CASE WHEN processing_status = 'APPROVED' THEN 1 ELSE 0 END),
					  SUM(CASE WHEN processing_status = 'REJECTED' THEN 1 ELSE 0 END),
					  SUM(CASE WHEN checker_by IS NOT NULL THEN 1 ELSE 0 END)
					FROM investment.fd_audit_booking_request
					WHERE requested_at >= $1::date AND requested_at < $2::date
					  AND ($3::text='' OR EXISTS(SELECT 1 FROM investment.fd_booking_request b WHERE b.booking_id=fd_audit_booking_request.booking_id AND b.entity_id=$3))`},
				{"CONFIRMATION", `
					SELECT 'CONFIRMATION',
					  COUNT(*),
					  SUM(CASE WHEN processing_status IN ('PENDING_APPROVAL','PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL') THEN 1 ELSE 0 END),
					  SUM(CASE WHEN processing_status = 'APPROVED' THEN 1 ELSE 0 END),
					  SUM(CASE WHEN processing_status = 'REJECTED' THEN 1 ELSE 0 END),
					  SUM(CASE WHEN checker_by IS NOT NULL THEN 1 ELSE 0 END)
					FROM investment.fd_audit_confirmation
					WHERE requested_at >= $1::date AND requested_at < $2::date
					  AND ($3::text='' OR EXISTS(SELECT 1 FROM investment.fd_confirmation cf WHERE cf.confirmation_id=fd_audit_confirmation.confirmation_id AND EXISTS(SELECT 1 FROM investment.fd_booking_request b WHERE b.booking_id=cf.booking_id AND b.entity_id=$3)))`},
				{"CASHFLOW", `
					SELECT 'CASHFLOW',
					  COUNT(*),
					  SUM(CASE WHEN processing_status IN ('PENDING_APPROVAL','PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL') THEN 1 ELSE 0 END),
					  SUM(CASE WHEN processing_status = 'APPROVED' THEN 1 ELSE 0 END),
					  SUM(CASE WHEN processing_status = 'REJECTED' THEN 1 ELSE 0 END),
					  SUM(CASE WHEN checker_by IS NOT NULL THEN 1 ELSE 0 END)
					FROM investment.fd_audit_cashflow_schedule
					WHERE requested_at >= $1::date AND requested_at < $2::date
					  AND ($3::text='' OR EXISTS(SELECT 1 FROM investment.fd_master fm WHERE fm.fd_id=fd_audit_cashflow_schedule.fd_id AND fm.entity_id=$3))`},
			}

			out := []summaryRow{}
			totalAll, totalChecked, totalPending := int64(0), int64(0), int64(0)
			for _, q := range queries {
				var sr summaryRow
				sr.Source = q.Source
				if err2 := pool.QueryRow(ctx, q.Query, startDate, endDate, entityFilter).
					Scan(&sr.Source, &sr.Total, &sr.Pending, &sr.Approved, &sr.Rejected, &sr.Checked); err2 != nil {
					api.LogError("[AuditDash] audit_summary query error (%s): %v", q.Source, err2)
					continue
				}
				totalAll += sr.Total
				totalChecked += sr.Checked
				totalPending += sr.Pending
				out = append(out, sr)
			}
			mcRate := 0.0
			if totalAll > 0 {
				mcRate = fdRound(float64(totalChecked)/float64(totalAll)*100, 1)
			}
			return map[string]interface{}{
				"by_source":           out,
				"total_records":       totalAll,
				"total_pending":       totalPending,
				"total_checked":       totalChecked,
				"maker_checker_rate":  mcRate,
			}, nil
		})

		// ── 2. Maker-checker rate (cross-table aggregate) ─────────────────────
		run("maker_checker_rate", func(ctx context.Context) (interface{}, error) {
			var checked, total int64
			err := pool.QueryRow(ctx, `
				SELECT
				  SUM(CASE WHEN checker_by IS NOT NULL THEN 1 ELSE 0 END) AS checked,
				  COUNT(*) AS total
				FROM (
				  SELECT checker_by, requested_at FROM investment.fd_audit_master
				  UNION ALL
				  SELECT checker_by, requested_at FROM investment.fd_audit_booking_request
				  UNION ALL
				  SELECT checker_by, requested_at FROM investment.fd_audit_confirmation
				  UNION ALL
				  SELECT checker_by, requested_at FROM investment.fd_audit_cashflow_schedule
				) t
				WHERE requested_at >= $1::date AND requested_at < $2::date`, startDate, endDate).Scan(&checked, &total)
			if err != nil {
				api.LogError("[AuditDash] maker_checker_rate query error: %v", err)
				return map[string]interface{}{"checked": 0, "total": 0, "rate_pct": 0}, nil
			}
			ratePct := 0.0
			if total > 0 {
				ratePct = fdRound(float64(checked)/float64(total)*100, 2)
			}
			return map[string]interface{}{
				"checked":  checked,
				"total":    total,
				"rate_pct": ratePct,
			}, nil
		})

		// ── 3. Unified audit log ──────────────────────────────────────────────
		run("audit_log", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT source, audit_id_text, ref_id, action_type, processing_status,
				       reason, requested_by, requested_at_str, checker_by, checker_at_str,
				       checker_comment, old_value, new_value
				FROM (
				  SELECT
				    'MASTER' AS source,
				    audit_id::text AS audit_id_text,
				    fd_id AS ref_id,
				    action_type,
				    processing_status,
				    COALESCE(reason,'') AS reason,
				    COALESCE(requested_by,'') AS requested_by,
				    COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS requested_at_str,
				    COALESCE(checker_by,'') AS checker_by,
				    COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS checker_at_str,
				    COALESCE(checker_comment,'') AS checker_comment,
				    COALESCE(old_fd_status,'') AS old_value,
				    '' AS new_value,
				    requested_at AS sort_ts
				  FROM investment.fd_audit_master
				  WHERE requested_at >= $1::date AND requested_at < $2::date
				    AND ($3::text='' OR EXISTS(SELECT 1 FROM investment.fd_master fm WHERE fm.fd_id=fd_audit_master.fd_id AND fm.entity_id=$3))
				    AND ($4::text='' OR action_type=$4)
				    AND ($5::text='' OR fd_id=$5)

				  UNION ALL

				  SELECT
				    'BOOKING',
				    audit_id::text,
				    booking_id,
				    action_type,
				    processing_status,
				    COALESCE(reason,''),
				    COALESCE(requested_by,''),
				    COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
				    COALESCE(checker_by,''),
				    COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
				    COALESCE(checker_comment,''),
				    COALESCE(old_booking_status,''),
				    '',
				    requested_at
				  FROM investment.fd_audit_booking_request
				  WHERE requested_at >= $1::date AND requested_at < $2::date
				    AND ($3::text='' OR EXISTS(SELECT 1 FROM investment.fd_booking_request b WHERE b.booking_id=fd_audit_booking_request.booking_id AND b.entity_id=$3))
				    AND ($4::text='' OR action_type=$4)

				  UNION ALL

				  SELECT
				    'CONFIRMATION',
				    audit_id::text,
				    confirmation_id,
				    action_type,
				    processing_status,
				    COALESCE(reason,''),
				    COALESCE(requested_by,''),
				    COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
				    COALESCE(checker_by,''),
				    COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
				    COALESCE(checker_comment,''),
				    COALESCE(old_confirmed_rate::text,''),
				    '',
				    requested_at
				  FROM investment.fd_audit_confirmation
				  WHERE requested_at >= $1::date AND requested_at < $2::date
				    AND ($4::text='' OR action_type=$4)

				  UNION ALL

				  SELECT
				    'CASHFLOW',
				    audit_id::text,
				    fd_id,
				    action_type,
				    processing_status,
				    COALESCE(reason,''),
				    COALESCE(requested_by,''),
				    COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
				    COALESCE(checker_by,''),
				    COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
				    COALESCE(checker_comment,''),
				    COALESCE(old_interest_accrued::text,''),
				    '',
				    requested_at
				  FROM investment.fd_audit_cashflow_schedule
				  WHERE requested_at >= $1::date AND requested_at < $2::date
				    AND ($4::text='' OR action_type=$4)
				    AND ($5::text='' OR fd_id=$5)
				) combined
				ORDER BY sort_ts DESC
				LIMIT 200`, startDate, endDate, entityFilter, actionFilter, fdFilter)
			if err != nil {
				api.LogError("[AuditDash] audit_log query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type logRow struct {
				Source          string `json:"source"`
				AuditID         string `json:"audit_id"`
				RefID           string `json:"ref_id"`
				ActionType      string `json:"action_type"`
				ProcessingStatus string `json:"processing_status"`
				Reason          string `json:"reason"`
				RequestedBy     string `json:"requested_by"`
				RequestedAt     string `json:"requested_at"`
				CheckerBy       string `json:"checker_by"`
				CheckerAt       string `json:"checker_at"`
				CheckerComment  string `json:"checker_comment"`
				OldValue        string `json:"old_value"`
				NewValue        string `json:"new_value"`
			}
			out := []logRow{}
			for rows.Next() {
				var lr logRow
				if err2 := rows.Scan(
					&lr.Source, &lr.AuditID, &lr.RefID,
					&lr.ActionType, &lr.ProcessingStatus, &lr.Reason,
					&lr.RequestedBy, &lr.RequestedAt,
					&lr.CheckerBy, &lr.CheckerAt, &lr.CheckerComment,
					&lr.OldValue, &lr.NewValue,
				); err2 != nil {
					api.LogError("[AuditDash] audit_log scan error: %v", err2)
					continue
				}
				out = append(out, lr)
			}
			return out, nil
		})

		// ── 4. Overrides list ─────────────────────────────────────────────────
		run("overrides", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  al.ledger_id,
				  al.fd_id,
				  al.entity_id,
				  al.bank_id,
				  COALESCE(TO_CHAR(al.accrual_period_start,'YYYY-MM-DD'),'') AS accrual_period,
				  COALESCE(al.override_amount,0) AS override_amount,
				  COALESCE(al.period_interest_accrued,0) AS original_amount,
				  COALESCE(al.override_reason_code,'') AS reason_code,
				  COALESCE(al.override_reason_text,'') AS reason_text,
				  COALESCE(al.override_status,'') AS override_status,
				  COALESCE(al.override_proposed_by,'') AS proposed_by,
				  COALESCE(al.override_approved_by,'') AS approved_by,
				  (al.override_attachment IS NOT NULL) AS has_evidence,
				  COALESCE(TO_CHAR(al.override_proposed_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS proposed_at,
				  al.ledger_row_status
				FROM investment.fd_accrual_ledger al
				WHERE al.is_overridden=true
				  AND al.is_deleted=false
				  AND ($1::text='' OR al.entity_id=$1)
				  AND ($2::text='' OR al.fd_id=$2)
				  AND (al.override_proposed_at IS NULL OR al.override_proposed_at >= $3::date)
				ORDER BY al.override_proposed_at DESC NULLS LAST
				LIMIT 100`, entityFilter, fdFilter, startDate)
			if err != nil {
				api.LogError("[AuditDash] overrides query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "missing_evidence_count": 0}, nil
			}
			defer rows.Close()

			type overrideRow struct {
				LedgerID        string  `json:"ledger_id"`
				FDID            string  `json:"fd_id"`
				EntityID        string  `json:"entity_id"`
				BankID          string  `json:"bank_id"`
				AccrualPeriod   string  `json:"accrual_period"`
				OverrideAmount  float64 `json:"override_amount"`
				OriginalAmount  float64 `json:"original_amount"`
				Delta           float64 `json:"delta"`
				ReasonCode      string  `json:"reason_code"`
				ReasonText      string  `json:"reason_text"`
				OverrideStatus  string  `json:"override_status"`
				ProposedBy      string  `json:"proposed_by"`
				ApprovedBy      string  `json:"approved_by"`
				HasEvidence     bool    `json:"has_evidence"`
				ProposedAt      string  `json:"proposed_at"`
				LedgerRowStatus string  `json:"ledger_row_status"`
			}
			out := []overrideRow{}
			missingEvidence := 0
			for rows.Next() {
				var or_ overrideRow
				if err2 := rows.Scan(
					&or_.LedgerID, &or_.FDID, &or_.EntityID, &or_.BankID,
					&or_.AccrualPeriod, &or_.OverrideAmount, &or_.OriginalAmount,
					&or_.ReasonCode, &or_.ReasonText, &or_.OverrideStatus,
					&or_.ProposedBy, &or_.ApprovedBy, &or_.HasEvidence,
					&or_.ProposedAt, &or_.LedgerRowStatus,
				); err2 != nil {
					api.LogError("[AuditDash] overrides scan error: %v", err2)
					continue
				}
				or_.OverrideAmount = fdRound(or_.OverrideAmount, 2)
				or_.OriginalAmount = fdRound(or_.OriginalAmount, 2)
				or_.Delta = fdRound(or_.OverrideAmount-or_.OriginalAmount, 2)
				if !or_.HasEvidence {
					missingEvidence++
				}
				out = append(out, or_)
			}
			return map[string]interface{}{
				"rows":                   out,
				"count":                  len(out),
				"missing_evidence_count": missingEvidence,
			}, nil
		})

		// ── 5. Missing evidence count ─────────────────────────────────────────
		run("missing_evidence", func(ctx context.Context) (interface{}, error) {
			var count int64
			err := pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM investment.fd_accrual_ledger
				WHERE is_overridden=true
				  AND override_attachment IS NULL
				  AND is_deleted=false
				  AND ($1::text='' OR entity_id=$1)
				  AND ($2::text='' OR fd_id=$2)`, entityFilter, fdFilter).Scan(&count)
			if err != nil {
				api.LogError("[AuditDash] missing_evidence query error: %v", err)
				return map[string]interface{}{"count": 0}, nil
			}
			return map[string]interface{}{"count": count}, nil
		})

		// ── 6. Period reopens ─────────────────────────────────────────────────
		run("period_reopens", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  lock_id,
				  entity_id,
				  COALESCE(financial_period,'') AS financial_period,
				  COALESCE(TO_CHAR(locked_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS locked_at,
				  COALESCE(locked_by,'') AS locked_by,
				  COALESCE(TO_CHAR(unlocked_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS unlocked_at,
				  COALESCE(unlocked_by,'') AS unlocked_by,
				  COALESCE(unlock_reason,'') AS unlock_reason,
				  is_locked
				FROM investment.fd_accrual_period_lock
				WHERE unlocked_at IS NOT NULL
				  AND ($1::text='' OR entity_id=$1)
				  AND unlocked_at >= $2::date
				ORDER BY unlocked_at DESC
				LIMIT 50`, entityFilter, startDate)
			if err != nil {
				api.LogError("[AuditDash] period_reopens query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type periodRow struct {
				LockID         string `json:"lock_id"`
				EntityID       string `json:"entity_id"`
				FinancialPeriod string `json:"financial_period"`
				LockedAt       string `json:"locked_at"`
				LockedBy       string `json:"locked_by"`
				UnlockedAt     string `json:"unlocked_at"`
				UnlockedBy     string `json:"unlocked_by"`
				UnlockReason   string `json:"unlock_reason"`
				IsLocked       bool   `json:"is_locked"`
			}
			out := []periodRow{}
			for rows.Next() {
				var pr periodRow
				if err2 := rows.Scan(
					&pr.LockID, &pr.EntityID, &pr.FinancialPeriod,
					&pr.LockedAt, &pr.LockedBy,
					&pr.UnlockedAt, &pr.UnlockedBy, &pr.UnlockReason, &pr.IsLocked,
				); err2 != nil {
					api.LogError("[AuditDash] period_reopens scan error: %v", err2)
					continue
				}
				out = append(out, pr)
			}
			return out, nil
		})

		// ── 7. Approvals register ─────────────────────────────────────────────
		run("approvals_register", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT audit_id_text, ref_id, approval_type, submitted_by, approved_by,
				       decision, requested_at, checker_at, checker_comment
				FROM (
				  SELECT
				    audit_id::text AS audit_id_text,
				    booking_id AS ref_id,
				    action_type AS approval_type,
				    COALESCE(requested_by,'') AS submitted_by,
				    COALESCE(checker_by,'') AS approved_by,
				    processing_status AS decision,
				    requested_at,
				    checker_at,
				    COALESCE(checker_comment,'') AS checker_comment
				  FROM investment.fd_audit_booking_request
				  WHERE checker_by IS NOT NULL
				    AND requested_at >= $1::date AND requested_at < $2::date
				    AND ($3::text='' OR EXISTS(SELECT 1 FROM investment.fd_booking_request b WHERE b.booking_id=fd_audit_booking_request.booking_id AND b.entity_id=$3))

				  UNION ALL

				  SELECT
				    audit_id::text,
				    fd_id,
				    action_type,
				    COALESCE(requested_by,''),
				    COALESCE(checker_by,''),
				    processing_status,
				    requested_at,
				    checker_at,
				    COALESCE(checker_comment,'')
				  FROM investment.fd_audit_master
				  WHERE checker_by IS NOT NULL
				    AND requested_at >= $1::date AND requested_at < $2::date
				    AND ($3::text='' OR EXISTS(SELECT 1 FROM investment.fd_master fm WHERE fm.fd_id=fd_audit_master.fd_id AND fm.entity_id=$3))

				  UNION ALL

				  SELECT
				    audit_id::text,
				    confirmation_id,
				    action_type,
				    COALESCE(requested_by,''),
				    COALESCE(checker_by,''),
				    processing_status,
				    requested_at,
				    checker_at,
				    COALESCE(checker_comment,'')
				  FROM investment.fd_audit_confirmation
				  WHERE checker_by IS NOT NULL
				    AND requested_at >= $1::date AND requested_at < $2::date
				) combined
				ORDER BY requested_at DESC
				LIMIT 100`, startDate, endDate, entityFilter)
			if err != nil {
				api.LogError("[AuditDash] approvals_register query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type approvalRow struct {
				AuditID        string `json:"audit_id"`
				RefID          string `json:"ref_id"`
				ApprovalType   string `json:"approval_type"`
				SubmittedBy    string `json:"submitted_by"`
				ApprovedBy     string `json:"approved_by"`
				Decision       string `json:"decision"`
				RequestedAt    string `json:"requested_at"`
				CheckerAt      string `json:"checker_at"`
				CheckerComment string `json:"checker_comment"`
			}
			out := []approvalRow{}
			for rows.Next() {
				var ar approvalRow
				var reqAt, chkAt *time.Time
				if err2 := rows.Scan(
					&ar.AuditID, &ar.RefID, &ar.ApprovalType,
					&ar.SubmittedBy, &ar.ApprovedBy, &ar.Decision,
					&reqAt, &chkAt, &ar.CheckerComment,
				); err2 != nil {
					api.LogError("[AuditDash] approvals_register scan error: %v", err2)
					continue
				}
				if reqAt != nil {
					ar.RequestedAt = reqAt.Format(time.RFC3339)
				}
				if chkAt != nil {
					ar.CheckerAt = chkAt.Format(time.RFC3339)
				}
				out = append(out, ar)
			}
			return out, nil
		})

		// ── 8. Evidence packs (period lock archive) ───────────────────────────
		run("evidence_packs", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  lock_id,
				  entity_id,
				  COALESCE(financial_period,'') AS financial_period,
				  COALESCE(TO_CHAR(locked_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS locked_at,
				  COALESCE(locked_by,'') AS locked_by,
				  is_locked,
				  (unlocked_at IS NOT NULL) AS has_been_reopened
				FROM investment.fd_accrual_period_lock
				WHERE ($1::text='' OR entity_id=$1)
				ORDER BY locked_at DESC
				LIMIT 100`, entityFilter)
			if err != nil {
				api.LogError("[AuditDash] evidence_packs query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type evidRow struct {
				LockID          string `json:"lock_id"`
				EntityID        string `json:"entity_id"`
				FinancialPeriod string `json:"financial_period"`
				LockedAt        string `json:"locked_at"`
				LockedBy        string `json:"locked_by"`
				IsLocked        bool   `json:"is_locked"`
				HasBeenReopened bool   `json:"has_been_reopened"`
			}
			out := []evidRow{}
			for rows.Next() {
				var er evidRow
				if err2 := rows.Scan(
					&er.LockID, &er.EntityID, &er.FinancialPeriod,
					&er.LockedAt, &er.LockedBy, &er.IsLocked, &er.HasBeenReopened,
				); err2 != nil {
					api.LogError("[AuditDash] evidence_packs scan error: %v", err2)
					continue
				}
				out = append(out, er)
			}
			return out, nil
		})

		// ── 9. Policy exceptions (variance + unresolved accrual) ──────────────
		run("policy_exceptions", func(ctx context.Context) (interface{}, error) {
			var openVariance, openAccrualExc int64
			pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM investment.fd_confirmation
				WHERE is_deleted=false
				  AND variance_flag=true
				  AND COALESCE(variance_action,'PENDING') = 'PENDING'
				  AND ($1::text='' OR EXISTS(
				    SELECT 1 FROM investment.fd_booking_request b
				    WHERE b.booking_id=fd_confirmation.booking_id AND b.entity_id=$1
				  ))`, entityFilter).Scan(&openVariance)

			pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM investment.fd_accrual_exception ae
				WHERE COALESCE(ae.is_deleted,false)=false
				  AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  AND ($1::text='' OR EXISTS (
				    SELECT 1 FROM investment.fd_master m WHERE m.fd_id=ae.fd_id AND m.entity_id=$1
				  ))`, entityFilter).Scan(&openAccrualExc)

			return map[string]interface{}{
				"open_variance_confirmations": openVariance,
				"open_accrual_exceptions":     openAccrualExc,
				"total":                        openVariance + openAccrualExc,
			}, nil
		})

		// ── 10. Transaction trace for a specific FD (optional) ────────────────
		if fdFilter != "" {
			run("transaction_trace", func(ctx context.Context) (interface{}, error) {
				// A. Master FD state
				type fdState struct {
					FDID          string  `json:"fd_id"`
					Bank          string  `json:"bank"`
					Entity        string  `json:"entity"`
					Principal     float64 `json:"principal"`
					InterestRate  float64 `json:"interest_rate"`
					Status        string  `json:"status"`
					MaturityDate  string  `json:"maturity_date"`
				}
				var fs fdState
				pool.QueryRow(ctx, `
					SELECT fd_id, COALESCE(bank_name,''), COALESCE(entity_name,''),
					  COALESCE(principal_amount,0), COALESCE(interest_rate,0),
					  COALESCE(fd_status,''), COALESCE(TO_CHAR(maturity_date,'YYYY-MM-DD'),'')
					FROM investment.fd_master WHERE fd_id=$1 AND is_deleted=false`, fdFilter).
					Scan(&fs.FDID, &fs.Bank, &fs.Entity, &fs.Principal,
						&fs.InterestRate, &fs.Status, &fs.MaturityDate)
				fs.Principal = fdRound(fs.Principal, 2)
				fs.InterestRate = fdRound(fs.InterestRate, 4)

				// B. Booking history
				bookingRows, _ := pool.Query(ctx, `
					SELECT booking_id, booking_status,
					  COALESCE(TO_CHAR(created_at,'YYYY-MM-DD'),''),
					  COALESCE(created_by,'')
					FROM investment.fd_booking_request
					WHERE (fd_id=$1 OR booking_id=(SELECT booking_id FROM investment.fd_master WHERE fd_id=$1 LIMIT 1))
					AND is_deleted=false ORDER BY created_at ASC`, fdFilter)
				type bRow struct {
					ID     string `json:"booking_id"`
					Status string `json:"status"`
					Date   string `json:"date"`
					By     string `json:"created_by"`
				}
				bookings := []bRow{}
				if bookingRows != nil {
					defer bookingRows.Close()
					for bookingRows.Next() {
						var br bRow
						bookingRows.Scan(&br.ID, &br.Status, &br.Date, &br.By)
						bookings = append(bookings, br)
					}
				}

				// C. Cashflow schedule
				cfRows, _ := pool.Query(ctx, `
					SELECT cashflow_id, event_type,
					  COALESCE(TO_CHAR(event_date,'YYYY-MM-DD'),''),
					  COALESCE(interest_accrued,0), COALESCE(net_cash_flow,0),
					  COALESCE(tds_amount,0), posting_status
					FROM investment.fd_cashflow_schedule
					WHERE fd_id=$1 AND is_deleted=false
					ORDER BY event_date ASC`, fdFilter)
				type cfRow struct {
					ID            string  `json:"cashflow_id"`
					EventType     string  `json:"event_type"`
					EventDate     string  `json:"event_date"`
					InterestAcc   float64 `json:"interest_accrued"`
					NetCashFlow   float64 `json:"net_cash_flow"`
					TDSAmount     float64 `json:"tds_amount"`
					PostingStatus string  `json:"posting_status"`
				}
				cashflows := []cfRow{}
				if cfRows != nil {
					defer cfRows.Close()
					for cfRows.Next() {
						var cr cfRow
						cfRows.Scan(&cr.ID, &cr.EventType, &cr.EventDate,
							&cr.InterestAcc, &cr.NetCashFlow, &cr.TDSAmount, &cr.PostingStatus)
						cr.InterestAcc = fdRound(cr.InterestAcc, 2)
						cr.NetCashFlow = fdRound(cr.NetCashFlow, 2)
						cr.TDSAmount = fdRound(cr.TDSAmount, 2)
						cashflows = append(cashflows, cr)
					}
				}

				// D. Audit history for this FD
				auditRows, _ := pool.Query(ctx, `
					SELECT source, audit_id_text, action_type, processing_status, requested_by, requested_at_str, checker_by
					FROM (
					  SELECT 'MASTER' AS source, audit_id::text AS audit_id_text, action_type, processing_status,
					    COALESCE(requested_by,'') AS requested_by,
					    COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI'),'') AS requested_at_str,
					    COALESCE(checker_by,'') AS checker_by, requested_at AS sort_ts
					  FROM investment.fd_audit_master WHERE fd_id=$1
					  UNION ALL
					  SELECT 'CASHFLOW', audit_id::text, action_type, processing_status,
					    COALESCE(requested_by,''), COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI'),''),
					    COALESCE(checker_by,''), requested_at
					  FROM investment.fd_audit_cashflow_schedule WHERE fd_id=$1
					) combined ORDER BY sort_ts ASC`, fdFilter)
				type auditTraceRow struct {
					Source      string `json:"source"`
					AuditID     string `json:"audit_id"`
					ActionType  string `json:"action_type"`
					Status      string `json:"processing_status"`
					RequestedBy string `json:"requested_by"`
					RequestedAt string `json:"requested_at"`
					CheckerBy   string `json:"checker_by"`
				}
				auditTrail := []auditTraceRow{}
				if auditRows != nil {
					defer auditRows.Close()
					for auditRows.Next() {
						var ar auditTraceRow
						auditRows.Scan(&ar.Source, &ar.AuditID, &ar.ActionType,
							&ar.Status, &ar.RequestedBy, &ar.RequestedAt, &ar.CheckerBy)
						auditTrail = append(auditTrail, ar)
					}
				}

				// E. Overrides on this FD
				overrideRows, _ := pool.Query(ctx, `
					SELECT ledger_id, COALESCE(TO_CHAR(accrual_period_start,'YYYY-MM-DD'),''),
					  COALESCE(override_amount,0), COALESCE(period_interest_accrued,0),
					  COALESCE(override_reason_code,''), COALESCE(override_status,''),
					  COALESCE(override_proposed_by,''), (override_attachment IS NOT NULL)
					FROM investment.fd_accrual_ledger
					WHERE fd_id=$1 AND is_overridden=true AND is_deleted=false
					ORDER BY accrual_period_start ASC`, fdFilter)
				type overrideTraceRow struct {
					LedgerID      string  `json:"ledger_id"`
					Period        string  `json:"period"`
					OverrideAmt   float64 `json:"override_amount"`
					OriginalAmt   float64 `json:"original_amount"`
					ReasonCode    string  `json:"reason_code"`
					Status        string  `json:"status"`
					ProposedBy    string  `json:"proposed_by"`
					HasEvidence   bool    `json:"has_evidence"`
				}
				overrides := []overrideTraceRow{}
				if overrideRows != nil {
					defer overrideRows.Close()
					for overrideRows.Next() {
						var or_ overrideTraceRow
						overrideRows.Scan(&or_.LedgerID, &or_.Period, &or_.OverrideAmt, &or_.OriginalAmt,
							&or_.ReasonCode, &or_.Status, &or_.ProposedBy, &or_.HasEvidence)
						or_.OverrideAmt = fdRound(or_.OverrideAmt, 2)
						or_.OriginalAmt = fdRound(or_.OriginalAmt, 2)
						overrides = append(overrides, or_)
					}
				}

				return map[string]interface{}{
					"fd":        fs,
					"bookings":  bookings,
					"cashflows": cashflows,
					"audit_trail": auditTrail,
					"overrides": overrides,
				}, nil
			})
		}

		wg.Wait()

		get := func(key string) interface{} {
			if res, ok := results[key]; ok && res.err == nil {
				return res.data
			}
			return nil
		}

		// aggregate KPIs
		var totalPending, totalRecords int64
		var makerCheckerRate float64
		if v := get("audit_summary"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["total_records"].(int64); ok2 {
					totalRecords = c
				}
				if c, ok2 := m["total_pending"].(int64); ok2 {
					totalPending = c
				}
				if c, ok2 := m["maker_checker_rate"].(float64); ok2 {
					makerCheckerRate = c
				}
			}
		}
		var missingEvCount int64
		if v := get("missing_evidence"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int64); ok2 {
					missingEvCount = c
				}
			}
		}

		payload := map[string]interface{}{
			"generated_at": now.Format(time.RFC3339),
			"filters": map[string]interface{}{
				"entity_id":   entityFilter,
				"start_date":  startDate,
				"end_date":    endDate,
				"action_type": actionFilter,
				"fd_id":       fdFilter,
			},
			"kpis": map[string]interface{}{
				"total_audit_records":      totalRecords,
				"total_pending_approvals":  totalPending,
				"maker_checker_rate_pct":   makerCheckerRate,
				"missing_evidence_count":   missingEvCount,
				"period_reopens":           countSlice(get("period_reopens")),
				"open_policy_exceptions":   getNestedInt64(get("policy_exceptions"), "total"),
			},
			"audit_summary":       get("audit_summary"),
			"maker_checker_rate":  get("maker_checker_rate"),
			"audit_log":           get("audit_log"),
			"overrides":           get("overrides"),
			"missing_evidence":    get("missing_evidence"),
			"period_reopens":      get("period_reopens"),
			"approvals_register":  get("approvals_register"),
			"evidence_packs":      get("evidence_packs"),
			"policy_exceptions":   get("policy_exceptions"),
			"transaction_trace":   get("transaction_trace"),
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}
