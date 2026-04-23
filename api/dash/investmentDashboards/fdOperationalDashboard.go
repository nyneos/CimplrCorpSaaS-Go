// Package investmentdashboards — FD Operational Team Dashboard
//
// POST /dash/investment/fd/operational-dashboard
//
// Returns all data required by the operational team view:
//   - Booking requests pending send (SLA tracking)
//   - Pending bank confirmations
//   - Unmatched interest receipts (statement reconciliation)
//   - TDS allocations pending
//   - Variance & exception cases
//   - Latest accrual run status
//   - Posting queue (GL posting batches)
//   - SLA distribution chart (work backlog by aging band)
//   - Operational summary KPIs
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

// ─── request type ────────────────────────────────────────────────────────────

type fdOperationalDashRequest struct {
	UserID    string `json:"user_id"`
	EntityID  string `json:"entity_id"`
	Currency  string `json:"currency"`
	Period    string `json:"period"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
}

// ─── handler ─────────────────────────────────────────────────────────────────

// GetFDOperationalDashboard returns the full Operational Team FD dashboard payload.
func GetFDOperationalDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req fdOperationalDashRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		if req.Currency == "" {
			req.Currency = "INR"
		}
		if req.Period == "" {
			req.Period = "MTD"
		}

		now := time.Now().UTC()
		ctx := r.Context()
		entityFilter := req.EntityID

		// ── period resolution (supports CUSTOM start/end dates) ──────────────────
		var opPeriodStart time.Time
		if req.Period == "CUSTOM" && req.StartDate != "" {
			if parsed, pErr := time.Parse(constants.DateFormat, req.StartDate); pErr == nil {
				opPeriodStart = parsed
			} else {
				opPeriodStart = periodStartDate("MTD", now)
			}
		} else {
			opPeriodStart = periodStartDate(req.Period, now)
		}
		opPeriodEnd := now
		if req.Period == "CUSTOM" && req.EndDate != "" {
			if parsed, pErr := time.Parse(constants.DateFormat, req.EndDate); pErr == nil {
				opPeriodEnd = parsed
			}
		}
		startDateStr := opPeriodStart.Format(constants.DateFormat)
		endDateStr := opPeriodEnd.Format(constants.DateFormat)

		type subResult struct {
			data interface{}
			err  error
		}
		results := make(map[string]subResult, 10)
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

		// ── 1. booking requests pending send ────────────────────────────────────────────────────────────────────
		run("booking_requests", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  b.booking_id,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0) AS principal,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS requested_at,
				  COALESCE(EXTRACT(DAY FROM NOW()-b.created_at)::int, 0) AS aging_days,
				  b.booking_status
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('DRAFT','APPROVAL_PENDING')
				  AND ($1::text='' OR b.entity_id=$1)
				  AND b.created_at >= $2::date AND b.created_at <= ($3::date + INTERVAL '1 day')
				ORDER BY b.created_at ASC
				LIMIT 100`, entityFilter, startDateStr, endDateStr)
			if err != nil {
				api.LogError("[OperationalDash] booking_requests query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "oldest_days": 0}, nil
			}
			defer rows.Close()

			type bkRow struct {
				BookingID   string  `json:"id"`
				Entity      string  `json:"entity"`
				Bank        string  `json:"bank"`
				Principal   float64 `json:"principal"`
				RequestedAt string  `json:"requested_at"`
				AgingDays   int     `json:"aging_days"`
				Status      string  `json:"status"`
				SLAStatus   string  `json:"sla_status"`
			}
			out := []bkRow{}
			maxAging := 0
			for rows.Next() {
				var br bkRow
				if err2 := rows.Scan(&br.BookingID, &br.Entity, &br.Bank, &br.Principal,
					&br.RequestedAt, &br.AgingDays, &br.Status); err2 != nil {
					api.LogError("[OperationalDash] booking_requests scan error: %v", err2)
					continue
				}
				br.Principal = fdRound(br.Principal, 2)
				switch {
				case br.AgingDays >= 3:
					br.SLAStatus = "Breached"
				case br.AgingDays >= 2:
					br.SLAStatus = "Warning"
				default:
					br.SLAStatus = "OK"
				}
				if br.AgingDays > maxAging {
					maxAging = br.AgingDays
				}
				out = append(out, br)
			}
			return map[string]interface{}{
				"rows":        out,
				"count":       len(out),
				"oldest_days": maxAging,
			}, nil
		})

		// ── 2. pending bank confirmations ───────────────────────────────────────────────────────────────────────────
		run("pending_confirmations", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  b.booking_id,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(m.bank_name, m.bank_id, b.bank_name, b.bank_id,'') AS bank,
				  COALESCE(b.principal_amount,0) AS principal,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD'),'') AS booking_date,
				  b.booking_status,
				  COALESCE(EXTRACT(DAY FROM NOW()-b.created_at)::int, 0) AS aging_days
				FROM investment.fd_booking_request b
				LEFT JOIN investment.fd_master m ON m.booking_id = b.booking_id AND m.is_deleted=false
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('SENT_TO_BANK','APPROVED')
				  AND ($1::text='' OR b.entity_id=$1)
				  AND b.created_at >= $2::date AND b.created_at <= ($3::date + INTERVAL '1 day')
				ORDER BY b.created_at ASC
				LIMIT 100`, entityFilter, startDateStr, endDateStr)
			if err != nil {
				api.LogError("[OperationalDash] pending_confirmations query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "overdue": 0}, nil
			}
			defer rows.Close()

			type confRow struct {
				BookingID   string  `json:"fd_ref"`
				Entity      string  `json:"entity"`
				Bank        string  `json:"bank"`
				Principal   float64 `json:"principal"`
				BookingDate string  `json:"booking_date"`
				Status      string  `json:"confirmation_status"`
				AgingDays   int     `json:"aging_days"`
				SLAStatus   string  `json:"sla_status"`
			}
			out := []confRow{}
			overdue := 0
			for rows.Next() {
				var cr confRow
				if err2 := rows.Scan(&cr.BookingID, &cr.Entity, &cr.Bank, &cr.Principal,
					&cr.BookingDate, &cr.Status, &cr.AgingDays); err2 != nil {
					api.LogError("[OperationalDash] pending_confirmations scan error: %v", err2)
					continue
				}
				cr.Principal = fdRound(cr.Principal, 2)
				switch {
				case cr.AgingDays >= 3:
					cr.SLAStatus = "Breached"
					overdue++
				case cr.AgingDays >= 2:
					cr.SLAStatus = "Warning"
				default:
					cr.SLAStatus = "OK"
				}
				out = append(out, cr)
			}
			return map[string]interface{}{
				"rows":    out,
				"count":   len(out),
				"overdue": overdue,
			}, nil
		})

		// ── 3. unmatched interest receipts ─────────────────────────────────────
		run("unmatched_receipts", func(ctx context.Context) (interface{}, error) {
			// fd_interest_receipt where fd_id is NULL or matching fd_master row is missing
			rows, err := pool.Query(ctx, `
				SELECT
				  ir.receipt_id,
				  COALESCE(ir.bank_name, ir.bank_id,'') AS bank,
				  COALESCE(ir.gross_interest_received,0) AS credit_amount,
				  COALESCE(ir.currency,'INR') AS currency,
				  COALESCE(TO_CHAR(ir.receipt_date,'YYYY-MM-DD'),'') AS transaction_date,
				  COALESCE(ir.narration,'') AS description,
				  COALESCE(ir.fd_id,'') AS suspected_fd_ref
				FROM investment.fd_interest_receipt ir
				WHERE ir.is_deleted=false
				  AND (ir.fd_id IS NULL OR ir.reconcile_status IN ('UNMATCHED','PENDING',''))
				  AND ($1::text='' OR ir.entity_id=$1)
				ORDER BY ir.receipt_date DESC
				LIMIT 100`, entityFilter)
			if err != nil {
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "total_amount": 0}, nil
			}
			defer rows.Close()

			type stmtRow struct {
				ReceiptID       string  `json:"statement_ref"`
				Bank            string  `json:"bank"`
				CreditAmount    float64 `json:"credit_amount"`
				Currency        string  `json:"currency"`
				TransactionDate string  `json:"transaction_date"`
				Description     string  `json:"description"`
				SuspectedFDRef  string  `json:"suspected_fd_ref"`
			}
			out := []stmtRow{}
			totalAmt := 0.0
			for rows.Next() {
				var sr stmtRow
				if err2 := rows.Scan(&sr.ReceiptID, &sr.Bank, &sr.CreditAmount,
					&sr.Currency, &sr.TransactionDate, &sr.Description,
					&sr.SuspectedFDRef); err2 == nil {
					sr.CreditAmount = fdRound(sr.CreditAmount, 2)
					totalAmt += sr.CreditAmount
					out = append(out, sr)
				}
			}
			return map[string]interface{}{
				"rows":         out,
				"count":        len(out),
				"total_amount": fdRound(totalAmt, 2),
			}, nil
		})

		// ── 4. TDS allocations pending ────────────────────────────────────────
		run("tds_pending", func(ctx context.Context) (interface{}, error) {
			var cnt int64
			var totalAmt float64
			err := pool.QueryRow(ctx, `
				SELECT COUNT(*), COALESCE(SUM(ir.tds_deducted),0)
				FROM investment.fd_interest_receipt ir
				WHERE ir.is_deleted=false
				  AND COALESCE(ir.tds_deducted,0) > 0
				  AND (ir.tds_allocation_status IS NULL OR ir.tds_allocation_status IN ('PENDING','UNALLOCATED'))
				  AND ($1::text='' OR ir.entity_id=$1)`, entityFilter).Scan(&cnt, &totalAmt)
			if err != nil {
				return map[string]interface{}{"count": 0, "total_amount": 0}, nil
			}
			return map[string]interface{}{
				"count":        cnt,
				"total_amount": fdRound(totalAmt, 2),
			}, nil
		})

		// ── 5. variance & exception cases ─────────────────────────────────────
		run("exceptions", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  ae.exception_id,
				  ae.fd_id AS fd_ref,
				  COALESCE(m.bank_name, m.bank_id,'') AS bank,
				  COALESCE(b.entity_name,'') AS entity,
				  COALESCE(ae.variance_amount,0) AS variance_amount,
				  COALESCE(ae.variance_pct,0) AS variance_pct,
				  COALESCE(ae.exception_type,'') AS exception_type,
				  COALESCE(ae.exception_status,'Open') AS status,
				  COALESCE(ae.reason_required,false) AS reason_required
				FROM investment.fd_accrual_exception ae
				LEFT JOIN investment.fd_master m ON m.fd_id = ae.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE COALESCE(ae.is_deleted,false)=false
				  AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				ORDER BY ae.created_at DESC
				LIMIT 200`, entityFilter)
			if err != nil {
				api.LogError("[OperationalDash] exceptions query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0, "impact": 0}, nil
			}
			defer rows.Close()

			type excRow struct {
				ExceptionID    string  `json:"exception_id"`
				FDRef          string  `json:"fd_ref"`
				Bank           string  `json:"bank"`
				Entity         string  `json:"entity"`
				VarianceAmount float64 `json:"variance_amount"`
				VariancePct    float64 `json:"variance_pct"`
				ExceptionType  string  `json:"exception_type"`
				Status         string  `json:"status"`
				ReasonRequired bool    `json:"reason_required"`
			}
			out := []excRow{}
			totalImpact := 0.0
			for rows.Next() {
				var er excRow
				if err2 := rows.Scan(&er.ExceptionID, &er.FDRef, &er.Bank, &er.Entity,
					&er.VarianceAmount, &er.VariancePct, &er.ExceptionType,
					&er.Status, &er.ReasonRequired); err2 == nil {
					er.VarianceAmount = fdRound(er.VarianceAmount, 2)
					er.VariancePct = fdRound(er.VariancePct, 2)
					totalImpact += er.VarianceAmount
					out = append(out, er)
				}
			}
			return map[string]interface{}{
				"rows":   out,
				"count":  len(out),
				"impact": fdRound(totalImpact, 2),
			}, nil
		})

		// ── 6. latest accrual run ─────────────────────────────────────────────
		run("accrual_run", func(ctx context.Context) (interface{}, error) {
			var runID, runStatus string
			var runTime string
			var processed, errors int64
			err := pool.QueryRow(ctx, `
				SELECT
				  COALESCE(run_id,'') AS run_id,
				  COALESCE(TO_CHAR(run_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS run_time,
				  COALESCE(run_status,'') AS run_status,
				  COALESCE(records_processed,0) AS records_processed,
				  COALESCE(errors_detected,0) AS errors_detected
				FROM investment.fd_accrual_run
				ORDER BY run_at DESC
				LIMIT 1`).Scan(&runID, &runTime, &runStatus, &processed, &errors)
			if err != nil {
				// No table or no rows — return placeholder
				return map[string]interface{}{
					"run_id":            "",
					"run_time":          "",
					"status":            "Unknown",
					"records_processed": 0,
					"errors_detected":   0,
				}, nil
			}
			return map[string]interface{}{
				"run_id":            runID,
				"run_time":          runTime,
				"status":            runStatus,
				"records_processed": processed,
				"errors_detected":   errors,
			}, nil
		})

		// ── 7. posting queue ─────────────────────────────────────────────────
		run("posting_queue", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  COALESCE(batch_id,'') AS batch_id,
				  COALESCE(records_count,0) AS records,
				  COALESCE(posting_status,'') AS status,
				  COALESCE(TO_CHAR(posted_at,'YYYY-MM-DD"T"HH24:MI:SS'),'') AS posting_time,
				  COALESCE(error_message,'') AS error_message
				FROM investment.fd_journal_posting_batch
				ORDER BY created_at DESC
				LIMIT 20`)
			if err != nil {
				return []interface{}{}, nil
			}
			defer rows.Close()

			type postRow struct {
				BatchID      string `json:"batch_id"`
				Records      int64  `json:"records"`
				Status       string `json:"status"`
				PostingTime  string `json:"posting_time"`
				ErrorMessage string `json:"error_message"`
			}
			out := []postRow{}
			for rows.Next() {
				var pr postRow
				if err2 := rows.Scan(&pr.BatchID, &pr.Records, &pr.Status,
					&pr.PostingTime, &pr.ErrorMessage); err2 == nil {
					out = append(out, pr)
				}
			}
			return out, nil
		})

		// ── 8. SLA distribution by aging band ────────────────────────────────
		run("sla_distribution", func(ctx context.Context) (interface{}, error) {
			// confirmations aging band
			confSQL := `
				SELECT
				  CASE
				    WHEN EXTRACT(DAY FROM NOW()-b.created_at) < 2  THEN '0-1 Days'
				    WHEN EXTRACT(DAY FROM NOW()-b.created_at) < 4  THEN '2-3 Days'
				    ELSE '>3 Days'
				  END AS band,
				  COUNT(*) AS cnt
				FROM investment.fd_booking_request b
				WHERE b.is_deleted=false
				  AND b.booking_status IN ('SENT_TO_BANK','APPROVED','APPROVAL_PENDING')
				  AND ($1::text='' OR b.entity_id=$1)
				GROUP BY 1`

			excSQL := `
				SELECT
				  CASE
				    WHEN EXTRACT(DAY FROM NOW()-ae.created_at) < 2  THEN '0-1 Days'
				    WHEN EXTRACT(DAY FROM NOW()-ae.created_at) < 4  THEN '2-3 Days'
				    ELSE '>3 Days'
				  END AS band,
				  COUNT(*) AS cnt
				FROM investment.fd_accrual_exception ae
				LEFT JOIN investment.fd_master m ON m.fd_id = ae.fd_id
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE COALESCE(ae.is_deleted,false)=false
				  AND ae.exception_status NOT IN ('RESOLVED','CLOSED')
				  AND ($1::text='' OR COALESCE(m.entity_id,b.entity_id)=$1)
				GROUP BY 1`

			type bandMap = map[string]int64
			confBands := bandMap{constants.DateRange0To1Days: 0, constants.DateRange2To3Days: 0, constants.DateRangeMoreThan3Days: 0}
			excBands := bandMap{constants.DateRange0To1Days: 0, constants.DateRange2To3Days: 0, constants.DateRangeMoreThan3Days: 0}

			if confRows, err2 := pool.Query(ctx, confSQL, entityFilter); err2 == nil {
				defer confRows.Close()
				for confRows.Next() {
					var band string
					var cnt int64
					if confRows.Scan(&band, &cnt) == nil {
						confBands[band] += cnt
					}
				}
			}
			if excRows, err2 := pool.Query(ctx, excSQL, entityFilter); err2 == nil {
				defer excRows.Close()
				for excRows.Next() {
					var band string
					var cnt int64
					if excRows.Scan(&band, &cnt) == nil {
						excBands[band] += cnt
					}
				}
			}

			type slaBandRow struct {
				SLA           string `json:"sla"`
				Confirmations int64  `json:"confirmations"`
				Exceptions    int64  `json:"exceptions"`
			}
			out := []slaBandRow{
				{SLA: constants.DateRange0To1Days, Confirmations: confBands[constants.DateRange0To1Days], Exceptions: excBands[constants.DateRange0To1Days]},
				{SLA: constants.DateRange2To3Days, Confirmations: confBands[constants.DateRange2To3Days], Exceptions: excBands[constants.DateRange2To3Days]},
				{SLA: constants.DateRangeMoreThan3Days, Confirmations: confBands[constants.DateRangeMoreThan3Days], Exceptions: excBands[constants.DateRangeMoreThan3Days]},
			}
			return out, nil
		})

		// ── 9. interest receipts (with audit/approval status) ──────────────────
		run("receipts", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  ir.receipt_id,
				  COALESCE(ir.fd_id,'')                       AS fd_id,
				  COALESCE(ir.entity_id,'')                   AS entity_id,
				  COALESCE(ir.entity_name,'')                 AS entity_name,
				  COALESCE(ir.bank_id,'')                     AS bank_id,
				  COALESCE(ir.bank_name,'')                   AS bank_name,
				  COALESCE(ir.fd_ref_no,'')                   AS fd_ref_no,
				  COALESCE(TO_CHAR(ir.receipt_date,'YYYY-MM-DD'),'') AS receipt_date,
				  COALESCE(TO_CHAR(ir.period_start,'YYYY-MM-DD'),'') AS period_start,
				  COALESCE(TO_CHAR(ir.period_end,'YYYY-MM-DD'),'')   AS period_end,
				  COALESCE(ir.gross_interest_received,0)      AS gross_interest,
				  COALESCE(ir.tds_amount_deducted,0)          AS tds_deducted,
				  COALESCE(ir.net_amount_received,0)          AS net_amount,
				  COALESCE(ir.receipt_status,'')              AS receipt_status,
				  COALESCE(ir.ingestion_mode,'')              AS ingestion_source,
				  COALESCE(la.processing_status,'')           AS processing_status,
				  COALESCE(la.action_type,'')                 AS action_type,
				  COALESCE(la.requested_by,'')                AS requested_by,
				  COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				  COALESCE(la.checker_by,'')                  AS checker_by,
				  COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
				  COALESCE(TO_CHAR(ir.created_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS created_at
				FROM investment.fd_interest_receipt ir
				LEFT JOIN LATERAL (
				  SELECT processing_status, action_type, requested_by, requested_at, checker_by, checker_at
				  FROM investment.fd_interest_receipt_audit
				  WHERE receipt_id = ir.receipt_id
				  ORDER BY created_at DESC LIMIT 1
				) la ON true
				WHERE ir.is_deleted = false
				  AND ($1::text='' OR ir.entity_id=$1)
				ORDER BY ir.created_at DESC
				LIMIT 200`, entityFilter)
			if err != nil {
				api.LogError("[OperationalDash] receipts query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0,
					"pending_count": 0, "approved_count": 0, "captured_count": 0}, nil
			}
			defer rows.Close()

			type receiptRow struct {
				ReceiptID        string  `json:"receipt_id"`
				FDID             string  `json:"fd_id"`
				EntityID         string  `json:"entity_id"`
				EntityName       string  `json:"entity_name"`
				BankID           string  `json:"bank_id"`
				BankName         string  `json:"bank_name"`
				FdRefNo          string  `json:"fd_ref_no"`
				ReceiptDate      string  `json:"receipt_date"`
				PeriodStart      string  `json:"period_start"`
				PeriodEnd        string  `json:"period_end"`
				GrossInterest    float64 `json:"gross_interest"`
				TDSDeducted      float64 `json:"tds_deducted"`
				NetAmount        float64 `json:"net_amount"`
				ReceiptStatus    string  `json:"receipt_status"`
				IngestionSource  string  `json:"ingestion_source"`
				ProcessingStatus string  `json:"processing_status"`
				ActionType       string  `json:"action_type"`
				RequestedBy      string  `json:"requested_by"`
				RequestedAt      string  `json:"requested_at"`
				CheckerBy        string  `json:"checker_by"`
				CheckerAt        string  `json:"checker_at"`
				CreatedAt        string  `json:"created_at"`
			}
			out := []receiptRow{}
			pendingCount, approvedCount, capturedCount := 0, 0, 0
			for rows.Next() {
				var rr receiptRow
				if err2 := rows.Scan(
					&rr.ReceiptID, &rr.FDID, &rr.EntityID, &rr.EntityName,
					&rr.BankID, &rr.BankName, &rr.FdRefNo,
					&rr.ReceiptDate, &rr.PeriodStart, &rr.PeriodEnd,
					&rr.GrossInterest, &rr.TDSDeducted, &rr.NetAmount,
					&rr.ReceiptStatus, &rr.IngestionSource,
					&rr.ProcessingStatus, &rr.ActionType,
					&rr.RequestedBy, &rr.RequestedAt, &rr.CheckerBy, &rr.CheckerAt,
					&rr.CreatedAt,
				); err2 != nil {
					api.LogError("[OperationalDash] receipts scan error: %v", err2)
					continue
				}
				rr.GrossInterest = fdRound(rr.GrossInterest, 2)
				rr.TDSDeducted = fdRound(rr.TDSDeducted, 2)
				rr.NetAmount = fdRound(rr.NetAmount, 2)
				switch rr.ProcessingStatus {
				case "PENDING_APPROVAL":
					pendingCount++
				case "APPROVED":
					approvedCount++
				default:
					if rr.ReceiptStatus == "CAPTURED" {
						capturedCount++
					}
				}
				out = append(out, rr)
			}
			return map[string]interface{}{
				"rows":           out,
				"count":          len(out),
				"pending_count":  pendingCount,
				"approved_count": approvedCount,
				"captured_count": capturedCount,
			}, nil
		})

		// ── 10. TDS receipts (with audit/approval status) ─────────────────────
		run("tds_receipts", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  t.tds_id,
				  COALESCE(t.receipt_id,'')                   AS receipt_id,
				  COALESCE(t.fd_id,'')                        AS fd_id,
				  COALESCE(t.entity_id,'')                    AS entity_id,
				  COALESCE(m.entity_name, t.entity_id, '')    AS entity_name,
				  COALESCE(t.bank_id,'')                      AS bank_id,
				  COALESCE(m.bank_name, t.bank_id, '')        AS bank_name,
				  COALESCE(t.fd_ref_no,'')                    AS fd_ref_no,
				  COALESCE(TO_CHAR(t.deduction_date,'YYYY-MM-DD'),'') AS deduction_date,
				  COALESCE(TO_CHAR(t.period_start,'YYYY-MM-DD'),'')   AS period_start,
				  COALESCE(TO_CHAR(t.period_end,'YYYY-MM-DD'),'')     AS period_end,
				  COALESCE(t.tds_deducted_actual,0)           AS tds_actual,
				  COALESCE(t.tds_expected,0)                  AS tds_expected,
				  COALESCE(t.tds_status,'')                   AS tds_status,
				  COALESCE(t.ingestion_source,'')             AS ingestion_source,
				  COALESCE(la.processing_status,'')           AS processing_status,
				  COALESCE(la.action_type,'')                 AS action_type,
				  COALESCE(la.requested_by,'')                AS requested_by,
				  COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				  COALESCE(la.checker_by,'')                  AS checker_by,
				  COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
				  COALESCE(TO_CHAR(t.created_at,'YYYY-MM-DD HH24:MI:SS'),'')    AS created_at
				FROM investment.fd_tds_receipt t
				LEFT JOIN investment.fd_master m ON m.fd_id = t.fd_id AND m.is_deleted = false
				LEFT JOIN LATERAL (
				  SELECT processing_status, action_type, requested_by, requested_at, checker_by, checker_at
				  FROM investment.fd_tds_receipt_audit
				  WHERE tds_id = t.tds_id
				  ORDER BY created_at DESC LIMIT 1
				) la ON true
				WHERE t.is_deleted = false
				  AND ($1::text='' OR t.entity_id=$1)
				ORDER BY t.created_at DESC
				LIMIT 200`, entityFilter)
			if err != nil {
				api.LogError("[OperationalDash] tds_receipts query error: %v", err)
				return map[string]interface{}{"rows": []interface{}{}, "count": 0,
					"pending_count": 0, "approved_count": 0}, nil
			}
			defer rows.Close()

			type tdsRow struct {
				TDSID            string  `json:"tds_id"`
				ReceiptID        string  `json:"receipt_id"`
				FDID             string  `json:"fd_id"`
				EntityID         string  `json:"entity_id"`
				EntityName       string  `json:"entity_name"`
				BankID           string  `json:"bank_id"`
				BankName         string  `json:"bank_name"`
				FdRefNo          string  `json:"fd_ref_no"`
				DeductionDate    string  `json:"deduction_date"`
				PeriodStart      string  `json:"period_start"`
				PeriodEnd        string  `json:"period_end"`
				TDSActual        float64 `json:"tds_actual"`
				TDSExpected      float64 `json:"tds_expected"`
				TDSStatus        string  `json:"tds_status"`
				IngestionSource  string  `json:"ingestion_source"`
				ProcessingStatus string  `json:"processing_status"`
				ActionType       string  `json:"action_type"`
				RequestedBy      string  `json:"requested_by"`
				RequestedAt      string  `json:"requested_at"`
				CheckerBy        string  `json:"checker_by"`
				CheckerAt        string  `json:"checker_at"`
				CreatedAt        string  `json:"created_at"`
			}
			out := []tdsRow{}
			pendingCount, approvedCount := 0, 0
			for rows.Next() {
				var tr tdsRow
				if err2 := rows.Scan(
					&tr.TDSID, &tr.ReceiptID, &tr.FDID, &tr.EntityID, &tr.EntityName,
					&tr.BankID, &tr.BankName, &tr.FdRefNo,
					&tr.DeductionDate, &tr.PeriodStart, &tr.PeriodEnd,
					&tr.TDSActual, &tr.TDSExpected,
					&tr.TDSStatus, &tr.IngestionSource,
					&tr.ProcessingStatus, &tr.ActionType,
					&tr.RequestedBy, &tr.RequestedAt, &tr.CheckerBy, &tr.CheckerAt,
					&tr.CreatedAt,
				); err2 != nil {
					api.LogError("[OperationalDash] tds_receipts scan error: %v", err2)
					continue
				}
				tr.TDSActual = fdRound(tr.TDSActual, 2)
				tr.TDSExpected = fdRound(tr.TDSExpected, 2)
				switch tr.ProcessingStatus {
				case "PENDING_APPROVAL":
					pendingCount++
				case "APPROVED":
					approvedCount++
				}
				out = append(out, tr)
			}
			return map[string]interface{}{
				"rows":           out,
				"count":          len(out),
				"pending_count":  pendingCount,
				"approved_count": approvedCount,
			}, nil
		})

		// ── 11. full FD list with all info ────────────────────────────────────
		run("fd_list", func(ctx context.Context) (interface{}, error) {
			rows, err := pool.Query(ctx, `
				SELECT
				  m.fd_id,
				  COALESCE(m.bank_name, m.bank_id,'') AS bank,
				  COALESCE(b.entity_name, m.entity_name, '') AS entity,
				  COALESCE(m.entity_id, b.entity_id, '') AS entity_id,
				  COALESCE(m.principal_amount,0) AS principal_amount,
				  COALESCE(m.interest_rate,0) AS interest_rate,
				  COALESCE(m.interest_type_code,'') AS interest_type,
				  COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				  COALESCE(m.fd_status,'') AS fd_status,
				  COALESCE(m.maturity_instructions,'') AS maturity_instructions,
				  COALESCE(b.booking_id,'') AS booking_id,
				  COALESCE(b.booking_status,'') AS booking_status,
				  COALESCE(TO_CHAR(b.created_at,'YYYY-MM-DD'),'') AS booking_date,
				  COALESCE(b.created_by, m.created_by,'') AS created_by,
				  COALESCE((m.maturity_date - CURRENT_DATE)::int, 0) AS days_to_maturity
				FROM investment.fd_master m
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = m.booking_id
				WHERE m.is_deleted=false
				  AND m.fd_status IN ('ACTIVE','MATURED','PENDING_ACTIVATION')
				  AND ($1::text='' OR COALESCE(m.entity_id, b.entity_id)=$1)
				ORDER BY m.maturity_date ASC NULLS LAST
				LIMIT 500`, entityFilter)
			if err != nil {
				api.LogError("[OperationalDash] fd_list query error: %v", err)
				return []interface{}{}, nil
			}
			defer rows.Close()

			type fdRow struct {
				FDID                 string  `json:"fd_id"`
				Bank                 string  `json:"bank"`
				Entity               string  `json:"entity"`
				EntityID             string  `json:"entity_id"`
				PrincipalAmount      float64 `json:"principal_amount"`
				InterestRate         float64 `json:"interest_rate"`
				InterestType         string  `json:"interest_type"`
				MaturityDate         string  `json:"maturity_date"`
				FDStatus             string  `json:"fd_status"`
				MaturityInstructions string  `json:"maturity_instructions"`
				BookingID            string  `json:"booking_id"`
				BookingStatus        string  `json:"booking_status"`
				BookingDate          string  `json:"booking_date"`
				CreatedBy            string  `json:"created_by"`
				DaysToMaturity       int     `json:"days_to_maturity"`
			}
			out := []fdRow{}
			for rows.Next() {
				var fr fdRow
				if err2 := rows.Scan(
					&fr.FDID, &fr.Bank, &fr.Entity, &fr.EntityID,
					&fr.PrincipalAmount, &fr.InterestRate, &fr.InterestType,
					&fr.MaturityDate, &fr.FDStatus, &fr.MaturityInstructions,
					&fr.BookingID, &fr.BookingStatus, &fr.BookingDate,
					&fr.CreatedBy, &fr.DaysToMaturity,
				); err2 != nil {
					api.LogError("[OperationalDash] fd_list scan error: %v", err2)
					continue
				}
				fr.PrincipalAmount = fdRound(fr.PrincipalAmount, 2)
				fr.InterestRate = fdRound(fr.InterestRate, 4)
				out = append(out, fr)
			}
			return out, nil
		})

		// wait
		wg.Wait()

		get := func(key string) interface{} {
			if r, ok := results[key]; ok && r.err == nil {
				return r.data
			}
			return nil
		}

		// Build operational summary KPI counts from results
		bookingCount := 0
		confirmCount, confirmOverdue := 0, 0
		unmatchedCount := 0
		tdsCount := 0
		excCount := 0
		failedPostings := 0
		receiptsPendingCount := 0
		tdsPendingApprovalCount := 0

		if v := get("booking_requests"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					bookingCount = c
				}
			}
		}
		if v := get("pending_confirmations"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					confirmCount = c
				}
				if c, ok2 := m["overdue"].(int); ok2 {
					confirmOverdue = c
				}
			}
		}
		if v := get("unmatched_receipts"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					unmatchedCount = c
				}
			}
		}
		if v := get("tds_pending"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int64); ok2 {
					tdsCount = int(c)
				}
			}
		}
		if v := get("exceptions"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["count"].(int); ok2 {
					excCount = c
				}
			}
		}
		if v := get("receipts"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["pending_count"].(int); ok2 {
					receiptsPendingCount = c
				}
			}
		}
		if v := get("tds_receipts"); v != nil {
			if m, ok := v.(map[string]interface{}); ok {
				if c, ok2 := m["pending_count"].(int); ok2 {
					tdsPendingApprovalCount = c
				}
			}
		}
		if v := get("posting_queue"); v != nil {
			b, _ := json.Marshal(v)
			var prRows []struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(b, &prRows) == nil {
				for _, pr := range prRows {
					if pr.Status == "Failed" || pr.Status == "FAILED" {
						failedPostings++
					}
				}
			}
		}

		payload := map[string]interface{}{
			"generated_at": now.Format(time.RFC3339),
			"filters": map[string]interface{}{
				"entity_id":  entityFilter,
				"currency":   req.Currency,
				"period":     req.Period,
				"start_date": startDateStr,
				"end_date":   endDateStr,
			},
			"kpis": map[string]interface{}{
				"booking_requests_pending": bookingCount,
				"confirmations_pending":    confirmCount,
				"confirmations_overdue":    confirmOverdue,
				"unmatched_receipts":       unmatchedCount,
				"tds_pending_count":        tdsCount,
				"tds_pending_amount":       getNestedFloat(get("tds_pending"), "total_amount"),
				"exceptions_open":              excCount,
				"exceptions_impact":            getNestedFloat(get("exceptions"), "impact"),
				"failed_posting_batches":       failedPostings,
				"receipts_pending_approval":    receiptsPendingCount,
				"tds_pending_approval":         tdsPendingApprovalCount,
				"total_work_items":             bookingCount + confirmCount + unmatchedCount + tdsCount + excCount + receiptsPendingCount + tdsPendingApprovalCount,
			},
			"tables": map[string]interface{}{
				"booking_requests":      get("booking_requests"),
				"pending_confirmations": get("pending_confirmations"),
				"unmatched_receipts":    get("unmatched_receipts"),
				"receipts":              get("receipts"),
				"tds_receipts":          get("tds_receipts"),
				"exceptions":            get("exceptions"),
				"posting_queue":         get("posting_queue"),
				"fd_list":               get("fd_list"),
			},
			"accrual_run":      get("accrual_run"),
			"tds_pending":      get("tds_pending"),
			"sla_distribution": get("sla_distribution"),
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}

// getNestedFloat safely extracts a float64 from a map[string]interface{}.
func getNestedFloat(v interface{}, key string) float64 {
	if v == nil {
		return 0
	}
	if m, ok := v.(map[string]interface{}); ok {
		if val, ok2 := m[key]; ok2 {
			switch f := val.(type) {
			case float64:
				return f
			case int64:
				return float64(f)
			case int:
				return float64(f)
			}
		}
	}
	return 0
}
