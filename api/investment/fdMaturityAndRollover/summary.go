package fdMaturityAndRollover

// summary.go — FD Maturity Summary API
//
// Handler:
//   GetFDMaturitySummary  POST /investment/fd/maturity/summary
//
// Returns a pivot-table-friendly summary of all FDs:
//   - One row per FD with key fields (bank, entity, principal, interest, dates, status)
//   - Aggregated summary section: totals by status + grand totals
// Filters (all optional): entity_id, bank_id, fd_status, closure_type, from_date, to_date

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Request / Response types ──────────────────────────────────────────────

type maturitySummaryRequest struct {
	UserID      string   `json:"user_id"`
	EntityIDs   []string `json:"entity_ids"`   // optional filter
	BankIDs     []string `json:"bank_ids"`     // optional filter
	FDStatuses  []string `json:"fd_statuses"`  // optional filter: ACTIVE / MATURED / PREMATURELY_CLOSED / ROLLED_OVER / CANCELLED
	ClosureType string   `json:"closure_type"` // optional: MATURITY / PREMATURE / ROLLOVER
	FromDate    string   `json:"from_date"`    // maturity_date >= from_date
	ToDate      string   `json:"to_date"`      // maturity_date <= to_date
}

type fdSummaryRow struct {
	FDID                 string  `json:"fd_id"`
	FDRefNo              string  `json:"fd_ref_no"`
	BankFDRefNo          string  `json:"bank_fd_ref_no"`
	EntityID             string  `json:"entity_id"`
	EntityName           string  `json:"entity_name"`
	BankID               string  `json:"bank_id"`
	BankName             string  `json:"bank_name"`
	PrincipalAmount      float64 `json:"principal_amount"`
	InterestRate         float64 `json:"interest_rate"`
	InterestTypeCode     string  `json:"interest_type_code"`
	TenureDays           int     `json:"tenure_days"`
	StartDate            *string `json:"start_date"`
	MaturityDate         *string `json:"maturity_date"`
	FDStatus             string  `json:"fd_status"`
	BookingID            string  `json:"booking_id"`
	BookingStatus        string  `json:"booking_status"`
	ClosureType          *string `json:"closure_type"`
	ClosureStatus        string  `json:"closure_status"`
	EffectiveClosureDate *string `json:"effective_closure_date"`
	AccruedInterest      float64 `json:"accrued_interest"`
	TDSDeducted          float64 `json:"tds_deducted"`
	NetPayout            float64 `json:"net_payout"`
	DaysHeld             int     `json:"days_held"`
	ReceiptCount         int     `json:"receipt_count"`
	LatestReceiptDate    string  `json:"latest_receipt_date"`
	ClosureRequestID     *string `json:"closure_request_id"`
	ApprovedAt           *string `json:"approved_at"`
}

type statusTotals struct {
	Count          int     `json:"count"`
	TotalPrincipal float64 `json:"total_principal"`
	TotalInterest  float64 `json:"total_accrued_interest"`
	TotalTDS       float64 `json:"total_tds"`
	TotalNetPayout float64 `json:"total_net_payout"`
}

type maturitySummaryResponse struct {
	Data    []fdSummaryRow           `json:"data"`
	Summary map[string]*statusTotals `json:"summary"`
	Grand   statusTotals             `json:"grand_total"`
}

// ─── Handler ────────────────────────────────────────────────────────────────

// GetFDMaturitySummary returns a pivot-table summary of all FDs.
// POST /investment/fd/maturity/summary
func GetFDMaturitySummary(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req maturitySummaryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		ctx := r.Context()

		// ── Build WHERE clauses (applied to fd_master only) ─────────────────
		masterWhere := []string{"(m.is_deleted IS NULL OR m.is_deleted = false)"}
		args := []interface{}{}
		idx := 1
		scope := ctxutil.FromContext(ctx)

		if len(req.EntityIDs) > 0 {
			placeholders := make([]string, len(req.EntityIDs))
			for i, eid := range req.EntityIDs {
				if !scope.HasEntityAccess(eid) {
					api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", eid))
					return
				}
				placeholders[i] = fmt.Sprintf("$%d", idx)
				args = append(args, eid)
				idx++
			}
			masterWhere = append(masterWhere, "m.entity_id IN ("+strings.Join(placeholders, ",")+")")
		} else if len(scope.EntityIDs) > 0 {
			masterWhere = append(masterWhere, fmt.Sprintf("m.entity_id = ANY($%d::text[])", idx))
			args = append(args, scope.EntityIDs)
			idx++
		}

		if len(req.BankIDs) > 0 {
			placeholders := make([]string, len(req.BankIDs))
			for i, bid := range req.BankIDs {
				if !scope.HasApprovedBank(bid) {
					api.RespondWithError(w, http.StatusForbidden, fmt.Sprintf("Bank '%s' is not within your approved bank scope.", bid))
					return
				}
				placeholders[i] = fmt.Sprintf("$%d", idx)
				args = append(args, bid)
				idx++
			}
			masterWhere = append(masterWhere, "m.bank_id IN ("+strings.Join(placeholders, ",")+")")
		} else if bankIDs := scope.BankIDs(); len(bankIDs) > 0 {
			masterWhere = append(masterWhere, fmt.Sprintf("(m.bank_id = '' OR m.bank_id = ANY($%d::text[]))", idx))
			args = append(args, bankIDs)
			idx++
		}

		if len(req.FDStatuses) > 0 {
			placeholders := make([]string, len(req.FDStatuses))
			for i, st := range req.FDStatuses {
				placeholders[i] = fmt.Sprintf("$%d", idx)
				args = append(args, st)
				idx++
			}
			masterWhere = append(masterWhere, "m.fd_status IN ("+strings.Join(placeholders, ",")+")")
		}

		// date range on maturity_date
		if req.FromDate != "" {
			masterWhere = append(masterWhere, fmt.Sprintf("m.maturity_date >= $%d", idx))
			args = append(args, req.FromDate)
			idx++
		}
		if req.ToDate != "" {
			masterWhere = append(masterWhere, fmt.Sprintf("m.maturity_date <= $%d", idx))
			args = append(args, req.ToDate)
			idx++
		}

		masterWhereSQL := "WHERE " + strings.Join(masterWhere, " AND ")

		// closure_type filter applied as outer WHERE on the derived table
		outerWhere := ""
		if req.ClosureType != "" {
			outerWhere = fmt.Sprintf("WHERE closure_type = $%d", idx)
			args = append(args, req.ClosureType)
			idx++
		}
		_ = idx // suppress unused warning

		// ── Resolve cashflow table (whichever exists in the DB) ──────────────
		cashflowTable := resolveCashflowTable(ctx, pool)

		// ── Build accrual LATERAL — prefer fd_accrual_ledger, fallback to cashflow ──
		var accLateral string
		if cashflowTable != "" {
			accLateral = fmt.Sprintf(`
			LEFT JOIN LATERAL (
				SELECT
					COALESCE(
						(SELECT SUM(period_interest_accrued) FROM investment.fd_accrual_ledger
						 WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false),
						(SELECT SUM(interest_accrued) FROM %s WHERE fd_id = m.fd_id AND (is_deleted IS NULL OR is_deleted=false)),
						0
					)::numeric AS total_interest_accrued,
					COALESCE(
						(SELECT SUM(tds_amount) FROM %s WHERE fd_id = m.fd_id AND (is_deleted IS NULL OR is_deleted=false)),
						0
					)::numeric AS total_tds
				FROM (SELECT 1) _dummy
			) acc ON true`, cashflowTable, cashflowTable)
		} else {
			accLateral = `LEFT JOIN LATERAL (
				SELECT
					COALESCE((SELECT SUM(period_interest_accrued) FROM investment.fd_accrual_ledger
					          WHERE fd_id = m.fd_id AND COALESCE(is_deleted,false)=false), 0)::numeric AS total_interest_accrued,
					0::numeric AS total_tds
				FROM (SELECT 1) _dummy
			) acc ON true`
		}

		// Both column names confirmed present in DB schema
		const startCol = "start_date"
		const maturityCol = "maturity_date"

		// Wrap in derived table so closure_type (LATERAL alias) can be filtered in outer WHERE
		query := fmt.Sprintf(`
			SELECT * FROM (
				SELECT
					m.fd_id,
					COALESCE(m.bank_fd_ref_no, m.fd_id)                     AS fd_ref_no,
					COALESCE(m.bank_fd_ref_no, '')                          AS bank_fd_ref_no,
					COALESCE(m.entity_id, '')                                AS entity_id,
					COALESCE(m.entity_name, '')                              AS entity_name,
					COALESCE(m.bank_id, '')                                  AS bank_id,
					COALESCE(m.bank_name, '')                                AS bank_name,
					COALESCE(m.principal_amount, 0)                          AS principal_amount,
					COALESCE(m.interest_rate, 0)                             AS interest_rate,
					COALESCE(m.interest_type_code, '')                       AS interest_type_code,
					COALESCE(m.tenure_days,
						(m.maturity_date - m.start_date),
						0
					)                                                        AS tenure_days,
					m.start_date::text                                       AS start_date,
					m.maturity_date::text                                    AS maturity_date,
					COALESCE(m.fd_status, 'UNKNOWN')                        AS fd_status,
					COALESCE(bk.booking_id, '')                              AS booking_id,
					COALESCE(bk.booking_status, '')                          AS booking_status,
					cr.closure_type,
					COALESCE(cr.closure_status, '')                          AS closure_status,
					cr.effective_closure_date::text                          AS effective_closure_date,
					COALESCE(acc.total_interest_accrued, 0)                 AS accrued_interest,
					COALESCE(acc.total_tds, 0)                              AS tds_deducted,
					COALESCE(cr.net_payout_amount, 0)                       AS net_payout,
					COALESCE(
						(COALESCE(cr.effective_closure_date, NOW()::date) - m.start_date),
						0
					)                                                        AS days_held,
					COALESCE(rct.receipt_count, 0)                           AS receipt_count,
					COALESCE(rct.latest_receipt_date, '')                    AS latest_receipt_date,
					cr.closure_request_id,
					cr.approved_at::text                                     AS approved_at
				FROM investment.fd_master m
				LEFT JOIN LATERAL (
					SELECT booking_id, booking_status
					FROM investment.fd_booking_request
					WHERE booking_id = m.booking_id
					  AND (is_deleted IS NULL OR is_deleted = false)
					LIMIT 1
				) bk ON true
				LEFT JOIN LATERAL (
					SELECT
						closure_request_id,
						closure_type,
						closure_status,
						effective_closure_date,
						net_payout_amount,
						approved_at
					FROM investment.fd_closure_request
					WHERE fd_id = m.fd_id
					  AND (is_deleted IS NULL OR is_deleted = false)
					ORDER BY created_at DESC
					LIMIT 1
				) cr ON true
				LEFT JOIN LATERAL (
					SELECT
						COUNT(*) AS receipt_count,
						COALESCE(MAX(TO_CHAR(receipt_date,'YYYY-MM-DD')),'') AS latest_receipt_date
					FROM investment.fd_interest_receipt
					WHERE fd_id = m.fd_id
					  AND is_deleted = false
				) rct ON true
				%s
				%s
			) sub
			%s
			ORDER BY sub.fd_status, sub.maturity_date ASC NULLS LAST
		`, accLateral, masterWhereSQL, outerWhere)
		_ = startCol
		_ = maturityCol

		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		data := []fdSummaryRow{}
		summary := map[string]*statusTotals{}
		grand := statusTotals{}

		for rows.Next() {
			var row fdSummaryRow
			var startDateStr, maturityDateStr *string
			var closureEffDate, closureReqID, approvedAt *string
			var closureType *string

			scanErr := rows.Scan(
				&row.FDID,
				&row.FDRefNo,
				&row.BankFDRefNo,
				&row.EntityID,
				&row.EntityName,
				&row.BankID,
				&row.BankName,
				&row.PrincipalAmount,
				&row.InterestRate,
				&row.InterestTypeCode,
				&row.TenureDays,
				&startDateStr,
				&maturityDateStr,
				&row.FDStatus,
				&row.BookingID,
				&row.BookingStatus,
				&closureType,
				&row.ClosureStatus,
				&closureEffDate,
				&row.AccruedInterest,
				&row.TDSDeducted,
				&row.NetPayout,
				&row.DaysHeld,
				&row.ReceiptCount,
				&row.LatestReceiptDate,
				&closureReqID,
				&approvedAt,
			)
			if scanErr != nil {
				api.LogError("[GetFDMaturitySummary] scan error fd=%s: %v", row.FDID, scanErr)
				continue
			}

			row.StartDate = startDateStr
			row.MaturityDate = maturityDateStr
			row.ClosureType = closureType
			row.EffectiveClosureDate = closureEffDate
			row.ClosureRequestID = closureReqID
			row.ApprovedAt = approvedAt

			data = append(data, row)

			// Accumulate summary
			st := row.FDStatus
			if _, ok := summary[st]; !ok {
				summary[st] = &statusTotals{}
			}
			summary[st].Count++
			summary[st].TotalPrincipal += row.PrincipalAmount
			summary[st].TotalInterest += row.AccruedInterest
			summary[st].TotalTDS += row.TDSDeducted
			summary[st].TotalNetPayout += row.NetPayout

			grand.Count++
			grand.TotalPrincipal += row.PrincipalAmount
			grand.TotalInterest += row.AccruedInterest
			grand.TotalTDS += row.TDSDeducted
			grand.TotalNetPayout += row.NetPayout
		}

		api.RespondWithPayload(w, true, "", maturitySummaryResponse{
			Data:    data,
			Summary: summary,
			Grand:   grand,
		})
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

// resolveCashflowTable returns the first cashflow table that actually exists in
// the DB (fd_cashflow_schedule → fd_master_cashflow_schedule → "").
func resolveCashflowTable(ctx context.Context, pool *pgxpool.Pool) string {
	candidates := []struct{ schema, table string }{
		{"investment", "fd_cashflow_schedule"},
		{"investment", "fd_master_cashflow_schedule"},
	}
	for _, c := range candidates {
		var exists bool
		_ = pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = $1 AND table_name = $2
			)`, c.schema, c.table).Scan(&exists)
		if exists {
			return c.schema + "." + c.table
		}
	}
	return ""
}
