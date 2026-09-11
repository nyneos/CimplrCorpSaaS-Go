package scope

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// eligibleFDStatuses are live / in-window statuses that may be pulled into a
// closing cycle. REJECTED is excluded by design.
var eligibleFDStatuses = []string{
	"ACTIVE",
	"ROLLED_OVER",
	"MATURED",
	"PREMATURELY_CLOSED",
}

// ListEligibleFDs handles POST /investment/fd-closing/scope/list-eligible.
// Returns FDs for an entity that overlap the closing period window and are in
// an eligible status. Used by Period Close Setup + Scope Selection "Load FDs".
func ListEligibleFDs(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntityID       string `json:"entity_id"`
			PeriodStart    string `json:"period_start"`
			PeriodEnd      string `json:"period_end"`
			IncludeMatured *bool  `json:"include_matured"`
			BankID         string `json:"bank_id"`       // optional scope filter
			CurrencyCode   string `json:"currency_code"` // accepted; fd_master has no currency column today
			CycleID        string `json:"cycle_id"`      // optional — exclude FDs already in this cycle
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.EntityID = strings.TrimSpace(req.EntityID)
		req.PeriodStart = strings.TrimSpace(req.PeriodStart)
		req.PeriodEnd = strings.TrimSpace(req.PeriodEnd)
		req.BankID = strings.TrimSpace(req.BankID)
		req.CurrencyCode = strings.TrimSpace(req.CurrencyCode)
		req.CycleID = strings.TrimSpace(req.CycleID)
		if req.EntityID == "" || req.PeriodStart == "" || req.PeriodEnd == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"entity_id, period_start and period_end are required")
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(req.EntityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+req.EntityID+"' is not within your authorized access scope.")
			return
		}

		includeMatured := true
		if req.IncludeMatured != nil {
			includeMatured = *req.IncludeMatured
		}

		statuses := append([]string{}, eligibleFDStatuses...)
		if !includeMatured {
			filtered := make([]string, 0, len(statuses))
			for _, s := range statuses {
				if s == "MATURED" || s == "PREMATURELY_CLOSED" {
					continue
				}
				filtered = append(filtered, s)
			}
			statuses = filtered
		}

		currencyExpr := resolveFDCurrencyExpr(ctx, pool)
		currencySelect := `'' AS currency_code`
		if currencyExpr != "" {
			currencySelect = fmt.Sprintf(`COALESCE((
					SELECT %s
					FROM investment.fd_confirmation c
					LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
					WHERE c.confirmation_id = m.confirmation_id
					LIMIT 1
				),'') AS currency_code`, currencyExpr)
		}

		q := fmt.Sprintf(`
			SELECT
				m.fd_id,
				COALESCE(NULLIF(BTRIM(m.bank_fd_ref_no), ''), m.fd_id) AS bank_fd_ref_no,
				COALESCE(m.entity_id,'') AS entity_id,
				COALESCE(m.entity_name,'') AS entity_name,
				COALESCE(m.bank_id,'') AS bank_id,
				COALESCE(m.bank_name,'') AS bank_name,
				COALESCE(m.principal_amount,0) AS principal_amount,
				COALESCE(m.fd_status,'') AS fd_status,
				COALESCE(TO_CHAR(m.start_date,'YYYY-MM-DD'),'') AS start_date,
				COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date,
				%s
			FROM investment.fd_master m
			WHERE COALESCE(m.is_deleted,false) = false`, currencySelect) + `
			  AND m.entity_id = $1
			  AND UPPER(COALESCE(m.fd_status,'')) = ANY($2::text[])
			  AND m.start_date IS NOT NULL
			  AND m.start_date <= $4::date
			  AND (m.maturity_date IS NULL OR m.maturity_date >= $3::date)`
		args := []interface{}{req.EntityID, statuses, req.PeriodStart, req.PeriodEnd}
		argIdx := 5

		// Optional bank filter — must match fd_master.bank_id when the cycle
		// header (or form) sets Bank (Optional). Without this, Citi Bank on
		// the cycle still returned Union Bank FDs for the same entity.
		if req.BankID != "" {
			q += fmt.Sprintf(` AND COALESCE(m.bank_id,'') = $%d`, argIdx)
			args = append(args, req.BankID)
			argIdx++
		}

		currencyFilterApplied := false
		if req.CurrencyCode != "" && currencyExpr != "" {
			q += fmt.Sprintf(`
			  AND EXISTS (
				SELECT 1 FROM investment.fd_confirmation c
				LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
				WHERE c.confirmation_id = m.confirmation_id
				  AND UPPER(%s) = UPPER($%d)
			  )`, currencyExpr, argIdx)
			args = append(args, req.CurrencyCode)
			argIdx++
			currencyFilterApplied = true
		}

		if req.CycleID != "" {
			q += fmt.Sprintf(`
			  AND NOT EXISTS (
				SELECT 1 FROM investment.fd_closing_cycle_fd_scope s
				WHERE s.cycle_id = $%d
				  AND s.fd_id = m.fd_id
				  AND s.is_deleted = false
				  AND s.selection_status IN ('SELECTED','APPROVED')
			  )`, argIdx)
			args = append(args, req.CycleID)
		}
		q += ` ORDER BY m.fd_id ASC`

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] ListEligibleFDs query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] ListEligibleFDs scan: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Eligible FDs loaded", map[string]interface{}{
			"rows":              out,
			"count":             len(out),
			"include_matured":   includeMatured,
			"bank_id":           req.BankID,
			"currency_code":     req.CurrencyCode,
			"currency_filter_applied": currencyFilterApplied,
			"bank_filter_applied":     req.BankID != "",
			"eligible_statuses":       statuses,
		})
	}
}

func resolveFDCurrencyExpr(ctx context.Context, pool *pgxpool.Pool) string {
	confCols, err := loadClosingTableColumns(ctx, pool, "investment", "fd_confirmation")
	if err != nil {
		return ""
	}
	bookingCols, err := loadClosingTableColumns(ctx, pool, "investment", "fd_booking_request")
	if err != nil {
		return ""
	}
	switch {
	case confCols["currency"]:
		return "COALESCE(c.currency,'')"
	case confCols["currency_code"]:
		return "COALESCE(c.currency_code,'')"
	case bookingCols["currency"]:
		return "COALESCE(b.currency,'')"
	case bookingCols["currency_code"]:
		return "COALESCE(b.currency_code,'')"
	}
	return ""
}

func loadClosingTableColumns(ctx context.Context, pool *pgxpool.Pool, schemaName, tableName string) (map[string]bool, error) {
	rows, err := pool.Query(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2`,
		schemaName, tableName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columns[columnName] = true
	}
	return columns, rows.Err()
}

// validateEligibleStatus rejects FDs that are not in the closing-eligible set
// (used by CreateScope so REJECTED / DRAFT / etc. cannot be added by ID).
func validateEligibleStatus(status string) error {
	u := strings.ToUpper(strings.TrimSpace(status))
	for _, s := range eligibleFDStatuses {
		if s == u {
			return nil
		}
	}
	return fmt.Errorf("FD status %q is not eligible for closing scope (allowed: ACTIVE, ROLLED_OVER, MATURED, PREMATURELY_CLOSED)", status)
}
