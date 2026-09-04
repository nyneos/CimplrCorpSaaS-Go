package scope

import (
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
			CycleID        string `json:"cycle_id"` // optional — exclude FDs already in this cycle
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.EntityID = strings.TrimSpace(req.EntityID)
		req.PeriodStart = strings.TrimSpace(req.PeriodStart)
		req.PeriodEnd = strings.TrimSpace(req.PeriodEnd)
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

		q := `
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
				COALESCE(TO_CHAR(m.maturity_date,'YYYY-MM-DD'),'') AS maturity_date
			FROM investment.fd_master m
			WHERE COALESCE(m.is_deleted,false) = false
			  AND m.entity_id = $1
			  AND UPPER(COALESCE(m.fd_status,'')) = ANY($2::text[])
			  AND m.start_date IS NOT NULL
			  AND m.start_date <= $4::date
			  AND (m.maturity_date IS NULL OR m.maturity_date >= $3::date)`
		args := []interface{}{req.EntityID, statuses, req.PeriodStart, req.PeriodEnd}

		if req.CycleID != "" {
			q += `
			  AND NOT EXISTS (
				SELECT 1 FROM investment.fd_closing_cycle_fd_scope s
				WHERE s.cycle_id = $5
				  AND s.fd_id = m.fd_id
				  AND s.is_deleted = false
				  AND s.selection_status IN ('SELECTED','APPROVED')
			  )`
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
			"eligible_statuses": statuses,
		})
	}
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
