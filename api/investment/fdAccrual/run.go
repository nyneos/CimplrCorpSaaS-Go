package fdAccrual

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func getUserEmail(userID string) string {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			return s.Email
		}
	}
	return ""
}

func nullIfEmpty(v string) interface{} {
	if v == "" {
		return nil
	}
	return v
}

// ─── 1. CreateAccrualRun ──────────────────────────────────────────────────────

func CreateAccrualRun(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID             string   `json:"user_id"`
			RunType            string   `json:"run_type"`
			RunMode            string   `json:"run_mode"`
			EntityID           string   `json:"entity_id"`
			EntityName         string   `json:"entity_name"`
			BankIDFilter       string   `json:"bank_id_filter"`
			FDStatusFilter     string   `json:"fd_status_filter"`
			AccrualPeriodStart string   `json:"accrual_period_start"`
			AccrualPeriodEnd   string   `json:"accrual_period_end"`
			FinancialPeriod    string   `json:"financial_period"`
			DayCountConvention string   `json:"day_count_convention"`
			RoundingRule       string   `json:"rounding_rule"`
			PrecisionDecimals  int      `json:"precision_decimals"`
			FDInclusionMethod  string   `json:"fd_inclusion_method"`
			FDInclusionList    []string `json:"fd_inclusion_list"`
			AcrualGranularity  string   `json:"accrual_granularity"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.AccrualPeriodStart == "" || req.AccrualPeriodEnd == "" {
			api.RespondWithError(w, http.StatusBadRequest, "accrual_period_start and accrual_period_end are required")
			return
		}
		if req.EntityID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_id is required")
			return
		}
		if req.RunMode == "" {
			req.RunMode = "SIMULATION"
		}
		if req.RunType == "" {
			req.RunType = "MONTHLY" // default; caller can pass DAILY|QUARTERLY|HALF_YEARLY|YEARLY
		}
		if req.DayCountConvention == "" {
			req.DayCountConvention = "ACT_365"
		}
		if req.RoundingRule == "" {
			req.RoundingRule = "ROUND"
		}
		if req.PrecisionDecimals <= 0 {
			req.PrecisionDecimals = 2
		}
		if req.FDStatusFilter == "" {
			req.FDStatusFilter = "ACTIVE"
		}
		if req.AcrualGranularity == "" {
			req.AcrualGranularity = "RUN"
		}
		req.AcrualGranularity = normalizeAccrualGranularity(req.AcrualGranularity)
		if !isValidAccrualGranularity(req.AcrualGranularity) {
			api.RespondWithError(w, http.StatusBadRequest,
				"accrual_granularity must be DAILY, MONTHLY, QUARTERLY, HALF_YEARLY, YEARLY, or RUN")
			return
		}
		req.RunType = normalizeAccrualGranularity(req.RunType)
		if req.RunType == "" {
			req.RunType = RunTypeForGranularity(req.AcrualGranularity)
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		fdListJSON := "[]"
		if len(req.FDInclusionList) > 0 {
			b, _ := json.Marshal(req.FDInclusionList)
			fdListJSON = string(b)
		}

		input := CreateAccrualRunInput{
			RunType:            req.RunType,
			RunMode:            req.RunMode,
			EntityID:           req.EntityID,
			EntityName:         req.EntityName,
			BankIDFilter:       req.BankIDFilter,
			FDStatusFilter:     req.FDStatusFilter,
			FinancialPeriod:    req.FinancialPeriod,
			DayCountConvention: req.DayCountConvention,
			RoundingRule:       req.RoundingRule,
			PrecisionDecimals:  req.PrecisionDecimals,
			FDInclusionMethod:  req.FDInclusionMethod,
			Granularity:        req.AcrualGranularity,
			CreatedBy:          userEmail,
		}
		var parseErr error
		input.AccrualPeriodStart, parseErr = time.Parse(constants.DateFormat, req.AccrualPeriodStart)
		if parseErr != nil {
			api.RespondWithError(w, http.StatusBadRequest, "accrual_period_start must be YYYY-MM-DD")
			return
		}
		input.AccrualPeriodEnd, parseErr = time.Parse(constants.DateFormat, req.AccrualPeriodEnd)
		if parseErr != nil {
			api.RespondWithError(w, http.StatusBadRequest, "accrual_period_end must be YYYY-MM-DD")
			return
		}
		if input.FinancialPeriod == "" {
			input.FinancialPeriod = buildAccrualPeriod(input.AccrualPeriodStart)
		}

		runID, err := createAccrualRunInternal(ctx, pgxPool, input)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Create accrual run failed: "+friendlyAccrualError(err, "run creation"))
			return
		}

		// Store inclusion list if any
		if len(req.FDInclusionList) > 0 {
			if _, execErr := pgxPool.Exec(ctx,
				`UPDATE investment.fd_accrual_run SET fd_inclusion_list = $1 WHERE run_id = $2`,
				fdListJSON, runID,
			); execErr != nil {
				api.LogError("[FDAccrual] fd_inclusion_list update failed run=%s: %v", runID, execErr)
			}
		}

		var createdRun struct {
			RunID              string      `json:"run_id"`
			RunType            string      `json:"run_type"`
			RunMode            string      `json:"run_mode"`
			RunStatus          string      `json:"run_status"`
			EntityID           string      `json:"entity_id"`
			EntityName         string      `json:"entity_name"`
			AccrualPeriodStart interface{} `json:"accrual_period_start"`
			AccrualPeriodEnd   interface{} `json:"accrual_period_end"`
			FinancialPeriod    string      `json:"financial_period"`
			DayCountConvention string      `json:"day_count_convention"`
			RoundingRule       string      `json:"rounding_rule"`
			PrecisionDecimals  int         `json:"precision_decimals"`
			FDStatusFilter     string      `json:"fd_status_filter"`
			FDInclusionMethod  string      `json:"fd_inclusion_method"`
			AcrualGranularity  string      `json:"accrual_granularity"`
			EngineVersion      string      `json:"engine_version"`
			CreatedBy          string      `json:"created_by"`
			CreatedAt          interface{} `json:"created_at"`
		}
		_ = pgxPool.QueryRow(ctx, `
			SELECT
				run_id,
				COALESCE(run_type,''),
				COALESCE(run_mode,'SIMULATION'),
				COALESCE(run_status,'DRAFT'),
				COALESCE(entity_id,''),
				COALESCE(entity_name,''),
				accrual_period_start,
				accrual_period_end,
				COALESCE(financial_period,''),
				COALESCE(day_count_convention,'ACT_365'),
				COALESCE(rounding_rule,'ROUND'),
				COALESCE(precision_decimals,2),
				COALESCE(fd_status_filter,'ACTIVE'),
				COALESCE(fd_inclusion_method,'ALL'),
				COALESCE(accrual_granularity,'RUN'),
				COALESCE(engine_version,''),
				COALESCE(created_by,''),
				created_at
			FROM investment.fd_accrual_run WHERE run_id=$1`, runID,
		).Scan(
			&createdRun.RunID, &createdRun.RunType, &createdRun.RunMode,
			&createdRun.RunStatus, &createdRun.EntityID, &createdRun.EntityName,
			&createdRun.AccrualPeriodStart, &createdRun.AccrualPeriodEnd,
			&createdRun.FinancialPeriod, &createdRun.DayCountConvention,
			&createdRun.RoundingRule, &createdRun.PrecisionDecimals,
			&createdRun.FDStatusFilter, &createdRun.FDInclusionMethod,
			&createdRun.AcrualGranularity, &createdRun.EngineVersion,
			&createdRun.CreatedBy, &createdRun.CreatedAt,
		)
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run": createdRun,
			"next_step": map[string]interface{}{
				"action":   "validate",
				"endpoint": "/investment/fd/accrual/run/validate",
				"body":     map[string]string{"run_id": runID},
			},
		})
		api.LogInfo("[FDAccrual] CreateAccrualRun: run_id=%s mode=%s entity=%s period=%s→%s",
			runID, req.RunMode, req.EntityID, req.AccrualPeriodStart, req.AccrualPeriodEnd)
	}
}

// ─── 2. ValidateScope ─────────────────────────────────────────────────────────

func ValidateScope(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			RunID  string `json:"run_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDRequired)
			return
		}

		ctx := r.Context()

		eligible, blockers, findings, err := validateAndPersistFindings(ctx, pgxPool, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Validation failed: "+err.Error())
			return
		}

		newStatus := "VALIDATED"
		validationFailed := false
		if blockers > 0 {
			newStatus = "VALIDATION_FAILED"
			validationFailed = true
		}
		_, _ = pgxPool.Exec(ctx,
			`UPDATE investment.fd_accrual_run SET run_status=$1, fds_in_scope=$2 WHERE run_id=$3`,
			newStatus, eligible, req.RunID)

		// ── Fetch validation findings for UI — joined with fd_master context ───
		findingRows, _ := pgxPool.Query(ctx, `
			SELECT
				COALESCE(vf.fd_id,'')                          AS fd_id,
				COALESCE(vf.fd_ref_no,'')                      AS fd_ref_no,
				COALESCE(vf.bank_name,'')                      AS bank_name,
				COALESCE(vf.issue_type,'')                     AS issue_type,
				COALESCE(vf.severity,'')                       AS severity,
				COALESCE(vf.issue_description,'')              AS issue_description,
				COALESCE(vf.suggested_action,'')               AS suggested_action,
				COALESCE(vf.is_resolved,false)                 AS is_resolved,
				TO_CHAR(vf.created_at,'YYYY-MM-DD HH24:MI:SS') AS created_at,
				COALESCE(fm.fd_status,'')                      AS fd_current_status,
				COALESCE(fm.principal_amount,0)                AS fd_principal,
				COALESCE(fm.interest_rate,0)                   AS fd_interest_rate,
				COALESCE(fm.cashflow_generated,false)          AS fd_cashflow_generated,
				TO_CHAR(fm.start_date,'YYYY-MM-DD')            AS fd_start_date,
				TO_CHAR(fm.maturity_date,'YYYY-MM-DD')         AS fd_maturity_date
			FROM investment.fd_accrual_validation_finding vf
			LEFT JOIN investment.fd_master fm
				ON fm.fd_id = vf.fd_id
				AND COALESCE(fm.is_deleted,false) = false
			WHERE vf.run_id = $1
			ORDER BY vf.severity DESC, vf.fd_id`, req.RunID)
		dbFindings := make([]map[string]interface{}, 0)
		warningFindings, infoFindings := 0, 0
		if findingRows != nil {
			flds := findingRows.FieldDescriptions()
			for findingRows.Next() {
				vals, _ := findingRows.Values()
				row := make(map[string]interface{}, len(flds))
				for i, f := range flds {
					if vals[i] == nil {
						row[string(f.Name)] = ""
					} else {
						row[string(f.Name)] = vals[i]
					}
				}
				dbFindings = append(dbFindings, row)
				switch row["severity"] {
				case "WARNING":
					warningFindings++
				default:
					infoFindings++
				}
			}
			findingRows.Close()
		}

		payload := buildAccrualResponse(ctx, pgxPool, req.RunID)
		payload["run_id"] = req.RunID
		payload["validation_summary"] = map[string]interface{}{
			"eligible_fds":  eligible,
			"blocker_count": blockers,
			"warning_count": warningFindings,
			"info_count":    infoFindings,
			"has_blockers":  blockers > 0,
			"status":        newStatus,
		}
		// prefer the DB-enriched findings for UI, but fall back to the in-memory findings
		if len(dbFindings) > 0 {
			payload["findings"] = dbFindings
			payload["finding_count"] = len(dbFindings)
		} else {
			payload["findings"] = findings
			payload["finding_count"] = len(findings)
		}

		if validationFailed {
			api.RespondWithPayload(w, false, fmt.Sprintf("Validation failed: %d blocker(s) found — run_id=%s status=VALIDATION_FAILED", blockers, req.RunID), payload)
			return
		}

		api.RespondWithPayload(w, true, "", payload)
		api.LogInfo("[FDAccrual] ValidateScope: run_id=%s eligible=%d blockers=%d status=%s",
			req.RunID, eligible, blockers, newStatus)
	}
}

// ─── 3. RunAccrual ────────────────────────────────────────────────────────────

func RunAccrual(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			RunID  string `json:"run_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDRequired)
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// VALIDATED gate: only VALIDATED runs can be executed.
		var gateStatus string
		if err := pgxPool.QueryRow(ctx,
			`SELECT COALESCE(run_status,'') FROM investment.fd_accrual_run WHERE run_id=$1`,
			req.RunID,
		).Scan(&gateStatus); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Run not found: "+err.Error())
			return
		}
		if gateStatus != "VALIDATED" {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf("Run must be in VALIDATED status before execution (current: %s). Call /validate first.", gateStatus))
			return
		}

		calculated, failed, err := executeAccrualRun(ctx, pgxPool, req.RunID, userEmail)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError,
				"Accrual execution failed: "+friendlyAccrualError(err, "accrual execution"))
			return
		}
		if calculated == 0 && failed == 0 {
			api.RespondWithError(w, http.StatusBadRequest,
				fmt.Sprintf(
					"Accrual run %s completed with 0 FDs calculated and 0 failures. "+
						"The FD scope became empty between validate and execute. "+
						"Check fd_status=ACTIVE and cashflow_generated=true for entity %s.",
					req.RunID, req.UserID,
				),
			)
			return
		}

		// ── Enrich: return full ledger table for immediate UI rendering ───────
		var runModeCheck string
		_ = pgxPool.QueryRow(ctx, `SELECT COALESCE(run_mode,'FINAL') FROM investment.fd_accrual_run WHERE run_id=$1`, req.RunID).Scan(&runModeCheck)
		payload := buildAccrualResponse(ctx, pgxPool, req.RunID)
		payload["calculated"] = calculated
		payload["failed"] = failed

		api.RespondWithPayload(w, true, "", payload)
		api.LogInfo("[FDAccrual] RunAccrual: run_id=%s calculated=%d failed=%d", req.RunID, calculated, failed)
	}
}

// ─── 4. GetAccrualLedger ──────────────────────────────────────────────────────

func GetAccrualLedger(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			RunID    string `json:"run_id"`
			EntityID string `json:"entity_id"` // optional extra filter
			BankID   string `json:"bank_id"`   // optional
			FDID     string `json:"fd_id"`     // optional
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDRequired)
			return
		}

		ctx := r.Context()

		extraFilter := ""
		var args []interface{}
		args = append(args, req.RunID)
		if req.BankID != "" {
			args = append(args, req.BankID)
			extraFilter += fmt.Sprintf(" AND l.bank_id = $%d", len(args))
		}
		if req.FDID != "" {
			args = append(args, req.FDID)
			extraFilter += fmt.Sprintf(" AND l.fd_id = $%d", len(args))
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				l.ledger_id, l.run_id, l.fd_id,
				COALESCE(l.fd_ref_no,'')           AS fd_ref_no,
				COALESCE(l.bank_id,'')             AS bank_id,
				COALESCE(l.bank_name,'')           AS bank_name,
				COALESCE(l.entity_id,'')           AS entity_id,
				COALESCE(l.entity_name,'')         AS entity_name,
				COALESCE(l.interest_type_code,'')  AS interest_type_code,
				COALESCE(l.principal_amount,0)     AS principal_amount,
				COALESCE(l.interest_rate,0)        AS interest_rate,
				l.accrual_period_start,
				l.accrual_period_end,
				COALESCE(l.accrual_days,0)         AS accrual_days,
				COALESCE(l.day_count_code,'ACT_365') AS day_count_code,
				COALESCE(l.divisor,365)            AS divisor,
				COALESCE(l.opening_principal,0)    AS opening_principal,
				COALESCE(l.daily_accrual_rate,0)   AS daily_accrual_rate,
				COALESCE(l.period_interest_accrued,0)  AS period_interest_accrued,
				COALESCE(l.opening_accrued_balance,0)  AS opening_accrued_balance,
				COALESCE(l.closing_accrued_balance,0)  AS closing_accrued_balance,
				COALESCE(l.interest_received_in_period,0) AS interest_received_in_period,
				COALESCE(l.tds_applicable_amount,0)    AS tds_applicable_amount,
				COALESCE(l.tds_deducted_in_period,0)   AS tds_deducted_in_period,
				COALESCE(l.net_interest_in_period,0)   AS net_interest_in_period,
				-- req baseline
				COALESCE(l.req_period_interest,0)      AS req_period_interest,
				COALESCE(l.req_accrual_days,0)         AS req_accrual_days,
				-- running totals per FD (within this run)
				SUM(l.period_interest_accrued) OVER (
					PARTITION BY l.fd_id ORDER BY l.accrual_period_start
				)                                      AS running_interest_accrued,
				SUM(l.tds_deducted_in_period) OVER (
					PARTITION BY l.fd_id ORDER BY l.accrual_period_start
				)                                      AS running_tds_deducted,
				SUM(l.net_interest_in_period) OVER (
					PARTITION BY l.fd_id ORDER BY l.accrual_period_start
				)                                      AS running_net_interest,
				-- variance indicator
				ROUND((COALESCE(l.period_interest_accrued,0) -
				       COALESCE(l.req_period_interest,0))::numeric, 4)
				                                       AS config_vs_req_variance,
				-- override flags
				COALESCE(l.is_overridden,false)        AS is_overridden,
				COALESCE(l.override_status,'')         AS override_status,
				COALESCE(l.override_amount,0)          AS override_amount,
				COALESCE(l.ledger_row_status,'')       AS ledger_row_status,
				COALESCE(l.config_holidays_excluded,0) AS holidays_excluded,
				COALESCE(l.config_rounding_rule,'ROUND') AS config_rounding_rule,
				COALESCE(l.req_rounding_rule,'ROUND')    AS req_rounding_rule,
				COALESCE(l.journal_entry_id,'')        AS journal_entry_id
			FROM investment.fd_accrual_ledger l
			WHERE l.run_id = $1
			  AND COALESCE(l.is_deleted,false) = false`+
			extraFilter+`
			ORDER BY l.entity_id, l.bank_name, l.fd_id, l.accrual_period_start`,
			args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		// Scan properly
		var ledger []map[string]interface{}
		var totalInterest, totalTDS, totalNet, totalReqInterest float64
		bankTotals := map[string]float64{} // bank_id → net interest
		fields := rows.FieldDescriptions()
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = nil
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			ledger = append(ledger, row)
			if v, ok := row["period_interest_accrued"]; ok && v != nil {
				if f, ok := v.(float64); ok {
					totalInterest += f
				}
			}
			if v, ok := row["tds_deducted_in_period"]; ok && v != nil {
				if f, ok := v.(float64); ok {
					totalTDS += f
				}
			}
			if v, ok := row["net_interest_in_period"]; ok && v != nil {
				if f, ok := v.(float64); ok {
					totalNet += f
				}
			}
			if v, ok := row["req_period_interest"]; ok && v != nil {
				if f, ok := v.(float64); ok {
					totalReqInterest += f
				}
			}
			bankID := ""
			if v, ok := row["bank_id"]; ok && v != nil {
				bankID, _ = v.(string)
			}
			if bankID != "" {
				if v, ok := row["net_interest_in_period"]; ok && v != nil {
					if f, ok := v.(float64); ok {
						bankTotals[bankID] += f
					}
				}
			}
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}

		// Build bank summary list
		bankSummaryList := make([]map[string]interface{}, 0, len(bankTotals))
		for bid, net := range bankTotals {
			bankSummaryList = append(bankSummaryList, map[string]interface{}{
				"bank_id":            bid,
				"total_net_interest": math.Round(net*100) / 100,
			})
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id": req.RunID,
			"count":  len(ledger),
			"summary": map[string]interface{}{
				"total_interest_accrued": math.Round(totalInterest*100) / 100,
				"total_tds_deducted":     math.Round(totalTDS*100) / 100,
				"total_net_interest":     math.Round(totalNet*100) / 100,
				"total_req_interest":     math.Round(totalReqInterest*100) / 100,
				"config_vs_req_variance": math.Round((totalInterest-totalReqInterest)*100) / 100,
				"fd_count":               len(ledger),
				"by_bank":                bankSummaryList,
			},
			"ledger": ledger,
		})
	}
}

// ─── 5. GetAccrualCalculationDetail ───────────────────────────────────────────
// Accepts ledger_id (preferred) or run_id+fd_id (backward-compatibility).
// Returns two full calculation views (req vs config) with variance_drivers
// and old_values for instant before/after comparison without joining audit.

func GetAccrualCalculationDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			LedgerID string `json:"ledger_id"` // preferred
			RunID    string `json:"run_id"`    // legacy fallback
			FDID     string `json:"fd_id"`     // legacy fallback
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.LedgerID == "" && (req.RunID == "" || req.FDID == "") {
			api.RespondWithError(w, http.StatusBadRequest,
				constants.ErrLedgerIDRequired)
			return
		}

		ctx := r.Context()

		// Build the WHERE clause — ledger_id takes priority
		var whereClause string
		var whereArgs []interface{}
		if req.LedgerID != "" {
			whereClause = "l.ledger_id = $1"
			whereArgs = []interface{}{req.LedgerID}
		} else {
			whereClause = "l.run_id = $1 AND l.fd_id = $2"
			whereArgs = []interface{}{req.RunID, req.FDID}
		}

		query := `
			SELECT
				l.ledger_id, l.run_id, l.fd_id,
				COALESCE(l.fd_ref_no,'')          AS fd_ref_no,
				COALESCE(l.bank_name,'')          AS bank_name,
				COALESCE(l.entity_id,'')          AS entity_id,
				COALESCE(l.interest_type_code,'') AS interest_type_code,
				COALESCE(l.principal_amount,0)    AS principal_amount,
				COALESCE(l.interest_rate,0)       AS interest_rate,
				l.fd_start_date, l.fd_maturity_date,
				-- Compounding specifics joined from master
				COALESCE(f.frequency_id, '')      AS frequency_id,
				COALESCE(f.frequency_id, '')      AS compounding_frequency,
				-- Period
				l.accrual_period_start, l.accrual_period_end,
				COALESCE(l.accrual_days,0)        AS accrual_days,
				-- Config-path (authoritative)
				COALESCE(l.day_count_code,'')     AS day_count_code,
				COALESCE(l.divisor,365)           AS divisor,
				COALESCE(l.opening_principal,0)   AS opening_principal,
				COALESCE(l.daily_accrual_rate,0)  AS daily_accrual_rate,
				COALESCE(l.period_interest_accrued,0)    AS period_interest_accrued,
				COALESCE(l.closing_accrued_balance,0)    AS closing_accrued_balance,
				COALESCE(l.tds_deducted_in_period,0)     AS tds_deducted_in_period,
				COALESCE(l.net_interest_in_period,0)     AS net_interest_in_period,
				COALESCE(l.formula_used,'')              AS formula_used,
				COALESCE(l.config_weekend_accrual,true)  AS config_weekend_accrual,
				COALESCE(l.config_holiday_accrual,true)  AS config_holiday_accrual,
				COALESCE(l.config_holidays_excluded,0)   AS config_holidays_excluded,
				COALESCE(l.config_rounding_rule,'ROUND') AS config_rounding_rule,
				COALESCE(l.config_precision_decimals,2)  AS config_precision_decimals,
				COALESCE(l.config_cap_date_adjustment,'') AS config_cap_date_adjustment,
				COALESCE(l.config_holiday_calendar_code,'') AS config_holiday_calendar_code,
				-- Req-path (run-level baseline)
				COALESCE(l.req_day_count_code,'')       AS req_day_count_code,
				COALESCE(l.req_divisor,365)             AS req_divisor,
				COALESCE(l.req_accrual_days,0)          AS req_accrual_days,
				COALESCE(l.req_opening_principal,0)     AS req_opening_principal,
				COALESCE(l.req_daily_accrual_rate,0)    AS req_daily_accrual_rate,
				COALESCE(l.req_period_interest,0)       AS req_period_interest,
				COALESCE(l.req_closing_balance,0)       AS req_closing_balance,
				COALESCE(l.req_tds_deducted,0)          AS req_tds_deducted,
				COALESCE(l.req_net_interest,0)          AS req_net_interest,
				COALESCE(l.req_formula_used,'')         AS req_formula_used,
				COALESCE(l.req_rounding_rule,'ROUND')   AS req_rounding_rule,
				COALESCE(l.req_precision_decimals,2)    AS req_precision_decimals,
				COALESCE(l.req_weekend_accrual,true)    AS req_weekend_accrual,
				COALESCE(l.req_holiday_accrual,true)    AS req_holiday_accrual,
				-- Old values (pre-change snapshot on ledger row)
				COALESCE(l.old_period_interest_accrued,0)  AS old_period_interest_accrued,
				COALESCE(l.old_closing_accrued_balance,0)  AS old_closing_accrued_balance,
				COALESCE(l.old_tds_deducted_in_period,0)   AS old_tds_deducted_in_period,
				COALESCE(l.old_net_interest_in_period,0)   AS old_net_interest_in_period,
				COALESCE(l.old_formula_used,'')            AS old_formula_used,
				COALESCE(l.old_ledger_row_status,'')       AS old_ledger_row_status,
				COALESCE(l.old_day_count_code,'')          AS old_day_count_code,
				COALESCE(l.old_opening_principal,0)        AS old_opening_principal,
				-- Override info
				COALESCE(l.is_overridden,false)            AS is_overridden,
				COALESCE(l.override_amount,0)              AS override_amount,
				COALESCE(l.override_adjustment,0)          AS override_adjustment,
				COALESCE(l.override_reason_code,'')        AS override_reason_code,
				COALESCE(l.override_reason_text,'')        AS override_reason_text,
				COALESCE(l.override_status,'')             AS override_status,
				COALESCE(l.override_proposed_by,'')        AS override_proposed_by,
				COALESCE(l.override_approved_by,'')        AS override_approved_by,
				COALESCE(l.ledger_row_status,'')           AS ledger_row_status,
				COALESCE(l.opening_accrued_balance,0)      AS opening_accrued_balance
			FROM investment.fd_accrual_ledger l
			LEFT JOIN investment.fd_master f ON f.fd_id = l.fd_id
			WHERE ` + whereClause + `
			  AND COALESCE(l.is_deleted,false) = false
			ORDER BY l.accrual_period_start ASC`

		rows, err := pgxPool.Query(ctx, query, whereArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		// One response per ledger row (may be multiple for sub-period granularity)
		type detailRow struct {
			LedgerID, RunID, FDID             string
			FdRefNo, BankName, EntityID       string
			InterestTypeCode                  string
			PrincipalAmount, InterestRate     float64
			FdStartDate, FdMaturityDate       time.Time
			FrequencyID, CompoundingFrequency string
			PeriodStart, PeriodEnd            time.Time
			// config
			AccrualDays                                  int
			DayCountCode                                 string
			Divisor                                      int
			OpeningPrincipal, DailyRate                  float64
			PeriodInterest, ClosingBal, TDS, NetInterest float64
			FormulaUsed                                  string
			ConfigWeekendAccrual, ConfigHolidayAccrual   bool
			ConfigHolidaysExcluded                       int
			ConfigRoundingRule                           string
			ConfigPrecisionDecimals                      int
			ConfigCapDateAdj, ConfigCalendar             string
			// req
			ReqDayCountCode                      string
			ReqDivisor, ReqAccrualDays           int
			ReqOpeningPrincipal, ReqDailyRate    float64
			ReqPeriodInterest, ReqClosingBal     float64
			ReqTDS, ReqNetInterest               float64
			ReqFormulaUsed, ReqRoundingRule      string
			ReqPrecisionDecimals                 int
			ReqWeekendAccrual, ReqHolidayAccrual bool
			// old
			OldPeriodInterest, OldClosingBal float64
			OldTDS, OldNetInterest           float64
			OldFormulaUsed, OldRowStatus     string
			OldDayCountCode                  string
			OldOpeningPrincipal              float64
			// override
			IsOverridden                           bool
			OverrideAmount, OverrideAdj            float64
			OverrideReasonCode, OverrideReasonText string
			OverrideStatus, OverrideProposedBy     string
			OverrideApprovedBy                     string
			LedgerRowStatus                        string
			OpeningAccruedBal                      float64
		}

		var ledgerRows []map[string]interface{}
		for rows.Next() {
			var d detailRow
			if sErr := rows.Scan(
				&d.LedgerID, &d.RunID, &d.FDID,
				&d.FdRefNo, &d.BankName, &d.EntityID,
				&d.InterestTypeCode,
				&d.PrincipalAmount, &d.InterestRate,
				&d.FdStartDate, &d.FdMaturityDate,
				&d.FrequencyID, &d.CompoundingFrequency,
				&d.PeriodStart, &d.PeriodEnd, &d.AccrualDays,
				// config
				&d.DayCountCode, &d.Divisor,
				&d.OpeningPrincipal, &d.DailyRate,
				&d.PeriodInterest, &d.ClosingBal, &d.TDS, &d.NetInterest,
				&d.FormulaUsed,
				&d.ConfigWeekendAccrual, &d.ConfigHolidayAccrual,
				&d.ConfigHolidaysExcluded,
				&d.ConfigRoundingRule, &d.ConfigPrecisionDecimals,
				&d.ConfigCapDateAdj, &d.ConfigCalendar,
				// req
				&d.ReqDayCountCode, &d.ReqDivisor, &d.ReqAccrualDays,
				&d.ReqOpeningPrincipal, &d.ReqDailyRate,
				&d.ReqPeriodInterest, &d.ReqClosingBal,
				&d.ReqTDS, &d.ReqNetInterest,
				&d.ReqFormulaUsed, &d.ReqRoundingRule, &d.ReqPrecisionDecimals,
				&d.ReqWeekendAccrual, &d.ReqHolidayAccrual,
				// old
				&d.OldPeriodInterest, &d.OldClosingBal,
				&d.OldTDS, &d.OldNetInterest,
				&d.OldFormulaUsed, &d.OldRowStatus,
				&d.OldDayCountCode, &d.OldOpeningPrincipal,
				// override
				&d.IsOverridden, &d.OverrideAmount, &d.OverrideAdj,
				&d.OverrideReasonCode, &d.OverrideReasonText,
				&d.OverrideStatus, &d.OverrideProposedBy, &d.OverrideApprovedBy,
				&d.LedgerRowStatus, &d.OpeningAccruedBal,
			); sErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "scan error: "+sErr.Error())
				return
			}

			// ── Build variance_drivers ─────────────────────────────────────────
			var drivers []string
			dayDiff := d.ReqAccrualDays - d.AccrualDays
			if dayDiff != 0 {
				drivers = append(drivers, fmt.Sprintf("accrual_day_diff: req=%d config=%d delta=%d (holiday_excl=%d)",
					d.ReqAccrualDays, d.AccrualDays, dayDiff, d.ConfigHolidaysExcluded))
			}
			if d.ReqDivisor != d.Divisor {
				drivers = append(drivers, fmt.Sprintf("divisor_diff: req=%d config=%d (day_count_code: req=%s config=%s)",
					d.ReqDivisor, d.Divisor, d.ReqDayCountCode, d.DayCountCode))
			}
			if d.ReqRoundingRule != d.ConfigRoundingRule || d.ReqPrecisionDecimals != d.ConfigPrecisionDecimals {
				drivers = append(drivers, fmt.Sprintf("rounding_diff: req=%s(%d) config=%s(%d)",
					d.ReqRoundingRule, d.ReqPrecisionDecimals,
					d.ConfigRoundingRule, d.ConfigPrecisionDecimals))
			}
			if d.ReqOpeningPrincipal != d.OpeningPrincipal {
				drivers = append(drivers, fmt.Sprintf("principal_diff: req=%.2f config=%.2f (COMPOUND FD compounding)",
					d.ReqOpeningPrincipal, d.OpeningPrincipal))
			}
			if !d.ConfigHolidayAccrual {
				drivers = append(drivers, fmt.Sprintf("holiday_exclusion: %d days excluded by bank calendar",
					d.ConfigHolidaysExcluded))
			}
			if len(drivers) == 0 {
				drivers = []string{"no_variance: req and config calculations match"}
			}

			periodInterestVariance := math.Round((d.ReqPeriodInterest-d.PeriodInterest)*100) / 100
			var variancePct float64
			if d.ReqPeriodInterest != 0 {
				variancePct = math.Round((periodInterestVariance/d.ReqPeriodInterest)*10000) / 100
			}

			row := map[string]interface{}{
				"ledger_id": d.LedgerID,
				"run_id":    d.RunID,
				"fd_snapshot": map[string]interface{}{
					"fd_id":            d.FDID,
					"fd_ref_no":        d.FdRefNo,
					"bank_name":        d.BankName,
					"entity_id":        d.EntityID,
					"interest_type":    d.InterestTypeCode,
					"principal_amount": d.PrincipalAmount,
					"interest_rate":    d.InterestRate,
					"frequency_id":     d.FrequencyID,
					"compounding_freq": d.CompoundingFrequency,
					"fd_start_date":    d.FdStartDate.Format(constants.DateFormat),
					"fd_maturity_date": d.FdMaturityDate.Format(constants.DateFormat),
				},
				"period_inputs": map[string]interface{}{
					"accrual_period_start": d.PeriodStart.Format(constants.DateFormat),
					"accrual_period_end":   d.PeriodEnd.Format(constants.DateFormat),
					"opening_accrued_bal":  d.OpeningAccruedBal,
				},
				"req_calculation": map[string]interface{}{
					"description":        "Run-level request params (entity baseline — no holiday exclusion)",
					"day_count_code":     d.ReqDayCountCode,
					"divisor":            d.ReqDivisor,
					"accrual_days":       d.ReqAccrualDays,
					"weekend_accrual":    d.ReqWeekendAccrual,
					"holiday_accrual":    d.ReqHolidayAccrual,
					"opening_principal":  d.ReqOpeningPrincipal,
					"rate_per_day":       d.ReqDailyRate,
					"period_interest":    d.ReqPeriodInterest,
					"closing_balance":    d.ReqClosingBal,
					"tds_deducted":       d.ReqTDS,
					"net_interest":       d.ReqNetInterest,
					"rounding_rule":      d.ReqRoundingRule,
					"precision_decimals": d.ReqPrecisionDecimals,
					"formula":            d.ReqFormulaUsed,
				},
				"config_calculation": map[string]interface{}{
					"description":         "Per-FD bank config (authoritative — holiday/rounding/convention-aware)",
					"day_count_code":      d.DayCountCode,
					"divisor":             d.Divisor,
					"accrual_days":        d.AccrualDays,
					"weekend_accrual":     d.ConfigWeekendAccrual,
					"holiday_accrual":     d.ConfigHolidayAccrual,
					"holidays_excluded":   d.ConfigHolidaysExcluded,
					"cap_date_adjustment": d.ConfigCapDateAdj,
					"holiday_calendar":    d.ConfigCalendar,
					"opening_principal":   d.OpeningPrincipal, // for compounded FD, this is the compound principal
					"rate_per_day":        d.DailyRate,
					"period_interest":     d.PeriodInterest,
					"closing_balance":     d.ClosingBal,
					"tds_deducted":        d.TDS,
					"net_interest":        d.NetInterest,
					"rounding_rule":       d.ConfigRoundingRule,
					"precision_decimals":  d.ConfigPrecisionDecimals,
					"formula":             d.FormulaUsed,
				},
				"variance": map[string]interface{}{
					"day_count_variance":       dayDiff,
					"period_interest_variance": periodInterestVariance,
					"variance_pct":             variancePct,
					"variance_drivers":         drivers,
				},
				"override_info": map[string]interface{}{
					"is_overridden":        d.IsOverridden,
					"override_amount":      d.OverrideAmount,
					"override_adjustment":  d.OverrideAdj,
					"override_reason_code": d.OverrideReasonCode,
					"override_reason_text": d.OverrideReasonText,
					"override_status":      d.OverrideStatus,
					"override_proposed_by": d.OverrideProposedBy,
					"override_approved_by": d.OverrideApprovedBy,
				},
				"old_values": map[string]interface{}{
					"old_period_interest":   d.OldPeriodInterest,
					"old_closing_balance":   d.OldClosingBal,
					"old_tds_deducted":      d.OldTDS,
					"old_net_interest":      d.OldNetInterest,
					"old_formula_used":      d.OldFormulaUsed,
					"old_row_status":        d.OldRowStatus,
					"old_day_count_code":    d.OldDayCountCode,
					"old_opening_principal": d.OldOpeningPrincipal,
				},
				"ledger_row_status": d.LedgerRowStatus,
				"engine_version":    "v2.1",
			}
			ledgerRows = append(ledgerRows, row)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}
		if len(ledgerRows) == 0 {
			api.RespondWithPayload(w, false, "No ledger entry found", nil)
			return
		}

		result := map[string]interface{}{"count": len(ledgerRows)}
		if len(ledgerRows) == 1 {
			// Single row — flatten for convenience
			result["detail"] = ledgerRows[0]
		} else {
			result["rows"] = ledgerRows
		}
		api.RespondWithPayload(w, true, "", result)
	}
}

// ─── 6. SubmitForApproval ─────────────────────────────────────────────────────

func SubmitForApproval(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			RunID  string `json:"run_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDRequired)
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var runStatus, runMode, entityID string
		var totalNet float64
		err := pgxPool.QueryRow(ctx, `
			SELECT run_status, run_mode,
			       COALESCE(entity_id,''),
			       COALESCE(total_interest_accrued,0)
			FROM investment.fd_accrual_run WHERE run_id = $1`, req.RunID,
		).Scan(&runStatus, &runMode, &entityID, &totalNet)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Run not found: "+err.Error())
			return
		}
		// SIMULATION runs may be submitted for workflow visibility; they will NOT post to GL on approval.
		// FINAL runs follow the normal approve → post-journals path.
		if runMode == "SIMULATION" {
			api.LogInfo("[FDAccrual] SubmitForApproval: run=%s is SIMULATION — will skip GL posting on approval", req.RunID)
		}
		if runStatus != "COMPUTED" {
			api.RespondWithError(w, http.StatusBadRequest, "Only COMPUTED runs can be submitted (current: "+runStatus+")")
			return
		}

		_, err = pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_run
			SET run_status = 'PENDING_APPROVAL', submitted_by = $1, submitted_at = now()
			WHERE run_id = $2`, userEmail, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Status update failed: "+err.Error())
			return
		}

		go func(runID, uID, uEmail, eID string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDAccrual] SubmitForApproval engine goroutine panic for run %s: %v", runID, rec)
				}
			}()
			bgCtx := context.Background()
			instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       eID,
				TransactionType:  "FD_ACCRUAL_APPROVE",
				RecordID:         runID,
				RecordTable:      "investment.fd_accrual_run",
				AuditTable:       "investment.fd_accrual_run_audit",
				AuditIDColumn:    "run_id",
				ActionType:       "CREATE",
				Amount:           amount,
				SubmittedBy:      uID,
				SubmittedByEmail: uEmail,
			})
			if err != nil {
				api.LogError("[FDAccrual] CreateInstance failed for run %s: %v", runID, err)
				return
			}
			api.LogInfo("[FDAccrual] CreateInstance %s → run %s PENDING_APPROVAL", instID, runID)
		}(req.RunID, req.UserID, userEmail, entityID, totalNet)

		go func(runID, eID, uEmail string, amount float64) {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDAccrual] SubmitForApproval notification goroutine panic for run %s: %v", runID, rec)
				}
			}()
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/run/submit", runID, map[string]interface{}{
				"entity_id":   eID,
				"record_id":   runID,
				"event":       "FD_ACCRUAL_RUN_SUBMITTED",
				"actor_email": uEmail,
				"amount":      amount,
			})
		}(req.RunID, entityID, userEmail, totalNet)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id": req.RunID,
			"status": "PENDING_APPROVAL",
		})
		api.LogInfo("[FDAccrual] SubmitForApproval: run_id=%s by=%s", req.RunID, userEmail)
	}
}

// ─── 7. BulkApproveAccrualRun ─────────────────────────────────────────────────

func BulkApproveAccrualRun(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			RunIDs  []string `json:"run_ids"`
			RoleID  string   `json:"role_id"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.RunIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "run_ids are required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.RunIDs))

		for _, runID := range req.RunIDs {
			res := map[string]interface{}{"run_id": runID}

			var instanceEyeID string
			err := pgxPool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance_eye ie
				JOIN uam.approval_instance i ON i.instance_id = ie.instance_id
				WHERE i.record_id = $1
				  AND i.module_code = 'FIXED_DEPOSIT'
				  AND i.status = 'PENDING'
				  AND ie.status = 'ACTIVE'
				ORDER BY ie.position LIMIT 1`, runID,
			).Scan(&instanceEyeID)

			if err != nil || instanceEyeID == "" {
				// Capture old values for audit before direct approve
				var oldStatus, oldMode, oldDcc, oldRound, oldPeriod, oldFinPeriod, oldFdFilter, oldInclusion string
				var oldPrec int
				var oldPeriodStart, oldPeriodEnd interface{}
				_ = pgxPool.QueryRow(ctx, `
					SELECT run_status, run_mode, day_count_convention, rounding_rule, precision_decimals,
					       accrual_period_start, accrual_period_end, financial_period,
					       fd_status_filter, fd_inclusion_method
					FROM investment.fd_accrual_run WHERE run_id=$1`, runID,
				).Scan(&oldStatus, &oldMode, &oldDcc, &oldRound, &oldPrec,
					&oldPeriodStart, &oldPeriodEnd, &oldFinPeriod, &oldFdFilter, &oldInclusion)
				_ = oldPeriod
				_, _ = pgxPool.Exec(ctx, `
					INSERT INTO investment.fd_accrual_run_audit (
						run_id, action_type, processing_status, reason,
						requested_by, requested_at,
						checker_by, checker_at, checker_comment,
						old_run_status, old_run_mode, old_day_count_convention,
						old_rounding_rule, old_precision_decimals,
						old_accrual_period_start, old_accrual_period_end,
						old_financial_period, old_fd_status_filter, old_fd_inclusion_method
					) VALUES ($1,'EDIT','APPROVED',$2,$3,now(),$3,now(),$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
					runID, req.Comment, userEmail, req.Comment,
					oldStatus, oldMode, oldDcc, oldRound, oldPrec,
					oldPeriodStart, oldPeriodEnd, oldFinPeriod, oldFdFilter, oldInclusion)

				// Direct approve
				_, dbErr := pgxPool.Exec(ctx, `
					UPDATE investment.fd_accrual_run
					SET run_status = 'APPROVED', updated_at = now()
					WHERE run_id = $1 AND run_status = 'PENDING_APPROVAL'`,
					runID)
				if dbErr != nil {
					res[constants.ValueSuccess] = false
					res[constants.ValueError] = "Direct approve failed: " + dbErr.Error()
					results = append(results, res)
					continue
				}
				// GL guard: SIMULATION runs are approved in workflow but NOT posted to GL
				var approveRunMode string
				_ = pgxPool.QueryRow(ctx,
					`SELECT COALESCE(run_mode,'FINAL') FROM investment.fd_accrual_run WHERE run_id=$1`, runID,
				).Scan(&approveRunMode)
				if strings.EqualFold(approveRunMode, "SIMULATION") {
					api.LogInfo("[FDAccrual] BulkApprove: run=%s is SIMULATION — skipping GL posting", runID)
					res["journals_posted"] = false
					res["simulation_note"] = "SIMULATION run: no GL journal entries created"
				} else {
					if jErr := postAccrualJournals(ctx, pgxPool, runID, userEmail); jErr != nil {
						api.LogError("[FDAccrual] Journal posting failed for run %s: %v", runID, jErr)
						res["journal_error"] = jErr.Error()
					} else {
						res["journals_posted"] = true
						// Mark POSTED and lock period
						_, _ = pgxPool.Exec(ctx,
							`UPDATE investment.fd_accrual_run SET run_status='POSTED', posting_status='POSTED', posting_completed_at=now() WHERE run_id=$1`,
							runID)
						var periodStart time.Time
						_ = pgxPool.QueryRow(ctx,
							`SELECT accrual_period_start FROM investment.fd_accrual_run WHERE run_id=$1`, runID,
						).Scan(&periodStart)
						if !periodStart.IsZero() {
							_, _ = pgxPool.Exec(ctx, `
								INSERT INTO investment.fd_accrual_period_lock (entity_id, financial_period, run_id, locked_at, locked_by, is_locked)
								SELECT entity_id, $1, $2, now(), $3, true
								FROM investment.fd_accrual_run WHERE run_id = $2
								ON CONFLICT (entity_id, financial_period) DO UPDATE
								  SET run_id=EXCLUDED.run_id, locked_at=now(), locked_by=EXCLUDED.locked_by, is_locked=true`,
								buildAccrualPeriod(periodStart), runID, userEmail)
							res["period_locked"] = buildAccrualPeriod(periodStart)
						}
					}
				}
				res[constants.ValueSuccess] = true
				res["status"] = "POSTED"
				results = append(results, res)
				continue
			}

			err = approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
				InstanceEyeID: instanceEyeID,
				ActorUserID:   req.UserID,
				ActorEmail:    userEmail,
				ActorRoleID:   req.RoleID,
				ActionType:    approvalengine.ActionApproved,
				Comment:       req.Comment,
			})
			if err != nil {
				res[constants.ValueSuccess] = false
				res[constants.ValueError] = "Approval engine error: " + err.Error()
				results = append(results, res)
				continue
			}

			res[constants.ValueSuccess] = true

			var instStatus string
			_ = pgxPool.QueryRow(ctx, `
				SELECT status FROM uam.approval_instance
				WHERE record_id = $1 AND module_code = 'FIXED_DEPOSIT' AND status != 'CANCELLED'
				ORDER BY submitted_at DESC LIMIT 1`, runID,
			).Scan(&instStatus)

			if instStatus == approvalengine.InstStatusApproved {
				// GL guard: SIMULATION runs skip GL posting
				var approveRunMode2 string
				_ = pgxPool.QueryRow(ctx,
					`SELECT COALESCE(run_mode,'FINAL') FROM investment.fd_accrual_run WHERE run_id=$1`, runID,
				).Scan(&approveRunMode2)
				if strings.EqualFold(approveRunMode2, "SIMULATION") {
					api.LogInfo("[FDAccrual] BulkApprove (engine): run=%s is SIMULATION — skipping GL posting", runID)
					res["journals_posted"] = false
					res["simulation_note"] = "SIMULATION run: no GL journal entries created"
				} else {
					if jErr := postAccrualJournals(ctx, pgxPool, runID, userEmail); jErr != nil {
						api.LogError("[FDAccrual] Journal posting failed for run %s: %v", runID, jErr)
						res["journal_error"] = jErr.Error()
					} else {
						res["journals_posted"] = true
					}
					// Mark POSTED
					_, _ = pgxPool.Exec(ctx,
						`UPDATE investment.fd_accrual_run SET run_status='POSTED', posting_status='POSTED', posting_completed_at=now() WHERE run_id=$1`,
						runID)
				}
				// Lock the period
				var periodStart time.Time
				_ = pgxPool.QueryRow(ctx,
					`SELECT accrual_period_start FROM investment.fd_accrual_run WHERE run_id=$1`, runID,
				).Scan(&periodStart)
				if !periodStart.IsZero() {
					_, _ = pgxPool.Exec(ctx, `
						INSERT INTO investment.fd_accrual_period_lock (entity_id, financial_period, run_id, locked_at, locked_by, is_locked)
						SELECT entity_id, $1, $2, now(), $3, true
						FROM investment.fd_accrual_run WHERE run_id = $2
						ON CONFLICT (entity_id, financial_period) DO UPDATE
						  SET run_id=EXCLUDED.run_id, locked_at=now(), locked_by=EXCLUDED.locked_by, is_locked=true`,
						buildAccrualPeriod(periodStart), runID, userEmail)
					res["period_locked"] = buildAccrualPeriod(periodStart)
				}
				res["status"] = "POSTED"
			} else {
				res["status"] = "PENDING_APPROVAL"
			}

			results = append(results, res)
		}

		api.RespondWithPayload(w, true, "", results)
		for _, runID := range req.RunIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDAccrual] BulkApproveAccrualRun notification goroutine panic for run %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/run/bulk-approve", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_ACCRUAL_RUN_APPROVED",
					"actor_email": uEmail,
				})
			}(runID, userEmail)
		}
		api.LogInfo("[FDAccrual] BulkApproveAccrualRun: %d runs processed by %s", len(req.RunIDs), userEmail)
	}
}

// ─── 8. BulkRejectAccrualRun ──────────────────────────────────────────────────

func BulkRejectAccrualRun(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			RunIDs  []string `json:"run_ids"`
			RoleID  string   `json:"role_id"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.RunIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "run_ids are required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.RunIDs))

		for _, runID := range req.RunIDs {
			res := map[string]interface{}{"run_id": runID}

			var instanceEyeID string
			_ = pgxPool.QueryRow(ctx, `
				SELECT ie.instance_eye_id
				FROM uam.approval_instance_eye ie
				JOIN uam.approval_instance i ON i.instance_id = ie.instance_id
				WHERE i.record_id = $1
				  AND i.module_code = 'FIXED_DEPOSIT'
				  AND i.status = 'PENDING'
				  AND ie.status = 'ACTIVE'
				ORDER BY ie.position LIMIT 1`, runID,
			).Scan(&instanceEyeID)

			if instanceEyeID == "" {
				// Capture old values for audit before direct reject
				var oldStatus, oldMode, oldDcc, oldRound, oldFinPeriod, oldFdFilter, oldInclusion string
				var oldPrec int
				var oldPeriodStart, oldPeriodEnd interface{}
				_ = pgxPool.QueryRow(ctx, `
					SELECT run_status, run_mode, day_count_convention, rounding_rule, precision_decimals,
					       accrual_period_start, accrual_period_end, financial_period,
					       fd_status_filter, fd_inclusion_method
					FROM investment.fd_accrual_run WHERE run_id=$1`, runID,
				).Scan(&oldStatus, &oldMode, &oldDcc, &oldRound, &oldPrec,
					&oldPeriodStart, &oldPeriodEnd, &oldFinPeriod, &oldFdFilter, &oldInclusion)
				_, _ = pgxPool.Exec(ctx, `
					INSERT INTO investment.fd_accrual_run_audit (
						run_id, action_type, processing_status, reason,
						requested_by, requested_at,
						checker_by, checker_at, checker_comment,
						old_run_status, old_run_mode, old_day_count_convention,
						old_rounding_rule, old_precision_decimals,
						old_accrual_period_start, old_accrual_period_end,
						old_financial_period, old_fd_status_filter, old_fd_inclusion_method
					) VALUES ($1,'EDIT','REJECTED',$2,$3,now(),$3,now(),$2,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
					runID, req.Comment, userEmail,
					oldStatus, oldMode, oldDcc, oldRound, oldPrec,
					oldPeriodStart, oldPeriodEnd, oldFinPeriod, oldFdFilter, oldInclusion)

				_, _ = pgxPool.Exec(ctx,
					`UPDATE investment.fd_accrual_run SET run_status='REJECTED', updated_at=now()
					 WHERE run_id=$1 AND run_status='PENDING_APPROVAL'`, runID)
				res[constants.ValueSuccess] = true
				res["status"] = "REJECTED"
				results = append(results, res)
				continue
			}

			err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
				InstanceEyeID: instanceEyeID,
				ActorUserID:   req.UserID,
				ActorEmail:    userEmail,
				ActorRoleID:   req.RoleID,
				ActionType:    approvalengine.ActionRejected,
				Comment:       req.Comment,
			})
			if err != nil {
				res[constants.ValueSuccess] = false
				res[constants.ValueError] = "Rejection engine error: " + err.Error()
				results = append(results, res)
				continue
			}

			_, _ = pgxPool.Exec(ctx,
				`UPDATE investment.fd_accrual_run SET run_status='REJECTED', updated_at=now() WHERE run_id=$1`,
				runID)
			res[constants.ValueSuccess] = true
			res["status"] = "REJECTED"
			results = append(results, res)
		}

		api.RespondWithPayload(w, true, "", results)
		for _, runID := range req.RunIDs {
			go func(id, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDAccrual] BulkRejectAccrualRun notification goroutine panic for run %s: %v", id, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/run/bulk-reject", id, map[string]interface{}{
					"record_id":   id,
					"event":       "FD_ACCRUAL_RUN_REJECTED",
					"actor_email": uEmail,
				})
			}(runID, userEmail)
		}
		api.LogInfo("[FDAccrual] BulkRejectAccrualRun: %d runs by %s", len(req.RunIDs), userEmail)
	}
}

// ─── 9. GetAccrualRuns ────────────────────────────────────────────────────────

func GetAccrualRuns(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string `json:"user_id"`
			EntityID      string `json:"entity_id"`
			RunType       string `json:"run_type"`        // filter: MANUAL, SCHEDULED_MONTHLY, etc.
			RunStatus     string `json:"run_status"`      // optional: PENDING_APPROVAL | APPROVED | REJECTED | POSTED | ...
			ScheduleID    string `json:"schedule_id"`     // filter by config_id (gets all runs from that schedule)
			OnlyScheduled bool   `json:"only_scheduled"`  // if true, show only scheduled runs (any frequency)
			IncludeAll    bool   `json:"include_all"`     // if true, include DRAFT/COMPUTED/etc. (ops only)
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		query := `
			SELECT
				r.run_id,
				r.run_type,
				r.run_mode,
				r.run_status,
				COALESCE(r.entity_id,'')            AS entity_id,
				COALESCE(r.entity_name,'')          AS entity_name,
				r.accrual_period_start,
				r.accrual_period_end,
				COALESCE(r.financial_period,'')     AS financial_period,
				COALESCE(r.fds_in_scope,0)          AS fds_in_scope,
				COALESCE(r.fds_calculated,0)        AS fds_calculated,
				COALESCE(r.fds_failed,0)            AS fds_failed,
				COALESCE(r.total_interest_accrued,0) AS total_interest_accrued,
				COALESCE(r.total_tds_deducted,0)    AS total_tds_deducted,
				COALESCE(r.created_by,'')           AS created_by,
				r.created_at,
				COALESCE(r.submitted_by,'')         AS submitted_by,
				r.submitted_at,
				r.posting_completed_at,
				(SELECT COUNT(*) FROM investment.fd_accrual_ledger l
				 WHERE l.run_id = r.run_id AND COALESCE(l.is_deleted,false) = false) AS ledger_count
			FROM investment.fd_accrual_run r
			WHERE COALESCE(r.is_deleted, false) = false`
		args := []interface{}{}
		argIdx := 1

		if !req.IncludeAll {
			query += ` AND r.run_status = ANY($` + fmt.Sprint(argIdx) + `)`
			args = append(args, []string{
				"PENDING_APPROVAL", "APPROVED", "REJECTED",
				"POSTED", "POSTED_TO_GL", "LOCKED",
			})
			argIdx++
		}

		if req.RunStatus != "" {
			st := strings.ToUpper(strings.TrimSpace(req.RunStatus))
			if !req.IncludeAll && !isAccrualRunApprovalListStatus(st) {
				api.RespondWithError(w, http.StatusBadRequest,
					"run_status must be PENDING_APPROVAL, APPROVED, REJECTED, POSTED, POSTED_TO_GL, or LOCKED")
				return
			}
			query += fmt.Sprintf(" AND r.run_status = $%d", argIdx)
			args = append(args, st)
			argIdx++
		}

		if req.EntityID != "" {
			query += fmt.Sprintf(" AND r.entity_id = $%d", argIdx)
			args = append(args, req.EntityID)
			argIdx++
		}

		if req.RunType != "" {
			query += fmt.Sprintf(" AND r.run_type = $%d", argIdx)
			args = append(args, req.RunType)
			argIdx++
		}

		if req.OnlyScheduled {
			query += " AND r.created_by = 'SCHEDULER'"
		}

		if req.ScheduleID != "" {
			// Get all runs that match the schedule's entity
			query += fmt.Sprintf(` AND EXISTS (
				SELECT 1 FROM investment.fd_accrual_schedule_config sc
				WHERE sc.config_id = $%d
				  AND sc.entity_id = r.entity_id
				  AND r.created_by = 'SCHEDULER'
			)`, argIdx)
			args = append(args, req.ScheduleID)
			argIdx++
		}

		query += " ORDER BY r.created_at DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		runs := make([]map[string]interface{}, 0)
		var runIDs []string
		fields := rows.FieldDescriptions()
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			runs = append(runs, row)

			if v, ok := row["run_id"]; ok && v != "" {
				if castID, ok := v.(string); ok {
					runIDs = append(runIDs, castID)
				}
			}
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}

		// Pull audit trail events for runs
		runAuditTrails := map[string][]map[string]interface{}{}
		runLatestProcessingStatus := map[string]string{}
		runHistory := map[string]map[string]string{}

		if len(runIDs) > 0 {
			auditRows, err := pgxPool.Query(ctx, `
				SELECT
					run_id, action_type, processing_status,
					COALESCE(reason,'') as reason, COALESCE(requested_by,'') as requested_by,
					requested_at,
					COALESCE(checker_by,'') as checker_by, checker_at,
					COALESCE(checker_comment,'') as checker_comment
				FROM investment.fd_accrual_run_audit
				WHERE run_id = ANY($1)
				ORDER BY requested_at DESC`, runIDs)

			if err == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var rID, actionType, status, reason, reqBy, checkBy, checkComment string
					var reqAt time.Time
					var checkAt *time.Time

					_ = auditRows.Scan(&rID, &actionType, &status, &reason, &reqBy, &reqAt, &checkBy, &checkAt, &checkComment)
					event := map[string]interface{}{
						"action_type":       actionType,
						"processing_status": status,
						"reason":            reason,
						"requested_by":      reqBy,
						"requested_at":      reqAt,
						"checker_by":        checkBy,
						"checker_at":        checkAt,
						"checker_comment":   checkComment,
					}

					runAuditTrails[rID] = append(runAuditTrails[rID], event)
					// Since ordered by DESC, the first one encountered is the latest requested_at
					if _, ok := runLatestProcessingStatus[rID]; !ok {
						runLatestProcessingStatus[rID] = status
					}
					// Build history pivot
					if _, ok := runHistory[rID]; !ok {
						runHistory[rID] = map[string]string{}
					}
					h := runHistory[rID]
					reqAtStr := reqAt.Format("2006-01-02 15:04:05")
					switch actionType {
					case "CREATE":
						if h["created_by"] == "" {
							h["created_by"] = reqBy
						}
						if h["created_at"] == "" {
							h["created_at"] = reqAtStr
						}
					case "EDIT":
						if h["edited_by"] == "" {
							h["edited_by"] = reqBy
						}
						if h["edited_at"] == "" {
							h["edited_at"] = reqAtStr
						}
					case "DELETE":
						if h["deleted_by"] == "" {
							h["deleted_by"] = reqBy
						}
						if h["deleted_at"] == "" {
							h["deleted_at"] = reqAtStr
						}
					}
				}
			}
		}

		// Inject processing status and audit trail back into the main run entries
		for _, row := range runs {
			id, _ := row["run_id"].(string)

			// If missing audit trail, default processing_status to standard logic
			if latestStat, ok := runLatestProcessingStatus[id]; ok && latestStat != "" {
				row["processing_status"] = latestStat
			} else {
				// Default fallback logic
				if runStatus, _ := row["run_status"].(string); runStatus == "PENDING_APPROVAL" {
					row["processing_status"] = "PENDING_APPROVAL"
				} else if runStatus == "POSTED" || runStatus == "APPROVED" {
					row["processing_status"] = "APPROVED"
				} else {
					row["processing_status"] = "SYSTEM"
				}
			}

			if events, ok := runAuditTrails[id]; ok {
				row["audit_trail"] = events
			} else {
				row["audit_trail"] = []interface{}{}
			}

			// Inject history pivot (created/edited/deleted by/at)
			if h, ok := runHistory[id]; ok {
				row["created_by_audit"] = h["created_by"]
				row["created_at_audit"] = h["created_at"]
				row["edited_by"] = h["edited_by"]
				row["edited_at"] = h["edited_at"]
				row["deleted_by"] = h["deleted_by"]
				row["deleted_at"] = h["deleted_at"]
			} else {
				row["created_by_audit"] = ""
				row["created_at_audit"] = ""
				row["edited_by"] = ""
				row["edited_at"] = ""
				row["deleted_by"] = ""
				row["deleted_at"] = ""
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"count": len(runs),
			"runs":  runs,
			"note":  "Lists submitted approval-queue runs only (PENDING_APPROVAL, APPROVED, REJECTED, POSTED+). Use include_all:true for DRAFT/COMPUTED/etc.",
		})
	}
}

// ─── 10. GetValidationFindings ────────────────────────────────────────────────

func GetValidationFindings(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			RunID  string `json:"run_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDRequired)
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT finding_id, fd_id,
			       COALESCE(fd_ref_no,''), COALESCE(bank_name,''),
			       issue_type, severity, issue_description, suggested_action,
			       is_resolved, created_at
			FROM investment.fd_accrual_validation_finding
			WHERE run_id = $1
			ORDER BY severity DESC, fd_id`, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		findings := make([]map[string]interface{}, 0)
		fields := rows.FieldDescriptions()
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			findings = append(findings, row)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":   req.RunID,
			"count":    len(findings),
			"findings": findings,
		})
	}
}

// ─── 11. GetExecutionLog ──────────────────────────────────────────────────────

func GetExecutionLog(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			RunID      string `json:"run_id"`      // optional - filter by specific run
			ScheduleID string `json:"schedule_id"` // optional - filter by schedule config (gets all runs from that schedule)
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		ctx := r.Context()

		// Build query based on filters provided
		var query string
		var args []interface{}

		if req.RunID != "" {
			// Single run - original behavior
			query = `
				SELECT log_id, run_id,
				       COALESCE(fd_id,'') AS fd_id,
				       log_level, event_type, message,
				       COALESCE(detail::text,'{}') AS detail,
				       logged_at
				FROM investment.fd_accrual_run_execution_log
				WHERE run_id = $1
				ORDER BY logged_at DESC`
			args = append(args, req.RunID)
		} else if req.ScheduleID != "" {
			// All execution logs for scheduler runs tied to this schedule (entity match or detail.config_id)
			query = `
				SELECT el.log_id, el.run_id,
				       COALESCE(el.fd_id,'') AS fd_id,
				       el.log_level, el.event_type, el.message,
				       COALESCE(el.detail::text,'{}') AS detail,
				       el.logged_at
				FROM investment.fd_accrual_run_execution_log el
				JOIN investment.fd_accrual_run r ON r.run_id = el.run_id
				WHERE r.created_by = 'SCHEDULER'
				  AND EXISTS (
					SELECT 1 FROM investment.fd_accrual_schedule_config sc
					WHERE sc.config_id = $1
					  AND (
					    sc.entity_id = r.entity_id
					    OR COALESCE(el.detail->>'schedule_config_id','') = sc.config_id::text
					  )
				  )
				ORDER BY el.logged_at DESC`
			args = append(args, req.ScheduleID)
		} else {
			// No filter - dump ALL execution logs across all runs
			query = `
				SELECT log_id, run_id,
				       COALESCE(fd_id,'') AS fd_id,
				       log_level, event_type, message,
				       COALESCE(detail::text,'{}') AS detail,
				       logged_at
				FROM investment.fd_accrual_run_execution_log
				ORDER BY logged_at DESC`
		}

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		logs := make([]map[string]interface{}, 0)
		fields := rows.FieldDescriptions()
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			logs = append(logs, row)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id": req.RunID,
			"count":  len(logs),
			"logs":   logs,
		})
	}
}

// ─── STEP 8: GetAccrualLedgerAudit ───────────────────────────────────────────

func GetAccrualLedgerAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			RunID    string `json:"run_id"`
			FDID     string `json:"fd_id"`     // optional
			LedgerID string `json:"ledger_id"` // optional
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" && req.FDID == "" && req.LedgerID == "" {
			api.RespondWithError(w, http.StatusBadRequest,
				"at least one of run_id, fd_id, or ledger_id is required")
			return
		}

		ctx := r.Context()

		query := `
			SELECT
				a.audit_id, a.ledger_id, a.run_id, a.fd_id,
				COALESCE(a.fd_ref_no,'')   AS fd_ref_no,
				COALESCE(a.bank_name,'')   AS bank_name,
				COALESCE(a.entity_id,'')   AS entity_id,
				COALESCE(a.action_type,'') AS action_type,
				COALESCE(a.processing_status,'') AS processing_status,
				COALESCE(a.requested_by,'') AS requested_by,
				a.requested_at,
				COALESCE(a.checker_by,'')  AS checker_by,
				a.checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(a.principal_amount,0),
				COALESCE(a.interest_rate,0),
				COALESCE(a.accrual_days,0),
				COALESCE(a.old_period_interest_accrued,0),
				COALESCE(a.old_closing_accrued_balance,0),
				COALESCE(a.old_tds_deducted_in_period,0),
				COALESCE(a.old_net_interest_in_period,0),
				COALESCE(a.old_ledger_row_status,''),
				COALESCE(a.old_formula_used,''),
				COALESCE(a.old_is_overridden,false),
				COALESCE(a.old_override_amount,0),
				COALESCE(a.old_override_status,'')
			FROM investment.fd_accrual_ledger_audit a
			WHERE 1=1`

		args := []interface{}{}
		argIdx := 1

		if req.RunID != "" {
			query += fmt.Sprintf(" AND a.run_id = $%d", argIdx)
			args = append(args, req.RunID)
			argIdx++
		}
		if req.FDID != "" {
			query += fmt.Sprintf(" AND a.fd_id = $%d", argIdx)
			args = append(args, req.FDID)
			argIdx++
		}
		if req.LedgerID != "" {
			query += fmt.Sprintf(" AND a.ledger_id = $%d", argIdx)
			args = append(args, req.LedgerID)
			argIdx++
		}
		query += " ORDER BY a.requested_at DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError,
				friendlyAccrualError(err, "ledger audit query"))
			return
		}
		defer rows.Close()

		audit := make([]map[string]interface{}, 0)
		fields := rows.FieldDescriptions()
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			audit = append(audit, row)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError,
				friendlyAccrualError(err, "ledger audit rows"))
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"count": len(audit),
			"audit": audit,
		})
	}
}

// ─── STEP 9: GetAccrualExceptions ────────────────────────────────────────────

func GetAccrualExceptions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			RunID  string `json:"run_id"`
			FDID   string `json:"fd_id"` // optional
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDRequired)
			return
		}

		ctx := r.Context()
		query := `
			SELECT
				e.exception_id, e.run_id, e.ledger_id, e.fd_id,
				COALESCE(e.fd_ref_no,'')              AS fd_ref_no,
				COALESCE(e.exception_type,'')         AS exception_type,
				COALESCE(e.exception_description,'')  AS exception_description,
				COALESCE(e.computed_amount,0)         AS computed_amount,
				COALESCE(e.proposed_override_amount,0) AS proposed_override_amount,
				COALESCE(e.override_reason_code,'')   AS override_reason_code,
				COALESCE(e.override_reason_text,'')   AS override_reason_text,
				COALESCE(e.exception_status,'')       AS exception_status,
				COALESCE(e.proposed_by,'')            AS proposed_by,
				e.proposed_at,
				COALESCE(e.approved_by,'')            AS approved_by,
				e.approved_at,
				COALESCE(e.checker_comment,'')        AS checker_comment,
				e.ledger_updated, e.ledger_updated_at,
				COALESCE(e.ledger_update_error,'')    AS ledger_update_error,
				e.created_at
			FROM investment.fd_accrual_exception e
			WHERE e.run_id = $1
			  AND COALESCE(e.is_deleted,false) = false`

		args := []interface{}{req.RunID}
		if req.FDID != "" {
			query += " AND e.fd_id = $2"
			args = append(args, req.FDID)
		}
		query += " ORDER BY e.created_at DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError,
				friendlyAccrualError(err, "exceptions query"))
			return
		}
		defer rows.Close()

		exceptions := make([]map[string]interface{}, 0)
		fields := rows.FieldDescriptions()
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			exceptions = append(exceptions, row)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError,
				friendlyAccrualError(err, "exceptions rows"))
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":     req.RunID,
			"count":      len(exceptions),
			"exceptions": exceptions,
		})
	}
}

// ─── 12. ProposeOverride ──────────────────────────────────────────────────────
// Propose captures OLD ledger values into fd_accrual_ledger_audit, then
// immediately recalculates the ledger row using the override amount and stores
// the new figures as a PROPOSED state.  The run totals are recalculated so the
// checker sees the impact before approving.  Approve/Reject only flip the status.

func ProposeOverride(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID             string  `json:"user_id"`
			LedgerID           string  `json:"ledger_id"` // preferred target
			RunID              string  `json:"run_id"`    // legacy fallback
			FDID               string  `json:"fd_id"`     // legacy fallback
			OverrideAmount     float64 `json:"override_amount"`
			OverrideReasonCode string  `json:"override_reason_code"`
			OverrideReasonText string  `json:"override_reason_text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.LedgerID == "" && (req.RunID == "" || req.FDID == "") {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrLedgerIDRequired)
			return
		}
		if req.OverrideAmount <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "override_amount must be positive")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var whereClause string
		var whereArgs []interface{}
		if req.LedgerID != "" {
			whereClause = "ledger_id = $1"
			whereArgs = []interface{}{req.LedgerID}
		} else {
			whereClause = "run_id = $1 AND fd_id = $2"
			whereArgs = []interface{}{req.RunID, req.FDID}
		}

		// ── Step 1: Load current ledger row so we can capture old values ──────
		var ledgerID, outRunID, outFDID, fdRefNo, bankID, bankName, entityID string
		var oldPeriodInterest, oldClosingBal, oldTDS, oldNetInterest float64
		var oldOpeningBal, oldInterestReceived, oldTDSApplicable float64
		var oldRowStatus, oldFormula string
		var oldIsOverridden bool
		var oldOverrideAmt, oldOverrideAdj float64
		var oldOverrideCode, oldOverrideText, oldOverrideStatus string
		var principal, interestRate float64
		var accrualDays int
		if err := pgxPool.QueryRow(ctx, `
			SELECT
				ledger_id, run_id, fd_id,
				COALESCE(fd_ref_no,''), COALESCE(bank_id,''), COALESCE(bank_name,''), COALESCE(entity_id,''),
				COALESCE(principal_amount,0), COALESCE(interest_rate,0), COALESCE(accrual_days,0),
				COALESCE(period_interest_accrued,0),
				COALESCE(closing_accrued_balance,0),
				COALESCE(tds_deducted_in_period,0),
				COALESCE(net_interest_in_period,0),
				COALESCE(ledger_row_status,''),
				COALESCE(formula_used,''),
				COALESCE(is_overridden,false),
				COALESCE(override_amount,0),
				COALESCE(override_adjustment,0),
				COALESCE(override_reason_code,''),
				COALESCE(override_reason_text,''),
				COALESCE(override_status,''),
				COALESCE(opening_accrued_balance,0),
				COALESCE(interest_received_in_period,0),
				COALESCE(tds_applicable_amount,0)
			FROM investment.fd_accrual_ledger
			WHERE `+whereClause,
			whereArgs...,
		).Scan(
			&ledgerID, &outRunID, &outFDID,
			&fdRefNo, &bankID, &bankName, &entityID,
			&principal, &interestRate, &accrualDays,
			&oldPeriodInterest, &oldClosingBal, &oldTDS, &oldNetInterest,
			&oldRowStatus, &oldFormula,
			&oldIsOverridden, &oldOverrideAmt, &oldOverrideAdj,
			&oldOverrideCode, &oldOverrideText, &oldOverrideStatus,
			&oldOpeningBal, &oldInterestReceived, &oldTDSApplicable,
		); err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Ledger row not found: "+err.Error())
			return
		}

		// Fill req with standard keys for downstream run total calcs
		req.RunID = outRunID
		req.FDID = outFDID

		// ── Step 2: Write audit row with ALL old values before any mutation ────
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_ledger_audit (
				ledger_id, run_id, fd_id,
				fd_ref_no, bank_id, bank_name, entity_id,
				principal_amount, interest_rate, accrual_days,
				action_type, processing_status, requested_by, requested_at,
				old_period_interest_accrued, old_closing_accrued_balance,
				old_tds_deducted_in_period, old_net_interest_in_period,
				old_ledger_row_status, old_formula_used,
				old_is_overridden, old_override_amount, old_override_adjustment,
				old_override_reason_code, old_override_reason_text, old_override_status,
				old_opening_accrued_balance, old_interest_received_in_period, old_tds_applicable_amount
			) VALUES (
				$1,$2,$3,
				$4,$5,$6,$7,
				$8,$9,$10,
				'EDIT','PENDING_APPROVAL',$11,now(),
				$12,$13,$14,$15,$16,$17,
				$18,$19,$20,$21,$22,$23,
				$24,$25,$26
			)`,
			ledgerID, req.RunID, req.FDID,
			fdRefNo, bankID, bankName, entityID,
			principal, interestRate, accrualDays,
			userEmail,
			oldPeriodInterest, oldClosingBal, oldTDS, oldNetInterest, oldRowStatus, oldFormula,
			oldIsOverridden, oldOverrideAmt, oldOverrideAdj, oldOverrideCode, oldOverrideText, oldOverrideStatus,
			oldOpeningBal, oldInterestReceived, oldTDSApplicable)

		// ── Step 3: Recalculate ledger figures using the override amount ───────
		newPeriodInterest := math.Round(req.OverrideAmount*100) / 100
		newClosingBal := math.Round((oldOpeningBal+req.OverrideAmount-oldInterestReceived)*100) / 100
		newNetInterest := math.Round((req.OverrideAmount-oldTDS)*100) / 100
		overrideAdj := math.Round((req.OverrideAmount-oldPeriodInterest)*100) / 100

		// ── Step 4: Write proposed figures into ledger row (status = PROPOSED) ─
		var err error
		_, err = pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_ledger
			SET is_overridden              = true,
			    override_amount            = $1,
			    override_reason_code       = $2,
			    override_reason_text       = $3,
			    override_status            = 'PROPOSED',
			    override_proposed_by       = $4,
			    override_proposed_at       = now(),
			    period_interest_accrued    = $5,
			    closing_accrued_balance    = $6,
			    net_interest_in_period     = $7,
			    override_adjustment        = $8,
			    ledger_row_status          = 'OVERRIDDEN',
			    updated_at                 = now()
			WHERE ledger_id = $9`,
			req.OverrideAmount, nullIfEmpty(req.OverrideReasonCode),
			nullIfEmpty(req.OverrideReasonText),
			userEmail,
			newPeriodInterest, newClosingBal, newNetInterest, overrideAdj,
			ledgerID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Propose override failed: "+friendlyAccrualError(err, "override proposal"))
			return
		}

		// ── Step 5: Recompute run-level totals across all ledger rows ─────────
		_, _ = pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_run r
			SET total_interest_accrued       = sub.total_interest,
			    total_tds_deducted           = sub.total_tds,
			    total_accrued_closing_balance = sub.total_net,
			    run_status                   = 'COMPUTED',
			    updated_at                   = now()
			FROM (
				SELECT
					ROUND(SUM(period_interest_accrued)::numeric,2)  AS total_interest,
					ROUND(SUM(tds_deducted_in_period)::numeric,2)   AS total_tds,
					ROUND(SUM(net_interest_in_period)::numeric,2)   AS total_net
				FROM investment.fd_accrual_ledger
				WHERE run_id=$1 AND COALESCE(is_deleted,false)=false
			) sub
			WHERE r.run_id=$1`, req.RunID)

		// ── Step 6: Register / refresh exception row for checker visibility ───
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_exception (
				run_id, ledger_id, fd_id, fd_ref_no,
				exception_type, exception_description,
				computed_amount, proposed_override_amount,
				override_reason_code, override_reason_text,
				override_effective_period,
				exception_status, proposed_by, proposed_at
			)
			SELECT
				l.run_id, l.ledger_id, l.fd_id, l.fd_ref_no,
				'MANUAL_OVERRIDE',
				COALESCE($1, 'Manual override proposed'),
				$2, $3,
				$4, $1, $5,
				'OVERRIDE_PROPOSED', $6, now()
			FROM investment.fd_accrual_ledger l
			WHERE l.ledger_id = $7
			ON CONFLICT DO NOTHING`,
			nullIfEmpty(req.OverrideReasonText),
			oldPeriodInterest,
			req.OverrideAmount,
			nullIfEmpty(req.OverrideReasonCode),
			userEmail,
			ledgerID,
		)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":                  req.RunID,
			"fd_id":                   req.FDID,
			"override_status":         "PROPOSED",
			"period_interest_accrued": newPeriodInterest,
			"closing_accrued_balance": newClosingBal,
			"net_interest_in_period":  newNetInterest,
			"override_adjustment":     overrideAdj,
			"old_period_interest":     oldPeriodInterest,
		})
		api.LogInfo("[FDAccrual] ProposeOverride: run=%s fd=%s amount=%.2f old_interest=%.2f new_interest=%.2f by=%s",
			req.RunID, req.FDID, req.OverrideAmount, oldPeriodInterest, newPeriodInterest, userEmail)
	}
}

// ─── 13. ApproveOverride ──────────────────────────────────────────────────────
// Figures were already recalculated by ProposeOverride.  Approve only:
//   1. Enforces maker≠checker
//   2. Writes an audit row (captures the proposed/current figures as "old")
//   3. Flips override_status → APPROVED
//   4. Keeps run_status = COMPUTED (already set by ProposeOverride)

func ApproveOverride(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			LedgerID string `json:"ledger_id"` // preferred
			RunID    string `json:"run_id"`    // legacy fallback
			FDID     string `json:"fd_id"`     // legacy fallback
			Comment  string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.LedgerID == "" && (req.RunID == "" || req.FDID == "") {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrLedgerIDRequired)
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var whereClause string
		var whereArgs []interface{}
		if req.LedgerID != "" {
			whereClause = "l.ledger_id = $1"
			whereArgs = []interface{}{req.LedgerID}
		} else {
			whereClause = "l.run_id = $1 AND l.fd_id = $2"
			whereArgs = []interface{}{req.RunID, req.FDID}
		}

		// ── Maker ≠ checker ───────────────────────────────────────────────────
		var ledgerID, proposedBy string
		var curPeriodInterest, curClosingBal, curTDS, curNetInterest float64
		var curOpeningBal, curInterestReceived, curTDSApplicable float64
		var curRowStatus, curFormula, fdRefNo, bankID, bankName, entityID string
		var curIsOverridden bool
		var curOverrideAmt, curOverrideAdj float64
		var curOverrideCode, curOverrideText, curOverrideStatus string
		var principal, interestRate float64
		var accrualDays int

		if err := pgxPool.QueryRow(ctx, `
			SELECT
				l.ledger_id,
				COALESCE(l.override_proposed_by,''),
				COALESCE(l.fd_ref_no,''), COALESCE(l.bank_id,''), COALESCE(l.bank_name,''), COALESCE(l.entity_id,''),
				COALESCE(l.principal_amount,0), COALESCE(l.interest_rate,0), COALESCE(l.accrual_days,0),
				COALESCE(l.period_interest_accrued,0), COALESCE(l.closing_accrued_balance,0),
				COALESCE(l.tds_deducted_in_period,0), COALESCE(l.net_interest_in_period,0),
				COALESCE(l.ledger_row_status,''), COALESCE(l.formula_used,''),
				COALESCE(l.is_overridden,false), COALESCE(l.override_amount,0), COALESCE(l.override_adjustment,0),
				COALESCE(l.override_reason_code,''), COALESCE(l.override_reason_text,''), COALESCE(l.override_status,''),
				COALESCE(l.opening_accrued_balance,0), COALESCE(l.interest_received_in_period,0), COALESCE(l.tds_applicable_amount,0)
			FROM investment.fd_accrual_ledger l
			WHERE `+whereClause+` AND COALESCE(l.override_status,'') = 'PROPOSED'
			  AND COALESCE(l.is_deleted,false) = false`,
			whereArgs...,
		).Scan(
			&ledgerID, &proposedBy,
			&fdRefNo, &bankID, &bankName, &entityID,
			&principal, &interestRate, &accrualDays,
			&curPeriodInterest, &curClosingBal, &curTDS, &curNetInterest,
			&curRowStatus, &curFormula,
			&curIsOverridden, &curOverrideAmt, &curOverrideAdj,
			&curOverrideCode, &curOverrideText, &curOverrideStatus,
			&curOpeningBal, &curInterestReceived, &curTDSApplicable,
		); err != nil {
			api.RespondWithError(w, http.StatusNotFound,
				fmt.Sprintf("No PROPOSED override found: %v", err))
			return
		}

		if proposedBy == userEmail {
			api.RespondWithError(w, http.StatusForbidden, "Maker cannot approve their own override")
			return
		}

		// ── Write audit row (proposed figures become the "old" snapshot) ──────
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_ledger_audit (
				ledger_id, run_id, fd_id,
				fd_ref_no, bank_id, bank_name, entity_id,
				principal_amount, interest_rate, accrual_days,
				action_type, processing_status, requested_by, requested_at,
				checker_by, checker_at, checker_comment,
				old_period_interest_accrued, old_closing_accrued_balance,
				old_tds_deducted_in_period, old_net_interest_in_period,
				old_ledger_row_status, old_formula_used,
				old_is_overridden, old_override_amount, old_override_adjustment,
				old_override_reason_code, old_override_reason_text, old_override_status,
				old_opening_accrued_balance, old_interest_received_in_period, old_tds_applicable_amount
			) VALUES (
				$1,$2,$3,
				$4,$5,$6,$7,
				$8,$9,$10,
				'OVERRIDE_APPROVE','APPROVED',$11,now(),
				$12,now(),$13,
				$14,$15,$16,$17,$18,$19,
				$20,$21,$22,$23,$24,$25,
				$26,$27,$28
			)`,
			ledgerID, req.RunID, req.FDID,
			fdRefNo, bankID, bankName, entityID,
			principal, interestRate, accrualDays,
			proposedBy,
			userEmail, nullIfEmpty(req.Comment),
			curPeriodInterest, curClosingBal, curTDS, curNetInterest, curRowStatus, curFormula,
			curIsOverridden, curOverrideAmt, curOverrideAdj, curOverrideCode, curOverrideText, curOverrideStatus,
			curOpeningBal, curInterestReceived, curTDSApplicable)

		// ── Flip status → APPROVED (figures already in ledger from ProposeOverride) ──
		_, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_ledger
			SET override_status          = 'APPROVED',
			    override_approved_by     = $1,
			    override_approved_at     = now(),
			    override_checker_comment = $2,
			    updated_at               = now()
			WHERE ledger_id = $3 AND override_status = 'PROPOSED'`,
			userEmail, nullIfEmpty(req.Comment), ledgerID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Approve override failed: "+friendlyAccrualError(err, "override approval"))
			return
		}

		// ── Sync exception table status ───────────────────────────────────────
		_, _ = pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_exception
			SET exception_status  = 'APPROVED',
			    approved_by       = $1,
			    approved_at       = now(),
			    checker_comment   = $2,
			    ledger_updated    = true,
			    ledger_updated_at = now()
			WHERE ledger_id = $3 AND exception_status = 'OVERRIDE_PROPOSED'`,
			userEmail, nullIfEmpty(req.Comment), ledgerID)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":                  req.RunID,
			"fd_id":                   req.FDID,
			"override_status":         "APPROVED",
			"period_interest_accrued": curPeriodInterest,
			"closing_accrued_balance": curClosingBal,
			"net_interest_in_period":  curNetInterest,
			"override_adjustment":     curOverrideAdj,
		})
		api.LogInfo("[FDAccrual] ApproveOverride: run=%s fd=%s by=%s (proposed by %s) override_amt=%.2f",
			req.RunID, req.FDID, userEmail, proposedBy, curOverrideAmt)
	}
}

// ─── 14. RejectOverride ───────────────────────────────────────────────────────
// Reject:
//   1. Captures the current (proposed) ledger figures into audit
//   2. Restores the original pre-propose figures from the OVERRIDE_PROPOSE audit row
//   3. Flips override_status → REJECTED and clears override fields
//   4. Recomputes run totals after the rollback

func RejectOverride(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string `json:"user_id"`
			RunID   string `json:"run_id"`
			FDID    string `json:"fd_id"`
			Comment string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" || req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDAndFDIDRequired)
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// ── Step 1: Load current (proposed) state for audit snapshot ─────────
		var ledgerID, fdRefNo, bankID, bankName, entityID string
		var principal, interestRate float64
		var accrualDays int
		var curPeriodInterest, curClosingBal, curTDS, curNetInterest float64
		var curOpeningBal, curInterestReceived, curTDSApplicable float64
		var curRowStatus, curFormula string
		var curIsOverridden bool
		var curOverrideAmt, curOverrideAdj float64
		var curOverrideCode, curOverrideText, curOverrideStatus string
		var proposedBy string
		if err := pgxPool.QueryRow(ctx, `
			SELECT
				ledger_id,
				COALESCE(fd_ref_no,''), COALESCE(bank_id,''), COALESCE(bank_name,''), COALESCE(entity_id,''),
				COALESCE(principal_amount,0), COALESCE(interest_rate,0), COALESCE(accrual_days,0),
				COALESCE(period_interest_accrued,0), COALESCE(closing_accrued_balance,0),
				COALESCE(tds_deducted_in_period,0), COALESCE(net_interest_in_period,0),
				COALESCE(ledger_row_status,''), COALESCE(formula_used,''),
				COALESCE(is_overridden,false),
				COALESCE(override_amount,0), COALESCE(override_adjustment,0),
				COALESCE(override_reason_code,''), COALESCE(override_reason_text,''), COALESCE(override_status,''),
				COALESCE(opening_accrued_balance,0), COALESCE(interest_received_in_period,0),
				COALESCE(tds_applicable_amount,0),
				COALESCE(override_proposed_by,'')
			FROM investment.fd_accrual_ledger
			WHERE run_id=$1 AND fd_id=$2 AND override_status='PROPOSED'`,
			req.RunID, req.FDID,
		).Scan(
			&ledgerID,
			&fdRefNo, &bankID, &bankName, &entityID,
			&principal, &interestRate, &accrualDays,
			&curPeriodInterest, &curClosingBal, &curTDS, &curNetInterest,
			&curRowStatus, &curFormula,
			&curIsOverridden, &curOverrideAmt, &curOverrideAdj,
			&curOverrideCode, &curOverrideText, &curOverrideStatus,
			&curOpeningBal, &curInterestReceived, &curTDSApplicable,
			&proposedBy,
		); err != nil {
			api.RespondWithError(w, http.StatusNotFound, "No PROPOSED override found for this run/fd")
			return
		}

		// ── Step 2: Write audit row for this rejection ────────────────────────
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_ledger_audit (
				ledger_id, run_id, fd_id,
				fd_ref_no, bank_id, bank_name, entity_id,
				principal_amount, interest_rate, accrual_days,
				action_type, processing_status, requested_by, requested_at,
				checker_by, checker_at, checker_comment,
				old_period_interest_accrued, old_closing_accrued_balance,
				old_tds_deducted_in_period, old_net_interest_in_period,
				old_ledger_row_status, old_formula_used,
				old_is_overridden, old_override_amount, old_override_adjustment,
				old_override_reason_code, old_override_reason_text, old_override_status,
				old_opening_accrued_balance, old_interest_received_in_period, old_tds_applicable_amount
			) VALUES (
				$1,$2,$3,
				$4,$5,$6,$7,
				$8,$9,$10,
				'OVERRIDE_REJECT','REJECTED',$11,now(),
				$12,now(),$13,
				$14,$15,$16,$17,$18,$19,
				$20,$21,$22,$23,$24,$25,
				$26,$27,$28
			)`,
			ledgerID, req.RunID, req.FDID,
			fdRefNo, bankID, bankName, entityID,
			principal, interestRate, accrualDays,
			proposedBy,
			userEmail, nullIfEmpty(req.Comment),
			curPeriodInterest, curClosingBal, curTDS, curNetInterest, curRowStatus, curFormula,
			curIsOverridden, curOverrideAmt, curOverrideAdj, curOverrideCode, curOverrideText, curOverrideStatus,
			curOpeningBal, curInterestReceived, curTDSApplicable)

		// ── Step 3: Restore original pre-propose figures from audit ──────────
		// The OVERRIDE_PROPOSE audit row stores the original values as old_*.
		var origPeriodInterest, origClosingBal, origTDS, origNetInterest float64
		var origFormula, origRowStatus string
		_ = pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(old_period_interest_accrued,0),
				COALESCE(old_closing_accrued_balance,0),
				COALESCE(old_tds_deducted_in_period,0),
				COALESCE(old_net_interest_in_period,0),
				COALESCE(old_formula_used,''),
				COALESCE(old_ledger_row_status,'CALCULATED')
			FROM investment.fd_accrual_ledger_audit
			WHERE ledger_id=$1 AND action_type='OVERRIDE_PROPOSE'
			ORDER BY requested_at DESC LIMIT 1`, ledgerID,
		).Scan(&origPeriodInterest, &origClosingBal, &origTDS, &origNetInterest, &origFormula, &origRowStatus)
		// If no prior audit exists (shouldn't happen), keep current values as fallback.
		if origRowStatus == "" {
			origRowStatus = "CALCULATED"
		}

		// ── Step 4: Revert ledger row to original figures + mark REJECTED ─────
		_, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_ledger
			SET override_status            = 'REJECTED',
			    override_rejected_by       = $1,
			    override_rejected_at       = now(),
			    override_checker_comment   = $2,
			    is_overridden              = false,
			    override_amount            = 0,
			    override_adjustment        = 0,
			    period_interest_accrued    = $3,
			    closing_accrued_balance    = $4,
			    tds_deducted_in_period     = $5,
			    net_interest_in_period     = $6,
			    ledger_row_status          = $7,
			    formula_used               = $8,
			    updated_at                 = now()
			WHERE run_id = $9 AND fd_id = $10 AND override_status = 'PROPOSED'`,
			userEmail, nullIfEmpty(req.Comment),
			origPeriodInterest, origClosingBal, origTDS, origNetInterest,
			origRowStatus, nullIfEmpty(origFormula),
			req.RunID, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Reject override failed: "+err.Error())
			return
		}

		// ── Step 5: Recompute run totals after rollback ───────────────────────
		_, _ = pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_run r
			SET total_interest_accrued        = sub.total_interest,
			    total_tds_deducted            = sub.total_tds,
			    total_accrued_closing_balance = sub.total_net,
			    run_status                    = 'COMPUTED',
			    updated_at                    = now()
			FROM (
				SELECT
					ROUND(SUM(period_interest_accrued)::numeric,2) AS total_interest,
					ROUND(SUM(tds_deducted_in_period)::numeric,2)  AS total_tds,
					ROUND(SUM(net_interest_in_period)::numeric,2)  AS total_net
				FROM investment.fd_accrual_ledger
				WHERE run_id=$1 AND COALESCE(is_deleted,false)=false
			) sub
			WHERE r.run_id=$1`, req.RunID)

		// ── Step 6: Sync exception table ─────────────────────────────────────
		_, _ = pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_exception
			SET exception_status = 'REJECTED',
			    approved_by      = $1,
			    approved_at      = now(),
			    checker_comment  = $2
			WHERE run_id = $3 AND fd_id = $4 AND exception_status = 'OVERRIDE_PROPOSED'`,
			userEmail, nullIfEmpty(req.Comment), req.RunID, req.FDID)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":                   req.RunID,
			"fd_id":                    req.FDID,
			"override_status":          "REJECTED",
			"restored_period_interest": origPeriodInterest,
			"restored_closing_balance": origClosingBal,
		})
		api.LogInfo("[FDAccrual] RejectOverride: run=%s fd=%s by=%s (proposed by %s) restored_interest=%.2f",
			req.RunID, req.FDID, userEmail, proposedBy, origPeriodInterest)
	}
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

// loadRunParams fetches the run row and builds AccrualRunParams.
func loadRunParams(ctx context.Context, pool *pgxpool.Pool, runID string) (AccrualRunParams, error) {
	var p AccrualRunParams
	var inclusionMethod, inclusionListJSON, bankFilter, fdStatusFilter, dayCountConv, roundingRule, granularity string
	var precisionDecimals int
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(entity_id,''),
			accrual_period_start,
			accrual_period_end,
			COALESCE(financial_period,''),
			COALESCE(day_count_convention,'ACT_365'),
			COALESCE(rounding_rule,'ROUND'),
			COALESCE(precision_decimals,2),
			COALESCE(bank_id_filter,''),
			COALESCE(fd_status_filter,'ACTIVE'),
			COALESCE(fd_inclusion_method,'ALL'),
			COALESCE(fd_inclusion_list,'[]'),
			COALESCE(accrual_granularity,'RUN')
		FROM investment.fd_accrual_run WHERE run_id = $1`, runID,
	).Scan(
		&p.EntityID,
		&p.PeriodStart, &p.PeriodEnd, &p.FinancialPeriod,
		&dayCountConv, &roundingRule, &precisionDecimals,
		&bankFilter, &fdStatusFilter, &inclusionMethod, &inclusionListJSON,
		&granularity,
	)
	if err != nil {
		return p, fmt.Errorf("loadRunParams: %w", err)
	}
	p.DayCountConvention = dayCountConv
	p.RoundingRule = roundingRule
	p.PrecisionDecimals = precisionDecimals
	p.BankIDFilter = bankFilter
	p.FDStatusFilter = fdStatusFilter
	p.FDInclusionMethod = inclusionMethod
	p.Granularity = granularity
	if inclusionListJSON != "" && inclusionListJSON != "[]" {
		_ = json.Unmarshal([]byte(inclusionListJSON), &p.FDInclusionList)
	}
	return p, nil
}

// createAccrualRunInternal inserts an fd_accrual_run row and returns the run_id.
func createAccrualRunInternal(ctx context.Context, pool *pgxpool.Pool, input CreateAccrualRunInput) (string, error) {
	fdStatus := input.FDStatusFilter
	if fdStatus == "" {
		fdStatus = "ACTIVE"
	}
	rounding := input.RoundingRule
	if rounding == "" {
		rounding = "ROUND"
	}
	precision := input.PrecisionDecimals
	if precision <= 0 {
		precision = 2
	}
	dayCount := input.DayCountConvention
	if dayCount == "" {
		dayCount = "ACT_365"
	}
	fdInclusion := input.FDInclusionMethod
	if fdInclusion == "" {
		fdInclusion = "ALL"
	}
	granularity := normalizeAccrualGranularity(input.Granularity)
	if granularity == "" {
		granularity = "RUN"
	}
	if !isValidAccrualGranularity(granularity) {
		return "", fmt.Errorf("invalid accrual_granularity %q", input.Granularity)
	}
	runType := normalizeAccrualGranularity(input.RunType)
	if runType == "" {
		runType = RunTypeForGranularity(granularity)
	}

	// Look up entity_name if not provided (fd_accrual_run.entity_name is NOT NULL)
	entityName := input.EntityName
	if entityName == "" && input.EntityID != "" {
		_ = pool.QueryRow(ctx,
			`SELECT COALESCE(entity_name, entity_id) FROM masterentitycash WHERE entity_id=$1 LIMIT 1`,
			input.EntityID,
		).Scan(&entityName)
		if entityName == "" {
			entityName = input.EntityID // fallback to entity_id
		}
	}

	var runID string
	err := pool.QueryRow(ctx, `
		INSERT INTO investment.fd_accrual_run (
			run_type, run_mode, run_status,
			entity_id, entity_name,
			bank_id_filter, fd_status_filter,
			accrual_period_start, accrual_period_end, financial_period,
			day_count_convention, rounding_rule, precision_decimals,
			fd_inclusion_method,
			accrual_granularity,
			engine_version,
			is_active, is_deleted, created_by, created_at
		) VALUES (
			$1, $2, 'DRAFT',
			$3, $4,
			$5, $6,
			$7, $8, $9,
			$10, $11, $12,
			$14,
			$15,
			'2.0',
			true, false, $13, now()
		) RETURNING run_id`,
		runType, input.RunMode,
		input.EntityID, entityName,
		nullIfEmpty(input.BankIDFilter), fdStatus,
		input.AccrualPeriodStart, input.AccrualPeriodEnd, input.FinancialPeriod,
		dayCount, rounding, precision,
		input.CreatedBy, fdInclusion, granularity,
	).Scan(&runID)
	return runID, err
}

// validateAndPersistFindings runs scope+validation and saves findings to DB.
func validateAndPersistFindings(ctx context.Context, pool *pgxpool.Pool, runID string) (eligibleCount int, blockerCount int, findings []map[string]interface{}, err error) {
	params, err := loadRunParams(ctx, pool, runID)
	if err != nil {
		return 0, 0, nil, err
	}

	fds, err := getFDsInScope(ctx, pool, params)
	if err != nil {
		return 0, 0, nil, err
	}

	if len(fds) == 0 {
		_, _ = pool.Exec(ctx,
			`DELETE FROM investment.fd_accrual_validation_finding WHERE run_id=$1`, runID)
		_, _ = pool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_validation_finding (
				run_id, fd_id, issue_type, severity,
				issue_description, suggested_action,
				is_resolved, created_at
			) VALUES ($1, NULL, 'NO_FDS_IN_SCOPE', 'BLOCKER', $2,
				'Check: (1) fd_status=ACTIVE with cashflow_generated=true, '+
				'(2) FD start_date <= period_end, '+
				'(3) FD maturity_date >= period_start, '+
				'(4) bank_id_filter must exactly match bank_id or bank_name.',
				false, now())`,
			runID,
			fmt.Sprintf(
				"No FDs found for entity=%s fd_status_filter=%s period=%s to %s bank_filter=%q. "+
					"Verify at least one FD is ACTIVE with cashflow_generated=true.",
				params.EntityID, params.FDStatusFilter,
				params.PeriodStart.Format(constants.DateFormat),
				params.PeriodEnd.Format(constants.DateFormat),
				params.BankIDFilter,
			),
		)
		_, _ = pool.Exec(ctx,
			`UPDATE investment.fd_accrual_run
			 SET run_status='VALIDATION_FAILED', fds_in_scope=0
			 WHERE run_id=$1`, runID)
		// return a structured finding so caller/UI can display details
		f := map[string]interface{}{
			"finding_id": fmt.Sprintf("%s-F%03d", runID, 1),
			"run_id":     runID,
			"fd_id":      "",
			"fd_ref_no":  "",
			"bank_name":  "",
			"issue_type": "NO_FDS_IN_SCOPE",
			"severity":   "BLOCKER",
			"issue_description": fmt.Sprintf(
				"No FDs found for entity=%s fd_status_filter=%s period=%s to %s bank_filter=%q",
				params.EntityID, params.FDStatusFilter,
				params.PeriodStart.Format(constants.DateFormat), params.PeriodEnd.Format(constants.DateFormat), params.BankIDFilter,
			),
			"suggested_action": "Verify FD scope and input filters",
			"detail":           map[string]interface{}{},
			"created_by":       "auto-validator",
			"created_at":       time.Now().UTC().Format(time.RFC3339),
		}
		return 0, 1, []map[string]interface{}{f}, nil
	}

	vf := validateFDsForAccrual(fds, params)

	// Delete existing findings for this run
	_, _ = pool.Exec(ctx, `DELETE FROM investment.fd_accrual_validation_finding WHERE run_id=$1`, runID)

	resultFindings := make([]map[string]interface{}, 0, len(vf))
	for i, f := range vf {
		// persist as before (legacy table fields)
		_, _ = pool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_validation_finding (
				run_id, fd_id, fd_ref_no, bank_name,
				issue_type, severity, issue_description, suggested_action,
				is_resolved, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false,now())`,
			runID, f.FDID, nullIfEmpty(f.FdRefNo), nullIfEmpty(f.BankName),
			f.IssueType, f.Severity, f.Description, nullIfEmpty(f.SuggestedAction))

		if f.Severity == "BLOCKER" {
			blockerCount++
		}

		fid := fmt.Sprintf("%s-F%03d", runID, i+1)
		rf := map[string]interface{}{
			"finding_id":        fid,
			"run_id":            runID,
			"fd_id":             f.FDID,
			"fd_ref_no":         f.FdRefNo,
			"bank_name":         f.BankName,
			"issue_type":        f.IssueType,
			"severity":          f.Severity,
			"issue_description": f.Description,
			"suggested_action":  f.SuggestedAction,
			"error_code":        f.IssueType,
			"location":          "validateFDsForAccrual",
			"detail":            map[string]interface{}{},
			"created_by":        "auto-validator",
			"created_at":        time.Now().UTC().Format(time.RFC3339),
			"resolved":          false,
		}
		resultFindings = append(resultFindings, rf)
	}

	return len(fds), blockerCount, resultFindings, nil
}

// executeAccrualRun performs the accrual calculation for all FDs in scope.
func executeAccrualRun(ctx context.Context, pool *pgxpool.Pool, runID string, executedBy string) (int, int, error) {
	params, err := loadRunParams(ctx, pool, runID)
	if err != nil {
		return 0, 0, err
	}

	// STEP 2: read run_mode to decide whether to write ledger rows
	var runMode string
	_ = pool.QueryRow(ctx,
		`SELECT COALESCE(run_mode,'FINAL') FROM investment.fd_accrual_run WHERE run_id=$1`,
		runID,
	).Scan(&runMode)
	isSimulation := strings.EqualFold(runMode, "SIMULATION")

	fds, err := getFDsInScope(ctx, pool, params)
	if err != nil {
		return 0, 0, err
	}

	if len(fds) == 0 {
		_, _ = pool.Exec(ctx,
			`UPDATE investment.fd_accrual_run
			 SET run_status='FAILED', fds_in_scope=0, fds_calculated=0,
			     completed_at=now(), updated_at=now()
			 WHERE run_id=$1`, runID)
		logAccrualEvent(ctx, pool, LogAccrualParams{
			RunID:     runID,
			FDID:      "",
			Level:     "ERROR",
			EventType: "SCOPE_EMPTY",
			Message: fmt.Sprintf(
				"Execute aborted: 0 FDs in scope. entity=%s fd_status_filter=%s period=%s to %s. "+
					"FDs may have changed status since validation.",
				params.EntityID, params.FDStatusFilter,
				params.PeriodStart.Format(constants.DateFormat),
				params.PeriodEnd.Format(constants.DateFormat),
			),
			Detail: nil,
		})
		return 0, 0, fmt.Errorf(
			"accrual execute failed: 0 FDs in scope for entity %s "+
				"(fd_status_filter=%s, period %s to %s). "+
				"Ensure FDs are ACTIVE with cashflow_generated=true and overlap the accrual period",
			params.EntityID, params.FDStatusFilter,
			params.PeriodStart.Format(constants.DateFormat),
			params.PeriodEnd.Format(constants.DateFormat),
		)
	}

	// Mark run as IN_PROGRESS
	_, _ = pool.Exec(ctx,
		`UPDATE investment.fd_accrual_run SET run_status='IN_PROGRESS', started_at=now(), fds_in_scope=$1 WHERE run_id=$2`,
		len(fds), runID)

	calculated := 0
	failed := 0
	var totalInterest, totalTDS, totalNet float64

	granularity := params.Granularity
	if granularity == "" {
		granularity = "RUN"
	}

	for _, fd := range fds {
		// ── Effective window for this FD (apply boundary conventions) ──────────
		fdEffStart := params.PeriodStart
		fdEffEnd := params.PeriodEnd

		// AccrualStartConvention: NEXT_DAY_AFTER_ISSUE means accrual starts on issue_date+1
		if strings.EqualFold(fd.AccrualStartConvention, "NEXT_DAY_AFTER_ISSUE") {
			if !fd.FdStartDate.IsZero() && fd.FdStartDate.After(fdEffStart) {
				fdEffStart = fd.FdStartDate
			} else if !fd.FdStartDate.IsZero() {
				// start is before period start; advance fdEffStart by 1 if fd starts exactly on period start
				if fd.FdStartDate.Equal(fdEffStart) {
					fdEffStart = fdEffStart.AddDate(0, 0, 1)
				}
			}
		} else {
			// DEFAULT: clip to fd_start_date
			if !fd.FdStartDate.IsZero() && fd.FdStartDate.After(fdEffStart) {
				fdEffStart = fd.FdStartDate
			}
		}

		// AccrualEndConvention: EXCLUDE_MATURITY_DATE means last day is maturity-1
		if strings.EqualFold(fd.AccrualEndConvention, "EXCLUDE_MATURITY_DATE") {
			if !fd.FdMaturityDate.IsZero() && fd.FdMaturityDate.Before(fdEffEnd) {
				fdEffEnd = fd.FdMaturityDate
			} else if !fd.FdMaturityDate.IsZero() && fd.FdMaturityDate.Equal(fdEffEnd) {
				fdEffEnd = fdEffEnd.AddDate(0, 0, -1)
			}
		} else {
			// DEFAULT: clip to fd_maturity_date
			if !fd.FdMaturityDate.IsZero() && fd.FdMaturityDate.Before(fdEffEnd) {
				fdEffEnd = fd.FdMaturityDate
			}
		}

		if !fdEffStart.Before(fdEffEnd) {
			logAccrualEvent(ctx, pool, LogAccrualParams{
				RunID:     runID,
				FDID:      fd.FDID,
				Level:     "INFO",
				EventType: "EXCLUDED",
				Message: fmt.Sprintf("FD effective window empty after boundary conventions: effStart=%s effEnd=%s",
					fdEffStart.Format(constants.DateFormat), fdEffEnd.Format(constants.DateFormat)),
				Detail: nil,
			})
			continue
		}

		// ── Generate sub-periods ───────────────────────────────────────────────
		subPeriods := generateSubPeriods(fdEffStart, fdEffEnd, granularity)

		openingBalance := getPriorRunClosingBalance(ctx, pool, params.EntityID, fd.FDID, params.PeriodStart)

		// COMPOUND principal tracking across sub-periods.
		// originalPrincipal is fd.PrincipalAmount; for COMPOUND, each sub-period's
		// opening principal = originalPrincipal + all accumulated accrued interest so far.
		originalPrincipal := fd.PrincipalAmount

		fdCalculated := false
		fdFailed := false

		for spIdx, sp := range subPeriods {
			// Build per-sub-period params
			subParams := params
			subParams.PeriodStart = sp[0]
			subParams.PeriodEnd = sp[1]

			// Always neutralise boundary conventions on fdForCalc: the conventions were already
			// applied in run.go when computing fdEffStart/fdEffEnd for generateSubPeriods, so
			// we must NOT re-apply them inside calculateAccrualForFD.
			fdForCalc := fd
			fdForCalc.AccrualStartConvention = "INCLUDE" // neutral: no +1 shift
			fdForCalc.AccrualEndConvention = "EXCLUDE"   // neutral: no +1 shift
			// For COMPOUND FDs after first sub-period, compound the principal:
			// use originalPrincipal + all interest accrued so far as the new base.
			if spIdx > 0 && strings.EqualFold(fd.InterestTypeCode, "COMPOUND") {
				fdForCalc.PrincipalAmount = originalPrincipal + openingBalance
			}

			result := calculateAccrualForFD(ctx, pool, fdForCalc, subParams, openingBalance)

			if result.LedgerRowStatus == "EXCLUDED" {
				logAccrualEvent(ctx, pool, LogAccrualParams{
					RunID:     runID,
					FDID:      fd.FDID,
					Level:     "INFO",
					EventType: "EXCLUDED",
					Message: fmt.Sprintf("Sub-period [%s,%s] excluded: %s",
						sp[0].Format(constants.DateFormat), sp[1].Format(constants.DateFormat),
						result.CalculationError),
					Detail: nil,
				})
				continue
			}

			// Accumulate totals
			totalInterest += result.PeriodInterestAccrued
			totalTDS += result.TDSDeductedInPeriod
			totalNet += result.NetInterestInPeriod

			// Advance compound principal for next sub-period: already handled via
			// originalPrincipal + openingBalance in the loop header above.
			_ = result.LedgerRowStatus // no-op placeholder

			// Roll opening balance forward for next sub-period
			openingBalance = result.ClosingAccruedBalance

			// Persist ledger for both SIMULATION and FINAL so /execute returns the same payload shape.
			// GL posting remains gated on approval when run_mode=SIMULATION.
			if isSimulation {
				logAccrualEvent(ctx, pool, LogAccrualParams{
					RunID:     runID,
					FDID:      fd.FDID,
					Level:     "INFO",
					EventType: "SIMULATED",
					Message: fmt.Sprintf(
						"SIMULATION fd=%s sub=[%s,%s] days=%d interest=%.4f tds=%.4f net=%.4f formula=%s",
						fd.FDID,
						sp[0].Format(constants.DateFormat), sp[1].Format(constants.DateFormat),
						result.AccrualDays,
						result.PeriodInterestAccrued,
						result.TDSDeductedInPeriod,
						result.NetInterestInPeriod,
						result.FormulaUsed,
					),
					Detail: nil,
				})
			}

			// Write ledger row for this sub-period (SIMULATION + FINAL)
			cashflowIDsJSON, _ := json.Marshal(result.CashflowRowIDs)

			_, upsertErr := pool.Exec(ctx, `
				INSERT INTO investment.fd_accrual_ledger (
					run_id, fd_id,
					fd_ref_no, bank_id, bank_name, entity_id, entity_name,
					interest_type_code, principal_amount, interest_rate,
					day_count_code, fd_start_date, fd_maturity_date,
					accrual_period_start, accrual_period_end, accrual_days,
					opening_principal, daily_accrual_rate, divisor,
					period_interest_accrued, opening_accrued_balance,
					interest_received_in_period, closing_accrued_balance,
					tds_applicable_amount, tds_deducted_in_period, net_interest_in_period,
					cashflow_row_ids,
					formula_used, ledger_row_status,
					-- req_* (run-level baseline)
					req_day_count_code, req_divisor, req_accrual_days,
					req_opening_principal, req_daily_accrual_rate,
					req_period_interest, req_closing_balance,
					req_tds_deducted, req_net_interest, req_formula_used,
					req_weekend_accrual, req_holiday_accrual,
					req_rounding_rule, req_precision_decimals,
					-- config_* (bank config params used)
					config_weekend_accrual, config_holiday_accrual,
					config_rounding_rule, config_precision_decimals,
					config_cap_date_adjustment, config_holiday_calendar_code,
					config_holidays_excluded,
					is_overridden, override_status, journal_entry_id,
					is_active, is_deleted, created_at
				) VALUES (
					$1,$2,
					$3,$4,$5,$6,$7,
					$8,$9,$10,
					$11,$12,$13,
					$14,$15,$16,
					$17,$18,$19,
					$20,$21,
					$22,$23,
					$24,$25,$26,
					$27,
					$28,$29,
					-- req_*
					$30,$31,$32,
					$33,$34,
					$35,$36,
					$37,$38,$39,
					$40,$41,
					$42,$43,
					-- config_*
					$44,$45,
					$46,$47,
					$48,$49,
					$50,
					false,null,null,
					true,false,now()
				)
				ON CONFLICT (run_id, fd_id, accrual_period_start) DO UPDATE SET
					period_interest_accrued     = EXCLUDED.period_interest_accrued,
					opening_accrued_balance     = EXCLUDED.opening_accrued_balance,
					interest_received_in_period = EXCLUDED.interest_received_in_period,
					closing_accrued_balance     = EXCLUDED.closing_accrued_balance,
					tds_applicable_amount       = EXCLUDED.tds_applicable_amount,
					tds_deducted_in_period      = EXCLUDED.tds_deducted_in_period,
					net_interest_in_period      = EXCLUDED.net_interest_in_period,
					cashflow_row_ids            = EXCLUDED.cashflow_row_ids,
					formula_used               = EXCLUDED.formula_used,
					ledger_row_status           = EXCLUDED.ledger_row_status,
					accrual_days               = EXCLUDED.accrual_days,
					daily_accrual_rate         = EXCLUDED.daily_accrual_rate,
					divisor                    = EXCLUDED.divisor,
					opening_principal          = EXCLUDED.opening_principal,
					-- req_* on conflict
					req_day_count_code         = EXCLUDED.req_day_count_code,
					req_divisor                = EXCLUDED.req_divisor,
					req_accrual_days           = EXCLUDED.req_accrual_days,
					req_opening_principal      = EXCLUDED.req_opening_principal,
					req_daily_accrual_rate     = EXCLUDED.req_daily_accrual_rate,
					req_period_interest        = EXCLUDED.req_period_interest,
					req_closing_balance        = EXCLUDED.req_closing_balance,
					req_tds_deducted           = EXCLUDED.req_tds_deducted,
					req_net_interest           = EXCLUDED.req_net_interest,
					req_formula_used           = EXCLUDED.req_formula_used,
					-- config_* on conflict
					config_weekend_accrual      = EXCLUDED.config_weekend_accrual,
					config_holiday_accrual      = EXCLUDED.config_holiday_accrual,
					config_rounding_rule        = EXCLUDED.config_rounding_rule,
					config_precision_decimals   = EXCLUDED.config_precision_decimals,
					config_cap_date_adjustment  = EXCLUDED.config_cap_date_adjustment,
					config_holiday_calendar_code = EXCLUDED.config_holiday_calendar_code,
					config_holidays_excluded    = EXCLUDED.config_holidays_excluded,
					updated_at                 = now()`,
				runID, fd.FDID,
				nullIfEmpty(result.FdRefNo), nullIfEmpty(result.BankID), nullIfEmpty(result.BankName),
				nullIfEmpty(result.EntityID), nullIfEmpty(result.EntityName),
				nullIfEmpty(result.InterestTypeCode), result.PrincipalAmount, result.InterestRate,
				nullIfEmpty(result.DayCountCode), result.FdStartDate, result.FdMaturityDate,
				result.AccrualPeriodStart, result.AccrualPeriodEnd, result.AccrualDays,
				result.OpeningPrincipal, result.DailyAccrualRate, result.Divisor,
				result.PeriodInterestAccrued, result.OpeningAccruedBalance,
				result.InterestReceivedInPeriod, result.ClosingAccruedBalance,
				result.TDSApplicableAmount, result.TDSDeductedInPeriod, result.NetInterestInPeriod,
				string(cashflowIDsJSON),
				result.FormulaUsed, result.LedgerRowStatus,
				// req_*
				nullIfEmpty(result.ReqDayCountCode), result.ReqDivisor, result.ReqAccrualDays,
				result.ReqOpeningPrincipal, result.ReqDailyAccrualRate,
				result.ReqPeriodInterest, result.ReqClosingBalance,
				result.ReqTDSDeducted, result.ReqNetInterest, nullIfEmpty(result.ReqFormulaUsed),
				result.ReqWeekendAccrual, result.ReqHolidayAccrual,
				nullIfEmpty(result.ReqRoundingRule), result.ReqPrecisionDecimals,
				// config_*
				result.ConfigWeekendAccrual, result.ConfigHolidayAccrual,
				nullIfEmpty(result.ConfigRoundingRule), result.ConfigPrecisionDecimals,
				nullIfEmpty(result.ConfigCapDateAdjustment), nullIfEmpty(result.ConfigHolidayCalendar),
				result.ConfigHolidaysExcluded,
			)
			if upsertErr != nil {
				fdFailed = true
				logAccrualEvent(ctx, pool, LogAccrualParams{
					RunID:     runID,
					FDID:      fd.FDID,
					Level:     "ERROR",
					EventType: "LEDGER_UPSERT",
					Message: fmt.Sprintf("Ledger upsert failed sub=[%s,%s]: %v",
						sp[0].Format(constants.DateFormat), sp[1].Format(constants.DateFormat), upsertErr),
					Detail: nil,
				})
				continue
			}

			fdCalculated = true

			interestVariance := result.ReqPeriodInterest - result.PeriodInterestAccrued
			logAccrualEvent(ctx, pool, LogAccrualParams{
				RunID:     runID,
				FDID:      fd.FDID,
				Level:     "INFO",
				EventType: "CALCULATED",
				Message: fmt.Sprintf(
					"sub=[%s,%s] config_days=%d req_days=%d config_interest=%.4f req_interest=%.4f variance=%.4f tds=%.4f holiday_excl=%d",
					sp[0].Format(constants.DateFormat), sp[1].Format(constants.DateFormat),
					result.AccrualDays, result.ReqAccrualDays,
					result.PeriodInterestAccrued, result.ReqPeriodInterest,
					interestVariance, result.TDSDeductedInPeriod,
					result.ConfigHolidaysExcluded),
				Detail: map[string]interface{}{
					"config_rate_per_day":    result.DailyAccrualRate,
					"req_rate_per_day":       result.ReqDailyAccrualRate,
					"config_accrual_days":    result.AccrualDays,
					"req_accrual_days":       result.ReqAccrualDays,
					"config_holiday_accrual": result.ConfigHolidayAccrual,
					"config_weekend_accrual": result.ConfigWeekendAccrual,
					"holidays_excluded":      result.ConfigHolidaysExcluded,
					"interest_variance":      interestVariance,
					"config_formula":         result.FormulaUsed,
					"req_formula":            result.ReqFormulaUsed,
				},
			})
		} // end sub-period loop

		if fdFailed && !fdCalculated {
			failed++
			continue
		}
		if !fdCalculated && !fdFailed {
			// All sub-periods excluded
			continue
		}
		if fdFailed {
			failed++
		}
		calculated++

		if isSimulation {
			continue
		}

		// ── One audit row per FD (query first ledger row for this run+fd) ───────
		var ledgerID string
		var prevPeriodInterest, prevClosingBal, prevTDS, prevNetInterest float64
		var prevStatus, prevFormula string
		var prevIsOverridden bool
		var prevOverrideAmt, prevOverrideAdj float64
		var prevOverrideCode, prevOverrideText, prevOverrideStatus string
		var prevOpeningBal, prevInterestReceived, prevTDSApplicable float64
		_ = pool.QueryRow(ctx, `
			SELECT
				ledger_id,
				COALESCE(period_interest_accrued,0),
				COALESCE(closing_accrued_balance,0),
				COALESCE(tds_deducted_in_period,0),
				COALESCE(net_interest_in_period,0),
				COALESCE(ledger_row_status,''),
				COALESCE(formula_used,''),
				COALESCE(is_overridden,false),
				COALESCE(override_amount,0),
				COALESCE(override_adjustment,0),
				COALESCE(override_reason_code,''),
				COALESCE(override_reason_text,''),
				COALESCE(override_status,''),
				COALESCE(opening_accrued_balance,0),
				COALESCE(interest_received_in_period,0),
				COALESCE(tds_applicable_amount,0)
			FROM investment.fd_accrual_ledger
			WHERE run_id=$1 AND fd_id=$2
			ORDER BY accrual_period_start ASC LIMIT 1`,
			runID, fd.FDID).Scan(&ledgerID,
			&prevPeriodInterest, &prevClosingBal, &prevTDS, &prevNetInterest,
			&prevStatus, &prevFormula,
			&prevIsOverridden, &prevOverrideAmt, &prevOverrideAdj,
			&prevOverrideCode, &prevOverrideText, &prevOverrideStatus,
			&prevOpeningBal, &prevInterestReceived, &prevTDSApplicable)

		if ledgerID != "" {
			// Use last result for summary fields
			lastResult := calculateAccrualForFD(ctx, pool, fd, params, 0) // just for meta fields
			_, _ = pool.Exec(ctx, `
				INSERT INTO investment.fd_accrual_ledger_audit (
					ledger_id, run_id, fd_id,
					fd_ref_no, bank_id, bank_name, entity_id,
					principal_amount, interest_rate, accrual_days,
					action_type, processing_status, requested_by, requested_at,
					old_period_interest_accrued, old_closing_accrued_balance,
					old_tds_deducted_in_period, old_net_interest_in_period,
					old_ledger_row_status, old_formula_used,
					old_is_overridden, old_override_amount, old_override_adjustment,
					old_override_reason_code, old_override_reason_text, old_override_status,
					old_opening_accrued_balance, old_interest_received_in_period, old_tds_applicable_amount
				) VALUES (
					$1,$2,$3,
					$4,$5,$6,$7,
					$8,$9,$10,
					'CALCULATE','CALCULATED',$11,now(),
					$12,$13,$14,$15,$16,$17,
					$18,$19,$20,$21,$22,$23,
					$24,$25,$26
				)`,
				ledgerID, runID, fd.FDID,
				fd.FdRefNo, fd.BankID, fd.BankName, fd.EntityID,
				fd.PrincipalAmount, fd.InterestRate, lastResult.AccrualDays,
				executedBy,
				prevPeriodInterest, prevClosingBal, prevTDS, prevNetInterest, prevStatus, prevFormula,
				prevIsOverridden, prevOverrideAmt, prevOverrideAdj, prevOverrideCode, prevOverrideText, prevOverrideStatus,
				prevOpeningBal, prevInterestReceived, prevTDSApplicable)
		}
	} // end FD loop

	// Log final summary including run mode
	api.LogInfo("[FDAccrual] executeAccrualRun complete: run=%s mode=%s calc=%d failed=%d total_interest=%.2f",
		runID, runMode, calculated, failed, totalInterest)

	// Update run summary
	newStatus := "COMPUTED"
	if calculated == 0 && failed > 0 {
		newStatus = "FAILED"
	}
	_, _ = pool.Exec(ctx, `
		UPDATE investment.fd_accrual_run
		SET run_status              = $1,
		    fds_calculated          = $2,
		    fds_failed              = $3,
		    total_interest_accrued  = $4,
		    total_tds_deducted      = $5,
		    total_accrued_closing_balance = $6,
		    execution_progress_pct  = 100,
		    completed_at            = now(),
		    updated_by              = $7,
		    updated_at              = now()
		WHERE run_id = $8`,
		newStatus,
		calculated, failed,
		math.Round(totalInterest*100)/100,
		math.Round(totalTDS*100)/100,
		math.Round(totalNet*100)/100,
		executedBy, runID,
	)

	if newStatus == "FAILED" {
		return 0, failed, fmt.Errorf(
			"accrual run FAILED: 0 of %d FD(s) calculated successfully for entity %s. "+
				"Check execution log: SELECT log_level, event_type, fd_id, message "+
				"FROM investment.fd_accrual_run_execution_log WHERE run_id='%s' "+
				"AND log_level='ERROR' ORDER BY logged_at",
			failed, params.EntityID, runID,
		)
	}

	return calculated, failed, nil
}

// submitAccrualRunForApproval updates status and fires CreateInstance.
func submitAccrualRunForApproval(ctx context.Context, pool *pgxpool.Pool, runID string, submittedBy string) error {
	var entityID string
	var totalNet float64
	err := pool.QueryRow(ctx,
		`SELECT COALESCE(entity_id,''), COALESCE(total_interest_accrued,0) FROM investment.fd_accrual_run WHERE run_id=$1`,
		runID,
	).Scan(&entityID, &totalNet)
	if err != nil {
		return fmt.Errorf("submitAccrualRunForApproval: load run: %w", err)
	}

	_, err = pool.Exec(ctx, `
		UPDATE investment.fd_accrual_run
		SET run_status='PENDING_APPROVAL', submitted_by=$1, submitted_at=now()
		WHERE run_id=$2`, submittedBy, runID)
	if err != nil {
		return fmt.Errorf("submitAccrualRunForApproval: update status: %w", err)
	}

	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDAccrual] submitAccrualRunForApproval engine goroutine panic for run %s: %v", runID, rec)
			}
		}()
		bgCtx := context.Background()
		instID, instErr := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
			ModuleCode:       "FIXED_DEPOSIT",
			EntityCode:       entityID,
			TransactionType:  "FD_ACCRUAL_APPROVE",
			RecordID:         runID,
			RecordTable:      "investment.fd_accrual_run",
			AuditTable:       "investment.fd_accrual_run_audit",
			AuditIDColumn:    "run_id",
			ActionType:       "CREATE",
			Amount:           totalNet,
			SubmittedBy:      submittedBy,
			SubmittedByEmail: "system@internal",
		})
		if instErr != nil {
			api.LogError("[FDAccrual] Scheduler CreateInstance failed for run %s: %v", runID, instErr)
			return
		}
		api.LogInfo("[FDAccrual] Scheduler CreateInstance %s → run %s PENDING_APPROVAL", instID, runID)
	}()

	go func(rID, eID, by string, amount float64) {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDAccrual] submitAccrualRunForApproval notification goroutine panic for run %s: %v", rID, rec)
			}
		}()
		notifcatalog.TriggerNotification(context.Background(), pool, "/investment/fd/accrual/run/auto-submit", rID, map[string]interface{}{
			"entity_id":    eID,
			"record_id":    rID,
			"event":        "FD_ACCRUAL_RUN_AUTO_SUBMITTED",
			"submitted_by": by,
			"amount":       amount,
		})
	}(runID, entityID, submittedBy, totalNet)

	return nil
}

// postAccrualJournals creates journal entries for every ledger row of an approved run.
func postAccrualJournals(ctx context.Context, pool *pgxpool.Pool, runID, userEmail string) error {
	rows, err := pool.Query(ctx, `
		SELECT
			l.ledger_id,
			l.fd_id,
			COALESCE(l.period_interest_accrued,0)   AS gross_interest,
			l.accrual_period_start, l.accrual_period_end,
			COALESCE(l.entity_id,'')                AS entity_id,
			COALESCE(l.entity_name,'')              AS entity_name,
			COALESCE(l.period_interest_accrued,0)   AS period_interest,
			COALESCE(l.closing_accrued_balance,0)   AS closing_balance,
			COALESCE(l.tds_deducted_in_period,0)    AS tds_deducted,
			COALESCE(l.formula_used,'')             AS formula_used
		FROM investment.fd_accrual_ledger l
		WHERE l.run_id = $1
		  AND COALESCE(l.is_deleted,false) = false
		  AND l.ledger_row_status IN ('CALCULATED','OVERRIDDEN')`, runID)
	if err != nil {
		return fmt.Errorf("postAccrualJournals query: %w", err)
	}
	defer rows.Close()

	type ledgerRow struct {
		LedgerID, FDID, EntityID, EntityName, FormulaUsed  string
		GrossInterest, PeriodInterest, ClosingBalance, TDS float64
		PeriodStart, PeriodEnd                             time.Time
	}
	var ledgerRows []ledgerRow
	for rows.Next() {
		var lr ledgerRow
		if err := rows.Scan(&lr.LedgerID, &lr.FDID, &lr.GrossInterest,
			&lr.PeriodStart, &lr.PeriodEnd, &lr.EntityID, &lr.EntityName,
			&lr.PeriodInterest, &lr.ClosingBalance, &lr.TDS, &lr.FormulaUsed); err != nil {
			return fmt.Errorf("postAccrualJournals scan: %w", err)
		}
		ledgerRows = append(ledgerRows, lr)
	}
	rows.Close()

	for _, lr := range ledgerRows {
		tx, txErr := pool.Begin(ctx)
		if txErr != nil {
			api.LogError("[FDAccrual] Journal inner tx begin failed fd %s: %v", lr.FDID, txErr)
			continue
		}

		var activityID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_activity (
				activity_type, activity_subtype, effective_date,
				accounting_period, data_source, status
			) VALUES ('FIXED_DEPOSIT','FD_INTEREST_ACCRUAL',$1,$2,'FD_ACCRUAL','APPROVED')
			RETURNING activity_id`,
			lr.PeriodEnd, buildAccrualPeriod(lr.PeriodEnd),
		).Scan(&activityID); err != nil {
			_ = tx.Rollback(ctx)
			api.LogError("[FDAccrual] Create activity failed fd %s: %v", lr.FDID, err)
			continue
		}

		amount := math.Round(lr.PeriodInterest*100) / 100
		if amount == 0 {
			_ = tx.Rollback(ctx)
			continue
		}

		description := fmt.Sprintf("FD accrual %s period %s→%s | fd_id=%s | run_id=%s",
			lr.FDID, lr.PeriodStart.Format(constants.DateFormat), lr.PeriodEnd.Format(constants.DateFormat), lr.FDID, runID)

		var entryID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_journal_entry (
				activity_id, entity_id, entity_name,
				entry_date, accounting_period,
				entry_type, description,
				total_debit, total_credit,
				status, created_by,
				fd_id, accrual_run_id, accrual_ledger_id
			) VALUES ($1,$2,$3,$4,$5,'FD_INTEREST_ACCRUAL',$6,$7,$8,'POSTED',$9,$10,$11,$12)
			RETURNING entry_id`,
			activityID, nullIfEmpty(lr.EntityID), nullIfEmpty(lr.EntityName),
			lr.PeriodEnd, buildAccrualPeriod(lr.PeriodEnd),
			description, amount, amount, userEmail,
			nullIfEmpty(lr.FDID), nullIfEmpty(runID), nullIfEmpty(lr.LedgerID),
		).Scan(&entryID); err != nil {
			_ = tx.Rollback(ctx)
			api.LogError("[FDAccrual] Insert journal entry failed fd %s: %v", lr.FDID, err)
			continue
		}

		// Line 1: debit interest receivable
		_, err1 := tx.Exec(ctx, `
			INSERT INTO investment.accounting_journal_entry_line (
				entry_id, line_number, account_number, account_name, account_type,
				debit_amount, credit_amount, narration
			) VALUES ($1,1,'INTEREST_RECEIVABLE','Interest Receivable on FD','ASSET',$2,0,$3)`,
			entryID, amount,
			fmt.Sprintf("FD accrual %s | accrual_run_id=%s | accrual_ledger_id=%s", lr.FDID, runID, lr.LedgerID))
		// Line 2: credit interest income
		_, err2 := tx.Exec(ctx, `
			INSERT INTO investment.accounting_journal_entry_line (
				entry_id, line_number, account_number, account_name, account_type,
				debit_amount, credit_amount, narration
			) VALUES ($1,2,'INTEREST_INCOME_FD','Interest Income - Fixed Deposit','INCOME',0,$2,$3)`,
			entryID, amount,
			fmt.Sprintf("FD accrual %s | accrual_run_id=%s | accrual_ledger_id=%s", lr.FDID, runID, lr.LedgerID))

		if err1 != nil || err2 != nil {
			_ = tx.Rollback(ctx)
			api.LogError("[FDAccrual] Insert journal lines failed fd %s: e1=%v e2=%v", lr.FDID, err1, err2)
			continue
		}

		if cerr := tx.Commit(ctx); cerr != nil {
			api.LogError("[FDAccrual] Journal commit failed fd %s: %v", lr.FDID, cerr)
			continue
		}

		// TDS journal: debit interest expense TDS / credit TDS payable (non-fatal)
		if tdsAmt := math.Round(lr.TDS*100) / 100; tdsAmt > 0 {
			tdsTx, tdsErr := pool.Begin(ctx)
			if tdsErr == nil {
				var tdsActivityID string
				tdsErr = tdsTx.QueryRow(ctx, `
					INSERT INTO investment.accounting_activity (
						activity_type, activity_subtype, effective_date,
						accounting_period, data_source, status
					) VALUES ('FIXED_DEPOSIT','FD_TDS_ACCRUAL',$1,$2,'FD_ACCRUAL','APPROVED')
					RETURNING activity_id`,
					lr.PeriodEnd, buildAccrualPeriod(lr.PeriodEnd),
				).Scan(&tdsActivityID)
				if tdsErr == nil {
					var tdsEntryID string
					tdsDesc := fmt.Sprintf("FD TDS accrual %s period %s→%s | run_id=%s",
						lr.FDID, lr.PeriodStart.Format(constants.DateFormat), lr.PeriodEnd.Format(constants.DateFormat), runID)
					tdsErr = tdsTx.QueryRow(ctx, `
						INSERT INTO investment.accounting_journal_entry (
							activity_id, entity_id, entity_name,
							entry_date, accounting_period,
							entry_type, description,
							total_debit, total_credit,
							status, created_by,
							fd_id, accrual_run_id, accrual_ledger_id
						) VALUES ($1,$2,$3,$4,$5,'FD_TDS_ACCRUAL',$6,$7,$8,'POSTED',$9,$10,$11,$12)
						RETURNING entry_id`,
						tdsActivityID, nullIfEmpty(lr.EntityID), nullIfEmpty(lr.EntityName),
						lr.PeriodEnd, buildAccrualPeriod(lr.PeriodEnd),
						tdsDesc, tdsAmt, tdsAmt, userEmail,
						nullIfEmpty(lr.FDID), nullIfEmpty(runID), nullIfEmpty(lr.LedgerID),
					).Scan(&tdsEntryID)
					if tdsErr == nil {
						tdsNarration := fmt.Sprintf("FD TDS %s | accrual_run_id=%s | accrual_ledger_id=%s", lr.FDID, runID, lr.LedgerID)
						_, tdsErr1 := tdsTx.Exec(ctx, `
							INSERT INTO investment.accounting_journal_entry_line (
								entry_id, line_number, account_number, account_name, account_type,
								debit_amount, credit_amount, narration
							) VALUES ($1,1,'TDS_EXPENSE','TDS on FD Interest','EXPENSE',$2,0,$3)`,
							tdsEntryID, tdsAmt, tdsNarration)
						_, tdsErr2 := tdsTx.Exec(ctx, `
							INSERT INTO investment.accounting_journal_entry_line (
								entry_id, line_number, account_number, account_name, account_type,
								debit_amount, credit_amount, narration
							) VALUES ($1,2,'TDS_PAYABLE','TDS Payable on FD','LIABILITY',0,$2,$3)`,
							tdsEntryID, tdsAmt, tdsNarration)
						if tdsErr1 != nil || tdsErr2 != nil {
							_ = tdsTx.Rollback(ctx)
							api.LogError("[FDAccrual] TDS journal lines failed fd %s: e1=%v e2=%v", lr.FDID, tdsErr1, tdsErr2)
						} else if cerr2 := tdsTx.Commit(ctx); cerr2 != nil {
							api.LogError("[FDAccrual] TDS journal commit failed fd %s: %v", lr.FDID, cerr2)
						} else {
							api.LogInfo("[FDAccrual] TDS journal posted fd=%s run=%s tds_entry=%s amt=%.2f", lr.FDID, runID, tdsEntryID, tdsAmt)
						}
					} else {
						_ = tdsTx.Rollback(ctx)
						api.LogError("[FDAccrual] TDS journal entry insert failed fd %s: %v", lr.FDID, tdsErr)
					}
				} else {
					_ = tdsTx.Rollback(ctx)
					api.LogError("[FDAccrual] TDS activity insert failed fd %s: %v", lr.FDID, tdsErr)
				}
			} else {
				api.LogError("[FDAccrual] TDS tx begin failed fd %s: %v", lr.FDID, tdsErr)
			}
		}

		// Audit ledger row: status CALCULATED → POSTED (full context + override snapshot)
		// Fetch current override state before marking POSTED
		var aOldIsOverridden bool
		var aOldOverrideAmt, aOldOverrideAdj, aOldTDSApplicable, aOldOpeningBal, aOldIntReceived float64
		var aOldOverrideCode, aOldOverrideText, aOldOverrideStatus string
		var aFdRefNo, aBankID, aBankName, aEntityID string
		var aPrincipal, aRate float64
		var aAccrualDays int
		_ = pool.QueryRow(ctx, `
			SELECT
				COALESCE(fd_ref_no,''), COALESCE(bank_id,''), COALESCE(bank_name,''), COALESCE(entity_id,''),
				COALESCE(principal_amount,0), COALESCE(interest_rate,0), COALESCE(accrual_days,0),
				COALESCE(is_overridden,false),
				COALESCE(override_amount,0), COALESCE(override_adjustment,0),
				COALESCE(override_reason_code,''), COALESCE(override_reason_text,''),
				COALESCE(override_status,''),
				COALESCE(opening_accrued_balance,0), COALESCE(interest_received_in_period,0),
				COALESCE(tds_applicable_amount,0)
			FROM investment.fd_accrual_ledger WHERE ledger_id=$1`, lr.LedgerID,
		).Scan(
			&aFdRefNo, &aBankID, &aBankName, &aEntityID,
			&aPrincipal, &aRate, &aAccrualDays,
			&aOldIsOverridden, &aOldOverrideAmt, &aOldOverrideAdj,
			&aOldOverrideCode, &aOldOverrideText, &aOldOverrideStatus,
			&aOldOpeningBal, &aOldIntReceived, &aOldTDSApplicable)
		_, _ = pool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_ledger_audit (
				ledger_id, run_id, fd_id,
				fd_ref_no, bank_id, bank_name, entity_id,
				principal_amount, interest_rate, accrual_days,
				action_type, processing_status, requested_by, requested_at,
				checker_by, checker_at, checker_comment,
				old_period_interest_accrued, old_closing_accrued_balance,
				old_tds_deducted_in_period, old_net_interest_in_period,
				old_ledger_row_status, old_formula_used,
				old_is_overridden, old_override_amount, old_override_adjustment,
				old_override_reason_code, old_override_reason_text, old_override_status,
				old_opening_accrued_balance, old_interest_received_in_period, old_tds_applicable_amount
			) VALUES (
				$1,$2,$3,
				$4,$5,$6,$7,
				$8,$9,$10,
				'EDIT','POSTED',$11,now(),
				$11,now(),'Journal posted',
				$12,$13,$14,$15,'CALCULATED',$16,
				$17,$18,$19,$20,$21,$22,
				$23,$24,$25
			)`,
			lr.LedgerID, runID, lr.FDID,
			aFdRefNo, aBankID, aBankName, aEntityID,
			aPrincipal, aRate, aAccrualDays,
			userEmail,
			lr.PeriodInterest, lr.ClosingBalance, lr.TDS, lr.GrossInterest, lr.FormulaUsed,
			aOldIsOverridden, aOldOverrideAmt, aOldOverrideAdj, aOldOverrideCode, aOldOverrideText, aOldOverrideStatus,
			aOldOpeningBal, aOldIntReceived, aOldTDSApplicable)

		// Update ledger with journal_entry_id
		_, _ = pool.Exec(ctx,
			`UPDATE investment.fd_accrual_ledger SET journal_entry_id=$1, ledger_row_status='POSTED', updated_at=now()
			 WHERE run_id=$2 AND fd_id=$3`,
			entryID, runID, lr.FDID)

		api.LogInfo("[FDAccrual] Journal posted fd=%s run=%s entry=%s", lr.FDID, runID, entryID)
	}

	return nil
}

// ─── 15. RecomputeAccrualRun ──────────────────────────────────────────────────
// Re-runs the accrual engine for one or more existing runs whose ledger is
// empty (or for any run regardless, if force=true).  Useful for backfilling
// runs that were created before the bank_name filter fix.

func RecomputeAccrualRun(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			RunIDs []string `json:"run_ids"` // explicit list – or leave empty + entity_id to auto-pick
			// If run_ids is empty, backfill all runs for entity_id with ledger_count=0
			EntityID string `json:"entity_id"`
			Force    bool   `json:"force"` // if true, re-run even if ledger is already populated
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// If no explicit run_ids, find all empty runs for the entity
		if len(req.RunIDs) == 0 {
			var rows []string
			q := `
				SELECT r.run_id
				FROM investment.fd_accrual_run r
				WHERE COALESCE(r.is_deleted,false)=false`
			args := []interface{}{}
			argIdx := 1
			if req.EntityID != "" {
				q += fmt.Sprintf(" AND r.entity_id=$%d", argIdx)
				args = append(args, req.EntityID)
				argIdx++
			}
			if !req.Force {
				q += ` AND (SELECT COUNT(*) FROM investment.fd_accrual_ledger l WHERE l.run_id=r.run_id AND COALESCE(l.is_deleted,false)=false)=0`
			}
			q += " ORDER BY r.created_at"
			dbRows, err := pgxPool.Query(ctx, q, args...)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
				return
			}
			for dbRows.Next() {
				var rid string
				_ = dbRows.Scan(&rid)
				rows = append(rows, rid)
			}
			dbRows.Close()
			req.RunIDs = rows
		}

		if len(req.RunIDs) == 0 {
			api.RespondWithPayload(w, true, "No runs to recompute", map[string]interface{}{"recomputed": 0})
			return
		}

		type runResult struct {
			RunID      string `json:"run_id"`
			Calculated int    `json:"calculated"`
			Failed     int    `json:"failed"`
			Error      string `json:"error,omitempty"`
		}

		results := make([]runResult, 0, len(req.RunIDs))
		for _, runID := range req.RunIDs {
			// Clear old ledger rows so we don't double-count
			_, _ = pgxPool.Exec(ctx,
				`DELETE FROM investment.fd_accrual_ledger WHERE run_id=$1`, runID)

			// Reset run to allow re-execution
			_, _ = pgxPool.Exec(ctx,
				`UPDATE investment.fd_accrual_run
				 SET run_status='DRAFT', fds_in_scope=0, fds_calculated=0, fds_failed=0,
				     total_interest_accrued=0, total_tds_deducted=0, total_accrued_closing_balance=0,
				     started_at=NULL, completed_at=NULL, updated_at=now()
				 WHERE run_id=$1`, runID)

			// STEP 6: Re-validate scope so fds_in_scope is accurate before execute
			eligible, blockers, _, valErr := validateAndPersistFindings(ctx, pgxPool, runID)
			if valErr != nil {
				res := runResult{RunID: runID, Error: "revalidate failed: " + friendlyAccrualError(valErr, "scope validation")}
				results = append(results, res)
				api.LogError("[FDAccrual] RecomputeAccrualRun validate: run=%s err=%v", runID, valErr)
				continue
			}
			newValStatus := "VALIDATED"
			if blockers > 0 || eligible == 0 {
				newValStatus = "VALIDATION_FAILED"
			}
			_, _ = pgxPool.Exec(ctx,
				`UPDATE investment.fd_accrual_run SET run_status=$1, fds_in_scope=$2 WHERE run_id=$3`,
				newValStatus, eligible, runID)

			if newValStatus == "VALIDATION_FAILED" {
				res := runResult{RunID: runID, Error: fmt.Sprintf(
					"recompute aborted: 0 FDs in scope or blockers found for run %s. "+
						"Check fd_status=ACTIVE and cashflow_generated=true.",
					runID)}
				results = append(results, res)
				api.LogInfo("[FDAccrual] RecomputeAccrualRun: run=%s aborted — VALIDATION_FAILED", runID)
				continue
			}

			calc, failed, err := executeAccrualRun(ctx, pgxPool, runID, userEmail)
			res := runResult{RunID: runID, Calculated: calc, Failed: failed}
			if err != nil {
				res.Error = err.Error()
				api.LogError("[FDAccrual] RecomputeAccrualRun: run=%s err=%v", runID, err)
			}
			results = append(results, res)
			api.LogInfo("[FDAccrual] RecomputeAccrualRun: run=%s calc=%d failed=%d", runID, calc, failed)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"recomputed": len(results),
			"results":    results,
		})
	}
}

// ─── 16. BulkGenerateMonthlyAccruals ─────────────────────────────────────────
// Creates, validates, and executes one accrual run per month between
// start_month (YYYY-MM) and end_month (YYYY-MM) inclusive.
// Skips months that already have a FINAL run for the entity.

func BulkGenerateMonthlyAccruals(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID             string `json:"user_id"`
			EntityID           string `json:"entity_id"`
			EntityName         string `json:"entity_name"`
			StartMonth         string `json:"start_month"` // YYYY-MM
			EndMonth           string `json:"end_month"`   // YYYY-MM  (inclusive)
			BankIDFilter       string `json:"bank_id_filter"`
			FDStatusFilter     string `json:"fd_status_filter"`
			DayCountConvention string `json:"day_count_convention"`
			RoundingRule       string `json:"rounding_rule"`
			PrecisionDecimals  int    `json:"precision_decimals"`
			RunMode            string `json:"run_mode"`      // SIMULATION or FINAL
			SkipExisting       bool   `json:"skip_existing"` // default true – skip months with existing FINAL run
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.EntityID == "" || req.StartMonth == "" || req.EndMonth == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_id, start_month and end_month are required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		// Defaults
		if req.RunMode == "" {
			req.RunMode = "FINAL"
		}
		if req.DayCountConvention == "" {
			req.DayCountConvention = "ACT_365"
		}
		if req.RoundingRule == "" {
			req.RoundingRule = "ROUND"
		}
		if req.PrecisionDecimals <= 0 {
			req.PrecisionDecimals = 2
		}
		if req.FDStatusFilter == "" {
			req.FDStatusFilter = "ACTIVE"
		}
		// SkipExisting defaults to true unless caller explicitly sets false
		skipExisting := true
		if !req.SkipExisting {
			skipExisting = false
		}

		// Parse months
		start, err := time.Parse(constants.DateFormatYearMonth, req.StartMonth)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "start_month must be YYYY-MM")
			return
		}
		end, err := time.Parse(constants.DateFormatYearMonth, req.EndMonth)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "end_month must be YYYY-MM")
			return
		}
		if end.Before(start) {
			api.RespondWithError(w, http.StatusBadRequest, "end_month must be >= start_month")
			return
		}

		ctx := r.Context()

		type monthResult struct {
			Month      string `json:"month"`
			RunID      string `json:"run_id"`
			Calculated int    `json:"calculated"`
			Failed     int    `json:"failed"`
			Skipped    bool   `json:"skipped,omitempty"`
			SkipReason string `json:"skip_reason,omitempty"`
			Error      string `json:"error,omitempty"`
		}

		var monthResults []monthResult

		for m := start; !m.After(end); m = m.AddDate(0, 1, 0) {
			monthStr := m.Format(constants.DateFormatYearMonth)
			// Period: 1st to last day of month
			periodStart := time.Date(m.Year(), m.Month(), 1, 0, 0, 0, 0, time.UTC)
			periodEnd := time.Date(m.Year(), m.Month()+1, 0, 0, 0, 0, 0, time.UTC) // last day

			res := monthResult{Month: monthStr}

			// Skip if a FINAL run already exists for this entity+month
			if skipExisting {
				var existingRunID string
				_ = pgxPool.QueryRow(ctx, `
					SELECT run_id FROM investment.fd_accrual_run
					WHERE entity_id=$1
					  AND run_mode='FINAL'
					  AND accrual_period_start=$2
					  AND run_status NOT IN ('DRAFT','VALIDATION_FAILED','REJECTED')
					  AND COALESCE(is_deleted,false)=false
					LIMIT 1`, req.EntityID, periodStart,
				).Scan(&existingRunID)
				if existingRunID != "" {
					res.RunID = existingRunID
					res.Skipped = true
					res.SkipReason = "FINAL run already exists: " + existingRunID
					monthResults = append(monthResults, res)
					api.LogInfo("[FDAccrual] BulkGenerate: skipping %s — existing run %s", monthStr, existingRunID)
					continue
				}
			}

			// Step 1: Create run
			input := CreateAccrualRunInput{
				RunType:            "MONTHLY", // bulk monthly generate always produces MONTHLY runs
				RunMode:            req.RunMode,
				EntityID:           req.EntityID,
				EntityName:         req.EntityName,
				BankIDFilter:       req.BankIDFilter,
				FDStatusFilter:     req.FDStatusFilter,
				FinancialPeriod:    buildAccrualPeriod(periodStart),
				DayCountConvention: req.DayCountConvention,
				RoundingRule:       req.RoundingRule,
				PrecisionDecimals:  req.PrecisionDecimals,
				AccrualPeriodStart: periodStart,
				AccrualPeriodEnd:   periodEnd,
				CreatedBy:          userEmail,
			}
			runID, createErr := createAccrualRunInternal(ctx, pgxPool, input)
			if createErr != nil {
				res.Error = "create failed: " + createErr.Error()
				monthResults = append(monthResults, res)
				api.LogError("[FDAccrual] BulkGenerate: create %s err=%v", monthStr, createErr)
				continue
			}
			res.RunID = runID

			// Step 2: Validate scope (updates fds_in_scope, status→VALIDATED/VALIDATION_FAILED)
			eligible, blockers, _, valErr := validateAndPersistFindings(ctx, pgxPool, runID)
			if valErr != nil {
				res.Error = "validate failed: " + valErr.Error()
				monthResults = append(monthResults, res)
				continue
			}
			newStatus := "VALIDATED"
			if blockers > 0 {
				newStatus = "VALIDATION_FAILED"
			}
			_, _ = pgxPool.Exec(ctx,
				`UPDATE investment.fd_accrual_run SET run_status=$1, fds_in_scope=$2 WHERE run_id=$3`,
				newStatus, eligible, runID)

			if eligible == 0 {
				res.Skipped = true
				res.SkipReason = fmt.Sprintf("no FDs in scope for %s (entity=%s)", monthStr, req.EntityID)
				_, _ = pgxPool.Exec(ctx,
					`UPDATE investment.fd_accrual_run SET run_status='COMPUTED' WHERE run_id=$1`, runID)
				monthResults = append(monthResults, res)
				api.LogInfo("[FDAccrual] BulkGenerate: %s run=%s — 0 FDs in scope", monthStr, runID)
				continue
			}

			// Step 3: Execute
			calc, failed, execErr := executeAccrualRun(ctx, pgxPool, runID, userEmail)
			res.Calculated = calc
			res.Failed = failed
			if execErr != nil {
				res.Error = "execute failed: " + execErr.Error()
				api.LogError("[FDAccrual] BulkGenerate: execute %s run=%s err=%v", monthStr, runID, execErr)
			}
			monthResults = append(monthResults, res)
			api.LogInfo("[FDAccrual] BulkGenerate: %s run=%s calc=%d failed=%d", monthStr, runID, calc, failed)
		}

		totalCalc, totalFailed, skipped := 0, 0, 0
		for _, mr := range monthResults {
			totalCalc += mr.Calculated
			totalFailed += mr.Failed
			if mr.Skipped {
				skipped++
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"months_processed": len(monthResults),
			"months_skipped":   skipped,
			"total_calculated": totalCalc,
			"total_failed":     totalFailed,
			"results":          monthResults,
		})
		api.LogInfo("[FDAccrual] BulkGenerateMonthlyAccruals: entity=%s %s→%s processed=%d skipped=%d",
			req.EntityID, req.StartMonth, req.EndMonth, len(monthResults), skipped)
	}
}

// buildAccrualResponse constructs a unified response payload for accrual run endpoints.
// SIMULATION and FINAL runs share the same shape: run_header, kpi, ledger, fd_kpis, execution_log.
func buildAccrualResponse(ctx context.Context, pool *pgxpool.Pool, runID string) map[string]interface{} {
	// ── Run header ────────────────────────────────────────────────────────────
	type runHeader struct {
		RunID           string  `json:"run_id"`
		RunType         string  `json:"run_type"`
		RunMode         string  `json:"run_mode"`
		RunStatus       string  `json:"run_status"`
		EntityID        string  `json:"entity_id"`
		EntityName      string  `json:"entity_name"`
		PeriodStart     string  `json:"accrual_period_start"`
		PeriodEnd       string  `json:"accrual_period_end"`
		FinancialPeriod string  `json:"financial_period"`
		Granularity     string  `json:"accrual_granularity"`
		FDsInScope      int     `json:"fds_in_scope"`
		FDsCalculated   int     `json:"fds_calculated"`
		FDsFailed       int     `json:"fds_failed"`
		TotalInterest   float64 `json:"total_interest_accrued"`
		TotalTDS        float64 `json:"total_tds_deducted"`
		TotalNet        float64 `json:"total_accrued_closing_balance"`
		CreatedAt       string  `json:"created_at"`
	}
	var hdr runHeader
	var periodStart, periodEnd time.Time
	_ = pool.QueryRow(ctx, `
		SELECT
			run_id,
			COALESCE(run_type,''),
			COALESCE(run_mode,'FINAL'),
			COALESCE(run_status,''),
			COALESCE(entity_id,''),
			COALESCE(entity_name,''),
			accrual_period_start,
			accrual_period_end,
			COALESCE(financial_period,''),
			COALESCE(accrual_granularity,'RUN'),
			COALESCE(fds_in_scope,0),
			COALESCE(fds_calculated,0),
			COALESCE(fds_failed,0),
			COALESCE(total_interest_accrued,0),
			COALESCE(total_tds_deducted,0),
			COALESCE(total_accrued_closing_balance,0),
			created_at
		FROM investment.fd_accrual_run WHERE run_id=$1`, runID).Scan(
		&hdr.RunID, &hdr.RunType, &hdr.RunMode, &hdr.RunStatus,
		&hdr.EntityID, &hdr.EntityName,
		&periodStart, &periodEnd,
		&hdr.FinancialPeriod, &hdr.Granularity,
		&hdr.FDsInScope, &hdr.FDsCalculated, &hdr.FDsFailed,
		&hdr.TotalInterest, &hdr.TotalTDS, &hdr.TotalNet,
		&hdr.CreatedAt,
	)
	hdr.PeriodStart = periodStart.Format(constants.DateFormat)
	hdr.PeriodEnd = periodEnd.Format(constants.DateFormat)

	// ── KPI summary ───────────────────────────────────────────────────────────
	kpi := map[string]interface{}{
		"total_interest_accrued": math.Round(hdr.TotalInterest*100) / 100,
		"total_tds_deducted":     math.Round(hdr.TotalTDS*100) / 100,
		"total_net_accrued":      math.Round(hdr.TotalNet*100) / 100,
		"fds_in_scope":           hdr.FDsInScope,
		"fds_calculated":         hdr.FDsCalculated,
		"fds_failed":             hdr.FDsFailed,
		"accrual_granularity":    hdr.Granularity,
	}

	isSimulation := strings.EqualFold(hdr.RunMode, "SIMULATION")
	payload := map[string]interface{}{
		"run_header":              hdr,
		"kpi":                     kpi,
		"run_mode":                hdr.RunMode,
		"is_simulation":           isSimulation,
		"posts_to_gl_on_approval": !isSimulation,
	}

	// ── Ledger rows (same for SIMULATION and FINAL) ───────────────────────────
	ledgerRows := []map[string]interface{}{}
	lR, lErr := pool.Query(ctx, `
		SELECT
			ledger_id,
			COALESCE(fd_id,''),
			COALESCE(fd_ref_no,''),
			COALESCE(bank_name,''),
			accrual_period_start,
			accrual_period_end,
			COALESCE(accrual_days,0),
			COALESCE(principal_amount,0),
			COALESCE(interest_rate,0),
			COALESCE(period_interest_accrued,0),
			COALESCE(tds_deducted_in_period,0),
			COALESCE(net_interest_in_period,0),
			COALESCE(closing_accrued_balance,0),
			COALESCE(ledger_row_status,''),
			COALESCE(formula_used,''),
			COALESCE(config_rounding_rule,'ROUND'),
			COALESCE(config_precision_decimals,2),
			COALESCE(req_period_interest,0),
			COALESCE(req_rounding_rule,'ROUND'),
			COALESCE(req_precision_decimals,2)
		FROM investment.fd_accrual_ledger
		WHERE run_id=$1 AND is_active=true AND is_deleted=false
		ORDER BY fd_id, accrual_period_start`, runID)
	if lErr == nil {
		defer lR.Close()
		for lR.Next() {
			var ledgerID, fdID, fdRefNo, bankName, rowStatus, formula string
			var configRoundRule, reqRoundRule string
			var lStart, lEnd time.Time
			var accrualDays, configDecimals, reqDecimals int
			var principal, rate, interest, tds, net, closing, reqInterest float64
			if sErr := lR.Scan(
				&ledgerID, &fdID, &fdRefNo, &bankName,
				&lStart, &lEnd, &accrualDays,
				&principal, &rate, &interest, &tds, &net, &closing,
				&rowStatus, &formula,
				&configRoundRule, &configDecimals,
				&reqInterest, &reqRoundRule, &reqDecimals,
			); sErr == nil {
				ledgerRows = append(ledgerRows, map[string]interface{}{
					"ledger_id":               ledgerID,
					"fd_id":                   fdID,
					"fd_ref_no":               fdRefNo,
					"bank_name":               bankName,
					"accrual_period_start":    lStart.Format(constants.DateFormat),
					"accrual_period_end":      lEnd.Format(constants.DateFormat),
					"accrual_days":            accrualDays,
					"principal_amount":        principal,
					"interest_rate":           rate,
					"period_interest_accrued": math.Round(interest*100) / 100,
					"tds_deducted_in_period":  math.Round(tds*100) / 100,
					"net_interest_in_period":  math.Round(net*100) / 100,
					"closing_accrued_balance": math.Round(closing*100) / 100,
					"ledger_row_status":       rowStatus,
					"formula_used":            formula,
					"config_rounding_rule":    configRoundRule,
					"config_precision_decimals": configDecimals,
					"req_period_interest":     math.Round(reqInterest*100) / 100,
					"req_rounding_rule":       reqRoundRule,
					"req_precision_decimals":  reqDecimals,
				})
			}
		}
	}

	// ── Enrich each ledger row with audit_trail + exception ──────────────
	for i, row := range ledgerRows {
			ledgerID, _ := row["ledger_id"].(string)
			if ledgerID == "" {
				ledgerRows[i]["audit_trail"] = []map[string]interface{}{}
				ledgerRows[i]["exception"] = nil
				continue
			}

			// Per-row audit trail
			auditRows := []map[string]interface{}{}
			aRows, aErr := pool.Query(ctx, `
				SELECT
					COALESCE(action_type,'')                                    AS action_type,
					COALESCE(processing_status,'')                              AS processing_status,
					COALESCE(requested_by,'')                                   AS requested_by,
					COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
					COALESCE(checker_by,'')                                     AS checker_by,
					COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
					COALESCE(checker_comment,'')                                AS checker_comment,
					COALESCE(old_ledger_row_status,'')                          AS old_status,
					COALESCE(old_period_interest_accrued,0)                     AS old_interest,
					COALESCE(old_closing_accrued_balance,0)                     AS old_closing_balance,
					COALESCE(old_formula_used,'')                               AS old_formula
				FROM investment.fd_accrual_ledger_audit
				WHERE ledger_id = $1
				ORDER BY requested_at DESC`, ledgerID)
			if aErr == nil {
				for aRows.Next() {
					var actionType, procStatus, reqBy, reqAt string
					var checkerBy, checkerAt, checkerComment string
					var oldStatus, oldFormula string
					var oldInterest, oldClosing float64
					if sErr := aRows.Scan(
						&actionType, &procStatus,
						&reqBy, &reqAt,
						&checkerBy, &checkerAt, &checkerComment,
						&oldStatus, &oldInterest, &oldClosing, &oldFormula,
					); sErr == nil {
						auditRows = append(auditRows, map[string]interface{}{
							"action_type":         actionType,
							"processing_status":   procStatus,
							"requested_by":        reqBy,
							"requested_at":        reqAt,
							"checker_by":          checkerBy,
							"checker_at":          checkerAt,
							"checker_comment":     checkerComment,
							"old_status":          oldStatus,
							"old_interest":        oldInterest,
							"old_closing_balance": oldClosing,
							"old_formula":         oldFormula,
						})
					}
				}
				aRows.Close()
			}
			ledgerRows[i]["audit_trail"] = auditRows

			// Per-row exception record
			var exceptionRecord interface{} = nil
			var exID, exType, exStatus string
			var exComputed, exProposed float64
			var exReasonCode, exReasonText string
			var exProposedBy, exApprovedBy, exCheckerComment string
			var exProposedAt, exApprovedAt interface{}
			exErr := pool.QueryRow(ctx, `
				SELECT
					COALESCE(exception_id,''),
					COALESCE(exception_type,''),
					COALESCE(exception_status,''),
					COALESCE(computed_amount,0),
					COALESCE(proposed_override_amount,0),
					COALESCE(override_reason_code,''),
					COALESCE(override_reason_text,''),
					COALESCE(proposed_by,''),
					proposed_at,
					COALESCE(approved_by,''),
					approved_at,
					COALESCE(checker_comment,'')
				FROM investment.fd_accrual_exception
				WHERE ledger_id = $1
				  AND COALESCE(is_deleted,false) = false
				ORDER BY created_at DESC LIMIT 1`, ledgerID,
			).Scan(
				&exID, &exType, &exStatus,
				&exComputed, &exProposed,
				&exReasonCode, &exReasonText,
				&exProposedBy, &exProposedAt,
				&exApprovedBy, &exApprovedAt,
				&exCheckerComment,
			)
			if exErr == nil && exID != "" {
				exceptionRecord = map[string]interface{}{
					"exception_id":             exID,
					"exception_type":           exType,
					"exception_status":         exStatus,
					"computed_amount":          exComputed,
					"proposed_override_amount": exProposed,
					"override_reason_code":     exReasonCode,
					"override_reason_text":     exReasonText,
					"proposed_by":              exProposedBy,
					"proposed_at":              exProposedAt,
					"approved_by":              exApprovedBy,
					"approved_at":              exApprovedAt,
					"checker_comment":          exCheckerComment,
				}
			}
			ledgerRows[i]["exception"] = exceptionRecord
		}

	payload["ledger"] = ledgerRows

	// ── Per-FD KPIs ───────────────────────────────────────────────────────
	fdKPIMap := map[string]map[string]interface{}{}
	for _, row := range ledgerRows {
			fdID := row["fd_id"].(string)
			entry, ok := fdKPIMap[fdID]
			if !ok {
				entry = map[string]interface{}{
					"fd_id":                  fdID,
					"fd_ref_no":              row["fd_ref_no"],
					"bank_name":              row["bank_name"],
					"total_interest_accrued": 0.0,
					"total_tds_deducted":     0.0,
					"total_net_accrued":      0.0,
					"sub_period_count":       0,
				}
				fdKPIMap[fdID] = entry
			}
			entry["total_interest_accrued"] = math.Round((entry["total_interest_accrued"].(float64)+row["period_interest_accrued"].(float64))*100) / 100
			entry["total_tds_deducted"] = math.Round((entry["total_tds_deducted"].(float64)+row["tds_deducted_in_period"].(float64))*100) / 100
			entry["total_net_accrued"] = math.Round((entry["total_net_accrued"].(float64)+row["net_interest_in_period"].(float64))*100) / 100
			entry["sub_period_count"] = entry["sub_period_count"].(int) + 1
		}
		fdKPIs := []map[string]interface{}{}
		for _, v := range fdKPIMap {
			fdKPIs = append(fdKPIs, v)
		}
	payload["fd_kpis"] = fdKPIs

	// ── Execution log ─────────────────────────────────────────────────────────
	execLog := []map[string]interface{}{}
	eR, eErr := pool.Query(ctx, `
		SELECT
			COALESCE(log_level,''),
			COALESCE(event_type,''),
			COALESCE(fd_id,''),
			COALESCE(message,''),
			logged_at
		FROM investment.fd_accrual_run_execution_log
		WHERE run_id=$1
		ORDER BY logged_at DESC LIMIT 200`, runID)
	if eErr == nil {
		defer eR.Close()
		for eR.Next() {
			var level, evtType, fdID, msg string
			var loggedAt time.Time
			if sErr := eR.Scan(&level, &evtType, &fdID, &msg, &loggedAt); sErr == nil {
				execLog = append(execLog, map[string]interface{}{
					"log_level":  level,
					"event_type": evtType,
					"fd_id":      fdID,
					"message":    msg,
					"logged_at":  loggedAt.Format(time.RFC3339),
				})
			}
		}
	}
	payload["execution_log"] = execLog

	return payload
}

// logAccrualEvent inserts a row into fd_accrual_run_execution_log.
// LogAccrualParams groups parameters for logAccrualEvent to keep signatures short.
type LogAccrualParams struct {
	RunID     string
	FDID      string
	Level     string
	EventType string
	Message   string
	Detail    map[string]interface{}
}

func logAccrualEvent(ctx context.Context, pool *pgxpool.Pool, p LogAccrualParams) {
	detailJSON := "{}"
	if p.Detail != nil {
		if b, err := json.Marshal(p.Detail); err == nil {
			detailJSON = string(b)
		}
	}
	fdIDPtr := interface{}(p.FDID)
	if p.FDID == "" {
		fdIDPtr = nil
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO investment.fd_accrual_run_execution_log (
			run_id, fd_id, log_level, event_type, message, detail, logged_at
		) VALUES ($1,$2,$3,$4,$5,$6::jsonb,now())`,
		p.RunID, fdIDPtr, p.Level, p.EventType, p.Message, detailJSON)
}

// ─── String helpers ───────────────────────────────────────────────────────────

func strSliceToCSV(sl []string) string {
	return strings.Join(sl, ",")
}

var _ = strSliceToCSV // suppress unused warning

// ─── Period Lock Status (TC-21) ───────────────────────────────────────────────

// CheckPeriodLockStatus reports whether a financial period (YYYY-MM) is locked
// for FINAL accrual runs. A period is considered locked when any FINAL run for
// the same entity + period has already been APPROVED/POSTED — i.e. once the
// books are closed, you cannot create another FINAL run for that period.
//
// Request:
//
//	{ "financial_period": "2026-04", "entity_id": "ENT-001" }
//
// Response:
//
//	{ "success": true, "rows": { "is_locked": true|false, "reason": "..." } }
func CheckPeriodLockStatus(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string `json:"user_id"`
			FinancialPeriod string `json:"financial_period"`
			EntityID        string `json:"entity_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		fp := strings.TrimSpace(req.FinancialPeriod)
		if fp == "" {
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"is_locked": false,
				"reason":    "no financial_period supplied",
			})
			return
		}

		ctx := r.Context()
		var posted int
		var lastStatus, lastRunID string
		query := `
			SELECT
				COUNT(*) FILTER (WHERE run_status IN ('APPROVED','POSTED','POSTED_TO_GL','LOCKED')) AS posted_runs,
				COALESCE(MAX(run_status),'') AS last_status,
				COALESCE(MAX(run_id),'')     AS last_run_id
			FROM investment.fd_accrual_run
			WHERE financial_period = $1
			  AND ($2 = '' OR entity_id = $2)
			  AND COALESCE(run_mode,'FINAL') = 'FINAL'`
		if err := pgxPool.QueryRow(ctx, query, fp, req.EntityID).Scan(&posted, &lastStatus, &lastRunID); err != nil {
			api.LogError("[FDAccrual] CheckPeriodLockStatus query error: %v", err)
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"is_locked": false,
				"reason":    "lock-status query failed; defaulting to unlocked",
			})
			return
		}

		isLocked := posted > 0
		reason := ""
		if isLocked {
			reason = fmt.Sprintf("Financial period %s is locked: a FINAL run is already %s (run_id=%s)", fp, lastStatus, lastRunID)
		} else {
			reason = fmt.Sprintf("Financial period %s is open for FINAL runs", fp)
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"is_locked":        isLocked,
			"financial_period": fp,
			"entity_id":        req.EntityID,
			"reason":           reason,
			"last_run_id":      lastRunID,
			"last_status":      lastStatus,
		})
	}
}
