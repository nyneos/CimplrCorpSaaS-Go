package fdAccrual

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	accountingworkbench "CimplrCorpSaas/api/investment/accountingWorkbench"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── helpers ────────────────────────────────────────────────────────────────

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

func coerceDateValue(v string) interface{} {
	if v == "" {
		return nil
	}
	return v
}

// ─── 1. CreateAccrualRun ─────────────────────────────────────────────────────

func CreateAccrualRun(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			RunMode     string   `json:"run_mode"`     // FULL or SIMULATION
			PeriodStart string   `json:"period_start"`
			PeriodEnd   string   `json:"period_end"`
			EntityIDs   []string `json:"entity_ids"`
			BankIDs     []string `json:"bank_ids"`
			FDIDs       []string `json:"fd_ids"`
			Description string   `json:"description"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.PeriodStart == "" || req.PeriodEnd == "" {
			api.RespondWithError(w, http.StatusBadRequest, "period_start and period_end are required")
			return
		}
		if req.RunMode == "" {
			req.RunMode = "FULL"
		}
		if req.RunMode != "FULL" && req.RunMode != "SIMULATION" {
			api.RespondWithError(w, http.StatusBadRequest, "run_mode must be FULL or SIMULATION")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var runID string
		err := pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_accrual_run (
				run_mode, period_start, period_end,
				scope_entity_ids, scope_bank_ids, scope_fd_ids,
				description, run_status, created_by, created_at
			) VALUES ($1, $2::date, $3::date, $4, $5, $6, $7, 'DRAFT', $8, now())
			RETURNING run_id`,
			req.RunMode, req.PeriodStart, req.PeriodEnd,
			nullIfEmpty(strings.Join(req.EntityIDs, ",")),
			nullIfEmpty(strings.Join(req.BankIDs, ",")),
			nullIfEmpty(strings.Join(req.FDIDs, ",")),
			nullIfEmpty(req.Description),
			userEmail,
		).Scan(&runID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Create accrual run failed: "+err.Error())
			return
		}

		// Audit
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_audit (
				run_id, action_type, processing_status, requested_by, requested_at
			) VALUES ($1, 'CREATE', 'DRAFT', $2, now())`, runID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":   runID,
			"run_mode": req.RunMode,
			"status":   "DRAFT",
		})
		api.LogInfo("[FDAccrual] CreateAccrualRun: run_id=%s mode=%s period=%s→%s", runID, req.RunMode, req.PeriodStart, req.PeriodEnd)
	}
}

// ─── 2. ValidateScope ────────────────────────────────────────────────────────

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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		ctx := r.Context()

		// Load run params from DB
		params, err := loadRunParams(ctx, pgxPool, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Load run failed: "+err.Error())
			return
		}

		fds, err := getFDsInScope(ctx, pgxPool, params)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Scope query failed: "+err.Error())
			return
		}

		findings := validateFDsForAccrual(fds, params)

		// Store findings
		for _, f := range findings {
			_, _ = pgxPool.Exec(ctx, `
				INSERT INTO investment.fd_accrual_validation_finding (
					run_id, fd_id, severity, code, message, created_at
				) VALUES ($1, $2, $3, $4, $5, now())`,
				req.RunID, f.FDID, f.Severity, f.Code, f.Message)
		}

		// Update run status
		newStatus := "VALIDATED"
		if hasErrors(findings) {
			newStatus = "VALIDATION_FAILED"
		}
		_, _ = pgxPool.Exec(ctx, `UPDATE investment.fd_accrual_run SET run_status=$1, fd_count=$2 WHERE run_id=$3`,
			newStatus, len(fds), req.RunID)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":     req.RunID,
			"fd_count":   len(fds),
			"findings":   len(findings),
			"has_errors": hasErrors(findings),
			"status":     newStatus,
		})
		api.LogInfo("[FDAccrual] ValidateScope: run_id=%s fds=%d findings=%d status=%s", req.RunID, len(fds), len(findings), newStatus)
	}
}

// ─── 3. RunAccrual ───────────────────────────────────────────────────────────

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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		params, err := loadRunParams(ctx, pgxPool, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Load run failed: "+err.Error())
			return
		}

		fds, err := getFDsInScope(ctx, pgxPool, params)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Scope query failed: "+err.Error())
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Begin tx failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		startTime := time.Now()
		var totalGross, totalTDS, totalNet float64

		for _, fd := range fds {
			priorClosing := getPriorRunClosingBalance(ctx, tx, fd.FDID, params.PeriodStart)
			result := calculateAccrualForFD(fd, params, priorClosing)

			// UPSERT ledger row
			_, err := tx.Exec(ctx, `
				INSERT INTO investment.fd_accrual_ledger (
					run_id, fd_id, period_start, period_end,
					days_in_period, day_count_used,
					opening_principal, gross_interest, tds_amount, net_interest,
					closing_balance, cumulative_accrual,
					is_overridden, created_at
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,false,now())
				ON CONFLICT (run_id, fd_id) DO UPDATE SET
					period_start = EXCLUDED.period_start,
					period_end = EXCLUDED.period_end,
					days_in_period = EXCLUDED.days_in_period,
					day_count_used = EXCLUDED.day_count_used,
					opening_principal = EXCLUDED.opening_principal,
					gross_interest = EXCLUDED.gross_interest,
					tds_amount = EXCLUDED.tds_amount,
					net_interest = EXCLUDED.net_interest,
					closing_balance = EXCLUDED.closing_balance,
					cumulative_accrual = EXCLUDED.cumulative_accrual,
					updated_at = now()`,
				req.RunID, fd.FDID, params.PeriodStart, params.PeriodEnd,
				result.DaysInPeriod, fd.DayCountConvention,
				result.OpeningPrincipal, result.GrossInterest, result.TDSAmount, result.NetInterest,
				result.ClosingPrincipal, result.CumulativeAccrual,
			)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("Ledger upsert failed for %s: %v", fd.FDID, err))
				return
			}

			// Execution log
			_, _ = tx.Exec(ctx, `
				INSERT INTO investment.fd_accrual_execution_log (
					run_id, fd_id, step, status, detail, logged_at
				) VALUES ($1,$2,'CALCULATE','SUCCESS',$3,now())`,
				req.RunID, fd.FDID,
				fmt.Sprintf("gross=%.2f tds=%.2f net=%.2f days=%d", result.GrossInterest, result.TDSAmount, result.NetInterest, result.DaysInPeriod))

			totalGross += result.GrossInterest
			totalTDS += result.TDSAmount
			totalNet += result.NetInterest
		}

		elapsed := time.Since(startTime)

		// Update run summary
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_accrual_run
			SET run_status = 'COMPUTED',
			    fd_count = $1,
			    total_gross_interest = $2,
			    total_tds = $3,
			    total_net_interest = $4,
			    execution_time_ms = $5,
			    computed_at = now(),
			    computed_by = $6
			WHERE run_id = $7`,
			len(fds),
			math.Round(totalGross*100)/100,
			math.Round(totalTDS*100)/100,
			math.Round(totalNet*100)/100,
			elapsed.Milliseconds(),
			userEmail,
			req.RunID,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Update run summary failed: "+err.Error())
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":         req.RunID,
			"status":         "COMPUTED",
			"fd_count":       len(fds),
			"total_gross":    math.Round(totalGross*100) / 100,
			"total_tds":      math.Round(totalTDS*100) / 100,
			"total_net":      math.Round(totalNet*100) / 100,
			"execution_ms":   elapsed.Milliseconds(),
		})
		api.LogInfo("[FDAccrual] RunAccrual: run_id=%s fds=%d gross=%.2f net=%.2f elapsed=%dms",
			req.RunID, len(fds), totalGross, totalNet, elapsed.Milliseconds())
	}
}

// ─── 4. GetAccrualLedger ─────────────────────────────────────────────────────

func GetAccrualLedger(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				l.ledger_id, l.run_id, l.fd_id,
				l.period_start, l.period_end, l.days_in_period,
				COALESCE(l.day_count_used,'ACT_365'),
				l.opening_principal, l.gross_interest, l.tds_amount, l.net_interest,
				l.closing_balance, l.cumulative_accrual,
				l.is_overridden,
				COALESCE(l.override_amount,0),
				COALESCE(l.override_reason,''),
				COALESCE(fm.entity_id,'') AS entity_id,
				COALESCE(fm.bank_id,'') AS bank_id,
				COALESCE(fm.bank_fd_reference,'') AS bank_fd_ref
			FROM investment.fd_accrual_ledger l
			LEFT JOIN investment.fd_master fm ON fm.fd_id = l.fd_id
			WHERE l.run_id = $1
			ORDER BY fm.entity_id, l.fd_id`,
			req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		ledger := make([]map[string]interface{}, 0)
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
			ledger = append(ledger, row)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row error: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id": req.RunID,
			"count":  len(ledger),
			"ledger": ledger,
		})
	}
}

// ─── 5. GetAccrualCalculationDetail ──────────────────────────────────────────

func GetAccrualCalculationDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			RunID  string `json:"run_id"`
			FDID   string `json:"fd_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" || req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "run_id and fd_id are required")
			return
		}

		ctx := r.Context()

		// Ledger row
		var ledger map[string]interface{}
		row, err := pgxPool.Query(ctx, `
			SELECT l.*, fm.entity_id, fm.bank_id, fm.principal_amount, fm.interest_rate,
			       fm.interest_type, fm.day_count_convention, fm.value_date, fm.maturity_date
			FROM investment.fd_accrual_ledger l
			LEFT JOIN investment.fd_master fm ON fm.fd_id = l.fd_id
			WHERE l.run_id = $1 AND l.fd_id = $2`, req.RunID, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer row.Close()

		fields := row.FieldDescriptions()
		if row.Next() {
			vals, _ := row.Values()
			ledger = make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					ledger[string(f.Name)] = ""
				} else {
					ledger[string(f.Name)] = vals[i]
				}
			}
		}
		row.Close()

		if ledger == nil {
			api.RespondWithPayload(w, false, "No ledger entry found for this run/fd", nil)
			return
		}

		// Override history
		overrides := make([]map[string]interface{}, 0)
		oRows, err := pgxPool.Query(ctx, `
			SELECT override_id, override_amount, override_reason, proposed_by, proposed_at,
			       COALESCE(approved_by,'') AS approved_by, approved_at, override_status
			FROM investment.fd_accrual_override
			WHERE run_id = $1 AND fd_id = $2
			ORDER BY proposed_at DESC`, req.RunID, req.FDID)
		if err == nil {
			defer oRows.Close()
			oFields := oRows.FieldDescriptions()
			for oRows.Next() {
				vals, _ := oRows.Values()
				m := make(map[string]interface{}, len(oFields))
				for i, f := range oFields {
					if vals[i] == nil {
						m[string(f.Name)] = ""
					} else {
						m[string(f.Name)] = vals[i]
					}
				}
				overrides = append(overrides, m)
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"ledger":    ledger,
			"overrides": overrides,
		})
	}
}

// ─── 6. SubmitForApproval ────────────────────────────────────────────────────

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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Check run status and mode — SIMULATION runs cannot be submitted
		var runStatus, runMode, entityID string
		var totalNet float64
		err := pgxPool.QueryRow(ctx, `
			SELECT run_status, run_mode, COALESCE(scope_entity_ids,''), COALESCE(total_net_interest,0)
			FROM investment.fd_accrual_run WHERE run_id = $1`, req.RunID,
		).Scan(&runStatus, &runMode, &entityID, &totalNet)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Run not found: "+err.Error())
			return
		}
		if runMode == "SIMULATION" {
			api.RespondWithError(w, http.StatusBadRequest, "SIMULATION runs cannot be submitted for approval")
			return
		}
		if runStatus != "COMPUTED" {
			api.RespondWithError(w, http.StatusBadRequest, "Only COMPUTED runs can be submitted (current: "+runStatus+")")
			return
		}

		// Update status
		_, err = pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_run SET run_status='PENDING_APPROVAL', submitted_by=$1, submitted_at=now()
			WHERE run_id=$2`, userEmail, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Status update failed: "+err.Error())
			return
		}

		// Audit
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_audit (
				run_id, action_type, processing_status, requested_by, requested_at
			) VALUES ($1, 'SUBMIT', 'PENDING_APPROVAL', $2, now())`, req.RunID, userEmail)

		// Fire approval engine
		go func(runID, uID, uEmail, eID string, amount float64) {
			bgCtx := context.Background()
			instID, err := approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:      "FIXED_DEPOSIT",
				EntityCode:      eID,
				TransactionType: "FD_ACCRUAL_RUN",
				RecordID:        runID,
				RecordTable:     "investment.fd_accrual_run",
				AuditTable:      "investment.fd_accrual_audit",
				AuditIDColumn:   "run_id",
				ActionType:      "CREATE",
				Amount:          amount,
				SubmittedBy:     uID,
				SubmittedByEmail: uEmail,
			})
			if err != nil {
				api.LogError("[FDAccrual] CreateInstance failed for run %s: %v", runID, err)
				return
			}
			if instID != "" {
				api.LogInfo("[FDAccrual] CreateInstance %s → run %s PENDING_APPROVAL", instID, runID)
			} else {
				api.LogInfo("[FDAccrual] No matrix for run %s — stays PENDING_APPROVAL", runID)
			}
		}(req.RunID, req.UserID, userEmail, entityID, totalNet)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id": req.RunID,
			"status": "PENDING_APPROVAL",
		})
		api.LogInfo("[FDAccrual] SubmitForApproval: run_id=%s", req.RunID)
	}
}

// ─── 7. BulkApproveAccrualRun ────────────────────────────────────────────────
// Engine-first: calls RecordAction on the approval instance. On final approval
// (all eyes done), posts journal entries and locks the period.

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

			// Step 1: Find active approval instance eye
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
				// No engine instance — direct approve
				_, dbErr := pgxPool.Exec(ctx, `
					UPDATE investment.fd_accrual_run
					SET run_status = 'APPROVED', approved_by = $1, approved_at = now()
					WHERE run_id = $2 AND run_status = 'PENDING_APPROVAL'`,
					userEmail, runID)
				if dbErr != nil {
					res[constants.ValueSuccess] = false
					res[constants.ValueError] = "Direct approve failed: " + dbErr.Error()
					results = append(results, res)
					continue
				}

				// Post journals for this run
				if jErr := postAccrualJournals(ctx, pgxPool, runID, userEmail); jErr != nil {
					api.LogError("[FDAccrual] Journal posting failed for approved run %s: %v", runID, jErr)
				}

				_, _ = pgxPool.Exec(ctx, `
					INSERT INTO investment.fd_accrual_audit (
						run_id, action_type, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment
					) VALUES ($1, 'APPROVE', 'APPROVED', $2, now(), $2, now(), $3)`, runID, userEmail, req.Comment)

				res[constants.ValueSuccess] = true
				res["status"] = "APPROVED"
				results = append(results, res)
				continue
			}

			// Step 2: Engine-first — RecordAction
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

			// Step 3: Check if instance is now fully approved
			var instStatus string
			_ = pgxPool.QueryRow(ctx, `
				SELECT status FROM uam.approval_instance
				WHERE record_id = $1 AND module_code = 'FIXED_DEPOSIT' AND status != 'CANCELLED'
				ORDER BY submitted_at DESC LIMIT 1`, runID,
			).Scan(&instStatus)

			if instStatus == approvalengine.InstStatusApproved {
				// Final approval — post journals in a separate tx per run
				if jErr := postAccrualJournals(ctx, pgxPool, runID, userEmail); jErr != nil {
					api.LogError("[FDAccrual] Journal posting failed for approved run %s: %v", runID, jErr)
					res["journal_error"] = jErr.Error()
				} else {
					res["journals_posted"] = true
				}

				// Lock the period
				var periodStart, periodEnd time.Time
				_ = pgxPool.QueryRow(ctx, `SELECT period_start, period_end FROM investment.fd_accrual_run WHERE run_id=$1`, runID).Scan(&periodStart, &periodEnd)
				if !periodStart.IsZero() {
					_, _ = pgxPool.Exec(ctx, `
						INSERT INTO investment.fd_accrual_period_lock (period_month, locked_by_run_id, locked_by, locked_at)
						VALUES ($1, $2, $3, now())
						ON CONFLICT (period_month) DO UPDATE SET locked_by_run_id = EXCLUDED.locked_by_run_id, locked_by = EXCLUDED.locked_by, locked_at = now()`,
						buildAccrualPeriod(periodStart), runID, userEmail)
					res["period_locked"] = buildAccrualPeriod(periodStart)
				}

				res["status"] = "APPROVED"
			} else {
				res["status"] = "PENDING_APPROVAL"
			}

			results = append(results, res)
		}

		api.RespondWithPayload(w, true, "", results)
		api.LogInfo("[FDAccrual] BulkApproveAccrualRun: %d runs processed", len(req.RunIDs))
	}
}

// ─── 8. BulkRejectAccrualRun ─────────────────────────────────────────────────

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
				// Direct reject
				_, _ = pgxPool.Exec(ctx, `
					UPDATE investment.fd_accrual_run SET run_status='REJECTED' WHERE run_id=$1 AND run_status='PENDING_APPROVAL'`, runID)
				_, _ = pgxPool.Exec(ctx, `
					INSERT INTO investment.fd_accrual_audit (
						run_id, action_type, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment
					) VALUES ($1, 'REJECT', 'REJECTED', $2, now(), $2, now(), $3)`, runID, userEmail, req.Comment)
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

			res[constants.ValueSuccess] = true
			res["status"] = "REJECTED"
			results = append(results, res)
		}

		api.RespondWithPayload(w, true, "", results)
		api.LogInfo("[FDAccrual] BulkRejectAccrualRun: %d runs", len(req.RunIDs))
	}
}

// ─── 9. GetAccrualRuns ───────────────────────────────────────────────────────

func GetAccrualRuns(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			WITH engine AS (
				SELECT
					i.record_id,
					i.instance_id,
					i.status AS engine_status,
					i.submitted_by_email AS engine_submitted_by,
					i.submitted_at AS engine_submitted_at,
					i.resolved_at AS engine_resolved_at,
					i.resolved_by_email AS engine_resolved_by
				FROM uam.approval_instance i
				WHERE i.module_code = 'FIXED_DEPOSIT'
				  AND i.transaction_type = 'FD_ACCRUAL_RUN'
				  AND i.is_deleted = false
			)
			SELECT
				r.run_id, r.run_mode, r.run_status,
				r.period_start, r.period_end,
				COALESCE(r.scope_entity_ids,''), COALESCE(r.scope_bank_ids,''),
				COALESCE(r.fd_count,0),
				COALESCE(r.total_gross_interest,0), COALESCE(r.total_tds,0), COALESCE(r.total_net_interest,0),
				COALESCE(r.description,''),
				COALESCE(r.created_by,''), r.created_at,
				COALESCE(r.submitted_by,''), r.submitted_at,
				COALESCE(r.approved_by,''), r.approved_at,
				COALESCE(r.computed_by,''), r.computed_at,
				COALESCE(r.execution_time_ms,0),
				COALESCE(e.instance_id,'') AS engine_instance_id,
				COALESCE(e.engine_status,'') AS engine_status,
				COALESCE(e.engine_submitted_by,'') AS engine_submitted_by,
				e.engine_submitted_at,
				e.engine_resolved_at,
				COALESCE(e.engine_resolved_by,'') AS engine_resolved_by
			FROM investment.fd_accrual_run r
			LEFT JOIN LATERAL (
				SELECT * FROM engine WHERE engine.record_id = r.run_id::text ORDER BY engine.engine_submitted_at DESC LIMIT 1
			) e ON true
			WHERE COALESCE(r.is_deleted, false) = false
			ORDER BY r.created_at DESC`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		runs := make([]map[string]interface{}, 0)
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
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"count": len(runs),
			"runs":  runs,
		})
	}
}

// ─── 10. GetValidationFindings ───────────────────────────────────────────────

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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT finding_id, fd_id, severity, code, message, created_at
			FROM investment.fd_accrual_validation_finding
			WHERE run_id = $1
			ORDER BY severity DESC, fd_id`, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
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

// ─── 11. GetExecutionLog ─────────────────────────────────────────────────────

func GetExecutionLog(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT log_id, run_id, fd_id, step, status, detail, logged_at
			FROM investment.fd_accrual_execution_log
			WHERE run_id = $1
			ORDER BY logged_at`, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
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

// ─── 12. ProposeOverride ─────────────────────────────────────────────────────

func ProposeOverride(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string  `json:"user_id"`
			RunID          string  `json:"run_id"`
			FDID           string  `json:"fd_id"`
			OverrideAmount float64 `json:"override_amount"`
			Reason         string  `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.RunID == "" || req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "run_id and fd_id are required")
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

		var overrideID string
		err := pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_accrual_override (
				run_id, fd_id, override_amount, override_reason,
				proposed_by, proposed_at, override_status
			) VALUES ($1, $2, $3, $4, $5, now(), 'PROPOSED')
			RETURNING override_id`,
			req.RunID, req.FDID, req.OverrideAmount, req.Reason, userEmail,
		).Scan(&overrideID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert override failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"override_id": overrideID,
			"status":      "PROPOSED",
		})
		api.LogInfo("[FDAccrual] ProposeOverride: override_id=%s run=%s fd=%s amount=%.2f", overrideID, req.RunID, req.FDID, req.OverrideAmount)
	}
}

// ─── 13. ApproveOverride ─────────────────────────────────────────────────────
// Maker ≠ Checker: the approver must not be the proposer.

func ApproveOverride(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			OverrideID string `json:"override_id"`
			Comment    string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.OverrideID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "override_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Maker ≠ checker check
		var proposedBy, runID, fdID string
		var overrideAmount float64
		err := pgxPool.QueryRow(ctx, `
			SELECT proposed_by, run_id, fd_id, override_amount
			FROM investment.fd_accrual_override
			WHERE override_id = $1 AND override_status = 'PROPOSED'`, req.OverrideID,
		).Scan(&proposedBy, &runID, &fdID, &overrideAmount)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Override not found or not in PROPOSED status")
			return
		}
		if proposedBy == userEmail {
			api.RespondWithError(w, http.StatusForbidden, "Maker cannot approve their own override (maker ≠ checker)")
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Begin tx failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Update override status
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_accrual_override
			SET override_status = 'APPROVED', approved_by = $1, approved_at = now(), approver_comment = $2
			WHERE override_id = $3`, userEmail, req.Comment, req.OverrideID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Override approve failed: "+err.Error())
			return
		}

		// Apply override to ledger
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_accrual_ledger
			SET is_overridden = true, override_amount = $1, override_reason = $2, updated_at = now()
			WHERE run_id = $3 AND fd_id = $4`, overrideAmount, "Override approved by "+userEmail, runID, fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Ledger override failed: "+err.Error())
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"override_id": req.OverrideID,
			"status":      "APPROVED",
		})
		api.LogInfo("[FDAccrual] ApproveOverride: override_id=%s by=%s", req.OverrideID, userEmail)
	}
}

// ─── 14. RejectOverride ──────────────────────────────────────────────────────

func RejectOverride(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			OverrideID string `json:"override_id"`
			Comment    string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.OverrideID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "override_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		_, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_override
			SET override_status = 'REJECTED', approved_by = $1, approved_at = now(), approver_comment = $2
			WHERE override_id = $3 AND override_status = 'PROPOSED'`,
			userEmail, req.Comment, req.OverrideID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Override reject failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"override_id": req.OverrideID,
			"status":      "REJECTED",
		})
		api.LogInfo("[FDAccrual] RejectOverride: override_id=%s by=%s", req.OverrideID, userEmail)
	}
}

// ─── Internal helpers ────────────────────────────────────────────────────────

// loadRunParams fetches the run row and builds AccrualRunParams.
func loadRunParams(ctx context.Context, exec queryExecutor, runID string) (*AccrualRunParams, error) {
	var p AccrualRunParams
	var entityCSV, bankCSV, fdCSV string
	err := exec.QueryRow(ctx, `
		SELECT run_mode, period_start, period_end,
		       COALESCE(scope_entity_ids,''), COALESCE(scope_bank_ids,''), COALESCE(scope_fd_ids,'')
		FROM investment.fd_accrual_run WHERE run_id = $1`, runID,
	).Scan(&p.RunMode, &p.PeriodStart, &p.PeriodEnd, &entityCSV, &bankCSV, &fdCSV)
	if err != nil {
		return nil, fmt.Errorf("loadRunParams: %w", err)
	}
	if entityCSV != "" {
		p.EntityIDs = strings.Split(entityCSV, ",")
	}
	if bankCSV != "" {
		p.BankIDs = strings.Split(bankCSV, ",")
	}
	if fdCSV != "" {
		p.FDIDs = strings.Split(fdCSV, ",")
	}
	if p.RoundDecimals == 0 {
		p.RoundDecimals = 2
	}
	return &p, nil
}

// postAccrualJournals creates journal entries for every ledger row of an approved run.
// Each run gets its own inner transaction so a single FD failure doesn't block others.
func postAccrualJournals(ctx context.Context, pool *pgxpool.Pool, runID, userEmail string) error {
	rows, err := pool.Query(ctx, `
		SELECT l.fd_id, l.net_interest, l.period_start, l.period_end,
		       COALESCE(fm.entity_id,'') AS entity_id,
		       COALESCE(fm.bank_account_id,'') AS bank_account_id
		FROM investment.fd_accrual_ledger l
		LEFT JOIN investment.fd_master fm ON fm.fd_id = l.fd_id
		WHERE l.run_id = $1`, runID)
	if err != nil {
		return fmt.Errorf("postAccrualJournals query: %w", err)
	}
	defer rows.Close()

	type ledgerRow struct {
		fdID, entityID, bankAccountID string
		netInterest                   float64
		periodStart, periodEnd        time.Time
	}
	var ledgerRows []ledgerRow
	for rows.Next() {
		var lr ledgerRow
		if err := rows.Scan(&lr.fdID, &lr.netInterest, &lr.periodStart, &lr.periodEnd, &lr.entityID, &lr.bankAccountID); err != nil {
			return fmt.Errorf("postAccrualJournals scan: %w", err)
		}
		ledgerRows = append(ledgerRows, lr)
	}
	rows.Close()

	for _, lr := range ledgerRows {
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogError("[FDAccrual] Journal inner tx begin failed for fd %s: %v", lr.fdID, err)
			continue
		}

		// Create accounting activity
		var activityID string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_activity (
				activity_type, activity_subtype, effective_date, accounting_period, data_source, status
			) VALUES ('FIXED_DEPOSIT', 'ACCRUAL', $1, $2, 'FD_ACCRUAL', 'APPROVED')
			RETURNING activity_id`,
			lr.periodEnd, buildAccrualPeriod(lr.periodEnd),
		).Scan(&activityID)
		if err != nil {
			tx.Rollback(ctx) //nolint:errcheck
			api.LogError("[FDAccrual] Create activity failed for fd %s: %v", lr.fdID, err)
			continue
		}

		// Audit for activity
		_, _ = tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (
				activity_id, actiontype, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment
			) VALUES ($1, 'CREATE', 'APPROVED', $2, now(), $2, now(), $3)`,
			activityID, userEmail, fmt.Sprintf("FD accrual run %s", runID))

		amount := math.Round(lr.netInterest*100) / 100

		// Build journal entry: debit interest receivable, credit interest income
		je := &accountingworkbench.JournalEntry{
			ActivityID:       activityID,
			EntityID:         lr.entityID,
			EntityName:       lr.entityID,
			EntryDate:        lr.periodEnd,
			AccountingPeriod: buildAccrualPeriod(lr.periodEnd),
			EntryType:        "FD_ACCRUAL",
			Description:      fmt.Sprintf("FD accrual %s period %s→%s", lr.fdID, lr.periodStart.Format("2006-01-02"), lr.periodEnd.Format("2006-01-02")),
			TotalDebit:       amount,
			TotalCredit:      amount,
			Lines: []accountingworkbench.JournalEntryLine{
				{
					LineNumber:    1,
					AccountNumber: "INTEREST_RECEIVABLE",
					AccountName:   "Interest Receivable on FD",
					AccountType:   "ASSET",
					DebitAmount:   amount,
					CreditAmount:  0,
					Narration:     fmt.Sprintf("FD accrual %s", lr.fdID),
				},
				{
					LineNumber:    2,
					AccountNumber: "INTEREST_INCOME_FD",
					AccountName:   "Interest Income - Fixed Deposit",
					AccountType:   "INCOME",
					DebitAmount:   0,
					CreditAmount:  amount,
					Narration:     fmt.Sprintf("FD accrual %s", lr.fdID),
				},
			},
		}

		// Save journal entry
		var entryID string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_journal_entry (
				activity_id, entity_id, entity_name, folio_id, demat_id, entry_date,
				accounting_period, entry_type, description, total_debit, total_credit, status, created_by
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'POSTED',$12)
			RETURNING entry_id`,
			je.ActivityID, je.EntityID, je.EntityName, je.FolioID, je.DematID, je.EntryDate,
			je.AccountingPeriod, je.EntryType, fmt.Sprintf("%s | fd_id=%s | run_id=%s", je.Description, lr.fdID, runID),
			je.TotalDebit, je.TotalCredit, userEmail,
		).Scan(&entryID)
		if err != nil {
			tx.Rollback(ctx) //nolint:errcheck
			api.LogError("[FDAccrual] Insert journal entry failed for fd %s: %v", lr.fdID, err)
			continue
		}

		for _, line := range je.Lines {
			_, err = tx.Exec(ctx, `
				INSERT INTO investment.accounting_journal_entry_line (
					entry_id, line_number, account_number, account_name, account_type,
					debit_amount, credit_amount, scheme_id, folio_id, demat_id, narration
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
				entryID, line.LineNumber, line.AccountNumber, line.AccountName, line.AccountType,
				line.DebitAmount, line.CreditAmount, line.SchemeID, line.FolioID, line.DematID,
				fmt.Sprintf("%s | fd_id=%s | run_id=%s", line.Narration, lr.fdID, runID),
			)
			if err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				api.LogError("[FDAccrual] Insert journal line failed for fd %s: %v", lr.fdID, err)
				break
			}
		}

		if err == nil {
			if cErr := tx.Commit(ctx); cErr != nil {
				api.LogError("[FDAccrual] Journal commit failed for fd %s: %v", lr.fdID, cErr)
			} else {
				api.LogInfo("[FDAccrual] Journal posted for fd %s run %s entry=%s", lr.fdID, runID, entryID)
			}
		}
	}

	return nil
}
