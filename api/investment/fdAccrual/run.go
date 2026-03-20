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
			req.RunType = "MANUAL"
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
			CreatedBy:          userEmail,
		}
		var parseErr error
		input.AccrualPeriodStart, parseErr = time.Parse("2006-01-02", req.AccrualPeriodStart)
		if parseErr != nil {
			api.RespondWithError(w, http.StatusBadRequest, "accrual_period_start must be YYYY-MM-DD")
			return
		}
		input.AccrualPeriodEnd, parseErr = time.Parse("2006-01-02", req.AccrualPeriodEnd)
		if parseErr != nil {
			api.RespondWithError(w, http.StatusBadRequest, "accrual_period_end must be YYYY-MM-DD")
			return
		}
		if input.FinancialPeriod == "" {
			input.FinancialPeriod = buildAccrualPeriod(input.AccrualPeriodStart)
		}

		runID, err := createAccrualRunInternal(ctx, pgxPool, input)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Create accrual run failed: "+err.Error())
			return
		}

		// Store inclusion list if any
		if len(req.FDInclusionList) > 0 {
			_, _ = pgxPool.Exec(ctx,
				`UPDATE investment.fd_accrual_run SET fd_inclusion_list = $1 WHERE run_id = $2`,
				fdListJSON, runID)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":   runID,
			"run_mode": req.RunMode,
			"status":   "DRAFT",
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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		ctx := r.Context()

		eligible, blockers, err := validateAndPersistFindings(ctx, pgxPool, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Validation failed: "+err.Error())
			return
		}

		newStatus := "VALIDATED"
		if blockers > 0 {
			newStatus = "VALIDATION_FAILED"
		}
		_, _ = pgxPool.Exec(ctx,
			`UPDATE investment.fd_accrual_run SET run_status=$1, fds_in_scope=$2 WHERE run_id=$3`,
			newStatus, eligible, req.RunID)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":        req.RunID,
			"eligible_fds":  eligible,
			"blocker_count": blockers,
			"has_blockers":  blockers > 0,
			"status":        newStatus,
		})
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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		calculated, failed, err := executeAccrualRun(ctx, pgxPool, req.RunID, userEmail)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Accrual run failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":     req.RunID,
			"status":     "COMPUTED",
			"calculated": calculated,
			"failed":     failed,
		})
		api.LogInfo("[FDAccrual] RunAccrual: run_id=%s calculated=%d failed=%d", req.RunID, calculated, failed)
	}
}

// ─── 4. GetAccrualLedger ──────────────────────────────────────────────────────

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
				COALESCE(l.fd_ref_no,'')        AS fd_ref_no,
				COALESCE(l.bank_id,'')          AS bank_id,
				COALESCE(l.bank_name,'')        AS bank_name,
				COALESCE(l.entity_id,'')        AS entity_id,
				COALESCE(l.entity_name,'')      AS entity_name,
				l.accrual_period_start, l.accrual_period_end, l.accrual_days,
				COALESCE(l.day_count_code,'ACT_365') AS day_count_code,
				COALESCE(l.opening_principal,0),
				COALESCE(l.period_interest_accrued,0),
				COALESCE(l.closing_accrued_balance,0),
				COALESCE(l.tds_deducted_in_period,0),
				COALESCE(l.net_interest_in_period,0),
				COALESCE(l.is_overridden,false),
				COALESCE(l.ledger_row_status,''),
				COALESCE(l.formula_used,''),
				COALESCE(l.journal_entry_id,'')
			FROM investment.fd_accrual_ledger l
			WHERE l.run_id = $1
			  AND COALESCE(l.is_deleted,false) = false
			ORDER BY l.entity_id, l.bank_name, l.fd_id`,
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

// ─── 5. GetAccrualCalculationDetail ───────────────────────────────────────────

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
		row, err := pgxPool.Query(ctx, `
			SELECT l.*,
			       COALESCE(fm.interest_type_code,'') AS fd_interest_type,
			       COALESCE(fm.day_count_code,'')     AS fd_day_count_code,
			       fm.start_date                      AS fd_start_date,
			       fm.maturity_date                   AS fd_maturity_date
			FROM investment.fd_accrual_ledger l
			LEFT JOIN investment.fd_master fm ON fm.fd_id = l.fd_id
			WHERE l.run_id = $1 AND l.fd_id = $2
			  AND COALESCE(l.is_deleted,false) = false`,
			req.RunID, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer row.Close()

		fields := row.FieldDescriptions()
		var ledger map[string]interface{}
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
		if err := row.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row error: "+err.Error())
			return
		}
		if ledger == nil {
			api.RespondWithPayload(w, false, "No ledger entry found for this run/fd", nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"ledger": ledger})
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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
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
		if runMode == "SIMULATION" {
			api.RespondWithError(w, http.StatusBadRequest, "SIMULATION runs cannot be submitted for approval")
			return
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
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		query := `
			SELECT
				r.run_id, r.run_type, r.run_mode, r.run_status,
				COALESCE(r.entity_id,''), COALESCE(r.entity_name,''),
				r.accrual_period_start, r.accrual_period_end,
				COALESCE(r.financial_period,''),
				COALESCE(r.fds_in_scope,0),
				COALESCE(r.fds_calculated,0), COALESCE(r.fds_failed,0),
				COALESCE(r.total_interest_accrued,0),
				COALESCE(r.total_tds_deducted,0),
				COALESCE(r.run_mode,''),
				COALESCE(r.created_by,''), r.created_at,
				COALESCE(r.submitted_by,''), r.submitted_at,
				r.posting_completed_at,
				(SELECT COUNT(*) FROM investment.fd_accrual_ledger l
				 WHERE l.run_id = r.run_id AND COALESCE(l.is_deleted,false) = false) AS ledger_count
			FROM investment.fd_accrual_run r
			WHERE COALESCE(r.is_deleted, false) = false`
		args := []interface{}{}
		if req.EntityID != "" {
			query += " AND r.entity_id = $1"
			args = append(args, req.EntityID)
		}
		query += " ORDER BY r.created_at DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
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
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row error: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"count": len(runs),
			"runs":  runs,
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
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
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

// ─── 11. GetExecutionLog ──────────────────────────────────────────────────────

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
			SELECT log_id, run_id,
			       COALESCE(fd_id,'') AS fd_id,
			       log_level, event_type, message,
			       COALESCE(detail::text,'{}') AS detail,
			       logged_at
			FROM investment.fd_accrual_run_execution_log
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

// ─── 12. ProposeOverride ──────────────────────────────────────────────────────

func ProposeOverride(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                string  `json:"user_id"`
			RunID                 string  `json:"run_id"`
			FDID                  string  `json:"fd_id"`
			OverrideAmount        float64 `json:"override_amount"`
			OverrideReasonCode    string  `json:"override_reason_code"`
			OverrideReasonText    string  `json:"override_reason_text"`
			OverrideEffPeriod     string  `json:"override_effective_period"`
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
		_, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_ledger
			SET is_overridden = true,
			    override_amount = $1,
			    override_reason_code = $2,
			    override_reason_text = $3,
			    override_effective_period = $4,
			    override_status = 'PROPOSED',
			    override_proposed_by = $5,
			    override_proposed_at = now(),
			    updated_at = now()
			WHERE run_id = $6 AND fd_id = $7`,
			req.OverrideAmount, nullIfEmpty(req.OverrideReasonCode),
			nullIfEmpty(req.OverrideReasonText), nullIfEmpty(req.OverrideEffPeriod),
			userEmail, req.RunID, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Propose override failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":          req.RunID,
			"fd_id":           req.FDID,
			"override_status": "PROPOSED",
		})
		api.LogInfo("[FDAccrual] ProposeOverride: run=%s fd=%s amount=%.2f by=%s",
			req.RunID, req.FDID, req.OverrideAmount, userEmail)
	}
}

// ─── 13. ApproveOverride ──────────────────────────────────────────────────────

func ApproveOverride(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondWithError(w, http.StatusBadRequest, "run_id and fd_id are required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Maker ≠ checker
		var proposedBy string
		_ = pgxPool.QueryRow(ctx,
			`SELECT COALESCE(override_proposed_by,'') FROM investment.fd_accrual_ledger WHERE run_id=$1 AND fd_id=$2`,
			req.RunID, req.FDID,
		).Scan(&proposedBy)
		if proposedBy == userEmail {
			api.RespondWithError(w, http.StatusForbidden, "Maker cannot approve their own override")
			return
		}

		_, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_ledger
			SET override_status = 'APPROVED',
			    override_approved_by = $1,
			    override_approved_at = now(),
			    override_checker_comment = $2,
			    updated_at = now()
			WHERE run_id = $3 AND fd_id = $4 AND override_status = 'PROPOSED'`,
			userEmail, nullIfEmpty(req.Comment), req.RunID, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Approve override failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":          req.RunID,
			"fd_id":           req.FDID,
			"override_status": "APPROVED",
		})
		api.LogInfo("[FDAccrual] ApproveOverride: run=%s fd=%s by=%s", req.RunID, req.FDID, userEmail)
	}
}

// ─── 14. RejectOverride ───────────────────────────────────────────────────────

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
			api.RespondWithError(w, http.StatusBadRequest, "run_id and fd_id are required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		_, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_ledger
			SET override_status = 'REJECTED',
			    override_rejected_by = $1,
			    override_rejected_at = now(),
			    override_checker_comment = $2,
			    is_overridden = false,
			    updated_at = now()
			WHERE run_id = $3 AND fd_id = $4 AND override_status = 'PROPOSED'`,
			userEmail, nullIfEmpty(req.Comment), req.RunID, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Reject override failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id":          req.RunID,
			"fd_id":           req.FDID,
			"override_status": "REJECTED",
		})
		api.LogInfo("[FDAccrual] RejectOverride: run=%s fd=%s by=%s", req.RunID, req.FDID, userEmail)
	}
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

// loadRunParams fetches the run row and builds AccrualRunParams.
func loadRunParams(ctx context.Context, pool *pgxpool.Pool, runID string) (AccrualRunParams, error) {
	var p AccrualRunParams
	var inclusionMethod, inclusionListJSON, bankFilter, fdStatusFilter, dayCountConv, roundingRule string
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
			COALESCE(fd_inclusion_list,'[]')
		FROM investment.fd_accrual_run WHERE run_id = $1`, runID,
	).Scan(
		&p.EntityID,
		&p.PeriodStart, &p.PeriodEnd, &p.FinancialPeriod,
		&dayCountConv, &roundingRule, &precisionDecimals,
		&bankFilter, &fdStatusFilter, &inclusionMethod, &inclusionListJSON,
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

	var runID string
	err := pool.QueryRow(ctx, `
		INSERT INTO investment.fd_accrual_run (
			run_type, run_mode, run_status,
			entity_id, entity_name,
			bank_id_filter, fd_status_filter,
			accrual_period_start, accrual_period_end, financial_period,
			day_count_convention, rounding_rule, precision_decimals,
			fd_inclusion_method,
			engine_version,
			is_active, is_deleted, created_by, created_at
		) VALUES (
			$1, $2, 'DRAFT',
			$3, $4,
			$5, $6,
			$7, $8, $9,
			$10, $11, $12,
			'ALL',
			'2.0',
			true, false, $13, now()
		) RETURNING run_id`,
		input.RunType, input.RunMode,
		input.EntityID, nullIfEmpty(input.EntityName),
		nullIfEmpty(input.BankIDFilter), fdStatus,
		input.AccrualPeriodStart, input.AccrualPeriodEnd, input.FinancialPeriod,
		dayCount, rounding, precision,
		input.CreatedBy,
	).Scan(&runID)
	return runID, err
}

// validateAndPersistFindings runs scope+validation and saves findings to DB.
func validateAndPersistFindings(ctx context.Context, pool *pgxpool.Pool, runID string) (eligibleCount int, blockerCount int, err error) {
	params, err := loadRunParams(ctx, pool, runID)
	if err != nil {
		return 0, 0, err
	}

	fds, err := getFDsInScope(ctx, pool, params)
	if err != nil {
		return 0, 0, err
	}

	findings := validateFDsForAccrual(fds, params)

	// Delete existing findings for this run
	_, _ = pool.Exec(ctx, `DELETE FROM investment.fd_accrual_validation_finding WHERE run_id=$1`, runID)

	for _, f := range findings {
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
	}

	return len(fds), blockerCount, nil
}

// executeAccrualRun performs the accrual calculation for all FDs in scope.
func executeAccrualRun(ctx context.Context, pool *pgxpool.Pool, runID string, executedBy string) (int, int, error) {
	params, err := loadRunParams(ctx, pool, runID)
	if err != nil {
		return 0, 0, err
	}

	fds, err := getFDsInScope(ctx, pool, params)
	if err != nil {
		return 0, 0, err
	}

	// Mark run as IN_PROGRESS
	_, _ = pool.Exec(ctx,
		`UPDATE investment.fd_accrual_run SET run_status='IN_PROGRESS', started_at=now(), fds_in_scope=$1 WHERE run_id=$2`,
		len(fds), runID)

	calculated := 0
	failed := 0
	var totalInterest, totalTDS, totalNet float64

	for _, fd := range fds {
		openingBalance := getPriorRunClosingBalance(ctx, pool, params.EntityID, fd.FDID, params.PeriodStart)
		result := calculateAccrualForFD(ctx, pool, fd, params, openingBalance)

		if result.LedgerRowStatus == "EXCLUDED" {
			logAccrualEvent(ctx, pool, runID, fd.FDID, "INFO", "EXCLUDED",
				fmt.Sprintf("FD excluded: %s", result.CalculationError), nil)
			continue
		}

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
				false,null,null,
				true,false,now()
			)
			ON CONFLICT (run_id, fd_id) DO UPDATE SET
				period_interest_accrued    = EXCLUDED.period_interest_accrued,
				opening_accrued_balance    = EXCLUDED.opening_accrued_balance,
				interest_received_in_period = EXCLUDED.interest_received_in_period,
				closing_accrued_balance    = EXCLUDED.closing_accrued_balance,
				tds_applicable_amount      = EXCLUDED.tds_applicable_amount,
				tds_deducted_in_period     = EXCLUDED.tds_deducted_in_period,
				net_interest_in_period     = EXCLUDED.net_interest_in_period,
				cashflow_row_ids           = EXCLUDED.cashflow_row_ids,
				formula_used               = EXCLUDED.formula_used,
				ledger_row_status          = EXCLUDED.ledger_row_status,
				accrual_days               = EXCLUDED.accrual_days,
				daily_accrual_rate         = EXCLUDED.daily_accrual_rate,
				divisor                    = EXCLUDED.divisor,
				opening_principal          = EXCLUDED.opening_principal,
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
		)
		if upsertErr != nil {
			failed++
			logAccrualEvent(ctx, pool, runID, fd.FDID, "ERROR", "LEDGER_UPSERT",
				fmt.Sprintf("Ledger upsert failed: %v", upsertErr), nil)
			continue
		}

		calculated++
		totalInterest += result.PeriodInterestAccrued
		totalTDS += result.TDSDeductedInPeriod
		totalNet += result.NetInterestInPeriod

		logAccrualEvent(ctx, pool, runID, fd.FDID, "INFO", "CALCULATED",
			fmt.Sprintf("interest=%.4f tds=%.4f net=%.4f days=%d",
				result.PeriodInterestAccrued, result.TDSDeductedInPeriod,
				result.NetInterestInPeriod, result.AccrualDays), nil)
	}

	// Update run summary
	newStatus := "COMPUTED"
	if failed > 0 && calculated == 0 {
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
			COALESCE(l.net_interest_in_period,0),
			l.accrual_period_start, l.accrual_period_end,
			COALESCE(l.entity_id,'') AS entity_id,
			COALESCE(l.entity_name,'') AS entity_name
		FROM investment.fd_accrual_ledger l
		WHERE l.run_id = $1
		  AND COALESCE(l.is_deleted,false) = false
		  AND l.ledger_row_status = 'CALCULATED'`, runID)
	if err != nil {
		return fmt.Errorf("postAccrualJournals query: %w", err)
	}
	defer rows.Close()

	type ledgerRow struct {
		LedgerID, FDID, EntityID, EntityName string
		NetInterest                          float64
		PeriodStart, PeriodEnd               time.Time
	}
	var ledgerRows []ledgerRow
	for rows.Next() {
		var lr ledgerRow
		if err := rows.Scan(&lr.LedgerID, &lr.FDID, &lr.NetInterest,
			&lr.PeriodStart, &lr.PeriodEnd, &lr.EntityID, &lr.EntityName); err != nil {
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

		amount := math.Round(lr.NetInterest*100) / 100
		if amount == 0 {
			_ = tx.Rollback(ctx)
			continue
		}

		description := fmt.Sprintf("FD accrual %s period %s→%s | fd_id=%s | run_id=%s",
			lr.FDID, lr.PeriodStart.Format("2006-01-02"), lr.PeriodEnd.Format("2006-01-02"), lr.FDID, runID)

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

		// Update ledger with journal_entry_id
		_, _ = pool.Exec(ctx,
			`UPDATE investment.fd_accrual_ledger SET journal_entry_id=$1, updated_at=now()
			 WHERE run_id=$2 AND fd_id=$3`,
			entryID, runID, lr.FDID)

		api.LogInfo("[FDAccrual] Journal posted fd=%s run=%s entry=%s", lr.FDID, runID, entryID)
	}

	return nil
}

// logAccrualEvent inserts a row into fd_accrual_run_execution_log.
func logAccrualEvent(ctx context.Context, pool *pgxpool.Pool,
	runID, fdID, level, eventType, message string, detail map[string]interface{}) {

	detailJSON := "{}"
	if detail != nil {
		if b, err := json.Marshal(detail); err == nil {
			detailJSON = string(b)
		}
	}
	fdIDPtr := interface{}(fdID)
	if fdID == "" {
		fdIDPtr = nil
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO investment.fd_accrual_run_execution_log (
			run_id, fd_id, log_level, event_type, message, detail, logged_at
		) VALUES ($1,$2,$3,$4,$5,$6::jsonb,now())`,
		runID, fdIDPtr, level, eventType, message, detailJSON)
}

// ─── String helpers ───────────────────────────────────────────────────────────

func strSliceToCSV(sl []string) string {
	return strings.Join(sl, ",")
}

var _ = strSliceToCSV // suppress unused warning
