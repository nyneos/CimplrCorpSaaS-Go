package fdAccrual

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── 1. CreateScheduleConfig ──────────────────────────────────────────────────

func CreateScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                string `json:"user_id"`
			EntityID              string `json:"entity_id"`
			EntityName            string `json:"entity_name"`
			ScheduleFrequency     string `json:"schedule_frequency"`  // MONTHLY / QUARTERLY / YEARLY
			RunDayOfMonth         int    `json:"run_day_of_month"`
			DefaultBankIDFilter   string `json:"default_bank_id_filter"`
			DefaultFDStatusFilter string `json:"default_fd_status_filter"`
			DefaultRunMode        string `json:"default_run_mode"`
			AutoSubmitForApproval bool   `json:"auto_submit_for_approval"`
			AcrualGranularity     string `json:"accrual_granularity"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.EntityID == "" || req.ScheduleFrequency == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_id and schedule_frequency are required")
			return
		}
		if req.RunDayOfMonth < 1 || req.RunDayOfMonth > 28 {
			api.RespondWithError(w, http.StatusBadRequest, "run_day_of_month must be between 1 and 28")
			return
		}
		if req.DefaultRunMode == "" {
			req.DefaultRunMode = "SIMULATION"
		}
		if req.DefaultFDStatusFilter == "" {
			req.DefaultFDStatusFilter = "ACTIVE"
		}
		if req.AcrualGranularity == "" {
			req.AcrualGranularity = "MONTHLY"
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// compute initial next_run_at
		nextRun := computeNextRunForConfig(req.ScheduleFrequency, req.RunDayOfMonth, time.Now())

		var configID string
		err := pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_accrual_schedule_config (
				entity_id, entity_name,
				schedule_frequency, run_day_of_month,
				default_bank_id_filter, default_fd_status_filter,
				default_run_mode, auto_submit_for_approval,
				is_active, next_run_at,
				accrual_granularity,
				created_by, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true,$9,$11,$10,now())
			RETURNING config_id`,
			req.EntityID, nullIfEmpty(req.EntityName),
			req.ScheduleFrequency, req.RunDayOfMonth,
			nullIfEmpty(req.DefaultBankIDFilter), req.DefaultFDStatusFilter,
			req.DefaultRunMode, req.AutoSubmitForApproval,
			nextRun, userEmail, req.AcrualGranularity,
		).Scan(&configID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Create schedule config failed: "+err.Error())
			return
		}

		// Audit trail
		_, _ = pgxPool.Exec(r.Context(), `
			INSERT INTO investment.fd_accrual_schedule_config_audit
				(config_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','ACTIVE',$2,now())`,
			configID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":          configID,
			"entity_id":          req.EntityID,
			"schedule_frequency": req.ScheduleFrequency,
			"next_run_at":        nextRun,
			"is_active":          true,
		})
		api.LogInfo("[FDAccrual] CreateScheduleConfig: config_id=%s entity=%s freq=%s", configID, req.EntityID, req.ScheduleFrequency)
	}
}

// ─── 2. UpdateScheduleConfig ──────────────────────────────────────────────────

func UpdateScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                 `json:"user_id"`
			ConfigID string                 `json:"config_id"`
			Fields   map[string]interface{} `json:"fields"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfigID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "config_id is required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "fields are required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Allowlist of updatable columns
		allowed := map[string]bool{
			"schedule_frequency":      true,
			"run_day_of_month":        true,
			"run_time":                true,
			"default_bank_id_filter":  true,
			"default_fd_status_filter": true,
			"default_run_mode":        true,
			"auto_submit_for_approval": true,
			"notification_recipients": true,
		}

		setParts := []string{"updated_by = $1", "updated_at = now()"}
		args := []interface{}{userEmail}

		for col, val := range req.Fields {
			if !allowed[col] {
				continue
			}
			args = append(args, val)
			setParts = append(setParts, fmt.Sprintf("%s = $%d", col, len(args)))
		}

		if len(setParts) <= 2 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields provided")
			return
		}

		// Snapshot old values before update
		var oldFreq, oldRunMode, oldBankFilter, oldFDFilter string
		var oldRunDay int
		var oldAutoSubmit bool
		var oldNotifRecipients []byte
		var oldRunTime interface{}
		_ = pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(schedule_frequency,''),
				COALESCE(run_day_of_month,1),
				run_time,
				COALESCE(default_run_mode,''),
				COALESCE(default_bank_id_filter,''),
				COALESCE(default_fd_status_filter,''),
				auto_submit_for_approval,
				COALESCE(notification_recipients,'[]'::jsonb)::text
			FROM investment.fd_accrual_schedule_config
			WHERE config_id=$1`, req.ConfigID,
		).Scan(&oldFreq, &oldRunDay, &oldRunTime, &oldRunMode, &oldBankFilter, &oldFDFilter, &oldAutoSubmit, &oldNotifRecipients)

		args = append(args, req.ConfigID)
		query := fmt.Sprintf(
			"UPDATE investment.fd_accrual_schedule_config SET %s WHERE config_id = $%d AND is_active = true",
			joinStrings(setParts, ", "),
			len(args),
		)
		ct, err := pgxPool.Exec(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Update schedule config failed: "+err.Error())
			return
		}
		if ct.RowsAffected() == 0 {
			api.RespondWithError(w, http.StatusNotFound, "schedule config not found or already disabled")
			return
		}

		// Audit trail with full old-value snapshot
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_schedule_config_audit (
				config_id, action_type, processing_status, requested_by, requested_at,
				old_schedule_frequency, old_run_day_of_month, old_run_time,
				old_default_run_mode, old_default_bank_id_filter, old_default_fd_status_filter,
				old_auto_submit_for_approval, old_notification_recipients, old_is_active
			) VALUES ($1,'EDIT','ACTIVE',$2,now(),$3,$4,$5,$6,$7,$8,$9,$10::jsonb,true)`,
			req.ConfigID, userEmail,
			oldFreq, oldRunDay, oldRunTime, oldRunMode, oldBankFilter, oldFDFilter,
			oldAutoSubmit, string(oldNotifRecipients))

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id": req.ConfigID,
			"updated":   true,
		})
		api.LogInfo("[FDAccrual] UpdateScheduleConfig: config_id=%s by=%s", req.ConfigID, userEmail)
	}
}

// ─── 3. GetScheduleConfigs ────────────────────────────────────────────────────

func GetScheduleConfigs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		query := `
			SELECT
				config_id,
				COALESCE(entity_id,''), COALESCE(entity_name,''),
				schedule_frequency,
				COALESCE(run_day_of_month,1),
				COALESCE(default_bank_id_filter,''),
				COALESCE(default_fd_status_filter,'ACTIVE'),
				default_run_mode,
				auto_submit_for_approval,
				COALESCE(last_run_status,''),
				last_run_at, next_run_at,
				is_active, created_at
			FROM investment.fd_accrual_schedule_config
			WHERE is_active = true`
		args := []interface{}{}
		if req.EntityID != "" {
			query += " AND entity_id = $1"
			args = append(args, req.EntityID)
		}
		query += " ORDER BY created_at DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		configs := make([]map[string]interface{}, 0)
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
			configs = append(configs, row)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row error: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"count":   len(configs),
			"configs": configs,
		})
	}
}

// ─── 4. DisableSchedule ───────────────────────────────────────────────────────

func DisableSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			ConfigID string `json:"config_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfigID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "config_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		ct, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_schedule_config
			SET is_active=false, updated_by=$1, updated_at=now()
			WHERE config_id=$2`, userEmail, req.ConfigID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Disable failed: "+err.Error())
			return
		}
		if ct.RowsAffected() == 0 {
			api.RespondWithError(w, http.StatusNotFound, "schedule config not found")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id": req.ConfigID,
			"is_active": false,
		})
		api.LogInfo("[FDAccrual] DisableSchedule: config_id=%s by=%s", req.ConfigID, userEmail)
	}
}

// ─── 5. EnableSchedule ────────────────────────────────────────────────────────

func EnableSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			ConfigID string `json:"config_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfigID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "config_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		ct, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_schedule_config
			SET is_active=true, updated_by=$1, updated_at=now()
			WHERE config_id=$2`, userEmail, req.ConfigID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Enable failed: "+err.Error())
			return
		}
		if ct.RowsAffected() == 0 {
			api.RespondWithError(w, http.StatusNotFound, "schedule config not found")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id": req.ConfigID,
			"is_active": true,
		})
		api.LogInfo("[FDAccrual] EnableSchedule: config_id=%s by=%s", req.ConfigID, userEmail)
	}
}

// ─── 6. ApproveAccrualSchedule ────────────────────────────────────────────────

func ApproveAccrualSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			RoleID   string `json:"role_id"`
			RunID    string `json:"run_id"`
			Comment  string `json:"comment"`
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

		var instanceEyeID string
		_ = pgxPool.QueryRow(ctx, `
			SELECT ie.instance_eye_id
			FROM uam.approval_instance_eye ie
			JOIN uam.approval_instance i ON i.instance_id = ie.instance_id
			WHERE i.record_id = $1
			  AND i.module_code = 'FIXED_DEPOSIT'
			  AND i.status = 'PENDING'
			  AND ie.status = 'ACTIVE'
			ORDER BY ie.position LIMIT 1`, req.RunID,
		).Scan(&instanceEyeID)

		if instanceEyeID == "" {
			_, _ = pgxPool.Exec(ctx, `
				UPDATE investment.fd_accrual_run
				SET run_status='APPROVED', updated_at=now()
				WHERE run_id=$1 AND run_status='PENDING_APPROVAL'`, req.RunID)
		} else {
			err := approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
				InstanceEyeID: instanceEyeID,
				ActorUserID:   req.UserID,
				ActorEmail:    userEmail,
				ActorRoleID:   req.RoleID,
				ActionType:    approvalengine.ActionApproved,
				Comment:       req.Comment,
			})
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Approval engine error: "+err.Error())
				return
			}
		}

		var instStatus string
		_ = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(run_status,'') FROM investment.fd_accrual_run WHERE run_id=$1`, req.RunID,
		).Scan(&instStatus)

		if instStatus == "APPROVED" {
			if jErr := postAccrualJournals(ctx, pgxPool, req.RunID, userEmail); jErr != nil {
				api.LogError("[FDAccrual] ApproveAccrualSchedule journal error run %s: %v", req.RunID, jErr)
			}
			_, _ = pgxPool.Exec(ctx,
				`UPDATE investment.fd_accrual_run SET run_status='POSTED', posting_status='POSTED', posting_completed_at=now() WHERE run_id=$1`,
				req.RunID)
			instStatus = "POSTED"
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id": req.RunID,
			"status": instStatus,
		})
		api.LogInfo("[FDAccrual] ApproveAccrualSchedule: run_id=%s by=%s result=%s", req.RunID, userEmail, instStatus)
	}
}

// ─── 7. RejectAccrualSchedule ─────────────────────────────────────────────────

func RejectAccrualSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string `json:"user_id"`
			RoleID  string `json:"role_id"`
			RunID   string `json:"run_id"`
			Comment string `json:"comment"`
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

		var instanceEyeID string
		_ = pgxPool.QueryRow(ctx, `
			SELECT ie.instance_eye_id
			FROM uam.approval_instance_eye ie
			JOIN uam.approval_instance i ON i.instance_id = ie.instance_id
			WHERE i.record_id = $1
			  AND i.module_code = 'FIXED_DEPOSIT'
			  AND i.status = 'PENDING'
			  AND ie.status = 'ACTIVE'
			ORDER BY ie.position LIMIT 1`, req.RunID,
		).Scan(&instanceEyeID)

		if instanceEyeID != "" {
			_ = approvalengine.RecordAction(ctx, pgxPool, approvalengine.ActionRequest{
				InstanceEyeID: instanceEyeID,
				ActorUserID:   req.UserID,
				ActorEmail:    userEmail,
				ActorRoleID:   req.RoleID,
				ActionType:    approvalengine.ActionRejected,
				Comment:       req.Comment,
			})
		}

		_, _ = pgxPool.Exec(ctx,
			`UPDATE investment.fd_accrual_run SET run_status='REJECTED', updated_at=now()
			 WHERE run_id=$1 AND run_status='PENDING_APPROVAL'`, req.RunID)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"run_id": req.RunID,
			"status": "REJECTED",
		})
		api.LogInfo("[FDAccrual] RejectAccrualSchedule: run_id=%s by=%s", req.RunID, userEmail)
	}
}

// ─── 8. DeleteScheduleConfig ──────────────────────────────────────────────────

func DeleteScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			ConfigID string `json:"config_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ConfigID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "config_id is required")
			return
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Snapshot old values before delete
		var dOldFreq, dOldRunMode, dOldBankFilter, dOldFDFilter string
		var dOldRunDay int
		var dOldAutoSubmit bool
		var dOldNotif []byte
		var dOldRunTime interface{}
		_ = pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(schedule_frequency,''),
				COALESCE(run_day_of_month,1),
				run_time,
				COALESCE(default_run_mode,''),
				COALESCE(default_bank_id_filter,''),
				COALESCE(default_fd_status_filter,''),
				auto_submit_for_approval,
				COALESCE(notification_recipients,'[]'::jsonb)::text
			FROM investment.fd_accrual_schedule_config
			WHERE config_id=$1`, req.ConfigID,
		).Scan(&dOldFreq, &dOldRunDay, &dOldRunTime, &dOldRunMode, &dOldBankFilter, &dOldFDFilter, &dOldAutoSubmit, &dOldNotif)

		ct, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_schedule_config
			SET is_active=false, updated_by=$1, updated_at=now()
			WHERE config_id=$2`, userEmail, req.ConfigID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Delete failed: "+err.Error())
			return
		}
		if ct.RowsAffected() == 0 {
			api.RespondWithError(w, http.StatusNotFound, "schedule config not found")
			return
		}

		// Audit trail with full old-value snapshot
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_schedule_config_audit (
				config_id, action_type, processing_status, requested_by, requested_at,
				old_schedule_frequency, old_run_day_of_month, old_run_time,
				old_default_run_mode, old_default_bank_id_filter, old_default_fd_status_filter,
				old_auto_submit_for_approval, old_notification_recipients, old_is_active
			) VALUES ($1,'DELETE','INACTIVE',$2,now(),$3,$4,$5,$6,$7,$8,$9,$10::jsonb,true)`,
			req.ConfigID, userEmail,
			dOldFreq, dOldRunDay, dOldRunTime, dOldRunMode, dOldBankFilter, dOldFDFilter,
			dOldAutoSubmit, string(dOldNotif))

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id": req.ConfigID,
			"deleted":   true,
		})
		api.LogInfo("[FDAccrual] DeleteScheduleConfig: config_id=%s by=%s", req.ConfigID, userEmail)
	}
}

// ─── Background Worker ────────────────────────────────────────────────────────

// StartAccrualSchedulerWorker runs continuously, checking every minute whether
// any schedule configs are due and firing accrual runs for them.
func StartAccrualSchedulerWorker(pool *pgxpool.Pool) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	api.LogInfo("[FDAccrual] Scheduler worker started")

	for range ticker.C {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					api.LogError("[FDAccrual] Scheduler panic: %v\n%s", rec, debug.Stack())
				}
			}()
			checkAndFireDueSchedules(context.Background(), pool)
		}()
	}
}

// checkAndFireDueSchedules queries all active configs where next_run_at <= now
// and fires an accrual run for each.
func checkAndFireDueSchedules(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `
		SELECT config_id, entity_id, entity_name,
		       schedule_frequency, run_day_of_month,
		       COALESCE(default_bank_id_filter,''),
		       COALESCE(default_fd_status_filter,'ACTIVE'),
		       default_run_mode,
		       auto_submit_for_approval,
		       COALESCE(accrual_granularity,'MONTHLY') AS accrual_granularity,
		       COALESCE(next_run_at, now()) AS next_run_at
		FROM investment.fd_accrual_schedule_config
		WHERE is_active = true
		  AND next_run_at <= now()
		ORDER BY next_run_at`)
	if err != nil {
		api.LogError("[FDAccrual] checkAndFireDueSchedules query: %v", err)
		return
	}
	defer rows.Close()

	type dueConfig struct {
		ConfigID, EntityID, EntityName          string
		ScheduleFrequency, BankFilter, FDStatus string
		DefaultRunMode                          string
		RunDayOfMonth                           int
		AutoSubmit                              bool
		Granularity                             string
		NextRunAt                               time.Time
	}
	var due []dueConfig
	for rows.Next() {
		var c dueConfig
		if err := rows.Scan(
			&c.ConfigID, &c.EntityID, &c.EntityName,
			&c.ScheduleFrequency, &c.RunDayOfMonth,
			&c.BankFilter, &c.FDStatus,
			&c.DefaultRunMode, &c.AutoSubmit,
			&c.Granularity, &c.NextRunAt,
		); err != nil {
			api.LogError("[FDAccrual] checkAndFireDueSchedules scan: %v", err)
			continue
		}
		due = append(due, c)
	}
	rows.Close()

	for _, c := range due {
		fireScheduledRun(ctx, pool, c.ConfigID, c.EntityID, c.EntityName,
			c.ScheduleFrequency, c.BankFilter, c.FDStatus,
			c.DefaultRunMode, c.RunDayOfMonth, c.AutoSubmit,
			c.Granularity, c.NextRunAt)
	}
}

// fireScheduledRun creates a run, validates, executes, and optionally submits.
func fireScheduledRun(
	ctx context.Context,
	pool *pgxpool.Pool,
	configID, entityID, entityName string,
	scheduleFreq, bankFilter, fdStatus string,
	runMode string,
	runDay int,
	autoSubmit bool,
	granularity string,
	scheduledAt time.Time,
) {
	// Derive period from scheduledAt (the config's next_run_at), not from now()
	var periodStart, periodEnd time.Time
	switch scheduleFreq {
	case "QUARTERLY":
		// Quarter that contains scheduledAt
		qMonth := ((int(scheduledAt.Month())-1)/3)*3 + 1
		periodStart = time.Date(scheduledAt.Year(), time.Month(qMonth), 1, 0, 0, 0, 0, time.UTC)
		periodEnd = periodStart.AddDate(0, 3, -1)
	case "YEARLY":
		periodStart = time.Date(scheduledAt.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		periodEnd = time.Date(scheduledAt.Year(), 12, 31, 0, 0, 0, 0, time.UTC)
	default: // MONTHLY
		periodStart = time.Date(scheduledAt.Year(), scheduledAt.Month(), 1, 0, 0, 0, 0, time.UTC)
		periodEnd = periodStart.AddDate(0, 1, -1)
	}
	financialPeriod := buildAccrualPeriod(periodStart)

	// Dynamic run_type reflects schedule frequency
	runType := "SCHEDULED_MONTHLY"
	switch scheduleFreq {
	case "QUARTERLY":
		runType = "SCHEDULED_QUARTERLY"
	case "YEARLY":
		runType = "SCHEDULED_YEARLY"
	}

	// Skip if a FINAL run already exists for this entity+period
	var existingFinalRun string
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(run_id,'') FROM investment.fd_accrual_run
		WHERE entity_id=$1
		  AND accrual_period_start=$2
		  AND accrual_period_end=$3
		  AND run_mode='FINAL'
		  AND run_status NOT IN ('FAILED','VALIDATION_FAILED')
		LIMIT 1`,
		entityID, periodStart, periodEnd,
	).Scan(&existingFinalRun)
	if existingFinalRun != "" {
		api.LogInfo("[FDAccrual] Scheduler skip entity=%s period=%s→%s — FINAL run %s already exists",
			entityID, periodStart.Format("2006-01-02"), periodEnd.Format("2006-01-02"), existingFinalRun)
		updateLastRunStatus(ctx, pool, configID, existingFinalRun, "SKIPPED_DUPLICATE", scheduleFreq, runDay)
		return
	}

	if granularity == "" {
		granularity = "MONTHLY"
	}

	input := CreateAccrualRunInput{
		RunType:            runType,
		RunMode:            runMode,
		EntityID:           entityID,
		EntityName:         entityName,
		BankIDFilter:       bankFilter,
		FDStatusFilter:     fdStatus,
		AccrualPeriodStart: periodStart,
		AccrualPeriodEnd:   periodEnd,
		FinancialPeriod:    financialPeriod,
		DayCountConvention: "ACT_365",
		RoundingRule:       "ROUND",
		PrecisionDecimals:  2,
		Granularity:        granularity,
		CreatedBy:          "SCHEDULER",
	}

	runID, err := createAccrualRunInternal(ctx, pool, input)
	if err != nil {
		api.LogError("[FDAccrual] Scheduler createRun failed entity=%s: %v", entityID, err)
		updateLastRunStatus(ctx, pool, configID, "", "CREATE_FAILED", scheduleFreq, runDay)
		return
	}
	api.LogInfo("[FDAccrual] Scheduler created run_id=%s entity=%s period=%s→%s",
		runID, entityID, periodStart.Format("2006-01-02"), periodEnd.Format("2006-01-02"))

	// Validate
	eligible, blockers, _, vErr := validateAndPersistFindings(ctx, pool, runID)
	if vErr != nil {
		api.LogError("[FDAccrual] Scheduler validate run=%s: %v", runID, vErr)
		updateLastRunStatus(ctx, pool, configID, runID, "VALIDATE_FAILED", scheduleFreq, runDay)
		return
	}
	newStatus := "VALIDATED"
	if blockers > 0 {
		newStatus = "VALIDATION_FAILED"
	}
	_, _ = pool.Exec(ctx,
		`UPDATE investment.fd_accrual_run SET run_status=$1, fds_in_scope=$2 WHERE run_id=$3`,
		newStatus, eligible, runID)

	if blockers > 0 {
		api.LogInfo("[FDAccrual] Scheduler run=%s has %d blockers — skipping execution", runID, blockers)
		updateLastRunStatus(ctx, pool, configID, runID, "VALIDATION_FAILED", scheduleFreq, runDay)
		return
	}

	// Execute
	calculated, failed, exErr := executeAccrualRun(ctx, pool, runID, "SCHEDULER")
	if exErr != nil {
		api.LogError("[FDAccrual] Scheduler execute run=%s: %v", runID, exErr)
		updateLastRunStatus(ctx, pool, configID, runID, "EXECUTE_FAILED", scheduleFreq, runDay)
		return
	}
	api.LogInfo("[FDAccrual] Scheduler run=%s calculated=%d failed=%d", runID, calculated, failed)

	// Submit for approval if configured
	finalStatus := "COMPUTED"
	if autoSubmit && runMode != "SIMULATION" {
		if sErr := submitAccrualRunForApproval(ctx, pool, runID, "SCHEDULER"); sErr != nil {
			api.LogError("[FDAccrual] Scheduler submit run=%s: %v", runID, sErr)
		} else {
			finalStatus = "PENDING_APPROVAL"
		}
	}

	updateLastRunStatus(ctx, pool, configID, runID, finalStatus, scheduleFreq, runDay)
}

// updateLastRunStatus bumps last_run_status and always advances next_run_at.
func updateLastRunStatus(ctx context.Context, pool *pgxpool.Pool, configID, runID, status, scheduleFreq string, runDay int) {
	var runIDArg interface{} = nil
	if runID != "" {
		runIDArg = runID
	}
	nextRun := computeNextRunForConfig(scheduleFreq, runDay, time.Now())
	_, _ = pool.Exec(ctx, `
		UPDATE investment.fd_accrual_schedule_config
		SET last_run_at=now(), last_run_id=$1, last_run_status=$2,
		    next_run_at=$3, updated_at=now()
		WHERE config_id=$4`,
		runIDArg, status, nextRun, configID)
}

// computeNextRunForConfig calculates the next fire time from the current time.
func computeNextRunForConfig(frequency string, runDay int, from time.Time) time.Time {
	if runDay < 1 {
		runDay = 1
	}
	if runDay > 28 {
		runDay = 28
	}
	var next time.Time
	switch frequency {
	case "QUARTERLY":
		next = from.AddDate(0, 3, 0)
	case "YEARLY":
		next = from.AddDate(1, 0, 0)
	default: // MONTHLY
		next = from.AddDate(0, 1, 0)
	}
	// clamp day
	maxDay := daysInMonth(next.Year(), next.Month())
	day := runDay
	if day > maxDay {
		day = maxDay
	}
	return time.Date(next.Year(), next.Month(), day, 0, 0, 0, 0, time.UTC)
}

// daysInMonth returns the number of days in a given month.
func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// joinStrings joins a slice with sep (to avoid importing strings twice).
func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}
