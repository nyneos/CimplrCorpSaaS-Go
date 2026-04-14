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
	notifcatalog "CimplrCorpSaas/api/notification/catalog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── 1. CreateScheduleConfig ──────────────────────────────────────────────────

func CreateScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                string `json:"user_id"`
			EntityID              string `json:"entity_id"`
			EntityName            string `json:"entity_name"`
			ScheduleFrequency     string `json:"schedule_frequency"` // MONTHLY / QUARTERLY / YEARLY
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
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true,$9,$10,$11,now())
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
		go func() {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/schedule/create", configID, map[string]interface{}{
				"record_id": configID, "event": "FD_ACCRUAL_SCHEDULE_CREATED", "actor_email": userEmail, "entity_id": req.EntityID,
			})
		}()
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
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
			"schedule_frequency":       true,
			"run_day_of_month":         true,
			"run_time":                 true,
			"default_bank_id_filter":   true,
			"default_fd_status_filter": true,
			"default_run_mode":         true,
			"auto_submit_for_approval": true,
			"notification_recipients":  true,
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
		go func() {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/schedule/update", req.ConfigID, map[string]interface{}{
				"record_id": req.ConfigID, "event": "FD_ACCRUAL_SCHEDULE_UPDATED", "actor_email": userEmail,
			})
		}()
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
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
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
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id": req.ConfigID,
			"is_active": false,
		})
		go func() {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/schedule/disable", req.ConfigID, map[string]interface{}{
				"record_id": req.ConfigID, "event": "FD_ACCRUAL_SCHEDULE_DISABLED", "actor_email": userEmail,
			})
		}()
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
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
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id": req.ConfigID,
			"is_active": true,
		})
		go func() {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/schedule/enable", req.ConfigID, map[string]interface{}{
				"record_id": req.ConfigID, "event": "FD_ACCRUAL_SCHEDULE_ENABLED", "actor_email": userEmail,
			})
		}()
		api.LogInfo("[FDAccrual] EnableSchedule: config_id=%s by=%s", req.ConfigID, userEmail)
	}
}

// ─── 6. ApproveAccrualSchedule ────────────────────────────────────────────────

func ApproveAccrualSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDRequired)
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
		go func(rID, uEmail, st string) {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/run/schedule-approve", rID, map[string]interface{}{
				"record_id": rID, "event": "FD_ACCRUAL_RUN_APPROVED", "actor_email": uEmail, "status": st,
			})
		}(req.RunID, userEmail, instStatus)
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrRunIDRequired)
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
		go func(rID, uEmail string) {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/run/schedule-reject", rID, map[string]interface{}{
				"record_id": rID, "event": "FD_ACCRUAL_RUN_REJECTED", "actor_email": uEmail,
			})
		}(req.RunID, userEmail)
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
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
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
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
		go func() {
			defer func() { recover() }() //nolint:errcheck
			notifcatalog.TriggerNotification(context.Background(), pgxPool, "/investment/fd/accrual/schedule/delete", req.ConfigID, map[string]interface{}{
				"record_id": req.ConfigID, "event": "FD_ACCRUAL_SCHEDULE_DELETED", "actor_email": userEmail,
			})
		}()
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
		fireScheduledRun(ctx, pool, FireScheduledParams{
			ConfigID:     c.ConfigID,
			EntityID:     c.EntityID,
			EntityName:   c.EntityName,
			ScheduleFreq: c.ScheduleFrequency,
			BankFilter:   c.BankFilter,
			FDStatus:     c.FDStatus,
			RunMode:      c.DefaultRunMode,
			RunDay:       c.RunDayOfMonth,
			AutoSubmit:   c.AutoSubmit,
			Granularity:  c.Granularity,
			ScheduledAt:  c.NextRunAt,
		})
	}
}

// fireScheduledRun creates a run, validates, executes, and optionally submits.
type FireScheduledParams struct {
	ConfigID     string
	EntityID     string
	EntityName   string
	ScheduleFreq string
	BankFilter   string
	FDStatus     string
	RunMode      string
	RunDay       int
	AutoSubmit   bool
	Granularity  string
	ScheduledAt  time.Time
}

func fireScheduledRun(
	ctx context.Context,
	pool *pgxpool.Pool,
	p FireScheduledParams,
) {
	// Map struct fields to local variables to keep existing function body unchanged
	configID := p.ConfigID
	entityID := p.EntityID
	entityName := p.EntityName
	scheduleFreq := p.ScheduleFreq
	bankFilter := p.BankFilter
	fdStatus := p.FDStatus
	runMode := p.RunMode
	runDay := p.RunDay
	autoSubmit := p.AutoSubmit
	granularity := p.Granularity
	scheduledAt := p.ScheduledAt
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

	// run_type must match the DB check constraint: MONTHLY / QUARTERLY / SCHEDULED / AD_HOC / MANUAL
	// Scheduler-fired runs always use the constraint-allowed values.
	runType := "MONTHLY"
	switch scheduleFreq {
	case "QUARTERLY":
		runType = "QUARTERLY"
	case "YEARLY":
		runType = "SCHEDULED" // no YEARLY in constraint; SCHEDULED is the closest
	}

	// ── Duplicate guard ─────────────────────────────────────────────────────
	// Skip if ANY scheduler-created run already exists for this entity+period+mode
	// that is not in a terminal failure state.  This prevents server restarts or
	// tick races from creating multiple runs for the same period.
	var existingRun string
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(run_id,'') FROM investment.fd_accrual_run
		WHERE entity_id=$1
		  AND accrual_period_start=$2
		  AND accrual_period_end=$3
		  AND run_mode=$4
		  AND created_by='SCHEDULER'
		  AND run_status NOT IN ('FAILED','VALIDATION_FAILED')
		LIMIT 1`,
		entityID, periodStart, periodEnd, runMode,
	).Scan(&existingRun)
	if existingRun != "" {
		api.LogInfo("[FDAccrual] Scheduler skip entity=%s period=%s→%s mode=%s — run %s already exists",
			entityID, periodStart.Format(constants.DateFormat), periodEnd.Format(constants.DateFormat), runMode, existingRun)
		updateLastRunStatus(ctx, pool, configID, existingRun, "SKIPPED_DUPLICATE", scheduleFreq, runDay)
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
		runID, entityID, periodStart.Format(constants.DateFormat), periodEnd.Format(constants.DateFormat))

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

// computeNextRunForConfig calculates the next fire time from the given reference time.
// It tries the current period's run_day first; only advances to the next period if
// that day has already passed (or is today, in which case it fires immediately).
func computeNextRunForConfig(frequency string, runDay int, from time.Time) time.Time {
	if runDay < 1 {
		runDay = 1
	}
	if runDay > 28 {
		runDay = 28
	}
	from = from.UTC()

	switch frequency {
	case "QUARTERLY":
		// Find the start of the current quarter
		currentQStartMonth := time.Month(((int(from.Month())-1)/3)*3 + 1)
		candidateInCurrentQ := time.Date(from.Year(), currentQStartMonth, 1, 0, 0, 0, 0, time.UTC)
		// run_day within the quarter = day of the first month of that quarter
		maxDay := daysInMonth(candidateInCurrentQ.Year(), candidateInCurrentQ.Month())
		day := runDay
		if day > maxDay {
			day = maxDay
		}
		candidate := time.Date(candidateInCurrentQ.Year(), candidateInCurrentQ.Month(), day, 0, 0, 0, 0, time.UTC)
		if !candidate.Before(from) {
			return candidate
		}
		// Advance one quarter
		next := candidateInCurrentQ.AddDate(0, 3, 0)
		maxDay = daysInMonth(next.Year(), next.Month())
		day = runDay
		if day > maxDay {
			day = maxDay
		}
		return time.Date(next.Year(), next.Month(), day, 0, 0, 0, 0, time.UTC)

	case "YEARLY":
		// Try current year first
		maxDay := daysInMonth(from.Year(), time.January)
		day := runDay
		if day > maxDay {
			day = maxDay
		}
		candidate := time.Date(from.Year(), time.January, day, 0, 0, 0, 0, time.UTC)
		if !candidate.Before(from) {
			return candidate
		}
		// Advance one year
		next := time.Date(from.Year()+1, time.January, 1, 0, 0, 0, 0, time.UTC)
		maxDay = daysInMonth(next.Year(), next.Month())
		day = runDay
		if day > maxDay {
			day = maxDay
		}
		return time.Date(next.Year(), next.Month(), day, 0, 0, 0, 0, time.UTC)

	default: // MONTHLY
		// Try current month's run_day first
		maxDay := daysInMonth(from.Year(), from.Month())
		day := runDay
		if day > maxDay {
			day = maxDay
		}
		candidate := time.Date(from.Year(), from.Month(), day, 0, 0, 0, 0, time.UTC)
		if !candidate.Before(from) {
			return candidate
		}
		// Day already passed this month — advance to next month
		next := time.Date(from.Year(), from.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		maxDay = daysInMonth(next.Year(), next.Month())
		day = runDay
		if day > maxDay {
			day = maxDay
		}
		return time.Date(next.Year(), next.Month(), day, 0, 0, 0, 0, time.UTC)
	}
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

// ─── NEW: GetScheduleConfigsWithAudit ────────────────────────────────────────
// Enhanced version that includes audit trail and latest action details
func GetScheduleConfigsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		
		// Main query with LATERAL join for latest audit
		query := `
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.config_id)
				a.config_id, a.action_type, a.processing_status,
				a.requested_by, a.requested_at,
				a.checker_by, a.checker_at, a.checker_comment
			FROM investment.fd_accrual_schedule_config_audit a
			ORDER BY a.config_id,
				GREATEST(
					COALESCE(a.requested_at,'1970-01-01'::timestamptz),
					COALESCE(a.checker_at,'1970-01-01'::timestamptz)
				) DESC
		),
		history AS (
			SELECT
				config_id,
				MAX(CASE WHEN action_type='CREATE' THEN requested_by END)                                   AS created_by_audit,
				MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at_audit,
				MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
				MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
				MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
				MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
			FROM investment.fd_accrual_schedule_config_audit
			GROUP BY config_id
		)
		SELECT
			c.config_id,
			COALESCE(c.entity_id,'')                    AS entity_id,
			COALESCE(c.entity_name,'')                  AS entity_name,
			c.schedule_frequency,
			COALESCE(c.run_day_of_month,1)              AS run_day_of_month,
			COALESCE(c.default_bank_id_filter,'')       AS default_bank_id_filter,
			COALESCE(c.default_fd_status_filter,'ACTIVE') AS default_fd_status_filter,
			c.default_run_mode,
			c.auto_submit_for_approval,
			COALESCE(c.accrual_granularity,'MONTHLY')   AS accrual_granularity,
			COALESCE(c.last_run_id,'')                  AS last_run_id,
			COALESCE(c.last_run_status,'')              AS last_run_status,
			c.last_run_at,
			c.next_run_at,
			c.is_active,
			c.created_at,
			COALESCE(c.created_by,'')                   AS created_by,
			c.updated_at,
			COALESCE(c.updated_by,'')                   AS updated_by,
			-- Audit fields
			COALESCE(l.action_type,'')                  AS audit_action_type,
			COALESCE(l.processing_status,'')            AS audit_processing_status,
			COALESCE(l.requested_by,'')                 AS audit_requested_by,
			l.requested_at                              AS audit_requested_at,
			COALESCE(l.checker_by,'')                   AS audit_checker_by,
			l.checker_at                                AS audit_checker_at,
			COALESCE(l.checker_comment,'')              AS audit_checker_comment,
			-- History fields
			COALESCE(h.created_by_audit,'')             AS created_by_audit,
			COALESCE(h.created_at_audit,'')             AS created_at_audit,
			COALESCE(h.edited_by,'')                    AS edited_by,
			COALESCE(h.edited_at,'')                    AS edited_at,
			COALESCE(h.deleted_by,'')                   AS deleted_by,
			COALESCE(h.deleted_at,'')                   AS deleted_at,
			-- Stats: count of runs created by this schedule
			(SELECT COUNT(*) FROM investment.fd_accrual_run r
			 WHERE r.entity_id = c.entity_id
			   AND r.created_by = 'SCHEDULER'
			   AND r.run_type LIKE 'SCHEDULED_%') AS total_runs_created
		FROM investment.fd_accrual_schedule_config c
		LEFT JOIN latest_audit l ON l.config_id = c.config_id
		LEFT JOIN history h ON h.config_id = c.config_id
		WHERE 1=1`
		
		args := []interface{}{}
		argIdx := 1
		
		if req.EntityID != "" {
			query += fmt.Sprintf(" AND c.entity_id = $%d", argIdx)
			args = append(args, req.EntityID)
			argIdx++
		}
		query += " ORDER BY c.created_at DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
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
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowError+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"count":   len(configs),
			"configs": configs,
		})
	}
}

// ─── NEW: GetScheduleConfigDetail ────────────────────────────────────────────
// Comprehensive detail view showing config + all runs + aggregated execution data
func GetScheduleConfigDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
			return
		}

		ctx := r.Context()
		
		// ── 1. Config base data with audit ──────────────────────────────────
		var configData map[string]interface{}
		configRow := pgxPool.QueryRow(ctx, `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.config_id)
					a.config_id, a.action_type, a.processing_status,
					a.requested_by, a.requested_at,
					a.checker_by, a.checker_at, a.checker_comment
				FROM investment.fd_accrual_schedule_config_audit a
				WHERE a.config_id = $1
				ORDER BY a.config_id,
					GREATEST(
						COALESCE(a.requested_at,'1970-01-01'::timestamptz),
						COALESCE(a.checker_at,'1970-01-01'::timestamptz)
					) DESC
			),
			history AS (
				SELECT
					config_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END)                                   AS created_by_audit,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at_audit,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.fd_accrual_schedule_config_audit
				WHERE config_id = $1
				GROUP BY config_id
			)
			SELECT
				c.config_id,
				COALESCE(c.entity_id,''),
				COALESCE(c.entity_name,''),
				c.schedule_frequency,
				c.run_day_of_month,
				COALESCE(c.default_bank_id_filter,''),
				COALESCE(c.default_fd_status_filter,''),
				c.default_run_mode,
				c.auto_submit_for_approval,
				COALESCE(c.accrual_granularity,''),
				COALESCE(c.last_run_id,''),
				COALESCE(c.last_run_status,''),
				c.last_run_at,
				c.next_run_at,
				c.is_active,
				c.created_at,
				COALESCE(c.created_by,''),
				c.updated_at,
				COALESCE(c.updated_by,''),
				COALESCE(l.action_type,''),
				COALESCE(l.processing_status,''),
				COALESCE(l.requested_by,''),
				l.requested_at,
				COALESCE(l.checker_by,''),
				l.checker_at,
				COALESCE(l.checker_comment,''),
				COALESCE(h.created_by_audit,''),
				COALESCE(h.created_at_audit,''),
				COALESCE(h.edited_by,''),
				COALESCE(h.edited_at,''),
				COALESCE(h.deleted_by,''),
				COALESCE(h.deleted_at,'')
			FROM investment.fd_accrual_schedule_config c
			LEFT JOIN latest_audit l ON l.config_id = c.config_id
			LEFT JOIN history h ON h.config_id = c.config_id
			WHERE c.config_id = $1`, req.ConfigID)
		
		var configID, entityID, entityName, scheduleFreq, bankFilter, fdFilter, runMode, granularity string
		var lastRunID, lastRunStatus, createdBy, updatedBy string
		var auditAction, auditStatus, auditReqBy, auditChkBy, auditComment string
		var histCreatedBy, histCreatedAt, histEditedBy, histEditedAt, histDeletedBy, histDeletedAt string
		var runDay int
		var autoSubmit, isActive bool
		var lastRunAt, nextRunAt, createdAt, updatedAt, auditReqAt, auditChkAt interface{}
		
		if err := configRow.Scan(
			&configID, &entityID, &entityName, &scheduleFreq, &runDay,
			&bankFilter, &fdFilter, &runMode, &autoSubmit, &granularity,
			&lastRunID, &lastRunStatus, &lastRunAt, &nextRunAt,
			&isActive, &createdAt, &createdBy, &updatedAt, &updatedBy,
			&auditAction, &auditStatus, &auditReqBy, &auditReqAt,
			&auditChkBy, &auditChkAt, &auditComment,
			&histCreatedBy, &histCreatedAt, &histEditedBy, &histEditedAt, &histDeletedBy, &histDeletedAt,
		); err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Schedule config not found: "+err.Error())
			return
		}
		
		configData = map[string]interface{}{
			"config_id":                 configID,
			"entity_id":                 entityID,
			"entity_name":               entityName,
			"schedule_frequency":        scheduleFreq,
			"run_day_of_month":          runDay,
			"default_bank_id_filter":    bankFilter,
			"default_fd_status_filter":  fdFilter,
			"default_run_mode":          runMode,
			"auto_submit_for_approval":  autoSubmit,
			"accrual_granularity":       granularity,
			"last_run_id":               lastRunID,
			"last_run_status":           lastRunStatus,
			"last_run_at":               lastRunAt,
			"next_run_at":               nextRunAt,
			"is_active":                 isActive,
			"created_at":                createdAt,
			"created_by":                createdBy,
			"updated_at":                updatedAt,
			"updated_by":                updatedBy,
			"audit_action_type":         auditAction,
			"audit_processing_status":   auditStatus,
			"audit_requested_by":        auditReqBy,
			"audit_requested_at":        auditReqAt,
			"audit_checker_by":          auditChkBy,
			"audit_checker_at":          auditChkAt,
			"audit_checker_comment":     auditComment,
			"created_by_audit":          histCreatedBy,
			"created_at_audit":          histCreatedAt,
			"edited_by":                 histEditedBy,
			"edited_at":                 histEditedAt,
			"deleted_by":                histDeletedBy,
			"deleted_at":                histDeletedAt,
		}

		// ── 2. All runs created by this schedule ────────────────────────────
		runsRows, err := pgxPool.Query(ctx, `
			SELECT
				r.run_id,
				r.run_type,
				r.run_mode,
				r.run_status,
				r.accrual_period_start,
				r.accrual_period_end,
				COALESCE(r.financial_period,''),
				COALESCE(r.fds_in_scope,0),
				COALESCE(r.fds_calculated,0),
				COALESCE(r.fds_failed,0),
				COALESCE(r.total_interest_accrued,0),
				COALESCE(r.total_tds_deducted,0),
				r.created_at,
				r.submitted_at,
				r.posting_completed_at,
				(SELECT COUNT(*) FROM investment.fd_accrual_ledger l
				 WHERE l.run_id = r.run_id AND COALESCE(l.is_deleted,false) = false) AS ledger_count,
				(SELECT COUNT(*) FROM investment.fd_accrual_validation_finding f
				 WHERE f.run_id = r.run_id) AS findings_count,
				(SELECT COUNT(*) FROM investment.fd_accrual_exception e
				 WHERE e.run_id = r.run_id AND COALESCE(e.is_deleted,false) = false) AS exceptions_count
			FROM investment.fd_accrual_run r
			WHERE r.entity_id = $1
			  AND r.created_by = 'SCHEDULER'
			  AND r.run_type LIKE 'SCHEDULED_%'
			  AND COALESCE(r.is_deleted,false) = false
			ORDER BY r.created_at DESC`, entityID)
		
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Runs query failed: "+err.Error())
			return
		}
		defer runsRows.Close()
		
		runs := make([]map[string]interface{}, 0)
		runsFields := runsRows.FieldDescriptions()
		for runsRows.Next() {
			vals, _ := runsRows.Values()
			row := make(map[string]interface{}, len(runsFields))
			for i, f := range runsFields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			runs = append(runs, row)
		}

		// ── 3. Global execution stats for this schedule ─────────────────────
		var globalStats map[string]interface{}
		var totalRuns, totalLedgers, totalFindings, totalExceptions int
		var totalInterest, totalTDS float64
		_ = pgxPool.QueryRow(ctx, `
			SELECT
				COUNT(DISTINCT r.run_id),
				COALESCE(SUM(r.fds_calculated),0),
				COALESCE(SUM(r.total_interest_accrued),0),
				COALESCE(SUM(r.total_tds_deducted),0),
				(SELECT COUNT(*) FROM investment.fd_accrual_validation_finding f
				 JOIN investment.fd_accrual_run r2 ON r2.run_id = f.run_id
				 WHERE r2.entity_id = $1 AND r2.created_by = 'SCHEDULER'),
				(SELECT COUNT(*) FROM investment.fd_accrual_exception e
				 JOIN investment.fd_accrual_run r3 ON r3.run_id = e.run_id
				 WHERE r3.entity_id = $1 AND r3.created_by = 'SCHEDULER'
				   AND COALESCE(e.is_deleted,false) = false)
			FROM investment.fd_accrual_run r
			WHERE r.entity_id = $1
			  AND r.created_by = 'SCHEDULER'
			  AND COALESCE(r.is_deleted,false) = false`, entityID,
		).Scan(&totalRuns, &totalLedgers, &totalInterest, &totalTDS, &totalFindings, &totalExceptions)
		
		globalStats = map[string]interface{}{
			"total_runs":            totalRuns,
			"total_ledgers":         totalLedgers,
			"total_interest":        totalInterest,
			"total_tds":             totalTDS,
			"total_findings":        totalFindings,
			"total_exceptions":      totalExceptions,
		}

		// ── 4. Build complete response ──────────────────────────────────────
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config":        configData,
			"runs":          runs,
			"runs_count":    len(runs),
			"global_stats":  globalStats,
		})
	}
}