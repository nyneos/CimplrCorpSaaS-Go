package fdAccrual

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/ctxutil"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"

	notifcatalog "CimplrCorpSaas/api/notification/catalog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── 1. CreateScheduleConfig ──────────────────────────────────────────────────

func requireScheduleConfigScope(ctx context.Context, pgxPool *pgxpool.Pool, configID string) (string, bool, error) {
	var entityID string
	err := pgxPool.QueryRow(ctx, `
		SELECT COALESCE(entity_id,'')
		FROM investment.fd_accrual_schedule_config
		WHERE config_id=$1 AND COALESCE(is_deleted,false)=false`, configID).Scan(&entityID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}
	if !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
		return entityID, false, nil
	}
	return entityID, true, nil
}

func CreateScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                string `json:"user_id"`
			EntityID              string `json:"entity_id"`
			EntityName            string `json:"entity_name"`
			ScheduleFrequency     string `json:"schedule_frequency"` // when the job fires: DAILY|MONTHLY|QUARTERLY|HALF_YEARLY|YEARLY
			PeriodCoverage        string `json:"period_coverage"`    // the date window each run covers: MONTHLY|QUARTERLY|HALF_YEARLY|YEARLY|RUN
			RunDayOfMonth         int    `json:"run_day_of_month"`
			RunTime               string `json:"run_time"` // HH:MM or HH:MM:SS (Asia/Kolkata)
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
			req.DefaultFDStatusFilter = constants.StatusActive
		}
		if req.AcrualGranularity == "" {
			req.AcrualGranularity = "MONTHLY"
		}
		if req.PeriodCoverage == "" {
			req.PeriodCoverage = "RUN"
		}
		if !isValidScheduleFrequency(req.ScheduleFrequency) {
			api.RespondWithError(w, http.StatusBadRequest,
				"schedule_frequency must be DAILY, MONTHLY, QUARTERLY, HALF_YEARLY, or YEARLY")
			return
		}
		if !isValidAccrualGranularity(req.AcrualGranularity) {
			api.RespondWithError(w, http.StatusBadRequest,
				"accrual_granularity must be DAILY, MONTHLY, QUARTERLY, HALF_YEARLY, YEARLY, or RUN")
			return
		}
		if !isValidPeriodCoverage(req.PeriodCoverage) {
			api.RespondWithError(w, http.StatusBadRequest,
				"period_coverage must be DAILY, MONTHLY, QUARTERLY, HALF_YEARLY, YEARLY, or RUN")
			return
		}

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(req.EntityID) {
			api.RespondWithError(w, http.StatusForbidden,
				fmt.Sprintf(constants.ErrEntityIDNotAuthorized, req.EntityID))
			return
		}
		if strings.TrimSpace(req.DefaultBankIDFilter) != "" && !scope.HasApprovedBank(req.DefaultBankIDFilter) {
			api.RespondWithError(w, http.StatusForbidden,
				fmt.Sprintf(constants.ErrBankNotApproved1, req.DefaultBankIDFilter))
			return
		}

		// Block if entity already has a config with the same (frequency, granularity) combination.
		// Same entity may have e.g. MONTHLY+MONTHLY and YEARLY+MONTHLY as separate configs,
		// but not two MONTHLY+MONTHLY configs.
		var existingConfigID string
		_ = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(config_id,'')
			FROM investment.fd_accrual_schedule_config
			WHERE entity_id           = $1
			  AND schedule_frequency  = $2
			  AND accrual_granularity = $3
			  AND COALESCE(is_deleted, false) = false
			LIMIT 1`, req.EntityID, req.ScheduleFrequency, req.AcrualGranularity).Scan(&existingConfigID)
		if existingConfigID != "" {
			api.RespondWithError(w, http.StatusConflict, fmt.Sprintf(
				"entity already has a %s/%s schedule config (%s); update or delete it first",
				req.ScheduleFrequency, req.AcrualGranularity, existingConfigID))
			return
		}

		runTimeDB, runTimeErr := parseRunTimeForDB(req.RunTime)
		if runTimeErr != nil {
			api.RespondWithError(w, http.StatusBadRequest, runTimeErr.Error())
			return
		}

		schedDraftRow := fdAccrualScheduleRow{
			EntityID:              req.EntityID,
			EntityName:            req.EntityName,
			ScheduleFrequency:     req.ScheduleFrequency,
			RunDayOfMonth:         req.RunDayOfMonth,
			RunTime:               req.RunTime,
			DefaultBankIDFilter:   req.DefaultBankIDFilter,
			DefaultFDStatusFilter: req.DefaultFDStatusFilter,
			DefaultRunMode:        req.DefaultRunMode,
			AutoSubmitForApproval: req.AutoSubmitForApproval,
			IsActive:              false,
			AccrualGranularity:    req.AcrualGranularity,
			PeriodCoverage:        req.PeriodCoverage,
			CreatedBy:             userEmail,
		}
		schedCreateOK, schedCreateMatrixID := fdAccrualSchedEnforceMatrix(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreCreate,
			HandlerName: "CreateScheduleConfig",
			APIPath:     "/investment/fd/accrual/schedule/create",
			EntityCode:  req.EntityID,
			Actor:       userEmail,
		}, buildFDAccrualSchedulePolicyFields(schedDraftRow))
		if !schedCreateOK {
			return
		}

		nextRun := computeNextRunAt(req.ScheduleFrequency, req.RunDayOfMonth, runTimeDB, time.Now())

		var configID string
		err := pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_accrual_schedule_config (
				entity_id, entity_name,
				schedule_frequency, run_day_of_month, run_time,
				default_bank_id_filter, default_fd_status_filter,
				default_run_mode, auto_submit_for_approval,
				is_active, next_run_at,
				accrual_granularity, period_coverage,
				created_by, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,false,$10,$11,$12,$13,now())
			RETURNING config_id`,
			req.EntityID, nullIfEmpty(req.EntityName),
			req.ScheduleFrequency, req.RunDayOfMonth, runTimeDB,
			nullIfEmpty(req.DefaultBankIDFilter), req.DefaultFDStatusFilter,
			req.DefaultRunMode, req.AutoSubmitForApproval,
			nextRun, req.AcrualGranularity, req.PeriodCoverage, userEmail,
		).Scan(&configID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Create schedule config failed: "+err.Error())
			return
		}

		// Audit trail (processing_status must match DB check constraint)
		if _, auditErr := pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_schedule_config_audit
				(config_id, action_type, processing_status, requested_by, requested_at, requested_ip)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`,
			configID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx))); auditErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Schedule audit insert failed: "+auditErr.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":          configID,
			"entity_id":          req.EntityID,
			"schedule_frequency": req.ScheduleFrequency,
			"period_coverage":    req.PeriodCoverage,
			"run_time":           req.RunTime,
			"next_run_at":        nextRun,
			"is_active":          false,
			"processing_status":  constants.StatusPendingApproval,
		})
		go func(cfgID, entID, uID, email, matrixID string) {
			defer func() { recover() }() //nolint:errcheck
			bg := context.Background()
			notifcatalog.TriggerNotification(bg, pgxPool, "/investment/fd/accrual/schedule/create", cfgID, map[string]interface{}{
				"record_id": cfgID, "event": "FD_ACCRUAL_SCHEDULE_CREATED", "actor_email": email, "entity_id": entID,
			})
			dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_FD", "FD_ACCRUAL_SCHED", "POST_CREATE", []string{cfgID}, email)
			_, _ = approvalengine.CreateInstance(bg, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:          "FIXED_DEPOSIT",
				EntityCode:          entID,
				TransactionType:     "FD_ACCRUAL_SCHEDULE_CREATE",
				RecordID:            cfgID,
				RecordTable:         constants.QuerryAccrualScheduleConfig,
				AuditTable:          constants.QuerryAccrualScheduleConfigAudit,
				AuditIDColumn:       "config_id",
				ActionType:          "CREATE",
				SubmittedBy:         uID,
				SubmittedByEmail:    email,
				MatrixID:            matrixID,
				RequirePinnedMatrix: true,
				AutoApplyIfUnpinned: true,
			})
		}(configID, req.EntityID, req.UserID, userEmail, schedCreateMatrixID)
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

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		entityID, ok, scopeErr := requireScheduleConfigScope(ctx, pgxPool, req.ConfigID)
		if scopeErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScheduleConfigLookupFailed+scopeErr.Error())
			return
		}
		if !ok {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
			return
		}
		if bankFilter, has := req.Fields["default_bank_id_filter"]; has {
			if bank := strings.TrimSpace(fmt.Sprint(bankFilter)); bank != "" && !ctxutil.FromContext(ctx).HasApprovedBank(bank) {
				api.RespondWithError(w, http.StatusForbidden,
					fmt.Sprintf(constants.ErrBankNotApproved1, bank))
				return
			}
		}

		// Allowlist of updatable columns
		allowed := map[string]bool{
			"schedule_frequency":       true,
			"period_coverage":          true,
			"run_day_of_month":         true,
			"run_time":                 true,
			"default_bank_id_filter":   true,
			"default_fd_status_filter": true,
			"default_run_mode":         true,
			"auto_submit_for_approval": true,
			"accrual_granularity":      true,
			"notification_recipients":  true,
		}

		setParts := []string{"updated_by = $1", "updated_at = now()"}
		args := []interface{}{userEmail}

		for col, val := range req.Fields {
			if !allowed[col] {
				continue
			}
			if col == "run_time" {
				if s, ok := val.(string); ok {
					parsed, pErr := parseRunTimeForDB(s)
					if pErr != nil {
						api.RespondWithError(w, http.StatusBadRequest, pErr.Error())
						return
					}
					val = parsed
				}
			}
			args = append(args, val)
			setParts = append(setParts, fmt.Sprintf("%s = $%d", col, len(args)))
		}

		if len(setParts) <= 2 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields provided")
			return
		}

		updateBaseRow, pfErr := loadFDAccrualScheduleRow(ctx, pgxPool, req.ConfigID)
		if pfErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScheduleConfigLookupFailed+pfErr.Error())
			return
		}
		updatedRow := applyFDAccrualScheduleEdits(updateBaseRow, req.Fields)
		schedEditOK, schedEditMatrixID := fdAccrualSchedEnforceMatrix(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreEdit,
			HandlerName: "UpdateScheduleConfig",
			APIPath:     "/investment/fd/accrual/schedule/update",
			EntityCode:  entityID,
			Actor:       userEmail,
		}, buildFDAccrualSchedulePolicyFields(updatedRow))
		if !schedEditOK {
			return
		}

		// Snapshot old values before update
		var oldFreq, oldRunMode, oldBankFilter, oldFDFilter, oldPeriodCoverage string
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
				COALESCE(notification_recipients,'[]'::jsonb)::text,
				COALESCE(period_coverage,'RUN')
			FROM investment.fd_accrual_schedule_config
			WHERE config_id=$1`, req.ConfigID,
		).Scan(&oldFreq, &oldRunDay, &oldRunTime, &oldRunMode, &oldBankFilter, &oldFDFilter, &oldAutoSubmit, &oldNotifRecipients, &oldPeriodCoverage)

		args = append(args, req.ConfigID)
		query := fmt.Sprintf(
			"UPDATE investment.fd_accrual_schedule_config SET %s WHERE config_id = $%d AND COALESCE(is_deleted,false)=false",
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

		// Recompute next_run_at when schedule timing fields change
		if _, freqOK := req.Fields["schedule_frequency"]; freqOK {
			recalcScheduleNextRun(ctx, pgxPool, req.ConfigID)
		} else if _, dayOK := req.Fields["run_day_of_month"]; dayOK {
			recalcScheduleNextRun(ctx, pgxPool, req.ConfigID)
		} else if _, timeOK := req.Fields["run_time"]; timeOK {
			recalcScheduleNextRun(ctx, pgxPool, req.ConfigID)
		}

		// Audit trail with full old-value snapshot
		_, _ = pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_schedule_config_audit (
				config_id, action_type, processing_status, requested_by, requested_at, requested_ip,
				old_schedule_frequency, old_run_day_of_month, old_run_time,
				old_default_run_mode, old_default_bank_id_filter, old_default_fd_status_filter,
				old_auto_submit_for_approval, old_notification_recipients, old_is_active,
				old_period_coverage
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,now(),$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,true,$12)`,
			req.ConfigID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldFreq, oldRunDay, oldRunTime, oldRunMode, oldBankFilter, oldFDFilter,
			oldAutoSubmit, string(oldNotifRecipients), oldPeriodCoverage)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":         req.ConfigID,
			"updated":           true,
			"processing_status": constants.StatusPendingEditApproval,
		})
		go func(cfgID, entID, uID, email, matrixID string) {
			defer func() { recover() }() //nolint:errcheck
			bg := context.Background()
			notifcatalog.TriggerNotification(bg, pgxPool, "/investment/fd/accrual/schedule/update", cfgID, map[string]interface{}{
				"record_id": cfgID, "event": "FD_ACCRUAL_SCHEDULE_UPDATED", "actor_email": email,
			})
			_, _ = approvalengine.CreateInstance(bg, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:          "FIXED_DEPOSIT",
				EntityCode:          entID,
				TransactionType:     "FD_ACCRUAL_SCHEDULE_EDIT",
				RecordID:            cfgID,
				RecordTable:         constants.QuerryAccrualScheduleConfig,
				AuditTable:          constants.QuerryAccrualScheduleConfigAudit,
				AuditIDColumn:       "config_id",
				ActionType:          "EDIT",
				SubmittedBy:         uID,
				SubmittedByEmail:    email,
				MatrixID:            matrixID,
				RequirePinnedMatrix: true,
				AutoApplyIfUnpinned: true,
			})
		}(req.ConfigID, entityID, req.UserID, userEmail, schedEditMatrixID)
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
		scope := ctxutil.FromContext(ctx)
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
				is_active, created_at,
				COALESCE(period_coverage,'RUN') AS period_coverage
			FROM investment.fd_accrual_schedule_config
			WHERE is_active = true`
		args := []interface{}{}
		if req.EntityID != "" {
			if !scope.HasEntityAccess(req.EntityID) {
				api.RespondWithError(w, http.StatusForbidden,
					fmt.Sprintf(constants.ErrEntityIDNotAuthorized, req.EntityID))
				return
			}
			query += " AND entity_id = $1"
			args = append(args, req.EntityID)
		} else if len(scope.EntityIDs) > 0 {
			query += " AND entity_id = ANY($1::text[])"
			args = append(args, scope.EntityIDs)
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

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		if _, ok, scopeErr := requireScheduleConfigScope(ctx, pgxPool, req.ConfigID); scopeErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScheduleConfigLookupFailed+scopeErr.Error())
			return
		} else if !ok {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
			return
		}
		schedFields, disableEntityID, pfErr := accrualSchedulePolicyFields(ctx, pgxPool, req.ConfigID)
		if pfErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pfErr.Error())
			return
		}
		if !fdAccrualSchedEnforce(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreEdit,
			HandlerName: "DisableSchedule",
			APIPath:     "/investment/fd/accrual/schedule/disable",
			EntityCode:  disableEntityID,
			Actor:       userEmail,
		}, schedFields) {
			return
		}
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

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		if _, ok, scopeErr := requireScheduleConfigScope(ctx, pgxPool, req.ConfigID); scopeErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScheduleConfigLookupFailed+scopeErr.Error())
			return
		} else if !ok {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
			return
		}
		schedFields, enableEntityID, pfErr := accrualSchedulePolicyFields(ctx, pgxPool, req.ConfigID)
		if pfErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pfErr.Error())
			return
		}
		if !fdAccrualSchedEnforce(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreEdit,
			HandlerName: "EnableSchedule",
			APIPath:     "/investment/fd/accrual/schedule/enable",
			EntityCode:  enableEntityID,
			Actor:       userEmail,
		}, schedFields) {
			return
		}
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

// ─── 6. ApproveScheduleConfig ───────────────────────────────────────────────────
// Approves pending CREATE / EDIT / DELETE requests on fd_accrual_schedule_config.

func ApproveScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ConfigID  string   `json:"config_id"`
			ConfigIDs []string `json:"config_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		configIDs := scheduleConfigIDsFromReq(req.ConfigID, req.ConfigIDs)
		if len(configIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
			return
		}

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		directConfigIDs := make([]string, 0, len(configIDs))
		approved := []string{}
		deleted := []string{}
		var errors []string
		for _, configID := range configIDs {
			comment := req.Comment
			if strings.TrimSpace(comment) == "" {
				comment = "Approved FD accrual schedule"
			}
			schedFields, approveEntityID, pfErr := accrualSchedulePolicyFields(ctx, pgxPool, configID)
			if pfErr != nil {
				errors = append(errors, configID+": "+pfErr.Error())
				continue
			}
			if ok, pmsg := fdAccrualSchedEnforceInline(ctx, r, pgxPool, enforceCtx{
				EventCode:   common.TriggerPreApprove,
				HandlerName: "ApproveAccrualSchedule",
				APIPath:     "/investment/fd/accrual/schedule/approve",
				EntityCode:  approveEntityID,
				Actor:       userEmail,
			}, schedFields); !ok {
				errors = append(errors, configID+": "+pmsg)
				continue
			}
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: configID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionApproved, Comment: comment})
			if actionErr != nil {
				errors = append(errors, configID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				if actionRes.InstanceStatus == approvalengine.InstStatusApproved {
					isDeleted, applyErr := applyApprovedScheduleConfig(ctx, pgxPool, configID, userEmail)
					if applyErr != nil {
						errors = append(errors, configID+": apply failed: "+applyErr.Error())
						continue
					}
					if isDeleted {
						deleted = append(deleted, configID)
					} else {
						approved = append(approved, configID)
					}
				}
				continue
			}
			if actionRes.CancelledStale {
				api.LogInfo("[FDAccrual] Cancelled stale schedule approval instance for config=%s: %s", configID, actionRes.Reason)
			} else if actionRes.Reason != "" {
				errors = append(errors, configID+": "+actionRes.Reason)
				continue
			}
			directConfigIDs = append(directConfigIDs, configID)
		}

		if len(directConfigIDs) > 0 {
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Transaction failed: "+err.Error())
				return
			}
			defer tx.Rollback(ctx) //nolint:errcheck

			directApproved, directDeleted, err := finalizeScheduleConfigAudits(ctx, tx, directConfigIDs, userEmail, req.Comment, true)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			approved = append(approved, directApproved...)
			deleted = append(deleted, directDeleted...)
		}

		for _, id := range approved {
			recalcScheduleNextRun(ctx, pgxPool, id)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"approved_config_ids": approved,
			"deleted_config_ids":  deleted,
			"approved_count":      len(approved),
			"deleted_count":       len(deleted),
			"errors":              errors,
			"checker":             userEmail,
		})
		for _, id := range approved {
			go func(cfgID, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDAccrual] ApproveScheduleConfig notification panic for %s: %v", cfgID, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/investment/fd/accrual/schedule/approve", cfgID, map[string]interface{}{
						"record_id":   cfgID,
						"event":       "FD_ACCRUAL_SCHEDULE_APPROVED",
						"actor_email": uEmail,
					})
				trigger := "POST_APPROVE"
				var actionType string
				if qerr := pgxPool.QueryRow(context.Background(), `
					SELECT COALESCE(action_type,'')
					FROM investment.fd_accrual_schedule_config_audit
					WHERE config_id=$1 AND processing_status IN ('APPROVED','DELETED')
					ORDER BY COALESCE(checker_at, requested_at) DESC NULLS LAST
					LIMIT 1`, cfgID).Scan(&actionType); qerr == nil {
					if strings.EqualFold(strings.TrimSpace(actionType), "EDIT") {
						trigger = "POST_EDIT"
					}
				}
				dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_FD", "FD_ACCRUAL_SCHED", trigger, []string{cfgID}, uEmail)
			}(id, userEmail)
		}
		for _, id := range deleted {
			go func(cfgID, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDAccrual] ApproveScheduleConfig delete notification panic for %s: %v", cfgID, rec)
					}
				}()
				dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_FD", "FD_ACCRUAL_SCHED", "POST_DELETE", []string{cfgID}, uEmail)
			}(id, userEmail)
		}
		api.LogInfo("[FDAccrual] ApproveScheduleConfig: approved=%d deleted=%d by=%s", len(approved), len(deleted), userEmail)
	}
}

// ApproveAccrualSchedule is kept as an alias for backward-compatible route wiring.
func ApproveAccrualSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return ApproveScheduleConfig(pgxPool)
}

// ─── 7. RejectScheduleConfig ────────────────────────────────────────────────────

func RejectScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ConfigID  string   `json:"config_id"`
			ConfigIDs []string `json:"config_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		configIDs := scheduleConfigIDsFromReq(req.ConfigID, req.ConfigIDs)
		if len(configIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrConfigIDRequired)
			return
		}

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		directConfigIDs := make([]string, 0, len(configIDs))
		rejected := []string{}
		var errors []string
		for _, configID := range configIDs {
			comment := req.Comment
			if strings.TrimSpace(comment) == "" {
				comment = "Rejected FD accrual schedule"
			}
			schedFields, rejectEntityID, pfErr := accrualSchedulePolicyFields(ctx, pgxPool, configID)
			if pfErr != nil {
				errors = append(errors, configID+": "+pfErr.Error())
				continue
			}
			if ok, pmsg := fdAccrualSchedEnforceInline(ctx, r, pgxPool, enforceCtx{
				EventCode:   common.TriggerPreReject,
				HandlerName: "RejectAccrualSchedule",
				APIPath:     "/investment/fd/accrual/schedule/reject",
				EntityCode:  rejectEntityID,
				Actor:       userEmail,
			}, schedFields); !ok {
				errors = append(errors, configID+": "+pmsg)
				continue
			}
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "FIXED_DEPOSIT", RecordID: configID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionRejected, Comment: comment})
			if actionErr != nil {
				errors = append(errors, configID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				if _, execErr := pgxPool.Exec(ctx, `
					UPDATE investment.fd_accrual_schedule_config
					SET is_active=false, updated_by=$1, updated_at=now()
					WHERE config_id=$2`, userEmail, configID); execErr != nil {
					errors = append(errors, configID+": status update failed: "+execErr.Error())
					continue
				}
				rejected = append(rejected, configID)
				continue
			}
			if actionRes.CancelledStale {
				api.LogInfo("[FDAccrual] Cancelled stale schedule rejection instance for config=%s: %s", configID, actionRes.Reason)
			} else if actionRes.Reason != "" {
				errors = append(errors, configID+": "+actionRes.Reason)
				continue
			}
			directConfigIDs = append(directConfigIDs, configID)
		}

		if len(directConfigIDs) > 0 {
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Transaction failed: "+err.Error())
				return
			}
			defer tx.Rollback(ctx) //nolint:errcheck

			if _, _, err = finalizeScheduleConfigAudits(ctx, tx, directConfigIDs, userEmail, req.Comment, false); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			rejected = append(rejected, directConfigIDs...)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rejected_config_ids": rejected,
			"rejected_count":      len(rejected),
			"errors":              errors,
			"checker":             userEmail,
		})
		for _, id := range rejected {
			go func(cfgID, uEmail string) {
				defer func() {
					if rec := recover(); rec != nil {
						api.LogError("[FDAccrual] RejectScheduleConfig notification panic for %s: %v", cfgID, rec)
					}
				}()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/investment/fd/accrual/schedule/reject", cfgID, map[string]interface{}{
						"record_id":   cfgID,
						"event":       "FD_ACCRUAL_SCHEDULE_REJECTED",
						"actor_email": uEmail,
					})
				dmsjobs.FireDmsEvent(pgxPool, "INVESTMENT_FD", "FD_ACCRUAL_SCHED", "POST_REJECT", []string{cfgID}, uEmail)
			}(id, userEmail)
		}
		api.LogInfo("[FDAccrual] RejectScheduleConfig: count=%d errors=%d by=%s", len(rejected), len(errors), userEmail)
	}
}

// RejectAccrualSchedule is kept as an alias for backward-compatible route wiring.
func RejectAccrualSchedule(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return RejectScheduleConfig(pgxPool)
}

func scheduleConfigIDsFromReq(configID string, configIDs []string) []string {
	ids := make([]string, 0, len(configIDs)+1)
	seen := make(map[string]bool)
	if configID != "" && !seen[configID] {
		ids = append(ids, configID)
		seen[configID] = true
	}
	for _, id := range configIDs {
		if id != "" && !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	return ids
}

// finalizeScheduleConfigAudits approves or rejects pending schedule audits and updates config rows.
func finalizeScheduleConfigAudits(
	ctx context.Context,
	tx pgx.Tx,
	configIDs []string,
	checkerEmail, comment string,
	approve bool,
) (approved []string, deleted []string, err error) {
	targetStatus := constants.StatusRejected
	if approve {
		targetStatus = constants.StatusApproved
	}

	rows, err := tx.Query(ctx, `
		SELECT DISTINCT ON (config_id)
			config_id, action_type, processing_status
		FROM investment.fd_accrual_schedule_config_audit
		WHERE config_id = ANY($1)
		ORDER BY config_id, requested_at DESC`, configIDs)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	type pendingRow struct {
		configID, actionType, status string
	}
	var pending []pendingRow
	for rows.Next() {
		var p pendingRow
		if err := rows.Scan(&p.configID, &p.actionType, &p.status); err != nil {
			return nil, nil, err
		}
		if strings.Contains(p.status, "PENDING") {
			pending = append(pending, p)
		}
	}

	for _, p := range pending {
		finalAuditStatus := targetStatus
		if approve && p.actionType == "DELETE" {
			finalAuditStatus = "DELETED"
		}

		ct, uErr := tx.Exec(ctx, `
			UPDATE investment.fd_accrual_schedule_config_audit
			SET processing_status=$1, checker_by=$2, checker_at=now(), checker_comment=$3, checker_ip=$4
			WHERE config_id=$5 AND processing_status=$6`,
			finalAuditStatus, api.SystemIfBlank(checkerEmail), comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)), p.configID, p.status)
		if uErr != nil {
			return nil, nil, uErr
		}
		if ct.RowsAffected() == 0 {
			continue
		}

		if approve {
			switch p.actionType {
			case "DELETE":
				_, _ = tx.Exec(ctx, `
					UPDATE investment.fd_accrual_schedule_config
					SET is_active=false, is_deleted=true, updated_by=$1, updated_at=now()
					WHERE config_id=$2`, checkerEmail, p.configID)
				deleted = append(deleted, p.configID)
			default:
				// Read timing params inside the tx to compute a guaranteed-future next_run_at
				// before setting is_active=true — avoids the race where the worker fires
				// immediately with a stale (past) next_run_at from creation time.
				var freq string
				var runDay int
				var runTimeVal interface{}
				_ = tx.QueryRow(ctx, `
					SELECT COALESCE(schedule_frequency,'MONTHLY'),
					       COALESCE(run_day_of_month,1),
					       run_time
					FROM investment.fd_accrual_schedule_config
					WHERE config_id=$1`, p.configID,
				).Scan(&freq, &runDay, &runTimeVal)
				nextRun := computeNextRunAt(freq, runDay, runTimeVal, time.Now())
				_, _ = tx.Exec(ctx, `
					UPDATE investment.fd_accrual_schedule_config
					SET is_active=true, next_run_at=$1, updated_by=$2, updated_at=now()
					WHERE config_id=$3`, nextRun, checkerEmail, p.configID)
				approved = append(approved, p.configID)
			}
		} else {
			_, _ = tx.Exec(ctx, `
				UPDATE investment.fd_accrual_schedule_config
				SET is_active=false, updated_by=$1, updated_at=now()
				WHERE config_id=$2`, checkerEmail, p.configID)
		}
	}
	return approved, deleted, nil
}

func applyApprovedScheduleConfig(ctx context.Context, pool *pgxpool.Pool, configID, checkerEmail string) (bool, error) {
	var actionType string
	if err := pool.QueryRow(ctx, `
		SELECT action_type
		FROM investment.fd_accrual_schedule_config_audit
		WHERE config_id=$1
		ORDER BY requested_at DESC NULLS LAST, audit_id DESC
		LIMIT 1`, configID).Scan(&actionType); err != nil {
		return false, err
	}
	if actionType == "DELETE" {
		_, err := pool.Exec(ctx, `
			UPDATE investment.fd_accrual_schedule_config
			SET is_active=false, is_deleted=true, updated_by=$1, updated_at=now()
			WHERE config_id=$2`, checkerEmail, configID)
		return true, err
	}
	// Compute next_run_at before activating to prevent the race where the background
	// worker sees is_active=true with a stale (past) next_run_at and fires immediately.
	nextRun := loadAndComputeNextRunAt(ctx, pool, configID)
	_, err := pool.Exec(ctx, `
		UPDATE investment.fd_accrual_schedule_config
		SET is_active=true, next_run_at=$1, updated_by=$2, updated_at=now()
		WHERE config_id=$3`, nextRun, checkerEmail, configID)
	return false, err
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

		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		entityID, ok, scopeErr := requireScheduleConfigScope(ctx, pgxPool, req.ConfigID)
		if scopeErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScheduleConfigLookupFailed+scopeErr.Error())
			return
		}
		if !ok {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
			return
		}

		// Snapshot old values before delete
		var dOldFreq, dOldRunMode, dOldBankFilter, dOldFDFilter, dOldPeriodCoverage string
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
				COALESCE(notification_recipients,'[]'::jsonb)::text,
				COALESCE(period_coverage,'RUN')
			FROM investment.fd_accrual_schedule_config
			WHERE config_id=$1`, req.ConfigID,
		).Scan(&dOldFreq, &dOldRunDay, &dOldRunTime, &dOldRunMode, &dOldBankFilter, &dOldFDFilter, &dOldAutoSubmit, &dOldNotif, &dOldPeriodCoverage)

		var exists bool
		_ = pgxPool.QueryRow(ctx, `SELECT true FROM investment.fd_accrual_schedule_config WHERE config_id=$1`, req.ConfigID).Scan(&exists)
		if !exists {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
			return
		}

		deleteRow, pfErr := loadFDAccrualScheduleRow(ctx, pgxPool, req.ConfigID)
		if pfErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScheduleConfigLookupFailed+pfErr.Error())
			return
		}
		schedDeleteOK, schedDeleteMatrixID := fdAccrualSchedEnforceMatrix(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreDelete,
			HandlerName: "DeleteScheduleConfig",
			APIPath:     "/investment/fd/accrual/schedule/delete",
			EntityCode:  entityID,
			Actor:       userEmail,
		}, buildFDAccrualSchedulePolicyFields(deleteRow))
		if !schedDeleteOK {
			return
		}

		// Audit trail — deactivate only after delete is approved
		if _, err := pgxPool.Exec(ctx, `
			INSERT INTO investment.fd_accrual_schedule_config_audit (
				config_id, action_type, processing_status, requested_by, requested_at, requested_ip,
				old_schedule_frequency, old_run_day_of_month, old_run_time,
				old_default_run_mode, old_default_bank_id_filter, old_default_fd_status_filter,
				old_auto_submit_for_approval, old_notification_recipients, old_is_active,
				old_period_coverage
			) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,now(),$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,true,$12)`,
			req.ConfigID, api.SystemIfBlank(userEmail), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			dOldFreq, dOldRunDay, dOldRunTime, dOldRunMode, dOldBankFilter, dOldFDFilter,
			dOldAutoSubmit, string(dOldNotif), dOldPeriodCoverage); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Delete audit insert failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":         req.ConfigID,
			"processing_status": constants.StatusPendingDeleteApproval,
		})
		go func(cfgID, entID, uID, email, matrixID string) {
			defer func() { recover() }() //nolint:errcheck
			bg := context.Background()
			notifcatalog.TriggerNotification(bg, pgxPool, "/investment/fd/accrual/schedule/delete", cfgID, map[string]interface{}{
				"record_id": cfgID, "event": "FD_ACCRUAL_SCHEDULE_DELETED", "actor_email": email,
			})
			_, _ = approvalengine.CreateInstance(bg, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:          "FIXED_DEPOSIT",
				EntityCode:          entID,
				TransactionType:     "FD_ACCRUAL_SCHEDULE_DELETE",
				RecordID:            cfgID,
				RecordTable:         constants.QuerryAccrualScheduleConfig,
				AuditTable:          constants.QuerryAccrualScheduleConfigAudit,
				AuditIDColumn:       "config_id",
				ActionType:          "DELETE",
				SubmittedBy:         uID,
				SubmittedByEmail:    email,
				MatrixID:            matrixID,
				RequirePinnedMatrix: true,
				AutoApplyIfUnpinned: true,
			})
		}(req.ConfigID, entityID, req.UserID, userEmail, schedDeleteMatrixID)
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
		       run_time,
		       COALESCE(default_bank_id_filter,''),
		       COALESCE(default_fd_status_filter,'ACTIVE'),
		       default_run_mode,
		       auto_submit_for_approval,
		       COALESCE(accrual_granularity,'MONTHLY') AS accrual_granularity,
		       COALESCE(period_coverage,'RUN') AS period_coverage,
		       COALESCE(next_run_at, now()) AS next_run_at
		FROM investment.fd_accrual_schedule_config c
		WHERE c.is_active = true
		  AND c.next_run_at IS NOT NULL
		  AND c.next_run_at <= now()
		  AND COALESCE((
			SELECT a.processing_status
			FROM investment.fd_accrual_schedule_config_audit a
			WHERE a.config_id = c.config_id
			ORDER BY a.requested_at DESC NULLS LAST
			LIMIT 1
		  ), '') = 'APPROVED'
		ORDER BY c.next_run_at`)
	if err != nil {
		api.LogError("[FDAccrual] checkAndFireDueSchedules query: %v", err)
		return
	}
	defer rows.Close()

	type dueConfig struct {
		ConfigID, EntityID, EntityName          string
		ScheduleFrequency, BankFilter, FDStatus string
		DefaultRunMode                          string
		PeriodCoverage                          string
		RunDayOfMonth                           int
		AutoSubmit                              bool
		Granularity                             string
		NextRunAt                               time.Time
	}
	var due []dueConfig
	for rows.Next() {
		var c dueConfig
		var runTime interface{} // stored in next_run_at; scanned to keep column alignment
		if err := rows.Scan(
			&c.ConfigID, &c.EntityID, &c.EntityName,
			&c.ScheduleFrequency, &c.RunDayOfMonth,
			&runTime,
			&c.BankFilter, &c.FDStatus,
			&c.DefaultRunMode, &c.AutoSubmit,
			&c.Granularity, &c.PeriodCoverage, &c.NextRunAt,
		); err != nil {
			api.LogError("[FDAccrual] checkAndFireDueSchedules scan: %v", err)
			continue
		}
		due = append(due, c)
	}
	rows.Close()

	api.LogInfo("[FDAccrual] Scheduler tick: %d due config(s)", len(due))
	for _, c := range due {
		fireScheduledRun(ctx, pool, FireScheduledParams{
			ConfigID:       c.ConfigID,
			EntityID:       c.EntityID,
			EntityName:     c.EntityName,
			ScheduleFreq:   c.ScheduleFrequency,
			PeriodCoverage: c.PeriodCoverage,
			BankFilter:     c.BankFilter,
			FDStatus:       c.FDStatus,
			RunMode:        c.DefaultRunMode,
			RunDay:         c.RunDayOfMonth,
			AutoSubmit:     c.AutoSubmit,
			Granularity:    c.Granularity,
			ScheduledAt:    c.NextRunAt,
		})
	}
}

// fireScheduledRun creates a run, validates, executes, and optionally submits.
type FireScheduledParams struct {
	ConfigID       string
	EntityID       string
	EntityName     string
	ScheduleFreq   string
	PeriodCoverage string // the date window each run covers (MONTHLY/QUARTERLY/HALF_YEARLY/YEARLY/RUN)
	BankFilter     string
	FDStatus       string
	RunMode        string
	RunDay         int
	AutoSubmit     bool
	Granularity    string
	ScheduledAt    time.Time
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
	autoSubmit := p.AutoSubmit
	granularity := p.Granularity
	scheduledAt := p.ScheduledAt
	if granularity == "" {
		granularity = "MONTHLY"
	}

	// period_coverage controls the date window each run covers.
	// accrual_granularity controls only the interest calculation steps inside the run.
	// When period_coverage=RUN (or empty), fall back to schedule_frequency for period bounds.
	periodWindow := strings.ToUpper(strings.TrimSpace(p.PeriodCoverage))
	if periodWindow == "" || periodWindow == "RUN" {
		periodWindow = periodGranularityForScheduler("RUN", scheduleFreq)
	}

	//  — CURRENT: calendar-month bounds (May 1 → May 31 when fire date is May 24).
	// To switch to FD-date-based "24 to 24" periods, replace the line below with:
	//
	//   anchorDay := p.RunDay  // e.g. 24 (the run_day_of_month from the schedule config)
	//   periodEnd   := time.Date(scheduledAt.Year(), scheduledAt.Month(), anchorDay, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	//   periodStart := time.Date(scheduledAt.Year(), scheduledAt.Month()-1, anchorDay, 0, 0, 0, 0, time.UTC)
	//   // Example: fire on May 24 → periodStart = Apr 24, periodEnd = May 23
	//   // Go's time.Date handles month underflow: Month(1)-1 = December of prior year automatically.
	//
	// After making this change also update AccrualPeriodBounds signature as described
	// in engine.go above the function definition.
	periodStart, periodEnd := AccrualPeriodBounds(scheduledAt, periodWindow)
	financialPeriod := buildAccrualPeriod(periodStart)
	runType := RunTypeForGranularity(granularity) // run_type reflects accrual_granularity, not period window

	api.LogInfo("[FDAccrual] Scheduler firing config=%s entity=%s schedule_freq=%s accrual_granularity=%s period_window=%s period=%s→%s bank_filter=%q",
		configID, entityID, scheduleFreq, granularity, periodWindow,
		periodStart.Format(constants.DateFormat), periodEnd.Format(constants.DateFormat),
		bankFilter)

	// ── Duplicate guard ─────────────────────────────────────────────────────
	// Skip if a scheduler-created run already exists for this entity+period+mode
	// that is not in a terminal failure state.
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
		logAccrualEvent(ctx, pool, LogAccrualParams{
			RunID:     existingRun,
			Level:     "INFO",
			EventType: "SCHEDULER_SKIPPED_DUPLICATE",
			Message: fmt.Sprintf("Scheduler skipped: run %s already covers entity=%s period=%s→%s mode=%s config=%s",
				existingRun, entityID,
				periodStart.Format(constants.DateFormat), periodEnd.Format(constants.DateFormat),
				runMode, configID),
			Detail: map[string]interface{}{
				"schedule_config_id":   configID,
				"duplicate_run_id":     existingRun,
				"accrual_period_start": periodStart.Format(constants.DateFormat),
				"accrual_period_end":   periodEnd.Format(constants.DateFormat),
				"run_mode":             runMode,
			},
		})
		// When the skipped run was FINAL, auto-fire a companion SIMULATION run so
		// the user gets a fresh simulation for this period (unless one already exists).
		if strings.ToUpper(runMode) == "FINAL" {
			var existingSim string
			_ = pool.QueryRow(ctx, `
				SELECT COALESCE(run_id,'') FROM investment.fd_accrual_run
				WHERE entity_id=$1
				  AND accrual_period_start=$2
				  AND accrual_period_end=$3
				  AND run_mode='SIMULATION'
				  AND created_by='SCHEDULER'
				  AND run_status NOT IN ('FAILED','VALIDATION_FAILED')
				LIMIT 1`,
				entityID, periodStart, periodEnd,
			).Scan(&existingSim)
			if existingSim == "" {
				fireScheduledRun(ctx, pool, FireScheduledParams{
					ConfigID:     p.ConfigID,
					EntityID:     p.EntityID,
					EntityName:   p.EntityName,
					ScheduleFreq: p.ScheduleFreq,
					BankFilter:   p.BankFilter,
					FDStatus:     p.FDStatus,
					RunMode:      "SIMULATION",
					RunDay:       p.RunDay,
					AutoSubmit:   false, // simulations never auto-submit
					Granularity:  p.Granularity,
					ScheduledAt:  p.ScheduledAt,
				})
			}
		}
		updateLastRunStatus(ctx, pool, configID, existingRun, "SKIPPED_DUPLICATE")
		return
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

	if ok, msg := EnforceScheduledAccrualFire(ctx, pool, p,
		periodStart.Format(constants.DateFormat), periodEnd.Format(constants.DateFormat)); !ok {
		api.LogError("[FDAccrual] Scheduler policy blocked config=%s entity=%s: %s", configID, entityID, msg)
		updateLastRunStatus(ctx, pool, configID, "", truncateStatus("POLICY_BLOCKED: "+msg))
		return
	}

	runID, err := createAccrualRunInternal(ctx, pool, input)
	if err != nil {
		api.LogError("[FDAccrual] Scheduler createRun failed entity=%s: %v", entityID, err)
		updateLastRunStatus(ctx, pool, configID, "", truncateStatus("CREATE_FAILED: "+err.Error()))
		return
	}
	api.LogInfo("[FDAccrual] Scheduler created run_id=%s entity=%s period=%s→%s",
		runID, entityID, periodStart.Format(constants.DateFormat), periodEnd.Format(constants.DateFormat))
	logAccrualEvent(ctx, pool, LogAccrualParams{
		RunID:     runID,
		Level:     "INFO",
		EventType: "SCHEDULER_CREATED",
		Message:   fmt.Sprintf("Scheduler config=%s created run for entity=%s", configID, entityID),
		Detail: map[string]interface{}{
			"schedule_config_id":  configID,
			"schedule_frequency":  scheduleFreq,
			"accrual_granularity": granularity,
			"period_start":        periodStart.Format(constants.DateFormat),
			"period_end":          periodEnd.Format(constants.DateFormat),
		},
	})

	// Validate
	eligible, blockers, _, vErr := validateAndPersistFindings(ctx, pool, runID)
	if vErr != nil {
		api.LogError("[FDAccrual] Scheduler validate run=%s: %v", runID, vErr)
		updateLastRunStatus(ctx, pool, configID, runID, "VALIDATE_FAILED")
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
		updateLastRunStatus(ctx, pool, configID, runID, "VALIDATION_FAILED")
		return
	}

	// Execute
	calculated, failed, exErr := executeAccrualRun(ctx, pool, runID, "SCHEDULER")
	if exErr != nil {
		api.LogError("[FDAccrual] Scheduler execute run=%s: %v", runID, exErr)
		updateLastRunStatus(ctx, pool, configID, runID, "EXECUTE_FAILED")
		return
	}
	api.LogInfo("[FDAccrual] Scheduler run=%s calculated=%d failed=%d", runID, calculated, failed)
	logAccrualEvent(ctx, pool, LogAccrualParams{
		RunID:     runID,
		Level:     "INFO",
		EventType: "SCHEDULER_EXECUTED",
		Message:   fmt.Sprintf("Scheduler executed run: calculated=%d failed=%d", calculated, failed),
		Detail: map[string]interface{}{
			"schedule_config_id": configID,
			"fds_calculated":     calculated,
			"fds_failed":         failed,
		},
	})

	// Submit for approval if configured
	finalStatus := "COMPUTED"
	if autoSubmit && runMode != "SIMULATION" {
		if sErr := submitAccrualRunForApproval(ctx, pool, runID, "SCHEDULER"); sErr != nil {
			api.LogError("[FDAccrual] Scheduler submit run=%s: %v", runID, sErr)
		} else {
			finalStatus = constants.StatusPendingApproval
		}
	}

	updateLastRunStatus(ctx, pool, configID, runID, finalStatus)
}

// updateLastRunStatus bumps last_run_status and always advances next_run_at.
func updateLastRunStatus(ctx context.Context, pool *pgxpool.Pool, configID, runID, status string) {
	var runIDArg interface{} = nil
	if runID != "" {
		runIDArg = runID
	}
	nextRun := loadAndComputeNextRunAt(ctx, pool, configID)
	_, _ = pool.Exec(ctx, `
		UPDATE investment.fd_accrual_schedule_config
		SET last_run_at=now(), last_run_id=$1, last_run_status=$2,
		    next_run_at=$3, updated_at=now()
		WHERE config_id=$4`,
		runIDArg, status, nextRun, configID)
}

// RecalcScheduleNextRun recalculates next_run_at from current config timing (e.g. after fixing run_time).
func RecalcScheduleNextRun(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		if getUserEmail(r.Context()) == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		nextRun := loadAndComputeNextRunAt(ctx, pgxPool, req.ConfigID)
		ct, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_schedule_config
			SET next_run_at=$1, updated_at=now()
			WHERE config_id=$2 AND is_active=true`, nextRun, req.ConfigID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Recalc failed: "+err.Error())
			return
		}
		if ct.RowsAffected() == 0 {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrScheduleConfigNotFound)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":   req.ConfigID,
			"next_run_at": nextRun,
		})
	}
}

// recalcScheduleNextRun reloads timing from DB and writes next_run_at (used after config edits).
func recalcScheduleNextRun(ctx context.Context, pool *pgxpool.Pool, configID string) {
	nextRun := loadAndComputeNextRunAt(ctx, pool, configID)
	_, _ = pool.Exec(ctx, `
		UPDATE investment.fd_accrual_schedule_config
		SET next_run_at=$1, updated_at=now()
		WHERE config_id=$2`, nextRun, configID)
}

// loadAndComputeNextRunAt reads schedule_frequency, run_day_of_month, run_time from config.
func loadAndComputeNextRunAt(ctx context.Context, pool *pgxpool.Pool, configID string) time.Time {
	var freq string
	var runDay int
	var runTime interface{}
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(schedule_frequency,'MONTHLY'),
		       COALESCE(run_day_of_month,1),
		       run_time
		FROM investment.fd_accrual_schedule_config
		WHERE config_id=$1`, configID,
	).Scan(&freq, &runDay, &runTime)
	if err != nil {
		return computeNextRunAt("MONTHLY", 1, nil, time.Now())
	}
	return computeNextRunAt(freq, runDay, runTime, time.Now())
}

// scheduleTZ is the timezone used for run_day / run_time from the UI (IST).
func scheduleTZ() *time.Location {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return time.UTC
	}
	return loc
}

// parseRunTimeForDB normalizes UI time strings for PostgreSQL TIME column.
func parseRunTimeForDB(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	h, m, sec, ok := parseRunTimeComponents(s)
	if !ok {
		return nil, fmt.Errorf("run_time must be HH:MM or HH:MM:SS")
	}
	return fmt.Sprintf("%02d:%02d:%02d", h, m, sec), nil
}

// computeNextRunAt combines calendar day (IST) + run_time and ensures the result is strictly after `from`.
func computeNextRunAt(frequency string, runDay int, runTime interface{}, from time.Time) time.Time {
	fromUTC := from.UTC()
	for attempt := 0; attempt < 48; attempt++ {
		dayUTC := computeNextRunForConfig(frequency, runDay, fromUTC)
		result := applyRunTimeUTC(dayUTC, runTime)
		if result.After(from) {
			return result
		}
		// This calendar slot (day at run_time) already passed.
		// We must advance fromUTC past the end of this calendar day to force the next period.
		fromUTC = time.Date(fromUTC.Year(), fromUTC.Month(), fromUTC.Day(), 0, 0, 0, 0, fromUTC.Location()).AddDate(0, 0, 1)
	}
	dayUTC := computeNextRunForConfig(frequency, runDay, fromUTC)
	return applyRunTimeUTC(dayUTC, runTime)
}

func applyRunTimeUTC(dayUTC time.Time, runTime interface{}) time.Time {
	loc := scheduleTZ()
	dayLocal := dayUTC.In(loc)
	h, m, s, ok := parseRunTimeComponents(runTime)
	if !ok {
		return time.Date(dayLocal.Year(), dayLocal.Month(), dayLocal.Day(), 0, 0, 0, 0, loc).UTC()
	}
	return time.Date(dayLocal.Year(), dayLocal.Month(), dayLocal.Day(), h, m, s, 0, loc).UTC()
}

func truncateStatus(s string) string {
	if len(s) > 200 {
		return s[:200]
	}
	return s
}

func parseRunTimeComponents(runTime interface{}) (hour, min, sec int, ok bool) {
	if runTime == nil {
		return 0, 0, 0, false
	}
	switch t := runTime.(type) {
	case time.Time:
		return t.Hour(), t.Minute(), t.Second(), true
	case pgtype.Time:
		// pgx v5 returns PostgreSQL TIME columns as pgtype.Time{Microseconds, Valid}
		if !t.Valid {
			return 0, 0, 0, false
		}
		total := t.Microseconds / 1_000_000 // convert to seconds
		h := int(total / 3600)
		m := int((total % 3600) / 60)
		s := int(total % 60)
		return h, m, s, true
	case string:
		if t == "" {
			return 0, 0, 0, false
		}
		for _, layout := range []string{"15:04:05", "15:04", time.RFC3339} {
			if parsed, err := time.Parse(layout, t); err == nil {
				return parsed.Hour(), parsed.Minute(), parsed.Second(), true
			}
		}
	}
	return 0, 0, 0, false
}

// computeNextRunForConfig calculates the next calendar fire day (local midnight, returned as UTC)
// in scheduleTZ. Same calendar day as run_day is returned even if `from` is later that day
// (run_time may still be in the future).
func computeNextRunForConfig(frequency string, runDay int, from time.Time) time.Time {
	if runDay < 1 {
		runDay = 1
	}
	if runDay > 28 {
		runDay = 28
	}
	loc := scheduleTZ()
	from = from.In(loc)

	switch frequency {
	case "DAILY":
		// Next calendar day (local)
		next := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, 1)
		return next.UTC()

	case "HALF_YEARLY":
		// Next half-year start (Jan 1 or Jul 1)
		var candidate time.Time
		if from.Month() < 7 {
			candidate = time.Date(from.Year(), time.July, 1, 0, 0, 0, 0, time.UTC)
		} else {
			candidate = time.Date(from.Year()+1, time.January, 1, 0, 0, 0, 0, time.UTC)
		}
		// Adjust to run_day within that month (capped at 28)
		maxDay := daysInMonth(candidate.Year(), candidate.Month())
		day := runDay
		if day > maxDay {
			day = maxDay
		}
		return time.Date(candidate.Year(), candidate.Month(), day, 0, 0, 0, 0, loc).UTC()

	case "QUARTERLY":
		// Find the start of the current quarter
		currentQStartMonth := time.Month(((int(from.Month())-1)/3)*3 + 1)
		candidateInCurrentQ := time.Date(from.Year(), currentQStartMonth, 1, 0, 0, 0, 0, loc)
		// run_day within the quarter = day of the first month of that quarter
		maxDay := daysInMonth(candidateInCurrentQ.Year(), candidateInCurrentQ.Month())
		day := runDay
		if day > maxDay {
			day = maxDay
		}
		candidate := time.Date(candidateInCurrentQ.Year(), candidateInCurrentQ.Month(), day, 0, 0, 0, 0, loc)
		if from.Day() == day && from.Month() == candidateInCurrentQ.Month() && from.Year() == candidate.Year() {
			return candidate.UTC()
		}
		if !candidate.Before(from.Truncate(24 * time.Hour)) {
			return candidate.UTC()
		}
		// Advance one quarter
		next := candidateInCurrentQ.AddDate(0, 3, 0)
		maxDay = daysInMonth(next.Year(), next.Month())
		day = runDay
		if day > maxDay {
			day = maxDay
		}
		return time.Date(next.Year(), next.Month(), day, 0, 0, 0, 0, loc).UTC()

	case "YEARLY":
		// Anniversary: same month/day each year (not forced to January)
		maxDay := daysInMonth(from.Year(), from.Month())
		day := runDay
		if day > maxDay {
			day = maxDay
		}
		candidate := time.Date(from.Year(), from.Month(), day, 0, 0, 0, 0, loc)
		if from.Day() == day && from.Month() == candidate.Month() && from.Year() == candidate.Year() {
			return candidate.UTC()
		}
		if !candidate.Before(from.Truncate(24 * time.Hour)) {
			return candidate.UTC()
		}
		next := time.Date(from.Year()+1, from.Month(), day, 0, 0, 0, 0, loc)
		maxDay = daysInMonth(next.Year(), next.Month())
		day = runDay
		if day > maxDay {
			day = maxDay
		}
		return next.UTC()

	default: // MONTHLY
		maxDay := daysInMonth(from.Year(), from.Month())
		day := runDay
		if day > maxDay {
			day = maxDay
		}
		candidate := time.Date(from.Year(), from.Month(), day, 0, 0, 0, 0, loc)
		// Today is run day — keep today so run_time can still be later today
		if from.Day() == day {
			return candidate.UTC()
		}
		if !candidate.Before(from.Truncate(24 * time.Hour)) {
			return candidate.UTC()
		}
		next := time.Date(from.Year(), from.Month()+1, 1, 0, 0, 0, 0, loc)
		maxDay = daysInMonth(next.Year(), next.Month())
		day = runDay
		if day > maxDay {
			day = maxDay
		}
		return time.Date(next.Year(), next.Month(), day, 0, 0, 0, 0, loc).UTC()
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

		// List query aligned with fd_booking_request list (processing_status, action_type, audit_id, …).
		query := `
		WITH latest_audit AS (
			SELECT DISTINCT ON (a.config_id)
				a.audit_id,
				a.config_id,
				a.action_type,
				a.processing_status,
				a.requested_by,
				a.requested_at,
				a.checker_by,
				a.checker_at,
				a.checker_comment,
				a.reason
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
				MAX(CASE WHEN action_type='CREATE' THEN requested_by END)                                   AS created_by,
				MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
				MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
				MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
				MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
				MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
			FROM investment.fd_accrual_schedule_config_audit
			GROUP BY config_id
		)
		SELECT
			c.config_id,
			COALESCE(c.entity_id,'')                    AS entity_id,
			COALESCE(c.entity_name,'')                  AS entity_name,
			c.schedule_frequency,
			COALESCE(c.run_day_of_month,1)              AS run_day_of_month,
			COALESCE(TO_CHAR(c.run_time,'HH24:MI:SS'),'') AS run_time,
			COALESCE(c.default_bank_id_filter,'')       AS default_bank_id_filter,
			COALESCE(c.default_fd_status_filter,'ACTIVE') AS default_fd_status_filter,
			c.default_run_mode,
			c.auto_submit_for_approval,
			COALESCE(c.accrual_granularity,'MONTHLY')   AS accrual_granularity,
			COALESCE(c.period_coverage,'RUN')           AS period_coverage,
			COALESCE(c.last_run_id,'')                  AS last_run_id,
			COALESCE(c.last_run_status,'')              AS last_run_status,
			COALESCE(TO_CHAR(c.last_run_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS last_run_at,
			COALESCE(TO_CHAR(c.next_run_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS next_run_at,
			c.is_active,
			COALESCE(c.is_deleted,false)                AS is_deleted,
			COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS record_created_at,
			COALESCE(c.created_by,'')                   AS record_created_by,
			COALESCE(TO_CHAR(c.updated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS record_updated_at,
			COALESCE(c.updated_by,'')                   AS record_updated_by,
			COALESCE(l.audit_id::text,'')                 AS audit_id,
			COALESCE(l.action_type,'')                  AS action_type,
			COALESCE(l.processing_status,'')            AS processing_status,
			COALESCE(l.processing_status,'')            AS approval_status,
			COALESCE(l.requested_by,'')                 AS requested_by,
			COALESCE(TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
			COALESCE(l.checker_by,'')                   AS checker_by,
			COALESCE(TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
			COALESCE(l.checker_comment,'')              AS checker_comment,
			COALESCE(l.reason,'')                       AS reason,
			COALESCE(h.created_by,'')                   AS created_by,
			COALESCE(h.created_at,'')                   AS created_at,
			COALESCE(h.edited_by,'')                    AS edited_by,
			COALESCE(h.edited_at,'')                    AS edited_at,
			COALESCE(h.deleted_by,'')                   AS deleted_by,
			COALESCE(h.deleted_at,'')                   AS deleted_at,
			(SELECT COUNT(DISTINCT r.run_id) FROM investment.fd_accrual_run r
			 WHERE r.created_by = 'SCHEDULER'
			   AND COALESCE(r.is_deleted,false) = false
			   AND (
			     r.run_id IN (
			       SELECT DISTINCT el.run_id
			       FROM investment.fd_accrual_run_execution_log el
			       WHERE COALESCE(el.detail->>'schedule_config_id','') = c.config_id::text
			     )
			     OR (c.last_run_id <> '' AND r.run_id = c.last_run_id)
			   )) AS total_runs_created
		FROM investment.fd_accrual_schedule_config c
		LEFT JOIN latest_audit l ON l.config_id = c.config_id
		LEFT JOIN history h ON h.config_id = c.config_id
		WHERE COALESCE(c.is_deleted,false) = false`

		args := []interface{}{}
		argIdx := 1

		if req.EntityID != "" {
			if code, msg := validateAccrualEntityParam(ctx, req.EntityID); code != 0 {
				api.RespondWithError(w, code, msg)
				return
			}
			query += fmt.Sprintf(" AND c.entity_id = $%d", argIdx)
			args = append(args, req.EntityID)
			argIdx++
		} else {
			query += accrualEntityScopePredicate(ctx, "c", &argIdx, &args)
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

		var approvalWorkflow *approvalengine.RichInstanceDetail
		var instanceID string
		_ = pgxPool.QueryRow(ctx, `
			SELECT instance_id
			FROM uam.approval_instance
			WHERE record_id = $1
			  AND module_code = 'FIXED_DEPOSIT'
			  AND status = 'PENDING'
			  AND is_deleted = false
			LIMIT 1`, req.ConfigID).Scan(&instanceID)
		if instanceID != "" {
			if richDetail, err := approvalengine.GetRichInstanceDetail(ctx, pgxPool, instanceID, req.UserID); err == nil {
				approvalWorkflow = richDetail
			} else {
				api.LogError("[FDAccrual] GetRichInstanceDetail failed config=%s: %v", req.ConfigID, err)
			}
		}

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
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS created_at_audit,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
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
				COALESCE(c.period_coverage,'RUN'),
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

		var configID, entityID, entityName, scheduleFreq, bankFilter, fdFilter, runMode, granularity, periodCoverage string
		var lastRunID, lastRunStatus, createdBy, updatedBy string
		var auditAction, auditStatus, auditReqBy, auditChkBy, auditComment string
		var histCreatedBy, histCreatedAt, histEditedBy, histEditedAt, histDeletedBy, histDeletedAt string
		var runDay int
		var autoSubmit, isActive bool
		var lastRunAt, nextRunAt, createdAt, updatedAt, auditReqAt, auditChkAt interface{}

		if err := configRow.Scan(
			&configID, &entityID, &entityName, &scheduleFreq, &runDay,
			&bankFilter, &fdFilter, &runMode, &autoSubmit, &granularity, &periodCoverage,
			&lastRunID, &lastRunStatus, &lastRunAt, &nextRunAt,
			&isActive, &createdAt, &createdBy, &updatedAt, &updatedBy,
			&auditAction, &auditStatus, &auditReqBy, &auditReqAt,
			&auditChkBy, &auditChkAt, &auditComment,
			&histCreatedBy, &histCreatedAt, &histEditedBy, &histEditedAt, &histDeletedBy, &histDeletedAt,
		); err != nil {
			api.RespondWithError(w, http.StatusNotFound, "Schedule config not found: "+err.Error())
			return
		}
		if code, msg := validateAccrualEntityParam(ctx, entityID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		configData = map[string]interface{}{
			"config_id":                configID,
			"entity_id":                entityID,
			"entity_name":              entityName,
			"schedule_frequency":       scheduleFreq,
			"period_coverage":          periodCoverage,
			"run_day_of_month":         runDay,
			"default_bank_id_filter":   bankFilter,
			"default_fd_status_filter": fdFilter,
			"default_run_mode":         runMode,
			"auto_submit_for_approval": autoSubmit,
			"accrual_granularity":      granularity,
			"last_run_id":              lastRunID,
			"last_run_status":          lastRunStatus,
			"last_run_at":              lastRunAt,
			"next_run_at":              nextRunAt,
			"is_active":                isActive,
			"created_at":               createdAt,
			"created_by":               createdBy,
			"updated_at":               updatedAt,
			"updated_by":               updatedBy,
			"audit_action_type":        auditAction,
			"audit_processing_status":  auditStatus,
			"audit_requested_by":       auditReqBy,
			"audit_requested_at":       auditReqAt,
			"audit_checker_by":         auditChkBy,
			"audit_checker_at":         auditChkAt,
			"audit_checker_comment":    auditComment,
			"created_by_audit":         histCreatedBy,
			"created_at_audit":         histCreatedAt,
			"edited_by":                histEditedBy,
			"edited_at":                histEditedAt,
			"deleted_by":               histDeletedBy,
			"deleted_at":               histDeletedAt,
			"approval_workflow":        approvalWorkflow,
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
			  AND COALESCE(r.is_deleted,false) = false
			ORDER BY r.created_at DESC
			LIMIT 100`, entityID)

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
			"total_runs":       totalRuns,
			"total_ledgers":    totalLedgers,
			"total_interest":   totalInterest,
			"total_tds":        totalTDS,
			"total_findings":   totalFindings,
			"total_exceptions": totalExceptions,
		}

		// ── 4. Build complete response ──────────────────────────────────────
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config":       configData,
			"runs":         runs,
			"runs_count":   len(runs),
			"global_stats": globalStats,
		})
	}
}

// normalizeAccrualExecutionLogRow parses detail JSON and ensures stable keys for the UI.
func normalizeAccrualExecutionLogRow(row map[string]interface{}) {
	if row == nil {
		return
	}
	if d, ok := row["detail"].(string); ok && strings.TrimSpace(d) != "" && d != "{}" {
		var parsed interface{}
		if err := json.Unmarshal([]byte(d), &parsed); err == nil {
			row["detail"] = parsed
		}
	}
}

// attachExecutionLogsToRuns copies logs onto each run as execution_logs (no top-level duplicate list).
func attachExecutionLogsToRuns(runs []map[string]interface{}, logsByRun map[string][]map[string]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(runs))
	for _, run := range runs {
		rid, _ := run["run_id"].(string)
		logs := logsByRun[rid]
		if logs == nil {
			logs = []map[string]interface{}{}
		}
		normalized := make([]map[string]interface{}, len(logs))
		for i, lg := range logs {
			cp := make(map[string]interface{}, len(lg))
			for k, v := range lg {
				cp[k] = v
			}
			normalizeAccrualExecutionLogRow(cp)
			normalized[i] = cp
		}
		runCopy := make(map[string]interface{}, len(run)+2)
		for k, v := range run {
			runCopy[k] = v
		}
		runCopy["execution_logs"] = normalized
		runCopy["execution_logs_count"] = len(normalized)
		out = append(out, runCopy)
	}
	return out
}

func scanRowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[string(f.Name)] = ""
			} else {
				row[string(f.Name)] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// loadSchedulerRunsForConfig returns SCHEDULER runs created by this schedule config only
// (matched via execution_log.detail.schedule_config_id, plus last_run_id fallback).
func loadSchedulerRunsForConfig(ctx context.Context, pool *pgxpool.Pool, configID, lastRunID string, limit int) ([]map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			r.run_id,
			r.run_type,
			r.run_mode,
			r.run_status,
			COALESCE(r.entity_id,'') AS entity_id,
			r.accrual_period_start,
			r.accrual_period_end,
			COALESCE(r.financial_period,'') AS financial_period,
			COALESCE(r.accrual_granularity,'') AS accrual_granularity,
			COALESCE(r.fds_in_scope,0) AS fds_in_scope,
			COALESCE(r.fds_calculated,0) AS fds_calculated,
			COALESCE(r.fds_failed,0) AS fds_failed,
			COALESCE(r.total_interest_accrued,0) AS total_interest_accrued,
			r.created_at,
			r.updated_at
		FROM investment.fd_accrual_run r
		WHERE r.created_by = 'SCHEDULER'
		  AND COALESCE(r.is_deleted,false) = false
		  AND (
		    r.run_id IN (
		      SELECT DISTINCT el.run_id
		      FROM investment.fd_accrual_run_execution_log el
		      WHERE COALESCE(el.detail->>'schedule_config_id','') = $1
		    )
		    OR ($2 <> '' AND r.run_id = $2)
		  )
		ORDER BY r.created_at DESC
		LIMIT $3`, configID, lastRunID, limit)
	if err != nil {
		return nil, err
	}
	return scanRowsToMaps(rows)
}

func loadExecutionLogsByRunIDs(ctx context.Context, pool *pgxpool.Pool, runIDs []string) (map[string][]map[string]interface{}, error) {
	logsByRun := make(map[string][]map[string]interface{})
	if len(runIDs) == 0 {
		return logsByRun, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT log_id, run_id,
		       COALESCE(fd_id,'') AS fd_id,
		       log_level, event_type, message,
		       COALESCE(detail::text,'{}') AS detail,
		       logged_at
		FROM investment.fd_accrual_run_execution_log
		WHERE run_id = ANY($1)
		ORDER BY logged_at DESC`, runIDs)
	if err != nil {
		return nil, err
	}
	logs, err := scanRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	for _, lg := range logs {
		rid, _ := lg["run_id"].(string)
		logsByRun[rid] = append(logsByRun[rid], lg)
	}
	return logsByRun, nil
}

// ─── GetScheduleExecutionLog ───────────────────────────────────────────────────
// POST /investment/fd/accrual/schedule/execution-log
//
// Response shape (under top-level "rows"):
//
//	{
//	  "count": 2,
//	  "schedules": [
//	    {
//	      "config_id": "ACSC-...",
//	      "entity_id": "...",
//	      "schedule_frequency": "MONTHLY",
//	      ... all schedule config fields on the same object ...,
//	      "runs_count": 3,
//	      "runs": [
//	        {
//	          "run_id": "ACRN-...",
//	          "execution_logs_count": 24,
//	          "execution_logs": [ ... only for this run ... ]
//	        }
//	      ]
//	    },
//	    { "config_id": "ACSC-OTHER", ..., "runs": [ ... only that schedule's runs ... ] }
//	  ]
//	}
//
// Each schedules[] item is one fd_accrual_schedule_config; runs[] contains only runs
// that scheduler created for that config_id (not all runs for the entity).

func GetScheduleExecutionLog(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			ConfigID string `json:"config_id"` // optional
			EntityID string `json:"entity_id"` // optional
			Limit    int    `json:"limit"`     // max runs per config (default 50)
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.Limit <= 0 || req.Limit > 200 {
			req.Limit = 50
		}

		ctx := r.Context()

		configQuery := `
			SELECT
				c.config_id,
				COALESCE(c.entity_id,'') AS entity_id,
				COALESCE(c.entity_name,'') AS entity_name,
				c.schedule_frequency,
				COALESCE(c.period_coverage,'RUN') AS period_coverage,
				COALESCE(c.run_day_of_month,1) AS run_day_of_month,
				c.run_time,
				COALESCE(c.default_bank_id_filter,'') AS default_bank_id_filter,
				COALESCE(c.default_fd_status_filter,'') AS default_fd_status_filter,
				COALESCE(c.default_run_mode,'') AS default_run_mode,
				COALESCE(c.accrual_granularity,'') AS accrual_granularity,
				COALESCE(c.last_run_id,'') AS last_run_id,
				COALESCE(c.last_run_status,'') AS last_run_status,
				c.last_run_at,
				c.next_run_at,
				c.is_active
			FROM investment.fd_accrual_schedule_config c
			WHERE COALESCE(c.is_active,false) = true`
		configArgs := []interface{}{}
		argIdx := 1
		if req.ConfigID != "" {
			if _, ok, err := requireScheduleConfigScope(ctx, pgxPool, req.ConfigID); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScheduleConfigLookupFailed+err.Error())
				return
			} else if !ok {
				api.RespondWithError(w, http.StatusForbidden, "Schedule config is not within your authorized entity scope.")
				return
			}
			configQuery += fmt.Sprintf(" AND c.config_id = $%d", argIdx)
			configArgs = append(configArgs, req.ConfigID)
			argIdx++
		}
		if req.EntityID != "" {
			if code, msg := validateAccrualEntityParam(ctx, req.EntityID); code != 0 {
				api.RespondWithError(w, code, msg)
				return
			}
			configQuery += fmt.Sprintf(" AND c.entity_id = $%d", argIdx)
			configArgs = append(configArgs, req.EntityID)
			argIdx++
		} else {
			configQuery += accrualEntityScopePredicate(ctx, "c", &argIdx, &configArgs)
		}
		configQuery += " ORDER BY c.next_run_at NULLS LAST, c.created_at DESC"

		configRows, err := pgxPool.Query(ctx, configQuery, configArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Schedule config query failed: "+err.Error())
			return
		}
		defer configRows.Close()

		type scheduleEntry struct {
			Config map[string]interface{}
		}
		entries := make([]scheduleEntry, 0)

		configFields := configRows.FieldDescriptions()
		for configRows.Next() {
			vals, _ := configRows.Values()
			row := make(map[string]interface{}, len(configFields))
			for i, f := range configFields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			entries = append(entries, scheduleEntry{Config: row})
		}
		if len(entries) == 0 {
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"count":     0,
				"schedules": []interface{}{},
			})
			return
		}

		schedules := make([]map[string]interface{}, 0, len(entries))
		for _, e := range entries {
			configID, _ := e.Config["config_id"].(string)
			lastRunID, _ := e.Config["last_run_id"].(string)

			runs, err := loadSchedulerRunsForConfig(ctx, pgxPool, configID, lastRunID, req.Limit)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError,
					fmt.Sprintf("Runs for schedule %s failed: %s", configID, err.Error()))
				return
			}

			runIDs := make([]string, 0, len(runs))
			for _, run := range runs {
				if rid, ok := run["run_id"].(string); ok && rid != "" {
					runIDs = append(runIDs, rid)
				}
			}
			logsByRun, err := loadExecutionLogsByRunIDs(ctx, pgxPool, runIDs)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError,
					fmt.Sprintf("Execution logs for schedule %s failed: %s", configID, err.Error()))
				return
			}

			runsWithLogs := attachExecutionLogsToRuns(runs, logsByRun)

			// One object per schedule: config fields + runs nested on the same struct.
			item := make(map[string]interface{}, len(e.Config)+2)
			for k, v := range e.Config {
				item[k] = v
			}
			item["runs"] = runsWithLogs
			item["runs_count"] = len(runsWithLogs)
			schedules = append(schedules, item)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"count":     len(schedules),
			"schedules": schedules,
		})
	}
}
