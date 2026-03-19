package fdAccrual

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── 1. CreateScheduleConfig ─────────────────────────────────────────────────

func CreateScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string `json:"user_id"`
			ScheduleName   string `json:"schedule_name"`
			FrequencyCode  string `json:"frequency_code"`  // MONTHLY / QUARTERLY / YEARLY
			RunDayOfMonth  int    `json:"run_day_of_month"` // 1-28
			RunMode        string `json:"run_mode"`         // FULL or SIMULATION
			ScopeEntityIDs string `json:"scope_entity_ids"` // comma-separated or empty
			ScopeBankIDs   string `json:"scope_bank_ids"`
			AutoSubmit     bool   `json:"auto_submit"` // auto-submit for approval after compute
			Description    string `json:"description"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ScheduleName == "" || req.FrequencyCode == "" {
			api.RespondWithError(w, http.StatusBadRequest, "schedule_name and frequency_code are required")
			return
		}
		if req.RunDayOfMonth < 1 || req.RunDayOfMonth > 28 {
			api.RespondWithError(w, http.StatusBadRequest, "run_day_of_month must be between 1 and 28")
			return
		}
		if req.RunMode == "" {
			req.RunMode = "FULL"
		}

		userEmail := getUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var configID string
		err := pgxPool.QueryRow(ctx, `
			INSERT INTO investment.fd_accrual_schedule_config (
				schedule_name, frequency_code, run_day_of_month, run_mode,
				scope_entity_ids, scope_bank_ids,
				auto_submit, description, is_enabled,
				created_by, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true,$9,now())
			RETURNING config_id`,
			req.ScheduleName, req.FrequencyCode, req.RunDayOfMonth, req.RunMode,
			nullIfEmpty(req.ScopeEntityIDs), nullIfEmpty(req.ScopeBankIDs),
			req.AutoSubmit, nullIfEmpty(req.Description),
			userEmail,
		).Scan(&configID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Create schedule config failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":     configID,
			"schedule_name": req.ScheduleName,
			"is_enabled":    true,
		})
		api.LogInfo("[FDAccrual] CreateScheduleConfig: config_id=%s name=%s freq=%s", configID, req.ScheduleName, req.FrequencyCode)
	}
}

// ─── 2. UpdateScheduleConfig ─────────────────────────────────────────────────

func UpdateScheduleConfig(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string                 `json:"user_id"`
			ConfigID       string                 `json:"config_id"`
			Fields         map[string]interface{} `json:"fields"`
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

		// Allowed update columns
		allowed := map[string]bool{
			"schedule_name": true, "frequency_code": true, "run_day_of_month": true,
			"run_mode": true, "scope_entity_ids": true, "scope_bank_ids": true,
			"auto_submit": true, "description": true,
		}

		setClauses := make([]string, 0)
		args := make([]interface{}, 0)
		argIdx := 1
		for k, v := range req.Fields {
			if !allowed[k] {
				continue
			}
			setClauses = append(setClauses, k+"=$"+intToStr(argIdx))
			args = append(args, v)
			argIdx++
		}
		if len(setClauses) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid fields to update")
			return
		}

		// Add updated_by, updated_at
		setClauses = append(setClauses, "updated_by=$"+intToStr(argIdx))
		args = append(args, userEmail)
		argIdx++
		setClauses = append(setClauses, "updated_at=now()")

		args = append(args, req.ConfigID)
		query := "UPDATE investment.fd_accrual_schedule_config SET " +
			joinStrings(setClauses, ", ") +
			" WHERE config_id=$" + intToStr(argIdx)

		_, err := pgxPool.Exec(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Update schedule config failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id": req.ConfigID,
			"updated":   true,
		})
		api.LogInfo("[FDAccrual] UpdateScheduleConfig: config_id=%s", req.ConfigID)
	}
}

// ─── 3. GetScheduleConfigs ───────────────────────────────────────────────────

func GetScheduleConfigs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				config_id, schedule_name, frequency_code, run_day_of_month, run_mode,
				COALESCE(scope_entity_ids,'') AS scope_entity_ids,
				COALESCE(scope_bank_ids,'') AS scope_bank_ids,
				auto_submit, COALESCE(description,'') AS description,
				is_enabled,
				COALESCE(created_by,'') AS created_by, created_at,
				COALESCE(updated_by,'') AS updated_by, updated_at,
				last_run_at, COALESCE(last_run_id,'') AS last_run_id
			FROM investment.fd_accrual_schedule_config
			WHERE COALESCE(is_deleted, false) = false
			ORDER BY created_at DESC`)
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

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"count":   len(configs),
			"configs": configs,
		})
	}
}

// ─── 4. DisableSchedule ──────────────────────────────────────────────────────

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
		_, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_schedule_config
			SET is_enabled = false, updated_by = $1, updated_at = now()
			WHERE config_id = $2`, userEmail, req.ConfigID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Disable schedule failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":  req.ConfigID,
			"is_enabled": false,
		})
		api.LogInfo("[FDAccrual] DisableSchedule: config_id=%s", req.ConfigID)
	}
}

// ─── 5. EnableSchedule ───────────────────────────────────────────────────────

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
		_, err := pgxPool.Exec(ctx, `
			UPDATE investment.fd_accrual_schedule_config
			SET is_enabled = true, updated_by = $1, updated_at = now()
			WHERE config_id = $2`, userEmail, req.ConfigID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Enable schedule failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"config_id":  req.ConfigID,
			"is_enabled": true,
		})
		api.LogInfo("[FDAccrual] EnableSchedule: config_id=%s", req.ConfigID)
	}
}

// ─── String helpers (avoid import cycle with strings package used in run.go) ─

func intToStr(i int) string {
	return fmt.Sprintf("%d", i)
}

func joinStrings(parts []string, sep string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += sep
		}
		result += p
	}
	return result
}
