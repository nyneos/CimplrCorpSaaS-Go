package sweepconfig

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"CimplrCorpSaas/internal/ctxutil"
	"CimplrCorpSaas/internal/jobs/dmsevent"
	"CimplrCorpSaas/internal/validation"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// resolveSweepConfigAmount picks the amount used to route the approval matrix
func resolveSweepConfigAmount(sweepAmount, bufferAmount *float64) float64 {
	if sweepAmount != nil {
		return *sweepAmount
	}
	if bufferAmount != nil {
		return *bufferAmount
	}
	return 0
}

func submitSweepConfigForApproval(pgxPool *pgxpool.Pool, sweepID, entityName, submittedByUserID, actorEmail, actionType string, amount float64, matrixID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := approvalengine.CreateInstance(ctx, pgxPool, approvalengine.InstanceRequest{
		ModuleCode:          "CASH",
		EntityCode:          entityName,
		TransactionType:     actionType,
		RecordID:            sweepID,
		RecordTable:         "cimplrcorpsaas.sweepconfiguration",
		AuditTable:          "cimplrcorpsaas.auditactionsweepconfiguration",
		AuditIDColumn:       "sweep_id",
		ActionType:          strings.Split(actionType, "_")[2],
		Amount:              amount,
		SubmittedBy:         submittedByUserID,
		SubmittedByEmail:    actorEmail,
		MatrixID:            matrixID,
		RequirePinnedMatrix: true,
		AutoApplyIfUnpinned: false,
	}); err != nil {
		api.LogError("approvalengine.CreateInstance failed for sweep config %s: %v", sweepID, err)
	}
}

const errFailedToFetchSweepConfigForPolicyCheck = "failed to fetch sweep config for policy check: "

func validateSweepConfigV2Scope(ctx context.Context, entityName, sourceBank, sourceAccount, targetBank, targetAccount string) string {
	return validation.ValidateCashMasterReferences(ctx, map[string]interface{}{
		"entity_name":           entityName,
		"bank_names":            []string{sourceBank, targetBank},
		"source_account_number": sourceAccount,
		"to_account_number":     targetAccount,
	})
}

// CreateSweepConfigurationV2 inserts a sweep configuration into sweepconfiguration table and creates a CREATE audit action (PENDING_APPROVAL)
func CreateSweepConfigurationV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID             string   `json:"user_id"`
			EntityName         string   `json:"entity_name"`
			SourceBankName     string   `json:"source_bank_name"`
			SourceBankAccount  string   `json:"source_bank_account"`
			TargetBankName     string   `json:"target_bank_name"`
			TargetBankAccount  string   `json:"target_bank_account"`
			SweepType          string   `json:"sweep_type"` // ZBA, CONCENTRATION, TARGET_BALANCE
			Frequency          string   `json:"frequency"`  // DAILY, WEEKLY, MONTHLY, SPECIFIC_DATE
			EffectiveDate      string   `json:"effective_date,omitempty"`
			ExecutionTime      string   `json:"execution_time"`
			BufferAmount       *float64 `json:"buffer_amount,omitempty"`
			SweepAmount        *float64 `json:"sweep_amount,omitempty"`
			RequiresInitiation *bool    `json:"requires_initiation,omitempty"`
			Reason             string   `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Validate entity / banks / accounts against middleware-provided context
		if strings.TrimSpace(req.EntityName) != "" {
			if !ctxutil.FromContext(ctx).HasEntityAccess(req.EntityName) {
				api.RespondWithResult(w, false, "unauthorized entity")
				return
			}
		}
		if msg := validateSweepConfigV2Scope(ctx, req.EntityName, req.SourceBankName, req.SourceBankAccount, req.TargetBankName, req.TargetBankAccount); msg != "" {
			api.RespondWithResult(w, false, msg)
			return
		}

		// Validate sweep_type (must be ZBA, CONCENTRATION, or TARGET_BALANCE)
		sweepTypeUpper := strings.ToUpper(strings.TrimSpace(req.SweepType))
		if sweepTypeUpper != "ZBA" && sweepTypeUpper != "CONCENTRATION" && sweepTypeUpper != "TARGET_BALANCE" {
			api.RespondWithResult(w, false, "invalid sweep_type. Allowed values: ZBA, CONCENTRATION, TARGET_BALANCE")
			return
		}

		// Validate frequency (must be DAILY, WEEKLY, MONTHLY, or SPECIFIC_DATE)
		frequencyUpper := strings.ToUpper(strings.TrimSpace(req.Frequency))
		if frequencyUpper != "DAILY" && frequencyUpper != "WEEKLY" && frequencyUpper != "MONTHLY" && frequencyUpper != "SPECIFIC_DATE" {
			api.RespondWithResult(w, false, "invalid frequency. Allowed values: DAILY, WEEKLY, MONTHLY, SPECIFIC_DATE")
			return
		}

		// resolve requested_by
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		ok, triggerMatrixID := runtime.EnforceWithMatrix(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreCreate,
			ModuleCode:          common.ModuleCash,
			SubModule:           "SWEEP_CONFIG",
			EntityCode:          req.EntityName,
			ActorUserID:         req.UserID,
			HandlerName:         "CreateSweepConfigurationV2",
			APIPath:             "/cash/sweep-config-v2/create",
			DefaultBlockMessage: "Sweep configuration create blocked by policy",
			Fields: buildSweepConfigPolicyFields(sweepConfigRow{
				EntityName:         req.EntityName,
				SourceBankName:     req.SourceBankName,
				SourceBankAccount:  req.SourceBankAccount,
				TargetBankName:     req.TargetBankName,
				TargetBankAccount:  req.TargetBankAccount,
				SweepType:          sweepTypeUpper,
				Frequency:          frequencyUpper,
				EffectiveDate:      req.EffectiveDate,
				ExecutionTime:      req.ExecutionTime,
				BufferAmount:       req.BufferAmount,
				SweepAmount:        req.SweepAmount,
				RequiresInitiation: req.RequiresInitiation == nil || *req.RequiresInitiation,
			}),
		})
		if !ok {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "failed to begin transaction: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// sweep_id is auto-generated by database DEFAULT, no need to provide it
		ins := `INSERT INTO cimplrcorpsaas.sweepconfiguration (
			entity_name, 
			source_bank_name, source_bank_account, 
			target_bank_name, target_bank_account, 
			sweep_type, frequency, 
			effective_date, execution_time, 
			buffer_amount, sweep_amount, 
			requires_initiation, 
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now(),now()) RETURNING sweep_id`

		var sweepID string
		err = tx.QueryRow(ctx, ins,
			nullifyEmpty(req.EntityName),
			nullifyEmpty(req.SourceBankName),
			nullifyEmpty(req.SourceBankAccount),
			nullifyEmpty(req.TargetBankName),
			nullifyEmpty(req.TargetBankAccount),
			sweepTypeUpper,
			frequencyUpper,
			nullifyEmpty(req.EffectiveDate),
			nullifyEmpty(req.ExecutionTime),
			nullifyFloat(req.BufferAmount),
			nullifyFloat(req.SweepAmount),
			nullifyBool(req.RequiresInitiation),
		).Scan(&sweepID)

		if err != nil {
			api.RespondWithResult(w, false, "failed to insert sweep configuration: "+err.Error())
			return
		}

		auditStatus := approvalengine.AuditStatus(triggerMatrixID, "PENDING_APPROVAL")
		if _, err := tx.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.auditactionsweepconfiguration (
				sweep_id, actiontype, processing_status, reason,
				requested_by, requested_at, requested_ip
			) VALUES ($1, 'CREATE', $2, $3, $4, now(), $5)`,
			sweepID, auditStatus, nullifyEmpty(req.Reason), requestedBy, api.ClientIPFromContext(ctx),
		); err != nil {
			api.RespondWithResult(w, false, "failed to create audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
			return
		}

		submitSweepConfigForApproval(
			pgxPool, sweepID, req.EntityName, req.UserID, requestedBy,
			"SWEEP_CONFIG_CREATE", resolveSweepConfigAmount(req.SweepAmount, req.BufferAmount), triggerMatrixID,
		)

		dmsevent.Fire(pgxPool, "CASH", "SWEEP_CONFIG", "POST_CREATE", []string{sweepID}, requestedBy)

		api.RespondWithResult(w, true, sweepID)
	}
}

// BulkCreateSweepConfigurationV2 creates multiple sweep configurations in a single transaction
func BulkCreateSweepConfigurationV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		type SweepConfigRequest struct {
			EntityName         string   `json:"entity_name"`
			SourceBankName     string   `json:"source_bank_name"`
			SourceBankAccount  string   `json:"source_bank_account"`
			TargetBankName     string   `json:"target_bank_name"`
			TargetBankAccount  string   `json:"target_bank_account"`
			SweepType          string   `json:"sweep_type"`
			Frequency          string   `json:"frequency"`
			EffectiveDate      string   `json:"effective_date,omitempty"`
			ExecutionTime      string   `json:"execution_time"`
			BufferAmount       *float64 `json:"buffer_amount,omitempty"`
			SweepAmount        *float64 `json:"sweep_amount,omitempty"`
			RequiresInitiation *bool    `json:"requires_initiation,omitempty"`
			Reason             string   `json:"reason,omitempty"`
		}

		var req struct {
			UserID  string               `json:"user_id"`
			Configs []SweepConfigRequest `json:"configs"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
			return
		}
		if len(req.Configs) == 0 {
			api.RespondWithResult(w, false, "configs array cannot be empty")
			return
		}

		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// resolve requested_by
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Start transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		var createdIDs []string

		type pendingConfig struct {
			sweepID    string
			entityName string
			amount     float64
			matrixID   string
		}
		var pendingConfigs []pendingConfig

		for i, cfg := range req.Configs {
			// Validate entity / banks / accounts against middleware-provided context
			if strings.TrimSpace(cfg.EntityName) != "" {
				if !ctxutil.FromContext(ctx).HasEntityAccess(cfg.EntityName) {
					api.RespondWithResult(w, false, fmt.Sprintf("config[%d]: unauthorized entity %s", i, cfg.EntityName))
					return
				}
			}
			if msg := validateSweepConfigV2Scope(ctx, cfg.EntityName, cfg.SourceBankName, cfg.SourceBankAccount, cfg.TargetBankName, cfg.TargetBankAccount); msg != "" {
				api.RespondWithResult(w, false, fmt.Sprintf("config[%d]: %s", i, msg))
				return
			}

			// Validate sweep_type
			sweepTypeUpper := strings.ToUpper(strings.TrimSpace(cfg.SweepType))
			if sweepTypeUpper != "ZBA" && sweepTypeUpper != "CONCENTRATION" && sweepTypeUpper != "TARGET_BALANCE" {
				api.RespondWithResult(w, false, fmt.Sprintf("config[%d]: invalid sweep_type %s", i, cfg.SweepType))
				return
			}

			// Validate frequency
			frequencyUpper := strings.ToUpper(strings.TrimSpace(cfg.Frequency))
			if frequencyUpper != "DAILY" && frequencyUpper != "WEEKLY" && frequencyUpper != "MONTHLY" && frequencyUpper != "SPECIFIC_DATE" {
				api.RespondWithResult(w, false, fmt.Sprintf("config[%d]: invalid frequency %s", i, cfg.Frequency))
				return
			}

			ok, msg, tID := runtime.EnforceInlineWithMatrix(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreCreate,
				ModuleCode:          common.ModuleCash,
				SubModule:           "SWEEP_CONFIG",
				EntityCode:          cfg.EntityName,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkCreateSweepConfigurationV2",
				APIPath:             "/cash/sweep-config-v2/bulk-create",
				DefaultBlockMessage: "Sweep configuration create blocked by policy",
				Fields: buildSweepConfigPolicyFields(sweepConfigRow{
					EntityName:         cfg.EntityName,
					SourceBankName:     cfg.SourceBankName,
					SourceBankAccount:  cfg.SourceBankAccount,
					TargetBankName:     cfg.TargetBankName,
					TargetBankAccount:  cfg.TargetBankAccount,
					SweepType:          sweepTypeUpper,
					Frequency:          frequencyUpper,
					EffectiveDate:      cfg.EffectiveDate,
					ExecutionTime:      cfg.ExecutionTime,
					BufferAmount:       cfg.BufferAmount,
					SweepAmount:        cfg.SweepAmount,
					RequiresInitiation: cfg.RequiresInitiation == nil || *cfg.RequiresInitiation,
				}),
			})
			if !ok {
				api.RespondWithResult(w, false, msg)
				return
			}

			// Insert sweep configuration
			ins := `INSERT INTO cimplrcorpsaas.sweepconfiguration (
				entity_name, 
				source_bank_name, source_bank_account, 
				target_bank_name, target_bank_account, 
				sweep_type, frequency, 
				effective_date, execution_time, 
				buffer_amount, sweep_amount, 
				requires_initiation, 
				created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now(),now()) RETURNING sweep_id`

			var sweepID string
			err := tx.QueryRow(ctx, ins,
				nullifyEmpty(cfg.EntityName),
				nullifyEmpty(cfg.SourceBankName),
				nullifyEmpty(cfg.SourceBankAccount),
				nullifyEmpty(cfg.TargetBankName),
				nullifyEmpty(cfg.TargetBankAccount),
				sweepTypeUpper,
				frequencyUpper,
				nullifyEmpty(cfg.EffectiveDate),
				nullifyEmpty(cfg.ExecutionTime),
				nullifyFloat(cfg.BufferAmount),
				nullifyFloat(cfg.SweepAmount),
				nullifyBool(cfg.RequiresInitiation),
			).Scan(&sweepID)

			if err != nil {
				api.RespondWithResult(w, false, fmt.Sprintf("config[%d]: failed to insert: %s", i, err.Error()))
				return
			}

			auditStatus := approvalengine.AuditStatus(tID, "PENDING_APPROVAL")
			if _, err := tx.Exec(ctx, `
				INSERT INTO cimplrcorpsaas.auditactionsweepconfiguration (
					sweep_id, actiontype, processing_status, reason,
					requested_by, requested_at, requested_ip
				) VALUES ($1, 'CREATE', $2, $3, $4, now(), $5)`,
				sweepID, auditStatus, nil, requestedBy, api.ClientIPFromContext(ctx),
			); err != nil {
				api.RespondWithResult(w, false, fmt.Sprintf("config[%d]: failed to create audit: %s", i, err.Error()))
				return
			}

			pendingConfigs = append(pendingConfigs, pendingConfig{
				sweepID:    sweepID,
				entityName: cfg.EntityName,
				amount:     resolveSweepConfigAmount(cfg.SweepAmount, cfg.BufferAmount),
				matrixID:   tID,
			})

			createdIDs = append(createdIDs, sweepID)
		}

		// Commit transaction
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
			return
		}
		committed = true

		for _, p := range pendingConfigs {
			submitSweepConfigForApproval(
				pgxPool, p.sweepID, p.entityName, req.UserID, requestedBy,
				"SWEEP_CONFIG_CREATE", p.amount, p.matrixID,
			)
		}

		dmsevent.Fire(pgxPool, "CASH", "SWEEP_CONFIG", "POST_CREATE", createdIDs, requestedBy)

		api.RespondWithPayload(w, true, fmt.Sprintf("created %d sweep configurations", len(createdIDs)), map[string]interface{}{
			"sweep_ids": createdIDs,
			"count":     len(createdIDs),
		})
		// Notify: pass FULL sweep config data for rich templates
		capturedIDs := createdIDs
		capturedUser := req.UserID
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepConfigNotifPayload(notifyCtx, pgxPool, capturedIDs, "CREATE", capturedUser)
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-config-v2/bulk-create",
			fmt.Sprintf("SWEEPCFG_CREATE/%s/%d", capturedUser, time.Now().UnixMilli()),
			payload.ToMap(),
		)
	}
}

// UpdateSweepConfigurationV2 updates allowed fields for a specific sweep_id and creates an EDIT audit (PENDING_EDIT_APPROVAL)
func UpdateSweepConfigurationV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string                 `json:"user_id"`
			SweepID string                 `json:"sweep_id"`
			Fields  map[string]interface{} `json:"fields"`
			Reason  string                 `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" || req.SweepID == "" {
			api.RespondWithResult(w, false, "user_id and sweep_id required")
			return
		}
		// user_id must match middleware-authenticated user
		ctx := r.Context()
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		currentRow, err := loadSweepConfigRow(ctx, pgxPool, req.SweepID)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch existing sweep config for policy check: "+err.Error())
			return
		}
		updatedRow := applySweepConfigEdits(currentRow, req.Fields)

		ok, triggerMatrixID := runtime.EnforceWithMatrix(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreEdit,
			ModuleCode:          common.ModuleCash,
			SubModule:           "SWEEP_CONFIG",
			EntityCode:          updatedRow.EntityName,
			ActorUserID:         req.UserID,
			HandlerName:         "UpdateSweepConfigurationV2",
			APIPath:             "/cash/sweep-config-v2/update",
			DefaultBlockMessage: "Sweep configuration update blocked by policy",
			Fields:              buildSweepConfigPolicyFields(updatedRow),
		})
		if !ok {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		sel := `SELECT 
			entity_name, 
			source_bank_name, source_bank_account, 
			target_bank_name, target_bank_account, 
			sweep_type, frequency, 
			effective_date, execution_time, 
			buffer_amount, sweep_amount, 
			requires_initiation 
		FROM cimplrcorpsaas.sweepconfiguration WHERE sweep_id=$1 FOR UPDATE`

		var curEntity, curSourceBank, curSourceAccount, curTargetBank, curTargetAccount, curSweepType, curFrequency sqlNullString
		var curEffectiveDate, curExecutionTime sqlNullString
		var curBufferAmount, curSweepAmount sqlNullFloat
		var curRequiresInitiation *bool

		if err := tx.QueryRow(ctx, sel, req.SweepID).Scan(
			&curEntity,
			&curSourceBank, &curSourceAccount,
			&curTargetBank, &curTargetAccount,
			&curSweepType, &curFrequency,
			&curEffectiveDate, &curExecutionTime,
			&curBufferAmount, &curSweepAmount,
			&curRequiresInitiation,
		); err != nil {
			api.RespondWithResult(w, false, "failed to fetch existing sweep config: "+err.Error())
			return
		}

		sets := []string{}
		args := []interface{}{}
		pos := 1

		addStrField := func(col, oldcol string, val interface{}, cur sqlNullString) {
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
			args = append(args, nullifyEmpty(fmt.Sprint(val)))
			args = append(args, cur.ValueOrZero())
			pos += 2
		}

		// addDateField normalizes date-like values to YYYY-MM-DD before binding
		addDateField := func(col, oldcol string, val interface{}, cur sqlNullString) {
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
			// convert val to string and attempt to parse
			s := fmt.Sprint(val)
			if t, err := parseDate(s); err == nil {
				args = append(args, t.Format(constants.DateFormat))
			} else {
				args = append(args, nullifyEmpty(s))
			}
			// Normalize the existing (old) value as well. cur may have been scanned
			// from a DB date/timestamp into sqlNullString and thus contain a
			// Go time.String() (e.g. "2026-02-19 00:00:00 +0000 UTC"). Try to parse
			// and format to YYYY-MM-DD before sending to the DB to avoid
			// invalid input syntax for date columns.
			oldVal := fmt.Sprint(cur.ValueOrZero())
			if oldVal == "" {
				args = append(args, "")
			} else if t2, err2 := parseDate(oldVal); err2 == nil {
				args = append(args, t2.Format(constants.DateFormat))
			} else {
				args = append(args, oldVal)
			}
			pos += 2
		}
		addFloatField := func(col, oldcol string, val interface{}, cur sqlNullFloat) {
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
			if val == nil {
				args = append(args, nil)
			} else {
				args = append(args, val)
			}
			args = append(args, cur.ValueOrZero())
			pos += 2
		}
		addBoolField := func(col, oldcol string, val interface{}, cur *bool) {
			// For boolean fields, we need to handle old_* field differently
			sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", col, pos, oldcol, pos+1))
			if val == nil {
				args = append(args, nil)
			} else {
				args = append(args, val)
			}
			if cur == nil {
				args = append(args, nil)
			} else {
				args = append(args, *cur)
			}
			pos += 2
		}

		finalEntity := strings.TrimSpace(fmt.Sprint(curEntity.ValueOrZero()))
		finalSourceBank := strings.TrimSpace(fmt.Sprint(curSourceBank.ValueOrZero()))
		finalSourceAccount := strings.TrimSpace(fmt.Sprint(curSourceAccount.ValueOrZero()))
		finalTargetBank := strings.TrimSpace(fmt.Sprint(curTargetBank.ValueOrZero()))
		finalTargetAccount := strings.TrimSpace(fmt.Sprint(curTargetAccount.ValueOrZero()))

		for k, v := range req.Fields {
			switch k {
			case "entity_name":
				if s := fmt.Sprint(v); strings.TrimSpace(s) != "" {
					if !ctxutil.FromContext(ctx).HasEntityAccess(s) {
						api.RespondWithResult(w, false, "unauthorized entity")
						return
					}
				}
				finalEntity = strings.TrimSpace(fmt.Sprint(v))
				addStrField("entity_name", "old_entity_name", v, curEntity)

			case "source_bank_name":
				if s := fmt.Sprint(v); strings.TrimSpace(s) != "" {
					if !api.IsBankAllowed(ctx, s) {
						api.RespondWithResult(w, false, "unauthorized source bank")
						return
					}
				}
				finalSourceBank = strings.TrimSpace(fmt.Sprint(v))
				addStrField("source_bank_name", "old_source_bank_name", v, curSourceBank)

			case "source_bank_account":
				finalSourceAccount = strings.TrimSpace(fmt.Sprint(v))
				addStrField("source_bank_account", "old_source_bank_account", v, curSourceAccount)

			case "target_bank_name":
				if s := fmt.Sprint(v); strings.TrimSpace(s) != "" {
					if !api.IsBankAllowed(ctx, s) {
						api.RespondWithResult(w, false, "unauthorized target bank")
						return
					}
				}
				finalTargetBank = strings.TrimSpace(fmt.Sprint(v))
				addStrField("target_bank_name", "old_target_bank_name", v, curTargetBank)

			case "target_bank_account":
				finalTargetAccount = strings.TrimSpace(fmt.Sprint(v))
				addStrField("target_bank_account", "old_target_bank_account", v, curTargetAccount)

			case "sweep_type":
				// Validate sweep_type
				sweepTypeUpper := strings.ToUpper(strings.TrimSpace(fmt.Sprint(v)))
				if sweepTypeUpper != "ZBA" && sweepTypeUpper != "CONCENTRATION" && sweepTypeUpper != "TARGET_BALANCE" {
					api.RespondWithResult(w, false, "invalid sweep_type. Allowed values: ZBA, CONCENTRATION, TARGET_BALANCE")
					return
				}
				addStrField("sweep_type", "old_sweep_type", sweepTypeUpper, curSweepType)

			case "frequency":
				// Validate frequency
				frequencyUpper := strings.ToUpper(strings.TrimSpace(fmt.Sprint(v)))
				if frequencyUpper != "DAILY" && frequencyUpper != "WEEKLY" && frequencyUpper != "MONTHLY" && frequencyUpper != "SPECIFIC_DATE" {
					api.RespondWithResult(w, false, "invalid frequency. Allowed values: DAILY, WEEKLY, MONTHLY, SPECIFIC_DATE")
					return
				}
				addStrField("frequency", "old_frequency", frequencyUpper, curFrequency)

			case "effective_date":
				addDateField("effective_date", "old_effective_date", v, curEffectiveDate)

			case "execution_time":
				addStrField("execution_time", "old_execution_time", v, curExecutionTime)

			case "buffer_amount":
				addFloatField("buffer_amount", "old_buffer_amount", v, curBufferAmount)

			case "sweep_amount":
				addFloatField("sweep_amount", "old_sweep_amount", v, curSweepAmount)

			case "requires_initiation":
				// Handle boolean field
				if v == nil {
					addBoolField("requires_initiation", "old_requires_initiation", nil, curRequiresInitiation)
				} else {
					boolVal, ok := v.(bool)
					if !ok {
						api.RespondWithResult(w, false, "requires_initiation must be a boolean value")
						return
					}
					addBoolField("requires_initiation", "old_requires_initiation", boolVal, curRequiresInitiation)
				}

			default:
				// ignore unknown fields
			}
		}

		if len(sets) == 0 {
			api.RespondWithResult(w, false, "no valid fields to update")
			return
		}
		if msg := validateSweepConfigV2Scope(ctx, finalEntity, finalSourceBank, finalSourceAccount, finalTargetBank, finalTargetAccount); msg != "" {
			api.RespondWithResult(w, false, msg)
			return
		}

		if err := approvalengine.CancelPendingInstances(ctx, pgxPool, "CASH", req.SweepID, requestedBy); err != nil {
			api.LogError("[SweepConfig] CancelPendingInstances failed for %s: %v", map[string]interface{}{"sweep_id": req.SweepID, "error": err.Error()})
		}

		q := "UPDATE cimplrcorpsaas.sweepconfiguration SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE sweep_id=$%d", pos)
		args = append(args, req.SweepID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithResult(w, false, "failed to update sweep config: "+err.Error())
			return
		}

		auditStatus := approvalengine.AuditStatus(triggerMatrixID, "PENDING_EDIT_APPROVAL")
		if _, err := tx.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.auditactionsweepconfiguration (
				sweep_id, actiontype, processing_status, reason,
				requested_by, requested_at, requested_ip
			) VALUES ($1, 'EDIT', $2, $3, $4, now(), $5)`,
			req.SweepID, auditStatus, nullifyEmpty(req.Reason), requestedBy, api.ClientIPFromContext(ctx),
		); err != nil {
			api.RespondWithResult(w, false, "failed to create audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
			return
		}
		committed = true

		var currentSweepAmount, currentBufferAmount *float64
		if curSweepAmount.Valid {
			currentSweepAmount = &curSweepAmount.F
		}
		if curBufferAmount.Valid {
			currentBufferAmount = &curBufferAmount.F
		}
		submitSweepConfigForApproval(
			pgxPool, req.SweepID, finalEntity, req.UserID, requestedBy,
			"SWEEP_CONFIG_EDIT", resolveSweepConfigAmount(currentSweepAmount, currentBufferAmount), triggerMatrixID,
		)

		dmsevent.Fire(pgxPool, "CASH", "SWEEP_CONFIG", "POST_EDIT", []string{req.SweepID}, requestedBy)

		// Notify: pass FULL sweep config data for rich templates
		capturedSweepID := req.SweepID
		capturedUser := req.UserID
		capturedReason := req.Reason
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepConfigNotifPayload(notifyCtx, pgxPool, []string{capturedSweepID}, "UPDATE", capturedUser)
		payloadMap := payload.ToMap()
		payloadMap["Reason"] = capturedReason
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-config-v2/update",
			fmt.Sprintf("SWEEPCFG_UPDATE/%s/%d", capturedSweepID, time.Now().UnixMilli()),
			payloadMap,
		)

		api.RespondWithResult(w, true, req.SweepID)
	}
}

// GetSweepConfigurationsV2 returns sweepconfiguration rows with latest audit info
func GetSweepConfigurationsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, "Missing user_id in body")
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// skip soft-deleted rows
		entityNames := api.GetEntityNamesFromCtx(ctx)
		var rows pgx.Rows
		var err error
		if len(entityNames) > 0 {
			// normalize entity names (trim + lower) and compare against lower(trim(entity_name)) in DB
			norm := make([]string, 0, len(entityNames))
			for _, n := range entityNames {
				if s := strings.TrimSpace(n); s != "" {
					norm = append(norm, strings.ToLower(s))
				}
			}
			if len(norm) == 0 {
				// nothing allowed
				api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
					"sweep_configurations": []map[string]interface{}{},
				})
				return
			}
			q := `SELECT 
								sweep_id, entity_name, 
								source_bank_name, source_bank_account, 
								target_bank_name, target_bank_account, 
								sweep_type, frequency,
								-- include latest approved balances (0 if none)
								COALESCE(bbal1.current_balance,0) AS source_current_balance,
								COALESCE(bbal2.current_balance,0) AS target_current_balance,
								effective_date, execution_time, 
								buffer_amount, sweep_amount, 
								requires_initiation,
								old_entity_name, 
								old_source_bank_name, old_source_bank_account, 
								old_target_bank_name, old_target_bank_account, 
								old_sweep_type, old_frequency, 
								old_effective_date, old_execution_time, 
								old_buffer_amount, old_sweep_amount,
								inst.instance_id, inst.status as inst_status
						FROM cimplrcorpsaas.sweepconfiguration 
						LEFT JOIN LATERAL (
								SELECT COALESCE(bbm.closing_balance,0) AS current_balance
								FROM public.bank_balances_manual bbm
								JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
								WHERE a.processing_status = 'APPROVED'
									AND bbm.account_no = COALESCE(sweepconfiguration.source_bank_account, sweepconfiguration.source_bank_account)
								ORDER BY bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
								LIMIT 1
						) bbal1 ON true
						LEFT JOIN LATERAL (
								SELECT COALESCE(bbm.closing_balance,0) AS current_balance
								FROM public.bank_balances_manual bbm
								JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
								WHERE a.processing_status = 'APPROVED'
									AND bbm.account_no = COALESCE(sweepconfiguration.target_bank_account, sweepconfiguration.target_bank_account)
								ORDER BY bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
								LIMIT 1
						) bbal2 ON true
						LEFT JOIN LATERAL (
							SELECT ai.instance_id, ai.status
							FROM uam.approval_instance ai
							WHERE ai.record_id = sweepconfiguration.sweep_id::text AND ai.module_code = 'CASH'
							  AND ai.status = 'PENDING' AND ai.is_deleted = false
							ORDER BY ai.submitted_at DESC, ai.instance_id DESC LIMIT 1
						) inst ON true
						WHERE is_deleted != TRUE AND lower(trim(entity_name)) = ANY($1) 
						ORDER BY GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp)) DESC, sweep_id`
			rows, err = pgxPool.Query(ctx, q, norm)
		} else {
			q := `SELECT 
								sweep_id, entity_name, 
								source_bank_name, source_bank_account, 
								target_bank_name, target_bank_account, 
								sweep_type, frequency,
								-- include latest approved balances (0 if none)
								COALESCE(bbal1.current_balance,0) AS source_current_balance,
								COALESCE(bbal2.current_balance,0) AS target_current_balance,
								effective_date, execution_time, 
								buffer_amount, sweep_amount, 
								requires_initiation,
								old_entity_name, 
								old_source_bank_name, old_source_bank_account, 
								old_target_bank_name, old_target_bank_account, 
								old_sweep_type, old_frequency, 
								old_effective_date, old_execution_time, 
								old_buffer_amount, old_sweep_amount,
								inst.instance_id, inst.status as inst_status
						FROM cimplrcorpsaas.sweepconfiguration 
						LEFT JOIN LATERAL (
								SELECT COALESCE(bbm.closing_balance,0) AS current_balance
								FROM public.bank_balances_manual bbm
								JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
								WHERE a.processing_status = 'APPROVED'
									AND bbm.account_no = COALESCE(sweepconfiguration.source_bank_account, sweepconfiguration.source_bank_account)
								ORDER BY bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
								LIMIT 1
						) bbal1 ON true
						LEFT JOIN LATERAL (
								SELECT COALESCE(bbm.closing_balance,0) AS current_balance
								FROM public.bank_balances_manual bbm
								JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
								WHERE a.processing_status = 'APPROVED'
									AND bbm.account_no = COALESCE(sweepconfiguration.target_bank_account, sweepconfiguration.target_bank_account)
								ORDER BY bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
								LIMIT 1
						) bbal2 ON true
						LEFT JOIN LATERAL (
							SELECT ai.instance_id, ai.status
							FROM uam.approval_instance ai
							WHERE ai.record_id = sweepconfiguration.sweep_id::text AND ai.module_code = 'CASH'
							  AND ai.status = 'PENDING' AND ai.is_deleted = false
							ORDER BY ai.submitted_at DESC, ai.instance_id DESC LIMIT 1
						) inst ON true
						WHERE is_deleted != TRUE 
						ORDER BY GREATEST(COALESCE(created_at, '1970-01-01'::timestamp), COALESCE(updated_at, '1970-01-01'::timestamp)) DESC, sweep_id`
			rows, err = pgxPool.Query(ctx, q)
		}
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var sweepID string
			var entity, sourceBank, sourceAccount, targetBank, targetAccount, sweepType, freq sqlNullString
			var sourceCurrBal, targetCurrBal sqlNullFloat
			var effectiveDate, execTime sqlNullString
			var bufferAmt, sweepAmt sqlNullFloat
			var requiresInitiation *bool
			var oldEntity, oldSourceBank, oldSourceAccount, oldTargetBank, oldTargetAccount, oldSweepType, oldFreq sqlNullString
			var oldEffectiveDate, oldExecTime sqlNullString
			var oldBufferAmt, oldSweepAmt sqlNullFloat
			var instID, instStatus *string

			if err := rows.Scan(
				&sweepID, &entity,
				&sourceBank, &sourceAccount,
				&targetBank, &targetAccount,
				&sweepType, &freq,
				&sourceCurrBal, &targetCurrBal,
				&effectiveDate, &execTime,
				&bufferAmt, &sweepAmt,
				&requiresInitiation,
				&oldEntity,
				&oldSourceBank, &oldSourceAccount,
				&oldTargetBank, &oldTargetAccount,
				&oldSweepType, &oldFreq,
				&oldEffectiveDate, &oldExecTime,
				&oldBufferAmt, &oldSweepAmt,
				&instID, &instStatus,
			); err != nil {
				api.RespondWithResult(w, false, "failed to read sweep configurations: "+err.Error())
				return
			}

			auditLatest := `SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason FROM cimplrcorpsaas.auditactionsweepconfiguration WHERE sweep_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST, action_id DESC LIMIT 1`
			var processingStatusPtr, requestedByPtr, actionTypePtr, actionIDPtr, checkerByPtr, checkerCommentPtr, reasonPtr *string
			var requestedAtPtr, checkerAtPtr *time.Time
			_ = pgxPool.QueryRow(ctx, auditLatest, sweepID).Scan(&processingStatusPtr, &requestedByPtr, &requestedAtPtr, &actionTypePtr, &actionIDPtr, &checkerByPtr, &checkerAtPtr, &checkerCommentPtr, &reasonPtr)

			// Then fetch recent CREATE/EDIT/DELETE entries to build created/edited/deleted summary
			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM cimplrcorpsaas.auditactionsweepconfiguration WHERE sweep_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC, action_id DESC`
			auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, sweepID)
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
			if auditErr == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var atype string
					var rbyPtr *string
					var ratPtr *time.Time
					if err := auditRows.Scan(&atype, &rbyPtr, &ratPtr); err == nil {
						auditInfo := api.GetAuditInfo(atype, rbyPtr, ratPtr)
						if atype == "CREATE" && createdBy == "" {
							createdBy = auditInfo.CreatedBy
							createdAt = auditInfo.CreatedAt
						} else if atype == constants.AuditActionEdit && editedBy == "" {
							editedBy = auditInfo.EditedBy
							editedAt = auditInfo.EditedAt
						} else if atype == constants.AuditActionDelete && deletedBy == "" {
							deletedBy = auditInfo.DeletedBy
							deletedAt = auditInfo.DeletedAt
						}
					}
				}
			}

			// apply context-level filters for banks and accounts
			sourceBankStr := fmt.Sprint(sourceBank.ValueOrZero())
			targetBankStr := fmt.Sprint(targetBank.ValueOrZero())
			sourceAccountStr := fmt.Sprint(sourceAccount.ValueOrZero())
			targetAccountStr := fmt.Sprint(targetAccount.ValueOrZero())

			if sourceBankStr != "" {
				if !api.IsBankAllowed(ctx, sourceBankStr) {
					continue
				}
			}
			if targetBankStr != "" {
				if !api.IsBankAllowed(ctx, targetBankStr) {
					continue
				}
			}
			if sourceAccountStr != "" {
				if !ctxutil.FromContext(ctx).HasApprovedBankAccount(sourceAccountStr) {
					continue
				}
			}
			if targetAccountStr != "" {
				if !ctxutil.FromContext(ctx).HasApprovedBankAccount(targetAccountStr) {
					continue
				}
			}

			m := map[string]interface{}{
				"sweep_id":                sweepID,
				"entity_name":             entity.ValueOrZero(),
				"source_bank_name":        sourceBank.ValueOrZero(),
				"source_bank_account":     sourceAccount.ValueOrZero(),
				"target_bank_name":        targetBank.ValueOrZero(),
				"target_bank_account":     targetAccount.ValueOrZero(),
				"sweep_type":              sweepType.ValueOrZero(),
				"frequency":               freq.ValueOrZero(),
				"effective_date":          effectiveDate.ValueOrZero(),
				"execution_time":          execTime.ValueOrZero(),
				"source_current_balance":  sourceCurrBal.ValueOrZero(),
				"target_current_balance":  targetCurrBal.ValueOrZero(),
				"buffer_amount":           bufferAmt.ValueOrZero(),
				"sweep_amount":            sweepAmt.ValueOrZero(),
				"requires_initiation":     requiresInitiation,
				"old_entity_name":         oldEntity.ValueOrZero(),
				"old_source_bank_name":    oldSourceBank.ValueOrZero(),
				"old_source_bank_account": oldSourceAccount.ValueOrZero(),
				"old_target_bank_name":    oldTargetBank.ValueOrZero(),
				"old_target_bank_account": oldTargetAccount.ValueOrZero(),
				"old_sweep_type":          oldSweepType.ValueOrZero(),
				"old_frequency":           oldFreq.ValueOrZero(),
				"old_effective_date":      oldEffectiveDate.ValueOrZero(),
				"old_execution_time":      oldExecTime.ValueOrZero(),
				"old_buffer_amount":       oldBufferAmt.ValueOrZero(),
				"old_sweep_amount":        oldSweepAmt.ValueOrZero(),
				"processing_status": func() string {
					if processingStatusPtr != nil {
						return *processingStatusPtr
					}
					return ""
				}(),
				"action_type": func() string {
					if actionTypePtr != nil {
						return *actionTypePtr
					}
					return ""
				}(),
				"action_id": func() string {
					if actionIDPtr != nil {
						return *actionIDPtr
					}
					return ""
				}(),
				"checker_by": api.GetAuditInfo("", checkerByPtr, checkerAtPtr).CreatedBy,
				"checker_at": api.GetAuditInfo("", checkerByPtr, checkerAtPtr).CreatedAt,
				"checker_comment": func() string {
					if checkerCommentPtr != nil {
						return *checkerCommentPtr
					}
					return ""
				}(),
				"reason": func() string {
					if reasonPtr != nil {
						return *reasonPtr
					}
					return ""
				}(),
				"created_by": createdBy,
				"created_at": createdAt,
				"edited_by":  editedBy,
				"edited_at":  editedAt,
				"deleted_by": deletedBy,
				"deleted_at": deletedAt,

				"pending_approval": instID != nil,
				"instance_id": func() string {
					if instID != nil {
						return *instID
					}
					return ""
				}(),
			}
			out = append(out, m)
		}
		if rows.Err() != nil {
			api.RespondWithResult(w, false, "DB rows error: "+rows.Err().Error())
			return
		}

		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"sweep_configurations": out,
		})
	}
}

// BulkApproveSweepConfigurationsV2 approves pending audit actions for given sweep_ids (V2 table)
func BulkApproveSweepConfigurationsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			SweepIDs []string `json:"sweep_ids"`
			Comment  string   `json:"comment,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.SweepIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		sel := `SELECT DISTINCT ON (sweep_id) action_id, sweep_id, actiontype, processing_status FROM cimplrcorpsaas.auditactionsweepconfiguration WHERE sweep_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY sweep_id, GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST, action_id DESC`
		rows, err := pgxPool.Query(ctx, sel, req.SweepIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		deleteSweepIDs := make([]string, 0)
		found := map[string]bool{}
		actionToSweepMap := make(map[string]string)

		for rows.Next() {
			var actionID, sweepID, actionType, procStatus string
			if err := rows.Scan(&actionID, &sweepID, &actionType, &procStatus); err != nil {
				api.RespondWithResult(w, false, "failed to read latest audits: "+err.Error())
				return
			}
			found[sweepID] = true
			if procStatus != constants.StatusPendingApproval && procStatus != constants.StatusPendingEditApproval && procStatus != constants.StatusPendingDeleteApproval {
				api.RespondWithResult(w, false, "cannot approve non-pending sweep: "+sweepID)
				return
			}
			actionIDs = append(actionIDs, actionID)
			actionToSweepMap[actionID] = sweepID
			if actionType == constants.AuditActionDelete {
				deleteSweepIDs = append(deleteSweepIDs, sweepID)
			}
		}

		missing := []string{}
		for _, id := range req.SweepIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		for _, sweepID := range req.SweepIDs {
			policyRow, perr := loadSweepConfigRow(ctx, pgxPool, sweepID)
			if perr != nil {
				api.RespondWithResult(w, false, errFailedToFetchSweepConfigForPolicyCheck+perr.Error())
				return
			}
			if ok, msg := runtime.EnforceInline(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreApprove,
				ModuleCode:          common.ModuleCash,
				SubModule:           "SWEEP_CONFIG",
				EntityCode:          policyRow.EntityName,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkApproveSweepConfigurationsV2",
				APIPath:             "/cash/sweep-config-v2/bulk-approve",
				DefaultBlockMessage: "Sweep configuration approve blocked by policy",
				Fields:              buildSweepConfigPolicyFields(policyRow),
			}); !ok {
				api.RespondWithResult(w, false, msg)
				return
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// ── Approval-matrix engine: attempt engine-side approve first.
		engineActed := make(map[string]bool)
		blocked := make(map[string]string)
		for _, sweepID := range req.SweepIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
				ModuleCode: "CASH", RecordID: sweepID,
				UserID: req.UserID, UserEmail: checkerBy,
				Action: approvalengine.ActionApproved, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[SweepConfig] ActOnPendingOrDiagnose approve failed for %s: %v", map[string]interface{}{"sweep_id": sweepID, "error": actionErr.Error()})
				blocked[sweepID] = actionErr.Error()
				continue
			}
			if actionRes.Acted {
				engineActed[sweepID] = true
			} else if actionRes.CancelledStale {
				api.LogInfo("[SweepConfig] cancelled stale approval instance for sweep %s", sweepID)
			} else if actionRes.Reason != "" {
				api.LogInfo("[SweepConfig] engine blocked sweep %s: %s", sweepID, actionRes.Reason)
				blocked[sweepID] = actionRes.Reason
			}
		}

		var legacyActionIDs []string
		var legacyDeleteIDs []string
		for _, id := range actionIDs {
			sID := actionToSweepMap[id]
			if !engineActed[sID] && blocked[sID] == "" {
				legacyActionIDs = append(legacyActionIDs, id)
			}
		}
		for _, id := range deleteSweepIDs {
			if !engineActed[id] && blocked[id] == "" {
				legacyDeleteIDs = append(legacyDeleteIDs, id)
			}
		}

		if len(legacyActionIDs) > 0 {
			upd := `UPDATE cimplrcorpsaas.auditactionsweepconfiguration SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3 WHERE action_id = ANY($4)`
			if _, err := tx.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), nullifyEmpty(api.ClientIPFromRequest(r)), legacyActionIDs); err != nil {
				api.RespondWithResult(w, false, "failed to approve actions: "+err.Error())
				return
			}
		}

		deleted := []string{}
		if len(legacyDeleteIDs) > 0 {
			// perform soft-delete
			updDel := `UPDATE cimplrcorpsaas.sweepconfiguration SET is_deleted = TRUE, updated_at = now() WHERE sweep_id = ANY($1) RETURNING sweep_id`
			drows, derr := tx.Query(ctx, updDel, legacyDeleteIDs)
			if derr != nil {
				api.RespondWithResult(w, false, "failed to soft delete sweeps: "+derr.Error())
				return
			}
			defer drows.Close()
			for drows.Next() {
				var id string
				if err := drows.Scan(&id); err != nil {
					api.RespondWithResult(w, false, "failed to read deleted sweeps: "+err.Error())
					return
				}
				deleted = append(deleted, id)
			}
			if drows.Err() != nil {
				api.RespondWithResult(w, false, "failed to read deleted sweeps: "+drows.Err().Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit approve: "+err.Error())
			return
		}
		committed = true

		dmsevent.Fire(pgxPool, "CASH", "SWEEP_CONFIG", "POST_APPROVE", req.SweepIDs, checkerBy)
		if len(deleted) > 0 {
			dmsevent.Fire(pgxPool, "CASH", "SWEEP_CONFIG", "POST_DELETE", deleted, checkerBy)
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"approved_count": len(actionIDs), "deleted": deleted})
		// Notify: FULL sweep config data for rich templates
		capturedIDs := req.SweepIDs
		capturedUser := req.UserID
		capturedComment := req.Comment
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepConfigNotifPayload(notifyCtx, pgxPool, capturedIDs, "APPROVE", capturedUser)
		payloadMap := payload.ToMap()
		payloadMap["CheckerComment"] = capturedComment
		payloadMap["DeletedIDs"] = deleted
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-config-v2/bulk-approve",
			fmt.Sprintf("SWEEPCFG_APPROVE/%s/%d", capturedUser, time.Now().UnixMilli()),
			payloadMap,
		)
	}
}

// BulkRejectSweepConfigurationsV2 rejects latest audit actions for given sweep_ids (V2 table)
func BulkRejectSweepConfigurationsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			SweepIDs []string `json:"sweep_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.SweepIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		sel := `SELECT DISTINCT ON (sweep_id) action_id, sweep_id, processing_status FROM cimplrcorpsaas.auditactionsweepconfiguration WHERE sweep_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY sweep_id, GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST, action_id DESC`
		rows, err := pgxPool.Query(ctx, sel, req.SweepIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		found := map[string]bool{}
		actionToSweepMap := make(map[string]string)
		for rows.Next() {
			var actionID, sweepID, procStatus string
			if err := rows.Scan(&actionID, &sweepID, &procStatus); err != nil {
				api.RespondWithResult(w, false, "failed to read latest audits: "+err.Error())
				return
			}
			found[sweepID] = true
			if procStatus != constants.StatusPendingApproval && procStatus != constants.StatusPendingEditApproval && procStatus != constants.StatusPendingDeleteApproval {
				api.RespondWithResult(w, false, "cannot reject non-pending sweep: "+sweepID)
				return
			}
			actionIDs = append(actionIDs, actionID)
			actionToSweepMap[actionID] = sweepID
		}
		missing := []string{}
		for _, id := range req.SweepIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		for _, sweepID := range req.SweepIDs {
			policyRow, perr := loadSweepConfigRow(ctx, pgxPool, sweepID)
			if perr != nil {
				api.RespondWithResult(w, false, errFailedToFetchSweepConfigForPolicyCheck+perr.Error())
				return
			}
			if ok, msg := runtime.EnforceInline(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreReject,
				ModuleCode:          common.ModuleCash,
				SubModule:           "SWEEP_CONFIG",
				EntityCode:          policyRow.EntityName,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkRejectSweepConfigurationsV2",
				APIPath:             "/cash/sweep-config-v2/bulk-reject",
				DefaultBlockMessage: "Sweep configuration reject blocked by policy",
				Fields:              buildSweepConfigPolicyFields(policyRow),
			}); !ok {
				api.RespondWithResult(w, false, msg)
				return
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// ── Approval-matrix engine: attempt engine-side reject first.
		engineActed := make(map[string]bool)
		blocked := make(map[string]string)
		for _, sweepID := range req.SweepIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
				ModuleCode: "CASH", RecordID: sweepID,
				UserID: req.UserID, UserEmail: checkerBy,
				Action: approvalengine.ActionRejected, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[SweepConfig] ActOnPendingOrDiagnose reject failed for %s: %v", map[string]interface{}{"sweep_id": sweepID, "error": actionErr.Error()})
				blocked[sweepID] = actionErr.Error()
				continue
			}
			if actionRes.Acted {
				engineActed[sweepID] = true
			} else if actionRes.CancelledStale {
				api.LogInfo("[SweepConfig] cancelled stale approval instance for sweep %s", sweepID)
			} else if actionRes.Reason != "" {
				api.LogInfo("[SweepConfig] engine blocked sweep %s: %s", sweepID, actionRes.Reason)
				blocked[sweepID] = actionRes.Reason
			}
		}

		var legacyActionIDs []string
		for _, id := range actionIDs {
			sID := actionToSweepMap[id]
			if !engineActed[sID] && blocked[sID] == "" {
				legacyActionIDs = append(legacyActionIDs, id)
			}
		}

		if len(legacyActionIDs) > 0 {
			upd := `UPDATE cimplrcorpsaas.auditactionsweepconfiguration SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3 WHERE action_id = ANY($4)`
			if _, err := tx.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), nullifyEmpty(api.ClientIPFromRequest(r)), legacyActionIDs); err != nil {
				api.RespondWithResult(w, false, "failed to reject actions: "+err.Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit reject: "+err.Error())
			return
		}
		committed = true

		dmsevent.Fire(pgxPool, "CASH", "SWEEP_CONFIG", "POST_REJECT", req.SweepIDs, checkerBy)

		api.RespondWithPayload(w, true, "", map[string]interface{}{"rejected_count": len(actionIDs)})
		// Notify: FULL sweep config data for rich templates
		capturedIDs := req.SweepIDs
		capturedUser := req.UserID
		capturedComment := req.Comment
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepConfigNotifPayload(notifyCtx, pgxPool, capturedIDs, "REJECT", capturedUser)
		payloadMap := payload.ToMap()
		payloadMap["CheckerComment"] = capturedComment
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-config-v2/bulk-reject",
			fmt.Sprintf("SWEEPCFG_REJECT/%s/%d", capturedUser, time.Now().UnixMilli()),
			payloadMap,
		)
	}
}

// BulkRequestDeleteSweepConfigurationsV2 inserts DELETE audit actions (PENDING_DELETE_APPROVAL) for sweep configs (V2 table)
func BulkRequestDeleteSweepConfigurationsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			SweepIDs []string `json:"sweep_ids"`
			Reason   string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.SweepIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrInvalidJSON)
			return
		}
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		deleteMatrixByID := map[string]string{}
		for _, sweepID := range req.SweepIDs {
			policyRow, perr := loadSweepConfigRow(ctx, pgxPool, sweepID)
			if perr != nil {
				api.RespondWithResult(w, false, errFailedToFetchSweepConfigForPolicyCheck+perr.Error())
				return
			}
			if ok, msg, tID := runtime.EnforceInlineWithMatrix(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreDelete,
				ModuleCode:          common.ModuleCash,
				SubModule:           "SWEEP_CONFIG",
				EntityCode:          policyRow.EntityName,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkRequestDeleteSweepConfigurationsV2",
				APIPath:             "/cash/sweep-config-v2/bulk-delete",
				DefaultBlockMessage: "Sweep configuration delete blocked by policy",
				Fields:              buildSweepConfigPolicyFields(policyRow),
			}); !ok {
				api.RespondWithResult(w, false, msg)
				return
			} else {
				deleteMatrixByID[sweepID] = tID
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		type pendingDel struct {
			sweepID    string
			entityName string
			amount     float64
		}
		var pendingDels []pendingDel

		for _, id := range req.SweepIDs {
			var entityName string
			var sweepAmt, bufferAmt sqlNullFloat
			if err := tx.QueryRow(ctx, "SELECT entity_name, sweep_amount, buffer_amount FROM cimplrcorpsaas.sweepconfiguration WHERE sweep_id=$1", id).Scan(&entityName, &sweepAmt, &bufferAmt); err != nil {
				api.RespondWithResult(w, false, "sweep config not found for "+id)
				return
			}
			var sAmt, bAmt *float64
			if sweepAmt.Valid {
				sAmt = &sweepAmt.F
			}
			if bufferAmt.Valid {
				bAmt = &bufferAmt.F
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO cimplrcorpsaas.auditactionsweepconfiguration (
					sweep_id, actiontype, processing_status, reason,
					requested_by, requested_at, requested_ip
				) VALUES ($1, 'DELETE', $2, $3, $4, now(), $5)`,
				id, constants.StatusPendingDeleteApproval, nullifyEmpty(req.Reason), requestedBy, nullifyEmpty(api.ClientIPFromRequest(r)),
			); err != nil {
				api.RespondWithResult(w, false, "failed to create delete request for "+id+": "+err.Error())
				return
			}

			pendingDels = append(pendingDels, pendingDel{
				sweepID:    id,
				entityName: entityName,
				amount:     resolveSweepConfigAmount(sAmt, bAmt),
			})
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
			return
		}
		committed = true

		for _, p := range pendingDels {
			if err := approvalengine.CancelPendingInstances(ctx, pgxPool, "CASH", p.sweepID, requestedBy); err != nil {
				api.LogError("[SweepConfig] CancelPendingInstances failed for delete on %s: %v", p.sweepID, err)
			}
			submitSweepConfigForApproval(
				pgxPool, p.sweepID, p.entityName, req.UserID, requestedBy,
				"SWEEP_CONFIG_DELETE", p.amount, deleteMatrixByID[p.sweepID],
			)
		}

		api.RespondWithResult(w, true, fmt.Sprintf("created %d delete requests", len(req.SweepIDs)))
		// Notify: FULL sweep config data for rich templates
		capturedIDs := req.SweepIDs
		capturedUser := req.UserID
		capturedReason := req.Reason
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepConfigNotifPayload(notifyCtx, pgxPool, capturedIDs, constants.AuditActionDelete, capturedUser)
		payloadMap := payload.ToMap()
		payloadMap["Reason"] = capturedReason
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-config-v2/bulk-delete",
			fmt.Sprintf("SWEEPCFG_DELETE/%s/%d", capturedUser, time.Now().UnixMilli()),
			payloadMap,
		)
	}
}

// GetApprovedActiveSweepConfigurations returns only APPROVED and ACTIVE sweep configurations
func GetApprovedActiveSweepConfigurations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, "Missing user_id in body")
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// Get entity filter
		entityNames := api.GetEntityNamesFromCtx(ctx)
		norm := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				norm = append(norm, strings.ToLower(s))
			}
		}

		var rows pgx.Rows
		var err error

		// Query only APPROVED configurations that are ACTIVE (is_deleted = false)
		// Filter out sweeps that already have APPROVED initiations
		q := `
			SELECT DISTINCT ON (sc.sweep_id)
				sc.sweep_id, 
				sc.entity_name, 
				sc.source_bank_name, 
				sc.source_bank_account, 
				sc.target_bank_name, 
				sc.target_bank_account, 
				sc.sweep_type, 
				sc.frequency, 
				sc.effective_date, 
				sc.execution_time, 
				sc.buffer_amount, 
				sc.sweep_amount, 
				sc.requires_initiation,
				sc.created_at
			FROM cimplrcorpsaas.sweepconfiguration sc
			JOIN cimplrcorpsaas.auditactionsweepconfiguration a 
				ON a.sweep_id = sc.sweep_id
			WHERE sc.is_deleted = false 
				AND a.processing_status = 'APPROVED'
				AND NOT EXISTS (
					SELECT 1 FROM cimplrcorpsaas.sweep_initiation si
					JOIN cimplrcorpsaas.auditactionsweepinitiation asi 
						ON asi.initiation_id = si.initiation_id
					WHERE si.sweep_id = sc.sweep_id 
				)
		`

		if len(norm) > 0 {
			q += ` AND lower(trim(sc.entity_name)) = ANY($1)`
			q += ` ORDER BY sc.sweep_id, a.requested_at DESC`
			rows, err = pgxPool.Query(ctx, q, norm)
		} else {
			q += ` ORDER BY sc.sweep_id, a.requested_at DESC`
			rows, err = pgxPool.Query(ctx, q)
		}

		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var sweepID string
			var entity, sourceBank, sourceAccount, targetBank, targetAccount, sweepType, freq sqlNullString
			var effectiveDate, execTime sqlNullString
			var bufferAmt, sweepAmt sqlNullFloat
			var requiresInitiation *bool
			var createdAt time.Time

			if err := rows.Scan(
				&sweepID, &entity,
				&sourceBank, &sourceAccount,
				&targetBank, &targetAccount,
				&sweepType, &freq,
				&effectiveDate, &execTime,
				&bufferAmt, &sweepAmt,
				&requiresInitiation,
				&createdAt,
			); err != nil {
				api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
				return
			}
			if msg := validateSweepConfigV2Scope(ctx,
				fmt.Sprint(entity.ValueOrZero()),
				fmt.Sprint(sourceBank.ValueOrZero()),
				fmt.Sprint(sourceAccount.ValueOrZero()),
				fmt.Sprint(targetBank.ValueOrZero()),
				fmt.Sprint(targetAccount.ValueOrZero()),
			); msg != "" {
				continue
			}

			m := map[string]interface{}{
				"sweep_id":            sweepID,
				"entity_name":         entity.ValueOrZero(),
				"source_bank_name":    sourceBank.ValueOrZero(),
				"source_bank_account": sourceAccount.ValueOrZero(),
				"target_bank_name":    targetBank.ValueOrZero(),
				"target_bank_account": targetAccount.ValueOrZero(),
				"sweep_type":          sweepType.ValueOrZero(),
				"frequency":           freq.ValueOrZero(),
				"effective_date":      effectiveDate.ValueOrZero(),
				"execution_time":      execTime.ValueOrZero(),
				"buffer_amount":       bufferAmt.ValueOrZero(),
				"sweep_amount":        sweepAmt.ValueOrZero(),
				"requires_initiation": requiresInitiation,
				"created_at":          createdAt,
			}
			out = append(out, m)
		}

		if rows.Err() != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
