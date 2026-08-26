package limit

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
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/validation"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const errFailedToLoadBankLimitForPolicyCheck = "failed to load bank limit for policy check: "

func validateLimitCashScope(ctx context.Context, refs map[string]interface{}) string {
	return validation.ValidateCashMasterReferences(ctx, refs)
}

func validateBankLimitRecordScope(ctx context.Context, pgxPool *pgxpool.Pool, limitID, overrideCurrency string) string {
	var entityName, bankName, currencyCode string
	if err := pgxPool.QueryRow(ctx, `
		SELECT COALESCE(entity_name, ''), COALESCE(bank_name, ''), COALESCE(currency_code, '')
		FROM cimplrcorpsaas.bank_limit
		WHERE limit_id = $1
	`, limitID).Scan(&entityName, &bankName, &currencyCode); err != nil {
		return "failed to validate limit scope: " + err.Error()
	}
	if strings.TrimSpace(overrideCurrency) != "" {
		currencyCode = overrideCurrency
	}
	return validateLimitCashScope(ctx, map[string]interface{}{
		"entity_name":   entityName,
		"bank_name":     bankName,
		"currency_code": currencyCode,
	})
}

// CreateBankLimit creates a new bank limit with PENDING_APPROVAL audit status
func CreateBankLimit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID             string   `json:"user_id"`
			EntityName         string   `json:"entity_name"`
			BankName           string   `json:"bank_name"`
			CoreLimitType      string   `json:"core_limit_type"`
			LimitType          string   `json:"limit_type"`
			LimitSubType       string   `json:"limit_sub_type"`
			SanctionDate       string   `json:"sanction_date"`
			EffectiveDate      string   `json:"effective_date"`
			CurrencyCode       string   `json:"currency_code"`
			SanctionedAmount   float64  `json:"sanctioned_amount"`
			FungibilityType    string   `json:"fungibility_type"`
			FungibilityPct     *float64 `json:"fungibility_pct"`
			SecurityType       string   `json:"security_type"`
			Remarks            string   `json:"remarks"`
			InitialUtilization *float64 `json:"initial_utilization"`
			Reason             string   `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
			return
		}

		// Validate entity access
		if !ctxutil.FromContext(ctx).HasEntityAccess(req.EntityName) {
			api.RespondWithResult(w, false, "unauthorized entity")
			return
		}
		if msg := validateLimitCashScope(ctx, map[string]interface{}{
			"entity_name":   req.EntityName,
			"bank_name":     req.BankName,
			"currency_code": req.CurrencyCode,
		}); msg != "" {
			api.RespondWithResult(w, false, msg)
			return
		}

		// Validate core_limit_type
		coreLimitType := strings.ToUpper(strings.TrimSpace(req.CoreLimitType))
		if coreLimitType != constants.FundBased && coreLimitType != constants.NonFundBased && coreLimitType != constants.TermLoans {
			api.RespondWithResult(w, false, "invalid core_limit_type. Allowed: Fund Based, Non Fund Based, Term Loans")
			return
		}

		// Validate fungibility_type
		fungibilityType := strings.ToUpper(strings.TrimSpace(req.FungibilityType))
		if fungibilityType != constants.InterCore && fungibilityType != constants.IntraCore && fungibilityType != constants.None {
			api.RespondWithResult(w, false, "invalid fungibility_type. Allowed: Inter-Core, Intra-Core, None")
			return
		}

		// Validate security_type
		securityType := strings.ToUpper(strings.TrimSpace(req.SecurityType))
		if securityType != "SECURED" && securityType != "UNSECURED" {
			api.RespondWithResult(w, false, "invalid security_type. Allowed: Secured, Unsecured")
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

		// Check for duplicate limit combination
		key := LimitUniqueKey{
			EntityName:     req.EntityName,
			BankName:       req.BankName,
			CoreLimitType:  coreLimitType,
			LimitType:      req.LimitType,
			LimitSubType:   req.LimitSubType,
			CurrencyCode:   strings.ToUpper(req.CurrencyCode),
			ExcludeLimitID: "",
		}
		if err := checkLimitUniqueness(ctx, pgxPool, key); err != nil {
			api.RespondWithResult(w, false, err.Error())
			return
		}

		ok, triggerMatrixID := runtime.EnforceWithMatrix(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreCreate,
			ModuleCode:          common.ModuleCash,
			SubModule:           "BANK_LIMIT",
			EntityCode:          req.EntityName,
			ActorUserID:         req.UserID,
			HandlerName:         "CreateBankLimit",
			APIPath:             "/cash/limit/create",
			DefaultBlockMessage: "Bank limit create blocked by policy",
			Fields: buildBankLimitPolicyFields(bankLimitRow{
				EntityName:         req.EntityName,
				BankName:           req.BankName,
				CoreLimitType:      coreLimitType,
				LimitType:          req.LimitType,
				LimitSubType:       req.LimitSubType,
				SanctionDate:       req.SanctionDate,
				EffectiveDate:      req.EffectiveDate,
				CurrencyCode:       req.CurrencyCode,
				SanctionedAmount:   req.SanctionedAmount,
				FungibilityType:    fungibilityType,
				FungibilityPct:     req.FungibilityPct,
				SecurityType:       securityType,
				Remarks:            req.Remarks,
				InitialUtilization: req.InitialUtilization,
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

		ins := `INSERT INTO cimplrcorpsaas.bank_limit (
			entity_name, bank_name, core_limit_type, limit_type, limit_sub_type,
			sanction_date, effective_date, currency_code, sanctioned_amount,
			fungibility_type, fungibility_pct, security_type, remarks, initial_utilization
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING limit_id`

		var limitID string
		err = tx.QueryRow(ctx, ins,
			req.EntityName, req.BankName, coreLimitType,
			nullifyEmpty(req.LimitType), nullifyEmpty(req.LimitSubType),
			nullifyEmpty(req.SanctionDate), nullifyEmpty(req.EffectiveDate),
			strings.ToUpper(req.CurrencyCode), req.SanctionedAmount,
			fungibilityType, nullifyFloat(req.FungibilityPct),
			securityType, nullifyEmpty(req.Remarks), nullifyFloat(req.InitialUtilization),
		).Scan(&limitID)

		if err != nil {
			api.RespondWithResult(w, false, parseLimitConstraintError(err))
			return
		}

		auditStatus := approvalengine.AuditStatus(triggerMatrixID, "PENDING_APPROVAL")
		if _, err := tx.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.auditactionbanklimit (
				limit_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip
			) VALUES ($1,'CREATE',$2,$3,$4,now(),$5)`,
			limitID, auditStatus, nullifyEmpty(req.Reason), requestedBy, api.ClientIPFromContext(ctx),
		); err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToCreateAudit+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
			return
		}

		submitBankLimitForApproval(pgxPool, submitBankLimitParams{
			LimitID: limitID, EntityName: req.EntityName, SubmittedByUserID: req.UserID, ActorEmail: requestedBy,
			TxType: "BANK_LIMIT_CREATE", Amount: req.SanctionedAmount, MatrixID: triggerMatrixID,
		})

		dmsevent.Fire(pgxPool, "CASH", "BANK_LIMIT", "POST_CREATE", []string{limitID}, requestedBy)

		api.RespondWithResult(w, true, limitID)
	}
}

// BulkCreateBankLimit creates multiple limits with individual transactions
func BulkCreateBankLimit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		type LimitRequest struct {
			EntityName         string   `json:"entity_name"`
			BankName           string   `json:"bank_name"`
			CoreLimitType      string   `json:"core_limit_type"`
			LimitType          string   `json:"limit_type"`
			LimitSubType       string   `json:"limit_sub_type"`
			SanctionDate       string   `json:"sanction_date"`
			EffectiveDate      string   `json:"effective_date"`
			CurrencyCode       string   `json:"currency_code"`
			SanctionedAmount   float64  `json:"sanctioned_amount"`
			FungibilityType    string   `json:"fungibility_type"`
			FungibilityPct     *float64 `json:"fungibility_pct"`
			SecurityType       string   `json:"security_type"`
			Remarks            string   `json:"remarks"`
			InitialUtilization *float64 `json:"initial_utilization"`
			Reason             string   `json:"reason"`
		}

		var req struct {
			UserID string         `json:"user_id"`
			Limits []LimitRequest `json:"limits"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.Limits) == 0 {
			api.RespondWithResult(w, false, "user_id and limits array required")
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

		results := make([]map[string]interface{}, 0, len(req.Limits))

		// OPTIMIZED: First validate all fields and prepare data for bulk uniqueness check
		validLimits := make([]struct {
			Index         int
			EntityName    string
			BankName      string
			CoreLimitType string
			LimitType     string
			LimitSubType  string
			CurrencyCode  string
			OriginalData  LimitRequest
		}, 0, len(req.Limits))

		// Pre-validate entity access and enum values
		for i, lim := range req.Limits {
			result := map[string]interface{}{"index": i}

			// Validate entity
			if !ctxutil.FromContext(ctx).HasEntityAccess(lim.EntityName) {
				result["success"] = false
				result["error"] = "unauthorized entity: " + lim.EntityName
				results = append(results, result)
				continue
			}
			if msg := validateLimitCashScope(ctx, map[string]interface{}{
				"entity_name":   lim.EntityName,
				"bank_name":     lim.BankName,
				"currency_code": lim.CurrencyCode,
			}); msg != "" {
				result["success"] = false
				result["error"] = msg
				results = append(results, result)
				continue
			}

			// Validate enums
			coreLimitType := strings.ToUpper(strings.TrimSpace(lim.CoreLimitType))
			if coreLimitType != constants.FundBased && coreLimitType != constants.NonFundBased && coreLimitType != constants.TermLoans {
				result["success"] = false
				result["error"] = "invalid core_limit_type"
				results = append(results, result)
				continue
			}

			fungibilityType := strings.ToUpper(strings.TrimSpace(lim.FungibilityType))
			if fungibilityType != constants.InterCore && fungibilityType != constants.IntraCore && fungibilityType != constants.None {
				result["success"] = false
				result["error"] = "invalid fungibility_type"
				results = append(results, result)
				continue
			}

			securityType := strings.ToUpper(strings.TrimSpace(lim.SecurityType))
			if securityType != "SECURED" && securityType != "UNSECURED" {
				result["success"] = false
				result["error"] = "invalid security_type"
				results = append(results, result)
				continue
			}

			// Add to valid limits for bulk uniqueness checking
			validLimits = append(validLimits, struct {
				Index         int
				EntityName    string
				BankName      string
				CoreLimitType string
				LimitType     string
				LimitSubType  string
				CurrencyCode  string
				OriginalData  LimitRequest
			}{
				Index:         i,
				EntityName:    lim.EntityName,
				BankName:      lim.BankName,
				CoreLimitType: coreLimitType,
				LimitType:     lim.LimitType,
				LimitSubType:  lim.LimitSubType,
				CurrencyCode:  strings.ToUpper(lim.CurrencyCode),
				OriginalData:  lim,
			})
		}

		// OPTIMIZED: Bulk uniqueness validation for all valid limits
		if len(validLimits) > 0 {
			uniquenessData := make([]struct {
				Index         int
				EntityName    string
				BankName      string
				CoreLimitType string
				LimitType     string
				LimitSubType  string
				CurrencyCode  string
			}, len(validLimits))

			for i, vl := range validLimits {
				uniquenessData[i] = struct {
					Index         int
					EntityName    string
					BankName      string
					CoreLimitType string
					LimitType     string
					LimitSubType  string
					CurrencyCode  string
				}{
					Index:         vl.Index,
					EntityName:    vl.EntityName,
					BankName:      vl.BankName,
					CoreLimitType: vl.CoreLimitType,
					LimitType:     vl.LimitType,
					LimitSubType:  vl.LimitSubType,
					CurrencyCode:  vl.CurrencyCode,
				}
			}

			uniquenessResults, err := validateBulkLimitUniqueness(ctx, pgxPool, uniquenessData)
			if err != nil {
				api.RespondWithResult(w, false, "bulk uniqueness validation failed: "+err.Error())
				return
			}

			// Process results based on uniqueness validation
			validLimitMap := make(map[int]bool) // Track which original indices are valid
			for _, ur := range uniquenessResults {
				if !ur.IsValid {
					result := map[string]interface{}{
						"index":   ur.Index,
						"success": false,
						"error":   ur.Error,
					}
					results = append(results, result)
				} else {
					validLimitMap[ur.Index] = true
				}
			}

			// Now process only the valid, unique limits
			for _, vl := range validLimits {
				if !validLimitMap[vl.Index] {
					continue // Skip limits that failed uniqueness validation
				}

				result := map[string]interface{}{"index": vl.Index}
				lim := vl.OriginalData

				// Get the processed enum values
				coreLimitType := vl.CoreLimitType
				fungibilityType := strings.ToUpper(strings.TrimSpace(lim.FungibilityType))
				securityType := strings.ToUpper(strings.TrimSpace(lim.SecurityType))

				ok, msg, tID := runtime.EnforceInlineWithMatrix(ctx, r, pgxPool, runtime.EnforceInput{
					EventCode:           common.TriggerPreCreate,
					ModuleCode:          common.ModuleCash,
					SubModule:           "BANK_LIMIT",
					EntityCode:          lim.EntityName,
					ActorUserID:         req.UserID,
					HandlerName:         "BulkCreateBankLimit",
					APIPath:             "/cash/limit/bulk-create",
					DefaultBlockMessage: "Bank limit create blocked by policy",
					Fields: mergeIndexField(buildBankLimitPolicyFields(bankLimitRow{
						EntityName:         lim.EntityName,
						BankName:           lim.BankName,
						CoreLimitType:      coreLimitType,
						LimitType:          lim.LimitType,
						LimitSubType:       lim.LimitSubType,
						SanctionDate:       lim.SanctionDate,
						EffectiveDate:      lim.EffectiveDate,
						CurrencyCode:       lim.CurrencyCode,
						SanctionedAmount:   lim.SanctionedAmount,
						FungibilityType:    fungibilityType,
						FungibilityPct:     lim.FungibilityPct,
						SecurityType:       securityType,
						Remarks:            lim.Remarks,
						InitialUtilization: lim.InitialUtilization,
					}), vl.Index),
				})
				if !ok {
					result["success"] = false
					result["error"] = msg
					results = append(results, result)
					continue
				}

				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					result["success"] = false
					result["error"] = constants.ErrFailedToBeginTransaction
					results = append(results, result)
					continue
				}

				ins := `INSERT INTO cimplrcorpsaas.bank_limit (
					entity_name, bank_name, core_limit_type, limit_type, limit_sub_type,
					sanction_date, effective_date, currency_code, sanctioned_amount,
					fungibility_type, fungibility_pct, security_type, remarks, initial_utilization
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING limit_id`

				var limitID string
				err = tx.QueryRow(ctx, ins,
					lim.EntityName, lim.BankName, coreLimitType,
					nullifyEmpty(lim.LimitType), nullifyEmpty(lim.LimitSubType),
					nullifyEmpty(lim.SanctionDate), nullifyEmpty(lim.EffectiveDate),
					strings.ToUpper(lim.CurrencyCode), lim.SanctionedAmount,
					fungibilityType, nullifyFloat(lim.FungibilityPct),
					securityType, nullifyEmpty(lim.Remarks), nullifyFloat(lim.InitialUtilization),
				).Scan(&limitID)

				if err != nil {
					tx.Rollback(ctx)
					result["success"] = false
					result["error"] = "failed to insert limit: " + err.Error()
					results = append(results, result)
					continue
				}

				auditStatus := approvalengine.AuditStatus(tID, "PENDING_APPROVAL")
				if _, err := tx.Exec(ctx, `
					INSERT INTO cimplrcorpsaas.auditactionbanklimit (
						limit_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip
					) VALUES ($1,'CREATE',$2,$3,$4,now(),$5)`,
					limitID, auditStatus, nil, requestedBy, api.ClientIPFromContext(ctx),
				); err != nil {
					tx.Rollback(ctx)
					result["success"] = false
					result["error"] = constants.ErrFailedToCreateAudit + err.Error()
					results = append(results, result)
					continue
				}

				if err := tx.Commit(ctx); err != nil {
					result["success"] = false
					result["error"] = constants.ErrTxCommitFailed
					results = append(results, result)
					continue
				}

				submitBankLimitForApproval(pgxPool, submitBankLimitParams{
					LimitID: limitID, EntityName: lim.EntityName, SubmittedByUserID: req.UserID, ActorEmail: requestedBy,
					TxType: "BANK_LIMIT_CREATE", Amount: lim.SanctionedAmount, MatrixID: tID,
				})

				result["success"] = true
				result["limit_id"] = limitID
				results = append(results, result)
			}
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
		// Notify: bulk limit creation submitted with FULL record data
		// Collect successful limit_ids from results
		createdLimitIDs := []string{}
		for _, r := range results {
			if success, ok := r["success"].(bool); ok && success {
				if limitID, ok := r["limit_id"].(string); ok && limitID != "" {
					createdLimitIDs = append(createdLimitIDs, limitID)
				}
			}
		}
		if len(createdLimitIDs) > 0 {
			dmsevent.Fire(pgxPool, "CASH", "BANK_LIMIT", "POST_CREATE", createdLimitIDs, requestedBy)
			// Pass req.UserID (not display name) so the notification dispatcher can resolve
			// the actor's entity via: SELECT business_unit_name FROM users WHERE id::text=$1
			payload := BuildLimitNotifPayload(context.Background(), pgxPool, createdLimitIDs, "CREATE", req.UserID)
			go catalog.TriggerNotification(
				context.Background(), pgxPool,
				"/cash/limit/bulk-create",
				fmt.Sprintf("LIMIT_BULK_CREATE/%s/%d", req.UserID, time.Now().UnixMilli()),
				payload.ToMap(),
			)
		}
	}
}

// UpdateBankLimit updates an existing limit and creates PENDING_EDIT_APPROVAL audit
func UpdateBankLimit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  string                 `json:"user_id"`
			LimitID string                 `json:"limit_id"`
			Fields  map[string]interface{} `json:"fields"`
			Reason  string                 `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || req.LimitID == "" {
			api.RespondWithResult(w, false, "user_id and limit_id required")
			return
		}

		if len(req.Fields) == 0 {
			api.RespondWithResult(w, false, "no fields provided to update")
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

		existingRow, err := loadBankLimitRow(ctx, pgxPool, req.LimitID)
		if err != nil {
			api.RespondWithResult(w, false, errFailedToLoadBankLimitForPolicyCheck+err.Error())
			return
		}
		mergedRow := applyBankLimitEdits(existingRow, req.Fields)
		ok, triggerMatrixID := runtime.EnforceWithMatrix(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreEdit,
			ModuleCode:          common.ModuleCash,
			SubModule:           "BANK_LIMIT",
			ActorUserID:         req.UserID,
			HandlerName:         "UpdateBankLimit",
			APIPath:             "/cash/limit/update",
			DefaultBlockMessage: "Bank limit update blocked by policy",
			Fields:              buildBankLimitPolicyFields(mergedRow),
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

		// Fetch current row to lock
		sel := `SELECT entity_name, bank_name, core_limit_type, limit_type, limit_sub_type, sanction_date, effective_date, currency_code, sanctioned_amount, fungibility_type, fungibility_pct, security_type, remarks, initial_utilization FROM cimplrcorpsaas.bank_limit WHERE limit_id = $1 FOR UPDATE`
		var curEntity, curBank, curCoreLimit, curLimitType, curLimitSub *string
		var curSanctionDate, curEffectiveDate *time.Time
		var curCurrency *string
		var curSanctionedAmount float64
		var curFungibilityType *string
		var curFungibilityPct *float64
		var curSecurity *string
		var curRemarks *string
		var curInitialUtilization *float64

		if err := tx.QueryRow(ctx, sel, req.LimitID).Scan(&curEntity, &curBank, &curCoreLimit, &curLimitType, &curLimitSub, &curSanctionDate, &curEffectiveDate, &curCurrency, &curSanctionedAmount, &curFungibilityType, &curFungibilityPct, &curSecurity, &curRemarks, &curInitialUtilization); err != nil {
			api.RespondWithResult(w, false, "failed to fetch current limit: "+err.Error())
			return
		}
		effectiveRefs := map[string]interface{}{
			"entity_name":   stringOrEmpty(curEntity),
			"bank_name":     stringOrEmpty(curBank),
			"currency_code": stringOrEmpty(curCurrency),
		}
		for k, v := range req.Fields {
			switch strings.ToLower(k) {
			case "entity_name":
				effectiveRefs["entity_name"] = v
			case "bank_name":
				effectiveRefs["bank_name"] = v
			case "currency_code":
				effectiveRefs["currency_code"] = v
			}
		}
		if msg := validateLimitCashScope(ctx, effectiveRefs); msg != "" {
			api.RespondWithResult(w, false, msg)
			return
		}

		// Build dynamic SET clause. For each provided field we first set old_<col> = <col>, then <col> = $N
		oldSets := []string{}
		newSets := []string{}
		args := []interface{}{}
		pos := 1

		// helper to add string field
		addStr := func(col string, value interface{}) {
			oldSets = append(oldSets, "old_"+col+" = "+col)
			newSets = append(newSets, col+" = $"+fmt.Sprint(pos))
			args = append(args, value)
			pos++
		}
		addFloat := func(col string, value interface{}) {
			oldSets = append(oldSets, "old_"+col+" = "+col)
			newSets = append(newSets, col+" = $"+fmt.Sprint(pos))
			args = append(args, value)
			pos++
		}

		for k, v := range req.Fields {
			switch strings.ToLower(k) {
			case "entity_name":
				if s, ok := v.(string); ok {
					addStr("entity_name", s)
				}
			case "bank_name":
				if s, ok := v.(string); ok {
					addStr("bank_name", s)
				}
			case "core_limit_type":
				if s, ok := v.(string); ok {
					val := strings.ToUpper(strings.TrimSpace(s))
					if val != constants.FundBased && val != constants.NonFundBased && val != constants.TermLoans {
						api.RespondWithResult(w, false, "invalid core_limit_type")
						return
					}
					addStr("core_limit_type", val)
				}
			case "limit_type":
				if s, ok := v.(string); ok {
					addStr("limit_type", s)
				}
			case "limit_sub_type":
				if s, ok := v.(string); ok {
					addStr("limit_sub_type", s)
				}
			case "sanction_date":
				if s, ok := v.(string); ok {
					addStr("sanction_date", s)
				}
			case "effective_date":
				if s, ok := v.(string); ok {
					addStr("effective_date", s)
				}
			case "currency_code":
				if s, ok := v.(string); ok {
					addStr("currency_code", strings.ToUpper(s))
				}
			case "sanctioned_amount":
				switch t := v.(type) {
				case float64:
					addFloat("sanctioned_amount", t)
				case int:
					addFloat("sanctioned_amount", float64(t))
				}
			case "fungibility_type":
				if s, ok := v.(string); ok {
					val := strings.ToUpper(strings.TrimSpace(s))
					if val != constants.InterCore && val != constants.IntraCore && val != constants.None {
						api.RespondWithResult(w, false, "invalid fungibility_type")
						return
					}
					addStr("fungibility_type", val)
				}
			case "fungibility_pct":
				switch t := v.(type) {
				case float64:
					addFloat("fungibility_pct", t)
				case int:
					addFloat("fungibility_pct", float64(t))
				}
			case "security_type":
				if s, ok := v.(string); ok {
					val := strings.ToUpper(strings.TrimSpace(s))
					if val != "SECURED" && val != "UNSECURED" {
						api.RespondWithResult(w, false, "invalid security_type")
						return
					}
					addStr("security_type", val)
				}
			case "remarks":
				if s, ok := v.(string); ok {
					addStr("remarks", s)
				}
			case "initial_utilization":
				switch t := v.(type) {
				case float64:
					addFloat("initial_utilization", t)
				case int:
					addFloat("initial_utilization", float64(t))
				}
			default:
				// ignore unknown fields
			}
		}

		if len(newSets) == 0 {
			api.RespondWithResult(w, false, "no valid fields provided to update")
			return
		}

		setClause := strings.Join(oldSets, ", ")
		if setClause != "" {
			setClause += ", "
		}
		setClause += strings.Join(newSets, ", ")

		q := "UPDATE cimplrcorpsaas.bank_limit SET " + setClause + " WHERE limit_id = $" + fmt.Sprint(pos)
		args = append(args, req.LimitID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithResult(w, false, "failed to update limit: "+err.Error())
			return
		}

		auditStatus := approvalengine.AuditStatus(triggerMatrixID, "PENDING_EDIT_APPROVAL")
		if _, err := tx.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.auditactionbanklimit (
				limit_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip
			) VALUES ($1,'EDIT',$2,$3,$4,now(),$5)`,
			req.LimitID, auditStatus, nullifyEmpty(req.Reason), requestedBy, api.ClientIPFromContext(ctx),
		); err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToCreateAudit+err.Error())
			return
		}

		// Cancel any pending approval instances for this record
		if err := approvalengine.CancelPendingInstances(ctx, pgxPool, "CASH", req.LimitID, requestedBy); err != nil {
			api.LogError("[BankLimit] Failed to cancel pending instances for %s: %v", req.LimitID, err.Error())
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
			return
		}

		// Amount logic: if SanctionedAmount was edited, use new, else use old
		amt := curSanctionedAmount
		if v, ok := req.Fields["sanctioned_amount"]; ok {
			if f, ok := v.(float64); ok {
				amt = f
			} else if i, ok := v.(int); ok {
				amt = float64(i)
			}
		}

		submitBankLimitForApproval(pgxPool, submitBankLimitParams{
			LimitID: req.LimitID, EntityName: stringOrEmpty(curEntity), SubmittedByUserID: req.UserID, ActorEmail: requestedBy,
			TxType: "BANK_LIMIT_EDIT", Amount: amt, MatrixID: triggerMatrixID,
		})

		dmsevent.Fire(pgxPool, "CASH", "BANK_LIMIT", "POST_EDIT", []string{req.LimitID}, requestedBy)

		api.RespondWithResult(w, true, req.LimitID)
		// Notify: limit updated with FULL record data
		capturedLimitID := req.LimitID
		capturedUser := req.UserID
		// Pass UserID (not display name) so dispatcher resolves actor entity correctly
		payload := BuildLimitNotifPayload(context.Background(), pgxPool, []string{capturedLimitID}, "UPDATE", capturedUser)
		go catalog.TriggerNotification(
			context.Background(), pgxPool,
			"/cash/limit/update",
			fmt.Sprintf("LIMIT_UPDATE/%s/%s", capturedLimitID, capturedUser),
			payload.ToMap(),
		)
	}
}

// DeleteBankLimit creates PENDING_DELETE_APPROVAL audit action
func DeleteBankLimit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			LimitIDs []string `json:"limit_ids"`
			Reason   string   `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.LimitIDs) == 0 {
			api.RespondWithResult(w, false, "user_id and limit_ids required")
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

		results := make([]map[string]interface{}, 0, len(req.LimitIDs))

		for i, limitID := range req.LimitIDs {
			result := map[string]interface{}{"index": i, "limit_id": limitID}

			policyRow, perr := loadBankLimitRow(ctx, pgxPool, limitID)
			if perr != nil {
				result["success"] = false
				result["error"] = errFailedToLoadBankLimitForPolicyCheck + perr.Error()
				results = append(results, result)
				continue
			}

			ok, msg, tID := runtime.EnforceInlineWithMatrix(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreDelete,
				ModuleCode:          common.ModuleCash,
				SubModule:           "BANK_LIMIT",
				ActorUserID:         req.UserID,
				HandlerName:         "DeleteBankLimit",
				APIPath:             "/cash/limit/delete",
				DefaultBlockMessage: "Bank limit delete blocked by policy",
				Fields:              buildBankLimitPolicyFields(policyRow),
			})
			if !ok {
				result["success"] = false
				result["error"] = msg
				results = append(results, result)
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				result["success"] = false
				result["error"] = constants.ErrFailedToBeginTransaction
				results = append(results, result)
				continue
			}

			// Cancel any pending approval instances for this record
			if err := approvalengine.CancelPendingInstances(ctx, pgxPool, "CASH", limitID, requestedBy); err != nil {
				api.LogError("[BankLimit] Failed to cancel pending instances for %s: %v", limitID, err.Error())
			}

			auditStatus := approvalengine.AuditStatus(tID, "PENDING_DELETE_APPROVAL")
			if _, err := tx.Exec(ctx, `
				INSERT INTO cimplrcorpsaas.auditactionbanklimit (
					limit_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip
				) VALUES ($1,'DELETE',$2,$3,$4,now(),$5)`,
				limitID, auditStatus, nullifyEmpty(req.Reason), requestedBy, api.ClientIPFromContext(ctx),
			); err != nil {
				tx.Rollback(ctx)
				result["success"] = false
				result["error"] = constants.ErrFailedToCreateAudit + err.Error()
				results = append(results, result)
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				result["success"] = false
				result["error"] = constants.ErrTxCommitFailed
				results = append(results, result)
				continue
			}

			// We need the amount for the approval instance. It comes from policyRow
			amt := policyRow.SanctionedAmount
			submitBankLimitForApproval(pgxPool, submitBankLimitParams{
				LimitID: limitID, EntityName: policyRow.EntityName, SubmittedByUserID: req.UserID, ActorEmail: requestedBy,
				TxType: "BANK_LIMIT_DELETE", Amount: amt, MatrixID: tID,
			})

			result["success"] = true
			results = append(results, result)
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
		// Notify: limits submitted for deletion with FULL record data
		// capturedUser = req.UserID (not display name) so dispatcher resolves entity correctly
		capturedUser := req.UserID
		capturedIDs := req.LimitIDs
		payload := BuildLimitNotifPayload(context.Background(), pgxPool, capturedIDs, "DELETE", capturedUser)
		go catalog.TriggerNotification(
			context.Background(), pgxPool,
			"/cash/limit/delete",
			fmt.Sprintf("LIMIT_DELETE/%s/%d", capturedUser, time.Now().UnixMilli()),
			payload.ToMap(),
		)
	}
}

// GetAllBankLimits returns all limits with latest audit info and entity filtering
func GetAllBankLimits(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		entityNames := api.GetEntityNamesFromCtx(ctx)

		query := `
			SELECT 
				l.limit_id, l.entity_name, l.bank_name, l.core_limit_type, l.limit_type, l.limit_sub_type,
				l.sanction_date, l.effective_date, l.currency_code, l.sanctioned_amount,
				l.fungibility_type, l.fungibility_pct, l.security_type, l.remarks, l.initial_utilization,
				l.old_entity_name, l.old_bank_name, l.old_core_limit_type, l.old_limit_type, l.old_limit_sub_type,
				l.old_sanction_date, l.old_effective_date, l.old_currency_code, l.old_sanctioned_amount,
				l.old_fungibility_type, l.old_fungibility_pct, l.old_security_type, l.old_remarks, l.old_initial_utilization,
				a.action_type, a.processing_status, a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason,
				(e.instance_id IS NOT NULL) AS pending_approval, e.instance_id
			FROM cimplrcorpsaas.bank_limit l
			LEFT JOIN LATERAL (
				SELECT action_type, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment, reason
				FROM cimplrcorpsaas.auditactionbanklimit
				WHERE limit_id = l.limit_id
				  AND action_type IN ('CREATE','EDIT','DELETE')
				ORDER BY GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST, action_id DESC
				LIMIT 1
		) a ON TRUE
		LEFT JOIN LATERAL (
			SELECT ai.instance_id
			FROM uam.approval_instance ai
			WHERE ai.record_id = l.limit_id::text AND ai.module_code = 'CASH'
			  AND ai.status = 'PENDING' AND ai.is_deleted = false
			ORDER BY ai.submitted_at DESC, ai.instance_id DESC LIMIT 1
		) e ON TRUE
		WHERE COALESCE(l.is_deleted, false) = false
			AND l.entity_name = ANY($1)
		ORDER BY GREATEST(
			COALESCE(a.requested_at, '1970-01-01'::timestamp),
			COALESCE(a.checker_at,   '1970-01-01'::timestamp)
		) DESC NULLS LAST,
		l.limit_id DESC`
		rows, err := pgxPool.Query(ctx, query, entityNames)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			var limitID, entityName, bankName, coreLimitType, currencyCode, fungibilityType, securityType string
			var limitType, limitSubType, remarks *string
			var sanctionDate, effectiveDate *time.Time
			var sanctionedAmount float64
			var fungibilityPct, initialUtilization *float64

			// Old values
			var oldEntityName, oldBankName, oldCoreLimitType, oldCurrencyCode, oldFungibilityType, oldSecurityType *string
			var oldLimitType, oldLimitSubType, oldRemarks *string
			var oldSanctionDate, oldEffectiveDate *time.Time
			var oldSanctionedAmount, oldFungibilityPct, oldInitialUtilization *float64

			var actionType, procStatus, requestedBy, checkerBy, checkerComment, reason *string
			var requestedAt, checkerAt *time.Time
			var pendingApproval *bool
			var instanceID *string

			err := rows.Scan(
				&limitID, &entityName, &bankName, &coreLimitType, &limitType, &limitSubType,
				&sanctionDate, &effectiveDate, &currencyCode, &sanctionedAmount,
				&fungibilityType, &fungibilityPct, &securityType, &remarks, &initialUtilization,
				&oldEntityName, &oldBankName, &oldCoreLimitType, &oldLimitType, &oldLimitSubType,
				&oldSanctionDate, &oldEffectiveDate, &oldCurrencyCode, &oldSanctionedAmount,
				&oldFungibilityType, &oldFungibilityPct, &oldSecurityType, &oldRemarks, &oldInitialUtilization,
				&actionType, &procStatus, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason,
				&pendingApproval, &instanceID,
			)
			if err != nil {
				continue
			}

			item := map[string]interface{}{
				"limit_id":            limitID,
				"entity_name":         entityName,
				"bank_name":           bankName,
				"core_limit_type":     coreLimitType,
				"limit_type":          stringOrEmpty(limitType),
				"limit_sub_type":      stringOrEmpty(limitSubType),
				"sanction_date":       timeOrEmpty(sanctionDate),
				"effective_date":      timeOrEmpty(effectiveDate),
				"currency_code":       currencyCode,
				"sanctioned_amount":   sanctionedAmount,
				"fungibility_type":    fungibilityType,
				"fungibility_pct":     floatOrZero(fungibilityPct),
				"security_type":       securityType,
				"remarks":             stringOrEmpty(remarks),
				"initial_utilization": floatOrZero(initialUtilization),

				"old_entity_name":         stringOrEmpty(oldEntityName),
				"old_bank_name":           stringOrEmpty(oldBankName),
				"old_core_limit_type":     stringOrEmpty(oldCoreLimitType),
				"old_limit_type":          stringOrEmpty(oldLimitType),
				"old_limit_sub_type":      stringOrEmpty(oldLimitSubType),
				"old_sanction_date":       timeOrEmpty(oldSanctionDate),
				"old_effective_date":      timeOrEmpty(oldEffectiveDate),
				"old_currency_code":       stringOrEmpty(oldCurrencyCode),
				"old_sanctioned_amount":   floatOrZero(oldSanctionedAmount),
				"old_fungibility_type":    stringOrEmpty(oldFungibilityType),
				"old_fungibility_pct":     floatOrZero(oldFungibilityPct),
				"old_security_type":       stringOrEmpty(oldSecurityType),
				"old_remarks":             stringOrEmpty(oldRemarks),
				"old_initial_utilization": floatOrZero(oldInitialUtilization),

				"action_type":       stringOrEmpty(actionType),
				"processing_status": stringOrEmpty(procStatus),
				"requested_by":      stringOrEmpty(requestedBy),
				"requested_at":      auditTimeOrEmpty(requestedAt),
				"checker_by":        stringOrEmpty(checkerBy),
				"checker_at":        auditTimeOrEmpty(checkerAt),
				"checker_comment":   stringOrEmpty(checkerComment),
				"reason":            stringOrEmpty(reason),

				"pending_approval": falseIfNilBool(pendingApproval),
				"instance_id":      stringOrEmpty(instanceID),
			}

			results = append(results, item)
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

// GetApprovedBankLimits returns only APPROVED limits
func GetApprovedBankLimits(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		entityNames := api.GetEntityNamesFromCtx(ctx)

		query := `
			WITH approved_utilizations AS (
				SELECT 
					u.limit_id,
					COALESCE(SUM(u.utilized_amount), 0) AS total_approved_utilization
				FROM cimplrcorpsaas.bank_limit_utilization u
				INNER JOIN LATERAL (
					SELECT processing_status
					FROM cimplrcorpsaas.auditactionbanklimitutilization
					WHERE utilization_id = u.utilization_id
					  AND action_type IN ('CREATE','EDIT','DELETE')
					ORDER BY requested_at DESC
					LIMIT 1
				) au ON au.processing_status = 'APPROVED'
				WHERE COALESCE(u.is_deleted, false) = false
				GROUP BY u.limit_id
			)
			SELECT 
				l.limit_id, l.entity_name, l.bank_name, l.core_limit_type, l.limit_type, l.limit_sub_type,
				l.sanction_date, l.effective_date, l.currency_code, l.sanctioned_amount,
				l.fungibility_type, l.fungibility_pct, l.security_type, l.remarks, l.initial_utilization,
				COALESCE(au.total_approved_utilization, 0) AS total_approved_utilization,
				COALESCE(l.initial_utilization, 0) + COALESCE(au.total_approved_utilization, 0) AS total_utilized,
				l.sanctioned_amount - (COALESCE(l.initial_utilization, 0) + COALESCE(au.total_approved_utilization, 0)) AS headroom,
				CASE 
					WHEN (COALESCE(l.initial_utilization, 0) + COALESCE(au.total_approved_utilization, 0)) > l.sanctioned_amount 
					THEN (COALESCE(l.initial_utilization, 0) + COALESCE(au.total_approved_utilization, 0)) - l.sanctioned_amount 
					ELSE 0 
				END AS over_utilization
			FROM cimplrcorpsaas.bank_limit l
			LEFT JOIN approved_utilizations au ON au.limit_id = l.limit_id
			INNER JOIN LATERAL (
				SELECT processing_status, requested_at, checker_at
				FROM cimplrcorpsaas.auditactionbanklimit
				WHERE limit_id = l.limit_id
				ORDER BY requested_at DESC
				LIMIT 1
		) a ON a.processing_status = 'APPROVED'
		WHERE COALESCE(l.is_deleted, false) = false
			AND l.entity_name = ANY($1)
		ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC`
		rows, err := pgxPool.Query(ctx, query, entityNames)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			var limitID, entityName, bankName, coreLimitType, currencyCode, fungibilityType, securityType string
			var limitType, limitSubType, remarks *string
			var sanctionDate, effectiveDate *time.Time
			var sanctionedAmount, totalApprovedUtilization, totalUtilized, headroom, overUtilization float64
			var fungibilityPct, initialUtilization *float64

			err := rows.Scan(
				&limitID, &entityName, &bankName, &coreLimitType, &limitType, &limitSubType,
				&sanctionDate, &effectiveDate, &currencyCode, &sanctionedAmount,
				&fungibilityType, &fungibilityPct, &securityType, &remarks, &initialUtilization,
				&totalApprovedUtilization, &totalUtilized, &headroom, &overUtilization,
			)
			if err != nil {
				continue
			}

			item := map[string]interface{}{
				"limit_id":                   limitID,
				"entity_name":                entityName,
				"bank_name":                  bankName,
				"core_limit_type":            coreLimitType,
				"limit_type":                 stringOrEmpty(limitType),
				"limit_sub_type":             stringOrEmpty(limitSubType),
				"sanction_date":              timeOrEmpty(sanctionDate),
				"effective_date":             timeOrEmpty(effectiveDate),
				"currency_code":              currencyCode,
				"sanctioned_amount":          sanctionedAmount,
				"fungibility_type":           fungibilityType,
				"fungibility_pct":            floatOrZero(fungibilityPct),
				"security_type":              securityType,
				"remarks":                    stringOrEmpty(remarks),
				"initial_utilization":        floatOrZero(initialUtilization),
				"total_approved_utilization": totalApprovedUtilization,
				"total_utilized":             totalUtilized,
				"headroom":                   headroom,
				"over_utilization":           overUtilization,
			}

			results = append(results, item)
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

// BulkApproveBankLimits approves pending audit actions for given limit_ids
func BulkApproveBankLimits(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			LimitIDs []string `json:"limit_ids"`
			Comment  string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.LimitIDs) == 0 {
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

		// Get latest audit actions for each limit
		sel := `SELECT DISTINCT ON (limit_id) action_id, limit_id, action_type
			FROM cimplrcorpsaas.auditactionbanklimit
			WHERE limit_id = ANY($1) AND action_type IN ('CREATE','EDIT','DELETE')
			ORDER BY limit_id, GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST, action_id DESC`

		rows, err := pgxPool.Query(ctx, sel, req.LimitIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		deleteIDs := make([]string, 0)
		found := map[string]bool{}
		actionToLimitMap := make(map[string]string)

		for rows.Next() {
			var actionID, limitID, actionType string
			if err := rows.Scan(&actionID, &limitID, &actionType); err != nil {
				continue
			}
			found[limitID] = true
			actionIDs = append(actionIDs, actionID)
			actionToLimitMap[actionID] = limitID
			if actionType == "DELETE" {
				deleteIDs = append(deleteIDs, limitID)
			}
		}

		missing := []string{}
		for _, id := range req.LimitIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		for _, limitID := range req.LimitIDs {
			policyRow, perr := loadBankLimitRow(ctx, pgxPool, limitID)
			if perr != nil {
				api.RespondWithResult(w, false, errFailedToLoadBankLimitForPolicyCheck+perr.Error())
				return
			}
			if ok, msg := runtime.EnforceInline(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreApprove,
				ModuleCode:          common.ModuleCash,
				SubModule:           "BANK_LIMIT",
				ActorUserID:         req.UserID,
				HandlerName:         "BulkApproveBankLimits",
				APIPath:             "/cash/limit/approve",
				DefaultBlockMessage: "Bank limit approve blocked by policy",
				Fields:              buildBankLimitPolicyFields(policyRow),
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

		// ── Approval-matrix engine: attempt engine-side approve first. A blocked
		// Reason (e.g. "not your turn in approval sequence") must exclude the
		// record from the legacy fallback entirely — only a genuine "no matrix
		// applies" case may fall through to the direct SQL stamp below.
		engineActed := make(map[string]bool)
		blocked := make(map[string]string)
		for _, limitID := range req.LimitIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
				ModuleCode: "CASH", RecordID: limitID,
				UserID: req.UserID, UserEmail: checkerBy,
				Action: approvalengine.ActionApproved, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[BankLimit] ActOnPendingOrDiagnose approve failed for %s: %v", limitID, actionErr.Error())
				blocked[limitID] = actionErr.Error()
				continue
			}
			if actionRes.Acted {
				engineActed[limitID] = true
			} else if actionRes.CancelledStale {
				api.LogInfo("[BankLimit] cancelled stale approval instance for limit %s", limitID)
			} else if actionRes.Reason != "" {
				api.LogInfo("[BankLimit] engine blocked limit %s: %s", limitID, actionRes.Reason)
				blocked[limitID] = actionRes.Reason
			}
		}

		var legacyActionIDs []string
		var legacyDeleteIDs []string
		for _, id := range actionIDs {
			limID := actionToLimitMap[id]
			if !engineActed[limID] && blocked[limID] == "" {
				legacyActionIDs = append(legacyActionIDs, id)
			}
		}
		for _, id := range deleteIDs {
			if !engineActed[id] && blocked[id] == "" {
				legacyDeleteIDs = append(legacyDeleteIDs, id)
			}
		}

		// Update audit status to APPROVED
		if len(legacyActionIDs) > 0 {
			upd := `UPDATE cimplrcorpsaas.auditactionbanklimit 
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id = ANY($4)`

			if _, err := tx.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), nullifyEmpty(api.ClientIPFromRequest(r)), legacyActionIDs); err != nil {
				api.RespondWithResult(w, false, "failed to approve: "+err.Error())
				return
			}
		}

		// Execute soft delete for DELETE action types
		deleted := []string{}
		if len(legacyDeleteIDs) > 0 {
			delUpd := `UPDATE cimplrcorpsaas.bank_limit SET is_deleted=true WHERE limit_id = ANY($1) RETURNING limit_id`
			drows, derr := tx.Query(ctx, delUpd, legacyDeleteIDs)
			if derr == nil {
				defer drows.Close()
				for drows.Next() {
					var id string
					if err := drows.Scan(&id); err != nil {
						logger.LogError("[BulkApproveBankLimits] deleted limit_id scan failed: %v", err)
					}
					deleted = append(deleted, id)
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit approve: "+err.Error())
			return
		}
		committed = true

		dmsevent.Fire(pgxPool, "CASH", "BANK_LIMIT", "POST_APPROVE", req.LimitIDs, checkerBy)
		if len(deleted) > 0 {
			dmsevent.Fire(pgxPool, "CASH", "BANK_LIMIT", "POST_DELETE", deleted, checkerBy)
		}

		resp := map[string]interface{}{
			"approved_count": len(actionIDs) - len(blocked),
			"deleted":        deleted,
		}
		if len(blocked) > 0 {
			resp["blocked"] = blocked
		}
		api.RespondWithPayload(w, true, "", resp)
		// Notify: limits approved with FULL record data
		// Pass req.UserID (not display name) so dispatcher resolves actor entity correctly
		capturedUser := req.UserID
		capturedIDs := req.LimitIDs
		capturedDeleted := deleted
		payload := BuildLimitNotifPayload(context.Background(), pgxPool, capturedIDs, "APPROVE", capturedUser)
		payloadMap := payload.ToMap()
		payloadMap["DeletedIDs"] = capturedDeleted
		go catalog.TriggerNotification(
			context.Background(), pgxPool,
			"/cash/limit/approve",
			fmt.Sprintf("LIMIT_APPROVE/%s/%d", capturedUser, time.Now().UnixMilli()),
			payloadMap,
		)
	}
}

// BulkRejectBankLimits rejects pending audit actions
func BulkRejectBankLimits(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			LimitIDs []string `json:"limit_ids"`
			Comment  string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.LimitIDs) == 0 {
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

		sel := `SELECT DISTINCT ON (limit_id) action_id, limit_id
			FROM cimplrcorpsaas.auditactionbanklimit
			WHERE limit_id = ANY($1) AND action_type IN ('CREATE','EDIT','DELETE')
			ORDER BY limit_id, GREATEST(COALESCE(checker_at, requested_at), requested_at) DESC NULLS LAST, action_id DESC`

		rows, err := pgxPool.Query(ctx, sel, req.LimitIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		found := map[string]bool{}
		actionToLimitMap := make(map[string]string)

		for rows.Next() {
			var actionID, limitID string
			if err := rows.Scan(&actionID, &limitID); err != nil {
				continue
			}
			found[limitID] = true
			actionIDs = append(actionIDs, actionID)
			actionToLimitMap[actionID] = limitID
		}

		missing := []string{}
		for _, id := range req.LimitIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		for _, limitID := range req.LimitIDs {
			policyRow, perr := loadBankLimitRow(ctx, pgxPool, limitID)
			if perr != nil {
				api.RespondWithResult(w, false, errFailedToLoadBankLimitForPolicyCheck+perr.Error())
				return
			}
			if ok, msg := runtime.EnforceInline(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreReject,
				ModuleCode:          common.ModuleCash,
				SubModule:           "BANK_LIMIT",
				ActorUserID:         req.UserID,
				HandlerName:         "BulkRejectBankLimits",
				APIPath:             "/cash/limit/reject",
				DefaultBlockMessage: "Bank limit reject blocked by policy",
				Fields:              buildBankLimitPolicyFields(policyRow),
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

		// ── Approval-matrix engine: attempt engine-side reject first. A blocked
		// Reason must exclude the record from the legacy fallback entirely.
		engineActed := make(map[string]bool)
		blocked := make(map[string]string)
		for _, limitID := range req.LimitIDs {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{
				ModuleCode: "CASH", RecordID: limitID,
				UserID: req.UserID, UserEmail: checkerBy,
				Action: approvalengine.ActionRejected, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[BankLimit] ActOnPendingOrDiagnose reject failed for %s: %v", limitID, actionErr.Error())
				blocked[limitID] = actionErr.Error()
				continue
			}
			if actionRes.Acted {
				engineActed[limitID] = true
			} else if actionRes.CancelledStale {
				api.LogInfo("[BankLimit] cancelled stale approval instance for limit %s", limitID)
			} else if actionRes.Reason != "" {
				api.LogInfo("[BankLimit] engine blocked limit %s: %s", limitID, actionRes.Reason)
				blocked[limitID] = actionRes.Reason
			}
		}

		var legacyActionIDs []string
		for _, id := range actionIDs {
			limID := actionToLimitMap[id]
			if !engineActed[limID] && blocked[limID] == "" {
				legacyActionIDs = append(legacyActionIDs, id)
			}
		}

		if len(legacyActionIDs) > 0 {
			upd := `UPDATE cimplrcorpsaas.auditactionbanklimit 
				SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3
				WHERE action_id = ANY($4)`

			if _, err := tx.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), nullifyEmpty(api.ClientIPFromRequest(r)), legacyActionIDs); err != nil {
				api.RespondWithResult(w, false, "failed to reject: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit reject: "+err.Error())
			return
		}
		committed = true

		dmsevent.Fire(pgxPool, "CASH", "BANK_LIMIT", "POST_REJECT", req.LimitIDs, checkerBy)

		respReject := map[string]interface{}{
			"rejected_count": len(actionIDs) - len(blocked),
		}
		if len(blocked) > 0 {
			respReject["blocked"] = blocked
		}
		api.RespondWithPayload(w, true, "", respReject)
		// Notify: limits rejected with FULL record data
		// Pass req.UserID (not display name) so dispatcher resolves actor entity correctly
		capturedUser := req.UserID
		capturedIDs := req.LimitIDs
		payload := BuildLimitNotifPayload(context.Background(), pgxPool, capturedIDs, "REJECT", capturedUser)
		go catalog.TriggerNotification(
			context.Background(), pgxPool,
			"/cash/limit/reject",
			fmt.Sprintf("LIMIT_REJECT/%s/%d", capturedUser, time.Now().UnixMilli()),
			payload.ToMap(),
		)
	}
}

// Helper functions
func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func floatOrZero(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}

func falseIfNilBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func timeOrEmpty(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(constants.DateTimeFormat)
}

func auditTimeOrEmpty(t *time.Time) string {
	if t == nil {
		return ""
	}
	return api.FormatAuditTimestampIST(*t)
}

// submitBankLimitParams bundles the fields needed to submit a bank-limit
// record to the approval engine.
type submitBankLimitParams struct {
	LimitID           string
	EntityName        string
	SubmittedByUserID string
	ActorEmail        string
	TxType            string
	Amount            float64
	MatrixID          string
}

func submitBankLimitForApproval(pool *pgxpool.Pool, p submitBankLimitParams) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := approvalengine.CreateInstance(ctx, pool, approvalengine.InstanceRequest{
		ModuleCode:          "CASH",
		EntityCode:          p.EntityName, // limit has EntityName directly
		TransactionType:     p.TxType,
		RecordID:            p.LimitID,
		RecordTable:         "cimplrcorpsaas.bank_limit",
		AuditTable:          "cimplrcorpsaas.auditactionbanklimit",
		AuditIDColumn:       "limit_id",
		ActionType:          strings.TrimPrefix(p.TxType, "BANK_LIMIT_"),
		Amount:              p.Amount,
		SubmittedBy:         p.SubmittedByUserID,
		SubmittedByEmail:    p.ActorEmail,
		MatrixID:            p.MatrixID,
		RequirePinnedMatrix: true,
		AutoApplyIfUnpinned: false,
	}); err != nil {
		api.LogError("[BankLimit] approvalengine.CreateInstance failed for %s (%s): %v", p.LimitID, p.TxType, err.Error())
	}
}
