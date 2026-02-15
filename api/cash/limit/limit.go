package limit

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
		if !api.IsEntityAllowed(ctx, req.EntityName) {
			api.RespondWithResult(w, false, "unauthorized entity")
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
			api.RespondWithResult(w, false, "failed to insert limit: "+err.Error())
			return
		}

		auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimit (
			limit_id, action_type, processing_status, reason, requested_by, requested_at
		) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,now())`

		if _, err := tx.Exec(ctx, auditQ, limitID, nullifyEmpty(req.Reason), requestedBy); err != nil {
			api.RespondWithResult(w, false, "failed to create audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit: "+err.Error())
			return
		}

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

		for i, lim := range req.Limits {
			result := map[string]interface{}{"index": i}

			// Validate entity
			if !api.IsEntityAllowed(ctx, lim.EntityName) {
				result["success"] = false
				result["error"] = "unauthorized entity: " + lim.EntityName
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
				api.RespondWithResult(w, false, "invalid fungibility_type. Allowed: Inter-Core, Intra-Core, None")
				return
			}
			securityType := strings.ToUpper(strings.TrimSpace(lim.SecurityType))
			if securityType != "SECURED" && securityType != "UNSECURED" {
				result["success"] = false
				result["error"] = "invalid security_type"
				results = append(results, result)
				continue
			}

			// Individual transaction per limit
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

			auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimit (
				limit_id, action_type, processing_status, reason, requested_by, requested_at
			) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,now())`

			if _, err := tx.Exec(ctx, auditQ, limitID, nullifyEmpty(lim.Reason), requestedBy); err != nil {
				tx.Rollback(ctx)
				result["success"] = false
				result["error"] = "failed to create audit"
				results = append(results, result)
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				result["success"] = false
				result["error"] = constants.ErrTxCommitFailed
				results = append(results, result)
				continue
			}

			result["success"] = true
			result["limit_id"] = limitID
			results = append(results, result)
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
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

		auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimit (
			limit_id, action_type, processing_status, reason, requested_by, requested_at
		) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())`

		if _, err := tx.Exec(ctx, auditQ, req.LimitID, nullifyEmpty(req.Reason), requestedBy); err != nil {
			api.RespondWithResult(w, false, "failed to create audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit: "+err.Error())
			return
		}

		api.RespondWithResult(w, true, req.LimitID)
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

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				result["success"] = false
				result["error"] = constants.ErrFailedToBeginTransaction
				results = append(results, result)
				continue
			}

			auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimit (
				limit_id, action_type, processing_status, reason, requested_by, requested_at
			) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`

			if _, err := tx.Exec(ctx, auditQ, limitID, nullifyEmpty(req.Reason), requestedBy); err != nil {
				tx.Rollback(ctx)
				result["success"] = false
				result["error"] = "failed to create delete audit"
				results = append(results, result)
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				result["success"] = false
				result["error"] = constants.ErrTxCommitFailed
				results = append(results, result)
				continue
			}

			result["success"] = true
			results = append(results, result)
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
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
				a.action_type, a.processing_status, a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM cimplrcorpsaas.bank_limit l
			LEFT JOIN LATERAL (
				SELECT action_type, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment, reason
				FROM cimplrcorpsaas.auditactionbanklimit
				WHERE limit_id = l.limit_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE COALESCE(l.is_deleted, false) = false
				AND l.entity_name = ANY($1)
			ORDER BY l.sanction_date DESC`

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

			err := rows.Scan(
				&limitID, &entityName, &bankName, &coreLimitType, &limitType, &limitSubType,
				&sanctionDate, &effectiveDate, &currencyCode, &sanctionedAmount,
				&fungibilityType, &fungibilityPct, &securityType, &remarks, &initialUtilization,
				&oldEntityName, &oldBankName, &oldCoreLimitType, &oldLimitType, &oldLimitSubType,
				&oldSanctionDate, &oldEffectiveDate, &oldCurrencyCode, &oldSanctionedAmount,
				&oldFungibilityType, &oldFungibilityPct, &oldSecurityType, &oldRemarks, &oldInitialUtilization,
				&actionType, &procStatus, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason,
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
				"requested_at":      timeOrEmpty(requestedAt),
				"checker_by":        stringOrEmpty(checkerBy),
				"checker_at":        timeOrEmpty(checkerAt),
				"checker_comment":   stringOrEmpty(checkerComment),
				"reason":            stringOrEmpty(reason),
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
			SELECT 
				l.limit_id, l.entity_name, l.bank_name, l.core_limit_type, l.limit_type, l.limit_sub_type,
				l.sanction_date, l.effective_date, l.currency_code, l.sanctioned_amount,
				l.fungibility_type, l.fungibility_pct, l.security_type, l.remarks, l.initial_utilization
			FROM cimplrcorpsaas.bank_limit l
			INNER JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.auditactionbanklimit
				WHERE limit_id = l.limit_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON a.processing_status = 'APPROVED'
			WHERE COALESCE(l.is_deleted, false) = false
				AND l.entity_name = ANY($1)
			ORDER BY l.sanction_date DESC`

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

			err := rows.Scan(
				&limitID, &entityName, &bankName, &coreLimitType, &limitType, &limitSubType,
				&sanctionDate, &effectiveDate, &currencyCode, &sanctionedAmount,
				&fungibilityType, &fungibilityPct, &securityType, &remarks, &initialUtilization,
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
			WHERE limit_id = ANY($1) 
			ORDER BY limit_id, requested_at DESC`

		rows, err := pgxPool.Query(ctx, sel, req.LimitIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		deleteIDs := make([]string, 0)
		found := map[string]bool{}

		for rows.Next() {
			var actionID, limitID, actionType string
			if err := rows.Scan(&actionID, &limitID, &actionType); err != nil {
				continue
			}
			found[limitID] = true
			actionIDs = append(actionIDs, actionID)
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

		// Update audit status to APPROVED
		upd := `UPDATE cimplrcorpsaas.auditactionbanklimit 
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 
			WHERE action_id = ANY($3)`

		if _, err := pgxPool.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), actionIDs); err != nil {
			api.RespondWithResult(w, false, "failed to approve: "+err.Error())
			return
		}

		// Execute soft delete for DELETE action types
		deleted := []string{}
		if len(deleteIDs) > 0 {
			delUpd := `UPDATE cimplrcorpsaas.bank_limit SET is_deleted=true WHERE limit_id = ANY($1) RETURNING limit_id`
			drows, derr := pgxPool.Query(ctx, delUpd, deleteIDs)
			if derr == nil {
				defer drows.Close()
				for drows.Next() {
					var id string
					drows.Scan(&id)
					deleted = append(deleted, id)
				}
			}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"approved_count": len(actionIDs),
			"deleted":        deleted,
		})
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
			WHERE limit_id = ANY($1) 
			ORDER BY limit_id, requested_at DESC`

		rows, err := pgxPool.Query(ctx, sel, req.LimitIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		found := map[string]bool{}

		for rows.Next() {
			var actionID, limitID string
			if err := rows.Scan(&actionID, &limitID); err != nil {
				continue
			}
			found[limitID] = true
			actionIDs = append(actionIDs, actionID)
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

		upd := `UPDATE cimplrcorpsaas.auditactionbanklimit 
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 
			WHERE action_id = ANY($3)`

		if _, err := pgxPool.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), actionIDs); err != nil {
			api.RespondWithResult(w, false, "failed to reject: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rejected_count": len(actionIDs),
		})
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

func timeOrEmpty(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(constants.DateTimeFormat)
}
