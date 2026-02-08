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
		if coreLimitType != "FUND BASED" && coreLimitType != "NON FUND BASED" && coreLimitType != "TERM LOANS" {
			api.RespondWithResult(w, false, "invalid core_limit_type. Allowed: Fund Based, Non Fund Based, Term Loans")
			return
		}

		// Validate fungibility_type
		fungibilityType := strings.ToUpper(strings.TrimSpace(req.FungibilityType))
		if fungibilityType != "FUNGIBLE" && fungibilityType != "NON-FUNGIBLE" {
			api.RespondWithResult(w, false, "invalid fungibility_type. Allowed: Fungible, Non-Fungible")
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
		if coreLimitType != "FUND BASED" && coreLimitType != "NON FUND BASED" && coreLimitType != "TERM LOANS" {
			result["success"] = false
			result["error"] = "invalid core_limit_type"
			results = append(results, result)
			continue
		}

		fungibilityType := strings.ToUpper(strings.TrimSpace(lim.FungibilityType))
			if fungibilityType != "FUNGIBLE" && fungibilityType != "NON-FUNGIBLE" {
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

			// Individual transaction per limit
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				result["success"] = false
				result["error"] = "failed to begin transaction"
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
				result["error"] = "failed to commit"
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
			UserID             string   `json:"user_id"`
			LimitID            string   `json:"limit_id"`
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

		if req.UserID == "" || req.LimitID == "" {
			api.RespondWithResult(w, false, "user_id and limit_id required")
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

		// Save old values before updating
		upd := `UPDATE cimplrcorpsaas.bank_limit SET
			old_entity_name = entity_name,
			old_bank_name = bank_name,
			old_core_limit_type = core_limit_type,
			old_limit_type = limit_type,
			old_limit_sub_type = limit_sub_type,
			old_sanction_date = sanction_date,
			old_effective_date = effective_date,
			old_currency_code = currency_code,
			old_sanctioned_amount = sanctioned_amount,
			old_fungibility_type = fungibility_type,
			old_fungibility_pct = fungibility_pct,
			old_security_type = security_type,
			old_remarks = remarks,
			old_initial_utilization = initial_utilization,
			entity_name = $2,
			bank_name = $3,
			core_limit_type = $4,
			limit_type = $5,
			limit_sub_type = $6,
			sanction_date = $7,
			effective_date = $8,
			currency_code = $9,
			sanctioned_amount = $10,
			fungibility_type = $11,
			fungibility_pct = $12,
			security_type = $13,
			remarks = $14,
			initial_utilization = $15
		WHERE limit_id = $1`

		_, err = tx.Exec(ctx, upd,
			req.LimitID,
			req.EntityName, req.BankName,
			strings.ToUpper(strings.TrimSpace(req.CoreLimitType)),
			nullifyEmpty(req.LimitType), nullifyEmpty(req.LimitSubType),
			nullifyEmpty(req.SanctionDate), nullifyEmpty(req.EffectiveDate),
			strings.ToUpper(req.CurrencyCode), req.SanctionedAmount,
			strings.ToUpper(strings.TrimSpace(req.FungibilityType)),
			nullifyFloat(req.FungibilityPct),
			strings.ToUpper(strings.TrimSpace(req.SecurityType)),
			nullifyEmpty(req.Remarks), nullifyFloat(req.InitialUtilization),
		)

		if err != nil {
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
				result["error"] = "failed to begin transaction"
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
				result["error"] = "failed to commit"
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
			api.RespondWithResult(w, false, "query failed: "+err.Error())
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
				
				"action_type":         stringOrEmpty(actionType),
				"processing_status":   stringOrEmpty(procStatus),
				"requested_by":        stringOrEmpty(requestedBy),
				"requested_at":        timeOrEmpty(requestedAt),
				"checker_by":          stringOrEmpty(checkerBy),
				"checker_at":          timeOrEmpty(checkerAt),
				"checker_comment":     stringOrEmpty(checkerComment),
				"reason":              stringOrEmpty(reason),
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
			api.RespondWithResult(w, false, "query failed: "+err.Error())
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
