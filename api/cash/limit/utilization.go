package limit

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// CreateUtilization creates a single utilization entry with PENDING_APPROVAL audit
func CreateUtilization(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID          string  `json:"user_id"`
			LimitID         string  `json:"limit_id"`
			UtilizationDate string  `json:"utilization_date"`
			CurrencyCode    string  `json:"currency_code"`
			UtilizedAmount  float64 `json:"utilized_amount"`
			Remarks         string  `json:"remarks"`
			ReferenceDoc    string  `json:"reference_doc"`
			EntryMode       string  `json:"entry_mode"`
			Reason          string  `json:"reason"`
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

		entryMode := strings.ToUpper(strings.TrimSpace(req.EntryMode))
		if entryMode == "" {
			entryMode = "MANUAL"
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "failed to begin transaction: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		ins := `INSERT INTO cimplrcorpsaas.bank_limit_utilization (
			limit_id, utilization_date, currency_code, utilized_amount, 
			remarks, reference_doc, entry_mode, status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,'DRAFT') RETURNING utilization_id`

		var utilizationID string
		err = tx.QueryRow(ctx, ins,
			req.LimitID, req.UtilizationDate, strings.ToUpper(req.CurrencyCode),
			req.UtilizedAmount, nullifyEmpty(req.Remarks), nullifyEmpty(req.ReferenceDoc),
			entryMode,
		).Scan(&utilizationID)

		if err != nil {
			api.RespondWithResult(w, false, "failed to insert utilization: "+err.Error())
			return
		}

		auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimitutilization (
			utilization_id, limit_id, action_type, processing_status, reason, requested_by, requested_at
		) VALUES ($1,$2,'CREATE','PENDING_APPROVAL',$3,$4,now())`

		if _, err := tx.Exec(ctx, auditQ, utilizationID, req.LimitID, nullifyEmpty(req.Reason), requestedBy); err != nil {
			api.RespondWithResult(w, false, "failed to create audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit: "+err.Error())
			return
		}

		api.RespondWithResult(w, true, utilizationID)
	}
}

// BulkCreateUtilization creates multiple utilization entries
func BulkCreateUtilization(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		type UtilRequest struct {
			LimitID         string  `json:"limit_id"`
			UtilizationDate string  `json:"utilization_date"`
			CurrencyCode    string  `json:"currency_code"`
			UtilizedAmount  float64 `json:"utilized_amount"`
			Remarks         string  `json:"remarks"`
			ReferenceDoc    string  `json:"reference_doc"`
			EntryMode       string  `json:"entry_mode"`
			Reason          string  `json:"reason"`
		}

		var req struct {
			UserID       string        `json:"user_id"`
			Utilizations []UtilRequest `json:"utilizations"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.Utilizations) == 0 {
			api.RespondWithResult(w, false, "user_id and utilizations array required")
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

		results := make([]map[string]interface{}, 0, len(req.Utilizations))

		for i, util := range req.Utilizations {
			result := map[string]interface{}{"index": i}

			entryMode := strings.ToUpper(strings.TrimSpace(util.EntryMode))
			if entryMode == "" {
				entryMode = "MANUAL"
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				result["success"] = false
				result["error"] = constants.ErrFailedToBeginTransaction
				results = append(results, result)
				continue
			}

			ins := `INSERT INTO cimplrcorpsaas.bank_limit_utilization (
				limit_id, utilization_date, currency_code, utilized_amount,
				remarks, reference_doc, entry_mode, status
			) VALUES ($1,$2,$3,$4,$5,$6,$7,'DRAFT') RETURNING utilization_id`

			var utilizationID string
			err = tx.QueryRow(ctx, ins,
				util.LimitID, util.UtilizationDate, strings.ToUpper(util.CurrencyCode),
				util.UtilizedAmount, nullifyEmpty(util.Remarks), nullifyEmpty(util.ReferenceDoc),
				entryMode,
			).Scan(&utilizationID)

			if err != nil {
				tx.Rollback(ctx)
				result["success"] = false
				result["error"] = "failed to insert: " + err.Error()
				results = append(results, result)
				continue
			}

			auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimitutilization (
				utilization_id, limit_id, action_type, processing_status, reason, requested_by, requested_at
			) VALUES ($1,$2,'CREATE','PENDING_APPROVAL',$3,$4,now())`

			if _, err := tx.Exec(ctx, auditQ, utilizationID, util.LimitID, nullifyEmpty(util.Reason), requestedBy); err != nil {
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
			result["utilization_id"] = utilizationID
			results = append(results, result)
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// UpdateUtilization updates an existing utilization with PENDING_EDIT_APPROVAL audit
func UpdateUtilization(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID        string                 `json:"user_id"`
			UtilizationID string                 `json:"utilization_id"`
			Fields        map[string]interface{} `json:"fields"`
			Reason        string                 `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || req.UtilizationID == "" {
			api.RespondWithResult(w, false, "user_id and utilization_id required")
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

		sel := `SELECT limit_id, utilization_date, currency_code, utilized_amount, remarks, reference_doc FROM cimplrcorpsaas.bank_limit_utilization WHERE utilization_id = $1 FOR UPDATE`
		var curLimitID *string
		var curUtilDate *time.Time
		var curCurrency *string
		var curUtilizedAmount float64
		var curRemarks *string
		var curReferenceDoc *string

		if err := tx.QueryRow(ctx, sel, req.UtilizationID).Scan(&curLimitID, &curUtilDate, &curCurrency, &curUtilizedAmount, &curRemarks, &curReferenceDoc); err != nil {
			api.RespondWithResult(w, false, "failed to fetch current utilization: "+err.Error())
			return
		}

		oldSets := []string{}
		newSets := []string{}
		args := []interface{}{}
		pos := 1

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
			case "limit_id":
				if s, ok := v.(string); ok {
					addStr("limit_id", s)
				}
			case "utilization_date":
				if s, ok := v.(string); ok {
					addStr("utilization_date", s)
				}
			case "currency_code":
				if s, ok := v.(string); ok {
					addStr("currency_code", strings.ToUpper(s))
				}
			case "utilized_amount":
				switch t := v.(type) {
				case float64:
					addFloat("utilized_amount", t)
				case int:
					addFloat("utilized_amount", float64(t))
				}
			case "remarks":
				if s, ok := v.(string); ok {
					addStr("remarks", s)
				}
			case "reference_doc":
				if s, ok := v.(string); ok {
					addStr("reference_doc", s)
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

		q := "UPDATE cimplrcorpsaas.bank_limit_utilization SET " + setClause + " WHERE utilization_id = $" + fmt.Sprint(pos)
		args = append(args, req.UtilizationID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithResult(w, false, "failed to update: "+err.Error())
			return
		}

		auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimitutilization (
			utilization_id, limit_id, action_type, processing_status, reason, requested_by, requested_at
		) VALUES ($1,$2,'EDIT','PENDING_EDIT_APPROVAL',$3,$4,now())`

		// pick limit id argument: prefer fields if supplied, else current
		limitForAudit := ""
		if v, ok := req.Fields["limit_id"]; ok {
			if s, sok := v.(string); sok {
				limitForAudit = s
			}
		}
		if limitForAudit == "" && curLimitID != nil {
			limitForAudit = *curLimitID
		}

		if _, err := tx.Exec(ctx, auditQ, req.UtilizationID, limitForAudit, nullifyEmpty(req.Reason), requestedBy); err != nil {
			api.RespondWithResult(w, false, "failed to create audit: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit: "+err.Error())
			return
		}

		api.RespondWithResult(w, true, req.UtilizationID)
	}
}

// DeleteUtilization creates PENDING_DELETE_APPROVAL audit
func DeleteUtilization(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID         string   `json:"user_id"`
			UtilizationIDs []string `json:"utilization_ids"`
			Reason         string   `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.UtilizationIDs) == 0 {
			api.RespondWithResult(w, false, "user_id and utilization_ids required")
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

		results := make([]map[string]interface{}, 0, len(req.UtilizationIDs))

		for i, utilizationID := range req.UtilizationIDs {
			result := map[string]interface{}{"index": i, "utilization_id": utilizationID}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				result["success"] = false
				result["error"] = constants.ErrFailedToBeginTransaction
				results = append(results, result)
				continue
			}

			// Get limit_id
			var limitID string
			getLimitQ := `SELECT limit_id FROM cimplrcorpsaas.bank_limit_utilization WHERE utilization_id=$1`
			if err := tx.QueryRow(ctx, getLimitQ, utilizationID).Scan(&limitID); err != nil {
				tx.Rollback(ctx)
				result["success"] = false
				result["error"] = "utilization not found"
				results = append(results, result)
				continue
			}

			auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimitutilization (
				utilization_id, limit_id, action_type, processing_status, reason, requested_by, requested_at
			) VALUES ($1,$2,'DELETE','PENDING_DELETE_APPROVAL',$3,$4,now())`

			if _, err := tx.Exec(ctx, auditQ, utilizationID, limitID, nullifyEmpty(req.Reason), requestedBy); err != nil {
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

// GetAllUtilizations returns all utilizations with latest audit info
func GetAllUtilizations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		query := `
			SELECT 
				u.utilization_id, u.limit_id, u.utilization_date, u.currency_code, u.utilized_amount,
				u.remarks, u.reference_doc, u.entry_mode, u.status,
				u.old_utilization_date, u.old_currency_code, u.old_utilized_amount, u.old_remarks, u.old_reference_doc,
				a.action_type, a.processing_status, a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason,

				-- limit fields
				l.limit_id, l.entity_name, l.bank_name, l.core_limit_type, l.limit_type, l.limit_sub_type,
				l.sanction_date, l.effective_date, l.currency_code as limit_currency_code, l.sanctioned_amount,
				l.fungibility_type, l.fungibility_pct, l.security_type, l.remarks as limit_remarks, l.initial_utilization,
				l.old_entity_name, l.old_bank_name, l.old_core_limit_type, l.old_limit_type, l.old_limit_sub_type,
				l.old_sanction_date, l.old_effective_date, l.old_currency_code, l.old_sanctioned_amount,
				l.old_fungibility_type, l.old_fungibility_pct, l.old_security_type, l.old_remarks, l.old_initial_utilization,
				la.action_type as limit_action_type, la.processing_status as limit_processing_status, la.requested_by as limit_requested_by, la.requested_at as limit_requested_at, la.checker_by as limit_checker_by, la.checker_at as limit_checker_at, la.checker_comment as limit_checker_comment, la.reason as limit_reason

			FROM cimplrcorpsaas.bank_limit_utilization u
			LEFT JOIN LATERAL (
				SELECT action_type, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment, reason
				FROM cimplrcorpsaas.auditactionbanklimitutilization
				WHERE utilization_id = u.utilization_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			LEFT JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
			LEFT JOIN LATERAL (
				SELECT action_type, processing_status, requested_by, requested_at, checker_by, checker_at, checker_comment, reason
				FROM cimplrcorpsaas.auditactionbanklimit
				WHERE limit_id = l.limit_id
				ORDER BY requested_at DESC
				LIMIT 1
			) la ON TRUE
			WHERE COALESCE(u.is_deleted, false) = false
			ORDER BY u.utilization_date DESC`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			var utilizationID, limitID, currencyCode, entryMode, status string
			var utilizationDate *time.Time
			var utilizedAmount float64
			var remarks, referenceDoc *string

			// Old values (utilization)
			var oldUtilizationDate *time.Time
			var oldCurrencyCode *string
			var oldUtilizedAmount *float64
			var oldRemarks, oldReferenceDoc *string

			var actionType, procStatus, requestedBy, checkerBy, checkerComment, reason *string
			var requestedAt, checkerAt *time.Time

			// limit fields
			var lLimitID, lEntityName, lBankName, lCoreLimitType string
			var lLimitType, lLimitSubType, lLimitRemarks *string
			var lSanctionDate, lEffectiveDate *time.Time
			var lLimitCurrencyCode *string
			var lSanctionedAmount float64
			var lFungibilityType *string
			var lFungibilityPct *float64
			var lSecurityType *string
			var lInitialUtilization *float64

			// old limit values
			var lOldEntityName, lOldBankName, lOldCoreLimitType, lOldLimitType, lOldLimitSubType *string
			var lOldSanctionDate, lOldEffectiveDate *time.Time
			var lOldCurrencyCode *string
			var lOldSanctionedAmount, lOldFungibilityPct, lOldInitialUtilization *float64
			var lOldFungibilityType, lOldSecurityType, lOldRemarks *string

			var limitActionType, limitProcStatus, limitRequestedBy, limitCheckerBy, limitCheckerComment, limitReason *string
			var limitRequestedAt, limitCheckerAt *time.Time

			err := rows.Scan(
				&utilizationID, &limitID, &utilizationDate, &currencyCode, &utilizedAmount,
				&remarks, &referenceDoc, &entryMode, &status,
				&oldUtilizationDate, &oldCurrencyCode, &oldUtilizedAmount, &oldRemarks, &oldReferenceDoc,
				&actionType, &procStatus, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason,

				&lLimitID, &lEntityName, &lBankName, &lCoreLimitType, &lLimitType, &lLimitSubType,
				&lSanctionDate, &lEffectiveDate, &lLimitCurrencyCode, &lSanctionedAmount,
				&lFungibilityType, &lFungibilityPct, &lSecurityType, &lLimitRemarks, &lInitialUtilization,
				&lOldEntityName, &lOldBankName, &lOldCoreLimitType, &lOldLimitType, &lOldLimitSubType,
				&lOldSanctionDate, &lOldEffectiveDate, &lOldCurrencyCode, &lOldSanctionedAmount,
				&lOldFungibilityType, &lOldFungibilityPct, &lOldSecurityType, &lOldRemarks, &lOldInitialUtilization,
				&limitActionType, &limitProcStatus, &limitRequestedBy, &limitRequestedAt, &limitCheckerBy, &limitCheckerAt, &limitCheckerComment, &limitReason,
			)
			if err != nil {
				continue
			}

			// KPI computations: available headroom and utilization percentage
			var lInitial float64
			if lInitialUtilization != nil {
				lInitial = *lInitialUtilization
			}
			limitAvailable := lSanctionedAmount - (lInitial + utilizedAmount)
			if limitAvailable < 0 {
				limitAvailable = 0
			}
			var limitUtilPct float64
			if lSanctionedAmount > 0 {
				limitUtilPct = (lInitial + utilizedAmount) / lSanctionedAmount
			}

			item := map[string]interface{}{
				"utilization_id":   utilizationID,
				"limit_id":         limitID,
				"utilization_date": timeOrEmpty(utilizationDate),
				"currency_code":    currencyCode,
				"utilized_amount":  utilizedAmount,
				"remarks":          stringOrEmpty(remarks),
				"reference_doc":    stringOrEmpty(referenceDoc),
				"entry_mode":       entryMode,
				"status":           status,

				"old_utilization_date": timeOrEmpty(oldUtilizationDate),
				"old_currency_code":    stringOrEmpty(oldCurrencyCode),
				"old_utilized_amount":  floatOrZero(oldUtilizedAmount),
				"old_remarks":          stringOrEmpty(oldRemarks),
				"old_reference_doc":    stringOrEmpty(oldReferenceDoc),

				"action_type":       stringOrEmpty(actionType),
				"processing_status": stringOrEmpty(procStatus),
				"requested_by":      stringOrEmpty(requestedBy),
				"requested_at":      timeOrEmpty(requestedAt),
				"checker_by":        stringOrEmpty(checkerBy),
				"checker_at":        timeOrEmpty(checkerAt),
				"checker_comment":   stringOrEmpty(checkerComment),
				"reason":            stringOrEmpty(reason),

				// flattened limit fields (prefixed with limit_)
				"limit_limit_id":            lLimitID,
				"limit_entity_name":         lEntityName,
				"limit_bank_name":           lBankName,
				"limit_core_limit_type":     lCoreLimitType,
				"limit_limit_type":          stringOrEmpty(lLimitType),
				"limit_limit_sub_type":      stringOrEmpty(lLimitSubType),
				"limit_sanction_date":       timeOrEmpty(lSanctionDate),
				"limit_effective_date":      timeOrEmpty(lEffectiveDate),
				"limit_currency_code":       stringOrEmpty(lLimitCurrencyCode),
				"limit_sanctioned_amount":   lSanctionedAmount,
				"limit_fungibility_type":    stringOrEmpty(lFungibilityType),
				"limit_fungibility_pct":     floatOrZero(lFungibilityPct),
				"limit_security_type":       stringOrEmpty(lSecurityType),
				"limit_remarks":             stringOrEmpty(lLimitRemarks),
				"limit_initial_utilization": floatOrZero(lInitialUtilization),

				"limit_old_entity_name":         stringOrEmpty(lOldEntityName),
				"limit_old_bank_name":           stringOrEmpty(lOldBankName),
				"limit_old_core_limit_type":     stringOrEmpty(lOldCoreLimitType),
				"limit_old_limit_type":          stringOrEmpty(lOldLimitType),
				"limit_old_limit_sub_type":      stringOrEmpty(lOldLimitSubType),
				"limit_old_sanction_date":       timeOrEmpty(lOldSanctionDate),
				"limit_old_effective_date":      timeOrEmpty(lOldEffectiveDate),
				"limit_old_currency_code":       stringOrEmpty(lOldCurrencyCode),
				"limit_old_sanctioned_amount":   floatOrZero(lOldSanctionedAmount),
				"limit_old_fungibility_type":    stringOrEmpty(lOldFungibilityType),
				"limit_old_fungibility_pct":     floatOrZero(lOldFungibilityPct),
				"limit_old_security_type":       stringOrEmpty(lOldSecurityType),
				"limit_old_remarks":             stringOrEmpty(lOldRemarks),
				"limit_old_initial_utilization": floatOrZero(lOldInitialUtilization),

				"limit_action_type":       stringOrEmpty(limitActionType),
				"limit_processing_status": stringOrEmpty(limitProcStatus),
				"limit_requested_by":      stringOrEmpty(limitRequestedBy),
				"limit_requested_at":      timeOrEmpty(limitRequestedAt),
				"limit_checker_by":        stringOrEmpty(limitCheckerBy),
				"limit_checker_at":        timeOrEmpty(limitCheckerAt),
				"limit_checker_comment":   stringOrEmpty(limitCheckerComment),
				"limit_reason":            stringOrEmpty(limitReason),
				// KPIs
				"limit_available":       limitAvailable,
				"limit_utilization_pct": limitUtilPct,
			}

			results = append(results, item)
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

// GetApprovedUtilizations returns only APPROVED utilizations
func GetApprovedUtilizations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		query := `
			SELECT 
				u.utilization_id, u.limit_id, u.utilization_date, u.currency_code, u.utilized_amount,
				u.remarks, u.reference_doc, u.entry_mode, u.status,

				l.limit_id, l.entity_name, l.bank_name, l.core_limit_type, l.limit_type, l.limit_sub_type,
				l.sanction_date, l.effective_date, l.currency_code as limit_currency_code, l.sanctioned_amount,
				l.fungibility_type, l.fungibility_pct, l.security_type, l.remarks as limit_remarks, l.initial_utilization,
				la.processing_status as limit_processing_status
			FROM cimplrcorpsaas.bank_limit_utilization u
			INNER JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.auditactionbanklimitutilization
				WHERE utilization_id = u.utilization_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON a.processing_status = 'APPROVED'
			LEFT JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.auditactionbanklimit
				WHERE limit_id = l.limit_id
				ORDER BY requested_at DESC
				LIMIT 1
			) la ON TRUE
			WHERE COALESCE(u.is_deleted, false) = false
			ORDER BY u.utilization_date DESC`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			var utilizationID, limitID, currencyCode, entryMode, status string
			var utilizationDate *time.Time
			var utilizedAmount float64
			var remarks, referenceDoc *string

			// limit fields
			// limit fields
			var lLimitID, lEntityName, lBankName, lCoreLimitType string
			var lLimitType, lLimitSubType, lLimitRemarks *string
			var lSanctionDate, lEffectiveDate *time.Time
			var lLimitCurrencyCode *string
			var lSanctionedAmount float64
			var lFungibilityType *string
			var lFungibilityPct *float64
			var lSecurityType *string
			var lInitialUtilization *float64
			var lLimitProcessingStatus *string

			err := rows.Scan(
				&utilizationID, &limitID, &utilizationDate, &currencyCode, &utilizedAmount,
				&remarks, &referenceDoc, &entryMode, &status,
				&lLimitID, &lEntityName, &lBankName, &lCoreLimitType, &lLimitType, &lLimitSubType,
				&lSanctionDate, &lEffectiveDate, &lLimitCurrencyCode, &lSanctionedAmount,
				&lFungibilityType, &lFungibilityPct, &lSecurityType, &lLimitRemarks, &lInitialUtilization,
				&lLimitProcessingStatus,
			)
			if err != nil {
				continue
			}

			// KPI computations for approved utilizations
			var lInitial float64
			if lInitialUtilization != nil {
				lInitial = *lInitialUtilization
			}
			limitAvailable := lSanctionedAmount - (lInitial + utilizedAmount)
			if limitAvailable < 0 {
				limitAvailable = 0
			}
			var limitUtilPct float64
			if lSanctionedAmount > 0 {
				limitUtilPct = (lInitial + utilizedAmount) / lSanctionedAmount
			}

			item := map[string]interface{}{
				"utilization_id":   utilizationID,
				"limit_id":         limitID,
				"utilization_date": timeOrEmpty(utilizationDate),
				"currency_code":    currencyCode,
				"utilized_amount":  utilizedAmount,
				"remarks":          stringOrEmpty(remarks),
				"reference_doc":    stringOrEmpty(referenceDoc),
				"entry_mode":       entryMode,
				"status":           status,

				"limit_limit_id":            lLimitID,
				"limit_entity_name":         lEntityName,
				"limit_bank_name":           lBankName,
				"limit_core_limit_type":     lCoreLimitType,
				"limit_limit_type":          stringOrEmpty(lLimitType),
				"limit_limit_sub_type":      stringOrEmpty(lLimitSubType),
				"limit_sanction_date":       timeOrEmpty(lSanctionDate),
				"limit_effective_date":      timeOrEmpty(lEffectiveDate),
				"limit_currency_code":       stringOrEmpty(lLimitCurrencyCode),
				"limit_sanctioned_amount":   lSanctionedAmount,
				"limit_fungibility_type":    stringOrEmpty(lFungibilityType),
				"limit_fungibility_pct":     floatOrZero(lFungibilityPct),
				"limit_security_type":       stringOrEmpty(lSecurityType),
				"limit_remarks":             stringOrEmpty(lLimitRemarks),
				"limit_initial_utilization": floatOrZero(lInitialUtilization),
				"limit_processing_status":   stringOrEmpty(lLimitProcessingStatus),
				// KPIs
				"limit_available":       limitAvailable,
				"limit_utilization_pct": limitUtilPct,
			}

			results = append(results, item)
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

// GetApprovedUtilizationsGrouped returns approved utilizations plus grouped KPIs by limit
func GetApprovedUtilizationsGrouped(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		query := `
			SELECT 
				u.utilization_id, u.limit_id, u.utilization_date, u.currency_code, u.utilized_amount,
				u.remarks, u.reference_doc, u.entry_mode, u.status,

				l.limit_id, l.entity_name, l.bank_name, l.core_limit_type, l.limit_type, l.limit_sub_type,
				l.sanction_date, l.effective_date, l.currency_code as limit_currency_code, l.sanctioned_amount,
				l.fungibility_type, l.fungibility_pct, l.security_type, l.remarks as limit_remarks, l.initial_utilization
			FROM cimplrcorpsaas.bank_limit_utilization u
			INNER JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.auditactionbanklimitutilization
				WHERE utilization_id = u.utilization_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON a.processing_status = 'APPROVED'
			LEFT JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
			WHERE COALESCE(u.is_deleted, false) = false
			ORDER BY u.utilization_date DESC`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		results := make([]map[string]interface{}, 0)

		// grouping by limit_id
		type grp struct {
			LimitID          string
			BankName         string
			SanctionedAmount float64
			TotalInitial     float64
			TotalUtilized    float64
		}
		groups := map[string]*grp{}

		for rows.Next() {
			var utilizationID, limitID, currencyCode, entryMode, status string
			var utilizationDate *time.Time
			var utilizedAmount float64
			var remarks, referenceDoc *string

			// limit fields
			var lLimitID, lEntityName, lBankName, lCoreLimitType string
			var lLimitType, lLimitSubType, lLimitRemarks *string
			var lSanctionDate, lEffectiveDate *time.Time
			var lLimitCurrencyCode *string
			var lSanctionedAmount float64
			var lFungibilityType *string
			var lFungibilityPct *float64
			var lSecurityType *string
			var lInitialUtilization *float64

			if err := rows.Scan(
				&utilizationID, &limitID, &utilizationDate, &currencyCode, &utilizedAmount,
				&remarks, &referenceDoc, &entryMode, &status,

				&lLimitID, &lEntityName, &lBankName, &lCoreLimitType, &lLimitType, &lLimitSubType,
				&lSanctionDate, &lEffectiveDate, &lLimitCurrencyCode, &lSanctionedAmount,
				&lFungibilityType, &lFungibilityPct, &lSecurityType, &lLimitRemarks, &lInitialUtilization,
			); err != nil {
				continue
			}

			// per-row KPI
			var lInitial float64
			if lInitialUtilization != nil {
				lInitial = *lInitialUtilization
			}
			limitAvailable := lSanctionedAmount - (lInitial + utilizedAmount)
			if limitAvailable < 0 {
				limitAvailable = 0
			}
			var limitUtilPct float64
			if lSanctionedAmount > 0 {
				limitUtilPct = (lInitial + utilizedAmount) / lSanctionedAmount
			}

			item := map[string]interface{}{
				"utilization_id":   utilizationID,
				"limit_id":         limitID,
				"utilization_date": timeOrEmpty(utilizationDate),
				"currency_code":    currencyCode,
				"utilized_amount":  utilizedAmount,
				"remarks":          stringOrEmpty(remarks),
				"reference_doc":    stringOrEmpty(referenceDoc),
				"entry_mode":       entryMode,
				"status":           status,

				"limit_limit_id":            lLimitID,
				"limit_entity_name":         lEntityName,
				"limit_bank_name":           lBankName,
				"limit_core_limit_type":     lCoreLimitType,
				"limit_limit_type":          stringOrEmpty(lLimitType),
				"limit_limit_sub_type":      stringOrEmpty(lLimitSubType),
				"limit_sanction_date":       timeOrEmpty(lSanctionDate),
				"limit_effective_date":      timeOrEmpty(lEffectiveDate),
				"limit_currency_code":       stringOrEmpty(lLimitCurrencyCode),
				"limit_sanctioned_amount":   lSanctionedAmount,
				"limit_fungibility_type":    stringOrEmpty(lFungibilityType),
				"limit_fungibility_pct":     floatOrZero(lFungibilityPct),
				"limit_security_type":       stringOrEmpty(lSecurityType),
				"limit_remarks":             stringOrEmpty(lLimitRemarks),
				"limit_initial_utilization": floatOrZero(lInitialUtilization),

				// KPIs
				"limit_available":       limitAvailable,
				"limit_utilization_pct": limitUtilPct,
			}

			// append row
			results = append(results, item)

			// accumulate group
			g, ok := groups[lLimitID]
			if !ok {
				g = &grp{LimitID: lLimitID, BankName: lBankName, SanctionedAmount: lSanctionedAmount}
				if lInitialUtilization != nil {
					g.TotalInitial = *lInitialUtilization
				}
				groups[lLimitID] = g
			}
			g.TotalUtilized += utilizedAmount
		}

		// build grouped slice with computed KPIs
		grouped := make([]map[string]interface{}, 0, len(groups))
		for _, g := range groups {
			avail := g.SanctionedAmount - (g.TotalInitial + g.TotalUtilized)
			if avail < 0 {
				avail = 0
			}
			var utilPct float64
			if g.SanctionedAmount > 0 {
				utilPct = (g.TotalInitial + g.TotalUtilized) / g.SanctionedAmount
			}
			grouped = append(grouped, map[string]interface{}{
				"limit_id":          g.LimitID,
				"bank_name":         g.BankName,
				"sanctioned_amount": g.SanctionedAmount,
				"total_initial":     g.TotalInitial,
				"total_utilized":    g.TotalUtilized,
				"available":         avail,
				"utilization_pct":   utilPct,
			})
		}

		payload := map[string]interface{}{
			"rows":   results,
			"groups": grouped,
		}

		api.RespondWithPayload(w, true, "", payload)
	}
}

// BulkApproveUtilizations approves pending audit actions
func BulkApproveUtilizations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID         string   `json:"user_id"`
			UtilizationIDs []string `json:"utilization_ids"`
			Comment        string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.UtilizationIDs) == 0 {
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

		sel := `SELECT DISTINCT ON (utilization_id) action_id, utilization_id, action_type 
			FROM cimplrcorpsaas.auditactionbanklimitutilization 
			WHERE utilization_id = ANY($1) 
			ORDER BY utilization_id, requested_at DESC`

		rows, err := pgxPool.Query(ctx, sel, req.UtilizationIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		deleteIDs := make([]string, 0)
		found := map[string]bool{}

		for rows.Next() {
			var actionID, utilizationID, actionType string
			if err := rows.Scan(&actionID, &utilizationID, &actionType); err != nil {
				continue
			}
			found[utilizationID] = true
			actionIDs = append(actionIDs, actionID)
			if actionType == "DELETE" {
				deleteIDs = append(deleteIDs, utilizationID)
			}
		}

		missing := []string{}
		for _, id := range req.UtilizationIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		upd := `UPDATE cimplrcorpsaas.auditactionbanklimitutilization 
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 
			WHERE action_id = ANY($3)`

		if _, err := pgxPool.Exec(ctx, upd, checkerBy, nullifyEmpty(req.Comment), actionIDs); err != nil {
			api.RespondWithResult(w, false, "failed to approve: "+err.Error())
			return
		}

		// Execute soft delete for DELETE action types
		deleted := []string{}
		if len(deleteIDs) > 0 {
			delUpd := `UPDATE cimplrcorpsaas.bank_limit_utilization SET is_deleted=true WHERE utilization_id = ANY($1) RETURNING utilization_id`
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

// BulkRejectUtilizations rejects pending audit actions
func BulkRejectUtilizations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID         string   `json:"user_id"`
			UtilizationIDs []string `json:"utilization_ids"`
			Comment        string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.UtilizationIDs) == 0 {
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

		sel := `SELECT DISTINCT ON (utilization_id) action_id, utilization_id 
			FROM cimplrcorpsaas.auditactionbanklimitutilization 
			WHERE utilization_id = ANY($1) 
			ORDER BY utilization_id, requested_at DESC`

		rows, err := pgxPool.Query(ctx, sel, req.UtilizationIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		found := map[string]bool{}

		for rows.Next() {
			var actionID, utilizationID string
			if err := rows.Scan(&actionID, &utilizationID); err != nil {
				continue
			}
			found[utilizationID] = true
			actionIDs = append(actionIDs, actionID)
		}

		missing := []string{}
		for _, id := range req.UtilizationIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			api.RespondWithResult(w, false, fmt.Sprintf("missing audit entries for: %v", missing))
			return
		}

		upd := `UPDATE cimplrcorpsaas.auditactionbanklimitutilization 
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

// UploadUtilization handles CSV/XLSX upload for bulk utilization creation
func UploadUtilization(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "failed to parse form: "+err.Error())
			return
		}

		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no file uploaded")
			return
		}

		file := files[0]
		f, err := file.Open()
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "failed to open file: "+err.Error())
			return
		}
		defer f.Close()

		ext := strings.ToLower(filepath.Ext(file.Filename))
		var rows [][]string

		if ext == ".csv" {
			rows, err = parseCSVUtilization(f)
		} else if ext == ".xlsx" || ext == ".xls" {
			rows, err = parseXLSXUtilization(file, f)
		} else {
			api.RespondWithError(w, http.StatusBadRequest, "unsupported file type. Use CSV or XLSX")
			return
		}

		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "failed to parse file: "+err.Error())
			return
		}

		if len(rows) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "file must contain header and at least one data row")
			return
		}

		// Process rows and create utilizations
		results := processUtilizationRows(ctx, pgxPool, rows, requestedBy)

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

func parseCSVUtilization(f multipart.File) ([][]string, error) {
	reader := csv.NewReader(f)
	reader.TrimLeadingSpace = true
	return reader.ReadAll()
}

func parseXLSXUtilization(fileHeader *multipart.FileHeader, f multipart.File) ([][]string, error) {
	tmpFile, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	xlsx, err := excelize.OpenReader(strings.NewReader(string(tmpFile)))
	if err != nil {
		return nil, err
	}
	defer xlsx.Close()

	sheetName := xlsx.GetSheetName(0)
	rows, err := xlsx.GetRows(sheetName)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func processUtilizationRows(ctx context.Context, pgxPool *pgxpool.Pool, rows [][]string, requestedBy string) []map[string]interface{} {
	header := rows[0]
	colMap := make(map[string]int)

	for i, h := range header {
		colMap[strings.ToLower(strings.TrimSpace(h))] = i
	}

	results := make([]map[string]interface{}, 0)

	for i := 1; i < len(rows); i++ {
		row := rows[i]
		result := map[string]interface{}{"row": i + 1}

		limitID := getCellValue(row, colMap, "limit_id")
		utilizationDate := getCellValue(row, colMap, "utilization_date")
		currencyCode := getCellValue(row, colMap, "currency_code")
		utilizedAmountStr := getCellValue(row, colMap, "utilized_amount")
		remarks := getCellValue(row, colMap, "remarks")
		referenceDoc := getCellValue(row, colMap, "reference_doc")

		if limitID == "" || utilizationDate == "" || currencyCode == "" || utilizedAmountStr == "" {
			result["success"] = false
			result["error"] = "missing required fields"
			results = append(results, result)
			continue
		}

		utilizedAmount, err := strconv.ParseFloat(utilizedAmountStr, 64)
		if err != nil {
			result["success"] = false
			result["error"] = "invalid utilized_amount"
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

		ins := `INSERT INTO cimplrcorpsaas.bank_limit_utilization (
			limit_id, utilization_date, currency_code, utilized_amount,
			remarks, reference_doc, entry_mode, status
		) VALUES ($1,$2,$3,$4,$5,$6,'UPLOAD','DRAFT') RETURNING utilization_id`

		var utilizationID string
		err = tx.QueryRow(ctx, ins,
			limitID, utilizationDate, strings.ToUpper(currencyCode), utilizedAmount,
			nullifyEmpty(remarks), nullifyEmpty(referenceDoc),
		).Scan(&utilizationID)

		if err != nil {
			tx.Rollback(ctx)
			result["success"] = false
			result["error"] = "failed to insert: " + err.Error()
			results = append(results, result)
			continue
		}

		auditQ := `INSERT INTO cimplrcorpsaas.auditactionbanklimitutilization (
			utilization_id, limit_id, action_type, processing_status, reason, requested_by, requested_at
		) VALUES ($1,$2,'CREATE','PENDING_APPROVAL',$3,$4,now())`

		if _, err := tx.Exec(ctx, auditQ, utilizationID, limitID, nil, requestedBy); err != nil {
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
		result["utilization_id"] = utilizationID
		results = append(results, result)
	}

	return results
}

func getCellValue(row []string, colMap map[string]int, colName string) string {
	idx, ok := colMap[colName]
	if !ok || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}
