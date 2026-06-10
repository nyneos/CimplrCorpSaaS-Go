package limit

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type limitAuditRequest struct {
	LimitID string `json:"limit_id"`
}

type utilizationAuditRequest struct {
	UtilizationID string `json:"utilization_id"`
}

const (
	limitPositionAuditModule    = "limit-position"
	limitUtilizationAuditModule = "limit-utilization"
	limitAdditionalFileAuditTbl = "cimplrcorpsaas.cash_additional_file_audit"
	limitAuditSourceWorkflow    = "WORKFLOW"
	limitAuditSourceFile        = "ADDITIONAL_FILE"
)

func GetLimitAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req limitAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.LimitID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "limit_id is required")
			return
		}

		ctx := r.Context()
		limitID := strings.TrimSpace(req.LimitID)
		if code, msg := validateLimitAuditAccess(ctx, pgxPool, limitID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		payload, err := readLimitWorkflowAudit(ctx, pgxPool, limitID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read limit audit history")
			return
		}
		payload, err = appendLimitAdditionalFileAudit(ctx, pgxPool, payload, limitPositionAuditModule, limitID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read limit file audit history")
			return
		}
		sortLimitAuditPayload(payload)
		writeLimitAuditPayload(w, payload)
	}
}

func GetUtilizationAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req utilizationAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UtilizationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "utilization_id is required")
			return
		}

		ctx := r.Context()
		utilizationID := strings.TrimSpace(req.UtilizationID)
		if code, msg := validateUtilizationAuditAccess(ctx, pgxPool, utilizationID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		payload, err := readUtilizationWorkflowAudit(ctx, pgxPool, utilizationID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read utilization audit history")
			return
		}
		payload, err = appendLimitAdditionalFileAudit(ctx, pgxPool, payload, limitUtilizationAuditModule, utilizationID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read utilization file audit history")
			return
		}
		sortLimitAuditPayload(payload)
		writeLimitAuditPayload(w, payload)
	}
}

func readLimitWorkflowAudit(ctx context.Context, pgxPool *pgxpool.Pool, limitID string) ([]map[string]interface{}, error) {
	rows, err := pgxPool.Query(ctx, `
		SELECT
			action_id,
			limit_id,
			action_type,
			processing_status,
			requested_by,
			requested_at,
			requested_ip,
			checker_by,
			checker_at,
			checker_ip,
			checker_comment,
			reason
		FROM cimplrcorpsaas.auditactionbanklimit
		WHERE limit_id = $1
		ORDER BY requested_at ASC, action_id ASC
	`, limitID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	payload := make([]map[string]interface{}, 0)
	for rows.Next() {
		entries, err := scanLimitWorkflowAuditRows(rows, func(action string) []map[string]interface{} {
			if !strings.EqualFold(action, constants.AuditActionEdit) && !strings.EqualFold(action, "EDIT") {
				return nil
			}
			return buildLimitChangeSummary(ctx, pgxPool, limitID)
		})
		if err != nil {
			return nil, err
		}
		payload = append(payload, entries...)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return payload, nil
}

func readUtilizationWorkflowAudit(ctx context.Context, pgxPool *pgxpool.Pool, utilizationID string) ([]map[string]interface{}, error) {
	rows, err := pgxPool.Query(ctx, `
		SELECT
			action_id,
			utilization_id,
			action_type,
			processing_status,
			requested_by,
			requested_at,
			requested_ip,
			checker_by,
			checker_at,
			checker_ip,
			checker_comment,
			reason
		FROM cimplrcorpsaas.auditactionbanklimitutilization
		WHERE utilization_id = $1
		ORDER BY requested_at ASC, action_id ASC
	`, utilizationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	payload := make([]map[string]interface{}, 0)
	for rows.Next() {
		entries, err := scanLimitWorkflowAuditRows(rows, func(action string) []map[string]interface{} {
			if !strings.EqualFold(action, constants.AuditActionEdit) && !strings.EqualFold(action, "EDIT") {
				return nil
			}
			return buildUtilizationChangeSummary(ctx, pgxPool, utilizationID)
		})
		if err != nil {
			return nil, err
		}
		payload = append(payload, entries...)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return payload, nil
}

type limitAuditScanner interface {
	Scan(dest ...interface{}) error
}

func scanLimitWorkflowAuditRows(row limitAuditScanner, changesFn func(action string) []map[string]interface{}) ([]map[string]interface{}, error) {
	var auditID interface{}
	var entityID string
	var action, status, requestedBy, requestedIP, checkerBy, checkerIP, checkerComment, reason sql.NullString
	var requestedAt, checkerAt sql.NullTime

	if err := row.Scan(&auditID, &entityID, &action, &status, &requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason); err != nil {
		return nil, err
	}

	actionType := limitAuditString(action)
	entry := map[string]interface{}{
		"audit_id":          limitAuditID(auditID),
		"entity_id":         entityID,
		"action_type":       actionType,
		"processing_status": limitAuditString(status),
		"requested_by":      limitAuditString(requestedBy),
		"requested_at":      limitAuditTime(requestedAt),
		"requested_ip":      limitAuditString(requestedIP),
		"checker_by":        limitAuditString(checkerBy),
		"checker_at":        limitAuditTime(checkerAt),
		"checker_ip":        limitAuditString(checkerIP),
		"checker_comment":   limitAuditString(checkerComment),
		"reason":            limitAuditString(reason),
		"source":            limitAuditSourceWorkflow,
	}
	if changes := changesFn(actionType); len(changes) > 0 {
		entry["change_summary"] = changes
	}

	return []map[string]interface{}{entry}, nil
}

func buildCheckerDecisionEntry(auditID interface{}, entityID, status string, checkerBy sql.NullString, checkerAt sql.NullTime, checkerComment sql.NullString) map[string]interface{} {
	if !checkerAt.Valid {
		return nil
	}

	action := ""
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case constants.StatusApproved:
		action = "APPROVE"
	case constants.StatusRejected:
		action = "REJECT"
	default:
		return nil
	}

	return map[string]interface{}{
		"audit_id":          fmt.Sprintf("%s-%s", limitAuditID(auditID), strings.ToLower(action)),
		"entity_id":         entityID,
		"action_type":       action,
		"processing_status": status,
		"requested_by":      limitAuditString(checkerBy),
		"requested_at":      limitAuditTime(checkerAt),
		"checker_by":        limitAuditString(checkerBy),
		"checker_at":        limitAuditTime(checkerAt),
		"checker_comment":   limitAuditString(checkerComment),
		"reason":            "",
		"source":            limitAuditSourceWorkflow,
	}
}

func appendLimitAdditionalFileAudit(ctx context.Context, pgxPool *pgxpool.Pool, payload []map[string]interface{}, moduleKey, parentID string) ([]map[string]interface{}, error) {
	rows, err := pgxPool.Query(ctx, `
		SELECT
			audit_id,
			parent_record_id,
			file_id,
			action_type,
			processing_status,
			requested_by,
			requested_at,
			requested_ip,
			checker_by,
			checker_at,
			checker_ip,
			checker_comment,
			reason
		FROM `+limitAdditionalFileAuditTbl+`
		WHERE module_key = $1
		  AND parent_record_id = $2
		ORDER BY requested_at ASC, audit_id ASC
	`, moduleKey, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		entries, err := scanLimitFileAuditRows(rows, moduleKey)
		if err != nil {
			return nil, err
		}
		payload = append(payload, entries...)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return payload, nil
}

func scanLimitFileAuditRows(row limitAuditScanner, moduleKey string) ([]map[string]interface{}, error) {
	var auditID int64
	var parentRecordID, fileID, action, status, requestedBy, requestedIP, checkerBy, checkerIP, checkerComment, reason sql.NullString
	var requestedAt, checkerAt sql.NullTime

	if err := row.Scan(&auditID, &parentRecordID, &fileID, &action, &status, &requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason); err != nil {
		return nil, err
	}

	actionType := limitAuditString(action)
	if strings.EqualFold(actionType, "CREATE") {
		actionType = "UPLOAD_FILE"
	}
	entry := map[string]interface{}{
		"audit_id":          fmt.Sprintf("file-%d", auditID),
		"entity_id":         limitAuditString(fileID),
		"parent_record_id":  limitAuditString(parentRecordID),
		"file_id":           limitAuditString(fileID),
		"module_key":        moduleKey,
		"action_type":       actionType,
		"processing_status": limitAuditString(status),
		"requested_by":      limitAuditString(requestedBy),
		"requested_at":      limitAuditTime(requestedAt),
		"requested_ip":      limitAuditString(requestedIP),
		"checker_by":        limitAuditString(checkerBy),
		"checker_at":        limitAuditTime(checkerAt),
		"checker_ip":        limitAuditString(checkerIP),
		"checker_comment":   limitAuditString(checkerComment),
		"reason":            limitAuditString(reason),
		"source":            limitAuditSourceFile,
	}

	return []map[string]interface{}{entry}, nil
}

func validateLimitAuditAccess(ctx context.Context, pgxPool *pgxpool.Pool, limitID string) (int, string) {
	var entityName string
	if err := pgxPool.QueryRow(ctx, `
		SELECT entity_name
		FROM cimplrcorpsaas.bank_limit
		WHERE limit_id = $1
		LIMIT 1
	`, limitID).Scan(&entityName); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return http.StatusNotFound, "limit not found"
		}
		return http.StatusInternalServerError, err.Error()
	}
	return validateLimitAuditEntityAccess(ctx, entityName)
}

func validateUtilizationAuditAccess(ctx context.Context, pgxPool *pgxpool.Pool, utilizationID string) (int, string) {
	var entityName string
	if err := pgxPool.QueryRow(ctx, `
		SELECT l.entity_name
		FROM cimplrcorpsaas.bank_limit_utilization u
		JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		WHERE u.utilization_id = $1
		LIMIT 1
	`, utilizationID).Scan(&entityName); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return http.StatusNotFound, "utilization not found"
		}
		return http.StatusInternalServerError, err.Error()
	}
	return validateLimitAuditEntityAccess(ctx, entityName)
}

func validateLimitAuditEntityAccess(ctx context.Context, entityName string) (int, string) {
	if len(api.GetEntityNamesFromCtx(ctx)) == 0 && len(ctxutil.FromContext(ctx).EntityIDs) == 0 {
		return 0, ""
	}
	if !ctxutil.FromContext(ctx).HasEntityAccess(entityName) {
		return http.StatusForbidden, "unauthorized entity: " + entityName
	}
	return 0, ""
}

func buildLimitChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, limitID string) []map[string]interface{} {
	var (
		entityName, oldEntityName                 string
		bankName, oldBankName                     string
		coreLimitType, oldCoreLimitType           string
		limitType, oldLimitType                   string
		limitSubType, oldLimitSubType             string
		sanctionDate, oldSanctionDate             string
		effectiveDate, oldEffectiveDate           string
		currencyCode, oldCurrencyCode             string
		sanctionedAmount, oldSanctionedAmount     string
		fungibilityType, oldFungibilityType       string
		fungibilityPct, oldFungibilityPct         string
		securityType, oldSecurityType             string
		remarks, oldRemarks                       string
		initialUtilization, oldInitialUtilization string
	)

	err := pgxPool.QueryRow(ctx, `
		SELECT
			COALESCE(entity_name, ''),
			COALESCE(old_entity_name, ''),
			COALESCE(bank_name, ''),
			COALESCE(old_bank_name, ''),
			COALESCE(core_limit_type, ''),
			COALESCE(old_core_limit_type, ''),
			COALESCE(limit_type, ''),
			COALESCE(old_limit_type, ''),
			COALESCE(limit_sub_type, ''),
			COALESCE(old_limit_sub_type, ''),
			COALESCE(TO_CHAR(sanction_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_sanction_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(effective_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_effective_date, 'YYYY-MM-DD'), ''),
			COALESCE(currency_code, ''),
			COALESCE(old_currency_code, ''),
			COALESCE(CAST(sanctioned_amount AS text), ''),
			COALESCE(CAST(old_sanctioned_amount AS text), ''),
			COALESCE(fungibility_type, ''),
			COALESCE(old_fungibility_type, ''),
			COALESCE(CAST(fungibility_pct AS text), ''),
			COALESCE(CAST(old_fungibility_pct AS text), ''),
			COALESCE(security_type, ''),
			COALESCE(old_security_type, ''),
			COALESCE(remarks, ''),
			COALESCE(old_remarks, ''),
			COALESCE(CAST(initial_utilization AS text), ''),
			COALESCE(CAST(old_initial_utilization AS text), '')
		FROM cimplrcorpsaas.bank_limit
		WHERE limit_id = $1
	`, limitID).Scan(
		&entityName, &oldEntityName,
		&bankName, &oldBankName,
		&coreLimitType, &oldCoreLimitType,
		&limitType, &oldLimitType,
		&limitSubType, &oldLimitSubType,
		&sanctionDate, &oldSanctionDate,
		&effectiveDate, &oldEffectiveDate,
		&currencyCode, &oldCurrencyCode,
		&sanctionedAmount, &oldSanctionedAmount,
		&fungibilityType, &oldFungibilityType,
		&fungibilityPct, &oldFungibilityPct,
		&securityType, &oldSecurityType,
		&remarks, &oldRemarks,
		&initialUtilization, &oldInitialUtilization,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	appendLimitAuditChange(&changes, "Entity", oldEntityName, entityName)
	appendLimitAuditChange(&changes, "Bank", oldBankName, bankName)
	appendLimitAuditChange(&changes, "Core Limit Type", oldCoreLimitType, coreLimitType)
	appendLimitAuditChange(&changes, "Limit Type", oldLimitType, limitType)
	appendLimitAuditChange(&changes, "Limit Sub Type", oldLimitSubType, limitSubType)
	appendLimitAuditChange(&changes, "Sanction Date", oldSanctionDate, sanctionDate)
	appendLimitAuditChange(&changes, "Effective Date", oldEffectiveDate, effectiveDate)
	appendLimitAuditChange(&changes, "Currency", oldCurrencyCode, currencyCode)
	appendLimitAuditChange(&changes, "Sanctioned Amount", oldSanctionedAmount, sanctionedAmount)
	appendLimitAuditChange(&changes, "Fungibility Type", oldFungibilityType, fungibilityType)
	appendLimitAuditChange(&changes, "Fungibility %", oldFungibilityPct, fungibilityPct)
	appendLimitAuditChange(&changes, "Security Type", oldSecurityType, securityType)
	appendLimitAuditChange(&changes, "Remarks", oldRemarks, remarks)
	appendLimitAuditChange(&changes, "Initial Utilization", oldInitialUtilization, initialUtilization)
	return changes
}

func buildUtilizationChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, utilizationID string) []map[string]interface{} {
	var (
		utilizationDate, oldUtilizationDate string
		currencyCode, oldCurrencyCode       string
		utilizedAmount, oldUtilizedAmount   string
		remarks, oldRemarks                 string
		referenceDoc, oldReferenceDoc       string
	)

	err := pgxPool.QueryRow(ctx, `
		SELECT
			COALESCE(TO_CHAR(utilization_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_utilization_date, 'YYYY-MM-DD'), ''),
			COALESCE(currency_code, ''),
			COALESCE(old_currency_code, ''),
			COALESCE(CAST(utilized_amount AS text), ''),
			COALESCE(CAST(old_utilized_amount AS text), ''),
			COALESCE(remarks, ''),
			COALESCE(old_remarks, ''),
			COALESCE(reference_doc, ''),
			COALESCE(old_reference_doc, '')
		FROM cimplrcorpsaas.bank_limit_utilization
		WHERE utilization_id = $1
	`, utilizationID).Scan(
		&utilizationDate, &oldUtilizationDate,
		&currencyCode, &oldCurrencyCode,
		&utilizedAmount, &oldUtilizedAmount,
		&remarks, &oldRemarks,
		&referenceDoc, &oldReferenceDoc,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	appendLimitAuditChange(&changes, "Utilization Date", oldUtilizationDate, utilizationDate)
	appendLimitAuditChange(&changes, "Currency", oldCurrencyCode, currencyCode)
	appendLimitAuditChange(&changes, "Utilized Amount", oldUtilizedAmount, utilizedAmount)
	appendLimitAuditChange(&changes, "Remarks", oldRemarks, remarks)
	appendLimitAuditChange(&changes, "Reference Document", oldReferenceDoc, referenceDoc)
	return changes
}

func appendLimitAuditChange(changes *[]map[string]interface{}, fieldName, oldValue, newValue string) {
	if strings.TrimSpace(oldValue) == strings.TrimSpace(newValue) {
		return
	}
	*changes = append(*changes, map[string]interface{}{
		"field":     fieldName,
		"old_value": oldValue,
		"new_value": newValue,
	})
}

func sortLimitAuditPayload(payload []map[string]interface{}) {
	sort.SliceStable(payload, func(i, j int) bool {
		left := limitAuditPayloadTime(payload[i]["requested_at"])
		right := limitAuditPayloadTime(payload[j]["requested_at"])
		if left.Equal(right) {
			return fmt.Sprint(payload[i]["audit_id"]) < fmt.Sprint(payload[j]["audit_id"])
		}
		if left.IsZero() {
			return false
		}
		if right.IsZero() {
			return true
		}
		return left.Before(right)
	})
}

func limitAuditPayloadTime(value interface{}) time.Time {
	switch typed := value.(type) {
	case time.Time:
		return typed
	case *time.Time:
		if typed == nil {
			return time.Time{}
		}
		return *typed
	default:
		return time.Time{}
	}
}

func writeLimitAuditPayload(w http.ResponseWriter, payload []map[string]interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"audit_logs": payload,
	})
}

func limitAuditString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func limitAuditTime(value sql.NullTime) interface{} {
	return api.FormatAuditTimestampNullIST(value)
}

func limitAuditID(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
