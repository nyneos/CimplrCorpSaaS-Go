package sweepconfig

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

type sweepConfigAuditRequest struct {
	SweepID string `json:"sweep_id"`
}

type sweepInitiationAuditRequest struct {
	InitiationID string `json:"initiation_id"`
}

const (
	sweepPlanningAdditionalFileAuditModule   = "sweep-planning"
	sweepInitiationAdditionalFileAuditModule = "sweep-initiation"
	sweepAdditionalFileAuditTable            = "cimplrcorpsaas.cash_additional_file_audit"
	sweepAuditSourceWorkflow                 = "WORKFLOW"
	sweepAuditSourceAdditionalFile           = "ADDITIONAL_FILE"
)

func GetSweepConfigAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req sweepConfigAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.SweepID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "sweep_id is required")
			return
		}

		ctx := r.Context()
		if code, msg := validateSweepConfigAuditAccess(ctx, pgxPool, req.SweepID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				action_id,
				sweep_id,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				checker_by,
				checker_at,
				checker_comment,
				reason
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			WHERE sweep_id = $1
			ORDER BY requested_at ASC, action_id ASC
		`, strings.TrimSpace(req.SweepID))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			entry, err := scanSweepAuditRow(rows)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read sweep config audit history")
				return
			}
			payload = append(payload, entry)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read sweep config audit history")
			return
		}
		payload, err = appendSweepAdditionalFileAudit(ctx, pgxPool, payload, sweepPlanningAdditionalFileAuditModule, strings.TrimSpace(req.SweepID))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read sweep planning file audit history")
			return
		}
		sortSweepAuditPayload(payload)

		writeSweepAuditPayload(w, payload)
	}
}

func GetSweepInitiationAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req sweepInitiationAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.InitiationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "initiation_id is required")
			return
		}

		ctx := r.Context()
		if code, msg := validateSweepInitiationAuditAccess(ctx, pgxPool, req.InitiationID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				action_id,
				initiation_id,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				checker_by,
				checker_at,
				checker_comment,
				reason
			FROM cimplrcorpsaas.auditactionsweepinitiation
			WHERE initiation_id = $1
			  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY requested_at ASC, action_id ASC
		`, strings.TrimSpace(req.InitiationID))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			entry, err := scanSweepAuditRow(rows)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read sweep initiation audit history")
				return
			}
			payload = append(payload, entry)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read sweep initiation audit history")
			return
		}
		payload, err = appendSweepAdditionalFileAudit(ctx, pgxPool, payload, sweepInitiationAdditionalFileAuditModule, strings.TrimSpace(req.InitiationID))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read sweep initiation file audit history")
			return
		}
		sortSweepAuditPayload(payload)

		writeSweepAuditPayload(w, payload)
	}
}

func validateSweepConfigAuditAccess(ctx context.Context, pgxPool *pgxpool.Pool, sweepID string) (int, string) {
	var entityName string
	if err := pgxPool.QueryRow(ctx, `
		SELECT entity_name
		FROM cimplrcorpsaas.sweepconfiguration
		WHERE sweep_id = $1
		LIMIT 1
	`, strings.TrimSpace(sweepID)).Scan(&entityName); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return http.StatusNotFound, "sweep configuration not found"
		}
		return http.StatusInternalServerError, err.Error()
	}
	return validateSweepAuditEntityAccess(ctx, entityName)
}

func validateSweepInitiationAuditAccess(ctx context.Context, pgxPool *pgxpool.Pool, initiationID string) (int, string) {
	var entityName string
	if err := pgxPool.QueryRow(ctx, `
		SELECT sc.entity_name
		FROM cimplrcorpsaas.sweep_initiation si
		JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = si.sweep_id
		WHERE si.initiation_id = $1
		LIMIT 1
	`, strings.TrimSpace(initiationID)).Scan(&entityName); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return http.StatusNotFound, "sweep initiation not found"
		}
		return http.StatusInternalServerError, err.Error()
	}
	return validateSweepAuditEntityAccess(ctx, entityName)
}

func validateSweepAuditEntityAccess(ctx context.Context, entityName string) (int, string) {
	if len(api.GetEntityNamesFromCtx(ctx)) == 0 && len(ctxutil.FromContext(ctx).EntityIDs) == 0 {
		return 0, ""
	}
	if !ctxutil.FromContext(ctx).HasEntityAccess(entityName) {
		return http.StatusForbidden, "unauthorized entity: " + entityName
	}
	return 0, ""
}

type sweepAuditScanner interface {
	Scan(dest ...interface{}) error
}

func scanSweepAuditRow(row sweepAuditScanner) (map[string]interface{}, error) {
	var auditID interface{}
	var entityID string
	var action, status, requestedBy, checkerBy, checkerComment, reason sql.NullString
	var requestedAt, checkerAt sql.NullTime

	if err := row.Scan(&auditID, &entityID, &action, &status, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"audit_id":          sweepAuditID(auditID),
		"entity_id":         entityID,
		"action_type":       sweepAuditString(action),
		"processing_status": sweepAuditString(status),
		"requested_by":      sweepAuditString(requestedBy),
		"requested_at":      sweepAuditTime(requestedAt),
		"checker_by":        sweepAuditString(checkerBy),
		"checker_at":        sweepAuditTime(checkerAt),
		"checker_comment":   sweepAuditString(checkerComment),
		"reason":            sweepAuditString(reason),
		"source":            sweepAuditSourceWorkflow,
	}, nil
}

func appendSweepAdditionalFileAudit(ctx context.Context, pgxPool *pgxpool.Pool, payload []map[string]interface{}, moduleKey, parentID string, excludedActions ...string) ([]map[string]interface{}, error) {
	rows, err := pgxPool.Query(ctx, `
		SELECT
			audit_id,
			parent_record_id,
			file_id,
			action_type,
			processing_status,
			requested_by,
			requested_at,
			checker_by,
			checker_at,
			checker_comment,
			reason
		FROM `+sweepAdditionalFileAuditTable+`
		WHERE module_key = $1
		  AND parent_record_id = $2
		ORDER BY requested_at ASC, audit_id ASC
	`, moduleKey, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	excluded := make(map[string]struct{}, len(excludedActions))
	for _, action := range excludedActions {
		if normalized := strings.ToUpper(strings.TrimSpace(action)); normalized != "" {
			excluded[normalized] = struct{}{}
		}
	}

	for rows.Next() {
		entry, err := scanSweepAdditionalFileAuditRow(rows, moduleKey)
		if err != nil {
			return nil, err
		}
		if _, skip := excluded[strings.ToUpper(strings.TrimSpace(fmt.Sprint(entry["raw_action_type"])))]; skip {
			continue
		}
		delete(entry, "raw_action_type")
		payload = append(payload, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return payload, nil
}

func scanSweepAdditionalFileAuditRow(row sweepAuditScanner, moduleKey string) (map[string]interface{}, error) {
	var auditID int64
	var parentRecordID, fileID, action, status, requestedBy, checkerBy, checkerComment, reason sql.NullString
	var requestedAt, checkerAt sql.NullTime

	if err := row.Scan(&auditID, &parentRecordID, &fileID, &action, &status, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
		return nil, err
	}

	actionType := sweepAuditString(action)
	if actionType == "CREATE" {
		actionType = "UPLOAD_FILE"
	}

	return map[string]interface{}{
		"audit_id":          fmt.Sprintf("file-%d", auditID),
		"entity_id":         sweepAuditString(fileID),
		"parent_record_id":  sweepAuditString(parentRecordID),
		"file_id":           sweepAuditString(fileID),
		"module_key":        moduleKey,
		"action_type":       actionType,
		"processing_status": sweepAuditString(status),
		"requested_by":      sweepAuditString(requestedBy),
		"requested_at":      sweepAuditTime(requestedAt),
		"checker_by":        sweepAuditString(checkerBy),
		"checker_at":        sweepAuditTime(checkerAt),
		"checker_comment":   sweepAuditString(checkerComment),
		"reason":            sweepAuditString(reason),
		"source":            sweepAuditSourceAdditionalFile,
		"raw_action_type":   sweepAuditString(action),
	}, nil
}

func sortSweepAuditPayload(payload []map[string]interface{}) {
	sort.SliceStable(payload, func(i, j int) bool {
		left := sweepAuditPayloadTime(payload[i]["requested_at"])
		right := sweepAuditPayloadTime(payload[j]["requested_at"])
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

func sweepAuditPayloadTime(value interface{}) time.Time {
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

func writeSweepAuditPayload(w http.ResponseWriter, payload []map[string]interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"audit_logs": payload,
	})
}

func sweepAuditString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func sweepAuditTime(value sql.NullTime) interface{} {
	if !value.Valid {
		return nil
	}
	return value.Time
}

func sweepAuditID(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
