package sweepconfig

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
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
				requested_ip,
				checker_by,
				checker_at,
				checker_ip,
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

		sortSweepAuditPayload(payload)

		approvalWorkflow := loadSweepConfigApprovalWorkflow(ctx, pgxPool, strings.TrimSpace(req.SweepID), api.GetUserIDFromCtx(ctx))

		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"audit_logs":        payload,
			"approval_workflow": approvalWorkflow,
		})
	}
}

// loadSweepConfigApprovalWorkflow finds the most recent CASH/SWEEP_CONFIG
// approval-matrix instance for this config and returns its rich detail for
// the ApprovalWorkflowViewer UI. Self-heals if no instance exists yet but a PENDING% audit row does.
func loadSweepConfigApprovalWorkflow(ctx context.Context, pgxPool *pgxpool.Pool, sweepID, viewerUserID string) interface{} {
	if sweepID == "" {
		return nil
	}

	var instanceID string
	_ = pgxPool.QueryRow(ctx, `
		SELECT instance_id
		FROM uam.approval_instance
		WHERE record_id = $1 AND module_code = 'CASH' AND is_deleted = false
		ORDER BY submitted_at DESC LIMIT 1`, sweepID,
	).Scan(&instanceID)

	if instanceID == "" {
		var pendingActionType, entityName, submittedByUserID, submittedByEmail string
		var sweepAmount, bufferAmount sql.NullFloat64
		scanErr := pgxPool.QueryRow(ctx, `
			SELECT a.actiontype, COALESCE(c.entity_name,''),
			       COALESCE(u.id,''), COALESCE(u.email,''),
			       c.sweep_amount, c.buffer_amount
			FROM cimplrcorpsaas.auditactionsweepconfiguration a
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = a.sweep_id
			LEFT JOIN public.users u ON u.employee_name = a.requested_by
			WHERE a.sweep_id = $1
			  AND a.processing_status LIKE 'PENDING%'
			ORDER BY a.requested_at DESC LIMIT 1`, sweepID,
		).Scan(&pendingActionType, &entityName, &submittedByUserID, &submittedByEmail, &sweepAmount, &bufferAmount)

		if scanErr == nil && pendingActionType != "" && submittedByUserID == "" {
			api.LogInfo("[SweepConfig] Self-heal skipped for %s: requester name did not resolve to a unique user_id", sweepID)
		}
		if scanErr == nil && pendingActionType != "" && submittedByUserID != "" {
			txType := map[string]string{
				"CREATE": "SWEEP_CONFIG_CREATE",
				"EDIT":   "SWEEP_CONFIG_EDIT",
				"DELETE": "SWEEP_CONFIG_DELETE",
			}[pendingActionType]
			if txType == "" {
				txType = "SWEEP_CONFIG_CREATE"
			}
			
			var swp, buf *float64
			if sweepAmount.Valid {
				v := sweepAmount.Float64
				swp = &v
			}
			if bufferAmount.Valid {
				v := bufferAmount.Float64
				buf = &v
			}
			var amount float64
			if swp != nil {
				amount = *swp
			} else if buf != nil {
				amount = *buf
			}

			newInstID, instErr := approvalengine.CreateInstance(ctx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:       "CASH",
				EntityCode:       entityName,
				TransactionType:  txType,
				RecordID:         sweepID,
				RecordTable:      "cimplrcorpsaas.sweepconfiguration",
				AuditTable:       "cimplrcorpsaas.auditactionsweepconfiguration",
				AuditIDColumn:    "sweep_id",
				ActionType:       pendingActionType,
				Amount:           amount,
				SubmittedBy:      submittedByUserID,
				SubmittedByEmail: submittedByEmail,
			})
			if instErr == nil {
				instanceID = newInstID
				api.LogInfo("[SweepConfig] Self-healed %s instance %s for sweep %s", txType, newInstID, sweepID)
			}
		}
	}

	if instanceID != "" {
		detail, err := approvalengine.GetRichInstanceDetail(ctx, pgxPool, instanceID, viewerUserID)
		if err == nil {
			return detail
		}
		api.LogError("[SweepConfig] GetRichInstanceDetail failed for instance %s: %v", instanceID, err)
	}

	return nil
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
				requested_ip,
				checker_by,
				checker_at,
				checker_ip,
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

		approvalWorkflow := loadSweepInitiationApprovalWorkflow(ctx, pgxPool, strings.TrimSpace(req.InitiationID), api.GetUserIDFromCtx(ctx))

		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"audit_logs":        payload,
			"approval_workflow": approvalWorkflow,
		})
	}
}

// loadSweepInitiationApprovalWorkflow finds the most recent CASH/SWEEP_INITIATION
// approval-matrix instance for this initiation and returns its rich detail for
// the ApprovalWorkflowViewer UI. Self-heals (mirrors fdBookingWorkbench's
// GetBookingDetail): if no instance exists yet but a PENDING% audit row does
// (e.g. record created before the engine was enabled), it creates the instance
// on the fly so the viewer always has something to show for pending records.
func loadSweepInitiationApprovalWorkflow(ctx context.Context, pgxPool *pgxpool.Pool, initiationID, viewerUserID string) interface{} {
	if initiationID == "" {
		return nil
	}

	var instanceID string
	_ = pgxPool.QueryRow(ctx, `
		SELECT instance_id
		FROM uam.approval_instance
		WHERE record_id = $1 AND module_code = 'CASH' AND is_deleted = false
		ORDER BY submitted_at DESC LIMIT 1`, initiationID,
	).Scan(&instanceID)

	if instanceID == "" {
		// requested_by on this audit table stores the requester's display name,
		// not a user_id — but uam.approval_instance.submitted_by has a hard FK
		// to public.users(id) (NOT NULL). Resolve the real user_id/email via a
		// best-effort join on employee_name; if it doesn't resolve to exactly
		// one user, skip self-heal rather than insert a value that fails the FK.
		var pendingActionType, entityName, submittedByUserID, submittedByEmail string
		var overriddenAmount, sweepAmount, bufferAmount sql.NullFloat64
		scanErr := pgxPool.QueryRow(ctx, `
			SELECT a.actiontype, COALESCE(c.entity_name,''),
			       COALESCE(u.id,''), COALESCE(u.email,''),
			       si.overridden_amount, c.sweep_amount, c.buffer_amount
			FROM cimplrcorpsaas.auditactionsweepinitiation a
			JOIN cimplrcorpsaas.sweep_initiation si ON si.initiation_id = a.initiation_id
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = si.sweep_id
			LEFT JOIN public.users u ON u.employee_name = a.requested_by
			WHERE a.initiation_id = $1
			  AND a.processing_status LIKE 'PENDING%'
			ORDER BY a.requested_at DESC LIMIT 1`, initiationID,
		).Scan(&pendingActionType, &entityName, &submittedByUserID, &submittedByEmail, &overriddenAmount, &sweepAmount, &bufferAmount)

		if scanErr == nil && pendingActionType != "" && submittedByUserID == "" {
			api.LogInfo("[SweepInitiation] Self-heal skipped for %s: requester name did not resolve to a unique user_id", initiationID)
		}
		if scanErr == nil && pendingActionType != "" && submittedByUserID != "" {
			txType := map[string]string{
				"CREATE": "SWEEP_INITIATION_CREATE",
				"EDIT":   "SWEEP_INITIATION_EDIT",
				"DELETE": "SWEEP_INITIATION_DELETE",
			}[pendingActionType]
			if txType == "" {
				txType = "SWEEP_INITIATION_CREATE"
			}
			var ovr, swp, buf *float64
			if overriddenAmount.Valid {
				v := overriddenAmount.Float64
				ovr = &v
			}
			if sweepAmount.Valid {
				v := sweepAmount.Float64
				swp = &v
			}
			if bufferAmount.Valid {
				v := bufferAmount.Float64
				buf = &v
			}
			newInstID, instErr := approvalengine.CreateInstance(ctx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:       "CASH",
				EntityCode:       entityName,
				TransactionType:  txType,
				RecordID:         initiationID,
				RecordTable:      "cimplrcorpsaas.sweep_initiation",
				AuditTable:       "cimplrcorpsaas.auditactionsweepinitiation",
				AuditIDColumn:    "initiation_id",
				ActionType:       pendingActionType,
				Amount:           resolveSweepInitiationAmount(ovr, swp, buf),
				SubmittedBy:      submittedByUserID,
				SubmittedByEmail: submittedByEmail,
			})
			if instErr != nil {
				api.LogError("[SweepInitiation] Self-heal CreateInstance for %s: %v", initiationID, instErr)
			} else if newInstID != "" {
				instanceID = newInstID
				api.LogInfo("[SweepInitiation] Self-heal: created instance %s for initiation %s", newInstID, initiationID)
			}
		}
	}

	if instanceID == "" {
		return nil
	}
	richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pgxPool, instanceID, viewerUserID)
	if richErr != nil {
		api.LogError("[SweepInitiation] GetRichInstanceDetail failed for instance=%s initiation=%s: %v", instanceID, initiationID, richErr)
		return nil
	}
	return richDetail
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
	var action, status, requestedBy, requestedIP, checkerBy, checkerIP, checkerComment, reason sql.NullString
	var requestedAt, checkerAt sql.NullTime

	if err := row.Scan(&auditID, &entityID, &action, &status, &requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"audit_id":          sweepAuditID(auditID),
		"entity_id":         entityID,
		"action_type":       sweepAuditString(action),
		"processing_status": sweepAuditString(status),
		"requested_by":      sweepAuditString(requestedBy),
		"requested_at":      sweepAuditTime(requestedAt),
		"requested_ip":      sweepAuditString(requestedIP),
		"checker_by":        sweepAuditString(checkerBy),
		"checker_at":        sweepAuditTime(checkerAt),
		"checker_ip":        sweepAuditString(checkerIP),
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
			requested_ip,
			checker_by,
			checker_at,
			checker_ip,
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
	var parentRecordID, fileID, action, status, requestedBy, requestedIP, checkerBy, checkerIP, checkerComment, reason sql.NullString
	var requestedAt, checkerAt sql.NullTime

	if err := row.Scan(&auditID, &parentRecordID, &fileID, &action, &status, &requestedBy, &requestedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason); err != nil {
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
		"requested_ip":      sweepAuditString(requestedIP),
		"checker_by":        sweepAuditString(checkerBy),
		"checker_at":        sweepAuditTime(checkerAt),
		"checker_ip":        sweepAuditString(checkerIP),
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
	api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
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
	return api.FormatAuditTimestampNullIST(value)
}

func sweepAuditID(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
