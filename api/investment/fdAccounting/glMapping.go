package fdAccounting

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AP-08 — GL Mapping & Accounting Rules (admin). Versioned per
// (entity_id, bank_id, event_type); one ACTIVE version per key.

const glMappingSelect = `
	SELECT
		m.mapping_id, m.mapping_version, m.entity_id, COALESCE(m.entity_name,'') AS entity_name,
		COALESCE(m.bank_id,'') AS bank_id, COALESCE(m.bank_name,'') AS bank_name,
		m.event_type, m.status, m.narration_template, m.rounding_decimals, m.rounding_method,
		COALESCE(m.remarks,'') AS remarks,
		COALESCE(m.created_by,'') AS created_by,
		COALESCE(TO_CHAR(m.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS created_at,
		COALESCE(m.activated_by,'') AS activated_by,
		COALESCE(TO_CHAR(m.activated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS activated_at,
		COALESCE(m.retired_by,'') AS retired_by,
		COALESCE(TO_CHAR(m.retired_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS retired_at,
		(SELECT COALESCE(json_agg(json_build_object(
			'line_id', ml.line_id, 'line_number', ml.line_number, 'leg', ml.leg,
			'gl_account_code', ml.gl_account_code, 'gl_account_name', COALESCE(ml.gl_account_name,''),
			'account_type', COALESCE(ml.account_type,''), 'amount_basis', ml.amount_basis,
			'cost_center', COALESCE(ml.cost_center,''), 'profit_center', COALESCE(ml.profit_center,''),
			'project_code', COALESCE(ml.project_code,''), 'tax_code', COALESCE(ml.tax_code,''),
			'line_narration', COALESCE(ml.line_narration,'')) ORDER BY ml.line_number), '[]'::json)
		 FROM ` + glMappingLineTable + ` ml WHERE ml.mapping_id = m.mapping_id) AS lines,
		COALESCE(l.actiontype,'') AS action_type,
		COALESCE(l.processing_status,'') AS processing_status,
		COALESCE(l.requested_by,'') AS requested_by,
		COALESCE(TO_CHAR(l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(l.checker_by,'') AS checker_by,
		COALESCE(TO_CHAR(l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(l.checker_comment,'') AS checker_comment,
		COALESCE(l.reason,'') AS reason
	FROM ` + glMappingTable + ` m
	LEFT JOIN LATERAL (
		SELECT a.* FROM ` + glMappingAudit + ` a
		WHERE a.mapping_id = m.mapping_id AND UPPER(COALESCE(a.actiontype,'')) <> 'UPLOAD_FILE'
		ORDER BY a.requested_at DESC LIMIT 1
	) l ON true
	WHERE m.is_deleted = false`

func glScope(scope ctxutil.RequestScope, args *[]interface{}) string {
	if scope.IsAdminOverride || len(scope.EntityIDs) == 0 {
		return ""
	}
	*args = append(*args, scope.EntityIDs)
	return " AND m.entity_id = ANY($" + strconv.Itoa(len(*args)) + "::text[])"
}

// ListGlMappings — POST /investment/fd/gl-mapping/list
func ListGlMappings(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntityID  string `json:"entity_id"`
			EventType string `json:"event_type"`
			Status    string `json:"status"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		ctx := r.Context()
		args := []interface{}{}
		q := glMappingSelect + glScope(ctxutil.FromContext(ctx), &args)
		if s := strings.TrimSpace(req.EntityID); s != "" {
			args = append(args, s)
			q += " AND m.entity_id = $" + strconv.Itoa(len(args))
		}
		if s := strings.TrimSpace(req.EventType); s != "" {
			args = append(args, s)
			q += " AND m.event_type = $" + strconv.Itoa(len(args))
		}
		if s := strings.TrimSpace(req.Status); s != "" {
			args = append(args, s)
			q += " AND m.status = $" + strconv.Itoa(len(args))
		}
		q += " ORDER BY m.entity_id, m.event_type, COALESCE(m.bank_id,''), m.mapping_version DESC"
		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDAccounting] ListGlMappings: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()
		out, err := scanRowsToMaps(rows)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}
		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
	}
}

// DetailGlMapping — POST /investment/fd/gl-mapping/detail
func DetailGlMapping(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			MappingID string `json:"mapping_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.MappingID) == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "mapping_id is required")
			return
		}
		ctx := r.Context()
		rows, err := pool.Query(ctx, glMappingSelect+" AND m.mapping_id = $1", strings.TrimSpace(req.MappingID))
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		out, _ := scanRowsToMaps(rows)
		rows.Close()
		if len(out) == 0 {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "mapping not found")
			return
		}
		aRows, err := pool.Query(ctx, `
			SELECT a.action_id::text AS action_id, a.mapping_id, COALESCE(a.actiontype,'') AS action_type,
			       COALESCE(a.processing_status,'') AS processing_status, COALESCE(a.reason,'') AS reason,
			       COALESCE(a.requested_by,'') AS requested_by,
			       COALESCE(TO_CHAR(a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
			       COALESCE(a.checker_by,'') AS checker_by,
			       COALESCE(TO_CHAR(a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
			       COALESCE(a.checker_comment,'') AS checker_comment
			FROM `+glMappingAudit+` a WHERE a.mapping_id = $1 ORDER BY a.requested_at DESC`, req.MappingID)
		var audit []map[string]interface{}
		if err == nil {
			audit, _ = scanRowsToMaps(aRows)
			aRows.Close()
		}
		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"mapping": out[0], "audit_history": audit})
	}
}

type glMappingLineInput struct {
	Leg           string `json:"leg"`
	GLAccountCode string `json:"gl_account_code"`
	GLAccountName string `json:"gl_account_name"`
	AccountType   string `json:"account_type"`
	AmountBasis   string `json:"amount_basis"`
	CostCenter    string `json:"cost_center"`
	ProfitCenter  string `json:"profit_center"`
	ProjectCode   string `json:"project_code"`
	TaxCode       string `json:"tax_code"`
	LineNarration string `json:"line_narration"`
}

// CreateGlMapping — POST /investment/fd/gl-mapping/create. Always creates a
// new version for the key (previous versions are never edited in place).
func CreateGlMapping(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntityID          string               `json:"entity_id"`
			EntityName        string               `json:"entity_name"`
			BankID            string               `json:"bank_id"`
			BankName          string               `json:"bank_name"`
			EventType         string               `json:"event_type"`
			NarrationTemplate string               `json:"narration_template"`
			RoundingDecimals  int                  `json:"rounding_decimals"`
			RoundingMethod    string               `json:"rounding_method"`
			Remarks           string               `json:"remarks"`
			Lines             []glMappingLineInput `json:"lines"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.EntityID, req.EventType = strings.TrimSpace(req.EntityID), strings.TrimSpace(req.EventType)
		if req.EntityID == "" || req.EventType == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "entity_id and event_type are required")
			return
		}
		if strings.TrimSpace(req.NarrationTemplate) == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "narration_template is required")
			return
		}
		hasDr, hasCr := false, false
		for i, l := range req.Lines {
			leg := strings.ToUpper(strings.TrimSpace(l.Leg))
			if leg != "DEBIT" && leg != "CREDIT" {
				fdclosingcommon.RespondError(w, http.StatusBadRequest, fmt.Sprintf("line %d: leg must be DEBIT or CREDIT", i+1))
				return
			}
			if strings.TrimSpace(l.GLAccountCode) == "" {
				fdclosingcommon.RespondError(w, http.StatusBadRequest, fmt.Sprintf("line %d: gl_account_code is required", i+1))
				return
			}
			hasDr = hasDr || leg == "DEBIT"
			hasCr = hasCr || leg == "CREDIT"
		}
		if !hasDr || !hasCr {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "mapping needs at least one DEBIT and one CREDIT line")
			return
		}
		if req.RoundingDecimals <= 0 {
			req.RoundingDecimals = 2
		}
		rm := strings.ToUpper(strings.TrimSpace(req.RoundingMethod))
		if rm != "FLOOR" && rm != "CEIL" {
			rm = "ROUND"
		}
		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var version int
		_ = tx.QueryRow(ctx, `
			SELECT COALESCE(MAX(mapping_version),0)+1 FROM `+glMappingTable+`
			WHERE entity_id = $1 AND COALESCE(bank_id,'') = $2 AND event_type = $3`,
			req.EntityID, strings.TrimSpace(req.BankID), req.EventType).Scan(&version)
		if version == 0 {
			version = 1
		}
		var mappingID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO `+glMappingTable+` (mapping_version, entity_id, entity_name, bank_id, bank_name, event_type, status,
				narration_template, rounding_decimals, rounding_method, remarks, created_by)
			VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),$6,'DRAFT',$7,$8,$9,NULLIF($10,''),$11)
			RETURNING mapping_id`,
			version, req.EntityID, strings.TrimSpace(req.EntityName), strings.TrimSpace(req.BankID), strings.TrimSpace(req.BankName),
			req.EventType, strings.TrimSpace(req.NarrationTemplate), req.RoundingDecimals, rm, strings.TrimSpace(req.Remarks),
			api.SystemIfBlank(actor.Email)).Scan(&mappingID); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "insert mapping: "+err.Error())
			return
		}
		for i, l := range req.Lines {
			basis := strings.ToUpper(strings.TrimSpace(l.AmountBasis))
			if basis == "" {
				basis = "FULL_AMOUNT"
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO `+glMappingLineTable+` (mapping_id, line_number, leg, gl_account_code, gl_account_name, account_type,
					amount_basis, cost_center, profit_center, project_code, tax_code, line_narration)
				VALUES ($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''),$7,NULLIF($8,''),NULLIF($9,''),NULLIF($10,''),NULLIF($11,''),NULLIF($12,''))`,
				mappingID, i+1, strings.ToUpper(strings.TrimSpace(l.Leg)), strings.TrimSpace(l.GLAccountCode), strings.TrimSpace(l.GLAccountName),
				strings.TrimSpace(l.AccountType), basis, l.CostCenter, l.ProfitCenter, l.ProjectCode, l.TaxCode, l.LineNarration); err != nil {
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, "insert mapping line: "+err.Error())
				return
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO `+glMappingAudit+` (mapping_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,now(),$4)`,
			mappingID, "Version "+strconv.Itoa(version)+" for "+req.EventType, api.SystemIfBlank(actor.Email),
			api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		fdclosingcommon.RespondSuccess(w, "GL mapping submitted for approval", map[string]interface{}{
			"mapping_id": mappingID, "mapping_version": version, "status": "DRAFT",
		})

		mid, entity, email, uid := mappingID, req.EntityID, actor.Email, actor.UserID
		runEngineInBackground(func(bgCtx context.Context) {
			if _, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode: moduleCode, EntityCode: entity, TransactionType: txGlMappingCreate,
				RecordID: mid, RecordTable: glMappingTable, AuditTable: glMappingAudit, AuditIDColumn: "mapping_id",
				ActionType: "CREATE", SubmittedBy: uid, SubmittedByEmail: email, RequirePinnedMatrix: true,
			}); err != nil {
				api.LogError("[FDAccounting] CreateInstance failed for GL mapping %s: %v", mid, err)
			}
		})
	}
}

type glActionRequest struct {
	MappingID  string   `json:"mapping_id"`
	MappingIDs []string `json:"mapping_ids"`
	Comment    string   `json:"comment"`
}

// ApproveGlMapping / RejectGlMapping — same engine-then-direct gating as journals.
func ApproveGlMapping(pool *pgxpool.Pool) http.HandlerFunc {
	return glAct(pool, approvalengine.ActionApproved)
}
func RejectGlMapping(pool *pgxpool.Pool) http.HandlerFunc {
	return glAct(pool, approvalengine.ActionRejected)
}

func glAct(pool *pgxpool.Pool, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req glActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := mergeIDs(req.MappingID, req.MappingIDs)
		if len(ids) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "mapping_id or mapping_ids is required")
			return
		}
		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		acted := 0
		var errs []string
		for _, id := range ids {
			res, err := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: moduleCode, RecordID: id, UserID: actor.UserID, UserEmail: actor.Email, Action: action, Comment: req.Comment,
			})
			if err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			if !res.Acted {
				if !res.CancelledStale && res.Reason != "" {
					errs = append(errs, id+": "+res.Reason)
					continue
				}
				tag, uerr := pool.Exec(ctx, `
					UPDATE `+glMappingAudit+` SET processing_status = $2, checker_by = $3, checker_at = now(), checker_comment = NULLIF($4,''), checker_ip = $5
					WHERE action_id = (SELECT action_id FROM `+glMappingAudit+` WHERE mapping_id = $1 AND processing_status LIKE 'PENDING%' ORDER BY requested_at DESC LIMIT 1)`,
					id, action, api.SystemIfBlank(actor.Email), req.Comment, api.SystemIfBlank(api.ClientIPFromContext(ctx)))
				if uerr != nil || tag.RowsAffected() == 0 {
					errs = append(errs, id+": no pending mapping action found")
					continue
				}
			}
			acted++
			if action == approvalengine.ActionApproved {
				_, _ = pool.Exec(ctx, `UPDATE `+glMappingTable+` SET status = 'APPROVED' WHERE mapping_id = $1 AND status = 'DRAFT'`, id)
			}
		}
		payload := map[string]interface{}{"acted": acted, "errors": errs, "checker": actor.Email}
		if acted == 0 && len(errs) > 0 {
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No mappings were actioned", payload)
			return
		}
		fdclosingcommon.RespondSuccess(w, "Mapping(s) "+strings.ToLower(action), payload)
	}
}

// ActivateGlMapping — APPROVED → ACTIVE; retires the current ACTIVE version for the same key.
func ActivateGlMapping(pool *pgxpool.Pool) http.HandlerFunc {
	return glTransition(pool, "ACTIVATE", "APPROVED", "ACTIVE")
}

// RetireGlMapping — ACTIVE → RETIRED.
func RetireGlMapping(pool *pgxpool.Pool) http.HandlerFunc {
	return glTransition(pool, "RETIRE", "ACTIVE", "RETIRED")
}

func glTransition(pool *pgxpool.Pool, actionType, from, to string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req glActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.MappingID) == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "mapping_id is required")
			return
		}
		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var entityID, bankID, eventType, status string
		err = tx.QueryRow(ctx, `SELECT entity_id, COALESCE(bank_id,''), event_type, status FROM `+glMappingTable+` WHERE mapping_id = $1 AND is_deleted = false FOR UPDATE`,
			req.MappingID).Scan(&entityID, &bankID, &eventType, &status)
		if err == pgx.ErrNoRows {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "mapping not found")
			return
		}
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		if status != from {
			fdclosingcommon.RespondError(w, http.StatusConflict, "mapping is "+status+"; "+actionType+" requires "+from)
			return
		}
		email := api.SystemIfBlank(actor.Email)
		if to == "ACTIVE" {
			if _, err := tx.Exec(ctx, `
				UPDATE `+glMappingTable+` SET status = 'RETIRED', retired_by = $4, retired_at = now()
				WHERE entity_id = $1 AND COALESCE(bank_id,'') = $2 AND event_type = $3 AND status = 'ACTIVE' AND is_deleted = false`,
				entityID, bankID, eventType, email); err != nil {
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `UPDATE `+glMappingTable+` SET status = 'ACTIVE', activated_by = $2, activated_at = now() WHERE mapping_id = $1`, req.MappingID, email); err != nil {
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
				return
			}
		} else {
			if _, err := tx.Exec(ctx, `UPDATE `+glMappingTable+` SET status = 'RETIRED', retired_by = $2, retired_at = now() WHERE mapping_id = $1`, req.MappingID, email); err != nil {
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
				return
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO `+glMappingAudit+` (mapping_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip, checker_by, checker_at, checker_ip)
			VALUES ($1,$2,'COMPLETED',NULLIF($3,''),$4,now(),$5,$4,now(),$5)`,
			req.MappingID, actionType, req.Comment, email, api.SystemIfBlank(api.ClientIPFromContext(ctx))); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}
		fdclosingcommon.RespondSuccess(w, "Mapping "+strings.ToLower(to), map[string]interface{}{"mapping_id": req.MappingID, "status": to})
	}
}

// ResolveActiveMapping is the hook producers will call in phase 3: returns the
// ACTIVE mapping id for (entity, bank, event) — bank-specific first, then
// entity-wide — or "" when none exists (caller falls back to built-in literals).
func ResolveActiveMapping(ctx context.Context, exec dbExec, entityID, bankID, eventType string) (string, error) {
	var id string
	err := exec.QueryRow(ctx, `
		SELECT mapping_id FROM `+glMappingTable+`
		WHERE entity_id = $1 AND event_type = $3 AND status = 'ACTIVE' AND is_deleted = false
		  AND (bank_id = $2 OR bank_id IS NULL)
		ORDER BY (bank_id IS NULL) ASC, mapping_version DESC LIMIT 1`, entityID, bankID, eventType).Scan(&id)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return id, err
}
