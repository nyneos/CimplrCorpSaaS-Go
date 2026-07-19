package counterpartyHub

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/internal/dependency"
	"CimplrCorpSaas/internal/logger"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Request types ─────────────────────────────────────────────────────────────

type CcpCsdInput struct {
	CounterpartyID      string `json:"counterparty_id"`
	EntityCode          string `json:"entity_code"`
	EntitySubType       string `json:"entity_sub_type"`
	LEI                 string `json:"lei"`
	RegulatoryBody      string `json:"regulatory_body,omitempty"`
	ParticipantID       string `json:"participant_id,omitempty"`
	ClearingAcctKMSRef  string `json:"clearing_acct_kms_ref"`
	MarginCallFrequency string `json:"margin_call_frequency,omitempty"`
}

func validateCcpCsdInput(inp CcpCsdInput) error {
	if strings.TrimSpace(inp.CounterpartyID) == "" {
		return errors.New("counterparty_id is required")
	}
	if strings.TrimSpace(inp.EntityCode) == "" {
		return errors.New("entity_code is required")
	}
	validSubTypes := map[string]bool{"CCP": true, "CSD": true, "SETTLEMENT_AGENT": true, "CUSTODIAN": true}
	if !validSubTypes[strings.ToUpper(inp.EntitySubType)] {
		return errors.New("entity_sub_type must be one of CCP, CSD, SETTLEMENT_AGENT, CUSTODIAN")
	}
	// LEI is mandatory for CCP/CSD
	if strings.TrimSpace(inp.LEI) == "" {
		return errors.New("lei is required for CCP/CSD")
	}
	if err := validateLEI(inp.LEI); err != nil {
		return err
	}
	if strings.TrimSpace(inp.RegulatoryBody) == "" {
		return errors.New("regulatory_body is required")
	}
	if strings.TrimSpace(inp.ParticipantID) == "" {
		return errors.New("participant_id is required")
	}
	if strings.TrimSpace(inp.ClearingAcctKMSRef) == "" {
		return errors.New("clearing_acct_kms_ref is required")
	}
	if err := validateKMSPath("clearing_acct_kms_ref", inp.ClearingAcctKMSRef); err != nil {
		return err
	}
	if inp.MarginCallFrequency != "" {
		validMCF := map[string]bool{"INTRADAY": true, "END_OF_DAY": true, "BOTH": true}
		if !validMCF[strings.ToUpper(inp.MarginCallFrequency)] {
			return errors.New("margin_call_frequency must be one of INTRADAY, END_OF_DAY, BOTH")
		}
	}
	return nil
}

// ── CreateCcpCsd ──────────────────────────────────────────────────────────────

func CreateCcpCsd(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			CcpCsdInput
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if err := validateCcpCsdInput(req.CcpCsdInput); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		var ccpID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO apibox.ccp_csd_master (
				counterparty_id, entity_code, entity_sub_type, lei, regulatory_body,
				participant_id, clearing_acct_kms_ref, margin_call_frequency
			) VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''))
			RETURNING ccp_csd_id`,
			req.CounterpartyID, req.EntityCode, strings.ToUpper(req.EntitySubType),
			req.LEI, req.RegulatoryBody, req.ParticipantID, req.ClearingAcctKMSRef,
			req.MarginCallFrequency,
		).Scan(&ccpID); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "CCP/CSD insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_ccp_csd (ccp_csd_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, ccpID, userEmail); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(id, uID, uEmail string) {
			defer func() { recover() }()
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer bgCancel()
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "CCP_CSD_CREATE",
				RecordID: id, RecordTable: "apibox.ccp_csd_master",
				AuditTable: "apibox.audit_ccp_csd", AuditIDColumn: "ccp_csd_id",
				ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(ccpID, req.UserID, userEmail)

		go func(id, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/counterparty-hub/ccp-csd/create", id,
				map[string]interface{}{"record_id": id, "event": "CCP_CSD_CREATED", "actor_email": uEmail},
			)
		}(ccpID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true, "ccp_csd_id": ccpID, "requested": userEmail,
		})
		api.LogInfo("CCP/CSD created: ID=%s by=%s", ccpID, userEmail)
	}
}

// ── CreateCcpCsdBulk ──────────────────────────────────────────────────────────

func CreateCcpCsdBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string        `json:"user_id"`
			Rows   []CcpCsdInput `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRowsProvided)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var errList []map[string]interface{}
		var inserted []map[string]interface{}

		for i, row := range req.Rows {
			if err := validateCcpCsdInput(row); err != nil {
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			id, err := insertCcpCsdInTx(ctx, pgxPool, row, userEmail)
			if err != nil {
				msg, _ := getUserFriendlyCounterpartyError(err, "Insert failed")
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: msg})
				continue
			}
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "ccp_csd_id": id})
		}

		api.RespondWithPayload(w, len(inserted) > 0, "", append(inserted, errList...))
	}
}

func insertCcpCsdRow(ctx context.Context, tx pgx.Tx, inp CcpCsdInput, userEmail string) (string, error) {
	var id string
	if err := tx.QueryRow(ctx, `
		INSERT INTO apibox.ccp_csd_master (
			counterparty_id, entity_code, entity_sub_type, lei, regulatory_body,
			participant_id, clearing_acct_kms_ref, margin_call_frequency
		) VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''))
		RETURNING ccp_csd_id`,
		inp.CounterpartyID, inp.EntityCode, strings.ToUpper(inp.EntitySubType),
		inp.LEI, inp.RegulatoryBody, inp.ParticipantID, inp.ClearingAcctKMSRef, inp.MarginCallFrequency,
	).Scan(&id); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO apibox.audit_ccp_csd (ccp_csd_id, action_type, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, id, userEmail); err != nil {
		return "", err
	}
	return id, nil
}

func insertCcpCsdInTx(ctx context.Context, pool *pgxpool.Pool, inp CcpCsdInput, userEmail string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id, err := insertCcpCsdRow(ctx, tx, inp, userEmail)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// ── UpdateCcpCsd ──────────────────────────────────────────────────────────────

func UpdateCcpCsd(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                 `json:"user_id"`
			CcpCsdID string                 `json:"ccp_csd_id"`
			Fields   map[string]interface{} `json:"fields"`
			Reason   string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.CcpCsdID == "" || len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "ccp_csd_id and fields are required")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		var oldSubType, oldLEI, oldParticipantID string
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(entity_sub_type,''), COALESCE(lei,''), COALESCE(participant_id,'')
			FROM apibox.ccp_csd_master WHERE ccp_csd_id=$1 FOR UPDATE`, req.CcpCsdID,
		).Scan(&oldSubType, &oldLEI, &oldParticipantID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "CCP/CSD record not found")
				return
			}
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		allowed := map[string]bool{"entity_sub_type": true, "lei": true, "regulatory_body": true, "participant_id": true, "clearing_acct_kms_ref": true, "margin_call_frequency": true}
		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if !allowed[k] {
				continue
			}
			if k == "clearing_acct_kms_ref" {
				if s, ok := v.(string); ok {
					if err := validateKMSPath("clearing_acct_kms_ref", s); err != nil {
						api.RespondWithError(w, http.StatusBadRequest, err.Error())
						return
					}
				}
			}
			sets = append(sets, fmt.Sprintf("%s=$%d", k, pos))
			args = append(args, v)
			pos++
		}
		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields")
			return
		}
		args = append(args, req.CcpCsdID)
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE apibox.ccp_csd_master SET %s WHERE ccp_csd_id=$%d", strings.Join(sets, ","), pos), args...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_ccp_csd (
				ccp_csd_id, action_type, processing_status, reason, requested_by, requested_at,
				old_entity_sub_type, old_lei, old_participant_id
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6)`,
			req.CcpCsdID, req.Reason, userEmail, oldSubType, oldLEI, oldParticipantID); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "ccp_csd_id": req.CcpCsdID})
	}
}

// ── BulkApproveCcpCsd ─────────────────────────────────────────────────────────

func BulkApproveCcpCsd(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			CcpCsdIDs []string `json:"ccp_csd_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE approvals only
		pendingDel := dependency.GetPendingDeleteIDs(r.Context(), pgxPool, "apibox.audit_ccp_csd", "ccp_csd_id", req.CcpCsdIDs)
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "masterccpcsd", pendingDel)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.CcpCsdIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.CcpCsdIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.CcpCsdIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoCCPCSDIDsProvided)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		rows, err := tx.Query(ctx, `
			SELECT audit_id, ccp_csd_id, action_type, requested_by
			FROM apibox.audit_ccp_csd
			WHERE ccp_csd_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
			FOR UPDATE`, req.CcpCsdIDs)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch audits failed")
			api.RespondWithError(w, status, msg)
			return
		}
		type ar struct{ AuditID, ID, ActionType, ReqBy string }
		var audits []ar
		for rows.Next() {
			var a ar
			if err := rows.Scan(&a.AuditID, &a.ID, &a.ActionType, &a.ReqBy); err != nil {
				logger.LogError("approveCcpCsd: audits scan failed: %v", err)
			}
			audits = append(audits, a)
		}
		rows.Close()

		var success, errList []map[string]interface{}
		for _, a := range audits {
			if a.ReqBy == userEmail {
				errList = append(errList, map[string]interface{}{"ccp_csd_id": a.ID, constants.ValueSuccess: false, constants.ValueError: "You cannot approve your own submission"})
				continue
			}
			if strings.ToUpper(a.ActionType) == "DELETE" {
				if _, err := tx.Exec(ctx, `UPDATE apibox.ccp_csd_master SET is_deleted=true WHERE ccp_csd_id=$1`, a.ID); err != nil {
					logger.LogError("approveCcpCsd: delete ccp_csd_master exec failed: %v", err)
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE apibox.audit_ccp_csd SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE audit_id=$3`, userEmail, req.Comment, a.AuditID); err != nil {
				errList = append(errList, map[string]interface{}{"ccp_csd_id": a.ID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "ccp_csd_id": a.ID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, len(success) > 0, "", append(success, errList...))
	}
}

// ── BulkRejectCcpCsd ──────────────────────────────────────────────────────────

func BulkRejectCcpCsd(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			CcpCsdIDs []string `json:"ccp_csd_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.CcpCsdIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoCCPCSDIDsProvided)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		res, err := tx.Exec(ctx, `
			UPDATE apibox.audit_ccp_csd SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE ccp_csd_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'`,
			userEmail, req.Comment, req.CcpCsdIDs)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Rejection failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, res.RowsAffected() > 0, "", map[string]interface{}{constants.ValueSuccess: true, "rejected_count": res.RowsAffected()})
	}
}

// ── BulkDeleteCcpCsd ──────────────────────────────────────────────────────────

func BulkDeleteCcpCsd(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			CcpCsdIDs []string `json:"ccp_csd_ids"`
			Reason    string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "masterccpcsd", req.CcpCsdIDs)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.CcpCsdIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.CcpCsdIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.CcpCsdIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoCCPCSDIDsProvided)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		var auditVals []string
		var auditArgs []interface{}
		argIdx := 1
		for _, id := range req.CcpCsdIDs {
			auditVals = append(auditVals, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", argIdx, argIdx+1, argIdx+2))
			auditArgs = append(auditArgs, id, req.Reason, userEmail)
			argIdx += 3
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO apibox.audit_ccp_csd (ccp_csd_id, action_type, processing_status, reason, requested_by, requested_at)
			VALUES %s`, strings.Join(auditVals, ",")), auditArgs...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "submitted_count": len(req.CcpCsdIDs)})
	}
}

// ── GetCcpCsdAll ──────────────────────────────────────────────────────────────

func GetCcpCsdAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.ccp_csd_id)
					a.ccp_csd_id, a.audit_id, a.action_type, a.processing_status,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at,
					a.checker_comment, a.reason,
					a.old_entity_sub_type, a.old_lei, a.old_participant_id
				FROM apibox.audit_ccp_csd a
				ORDER BY a.ccp_csd_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT ccp_csd_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS edited_at
				FROM apibox.audit_ccp_csd
				GROUP BY ccp_csd_id
			)
			SELECT
				c.ccp_csd_id, c.counterparty_id, c.entity_code, c.entity_sub_type, c.lei,
				c.regulatory_body, c.participant_id, c.clearing_acct_kms_ref,
				COALESCE(c.margin_call_frequency,'') AS margin_call_frequency,
				COALESCE(c.is_deleted,false) AS is_deleted,
				COALESCE(l.audit_id::text,'') AS audit_id,
				COALESCE(l.action_type,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.requested_by,'') AS requested_by,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(l.old_entity_sub_type,'') AS old_entity_sub_type,
				COALESCE(l.old_lei,'') AS old_lei,
				COALESCE(l.old_participant_id,'') AS old_participant_id,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at
			FROM apibox.ccp_csd_master c
			LEFT JOIN latest_audit l ON l.ccp_csd_id = c.ccp_csd_id
			LEFT JOIN history h ON h.ccp_csd_id = c.ccp_csd_id
			WHERE COALESCE(c.is_deleted,false) = false
			ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamp),COALESCE(l.checker_at,'1970-01-01'::timestamp)) DESC`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()
		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
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
			out = append(out, row)
		}
		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"rows": out})
	}
}

// ── GetCcpCsdAuditHistory / GetCcpCsdApprovedActive / GetCcpCsdDetail ─────────

func GetCcpCsdAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.URL.Query().Get("ccp_csd_id")
		q := `SELECT audit_id, ccp_csd_id, action_type, processing_status, reason,
			         requested_by, TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
			         COALESCE(checker_by,''), COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
			         COALESCE(checker_comment,'')
			  FROM apibox.audit_ccp_csd`
		var args []interface{}
		if id != "" {
			q += " WHERE ccp_csd_id=$1 ORDER BY requested_at DESC"
			args = []interface{}{id}
		} else {
			q += " ORDER BY requested_at DESC LIMIT 500"
		}
		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
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
			out = append(out, row)
		}
		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"rows": out})
	}
}

func GetCcpCsdApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cpID := r.URL.Query().Get("counterparty_id")
		q := `SELECT c.ccp_csd_id, c.counterparty_id, c.entity_code, c.entity_sub_type, c.lei, c.regulatory_body, c.participant_id
			  FROM apibox.ccp_csd_master c
			  INNER JOIN apibox.counterparty_master cp ON cp.counterparty_id=c.counterparty_id
			  WHERE cp.status='ACTIVE' AND COALESCE(cp.is_deleted,false)=false AND COALESCE(c.is_deleted,false)=false`
		var args []interface{}
		if cpID != "" {
			q += " AND c.counterparty_id=$1"
			args = append(args, cpID)
		}
		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()
		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
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
			out = append(out, row)
		}
		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{"rows": out})
	}
}

func GetCcpCsdDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.URL.Query().Get("ccp_csd_id")
		if id == "" {
			api.RespondWithError(w, http.StatusBadRequest, "ccp_csd_id is required")
			return
		}
		q := `SELECT ccp_csd_id, counterparty_id, entity_code, entity_sub_type, lei, regulatory_body,
			         participant_id, clearing_acct_kms_ref, COALESCE(margin_call_frequency,''), COALESCE(is_deleted,false)
			  FROM apibox.ccp_csd_master WHERE ccp_csd_id=$1`
		var cid, cpid, ecode, esubtype, lei, regBody, participantID, kmsRef, mcf string
		var isDeleted bool
		if err := pgxPool.QueryRow(ctx, q, id).Scan(&cid, &cpid, &ecode, &esubtype, &lei, &regBody, &participantID, &kmsRef, &mcf, &isDeleted); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "CCP/CSD record not found")
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"ccp_csd_id": cid, "counterparty_id": cpid, "entity_code": ecode, "entity_sub_type": esubtype,
			"lei": lei, "regulatory_body": regBody, "participant_id": participantID,
			"clearing_acct_kms_ref": kmsRef, "margin_call_frequency": mcf, "is_deleted": isDeleted,
		})
	}
}
