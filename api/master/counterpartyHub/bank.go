package counterpartyHub

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Request types ─────────────────────────────────────────────────────────────

type BankInput struct {
	CounterpartyID string `json:"counterparty_id"`
	BankCode       string `json:"bank_code"`
	BankName       string `json:"bank_name"`
	BankType       string `json:"bank_type"`
	RoutingNumber  string `json:"routing_number,omitempty"`
	EntityCode     string `json:"entity_code"`
}

var validBankTypes = map[string]bool{
	"COMMERCIAL": true, "CENTRAL_BANK": true, "CORRESPONDENT": true, "CUSTODIAN": true,
}

var bankCodeRegex = validBankCodeRegex()

func validBankCodeRegex() func(string) bool {
	// ^[A-Z0-9]{2,20}$ — same charset as counterparty code but without _ and -
	return func(s string) bool {
		if len(s) < 2 || len(s) > 20 {
			return false
		}
		for _, c := range s {
			if !((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
				return false
			}
		}
		return true
	}
}

func validateBankInput(inp BankInput) error {
	if strings.TrimSpace(inp.CounterpartyID) == "" {
		return errors.New("counterparty_id is required")
	}
	return validateBankFields(inp)
}

func validateBankFields(inp BankInput) error {
	code := strings.ToUpper(strings.TrimSpace(inp.BankCode))
	if code == "" {
		return errors.New("bank_code is required")
	}
	if !bankCodeRegex(code) {
		return errors.New("bank_code must be 2–20 uppercase alphanumeric characters")
	}
	if err := validateNoBIC(code); err != nil {
		return err
	}

	name := strings.TrimSpace(inp.BankName)
	if len(name) < 2 || len(name) > 200 {
		return errors.New("bank_name must be 2–200 characters")
	}
	if err := validateNoBIC(name); err != nil {
		return err
	}

	if !validBankTypes[strings.ToUpper(inp.BankType)] {
		return errors.New("bank_type must be one of COMMERCIAL, CENTRAL_BANK, CORRESPONDENT, CUSTODIAN")
	}
	if strings.TrimSpace(inp.EntityCode) == "" {
		return errors.New("entity_code is required")
	}
	if inp.RoutingNumber != "" {
		if err := validateNoBIC(inp.RoutingNumber); err != nil {
			return err
		}
	}
	return nil
}

// ── insertBankRow ─────────────────────────────────────────────────────────────
// Writes bank_master + audit into an existing tx. Caller owns commit/rollback.

func insertBankRow(ctx context.Context, tx pgx.Tx, inp BankInput, userEmail string) (string, error) {
	var id string
	if err := tx.QueryRow(ctx, `
		INSERT INTO apibox.bank_master (
			counterparty_id, bank_code, bank_name, bank_type, routing_number, entity_code
		) VALUES ($1,$2,$3,$4,NULLIF($5,''),$6)
		RETURNING bank_id`,
		inp.CounterpartyID, strings.ToUpper(inp.BankCode), inp.BankName,
		strings.ToUpper(inp.BankType), inp.RoutingNumber, inp.EntityCode,
	).Scan(&id); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO apibox.audit_bank_master (bank_id, action_type, processing_status, requested_by, requested_at)
		VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, id, userEmail); err != nil {
		return "", err
	}
	return id, nil
}

func insertBankInTx(ctx context.Context, pool *pgxpool.Pool, inp BankInput, userEmail string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id, err := insertBankRow(ctx, tx, inp, userEmail)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// ── CreateBank ────────────────────────────────────────────────────────────────
// Standalone: adds bank details to an EXISTING counterparty_master record.

func CreateBank(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			BankInput
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if err := validateBankInput(req.BankInput); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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

		bankID, err := insertBankRow(ctx, tx, req.BankInput, userEmail)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Bank insert failed")
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
			bgCtx := context.Background()
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "BANK_CREATE",
				RecordID: id, RecordTable: "apibox.bank_master",
				AuditTable: "apibox.audit_bank_master", AuditIDColumn: "bank_id",
				ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(bankID, req.UserID, userEmail)

		go func(id, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/counterparty-hub/bank/create", id,
				map[string]interface{}{"record_id": id, "event": "BANK_CREATED", "actor_email": uEmail},
			)
		}(bankID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true, "bank_id": bankID, "requested": userEmail,
		})
		api.LogInfo("Bank created: ID=%s by=%s", bankID, userEmail)
	}
}

// ── CreateBankBulk ────────────────────────────────────────────────────────────

func CreateBankBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string      `json:"user_id"`
			Rows   []BankInput `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRowsProvided)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var errList []map[string]interface{}
		var inserted []map[string]interface{}

		for i, row := range req.Rows {
			if err := validateBankInput(row); err != nil {
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			id, err := insertBankInTx(ctx, pgxPool, row, userEmail)
			if err != nil {
				msg, _ := getUserFriendlyCounterpartyError(err, "Insert failed")
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: msg})
				continue
			}
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "bank_id": id})
		}

		api.RespondWithPayload(w, len(inserted) > 0, "", append(inserted, errList...))
	}
}

// ── UpdateBank ────────────────────────────────────────────────────────────────

func UpdateBank(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                 `json:"user_id"`
			BankID string                 `json:"bank_id"`
			Fields map[string]interface{} `json:"fields"`
			Reason string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.BankID == "" || len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "bank_id and fields are required")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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

		var oldBankCode, oldBankName, oldBankType, oldEntityCode string
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(bank_code,''), COALESCE(bank_name,''), COALESCE(bank_type,''), COALESCE(entity_code,'')
			FROM apibox.bank_master WHERE bank_id=$1 FOR UPDATE`, req.BankID,
		).Scan(&oldBankCode, &oldBankName, &oldBankType, &oldEntityCode); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Bank record not found")
				return
			}
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		allowed := map[string]bool{"bank_name": true, "bank_type": true, "routing_number": true, "entity_code": true}
		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if !allowed[k] {
				continue
			}
			if k == "bank_name" {
				if s, ok := v.(string); ok {
					if err := validateNoBIC(s); err != nil {
						api.RespondWithError(w, http.StatusBadRequest, err.Error())
						return
					}
				}
			}
			if k == "bank_type" {
				if s, ok := v.(string); ok && !validBankTypes[strings.ToUpper(s)] {
					api.RespondWithError(w, http.StatusBadRequest, "bank_type must be one of COMMERCIAL, CENTRAL_BANK, CORRESPONDENT, CUSTODIAN")
					return
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
		args = append(args, req.BankID)
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE apibox.bank_master SET %s WHERE bank_id=$%d", strings.Join(sets, ","), pos), args...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_bank_master (
				bank_id, action_type, processing_status, reason, requested_by, requested_at,
				old_bank_code, old_bank_name, old_bank_type, old_entity_code
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6,$7)`,
			req.BankID, req.Reason, userEmail, oldBankCode, oldBankName, oldBankType, oldEntityCode); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "bank_id": req.BankID})
	}
}

// ── BulkApproveBank ───────────────────────────────────────────────────────────

func BulkApproveBank(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			BankIDs []string `json:"bank_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.BankIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No bank_ids provided")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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
			SELECT audit_id, bank_id, action_type, requested_by
			FROM apibox.audit_bank_master
			WHERE bank_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
			FOR UPDATE`, req.BankIDs)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch audits failed")
			api.RespondWithError(w, status, msg)
			return
		}
		type ar struct{ AuditID, ID, ActionType, ReqBy string }
		var audits []ar
		for rows.Next() {
			var a ar
			rows.Scan(&a.AuditID, &a.ID, &a.ActionType, &a.ReqBy)
			audits = append(audits, a)
		}
		rows.Close()

		var success, errList []map[string]interface{}
		for _, a := range audits {
			if a.ReqBy == userEmail {
				errList = append(errList, map[string]interface{}{"bank_id": a.ID, constants.ValueSuccess: false, constants.ValueError: "You cannot approve your own submission"})
				continue
			}
			if strings.ToUpper(a.ActionType) == "DELETE" {
				tx.Exec(ctx, `UPDATE apibox.bank_master SET is_deleted=true WHERE bank_id=$1`, a.ID)
			}
			if _, err := tx.Exec(ctx, `UPDATE apibox.audit_bank_master SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE audit_id=$3`, userEmail, req.Comment, a.AuditID); err != nil {
				errList = append(errList, map[string]interface{}{"bank_id": a.ID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "bank_id": a.ID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, len(success) > 0, "", append(success, errList...))
	}
}

// ── BulkRejectBank ────────────────────────────────────────────────────────────

func BulkRejectBank(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			BankIDs []string `json:"bank_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.BankIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No bank_ids provided")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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
			UPDATE apibox.audit_bank_master SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE bank_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'`,
			userEmail, req.Comment, req.BankIDs)
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

// ── BulkDeleteBank ────────────────────────────────────────────────────────────

func BulkDeleteBank(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			BankIDs []string `json:"bank_ids"`
			Reason  string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.BankIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No bank_ids provided")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
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
		for _, id := range req.BankIDs {
			auditVals = append(auditVals, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", argIdx, argIdx+1, argIdx+2))
			auditArgs = append(auditArgs, id, req.Reason, userEmail)
			argIdx += 3
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO apibox.audit_bank_master (bank_id, action_type, processing_status, reason, requested_by, requested_at)
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
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "submitted_count": len(req.BankIDs)})
	}
}

// ── GetBankAll ────────────────────────────────────────────────────────────────

func GetBankAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.bank_id)
					a.bank_id, a.audit_id, a.action_type, a.processing_status,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at,
					a.checker_comment, a.reason,
					a.old_bank_code, a.old_bank_name, a.old_bank_type, a.old_entity_code
				FROM apibox.audit_bank_master a
				ORDER BY a.bank_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT bank_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at
				FROM apibox.audit_bank_master
				GROUP BY bank_id
			)
			SELECT
				b.bank_id, b.counterparty_id, b.bank_code, b.bank_name, b.bank_type,
				b.routing_number, b.entity_code,
				la.audit_id, la.action_type, la.processing_status,
				la.requested_by, TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				la.checker_by, TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				la.checker_comment, la.reason,
				la.old_bank_code, la.old_bank_name, la.old_bank_type, la.old_entity_code,
				h.created_by, h.created_at AS history_created_at,
				h.edited_by, h.edited_at AS history_edited_at
			FROM apibox.bank_master b
			LEFT JOIN latest_audit la ON la.bank_id = b.bank_id
			LEFT JOIN history h ON h.bank_id = b.bank_id
			ORDER BY b.bank_code`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Query failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fds := rows.FieldDescriptions()
		var result []map[string]interface{}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			row := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				row[string(fd.Name)] = vals[i]
			}
			result = append(result, row)
		}
		if result == nil {
			result = []map[string]interface{}{}
		}
		api.RespondWithPayload(w, true, "", result)
	}
}

// ── GetBankAuditHistory ───────────────────────────────────────────────────────

func GetBankAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bankID := r.URL.Query().Get("bank_id")

		baseQ := `
			SELECT audit_id, bank_id, action_type, processing_status, reason,
				requested_by, TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				checker_by, TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				checker_comment, old_bank_code, old_bank_name, old_bank_type, old_entity_code
			FROM apibox.audit_bank_master`

		var rows pgx.Rows
		var err error
		if bankID != "" {
			rows, err = pgxPool.Query(ctx, baseQ+" WHERE bank_id=$1 ORDER BY requested_at DESC", bankID)
		} else {
			rows, err = pgxPool.Query(ctx, baseQ+" ORDER BY requested_at DESC")
		}
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Audit history query failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fds := rows.FieldDescriptions()
		var result []map[string]interface{}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			row := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				row[string(fd.Name)] = vals[i]
			}
			result = append(result, row)
		}
		if result == nil {
			result = []map[string]interface{}{}
		}
		api.RespondWithPayload(w, true, "", result)
	}
}

// ── GetBankApprovedActive ─────────────────────────────────────────────────────

func GetBankApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		bankType := r.URL.Query().Get("type")
		entityCode := r.URL.Query().Get("entity_code")

		q := `
			SELECT b.bank_id, b.counterparty_id, b.bank_code, b.bank_name,
				b.bank_type, b.routing_number, b.entity_code
			FROM apibox.bank_master b
			JOIN apibox.counterparty_master c ON c.counterparty_id = b.counterparty_id
			WHERE c.status='ACTIVE' AND c.is_deleted=false`
		var args []interface{}
		pos := 1
		if bankType != "" {
			q += fmt.Sprintf(" AND b.bank_type=$%d", pos)
			args = append(args, strings.ToUpper(bankType))
			pos++
		}
		if entityCode != "" {
			q += fmt.Sprintf(" AND b.entity_code=$%d", pos)
			args = append(args, entityCode)
		}
		q += " ORDER BY b.bank_code"

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Query failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fds := rows.FieldDescriptions()
		var result []map[string]interface{}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				continue
			}
			row := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				row[string(fd.Name)] = vals[i]
			}
			result = append(result, row)
		}
		if result == nil {
			result = []map[string]interface{}{}
		}
		api.RespondWithPayload(w, true, "", result)
	}
}

// ── GetBankDetail ─────────────────────────────────────────────────────────────

func GetBankDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bankID := r.URL.Query().Get("bank_id")
		if bankID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "bank_id query param is required")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				b.bank_id, b.counterparty_id, b.bank_code, b.bank_name,
				b.bank_type, b.routing_number, b.entity_code,
				(SELECT row_to_json(a) FROM (
					SELECT audit_id, action_type, processing_status, reason,
						requested_by, TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
						checker_by, TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
						checker_comment
					FROM apibox.audit_bank_master
					WHERE bank_id = b.bank_id
					ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
					LIMIT 1
				) a) AS latest_audit
			FROM apibox.bank_master b
			WHERE b.bank_id=$1`, bankID)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Detail query failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fds := rows.FieldDescriptions()
		if rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to read row")
				return
			}
			row := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				row[string(fd.Name)] = vals[i]
			}
			api.RespondWithPayload(w, true, "", row)
			return
		}
		api.RespondWithError(w, http.StatusNotFound, "Bank record not found")
	}
}
