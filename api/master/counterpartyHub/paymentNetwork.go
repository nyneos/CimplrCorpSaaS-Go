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

type PaymentNetworkInput struct {
	CounterpartyID        string   `json:"counterparty_id"`
	NetworkCode           string   `json:"network_code"`
	NetworkType           string   `json:"network_type"`
	BICCode               string   `json:"bic_code"`
	RoutingNumber         string   `json:"routing_number"`
	IBANSupported         bool     `json:"iban_supported"`
	SettlementCurrencies  []string `json:"settlement_currencies"`
	CutOffTime            string   `json:"cut_off_time"`
	Timezone              string   `json:"timezone"`
	APIEndpointKMSRef     string   `json:"api_endpoint_kms_ref"`
}

var validNetworkTypes = map[string]bool{
	"SWIFT": true, "CHIPS": true, "ACH": true, "SEPA": true,
	"RTGS": true, "NEFT": true, "IMPS": true, "OTHER": true,
}

func validatePaymentNetworkInput(inp PaymentNetworkInput) error {
	if strings.TrimSpace(inp.CounterpartyID) == "" {
		return errors.New("counterparty_id is required")
	}
	if strings.TrimSpace(inp.NetworkCode) == "" {
		return errors.New("network_code is required")
	}
	if !validNetworkTypes[strings.ToUpper(inp.NetworkType)] {
		return errors.New("network_type must be one of SWIFT, CHIPS, ACH, SEPA, RTGS, NEFT, IMPS, OTHER")
	}
	if len(inp.SettlementCurrencies) == 0 {
		return errors.New("at least one settlement_currency is required")
	}
	for _, c := range inp.SettlementCurrencies {
		if len(strings.TrimSpace(c)) != 3 {
			return fmt.Errorf("settlement_currency '%s' must be a 3-character ISO code", c)
		}
	}
	if inp.APIEndpointKMSRef != "" {
		if err := validateKMSPath(inp.APIEndpointKMSRef); err != nil {
			return err
		}
	}
	return nil
}

// ── CreatePaymentNetwork ──────────────────────────────────────────────────────

func CreatePaymentNetwork(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			PaymentNetworkInput
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if err := validatePaymentNetworkInput(req.PaymentNetworkInput); err != nil {
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

		var pnID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO apibox.payment_network_master (
				counterparty_id, network_code, network_type, bic_code, routing_number,
				iban_supported, settlement_currencies, cut_off_time, timezone,
				api_endpoint_kms_ref
			) VALUES ($1,$2,$3,NULLIF($4,''),NULLIF($5,''),$6,$7,NULLIF($8,'')::time,NULLIF($9,''),NULLIF($10,''))
			RETURNING payment_network_id`,
			req.CounterpartyID, req.NetworkCode, strings.ToUpper(req.NetworkType),
			req.BICCode, req.RoutingNumber, req.IBANSupported, req.SettlementCurrencies,
			req.CutOffTime, req.Timezone, req.APIEndpointKMSRef,
		).Scan(&pnID); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Payment network insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_payment_network (payment_network_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, pnID, userEmail); err != nil {
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
			bgCtx := context.Background()
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "PAYMENT_NETWORK_CREATE",
				RecordID: id, RecordTable: "apibox.payment_network_master",
				AuditTable: "apibox.audit_payment_network", AuditIDColumn: "payment_network_id",
				ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(pnID, req.UserID, userEmail)

		go func(id, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/counterparty-hub/payment-network/create", id,
				map[string]interface{}{"record_id": id, "event": "PAYMENT_NETWORK_CREATED", "actor_email": uEmail},
			)
		}(pnID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true, "payment_network_id": pnID, "requested": userEmail,
		})
		api.LogInfo("PaymentNetwork created: ID=%s by=%s", pnID, userEmail)
	}
}

// ── CreatePaymentNetworkBulk ──────────────────────────────────────────────────

func CreatePaymentNetworkBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                `json:"user_id"`
			Rows   []PaymentNetworkInput `json:"rows"`
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
			if err := validatePaymentNetworkInput(row); err != nil {
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			id, err := insertPaymentNetworkInTx(ctx, pgxPool, row, userEmail)
			if err != nil {
				msg, _ := getUserFriendlyCounterpartyError(err, "Insert failed")
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: msg})
				continue
			}
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "payment_network_id": id})
		}

		api.RespondWithPayload(w, len(inserted) > 0, "", append(inserted, errList...))
	}
}

func insertPaymentNetworkRow(ctx context.Context, tx pgx.Tx, inp PaymentNetworkInput, userEmail string) (string, error) {
	var id string
	if err := tx.QueryRow(ctx, `
		INSERT INTO apibox.payment_network_master (
			counterparty_id, network_code, network_type, bic_code, routing_number,
			iban_supported, settlement_currencies, cut_off_time, timezone, api_endpoint_kms_ref
		) VALUES ($1,$2,$3,NULLIF($4,''),NULLIF($5,''),$6,$7,NULLIF($8,'')::time,NULLIF($9,''),NULLIF($10,''))
		RETURNING payment_network_id`,
		inp.CounterpartyID, inp.NetworkCode, strings.ToUpper(inp.NetworkType),
		inp.BICCode, inp.RoutingNumber, inp.IBANSupported, inp.SettlementCurrencies,
		inp.CutOffTime, inp.Timezone, inp.APIEndpointKMSRef,
	).Scan(&id); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO apibox.audit_payment_network (payment_network_id, action_type, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, id, userEmail); err != nil {
		return "", err
	}
	return id, nil
}

func insertPaymentNetworkInTx(ctx context.Context, pool *pgxpool.Pool, inp PaymentNetworkInput, userEmail string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id, err := insertPaymentNetworkRow(ctx, tx, inp, userEmail)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// ── UpdatePaymentNetwork ──────────────────────────────────────────────────────

func UpdatePaymentNetwork(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string                 `json:"user_id"`
			PaymentNetworkID  string                 `json:"payment_network_id"`
			Fields            map[string]interface{} `json:"fields"`
			Reason            string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.PaymentNetworkID == "" || len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "payment_network_id and fields are required")
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

		var oldNetworkType, oldRoutingNumber, oldBICCode string
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(network_type,''), COALESCE(routing_number,''), COALESCE(bic_code,'')
			FROM apibox.payment_network_master WHERE payment_network_id=$1 FOR UPDATE`, req.PaymentNetworkID,
		).Scan(&oldNetworkType, &oldRoutingNumber, &oldBICCode); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Payment network record not found")
				return
			}
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		allowed := map[string]bool{
			"network_type": true, "bic_code": true, "routing_number": true,
			"iban_supported": true, "settlement_currencies": true, "cut_off_time": true,
			"timezone": true, "api_endpoint_kms_ref": true,
		}
		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if !allowed[k] {
				continue
			}
			if k == "api_endpoint_kms_ref" {
				if s, ok := v.(string); ok {
					if err := validateKMSPath(s); err != nil {
						api.RespondWithError(w, http.StatusBadRequest, err.Error())
						return
					}
				}
			}
			if k == "network_type" {
				if s, ok := v.(string); ok {
					if !validNetworkTypes[strings.ToUpper(s)] {
						api.RespondWithError(w, http.StatusBadRequest, "network_type must be one of SWIFT, CHIPS, ACH, SEPA, RTGS, NEFT, IMPS, OTHER")
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
		args = append(args, req.PaymentNetworkID)
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE apibox.payment_network_master SET %s WHERE payment_network_id=$%d", strings.Join(sets, ","), pos), args...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_payment_network (
				payment_network_id, action_type, processing_status, reason, requested_by, requested_at,
				old_network_type, old_routing_number, old_bic_code
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6)`,
			req.PaymentNetworkID, req.Reason, userEmail, oldNetworkType, oldRoutingNumber, oldBICCode); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "payment_network_id": req.PaymentNetworkID})
	}
}

// ── BulkApprovePaymentNetwork ─────────────────────────────────────────────────

func BulkApprovePaymentNetwork(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			PaymentNetworkIDs []string `json:"payment_network_ids"`
			Comment           string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.PaymentNetworkIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No payment_network_ids provided")
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
			SELECT audit_id, payment_network_id, action_type, requested_by
			FROM apibox.audit_payment_network
			WHERE payment_network_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
			FOR UPDATE`, req.PaymentNetworkIDs)
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
				errList = append(errList, map[string]interface{}{"payment_network_id": a.ID, constants.ValueSuccess: false, constants.ValueError: "You cannot approve your own submission"})
				continue
			}
			if strings.ToUpper(a.ActionType) == "DELETE" {
				tx.Exec(ctx, `UPDATE apibox.payment_network_master SET is_deleted=true WHERE payment_network_id=$1`, a.ID)
			}
			if _, err := tx.Exec(ctx, `UPDATE apibox.audit_payment_network SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE audit_id=$3`, userEmail, req.Comment, a.AuditID); err != nil {
				errList = append(errList, map[string]interface{}{"payment_network_id": a.ID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "payment_network_id": a.ID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, len(success) > 0, "", append(success, errList...))
	}
}

// ── BulkRejectPaymentNetwork ──────────────────────────────────────────────────

func BulkRejectPaymentNetwork(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			PaymentNetworkIDs []string `json:"payment_network_ids"`
			Comment           string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.PaymentNetworkIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No payment_network_ids provided")
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
			UPDATE apibox.audit_payment_network SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE payment_network_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'`,
			userEmail, req.Comment, req.PaymentNetworkIDs)
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

// ── BulkDeletePaymentNetwork ──────────────────────────────────────────────────

func BulkDeletePaymentNetwork(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			PaymentNetworkIDs []string `json:"payment_network_ids"`
			Reason            string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.PaymentNetworkIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No payment_network_ids provided")
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
		for _, id := range req.PaymentNetworkIDs {
			auditVals = append(auditVals, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", argIdx, argIdx+1, argIdx+2))
			auditArgs = append(auditArgs, id, req.Reason, userEmail)
			argIdx += 3
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO apibox.audit_payment_network (payment_network_id, action_type, processing_status, reason, requested_by, requested_at)
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
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "submitted_count": len(req.PaymentNetworkIDs)})
	}
}

// ── GetPaymentNetworkAll ──────────────────────────────────────────────────────

func GetPaymentNetworkAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.payment_network_id)
					a.payment_network_id, a.audit_id, a.action_type, a.processing_status,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at,
					a.checker_comment, a.reason,
					a.old_network_type, a.old_routing_number, a.old_bic_code
				FROM apibox.audit_payment_network a
				ORDER BY a.payment_network_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT payment_network_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at
				FROM apibox.audit_payment_network
				GROUP BY payment_network_id
			)
			SELECT
				p.payment_network_id, p.counterparty_id, p.network_code, p.network_type,
				p.bic_code, p.routing_number, p.iban_supported, p.settlement_currencies,
				p.cut_off_time, p.timezone, p.api_endpoint_kms_ref,
				p.status, p.is_active, p.is_deleted,
				TO_CHAR(p.created_at,'YYYY-MM-DD HH24:MI:SS') AS created_at,
				TO_CHAR(p.updated_at,'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				la.audit_id, la.action_type, la.processing_status,
				la.requested_by, TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				la.checker_by, TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				la.checker_comment, la.reason,
				la.old_network_type, la.old_routing_number, la.old_bic_code,
				h.created_by, h.created_at AS history_created_at,
				h.edited_by, h.edited_at AS history_edited_at
			FROM apibox.payment_network_master p
			LEFT JOIN latest_audit la ON la.payment_network_id = p.payment_network_id
			LEFT JOIN history h ON h.payment_network_id = p.payment_network_id
			WHERE p.is_deleted = false
			ORDER BY p.created_at DESC`

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

// ── GetPaymentNetworkAuditHistory ─────────────────────────────────────────────

func GetPaymentNetworkAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		pnID := r.URL.Query().Get("payment_network_id")

		baseQ := `
			SELECT audit_id, payment_network_id, action_type, processing_status, reason,
				requested_by, TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				checker_by, TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				checker_comment, old_network_type, old_routing_number, old_bic_code
			FROM apibox.audit_payment_network`

		var rows pgx.Rows
		var err error
		if pnID != "" {
			rows, err = pgxPool.Query(ctx, baseQ+" WHERE payment_network_id=$1 ORDER BY requested_at DESC", pnID)
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

// ── GetPaymentNetworkApprovedActive ───────────────────────────────────────────

func GetPaymentNetworkApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		networkType := r.URL.Query().Get("type")

		var q string
		var args []interface{}
		if networkType != "" {
			q = `SELECT payment_network_id, counterparty_id, network_code, network_type,
				bic_code, routing_number, iban_supported, settlement_currencies,
				cut_off_time, timezone
				FROM apibox.payment_network_master
				WHERE status='ACTIVE' AND is_active=true AND is_deleted=false AND network_type=$1
				ORDER BY network_code`
			args = []interface{}{strings.ToUpper(networkType)}
		} else {
			q = `SELECT payment_network_id, counterparty_id, network_code, network_type,
				bic_code, routing_number, iban_supported, settlement_currencies,
				cut_off_time, timezone
				FROM apibox.payment_network_master
				WHERE status='ACTIVE' AND is_active=true AND is_deleted=false
				ORDER BY network_code`
		}

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

// ── GetPaymentNetworkDetail ───────────────────────────────────────────────────

func GetPaymentNetworkDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pnID := r.URL.Query().Get("payment_network_id")
		if pnID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "payment_network_id query param is required")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				p.payment_network_id, p.counterparty_id, p.network_code, p.network_type,
				p.bic_code, p.routing_number, p.iban_supported, p.settlement_currencies,
				p.cut_off_time, p.timezone, p.api_endpoint_kms_ref,
				p.status, p.is_active, p.is_deleted,
				TO_CHAR(p.created_at,'YYYY-MM-DD HH24:MI:SS') AS created_at,
				TO_CHAR(p.updated_at,'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				(SELECT row_to_json(a) FROM (
					SELECT audit_id, action_type, processing_status, reason,
						requested_by, TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
						checker_by, TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
						checker_comment
					FROM apibox.audit_payment_network
					WHERE payment_network_id = p.payment_network_id
					ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
					LIMIT 1
				) a) AS latest_audit
			FROM apibox.payment_network_master p
			WHERE p.payment_network_id=$1`, pnID)
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
		api.RespondWithError(w, http.StatusNotFound, "Payment network record not found")
	}
}
