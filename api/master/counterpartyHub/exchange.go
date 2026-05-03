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

type ExchangeInput struct {
	CounterpartyID      string   `json:"counterparty_id"`
	MICCode             string   `json:"mic_code"`
	ExchangeType        string   `json:"exchange_type"`
	RegulatoryBody      string   `json:"regulatory_body"`
	OperatingHoursOpen  string   `json:"operating_hours_open,omitempty"`
	OperatingHoursClose string   `json:"operating_hours_close,omitempty"`
	Timezone            string   `json:"timezone"`
	SettlementCycle     string   `json:"settlement_cycle,omitempty"`
	HolidayCalendarRef  string   `json:"holiday_calendar_ref,omitempty"`
	AssetClasses        []string `json:"asset_classes,omitempty"`
	// FIX session fields
	ConnectivityProtocol string `json:"connectivity_protocol,omitempty"`
	SessionRole          string `json:"session_role,omitempty"`
	SenderCompID         string `json:"sender_comp_id,omitempty"`
	TargetCompID         string `json:"target_comp_id,omitempty"`
	PrimaryHost          string `json:"primary_host,omitempty"`
	PrimaryPort          int    `json:"primary_port,omitempty"`
	FailoverHost         string `json:"failover_host,omitempty"`
	FailoverPort         int    `json:"failover_port,omitempty"`
	FIXCredsKMSRef       string `json:"fix_creds_kms_ref,omitempty"`
	HeartbeatInterval    int    `json:"heartbeat_interval,omitempty"`
	TLSVersion           string `json:"tls_version,omitempty"`
}

func validateExchangeInput(inp ExchangeInput) error {
	if strings.TrimSpace(inp.CounterpartyID) == "" {
		return errors.New("counterparty_id is required")
	}
	if err := validateMICCode(inp.MICCode); err != nil {
		return err
	}
	if strings.TrimSpace(inp.ExchangeType) == "" {
		return errors.New("exchange_type is required")
	}
	validTypes := map[string]bool{"STOCK": true, "DERIVATIVES": true, "COMMODITY": true, "MTF": true, "DARK_POOL": true, "HYBRID": true}
	if !validTypes[strings.ToUpper(inp.ExchangeType)] {
		return errors.New("exchange_type must be one of STOCK, DERIVATIVES, COMMODITY, MTF, DARK_POOL, HYBRID")
	}
	if strings.TrimSpace(inp.RegulatoryBody) == "" {
		return errors.New("regulatory_body is required")
	}
	if strings.TrimSpace(inp.Timezone) == "" {
		return errors.New("timezone is required")
	}
	// FIX session: if connectivity protocol starts with FIX_, fix_creds_kms_ref is required
	if strings.HasPrefix(strings.ToUpper(inp.ConnectivityProtocol), "FIX_") {
		if strings.TrimSpace(inp.FIXCredsKMSRef) == "" {
			return errors.New("fix_creds_kms_ref is required when connectivity_protocol is FIX")
		}
		if err := validateKMSPath("fix_creds_kms_ref", inp.FIXCredsKMSRef); err != nil {
			return err
		}
	} else if inp.FIXCredsKMSRef != "" {
		if err := validateKMSPath("fix_creds_kms_ref", inp.FIXCredsKMSRef); err != nil {
			return err
		}
	}
	return nil
}

// ── CreateExchange ────────────────────────────────────────────────────────────

func CreateExchange(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			ExchangeInput
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.Error(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if err := validateExchangeInput(req.ExchangeInput); err != nil {
			api.Error(w, http.StatusBadRequest, err.Error())
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
			api.Error(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		settlementCycle := req.SettlementCycle
		if settlementCycle == "" {
			settlementCycle = "T+2"
		}
		sessionRole := req.SessionRole
		if sessionRole == "" {
			sessionRole = "INITIATOR"
		}
		heartbeat := req.HeartbeatInterval
		if heartbeat == 0 {
			heartbeat = 30
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.Error(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		var exchangeID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO apibox.exchange_master (
				counterparty_id, mic_code, exchange_type, regulatory_body,
				operating_hours_open, operating_hours_close, timezone, settlement_cycle,
				holiday_calendar_ref
			) VALUES ($1,$2,$3,$4,NULLIF($5,'')::time,NULLIF($6,'')::time,$7,$8,NULLIF($9,''))
			RETURNING exchange_id`,
			req.CounterpartyID, strings.ToUpper(req.MICCode), strings.ToUpper(req.ExchangeType),
			req.RegulatoryBody, req.OperatingHoursOpen, req.OperatingHoursClose, req.Timezone,
			settlementCycle, req.HolidayCalendarRef,
		).Scan(&exchangeID); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Exchange insert failed")
			api.Error(w, status, msg)
			return
		}

		// Insert asset classes
		for _, ac := range req.AssetClasses {
			if _, err := tx.Exec(ctx,
				`INSERT INTO apibox.exchange_asset_classes (exchange_id, asset_class) VALUES ($1,$2)`,
				exchangeID, strings.ToUpper(ac)); err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, "Asset class insert failed")
				api.Error(w, status, msg)
				return
			}
		}

		// Insert FIX session if KMS ref provided
		if req.FIXCredsKMSRef != "" {
			if _, err := tx.Exec(ctx, `
				INSERT INTO apibox.exchange_fix_session (
					exchange_id, session_role, sender_comp_id, target_comp_id,
					primary_host, primary_port, failover_host, failover_port,
					fix_creds_kms_ref, heartbeat_interval, tls_version
				) VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),NULLIF($6,0),NULLIF($7,''),NULLIF($8,0),$9,$10,NULLIF($11,''))`,
				exchangeID, sessionRole, req.SenderCompID, req.TargetCompID,
				req.PrimaryHost, req.PrimaryPort, req.FailoverHost, req.FailoverPort,
				req.FIXCredsKMSRef, heartbeat, req.TLSVersion,
			); err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, "FIX session insert failed")
				api.Error(w, status, msg)
				return
			}
		}

		// Audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_exchange_master
				(exchange_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, exchangeID, userEmail); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.Error(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.Error(w, status, msg)
			return
		}

		go func(eid, uID, uEmail string) {
			defer func() { recover() }()
			bgCtx := context.Background()
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "EXCHANGE_MASTER_CREATE",
				RecordID: eid, RecordTable: "apibox.exchange_master",
				AuditTable: "apibox.audit_exchange_master", AuditIDColumn: "exchange_id",
				ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(exchangeID, req.UserID, userEmail)

		go func(eid, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/counterparty-hub/exchange/create", eid,
				map[string]interface{}{"record_id": eid, "event": "EXCHANGE_MASTER_CREATED", "actor_email": uEmail},
			)
		}(exchangeID, userEmail)

		api.Success(w, http.StatusOK, map[string]interface{}{
			constants.ValueSuccess: true, "exchange_id": exchangeID, "mic_code": strings.ToUpper(req.MICCode), "requested": userEmail}, "")
		api.LogInfo("Exchange created: ID=%s MIC=%s by=%s", exchangeID, req.MICCode, userEmail)
	}
}

// ── CreateExchangeBulk ────────────────────────────────────────────────────────

func CreateExchangeBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string          `json:"user_id"`
			Rows   []ExchangeInput `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.Error(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.Error(w, http.StatusBadRequest, constants.ErrNoRowsProvided)
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
			api.Error(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var errorsList []map[string]interface{}
		var insertedRecords []map[string]interface{}

		for i, row := range req.Rows {
			if err := validateExchangeInput(row); err != nil {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error(),
				})
				continue
			}
			// Insert single in its own sub-handler — delegate to internal helper
			eid, err := insertExchangeInTx(ctx, pgxPool, row, userEmail)
			if err != nil {
				msg, _ := getUserFriendlyCounterpartyError(err, "Exchange insert failed")
				errorsList = append(errorsList, map[string]interface{}{
					"row_index": i, constants.ValueSuccess: false, constants.ValueError: msg,
				})
				continue
			}
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true, "exchange_id": eid, "counterparty_id": row.CounterpartyID,
			})
		}

		allResults := append(insertedRecords, errorsList...)
		api.Success(w, http.StatusOK, allResults, "")
		api.LogInfo("Bulk create exchange: %d inserted, %d errors", len(insertedRecords), len(errorsList))
	}
}

// insertExchangeInTx inserts one exchange row (with sub-tables) in its own transaction.
// insertExchangeRow writes exchange + sub-tables + audit into an existing tx.
// The caller owns Begin/Commit/Rollback.
func insertExchangeRow(ctx context.Context, tx pgx.Tx, inp ExchangeInput, userEmail string) (string, error) {
	settlementCycle := inp.SettlementCycle
	if settlementCycle == "" {
		settlementCycle = "T+2"
	}
	sessionRole := inp.SessionRole
	if sessionRole == "" {
		sessionRole = "INITIATOR"
	}
	heartbeat := inp.HeartbeatInterval
	if heartbeat == 0 {
		heartbeat = 30
	}

	var eid string
	if err := tx.QueryRow(ctx, `
		INSERT INTO apibox.exchange_master (
			counterparty_id, mic_code, exchange_type, regulatory_body,
			operating_hours_open, operating_hours_close, timezone, settlement_cycle, holiday_calendar_ref
		) VALUES ($1,$2,$3,$4,NULLIF($5,'')::time,NULLIF($6,'')::time,$7,$8,NULLIF($9,''))
		RETURNING exchange_id`,
		inp.CounterpartyID, strings.ToUpper(inp.MICCode), strings.ToUpper(inp.ExchangeType),
		inp.RegulatoryBody, inp.OperatingHoursOpen, inp.OperatingHoursClose,
		inp.Timezone, settlementCycle, inp.HolidayCalendarRef,
	).Scan(&eid); err != nil {
		return "", err
	}

	for _, ac := range inp.AssetClasses {
		if _, err := tx.Exec(ctx,
			`INSERT INTO apibox.exchange_asset_classes (exchange_id, asset_class) VALUES ($1,$2)`,
			eid, strings.ToUpper(ac)); err != nil {
			return "", err
		}
	}

	if inp.FIXCredsKMSRef != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.exchange_fix_session (
				exchange_id, session_role, sender_comp_id, target_comp_id,
				primary_host, primary_port, failover_host, failover_port,
				fix_creds_kms_ref, heartbeat_interval, tls_version
			) VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),NULLIF($6,0),NULLIF($7,''),NULLIF($8,0),$9,$10,NULLIF($11,''))`,
			eid, sessionRole, inp.SenderCompID, inp.TargetCompID,
			inp.PrimaryHost, inp.PrimaryPort, inp.FailoverHost, inp.FailoverPort,
			inp.FIXCredsKMSRef, heartbeat, inp.TLSVersion,
		); err != nil {
			return "", err
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO apibox.audit_exchange_master (exchange_id, action_type, processing_status, requested_by, requested_at)
		VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, eid, userEmail); err != nil {
		return "", err
	}

	return eid, nil
}

func insertExchangeInTx(ctx context.Context, pool *pgxpool.Pool, inp ExchangeInput, userEmail string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id, err := insertExchangeRow(ctx, tx, inp, userEmail)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// ── UpdateExchange ────────────────────────────────────────────────────────────

func UpdateExchange(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string                 `json:"user_id"`
			ExchangeID string                 `json:"exchange_id"`
			Fields     map[string]interface{} `json:"fields"`
			Reason     string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.Error(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ExchangeID == "" || len(req.Fields) == 0 {
			api.Error(w, http.StatusBadRequest, "exchange_id and fields are required")
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
			api.Error(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.Error(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		var oldMIC, oldType, oldRegBody, oldTZ string
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(mic_code,''), COALESCE(exchange_type,''), COALESCE(regulatory_body,''), COALESCE(timezone,'')
			FROM apibox.exchange_master WHERE exchange_id=$1 FOR UPDATE`, req.ExchangeID,
		).Scan(&oldMIC, &oldType, &oldRegBody, &oldTZ); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.Error(w, http.StatusNotFound, "Exchange record not found")
				return
			}
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch failed")
			api.Error(w, status, msg)
			return
		}

		allowed := map[string]bool{
			"mic_code": true, "exchange_type": true, "regulatory_body": true,
			"operating_hours_open": true, "operating_hours_close": true,
			"timezone": true, "settlement_cycle": true, "holiday_calendar_ref": true,
		}
		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if !allowed[k] {
				continue
			}
			sets = append(sets, fmt.Sprintf("%s=$%d", k, pos))
			args = append(args, v)
			pos++
		}
		if len(sets) == 0 {
			api.Error(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}
		args = append(args, req.ExchangeID)
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE apibox.exchange_master SET %s WHERE exchange_id=$%d",
			strings.Join(sets, ","), pos), args...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Update failed")
			api.Error(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_exchange_master (
				exchange_id, action_type, processing_status, reason, requested_by, requested_at,
				old_mic_code, old_exchange_type, old_regulatory_body, old_timezone
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6,$7)`,
			req.ExchangeID, req.Reason, userEmail, oldMIC, oldType, oldRegBody, oldTZ); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.Error(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.Error(w, status, msg)
			return
		}

		api.Success(w, http.StatusOK, map[string]interface{}{constants.ValueSuccess: true, "exchange_id": req.ExchangeID}, "")
		api.LogInfo("Exchange updated: ID=%s by=%s", req.ExchangeID, userEmail)
	}
}

// ── BulkApproveExchange ───────────────────────────────────────────────────────

func BulkApproveExchange(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ExchangeIDs []string `json:"exchange_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.Error(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ExchangeIDs) == 0 {
			api.Error(w, http.StatusBadRequest, constants.ErrNoExchangeIDsProvided)
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
			api.Error(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.Error(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		rows, err := tx.Query(ctx, `
			SELECT audit_id, exchange_id, action_type, requested_by
			FROM apibox.audit_exchange_master
			WHERE exchange_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
			FOR UPDATE`, req.ExchangeIDs)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch audits failed")
			api.Error(w, status, msg)
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
				errList = append(errList, map[string]interface{}{
					"exchange_id": a.ID, constants.ValueSuccess: false, constants.ValueError: "You cannot approve your own submission",
				})
				continue
			}
			if strings.ToUpper(a.ActionType) == "DELETE" {
				tx.Exec(ctx, `UPDATE apibox.exchange_master SET is_deleted=true WHERE exchange_id=$1`, a.ID)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE apibox.audit_exchange_master
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE audit_id=$3`, userEmail, req.Comment, a.AuditID); err != nil {
				errList = append(errList, map[string]interface{}{"exchange_id": a.ID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "exchange_id": a.ID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.Error(w, status, msg)
			return
		}
		api.Success(w, http.StatusOK, append(success, errList...), "")
	}
}

// ── BulkRejectExchange ────────────────────────────────────────────────────────

func BulkRejectExchange(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ExchangeIDs []string `json:"exchange_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.Error(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ExchangeIDs) == 0 {
			api.Error(w, http.StatusBadRequest, constants.ErrNoExchangeIDsProvided)
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
			api.Error(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.Error(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		res, err := tx.Exec(ctx, `
			UPDATE apibox.audit_exchange_master
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE exchange_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'`,
			userEmail, req.Comment, req.ExchangeIDs)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Rejection failed")
			api.Error(w, status, msg)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.Error(w, status, msg)
			return
		}
		api.Success(w, http.StatusOK, map[string]interface{}{
			constants.ValueSuccess: true, "rejected_count": res.RowsAffected(), "checker": userEmail}, "")
	}
}

// ── BulkDeleteExchange ────────────────────────────────────────────────────────

func BulkDeleteExchange(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ExchangeIDs []string `json:"exchange_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.Error(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ExchangeIDs) == 0 {
			api.Error(w, http.StatusBadRequest, constants.ErrNoExchangeIDsProvided)
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
			api.Error(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.Error(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		var auditVals []string
		var auditArgs []interface{}
		argIdx := 1
		for _, id := range req.ExchangeIDs {
			auditVals = append(auditVals, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", argIdx, argIdx+1, argIdx+2))
			auditArgs = append(auditArgs, id, req.Reason, userEmail)
			argIdx += 3
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO apibox.audit_exchange_master
				(exchange_id, action_type, processing_status, reason, requested_by, requested_at)
			VALUES %s`, strings.Join(auditVals, ",")), auditArgs...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.Error(w, status, msg)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.Error(w, status, msg)
			return
		}
		api.Success(w, http.StatusOK, map[string]interface{}{
			constants.ValueSuccess: true, "submitted_count": len(req.ExchangeIDs)}, "")
	}
}

// ── GetExchangeAll ────────────────────────────────────────────────────────────

func GetExchangeAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.exchange_id)
					a.exchange_id, a.audit_id, a.action_type, a.processing_status,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at,
					a.checker_comment, a.reason,
					a.old_mic_code, a.old_exchange_type, a.old_regulatory_body, a.old_timezone
				FROM apibox.audit_exchange_master a
				ORDER BY a.exchange_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT exchange_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at
				FROM apibox.audit_exchange_master
				GROUP BY exchange_id
			)
			SELECT
				e.exchange_id, e.counterparty_id, e.mic_code, e.exchange_type, e.regulatory_body,
				COALESCE(TO_CHAR(e.operating_hours_open,'HH24:MI'),'') AS operating_hours_open,
				COALESCE(TO_CHAR(e.operating_hours_close,'HH24:MI'),'') AS operating_hours_close,
				e.timezone, e.settlement_cycle,
				COALESCE(e.holiday_calendar_ref,'') AS holiday_calendar_ref,
				COALESCE(l.audit_id::text,'') AS audit_id,
				COALESCE(l.action_type,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.requested_by,'') AS requested_by,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(l.old_mic_code,'') AS old_mic_code,
				COALESCE(l.old_exchange_type,'') AS old_exchange_type,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at
			FROM apibox.exchange_master e
			LEFT JOIN latest_audit l ON l.exchange_id = e.exchange_id
			LEFT JOIN history h ON h.exchange_id = e.exchange_id
			WHERE COALESCE(e.is_deleted,false) = false
			ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamp),COALESCE(l.checker_at,'1970-01-01'::timestamp)) DESC`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.Error(w, status, msg)
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		api.Success(w, http.StatusOK, map[string]interface{}{constants.ValueSuccess: true, "data": out}, "")
	}
}

// ── GetExchangeAuditHistory ───────────────────────────────────────────────────

func GetExchangeAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.URL.Query().Get("exchange_id")
		q := `SELECT audit_id, exchange_id, action_type, processing_status, reason,
			         requested_by, TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS'),
			         COALESCE(checker_by,''), COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),''),
			         COALESCE(checker_comment,'')
			  FROM apibox.audit_exchange_master`
		var args []interface{}
		if id != "" {
			q += " WHERE exchange_id=$1 ORDER BY requested_at DESC"
			args = []interface{}{id}
		} else {
			q += " ORDER BY requested_at DESC LIMIT 500"
		}
		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.Error(w, http.StatusInternalServerError, err.Error())
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		api.Success(w, http.StatusOK, map[string]interface{}{constants.ValueSuccess: true, "data": out}, "")
	}
}

// ── GetExchangeApprovedActive ─────────────────────────────────────────────────

func GetExchangeApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cpID := r.URL.Query().Get("counterparty_id")
		q := `
			SELECT e.exchange_id, e.counterparty_id, e.mic_code, e.exchange_type,
			       e.regulatory_body, e.timezone, e.settlement_cycle,
			       COALESCE(TO_CHAR(e.operating_hours_open,'HH24:MI'),'') AS operating_hours_open,
			       COALESCE(TO_CHAR(e.operating_hours_close,'HH24:MI'),'') AS operating_hours_close
			FROM apibox.exchange_master e
			INNER JOIN apibox.counterparty_master cp ON cp.counterparty_id = e.counterparty_id
			WHERE cp.status='ACTIVE' AND COALESCE(cp.is_deleted,false)=false
			  AND COALESCE(e.is_deleted,false)=false`
		var args []interface{}
		if cpID != "" {
			q += " AND e.counterparty_id=$1"
			args = append(args, cpID)
		}
		q += " ORDER BY e.mic_code ASC"
		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.Error(w, status, msg)
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		api.Success(w, http.StatusOK, map[string]interface{}{constants.ValueSuccess: true, "data": out}, "")
	}
}

// ── GetExchangeDetail ─────────────────────────────────────────────────────────

func GetExchangeDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.URL.Query().Get("exchange_id")
		if id == "" {
			api.Error(w, http.StatusBadRequest, "exchange_id is required")
			return
		}
		q := `
			SELECT e.exchange_id, e.counterparty_id, e.mic_code, e.exchange_type,
			       e.regulatory_body, e.timezone, e.settlement_cycle,
			       COALESCE(TO_CHAR(e.operating_hours_open,'HH24:MI'),''),
			       COALESCE(TO_CHAR(e.operating_hours_close,'HH24:MI'),''),
			       COALESCE(e.holiday_calendar_ref,''),
			       COALESCE(e.is_deleted,false)
			FROM apibox.exchange_master e WHERE e.exchange_id=$1`
		var eid, cpid, mic, exType, regBody, tz, sc, opOpen, opClose, calRef string
		var isDeleted bool
		if err := pgxPool.QueryRow(ctx, q, id).Scan(
			&eid, &cpid, &mic, &exType, &regBody, &tz, &sc, &opOpen, &opClose, &calRef, &isDeleted,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.Error(w, http.StatusNotFound, "Exchange not found")
				return
			}
			api.Error(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Asset classes
		acRows, _ := pgxPool.Query(ctx, `SELECT asset_class FROM apibox.exchange_asset_classes WHERE exchange_id=$1`, id)
		var assetClasses []string
		for acRows.Next() {
			var ac string
			acRows.Scan(&ac)
			assetClasses = append(assetClasses, ac)
		}
		acRows.Close()

		result := map[string]interface{}{
			"exchange_id": eid, "counterparty_id": cpid, "mic_code": mic,
			"exchange_type": exType, "regulatory_body": regBody, "timezone": tz,
			"settlement_cycle": sc, "operating_hours_open": opOpen, "operating_hours_close": opClose,
			"holiday_calendar_ref": calRef, "is_deleted": isDeleted, "asset_classes": assetClasses,
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		api.Success(w, http.StatusOK, map[string]interface{}{constants.ValueSuccess: true, "data": result}, "")
	}
}
