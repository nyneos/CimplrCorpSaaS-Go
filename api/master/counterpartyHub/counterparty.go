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

type CounterpartyInput struct {
	CounterpartyCode string  `json:"counterparty_code"`
	CounterpartyName string  `json:"counterparty_name"`
	ShortName        string  `json:"short_name,omitempty"`
	CounterpartyType string  `json:"counterparty_type"`
	Country          string  `json:"country"`
	PrimaryCurrency  string  `json:"primary_currency"`
	LEI              string  `json:"lei,omitempty"`
	RMEmail          string  `json:"rm_email,omitempty"`
	EffFrom          string  `json:"eff_from"`
	EffTo            *string `json:"eff_to,omitempty"`
	Notes            string  `json:"notes,omitempty"`
}

type CreateCounterpartyRequest struct {
	UserID string `json:"user_id"`
	CounterpartyInput
	// Type-specific child payloads — one must be present matching counterparty_type.
	Bank           *BankInput           `json:"bank,omitempty"`
	Exchange       *ExchangeInput       `json:"exchange,omitempty"`
	DataProvider   *DataProviderInput   `json:"data_provider,omitempty"`
	CcpCsd         *CcpCsdInput         `json:"ccp_csd,omitempty"`
	PaymentNetwork *PaymentNetworkInput `json:"payment_network,omitempty"`
	ERPSystem      *ERPSystemInput      `json:"erp_system,omitempty"`
}

type CreateCounterpartyBulkRequest struct {
	UserID string              `json:"user_id"`
	Rows   []CounterpartyInput `json:"rows"`
}

type UpdateCounterpartyRequest struct {
	UserID          string                 `json:"user_id"`
	CounterpartyID  string                 `json:"counterparty_id"`
	Fields          map[string]interface{} `json:"fields"`
	Reason          string                 `json:"reason"`
}

type BulkApproveCounterpartyRequest struct {
	UserID          string   `json:"user_id"`
	CounterpartyIDs []string `json:"counterparty_ids"`
	Comment         string   `json:"comment"`
}

type BulkRejectCounterpartyRequest struct {
	UserID          string   `json:"user_id"`
	CounterpartyIDs []string `json:"counterparty_ids"`
	Comment         string   `json:"comment"`
}

type BulkDeleteCounterpartyRequest struct {
	UserID          string   `json:"user_id"`
	CounterpartyIDs []string `json:"counterparty_ids"`
	Reason          string   `json:"reason"`
}

// ── Validation ────────────────────────────────────────────────────────────────

func validateCounterpartyInput(inp CounterpartyInput) error {
	if strings.TrimSpace(inp.CounterpartyCode) == "" {
		return errors.New("counterparty_code is required")
	}
	if err := validateCounterpartyCode(inp.CounterpartyCode); err != nil {
		return err
	}
	if strings.TrimSpace(inp.CounterpartyName) == "" {
		return errors.New("counterparty_name is required")
	}
	if strings.TrimSpace(inp.CounterpartyType) == "" {
		return errors.New("counterparty_type is required")
	}
	validTypes := map[string]bool{
		"BANK": true, "EXCHANGE": true, "DATA_PROVIDER": true,
		"CCP_CSD": true, "PAYMENT_NETWORK": true, "ERP_SYSTEM": true,
	}
	if !validTypes[strings.ToUpper(inp.CounterpartyType)] {
		return errors.New("counterparty_type must be one of BANK, EXCHANGE, DATA_PROVIDER, CCP_CSD, PAYMENT_NETWORK, ERP_SYSTEM")
	}
	if len(strings.TrimSpace(inp.Country)) != 2 {
		return errors.New("country must be a 2-character ISO 3166-1 alpha-2 code")
	}
	if len(strings.TrimSpace(inp.PrimaryCurrency)) != 3 {
		return errors.New("primary_currency must be a 3-character ISO 4217 code")
	}
	if inp.LEI != "" {
		if err := validateLEI(inp.LEI); err != nil {
			return err
		}
	}
	if strings.TrimSpace(inp.EffFrom) == "" {
		return errors.New("eff_from is required")
	}
	return nil
}

// ── CreateCounterpartyMaster ──────────────────────────────────────────────────
// Combined dispatcher: inserts counterparty_master + the type-specific child
// table in one transaction. The child payload key must match counterparty_type.

func CreateCounterpartyMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateCounterpartyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if err := validateCounterpartyInput(req.CounterpartyInput); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate child payload presence (counterparty_id not yet known — checked later).
		cpType := strings.ToUpper(req.CounterpartyType)
		switch cpType {
		case "BANK":
			if req.Bank == nil {
				api.RespondWithError(w, http.StatusBadRequest, `"bank" payload is required for counterparty_type=BANK`)
				return
			}
			if err := validateBankFields(*req.Bank); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
		case "EXCHANGE":
			if req.Exchange == nil {
				api.RespondWithError(w, http.StatusBadRequest, `"exchange" payload is required for counterparty_type=EXCHANGE`)
				return
			}
			req.Exchange.CounterpartyID = "PLACEHOLDER"
			if err := validateExchangeInput(*req.Exchange); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
		case "DATA_PROVIDER":
			if req.DataProvider == nil {
				api.RespondWithError(w, http.StatusBadRequest, `"data_provider" payload is required for counterparty_type=DATA_PROVIDER`)
				return
			}
			req.DataProvider.CounterpartyID = "PLACEHOLDER"
			if err := validateDataProviderInput(*req.DataProvider); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
		case "CCP_CSD":
			if req.CcpCsd == nil {
				api.RespondWithError(w, http.StatusBadRequest, `"ccp_csd" payload is required for counterparty_type=CCP_CSD`)
				return
			}
			req.CcpCsd.CounterpartyID = "PLACEHOLDER"
			if err := validateCcpCsdInput(*req.CcpCsd); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
		case "PAYMENT_NETWORK":
			if req.PaymentNetwork == nil {
				api.RespondWithError(w, http.StatusBadRequest, `"payment_network" payload is required for counterparty_type=PAYMENT_NETWORK`)
				return
			}
			req.PaymentNetwork.CounterpartyID = "PLACEHOLDER"
			if err := validatePaymentNetworkInput(*req.PaymentNetwork); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
		case "ERP_SYSTEM":
			if req.ERPSystem == nil {
				api.RespondWithError(w, http.StatusBadRequest, `"erp_system" payload is required for counterparty_type=ERP_SYSTEM`)
				return
			}
			req.ERPSystem.CounterpartyID = "PLACEHOLDER"
			if err := validateERPSystemInput(*req.ERPSystem); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
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

		// Insert parent
		var counterpartyID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO apibox.counterparty_master (
				counterparty_code, counterparty_name, short_name, counterparty_type,
				country, primary_currency, lei, rm_email, eff_from, eff_to,
				notes, status, created_by
			) VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),$9,$10,NULLIF($11,''),'DRAFT',$12)
			RETURNING counterparty_id`,
			strings.ToUpper(req.CounterpartyCode), req.CounterpartyName, req.ShortName,
			cpType, strings.ToUpper(req.Country), strings.ToUpper(req.PrimaryCurrency),
			req.LEI, req.RMEmail, req.EffFrom, req.EffTo, req.Notes, userEmail,
		).Scan(&counterpartyID); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Insert child record (sets counterparty_id, then delegates to tx-based helper)
		var childID string
		switch cpType {
		case "BANK":
			req.Bank.CounterpartyID = counterpartyID
			childID, err = insertBankRow(ctx, tx, *req.Bank, userEmail)
		case "EXCHANGE":
			req.Exchange.CounterpartyID = counterpartyID
			childID, err = insertExchangeRow(ctx, tx, *req.Exchange, userEmail)
		case "DATA_PROVIDER":
			req.DataProvider.CounterpartyID = counterpartyID
			childID, err = insertDataProviderRow(ctx, tx, *req.DataProvider, userEmail)
		case "CCP_CSD":
			req.CcpCsd.CounterpartyID = counterpartyID
			childID, err = insertCcpCsdRow(ctx, tx, *req.CcpCsd, userEmail)
		case "PAYMENT_NETWORK":
			req.PaymentNetwork.CounterpartyID = counterpartyID
			childID, err = insertPaymentNetworkRow(ctx, tx, *req.PaymentNetwork, userEmail)
		case "ERP_SYSTEM":
			req.ERPSystem.CounterpartyID = counterpartyID
			childID, err = insertERPSystemRow(ctx, tx, *req.ERPSystem, userEmail)
		}
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Child record insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Insert parent audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_counterparty_master
				(counterparty_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, counterpartyID, userEmail); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(cpID, uID, uEmail string) {
			defer func() { recover() }()
			bgCtx := context.Background()
			_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "COUNTERPARTY_HUB", cpID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "COUNTERPARTY_MASTER_CREATE",
				RecordID: cpID, RecordTable: "apibox.counterparty_master",
				AuditTable: "apibox.audit_counterparty_master", AuditIDColumn: "counterparty_id",
				ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(counterpartyID, req.UserID, userEmail)

		go func(cpID, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/counterparty-hub/counterparty/create", cpID,
				map[string]interface{}{"record_id": cpID, "event": "COUNTERPARTY_MASTER_CREATED", "actor_email": uEmail},
			)
		}(counterpartyID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess:  true,
			"counterparty_id":       counterpartyID,
			"child_id":              childID,
			"counterparty_type":     cpType,
			"counterparty_code":     strings.ToUpper(req.CounterpartyCode),
			"requested":             userEmail,
		})
		api.LogInfo("Counterparty+child created: ID=%s type=%s childID=%s by=%s", counterpartyID, cpType, childID, userEmail)
	}
}

// ── CreateCounterpartyMasterBulk ─────────────────────────────────────────────

func CreateCounterpartyMasterBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateCounterpartyBulkRequest
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
		var validRows []CounterpartyInput
		var errorsList []map[string]interface{}

		for i, row := range req.Rows {
			if err := validateCounterpartyInput(row); err != nil {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
					"counterparty_code":    row.CounterpartyCode,
				})
			} else {
				validRows = append(validRows, row)
			}
		}

		if len(validRows) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errorsList)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		var insertedIDs []string
		var insertedRecords []map[string]interface{}

		for i, row := range validRows {
			var cpID string
			insertQ := `
				INSERT INTO apibox.counterparty_master (
					counterparty_code, counterparty_name, short_name, counterparty_type,
					country, primary_currency, lei, rm_email, eff_from, eff_to,
					notes, status, created_by
				) VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),$9,$10,NULLIF($11,''),'DRAFT',$12)
				RETURNING counterparty_id`
			if err := tx.QueryRow(ctx, insertQ,
				strings.ToUpper(row.CounterpartyCode), row.CounterpartyName, row.ShortName,
				strings.ToUpper(row.CounterpartyType), strings.ToUpper(row.Country),
				strings.ToUpper(row.PrimaryCurrency), row.LEI, row.RMEmail,
				row.EffFrom, row.EffTo, row.Notes, userEmail,
			).Scan(&cpID); err != nil {
				msg, _ := getUserFriendlyCounterpartyError(err, "Insert failed for row")
				errorsList = append(errorsList, map[string]interface{}{
					"row_index": i, constants.ValueSuccess: false, constants.ValueError: msg,
					"counterparty_code": row.CounterpartyCode,
				})
				continue
			}
			insertedIDs = append(insertedIDs, cpID)
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true,
				"counterparty_id":      cpID,
				"counterparty_code":    strings.ToUpper(row.CounterpartyCode),
			})
		}

		if len(insertedIDs) > 0 {
			auditValues := make([]string, len(insertedIDs))
			auditArgs := make([]interface{}, 0, len(insertedIDs)*2)
			for i, id := range insertedIDs {
				auditValues[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf(`
				INSERT INTO apibox.audit_counterparty_master
					(counterparty_id, action_type, processing_status, requested_by, requested_at)
				VALUES %s`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		for _, cpID := range insertedIDs {
			go func(id, uID, uEmail string) {
				defer func() { recover() }()
				bgCtx := context.Background()
				_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode: "COUNTERPARTY_HUB", TransactionType: "COUNTERPARTY_MASTER_CREATE",
					RecordID: id, RecordTable: "apibox.counterparty_master",
					AuditTable: "apibox.audit_counterparty_master", AuditIDColumn: "counterparty_id",
					ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail,
				})
			}(cpID, req.UserID, userEmail)
		}

		allResults := append(insertedRecords, errorsList...)
		api.RespondWithPayload(w, len(insertedRecords) > 0, "", allResults)
		api.LogInfo("Bulk create counterparty: %d inserted, %d errors", len(insertedRecords), len(errorsList))
	}
}

// ── insertCounterpartyInTx ────────────────────────────────────────────────────
// Used by the bulk-upload handler to insert a single counterparty in its own tx.

func insertCounterpartyInTx(ctx context.Context, pool *pgxpool.Pool, inp CounterpartyInput, userEmail string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var id string
	if err := tx.QueryRow(ctx, `
		INSERT INTO apibox.counterparty_master (
			counterparty_code, counterparty_name, short_name, counterparty_type,
			country, primary_currency, lei, rm_email, eff_from, eff_to,
			notes, status, created_by
		) VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),$9,$10,NULLIF($11,''),'DRAFT',$12)
		RETURNING counterparty_id`,
		strings.ToUpper(inp.CounterpartyCode), inp.CounterpartyName, inp.ShortName,
		strings.ToUpper(inp.CounterpartyType), strings.ToUpper(inp.Country),
		strings.ToUpper(inp.PrimaryCurrency), inp.LEI, inp.RMEmail,
		inp.EffFrom, inp.EffTo, inp.Notes, userEmail,
	).Scan(&id); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO apibox.audit_counterparty_master (counterparty_id, action_type, processing_status, requested_by, requested_at)
		VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, id, userEmail); err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// ── UpdateCounterpartyMaster ──────────────────────────────────────────────────

func UpdateCounterpartyMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateCounterpartyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.CounterpartyID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_id is required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No fields provided for update")
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

		// Fetch current row for old values + status check
		var oldCode, oldName, oldStatus, oldSyncStatus string
		var oldEffFrom string
		var oldEffTo *string
		var oldIsActive bool
		selQ := `
			SELECT counterparty_code, counterparty_name, status, platform_sync_status,
			       TO_CHAR(eff_from,'YYYY-MM-DD'), TO_CHAR(eff_to,'YYYY-MM-DD'), COALESCE(is_active,true)
			FROM apibox.counterparty_master
			WHERE counterparty_id=$1 AND COALESCE(is_deleted,false)=false
			FOR UPDATE`
		if err := tx.QueryRow(ctx, selQ, req.CounterpartyID).Scan(
			&oldCode, &oldName, &oldStatus, &oldSyncStatus, &oldEffFrom, &oldEffTo, &oldIsActive,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Counterparty record not found")
				return
			}
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Reject counterparty_code change once ACTIVE
		if _, changing := req.Fields["counterparty_code"]; changing && oldStatus == "ACTIVE" {
			api.RespondWithError(w, http.StatusBadRequest, "Counterparty code cannot be changed after activation")
			return
		}

		allowedFields := map[string]bool{
			"counterparty_name": true, "short_name": true, "country": true,
			"primary_currency": true, "lei": true, "rm_email": true,
			"platform_sync_status": true, "eff_from": true, "eff_to": true,
			"notes": true, "is_active": true, "status": true,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if !allowedFields[k] {
				continue
			}
			sets = append(sets, fmt.Sprintf("%s=$%d", k, pos))
			args = append(args, v)
			pos++
		}
		sets = append(sets, fmt.Sprintf("updated_by=$%d, updated_at=now()", pos))
		args = append(args, userEmail)
		pos++

		if len(sets) == 1 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}

		updQ := fmt.Sprintf("UPDATE apibox.counterparty_master SET %s WHERE counterparty_id=$%d",
			strings.Join(sets, ", "), pos)
		args = append(args, req.CounterpartyID)
		if _, err := tx.Exec(ctx, updQ, args...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		auditQ := `
			INSERT INTO apibox.audit_counterparty_master (
				counterparty_id, action_type, processing_status, reason, requested_by, requested_at,
				old_counterparty_code, old_counterparty_name, old_status,
				old_platform_sync_status, old_eff_from, old_eff_to, old_is_active
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6,$7,$8,$9,$10)`
		if _, err := tx.Exec(ctx, auditQ,
			req.CounterpartyID, req.Reason, userEmail,
			oldCode, oldName, oldStatus, oldSyncStatus, oldEffFrom, oldEffTo, oldIsActive,
		); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(cpID, uID, uEmail string) {
			defer func() { recover() }()
			bgCtx := context.Background()
			_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "COUNTERPARTY_HUB", cpID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "COUNTERPARTY_MASTER_EDIT",
				RecordID: cpID, RecordTable: "apibox.counterparty_master",
				AuditTable: "apibox.audit_counterparty_master", AuditIDColumn: "counterparty_id",
				ActionType: "EDIT", SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(req.CounterpartyID, req.UserID, userEmail)

		go func(cpID, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/counterparty-hub/update", cpID,
				map[string]interface{}{"record_id": cpID, "event": "COUNTERPARTY_MASTER_UPDATED", "actor_email": uEmail},
			)
		}(req.CounterpartyID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true, "counterparty_id": req.CounterpartyID,
		})
		api.LogInfo("Counterparty updated: ID=%s by=%s", req.CounterpartyID, userEmail)
	}
}

// ── BulkApproveCounterpartyMaster ─────────────────────────────────────────────

func BulkApproveCounterpartyMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BulkApproveCounterpartyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.CounterpartyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No counterparty_ids provided")
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
			SELECT a.audit_id, a.counterparty_id, a.action_type, a.processing_status, a.requested_by,
			       COALESCE(m.counterparty_type,'') AS counterparty_type
			FROM apibox.audit_counterparty_master a
			LEFT JOIN apibox.counterparty_master m ON m.counterparty_id = a.counterparty_id
			WHERE a.counterparty_id = ANY($1::text[]) AND a.processing_status LIKE 'PENDING%'
			FOR UPDATE OF a`, req.CounterpartyIDs)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch audit rows failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		type auditRow struct {
			AuditID     string
			CPID        string
			ActionType  string
			Status      string
			RequestedBy string
			CPType      string
		}
		var audits []auditRow
		for rows.Next() {
			var a auditRow
			if err := rows.Scan(&a.AuditID, &a.CPID, &a.ActionType, &a.Status, &a.RequestedBy, &a.CPType); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Audit scan failed: "+err.Error())
				return
			}
			audits = append(audits, a)
		}
		rows.Close()

		if len(audits) == 0 {
			api.RespondWithPayload(w, false, "No pending audit records found for provided counterparty_ids", nil)
			return
		}

		var success []map[string]interface{}
		var errorsList []map[string]interface{}

		for _, a := range audits {
			// Maker-checker enforcement
			if a.RequestedBy == userEmail {
				errorsList = append(errorsList, map[string]interface{}{
					"counterparty_id": a.CPID, constants.ValueSuccess: false,
					constants.ValueError: "You cannot approve your own submission",
				})
				continue
			}

			if strings.ToUpper(a.ActionType) == "DELETE" {
				if _, err := tx.Exec(ctx, `
					UPDATE apibox.counterparty_master SET is_deleted=true, updated_by=$1, updated_at=now()
					WHERE counterparty_id=$2`, userEmail, a.CPID); err != nil {
					errorsList = append(errorsList, map[string]interface{}{
						"counterparty_id": a.CPID, constants.ValueSuccess: false, constants.ValueError: err.Error(),
					})
					continue
				}
			} else {
				if _, err := tx.Exec(ctx, `
					UPDATE apibox.counterparty_master SET status='ACTIVE', updated_by=$1, updated_at=now()
					WHERE counterparty_id=$2 AND status IN ('DRAFT','PENDING_APPROVAL')`,
					userEmail, a.CPID); err != nil {
					errorsList = append(errorsList, map[string]interface{}{
						"counterparty_id": a.CPID, constants.ValueSuccess: false, constants.ValueError: err.Error(),
					})
					continue
				}
			}

			// Cascade to linked child table
			if strings.ToUpper(a.ActionType) == "DELETE" {
				switch a.CPType {
				case "BANK":
					tx.Exec(ctx, `UPDATE apibox.bank_master SET is_deleted=true WHERE counterparty_id=$1`, a.CPID)
				case "EXCHANGE":
					tx.Exec(ctx, `UPDATE apibox.exchange_master SET is_deleted=true WHERE counterparty_id=$1`, a.CPID)
				case "DATA_PROVIDER":
					tx.Exec(ctx, `UPDATE apibox.data_provider_master SET is_deleted=true WHERE counterparty_id=$1`, a.CPID)
				case "CCP_CSD":
					tx.Exec(ctx, `UPDATE apibox.ccp_csd_master SET is_deleted=true WHERE counterparty_id=$1`, a.CPID)
				case "PAYMENT_NETWORK":
					tx.Exec(ctx, `UPDATE apibox.payment_network_master SET is_deleted=true WHERE counterparty_id=$1`, a.CPID)
				case "ERP_SYSTEM":
					tx.Exec(ctx, `UPDATE apibox.erp_system_master SET is_deleted=true WHERE counterparty_id=$1`, a.CPID)
				}
			} else {
				switch a.CPType {
				case "PAYMENT_NETWORK":
					tx.Exec(ctx, `UPDATE apibox.payment_network_master SET status='ACTIVE' WHERE counterparty_id=$1`, a.CPID)
				case "ERP_SYSTEM":
					tx.Exec(ctx, `UPDATE apibox.erp_system_master SET status='ACTIVE' WHERE counterparty_id=$1`, a.CPID)
				}
			}

			if _, err := tx.Exec(ctx, `
				UPDATE apibox.audit_counterparty_master
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE audit_id=$3`, userEmail, req.Comment, a.AuditID); err != nil {
				errorsList = append(errorsList, map[string]interface{}{
					"counterparty_id": a.CPID, constants.ValueSuccess: false, constants.ValueError: err.Error(),
				})
				continue
			}
			success = append(success, map[string]interface{}{
				constants.ValueSuccess: true, "counterparty_id": a.CPID, "audit_id": a.AuditID,
			})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		for _, s := range success {
			cpID := s["counterparty_id"].(string)
			go func(id, uEmail string) {
				defer func() { recover() }()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/master/counterparty-hub/bulk-approve", id,
					map[string]interface{}{"record_id": id, "event": "COUNTERPARTY_MASTER_APPROVED", "actor_email": uEmail},
				)
			}(cpID, userEmail)
		}

		allResults := append(success, errorsList...)
		api.RespondWithPayload(w, len(success) > 0, "", allResults)
		api.LogInfo("Bulk approve counterparty: %d approved, %d errors", len(success), len(errorsList))
	}
}

// ── BulkRejectCounterpartyMaster ──────────────────────────────────────────────

func BulkRejectCounterpartyMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BulkRejectCounterpartyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.CounterpartyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No counterparty_ids provided")
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

		// Enforce maker-checker: fetch requested_by for all pending audits
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Check for self-reject
		selfRows, _ := tx.Query(ctx, `
			SELECT counterparty_id FROM apibox.audit_counterparty_master
			WHERE counterparty_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
			  AND requested_by = $2`, req.CounterpartyIDs, userEmail)
		var selfIDs []string
		for selfRows.Next() {
			var id string
			selfRows.Scan(&id)
			selfIDs = append(selfIDs, id)
		}
		selfRows.Close()
		selfIDSet := make(map[string]bool)
		for _, id := range selfIDs {
			selfIDSet[id] = true
		}

		var rejectIDs []string
		for _, id := range req.CounterpartyIDs {
			if !selfIDSet[id] {
				rejectIDs = append(rejectIDs, id)
			}
		}

		var updated int64
		if len(rejectIDs) > 0 {
			res, err := tx.Exec(ctx, `
				UPDATE apibox.audit_counterparty_master
				SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE counterparty_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'`,
				userEmail, req.Comment, rejectIDs)
			if err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, "Rejection failed")
				api.RespondWithError(w, status, msg)
				return
			}
			updated = res.RowsAffected()
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		var response = map[string]interface{}{
			constants.ValueSuccess: updated > 0,
			"rejected_count":       updated,
			"checker":              userEmail,
		}
		if len(selfIDs) > 0 {
			response["self_reject_skipped"] = selfIDs
		}
		api.RespondWithPayload(w, updated > 0, "", response)
		api.LogInfo("Bulk reject counterparty: %d rejected by %s", updated, userEmail)
	}
}

// ── BulkDeleteCounterpartyMaster ──────────────────────────────────────────────

func BulkDeleteCounterpartyMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BulkDeleteCounterpartyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.CounterpartyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No counterparty_ids provided")
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

		existRows, err := tx.Query(ctx,
			`SELECT counterparty_id FROM apibox.counterparty_master WHERE counterparty_id = ANY($1::text[]) AND COALESCE(is_deleted,false)=false`,
			req.CounterpartyIDs)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch existing IDs failed")
			api.RespondWithError(w, status, msg)
			return
		}
		found := make(map[string]bool)
		for existRows.Next() {
			var id string
			existRows.Scan(&id)
			found[id] = true
		}
		existRows.Close()

		var auditValues []string
		var auditArgs []interface{}
		var errorsList []map[string]interface{}
		var successList []map[string]interface{}
		argIdx := 1

		for _, pid := range req.CounterpartyIDs {
			if !found[pid] {
				errorsList = append(errorsList, map[string]interface{}{
					"counterparty_id": pid, constants.ValueSuccess: false, constants.ValueError: "Record not found",
				})
				continue
			}
			auditValues = append(auditValues, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", argIdx, argIdx+1, argIdx+2))
			auditArgs = append(auditArgs, pid, req.Reason, userEmail)
			argIdx += 3
			successList = append(successList, map[string]interface{}{constants.ValueSuccess: true, "counterparty_id": pid})
		}

		if len(auditValues) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errorsList)
			return
		}

		auditQ := fmt.Sprintf(`
			INSERT INTO apibox.audit_counterparty_master
				(counterparty_id, action_type, processing_status, reason, requested_by, requested_at)
			VALUES %s`, strings.Join(auditValues, ","))
		if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		for _, s := range successList {
			go func(id, uEmail string) {
				defer func() { recover() }()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/master/counterparty-hub/bulk-delete", id,
					map[string]interface{}{"record_id": id, "event": "COUNTERPARTY_MASTER_DELETE_REQUESTED", "actor_email": uEmail},
				)
			}(s["counterparty_id"].(string), userEmail)
		}

		allResults := append(successList, errorsList...)
		api.RespondWithPayload(w, len(successList) > 0, "", allResults)
		api.LogInfo("Bulk delete request: %d submitted, %d errors", len(successList), len(errorsList))
	}
}

// ── GetCounterpartyMasterAll ──────────────────────────────────────────────────

func GetCounterpartyMasterAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.counterparty_id)
					a.counterparty_id, a.audit_id, a.action_type, a.processing_status,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at,
					a.checker_comment, a.reason,
					a.old_counterparty_code, a.old_counterparty_name, a.old_status,
					a.old_platform_sync_status, a.old_eff_from, a.old_eff_to, a.old_is_active
				FROM apibox.audit_counterparty_master a
				ORDER BY a.counterparty_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT counterparty_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at
				FROM apibox.audit_counterparty_master
				GROUP BY counterparty_id
			)
			SELECT
				m.counterparty_id,
				m.counterparty_code,
				m.counterparty_name,
				COALESCE(m.short_name,'') AS short_name,
				m.counterparty_type,
				m.country,
				m.primary_currency,
				COALESCE(m.lei,'') AS lei,
				COALESCE(m.rm_email,'') AS rm_email,
				COALESCE(m.platform_sync_status,'') AS platform_sync_status,
				COALESCE(m.cilink_platform_id,'') AS cilink_platform_id,
				m.status,
				COALESCE(m.notes,'') AS notes,
				TO_CHAR(m.eff_from,'YYYY-MM-DD') AS eff_from,
				COALESCE(TO_CHAR(m.eff_to,'YYYY-MM-DD'),'') AS eff_to,
				COALESCE(m.is_active,true) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(l.audit_id::text,'') AS audit_id,
				COALESCE(l.action_type,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.requested_by,'') AS requested_by,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(l.old_counterparty_code,'') AS old_counterparty_code,
				COALESCE(l.old_counterparty_name,'') AS old_counterparty_name,
				COALESCE(l.old_status,'') AS old_status,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				-- child: BANK
				COALESCE(b.bank_id::text,'') AS child_bank_id,
				COALESCE(b.bank_code,'') AS child_bank_code,
				COALESCE(b.bank_name,'') AS child_bank_name,
				COALESCE(b.bank_type,'') AS child_bank_type,
				-- child: EXCHANGE
				COALESCE(e.exchange_id::text,'') AS child_exchange_id,
				COALESCE(e.mic_code,'') AS child_mic_code,
				COALESCE(e.exchange_type,'') AS child_exchange_type,
				-- child: DATA_PROVIDER
				COALESCE(dp.provider_id::text,'') AS child_provider_id,
				COALESCE(dp.provider_code,'') AS child_provider_code,
				COALESCE(dp.provider_type,'') AS child_provider_type,
				-- child: CCP_CSD
				COALESCE(ccs.ccp_csd_id::text,'') AS child_ccp_csd_id,
				COALESCE(ccs.entity_code,'') AS child_ccp_entity_code,
				COALESCE(ccs.entity_sub_type,'') AS child_ccp_entity_sub_type,
				-- child: PAYMENT_NETWORK
				COALESCE(pn.payment_network_id::text,'') AS child_payment_network_id,
				COALESCE(pn.network_code,'') AS child_network_code,
				COALESCE(pn.network_type,'') AS child_network_type,
				-- child: ERP_SYSTEM
				COALESCE(erp.erp_system_id::text,'') AS child_erp_system_id,
				COALESCE(erp.erp_code,'') AS child_erp_code,
				COALESCE(erp.erp_type,'') AS child_erp_type
			FROM apibox.counterparty_master m
			LEFT JOIN latest_audit l ON l.counterparty_id = m.counterparty_id
			LEFT JOIN history h ON h.counterparty_id = m.counterparty_id
			LEFT JOIN apibox.bank_master b ON b.counterparty_id = m.counterparty_id AND COALESCE(b.is_deleted,false)=false
			LEFT JOIN apibox.exchange_master e ON e.counterparty_id = m.counterparty_id AND COALESCE(e.is_deleted,false)=false
			LEFT JOIN apibox.data_provider_master dp ON dp.counterparty_id = m.counterparty_id AND COALESCE(dp.is_deleted,false)=false
			LEFT JOIN apibox.ccp_csd_master ccs ON ccs.counterparty_id = m.counterparty_id AND COALESCE(ccs.is_deleted,false)=false
			LEFT JOIN apibox.payment_network_master pn ON pn.counterparty_id = m.counterparty_id AND COALESCE(pn.is_deleted,false)=false
			LEFT JOIN apibox.erp_system_master erp ON erp.counterparty_id = m.counterparty_id AND COALESCE(erp.is_deleted,false)=false
			WHERE COALESCE(m.is_deleted,false) = false
			ORDER BY GREATEST(
				COALESCE(l.requested_at,'1970-01-01'::timestamp),
				COALESCE(l.checker_at,'1970-01-01'::timestamp)
			) DESC`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 200)
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
		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row scan error: "+rows.Err().Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
		api.LogInfo("GetCounterpartyMasterAll: returned %d rows", len(out))
	}
}

// ── GetCounterpartyMasterAuditHistory ─────────────────────────────────────────

func GetCounterpartyMasterAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cpID := r.URL.Query().Get("counterparty_id")

		var q string
		var args []interface{}
		base := `
			SELECT audit_id, counterparty_id, action_type, processing_status,
				reason, requested_by,
				TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
				COALESCE(checker_comment,'') AS checker_comment,
				COALESCE(old_counterparty_code,'') AS old_counterparty_code,
				COALESCE(old_counterparty_name,'') AS old_counterparty_name,
				COALESCE(old_status,'') AS old_status
			FROM apibox.audit_counterparty_master`
		if cpID != "" {
			q = base + " WHERE counterparty_id=$1 ORDER BY requested_at DESC"
			args = []interface{}{cpID}
		} else {
			q = base + " ORDER BY requested_at DESC LIMIT 500"
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
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
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

// ── GetCounterpartyMasterApprovedActive ───────────────────────────────────────

func GetCounterpartyMasterApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cpType := strings.TrimSpace(r.URL.Query().Get("type"))

		q := `
			SELECT counterparty_id, counterparty_code, counterparty_name,
			       COALESCE(short_name,'') AS short_name, counterparty_type,
			       country, primary_currency, COALESCE(lei,'') AS lei,
			       COALESCE(rm_email,'') AS rm_email, platform_sync_status,
			       TO_CHAR(eff_from,'YYYY-MM-DD') AS eff_from,
			       COALESCE(TO_CHAR(eff_to,'YYYY-MM-DD'),'') AS eff_to
			FROM apibox.counterparty_master
			WHERE status='ACTIVE' AND COALESCE(is_active,true)=true AND COALESCE(is_deleted,false)=false
			  AND (eff_from <= now()::date) AND (eff_to IS NULL OR eff_to >= now()::date)`
		var args []interface{}
		if cpType != "" {
			q += " AND counterparty_type=$1"
			args = append(args, strings.ToUpper(cpType))
		}
		q += " ORDER BY counterparty_name ASC"

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
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
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

// ── GetCounterpartyMasterDetail ───────────────────────────────────────────────

func GetCounterpartyMasterDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cpID := r.URL.Query().Get("counterparty_id")
		if cpID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_id query param is required")
			return
		}

		// ── 1. Parent record ──────────────────────────────────────────────────
		prows, err := pgxPool.Query(ctx, `
			SELECT
				m.counterparty_id, m.counterparty_code, m.counterparty_name,
				COALESCE(m.short_name,'') AS short_name, m.counterparty_type,
				m.country, m.primary_currency, COALESCE(m.lei,'') AS lei,
				COALESCE(m.rm_email,'') AS rm_email,
				COALESCE(m.platform_sync_status,'') AS platform_sync_status,
				COALESCE(m.cilink_platform_id,'') AS cilink_platform_id,
				m.status, COALESCE(m.notes,'') AS notes,
				TO_CHAR(m.eff_from,'YYYY-MM-DD') AS eff_from,
				COALESCE(TO_CHAR(m.eff_to,'YYYY-MM-DD'),'') AS eff_to,
				COALESCE(m.is_active,true) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(m.created_by,'') AS created_by,
				COALESCE(TO_CHAR(m.created_at,'YYYY-MM-DD HH24:MI:SS'),'') AS created_at,
				COALESCE(m.updated_by,'') AS updated_by,
				COALESCE(TO_CHAR(m.updated_at,'YYYY-MM-DD HH24:MI:SS'),'') AS updated_at
			FROM apibox.counterparty_master m
			WHERE m.counterparty_id=$1`, cpID)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer prows.Close()

		if !prows.Next() {
			api.RespondWithError(w, http.StatusNotFound, "Counterparty not found")
			return
		}
		pFields := prows.FieldDescriptions()
		pVals, _ := prows.Values()
		result := make(map[string]interface{}, len(pFields)+10)
		for i, f := range pFields {
			if pVals[i] == nil {
				result[string(f.Name)] = ""
			} else {
				result[string(f.Name)] = pVals[i]
			}
		}
		prows.Close()

		cpType := ""
		if v, ok := result["counterparty_type"]; ok {
			cpType, _ = v.(string)
		}

		// ── 2. Full audit history ─────────────────────────────────────────────
		arows, err := pgxPool.Query(ctx, `
			SELECT
				audit_id::text, counterparty_id, action_type, processing_status,
				COALESCE(reason,'') AS reason,
				COALESCE(requested_by,'') AS requested_by,
				COALESCE(TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
				COALESCE(checker_comment,'') AS checker_comment,
				COALESCE(old_counterparty_code,'') AS old_counterparty_code,
				COALESCE(old_counterparty_name,'') AS old_counterparty_name,
				COALESCE(old_status,'') AS old_status,
				COALESCE(old_platform_sync_status,'') AS old_platform_sync_status,
				COALESCE(TO_CHAR(old_eff_from,'YYYY-MM-DD'),'') AS old_eff_from,
				COALESCE(TO_CHAR(old_eff_to,'YYYY-MM-DD'),'') AS old_eff_to
			FROM apibox.audit_counterparty_master
			WHERE counterparty_id=$1
			ORDER BY GREATEST(
				COALESCE(requested_at,'1970-01-01'::timestamp),
				COALESCE(checker_at,'1970-01-01'::timestamp)
			) DESC`, cpID)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Audit history query failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer arows.Close()

		aFields := arows.FieldDescriptions()
		var audits []map[string]interface{}
		for arows.Next() {
			aVals, _ := arows.Values()
			ar := make(map[string]interface{}, len(aFields))
			for i, f := range aFields {
				if aVals[i] == nil {
					ar[string(f.Name)] = ""
				} else {
					ar[string(f.Name)] = aVals[i]
				}
			}
			audits = append(audits, ar)
		}
		arows.Close()
		if audits == nil {
			audits = []map[string]interface{}{}
		}

		// ── 3. Child entity details ───────────────────────────────────────────
		childData, _ := fetchLinkedChildDetail(ctx, pgxPool, cpType, cpID)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 result,
			"audits":               audits,
			"child":                childData,
		})
	}
}

// fetchLinkedChildDetail returns the type-specific child record for a counterparty.
func fetchLinkedChildDetail(ctx context.Context, pool *pgxpool.Pool, cpType, cpID string) (map[string]interface{}, error) {
	var q string
	switch strings.ToUpper(cpType) {
	case "BANK":
		q = `SELECT bank_id::text, counterparty_id, bank_code, bank_name, bank_type,
		            COALESCE(routing_number,'') AS routing_number, entity_code
		     FROM apibox.bank_master
		     WHERE counterparty_id=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`
	case "EXCHANGE":
		q = `SELECT exchange_id::text, counterparty_id, mic_code, exchange_type, regulatory_body,
		            COALESCE(TO_CHAR(operating_hours_open,'HH24:MI'),'') AS operating_hours_open,
		            COALESCE(TO_CHAR(operating_hours_close,'HH24:MI'),'') AS operating_hours_close,
		            timezone, COALESCE(settlement_cycle,'') AS settlement_cycle,
		            COALESCE(holiday_calendar_ref,'') AS holiday_calendar_ref
		     FROM apibox.exchange_master
		     WHERE counterparty_id=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`
	case "DATA_PROVIDER":
		q = `SELECT provider_id::text, counterparty_id, provider_code, provider_type, delivery_mechanism,
		            api_creds_kms_ref, COALESCE(entitlement_codes,'') AS entitlement_codes,
		            COALESCE(TO_CHAR(renewal_date,'YYYY-MM-DD'),'') AS renewal_date, refresh_interval_sec
		     FROM apibox.data_provider_master
		     WHERE counterparty_id=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`
	case "CCP_CSD":
		q = `SELECT ccp_csd_id::text, counterparty_id, entity_code, entity_sub_type, lei, regulatory_body,
		            participant_id, clearing_acct_kms_ref,
		            COALESCE(margin_call_frequency,'') AS margin_call_frequency
		     FROM apibox.ccp_csd_master
		     WHERE counterparty_id=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`
	case "PAYMENT_NETWORK":
		q = `SELECT payment_network_id::text, counterparty_id, network_code, network_type,
		            COALESCE(bic_code,'') AS bic_code, COALESCE(routing_number,'') AS routing_number,
		            iban_supported, settlement_currencies,
		            COALESCE(TO_CHAR(cut_off_time,'HH24:MI'),'') AS cut_off_time,
		            timezone, COALESCE(api_endpoint_kms_ref,'') AS api_endpoint_kms_ref
		     FROM apibox.payment_network_master
		     WHERE counterparty_id=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`
	case "ERP_SYSTEM":
		q = `SELECT erp_system_id::text, counterparty_id, erp_code, erp_type,
		            COALESCE(version,'') AS version, base_url,
		            COALESCE(timezone,'') AS timezone, default_currency
		     FROM apibox.erp_system_master
		     WHERE counterparty_id=$1 AND COALESCE(is_deleted,false)=false LIMIT 1`
	default:
		return nil, nil
	}

	crows, err := pool.Query(ctx, q, cpID)
	if err != nil {
		return nil, err
	}
	defer crows.Close()
	if !crows.Next() {
		return nil, nil
	}
	cFields := crows.FieldDescriptions()
	cVals, _ := crows.Values()
	out := make(map[string]interface{}, len(cFields))
	for i, f := range cFields {
		if cVals[i] == nil {
			out[string(f.Name)] = ""
		} else {
			out[string(f.Name)] = cVals[i]
		}
	}
	return out, nil
}
