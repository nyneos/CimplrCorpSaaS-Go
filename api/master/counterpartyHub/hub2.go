package counterpartyHub

// hub2.go — Counterparty Hub v2 unified handlers
//
// Routes (all under /master/v2/counterparty-hub/):
//   POST  /create          → CreateCounterparty   (single, all types)
//   POST  /bulk-approve    → BulkApproveCounterparty
//   GET   /all             → GetCounterpartyAll   (?type=BANK|...|omit for all)
//   GET   /approved-active → GetCounterpartyApprovedActive (?type= optional)
//
// The counterparty_type field in the request payload routes every write to the
// correct typed master table via insertTypedDetail (helpers.go) +
// insertBankDetail / insertExchangeDetail / ... (hub2_typed_inserts.go).
//
// All reads use the BRD schema:  counterparty + audit_counterparty (public schema).
// Typed master reads return the joined row from bank_master / exchange_master / etc.

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/dependency"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Request / response types ──────────────────────────────────────────────────

// HubCreateRequest is the flat unified payload for /master/v2/counterparty-hub/create.
// counterparty_type determines which typed-master fields are required and used.
type HubCreateRequest struct {
	UserID string `json:"user_id"`

	// BRD 5.3 common fields
	CounterpartyCode string `json:"counterparty_code"`
	CounterpartyName string `json:"counterparty_name"`
	ShortName        string `json:"short_name,omitempty"`
	CounterpartyType string `json:"counterparty_type"` // BANK|EXCHANGE|DATA_PROVIDER|CCP_CSD|PAYMENT_NETWORK|ERP_SYSTEM
	Country          string `json:"country"`
	PrimaryCurrency  string `json:"primary_currency"`
	LEI              string `json:"lei,omitempty"`
	EffectiveFrom    string `json:"effective_from"`
	RMEmail          string `json:"rm_email,omitempty"`
	Notes            string `json:"notes,omitempty"`

	// BANK fields
	BankCode      string `json:"bank_code,omitempty"`
	BankName      string `json:"bank_name,omitempty"`
	BankType      string `json:"bank_type,omitempty"`
	RoutingNumber string `json:"routing_number,omitempty"`
	EntityCode    string `json:"entity_code,omitempty"`

	// EXCHANGE + DATA_PROVIDER shared fields
	ConnectivityProtocol string `json:"connectivity_protocol,omitempty"` // EXCHANGE: FIX_SESSION, etc. | DATA_PROVIDER: REST, WEBSOCKET, SFTP, FTP

	// EXCHANGE fields
	ExchangeCode    string   `json:"exchange_code,omitempty"`
	MICCode         string   `json:"mic_code,omitempty"`
	AssetClasses    []string `json:"asset_classes,omitempty"`
	FIXSessionRole  string   `json:"fix_session_role,omitempty"`
	FIXSenderCompID string   `json:"fix_sender_comp_id,omitempty"`
	FIXTargetCompID string   `json:"fix_target_comp_id,omitempty"`
	FIXPrimaryHost  string   `json:"fix_primary_host,omitempty"`
	FIXPrimaryPort  int      `json:"fix_primary_port,omitempty"`
	FIXFailoverHost string   `json:"fix_failover_host,omitempty"`
	FIXFailoverPort int      `json:"fix_failover_port,omitempty"`
	FIXCredsKMSRef  string   `json:"fix_creds_kms_ref,omitempty"`
	FIXHeartbeatSec int      `json:"fix_heartbeat_sec,omitempty"`

	// DATA_PROVIDER fields
	ProviderCode       string   `json:"provider_code,omitempty"`
	DataTypes          []string `json:"data_types,omitempty"`
	APICredsKMSRef     string   `json:"api_creds_kms_ref,omitempty"`
	RefreshIntervalSec int      `json:"refresh_interval_sec,omitempty"`
	EntitlementCodes   string   `json:"entitlement_codes,omitempty"`
	RenewalDate        string   `json:"renewal_date,omitempty"`

	// CCP_CSD fields
	EntityCodeCCP      string `json:"entity_code_ccp,omitempty"`
	EntitySubType      string `json:"entity_sub_type,omitempty"`
	ClearingAcctKMSRef string `json:"clearing_acct_kms_ref,omitempty"`

	// PAYMENT_NETWORK fields
	NetworkCode  string  `json:"network_code,omitempty"`
	NetworkType  string  `json:"network_type,omitempty"`
	MaxTxnAmount float64 `json:"max_txn_amount,omitempty"`

	// ERP_SYSTEM fields
	SystemCode          string `json:"system_code,omitempty"`
	ERPType             string `json:"erp_type,omitempty"`
	Version             string `json:"version,omitempty"`
	HostedBy            string `json:"hosted_by,omitempty"`
	SystemURL           string `json:"system_url,omitempty"`
	AuthType            string `json:"auth_type,omitempty"`
	TokenEndpointKMSRef string `json:"token_endpoint_kms_ref,omitempty"`
	ClientIDKMSRef      string `json:"client_id_kms_ref,omitempty"`
	ClientSecretKMSRef  string `json:"client_secret_kms_ref,omitempty"`
}

// validateHubCreateRequest runs all common + type-specific checks before touching the DB.
func validateHubCreateRequest(req HubCreateRequest) error {
	if err := validateCounterpartyCode(req.CounterpartyCode); err != nil {
		return err
	}
	if len(strings.TrimSpace(req.CounterpartyName)) < 2 || len(req.CounterpartyName) > 200 {
		return fmt.Errorf("counterparty_name must be 2–200 characters")
	}
	if typedMasterTable(req.CounterpartyType) == "" {
		return fmt.Errorf("counterparty_type must be one of: BANK, EXCHANGE, DATA_PROVIDER, CCP_CSD, PAYMENT_NETWORK, ERP_SYSTEM")
	}
	if len(strings.TrimSpace(req.Country)) != 2 {
		return fmt.Errorf("country must be a 2-letter ISO 3166-1 alpha-2 code")
	}
	if len(strings.TrimSpace(req.PrimaryCurrency)) != 3 {
		return fmt.Errorf("primary_currency must be a 3-letter ISO 4217 code")
	}
	if req.EffectiveFrom == "" {
		return fmt.Errorf("effective_from is required")
	}
	if _, err := time.Parse(constants.DateFormat, req.EffectiveFrom); err != nil {
		return fmt.Errorf("effective_from must be YYYY-MM-DD")
	}
	if req.LEI != "" {
		if err := validateLEI(req.LEI); err != nil {
			return err
		}
	}
	if len(req.Notes) > 1000 {
		return fmt.Errorf("notes must not exceed 1000 characters")
	}

	switch req.CounterpartyType {
	case "BANK":
		if req.BankCode == "" {
			return fmt.Errorf("bank_code is required for BANK type")
		}
		if req.BankName == "" {
			return fmt.Errorf("bank_name is required for BANK type")
		}
		if req.BankType == "" {
			return fmt.Errorf("bank_type is required for BANK type")
		}
		if req.EntityCode == "" {
			return fmt.Errorf("entity_code is required for BANK type")
		}
		if err := validateNoBIC(req.BankCode); err != nil {
			return err
		}

	case "EXCHANGE":
		if req.ExchangeCode == "" {
			return fmt.Errorf("exchange_code is required for EXCHANGE type")
		}
		if err := validateMICCode(req.MICCode); err != nil {
			return err
		}
		if len(req.AssetClasses) == 0 {
			return fmt.Errorf("at least one asset_class is required for EXCHANGE type")
		}
		if req.ConnectivityProtocol == "" {
			return fmt.Errorf("connectivity_protocol is required for EXCHANGE type")
		}
		if strings.HasPrefix(req.ConnectivityProtocol, "FIX") {
			if err := validateKMSPath("fix_creds_kms_ref", req.FIXCredsKMSRef); err != nil {
				return err
			}
		}

	case "DATA_PROVIDER":
		if req.ProviderCode == "" {
			return fmt.Errorf("provider_code is required for DATA_PROVIDER type")
		}
		if len(req.DataTypes) == 0 {
			return fmt.Errorf("at least one data_type is required for DATA_PROVIDER type")
		}
		if req.ConnectivityProtocol == "" {
			return fmt.Errorf("connectivity_protocol is required for DATA_PROVIDER type")
		}
		switch req.ConnectivityProtocol {
		case "REST", "WEBSOCKET", "SFTP", "FTP":
			// valid
		default:
			return fmt.Errorf("connectivity_protocol must be one of REST, WEBSOCKET, SFTP, FTP for DATA_PROVIDER (got %q)", req.ConnectivityProtocol)
		}
		if err := validateKMSPath("api_creds_kms_ref", req.APICredsKMSRef); err != nil {
			return err
		}
		if req.APICredsKMSRef == "" {
			return fmt.Errorf("api_creds_kms_ref is required for DATA_PROVIDER type")
		}
		if req.RefreshIntervalSec < 1 || req.RefreshIntervalSec > 86400 {
			return fmt.Errorf("refresh_interval_sec must be between 1 and 86400")
		}

	case "CCP_CSD":
		if req.LEI == "" {
			return fmt.Errorf("lei is mandatory for CCP_CSD type")
		}
		if err := validateLEI(req.LEI); err != nil {
			return err
		}
		if req.EntityCodeCCP == "" {
			return fmt.Errorf("entity_code_ccp is required for CCP_CSD type")
		}
		if req.EntitySubType == "" {
			return fmt.Errorf("entity_sub_type is required for CCP_CSD type")
		}
		if err := validateKMSPath("clearing_acct_kms_ref", req.ClearingAcctKMSRef); err != nil {
			return err
		}
		if req.ClearingAcctKMSRef == "" {
			return fmt.Errorf("clearing_acct_kms_ref is required for CCP_CSD type")
		}

	case "PAYMENT_NETWORK":
		if req.NetworkCode == "" {
			return fmt.Errorf("network_code is required for PAYMENT_NETWORK type")
		}
		if req.NetworkType == "" {
			return fmt.Errorf("network_type is required for PAYMENT_NETWORK type")
		}
		if req.MaxTxnAmount < 0 {
			return fmt.Errorf("max_txn_amount must be positive")
		}

	case "ERP_SYSTEM":
		if req.SystemCode == "" {
			return fmt.Errorf("system_code is required for ERP_SYSTEM type")
		}
		if req.ERPType == "" {
			return fmt.Errorf("erp_type is required for ERP_SYSTEM type")
		}
		if req.HostedBy == "" {
			return fmt.Errorf("hosted_by is required for ERP_SYSTEM type")
		}
		if !strings.HasPrefix(req.SystemURL, "https://") {
			return fmt.Errorf("system_url must start with https://")
		}
		if req.AuthType == "" {
			return fmt.Errorf("auth_type is required for ERP_SYSTEM type")
		}
		switch req.AuthType {
		case "OAUTH2":
			if err := validateKMSPath("token_endpoint_kms_ref", req.TokenEndpointKMSRef); err != nil {
				return err
			}
			if err := validateKMSPath("client_id_kms_ref", req.ClientIDKMSRef); err != nil {
				return err
			}
			if err := validateKMSPath("client_secret_kms_ref", req.ClientSecretKMSRef); err != nil {
				return err
			}
			if req.TokenEndpointKMSRef == "" || req.ClientIDKMSRef == "" || req.ClientSecretKMSRef == "" {
				return fmt.Errorf("token_endpoint_kms_ref, client_id_kms_ref, client_secret_kms_ref are required for OAUTH2")
			}
		case "API_KEY":
			if err := validateKMSPath("client_id_kms_ref", req.ClientIDKMSRef); err != nil {
				return err
			}
			if req.ClientIDKMSRef == "" {
				return fmt.Errorf("client_id_kms_ref is required for API_KEY auth type")
			}
		case "BASIC_AUTH":
			if err := validateKMSPath("client_secret_kms_ref", req.ClientSecretKMSRef); err != nil {
				return err
			}
			if req.ClientSecretKMSRef == "" {
				return fmt.Errorf("client_secret_kms_ref is required for BASIC_AUTH type")
			}
		}
	}
	return nil
}

// ── CreateCounterparty ────────────────────────────────────────────────────────

// CreateCounterparty handles POST /master/v2/counterparty-hub/create.
// Writes counterparty + typed master + both audit rows atomically, then fires
// the approval engine and notification in separate goroutines.
func CreateCounterparty(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req HubCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if err := validateHubCreateRequest(req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		ok, createMatrixID := hubEnforceMatrix(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreCreate,
			HandlerName: "CreateCounterparty",
			APIPath:     "/master/v2/counterparty-hub/create",
			EntityCode:  req.CounterpartyCode,
			Actor:       userEmail,
		}, nil)
		if !ok {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// ── Step 1: INSERT counterparty (parent) ──────────────────────────────
		var counterpartyID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO apibox_svc.counterparty (
				counterparty_code, counterparty_name, short_name, counterparty_type,
				country, primary_currency, lei, effective_from, rm_email, notes,
				status, created_by
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'DRAFT',$11)
			RETURNING counterparty_id`,
			strings.ToUpper(req.CounterpartyCode),
			req.CounterpartyName,
			nilIfEmpty(req.ShortName),
			req.CounterpartyType,
			strings.ToUpper(req.Country),
			strings.ToUpper(req.PrimaryCurrency),
			nilIfEmpty(req.LEI),
			req.EffectiveFrom,
			nilIfEmpty(req.RMEmail),
			nilIfEmpty(req.Notes),
			userEmail,
		).Scan(&counterpartyID); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "counterparty insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// ── Step 2: INSERT typed master (routes by counterparty_type) ─────────
		fields := reqToFieldsMap(req)
		typedID, err := insertTypedDetail(ctx, tx, counterpartyID, req.CounterpartyType, fields)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, req.CounterpartyType+" detail insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// ── Step 3: INSERT audit_counterparty ─────────────────────────────────
		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox_svc.audit_counterparty
				(counterparty_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`,
			counterpartyID, userEmail,
		); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// ── Step 4: INSERT audit_{typed_master} ───────────────────────────────
		if err := insertTypedAudit(ctx, tx, req.CounterpartyType, typedID, counterpartyID, userEmail); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		// ── Post-commit: approval engine + notification ───────────────────────
		go func(cpID, uID, uEmail, cpType, matrixID string) {
			defer func() { recover() }()
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer bgCancel()
			_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "COUNTERPARTY_HUB", cpID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode:       "COUNTERPARTY_HUB",
				TransactionType:  "COUNTERPARTY_CREATE",
				RecordID:         cpID,
				RecordTable:      constants.ErrCounterpartyServiceTable,
				AuditTable:       constants.ErrAuditCounterpartyServiceTable,
				AuditIDColumn:    "counterparty_id",
				ActionType:       "CREATE",
				SubmittedBy:      uID,
				SubmittedByEmail: uEmail,
				MatrixID:         matrixID,
			})
		}(counterpartyID, req.UserID, userEmail, req.CounterpartyType, createMatrixID)

		go func(cpID, uEmail, cpType string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/v2/counterparty-hub/create", cpID,
				map[string]interface{}{
					"record_id":         cpID,
					"counterparty_type": cpType,
					"event":             "COUNTERPARTY_CREATED",
					"actor_email":       uEmail,
				})
		}(counterpartyID, userEmail, req.CounterpartyType)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"counterparty_id":   counterpartyID,
			"counterparty_code": strings.ToUpper(req.CounterpartyCode),
			"counterparty_type": req.CounterpartyType,
			"requested":         userEmail,
		})
		api.LogInfo("Counterparty created: id=%s type=%s code=%s by=%s",
			counterpartyID, req.CounterpartyType, req.CounterpartyCode, userEmail)
	}
}

// ── BulkApproveCounterparty ───────────────────────────────────────────────────

// BulkApproveCounterparty handles POST /master/v2/counterparty-hub/bulk-approve.
// For each counterparty_id, tries the approval engine path first; falls back to
// direct audit stamp when no matrix is configured.
func BulkApproveCounterparty(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			CounterpartyIDs []string `json:"counterparty_ids"`
			Comment         string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE approvals only
		pendingDel := dependency.GetPendingDeleteIDs(r.Context(), pgxPool, "apibox_svc.audit_counterparty", "counterparty_id", req.CounterpartyIDs)
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "mastercounterparty", pendingDel)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.CounterpartyIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.CounterpartyIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.CounterpartyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrCounterpartyIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		engineActed, directActed := 0, 0
		var errs []string

		for _, cpID := range req.CounterpartyIDs {
			// Fetch type and current status
			var cpType, currentStatus string
			if err := pgxPool.QueryRow(ctx,
				`SELECT counterparty_type, status FROM apibox_svc.counterparty WHERE counterparty_id=$1`,
				cpID).Scan(&cpType, &currentStatus); err != nil {
				errs = append(errs, cpID+": not found")
				continue
			}
			if currentStatus == constants.StatusActive {
				errs = append(errs, cpID+": already ACTIVE — skipped")
				continue
			}

			// Maker / checker guard
			var requestedBy string
			_ = pgxPool.QueryRow(ctx, `
			SELECT requested_by FROM apibox_svc.audit_counterparty
			WHERE counterparty_id=$1 AND processing_status LIKE 'PENDING%'
			ORDER BY requested_at DESC LIMIT 1`, cpID).Scan(&requestedBy)
			if requestedBy == userEmail {
				errs = append(errs, cpID+": "+constants.ErrMakerCheckerSamePerson)
				continue
			}

			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "COUNTERPARTY_HUB", RecordID: cpID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionApproved, Comment: req.Comment})
			if actionErr != nil {
				errs = append(errs, cpID+": "+actionErr.Error())
				continue
			}
			if actionRes.Acted {
				engineActed++

				// Check if instance is now fully APPROVED → flip status
				if actionRes.InstanceStatus == constants.StatusApproved {
					activateCounterparty(ctx, pgxPool, cpID, cpType, userEmail, req.Comment)
				}
			} else {
				if actionRes.CancelledStale {
					api.LogInfo("[CounterpartyHub] Cancelled stale approval instance for counterparty=%s: %s", cpID, actionRes.Reason)
				} else if actionRes.Reason != "" {
					errs = append(errs, cpID+": "+actionRes.Reason)
					continue
				}

				// Direct stamp (no approval matrix configured)
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					errs = append(errs, cpID+": tx begin failed")
					continue
				}

				tag, err1 := tx.Exec(ctx, `
				UPDATE apibox_svc.audit_counterparty
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE counterparty_id=$3 AND processing_status LIKE 'PENDING%'`,
					userEmail, req.Comment, cpID)

				_, err2 := tx.Exec(ctx, `
				UPDATE apibox_svc.counterparty SET status='ACTIVE', updated_by=$1, updated_at=now()
				WHERE counterparty_id=$2 AND status IN ('DRAFT','PENDING_APPROVAL')`,
					userEmail, cpID)

				// Stamp typed audit as well
				auditTbl := typedAuditTable(cpType)
				idCol := typedIDColumn(cpType)
				masterTbl := typedMasterTable(cpType)
				var err3 error
				if auditTbl != "" && idCol != "" && masterTbl != "" {
					_, err3 = tx.Exec(ctx, fmt.Sprintf(`
						UPDATE %s
						SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
						WHERE %s = (SELECT %s FROM %s WHERE counterparty_id=$3)
						AND processing_status LIKE 'PENDING%%'`,
						auditTbl, idCol, idCol, masterTbl),
						userEmail, req.Comment, cpID)
				}

				if err1 != nil || err2 != nil || err3 != nil {
					_ = tx.Rollback(ctx)
					errs = append(errs, cpID+": stamp failed")
					continue
				}
				if tag.RowsAffected() == 0 {
					_ = tx.Rollback(ctx)
					errs = append(errs, cpID+": no pending audit row found")
					continue
				}
				if err := tx.Commit(ctx); err != nil {
					errs = append(errs, cpID+constants.ErrCommitFailed)
					continue
				}
				directActed++
			}

			go func(id, uEmail, cpT string) {
				defer func() { recover() }()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/master/v2/counterparty-hub/bulk-approve", id,
					map[string]interface{}{
						"record_id":         id,
						"counterparty_type": cpT,
						"event":             "COUNTERPARTY_APPROVED",
						"actor_email":       uEmail,
					})
			}(cpID, userEmail, cpType)
		}

		total := engineActed + directActed
		api.RespondWithPayload(w, total > 0 || len(errs) == 0, "", map[string]interface{}{
			"engine_acted": engineActed,
			"direct_acted": directActed,
			"errors":       errs,
			"checker":      userEmail,
		})
	}
}

// activateCounterparty flips counterparty.status = ACTIVE and stamps the typed audit
// as APPROVED. Called when the approval engine signals a fully approved instance.
func activateCounterparty(ctx context.Context, pool *pgxpool.Pool,
	cpID, cpType, userEmail, comment string) {
	tx, txErr := pool.Begin(ctx)
	if txErr != nil {
		return
	}
	txOk := false
	defer func() {
		if !txOk {
			tx.Rollback(ctx) //nolint:errcheck
		}
	}()

	_, _ = tx.Exec(ctx, `
		UPDATE apibox_svc.counterparty SET status='ACTIVE', updated_by=$1, updated_at=now()
		WHERE counterparty_id=$2`, userEmail, cpID)

	auditTbl := typedAuditTable(cpType)
	idCol := typedIDColumn(cpType)
	masterTbl := typedMasterTable(cpType)
	if auditTbl != "" && idCol != "" && masterTbl != "" {
		_, _ = tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE %s = (SELECT %s FROM %s WHERE counterparty_id=$3)
			AND processing_status LIKE 'PENDING%%'`,
			auditTbl, idCol, idCol, masterTbl),
			userEmail, comment, cpID)
	}

	if err := tx.Commit(ctx); err == nil {
		txOk = true
	}
}

// ── GetCounterpartyAll ────────────────────────────────────────────────────────

// GetCounterpartyAll handles GET /master/v2/counterparty-hub/all
// Query params:
//
//	?type=BANK|EXCHANGE|DATA_PROVIDER|CCP_CSD|PAYMENT_NETWORK|ERP_SYSTEM  (optional — omit for all)
//	?status=PENDING_APPROVAL  (optional)
func GetCounterpartyAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cpType := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("type")))
		statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
		ctx := r.Context()

		where := "WHERE c.is_deleted = FALSE"
		var args []interface{}
		pos := 1
		if cpType != "" {
			where += fmt.Sprintf(" AND c.counterparty_type = $%d", pos)
			args = append(args, cpType)
			pos++
		}
		if statusFilter != "" {
			where += fmt.Sprintf(" AND c.status = $%d", pos)
			args = append(args, statusFilter)
		}

		q := fmt.Sprintf(`
			SELECT
				c.counterparty_id, c.counterparty_code, c.counterparty_name,
				COALESCE(c.short_name,'')           AS short_name,
				c.counterparty_type,
				c.country, c.primary_currency,
				COALESCE(c.lei,'')                  AS lei,
				COALESCE(c.rm_email,'')             AS rm_email,
				c.status,
				COALESCE(c.platform_sync_status,'') AS platform_sync_status,
				TO_CHAR(c.effective_from,'YYYY-MM-DD') AS effective_from,
				COALESCE(c.notes,'')                AS notes,
				COALESCE(c.is_deleted,false)        AS is_deleted,
				COALESCE(c.created_by,'')           AS created_by,
				COALESCE(TO_CHAR(c.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS created_at,
				-- latest audit (LATERAL to avoid DISTINCT ON + GROUP BY conflict)
				COALESCE(la.audit_id::text,'')      AS audit_id,
				COALESCE(la.action_type,'')         AS action_type,
				COALESCE(la.processing_status,'')   AS processing_status,
				COALESCE(la.requested_by,'')        AS requested_by,
				COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS requested_at,
				COALESCE(la.checker_by,'')          AS checker_by,
				COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'')  AS checker_at,
				COALESCE(la.checker_comment,'')     AS checker_comment,
				COALESCE(la.reason,'')              AS reason
			FROM apibox_svc.counterparty c
			LEFT JOIN LATERAL (
				SELECT *
				FROM apibox_svc.audit_counterparty
				WHERE counterparty_id = c.counterparty_id
				ORDER BY GREATEST(
					COALESCE(requested_at,'1970-01-01'::timestamptz),
					COALESCE(checker_at,'1970-01-01'::timestamptz)
				) DESC
				LIMIT 1
			) la ON TRUE
			%s
			ORDER BY GREATEST(
				COALESCE(la.requested_at,'1970-01-01'::timestamptz),
				COALESCE(la.checker_at,'1970-01-01'::timestamptz)
			) DESC`, where)

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fds := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fds))
			for i, f := range fds {
				if vals[i] == nil {
					row[string(f.Name)] = nil
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			out = append(out, row)
		}
		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "row scan error: "+rows.Err().Error())
			return
		}
		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("GetCounterpartyAll: returned %d rows (type=%q status=%q)", len(out), cpType, statusFilter)
	}
}

// ── GetCounterpartyApprovedActive ─────────────────────────────────────────────

// GetCounterpartyApprovedActive handles GET /master/v2/counterparty-hub/approved-active
// Query params:
//
//	?type=BANK|EXCHANGE|DATA_PROVIDER|CCP_CSD|PAYMENT_NETWORK|ERP_SYSTEM
//
// Without ?type= → returns all ACTIVE counterparties with parent fields only (dropdown use).
// With    ?type= → returns the full typed JOIN including all typed-master columns.
func GetCounterpartyApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cpType := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("type")))
		ctx := r.Context()

		var q string
		var args []interface{}

		if cpType != "" {
			// Full typed join — returns parent + typed master columns
			q = buildApprovedActiveTypedQuery(cpType)
			args = []interface{}{cpType}
		} else {
			// Minimal — all types, parent fields only (for generic dropdowns)
			q = `
				SELECT
					c.counterparty_id, c.counterparty_code, c.counterparty_name,
					COALESCE(c.short_name,'')           AS short_name,
					c.counterparty_type, c.country, c.primary_currency,
					COALESCE(c.lei,'')                  AS lei,
					c.status,
					COALESCE(c.platform_sync_status,'') AS platform_sync_status,
					TO_CHAR(c.effective_from,'YYYY-MM-DD') AS effective_from
				FROM apibox_svc.counterparty c
				WHERE c.status = 'ACTIVE' AND c.is_deleted = FALSE
				ORDER BY c.counterparty_type, c.counterparty_name ASC`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fds := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fds))
			for i, f := range fds {
				if vals[i] == nil {
					row[string(f.Name)] = nil
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			out = append(out, row)
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// buildApprovedActiveTypedQuery returns the SELECT that joins counterparty + typed master
// for status=ACTIVE records of the given type. $1 must be the cpType string.
func buildApprovedActiveTypedQuery(cpType string) string {
	base := `
		SELECT
			c.counterparty_id, c.counterparty_code, c.counterparty_name,
			COALESCE(c.short_name,'') AS short_name,
			c.counterparty_type, c.country, c.primary_currency,
			COALESCE(c.lei,'') AS lei, c.status,
			COALESCE(c.platform_sync_status,'') AS platform_sync_status,
			TO_CHAR(c.effective_from,'YYYY-MM-DD') AS effective_from`

	switch cpType {
	case "BANK":
		return base + `,
			bm.bank_id, bm.bank_code, bm.bank_name, bm.bank_type,
			COALESCE(bm.routing_number,'') AS routing_number, bm.entity_code
		FROM apibox_svc.counterparty c
		JOIN apibox_svc.bank_master bm ON bm.counterparty_id = c.counterparty_id
		WHERE c.status='ACTIVE' AND c.is_deleted=FALSE AND c.counterparty_type=$1
		ORDER BY c.counterparty_name`

	case "EXCHANGE":
		return base + `,
			em.exchange_id, em.exchange_code, em.mic_code, em.connectivity_protocol,
			COALESCE(em.fix_session_role,'') AS fix_session_role,
			COALESCE(em.fix_primary_host,'') AS fix_primary_host,
			COALESCE(em.fix_primary_port,0) AS fix_primary_port,
			CASE WHEN em.fix_creds_kms_ref LIKE 'vault/%' THEN '••••••'
			     ELSE COALESCE(em.fix_creds_kms_ref,'') END AS fix_creds_kms_ref,
			COALESCE(em.fix_heartbeat_sec,30) AS fix_heartbeat_sec,
			COALESCE(array_agg(DISTINCT eac.asset_class) FILTER (WHERE eac.asset_class IS NOT NULL), '{}') AS asset_classes
		FROM apibox_svc.counterparty c
		JOIN apibox_svc.exchange_master em ON em.counterparty_id = c.counterparty_id
		LEFT JOIN apibox_svc.exchange_asset_classes eac ON eac.exchange_id = em.exchange_id
		WHERE c.status='ACTIVE' AND c.is_deleted=FALSE AND c.counterparty_type=$1
		GROUP BY c.counterparty_id, c.counterparty_code, c.counterparty_name, c.short_name,
		         c.counterparty_type, c.country, c.primary_currency, c.lei, c.status,
		         c.platform_sync_status, c.effective_from,
		         em.exchange_id, em.exchange_code, em.mic_code, em.connectivity_protocol,
		         em.fix_session_role, em.fix_primary_host, em.fix_primary_port,
		         em.fix_creds_kms_ref, em.fix_heartbeat_sec
		ORDER BY c.counterparty_name`

	case "DATA_PROVIDER":
		return base + `,
			dp.provider_id, dp.provider_code, dp.connectivity_protocol,
			CASE WHEN dp.api_creds_kms_ref LIKE 'vault/%' THEN '••••••'
			     ELSE dp.api_creds_kms_ref END AS api_creds_kms_ref,
			dp.refresh_interval_sec,
			COALESCE(dp.entitlement_codes,'') AS entitlement_codes,
			COALESCE(TO_CHAR(dp.renewal_date,'YYYY-MM-DD'),'') AS renewal_date,
			COALESCE(array_agg(DISTINCT ddt.data_type) FILTER (WHERE ddt.data_type IS NOT NULL), '{}') AS data_types
		FROM apibox_svc.counterparty c
		JOIN apibox_svc.data_provider_master dp ON dp.counterparty_id = c.counterparty_id
		LEFT JOIN apibox_svc.dp_data_types ddt ON ddt.provider_id = dp.provider_id
		WHERE c.status='ACTIVE' AND c.is_deleted=FALSE AND c.counterparty_type=$1
		GROUP BY c.counterparty_id, c.counterparty_code, c.counterparty_name, c.short_name,
		         c.counterparty_type, c.country, c.primary_currency, c.lei, c.status,
		         c.platform_sync_status, c.effective_from,
		         dp.provider_id, dp.provider_code, dp.connectivity_protocol,
		         dp.api_creds_kms_ref, dp.refresh_interval_sec, dp.entitlement_codes, dp.renewal_date
		ORDER BY c.counterparty_name`

	case "CCP_CSD":
		return base + `,
			cc.ccp_csd_id, cc.entity_code, cc.entity_sub_type,
			CASE WHEN cc.clearing_acct_kms_ref LIKE 'vault/%' THEN '••••••'
			     ELSE cc.clearing_acct_kms_ref END AS clearing_acct_kms_ref
		FROM apibox_svc.counterparty c
		JOIN apibox_svc.ccp_csd_master cc ON cc.counterparty_id = c.counterparty_id
		WHERE c.status='ACTIVE' AND c.is_deleted=FALSE AND c.counterparty_type=$1
		ORDER BY c.counterparty_name`

	case "PAYMENT_NETWORK":
		return base + `,
			pn.network_id, pn.network_code, pn.network_type,
			COALESCE(pn.max_txn_amount,0) AS max_txn_amount
		FROM apibox_svc.counterparty c
		JOIN apibox_svc.payment_network_master pn ON pn.counterparty_id = c.counterparty_id
		WHERE c.status='ACTIVE' AND c.is_deleted=FALSE AND c.counterparty_type=$1
		ORDER BY c.counterparty_name`

	case "ERP_SYSTEM":
		return base + `,
			erp.erp_id, erp.system_code, erp.erp_type,
			COALESCE(erp.version,'') AS version, erp.hosted_by, erp.system_url, erp.auth_type,
			CASE WHEN erp.token_endpoint_kms_ref LIKE 'vault/%' THEN '••••••'
			     ELSE COALESCE(erp.token_endpoint_kms_ref,'') END AS token_endpoint_kms_ref,
			CASE WHEN erp.client_id_kms_ref LIKE 'vault/%' THEN '••••••'
			     ELSE COALESCE(erp.client_id_kms_ref,'') END AS client_id_kms_ref,
			CASE WHEN erp.client_secret_kms_ref LIKE 'vault/%' THEN '••••••'
			     ELSE COALESCE(erp.client_secret_kms_ref,'') END AS client_secret_kms_ref
		FROM apibox_svc.counterparty c
		JOIN apibox_svc.erp_system_master erp ON erp.counterparty_id = c.counterparty_id
		WHERE c.status='ACTIVE' AND c.is_deleted=FALSE AND c.counterparty_type=$1
		ORDER BY c.counterparty_name`
	}

	// Fallback — should never happen after validation
	return `SELECT c.counterparty_id, c.counterparty_code, c.counterparty_name, c.counterparty_type
		FROM apibox_svc.counterparty c WHERE c.status='ACTIVE' AND c.is_deleted=FALSE AND c.counterparty_type=$1`
}

// ── CreateCounterpartyBulk ────────────────────────────────────────────────────

// CreateCounterpartyBulk handles POST /master/v2/counterparty-hub/create-bulk.
// Each row in "rows" is a HubCreateRequest (minus user_id — that comes at the top level).
// Rows are processed independently; failures on individual rows are collected and returned.
func CreateCounterpartyBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string             `json:"user_id"`
			Rows   []HubCreateRequest `json:"rows"`
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
		var inserted, errList []map[string]interface{}

		for i, row := range req.Rows {
			row.UserID = req.UserID
			if err := validateHubCreateRequest(row); err != nil {
				errList = append(errList, map[string]interface{}{
					"row_index": i, "success": false, "error": err.Error(),
					"counterparty_code": row.CounterpartyCode,
				})
				continue
			}

			ok, pmsg, bulkMatrixID := hubEnforceInlineWithMatrix(ctx, r, pgxPool, enforceCtx{
				EventCode:   common.TriggerPreCreate,
				HandlerName: "CreateCounterpartyBulk",
				APIPath:     "/master/v2/counterparty-hub/create-bulk",
				EntityCode:  row.CounterpartyCode,
				Actor:       userEmail,
			}, nil)
			if !ok {
				errList = append(errList, map[string]interface{}{
					"row_index": i, "success": false, "error": pmsg,
					"counterparty_code": row.CounterpartyCode,
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				errList = append(errList, map[string]interface{}{
					"row_index": i, "success": false, "error": constants.ErrTransactionFailed,
				})
				continue
			}

			var cpID string
			err = tx.QueryRow(ctx, `
				INSERT INTO apibox_svc.counterparty (
					counterparty_code, counterparty_name, short_name, counterparty_type,
					country, primary_currency, lei, effective_from, rm_email, notes,
					status, created_by
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'DRAFT',$11)
				RETURNING counterparty_id`,
				strings.ToUpper(row.CounterpartyCode), row.CounterpartyName, nilIfEmpty(row.ShortName),
				row.CounterpartyType, strings.ToUpper(row.Country), strings.ToUpper(row.PrimaryCurrency),
				nilIfEmpty(row.LEI), row.EffectiveFrom, nilIfEmpty(row.RMEmail), nilIfEmpty(row.Notes),
				userEmail,
			).Scan(&cpID)
			if err != nil {
				_ = tx.Rollback(ctx)
				msg, _ := getUserFriendlyCounterpartyError(err, "insert failed")
				errList = append(errList, map[string]interface{}{
					"row_index": i, "success": false, "error": msg,
					"counterparty_code": row.CounterpartyCode,
				})
				continue
			}

			typedID, err := insertTypedDetail(ctx, tx, cpID, row.CounterpartyType, reqToFieldsMap(row))
			if err != nil {
				_ = tx.Rollback(ctx)
				msg, _ := getUserFriendlyCounterpartyError(err, "typed insert failed")
				errList = append(errList, map[string]interface{}{
					"row_index": i, "success": false, "error": msg,
					"counterparty_code": row.CounterpartyCode,
				})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO apibox_svc.audit_counterparty
					(counterparty_id, action_type, processing_status, requested_by, requested_at)
				VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, cpID, userEmail); err != nil {
				_ = tx.Rollback(ctx)
				errList = append(errList, map[string]interface{}{
					"row_index": i, "success": false, "error": constants.ErrAuditInsertFailed,
				})
				continue
			}

			if err := insertTypedAudit(ctx, tx, row.CounterpartyType, typedID, cpID, userEmail); err != nil {
				_ = tx.Rollback(ctx)
				errList = append(errList, map[string]interface{}{
					"row_index": i, "success": false, "error": constants.ErrAuditInsertFailed,
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				errList = append(errList, map[string]interface{}{
					"row_index": i, "success": false, "error": constants.ErrCommitFailed + err.Error(),
				})
				continue
			}

			inserted = append(inserted, map[string]interface{}{
				"success": true, "counterparty_id": cpID,
				"counterparty_code": strings.ToUpper(row.CounterpartyCode),
				"counterparty_type": row.CounterpartyType,
			})

			go func(id, uID, uEmail, cpType, matrixID string) {
				defer func() { recover() }()
				bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer bgCancel()
				_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "COUNTERPARTY_HUB", id, uEmail)
				_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
					ModuleCode: "COUNTERPARTY_HUB", TransactionType: "COUNTERPARTY_CREATE",
					RecordID: id, RecordTable: constants.ErrCounterpartyServiceTable,
					AuditTable: constants.ErrAuditCounterpartyServiceTable, AuditIDColumn: "counterparty_id",
					ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail,
					MatrixID: matrixID,
				})
			}(cpID, req.UserID, userEmail, row.CounterpartyType, bulkMatrixID)
		}

		all := append(inserted, errList...)
		api.RespondWithPayload(w, len(inserted) > 0, "", all)
		api.LogInfo("BulkCreate counterparty: %d inserted, %d errors", len(inserted), len(errList))
	}
}

// ── UpdateCounterparty ────────────────────────────────────────────────────────

// UpdateCounterparty handles POST /master/v2/counterparty-hub/update.
// Request: { user_id, counterparty_id, fields:{...}, reason }
// fields may contain parent-level OR typed-master-level columns — both are handled.
// counterparty_code is locked once status=ACTIVE. counterparty_type is never changeable.
func UpdateCounterparty(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string                 `json:"user_id"`
			CounterpartyID string                 `json:"counterparty_id"`
			Fields         map[string]interface{} `json:"fields"`
			Reason         string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.CounterpartyID == "" || len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_id and fields are required")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		ok, editMatrixID := hubEnforceMatrix(ctx, w, r, pgxPool, enforceCtx{
			EventCode:   common.TriggerPreEdit,
			HandlerName: "UpdateCounterparty",
			APIPath:     "/master/v2/counterparty-hub/update",
			EntityCode:  req.CounterpartyID,
			Actor:       userEmail,
		}, req.Fields)
		if !ok {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Fetch current state + lock
		var cpType, oldStatus, oldName, oldCountry, oldCurrency string
		var oldLEI, oldRMEmail, oldNotes, oldShortName string
		if err := tx.QueryRow(ctx, `
			SELECT counterparty_type, status,
			       COALESCE(counterparty_name,''), COALESCE(country,''),
			       COALESCE(primary_currency,''), COALESCE(lei,''),
			       COALESCE(rm_email,''), COALESCE(notes,''), COALESCE(short_name,'')
			FROM apibox_svc.counterparty
			WHERE counterparty_id=$1 AND is_deleted=FALSE
			FOR UPDATE`, req.CounterpartyID,
		).Scan(&cpType, &oldStatus, &oldName, &oldCountry, &oldCurrency,
			&oldLEI, &oldRMEmail, &oldNotes, &oldShortName); err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrCounterpartyNotFound1)
			return
		}

		if _, changing := req.Fields["counterparty_code"]; changing && oldStatus == constants.StatusActive {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_code cannot be changed after ACTIVE")
			return
		}
		if _, changing := req.Fields["counterparty_type"]; changing {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_type cannot be changed")
			return
		}

		// Allowed parent fields
		parentAllowed := map[string]bool{
			"counterparty_name": true, "short_name": true, "country": true,
			"primary_currency": true, "lei": true, "effective_from": true,
			"rm_email": true, "notes": true,
		}

		// Allowed typed fields per type
		typedAllowed := map[string]map[string]bool{
			"BANK": {
				"bank_name": true, "bank_type": true, "routing_number": true, "entity_code": true,
			},
			"EXCHANGE": {
				"connectivity_protocol": true, "fix_session_role": true,
				"fix_sender_comp_id": true, "fix_target_comp_id": true,
				"fix_primary_host": true, "fix_primary_port": true,
				"fix_failover_host": true, "fix_failover_port": true,
				"fix_creds_kms_ref": true, "fix_heartbeat_sec": true,
			},
			"DATA_PROVIDER": {
				"connectivity_protocol": true, "api_creds_kms_ref": true,
				"refresh_interval_sec": true, "entitlement_codes": true, "renewal_date": true,
			},
			"CCP_CSD": {
				"entity_sub_type": true, "clearing_acct_kms_ref": true,
			},
			"PAYMENT_NETWORK": {
				"network_type": true, "max_txn_amount": true,
			},
			"ERP_SYSTEM": {
				"version": true, "hosted_by": true, "system_url": true, "auth_type": true,
				"token_endpoint_kms_ref": true, "client_id_kms_ref": true, "client_secret_kms_ref": true,
			},
		}

		// Split fields into parent vs typed
		var parentSets, typedSets []string
		var parentArgs, typedArgs []interface{}
		pp, tp := 1, 1
		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if parentAllowed[k] {
				parentSets = append(parentSets, fmt.Sprintf("%s=$%d", k, pp))
				parentArgs = append(parentArgs, v)
				pp++
			} else if typedAllowed[cpType][k] {
				typedSets = append(typedSets, fmt.Sprintf("%s=$%d", k, tp))
				typedArgs = append(typedArgs, v)
				tp++
			}
		}

		if len(parentSets) == 0 && len(typedSets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields found")
			return
		}

		// UPDATE counterparty (parent)
		if len(parentSets) > 0 {
			parentSets = append(parentSets, fmt.Sprintf("updated_by=$%d, updated_at=now()", pp))
			parentArgs = append(parentArgs, userEmail, req.CounterpartyID)
			_, err := tx.Exec(ctx,
				fmt.Sprintf("UPDATE counterparty SET %s WHERE counterparty_id=$%d",
					strings.Join(parentSets, ", "), pp+1),
				parentArgs...,
			)
			if err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, "parent update failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		// UPDATE typed master
		if len(typedSets) > 0 {
			masterTbl := typedMasterTable(cpType)
			idCol := typedIDColumn(cpType)
			typedArgs = append(typedArgs, req.CounterpartyID)
			_, err := tx.Exec(ctx,
				fmt.Sprintf("UPDATE %s SET %s WHERE counterparty_id=$%d",
					masterTbl, strings.Join(typedSets, ", "), tp),
				typedArgs...,
			)
			if err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, "typed master update failed")
				api.RespondWithError(w, status, msg)
				return
			}
			_ = idCol // suppress unused warning
		}

		// INSERT audit_counterparty (PENDING_EDIT_APPROVAL)
		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox_svc.audit_counterparty
				(counterparty_id, action_type, processing_status, reason, requested_by, requested_at,
				 old_counterparty_name, old_short_name, old_country, old_primary_currency,
				 old_lei, old_rm_email, old_notes)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6,$7,$8,$9,$10)`,
			req.CounterpartyID, req.Reason, userEmail,
			oldName, oldShortName, oldCountry, oldCurrency, oldLEI, oldRMEmail, oldNotes,
		); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// INSERT audit_{typed_master} (PENDING_EDIT_APPROVAL) — only if typed fields changed
		if len(typedSets) > 0 {
			auditTbl := typedAuditTable(cpType)
			idCol := typedIDColumn(cpType)
			masterTbl := typedMasterTable(cpType)
			if _, err := tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (%s, counterparty_id, action_type, processing_status, reason, requested_by, requested_at)
				VALUES ((SELECT %s FROM %s WHERE counterparty_id=$1), $1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())`,
				auditTbl, idCol, idCol, masterTbl),
				req.CounterpartyID, req.Reason, userEmail,
			); err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(cpID, uID, uEmail, matrixID string) {
			defer func() { recover() }()
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer bgCancel()
			_ = approvalengine.CancelPendingInstances(bgCtx, pgxPool, "COUNTERPARTY_HUB", cpID, uEmail)
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "COUNTERPARTY_EDIT",
				RecordID: cpID, RecordTable: constants.ErrCounterpartyServiceTable,
				AuditTable: constants.ErrAuditCounterpartyServiceTable, AuditIDColumn: "counterparty_id",
				ActionType: "EDIT", SubmittedBy: uID, SubmittedByEmail: uEmail,
				MatrixID: matrixID,
			})
		}(req.CounterpartyID, req.UserID, userEmail, editMatrixID)

		go func(cpID, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/v2/counterparty-hub/update", cpID,
				map[string]interface{}{
					"record_id": cpID, "event": "COUNTERPARTY_UPDATED", "actor_email": uEmail,
				})
		}(req.CounterpartyID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"counterparty_id": req.CounterpartyID, "updated_by": userEmail,
		})
		api.LogInfo("Counterparty updated: id=%s by=%s", req.CounterpartyID, userEmail)
	}
}

// ── BulkRejectCounterparty ────────────────────────────────────────────────────

// BulkRejectCounterparty handles POST /master/v2/counterparty-hub/bulk-reject.
// Request: { user_id, counterparty_ids:[], comment }
func BulkRejectCounterparty(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			CounterpartyIDs []string `json:"counterparty_ids"`
			Comment         string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.CounterpartyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrCounterpartyIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var rejected, errList []map[string]interface{}

		for _, cpID := range req.CounterpartyIDs {
			// Self-reject guard: skip if requester == checker
			var requestedBy string
			_ = pgxPool.QueryRow(ctx, `
			SELECT requested_by FROM apibox_svc.audit_counterparty
			WHERE counterparty_id=$1 AND processing_status LIKE 'PENDING%'
			ORDER BY requested_at DESC LIMIT 1`, cpID).Scan(&requestedBy)
			if requestedBy == userEmail {
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false,
					"error": constants.ErrMakerCheckerSamePerson,
				})
				continue
			}

			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pgxPool, approvalengine.ActOnPendingRequest{ModuleCode: "COUNTERPARTY_HUB", RecordID: cpID, UserID: req.UserID, UserEmail: userEmail, RoleID: "", Action: approvalengine.ActionRejected, Comment: req.Comment})
			if actionErr != nil {
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": actionErr.Error(),
				})
				continue
			}
			if actionRes.Acted {
				rejected = append(rejected, map[string]interface{}{
					"counterparty_id": cpID, "success": true, "approval_engine": true,
				})
				continue
			}
			if actionRes.CancelledStale {
				api.LogInfo("[CounterpartyHub] Cancelled stale approval instance for counterparty=%s: %s", cpID, actionRes.Reason)
			} else if actionRes.Reason != "" {
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": actionRes.Reason,
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": constants.ErrTransactionFailed,
				})
				continue
			}

			// Fetch type for typed audit stamp
			var cpType string
			_ = pgxPool.QueryRow(ctx, `SELECT counterparty_type FROM apibox_svc.counterparty WHERE counterparty_id=$1`, cpID).Scan(&cpType)

			tag, err1 := tx.Exec(ctx, `
				UPDATE apibox_svc.audit_counterparty
				SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE counterparty_id=$3 AND processing_status LIKE 'PENDING%'`,
				userEmail, req.Comment, cpID)

			// Also stamp typed audit
			var err2 error
			if auditTbl := typedAuditTable(cpType); auditTbl != "" {
				idCol := typedIDColumn(cpType)
				masterTbl := typedMasterTable(cpType)
				_, err2 = tx.Exec(ctx, fmt.Sprintf(`
					UPDATE %s
					SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
					WHERE %s = (SELECT %s FROM %s WHERE counterparty_id=$3)
					AND processing_status LIKE 'PENDING%%'`,
					auditTbl, idCol, idCol, masterTbl),
					userEmail, req.Comment, cpID)
			}

			if err1 != nil || err2 != nil {
				_ = tx.Rollback(ctx)
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": "stamp failed",
				})
				continue
			}
			if tag.RowsAffected() == 0 {
				_ = tx.Rollback(ctx)
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": "no pending audit row",
				})
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": constants.ErrCommitFailed + err.Error(),
				})
				continue
			}
			rejected = append(rejected, map[string]interface{}{
				"counterparty_id": cpID, "success": true,
			})

			go func(id, uEmail string) {
				defer func() { recover() }()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/master/v2/counterparty-hub/bulk-reject", id,
					map[string]interface{}{
						"record_id": id, "event": "COUNTERPARTY_REJECTED", "actor_email": uEmail,
					})
			}(cpID, userEmail)
		}

		all := append(rejected, errList...)
		api.RespondWithPayload(w, len(rejected) > 0, "", map[string]interface{}{
			"rejected_count": len(rejected),
			"errors":         errList,
			"checker":        userEmail,
			"results":        all,
		})
	}
}

// ── BulkDeleteCounterparty ────────────────────────────────────────────────────

// BulkDeleteCounterparty handles POST /master/v2/counterparty-hub/bulk-delete.
// Audit-first soft delete: inserts PENDING_DELETE_APPROVAL rows in both audit tables.
// Actual is_deleted=true is set when BulkApprove processes a DELETE action_type.
// Request: { user_id, counterparty_ids:[], reason }
func BulkDeleteCounterparty(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			CounterpartyIDs []string `json:"counterparty_ids"`
			Reason          string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "mastercounterparty", req.CounterpartyIDs)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.CounterpartyIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.CounterpartyIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.CounterpartyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrCounterpartyIDsRequired)
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		var submitted, errList []map[string]interface{}

		for _, cpID := range req.CounterpartyIDs {
			// Verify record exists and is not already deleted
			var cpType string
			if err := pgxPool.QueryRow(ctx,
				`SELECT counterparty_type FROM apibox_svc.counterparty WHERE counterparty_id=$1 AND is_deleted=FALSE`,
				cpID).Scan(&cpType); err != nil {
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": "not found or already deleted",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": constants.ErrTransactionFailed,
				})
				continue
			}

			// Audit row on parent
			if _, err := tx.Exec(ctx, `
				INSERT INTO apibox_svc.audit_counterparty
					(counterparty_id, action_type, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`,
				cpID, req.Reason, userEmail,
			); err != nil {
				_ = tx.Rollback(ctx)
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": constants.ErrAuditInsertFailed,
				})
				continue
			}

			// Audit row on typed master
			auditTbl := typedAuditTable(cpType)
			idCol := typedIDColumn(cpType)
			masterTbl := typedMasterTable(cpType)
			if auditTbl != "" {
				if _, err := tx.Exec(ctx, fmt.Sprintf(`
					INSERT INTO %s (%s, counterparty_id, action_type, processing_status, reason, requested_by, requested_at)
					VALUES ((SELECT %s FROM %s WHERE counterparty_id=$1), $1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`,
					auditTbl, idCol, idCol, masterTbl),
					cpID, req.Reason, userEmail,
				); err != nil {
					_ = tx.Rollback(ctx)
					errList = append(errList, map[string]interface{}{
						"counterparty_id": cpID, "success": false, "error": constants.ErrAuditInsertFailed,
					})
					continue
				}
			}

			if err := tx.Commit(ctx); err != nil {
				errList = append(errList, map[string]interface{}{
					"counterparty_id": cpID, "success": false, "error": constants.ErrCommitFailed + err.Error(),
				})
				continue
			}
			submitted = append(submitted, map[string]interface{}{
				"counterparty_id": cpID, "success": true,
			})

			go func(id, uEmail string) {
				defer func() { recover() }()
				notifcatalog.TriggerNotification(context.Background(), pgxPool,
					"/master/v2/counterparty-hub/bulk-delete", id,
					map[string]interface{}{
						"record_id": id, "event": "COUNTERPARTY_DELETE_REQUESTED", "actor_email": uEmail,
					})
			}(cpID, userEmail)
		}

		api.RespondWithPayload(w, len(submitted) > 0, "", map[string]interface{}{
			"submitted_count": len(submitted),
			"errors":          errList,
			"requested_by":    userEmail,
		})
	}
}

// ── GetCounterpartyDetail ─────────────────────────────────────────────────────

// GetCounterpartyDetail handles GET /master/v2/counterparty-hub/detail?counterparty_id=CP-XXX
// Returns: counterparty parent + typed_detail (with KMS values masked) + audit_history + approval_workflow.
func GetCounterpartyDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cpID := r.URL.Query().Get("counterparty_id")
		if cpID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_id query param is required")
			return
		}
		ctx := r.Context()

		// ── Parent row ────────────────────────────────────────────────────────
		prows, err := pgxPool.Query(ctx, `
			SELECT
				counterparty_id, counterparty_code, counterparty_name,
				COALESCE(short_name,'') AS short_name, counterparty_type,
				country, primary_currency, COALESCE(lei,'') AS lei,
				COALESCE(rm_email,'') AS rm_email,
				COALESCE(platform_sync_status,'') AS platform_sync_status,
				COALESCE(cilink_platform_id,'') AS cilink_platform_id,
				status, COALESCE(notes,'') AS notes,
				TO_CHAR(effective_from,'YYYY-MM-DD') AS effective_from,
				is_deleted,
				COALESCE(created_by,'') AS created_by,
				COALESCE(TO_CHAR(created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS created_at,
				COALESCE(updated_by,'') AS updated_by,
				COALESCE(TO_CHAR(updated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS updated_at
			FROM apibox_svc.counterparty WHERE counterparty_id=$1`, cpID)
		if err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer prows.Close()

		if !prows.Next() {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrCounterpartyNotFound1)
			return
		}
		pFds := prows.FieldDescriptions()
		pVals, _ := prows.Values()
		cpRow := make(map[string]interface{}, len(pFds))
		for i, f := range pFds {
			cpRow[string(f.Name)] = pVals[i]
		}
		prows.Close()

		cpType, _ := cpRow["counterparty_type"].(string)

		// ── Typed master detail ───────────────────────────────────────────────
		typedDetail := fetchHub2TypedRow(ctx, pgxPool, cpType, cpID)

		// ── Merged audit history ──────────────────────────────────────────────
		auditHistory := fetchMergedHub2AuditHistory(ctx, pgxPool, cpType, cpID)

		// ── Approval workflow ─────────────────────────────────────────────────
		var approvalWorkflow interface{}
		var instanceID string
		_ = pgxPool.QueryRow(ctx, `
			SELECT instance_id FROM uam.approval_instance
			WHERE record_id=$1 AND module_code='COUNTERPARTY_HUB' AND is_deleted=false
			ORDER BY submitted_at DESC LIMIT 1`, cpID).Scan(&instanceID)
		if instanceID != "" {
			approvalWorkflow, _ = approvalengine.GetRichInstanceDetail(ctx, pgxPool, instanceID, "")
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"counterparty":      cpRow,
			"typed_detail":      typedDetail,
			"audit_history":     auditHistory,
			"approval_workflow": approvalWorkflow,
		})
	}
}

// fetchHub2TypedRow fetches the typed master row for a given counterparty (with KMS masking).
func fetchHub2TypedRow(ctx context.Context, pool *pgxpool.Pool, cpType, cpID string) map[string]interface{} {
	var q string
	switch cpType {
	case "BANK":
		q = `SELECT bank_id, bank_code, bank_name, bank_type,
			        COALESCE(routing_number,'') AS routing_number, entity_code
			 FROM apibox_svc.bank_master WHERE counterparty_id=$1`
	case "EXCHANGE":
		q = `SELECT em.exchange_id, em.exchange_code, em.mic_code, em.connectivity_protocol,
			        COALESCE(em.fix_session_role,'') AS fix_session_role,
			        COALESCE(em.fix_sender_comp_id,'') AS fix_sender_comp_id,
			        COALESCE(em.fix_target_comp_id,'') AS fix_target_comp_id,
			        COALESCE(em.fix_primary_host,'') AS fix_primary_host,
			        COALESCE(em.fix_primary_port,0) AS fix_primary_port,
			        COALESCE(em.fix_failover_host,'') AS fix_failover_host,
			        COALESCE(em.fix_failover_port,0) AS fix_failover_port,
			        CASE WHEN em.fix_creds_kms_ref LIKE 'vault/%' THEN '••••••'
			             ELSE COALESCE(em.fix_creds_kms_ref,'') END AS fix_creds_kms_ref,
			        COALESCE(em.fix_heartbeat_sec,30) AS fix_heartbeat_sec,
			        COALESCE(array_agg(DISTINCT eac.asset_class) FILTER (WHERE eac.asset_class IS NOT NULL), '{}') AS asset_classes
			 FROM apibox_svc.exchange_master em
			 LEFT JOIN apibox_svc.exchange_asset_classes eac ON eac.exchange_id = em.exchange_id
			 WHERE em.counterparty_id=$1
			 GROUP BY em.exchange_id, em.exchange_code, em.mic_code, em.connectivity_protocol,
			          em.fix_session_role, em.fix_sender_comp_id, em.fix_target_comp_id,
			          em.fix_primary_host, em.fix_primary_port, em.fix_failover_host,
			          em.fix_failover_port, em.fix_creds_kms_ref, em.fix_heartbeat_sec`
	case "DATA_PROVIDER":
		q = `SELECT dp.provider_id, dp.provider_code, dp.connectivity_protocol,
			        CASE WHEN dp.api_creds_kms_ref LIKE 'vault/%' THEN '••••••'
			             ELSE dp.api_creds_kms_ref END AS api_creds_kms_ref,
			        dp.refresh_interval_sec,
			        COALESCE(dp.entitlement_codes,'') AS entitlement_codes,
			        COALESCE(TO_CHAR(dp.renewal_date,'YYYY-MM-DD'),'') AS renewal_date,
			        COALESCE(array_agg(DISTINCT ddt.data_type) FILTER (WHERE ddt.data_type IS NOT NULL), '{}') AS data_types
			 FROM apibox_svc.data_provider_master dp
			 LEFT JOIN apibox_svc.dp_data_types ddt ON ddt.provider_id = dp.provider_id
			 WHERE dp.counterparty_id=$1
			 GROUP BY dp.provider_id, dp.provider_code, dp.connectivity_protocol,
			          dp.api_creds_kms_ref, dp.refresh_interval_sec, dp.entitlement_codes, dp.renewal_date`
	case "CCP_CSD":
		q = `SELECT ccp_csd_id, entity_code, entity_sub_type,
			        CASE WHEN clearing_acct_kms_ref LIKE 'vault/%' THEN '••••••'
			             ELSE clearing_acct_kms_ref END AS clearing_acct_kms_ref
			 FROM apibox_svc.ccp_csd_master WHERE counterparty_id=$1`
	case "PAYMENT_NETWORK":
		q = `SELECT network_id, network_code, network_type,
			        COALESCE(max_txn_amount,0) AS max_txn_amount
			 FROM apibox_svc.payment_network_master WHERE counterparty_id=$1`
	case "ERP_SYSTEM":
		q = `SELECT erp_id, system_code, erp_type, COALESCE(version,'') AS version,
			        hosted_by, system_url, auth_type,
			        CASE WHEN token_endpoint_kms_ref LIKE 'vault/%' THEN '••••••'
			             ELSE COALESCE(token_endpoint_kms_ref,'') END AS token_endpoint_kms_ref,
			        CASE WHEN client_id_kms_ref LIKE 'vault/%' THEN '••••••'
			             ELSE COALESCE(client_id_kms_ref,'') END AS client_id_kms_ref,
			        CASE WHEN client_secret_kms_ref LIKE 'vault/%' THEN '••••••'
			             ELSE COALESCE(client_secret_kms_ref,'') END AS client_secret_kms_ref
			 FROM apibox_svc.erp_system_master WHERE counterparty_id=$1`
	default:
		return nil
	}

	rows, err := pool.Query(ctx, q, cpID)
	if err != nil || !rows.Next() {
		if rows != nil {
			rows.Close()
		}
		return nil
	}
	defer rows.Close()
	fds := rows.FieldDescriptions()
	vals, _ := rows.Values()
	out := make(map[string]interface{}, len(fds))
	for i, f := range fds {
		out[string(f.Name)] = vals[i]
	}
	return out
}

// ── GetCounterpartyAuditHistory ───────────────────────────────────────────────

// GetCounterpartyAuditHistory handles GET /master/v2/counterparty-hub/audit-history?counterparty_id=CP-XXX
// Returns rows from audit_counterparty UNION ALL audit_{typed_master}, sorted newest first.
func GetCounterpartyAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cpID := r.URL.Query().Get("counterparty_id")
		if cpID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_id query param is required")
			return
		}
		ctx := r.Context()

		// Fetch type first
		var cpType string
		if err := pgxPool.QueryRow(ctx,
			`SELECT counterparty_type FROM apibox_svc.counterparty WHERE counterparty_id=$1`, cpID).Scan(&cpType); err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrCounterpartyNotFound1)
			return
		}

		out := fetchMergedHub2AuditHistory(ctx, pgxPool, cpType, cpID)
		api.RespondWithPayload(w, true, "", out)
	}
}

// fetchMergedHub2AuditHistory returns merged, time-sorted audit rows from
// audit_counterparty + audit_{typed_master} for a given counterparty.
func fetchMergedHub2AuditHistory(ctx context.Context, pool *pgxpool.Pool, cpType, cpID string) []map[string]interface{} {
	// Always query audit_counterparty
	const parentQ = `
		SELECT 'COUNTERPARTY' AS audit_source, audit_id::text, counterparty_id,
		       action_type, processing_status, COALESCE(reason,'') AS reason,
		       requested_by, requested_at, COALESCE(checker_by,'') AS checker_by,
		       checker_at, COALESCE(checker_comment,'') AS checker_comment
		FROM apibox_svc.audit_counterparty WHERE counterparty_id=$1`

	rows, err := pool.Query(ctx, parentQ, cpID)
	out := make([]map[string]interface{}, 0, 20)
	if err == nil {
		fds := rows.FieldDescriptions()
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fds))
			for i, f := range fds {
				row[string(f.Name)] = vals[i]
			}
			out = append(out, row)
		}
		rows.Close()
	}

	// Query typed audit table if type is known
	auditTbl := typedAuditTable(cpType)
	idCol := typedIDColumn(cpType)
	masterTbl := typedMasterTable(cpType)
	if auditTbl != "" {
		typedQ := fmt.Sprintf(`
			SELECT '%s' AS audit_source, a.audit_id::text, a.counterparty_id,
			       a.action_type, a.processing_status, COALESCE(a.reason,'') AS reason,
			       a.requested_by, a.requested_at, COALESCE(a.checker_by,'') AS checker_by,
			       a.checker_at, COALESCE(a.checker_comment,'') AS checker_comment
			FROM %s a
			WHERE a.%s = (SELECT %s FROM %s WHERE counterparty_id=$1)`,
			cpType, auditTbl, idCol, idCol, masterTbl)

		trows, err := pool.Query(ctx, typedQ, cpID)
		if err == nil {
			fds := trows.FieldDescriptions()
			for trows.Next() {
				vals, _ := trows.Values()
				row := make(map[string]interface{}, len(fds))
				for i, f := range fds {
					row[string(f.Name)] = vals[i]
				}
				out = append(out, row)
			}
			trows.Close()
		}
	}

	// Sort combined slice by requested_at DESC (simple insertion-sort on the small set)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0; j-- {
			ra1, _ := out[j]["requested_at"]
			ra0, _ := out[j-1]["requested_at"]
			if fmt.Sprintf("%v", ra1) > fmt.Sprintf("%v", ra0) {
				out[j], out[j-1] = out[j-1], out[j]
			} else {
				break
			}
		}
	}
	return out
}
