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

type ERPAuthConfigInput struct {
	AuthType            string `json:"auth_type"`                        // OAUTH2 | BASIC_AUTH | API_KEY | CERT
	TokenEndpointKMSRef string `json:"token_endpoint_kms_ref,omitempty"` // required for OAUTH2
	ClientIDKMSRef      string `json:"client_id_kms_ref,omitempty"`      // required for OAUTH2
	ClientSecretKMSRef  string `json:"client_secret_kms_ref,omitempty"`  // required for OAUTH2 and BASIC_AUTH
	APIKeyKMSRef        string `json:"api_key_kms_ref,omitempty"`        // required for API_KEY
	CertKMSRef          string `json:"cert_kms_ref,omitempty"`           // required for CERT
	Scopes              string `json:"scopes,omitempty"`
}

type ERPSystemInput struct {
	CounterpartyID  string             `json:"counterparty_id"`
	ERPCode         string             `json:"erp_code"`
	ERPType         string             `json:"erp_type"`
	Version         string             `json:"version,omitempty"`
	BaseURL         string             `json:"base_url"`
	Timezone        string             `json:"timezone,omitempty"`
	DefaultCurrency string             `json:"default_currency"`
	AuthConfig      ERPAuthConfigInput `json:"auth_config"`
}

var validERPTypes = map[string]bool{
	"SAP": true, "ORACLE": true, "DYNAMICS": true, "NETSUITE": true,
	"TALLY": true, "QUICKBOOKS": true, "OTHER": true,
}

var validAuthTypes = map[string]bool{
	"OAUTH2": true, "BASIC_AUTH": true, "API_KEY": true, "CERT": true,
}

func validateERPSystemInput(inp ERPSystemInput) error {
	if strings.TrimSpace(inp.CounterpartyID) == "" {
		return errors.New("counterparty_id is required")
	}
	if strings.TrimSpace(inp.ERPCode) == "" {
		return errors.New("erp_code is required")
	}
	if !validERPTypes[strings.ToUpper(inp.ERPType)] {
		return errors.New("erp_type must be one of SAP, ORACLE, DYNAMICS, NETSUITE, TALLY, QUICKBOOKS, OTHER")
	}
	if strings.TrimSpace(inp.BaseURL) == "" {
		return errors.New("base_url is required")
	}
	if strings.TrimSpace(inp.DefaultCurrency) == "" || len(strings.TrimSpace(inp.DefaultCurrency)) != 3 {
		return errors.New("default_currency must be a 3-character ISO code")
	}

	// Auth config validation
	authType := strings.ToUpper(inp.AuthConfig.AuthType)
	if !validAuthTypes[authType] {
		return errors.New("auth_config.auth_type must be one of OAUTH2, BASIC_AUTH, API_KEY, CERT")
	}
	if authType == "OAUTH2" {
		if strings.TrimSpace(inp.AuthConfig.TokenEndpointKMSRef) == "" {
			return errors.New("auth_config.token_endpoint_kms_ref is required for OAUTH2")
		}
		if err := validateKMSPath("token_endpoint_kms_ref", inp.AuthConfig.TokenEndpointKMSRef); err != nil {
			return fmt.Errorf("auth_config.token_endpoint_kms_ref: %w", err)
		}
		if strings.TrimSpace(inp.AuthConfig.ClientIDKMSRef) == "" {
			return errors.New("auth_config.client_id_kms_ref is required for OAUTH2")
		}
		if err := validateKMSPath("client_id_kms_ref", inp.AuthConfig.ClientIDKMSRef); err != nil {
			return fmt.Errorf("auth_config.client_id_kms_ref: %w", err)
		}
		if strings.TrimSpace(inp.AuthConfig.ClientSecretKMSRef) == "" {
			return errors.New("auth_config.client_secret_kms_ref is required for OAUTH2")
		}
		if err := validateKMSPath("client_secret_kms_ref", inp.AuthConfig.ClientSecretKMSRef); err != nil {
			return fmt.Errorf("auth_config.client_secret_kms_ref: %w", err)
		}
	}
	if authType == "BASIC_AUTH" {
		if strings.TrimSpace(inp.AuthConfig.ClientSecretKMSRef) == "" {
			return errors.New("auth_config.client_secret_kms_ref is required for BASIC_AUTH")
		}
		if err := validateKMSPath("client_secret_kms_ref", inp.AuthConfig.ClientSecretKMSRef); err != nil {
			return fmt.Errorf("auth_config.client_secret_kms_ref: %w", err)
		}
	}
	if authType == "API_KEY" {
		if strings.TrimSpace(inp.AuthConfig.APIKeyKMSRef) == "" {
			return errors.New("auth_config.api_key_kms_ref is required for API_KEY")
		}
		if err := validateKMSPath("api_key_kms_ref", inp.AuthConfig.APIKeyKMSRef); err != nil {
			return fmt.Errorf("auth_config.api_key_kms_ref: %w", err)
		}
	}
	if authType == "CERT" {
		if strings.TrimSpace(inp.AuthConfig.CertKMSRef) == "" {
			return errors.New("auth_config.cert_kms_ref is required for CERT")
		}
		if err := validateKMSPath("cert_kms_ref", inp.AuthConfig.CertKMSRef); err != nil {
			return fmt.Errorf("auth_config.cert_kms_ref: %w", err)
		}
	}
	return nil
}

// ── CreateERPSystem ───────────────────────────────────────────────────────────

func CreateERPSystem(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			ERPSystemInput
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if err := validateERPSystemInput(req.ERPSystemInput); err != nil {
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

		var erpID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO apibox.erp_system_master (
				counterparty_id, erp_code, erp_type, version, base_url, timezone, default_currency
			) VALUES ($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''),$7)
			RETURNING erp_system_id`,
			req.CounterpartyID, req.ERPCode, strings.ToUpper(req.ERPType),
			req.Version, req.BaseURL, req.Timezone, strings.ToUpper(req.DefaultCurrency),
		).Scan(&erpID); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "ERP system insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		authType := strings.ToUpper(req.AuthConfig.AuthType)
		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.erp_auth_config (
				erp_system_id, auth_type,
				token_endpoint_kms_ref, client_id_kms_ref, client_secret_kms_ref,
				api_key_kms_ref, cert_kms_ref, scopes
			) VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),NULLIF($6,''),NULLIF($7,''),NULLIF($8,''))`,
			erpID, authType,
			req.AuthConfig.TokenEndpointKMSRef, req.AuthConfig.ClientIDKMSRef,
			req.AuthConfig.ClientSecretKMSRef, req.AuthConfig.APIKeyKMSRef,
			req.AuthConfig.CertKMSRef, req.AuthConfig.Scopes,
		); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "ERP auth config insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_erp_system (erp_system_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, erpID, userEmail); err != nil {
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
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "ERP_SYSTEM_CREATE",
				RecordID: id, RecordTable: "apibox.erp_system_master",
				AuditTable: "apibox.audit_erp_system", AuditIDColumn: "erp_system_id",
				ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail, MatrixID: "",
			})
		}(erpID, req.UserID, userEmail)

		go func(id, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/counterparty-hub/erp-system/create", id,
				map[string]interface{}{"record_id": id, "event": "ERP_SYSTEM_CREATED", "actor_email": uEmail},
			)
		}(erpID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true, "erp_system_id": erpID, "requested": userEmail,
		})
		api.LogInfo("ERP system created: ID=%s by=%s", erpID, userEmail)
	}
}

// ── CreateERPSystemBulk ───────────────────────────────────────────────────────

func CreateERPSystemBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string           `json:"user_id"`
			Rows   []ERPSystemInput `json:"rows"`
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
			if err := validateERPSystemInput(row); err != nil {
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			id, err := insertERPSystemInTx(ctx, pgxPool, row, userEmail)
			if err != nil {
				msg, _ := getUserFriendlyCounterpartyError(err, "Insert failed")
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: msg})
				continue
			}
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "erp_system_id": id})
		}

		api.RespondWithPayload(w, len(inserted) > 0, "", append(inserted, errList...))
	}
}

func insertERPSystemRow(ctx context.Context, tx pgx.Tx, inp ERPSystemInput, userEmail string) (string, error) {
	var id string
	if err := tx.QueryRow(ctx, `
		INSERT INTO apibox.erp_system_master (
			counterparty_id, erp_code, erp_type, version, base_url, timezone, default_currency
		) VALUES ($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''),$7)
		RETURNING erp_system_id`,
		inp.CounterpartyID, inp.ERPCode, strings.ToUpper(inp.ERPType),
		inp.Version, inp.BaseURL, inp.Timezone, strings.ToUpper(inp.DefaultCurrency),
	).Scan(&id); err != nil {
		return "", err
	}

	authType := strings.ToUpper(inp.AuthConfig.AuthType)
	if _, err := tx.Exec(ctx, `
		INSERT INTO apibox.erp_auth_config (
			erp_system_id, auth_type,
			token_endpoint_kms_ref, client_id_kms_ref, client_secret_kms_ref,
			api_key_kms_ref, cert_kms_ref, scopes
		) VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),NULLIF($6,''),NULLIF($7,''),NULLIF($8,''))`,
		id, authType,
		inp.AuthConfig.TokenEndpointKMSRef, inp.AuthConfig.ClientIDKMSRef,
		inp.AuthConfig.ClientSecretKMSRef, inp.AuthConfig.APIKeyKMSRef,
		inp.AuthConfig.CertKMSRef, inp.AuthConfig.Scopes,
	); err != nil {
		return "", err
	}

	if _, err := tx.Exec(ctx, `INSERT INTO apibox.audit_erp_system (erp_system_id, action_type, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, id, userEmail); err != nil {
		return "", err
	}
	return id, nil
}

func insertERPSystemInTx(ctx context.Context, pool *pgxpool.Pool, inp ERPSystemInput, userEmail string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id, err := insertERPSystemRow(ctx, tx, inp, userEmail)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// ── UpdateERPSystem ───────────────────────────────────────────────────────────

func UpdateERPSystem(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string                 `json:"user_id"`
			ERPSystemID string                 `json:"erp_system_id"`
			Fields      map[string]interface{} `json:"fields"`
			AuthConfig  *ERPAuthConfigInput    `json:"auth_config,omitempty"`
			Reason      string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ERPSystemID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "erp_system_id is required")
			return
		}
		if len(req.Fields) == 0 && req.AuthConfig == nil {
			api.RespondWithError(w, http.StatusBadRequest, "fields or auth_config is required")
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

		var oldERPType, oldBaseURL, oldVersion string
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(erp_type,''), COALESCE(base_url,''), COALESCE(version,'')
			FROM apibox.erp_system_master WHERE erp_system_id=$1 FOR UPDATE`, req.ERPSystemID,
		).Scan(&oldERPType, &oldBaseURL, &oldVersion); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "ERP system record not found")
				return
			}
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Update erp_system_master fields
		if len(req.Fields) > 0 {
			allowedMain := map[string]bool{
				"erp_type": true, "version": true, "base_url": true,
				"timezone": true, "default_currency": true,
			}
			var sets []string
			var args []interface{}
			pos := 1
			for k, v := range req.Fields {
				k = strings.ToLower(k)
				if !allowedMain[k] {
					continue
				}
				sets = append(sets, fmt.Sprintf("%s=$%d", k, pos))
				args = append(args, v)
				pos++
			}
			if len(sets) > 0 {
				args = append(args, req.ERPSystemID)
				if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE apibox.erp_system_master SET %s WHERE erp_system_id=$%d", strings.Join(sets, ","), pos), args...); err != nil {
					msg, status := getUserFriendlyCounterpartyError(err, "ERP system update failed")
					api.RespondWithError(w, status, msg)
					return
				}
			}
		}

		// Update erp_auth_config if provided
		if req.AuthConfig != nil {
			authType := strings.ToUpper(req.AuthConfig.AuthType)
			if !validAuthTypes[authType] {
				api.RespondWithError(w, http.StatusBadRequest, "auth_config.auth_type must be one of OAUTH2, BASIC_AUTH, API_KEY, CERT")
				return
			}
			// Validate KMS refs for the new auth config
			if authType == "OAUTH2" {
				if req.AuthConfig.TokenEndpointKMSRef != "" {
					if err := validateKMSPath("token_endpoint_kms_ref", req.AuthConfig.TokenEndpointKMSRef); err != nil {
						api.RespondWithError(w, http.StatusBadRequest, err.Error())
						return
					}
				}
				if req.AuthConfig.ClientIDKMSRef != "" {
					if err := validateKMSPath("client_id_kms_ref", req.AuthConfig.ClientIDKMSRef); err != nil {
						api.RespondWithError(w, http.StatusBadRequest, err.Error())
						return
					}
				}
				if req.AuthConfig.ClientSecretKMSRef != "" {
					if err := validateKMSPath("client_secret_kms_ref", req.AuthConfig.ClientSecretKMSRef); err != nil {
						api.RespondWithError(w, http.StatusBadRequest, err.Error())
						return
					}
				}
			}
			if authType == "BASIC_AUTH" && req.AuthConfig.ClientSecretKMSRef != "" {
				if err := validateKMSPath("client_secret_kms_ref", req.AuthConfig.ClientSecretKMSRef); err != nil {
					api.RespondWithError(w, http.StatusBadRequest, err.Error())
					return
				}
			}
			if authType == "API_KEY" && req.AuthConfig.APIKeyKMSRef != "" {
				if err := validateKMSPath("api_key_kms_ref", req.AuthConfig.APIKeyKMSRef); err != nil {
					api.RespondWithError(w, http.StatusBadRequest, err.Error())
					return
				}
			}
			if authType == "CERT" && req.AuthConfig.CertKMSRef != "" {
				if err := validateKMSPath("cert_kms_ref", req.AuthConfig.CertKMSRef); err != nil {
					api.RespondWithError(w, http.StatusBadRequest, err.Error())
					return
				}
			}

			if _, err := tx.Exec(ctx, `
				UPDATE apibox.erp_auth_config SET
					auth_type=$1,
					token_endpoint_kms_ref=NULLIF($2,''),
					client_id_kms_ref=NULLIF($3,''),
					client_secret_kms_ref=NULLIF($4,''),
					api_key_kms_ref=NULLIF($5,''),
					cert_kms_ref=NULLIF($6,''),
					scopes=NULLIF($7,'')
				WHERE erp_system_id=$8`,
				authType,
				req.AuthConfig.TokenEndpointKMSRef, req.AuthConfig.ClientIDKMSRef,
				req.AuthConfig.ClientSecretKMSRef, req.AuthConfig.APIKeyKMSRef,
				req.AuthConfig.CertKMSRef, req.AuthConfig.Scopes,
				req.ERPSystemID,
			); err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, "ERP auth config update failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_erp_system (
				erp_system_id, action_type, processing_status, reason, requested_by, requested_at,
				old_erp_type, old_base_url, old_version
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6)`,
			req.ERPSystemID, req.Reason, userEmail, oldERPType, oldBaseURL, oldVersion); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "erp_system_id": req.ERPSystemID})
	}
}

// ── BulkApproveERPSystem ──────────────────────────────────────────────────────

func BulkApproveERPSystem(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			ERPSystemIDs []string `json:"erp_system_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE approvals only
		pendingDel := dependency.GetPendingDeleteIDs(r.Context(), pgxPool, "apibox.audit_erp_system", "erp_id", req.ERPSystemIDs)
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "mastererpsystem", pendingDel)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.ERPSystemIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.ERPSystemIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.ERPSystemIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoErpSystemIDsProvided)
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
			SELECT audit_id, erp_system_id, action_type, requested_by
			FROM apibox.audit_erp_system
			WHERE erp_system_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
			FOR UPDATE`, req.ERPSystemIDs)
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
				logger.LogError("approveErp: audits scan failed: %v", err)
			}
			audits = append(audits, a)
		}
		rows.Close()

		var success, errList []map[string]interface{}
		for _, a := range audits {
			if a.ReqBy == userEmail {
				errList = append(errList, map[string]interface{}{"erp_system_id": a.ID, constants.ValueSuccess: false, constants.ValueError: "You cannot approve your own submission"})
				continue
			}
			if strings.ToUpper(a.ActionType) == "DELETE" {
				if _, err := tx.Exec(ctx, `UPDATE apibox.erp_system_master SET is_deleted=true WHERE erp_system_id=$1`, a.ID); err != nil {
					logger.LogError("approveErp: delete erp_system_master exec failed: %v", err)
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE apibox.audit_erp_system SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE audit_id=$3`, userEmail, req.Comment, a.AuditID); err != nil {
				errList = append(errList, map[string]interface{}{"erp_system_id": a.ID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "erp_system_id": a.ID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, len(success) > 0, "", append(success, errList...))
	}
}

// ── BulkRejectERPSystem ───────────────────────────────────────────────────────

func BulkRejectERPSystem(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			ERPSystemIDs []string `json:"erp_system_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ERPSystemIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoErpSystemIDsProvided)
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
			UPDATE apibox.audit_erp_system SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE erp_system_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'`,
			userEmail, req.Comment, req.ERPSystemIDs)
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

// ── BulkDeleteERPSystem ───────────────────────────────────────────────────────

func BulkDeleteERPSystem(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			ERPSystemIDs []string `json:"erp_system_ids"`
			Reason       string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "mastererpsystem", req.ERPSystemIDs)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.ERPSystemIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.ERPSystemIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.ERPSystemIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoErpSystemIDsProvided)
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
		for _, id := range req.ERPSystemIDs {
			auditVals = append(auditVals, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", argIdx, argIdx+1, argIdx+2))
			auditArgs = append(auditArgs, id, req.Reason, userEmail)
			argIdx += 3
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO apibox.audit_erp_system (erp_system_id, action_type, processing_status, reason, requested_by, requested_at)
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
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "submitted_count": len(req.ERPSystemIDs)})
	}
}

// ── GetERPSystemAll ───────────────────────────────────────────────────────────

func GetERPSystemAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.erp_system_id)
					a.erp_system_id, a.audit_id, a.action_type, a.processing_status,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at,
					a.checker_comment, a.reason,
					a.old_erp_type, a.old_base_url, a.old_version
				FROM apibox.audit_erp_system a
				ORDER BY a.erp_system_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT erp_system_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS edited_at
				FROM apibox.audit_erp_system
				GROUP BY erp_system_id
			)
			SELECT
				e.erp_system_id, e.counterparty_id, e.erp_code, e.erp_type, e.version,
				e.base_url, e.timezone, e.default_currency,
				e.status, e.is_active, e.is_deleted,
				TO_CHAR(e.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
				TO_CHAR(e.updated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
				ac.auth_type, ac.scopes,
				la.audit_id, la.action_type, la.processing_status,
				la.requested_by, TO_CHAR(la.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				la.checker_by, TO_CHAR(la.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
				la.checker_comment, la.reason,
				la.old_erp_type, la.old_base_url, la.old_version,
				h.created_by, h.created_at AS history_created_at,
				h.edited_by, h.edited_at AS history_edited_at
			FROM apibox.erp_system_master e
			LEFT JOIN apibox.erp_auth_config ac ON ac.erp_system_id = e.erp_system_id
			LEFT JOIN latest_audit la ON la.erp_system_id = e.erp_system_id
			LEFT JOIN history h ON h.erp_system_id = e.erp_system_id
			WHERE e.is_deleted = false
			ORDER BY e.created_at DESC`

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

// ── GetERPSystemAuditHistory ──────────────────────────────────────────────────

func GetERPSystemAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		erpID := r.URL.Query().Get("erp_system_id")

		baseQ := `
			SELECT audit_id, erp_system_id, action_type, processing_status, reason,
				requested_by, TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				checker_by, TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
				checker_comment, old_erp_type, old_base_url, old_version
			FROM apibox.audit_erp_system`

		var rows pgx.Rows
		var err error
		if erpID != "" {
			rows, err = pgxPool.Query(ctx, baseQ+" WHERE erp_system_id=$1 ORDER BY requested_at DESC", erpID)
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

// ── GetERPSystemApprovedActive ────────────────────────────────────────────────

func GetERPSystemApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		erpType := r.URL.Query().Get("type")

		var q string
		var args []interface{}
		if erpType != "" {
			q = `SELECT e.erp_system_id, e.counterparty_id, e.erp_code, e.erp_type, e.version,
				e.base_url, e.timezone, e.default_currency, ac.auth_type
				FROM apibox.erp_system_master e
				LEFT JOIN apibox.erp_auth_config ac ON ac.erp_system_id = e.erp_system_id
				WHERE e.status='ACTIVE' AND e.is_active=true AND e.is_deleted=false AND e.erp_type=$1
				ORDER BY e.erp_code`
			args = []interface{}{strings.ToUpper(erpType)}
		} else {
			q = `SELECT e.erp_system_id, e.counterparty_id, e.erp_code, e.erp_type, e.version,
				e.base_url, e.timezone, e.default_currency, ac.auth_type
				FROM apibox.erp_system_master e
				LEFT JOIN apibox.erp_auth_config ac ON ac.erp_system_id = e.erp_system_id
				WHERE e.status='ACTIVE' AND e.is_active=true AND e.is_deleted=false
				ORDER BY e.erp_code`
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

// ── GetERPSystemDetail ────────────────────────────────────────────────────────

func GetERPSystemDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		erpID := r.URL.Query().Get("erp_system_id")
		if erpID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "erp_system_id query param is required")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				e.erp_system_id, e.counterparty_id, e.erp_code, e.erp_type, e.version,
				e.base_url, e.timezone, e.default_currency,
				e.status, e.is_active, e.is_deleted,
				TO_CHAR(e.created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at,
				TO_CHAR(e.updated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS updated_at,
				ac.auth_type, ac.scopes,
				(SELECT row_to_json(a) FROM (
					SELECT audit_id, action_type, processing_status, reason,
						requested_by, TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
						checker_by, TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
						checker_comment
					FROM apibox.audit_erp_system
					WHERE erp_system_id = e.erp_system_id
					ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
					LIMIT 1
				) a) AS latest_audit
			FROM apibox.erp_system_master e
			LEFT JOIN apibox.erp_auth_config ac ON ac.erp_system_id = e.erp_system_id
			WHERE e.erp_system_id=$1`, erpID)
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
		api.RespondWithError(w, http.StatusNotFound, "ERP system record not found")
	}
}
