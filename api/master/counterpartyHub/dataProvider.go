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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Request types ─────────────────────────────────────────────────────────────

type DataProviderInput struct {
	CounterpartyID     string   `json:"counterparty_id"`
	ProviderCode       string   `json:"provider_code"`
	ProviderType       string   `json:"provider_type"`
	DeliveryMechanism  string   `json:"delivery_mechanism"`
	APICredsKMSRef     string   `json:"api_creds_kms_ref"`
	EntitlementCodes   string   `json:"entitlement_codes,omitempty"`
	RenewalDate        string   `json:"renewal_date,omitempty"`
	RefreshIntervalSec int      `json:"refresh_interval_sec"`
	DataTypes          []string `json:"data_types,omitempty"`
}

func validateDataProviderInput(inp DataProviderInput) error {
	if strings.TrimSpace(inp.CounterpartyID) == "" {
		return errors.New("counterparty_id is required")
	}
	if strings.TrimSpace(inp.ProviderCode) == "" {
		return errors.New("provider_code is required")
	}
	validTypes := map[string]bool{"REAL_TIME": true, "DELAYED": true, "EOD_BATCH": true, "REFERENCE": true, "RATINGS": true, "ANALYTICS": true}
	if !validTypes[strings.ToUpper(inp.ProviderType)] {
		return errors.New("provider_type must be one of REAL_TIME, DELAYED, EOD_BATCH, REFERENCE, RATINGS, ANALYTICS")
	}
	validDelivery := map[string]bool{"REST": true, "WEBSOCKET": true, "SFTP": true, "FTP": true}
	if !validDelivery[strings.ToUpper(inp.DeliveryMechanism)] {
		return errors.New("delivery_mechanism must be one of REST, WEBSOCKET, SFTP, FTP")
	}
	if strings.TrimSpace(inp.APICredsKMSRef) == "" {
		return errors.New("api_creds_kms_ref is required")
	}
	if err := validateKMSPath("api_creds_kms_ref", inp.APICredsKMSRef); err != nil {
		return err
	}
	if inp.RefreshIntervalSec < 1 || inp.RefreshIntervalSec > 86400 {
		return errors.New("refresh_interval_sec must be between 1 and 86400")
	}
	return nil
}

// ── CreateDataProvider ────────────────────────────────────────────────────────

func CreateDataProvider(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			DataProviderInput
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if err := validateDataProviderInput(req.DataProviderInput); err != nil {
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

		var providerID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO apibox.data_provider_master (
				counterparty_id, provider_code, provider_type, delivery_mechanism,
				api_creds_kms_ref, entitlement_codes, renewal_date, refresh_interval_sec
			) VALUES ($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),$8)
			RETURNING provider_id`,
			req.CounterpartyID, req.ProviderCode, strings.ToUpper(req.ProviderType),
			strings.ToUpper(req.DeliveryMechanism), req.APICredsKMSRef,
			req.EntitlementCodes, req.RenewalDate, req.RefreshIntervalSec,
		).Scan(&providerID); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Data provider insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		for _, dt := range req.DataTypes {
			if _, err := tx.Exec(ctx,
				`INSERT INTO apibox.data_provider_data_types (provider_id, data_type) VALUES ($1,$2)`,
				providerID, strings.ToUpper(dt)); err != nil {
				msg, status := getUserFriendlyCounterpartyError(err, "Data type insert failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_data_provider
				(provider_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, providerID, userEmail); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		go func(pid, uID, uEmail string) {
			defer func() { recover() }()
			bgCtx := context.Background()
			_, _ = approvalengine.CreateInstance(bgCtx, pgxPool, approvalengine.InstanceRequest{
				ModuleCode: "COUNTERPARTY_HUB", TransactionType: "DATA_PROVIDER_CREATE",
				RecordID: pid, RecordTable: "apibox.data_provider_master",
				AuditTable: "apibox.audit_data_provider", AuditIDColumn: "provider_id",
				ActionType: "CREATE", SubmittedBy: uID, SubmittedByEmail: uEmail,
			})
		}(providerID, req.UserID, userEmail)

		go func(pid, uEmail string) {
			defer func() { recover() }()
			notifcatalog.TriggerNotification(context.Background(), pgxPool,
				"/master/counterparty-hub/data-provider/create", pid,
				map[string]interface{}{"record_id": pid, "event": "DATA_PROVIDER_CREATED", "actor_email": uEmail},
			)
		}(providerID, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true, "provider_id": providerID, "requested": userEmail,
		})
		api.LogInfo("DataProvider created: ID=%s by=%s", providerID, userEmail)
	}
}

// ── CreateDataProviderBulk ────────────────────────────────────────────────────

func CreateDataProviderBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string              `json:"user_id"`
			Rows   []DataProviderInput `json:"rows"`
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
			if err := validateDataProviderInput(row); err != nil {
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			pid, err := insertDataProviderInTx(ctx, pgxPool, row, userEmail)
			if err != nil {
				msg, _ := getUserFriendlyCounterpartyError(err, "Insert failed")
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: msg})
				continue
			}
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "provider_id": pid})
		}

		api.RespondWithPayload(w, len(inserted) > 0, "", append(inserted, errList...))
	}
}

func insertDataProviderRow(ctx context.Context, tx pgx.Tx, inp DataProviderInput, userEmail string) (string, error) {
	var pid string
	if err := tx.QueryRow(ctx, `
		INSERT INTO apibox.data_provider_master (
			counterparty_id, provider_code, provider_type, delivery_mechanism,
			api_creds_kms_ref, entitlement_codes, renewal_date, refresh_interval_sec
		) VALUES ($1,$2,$3,$4,$5,NULLIF($6,''),NULLIF($7,''),$8)
		RETURNING provider_id`,
		inp.CounterpartyID, inp.ProviderCode, strings.ToUpper(inp.ProviderType),
		strings.ToUpper(inp.DeliveryMechanism), inp.APICredsKMSRef,
		inp.EntitlementCodes, inp.RenewalDate, inp.RefreshIntervalSec,
	).Scan(&pid); err != nil {
		return "", err
	}
	for _, dt := range inp.DataTypes {
		if _, err := tx.Exec(ctx, `INSERT INTO apibox.data_provider_data_types (provider_id, data_type) VALUES ($1,$2)`, pid, strings.ToUpper(dt)); err != nil {
			return "", err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO apibox.audit_data_provider (provider_id, action_type, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, pid, userEmail); err != nil {
		return "", err
	}
	return pid, nil
}

func insertDataProviderInTx(ctx context.Context, pool *pgxpool.Pool, inp DataProviderInput, userEmail string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id, err := insertDataProviderRow(ctx, tx, inp, userEmail)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// ── UpdateDataProvider ────────────────────────────────────────────────────────

func UpdateDataProvider(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string                 `json:"user_id"`
			ProviderID string                 `json:"provider_id"`
			Fields     map[string]interface{} `json:"fields"`
			Reason     string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ProviderID == "" || len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "provider_id and fields are required")
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

		var oldType, oldDelivery string
		var oldRefresh int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(provider_type,''), COALESCE(delivery_mechanism,''), COALESCE(refresh_interval_sec,0)
			 FROM apibox.data_provider_master WHERE provider_id=$1 FOR UPDATE`, req.ProviderID,
		).Scan(&oldType, &oldDelivery, &oldRefresh); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Data provider not found")
				return
			}
			msg, status := getUserFriendlyCounterpartyError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		allowed := map[string]bool{"provider_type": true, "delivery_mechanism": true, "api_creds_kms_ref": true, "entitlement_codes": true, "renewal_date": true, "refresh_interval_sec": true}
		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if !allowed[k] {
				continue
			}
			if k == "api_creds_kms_ref" {
				if s, ok := v.(string); ok {
					if err := validateKMSPath("api_creds_kms_ref", s); err != nil {
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
		args = append(args, req.ProviderID)
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE apibox.data_provider_master SET %s WHERE provider_id=$%d", strings.Join(sets, ","), pos), args...); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, "Update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO apibox.audit_data_provider (
				provider_id, action_type, processing_status, reason, requested_by, requested_at,
				old_provider_type, old_delivery_mechanism, old_refresh_interval_sec
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6)`,
			req.ProviderID, req.Reason, userEmail, oldType, oldDelivery, oldRefresh); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "provider_id": req.ProviderID})
	}
}

// ── BulkApproveDataProvider ───────────────────────────────────────────────────

func BulkApproveDataProvider(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ProviderIDs []string `json:"provider_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE approvals only
		pendingDel := dependency.GetPendingDeleteIDs(r.Context(), pgxPool, "apibox.audit_data_provider", "provider_id", req.ProviderIDs)
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "masterdataprovider", pendingDel)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.ProviderIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.ProviderIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.ProviderIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoProviderIDsProvided)
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
			SELECT audit_id, provider_id, action_type, requested_by
			FROM apibox.audit_data_provider
			WHERE provider_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
			FOR UPDATE`, req.ProviderIDs)
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
				logger.LogError("approveDataProvider: audits scan failed: %v", err)
			}
			audits = append(audits, a)
		}
		rows.Close()

		var success, errList []map[string]interface{}
		for _, a := range audits {
			if a.ReqBy == userEmail {
				errList = append(errList, map[string]interface{}{
					"provider_id": a.ID, constants.ValueSuccess: false, constants.ValueError: "You cannot approve your own submission",
				})
				continue
			}
			if strings.ToUpper(a.ActionType) == "DELETE" {
				if _, err := tx.Exec(ctx, `UPDATE apibox.data_provider_master SET is_deleted=true WHERE provider_id=$1`, a.ID); err != nil {
					logger.LogError("approveDataProvider: delete data_provider_master exec failed: %v", err)
				}
			}
			if _, err := tx.Exec(ctx, `
				UPDATE apibox.audit_data_provider
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE audit_id=$3`, userEmail, req.Comment, a.AuditID); err != nil {
				errList = append(errList, map[string]interface{}{"provider_id": a.ID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}
			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "provider_id": a.ID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCounterpartyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, len(success) > 0, "", append(success, errList...))
	}
}

// ── BulkRejectDataProvider ────────────────────────────────────────────────────

func BulkRejectDataProvider(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ProviderIDs []string `json:"provider_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.ProviderIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoProviderIDsProvided)
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
			UPDATE apibox.audit_data_provider
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE provider_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'`,
			userEmail, req.Comment, req.ProviderIDs)
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
		api.RespondWithPayload(w, res.RowsAffected() > 0, "", map[string]interface{}{
			constants.ValueSuccess: true, "rejected_count": res.RowsAffected(),
		})
	}
}

// ── BulkDeleteDataProvider ────────────────────────────────────────────────────

func BulkDeleteDataProvider(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			ProviderIDs []string `json:"provider_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		// [AUTO] Partial Success Filter for DELETE
		blocked := dependency.CheckBulkBlockers(r.Context(), pgxPool, "masterdataprovider", req.ProviderIDs)
		if len(blocked) > 0 {
			blockedSet := make(map[string]bool)
			for _, b := range blocked {
				blockedSet[b["id"].(string)] = true
			}
			var unblocked []string
			for _, id := range req.ProviderIDs {
				if !blockedSet[id] {
					unblocked = append(unblocked, id)
				}
			}
			req.ProviderIDs = unblocked
		}
		w = dependency.NewBulkResponseInterceptor(w, blocked)
		if len(req.ProviderIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoProviderIDsProvided)
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
		for _, id := range req.ProviderIDs {
			auditVals = append(auditVals, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", argIdx, argIdx+1, argIdx+2))
			auditArgs = append(auditArgs, id, req.Reason, userEmail)
			argIdx += 3
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO apibox.audit_data_provider (provider_id, action_type, processing_status, reason, requested_by, requested_at)
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
		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "submitted_count": len(req.ProviderIDs)})
	}
}

// ── GetDataProviderAll ────────────────────────────────────────────────────────

func GetDataProviderAll(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.provider_id)
					a.provider_id, a.audit_id, a.action_type, a.processing_status,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at,
					a.checker_comment, a.reason,
					a.old_provider_type, a.old_delivery_mechanism, a.old_refresh_interval_sec
				FROM apibox.audit_data_provider a
				ORDER BY a.provider_id,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
				                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT provider_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS edited_at
				FROM apibox.audit_data_provider
				GROUP BY provider_id
			)
			SELECT
				dp.provider_id, dp.counterparty_id, dp.provider_code, dp.provider_type,
				dp.delivery_mechanism, dp.api_creds_kms_ref,
				COALESCE(dp.entitlement_codes,'') AS entitlement_codes,
				COALESCE(TO_CHAR(dp.renewal_date,'YYYY-MM-DD'),'') AS renewal_date,
				dp.refresh_interval_sec,
				COALESCE(l.audit_id::text,'') AS audit_id,
				COALESCE(l.action_type,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.requested_by,'') AS requested_by,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				COALESCE(l.old_provider_type,'') AS old_provider_type,
				COALESCE(l.old_delivery_mechanism,'') AS old_delivery_mechanism,
				COALESCE(l.old_refresh_interval_sec,0) AS old_refresh_interval_sec,
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at
			FROM apibox.data_provider_master dp
			LEFT JOIN latest_audit l ON l.provider_id = dp.provider_id
			LEFT JOIN history h ON h.provider_id = dp.provider_id
			WHERE COALESCE(dp.is_deleted,false) = false
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

// ── GetDataProviderAuditHistory ───────────────────────────────────────────────

func GetDataProviderAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.URL.Query().Get("provider_id")
		q := `SELECT audit_id, provider_id, action_type, processing_status, reason,
			         requested_by, TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
			         COALESCE(checker_by,''), COALESCE(TO_CHAR(checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
			         COALESCE(checker_comment,''), COALESCE(old_provider_type,''), COALESCE(old_delivery_mechanism,'')
			  FROM apibox.audit_data_provider`
		var args []interface{}
		if id != "" {
			q += " WHERE provider_id=$1 ORDER BY requested_at DESC"
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

// ── GetDataProviderApprovedActive ─────────────────────────────────────────────

func GetDataProviderApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cpID := r.URL.Query().Get("counterparty_id")
		q := `SELECT dp.provider_id, dp.counterparty_id, dp.provider_code, dp.provider_type,
				     dp.delivery_mechanism, dp.refresh_interval_sec
			  FROM apibox.data_provider_master dp
			  INNER JOIN apibox.counterparty_master cp ON cp.counterparty_id=dp.counterparty_id
			  WHERE cp.status='ACTIVE' AND COALESCE(cp.is_deleted,false)=false AND COALESCE(dp.is_deleted,false)=false`
		var args []interface{}
		if cpID != "" {
			q += " AND dp.counterparty_id=$1"
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rows": out})
	}
}

// ── GetDataProviderDetail ─────────────────────────────────────────────────────

func GetDataProviderDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.URL.Query().Get("provider_id")
		if id == "" {
			api.RespondWithError(w, http.StatusBadRequest, "provider_id is required")
			return
		}

		var pid, cpid, pcode, ptype, delivery, apiCreds string
		var entCodes, renewal string
		var refresh int
		if err := pgxPool.QueryRow(ctx, `
			SELECT provider_id, counterparty_id, provider_code, provider_type, delivery_mechanism,
			       api_creds_kms_ref, COALESCE(entitlement_codes,''),
			       COALESCE(TO_CHAR(renewal_date,'YYYY-MM-DD'),''), refresh_interval_sec
			FROM apibox.data_provider_master WHERE provider_id=$1`, id,
		).Scan(&pid, &cpid, &pcode, &ptype, &delivery, &apiCreds, &entCodes, &renewal, &refresh); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Data provider not found")
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		dtRows, _ := pgxPool.Query(ctx, `SELECT data_type FROM apibox.data_provider_data_types WHERE provider_id=$1`, id)
		var dataTypes []string
		for dtRows.Next() {
			var dt string
			if err := dtRows.Scan(&dt); err != nil {
				logger.LogError("getDataProvider: data type scan failed: %v", err)
			}
			dataTypes = append(dataTypes, dt)
		}
		dtRows.Close()

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data": map[string]interface{}{
				"provider_id": pid, "counterparty_id": cpid, "provider_code": pcode,
				"provider_type": ptype, "delivery_mechanism": delivery, "api_creds_kms_ref": apiCreds,
				"entitlement_codes": entCodes, "renewal_date": renewal, "refresh_interval_sec": refresh,
				"data_types": dataTypes,
			},
		})
	}
}
