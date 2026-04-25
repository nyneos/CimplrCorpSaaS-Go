package counterpartyHub

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// insertBankDetail inserts one bank_master row for Hub v2 (BRD 5.4 schema).
// tx is already open; caller handles commit/rollback.
func insertBankDetail(ctx context.Context, tx pgx.Tx, cpID string, f map[string]interface{}) (string, error) {
	var id string
	err := tx.QueryRow(ctx, `
		INSERT INTO apibox_svc.bank_master (
			counterparty_id, bank_code, bank_name, bank_type, routing_number, entity_code
		) VALUES ($1,$2,$3,$4,$5,$6)
		RETURNING bank_id`,
		cpID,
		f["bank_code"], f["bank_name"], f["bank_type"],
		nilIfEmpty(f["routing_number"]), f["entity_code"],
	).Scan(&id)
	return id, err
}

// insertExchangeDetail inserts exchange_master + exchange_asset_classes rows (BRD 5.5 schema).
func insertExchangeDetail(ctx context.Context, tx pgx.Tx, cpID string, f map[string]interface{}) (string, error) {
	var id string
	err := tx.QueryRow(ctx, `
		INSERT INTO apibox_svc.exchange_master (
			counterparty_id, exchange_code, mic_code, connectivity_protocol,
			fix_session_role, fix_sender_comp_id, fix_target_comp_id,
			fix_primary_host, fix_primary_port, fix_failover_host, fix_failover_port,
			fix_creds_kms_ref, fix_heartbeat_sec
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		RETURNING exchange_id`,
		cpID,
		f["exchange_code"], f["mic_code"], f["connectivity_protocol"],
		nilIfEmpty(f["fix_session_role"]), nilIfEmpty(f["fix_sender_comp_id"]),
		nilIfEmpty(f["fix_target_comp_id"]), nilIfEmpty(f["fix_primary_host"]),
		nilIfZero(f["fix_primary_port"]), nilIfEmpty(f["fix_failover_host"]),
		nilIfZero(f["fix_failover_port"]), nilIfEmpty(f["fix_creds_kms_ref"]),
		intOrDefault(f["fix_heartbeat_sec"], 30),
	).Scan(&id)
	if err != nil {
		return "", err
	}
	for _, ac := range toStringSlice(f["asset_classes"]) {
		if _, err := tx.Exec(ctx,
			`INSERT INTO apibox_svc.exchange_asset_classes (exchange_id, asset_class) VALUES ($1,$2)`,
			id, ac); err != nil {
			return "", err
		}
	}
	return id, nil
}

// insertDataProviderDetail inserts data_provider_master + dp_data_types rows (BRD 5.6 schema).
func insertDataProviderDetail(ctx context.Context, tx pgx.Tx, cpID string, f map[string]interface{}) (string, error) {
	var id string
	err := tx.QueryRow(ctx, `
		INSERT INTO apibox_svc.data_provider_master (
			counterparty_id, provider_code, connectivity_protocol,
			api_creds_kms_ref, refresh_interval_sec, entitlement_codes, renewal_date
		) VALUES ($1,$2,$3,$4,$5,$6,$7)
		RETURNING provider_id`,
		cpID,
		f["provider_code"], f["connectivity_protocol"],
		f["api_creds_kms_ref"], intOrDefault(f["refresh_interval_sec"], 60),
		nilIfEmpty(f["entitlement_codes"]), nilIfEmpty(f["renewal_date"]),
	).Scan(&id)
	if err != nil {
		return "", err
	}
	for _, dt := range toStringSlice(f["data_types"]) {
		if _, err := tx.Exec(ctx,
			`INSERT INTO apibox_svc.dp_data_types (provider_id, data_type) VALUES ($1,$2)`,
			id, dt); err != nil {
			return "", err
		}
	}
	return id, nil
}

// insertCcpCsdDetail inserts ccp_csd_master row (BRD 5.7 schema).
func insertCcpCsdDetail(ctx context.Context, tx pgx.Tx, cpID string, f map[string]interface{}) (string, error) {
	var id string
	err := tx.QueryRow(ctx, `
		INSERT INTO apibox_svc.ccp_csd_master (
			counterparty_id, entity_code, entity_sub_type, clearing_acct_kms_ref
		) VALUES ($1,$2,$3,$4)
		RETURNING ccp_csd_id`,
		cpID,
		f["entity_code_ccp"], f["entity_sub_type"], f["clearing_acct_kms_ref"],
	).Scan(&id)
	return id, err
}

// insertPaymentNetworkDetail inserts payment_network_master row (BRD 5.8 schema).
func insertPaymentNetworkDetail(ctx context.Context, tx pgx.Tx, cpID string, f map[string]interface{}) (string, error) {
	var id string
	err := tx.QueryRow(ctx, `
		INSERT INTO apibox_svc.payment_network_master (
			counterparty_id, network_code, network_type, max_txn_amount
		) VALUES ($1,$2,$3,$4)
		RETURNING network_id`,
		cpID,
		f["network_code"], f["network_type"], nilIfZero(f["max_txn_amount"]),
	).Scan(&id)
	return id, err
}

// insertErpDetail inserts erp_system_master row (BRD 5.9 schema).
func insertErpDetail(ctx context.Context, tx pgx.Tx, cpID string, f map[string]interface{}) (string, error) {
	var id string
	err := tx.QueryRow(ctx, `
		INSERT INTO apibox_svc.erp_system_master (
			counterparty_id, system_code, erp_type, version, hosted_by,
			system_url, auth_type,
			token_endpoint_kms_ref, client_id_kms_ref, client_secret_kms_ref
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING erp_id`,
		cpID,
		f["system_code"], f["erp_type"], nilIfEmpty(f["version"]),
		f["hosted_by"], f["system_url"], f["auth_type"],
		nilIfEmpty(f["token_endpoint_kms_ref"]),
		nilIfEmpty(f["client_id_kms_ref"]),
		nilIfEmpty(f["client_secret_kms_ref"]),
	).Scan(&id)
	return id, err
}

// reqToFieldsMap converts a HubCreateRequest to the map[string]interface{} consumed
// by insertTypedDetail. All field keys match the JSON tag names.
func reqToFieldsMap(req HubCreateRequest) map[string]interface{} {
	return map[string]interface{}{
		// BANK
		"bank_code":      req.BankCode,
		"bank_name":      req.BankName,
		"bank_type":      req.BankType,
		"routing_number": req.RoutingNumber,
		"entity_code":    req.EntityCode,

		// EXCHANGE
		"exchange_code":          req.ExchangeCode,
		"mic_code":               req.MICCode,
		"asset_classes":          req.AssetClasses,
		"connectivity_protocol":  req.ConnectivityProtocol,
		"fix_session_role":        req.FIXSessionRole,
		"fix_sender_comp_id":      req.FIXSenderCompID,
		"fix_target_comp_id":      req.FIXTargetCompID,
		"fix_primary_host":        req.FIXPrimaryHost,
		"fix_primary_port":        req.FIXPrimaryPort,
		"fix_failover_host":       req.FIXFailoverHost,
		"fix_failover_port":       req.FIXFailoverPort,
		"fix_creds_kms_ref":       req.FIXCredsKMSRef,
		"fix_heartbeat_sec":       req.FIXHeartbeatSec,

		// DATA_PROVIDER
		"provider_code":       req.ProviderCode,
		"data_types":          req.DataTypes,
		"api_creds_kms_ref":   req.APICredsKMSRef,
		"refresh_interval_sec": req.RefreshIntervalSec,
		"entitlement_codes":   req.EntitlementCodes,
		"renewal_date":        req.RenewalDate,

		// CCP_CSD
		"entity_code_ccp":      req.EntityCodeCCP,
		"entity_sub_type":      req.EntitySubType,
		"clearing_acct_kms_ref": req.ClearingAcctKMSRef,

		// PAYMENT_NETWORK
		"network_code":    req.NetworkCode,
		"network_type":    req.NetworkType,
		"max_txn_amount":  req.MaxTxnAmount,

		// ERP_SYSTEM
		"system_code":             req.SystemCode,
		"erp_type":                req.ERPType,
		"version":                 req.Version,
		"hosted_by":               req.HostedBy,
		"system_url":              req.SystemURL,
		"auth_type":               req.AuthType,
		"token_endpoint_kms_ref":  req.TokenEndpointKMSRef,
		"client_id_kms_ref":       req.ClientIDKMSRef,
		"client_secret_kms_ref":   req.ClientSecretKMSRef,
	}
}

// insertBankDetail audit shim — inserts audit_bank_master row via typed audit table.
// Called from hub2.go after insertTypedDetail returns the typed ID.
func insertTypedAudit(ctx context.Context, tx pgx.Tx, cpType, typedID, cpID, userEmail string) error {
	auditTable := typedAuditTable(cpType)
	idCol := typedIDColumn(cpType)
	if auditTable == "" || idCol == "" {
		return fmt.Errorf("unknown counterparty_type for audit: %s", cpType)
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (%s, counterparty_id, action_type, processing_status, requested_by, requested_at)
		VALUES ($1,$2,'CREATE','PENDING_APPROVAL',$3,now())`,
		auditTable, idCol),
		typedID, cpID, userEmail,
	)
	return err
}
