package counterpartyHub

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// ── UploadCounterpartyHub ─────────────────────────────────────────────────────
//
// POST /master/counterparty-hub/upload
// Multipart form fields:
//   user_id          – required
//   counterparty_type – required: BANK | EXCHANGE | DATA_PROVIDER | CCP_CSD | PAYMENT_NETWORK | ERP_SYSTEM
//   file             – required: CSV or XLSX
//
// The handler parses the file into []map[string]string (header → value),
// then dispatches to the appropriate typed insert function.

func UploadCounterpartyHub(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse multipart form")
			return
		}

		userID := strings.TrimSpace(r.FormValue("user_id"))
		cpType := strings.ToUpper(strings.TrimSpace(r.FormValue("counterparty_type")))

		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id is required")
			return
		}
		validTypes := map[string]bool{
			"BANK": true, "EXCHANGE": true, "DATA_PROVIDER": true,
			"CCP_CSD": true, "PAYMENT_NETWORK": true, "ERP_SYSTEM": true,
		}
		if !validTypes[cpType] {
			api.RespondWithError(w, http.StatusBadRequest, "counterparty_type must be one of BANK, EXCHANGE, DATA_PROVIDER, CCP_CSD, PAYMENT_NETWORK, ERP_SYSTEM")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "file is required")
			return
		}
		defer file.Close()

		filename := strings.ToLower(header.Filename)
		var rows []map[string]string

		switch {
		case strings.HasSuffix(filename, ".csv"):
			rows, err = parseCSV(file)
		case strings.HasSuffix(filename, ".xlsx"):
			rows, err = parseXLSX(file)
		default:
			api.RespondWithError(w, http.StatusBadRequest, "Only CSV (.csv) and Excel (.xlsx) files are supported")
			return
		}
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Failed to parse file: %s", err.Error()))
			return
		}
		if len(rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "File contains no data rows")
			return
		}

		ctx := r.Context()
		var inserted []map[string]interface{}
		var errList []map[string]interface{}

		for i, row := range rows {
			var id string
			var insertErr error

			switch cpType {
			case "BANK":
				cpInp, bankInp := bankRowFromCSV(row)
				if err := validateCounterpartyInput(cpInp); err != nil {
					errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
				if err := validateBankFields(bankInp); err != nil {
					errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
				id, insertErr = insertCounterpartyWithBankInTx(ctx, pgxPool, cpInp, bankInp, userEmail)

			case "EXCHANGE":
				inp := exchangeInputFromRow(row)
				if err := validateExchangeInput(inp); err != nil {
					errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
				id, insertErr = insertExchangeInTx(ctx, pgxPool, inp, userEmail)

			case "DATA_PROVIDER":
				inp := dataProviderInputFromRow(row)
				if err := validateDataProviderInput(inp); err != nil {
					errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
				id, insertErr = insertDataProviderInTx(ctx, pgxPool, inp, userEmail)

			case "CCP_CSD":
				inp := ccpCsdInputFromRow(row)
				if err := validateCcpCsdInput(inp); err != nil {
					errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
				id, insertErr = insertCcpCsdInTx(ctx, pgxPool, inp, userEmail)

			case "PAYMENT_NETWORK":
				inp := paymentNetworkInputFromRow(row)
				if err := validatePaymentNetworkInput(inp); err != nil {
					errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
				id, insertErr = insertPaymentNetworkInTx(ctx, pgxPool, inp, userEmail)

			case "ERP_SYSTEM":
				inp := erpSystemInputFromRow(row)
				if err := validateERPSystemInput(inp); err != nil {
					errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
				id, insertErr = insertERPSystemInTx(ctx, pgxPool, inp, userEmail)
			}

			if insertErr != nil {
				msg, _ := getUserFriendlyCounterpartyError(insertErr, "Insert failed")
				errList = append(errList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: msg})
				continue
			}
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "id": id, "counterparty_type": cpType})
		}

		api.RespondWithPayload(w, len(inserted) > 0, "", map[string]interface{}{
			"inserted_count": len(inserted),
			"error_count":    len(errList),
			"results":        append(inserted, errList...),
		})
	}
}

// ── File parsers ──────────────────────────────────────────────────────────────

func parseCSV(r io.Reader) ([]map[string]string, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("reading CSV header: %w", err)
	}
	for i, h := range headers {
		headers[i] = strings.ToLower(strings.TrimSpace(h))
	}

	var rows []map[string]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading CSV row: %w", err)
		}
		row := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(record) {
				row[h] = strings.TrimSpace(record[i])
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseXLSX(r io.Reader) ([]map[string]string, error) {
	f, err := excelize.OpenReader(r)
	if err != nil {
		return nil, fmt.Errorf("opening xlsx: %w", err)
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("no sheets found in xlsx")
	}

	allRows, err := f.GetRows(sheets[0])
	if err != nil {
		return nil, fmt.Errorf("reading xlsx rows: %w", err)
	}
	if len(allRows) < 2 {
		return nil, nil
	}

	headers := make([]string, len(allRows[0]))
	for i, h := range allRows[0] {
		headers[i] = strings.ToLower(strings.TrimSpace(h))
	}

	var rows []map[string]string
	for _, record := range allRows[1:] {
		row := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(record) {
				row[h] = strings.TrimSpace(record[i])
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ── Row → struct mappers ──────────────────────────────────────────────────────

// bankRowFromCSV splits a flat CSV row into the parent CounterpartyInput and the
// child BankInput so both can be validated and inserted together.
func bankRowFromCSV(row map[string]string) (CounterpartyInput, BankInput) {
	effTo := row["eff_to"]
	var effToPtr *string
	if effTo != "" {
		effToPtr = &effTo
	}
	cp := CounterpartyInput{
		CounterpartyCode: row["counterparty_code"],
		CounterpartyName: row["counterparty_name"],
		ShortName:        row["short_name"],
		CounterpartyType: "BANK",
		Country:          row["country"],
		PrimaryCurrency:  row["primary_currency"],
		LEI:              row["lei"],
		RMEmail:          row["rm_email"],
		EffFrom:          row["eff_from"],
		EffTo:            effToPtr,
		Notes:            row["notes"],
	}
	bank := BankInput{
		BankCode:      row["bank_code"],
		BankName:      row["bank_name"],
		BankType:      row["bank_type"],
		RoutingNumber: row["routing_number"],
		EntityCode:    row["entity_code"],
	}
	return cp, bank
}

// insertCounterpartyWithBankInTx inserts counterparty_master + bank_master + both
// audit rows in a single transaction. Used by the bulk-upload handler.
func insertCounterpartyWithBankInTx(ctx context.Context, pool *pgxpool.Pool, cp CounterpartyInput, bank BankInput, userEmail string) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var cpID string
	if err := tx.QueryRow(ctx, `
		INSERT INTO apibox.counterparty_master (
			counterparty_code, counterparty_name, short_name, counterparty_type,
			country, primary_currency, lei, rm_email, eff_from, eff_to,
			notes, status, created_by
		) VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),$9,$10,NULLIF($11,''),'DRAFT',$12)
		RETURNING counterparty_id`,
		strings.ToUpper(cp.CounterpartyCode), cp.CounterpartyName, cp.ShortName,
		"BANK", strings.ToUpper(cp.Country), strings.ToUpper(cp.PrimaryCurrency),
		cp.LEI, cp.RMEmail, cp.EffFrom, cp.EffTo, cp.Notes, userEmail,
	).Scan(&cpID); err != nil {
		return "", err
	}

	bank.CounterpartyID = cpID
	if _, err := insertBankRow(ctx, tx, bank, userEmail); err != nil {
		return "", err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO apibox.audit_counterparty_master (counterparty_id, action_type, processing_status, requested_by, requested_at)
		VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, cpID, userEmail); err != nil {
		return "", err
	}

	return cpID, tx.Commit(ctx)
}

func exchangeInputFromRow(row map[string]string) ExchangeInput {
	assetClasses := splitCSVField(row["asset_classes"])
	return ExchangeInput{
		CounterpartyID:       row["counterparty_id"],
		MICCode:              row["mic_code"],
		ExchangeType:         row["exchange_type"],
		RegulatoryBody:       row["regulatory_body"],
		OperatingHoursOpen:   row["operating_hours_open"],
		OperatingHoursClose:  row["operating_hours_close"],
		Timezone:             row["timezone"],
		SettlementCycle:      row["settlement_cycle"],
		HolidayCalendarRef:   row["holiday_calendar_ref"],
		AssetClasses:         assetClasses,
		ConnectivityProtocol: row["connectivity_protocol"],
		SessionRole:          row["session_role"],
		SenderCompID:         row["sender_comp_id"],
		TargetCompID:         row["target_comp_id"],
		PrimaryHost:          row["primary_host"],
		PrimaryPort:          parseInt(row["primary_port"]),
		FailoverHost:         row["failover_host"],
		FailoverPort:         parseInt(row["failover_port"]),
		FIXCredsKMSRef:       row["fix_creds_kms_ref"],
		HeartbeatInterval:    parseInt(row["heartbeat_interval"]),
		TLSVersion:           row["tls_version"],
	}
}

func dataProviderInputFromRow(row map[string]string) DataProviderInput {
	dataTypes := splitCSVField(row["data_types"])
	refreshInterval, _ := strconv.Atoi(row["refresh_interval_sec"])
	return DataProviderInput{
		CounterpartyID:     row["counterparty_id"],
		ProviderCode:       row["provider_code"],
		ProviderType:       row["provider_type"],
		DeliveryMechanism:  row["delivery_mechanism"],
		APICredsKMSRef:     row["api_creds_kms_ref"],
		EntitlementCodes:   row["entitlement_codes"],
		RenewalDate:        row["renewal_date"],
		RefreshIntervalSec: refreshInterval,
		DataTypes:          dataTypes,
	}
}

func ccpCsdInputFromRow(row map[string]string) CcpCsdInput {
	return CcpCsdInput{
		CounterpartyID:      row["counterparty_id"],
		EntityCode:          row["entity_code"],
		EntitySubType:       row["entity_sub_type"],
		LEI:                 row["lei"],
		RegulatoryBody:      row["regulatory_body"],
		ParticipantID:       row["participant_id"],
		ClearingAcctKMSRef:  row["clearing_acct_kms_ref"],
		MarginCallFrequency: row["margin_call_frequency"],
	}
}

func paymentNetworkInputFromRow(row map[string]string) PaymentNetworkInput {
	currencies := splitCSVField(row["settlement_currencies"])
	return PaymentNetworkInput{
		CounterpartyID:       row["counterparty_id"],
		NetworkCode:          row["network_code"],
		NetworkType:          row["network_type"],
		BICCode:              row["bic_code"],
		RoutingNumber:        row["routing_number"],
		IBANSupported:        row["iban_supported"] == "true",
		SettlementCurrencies: currencies,
		CutOffTime:           row["cut_off_time"],
		Timezone:             row["timezone"],
		APIEndpointKMSRef:    row["api_endpoint_kms_ref"],
	}
}

func erpSystemInputFromRow(row map[string]string) ERPSystemInput {
	return ERPSystemInput{
		CounterpartyID:  row["counterparty_id"],
		ERPCode:         row["erp_code"],
		ERPType:         row["erp_type"],
		Version:         row["version"],
		BaseURL:         row["base_url"],
		Timezone:        row["timezone"],
		DefaultCurrency: row["default_currency"],
		AuthConfig: ERPAuthConfigInput{
			AuthType:            row["auth_type"],
			TokenEndpointKMSRef: row["token_endpoint_kms_ref"],
			ClientIDKMSRef:      row["client_id_kms_ref"],
			ClientSecretKMSRef:  row["client_secret_kms_ref"],
			APIKeyKMSRef:        row["api_key_kms_ref"],
			CertKMSRef:          row["cert_kms_ref"],
			Scopes:              row["scopes"],
		},
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// splitCSVField splits a semicolon-separated cell value into a slice.
// Supports both semicolon (Excel-friendly) and pipe separators.
func splitCSVField(s string) []string {
	if s == "" {
		return nil
	}
	sep := ";"
	if strings.Contains(s, "|") {
		sep = "|"
	}
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func parseInt(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

// ── GetUploadTemplate ─────────────────────────────────────────────────────────
//
// GET /master/counterparty-hub/upload/template?type=EXCHANGE
// Returns a JSON list of expected column headers for the given counterparty_type.

func GetUploadTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	templates := map[string][]string{
		"BANK": {
			"counterparty_code", "counterparty_name", "short_name", "country",
			"primary_currency", "lei", "rm_email", "eff_from", "eff_to", "notes",
		},
		"EXCHANGE": {
			"counterparty_id", "mic_code", "exchange_type", "regulatory_body",
			"operating_hours_open", "operating_hours_close", "timezone", "settlement_cycle",
			"holiday_calendar_ref", "asset_classes", "connectivity_protocol", "session_role",
			"sender_comp_id", "target_comp_id", "primary_host", "primary_port",
			"failover_host", "failover_port", "fix_creds_kms_ref", "heartbeat_interval", "tls_version",
		},
		"DATA_PROVIDER": {
			"counterparty_id", "provider_code", "provider_type", "delivery_mechanism",
			"api_creds_kms_ref", "entitlement_codes", "renewal_date",
			"refresh_interval_sec", "data_types",
		},
		"CCP_CSD": {
			"counterparty_id", "entity_code", "entity_sub_type", "lei",
			"regulatory_body", "participant_id", "clearing_acct_kms_ref",
			"margin_call_frequency",
		},
		"PAYMENT_NETWORK": {
			"counterparty_id", "network_code", "network_type", "bic_code",
			"routing_number", "iban_supported", "settlement_currencies",
			"cut_off_time", "timezone", "api_endpoint_kms_ref",
		},
		"ERP_SYSTEM": {
			"counterparty_id", "erp_code", "erp_type", "version", "base_url",
			"timezone", "default_currency", "auth_type", "token_endpoint_kms_ref",
			"client_id_kms_ref", "client_secret_kms_ref", "api_key_kms_ref",
			"cert_kms_ref", "scopes",
		},
	}

	return func(w http.ResponseWriter, r *http.Request) {
		cpType := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("type")))
		cols, ok := templates[cpType]
		if !ok {
			api.RespondWithError(w, http.StatusBadRequest, "type must be one of BANK, EXCHANGE, DATA_PROVIDER, CCP_CSD, PAYMENT_NETWORK, ERP_SYSTEM")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"counterparty_type": cpType,
			"columns":           cols,
		})
	}
}
