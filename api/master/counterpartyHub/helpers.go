package counterpartyHub

import (
	"context"
	"math/big"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// getUserFriendlyCounterpartyError maps DB/pgconn errors to user-facing messages.
// It unwraps *pgconn.PgError first for precise constraint/type details.
func getUserFriendlyCounterpartyError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		api.LogError("PgError [%s] constraint=%s table=%s detail=%s", pgErr.Code, pgErr.ConstraintName, pgErr.TableName, pgErr.Detail)
		switch pgErr.Code {
		case "23505": // unique_violation
			switch {
			case strings.Contains(pgErr.ConstraintName, "uniq_cp_code"):
				return "Counterparty code already exists for this tenant.", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "uniq_bank_code"):
				return "Bank code already exists.", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "uniq_exchange_code"):
				return "Exchange code already exists.", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "uniq_mic_code"):
				return "MIC code already exists. MIC codes are globally unique (ISO 10383).", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "uniq_provider_code"):
				return "Provider code already exists.", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "uniq_ccp_entity_code"):
				return "Entity code already exists for this CCP/CSD.", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "uniq_ccp_lei"):
				return "LEI already registered to another CCP/CSD record.", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "uniq_network_code"):
				return "Network code already exists.", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "uniq_erp_system_code"):
				return "ERP system code already exists.", http.StatusOK
			case strings.Contains(pgErr.ConstraintName, "_cp_unique"):
				return "A " + pgErr.TableName + " record already exists for this counterparty.", http.StatusOK
			}
			return "Duplicate entry — this record already exists.", http.StatusOK
		case "23503": // foreign_key_violation
			return "Referenced record not found or is not active.", http.StatusOK
		case "23514": // check_violation
			return "Invalid value for field: " + pgErr.ConstraintName, http.StatusBadRequest
		case "22P02": // invalid_text_representation (enum cast failure)
			return "Invalid option selected for one of the dropdown fields.", http.StatusBadRequest
		case "42804": // datatype_mismatch
			return fmt.Sprintf("Field type mismatch: %s", pgErr.Message), http.StatusBadRequest
		}
		return fmt.Sprintf("Database error [%s]: %s", pgErr.Code, pgErr.Message), http.StatusInternalServerError
	}

	errStr := strings.ToLower(err.Error())

	if strings.Contains(errStr, "tx begin") || strings.Contains(errStr, "commit failed") || strings.Contains(errStr, "audit insert") {
		return err.Error(), http.StatusOK
	}
	if strings.Contains(errStr, "duplicate key") || strings.Contains(errStr, "unique constraint") {
		return "Duplicate entry detected. This record already exists in the system.", http.StatusOK
	}
	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or is not active.", http.StatusOK
	}
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue. Please retry.", http.StatusServiceUnavailable
	}

	return "Internal server error: " + context, http.StatusInternalServerError
}

// ── KMS path validation ───────────────────────────────────────────────────────

var kmsPathRegex = regexp.MustCompile(`^vault/cimplr/[^/\s]+/[^/\s]+/[^/\s]+$`)

// validateKMSPath rejects raw credentials. Path must be vault/cimplr/{tenant}/{cp}/{type}.
func validateKMSPath(field, value string) error {
	if value == "" {
		return nil
	}
	if !kmsPathRegex.MatchString(value) {
		return fmt.Errorf("%s must be a KMS vault path (vault/cimplr/{tenant}/{counterparty}/{type}). Raw credentials are not accepted.", field)
	}
	return nil
}

// ── Counterparty code validation ──────────────────────────────────────────────

var cpCodeRegex = regexp.MustCompile(`^[A-Z0-9_-]{2,20}$`)

// validateCounterpartyCode enforces 2-20 uppercase alphanumeric + _ and - chars.
func validateCounterpartyCode(code string) error {
	if !cpCodeRegex.MatchString(code) {
		return errors.New("Counterparty code must be 2-20 uppercase alphanumeric characters (A-Z, 0-9, _, -)")
	}
	return nil
}

// ── MIC code validation ───────────────────────────────────────────────────────

var micCodeRegex = regexp.MustCompile(`^[A-Z]{4}$`)

// validateMICCode enforces exactly 4 uppercase letters per ISO 10383.
func validateMICCode(code string) error {
	if !micCodeRegex.MatchString(code) {
		return errors.New("MIC code must be exactly 4 uppercase letters (ISO 10383)")
	}
	return nil
}

// ── SWIFT / BIC rejection ─────────────────────────────────────────────────────

var swiftBicRegex = regexp.MustCompile(`^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$`)

// validateNoBIC rejects values that look like SWIFT/BIC codes.
// BIC codes are account-level identifiers and must not appear on Bank Master.
func validateNoBIC(value string) error {
	if swiftBicRegex.MatchString(strings.ToUpper(strings.TrimSpace(value))) {
		return errors.New("BIC codes are account-level. Add BIC on the bank account record.")
	}
	return nil
}

// ── LEI validation ────────────────────────────────────────────────────────────

var leiCharRegex = regexp.MustCompile(`^[A-Z0-9]{20}$`)

// validateLEI validates length, character set, and ISO 17442 Luhn mod 97-10 checksum.
func validateLEI(lei string) error {
	if lei == "" {
		return nil
	}
	if len(lei) != 20 {
		return errors.New("LEI must be exactly 20 characters")
	}
	if !leiCharRegex.MatchString(lei) {
		return errors.New("LEI must contain only uppercase letters and digits")
	}
	// ISO 17442: move last 4 chars to front, convert A-Z to 10-35, mod 97 must be 1.
	rearranged := lei[18:] + lei[:18]
	numStr := ""
	for _, c := range rearranged {
		if c >= 'A' && c <= 'Z' {
			numStr += fmt.Sprintf("%d", int(c-'A')+10)
		} else {
			numStr += string(c)
		}
	}
	n := new(big.Int)
	n.SetString(numStr, 10)
	mod := new(big.Int).Mod(n, big.NewInt(97))
	if mod.Int64() != 1 {
		return errors.New("LEI checksum is invalid. Please verify the LEI with GLEIF.")
	}
	return nil
}

// ── Type routing helpers (Hub 2.0) ────────────────────────────────────────────

// hub2Schema is the PostgreSQL schema that owns all hub-v2 tables.
const hub2Schema = "apibox_svc."

func typedMasterTable(cpType string) string {
	switch cpType {
	case "BANK":
		return hub2Schema + "bank_master"
	case "EXCHANGE":
		return hub2Schema + "exchange_master"
	case "DATA_PROVIDER":
		return hub2Schema + "data_provider_master"
	case "CCP_CSD":
		return hub2Schema + "ccp_csd_master"
	case "PAYMENT_NETWORK":
		return hub2Schema + "payment_network_master"
	case "ERP_SYSTEM":
		return hub2Schema + "erp_system_master"
	default:
		return ""
	}
}

func typedAuditTable(cpType string) string {
	switch cpType {
	case "BANK":
		return hub2Schema + "audit_bank_master"
	case "EXCHANGE":
		return hub2Schema + "audit_exchange_master"
	case "DATA_PROVIDER":
		return hub2Schema + "audit_data_provider_master"
	case "CCP_CSD":
		return hub2Schema + "audit_ccp_csd_master"
	case "PAYMENT_NETWORK":
		return hub2Schema + "audit_payment_network_master"
	case "ERP_SYSTEM":
		return hub2Schema + "audit_erp_system_master"
	default:
		return ""
	}
}

func typedIDColumn(cpType string) string {
	switch cpType {
	case "BANK":
		return "bank_id"
	case "EXCHANGE":
		return "exchange_id"
	case "DATA_PROVIDER":
		return "provider_id"
	case "CCP_CSD":
		return "ccp_csd_id"
	case "PAYMENT_NETWORK":
		return "network_id"
	case "ERP_SYSTEM":
		return "erp_id"
	default:
		return ""
	}
}

func hasMultiSelectMapping(cpType string) bool {
	return cpType == "EXCHANGE" || cpType == "DATA_PROVIDER"
}

func mappingTable(cpType string) (table, fkCol, valueCol string) {
	switch cpType {
	case "EXCHANGE":
		return hub2Schema + "exchange_asset_classes", "exchange_id", "asset_class"
	case "DATA_PROVIDER":
		return hub2Schema + "dp_data_types", "provider_id", "data_type"
	}
	return "", "", ""
}

// ── Small coercion helpers (Hub 2.0) ──────────────────────────────────────────

func nilIfEmpty(v interface{}) interface{} {
	switch x := v.(type) {
	case string:
		if strings.TrimSpace(x) == "" {
			return nil
		}
		return x
	default:
		return v
	}
}

func nilIfZero(v interface{}) interface{} {
	switch x := v.(type) {
	case int:
		if x == 0 {
			return nil
		}
		return x
	case int32:
		if x == 0 {
			return nil
		}
		return x
	case int64:
		if x == 0 {
			return nil
		}
		return x
	case float64:
		if x == 0 {
			return nil
		}
		return x
	default:
		return v
	}
}

func intOrDefault(v interface{}, def int) int {
	switch x := v.(type) {
	case int:
		if x == 0 {
			return def
		}
		return x
	case float64:
		if int(x) == 0 {
			return def
		}
		return int(x)
	case string:
		if strings.TrimSpace(x) == "" {
			return def
		}
		var out int
		_, _ = fmt.Sscanf(x, "%d", &out)
		if out == 0 {
			return def
		}
		return out
	default:
		return def
	}
}

func toStringSlice(v interface{}) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []interface{}:
		out := make([]string, 0, len(x))
		for _, it := range x {
			if it == nil {
				continue
			}
			out = append(out, fmt.Sprintf("%v", it))
		}
		return out
	case string:
		if strings.TrimSpace(x) == "" {
			return nil
		}
		return []string{x}
	default:
		return nil
	}
}

// insertTypedDetail inserts one row into the correct typed master table.
// tx must already be open. Returns the generated typed ID.
// fields is the decoded request body as map[string]interface{}.
func insertTypedDetail(ctx context.Context, tx pgx.Tx, cpID, cpType string, fields map[string]interface{}) (string, error) {
	switch cpType {
	case "BANK":
		return insertBankDetail(ctx, tx, cpID, fields)
	case "EXCHANGE":
		return insertExchangeDetail(ctx, tx, cpID, fields)
	case "DATA_PROVIDER":
		return insertDataProviderDetail(ctx, tx, cpID, fields)
	case "CCP_CSD":
		return insertCcpCsdDetail(ctx, tx, cpID, fields)
	case "PAYMENT_NETWORK":
		return insertPaymentNetworkDetail(ctx, tx, cpID, fields)
	case "ERP_SYSTEM":
		return insertErpDetail(ctx, tx, cpID, fields)
	default:
		return "", fmt.Errorf("unsupported counterparty_type: %s", cpType)
	}
}

