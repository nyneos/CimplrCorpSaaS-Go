package counterpartyHub

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"CimplrCorpSaas/api"

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
			if strings.Contains(pgErr.ConstraintName, "uniq_cp_code_active") ||
				strings.Contains(pgErr.ConstraintName, "counterparty_code") {
				return "Counterparty code already exists. Each counterparty code must be unique.", http.StatusOK
			}
			return "Duplicate entry detected. This record already exists in the system.", http.StatusOK
		case "23503": // foreign_key_violation
			return "Referenced record not found or is invalid.", http.StatusOK
		case "23514": // check_violation
			return fmt.Sprintf("Invalid value: check constraint '%s' failed", pgErr.ConstraintName), http.StatusBadRequest
		case "22P02": // invalid_text_representation (enum cast failure)
			return "Invalid enum value provided for one of the fields", http.StatusBadRequest
		case "42804": // datatype_mismatch
			return fmt.Sprintf("Field type mismatch: %s", pgErr.Message), http.StatusBadRequest
		}
		return fmt.Sprintf("Database error [%s]: %s", pgErr.Code, pgErr.Message), http.StatusInternalServerError
	}

	errStr := strings.ToLower(err.Error())

	if strings.Contains(errStr, "duplicate key") || strings.Contains(errStr, "unique constraint") {
		return "Duplicate entry detected. This record already exists in the system.", http.StatusOK
	}
	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or is invalid.", http.StatusOK
	}
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue. Please try again later.", http.StatusServiceUnavailable
	}

	return "Internal server error: " + context, http.StatusInternalServerError
}

// ── KMS path validation ───────────────────────────────────────────────────────

var kmsPathRegex = regexp.MustCompile(`^vault/cimplr/[^/\s]+/[^/\s]+/[^/\s]+$`)

// validateKMSPath rejects raw credentials. Path must be vault/cimplr/{tenant}/{cp}/{type}.
func validateKMSPath(path string) error {
	if path == "" {
		return nil
	}
	if !kmsPathRegex.MatchString(path) {
		return errors.New("KMS path must follow format vault/cimplr/{tenant}/{counterparty}/{type}. Raw credentials are not accepted.")
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
	// ISO 17442: rearrange last 4 chars to front, convert letters A=10..Z=35, then mod 97 must = 1
	rearranged := lei[4:] + lei[:4]
	numStr := ""
	for _, c := range rearranged {
		if c >= 'A' && c <= 'Z' {
			numStr += fmt.Sprintf("%d", int(c-'A')+10)
		} else {
			numStr += string(c)
		}
	}
	remainder := 0
	for _, ch := range numStr {
		remainder = (remainder*10 + int(ch-'0')) % 97
	}
	if remainder != 1 {
		return errors.New("LEI checksum is invalid (ISO 17442)")
	}
	return nil
}
