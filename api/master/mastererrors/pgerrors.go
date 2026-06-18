// Package mastererrors maps PostgreSQL constraint/index violations to user-facing messages.
package mastererrors

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgconn"
)

// uniqueMessages maps pg constraint / unique-index names to a specific user message.
// Index names come from pg_indexes on RDS + Supabase (partial unique WHERE is_deleted = false).
var uniqueMessages = map[string]string{
	// AMC
	"unique_amc_name_not_deleted":            constants.ErrAMCNameAlreadyExists,
	"unique_internal_amc_code_not_deleted":   constants.ErrInternalAMCCodeAlreadyExists,
	"unique_amc_code_not_deleted":            constants.ErrInternalAMCCodeAlreadyExists, // legacy alias
	"masteramc_amc_name_key":                 constants.ErrAMCNameAlreadyExists,
	"masteramc_internal_amc_code_key":        constants.ErrInternalAMCCodeAlreadyExists,
	"unique_sebi_registration_not_deleted":   constants.ErrSEBIRegistrationAlreadyExists,
	"masteramc_sebi_registration_number_key": constants.ErrSEBIRegistrationAlreadyExists,

	// Scheme
	"unique_scheme_isin_not_deleted":          "Scheme with this ISIN already exists. Please use a different ISIN.",
	"unique_scheme_name_not_deleted":          "Scheme name already exists. Please use a different name.",
	"unique_internal_scheme_code_not_deleted": "Internal scheme code already exists. Please use a different code.",
	"unique_amfi_scheme_code_not_deleted":     "AMFI scheme code already exists. Please use a different code.",

	// DP / Demat / Folio
	"unique_dp_code_not_deleted":         "DP code already exists. Please use a different code.",
	"unique_entity_demat_not_deleted":    "Demat account already exists for this entity.",
	"unique_active_folio_per_entity_amc": "Folio number already exists for this entity and AMC combination. Please use a different folio number.",

	// Cash masters
	"unique_bank_name_not_deleted":                       "Bank name already exists. Please use a different name.",
	"unique_counterparty_name_not_deleted":               "Counterparty name already exists. Please use a different name.",
	"unique_counterparty_code_not_deleted":               "Counterparty code already exists. Please use a different code.",
	"unique_centre_name_not_deleted":                     "Cost/profit centre name already exists. Please use a different name.",
	"unique_centre_code_not_deleted":                     "Cost/profit centre code already exists. Please use a different code.",
	"unique_category_name_not_deleted":                   "Cash flow category name already exists. Please use a different name.",
	"unique_entity_name_not_deleted":                     constants.ErrEntityNameAlreadyExists,
	"idx_masterentitycash_name_not_deleted":              constants.ErrEntityNameAlreadyExists,
	"idx_masterentitycash_unique_identifier_not_deleted": "Unique identifier already exists. Please use a different value.",
	"unique_gl_account_code_not_deleted":                 "GL account code already exists. Please use a different code.",
	"unique_gl_account_name_not_deleted":                 "GL account name already exists. Please use a different name.",
	"unique_type_code_not_deleted":                       "Type code already exists. Please use a different code.",
	"unique_type_name_not_deleted":                       "Type name already exists. Please use a different name.",
	"mastercurrency_currency_code_key":                   "Currency code already exists. Please use a different code.",

	// Investment config masters (common patterns)
	"uniq_bank_config_active":        "An active bank configuration with the same keys already exists.",
	"uniq_fd_rate_card_active":       "An active FD rate card with the same keys already exists.",
	"uniq_penalty_structure_active":  "An active penalty structure with the same keys already exists.",
	"uniq_frequency_code_active":     "Compounding frequency code already exists. Please use a different code.",
	"uniq_frequency_name_active":     "Compounding frequency name already exists. Please use a different name.",
	"uniq_day_count_id_active":       "Day count convention ID already exists.",
	"uniq_day_count_name_active":     "Day count convention name already exists. Please use a different name.",
	"uniq_interest_type_code_active": "Interest type code already exists. Please use a different code.",
	"uniq_interest_type_name_active": "Interest type name already exists. Please use a different name.",
	"uniq_tds_code_active":           "TDS plan code already exists. Please use a different code.",
	"uniq_tds_name_active":           "TDS plan name already exists. Please use a different name.",

	// Calendar / holiday
	"unique_calendar_code_not_deleted":        "Calendar code already exists. Please use a different code.",
	"unique_calendar_name_not_deleted":        "Calendar name already exists. Please use a different name.",
	"ux_calendar_code_active":                 "Calendar code already exists. Please use a different code.",
	"unique_holiday_per_calendar_not_deleted": "This holiday already exists for the selected calendar.",
	"ux_holiday_unique_active":                "This holiday already exists for the selected calendar.",

	// Bank account
	"unique_account_number_not_deleted":                      "Account number already exists. Please use a different account number.",
	"unique_bank_account_not_deleted":                        "This bank account combination already exists.",
	"uk_masterbankaccount_bank_account_not_deleted":          "This bank account combination already exists.",
	"masterclearingcode_account_id_code_type_key":            "Clearing code for this account and type already exists.",
	"unique_masterclearingcode_account_codetype_not_deleted": "Clearing code for this account and type already exists.",

	// Counterparty hub
	"uniq_cp_code":         "Counterparty code already exists for this tenant.",
	"uniq_bank_code":       "Bank code already exists.",
	"uniq_exchange_code":   "Exchange code already exists.",
	"uniq_mic_code":        "MIC code already exists. MIC codes are globally unique (ISO 10383).",
	"uniq_provider_code":   "Provider code already exists.",
	"uniq_ccp_entity_code": "Entity code already exists for this CCP/CSD.",
	"uniq_ccp_lei":         "LEI already registered to another CCP/CSD record.",
	"uniq_network_code":    "Network code already exists.",
	"uniq_erp_system_code": "ERP system code already exists.",
}

// columnMessages fallback when constraint name is unknown but Detail names the column.
var columnMessages = map[string]string{
	"internal_amc_code":    constants.ErrInternalAMCCodeAlreadyExists,
	"amc_name":             constants.ErrAMCNameAlreadyExists,
	"sebi_registration_no": constants.ErrSEBIRegistrationAlreadyExists,
	"scheme_name":          "Scheme name already exists. Please use a different name.",
	"internal_scheme_code": "Internal scheme code already exists. Please use a different code.",
	"isin":                 "Scheme with this ISIN already exists. Please use a different ISIN.",
	"amfi_scheme_code":     "AMFI scheme code already exists. Please use a different code.",
	"dp_code":              "DP code already exists. Please use a different code.",
	"folio_number":         "Folio number already exists. Please use a different folio number.",
	"bank_name":            "Bank name already exists. Please use a different name.",
	"counterparty_name":    "Counterparty name already exists. Please use a different name.",
	"counterparty_code":    "Counterparty code already exists. Please use a different code.",
	"centre_name":          "Cost/profit centre name already exists. Please use a different name.",
	"centre_code":          "Cost/profit centre code already exists. Please use a different code.",
	"category_name":        "Cash flow category name already exists. Please use a different name.",
	"entity_name":          constants.ErrEntityNameAlreadyExists,
	"unique_identifier":    "Unique identifier already exists. Please use a different value.",
	"gl_account_code":      "GL account code already exists. Please use a different code.",
	"gl_account_name":      "GL account name already exists. Please use a different name.",
	"type_code":            "Type code already exists. Please use a different code.",
	"type_name":            "Type name already exists. Please use a different name.",
	"currency_code":        "Currency code already exists. Please use a different code.",
	"interest_type_code":   "Interest type code already exists. Please use a different code.",
	"interest_type_name":   "Interest type name already exists. Please use a different name.",
	"frequency_code":       "Compounding frequency code already exists. Please use a different code.",
	"frequency_name":       "Compounding frequency name already exists. Please use a different name.",
	"day_count_code":       "Day count convention code already exists. Please use a different code.",
	"day_count_name":       "Day count convention name already exists. Please use a different name.",
	"tds_plan_code":        "TDS plan code already exists. Please use a different code.",
	"tds_plan_name":        "TDS plan name already exists. Please use a different name.",
	"demat_account_number": "Demat account number already exists for this entity.",
}

var duplicateColumnRe = regexp.MustCompile(`(?i)Key \(([^)]+)\)=\(`)

// TryUniqueViolation returns a specific message when err is a PostgreSQL unique violation (23505).
func TryUniqueViolation(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		api.LogError("master unique violation constraint=%s table=%s detail=%s",
			pgErr.ConstraintName, pgErr.TableName, pgErr.Detail)

		if msg, ok := uniqueMessages[pgErr.ConstraintName]; ok {
			return msg, true
		}
		cn := strings.ToLower(pgErr.ConstraintName)
		for key, msg := range uniqueMessages {
			if strings.Contains(cn, strings.ToLower(key)) {
				return msg, true
			}
		}
		if col := extractDuplicateColumn(pgErr.Detail); col != "" {
			if msg, ok := columnMessages[col]; ok {
				return msg, true
			}
			return fmt.Sprintf("%s already exists. Please use a different value.", humanizeColumn(col)), true
		}
		return "Duplicate entry — this value already exists.", true
	}

	// Fallback: string match on wrapped errors (older drivers / wrapped pgx errors)
	errStr := err.Error()
	for key, msg := range uniqueMessages {
		if strings.Contains(errStr, key) {
			return msg, true
		}
	}
	if m := duplicateColumnRe.FindStringSubmatch(errStr); len(m) > 1 {
		col := strings.TrimSpace(m[1])
		if msg, ok := columnMessages[col]; ok {
			return msg, true
		}
	}

	return "", false
}

func extractDuplicateColumn(detail string) string {
	m := duplicateColumnRe.FindStringSubmatch(detail)
	if len(m) < 2 {
		return ""
	}
	// composite keys: "entity_name, amc_name, folio_number" — use first meaningful column
	parts := strings.Split(m[1], ",")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func humanizeColumn(col string) string {
	col = strings.ReplaceAll(col, "_", " ")
	return strings.TrimSpace(col)
}
