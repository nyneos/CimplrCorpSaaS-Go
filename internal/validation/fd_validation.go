package validation

import (
	"context"
	"fmt"
	"strings"
)

// ValidateFDMasterReferences cross-references incoming request fields against the active master data loaded into the context by middlewares.
// It returns an error message string if any validation fails, or an empty string if everything is valid.
func ValidateFDMasterReferences(ctx context.Context, fields map[string]interface{}) string {
	// 1. Validate Entity Scope
	if entityID := fieldString(fields, "entity_id"); entityID != "" {
		if allowedEntities, ok := ctx.Value("entity_ids").([]string); ok && !stringInSlice(entityID, allowedEntities) {
			return fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", entityID)
		}
	}

	// 2. Validate Bank
	if banks, ok := ctx.Value("BankInfo").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, banks, []string{"bank_id", "bank_name", "bank_code", "bank_short_name"}, []string{"bank_id", "bank_name", "bank_code", "bank_short_name"}, "Bank"); errMsg != "" {
			return errMsg
		}
	}

	// 3. Validate Bank Account (Source Account)
	if accounts, ok := ctx.Value("ApprovedBankAccounts").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, accounts, []string{"bank_account_id", "source_account_id", "account_id", "source_account_number", "account_number"}, []string{"account_id", "source_account_id", "account_number", "source_account_number"}, "Bank Account"); errMsg != "" {
			return errMsg
		}
	}

	// 4. Validate Interest Type
	if types, ok := ctx.Value("ApprovedInterestTypes").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, types, []string{"interest_type", "interest_type_id", "interest_type_code", "confirmed_interest_type_code"}, []string{"interest_id", "interest_type_id", "interest_type_code", "interest_type_name"}, "Interest Type"); errMsg != "" {
			return errMsg
		}
	}

	// 5. Validate Currency
	if currencies, ok := ctx.Value("ActiveCurrencies").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, currencies, []string{"currency", "currency_code", "currency_id"}, []string{"currency_id", "currency_code", "currency_name"}, "Currency"); errMsg != "" {
			return errMsg
		}
	}

	// 6. Validate Day Count Convention
	if dayCounts, ok := ctx.Value("ApprovedDayCounts").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, dayCounts, []string{"day_count_code", "day_count_convention", "day_count_name"}, []string{"day_count_code", "day_count_name", "convention_type"}, "Day Count Convention"); errMsg != "" {
			return errMsg
		}
	}

	// 7. Validate TDS Plan
	if tdsPlans, ok := ctx.Value("ApprovedTDSPlans").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, tdsPlans, []string{"tds_plan_id", "tds_plan_code", "tds_plan_name"}, []string{"tds_plan_id", "tds_plan_code", "tds_plan_name"}, "TDS Plan"); errMsg != "" {
			return errMsg
		}
	}

	// 8. Validate Compounding Frequency (interest_payout_frequency / frequency_id)
	if freqs, ok := ctx.Value("ApprovedCompoundingFrequencies").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, freqs, []string{"frequency_id", "interest_payout_frequency", "payout_frequency_id", "accrual_frequency_code", "compounding_frequency", "confirmed_frequency_id"}, []string{"frequency_id", "frequency_code", "frequency_name"}, "Frequency"); errMsg != "" {
			return errMsg
		}
	}

	// 9. Validate FD Bank Config
	if configs, ok := ctx.Value("ApprovedBankConfigs").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, configs, []string{"bank_config_id", "config_id"}, []string{"config_id", "bank_config_id"}, "Bank Config"); errMsg != "" {
			return errMsg
		}
	}

	// 10. Validate Penalty Structure
	if penalties, ok := ctx.Value("ApprovedPenaltyStructures").([]map[string]string); ok {
		if errMsg := validateFieldReferences(fields, penalties, []string{"penalty_structure_id", "penalty_id"}, []string{"penalty_id", "penalty_structure_id"}, "Penalty Structure"); errMsg != "" {
			return errMsg
		}
	}

	return ""
}

func validateFieldReferences(fields map[string]interface{}, rows []map[string]string, fieldKeys []string, rowKeys []string, label string) string {
	for _, key := range fieldKeys {
		if value := fieldString(fields, key); value != "" {
			if !rowHasAny(rows, value, rowKeys...) {
				return fmt.Sprintf("%s '%s' is not valid or not approved.", label, value)
			}
		}
	}
	return ""
}

func fieldString(fields map[string]interface{}, key string) string {
	value, ok := fields[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func stringInSlice(needle string, haystack []string) bool {
	for _, item := range haystack {
		if strings.EqualFold(strings.TrimSpace(item), needle) {
			return true
		}
	}
	return false
}

func rowHasAny(rows []map[string]string, needle string, keys ...string) bool {
	for _, row := range rows {
		for _, key := range keys {
			if strings.EqualFold(strings.TrimSpace(row[key]), needle) {
				return true
			}
		}
	}
	return false
}
