package validation

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"strings"
)

// ValidateMFMasterReferences cross-references incoming request fields against the active master data loaded into the context by InvestmentMFMiddleware.
// It returns an error message string if any validation fails, or an empty string if everything is valid.
func ValidateMFMasterReferences(ctx context.Context, fields map[string]interface{}) string {
	// 1. Validate Entity Scope (accept entity name or entity_id in entity_name field)
	if allowedEntities, ok := ctx.Value("entity_names").([]string); ok {
		allowedEntityIDs, _ := ctx.Value("entity_ids").([]string)
		for _, entityName := range mfFieldStrings(fields, "entity_name", "entity_names") {
			if stringInSlice(entityName, allowedEntities) {
				continue
			}
			if len(allowedEntityIDs) > 0 && stringInSlice(entityName, allowedEntityIDs) {
				continue
			}
			return fmt.Sprintf("Entity '%s' is not within your authorized access scope.", entityName)
		}
	}
	if allowedEntityIDs, ok := ctx.Value("entity_ids").([]string); ok {
		for _, entityID := range mfFieldStrings(fields, "entity_id", "entity_ids") {
			if !stringInSlice(entityID, allowedEntityIDs) {
				return fmt.Sprintf(constants.ErrEntityIDNotAuthorized, entityID)
			}
		}
	}

	// 2. Validate AMC
	if amcs, ok := ctx.Value("ApprovedAMCs").([]map[string]string); ok {
		if errMsg := validateMFValues(fields, amcs, []string{"amc_id", "amc_ids", "amc_name", "amc_names"}, []string{"amc_id", "amc_name", "internal_amc_code"}, "AMC"); errMsg != "" {
			return errMsg
		}
	}

	// 3. Validate Scheme
	if schemes, ok := ctx.Value("ApprovedSchemes").([]map[string]string); ok {
		if errMsg := validateMFValues(fields, schemes, []string{"scheme_id", "scheme_ids", "source_scheme_id", "target_scheme_id", "scheme_name", "scheme_names", "scheme_internal_code", "scheme_internal_codes", "isin"}, []string{"scheme_id", "scheme_name", "isin", "internal_scheme_code"}, "Scheme"); errMsg != "" {
			return errMsg
		}
	}

	// 4. Validate Folio
	if folios, ok := ctx.Value("ApprovedFolios").([]map[string]string); ok {
		if errMsg := validateMFValues(fields, folios, []string{"folio_id", "folio_ids", "folio_number", "folio_numbers"}, []string{"folio_id", "folio_number"}, "Folio"); errMsg != "" {
			return errMsg
		}
	}

	// 5. Validate Depository Participant (DP)
	if dps, ok := ctx.Value("ApprovedDPs").([]map[string]string); ok {
		if errMsg := validateMFValues(fields, dps, []string{"dp_id", "dp_ids", "dp_name", "dp_code"}, []string{"dp_id", "dp_name", "dp_code"}, "DP"); errMsg != "" {
			return errMsg
		}
	}

	// 6. Validate Demat Account
	if demats, ok := ctx.Value("ApprovedDemats").([]map[string]string); ok {
		if errMsg := validateMFValues(fields, demats, []string{"demat_id", "demat_ids", "demat_account_number", "demat_acc_number"}, []string{"demat_id", "demat_account_number"}, "Demat Account"); errMsg != "" {
			return errMsg
		}
	}

	// 7. Validate optional bank/account references used by onboarding and settlement flows.
	if banks, ok := ctx.Value("BankInfo").([]map[string]string); ok {
		if errMsg := validateMFValues(fields, banks, []string{"bank_id", "bank_name", "amc_bank_name", "bank_short_name"}, []string{"bank_id", "bank_name", "bank_short_name"}, "Bank"); errMsg != "" {
			return errMsg
		}
	}
	if accounts, ok := ctx.Value("ApprovedBankAccounts").([]map[string]string); ok {
		if errMsg := validateMFValues(fields, accounts, []string{"bank_account_id", "account_id", "account_number", "amc_bank_account_no", "default_account_number"}, []string{"account_id", "account_number"}, "Bank Account"); errMsg != "" {
			return errMsg
		}
	}

	return ""
}

func validateMFValues(fields map[string]interface{}, rows []map[string]string, fieldKeys []string, rowKeys []string, label string) string {
	for _, fieldKey := range fieldKeys {
		for _, value := range mfFieldStrings(fields, fieldKey) {
			if !rowHasAny(rows, value, rowKeys...) {
				return fmt.Sprintf("%s '%s' is not valid or not approved.", label, value)
			}
		}
	}
	return ""
}

func mfFieldStrings(fields map[string]interface{}, keys ...string) []string {
	out := make([]string, 0)
	for _, key := range keys {
		value, ok := fields[key]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case []string:
			for _, item := range typed {
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					out = append(out, trimmed)
				}
			}
		case *string:
			if typed != nil {
				if trimmed := strings.TrimSpace(*typed); trimmed != "" {
					out = append(out, trimmed)
				}
			}
		case []interface{}:
			for _, item := range typed {
				if trimmed := strings.TrimSpace(fmt.Sprint(item)); trimmed != "" {
					out = append(out, trimmed)
				}
			}
		default:
			if trimmed := strings.TrimSpace(fmt.Sprint(value)); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}
