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
	if val, ok := fields["entity_id"]; ok {
		entityID := strings.TrimSpace(fmt.Sprint(val))
		if entityID != "" {
			if allowedEntities, ok := ctx.Value("entity_ids").([]string); ok {
				found := false
				for _, allowed := range allowedEntities {
					if allowed == entityID {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Entity ID '%s' is not within your authorized access scope.", entityID)
				}
			}
		}
	}

	// 2. Validate Bank
	if val, ok := fields["bank_id"]; ok {
		bankID := strings.TrimSpace(fmt.Sprint(val))
		if bankID != "" {
			if banks, ok := ctx.Value("BankInfo").([]map[string]string); ok {
				found := false
				for _, b := range banks {
					if b["bank_id"] == bankID {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Bank ID '%s' is not valid or not approved.", bankID)
				}
			}
		}
	}

	// 3. Validate Bank Account (Source Account)
	if val, ok := fields["bank_account_id"]; ok {
		accountID := strings.TrimSpace(fmt.Sprint(val))
		if accountID != "" {
			if accounts, ok := ctx.Value("ApprovedBankAccounts").([]map[string]string); ok {
				found := false
				for _, a := range accounts {
					if a["account_id"] == accountID {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Bank Account ID '%s' is not valid or not approved.", accountID)
				}
			}
		}
	}

	// 4. Validate Interest Type
	if val, ok := fields["interest_type"]; ok {
		interestType := strings.TrimSpace(fmt.Sprint(val))
		if interestType != "" {
			if types, ok := ctx.Value("ApprovedInterestTypes").([]map[string]string); ok {
				found := false
				for _, t := range types {
					if t["interest_type_code"] == interestType || t["interest_type_name"] == interestType {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Interest Type '%s' is not valid or not approved.", interestType)
				}
			}
		}
	}

	// 5. Validate Currency
	if val, ok := fields["currency"]; ok {
		currency := strings.TrimSpace(fmt.Sprint(val))
		if currency != "" {
			if currencies, ok := ctx.Value("ActiveCurrencies").([]map[string]string); ok {
				found := false
				for _, c := range currencies {
					if c["currency_code"] == currency || c["currency_id"] == currency {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Currency '%s' is not valid or active.", currency)
				}
			}
		}
	}

	// 6. Validate Day Count Convention
	if val, ok := fields["day_count_code"]; ok {
		dayCount := strings.TrimSpace(fmt.Sprint(val))
		if dayCount != "" {
			if dayCounts, ok := ctx.Value("ApprovedDayCounts").([]map[string]string); ok {
				found := false
				for _, d := range dayCounts {
					if d["day_count_code"] == dayCount {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Day Count Convention '%s' is not valid or not approved.", dayCount)
				}
			}
		}
	}

	// 7. Validate TDS Plan
	if val, ok := fields["tds_plan_id"]; ok {
		tdsPlan := strings.TrimSpace(fmt.Sprint(val))
		if tdsPlan != "" {
			if tdsPlans, ok := ctx.Value("ApprovedTDSPlans").([]map[string]string); ok {
				found := false
				for _, t := range tdsPlans {
					if t["tds_plan_id"] == tdsPlan {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("TDS Plan ID '%s' is not valid or not approved.", tdsPlan)
				}
			}
		}
	}

	// 8. Validate Compounding Frequency (interest_payout_frequency / frequency_id)
	if val, ok := fields["frequency_id"]; ok {
		freq := strings.TrimSpace(fmt.Sprint(val))
		if freq != "" {
			if freqs, ok := ctx.Value("ApprovedCompoundingFrequencies").([]map[string]string); ok {
				found := false
				for _, f := range freqs {
					if f["frequency_id"] == freq || f["frequency_code"] == freq {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Frequency '%s' is not valid or not approved.", freq)
				}
			}
		}
	}

	return ""
}
