package validation

import (
	"context"
	"fmt"
	"strings"
)

// ValidateCashMasterReferences cross-references incoming request fields against the active master data loaded into the context by CashMiddleware.
// It returns an error message string if any validation fails, or an empty string if everything is valid.
func ValidateCashMasterReferences(ctx context.Context, fields map[string]interface{}) string {
	// 1. Validate Entity Name
	if val, ok := fields["entity_name"]; ok {
		entityName := strings.TrimSpace(fmt.Sprint(val))
		if entityName != "" {
			if allowedEntities, ok := ctx.Value("entity_names").([]string); ok {
				found := false
				for _, allowed := range allowedEntities {
					if strings.EqualFold(allowed, entityName) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Entity '%s' is not within your authorized access scope.", entityName)
				}
			}
		}
	}

	// 2. Validate Bank
	if val, ok := fields["bank_name"]; ok {
		bankName := strings.TrimSpace(fmt.Sprint(val))
		if bankName != "" {
			if banks, ok := ctx.Value("BankInfo").([]map[string]string); ok {
				found := false
				for _, b := range banks {
					if strings.EqualFold(b["bank_name"], bankName) || strings.EqualFold(b["bank_id"], bankName) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Bank '%s' is not valid or not approved.", bankName)
				}
			}
		}
	}

	// 3. Validate Bank Account
	if val, ok := fields["bank_account_number"]; ok {
		accountNum := strings.TrimSpace(fmt.Sprint(val))
		if accountNum != "" {
			if accounts, ok := ctx.Value("ApprovedBankAccounts").([]map[string]string); ok {
				found := false
				for _, a := range accounts {
					if strings.EqualFold(a["account_number"], accountNum) || strings.EqualFold(a["account_id"], accountNum) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Bank Account '%s' is not valid or not approved.", accountNum)
				}
			}
		}
	}

	// 4. Validate Currency
	if val, ok := fields["currency"]; ok {
		currency := strings.TrimSpace(fmt.Sprint(val))
		if currency != "" {
			if currencies, ok := ctx.Value("ActiveCurrencies").([]map[string]string); ok {
				found := false
				for _, c := range currencies {
					if strings.EqualFold(c["currency_code"], currency) || strings.EqualFold(c["currency_id"], currency) {
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

	// 5. Validate Category
	if val, ok := fields["category_id"]; ok {
		category := strings.TrimSpace(fmt.Sprint(val))
		if category != "" {
			if categories, ok := ctx.Value("CashFlowCategories").([]map[string]string); ok {
				found := false
				for _, c := range categories {
					if strings.EqualFold(c["category_id"], category) || strings.EqualFold(c["category_name"], category) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Category '%s' is not valid or approved.", category)
				}
			}
		}
	}

	// 6. Validate Counterparty
	if val, ok := fields["counterparty_id"]; ok {
		counterparty := strings.TrimSpace(fmt.Sprint(val))
		if counterparty != "" {
			if counterparties, ok := ctx.Value("ApprovedCounterparties").([]map[string]string); ok {
				found := false
				for _, c := range counterparties {
					if strings.EqualFold(c["counterparty_id"], counterparty) || strings.EqualFold(c["counterparty_name"], counterparty) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Counterparty '%s' is not valid or approved.", counterparty)
				}
			}
		}
	}
	
	// 7. Validate Profit Center
	if val, ok := fields["centre_id"]; ok {
		center := strings.TrimSpace(fmt.Sprint(val))
		if center != "" {
			if centers, ok := ctx.Value("ApprovedCostProfitCenters").([]map[string]string); ok {
				found := false
				for _, c := range centers {
					if strings.EqualFold(c["centre_id"], center) || strings.EqualFold(c["centre_name"], center) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Cost/Profit Center '%s' is not valid or approved.", center)
				}
			}
		}
	}

	return ""
}
