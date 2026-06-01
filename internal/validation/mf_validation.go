package validation

import (
	"context"
	"fmt"
	"strings"
)

// ValidateMFMasterReferences cross-references incoming request fields against the active master data loaded into the context by InvestmentMFMiddleware.
// It returns an error message string if any validation fails, or an empty string if everything is valid.
func ValidateMFMasterReferences(ctx context.Context, fields map[string]interface{}) string {
	// 1. Validate Entity Scope
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

	// 2. Validate AMC
	if val, ok := fields["amc_id"]; ok {
		amcID := strings.TrimSpace(fmt.Sprint(val))
		if amcID != "" {
			if amcs, ok := ctx.Value("ApprovedAMCs").([]map[string]string); ok {
				found := false
				for _, a := range amcs {
					if strings.EqualFold(a["amc_id"], amcID) || strings.EqualFold(a["amc_name"], amcID) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("AMC '%s' is not valid or not approved.", amcID)
				}
			}
		}
	}

	// 3. Validate Scheme
	if val, ok := fields["scheme_id"]; ok {
		schemeID := strings.TrimSpace(fmt.Sprint(val))
		if schemeID != "" {
			if schemes, ok := ctx.Value("ApprovedSchemes").([]map[string]string); ok {
				found := false
				for _, s := range schemes {
					if strings.EqualFold(s["scheme_id"], schemeID) || strings.EqualFold(s["scheme_name"], schemeID) || strings.EqualFold(s["isin"], schemeID) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Scheme '%s' is not valid or not approved.", schemeID)
				}
			}
		}
	}

	// 4. Validate Folio
	if val, ok := fields["folio_id"]; ok {
		folioID := strings.TrimSpace(fmt.Sprint(val))
		if folioID != "" {
			if folios, ok := ctx.Value("ApprovedFolios").([]map[string]string); ok {
				found := false
				for _, f := range folios {
					if strings.EqualFold(f["folio_id"], folioID) || strings.EqualFold(f["folio_number"], folioID) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Folio '%s' is not valid or not approved.", folioID)
				}
			}
		}
	}

	// 5. Validate Depository Participant (DP)
	if val, ok := fields["dp_id"]; ok {
		dpID := strings.TrimSpace(fmt.Sprint(val))
		if dpID != "" {
			if dps, ok := ctx.Value("ApprovedDPs").([]map[string]string); ok {
				found := false
				for _, d := range dps {
					if strings.EqualFold(d["dp_id"], dpID) || strings.EqualFold(d["dp_name"], dpID) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("DP '%s' is not valid or not approved.", dpID)
				}
			}
		}
	}

	// 6. Validate Demat Account
	if val, ok := fields["demat_id"]; ok {
		dematID := strings.TrimSpace(fmt.Sprint(val))
		if dematID != "" {
			if demats, ok := ctx.Value("ApprovedDemats").([]map[string]string); ok {
				found := false
				for _, d := range demats {
					if strings.EqualFold(d["demat_id"], dematID) || strings.EqualFold(d["demat_account_number"], dematID) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Sprintf("Demat Account '%s' is not valid or not approved.", dematID)
				}
			}
		}
	}

	return ""
}
