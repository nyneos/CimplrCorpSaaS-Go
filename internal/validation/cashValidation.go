package validation

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"reflect"
	"strings"
)

// ValidateCashMasterReferences cross-references incoming request fields against the active master data loaded into the context by CashMiddleware.
// It returns an error message string if any validation fails, or an empty string if everything is valid.
func ValidateCashMasterReferences(ctx context.Context, fields map[string]interface{}) string {
	if errMsg := validateEntityRefs(ctx, fields); errMsg != "" {
		return errMsg
	}
	if errMsg := validateRows(ctx, fields, "BankInfo", []string{"bank_id", "bank_ids", "bank_name", "bank_names", "bank_code", "bank_codes", "bank_short_name"}, []string{"bank_id", "bank_name", "bank_code", "bank_short_name"}, "Bank"); errMsg != "" {
		return errMsg
	}
	if errMsg := validateRowsWithFallback(ctx, fields, "ApprovedBankAccounts", api.ApprovedBankAccountsKey, []string{"bank_account_id", "bank_account_ids", "account_id", "account_ids", "bank_account_number", "bank_account_numbers", "account_number", "account_numbers", "source_account_number", "from_account_number", "to_account_number"}, []string{"account_id", "bank_account_id", "account_number"}, "Bank Account"); errMsg != "" {
		return errMsg
	}
	if errMsg := validateRows(ctx, fields, "ActiveCurrencies", []string{"currency", "currencies", "currency_code", "currency_codes", "currency_id", "currency_ids"}, []string{"currency_id", "currency_code", "currency_name"}, "Currency"); errMsg != "" {
		return errMsg
	}
	if errMsg := validateRows(ctx, fields, "CashFlowCategories", []string{"category_id", "category_ids", "category_name", "category_names", "cashflow_category_id"}, []string{"category_id", "category_name"}, "Category"); errMsg != "" {
		return errMsg
	}
	if errMsg := validateRows(ctx, fields, "ApprovedCounterparties", []string{"counterparty_id", "counterparty_ids", "counterparty_name", "counterparty_names"}, []string{"counterparty_id", "counterparty_name"}, "Counterparty"); errMsg != "" {
		return errMsg
	}
	if errMsg := validateRows(ctx, fields, "ApprovedCostProfitCenters", []string{"centre_id", "centre_ids", "center_id", "center_ids", "cost_profit_center_id", "cost_profit_center_ids", "centre_name", "center_name"}, []string{"centre_id", "center_id", "centre_name", "center_name"}, "Cost/Profit Center"); errMsg != "" {
		return errMsg
	}
	if errMsg := validateRows(ctx, fields, "ApprovedGLAccounts", []string{"gl_account_id", "gl_account_ids", "gl_account_code", "gl_account_codes", "gl_account_name"}, []string{"gl_account_id", "gl_account_code", "gl_account_name"}, "GL Account"); errMsg != "" {
		return errMsg
	}

	return ""
}

func validateEntityRefs(ctx context.Context, fields map[string]interface{}) string {
	for _, entityID := range fieldValues(fields, []string{"entity_id", "entity_ids"}) {
		if allowedEntities, ok := ctx.Value("entity_ids").([]string); ok && len(allowedEntities) > 0 && !containsFold(allowedEntities, entityID) {
			return fmt.Sprintf(constants.ErrEntityIDNotAuthorized, entityID)
		}
	}
	for _, entityName := range fieldValues(fields, []string{"entity_name", "entity_names"}) {
		if allowedEntities, ok := ctx.Value("entity_names").([]string); ok && len(allowedEntities) > 0 && !containsFold(allowedEntities, entityName) {
			return fmt.Sprintf("Entity '%s' is not within your authorized access scope.", entityName)
		}
	}
	return ""
}

func validateRows(ctx context.Context, fields map[string]interface{}, ctxKey string, fieldNames, rowKeys []string, label string) string {
	rows, ok := ctx.Value(ctxKey).([]map[string]string)
	if !ok || len(rows) == 0 {
		return ""
	}
	for _, value := range fieldValues(fields, fieldNames) {
		if !rowContains(rows, rowKeys, value) {
			return fmt.Sprintf("%s '%s' is not valid or approved.", label, value)
		}
	}
	return ""
}

func validateRowsWithFallback(ctx context.Context, fields map[string]interface{}, stringCtxKey string, typedCtxKey interface{}, fieldNames, rowKeys []string, label string) string {
	rows, ok := ctx.Value(typedCtxKey).([]map[string]string)
	if !ok || len(rows) == 0 {
		rows, ok = ctx.Value(stringCtxKey).([]map[string]string)
	}
	if !ok || len(rows) == 0 {
		return ""
	}
	for _, value := range fieldValues(fields, fieldNames) {
		if !rowContains(rows, rowKeys, value) {
			return fmt.Sprintf("%s '%s' is not valid or approved.", label, value)
		}
	}
	return ""
}

func fieldValues(fields map[string]interface{}, names []string) []string {
	out := []string{}
	for _, name := range names {
		val, ok := fields[name]
		if !ok || val == nil {
			continue
		}
		out = append(out, flattenValue(val)...)
	}
	return out
}

func flattenValue(val interface{}) []string {
	if val == nil {
		return nil
	}
	if s, ok := val.(string); ok {
		return splitStrings(s)
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		out := []string{}
		for i := 0; i < rv.Len(); i++ {
			out = append(out, flattenValue(rv.Index(i).Interface())...)
		}
		return out
	}
	return splitStrings(fmt.Sprint(val))
}

func splitStrings(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func rowContains(rows []map[string]string, keys []string, value string) bool {
	for _, row := range rows {
		for _, key := range keys {
			if strings.EqualFold(strings.TrimSpace(row[key]), strings.TrimSpace(value)) {
				return true
			}
		}
	}
	return false
}

func containsFold(values []string, value string) bool {
	for _, candidate := range values {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(value)) {
			return true
		}
	}
	return false
}
