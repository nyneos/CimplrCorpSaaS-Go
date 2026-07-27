package runtime

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// FD booking field_code → CDM path comes from domain_catalog.field.
// Synonyms map API / DB column names onto catalog field_codes that have a cdm_path.

var fdFieldSynonyms = map[string]string{
	"tenure_days":               "tenor_days",
	"tenure_months":             "tenor_months",
	"expected_maturity_date":    "maturity_date",
	"interest_type":             "interest_type_code",
	"entity_code":               "entity_code", // also mirrored from entity_id when empty
	"bank_account_id":           "source_account_id",
	"interest_payout_frequency": "frequency_id",
	"compounding_frequency":     "frequency_id",
	"day_count_convention":      "day_count_code",
	"notes":                     "booking_remarks",
}

func defaultFdCDMPaths() []cdmFieldRow {
	return []cdmFieldRow{
		{"booking_id", "investment.fd.booking_id"},
		{"entity_id", "investment.fd.entity_id"},
		{"entity_code", "investment.fd.entity_code"},
		{"entity_name", "investment.fd.entity_name"},
		{"bank_id", "investment.fd.bank_id"},
		{"bank_name", "investment.fd.bank_name"},
		{"principal_amount", "investment.fd.principal_amount"},
		{"interest_rate", "investment.fd.interest_rate"},
		{"interest_type_code", "investment.fd.interest_type_code"},
		{"interest_type_id", "investment.fd.interest_type_id"},
		{"tenor_days", "investment.fd.tenor_days"},
		{"tenor_months", "investment.fd.tenor_months"},
		{"tenure_years", "investment.fd.tenure_years"},
		{"tenor_type", "investment.fd.tenor_type"},
		{"expected_start_date", "investment.fd.expected_start_date"},
		{"value_date", "investment.fd.value_date"},
		{"maturity_date", "investment.fd.maturity_date"},
		{"value_type", "investment.fd.value_type"},
		{"frequency_id", "investment.fd.frequency_id"},
		{"payout_frequency_id", "investment.fd.payout_frequency_id"},
		{"accrual_frequency_code", "investment.fd.accrual_frequency_code"},
		{"reset_type", "investment.fd.reset_type"},
		{"day_count_code", "investment.fd.day_count_code"},
		{"tds_plan_id", "investment.fd.tds_plan_id"},
		{"bank_config_id", "investment.fd.bank_config_id"},
		{"source_account_id", "investment.fd.source_account_id"},
		{"source_account_number", "investment.fd.source_account_number"},
		{"product_code", "investment.fd.product_code"},
		{"auto_renewal", "investment.fd.auto_renewal"},
		{"booking_remarks", "investment.fd.booking_remarks"},
		{"booking_status", "investment.fd.booking_status"},
		{"offer_valid_till", "investment.fd.offer_valid_till"},
		{"requested_at", "investment.fd.requested_at"},
	}
}

// NormalizeFdBookingFields expands API/DB synonyms onto catalog field_codes.
func NormalizeFdBookingFields(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in)+8)
	for k, v := range in {
		key := strings.TrimSpace(k)
		if key == "" || v == nil {
			continue
		}
		out[key] = v
		if canon, ok := fdFieldSynonyms[key]; ok {
			if _, exists := out[canon]; !exists {
				out[canon] = v
			}
		}
	}
	if _, ok := out["entity_code"]; !ok {
		if eid, ok := out["entity_id"]; ok {
			out["entity_code"] = eid
		}
	}
	return out
}

// BuildFdBookingVariables maps an FD booking payload onto CDM evaluation keys
// using domain_catalog.field.cdm_path (SSOT). Falls back to hardcoded paths if
// the catalog is empty or unavailable (pilot safety).
func BuildFdBookingVariables(ctx context.Context, pool *pgxpool.Pool, fields map[string]interface{}) (map[string]string, error) {
	norm := NormalizeFdBookingFields(fields)
	paths, err := LoadSubModuleCDMPaths(ctx, pool, "FD_BOOKING")
	if err != nil || len(paths) == 0 {
		paths = defaultFdCDMPaths()
	}
	out := make(map[string]string, len(paths))
	for _, row := range paths {
		raw, ok := norm[row.FieldCode]
		if !ok || raw == nil {
			continue
		}
		s := stringifyCDMValue(raw)
		if s == "" {
			continue
		}
		out[row.CDMPath] = s
	}
	return out, nil
}
