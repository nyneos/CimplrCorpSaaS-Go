package runtime

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// FD booking field_code → CDM path comes from domain_catalog.field.
// Synonyms map API / DB column names onto catalog field_codes that have a cdm_path.

var fdFieldSynonyms = map[string]string{
	"tenure_days":            "tenor_days",
	"tenure_months":          "tenor_months",
	"expected_maturity_date": "maturity_date",
	"interest_type":          "interest_type_code",
	"entity_code":            "entity_code", // also mirrored from entity_id when empty
}

func defaultFdCDMPaths() []cdmFieldRow {
	return []cdmFieldRow{
		{"booking_id", "investment.fd.booking_id"},
		{"entity_id", "investment.fd.entity_id"},
		{"entity_code", "investment.fd.entity_code"},
		{"bank_id", "investment.fd.bank_id"},
		{"principal_amount", "investment.fd.principal_amount"},
		{"interest_rate", "investment.fd.interest_rate"},
		{"interest_type_code", "investment.fd.interest_type_code"},
		{"tenor_days", "investment.fd.tenor_days"},
		{"tenor_months", "investment.fd.tenor_months"},
		{"expected_start_date", "investment.fd.expected_start_date"},
		{"value_date", "investment.fd.value_date"},
		{"maturity_date", "investment.fd.maturity_date"},
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
