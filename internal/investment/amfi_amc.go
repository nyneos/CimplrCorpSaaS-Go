package investment

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const amfiAMCNameHelp = "AMC name must exactly match an official name from AMFI scheme master data (Association of Mutual Funds in India). Use the AMC Master dropdown or /master/amfi/distinct-amcs — do not use NAV-file variants ending in 'Mutual Fund' unless that exact name exists in AMFI scheme data."

// LoadAMFISchemeAMCNames returns canonical AMC names from amfi_scheme_master_staging
// keyed by lower(trim(name)) -> canonical casing from AMFI.
func LoadAMFISchemeAMCNames(ctx context.Context, db *pgxpool.Pool) (map[string]string, error) {
	rows, err := db.Query(ctx, `
		SELECT DISTINCT amc_name
		FROM investment.amfi_scheme_master_staging
		WHERE amc_name IS NOT NULL AND TRIM(amc_name) <> ''
		  AND amc_name NOT LIKE 'Open Ended Schemes%'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		key := normalizeAMCKey(name)
		if key != "" {
			out[key] = strings.TrimSpace(name)
		}
	}
	return out, nil
}

func normalizeAMCKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// ValidateAMFISchemeAMCName checks that name exists in AMFI scheme staging.
// Returns canonical AMFI name and empty errMsg on success.
func ValidateAMFISchemeAMCName(ctx context.Context, db *pgxpool.Pool, name string) (canonical string, errMsg string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", "AMC name is required."
	}
	if strings.HasPrefix(name, "Open Ended Schemes") {
		return "", "Invalid AMC name (parsed category header, not an AMC). " + amfiAMCNameHelp
	}

	names, err := LoadAMFISchemeAMCNames(ctx, db)
	if err != nil {
		return "", "Failed to validate AMC name against AMFI data."
	}
	if canonical, ok := names[normalizeAMCKey(name)]; ok {
		return canonical, ""
	}
	return "", fmt.Sprintf("AMC name '%s' is not a valid AMFI scheme master name. %s", name, amfiAMCNameHelp)
}

// ResolveToSchemeAMCName maps a masteramc or NAV-variant name to the canonical scheme-file AMC name.
func ResolveToSchemeAMCName(ctx context.Context, db *pgxpool.Pool, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil
	}

	names, err := LoadAMFISchemeAMCNames(ctx, db)
	if err != nil {
		return "", err
	}
	if canonical, ok := names[normalizeAMCKey(name)]; ok {
		return canonical, nil
	}

	// NAV-file variant: "Foo Mutual Fund" -> scheme name with prefix "Foo"
	lower := strings.ToLower(name)
	if strings.HasSuffix(lower, " mutual fund") {
		prefix := strings.TrimSpace(name[:len(name)-len(" Mutual Fund")])
		prefixKey := normalizeAMCKey(prefix)
		for key, canonical := range names {
			if strings.HasPrefix(key, prefixKey) || strings.Contains(key, prefixKey) {
				return canonical, nil
			}
		}
	}

	return "", nil
}

// ResolveManyToSchemeAMCNames resolves a list of master AMC names to distinct scheme-file names.
func ResolveManyToSchemeAMCNames(ctx context.Context, db *pgxpool.Pool, names []string) ([]string, error) {
	seen := make(map[string]bool)
	var out []string
	for _, n := range names {
		resolved, err := ResolveToSchemeAMCName(ctx, db, n)
		if err != nil {
			return nil, err
		}
		if resolved == "" {
			continue
		}
		key := normalizeAMCKey(resolved)
		if !seen[key] {
			seen[key] = true
			out = append(out, resolved)
		}
	}
	return out, nil
}
