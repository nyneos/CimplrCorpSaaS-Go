package cdm

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// validateCatalogBinding keeps CDM authoring a labeled projection of one
// canonical domain_catalog field. Identity/type/unit come from the catalog;
// label, description, alias, and nullable remain CDM metadata.
func validateCatalogBinding(
	ctx context.Context,
	pool *pgxpool.Pool,
	sourceSystem, name, dataType, unit string,
) error {
	sourceSystem = strings.TrimSpace(sourceSystem)
	name = strings.TrimSpace(name)
	dataType = strings.TrimSpace(dataType)
	unit = strings.TrimSpace(unit)
	if sourceSystem == "" {
		return fmt.Errorf("source_system (canonical sub-module code) is required")
	}

	var catalogType, catalogUnit string
	err := pool.QueryRow(ctx, `
		SELECT f.data_type, COALESCE(f.unit, '')
		  FROM domain_catalog.field f
		  JOIN domain_catalog.sub_module s
		    ON s.sub_module_code = f.sub_module_code
		   AND s.is_deleted = false
		 WHERE f.sub_module_code = $1
		   AND f.cdm_path = $2
		   AND f.is_deleted = false
		 ORDER BY f.sort_order, f.field_code
		 LIMIT 1`,
		sourceSystem, name,
	).Scan(&catalogType, &catalogUnit)
	if err != nil {
		return fmt.Errorf("CDM path %q is not defined for catalog sub-module %q; add the field to domain_catalog first", name, sourceSystem)
	}
	if dataType != catalogType {
		return fmt.Errorf("data_type must come from domain_catalog: expected %q, got %q", catalogType, dataType)
	}
	if unit != catalogUnit {
		return fmt.Errorf("unit must come from domain_catalog: expected %q, got %q", catalogUnit, unit)
	}
	return nil
}
