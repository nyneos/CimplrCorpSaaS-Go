package validation

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ValidationResult contains all pre-validated data for a request.
type ValidationResult struct {
	UserID string

	// Multi-entity: all root entity IDs/names directly mapped to this user via
	// user_entity_mappings.
	RootEntityIDs   []string
	RootEntityNames []string

	// Backward-compat convenience fields: first element of RootEntityIDs/Names.
	RootEntityID   string
	RootEntityName string
}

// PreValidateRequest validates the user and resolves all entity roots the user has access to.
//
// Resolution order:
//  1. Query user_entity_mappings — supports multiple directly-assigned entities.
//  2. If none found, fall back to legacy users.business_unit_name → masterentitycash lookup.
func PreValidateRequest(ctx context.Context, db *pgxpool.Pool, userID string) (*ValidationResult, error) {
	if userID == "" {
		return nil, fmt.Errorf(constants.ErrUserIIsRequired)
	}

	// Step 1: confirm the user exists.
	var result ValidationResult
	err := db.QueryRow(ctx,
		"SELECT id FROM users WHERE id = $1 LIMIT 1",
		userID,
	).Scan(&result.UserID)
	if err != nil {
		return nil, fmt.Errorf("failed to validate user: %w", err)
	}
	if result.UserID == "" {
		return nil, fmt.Errorf("user not found: %s", userID)
	}

	// Step 2: load all directly-mapped entities from user_entity_mappings.
	mapRows, mapErr := db.Query(ctx,
		"SELECT entity_id, entity_name FROM user_entity_mappings WHERE user_id = $1 ORDER BY entity_id",
		userID,
	)
	if mapErr == nil {
		defer mapRows.Close()
		for mapRows.Next() {
			var eid, ename string
			if scanErr := mapRows.Scan(&eid, &ename); scanErr == nil && eid != "" {
				result.RootEntityIDs = append(result.RootEntityIDs, eid)
				result.RootEntityNames = append(result.RootEntityNames, ename)
			}
		}
	}

	// Populate backward-compat single-value fields (first element).
	if len(result.RootEntityIDs) > 0 {
		result.RootEntityID = result.RootEntityIDs[0]
		result.RootEntityName = result.RootEntityNames[0]
	}

	return &result, nil
}
