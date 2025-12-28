package validation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ValidationResult contains all pre-validated data for a request
type ValidationResult struct {
	UserID         string
	BusinessUnit   string
	RootEntityID   string
	RootEntityName string
}

// PreValidateRequest performs a single optimized query to validate user and get core metadata
// This replaces multiple middleware queries with ONE database call
func PreValidateRequest(ctx context.Context, db *pgxpool.Pool, userID string) (*ValidationResult, error) {
	if userID == "" {
		return nil, fmt.Errorf("user_id is required")
	}

	// Single mega-query combining:
	// 1. User lookup + business unit
	// 2. Root entity discovery via recursive CTE
	// This replaces 3-4 separate middleware queries
	query := `
		WITH user_info AS (
			SELECT
				id as user_id,
				business_unit_name
			FROM users
			WHERE id = $1
			LIMIT 1
		),
		root_entity AS (
			SELECT
				entity_id,
				entity_name
			FROM masterentitycash
			WHERE entity_name = (SELECT business_unit_name FROM user_info)
			  AND COALESCE(is_deleted, false) = false
			  AND LOWER(active_status) = 'active'
			LIMIT 1
		)
		SELECT
			u.user_id,
			u.business_unit_name,
			COALESCE(e.entity_id, ''),
			COALESCE(e.entity_name, '')
		FROM user_info u
		LEFT JOIN root_entity e ON true
	`

	var result ValidationResult
	err := db.QueryRow(ctx, query, userID).Scan(
		&result.UserID,
		&result.BusinessUnit,
		&result.RootEntityID,
		&result.RootEntityName,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to validate user: %w", err)
	}

	if result.UserID == "" {
		return nil, fmt.Errorf("user not found: %s", userID)
	}

	return &result, nil
}
