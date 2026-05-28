package limit

import (
	"context"
	"fmt"

	// "time"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LimitUniqueKey groups parameters for uniqueness checks to keep signatures small
type LimitUniqueKey struct {
	EntityName     string
	BankName       string
	CoreLimitType  string
	LimitType      string
	LimitSubType   string
	CurrencyCode   string
	ExcludeLimitID string
}

// checkLimitUniqueness validates that the limit combination doesn't already exist for active records
func checkLimitUniqueness(ctx context.Context, pgxPool *pgxpool.Pool, key LimitUniqueKey) error {
	var query string
	var args []interface{}

	if key.ExcludeLimitID != "" {
		// For updates, exclude the current limit ID
		query = `SELECT 1 FROM cimplrcorpsaas.bank_limit 
				WHERE entity_name = $1 AND bank_name = $2 AND core_limit_type = $3 
				AND COALESCE(limit_type, '') = $4 AND COALESCE(limit_sub_type, '') = $5 
				AND currency_code = $6 AND limit_id != $7 
				AND COALESCE(is_deleted, false) = false LIMIT 1`
		args = []interface{}{key.EntityName, key.BankName, key.CoreLimitType, key.LimitType, key.LimitSubType, key.CurrencyCode, key.ExcludeLimitID}
	} else {
		// For creates, check if any active record exists
		query = `SELECT 1 FROM cimplrcorpsaas.bank_limit 
				WHERE entity_name = $1 AND bank_name = $2 AND core_limit_type = $3 
				AND COALESCE(limit_type, '') = $4 AND COALESCE(limit_sub_type, '') = $5 
				AND currency_code = $6 
				AND COALESCE(is_deleted, false) = false LIMIT 1`
		args = []interface{}{key.EntityName, key.BankName, key.CoreLimitType, key.LimitType, key.LimitSubType, key.CurrencyCode}
	}

	var exists int
	err := pgxPool.QueryRow(ctx, query, args...).Scan(&exists)
	if err == nil {
		return fmt.Errorf("duplicate limit: combination of entity '%s', bank '%s', core limit type '%s', limit type '%s', limit sub type '%s', currency '%s' already exists",
			key.EntityName, key.BankName, key.CoreLimitType, key.LimitType, key.LimitSubType, key.CurrencyCode)
	}

	// If error is 'no rows', that's expected (no duplicate found)
	return nil
}

// Helper functions for nullable fields
func nullifyEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullifyFloat(f *float64) interface{} {
	if f == nil {
		return nil
	}
	return *f
}

func nullifyBool(b *bool) interface{} {
	if b == nil {
		return nil
	}
	return *b
}

// validateUtilizationLimit checks if the new utilization would exceed the sanctioned limit
func validateUtilizationLimit(ctx context.Context, pgxPool *pgxpool.Pool, limitID string, newUtilizedAmount float64) error {
	query := `
		WITH limit_info AS (
			SELECT 
				l.sanctioned_amount,
				COALESCE(l.initial_utilization, 0) AS initial_utilization,
				l.entity_name,
				l.bank_name,
				l.core_limit_type
			FROM cimplrcorpsaas.bank_limit l
			INNER JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.auditactionbanklimit
				WHERE limit_id = l.limit_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON a.processing_status = 'APPROVED'
			WHERE l.limit_id = $1 AND COALESCE(l.is_deleted, false) = false
		),
		approved_utilizations AS (
			SELECT 
				COALESCE(SUM(u.utilized_amount), 0) AS total_approved_utilization
			FROM cimplrcorpsaas.bank_limit_utilization u
			INNER JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.auditactionbanklimitutilization
				WHERE utilization_id = u.utilization_id
				ORDER BY requested_at DESC
				LIMIT 1
			) au ON au.processing_status = 'APPROVED'
			WHERE u.limit_id = $1 AND COALESCE(u.is_deleted, false) = false
		)
		SELECT 
			li.sanctioned_amount,
			li.initial_utilization,
			COALESCE(au.total_approved_utilization, 0) AS total_approved_utilization,
			li.entity_name,
			li.bank_name,
			li.core_limit_type
		FROM limit_info li
		CROSS JOIN approved_utilizations au
	`

	var sanctionedAmount, initialUtilization, totalApprovedUtilization float64
	var entityName, bankName, coreLimitType string

	err := pgxPool.QueryRow(ctx, query, limitID).Scan(
		&sanctionedAmount, &initialUtilization, &totalApprovedUtilization,
		&entityName, &bankName, &coreLimitType,
	)
	if err != nil {
		return fmt.Errorf("failed to fetch limit information: %v", err)
	}

	// Calculate total utilization if this new amount were approved
	totalUtilization := initialUtilization + totalApprovedUtilization + newUtilizedAmount

	if totalUtilization > sanctionedAmount {
		exceededBy := totalUtilization - sanctionedAmount
		return fmt.Errorf("limit exceeded: this utilization would exceed the sanctioned limit by %.2f. Current utilization: %.2f, Sanctioned limit: %.2f (Entity: %s, Bank: %s, Limit Type: %s)",
			exceededBy, initialUtilization+totalApprovedUtilization, sanctionedAmount, entityName, bankName, coreLimitType)
	}

	return nil
}
