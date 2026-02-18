package limit

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// BulkValidationResult contains validation results for bulk operations
type BulkValidationResult struct {
	Index          int    `json:"index"`
	IsValid        bool   `json:"is_valid"`
	Error          string `json:"error,omitempty"`
	LimitID        string `json:"limit_id,omitempty"`
	EntityName     string `json:"entity_name,omitempty"`
	BankName       string `json:"bank_name,omitempty"`
	UtilizedAmount float64 `json:"utilized_amount,omitempty"`
}

// validateBulkLimitUniqueness validates uniqueness for multiple limit records in a single query
// Time Complexity: O(1) database call + O(n*m) memory processing where n=input records, m=existing records
func validateBulkLimitUniqueness(ctx context.Context, pgxPool *pgxpool.Pool, limits []struct {
	Index          int
	EntityName     string
	BankName       string
	CoreLimitType  string
	LimitType      string
	LimitSubType   string
	CurrencyCode   string
}) ([]BulkValidationResult, error) {
	
	if len(limits) == 0 {
		return []BulkValidationResult{}, nil
	}

	// Build a single query to check all combinations at once
	placeholders := make([]string, len(limits))
	args := make([]interface{}, 0, len(limits)*6)
	
	for i, limit := range limits {
		placeholderGroup := fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", 
			i*6+1, i*6+2, i*6+3, i*6+4, i*6+5, i*6+6)
		placeholders[i] = placeholderGroup
		
		args = append(args, limit.EntityName, limit.BankName, limit.CoreLimitType,
			limit.LimitType, limit.LimitSubType, strings.ToUpper(limit.CurrencyCode))
	}

	query := fmt.Sprintf(`
		SELECT entity_name, bank_name, core_limit_type, 
			   COALESCE(limit_type, '') as limit_type,
			   COALESCE(limit_sub_type, '') as limit_sub_type, 
			   currency_code
		FROM cimplrcorpsaas.bank_limit 
		WHERE (entity_name, bank_name, core_limit_type, 
			   COALESCE(limit_type, ''), COALESCE(limit_sub_type, ''), currency_code) 
		IN (VALUES %s)
		AND COALESCE(is_deleted, false) = false`,
		strings.Join(placeholders, ","))

	rows, err := pgxPool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to check bulk uniqueness: %v", err)
	}
	defer rows.Close()

	// Create a map of existing combinations
	existing := make(map[string]bool)
	for rows.Next() {
		var entity, bank, coreType, limitType, limitSubType, currency string
		if err := rows.Scan(&entity, &bank, &coreType, &limitType, &limitSubType, &currency); err != nil {
			continue
		}
		key := fmt.Sprintf("%s|%s|%s|%s|%s|%s", entity, bank, coreType, limitType, limitSubType, currency)
		existing[key] = true
	}

	// Validate each input record
	results := make([]BulkValidationResult, len(limits))
	for i, limit := range limits {
		key := fmt.Sprintf("%s|%s|%s|%s|%s|%s", 
			limit.EntityName, limit.BankName, limit.CoreLimitType,
			limit.LimitType, limit.LimitSubType, strings.ToUpper(limit.CurrencyCode))
		
		if existing[key] {
			results[i] = BulkValidationResult{
				Index:   limit.Index,
				IsValid: false,
				Error:   fmt.Sprintf("duplicate limit: combination already exists for entity '%s', bank '%s'", limit.EntityName, limit.BankName),
			}
		} else {
			results[i] = BulkValidationResult{
				Index:   limit.Index,
				IsValid: true,
			}
		}
	}

	return results, nil
}

// validateBulkUtilizationLimits validates multiple utilization records against their limits in optimized batches
// Time Complexity: O(k) where k = number of unique limit_ids (typically much less than n input records)
func validateBulkUtilizationLimits(ctx context.Context, pgxPool *pgxpool.Pool, utilizations []struct {
	Index          int
	LimitID        string
	UtilizedAmount float64
}) ([]BulkValidationResult, error) {
	
	if len(utilizations) == 0 {
		return []BulkValidationResult{}, nil
	}

	// Group utilizations by limit_id to batch validation
	limitGroups := make(map[string][]int) // limit_id -> []utilization_indices
	limitAmounts := make(map[string]float64) // limit_id -> total_new_amount
	
	for i, util := range utilizations {
		limitGroups[util.LimitID] = append(limitGroups[util.LimitID], i)
		limitAmounts[util.LimitID] += util.UtilizedAmount
	}

	// Get unique limit IDs
	uniqueLimitIDs := make([]string, 0, len(limitGroups))
	for limitID := range limitGroups {
		uniqueLimitIDs = append(uniqueLimitIDs, limitID)
	}

	// Single query to get all limit information
	query := `
		WITH limit_info AS (
			SELECT 
				l.limit_id,
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
			WHERE l.limit_id = ANY($1) AND COALESCE(l.is_deleted, false) = false
		),
		approved_utilizations AS (
			SELECT 
				u.limit_id,
				COALESCE(SUM(u.utilized_amount), 0) AS total_approved_utilization
			FROM cimplrcorpsaas.bank_limit_utilization u
			INNER JOIN LATERAL (
				SELECT processing_status
				FROM cimplrcorpsaas.auditactionbanklimitutilization
				WHERE utilization_id = u.utilization_id
				ORDER BY requested_at DESC
				LIMIT 1
			) au ON au.processing_status = 'APPROVED'
			WHERE u.limit_id = ANY($1) AND COALESCE(u.is_deleted, false) = false
			GROUP BY u.limit_id
		)
		SELECT 
			li.limit_id,
			li.sanctioned_amount,
			li.initial_utilization,
			COALESCE(au.total_approved_utilization, 0) AS total_approved_utilization,
			li.entity_name,
			li.bank_name,
			li.core_limit_type
		FROM limit_info li
		LEFT JOIN approved_utilizations au ON au.limit_id = li.limit_id
	`

	rows, err := pgxPool.Query(ctx, query, uniqueLimitIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch bulk limit information: %v", err)
	}
	defer rows.Close()

	// Store limit information
	limitInfo := make(map[string]struct {
		SanctionedAmount       float64
		InitialUtilization     float64
		TotalApprovedUtilization float64
		EntityName             string
		BankName               string
		CoreLimitType          string
	})

	for rows.Next() {
		var limitID, entityName, bankName, coreLimitType string
		var sanctionedAmount, initialUtilization, totalApprovedUtilization float64
		
		if err := rows.Scan(&limitID, &sanctionedAmount, &initialUtilization, 
			&totalApprovedUtilization, &entityName, &bankName, &coreLimitType); err != nil {
			continue
		}
		
		limitInfo[limitID] = struct {
			SanctionedAmount       float64
			InitialUtilization     float64
			TotalApprovedUtilization float64
			EntityName             string
			BankName               string
			CoreLimitType          string
		}{sanctionedAmount, initialUtilization, totalApprovedUtilization, entityName, bankName, coreLimitType}
	}

	// Validate each utilization
	results := make([]BulkValidationResult, len(utilizations))
	for i, util := range utilizations {
		info, exists := limitInfo[util.LimitID]
		if !exists {
			results[i] = BulkValidationResult{
				Index:     util.Index,
				IsValid:   false,
				Error:     fmt.Sprintf("limit not found or not approved: %s", util.LimitID),
				LimitID:   util.LimitID,
			}
			continue
		}

		// Calculate if this utilization would exceed the limit
		currentUtilization := info.InitialUtilization + info.TotalApprovedUtilization
		
		// For bulk operations, we need to consider the cumulative effect of all utilizations for this limit
		totalNewAmount := limitAmounts[util.LimitID]
		totalUtilization := currentUtilization + totalNewAmount

		if totalUtilization > info.SanctionedAmount {
			exceededBy := totalUtilization - info.SanctionedAmount
			results[i] = BulkValidationResult{
				Index:          util.Index,
				IsValid:        false,
				Error:          fmt.Sprintf("limit exceeded: batch would exceed sanctioned limit by %.2f. Current utilization: %.2f, Batch total: %.2f, Sanctioned limit: %.2f (Entity: %s, Bank: %s, Limit Type: %s)",
					exceededBy, currentUtilization, totalNewAmount, info.SanctionedAmount, info.EntityName, info.BankName, info.CoreLimitType),
				LimitID:        util.LimitID,
				EntityName:     info.EntityName,
				BankName:       info.BankName,
				UtilizedAmount: util.UtilizedAmount,
			}
		} else {
			results[i] = BulkValidationResult{
				Index:          util.Index,
				IsValid:        true,
				LimitID:        util.LimitID,
				EntityName:     info.EntityName,
				BankName:       info.BankName,
				UtilizedAmount: util.UtilizedAmount,
			}
		}
	}

	return results, nil
}