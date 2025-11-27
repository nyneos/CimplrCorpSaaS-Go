package accountingworkbench

import (
	"context"
	"fmt"
	"math"
)

// ProcessCorporateAction executes the corporate action logic on master scheme table
// Called during approval to apply the corporate action changes
func ProcessCorporateAction(ctx context.Context, tx DBExecutor, activityID string) error {
	// Fetch all corporate actions for this activity
	rows, err := tx.Query(ctx, `
		SELECT ca.ca_id, ca.action_type, ca.source_scheme_id, ca.target_scheme_id, 
		       ca.new_scheme_name, ca.ratio_new, ca.ratio_old, ca.conversion_ratio, ca.bonus_units
		FROM investment.accounting_corporate_action ca
		WHERE ca.activity_id = $1 AND COALESCE(ca.is_deleted, false) = false
	`, activityID)
	if err != nil {
		return fmt.Errorf("failed to fetch corporate actions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var caID, actionType, sourceSchemeID string
		var targetSchemeID, newSchemeName *string
		var ratioNew, ratioOld, conversionRatio, bonusUnits *float64

		if err := rows.Scan(&caID, &actionType, &sourceSchemeID, &targetSchemeID, 
			&newSchemeName, &ratioNew, &ratioOld, &conversionRatio, &bonusUnits); err != nil {
			return fmt.Errorf("failed to scan corporate action: %w", err)
		}

		switch actionType {
		case "SCHEME_MERGER":
			if err := processMerger(ctx, tx, sourceSchemeID, targetSchemeID, conversionRatio); err != nil {
				return fmt.Errorf("merger processing failed: %w", err)
			}

		case "SCHEME_NAME_CHANGE":
			if err := processNameChange(ctx, tx, sourceSchemeID, newSchemeName); err != nil {
				return fmt.Errorf("name change processing failed: %w", err)
			}

		case "SPLIT":
			if err := processSplit(ctx, tx, sourceSchemeID, ratioNew, ratioOld); err != nil {
				return fmt.Errorf("split processing failed: %w", err)
			}

		case "BONUS":
			if err := processBonus(ctx, tx, sourceSchemeID, ratioNew, ratioOld); err != nil {
				return fmt.Errorf("bonus processing failed: %w", err)
			}

		default:
			return fmt.Errorf("unsupported action type: %s", actionType)
		}
	}

	return rows.Err()
}

// processMerger: Merge scheme A into B, update scheme name in master table
// New Units in B = round_units(Units in A * Conversion Ratio)
func processMerger(ctx context.Context, tx DBExecutor, sourceSchemeID string, targetSchemeID *string, conversionRatio *float64) error {
	if targetSchemeID == nil || *targetSchemeID == "" {
		return fmt.Errorf("target_scheme_id is required for merger")
	}
	if conversionRatio == nil || *conversionRatio <= 0 {
		*conversionRatio = 1.0 // Default 1:1 if not specified
	}

	// Get source scheme name
	var sourceSchemeName string
	err := tx.QueryRow(ctx, `
		SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1
	`, sourceSchemeID).Scan(&sourceSchemeName)
	if err != nil {
		return fmt.Errorf("source scheme not found: %w", err)
	}

	// Update the source scheme to point to target (mark as merged)
	_, err = tx.Exec(ctx, `
		UPDATE investment.masterscheme 
		SET scheme_name = $1 || ' (Merged into ' || $2 || ')',
		    is_active = false,
		    updated_at = now()
		WHERE scheme_id = $3
	`, sourceSchemeName, *targetSchemeID, sourceSchemeID)
	if err != nil {
		return fmt.Errorf("failed to update source scheme: %w", err)
	}

	// Update portfolio holdings: merge source into target with conversion ratio
	// This affects portfolio tracking tables
	_, err = tx.Exec(ctx, `
		UPDATE investment.portfolio_snapshot
		SET scheme_id = $1,
		    total_units = ROUND(total_units * $2, 3),
		    updated_at = now()
		WHERE scheme_id = $3
	`, *targetSchemeID, *conversionRatio, sourceSchemeID)
	if err != nil {
		return fmt.Errorf("failed to update portfolio snapshot: %w", err)
	}

	return nil
}

// processNameChange: Update scheme name in master table
func processNameChange(ctx context.Context, tx DBExecutor, sourceSchemeID string, newSchemeName *string) error {
	if newSchemeName == nil || *newSchemeName == "" {
		return fmt.Errorf("new_scheme_name is required for name change")
	}

	_, err := tx.Exec(ctx, `
		UPDATE investment.masterscheme 
		SET scheme_name = $1,
		    updated_at = now()
		WHERE scheme_id = $2
	`, *newSchemeName, sourceSchemeID)
	
	if err != nil {
		return fmt.Errorf("failed to update scheme name: %w", err)
	}

	return nil
}

// processSplit: Split (a for b): New Units = Old Units * a / b; New Avg. Cost = Old Avg. Cost * b / a
func processSplit(ctx context.Context, tx DBExecutor, sourceSchemeID string, ratioNew *float64, ratioOld *float64) error {
	if ratioNew == nil || ratioOld == nil || *ratioNew <= 0 || *ratioOld <= 0 {
		return fmt.Errorf("valid ratio_new (a) and ratio_old (b) are required for split")
	}

	// Split ratio: a for b means you get 'a' new units for every 'b' old units
	splitMultiplier := *ratioNew / *ratioOld

	// Update portfolio holdings
	_, err := tx.Exec(ctx, `
		UPDATE investment.portfolio_snapshot
		SET total_units = ROUND(total_units * $1, 3),
		    average_cost = average_cost * $2,
		    updated_at = now()
		WHERE scheme_id = $3
	`, splitMultiplier, *ratioOld / *ratioNew, sourceSchemeID)
	
	if err != nil {
		return fmt.Errorf("failed to process split: %w", err)
	}

	// Log split in scheme master (optional)
	_, err = tx.Exec(ctx, `
		UPDATE investment.masterscheme 
		SET scheme_name = scheme_name || ' (Split ' || $1::text || ':' || $2::text || ')',
		    updated_at = now()
		WHERE scheme_id = $3 
		  AND scheme_name NOT LIKE '%Split%'
	`, *ratioNew, *ratioOld, sourceSchemeID)

	return err
}

// processBonus: Bonus (x for y): Bonus Units = floor_to_precision(Old Units * x / y)
// New Avg. Cost = Old Cost Basis / (Old Units + Bonus Units)
func processBonus(ctx context.Context, tx DBExecutor, sourceSchemeID string, ratioNew *float64, ratioOld *float64) error {
	if ratioNew == nil || ratioOld == nil || *ratioNew <= 0 || *ratioOld <= 0 {
		return fmt.Errorf("valid ratio_new (x) and ratio_old (y) are required for bonus")
	}

	// Bonus ratio: x for y means you get 'x' bonus units for every 'y' held units
	bonusRatio := *ratioNew / *ratioOld

	// Fetch all holdings for this scheme
	rows, err := tx.Query(ctx, `
		SELECT folio_id, folio_number, demat_id, demat_acc_number, total_units, 
		       average_cost, entity_name
		FROM investment.portfolio_snapshot
		WHERE scheme_id = $1 AND total_units > 0
		FOR UPDATE
	`, sourceSchemeID)
	if err != nil {
		return fmt.Errorf("failed to fetch holdings: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var folioID, folioNumber, dematID, dematAccNumber, entityName *string
		var totalUnits, avgCost float64

		if err := rows.Scan(&folioID, &folioNumber, &dematID, &dematAccNumber, &totalUnits, &avgCost, &entityName); err != nil {
			return fmt.Errorf("failed to scan holding: %w", err)
		}

		// Calculate bonus units: floor to 3 decimal places
		bonusUnits := math.Floor((totalUnits * bonusRatio) * 1000) / 1000
		newTotalUnits := totalUnits + bonusUnits

		// Calculate new average cost: Old Cost Basis / New Total Units
		oldCostBasis := totalUnits * avgCost
		newAvgCost := oldCostBasis / newTotalUnits

		// Update holding
		_, err := tx.Exec(ctx, `
			UPDATE investment.portfolio_snapshot
			SET total_units = $1,
			    average_cost = $2,
			    updated_at = now()
			WHERE scheme_id = $3 
			  AND COALESCE(folio_id, '') = COALESCE($4, '')
			  AND COALESCE(folio_number, '') = COALESCE($5, '')
			  AND COALESCE(demat_id, '') = COALESCE($6, '')
			  AND COALESCE(demat_acc_number, '') = COALESCE($7, '')
			  AND COALESCE(entity_name, '') = COALESCE($8, '')
		`, newTotalUnits, newAvgCost, sourceSchemeID, folioID, folioNumber, dematID, dematAccNumber, entityName)
		
		if err != nil {
			return fmt.Errorf("failed to update holding with bonus: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating holdings: %w", err)
	}

	// Log bonus in scheme master (optional)
	_, err = tx.Exec(ctx, `
		UPDATE investment.masterscheme 
		SET scheme_name = scheme_name || ' (Bonus ' || $1::text || ':' || $2::text || ')',
		    updated_at = now()
		WHERE scheme_id = $3
		  AND scheme_name NOT LIKE '%Bonus%'
	`, *ratioNew, *ratioOld, sourceSchemeID)

	return err
}
