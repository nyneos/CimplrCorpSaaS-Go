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
		       ca.new_scheme_name, 
		       COALESCE(ca.ratio_new, 0) as ratio_new, 
		       COALESCE(ca.ratio_old, 0) as ratio_old, 
		       COALESCE(ca.conversion_ratio, 0) as conversion_ratio, 
		       COALESCE(ca.bonus_units, 0) as bonus_units
		FROM investment.accounting_corporate_action ca
		WHERE ca.activity_id = $1 AND COALESCE(ca.is_deleted, false) = false
	`, activityID)
	if err != nil {
		return fmt.Errorf("failed to fetch corporate actions: %w", err)
	}

	// Collect all affected scheme IDs for snapshot
	affectedSchemes := make(map[string]bool)

	// READ ALL ROWS FIRST - close before processing to avoid conn busy
	type CARecord struct {
		CaID            string
		ActionType      string
		SourceSchemeID  string
		TargetSchemeID  *string
		NewSchemeName   *string
		RatioNew        float64
		RatioOld        float64
		ConversionRatio float64
		BonusUnits      float64
	}
	var caRecords []CARecord

	for rows.Next() {
		var rec CARecord
		if err := rows.Scan(&rec.CaID, &rec.ActionType, &rec.SourceSchemeID, &rec.TargetSchemeID,
			&rec.NewSchemeName, &rec.RatioNew, &rec.RatioOld, &rec.ConversionRatio, &rec.BonusUnits); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan corporate action: %w", err)
		}
		caRecords = append(caRecords, rec)

		// Collect affected schemes
		affectedSchemes[rec.SourceSchemeID] = true
		if rec.TargetSchemeID != nil && *rec.TargetSchemeID != "" {
			affectedSchemes[*rec.TargetSchemeID] = true
		}
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return err
	}

	// Convert affected schemes map to slice
	schemeIDs := make([]string, 0, len(affectedSchemes))
	for schemeID := range affectedSchemes {
		schemeIDs = append(schemeIDs, schemeID)
	}

	// CAPTURE SNAPSHOT BEFORE making any changes
	if len(caRecords) > 0 && len(schemeIDs) > 0 {
		snapshot, err := CaptureSnapshotBeforeCorporateAction(ctx, tx, activityID, caRecords[0].ActionType, schemeIDs)
		if err != nil {
			return fmt.Errorf("failed to capture snapshot: %w", err)
		}

		// Save snapshot to audit table
		if err := SaveSnapshotToAudit(ctx, tx, activityID, snapshot, true); err != nil {
			return fmt.Errorf("failed to save snapshot: %w", err)
		}
	}

	// NOW PROCESS - connection is free for sub-queries
	for _, rec := range caRecords {
		// Convert zero values to pointers for null handling
		var ratioNewPtr, ratioOldPtr, conversionRatioPtr *float64
		if rec.RatioNew != 0 {
			ratioNewPtr = &rec.RatioNew
		}
		if rec.RatioOld != 0 {
			ratioOldPtr = &rec.RatioOld
		}
		if rec.ConversionRatio != 0 {
			conversionRatioPtr = &rec.ConversionRatio
		}

		switch rec.ActionType {
		case "SCHEME_MERGER":
			if err := processMerger(ctx, tx, rec.SourceSchemeID, rec.TargetSchemeID, conversionRatioPtr, activityID); err != nil {
				return fmt.Errorf("merger processing failed: %w", err)
			}

		case "SCHEME_NAME_CHANGE":
			if err := processNameChange(ctx, tx, rec.SourceSchemeID, rec.NewSchemeName); err != nil {
				return fmt.Errorf("name change processing failed: %w", err)
			}

		case "SPLIT":
			if ratioNewPtr == nil || ratioOldPtr == nil {
				return fmt.Errorf("SPLIT requires ratio_new and ratio_old - please provide split ratio values (e.g., 2:1 split = ratio_new:2, ratio_old:1). Current values: ratio_new=%v, ratio_old=%v", rec.RatioNew, rec.RatioOld)
			}
			if err := processSplit(ctx, tx, rec.SourceSchemeID, ratioNewPtr, ratioOldPtr, activityID); err != nil {
				return fmt.Errorf("split processing failed: %w", err)
			}

		case "BONUS":
			if ratioNewPtr == nil || ratioOldPtr == nil {
				return fmt.Errorf("BONUS requires ratio_new and ratio_old - please provide bonus ratio values (e.g., 1:2 bonus = ratio_new:1, ratio_old:2). Current values: ratio_new=%v, ratio_old=%v", rec.RatioNew, rec.RatioOld)
			}
			if err := processBonus(ctx, tx, rec.SourceSchemeID, ratioNewPtr, ratioOldPtr, activityID); err != nil {
				return fmt.Errorf("bonus processing failed: %w", err)
			}

		default:
			return fmt.Errorf("unsupported action type: %s", rec.ActionType)
		}
	}

	return nil
}

// processMerger: Merge scheme A into B, update scheme name in master table
// New Units in B = round_units(Units in A * Conversion Ratio)
func processMerger(ctx context.Context, tx DBExecutor, sourceSchemeID string, targetSchemeID *string, conversionRatio *float64, activityID string) error {
	if targetSchemeID == nil || *targetSchemeID == "" {
		return fmt.Errorf("SCHEME_MERGER requires target_scheme_id - please specify the target scheme for merger")
	}

	// Default conversion ratio to 1:1 if not specified
	var ratio float64 = 1.0
	if conversionRatio != nil && *conversionRatio > 0 {
		ratio = *conversionRatio
	}

	// Get source scheme name
	var sourceSchemeName string
	err := tx.QueryRow(ctx, `
		SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1
	`, sourceSchemeID).Scan(&sourceSchemeName)
	if err != nil {
		return fmt.Errorf("source scheme not found: %w", err)
	}

	// Note: Do not modify master scheme name here. Name changes are handled
	// only via the SCHEME_NAME_CHANGE action. Merger should update holdings
	// and scheme_id references (done below) but not overwrite master names.

	// Update portfolio holdings: merge source into target with conversion ratio
	// This affects portfolio tracking tables
	_, err = tx.Exec(ctx, `
		UPDATE investment.portfolio_snapshot
		SET scheme_id = $1,
		    total_units = ROUND(total_units * $2, 3)
		WHERE scheme_id = $3
	`, *targetSchemeID, ratio, sourceSchemeID)
	if err != nil {
		return fmt.Errorf("failed to update portfolio snapshot: %w", err)
	}

	// UPDATE ONBOARD_TRANSACTION (source of truth) - merge scheme A into B
	_, err = tx.Exec(ctx, `
		UPDATE investment.onboard_transaction
		SET scheme_id = $1,
		    units = ROUND(units * $2, 3)
		WHERE scheme_id = $3
	`, *targetSchemeID, ratio, sourceSchemeID)
	if err != nil {
		return fmt.Errorf("failed to update onboard_transaction for merger: %w", err)
	}

	return nil
}

// processNameChange: Update scheme name in master table
func processNameChange(ctx context.Context, tx DBExecutor, sourceSchemeID string, newSchemeName *string) error {
	if newSchemeName == nil || *newSchemeName == "" {
		return fmt.Errorf("SCHEME_NAME_CHANGE requires new_scheme_name - please specify the new name for the scheme")
	}

	_, err := tx.Exec(ctx, `
		UPDATE investment.masterscheme 
		SET scheme_name = $1
		WHERE scheme_id = $2
	`, *newSchemeName, sourceSchemeID)

	if err != nil {
		return fmt.Errorf("failed to update scheme name: %w", err)
	}

	return nil
}

// processSplit: Split (a for b): New Units = Old Units * a / b; New Avg. Cost = Old Avg. Cost * b / a
func processSplit(ctx context.Context, tx DBExecutor, sourceSchemeID string, ratioNew *float64, ratioOld *float64, activityID string) error {
	if ratioNew == nil || ratioOld == nil || *ratioNew <= 0 || *ratioOld <= 0 {
		return fmt.Errorf("valid ratio_new (a) and ratio_old (b) are required for split")
	}

	// Split ratio: a for b means you get 'a' new units for every 'b' old units
	splitMultiplier := *ratioNew / *ratioOld

	// Update portfolio holdings
	_, err := tx.Exec(ctx, `
		UPDATE investment.portfolio_snapshot
		SET total_units = ROUND(total_units * $1, 3),
		    avg_nav = avg_nav * $2
		WHERE scheme_id = $3
	`, splitMultiplier, *ratioOld / *ratioNew, sourceSchemeID)

	if err != nil {
		return fmt.Errorf("failed to process split: %w", err)
	}

	// UPDATE ONBOARD_TRANSACTION (source of truth) - apply split ratio
	_, err = tx.Exec(ctx, `
		UPDATE investment.onboard_transaction
		SET units = ROUND(units * $1, 3),
		    nav = nav * $2
		WHERE scheme_id = $3
	`, splitMultiplier, *ratioOld / *ratioNew, sourceSchemeID)

	if err != nil {
		return fmt.Errorf("failed to update onboard_transaction for split: %w", err)
	}

	// Do not change master scheme name for split actions. Only update
	// portfolio snapshot totals and avg_nav above. Name metadata should be
	// changed only via explicit SCHEME_NAME_CHANGE actions.

	return nil
}

// processBonus: Bonus (x for y): Bonus Units = floor_to_precision(Old Units * x / y)
// New Avg. Cost = Old Cost Basis / (Old Units + Bonus Units)
func processBonus(ctx context.Context, tx DBExecutor, sourceSchemeID string, ratioNew *float64, ratioOld *float64, activityID string) error {
	if ratioNew == nil || ratioOld == nil || *ratioNew <= 0 || *ratioOld <= 0 {
		return fmt.Errorf("valid ratio_new (x) and ratio_old (y) are required for bonus")
	}

	// Bonus ratio: x for y means you get 'x' bonus units for every 'y' held units
	bonusRatio := *ratioNew / *ratioOld

	// Struct to hold holding data
	type holdingData struct {
		folioNumber    *string
		dematAccNumber *string
		entityName     *string
		totalUnits     float64
		avgCost        float64
	}

	// Fetch all holdings for this scheme
	rows, err := tx.Query(ctx, `
		SELECT folio_number, demat_acc_number, total_units, 
		       avg_nav, entity_name
		FROM investment.portfolio_snapshot
		WHERE scheme_id = $1 AND total_units > 0
		FOR UPDATE
	`, sourceSchemeID)
	if err != nil {
		return fmt.Errorf("failed to fetch holdings: %w", err)
	}

	// Collect all holdings first to avoid "conn busy" error
	var holdings []holdingData
	for rows.Next() {
		var h holdingData
		if err := rows.Scan(&h.folioNumber, &h.dematAccNumber, &h.totalUnits, &h.avgCost, &h.entityName); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan holding: %w", err)
		}
		holdings = append(holdings, h)
	}
	rows.Close() // Close rows before executing updates

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating holdings: %w", err)
	}

	// Now update each holding
	for _, h := range holdings {
		// Calculate bonus units: floor to 3 decimal places
		bonusUnits := math.Floor((h.totalUnits*bonusRatio)*1000) / 1000
		newTotalUnits := h.totalUnits + bonusUnits

		// Calculate new average cost: Old Cost Basis / New Total Units
		oldCostBasis := h.totalUnits * h.avgCost
		newAvgCost := oldCostBasis / newTotalUnits

		// Update holding
		_, err := tx.Exec(ctx, `
			UPDATE investment.portfolio_snapshot
			SET total_units = $1,
			    avg_nav = $2
			WHERE scheme_id = $3 
			  AND COALESCE(folio_number, '') = COALESCE($4, '')
			  AND COALESCE(demat_acc_number, '') = COALESCE($5, '')
			  AND COALESCE(entity_name, '') = COALESCE($6, '')
		`, newTotalUnits, newAvgCost, sourceSchemeID, h.folioNumber, h.dematAccNumber, h.entityName)

		if err != nil {
			return fmt.Errorf("failed to update holding with bonus: %w", err)
		}
	}

	// UPDATE ONBOARD_TRANSACTION (source of truth) - bonus doesn't change individual transactions
	// Instead, we need to INSERT synthetic bonus transactions to maintain balance
	// For each unique folio/demat + scheme combination, insert a bonus transaction
	rows2, err := tx.Query(ctx, `
		SELECT DISTINCT
			COALESCE(folio_number, '') as folio_number,
			COALESCE(folio_id, '') as folio_id,
			COALESCE(demat_acc_number, '') as demat_acc_number,
			COALESCE(demat_id, '') as demat_id,
			scheme_id,
			SUM(units) as total_units
		FROM investment.onboard_transaction
		WHERE scheme_id = $1
		GROUP BY folio_number, folio_id, demat_acc_number, demat_id, scheme_id
		HAVING SUM(units) > 0
	`, sourceSchemeID)
	if err != nil {
		return fmt.Errorf("failed to fetch transaction groups for bonus: %w", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var folioNumber, folioID, dematAcc, dematID, schemeID string
		var totalUnits float64
		if err := rows2.Scan(&folioNumber, &folioID, &dematAcc, &dematID, &schemeID, &totalUnits); err != nil {
			continue
		}

		// Calculate bonus units
		bonusUnitsCalc := math.Floor((totalUnits*bonusRatio)*1000) / 1000
		if bonusUnitsCalc <= 0 {
			continue
		}

		// Insert bonus transaction with NAV = 0 (zero cost basis)
		_, err := tx.Exec(ctx, `
			INSERT INTO investment.onboard_transaction (
				transaction_date, transaction_type, folio_number, folio_id,
				demat_acc_number, demat_id, scheme_id, units, nav, amount, 
				entity_name, batch_id
			)
			SELECT 
				CURRENT_DATE as transaction_date,
				'BONUS' as transaction_type,
				$1, $2, $3, $4, $5, $6,
				0 as nav,  -- Bonus units have zero cost
				0 as amount,
				entity_name,
				'BONUS-' || $7 as batch_id
			FROM investment.onboard_transaction
			WHERE scheme_id = $5
			  AND COALESCE(folio_number, '') = $1
			  AND COALESCE(demat_acc_number, '') = $3
			LIMIT 1
		`, folioNumber, folioID, dematAcc, dematID, schemeID, bonusUnitsCalc, activityID)

		if err != nil {
			return fmt.Errorf("failed to insert bonus transaction: %w", err)
		}
	}

	// Do not modify master scheme name for bonus actions. Bonus adjustments
	// are applied to holdings only; master scheme name changes belong to
	// explicit SCHEME_NAME_CHANGE actions.

	return nil
}

// ProcessFVONavOverride validates FVO records during approval
// FVO doesn't modify master tables - it just creates accounting entries using override_nav
// The override_nav is used in journal generation to adjust valuations
func ProcessFVONavOverride(ctx context.Context, tx DBExecutor, activityID string) error {
	// Validate that all FVO records reference valid schemes
	var invalidSchemes []string
	rows, err := tx.Query(ctx, `
		SELECT f.scheme_id
		FROM investment.accounting_fvo f
		LEFT JOIN investment.masterscheme ms ON ms.scheme_id = f.scheme_id
		WHERE f.activity_id = $1 
		  AND COALESCE(f.is_deleted, false) = false
		  AND ms.scheme_id IS NULL
	`, activityID)
	if err != nil {
		return fmt.Errorf("failed to validate FVO schemes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schemeID string
		if err := rows.Scan(&schemeID); err != nil {
			return fmt.Errorf("failed to scan invalid scheme: %w", err)
		}
		invalidSchemes = append(invalidSchemes, schemeID)
	}

	if len(invalidSchemes) > 0 {
		return fmt.Errorf("FVO contains invalid scheme_ids: %v", invalidSchemes)
	}

	// FVO is valid - journal generation will use override_nav from FVO records
	// No master table updates needed
	return nil
}
