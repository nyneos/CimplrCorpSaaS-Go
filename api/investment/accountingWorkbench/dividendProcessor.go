package accountingworkbench

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"
	"log"
	"time"
)

// ProcessDividendReinvestment creates buy transaction when dividend is reinvested
// Called during approval for REINVEST type dividends
func ProcessDividendReinvestment(ctx context.Context, tx DBExecutor, activityID string) error {
	log.Printf("[DEBUG DIVIDEND] ProcessDividendReinvestment called for activity: %s", activityID)

	// First, check ALL dividends for this activity (debugging)
	debugRows, _ := tx.Query(ctx, `
		SELECT dividend_id, transaction_type, dividend_amount, reinvest_nav, reinvest_units
		FROM investment.accounting_dividend
		WHERE activity_id = $1
	`, activityID)
	if debugRows != nil {
		for debugRows.Next() {
			var did, ttype string
			var amt, nav, units float64
			if err := debugRows.Scan(&did, &ttype, &amt, &nav, &units); err == nil {
				log.Printf("[DEBUG DIVIDEND] Found dividend: %s | Type: %s | Amount: %.2f | Nav: %.4f | Units: %.6f",
					did, ttype, amt, nav, units)
			}
		}
		debugRows.Close()
	}

	// Fetch all REINVEST dividends for this activity
	rows, err := tx.Query(ctx, `
		SELECT 
			d.dividend_id,
			d.scheme_id,
			d.folio_id,
			d.dividend_amount,
			d.reinvest_nav,
			d.reinvest_units,
			COALESCE(d.payment_date::text, '')
		FROM investment.accounting_dividend d
		WHERE d.activity_id = $1
		  AND COALESCE(d.is_deleted, false) = false
		  AND LOWER(d.transaction_type) IN ('reinvest', 'reinvestment')
		  AND d.reinvest_units > 0
	`, activityID)
	if err != nil {
		return fmt.Errorf("failed to fetch reinvest dividends: %w", err)
	}

	type DividendReinvest struct {
		DividendID     string
		SchemeID       string
		FolioID        *string
		DividendAmount float64
		ReinvestNAV    float64
		ReinvestUnits  float64
		PaymentDate    string
	}

	var reinvests []DividendReinvest
	for rows.Next() {
		var dr DividendReinvest
		if err := rows.Scan(&dr.DividendID, &dr.SchemeID, &dr.FolioID, &dr.DividendAmount,
			&dr.ReinvestNAV, &dr.ReinvestUnits, &dr.PaymentDate); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan dividend: %w", err)
		}
		reinvests = append(reinvests, dr)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return err
	}

	log.Printf("[DEBUG DIVIDEND] Found %d reinvest dividends with units > 0", len(reinvests))

	if len(reinvests) == 0 {
		log.Printf("[DEBUG DIVIDEND] No reinvestments to process (all dividends have reinvest_units = 0 or are PAYOUT type)")
		return nil
	}

	// For each reinvestment, create a BUY transaction in onboard_transaction
	for _, dr := range reinvests {
		log.Printf("[DEBUG DIVIDEND] Processing reinvest: %s | Scheme: %s | FolioID from dividend: %v | Units: %.6f | NAV: %.4f | Amount: %.2f",
			dr.DividendID, dr.SchemeID, dr.FolioID, dr.ReinvestUnits, dr.ReinvestNAV, dr.DividendAmount)

		// Get folio details if folio_id is provided
		var folioNumber, entityName, folioID string
		var dematAccNumber, dematID *string

		if dr.FolioID != nil && *dr.FolioID != "" {
			// Folio specified in dividend - use it
			log.Printf("[DEBUG DIVIDEND] Fetching folio details for folio_id: %s", *dr.FolioID)
			err := tx.QueryRow(ctx, `
				SELECT 
					folio_id,
					folio_number, 
					COALESCE(entity_name, '')
				FROM investment.masterfolio
				WHERE folio_id = $1
			`, *dr.FolioID).Scan(&folioID, &folioNumber, &entityName)
			if err != nil {
				log.Printf("[ERROR DIVIDEND] Failed to fetch folio %s: %v", *dr.FolioID, err)
				return fmt.Errorf("failed to fetch folio %s: %w", *dr.FolioID, err)
			}

			log.Printf("[DEBUG DIVIDEND] Found folio: ID=%s, Number=%s, Entity=%s", folioID, folioNumber, entityName)
		} else {
			// No folio specified - find one linked to this scheme
			log.Printf("[DEBUG DIVIDEND] No folio_id in dividend, searching for scheme %s", dr.SchemeID)
			err := tx.QueryRow(ctx, `
				SELECT 
					f.folio_id,
					f.folio_number, 
					COALESCE(f.entity_name, '')
				FROM investment.folioschememapping fsm
				JOIN investment.masterfolio f ON f.folio_id = fsm.folio_id
				WHERE fsm.scheme_id = $1
				  AND COALESCE(fsm.status, 'Active') = 'Active'
				  AND COALESCE(f.is_deleted, false) = false
				ORDER BY f.folio_id
				LIMIT 1
			`, dr.SchemeID).Scan(&folioID, &folioNumber, &entityName)
			if err != nil {
				log.Printf("[ERROR DIVIDEND] No active folio found for scheme %s: %v", dr.SchemeID, err)
				return fmt.Errorf("no active folio found for scheme %s: %w", dr.SchemeID, err)
			}

			log.Printf("[DEBUG DIVIDEND] Found folio via scheme mapping: ID=%s, Number=%s, Entity=%s", folioID, folioNumber, entityName)
		}

		// Try to find linked demat account for this folio
		// Check if there's a default demat linked to this entity that we can use
		var tempDematAcc, tempDematID string
		err = tx.QueryRow(ctx, `
			SELECT demat_account_number, demat_id
			FROM investment.masterdemataccount
			WHERE entity_name = $1
			  AND COALESCE(is_deleted, false) = false
			  AND COALESCE(status, 'Active') = 'Active'
			ORDER BY created_at DESC
			LIMIT 1
		`, entityName).Scan(&tempDematAcc, &tempDematID)
		if err == nil {
			dematAccNumber = &tempDematAcc
			dematID = &tempDematID
			log.Printf("[DEBUG DIVIDEND] Found linked demat: AccNumber=%s, DematID=%s", *dematAccNumber, *dematID)
		} else {
			log.Printf("[DEBUG DIVIDEND] No demat account found for entity %s (this is OK, will be NULL)", entityName)
		}

		// Get scheme internal code
		var schemeInternalCode *string
		err = tx.QueryRow(ctx, `
			SELECT internal_scheme_code 
			FROM investment.masterscheme 
			WHERE scheme_id = $1
		`, dr.SchemeID).Scan(&schemeInternalCode)
		if err != nil {
			log.Printf("[WARN DIVIDEND] Could not fetch scheme internal code for %s: %v", dr.SchemeID, err)
		}

		// Parse payment date
		paymentDate := dr.PaymentDate
		if paymentDate == "" {
			paymentDate = time.Now().Format(constants.DateFormat)
		}

		// Generate a unique batch_id for this dividend reinvestment transaction
		var batchID string
		err = tx.QueryRow(ctx, `SELECT gen_random_uuid()::text`).Scan(&batchID)
		if err != nil {
			log.Printf("[ERROR DIVIDEND] Failed to generate batch_id for dividend %s: %v", dr.DividendID, err)
			return fmt.Errorf("failed to generate batch_id: %w", err)
		}

		log.Printf("[DEBUG DIVIDEND] Inserting transaction: Date=%s, Type=PURCHASE, SchemeCode=%v, FolioNum=%s, FolioID=%s, DematAcc=%v, DematID=%v, SchemeID=%s, Units=%.6f, NAV=%.4f, Amount=%.2f, Entity=%s, BatchID=%s",
			paymentDate, schemeInternalCode, folioNumber, folioID, dematAccNumber, dematID, dr.SchemeID, dr.ReinvestUnits, dr.ReinvestNAV, dr.DividendAmount, entityName, batchID)

		// Insert BUY transaction for dividend reinvestment
		// Note: demat fields populated if entity has an active demat account linked
		_, err = tx.Exec(ctx, `
			INSERT INTO investment.onboard_transaction (
				transaction_date,
				transaction_type,
				scheme_internal_code,
				folio_number,
				folio_id,
				demat_acc_number,
				demat_id,
				scheme_id,
				units,
				nav,
				amount,
				entity_name,
				batch_id,
				created_at
			) VALUES (
				$1::date,
				'PURCHASE',
				$2,
				$3,
				$4,
				$5,
				$6,
				$7,
				$8,
				$9,
				$10,
				$11,
				$12::uuid,
				NOW()
			)
		`, paymentDate, schemeInternalCode, folioNumber, folioID, dematAccNumber, dematID,
			dr.SchemeID, dr.ReinvestUnits, dr.ReinvestNAV, dr.DividendAmount, entityName, batchID)

		if err != nil {
			log.Printf("[ERROR DIVIDEND] Failed to create onboard_transaction for dividend %s: %v", dr.DividendID, err)
			return fmt.Errorf("failed to create reinvestment transaction for dividend %s: %w", dr.DividendID, err)
		}

		log.Printf("[DEBUG DIVIDEND] Successfully created onboard_transaction for dividend %s", dr.DividendID)
	}

	log.Printf("[DEBUG DIVIDEND] ProcessDividendReinvestment completed successfully for activity %s", activityID)
	return nil
}

// RefreshPortfolioAfterCorporateAction recalculates portfolio_snapshot after transaction changes
// This is critical because onboard_transaction is the source of truth
// NOTE: This is a simplified version that rebuilds affected schemes only
func RefreshPortfolioAfterCorporateAction(ctx context.Context, tx DBExecutor, affectedSchemeIDs []string, entityName string) error {
	if len(affectedSchemeIDs) == 0 {
		return nil
	}

	// Capture existing batch_id before deletion (to preserve batch tracking)
	var existingBatchID string
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(batch_id::text, gen_random_uuid()::text)
		FROM investment.portfolio_snapshot
		WHERE scheme_id = ANY($1)
		  AND ($2::text IS NULL OR entity_name = $2)
		LIMIT 1
	`, affectedSchemeIDs, nullIfEmptyString(entityName)).Scan(&existingBatchID)

	// If no existing batch_id found, generate a new one
	if err != nil || existingBatchID == "" {
		err = tx.QueryRow(ctx, `SELECT gen_random_uuid()::text`).Scan(&existingBatchID)
		if err != nil {
			return fmt.Errorf("failed to generate batch_id: %w", err)
		}
	}

	// Delete existing portfolio snapshots for affected schemes
	_, err = tx.Exec(ctx, `
		DELETE FROM investment.portfolio_snapshot
		WHERE scheme_id = ANY($1)
		  AND ($2::text IS NULL OR entity_name = $2)
	`, affectedSchemeIDs, nullIfEmptyString(entityName))
	if err != nil {
		return fmt.Errorf("failed to delete old portfolio snapshots: %w", err)
	}

	// Rebuild portfolio snapshots from onboard_transaction using EXACT same logic as RefreshPortfolioSnapshots
	_, err = tx.Exec(ctx, `
		WITH scheme_resolved AS (
			SELECT
				ot.transaction_date,
				ot.transaction_type,
				ot.amount,
				ot.units,
				ot.nav,
				ot.batch_id,
				COALESCE(mf.entity_name, md.entity_name, ot.entity_name) AS entity_name,
				ot.folio_number,
				ot.demat_acc_number,
				ot.folio_id,
				ot.demat_id,
				COALESCE(ot.scheme_id, fsm.scheme_id, ms2.scheme_id, ms.scheme_id) AS scheme_id,
				COALESCE(ms2.scheme_name, ms.scheme_name) AS scheme_name,
				COALESCE(ms2.isin, ms.isin) AS isin
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.masterfolio mf ON mf.folio_id = ot.folio_id
			LEFT JOIN investment.masterdemataccount md ON md.demat_id = ot.demat_id
			LEFT JOIN investment.folioschememapping fsm ON fsm.folio_id = ot.folio_id
			LEFT JOIN investment.masterscheme ms ON ms.scheme_id = fsm.scheme_id
			LEFT JOIN investment.masterscheme ms2 ON (ms2.scheme_id::text = ot.scheme_id OR ms2.internal_scheme_code = ot.scheme_internal_code)
			WHERE ($2::text IS NULL OR COALESCE(mf.entity_name, md.entity_name, ot.entity_name) = $2)
		),
		transaction_summary AS (
			SELECT
				entity_name,
				folio_number,
				demat_acc_number,
				folio_id,
				demat_id,
				scheme_id,
				scheme_name,
				isin,
				SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription','bonus') THEN units
						 WHEN LOWER(transaction_type) IN ('sell','redemption') THEN -units
						 ELSE units END) AS total_units,
				SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN amount ELSE 0 END) AS total_invested_amount,
				CASE WHEN SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN units ELSE 0 END) = 0 THEN 0
					 ELSE SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN nav * units ELSE 0 END) /
						  SUM(CASE WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN units ELSE 0 END)
				END AS avg_nav
			FROM scheme_resolved
			WHERE scheme_id = ANY($1)
			GROUP BY entity_name, folio_number, demat_acc_number, folio_id, demat_id, scheme_id, scheme_name, isin
		),
		latest_nav AS (
			SELECT DISTINCT ON (scheme_name)
				scheme_name,
				isin_div_payout_growth as isin,
				nav_value,
				nav_date
			FROM investment.amfi_nav_staging
			ORDER BY scheme_name, nav_date DESC
		)
		INSERT INTO investment.portfolio_snapshot (
			batch_id, entity_name, folio_number, demat_acc_number, folio_id, demat_id,
			scheme_id, scheme_name, isin, total_units, avg_nav, current_nav, current_value,
			total_invested_amount, gain_loss, gain_losss_percent, created_at
		)
		SELECT
			$3::uuid,
			ts.entity_name,
			ts.folio_number,
			ts.demat_acc_number,
			ts.folio_id,
			ts.demat_id,
			ts.scheme_id,
			ts.scheme_name,
			ts.isin,
			ts.total_units,
			ts.avg_nav,
			COALESCE(ln.nav_value, 0) AS current_nav,
			ts.total_units * COALESCE(ln.nav_value, 0) AS current_value,
			ts.total_invested_amount,
			(ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount AS gain_loss,
			CASE WHEN ts.total_invested_amount = 0 THEN 0 ELSE (((ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount) / ts.total_invested_amount) * 100 END AS gain_losss_percent,
			NOW()
		FROM transaction_summary ts
		LEFT JOIN latest_nav ln ON (ln.scheme_name = ts.scheme_name OR ln.isin = ts.isin)
		WHERE ts.total_units > 0
	`, affectedSchemeIDs, nullIfEmptyString(entityName), existingBatchID)

	if err != nil {
		return fmt.Errorf("failed to rebuild portfolio snapshots: %w", err)
	}

	return nil
}
