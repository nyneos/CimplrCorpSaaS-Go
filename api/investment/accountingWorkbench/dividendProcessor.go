package accountingworkbench

import (
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/portfolio"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"CimplrCorpSaas/internal/logger"
)

// ProcessDividendReinvestment creates buy transaction when dividend is reinvested
// Called during approval for REINVEST type dividends
func ProcessDividendReinvestment(ctx context.Context, tx DBExecutor, activityID string) error {
	logger.LogInfo("[DEBUG DIVIDEND] ProcessDividendReinvestment called for activity: %s", activityID)

	// First, check ALL dividends for this activity (debugging)
	debugRows, err := tx.Query(ctx, `
		SELECT dividend_id, transaction_type, dividend_amount, reinvest_nav, reinvest_units
		FROM investment.accounting_dividend
		WHERE activity_id = $1
	`, activityID)
	if err != nil {
		logger.LogError("[DEBUG DIVIDEND] debug query failed: %v", err)
	} else {
		for debugRows.Next() {
			var did, ttype string
			var amt, nav, units float64
			if err := debugRows.Scan(&did, &ttype, &amt, &nav, &units); err == nil {
				logger.LogInfo("[DEBUG DIVIDEND] Found dividend: %s | Type: %s | Amount: %.2f | Nav: %.4f | Units: %.6f",
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

	logger.LogInfo("[DEBUG DIVIDEND] Found %d reinvest dividends with units > 0", len(reinvests))

	if len(reinvests) == 0 {
		logger.LogInfo("[DEBUG DIVIDEND] No reinvestments to process (all dividends have reinvest_units = 0 or are PAYOUT type)")
		return nil
	}

	// For each reinvestment, create a BUY transaction in onboard_transaction
	for _, dr := range reinvests {
		logger.LogInfo("[DEBUG DIVIDEND] Processing reinvest: %s | Scheme: %s | FolioID from dividend: %v | Units: %.6f | NAV: %.4f | Amount: %.2f",
			dr.DividendID, dr.SchemeID, dr.FolioID, dr.ReinvestUnits, dr.ReinvestNAV, dr.DividendAmount)

		// Get folio details if folio_id is provided
		var folioNumber, entityName, folioID string
		var dematAccNumber, dematID *string

		if dr.FolioID != nil && *dr.FolioID != "" {
			// Folio specified in dividend - use it
			logger.LogInfo("[DEBUG DIVIDEND] Fetching folio details for folio_id: %s", *dr.FolioID)
			err := tx.QueryRow(ctx, `
				SELECT 
					folio_id,
					folio_number, 
					COALESCE(entity_name, '')
				FROM investment.masterfolio
				WHERE folio_id = $1
			`, *dr.FolioID).Scan(&folioID, &folioNumber, &entityName)
			if err != nil {
				logger.LogError("[ERROR DIVIDEND] Failed to fetch folio %s: %v", *dr.FolioID, err)
				return fmt.Errorf("failed to fetch folio %s: %w", *dr.FolioID, err)
			}

			logger.LogInfo("[DEBUG DIVIDEND] Found folio: ID=%s, Number=%s, Entity=%s", folioID, folioNumber, entityName)
		} else {
			// No folio specified - find one linked to this scheme
			logger.LogInfo("[DEBUG DIVIDEND] No folio_id in dividend, searching for scheme %s", dr.SchemeID)
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
				logger.LogError("[ERROR DIVIDEND] No active folio found for scheme %s: %v", dr.SchemeID, err)
				return fmt.Errorf("no active folio found for scheme %s: %w", dr.SchemeID, err)
			}

			logger.LogInfo("[DEBUG DIVIDEND] Found folio via scheme mapping: ID=%s, Number=%s, Entity=%s", folioID, folioNumber, entityName)
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
			logger.LogInfo("[DEBUG DIVIDEND] Found linked demat: AccNumber=%s, DematID=%s", *dematAccNumber, *dematID)
		} else {
			logger.LogInfo("[DEBUG DIVIDEND] No demat account found for entity %s (this is OK, will be NULL)", entityName)
		}

		// Get scheme internal code
		var schemeInternalCode *string
		err = tx.QueryRow(ctx, `
			SELECT internal_scheme_code 
			FROM investment.masterscheme 
			WHERE scheme_id = $1
		`, dr.SchemeID).Scan(&schemeInternalCode)
		if err != nil {
			logger.LogError("[WARN DIVIDEND] Could not fetch scheme internal code for %s: %v", dr.SchemeID, err)
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
			logger.LogError("[ERROR DIVIDEND] Failed to generate batch_id for dividend %s: %v", dr.DividendID, err)
			return fmt.Errorf("failed to generate batch_id: %w", err)
		}

		logger.LogInfo("[DEBUG DIVIDEND] Inserting transaction: Date=%s, Type=PURCHASE, SchemeCode=%v, FolioNum=%s, FolioID=%s, DematAcc=%v, DematID=%v, SchemeID=%s, Units=%.6f, NAV=%.4f, Amount=%.2f, Entity=%s, BatchID=%s",
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
			logger.LogError("[ERROR DIVIDEND] Failed to create onboard_transaction for dividend %s: %v", dr.DividendID, err)
			return fmt.Errorf("failed to create reinvestment transaction for dividend %s: %w", dr.DividendID, err)
		}

		logger.LogInfo("[DEBUG DIVIDEND] Successfully created onboard_transaction for dividend %s", dr.DividendID)
	}

	logger.LogInfo("[DEBUG DIVIDEND] ProcessDividendReinvestment completed successfully for activity %s", activityID)
	return nil
}

// RefreshPortfolioAfterCorporateAction rebuilds portfolio_snapshot for entities
// holding the affected schemes, using the shared portfolio refresh SQL.
func RefreshPortfolioAfterCorporateAction(ctx context.Context, tx DBExecutor, affectedSchemeIDs []string, entityName string) error {
	if len(affectedSchemeIDs) == 0 {
		return nil
	}

	entityNames, err := resolveEntitiesForPortfolioRefresh(ctx, tx, affectedSchemeIDs, entityName)
	if err != nil {
		return err
	}

	batchID := uuid.New().String()

	if len(entityNames) > 0 {
		if _, err = tx.Exec(ctx, `DELETE FROM investment.portfolio_snapshot WHERE entity_name = ANY($1::text[])`, entityNames); err != nil {
			return fmt.Errorf("delete snapshots: %w", err)
		}
		if _, err = tx.Exec(ctx, portfolio.PortfolioSnapshotInsertSQL, entityNames, batchID); err != nil {
			return fmt.Errorf("rebuild portfolio snapshots: %w", err)
		}
		return nil
	}

	if _, err = tx.Exec(ctx, `DELETE FROM investment.portfolio_snapshot`); err != nil {
		return fmt.Errorf("delete snapshots: %w", err)
	}
	if _, err = tx.Exec(ctx, portfolio.PortfolioSnapshotInsertSQL, nil, batchID); err != nil {
		return fmt.Errorf("rebuild portfolio snapshots: %w", err)
	}
	return nil
}

func resolveEntitiesForPortfolioRefresh(ctx context.Context, tx DBExecutor, affectedSchemeIDs []string, entityName string) ([]string, error) {
	if trimmed := strings.TrimSpace(entityName); trimmed != "" {
		return []string{trimmed}, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT DISTINCT TRIM(entity_name)
		FROM investment.portfolio_snapshot
		WHERE scheme_id = ANY($1)
		  AND NULLIF(TRIM(entity_name), '') IS NOT NULL
		UNION
		SELECT DISTINCT TRIM(mf.entity_name)
		FROM investment.masterfolio mf
		JOIN investment.folioschememapping fsm ON fsm.folio_id = mf.folio_id
		WHERE fsm.scheme_id::text = ANY($1)
		  AND NULLIF(TRIM(mf.entity_name), '') IS NOT NULL
	`, affectedSchemeIDs)
	if err != nil {
		return nil, fmt.Errorf("resolve entities for refresh: %w", err)
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		key := strings.ToUpper(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		names = append(names, name)
	}
	return names, rows.Err()
}
