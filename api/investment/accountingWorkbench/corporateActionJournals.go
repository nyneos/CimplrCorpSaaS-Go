package accountingworkbench

import (
	"context"
	"fmt"
	"time"
)

// CARecord represents a corporate action record
type CARecord struct {
	CaID             string
	ActionType       string
	SourceSchemeID   string
	TargetSchemeID   *string
	RatioNew         float64
	RatioOld         float64
	ConversionRatio  float64
	EffectiveDate    time.Time
	AccountingPeriod string
}

// GenerateCorporateActionJournals creates journal entries for corporate actions
// Based on Journal Entry Policy Matrix
func GenerateCorporateActionJournals(ctx context.Context, tx DBExecutor, activityID string) error {
	// Fetch corporate action details
	rows, err := tx.Query(ctx, `
		SELECT 
			ca.ca_id,
			ca.action_type,
			ca.source_scheme_id,
			ca.target_scheme_id,
			COALESCE(ca.ratio_new, 0) as ratio_new,
			COALESCE(ca.ratio_old, 0) as ratio_old,
			COALESCE(ca.conversion_ratio, 0) as conversion_ratio,
			a.effective_date,
			a.accounting_period
		FROM investment.accounting_corporate_action ca
		JOIN investment.accounting_activity a ON a.activity_id = ca.activity_id
		WHERE ca.activity_id = $1 
		  AND COALESCE(ca.is_deleted, false) = false
	`, activityID)
	if err != nil {
		return fmt.Errorf("failed to fetch corporate action details: %w", err)
	}
	defer rows.Close()

	var caRecords []CARecord
	for rows.Next() {
		var rec CARecord
		if err := rows.Scan(&rec.CaID, &rec.ActionType, &rec.SourceSchemeID, &rec.TargetSchemeID,
			&rec.RatioNew, &rec.RatioOld, &rec.ConversionRatio, &rec.EffectiveDate, &rec.AccountingPeriod); err != nil {
			continue
		}
		caRecords = append(caRecords, rec)
	}

	// Generate journal entries for each corporate action
	for _, rec := range caRecords {
		switch rec.ActionType {
		case "SCHEME_MERGER":
			if err := generateMergerJournal(ctx, tx, activityID, rec); err != nil {
				return fmt.Errorf("failed to generate merger journal: %w", err)
			}

		case "SCHEME_NAME_CHANGE":
			// Name change: No journal entry required (memo only)
			// Just log for audit trail
			continue

		case "SPLIT":
			if err := generateSplitJournal(ctx, tx, activityID, rec); err != nil {
				return fmt.Errorf("failed to generate split journal: %w", err)
			}

		case "BONUS":
			if err := generateBonusJournal(ctx, tx, activityID, rec); err != nil {
				return fmt.Errorf("failed to generate bonus journal: %w", err)
			}
		}
	}

	return nil
}

// generateMergerJournal creates journal entry for scheme merger
// Dr: Investment - Scheme B (target)
// Cr: Investment - Scheme A (source)
// Amount: Cost Basis of A
func generateMergerJournal(ctx context.Context, tx DBExecutor, activityID string, rec CARecord) error {
	if rec.TargetSchemeID == nil || *rec.TargetSchemeID == "" {
		return fmt.Errorf("target scheme required for merger")
	}

	// Get source scheme name
	var sourceSchemeName string
	err := tx.QueryRow(ctx, `
		SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1
	`, rec.SourceSchemeID).Scan(&sourceSchemeName)
	if err != nil {
		return fmt.Errorf("source scheme not found: %w", err)
	}

	// Get target scheme name
	var targetSchemeName string
	err = tx.QueryRow(ctx, `
		SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1
	`, *rec.TargetSchemeID).Scan(&targetSchemeName)
	if err != nil {
		return fmt.Errorf("target scheme not found: %w", err)
	}

	// Calculate total cost basis being transferred from portfolio
	var totalCostBasis float64
	var entityName string
	err = tx.QueryRow(ctx, `
		SELECT 
			COALESCE(SUM(total_invested_amount), 0),
			COALESCE(MAX(entity_name), '') as entity_name
		FROM investment.portfolio_snapshot
		WHERE scheme_id = $1
		GROUP BY entity_name
		LIMIT 1
	`, rec.SourceSchemeID).Scan(&totalCostBasis, &entityName)

	// If no portfolio exists, check onboard_transaction
	if err != nil || totalCostBasis == 0 {
		err = tx.QueryRow(ctx, `
			SELECT 
				COALESCE(SUM(CASE 
					WHEN LOWER(transaction_type) IN ('purchase','buy','subscription') THEN amount
					WHEN LOWER(transaction_type) IN ('sell','redemption') THEN -amount
					ELSE 0
				END), 0),
				COALESCE(MAX(entity_name), '') as entity_name
			FROM investment.onboard_transaction
			WHERE scheme_id = $1
		`, rec.SourceSchemeID).Scan(&totalCostBasis, &entityName)
	}

	// If still no data, skip journal entry (no holdings to merge)
	if totalCostBasis == 0 {
		return nil
	}

	// Create journal entry
	je := &JournalEntry{
		ActivityID:       activityID,
		EntityID:         entityName,
		EntityName:       entityName,
		EntryDate:        rec.EffectiveDate,
		AccountingPeriod: rec.AccountingPeriod,
		EntryType:        "CORPORATE_ACTION",
		Description:      fmt.Sprintf("Scheme Merger: %s → %s", sourceSchemeName, targetSchemeName),
		Lines:            []JournalEntryLine{},
	}

	// Debit: Investment in Target Scheme (increase)
	je.Lines = append(je.Lines, JournalEntryLine{
		LineNumber:    1,
		AccountNumber: fmt.Sprintf("INV-%s", *rec.TargetSchemeID),
		AccountName:   fmt.Sprintf("Investment - %s", targetSchemeName),
		AccountType:   "ASSET",
		DebitAmount:   totalCostBasis,
		CreditAmount:  0,
		SchemeID:      *rec.TargetSchemeID,
		Narration:     fmt.Sprintf("Merger: Units transferred from %s", sourceSchemeName),
	})

	// Credit: Investment in Source Scheme (decrease)
	je.Lines = append(je.Lines, JournalEntryLine{
		LineNumber:    2,
		AccountNumber: fmt.Sprintf("INV-%s", rec.SourceSchemeID),
		AccountName:   fmt.Sprintf("Investment - %s", sourceSchemeName),
		AccountType:   "ASSET",
		DebitAmount:   0,
		CreditAmount:  totalCostBasis,
		SchemeID:      rec.SourceSchemeID,
		Narration:     fmt.Sprintf("Merger: Units transferred to %s", targetSchemeName),
	})

	je.TotalDebit = totalCostBasis
	je.TotalCredit = totalCostBasis

	return SaveJournalEntry(ctx, tx, je)
}

// generateSplitJournal creates journal entry for stock split
// Split is NAV adjustment only - no P&L impact
// Dr: Investment Account (memo - old cost basis)
// Cr: Investment Account (memo - new cost basis)
// Amount: Same (no change in total investment value)
func generateSplitJournal(ctx context.Context, tx DBExecutor, activityID string, rec CARecord) error {
	// Get scheme name
	var schemeName string
	err := tx.QueryRow(ctx, `
		SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1
	`, rec.SourceSchemeID).Scan(&schemeName)
	if err != nil {
		return fmt.Errorf("scheme not found: %w", err)
	}

	// Calculate total investment value (remains unchanged after split)
	var totalInvestment float64
	var entityName string
	err = tx.QueryRow(ctx, `
		SELECT 
			COALESCE(SUM(total_invested_amount), 0),
			COALESCE(MAX(entity_name), '') as entity_name
		FROM investment.portfolio_snapshot
		WHERE scheme_id = $1
		GROUP BY entity_name
		LIMIT 1
	`, rec.SourceSchemeID).Scan(&totalInvestment, &entityName)

	// If no portfolio, skip (no holdings to split)
	if totalInvestment == 0 {
		return nil
	}

	// Create memo journal entry
	je := &JournalEntry{
		ActivityID:       activityID,
		EntityID:         entityName,
		EntityName:       entityName,
		EntryDate:        rec.EffectiveDate,
		AccountingPeriod: rec.AccountingPeriod,
		EntryType:        "CORPORATE_ACTION",
		Description:      fmt.Sprintf("Split %d:%d - %s (Memo Entry - No P&L Impact)", int(rec.RatioNew), int(rec.RatioOld), schemeName),
		Lines:            []JournalEntryLine{},
	}

	// Debit: Investment Account (reversal of old basis)
	je.Lines = append(je.Lines, JournalEntryLine{
		LineNumber:    1,
		AccountNumber: fmt.Sprintf("INV-%s", rec.SourceSchemeID),
		AccountName:   fmt.Sprintf("Investment - %s", schemeName),
		AccountType:   "ASSET",
		DebitAmount:   totalInvestment,
		CreditAmount:  0,
		SchemeID:      rec.SourceSchemeID,
		Narration:     fmt.Sprintf("Split %d:%d - Units multiplied by %.2f, NAV adjusted", int(rec.RatioNew), int(rec.RatioOld), rec.RatioNew/rec.RatioOld),
	})

	// Credit: Investment Account (new basis - same amount)
	je.Lines = append(je.Lines, JournalEntryLine{
		LineNumber:    2,
		AccountNumber: fmt.Sprintf("INV-%s", rec.SourceSchemeID),
		AccountName:   fmt.Sprintf("Investment - %s", schemeName),
		AccountType:   "ASSET",
		DebitAmount:   0,
		CreditAmount:  totalInvestment,
		SchemeID:      rec.SourceSchemeID,
		Narration:     fmt.Sprintf("Split adjustment - Total investment value unchanged: ₹%.2f", totalInvestment),
	})

	je.TotalDebit = totalInvestment
	je.TotalCredit = totalInvestment

	return SaveJournalEntry(ctx, tx, je)
}

// generateBonusJournal creates journal entry for bonus issue
// Bonus shares are received at zero cost
// Dr: Investment Account (bonus units at ₹0)
// Cr: Bonus Income / Capital Reserve (₹0)
func generateBonusJournal(ctx context.Context, tx DBExecutor, activityID string, rec CARecord) error {
	// Get scheme name
	var schemeName string
	err := tx.QueryRow(ctx, `
		SELECT scheme_name FROM investment.masterscheme WHERE scheme_id = $1
	`, rec.SourceSchemeID).Scan(&schemeName)
	if err != nil {
		return fmt.Errorf("scheme not found: %w", err)
	}

	// Calculate bonus units being issued
	var totalBonusUnits float64
	var entityName string
	err = tx.QueryRow(ctx, `
		SELECT 
			COALESCE(SUM(total_units), 0),
			COALESCE(MAX(entity_name), '') as entity_name
		FROM investment.portfolio_snapshot
		WHERE scheme_id = $1
		GROUP BY entity_name
		LIMIT 1
	`, rec.SourceSchemeID).Scan(&totalBonusUnits, &entityName)

	if totalBonusUnits == 0 {
		return nil // No holdings, no bonus
	}

	// Calculate bonus units: floor((existing units × ratio_new / ratio_old) × 1000) / 1000
	bonusUnits := float64(int((totalBonusUnits*rec.RatioNew/rec.RatioOld)*1000)) / 1000

	// Create memo journal entry (₹0 transaction)
	je := &JournalEntry{
		ActivityID:       activityID,
		EntityID:         entityName,
		EntityName:       entityName,
		EntryDate:        rec.EffectiveDate,
		AccountingPeriod: rec.AccountingPeriod,
		EntryType:        "CORPORATE_ACTION",
		Description:      fmt.Sprintf("Bonus Issue %d:%d - %s (Memo Entry)", int(rec.RatioNew), int(rec.RatioOld), schemeName),
		Lines:            []JournalEntryLine{},
	}

	// Use 0.01 for memo entry to keep journal balanced
	memoAmount := 0.01

	// Debit: Investment Account (bonus units received)
	je.Lines = append(je.Lines, JournalEntryLine{
		LineNumber:    1,
		AccountNumber: fmt.Sprintf("INV-%s", rec.SourceSchemeID),
		AccountName:   fmt.Sprintf("Investment - %s", schemeName),
		AccountType:   "ASSET",
		DebitAmount:   memoAmount,
		CreditAmount:  0,
		SchemeID:      rec.SourceSchemeID,
		Narration:     fmt.Sprintf("Bonus units issued: %.3f units at ₹0 cost", bonusUnits),
	})

	// Credit: Bonus Income / Capital Reserve
	je.Lines = append(je.Lines, JournalEntryLine{
		LineNumber:    2,
		AccountNumber: "INC-BONUS",
		AccountName:   "Bonus Income / Capital Reserve",
		AccountType:   "INCOME",
		DebitAmount:   0,
		CreditAmount:  memoAmount,
		SchemeID:      rec.SourceSchemeID,
		Narration:     fmt.Sprintf("Bonus issue %d:%d - %.3f units received", int(rec.RatioNew), int(rec.RatioOld), bonusUnits),
	})

	je.TotalDebit = memoAmount
	je.TotalCredit = memoAmount

	return SaveJournalEntry(ctx, tx, je)
}
