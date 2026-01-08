package accountingworkbench

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Snapshot represents the complete state before/after a corporate action
type Snapshot struct {
	Timestamp          time.Time                 `json:"timestamp"`
	EffectiveDate      string                    `json:"effective_date"`
	ActionType         string                    `json:"action_type"`
	AffectedSchemes    []string                  `json:"affected_schemes"`
	AffectedEntities   []string                  `json:"affected_entities"`
	MasterScheme       map[string]SchemeSnapshot `json:"masterscheme"`
	PortfolioSnapshot  []PortfolioSnapshotRecord `json:"portfolio_snapshot"`
	OnboardTransaction []TransactionSnapshot     `json:"onboard_transaction"`
	Changes            map[string]interface{}    `json:"changes,omitempty"`
}

type SchemeSnapshot struct {
	SchemeID     string  `json:"scheme_id"`
	SchemeName   string  `json:"scheme_name"`
	ISIN         string  `json:"isin,omitempty"`
	InternalCode string  `json:"internal_code,omitempty"`
	NAV          float64 `json:"nav,omitempty"`
	NAVDate      string  `json:"nav_date,omitempty"`
}

type PortfolioSnapshotRecord struct {
	EntityName     string  `json:"entity_name"`
	FolioNumber    string  `json:"folio_number,omitempty"`
	FolioID        string  `json:"folio_id,omitempty"`
	DematAccNumber string  `json:"demat_acc_number,omitempty"`
	DematID        string  `json:"demat_id,omitempty"`
	SchemeID       string  `json:"scheme_id"`
	TotalUnits     float64 `json:"total_units"`
	AvgNAV         float64 `json:"avg_nav"`
	TotalCostBasis float64 `json:"total_cost_basis,omitempty"`
}

type TransactionSnapshot struct {
	ID                 int64   `json:"id"`
	TransactionDate    string  `json:"transaction_date"`
	TransactionType    string  `json:"transaction_type"`
	FolioNumber        string  `json:"folio_number,omitempty"`
	FolioID            string  `json:"folio_id,omitempty"`
	DematAccNumber     string  `json:"demat_acc_number,omitempty"`
	DematID            string  `json:"demat_id,omitempty"`
	SchemeID           string  `json:"scheme_id,omitempty"`
	SchemeInternalCode string  `json:"scheme_internal_code,omitempty"`
	Units              float64 `json:"units"`
	NAV                float64 `json:"nav"`
	Amount             float64 `json:"amount"`
}

// CaptureSnapshotBeforeCorporateAction captures the complete state before processing corporate action
func CaptureSnapshotBeforeCorporateAction(ctx context.Context, tx DBExecutor, activityID string, actionType string, affectedSchemeIDs []string) (*Snapshot, error) {
	snapshot := &Snapshot{
		Timestamp:          time.Now(),
		ActionType:         actionType,
		AffectedSchemes:    affectedSchemeIDs,
		MasterScheme:       make(map[string]SchemeSnapshot),
		PortfolioSnapshot:  []PortfolioSnapshotRecord{},
		OnboardTransaction: []TransactionSnapshot{},
		Changes:            make(map[string]interface{}),
	}

	// Get effective_date from accounting_activity
	var effectiveDate string
	err := tx.QueryRow(ctx, `
		SELECT TO_CHAR(effective_date, 'YYYY-MM-DD')
		FROM investment.accounting_activity
		WHERE activity_id = $1
	`, activityID).Scan(&effectiveDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get effective_date: %w", err)
	}
	snapshot.EffectiveDate = effectiveDate

	// 1. Capture masterscheme data for affected schemes
	if len(affectedSchemeIDs) > 0 {
		rows, err := tx.Query(ctx, `
			SELECT scheme_id, scheme_name, COALESCE(isin, ''), COALESCE(internal_scheme_code, '')
			FROM investment.masterscheme
			WHERE scheme_id = ANY($1)
		`, affectedSchemeIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to capture masterscheme: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var ss SchemeSnapshot
			if err := rows.Scan(&ss.SchemeID, &ss.SchemeName, &ss.ISIN, &ss.InternalCode); err != nil {
				continue
			}
			snapshot.MasterScheme[ss.SchemeID] = ss
		}
	}

	// 2. Capture portfolio_snapshot for affected schemes
	if len(affectedSchemeIDs) > 0 {
		rows, err := tx.Query(ctx, `
			SELECT 
				COALESCE(entity_name, '') as entity_name,
				COALESCE(folio_number, '') as folio_number,
				COALESCE(folio_id, '') as folio_id,
				COALESCE(demat_acc_number, '') as demat_acc_number,
				COALESCE(demat_id, '') as demat_id,
				scheme_id,
				COALESCE(total_units, 0) as total_units,
				COALESCE(avg_nav, 0) as avg_nav,
				COALESCE(total_invested_amount, 0) as total_invested_amount
			FROM investment.portfolio_snapshot
			WHERE scheme_id = ANY($1)
			  AND total_units > 0
		`, affectedSchemeIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to capture portfolio_snapshot: %w", err)
		}
		defer rows.Close()

		entitySet := make(map[string]bool)
		for rows.Next() {
			var ps PortfolioSnapshotRecord
			if err := rows.Scan(&ps.EntityName, &ps.FolioNumber, &ps.FolioID, &ps.DematAccNumber,
				&ps.DematID, &ps.SchemeID, &ps.TotalUnits, &ps.AvgNAV, &ps.TotalCostBasis); err != nil {
				continue
			}
			snapshot.PortfolioSnapshot = append(snapshot.PortfolioSnapshot, ps)
			if ps.EntityName != "" {
				entitySet[ps.EntityName] = true
			}
		}

		// Extract affected entities
		for entity := range entitySet {
			snapshot.AffectedEntities = append(snapshot.AffectedEntities, entity)
		}
	}

	// 3. Capture onboard_transaction for affected schemes
	// This is CRITICAL - source of truth that gets modified by corporate actions
	if len(affectedSchemeIDs) > 0 {
		rows, err := tx.Query(ctx, `
			SELECT 
				id,
				TO_CHAR(transaction_date, 'YYYY-MM-DD') as transaction_date,
				COALESCE(transaction_type, '') as transaction_type,
				COALESCE(folio_number, '') as folio_number,
				COALESCE(folio_id, '') as folio_id,
				COALESCE(demat_acc_number, '') as demat_acc_number,
				COALESCE(demat_id, '') as demat_id,
				COALESCE(scheme_id, '') as scheme_id,
				COALESCE(scheme_internal_code, '') as scheme_internal_code,
				COALESCE(units, 0) as units,
				COALESCE(nav, 0) as nav,
				COALESCE(amount, 0) as amount
			FROM investment.onboard_transaction
			WHERE scheme_id = ANY($1)
			ORDER BY transaction_date, id
		`, affectedSchemeIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to capture onboard_transaction: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var ts TransactionSnapshot
			if err := rows.Scan(&ts.ID, &ts.TransactionDate, &ts.TransactionType, &ts.FolioNumber,
				&ts.FolioID, &ts.DematAccNumber, &ts.DematID, &ts.SchemeID, &ts.SchemeInternalCode,
				&ts.Units, &ts.NAV, &ts.Amount); err != nil {
				continue
			}
			snapshot.OnboardTransaction = append(snapshot.OnboardTransaction, ts)
		}
	}

	return snapshot, nil
}

// SaveSnapshotToAudit saves the snapshot JSONB to the audit table
func SaveSnapshotToAudit(ctx context.Context, tx DBExecutor, activityID string, snapshot *Snapshot, isBefore bool) error {
	snapshotJSON, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	column := "snapshot_before"
	if !isBefore {
		column = "snapshot_after"
	}

	// Update the most recent audit record for this activity (regardless of actiontype)
	// This works because corporate action processing happens AFTER approval status is set
	query := fmt.Sprintf(`
		UPDATE investment.auditactionaccountingactivity
		SET %s = $1
		WHERE activity_id = $2
		  AND processing_status = 'APPROVED'
		  AND requested_at = (
			SELECT MAX(requested_at) 
			FROM investment.auditactionaccountingactivity 
			WHERE activity_id = $2
		  )
	`, column)

	result, err := tx.Exec(ctx, query, snapshotJSON, activityID)
	if err != nil {
		return fmt.Errorf("failed to save snapshot to audit: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("no audit record found to update snapshot for activity_id=%s (processing_status must be APPROVED)", activityID)
	}

	return nil
}

// GetDataAsOf retrieves historical data based on as_of_date
// Returns data from snapshot_before if effective_date > as_of_date, otherwise current master data
func GetDataAsOf(ctx context.Context, executor DBExecutor, schemeID string, asOfDate string) (*SchemeSnapshot, *[]PortfolioSnapshotRecord, error) {
	// Find the most recent APPROVED activity with effective_date > as_of_date
	query := `
		SELECT aud.snapshot_before
		FROM investment.auditactionaccountingactivity aud
		JOIN investment.accounting_activity a ON a.activity_id = aud.activity_id
		WHERE aud.processing_status = 'APPROVED'
		  AND a.effective_date > $1::date
		  AND aud.snapshot_before ? 'masterscheme'
		  AND aud.snapshot_before->'masterscheme' ? $2
		ORDER BY a.effective_date ASC, aud.requested_at ASC
		LIMIT 1
	`

	var snapshotJSON []byte
	err := executor.QueryRow(ctx, query, asOfDate, schemeID).Scan(&snapshotJSON)

	if err != nil {
		// No snapshot found - use current master data
		return getCurrentMasterData(ctx, executor, schemeID)
	}

	// Parse snapshot
	var snapshot Snapshot
	if err := json.Unmarshal(snapshotJSON, &snapshot); err != nil {
		return nil, nil, fmt.Errorf("failed to parse snapshot: %w", err)
	}

	// Extract scheme data
	schemeData, ok := snapshot.MasterScheme[schemeID]
	if !ok {
		return getCurrentMasterData(ctx, executor, schemeID)
	}

	// Extract portfolio data for this scheme
	var portfolioData []PortfolioSnapshotRecord
	for _, ps := range snapshot.PortfolioSnapshot {
		if ps.SchemeID == schemeID {
			portfolioData = append(portfolioData, ps)
		}
	}

	return &schemeData, &portfolioData, nil
}

// getCurrentMasterData fetches current (live) data from master tables
func getCurrentMasterData(ctx context.Context, executor DBExecutor, schemeID string) (*SchemeSnapshot, *[]PortfolioSnapshotRecord, error) {
	var ss SchemeSnapshot
	err := executor.QueryRow(ctx, `
		SELECT scheme_id, scheme_name, COALESCE(isin, ''), COALESCE(internal_scheme_code, '')
		FROM investment.masterscheme
		WHERE scheme_id = $1
	`, schemeID).Scan(&ss.SchemeID, &ss.SchemeName, &ss.ISIN, &ss.InternalCode)

	if err != nil {
		return nil, nil, fmt.Errorf("scheme not found: %w", err)
	}

	// Get portfolio data
	rows, err := executor.Query(ctx, `
		SELECT 
			COALESCE(entity_name, ''),
			COALESCE(folio_number, ''),
			COALESCE(folio_id, ''),
			COALESCE(demat_acc_number, ''),
			COALESCE(demat_id, ''),
			scheme_id,
			COALESCE(total_units, 0),
			COALESCE(avg_nav, 0),
			COALESCE(total_cost_basis, 0)
		FROM investment.portfolio_snapshot
		WHERE scheme_id = $1
		  AND total_units > 0
	`, schemeID)
	if err != nil {
		return &ss, &[]PortfolioSnapshotRecord{}, nil
	}
	defer rows.Close()

	var portfolioData []PortfolioSnapshotRecord
	for rows.Next() {
		var ps PortfolioSnapshotRecord
		if err := rows.Scan(&ps.EntityName, &ps.FolioNumber, &ps.FolioID, &ps.DematAccNumber,
			&ps.DematID, &ps.SchemeID, &ps.TotalUnits, &ps.AvgNAV, &ps.TotalCostBasis); err != nil {
			continue
		}
		portfolioData = append(portfolioData, ps)
	}

	return &ss, &portfolioData, nil
}
