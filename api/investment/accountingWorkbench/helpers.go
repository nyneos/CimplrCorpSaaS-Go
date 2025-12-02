package accountingworkbench

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Global settings cache
var (
	globalSettings *SettingsCache
	settingsMux    sync.RWMutex
)

// SettingsCache holds frequently used settings
type SettingsCache struct {
	UnitsPrecision    int
	NAVPrecision      int
	CurrencyPrecision int
	RoundingMode      string
}

// LoadSettings fetches settings from the single-row accounting_setting table  
func LoadSettings(ctx context.Context, pool *pgxpool.Pool) (*SettingsCache, error) {
	query := `
		SELECT units_precision, nav_precision, currency_precision, rounding_mode
		FROM investment.accounting_setting 
		LIMIT 1
	`

	var cache SettingsCache
	err := pool.QueryRow(ctx, query).Scan(
		&cache.UnitsPrecision,
		&cache.NAVPrecision,
		&cache.CurrencyPrecision,
		&cache.RoundingMode,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}

	// Update global cache
	settingsMux.Lock()
	globalSettings = &cache
	settingsMux.Unlock()

	return &cache, nil
}

// GetCachedSettings returns the cached settings (or loads if not cached)
func GetCachedSettings() *SettingsCache {
	settingsMux.RLock()
	defer settingsMux.RUnlock()
	
	if globalSettings == nil {
		// Return default values if not loaded
		return &SettingsCache{
			UnitsPrecision:    3,
			NAVPrecision:      4,
			CurrencyPrecision: 2,
			RoundingMode:      "BANKERS",
		}
	}
	return globalSettings
}

// RoundUnits rounds units based on precision and rounding mode
func RoundUnits(value float64, precision int, roundingMode string) float64 {
	multiplier := math.Pow(10, float64(precision))
	mode := strings.ToUpper(roundingMode)

	switch mode {
	case "BANKERS":
		// Banker's rounding (round half to even)
		return math.Round(value*multiplier) / multiplier
	case "UP":
		return math.Ceil(value*multiplier) / multiplier
	case "DOWN":
		return math.Floor(value*multiplier) / multiplier
	case "FLOOR":
		return math.Floor(value*multiplier) / multiplier
	case "CEILING":
		return math.Ceil(value*multiplier) / multiplier
	default:
		// Default to banker's rounding
		return math.Round(value*multiplier) / multiplier
	}
}

// RoundAmount rounds monetary amounts
func RoundAmount(value float64, precision int) float64 {
	multiplier := math.Pow(10, float64(precision))
	return math.Round(value*multiplier) / multiplier
}

// Portfolio Update Structs
type PortfolioHolding struct {
	EntityID       string
	FolioID        string
	DematID        string
	SchemeID       string
	SchemeName     string
	Units          float64
	AvgCostPerUnit float64
	TotalCostBasis float64
}

// GetHoldingsByScheme fetches current holdings for a scheme
func GetHoldingsByScheme(ctx context.Context, pool *pgxpool.Pool, schemeID string, entityID string) ([]PortfolioHolding, error) {
	query := `
		SELECT 
			ps.entity_id,
			ps.folio_id,
			ps.demat_id,
			ps.scheme_id,
			ms.scheme_name,
			ps.units,
			ps.avg_cost_per_unit,
			ps.total_cost_basis
		FROM investment.portfolio_snapshot ps
		LEFT JOIN investment.masterscheme ms ON ms.scheme_id = ps.scheme_id
		WHERE ps.scheme_id = $1
	`
	args := []interface{}{schemeID}

	if entityID != "" {
		query += " AND ps.entity_id = $2"
		args = append(args, entityID)
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch holdings: %w", err)
	}
	defer rows.Close()

	holdings := []PortfolioHolding{}
	for rows.Next() {
		var h PortfolioHolding
		err := rows.Scan(
			&h.EntityID,
			&h.FolioID,
			&h.DematID,
			&h.SchemeID,
			&h.SchemeName,
			&h.Units,
			&h.AvgCostPerUnit,
			&h.TotalCostBasis,
		)
		if err != nil {
			log.Printf("Error scanning holding: %v", err)
			continue
		}
		holdings = append(holdings, h)
	}

	return holdings, nil
}

// UpdatePortfolioHolding updates or inserts a portfolio holding
func UpdatePortfolioHolding(ctx context.Context, tx interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (interface{}, error)
}, holding PortfolioHolding) error {

	query := `
		INSERT INTO investment.portfolio_snapshot 
		(entity_id, folio_id, demat_id, scheme_id, units, avg_cost_per_unit, total_cost_basis, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (entity_id, folio_id, demat_id, scheme_id)
		DO UPDATE SET
			units = EXCLUDED.units,
			avg_cost_per_unit = EXCLUDED.avg_cost_per_unit,
			total_cost_basis = EXCLUDED.total_cost_basis,
			updated_at = NOW()
	`

	_, err := tx.Exec(ctx, query,
		holding.EntityID,
		holding.FolioID,
		holding.DematID,
		holding.SchemeID,
		holding.Units,
		holding.AvgCostPerUnit,
		holding.TotalCostBasis,
	)

	if err != nil {
		return fmt.Errorf("failed to update portfolio holding: %w", err)
	}

	return nil
}

// DeletePortfolioHolding removes a holding (when units become 0)
func DeletePortfolioHolding(ctx context.Context, tx interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (interface{}, error)
}, entityID, folioID, dematID, schemeID string) error {

	query := `
		DELETE FROM investment.portfolio_snapshot 
		WHERE entity_id = $1 AND folio_id = $2 AND demat_id = $3 AND scheme_id = $4
	`

	_, err := tx.Exec(ctx, query, entityID, folioID, dematID, schemeID)
	if err != nil {
		return fmt.Errorf("failed to delete portfolio holding: %w", err)
	}

	return nil
}
