package validation

import (
	"CimplrCorpSaas/api/auth"
	usersession "CimplrCorpSaas/internal/session"
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ExtractUserID(r *http.Request) (string, error) {
	s, present := usersession.HeaderSession(r)
	if !present || s == nil {
		return "", usersession.ErrSessionRequired
	}
	return s.UserID, nil
}

func ValidateSession(sessionID string) *auth.UserSession {
	s := auth.SessionByID(strings.TrimSpace(sessionID))
	if !s.LoggedIn() {
		return nil
	}
	return s
}

// GetUserEntities retrieves all accessible entities for a user (lazy loaded when needed)
// Uses recursive CTE to get entire entity hierarchy
func GetUserEntities(ctx context.Context, db *pgxpool.Pool, userID string, businessUnit string) ([]string, error) {
	query := `
		WITH RECURSIVE entity_tree AS (
			SELECT entity_id, entity_name, parent_entity_name, 1 AS level, ARRAY[entity_id::text] AS visited
			FROM masterentitycash
			WHERE entity_name = $1
			  AND COALESCE(is_deleted, false) = false
			  AND LOWER(active_status) = 'active'

			UNION ALL

			SELECT e.entity_id, e.entity_name, e.parent_entity_name, et.level + 1, et.visited || e.entity_id::text
			FROM masterentitycash e
			INNER JOIN entity_tree et ON e.parent_entity_name = et.entity_name
			WHERE COALESCE(e.is_deleted, false) = false
			  AND LOWER(e.active_status) = 'active'
			  AND NOT (e.entity_id::text = ANY(et.visited))
			  AND et.level < 32
		)
		SELECT entity_id FROM entity_tree
		ORDER BY level, entity_name
	`

	rows, err := db.Query(ctx, query, businessUnit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch entities: %w", err)
	}
	defer rows.Close()

	entities := []string{}
	for rows.Next() {
		var entityID string
		if err := rows.Scan(&entityID); err == nil {
			entities = append(entities, entityID)
		}
	}

	return entities, nil
}

// CurrencyInfo represents a validated currency
type CurrencyInfo struct {
	CurrencyID   string
	CurrencyCode string
	CurrencyName string
}

// GetApprovedCurrencies retrieves all approved active currencies (lazy loaded)
func GetApprovedCurrencies(ctx context.Context, db *pgxpool.Pool) ([]CurrencyInfo, error) {
	query := `
		WITH latest AS (
			SELECT DISTINCT ON (currency_id)
				currency_id,
				processing_status
			FROM auditactioncurrency
			ORDER BY currency_id, requested_at DESC
		)
		SELECT
			m.currency_id,
			m.currency_code,
			m.currency_name
		FROM mastercurrency m
		JOIN latest l ON l.currency_id = m.currency_id
		WHERE l.processing_status = 'APPROVED'
		  AND LOWER(m.status) = 'active'
		ORDER BY m.currency_code
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch currencies: %w", err)
	}
	defer rows.Close()

	currencies := []CurrencyInfo{}
	for rows.Next() {
		var c CurrencyInfo
		if err := rows.Scan(&c.CurrencyID, &c.CurrencyCode, &c.CurrencyName); err == nil {
			currencies = append(currencies, c)
		}
	}

	return currencies, nil
}

// BankInfo represents a validated bank
type BankInfo struct {
	BankID   string
	BankName string
}

// GetApprovedBanks retrieves all approved active banks (lazy loaded)
func GetApprovedBanks(ctx context.Context, db *pgxpool.Pool) ([]BankInfo, error) {
	query := `
		WITH latest AS (
			SELECT DISTINCT ON (bank_id)
				bank_id,
				processing_status
			FROM auditactionbank
			ORDER BY bank_id, requested_at DESC
		)
		SELECT
			m.bank_id,
			m.bank_name
		FROM masterbank m
		JOIN latest l ON l.bank_id = m.bank_id
		WHERE l.processing_status = 'APPROVED'
		  AND LOWER(m.active_status) = 'active'
		  AND COALESCE(m.is_deleted, false) = false
		ORDER BY m.bank_name
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch banks: %w", err)
	}
	defer rows.Close()

	banks := []BankInfo{}
	for rows.Next() {
		var b BankInfo
		if err := rows.Scan(&b.BankID, &b.BankName); err == nil {
			banks = append(banks, b)
		}
	}

	return banks, nil
}

// NormalizeString trims whitespace and converts to lowercase for comparisons
func NormalizeString(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// ValidateCurrencyCode checks if a currency code is in the approved list
func ValidateCurrencyCode(code string, approved []CurrencyInfo) bool {
	normalized := strings.ToUpper(strings.TrimSpace(code))
	for _, c := range approved {
		if strings.ToUpper(c.CurrencyCode) == normalized {
			return true
		}
	}
	return false
}

// ValidateBankID checks if a bank ID is in the approved list
func ValidateBankID(bankID string, approved []BankInfo) bool {
	normalized := strings.TrimSpace(bankID)
	for _, b := range approved {
		if b.BankID == normalized {
			return true
		}
	}
	return false
}
