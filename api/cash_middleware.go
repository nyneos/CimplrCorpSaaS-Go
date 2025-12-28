package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/auth"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

type cashContextKey string

const (
	CashBankAccountsKey cashContextKey = "cashBankAccounts"
	// slice of BankInfo
	CashBanksKey     cashContextKey = "cashBanks"
	CashEntityIDsKey cashContextKey = "cashEntityIDs"
	// slice of BankAccountInfo (detailed)
	CashBankAccountsInfoKey cashContextKey = "cashBankAccountsInfo"
	CashCurrenciesKey       cashContextKey = "cashCurrencies"
	// map: currency_code -> decimal_places
	CashCurrencyDecimalsKey cashContextKey = "cashCurrencyDecimals"
	CashCategoriesKey       cashContextKey = "cashCategories"
	CashGLAccountsKey       cashContextKey = "cashGLAccounts"
)

// BankInfo holds minimal bank metadata returned to handlers
type BankInfo struct {
	ID           string `json:"bank_id"`
	Name         string `json:"bank_name"`
	ShortName    string `json:"bank_short_name"`
	SwiftBIC     string `json:"swift_bic_code"`
	Country      string `json:"country_of_headquarters"`
	Connectivity string `json:"connectivity_type"`
}

// BankAccountInfo holds an account number and its bank metadata
type BankAccountInfo struct {
	AccountNumber string `json:"account_number"`
	BankID        string `json:"bank_id"`
	BankName      string `json:"bank_name"`
}

// CashContextMiddleware loads common master lookups used by cash handlers and
// attaches them to the request context. It requires a DB connection to run
// simple SELECT queries.
func CashContextMiddleware(pgxPool *pgxpool.Pool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var userID string
			ct := r.Header.Get(constants.ContentTypeText)
			if strings.HasPrefix(ct, constants.ContentTypeJSON) && (r.Method == "POST" || r.Method == "PUT") {
				var bodyMap map[string]interface{}
				_ = json.NewDecoder(r.Body).Decode(&bodyMap)
				if uid, ok := bodyMap[constants.KeyUserID].(string); ok {
					userID = uid
				}
				// reset body
				bodyBytes, _ := json.Marshal(bodyMap)
				r.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
			} else if strings.HasPrefix(ct, constants.ContentTypeMultipart) && (r.Method == "POST" || r.Method == "PUT") {
				if err := r.ParseMultipartForm(32 << 20); err == nil {
					userID = r.FormValue(constants.KeyUserID)
				}
			}

			if userID == "" {
				LogError("Missing user_id in request (cash middleware)")
				RespondWithPayload(w, false, "Missing user_id", nil)
				return
			}

			// Validate session
			found := false
			activeSessions := auth.GetActiveSessions()
			for _, s := range activeSessions {
				if s.UserID == userID {
					found = true
					break
				}
			}
			if !found {
				LogError("Invalid session for user_id: %s", userID)
				RespondWithPayload(w, false, constants.ErrInvalidSessionShort, nil)
				return
			}

			// Get user's business unit name from users table
			var userBu string
			if err := pgxPool.QueryRow(r.Context(), "SELECT business_unit_name FROM users WHERE id = $1", userID).Scan(&userBu); err != nil || userBu == "" {
				LogError("User not found or has no business unit assigned for user_id: %s", userID)
				RespondWithPayload(w, false, "User not found or has no business unit assigned", nil)
				return
			}

		// Find root entity id in masterentitycash for this business unit
		// Try exact match first, then case-insensitive match
		var rootEntityId string
		query := `SELECT entity_id FROM masterentitycash 
			WHERE entity_name = $1 
			AND (is_deleted = false OR is_deleted IS NULL) 
			AND (is_top_level_entity = TRUE OR LOWER(active_status) = 'active')`
		
		err := pgxPool.QueryRow(r.Context(), query, userBu).Scan(&rootEntityId)
		
		// If exact match fails, try case-insensitive and trimmed match
		if err != nil {
			LogError("Exact match failed for userBu: %s, trying case-insensitive match", userBu)
			query = `SELECT entity_id FROM masterentitycash 
				WHERE UPPER(TRIM(entity_name)) = UPPER(TRIM($1))
				AND (is_deleted = false OR is_deleted IS NULL) 
				AND (is_top_level_entity = TRUE OR LOWER(active_status) = 'active')
				LIMIT 1`
			err = pgxPool.QueryRow(r.Context(), query, userBu).Scan(&rootEntityId)
		}
		
		// If still not found, try in masterentity as fallback
		if err != nil {
			LogError("Entity not found in masterentitycash for userBu: %s, checking masterentity", userBu)
			query = `SELECT entity_id FROM masterentity 
				WHERE UPPER(TRIM(entity_name)) = UPPER(TRIM($1))
				AND (is_deleted = false OR is_deleted IS NULL)
				LIMIT 1`
			err = pgxPool.QueryRow(r.Context(), query, userBu).Scan(&rootEntityId)
		}
		
		if err != nil {
			LogError("Business unit entity not found in any table for userBu: %s (error: %v)", userBu, err)
			RespondWithPayload(w, false, "Business unit entity not found. Please ensure your business unit is properly configured in the system.", nil)
			return
		}
		
		LogError("Found entity_id: %s for userBu: %s", rootEntityId, userBu)			// Find all descendant business units using recursive CTE over cashentityrelationships
			buRows, buErr := pgxPool.Query(r.Context(), `
             WITH RECURSIVE descendants AS (
    -- Start from the root entity
    SELECT 
        me.entity_id,
        me.entity_name
    FROM masterentitycash me
    INNER JOIN LATERAL (
        SELECT aa.processing_status
        FROM auditactionentity aa
        WHERE aa.entity_id = me.entity_id
        ORDER BY aa.requested_at DESC
        LIMIT 1
    ) astatus ON TRUE
    WHERE me.entity_id = $1
      AND COALESCE(me.is_deleted, false) = false
      AND (LOWER(me.active_status) = 'active' OR me.is_top_level_entity = TRUE)
      AND astatus.processing_status = 'APPROVED'

    UNION ALL
    
    -- Recursive step: fetch children (cashentityrelationships uses entity_name, NOT entity_id!)
    SELECT 
        child.entity_id,
        child.entity_name
    FROM masterentitycash child
    INNER JOIN cashentityrelationships er 
        ON child.entity_name = er.child_entity_name
    INNER JOIN descendants d 
        ON er.parent_entity_name = d.entity_name
    INNER JOIN LATERAL (
        SELECT aa.processing_status
        FROM auditactionentity aa
        WHERE aa.entity_id = child.entity_id
        ORDER BY aa.requested_at DESC
        LIMIT 1
    ) astatus ON TRUE
    WHERE COALESCE(child.is_deleted, false) = false
      AND (LOWER(child.active_status) = 'active' OR child.is_top_level_entity = TRUE)
      AND astatus.processing_status = 'APPROVED'
      AND (LOWER(er.status) = 'active' OR er.status IS NULL)
)
SELECT entity_name 
FROM descendants;

            `, rootEntityId)
			if buErr != nil {
				LogError("Descendants query execution failed for rootEntityId: %s, error: %v", rootEntityId, buErr)
				RespondWithPayload(w, false, fmt.Sprintf("Failed to fetch descendants: %v", buErr), nil)
				return
			}
			defer buRows.Close()

			var buNames []string
			var buEntityIDs []string
			for buRows.Next() {
				var id, name string
				if err := buRows.Scan(&id, &name); err == nil {
					buEntityIDs = append(buEntityIDs, id)
					buNames = append(buNames, name)
				}
			}
			if len(buNames) == 0 {
				LogError("No accessible business units found for user_id: %s", userID)
				RespondWithPayload(w, false, constants.ErrNoAccessibleBusinessUnit, nil)
				return
			}

			// Load currencies along with decimal places (only active + APPROVED audit)
			var currencies []string
			currencyDecimals := make(map[string]int)
			curRows, curErr := pgxPool.Query(r.Context(), `
SELECT 
	mc.currency_code,
	mc.decimal_places
FROM mastercurrency mc
INNER JOIN LATERAL (
	SELECT aac.processing_status
	FROM auditactioncurrency aac
	WHERE aac.currency_id = mc.currency_id
	ORDER BY aac.requested_at DESC
	LIMIT 1
) astatus ON TRUE
WHERE COALESCE(mc.status, 'Inactive') ILIKE 'Active'
  AND astatus.processing_status = 'APPROVED'
  AND mc.currency_code IS NOT NULL
ORDER BY mc.currency_code;
			`)
			if curErr == nil {
				defer curRows.Close()
				for curRows.Next() {
					var code string
					var decimals int
					if err := curRows.Scan(&code, &decimals); err == nil {
						currencies = append(currencies, code)
						currencyDecimals[code] = decimals
					}
				}
			}

			// Fetch approved banks with latest audit status = APPROVED and active_status = 'active'
			var banks []BankInfo
			bankRows, bankErr := pgxPool.Query(r.Context(), `
SELECT 
	mb.bank_id,
	mb.bank_name,
	mb.bank_short_name,
	mb.swift_bic_code,
	mb.country_of_headquarters,
	mb.connectivity_type
FROM masterbank mb
INNER JOIN LATERAL (
	SELECT aab.processing_status
	FROM auditactionbank aab
	WHERE aab.bank_id = mb.bank_id
	ORDER BY aab.requested_at DESC
	LIMIT 1
) astatus ON TRUE
WHERE LOWER(mb.active_status) = 'active'
  AND astatus.processing_status = 'APPROVED'
ORDER BY mb.bank_name;
			`)
			if bankErr == nil {
				defer bankRows.Close()
				for bankRows.Next() {
					var b BankInfo
					if err := bankRows.Scan(&b.ID, &b.Name, &b.ShortName, &b.SwiftBIC, &b.Country, &b.Connectivity); err == nil {
						banks = append(banks, b)
					}
				}
			}

			// Load lookups: bank accounts (filtered by entity IDs), cashflow categories, gl accounts
			var bankAccounts []string
			var bankAccountsInfo []BankAccountInfo
			// fetch only bank accounts belonging to allowed entity ids and approved
			// also join to masterbank (only approved banks) to surface bank metadata
			if len(buEntityIDs) > 0 {
				rows, err := pgxPool.Query(r.Context(), `
SELECT a.account_number, mb.bank_id, mb.bank_name
FROM masterbankaccount a
INNER JOIN LATERAL (
	SELECT aa.processing_status
	FROM auditactionbankaccount aa
	WHERE aa.account_id = a.account_id
	ORDER BY aa.requested_at DESC
	LIMIT 1
) astatus ON TRUE
INNER JOIN masterbank mb ON mb.bank_id = a.bank_id
INNER JOIN LATERAL (
	SELECT aab.processing_status
	FROM auditactionbank aab
	WHERE aab.bank_id = mb.bank_id
	ORDER BY aab.requested_at DESC
	LIMIT 1
) bastatus ON TRUE
WHERE COALESCE(a.is_deleted, false) = false
  AND LOWER(a.status) = 'active'
  AND astatus.processing_status = 'APPROVED'
  AND COALESCE(mb.active_status, '') IS NOT NULL
  AND LOWER(mb.active_status) = 'active'
  AND bastatus.processing_status = 'APPROVED'
  AND a.entity_id = ANY($1)
ORDER BY a.account_number;
				`, buEntityIDs)
				if err == nil {
					defer rows.Close()
					for rows.Next() {
						var acc, bid, bname string
						if err := rows.Scan(&acc, &bid, &bname); err == nil {
							bankAccounts = append(bankAccounts, acc)
							bankAccountsInfo = append(bankAccountsInfo, BankAccountInfo{AccountNumber: acc, BankID: bid, BankName: bname})
						}
					}
				}
			}

			if len(bankAccounts) == 0 {
				// Friendly error: no accessible bank accounts for user's business units
				LogError("No accessible bank accounts for business units: %v", buEntityIDs)
				RespondWithPayload(w, false, "No accessible bank accounts found for your business units", nil)
				return
			}

			var categories []string
			catRows, catErr := pgxPool.Query(r.Context(), "SELECT category_name FROM mastercashflowcategory WHERE (is_deleted = false OR is_deleted IS NULL) AND (approval_status = 'APPROVED' OR LOWER(approval_status) = 'approved')")
			if catErr == nil {
				defer catRows.Close()
				for catRows.Next() {
					var cn string
					if err := catRows.Scan(&cn); err == nil {
						categories = append(categories, cn)
					}
				}
			}

			var glAccounts []string
			glRows, glErr := pgxPool.Query(r.Context(), "SELECT gl_account_code FROM masterglaccount WHERE (is_deleted = false OR is_deleted IS NULL) AND (LOWER(status) = 'active' OR status = 'Active')")
			if glErr == nil {
				defer glRows.Close()
				for glRows.Next() {
					var g string
					if err := glRows.Scan(&g); err == nil {
						glAccounts = append(glAccounts, g)
					}
				}
			}

			// Attach business unit names slice for downstream handlers
			ctx := context.WithValue(r.Context(), BusinessUnitsKey, buNames)
			ctx = context.WithValue(ctx, CashEntityIDsKey, buEntityIDs)
			ctx = context.WithValue(ctx, CashCurrenciesKey, currencies)
			ctx = context.WithValue(ctx, CashCurrencyDecimalsKey, currencyDecimals)
			ctx = context.WithValue(ctx, CashBanksKey, banks)
			ctx = context.WithValue(ctx, CashBankAccountsKey, bankAccounts)
			ctx = context.WithValue(ctx, CashBankAccountsInfoKey, bankAccountsInfo)
			ctx = context.WithValue(ctx, CashCategoriesKey, categories)
			ctx = context.WithValue(ctx, CashGLAccountsKey, glAccounts)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
