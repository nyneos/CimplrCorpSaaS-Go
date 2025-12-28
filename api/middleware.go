package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
)

type contextKey string

const (
	BusinessUnitsKey contextKey = "businessUnits"
	EntityIDsKey     contextKey = "entityIDs"
)

// Helper functions for context retrieval (used by middlewares in subdirectory)
func GetEntityNamesFromCtx(ctx context.Context) []string {
	if names, ok := ctx.Value(BusinessUnitsKey).([]string); ok {
		return names
	}
	return []string{}
}

func GetEntityIDsFromCtx(ctx context.Context) []string {
	if ids, ok := ctx.Value(EntityIDsKey).([]string); ok {
		return ids
	}
	// Fallback to CashEntityIDsKey for cash module
	if ids, ok := ctx.Value("CashEntityIDs").([]string); ok {
		return ids
	}
	return []string{}
}

func GetCurrencyCodesFromCtx(ctx context.Context) []string {
	if codes, ok := ctx.Value("ActiveCurrencies").([]map[string]string); ok {
		var currCodes []string
		for _, c := range codes {
			if code, exists := c["currency_code"]; exists {
				currCodes = append(currCodes, code)
			}
		}
		return currCodes
	}
	return []string{}
}

func GetBankNamesFromCtx(ctx context.Context) []string {
	if banks, ok := ctx.Value("BankInfo").([]map[string]string); ok {
		var bankNames []string
		for _, b := range banks {
			if name, exists := b["bank_name"]; exists {
				bankNames = append(bankNames, name)
			}
		}
		return bankNames
	}
	// Fallback: try single bank
	if bank, ok := ctx.Value("BankInfo").(map[string]string); ok {
		if name, exists := bank["bank_name"]; exists {
			return []string{name}
		}
	}
	return []string{}
}

func GetCashFlowCategoryNamesFromCtx(ctx context.Context) []string {
	categories := GetCashFlowCategoriesFromCtx(ctx)
	names := make([]string, 0, len(categories))
	for _, cat := range categories {
		if name, ok := cat["category_name"]; ok {
			names = append(names, name)
		}
	}
	return names
}

func GetCashFlowCategoriesFromCtx(ctx context.Context) []map[string]string {
	if categories, ok := ctx.Value("CashFlowCategories").([]map[string]string); ok {
		return categories
	}
	return []map[string]string{}
}

func GetBankInfoFromCtx(ctx context.Context) map[string]string {
	if bank, ok := ctx.Value("BankInfo").(map[string]string); ok {
		return bank
	}
	return nil
}

func GetCurrenciesFromCtx(ctx context.Context) []map[string]string {
	if currencies, ok := ctx.Value("ActiveCurrencies").([]map[string]string); ok {
		return currencies
	}
	return []map[string]string{}
}

func GetSessionFromCtx(ctx context.Context) *auth.UserSession {
	if session, ok := ctx.Value("session").(*auth.UserSession); ok {
		return session
	}
	return nil
}

func GetUserIDFromCtx(ctx context.Context) string {
	if userID, ok := ctx.Value("user_id").(string); ok {
		return userID
	}
	return ""
}

// Validation helpers
func IsEntityAllowed(ctx context.Context, entityIdentifier string) bool {
	entityNames := GetEntityNamesFromCtx(ctx)
	entityIDs := GetEntityIDsFromCtx(ctx)
	
	if len(entityNames) == 0 && len(entityIDs) == 0 {
		return false
	}
	
	identifierUpper := strings.ToUpper(strings.TrimSpace(entityIdentifier))
	
	// Check against entity IDs first
	for _, id := range entityIDs {
		if strings.ToUpper(strings.TrimSpace(id)) == identifierUpper {
			return true
		}
	}
	
	// Check against entity names
	for _, name := range entityNames {
		if strings.ToUpper(strings.TrimSpace(name)) == identifierUpper {
			return true
		}
	}
	
	return false
}

func IsCurrencyAllowed(ctx context.Context, currency string) bool {
	currencies := GetCurrencyCodesFromCtx(ctx)
	if len(currencies) == 0 {
		return false
	}
	currUpper := strings.ToUpper(strings.TrimSpace(currency))
	for _, c := range currencies {
		if strings.ToUpper(strings.TrimSpace(c)) == currUpper {
			return true
		}
	}
	return false
}

func IsBankAllowed(ctx context.Context, bankName string) bool {
	banks := GetBankNamesFromCtx(ctx)
	if len(banks) == 0 {
		// Check single bank info
		bankInfo := GetBankInfoFromCtx(ctx)
		if bankInfo != nil {
			if name, exists := bankInfo["bank_name"]; exists {
				return strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(bankName))
			}
		}
		return false
	}
	bankUpper := strings.ToUpper(strings.TrimSpace(bankName))
	for _, b := range banks {
		if strings.ToUpper(strings.TrimSpace(b)) == bankUpper {
			return true
		}
	}
	return false
}

func IsCashFlowCategoryAllowed(ctx context.Context, categoryName string) bool {
	categories := GetCashFlowCategoryNamesFromCtx(ctx)
	if len(categories) == 0 {
		return false
	}
	catUpper := strings.ToUpper(strings.TrimSpace(categoryName))
	for _, c := range categories {
		if strings.ToUpper(strings.TrimSpace(c)) == catUpper {
			return true
		}
	}
	return false
}

func BusinessUnitMiddleware(db *sql.DB) func(http.Handler) http.Handler {
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
				// Re-marshal and reset body for downstream handlers
				bodyBytes, _ := json.Marshal(bodyMap)
				r.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
			} else if strings.HasPrefix(ct, constants.ContentTypeMultipart) && (r.Method == "POST" || r.Method == "PUT") {
				// Parse multipart form to get user_id
				err := r.ParseMultipartForm(32 << 20) // 32MB
				if err == nil {
					userID = r.FormValue(constants.KeyUserID)
				}
			}

			if userID == "" {
				log.Println("[ERROR] Missing user_id in request")
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrMissingUserID,
				})
				return
			}

			// Validate session
			found := false
			activeSessions := auth.GetActiveSessions()
			for _, session := range activeSessions {
				if session.UserID == userID {
					found = true
					break
				}
			}
			if !found {
				log.Println("[ERROR] Invalid session for user_id:", userID)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrInvalidSession,
				})
				return
			}

			// Get user's business unit name
			var userBu string
			err := db.QueryRow("SELECT business_unit_name FROM users WHERE id = $1", userID).Scan(&userBu)
			if err != nil || userBu == "" {
				log.Println("[ERROR] User not found or has no business unit assigned for user_id:", userID)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrNoAccessibleBusinessUnit,
				})
				return
			}

		// Find root entity id - check both masterentitycash and masterentity
		var rootEntityId string
		
		// Try 1: Exact match in masterentitycash
		query := `SELECT entity_id FROM masterentitycash 
			WHERE entity_name = $1 
			AND (is_deleted = false OR is_deleted IS NULL) 
			AND (is_top_level_entity = TRUE OR LOWER(active_status) = 'active')`
		err = db.QueryRow(query, userBu).Scan(&rootEntityId)
		
		// Try 2: Case-insensitive match in masterentitycash
		if err != nil {
			query = `SELECT entity_id FROM masterentitycash 
				WHERE UPPER(TRIM(entity_name)) = UPPER(TRIM($1))
				AND (is_deleted = false OR is_deleted IS NULL) 
				AND (is_top_level_entity = TRUE OR LOWER(active_status) = 'active')
				LIMIT 1`
			err = db.QueryRow(query, userBu).Scan(&rootEntityId)
		}
		
		// Try 3: Exact match in masterentity (fallback)
		if err != nil {
			query = `SELECT entity_id FROM masterEntity 
				WHERE entity_name = $1 
				AND (is_deleted = false OR is_deleted IS NULL) 
				AND (is_top_level_entity = TRUE OR approval_status ILIKE 'approved')`
			err = db.QueryRow(query, userBu).Scan(&rootEntityId)
		}
		
		// Try 4: Case-insensitive match in masterentity (final fallback)
		if err != nil {
			query = `SELECT entity_id FROM masterEntity 
				WHERE UPPER(TRIM(entity_name)) = UPPER(TRIM($1))
				AND (is_deleted = false OR is_deleted IS NULL) 
				AND (is_top_level_entity = TRUE OR approval_status ILIKE 'approved')
				LIMIT 1`
			err = db.QueryRow(query, userBu).Scan(&rootEntityId)
		}
		
		if err != nil {
			log.Printf("[ERROR] Business unit entity NOT FOUND in masterentitycash OR masterentity for userBu: '%s' (error: %v)", userBu, err)
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				constants.ValueSuccess: false,
				constants.ValueError:   "Business unit '" + userBu + "' not found in system. Please contact administrator.",
			})
			return
		}
		
		
		// Try FIRST query: masterentitycash with cashentityrelationships
		rows1, err1 := db.Query(`
               WITH RECURSIVE descendants AS (
                    SELECT entity_id, entity_name 
                    FROM masterentitycash 
                    WHERE entity_id = $1 AND (is_deleted = false OR is_deleted IS NULL)
                    
                    UNION ALL
                    
                    SELECT me.entity_id, me.entity_name
                    FROM masterentitycash me
                    INNER JOIN cashentityrelationships er ON me.entity_name = er.child_entity_name
                    INNER JOIN descendants d ON er.parent_entity_name = d.entity_name
                    WHERE (me.is_deleted = false OR me.is_deleted IS NULL)
                      AND (LOWER(er.status) = 'active' OR er.status IS NULL)
                )
                SELECT DISTINCT entity_id, entity_name FROM descendants
            `, rootEntityId)
		
		var buNames []string
		var buEntityIDs []string
		
		// Process first query results
		if err1 == nil {
			defer rows1.Close()
			for rows1.Next() {
				var entityID, entityName string
				if err := rows1.Scan(&entityID, &entityName); err == nil {
					buEntityIDs = append(buEntityIDs, entityID)
					buNames = append(buNames, entityName)
				}
			}
		} else {
			log.Printf("[WARN] masterentitycash query failed: %v", err1)
		}
		
		// Try SECOND query: masterentity with entityRelationships
		rows2, err2 := db.Query(`
               WITH RECURSIVE descendants AS (
                    SELECT entity_id, entity_name 
                    FROM masterEntity 
                    WHERE entity_id = $1 AND (is_deleted = false OR is_deleted IS NULL)
                    
                    UNION ALL
                    
                    SELECT me.entity_id, me.entity_name
                    FROM masterEntity me
                    INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
                    INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
                    WHERE (me.is_deleted = false OR me.is_deleted IS NULL)
                )
                SELECT DISTINCT entity_id, entity_name FROM descendants
            `, rootEntityId)
		
		// Process second query results
		if err2 == nil {
			defer rows2.Close()
			for rows2.Next() {
				var entityID, entityName string
				if err := rows2.Scan(&entityID, &entityName); err == nil {
					// Avoid duplicates
					exists := false
					for _, id := range buEntityIDs {
						if id == entityID {
							exists = true
							break
						}
					}
					if !exists {
						buEntityIDs = append(buEntityIDs, entityID)
						buNames = append(buNames, entityName)
					}
				}
			}
		} else {
			log.Printf("[WARN] masterentity query failed: %v", err2)
		}
		
		
		// If BOTH queries failed or returned nothing, error out
		if len(buNames) == 0 {
			log.Printf("[ERROR] No accessible business units found in either table for rootEntityId: %s", rootEntityId)
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				constants.ValueSuccess: false,
				constants.ValueError:   "No accessible business units found",
			})
			return
		}
			// Attach to context and call next
			
			ctx := context.WithValue(r.Context(), BusinessUnitsKey, buNames)
			ctx = context.WithValue(ctx, EntityIDsKey, buEntityIDs)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
