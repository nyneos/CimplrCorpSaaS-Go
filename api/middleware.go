package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"

	"CimplrCorpSaas/internal/logger")

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

func GetUserEmailFromCtx(ctx context.Context) string {
	if s := GetSessionFromCtx(ctx); s != nil {
		return s.Email
	}
	return ""
}

func GetUserNameFromCtx(ctx context.Context) string {
	if s := GetSessionFromCtx(ctx); s != nil {
		return s.Name
	}
	return ""
}

func GetUserRoleFromCtx(ctx context.Context) string {
	if s := GetSessionFromCtx(ctx); s != nil {
		return s.Role
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

// GetApprovedBanksFromCtx returns approved bank rows from prevalidation (bank_id, bank_name, bank_short_name).
func GetApprovedBanksFromCtx(ctx context.Context) []map[string]string {
	if banks, ok := ctx.Value("BankInfo").([]map[string]string); ok {
		return banks
	}
	return nil
}

// ResolveBankNameNormForFilter maps bank_id, bank short name, or display name to lower(trim(bank_name)) for SQL filters.
func ResolveBankNameNormForFilter(ctx context.Context, bankIdentifier string) (norm string, ok bool) {
	bankIdentifier = strings.TrimSpace(bankIdentifier)
	if bankIdentifier == "" {
		return "", false
	}
	for _, b := range GetApprovedBanksFromCtx(ctx) {
		if b == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(b["bank_id"]), bankIdentifier) ||
			strings.EqualFold(strings.TrimSpace(b["bank_name"]), bankIdentifier) ||
			strings.EqualFold(strings.TrimSpace(b["bank_short_name"]), bankIdentifier) {
			n := strings.TrimSpace(b["bank_name"])
			if n == "" {
				continue
			}
			return strings.ToLower(n), true
		}
	}
	for _, n := range GetBankNamesFromCtx(ctx) {
		if strings.EqualFold(strings.TrimSpace(n), bankIdentifier) {
			return strings.ToLower(strings.TrimSpace(n)), true
		}
	}
	if bankInfo := GetBankInfoFromCtx(ctx); bankInfo != nil {
		if strings.EqualFold(strings.TrimSpace(bankInfo["bank_name"]), bankIdentifier) {
			return strings.ToLower(strings.TrimSpace(bankInfo["bank_name"])), true
		}
		if strings.EqualFold(strings.TrimSpace(bankInfo["bank_id"]), bankIdentifier) ||
			strings.EqualFold(strings.TrimSpace(bankInfo["bank_short_name"]), bankIdentifier) {
			n := strings.TrimSpace(bankInfo["bank_name"])
			if n != "" {
				return strings.ToLower(n), true
			}
		}
	}
	return "", false
}

// ResolveBankIDForFilter returns master bank_id when the identifier is an exact approved bank_id
// (e.g. BANK-…) or when it matches exactly one approved bank by name or short_name.
// Use this for SQL filters that must not collapse distinct banks sharing the same display name.
func ResolveBankIDForFilter(ctx context.Context, bankIdentifier string) (bankID string, ok bool) {
	bankIdentifier = strings.TrimSpace(bankIdentifier)
	if bankIdentifier == "" {
		return "", false
	}
	banks := GetApprovedBanksFromCtx(ctx)
	for _, b := range banks {
		if b == nil {
			continue
		}
		id := strings.TrimSpace(b["bank_id"])
		if id != "" && strings.EqualFold(id, bankIdentifier) {
			return id, true
		}
	}
	var hits []string
	seen := make(map[string]bool)
	for _, b := range banks {
		if b == nil {
			continue
		}
		id := strings.TrimSpace(b["bank_id"])
		if id == "" || seen[id] {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(b["bank_name"]), bankIdentifier) ||
			strings.EqualFold(strings.TrimSpace(b["bank_short_name"]), bankIdentifier) {
			hits = append(hits, id)
			seen[id] = true
		}
	}
	if len(hits) == 1 {
		return hits[0], true
	}
	return "", false
}

// ResolveCurrencyCodeUpperForFilter maps currency_id, code, or name to upper(trim(currency_code)).
func ResolveCurrencyCodeUpperForFilter(ctx context.Context, currencyIdentifier string) (code string, ok bool) {
	currencyIdentifier = strings.TrimSpace(currencyIdentifier)
	if currencyIdentifier == "" {
		return "", false
	}
	for _, c := range GetCurrenciesFromCtx(ctx) {
		if c == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(c["currency_id"]), currencyIdentifier) ||
			strings.EqualFold(strings.TrimSpace(c["currency_code"]), currencyIdentifier) ||
			strings.EqualFold(strings.TrimSpace(c["currency_name"]), currencyIdentifier) {
			cc := strings.TrimSpace(c["currency_code"])
			if cc == "" {
				continue
			}
			return strings.ToUpper(cc), true
		}
	}
	for _, c := range GetCurrencyCodesFromCtx(ctx) {
		if strings.EqualFold(strings.TrimSpace(c), currencyIdentifier) {
			return strings.ToUpper(strings.TrimSpace(c)), true
		}
	}
	return "", false
}

// ResolveEntityIDForFilter maps entity_id or entity_name (paired with IDs from prevalidation) to entity_id.
func ResolveEntityIDForFilter(ctx context.Context, entityIdentifier string) (entityID string, ok bool) {
	entityIdentifier = strings.TrimSpace(entityIdentifier)
	if entityIdentifier == "" {
		return "", false
	}
	ids := GetEntityIDsFromCtx(ctx)
	names := GetEntityNamesFromCtx(ctx)
	up := strings.ToUpper(entityIdentifier)
	for _, id := range ids {
		if strings.ToUpper(strings.TrimSpace(id)) == up {
			return strings.TrimSpace(id), true
		}
	}
	for i, name := range names {
		if strings.EqualFold(strings.TrimSpace(name), entityIdentifier) {
			if i < len(ids) {
				return strings.TrimSpace(ids[i]), true
			}
			return "", false
		}
	}
	return "", false
}

func IsCurrencyAllowed(ctx context.Context, currency string) bool {
	currency = strings.TrimSpace(currency)
	if currency == "" {
		return false
	}
	_, ok := ResolveCurrencyCodeUpperForFilter(ctx, currency)
	return ok
}

func IsBankAllowed(ctx context.Context, bankIdentifier string) bool {
	bankIdentifier = strings.TrimSpace(bankIdentifier)
	if bankIdentifier == "" {
		return false
	}
	_, ok := ResolveBankNameNormForFilter(ctx, bankIdentifier)
	return ok
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

			// Debug: log every request hitting the middleware so we can trace timing.
			logger.LogInfo("[BUMiddleware] %s %s ct=%q", r.Method, r.URL.Path, ct)

			if strings.HasPrefix(ct, constants.ContentTypeJSON) && (r.Method == "POST" || r.Method == "PUT" || r.Method == "DELETE" || r.Method == "PATCH") {
				var bodyMap map[string]interface{}
				if decErr := json.NewDecoder(r.Body).Decode(&bodyMap); decErr != nil {
					logger.LogError("[BUMiddleware] body decode error for %s %s: %v", r.Method, r.URL.Path, decErr)
				}
				if uid, ok := bodyMap[constants.KeyUserID].(string); ok {
					userID = uid
				} else if uid, ok := bodyMap[constants.KeyUserID]; ok {
					// handle numeric user_id encoded as float64 by JSON decoder
					userID = fmt.Sprintf("%v", uid)
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
			} else if r.Method == "POST" || r.Method == "PUT" || r.Method == "DELETE" || r.Method == "PATCH" {
				// Content-Type was not JSON — try to read body anyway for user_id
				logger.LogInfo("[BUMiddleware] WARNING: %s %s missing Content-Type:application/json — will fall back to query param for user_id", r.Method, r.URL.Path)
			}
			// For GET/HEAD/OPTIONS or when body didn't contain user_id, fall back to query param
			if userID == "" {
				userID = r.URL.Query().Get(constants.KeyUserID)
			}

			if userID == "" {
				logger.LogInfo("[BUMiddleware] BLOCKED %s %s — missing user_id (ct=%q)", r.Method, r.URL.Path, ct)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrMissingUserID,
				})
				return
			}

			// Validate session and capture it for context injection
			var matchedSession *auth.UserSession
			for _, s := range auth.GetActiveSessions() {
				if s.UserID == userID {
					matchedSession = s
					break
				}
			}
			if matchedSession == nil {
				logger.LogError("[BUMiddleware] BLOCKED %s %s — invalid session for user_id=%s", r.Method, r.URL.Path, userID)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrInvalidSession,
				})
				return
			}

			// Resolve entity scope.
			// New model: query user_entity_mappings for all directly-assigned entities.
			// Fallback: legacy users.business_unit_name → single root entity lookup.
			var rootEntityIds []string
			{
				mRows, mErr := db.Query(
					"SELECT entity_id::text FROM user_entity_mappings WHERE user_id = $1",
					userID,
				)
				if mErr == nil {
					defer mRows.Close()
					for mRows.Next() {
						var eid string
						if mRows.Scan(&eid) == nil && eid != "" {
							rootEntityIds = append(rootEntityIds, eid)
						}
					}
				}
			}

			if len(rootEntityIds) == 0 {
				logger.LogInfo("[BUMiddleware] BLOCKED %s %s — no entity mapping for user_id=%s", r.Method, r.URL.Path, userID)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				w.WriteHeader(http.StatusForbidden)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrNoAccessibleBusinessUnit,
				})
				return
			}

			// For each root entity, recursively fetch all descendants. Deduplicate.
			entitySeen := make(map[string]bool)
			var buNames []string
			var buEntityIDs []string

			for _, rootEntityId := range rootEntityIds {
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
				if err1 == nil {
					defer rows1.Close()
					for rows1.Next() {
						var entityID, entityName string
						if rows1.Scan(&entityID, &entityName) == nil && !entitySeen[entityID] {
							entitySeen[entityID] = true
							buEntityIDs = append(buEntityIDs, entityID)
							buNames = append(buNames, entityName)
						}
					}
				} else {
					logger.LogError("[WARN] masterentitycash recursive query failed for root=%s: %v", rootEntityId, err1)
				}

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
				if err2 == nil {
					defer rows2.Close()
					for rows2.Next() {
						var entityID, entityName string
						if rows2.Scan(&entityID, &entityName) == nil && !entitySeen[entityID] {
							entitySeen[entityID] = true
							buEntityIDs = append(buEntityIDs, entityID)
							buNames = append(buNames, entityName)
						}
					}
				} else {
					logger.LogError("[WARN] masterentity recursive query failed for root=%s: %v", rootEntityId, err2)
				}
			}

			if len(buNames) == 0 {
				logger.LogError("No accessible business units found for user_id: %s", userID)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   "No accessible business units found",
				})
				return
			}
			// Attach entity scope, session, and user identity to context
			ctx := context.WithValue(r.Context(), BusinessUnitsKey, buNames)
			ctx = context.WithValue(ctx, EntityIDsKey, buEntityIDs)
			ctx = context.WithValue(ctx, "session", matchedSession)
			ctx = context.WithValue(ctx, "user_id", userID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
