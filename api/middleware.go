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

const BusinessUnitsKey contextKey = "businessUnits"

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

			// Find root entity id
			var rootEntityId string
			err = db.QueryRow(
				"SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (is_deleted = false OR is_deleted IS NULL) AND (is_top_level_entity = TRUE OR (approval_status = 'Approved' OR approval_status = 'approved'))",
				userBu,
			).Scan(&rootEntityId)
			if err != nil {
				log.Println("[ERROR] Business unit entity not found for userBu:", userBu)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrNoAccessibleBusinessUnit,
				})
				return
			}

			// Find all descendant business units using recursive CTE
			rows, err := db.Query(`
               WITH RECURSIVE descendants AS (
                    SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
                    UNION ALL
                    SELECT me.entity_id, me.entity_name
                    FROM masterEntity me
                    INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
                    INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
                    WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
                )
                SELECT entity_name FROM descendants
            `, rootEntityId)
			if err != nil {
				log.Println("[ERROR] No accessible business units found for rootEntityId:", rootEntityId)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrNoAccessibleBusinessUnit,
				})
				return
			}
			defer rows.Close()

			var buNames []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err == nil {
					buNames = append(buNames, name)
				}
			}
			if len(buNames) == 0 {
				log.Println("[ERROR] No accessible business units found for user_id:", userID)
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrNoAccessibleBusinessUnit,
				})
				return
			}
			// Attach to context and call next
			ctx := context.WithValue(r.Context(), BusinessUnitsKey, buNames)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
