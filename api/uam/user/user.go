package user

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/api/utils"
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

// generateRandomPassword returns a cryptographically random password of the
// requested length using a charset that satisfies most complexity requirements.
func generateRandomPassword(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
	charsetLen := big.NewInt(int64(len(charset)))
	result := make([]byte, length)
	for i := range result {
		idx, err := crand.Int(crand.Reader, charsetLen)
		if err != nil {
			return "", err
		}
		result[i] = charset[idx.Int64()]
	}
	return string(result), nil
}

// sensitiveUserCols lists columns that must never be sent to the client.
var sensitiveUserCols = map[string]bool{
	"password":    true,
	"created_by":  true,
	"updated_by":  true,
	"approved_by": true,
	"rejected_by": true,
	"created_at":  true,
	"updated_at":  true,
	"approved_at": true,
	"rejected_at": true,
}

func sanitizeUserMap(m map[string]interface{}) map[string]interface{} {
	for col := range sensitiveUserCols {
		delete(m, col)
	}
	return m
}

// Helper: send JSON error response
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

// respondWithInternalError logs the real error internally and returns a generic
// 500 message to the client so DB internals are never exposed.
func respondWithInternalError(w http.ResponseWriter, err error) {
	log.Println("[ERROR] user:", err)
	respondWithError(w, http.StatusInternalServerError, "Internal server error")
}

// decodeSQLValue converts SQL driver types to JSON-friendly Go values.
// In particular, database/sql returns JSON columns as []byte — when left
// as-is they become base64 strings when encoded by json. Attempt to
// unmarshal []byte into an interface{}; on failure return string(b).
func decodeSQLValue(v interface{}) interface{} {
	switch t := v.(type) {
	case []byte:
		var out interface{}
		if err := json.Unmarshal(t, &out); err == nil {
			return out
		}
		return string(t)
	default:
		return t
	}
}

// Handler: Create user
func CreateUser(db *sql.DB, pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AuthenticationType   string `json:"authentication_type"`
			EmployeeName         string `json:"employee_name"`
			Role                 string `json:"role"`
			UsernameOrEmployeeID string `json:"username_or_employee_id"`
			Email                string `json:"email"`
			Mobile               string `json:"mobile"`
			Address              string `json:"address"`
			UserID               string `json:"user_id"`
			// Password provisioning
			PasswordType        string `json:"password_type"`         // "auto" | "custom"
			Password            string `json:"password"`              // plain-text for "custom"
			ForcePasswordChange bool   `json:"force_password_change"` // require change on first login
			// Multi-entity mapping: list of entities this user is directly assigned to.
			EntityMappings []struct {
				EntityID   string `json:"entity_id"`
				EntityName string `json:"entity_name"`
			} `json:"entity_mappings"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		// Get createdBy from session
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		// --- Password provisioning -----------------------------------------------
		cimplr := req.Password
		if req.PasswordType == "auto" || cimplr == "" {
			// var genErr error
			// cimplr, genErr = generateRandomPassword(16)
			// if genErr != nil {
			// 	respondWithError(w, http.StatusInternalServerError, "Failed to generate password")
			// 	return
			// }cimplr = "changeme"
			envDefaultPassword := strings.TrimSpace(os.Getenv("UAM_DEFAULT_PASSWORD"))
			if envDefaultPassword != "" {
				cimplr = envDefaultPassword
			} else {
				cimplr = "changeme"
			}
		}
		hashedPassword, hashErr := bcrypt.GenerateFromPassword([]byte(cimplr), bcrypt.DefaultCost)
		if hashErr != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to hash password")
			return
		}
		// -------------------------------------------------------------------------

		tx, err := db.Begin()
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer tx.Rollback()

		// Uniqueness checks: ensure username_or_employee_id and email are unique
		var existingID string
		var existingUsername, existingEmail string
		err = tx.QueryRow("SELECT id, username_or_employee_id, email FROM users WHERE username_or_employee_id = $1 OR email = $2 LIMIT 1", req.UsernameOrEmployeeID, req.Email).Scan(&existingID, &existingUsername, &existingEmail)
		if err == nil {
			// conflict
			msg := ""
			if existingUsername == req.UsernameOrEmployeeID && existingEmail == req.Email {
				msg = fmt.Sprintf("user with username '%s' and email '%s' already exists (id=%s)", existingUsername, existingEmail, existingID)
			} else if existingUsername == req.UsernameOrEmployeeID {
				msg = fmt.Sprintf("username '%s' already exists (id=%s)", existingUsername, existingID)
			} else {
				msg = fmt.Sprintf("email '%s' already exists (id=%s)", existingEmail, existingID)
			}
			respondWithError(w, http.StatusBadRequest, msg)
			return
		} else if err != sql.ErrNoRows {
			log.Println("[ERROR] user uniqueness check:", err)
			respondWithError(w, http.StatusInternalServerError, "Internal server error")
			return
		}
		var userId string
		err = tx.QueryRow(`INSERT INTO users (
				authentication_type,
				employee_name,
				username_or_employee_id,
				email,
				mobile,
				address,
				password,
				force_password_change,
				status,
				created_at,
				created_by
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', NOW(), $9) RETURNING id`,
			req.AuthenticationType,
			req.EmployeeName,
			req.UsernameOrEmployeeID,
			req.Email,
			req.Mobile,
			req.Address,
			string(hashedPassword),
			req.ForcePasswordChange,
			createdBy,
		).Scan(&userId)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		var roleId string
		err = tx.QueryRow(`SELECT id FROM roles WHERE name = $1 OR rolecode = $1`, req.Role).Scan(&roleId)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Role '"+req.Role+"' not found in roles table")
			return
		}
		_, err = tx.Exec(`INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2)`, userId, roleId)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		// Insert entity mappings
		for _, em := range req.EntityMappings {
			if em.EntityID == "" {
				continue
			}
			_, err = tx.Exec(`
				INSERT INTO user_entity_mappings (user_id, entity_id, entity_name)
				VALUES ($1, $2, $3)
				ON CONFLICT (user_id, entity_id) DO UPDATE SET entity_name = EXCLUDED.entity_name`,
				userId, em.EntityID, em.EntityName,
			)
			if err != nil {
				log.Println("[ERROR] user entity mapping insert:", err)
				respondWithError(w, http.StatusInternalServerError, "Internal server error")
				return
			}
		}
		tx.Commit()

		// Fire welcome / credentials email (fire-and-forget)
		if pool != nil {
			go catalog.TriggerNotification(
				context.Background(), pool,
				"/uam/users/create-user",
				"USR-"+userId,
				map[string]interface{}{
					"UserID":       userId,
					"EmployeeName": req.EmployeeName,
					"Email":        req.Email,
					"Role":         req.Role,
					"Password":     cimplr,
				},
			)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"user_id":        userId,
			"role_id":        roleId,
			"plain_password": cimplr,
		})
	}
}

// Handler: Get users for accessible business units
func GetUsers(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Query().Get("status")
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}
		// Get entity IDs from context (set by middleware via user_entity_mappings).
		// Admin override sessions are allowed even if entity list is unexpectedly empty.
		entityIDs, _ := r.Context().Value(api.EntityIDsKey).([]string)
		isAdminOverride, _ := r.Context().Value("is_admin_override").(bool)
		if len(entityIDs) == 0 && !isAdminOverride {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Pagination
		pagination, err := utils.ExtractPagination(r)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Count total — users accessible via entity mapping.
		countArgs := []interface{}{pq.Array(entityIDs)}
		paramIdx := 2
		countQuery := `SELECT COUNT(*) FROM users u WHERE
			EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = u.id AND uem.entity_id = ANY($1::text[]))`
		if status != "" {
			countQuery += fmt.Sprintf(" AND u.status = $%d", paramIdx)
			countArgs = append(countArgs, status)
			paramIdx++
		}
		total, _ := utils.CountTotal(db, countQuery, countArgs...)
		pagination.SetPaginationStats(total)

		// Build paginated query. Return entity mappings as JSON array.
		args := []interface{}{pq.Array(entityIDs)}
		pIdx := 2
		query := `SELECT u.*,
	COALESCE(rr.role_name, '') AS role_name,
	COALESCE(rr.role_status, '') AS role_status,
	COALESCE(rr.role_permission_status, '') AS role_permission_status,
	COALESCE(rr.role_code, '') AS role_code,
	COALESCE(em.entity_mappings, '[]'::json) AS entity_mappings
FROM users u
LEFT JOIN LATERAL (
	SELECT r.name AS role_name,
				 COALESCE(r.status, '') AS role_status,
				 COALESCE(r.roles_permission_status, '') AS role_permission_status,
				 COALESCE(r.role_code, r.rolecode) AS role_code
	FROM user_roles ur
	JOIN roles r ON ur.role_id = r.id
	WHERE ur.user_id = u.id
	LIMIT 1
) rr ON true
LEFT JOIN LATERAL (
	SELECT COALESCE(
			json_agg(json_build_object('entity_id', entity_id, 'entity_name', entity_name)),
			'[]'::json
		) AS entity_mappings
	FROM user_entity_mappings WHERE user_id = u.id
) em ON true
WHERE EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = u.id AND uem.entity_id = ANY($1::text[]))`
		if status != "" {
			query += fmt.Sprintf(" AND u.status = $%d", pIdx)
			args = append(args, status)
			pIdx++
		}
		query += fmt.Sprintf(" ORDER BY GREATEST(COALESCE(u.created_at, '1970-01-01'::timestamp), COALESCE(u.approved_at, '1970-01-01'::timestamp), COALESCE(u.updated_at, '1970-01-01'::timestamp), COALESCE(u.rejected_at, '1970-01-01'::timestamp)) DESC LIMIT $%d OFFSET $%d", pIdx, pIdx+1)
		args = append(args, pagination.Limit, pagination.Offset)

		rows, err := db.Query(query, args...)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		users := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			rows.Scan(valPtrs...)
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = decodeSQLValue(vals[i])
			}
			users = append(users, sanitizeUserMap(rowMap))
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"users":      users,
			"pagination": pagination,
		})
	}
}

// Handler: Get user by ID (from request body, with business unit middleware)
func GetUserById(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Missing or invalid user_id in request body")
			return
		}
		// Get entity IDs from context (set by middleware via user_entity_mappings)
		entityIDs, _ := r.Context().Value(api.EntityIDsKey).([]string)
		if len(entityIDs) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Query for user by ID — also return their entity mappings as JSON array.
		rows, err := db.Query(`
			SELECT u.*,
				COALESCE(em.entity_mappings, '[]'::json) AS entity_mappings
			FROM users u
			LEFT JOIN LATERAL (
				SELECT COALESCE(
					json_agg(json_build_object('entity_id', entity_id, 'entity_name', entity_name)),
					'[]'::json
				) AS entity_mappings
				FROM user_entity_mappings WHERE user_id = u.id
			) em ON true
			WHERE u.id = $1
			  AND EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = u.id AND uem.entity_id = ANY($2::text[]))`,
			req.UserID, pq.Array(entityIDs),
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		if !rows.Next() {
			respondWithError(w, http.StatusNotFound, "User not found")
			return
		}
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			respondWithInternalError(w, err)
			return
		}
		userMap := map[string]interface{}{}
		for i, col := range cols {
			userMap[col] = decodeSQLValue(vals[i])
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"user":    sanitizeUserMap(userMap),
		})
	}
}

// Handler: Get all approved users
// Expects JSON body: { "user_id": "<session-user-id>" }
// Returns all users with status = 'Approved'
func GetApprovedUser(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID     string `json:"user_id"`
			EntityName string `json:"entity_name"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Missing user_id in request body")
			return
		}

		// Get entity IDs from middleware (via user_entity_mappings)
		entityIDs, _ := r.Context().Value(api.EntityIDsKey).([]string)
		if len(entityIDs) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Build query — if entity_name supplied, narrow to users mapped to that entity.
		var rows *sql.Rows
		var err error
		if req.EntityName != "" {
			rows, err = db.Query(`
				SELECT * FROM users u
				WHERE EXISTS (
					SELECT 1 FROM user_entity_mappings uem
					JOIN masterentitycash ec ON ec.entity_id::text = uem.entity_id
					WHERE uem.user_id = u.id
					  AND uem.entity_id = ANY($1::text[])
					  AND UPPER(TRIM(ec.entity_name)) = UPPER(TRIM($2))
				)
				  AND LOWER(TRIM(u.status)) = 'approved'
				ORDER BY u.id DESC
			`, pq.Array(entityIDs), req.EntityName)
		} else {
			rows, err = db.Query(`
				SELECT * FROM users u
				WHERE EXISTS (
					SELECT 1 FROM user_entity_mappings uem
					WHERE uem.user_id = u.id AND uem.entity_id = ANY($1::text[])
				)
				  AND LOWER(TRIM(u.status)) = 'approved'
				ORDER BY u.id DESC
			`, pq.Array(entityIDs))
		}

		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		users := []map[string]interface{}{}

		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))

			for i := range vals {
				valPtrs[i] = &vals[i]
			}

			rows.Scan(valPtrs...)

			userMap := map[string]interface{}{}
			for i, col := range cols {
				userMap[col] = decodeSQLValue(vals[i])
			}

			users = append(users, sanitizeUserMap(userMap))
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"users":   users,
		})
	}
}

// Handler: Update user
func UpdateUser(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		id, idOk := req["id"].(string)
		userID, userIDOk := req["user_id"].(string)
		if !idOk || !userIDOk || id == "" || userID == "" {
			respondWithError(w, http.StatusBadRequest, "Missing id or user_id")
			return
		}
		entityIDs, _ := r.Context().Value(api.EntityIDsKey).([]string)
		if len(entityIDs) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Extract entity_mappings before building the SQL SET clause (it is not a column).
		var newEntityMappings []struct {
			EntityID   string
			EntityName string
		}
		if rawMappings, hasMappings := req["entity_mappings"]; hasMappings {
			if mappingsSlice, ok := rawMappings.([]interface{}); ok {
				for _, m := range mappingsSlice {
					if mMap, ok := m.(map[string]interface{}); ok {
						eid, _ := mMap["entity_id"].(string)
						ename, _ := mMap["entity_name"].(string)
						if eid != "" {
							newEntityMappings = append(newEntityMappings, struct{ EntityID, EntityName string }{eid, ename})
						}
					}
				}
			}
			delete(req, "entity_mappings")
		}
		// Get updated_by from session
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				updatedBy = s.Email
				break
			}
		}
		if updatedBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		// Allowed fields to update
		allowed := map[string]bool{
			"authentication_type":     true,
			"employee_name":           true,
			"username_or_employee_id": true,
			"email":                   true,
			"mobile":                  true,
			"address":                 true,
			"status":                  true,
			"approved_by":             true,
			"approved_at":             true,
			"rejected_by":             true,
			"rejected_at":             true,
			"approval_comment":        true,
			"password":                true,
		}
		fields := map[string]interface{}{}
		for k, v := range req {
			if allowed[k] {
				// Hash password before storing
				if k == "password" {
					if pw, ok := v.(string); ok && pw != "" {
						hashed, err := auth.HashPassword(pw)
						if err != nil {
							respondWithError(w, http.StatusInternalServerError, "Failed to hash password")
							return
						}
						v = hashed
					}
				}
				fields[k] = v
			}
		}
		fields["updated_by"] = updatedBy
		// record update timestamp for auditing
		fields["updated_at"] = time.Now()
		// When an update occurs, default the status to 'pending' so the
		// update goes through the approval workflow unless the caller
		// explicitly provided a status field.
		if _, hasStatus := fields["status"]; !hasStatus {
			fields["status"] = "pending"
		}
		if len(fields) == 0 {
			respondWithError(w, http.StatusBadRequest, "No valid fields to update")
			return
		}
		// Build query
		keys := make([]string, 0, len(fields))
		values := make([]interface{}, 0, len(fields))
		setClause := ""
		for k := range fields {
			keys = append(keys, k)
		}
		for idx, k := range keys {
			if idx > 0 {
				setClause += ", "
			}
			setClause += k + " = $" + fmt.Sprint(idx+1)
			values = append(values, fields[k])
		}
		query := fmt.Sprintf(
			`UPDATE users SET %s WHERE id = $%d AND
				EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = users.id AND uem.entity_id = ANY($%d::text[]))
			RETURNING *`,
			setClause, len(keys)+1, len(keys)+2,
		)
		values = append(values, id, pq.Array(entityIDs))
		rows, err := db.Query(query, values...)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		if !rows.Next() {
			respondWithError(w, http.StatusNotFound, "User not found or not accessible")
			return
		}
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		rows.Scan(valPtrs...)
			userMap := map[string]interface{}{}
			for i, col := range cols {
				userMap[col] = decodeSQLValue(vals[i])
			}
		// Replace entity mappings if the caller provided a new list.
		if len(newEntityMappings) > 0 {
			db.Exec("DELETE FROM user_entity_mappings WHERE user_id = $1", id)
			for _, em := range newEntityMappings {
				db.Exec(`
					INSERT INTO user_entity_mappings (user_id, entity_id, entity_name)
					VALUES ($1, $2, $3)
					ON CONFLICT (user_id, entity_id) DO UPDATE SET entity_name = EXCLUDED.entity_name`,
					id, em.EntityID, em.EntityName,
				)
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "user": sanitizeUserMap(userMap)})
	}
}

// Helper: convert camelCase to snake_case
// func toSnakeCase(str string) string {
//     var result []rune
//     for i, r := range str {
//         if i > 0 && r >= 'A' && r <= 'Z' {
//             result = append(result, '_', r+'a'-'A')
//         } else {
//             result = append(result, r)
//         }
//     }
//     return string(result)
// }

// Handler: Delete user (soft delete)
func DeleteUser(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"` // for middleware
			ID     string   `json:"id"`      // single user to delete (backwards compat)
			Ids    []string `json:"ids"`     // bulk ids to delete
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || (req.ID == "" && len(req.Ids) == 0) {
			respondWithError(w, http.StatusBadRequest, "Missing user_id or id/ids in request body")
			return
		}
		entityIDs, _ := r.Context().Value(api.EntityIDsKey).([]string)
		if len(entityIDs) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Determine who requested the delete for auditing
		deleter := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				deleter = s.Email
				break
			}
		}
		if deleter == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		// determine target ids (single or bulk)
		targetIds := req.Ids
		if len(targetIds) == 0 && strings.TrimSpace(req.ID) != "" {
			targetIds = []string{req.ID}
		}

		rows, err := db.Query(`
			UPDATE users SET status = 'Delete-Approval', updated_by = $1, updated_at = NOW()
			WHERE id = ANY($2)
			  AND EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = users.id AND uem.entity_id = ANY($3::text[]))
			RETURNING *`,
			deleter, pq.Array(targetIds), pq.Array(entityIDs),
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		updated := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			rows.Scan(valPtrs...)
					userMap := map[string]interface{}{}
					for i, col := range cols {
						userMap[col] = decodeSQLValue(vals[i])
					}
			updated = append(updated, userMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"users":   updated,
		})
	}
}

// Handler: Approve multiple users
func ApproveMultipleUsers(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"` // for middleware
			Ids    []string `json:"ids"`     // users to approve/delete
			// ApprovedBy     string   `json:"approved_by"`
			ApprovalComment string `json:"approval_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.Ids) == 0 || req.ApprovalComment == "" {
			respondWithError(w, http.StatusBadRequest, "ids and approval_comment are required")
			return
		}
		entityIDs, _ := r.Context().Value(api.EntityIDsKey).([]string)
		if len(entityIDs) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Get existing users and their status
		rows, err := db.Query(`
			SELECT id, status FROM users u WHERE id = ANY($1)
			  AND EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = u.id AND uem.entity_id = ANY($2::text[]))`,
			pq.Array(req.Ids), pq.Array(entityIDs),
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		toDelete := []string{}
		toApprove := []string{}
		for rows.Next() {
			var id, status string
			rows.Scan(&id, &status)
			if status == constants.StatusCodeDeleteApproval {
				toDelete = append(toDelete, id)
			} else {
				toApprove = append(toApprove, id)
			}
		}
		results := map[string]interface{}{
			"deleted":  []map[string]interface{}{},
			"approved": []map[string]interface{}{},
		}
		// Delete users and their user_roles
		if len(toDelete) > 0 {
			// Delete user_roles first for referential integrity
			_, err := db.Exec(
				"DELETE FROM user_roles WHERE user_id = ANY($1)",
				pq.Array(toDelete),
			)
			if err != nil {
				log.Println("[ERROR] delete user_roles:", err)
				respondWithError(w, http.StatusInternalServerError, "Internal server error")
				return
			}
			delRows, err := db.Query(`
				DELETE FROM users WHERE id = ANY($1)
				  AND EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = users.id AND uem.entity_id = ANY($2::text[]))
				RETURNING *`,
				pq.Array(toDelete), pq.Array(entityIDs),
			)
			if err == nil {
				defer delRows.Close()
				cols, _ := delRows.Columns()
				for delRows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					delRows.Scan(valPtrs...)
					userMap := map[string]interface{}{}
					for i, col := range cols {
						userMap[col] = decodeSQLValue(vals[i])
					}
					results["deleted"] = append(results["deleted"].([]map[string]interface{}), sanitizeUserMap(userMap))
				}
			}
		}
		// Get approved_by from session
		approvedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				approvedBy = s.Email
				break
			}
		}
		if approvedBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		// Approve users
		if len(toApprove) > 0 {
			appRows, err := db.Query(`
				UPDATE users SET status = 'Approved', approved_by = $1, approved_at = NOW(), approval_comment = $2
				WHERE id = ANY($3)
				  AND EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = users.id AND uem.entity_id = ANY($4::text[]))
				RETURNING *`,
				approvedBy, req.ApprovalComment, pq.Array(toApprove), pq.Array(entityIDs),
			)
			if err == nil {
				defer appRows.Close()
				cols, _ := appRows.Columns()
				for appRows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					appRows.Scan(valPtrs...)
					userMap := map[string]interface{}{}
					for i, col := range cols {
						userMap[col] = decodeSQLValue(vals[i])
					}
					results["approved"] = append(results["approved"].([]map[string]interface{}), sanitizeUserMap(userMap))
				}
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "deleted": results["deleted"], "approved": results["approved"]})
	}
}

// Handler: Reject multiple users
func RejectMultipleUsers(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"` // for middleware
			Ids    []string `json:"ids"`     // users to reject
			// RejectedBy       string   `json:"rejected_by"`
			RejectionComment string `json:"rejection_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.Ids) == 0 {
			respondWithError(w, http.StatusBadRequest, "ids and rejected_by are required")
			return
		}
		entityIDs, _ := r.Context().Value(api.EntityIDsKey).([]string)
		if len(entityIDs) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Get rejected_by from session
		rejectedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				rejectedBy = s.Email
				break
			}
		}
		if rejectedBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		rows, err := db.Query(`
			UPDATE users SET status = 'Rejected', rejected_by = $1, rejected_at = NOW(), approval_comment = $2
			WHERE id = ANY($3)
			  AND EXISTS (SELECT 1 FROM user_entity_mappings uem WHERE uem.user_id = users.id AND uem.entity_id = ANY($4::text[]))
			RETURNING *`,
			rejectedBy, req.RejectionComment, pq.Array(req.Ids), pq.Array(entityIDs),
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		updated := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			rows.Scan(valPtrs...)
			userMap := map[string]interface{}{}
			for i, col := range cols {
				userMap[col] = vals[i]
			}
			updated = append(updated, sanitizeUserMap(userMap))
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}
