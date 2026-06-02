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
// This is a second-line-of-defence deny-list; the primary defence is the
// explicit column list used in every SELECT on the users table.
var sensitiveUserCols = map[string]bool{
	"password":    true,
	"mfa_secret":  true, // raw TOTP seed — must never leave the server
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
	respondWithError(w, http.StatusInternalServerError, constants.ErrInternalServer)
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

const userEntityAccessExistsSQL = `
	EXISTS (
		SELECT 1 FROM user_entity_mappings uem
		WHERE uem.user_id = u.id
		  AND COALESCE(uem.is_deleted, false) = false
		  AND uem.entity_id = ANY($%d::text[])
	)`

const usersTableEntityAccessExistsSQL = `
	EXISTS (
		SELECT 1 FROM user_entity_mappings uem
		WHERE uem.user_id = users.id
		  AND COALESCE(uem.is_deleted, false) = false
		  AND uem.entity_id = ANY($%d::text[])
	)`

const activeUserRoleLateralSQL = `
LEFT JOIN LATERAL (
	SELECT r.name AS role_name,
	       COALESCE(r.status, '') AS role_status,
	       COALESCE(r.roles_permission_status, '') AS role_permission_status,
	       COALESCE(r.role_code, r.rolecode) AS role_code
	FROM user_roles ur
	JOIN roles r ON ur.role_id = r.id
	WHERE ur.user_id = u.id
	  AND COALESCE(ur.is_deleted, false) = false
	ORDER BY ur.approved_at DESC NULLS LAST, ur.role_id DESC
	LIMIT 1
) rr ON true`

func normalizeUserIdentifier(value string) string {
	return strings.TrimSpace(value)
}

func entityIDSet(entityIDs []string) map[string]bool {
	set := make(map[string]bool, len(entityIDs))
	for _, id := range entityIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			set[id] = true
		}
	}
	return set
}

func prevalidationEntityScope(ctx context.Context) ([]string, bool) {
	entityIDs := api.GetEntityIDsFromCtx(ctx)
	isAdminOverride, _ := ctx.Value("is_admin_override").(bool)
	return entityIDs, isAdminOverride
}

func requirePrevalidationEntityScope(ctx context.Context) ([]string, bool, int, string) {
	entityIDs, isAdminOverride := prevalidationEntityScope(ctx)
	if len(entityIDs) == 0 && !isAdminOverride {
		return nil, false, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit
	}
	return entityIDs, isAdminOverride, 0, ""
}

func validateEntityMappingsInScope(
	entityIDs []string,
	mappings []struct{ EntityID, EntityName string },
) (int, string) {
	accessible := entityIDSet(entityIDs)
	if len(accessible) == 0 {
		return 0, ""
	}
	valid := 0
	for _, em := range mappings {
		entityID := strings.TrimSpace(em.EntityID)
		if entityID == "" {
			continue
		}
		if !accessible[entityID] {
			label := strings.TrimSpace(em.EntityName)
			if label == "" {
				label = entityID
			}
			return http.StatusBadRequest, fmt.Sprintf("Entity '%s' is outside your accessible scope", label)
		}
		valid++
	}
	if valid == 0 {
		return http.StatusBadRequest, "At least one accessible entity mapping is required"
	}
	return 0, ""
}

func upsertActiveUserRole(exec sqlExecutor, userID, roleName string) error {
	roleName = strings.TrimSpace(roleName)
	if roleName == "" {
		return nil
	}

	var roleID string
	err := exec.QueryRow(
		`SELECT id FROM roles WHERE name = $1 OR rolecode = $1 OR role_code = $1 LIMIT 1`,
		roleName,
	).Scan(&roleID)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("role '%s' not found in roles table", roleName)
		}
		return err
	}

	// 1. Clean up any soft-deleted roles for this user first.
	// This prevents the unique constraint error when updating the active row to a role_id that was previously soft-deleted.
	if _, err := exec.Exec(`DELETE FROM user_roles WHERE user_id = $1 AND is_deleted = true`, userID); err != nil {
		return err
	}

	// 2. Update the single existing active row to the new role
	updated, err := exec.Exec(
		`UPDATE user_roles SET role_id = $2, is_deleted = false WHERE user_id = $1`,
		userID, roleID,
	)
	if err != nil {
		return err
	}

	// 3. If the user had no active row to update, insert one
	if rows, _ := updated.RowsAffected(); rows == 0 {
		if _, err := exec.Exec(
			`INSERT INTO user_roles (user_id, role_id, is_deleted) VALUES ($1, $2, false)`,
			userID, roleID,
		); err != nil {
			return err
		}
	}
	return nil
}

type sqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
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
		createdBy := api.GetUserEmailFromCtx(r.Context())
		if createdBy == "" {
			createdBy = api.RequestedByFromCtx(r.Context(), api.GetUserIDFromCtx(r.Context()))
		}
		if createdBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		entityIDs, isAdminOverride, statusCode, errMsg := requirePrevalidationEntityScope(r.Context())
		if statusCode != 0 {
			respondWithError(w, statusCode, errMsg)
			return
		}

		req.UsernameOrEmployeeID = normalizeUserIdentifier(req.UsernameOrEmployeeID)
		req.Email = normalizeUserIdentifier(strings.ToLower(req.Email))
		req.EmployeeName = normalizeUserIdentifier(req.EmployeeName)
		req.Role = normalizeUserIdentifier(req.Role)
		if req.UsernameOrEmployeeID == "" || req.Email == "" {
			respondWithError(w, http.StatusBadRequest, "Username and email are required")
			return
		}

		validEntityMappings := make([]struct{ EntityID, EntityName string }, 0, len(req.EntityMappings))
		for _, em := range req.EntityMappings {
			entityID := strings.TrimSpace(em.EntityID)
			if entityID == "" {
				continue
			}
			validEntityMappings = append(validEntityMappings, struct {
				EntityID   string
				EntityName string
			}{entityID, strings.TrimSpace(em.EntityName)})
		}
		if !isAdminOverride {
			if statusCode, errMsg := validateEntityMappingsInScope(entityIDs, validEntityMappings); statusCode != 0 {
				respondWithError(w, statusCode, errMsg)
				return
			}
		} else if len(validEntityMappings) == 0 {
			respondWithError(w, http.StatusBadRequest, "At least one entity mapping is required")
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
		ctx := r.Context()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer tx.Rollback()

		// Uniqueness checks: ensure username_or_employee_id and email are unique (exclude soft-deleted users)
		var existingID string
		var existingUsername, existingEmail string
		err = tx.QueryRow(`
			SELECT id, username_or_employee_id, email
			FROM users
			WHERE COALESCE(is_deleted, false) = false
			  AND (
			    TRIM(username_or_employee_id) = $1
			    OR LOWER(TRIM(email)) = $2
			  )
			LIMIT 1`,
			req.UsernameOrEmployeeID, req.Email,
		).Scan(&existingID, &existingUsername, &existingEmail)
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
			respondWithError(w, http.StatusInternalServerError, constants.ErrInternalServer)
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
		err = tx.QueryRow(`SELECT id FROM roles WHERE name = $1 OR rolecode = $1 OR role_code = $1 LIMIT 1`, req.Role).Scan(&roleId)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Role '"+req.Role+"' not found in roles table")
			return
		}
		if _, err = tx.Exec(
			`UPDATE user_roles SET is_deleted = true WHERE user_id = $1 AND COALESCE(is_deleted, false) = false`,
			userId,
		); err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		_, err = tx.Exec(
			`INSERT INTO user_roles (user_id, role_id, is_deleted) VALUES ($1, $2, false)`,
			userId, roleId,
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		// Insert entity mappings
		for _, em := range validEntityMappings {
			_, err = tx.Exec(`
				INSERT INTO user_entity_mappings (user_id, entity_id, entity_name, is_deleted)
				VALUES ($1, $2, $3, false)
				ON CONFLICT (user_id, entity_id) DO UPDATE SET entity_name = EXCLUDED.entity_name, is_deleted = false`,
				userId, em.EntityID, em.EntityName,
			)
			if err != nil {
				log.Println("[ERROR] user entity mapping insert:", err)
				respondWithError(w, http.StatusInternalServerError, constants.ErrInternalServer)
				return
			}
		}
		if err := tx.Commit(); err != nil {
			respondWithInternalError(w, err)
			return
		}

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
			if api.GetUserIDFromCtx(r.Context()) == "" {
				respondWithError(w, http.StatusBadRequest, "Please login to continue.")
				return
			}
		}
		entityIDs, isAdminOverride, statusCode, errMsg := requirePrevalidationEntityScope(r.Context())
		if statusCode != 0 {
			respondWithError(w, statusCode, errMsg)
			return
		}
		_ = isAdminOverride
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
			COALESCE(u.is_deleted, false) = false AND` + fmt.Sprintf(userEntityAccessExistsSQL, 1)
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
FROM users u` + activeUserRoleLateralSQL + `
LEFT JOIN LATERAL (
	SELECT COALESCE(
			json_agg(json_build_object('entity_id', entity_id, 'entity_name', entity_name)),
			'[]'::json
		) AS entity_mappings
	FROM user_entity_mappings WHERE user_id = u.id AND COALESCE(is_deleted, false) = false
) em ON true
WHERE COALESCE(u.is_deleted, false) = false AND` + fmt.Sprintf(userEntityAccessExistsSQL, 1)
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Missing or invalid user_id in request body")
			return
		}
		if strings.TrimSpace(req.UserID) == "" {
			req.UserID = api.GetUserIDFromCtx(r.Context())
		}
		if strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, "Missing or invalid user_id in request body")
			return
		}
		entityIDs, _, statusCode, errMsg := requirePrevalidationEntityScope(r.Context())
		if statusCode != 0 {
			respondWithError(w, statusCode, errMsg)
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
				FROM user_entity_mappings
				WHERE user_id = u.id AND COALESCE(is_deleted, false) = false
			) em ON true
			WHERE u.id = $1 AND COALESCE(u.is_deleted, false) = false
			  AND EXISTS (
				SELECT 1 FROM user_entity_mappings uem
				WHERE uem.user_id = u.id
				  AND COALESCE(uem.is_deleted, false) = false
				  AND uem.entity_id = ANY($2::text[])
			  )`,
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
			if api.GetUserIDFromCtx(r.Context()) == "" {
				respondWithError(w, http.StatusBadRequest, "Missing user_id in request body")
				return
			}
		}

		entityIDs, _, statusCode, errMsg := requirePrevalidationEntityScope(r.Context())
		if statusCode != 0 {
			respondWithError(w, statusCode, errMsg)
			return
		}

		entityAccessExists := `
				EXISTS (
					SELECT 1 FROM user_entity_mappings uem
					WHERE uem.user_id = u.id
					  AND COALESCE(uem.is_deleted, false) = false
					  AND uem.entity_id = ANY($1::text[])
				)`
		var rows *sql.Rows
		var err error
		if req.EntityName != "" {
			rows, err = db.Query(`
				SELECT * FROM users u
				WHERE `+entityAccessExists+`
				  AND EXISTS (
					SELECT 1 FROM user_entity_mappings uem
					JOIN masterentitycash ec ON ec.entity_id::text = uem.entity_id
					WHERE uem.user_id = u.id
					  AND COALESCE(uem.is_deleted, false) = false
					  AND uem.entity_id = ANY($1::text[])
					  AND UPPER(TRIM(ec.entity_name)) = UPPER(TRIM($2))
				  )
				  AND LOWER(TRIM(u.status)) = 'approved'
				  AND COALESCE(u.is_deleted, false) = false
				ORDER BY u.id DESC
			`, pq.Array(entityIDs), req.EntityName)
		} else {
			rows, err = db.Query(`
				SELECT * FROM users u
				WHERE `+entityAccessExists+`
				  AND LOWER(TRIM(u.status)) = 'approved'
				  AND COALESCE(u.is_deleted, false) = false
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
		id, _ := req["id"].(string)
		id = strings.TrimSpace(id)
		if id == "" {
			respondWithError(w, http.StatusBadRequest, "Missing id")
			return
		}

		entityIDs, isAdminOverride, statusCode, errMsg := requirePrevalidationEntityScope(r.Context())
		if statusCode != 0 {
			respondWithError(w, statusCode, errMsg)
			return
		}

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
						if strings.TrimSpace(eid) != "" {
							newEntityMappings = append(newEntityMappings, struct{ EntityID, EntityName string }{
								strings.TrimSpace(eid), strings.TrimSpace(ename),
							})
						}
					}
				}
			}
			delete(req, "entity_mappings")
		}

		var newRole string
		if roleVal, ok := req["role"].(string); ok && strings.TrimSpace(roleVal) != "" {
			newRole = strings.TrimSpace(roleVal)
		} else if roleNameVal, ok := req["role_name"].(string); ok && strings.TrimSpace(roleNameVal) != "" {
			newRole = strings.TrimSpace(roleNameVal)
		}
		for _, metaKey := range []string{
			"role", "role_name", "role_code", "role_status", "role_permission_status",
			"status_change_request", "reason", "sr_no", "created_at", "updated_at",
			"approved_at", "approved_by", "rejected_at", "rejected_by", "approval_comment",
			"user_id",
		} {
			delete(req, metaKey)
		}

		if len(newEntityMappings) > 0 && !isAdminOverride {
			if statusCode, errMsg := validateEntityMappingsInScope(entityIDs, newEntityMappings); statusCode != 0 {
				respondWithError(w, statusCode, errMsg)
				return
			}
		}

		updatedBy := api.GetUserEmailFromCtx(r.Context())
		if updatedBy == "" {
			updatedBy = api.RequestedByFromCtx(r.Context(), api.GetUserIDFromCtx(r.Context()))
		}
		if updatedBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

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
		fields["updated_at"] = time.Now()
		if _, hasStatus := fields["status"]; !hasStatus {
			fields["status"] = "pending"
		}
		if len(fields) == 0 && newRole == "" && len(newEntityMappings) == 0 {
			respondWithError(w, http.StatusBadRequest, "No valid fields to update")
			return
		}

		var userMap map[string]interface{}
		if len(fields) > 0 {
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
				`UPDATE users SET %s WHERE id = $%d AND`+fmt.Sprintf(usersTableEntityAccessExistsSQL, len(keys)+2)+`
				RETURNING *`,
				setClause, len(keys)+1,
			)
			values = append(values, id, pq.Array(entityIDs))
			rows, err := db.Query(query, values...)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, err.Error())
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
			userMap = map[string]interface{}{}
			for i, col := range cols {
				userMap[col] = decodeSQLValue(vals[i])
			}
		} else {
			var count int
			checkQuery := `SELECT COUNT(*) FROM users u WHERE u.id = $1 AND COALESCE(u.is_deleted, false) = false AND` +
				fmt.Sprintf(userEntityAccessExistsSQL, 2)
			if err := db.QueryRow(checkQuery, id, pq.Array(entityIDs)).Scan(&count); err != nil || count == 0 {
				respondWithError(w, http.StatusNotFound, "User not found or not accessible")
				return
			}
		}

		if len(newEntityMappings) > 0 {
			tx, err := db.BeginTx(r.Context(), nil)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to start transaction: "+err.Error())
				return
			}
			defer tx.Rollback()

			if _, err := tx.Exec("UPDATE user_entity_mappings SET is_deleted = true WHERE user_id = $1", id); err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to soft-delete old mappings: "+err.Error())
				return
			}

			for _, em := range newEntityMappings {
				if _, err := tx.Exec(`
					INSERT INTO user_entity_mappings (user_id, entity_id, entity_name, is_deleted)
					VALUES ($1, $2, $3, false)
					ON CONFLICT (user_id, entity_id) DO UPDATE SET entity_name = EXCLUDED.entity_name, is_deleted = false`,
					id, em.EntityID, em.EntityName,
				); err != nil {
					respondWithError(w, http.StatusInternalServerError, "Failed to insert entity mapping: "+err.Error())
					return
				}
			}

			if err := tx.Commit(); err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to commit entity mappings: "+err.Error())
				return
			}
		}

		if newRole != "" {
			if err := upsertActiveUserRole(db, id, newRole); err != nil {
				respondWithError(w, http.StatusBadRequest, err.Error())
				return
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
		entityIDs, _, statusCode, errMsg := requirePrevalidationEntityScope(r.Context())
		if statusCode != 0 {
			respondWithError(w, statusCode, errMsg)
			return
		}
		deleter := api.GetUserEmailFromCtx(r.Context())
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
			  AND EXISTS (
				SELECT 1 FROM user_entity_mappings uem
				WHERE uem.user_id = users.id
				  AND COALESCE(uem.is_deleted, false) = false
				  AND uem.entity_id = ANY($3::text[])
			  )
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
		entityIDs, _, statusCode, errMsg := requirePrevalidationEntityScope(r.Context())
		if statusCode != 0 {
			respondWithError(w, statusCode, errMsg)
			return
		}
		// Get existing users and their status
		rows, err := db.Query(`
			SELECT id, status FROM users u WHERE id = ANY($1)
			  AND EXISTS (
				SELECT 1 FROM user_entity_mappings uem
				WHERE uem.user_id = u.id
				  AND COALESCE(uem.is_deleted, false) = false
				  AND uem.entity_id = ANY($2::text[])
			  )`,
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
				"UPDATE user_roles SET is_deleted = true WHERE user_id = ANY($1)",
				pq.Array(toDelete),
			)
			if err != nil {
				log.Println("[ERROR] delete user_roles:", err)
				respondWithError(w, http.StatusInternalServerError, constants.ErrInternalServer)
				return
			}
			// Soft delete users (set is_deleted = true)
			delRows, err := db.Query(`
				UPDATE users SET is_deleted = true, status = 'Deleted', updated_at = NOW()
				WHERE id = ANY($1)
				  AND EXISTS (
					SELECT 1 FROM user_entity_mappings uem
					WHERE uem.user_id = users.id
					  AND COALESCE(uem.is_deleted, false) = false
					  AND uem.entity_id = ANY($2::text[])
				  )
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
		approvedBy := api.GetUserEmailFromCtx(r.Context())
		if approvedBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		// Approve users
		if len(toApprove) > 0 {
			appRows, err := db.Query(`
				UPDATE users SET status = 'Approved', approved_by = $1, approved_at = NOW(), approval_comment = $2
				WHERE id = ANY($3)
				  AND EXISTS (
					SELECT 1 FROM user_entity_mappings uem
					WHERE uem.user_id = users.id
					  AND COALESCE(uem.is_deleted, false) = false
					  AND uem.entity_id = ANY($4::text[])
				  )
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
		entityIDs, _, statusCode, errMsg := requirePrevalidationEntityScope(r.Context())
		if statusCode != 0 {
			respondWithError(w, statusCode, errMsg)
			return
		}
		rejectedBy := api.GetUserEmailFromCtx(r.Context())
		if rejectedBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		rows, err := db.Query(`
			UPDATE users SET status = 'Rejected', rejected_by = $1, rejected_at = NOW(), approval_comment = $2
			WHERE id = ANY($3)
			  AND EXISTS (
				SELECT 1 FROM user_entity_mappings uem
				WHERE uem.user_id = users.id
				  AND COALESCE(uem.is_deleted, false) = false
				  AND uem.entity_id = ANY($4::text[])
			  )
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
