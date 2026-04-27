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

// Helper: send JSON error response
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Printf("[ERROR: %v] [UAM] [User] request failed", errMsg)
	clientMsg := "request failed"
	switch status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		clientMsg = "invalid request"
	case http.StatusUnauthorized:
		clientMsg = "unauthorized"
	case http.StatusNotFound:
		clientMsg = "not found"
	default:
		if status >= http.StatusInternalServerError {
			clientMsg = "internal server error"
		}
	}
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   clientMsg,
	})
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
			BusinessUnitName     string `json:"business_unit_name"`
			UserID               string `json:"user_id"`
			// Password provisioning
			PasswordType        string `json:"password_type"`         // "auto" | "custom"
			Password            string `json:"password"`              // plain-text for "custom"
			ForcePasswordChange bool   `json:"force_password_change"` // require change on first login
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
			// }
			cimplr = "changeme"
		}
		hashedPassword, hashErr := bcrypt.GenerateFromPassword([]byte(cimplr), bcrypt.DefaultCost)
		if hashErr != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to hash password")
			return
		}
		// -------------------------------------------------------------------------

		tx, err := db.Begin()
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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
			respondWithError(w, http.StatusInternalServerError, constants.ErrFailedToValidateUniqueness+err.Error())
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
				business_unit_name,
				password,
				force_password_change,
				status,
				created_at,
				created_by
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', NOW(), $10) RETURNING id`,
			req.AuthenticationType,
			req.EmployeeName,
			req.UsernameOrEmployeeID,
			req.Email,
			req.Mobile,
			req.Address,
			req.BusinessUnitName,
			string(hashedPassword),
			req.ForcePasswordChange,
			createdBy,
		).Scan(&userId)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
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
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
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
		// Get business units from context (set by middleware)
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Pagination
		pagination, err := utils.ExtractPagination(r)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Count total (respect status filter) - build paramized query with dynamic placeholders
		countArgs := []interface{}{pq.Array(buNames)}
		paramIdx := 2
		countQuery := "SELECT COUNT(*) FROM users WHERE business_unit_name = ANY($1::text[])"
		if status != "" {
			countQuery += fmt.Sprintf(" AND status = $%d", paramIdx)
			countArgs = append(countArgs, status)
			paramIdx++
		}
		total, _ := utils.CountTotal(db, countQuery, countArgs...)
		pagination.SetPaginationStats(total)

		// Build paginated query with dynamic parameter positions.
		// Fetch role info via LEFT JOIN LATERAL to avoid per-row queries.
		args := []interface{}{pq.Array(buNames)}
		pIdx := 2
		query := `SELECT u.*,
	COALESCE(rr.role_name, '') AS role_name,
	COALESCE(rr.role_status, '') AS role_status,
	COALESCE(rr.role_permission_status, '') AS role_permission_status,
	COALESCE(rr.role_code, '') AS role_code
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
WHERE u.business_unit_name = ANY($1::text[])`
		if status != "" {
			query += fmt.Sprintf(" AND u.status = $%d", pIdx)
			args = append(args, status)
			pIdx++
		}
		// limit and offset take the next two parameter positions
		// Order by the latest of created_at, approved_at, updated_at or rejected_at so
		// recently created/approved/updated/rejected users surface first.
		query += fmt.Sprintf(" ORDER BY GREATEST(COALESCE(u.created_at, '1970-01-01'::timestamp), COALESCE(u.approved_at, '1970-01-01'::timestamp), COALESCE(u.updated_at, '1970-01-01'::timestamp), COALESCE(u.rejected_at, '1970-01-01'::timestamp)) DESC LIMIT $%d OFFSET $%d", pIdx, pIdx+1)
		args = append(args, pagination.Limit, pagination.Offset)

		rows, err := db.QueryContext(r.Context(), query, args...)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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
				rowMap[col] = vals[i]
			}
			users = append(users, rowMap)
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
		// Get business units from context (set by middleware)
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Query for user by ID, restrict to accessible business units
		rows, err := db.QueryContext(r.Context(), "SELECT * FROM users WHERE id = $1 AND business_unit_name = ANY($2::text[])", req.UserID, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			respondWithError(w, http.StatusNotFound, "User not found")
			return
		}
		userMap := map[string]interface{}{}
		for i, col := range cols {
			userMap[col] = vals[i]
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"user":    userMap,
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

		// Get business units from middleware
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Build query — if entity_name is supplied, narrow down to that specific entity
		// on top of the middleware-provided BU list.
		var rows *sql.Rows
		var err error
		if req.EntityName != "" {
			rows, err = db.QueryContext(r.Context(), `
				SELECT * FROM users
				WHERE business_unit_name = ANY($1::text[])
				  AND business_unit_name = $2
				  AND LOWER(TRIM(status)) = 'approved'
				ORDER BY id DESC
			`, pq.Array(buNames), req.EntityName)
		} else {
			rows, err = db.QueryContext(r.Context(), `
				SELECT * FROM users
				WHERE business_unit_name = ANY($1::text[])
				  AND LOWER(TRIM(status)) = 'approved'
				ORDER BY id DESC
			`, pq.Array(buNames))
		}

		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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
				userMap[col] = vals[i]
			}

			users = append(users, userMap)
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
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
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
			"business_unit_name":      true,
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
		if _, hasStatus := fields["status"]; hasStatus {
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
			"UPDATE users SET %s WHERE id = $%d AND business_unit_name = ANY($%d) RETURNING *",
			setClause, len(keys)+1, len(keys)+2,
		)
		values = append(values, id, pq.Array(buNames))
		rows, err := db.QueryContext(r.Context(), query, values...)
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
		userMap := map[string]interface{}{}
		for i, col := range cols {
			userMap[col] = vals[i]
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "user": userMap})
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
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
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

		rows, err := db.QueryContext(r.Context(),
			"UPDATE users SET status = 'Delete-Approval', updated_by = $1, updated_at = NOW() WHERE id = ANY($2) AND business_unit_name = ANY($3::text[]) RETURNING *",
			deleter, pq.Array(targetIds), pq.Array(buNames),
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Get existing users and their status
		rows, err := db.QueryContext(r.Context(),
			"SELECT id, status FROM users WHERE id = ANY($1) AND business_unit_name = ANY($2::text[])",
			pq.Array(req.Ids), pq.Array(buNames),
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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
			_, err := db.ExecContext(r.Context(),
				"DELETE FROM user_roles WHERE user_id = ANY($1)",
				pq.Array(toDelete),
			)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to delete user_roles: "+err.Error())
				return
			}
			delRows, err := db.QueryContext(r.Context(),
				"DELETE FROM users WHERE id = ANY($1) AND business_unit_name = ANY($2::text[]) RETURNING *",
				pq.Array(toDelete), pq.Array(buNames),
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
						userMap[col] = vals[i]
					}
					results["deleted"] = append(results["deleted"].([]map[string]interface{}), userMap)
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
			appRows, err := db.QueryContext(r.Context(),
				"UPDATE users SET status = 'Approved', approved_by = $1, approved_at = NOW(), approval_comment = $2 WHERE id = ANY($3) AND business_unit_name = ANY($4::text[]) RETURNING *",
				approvedBy, req.ApprovalComment, pq.Array(toApprove), pq.Array(buNames),
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
						userMap[col] = vals[i]
					}
					results["approved"] = append(results["approved"].([]map[string]interface{}), userMap)
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
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
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
		rows, err := db.QueryContext(r.Context(),
			"UPDATE users SET status = 'Rejected', rejected_by = $1, rejected_at = NOW(), approval_comment = $2 WHERE id = ANY($3) AND business_unit_name = ANY($4::text[]) RETURNING *",
			rejectedBy, req.RejectionComment, pq.Array(req.Ids), pq.Array(buNames),
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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
			updated = append(updated, userMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}
