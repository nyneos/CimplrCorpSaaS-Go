package user

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/utils"
	"database/sql"
	"encoding/json"

	"fmt"
	"net/http"

	"github.com/lib/pq"
)

// Helper: send JSON error response
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

// Handler: Create user
func CreateUser(db *sql.DB) http.HandlerFunc {
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
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		// Get created_by from session
		created_by := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				created_by = s.Email
				break
			}
		}
		if created_by == "" {
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		tx, err := db.Begin()
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback()
		var userId string
		err = tx.QueryRow(`INSERT INTO users (
			authentication_type,
			employee_name,
			username_or_employee_id,
			email,
			mobile,
			address,
			business_unit_name,
			status,
			created_by
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8) RETURNING id`,
			req.AuthenticationType,
			req.EmployeeName,
			req.UsernameOrEmployeeID,
			req.Email,
			req.Mobile,
			req.Address,
			req.BusinessUnitName,
			created_by,
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"user_id": userId,
			"role_id": roleId,
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
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		// Pagination
		pagination, err := utils.ExtractPagination(r)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Count total (respect status filter)
		countQuery := "SELECT COUNT(*) FROM users WHERE business_unit_name = ANY($1)"
		countArgs := []interface{}{pq.Array(buNames)}
		if status != "" {
			countQuery += " AND status = $2"
			countArgs = append(countArgs, status)
		}
		total, _ := utils.CountTotal(db, countQuery, countArgs...)
		pagination.SetPaginationStats(total)

		// Build paginated query
		query := "SELECT * FROM users WHERE business_unit_name = ANY($1)"
		args := []interface{}{pq.Array(buNames)}
		if status != "" {
			query += " AND status = $2"
			args = append(args, status)
		}
		query += " ORDER BY id DESC LIMIT $3 OFFSET $4"
		args = append(args, pagination.Limit, pagination.Offset)

		rows, err := db.Query(query, args...)
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
		w.Header().Set("Content-Type", "application/json")
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
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		// Query for user by ID, restrict to accessible business units
		rows, err := db.Query("SELECT * FROM users WHERE id = $1 AND business_unit_name = ANY($2)", req.UserID, pq.Array(buNames))
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"user":    userMap,
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
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
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
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
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
				fields[k] = v
			}
		}
		fields["updated_by"] = updatedBy
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
		userMap := map[string]interface{}{}
		for i, col := range cols {
			userMap[col] = vals[i]
		}
		w.Header().Set("Content-Type", "application/json")
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
			UserID string `json:"user_id"` // for middleware
			ID     string `json:"id"`      // user to delete
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.ID == "" {
			respondWithError(w, http.StatusBadRequest, "Missing user_id or id in request body")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		rows, err := db.Query(
			"UPDATE users SET status = 'Delete-Approval' WHERE id = $1 AND business_unit_name = ANY($2) RETURNING *",
			req.ID, pq.Array(buNames),
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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
		rows.Scan(valPtrs...)
		userMap := map[string]interface{}{}
		for i, col := range cols {
			userMap[col] = vals[i]
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"user":    userMap,
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
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		// Get existing users and their status
		rows, err := db.Query(
			"SELECT id, status FROM users WHERE id = ANY($1) AND business_unit_name = ANY($2)",
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
			if status == "Delete-Approval" {
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
				respondWithError(w, http.StatusInternalServerError, "Failed to delete user_roles: "+err.Error())
				return
			}
			delRows, err := db.Query(
				"DELETE FROM users WHERE id = ANY($1) AND business_unit_name = ANY($2) RETURNING *",
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
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		// Approve users
		if len(toApprove) > 0 {
			appRows, err := db.Query(
				"UPDATE users SET status = 'Approved', approved_by = $1, approved_at = NOW(), approval_comment = $2 WHERE id = ANY($3) AND business_unit_name = ANY($4) RETURNING *",
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
		w.Header().Set("Content-Type", "application/json")
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
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
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
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		rows, err := db.Query(
			"UPDATE users SET status = 'Rejected', rejected_by = $1, rejected_at = NOW(), approval_comment = $2 WHERE id = ANY($3) AND business_unit_name = ANY($4) RETURNING *",
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}
