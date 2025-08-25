package permissions

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
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

func UpsertRolePermissions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                 `json:"user_id"`
			RoleName string                 `json:"roleName"`
			Pages    map[string]interface{} `json:"pages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.RoleName == "" || req.Pages == nil {
			respondWithError(w, http.StatusBadRequest, "user_id, roleName and pages object required")
			return
		}

		// Step 1: Get role_id
		var roleID int
		err := db.QueryRow("SELECT id FROM roles WHERE name = $1", req.RoleName).Scan(&roleID)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "Role not found")
			return
		}

		// Step 2: Flatten new structure
		perms := []struct {
			Page    string
			Tab     *string
			Action  string
			Allowed interface{}
		}{}
		for page, pageObjRaw := range req.Pages {
			pageObj, ok := pageObjRaw.(map[string]interface{})
			if !ok {
				continue
			}
			// pagePermissions
			if ppRaw, ok := pageObj["pagePermissions"]; ok {
				if pagePermissions, ok := ppRaw.(map[string]interface{}); ok {
					for action, allowed := range pagePermissions {
						perms = append(perms, struct {
							Page    string
							Tab     *string
							Action  string
							Allowed interface{}
						}{Page: page, Tab: nil, Action: action, Allowed: allowed})
					}
				}
			}
			// tabs
			if tabsRaw, ok := pageObj["tabs"]; ok {
				if tabs, ok := tabsRaw.(map[string]interface{}); ok {
					for tab, tabObjRaw := range tabs {
						if tabObj, ok := tabObjRaw.(map[string]interface{}); ok {
							for action, allowed := range tabObj {
								tabStr := tab
								perms = append(perms, struct {
									Page    string
									Tab     *string
									Action  string
									Allowed interface{}
								}{Page: page, Tab: &tabStr, Action: action, Allowed: allowed})
							}
						}
					}
				}
			}
		}

		// Step 3: Get all unique permissions (page, tab, action)
		uniquePermsMap := map[string][3]interface{}{}
		for _, p := range perms {
			key := fmt.Sprintf("%s|%v|%s", p.Page, p.Tab, p.Action)
			uniquePermsMap[key] = [3]interface{}{p.Page, p.Tab, p.Action}
		}
		uniquePermsArr := make([][3]interface{}, 0, len(uniquePermsMap))
		for _, v := range uniquePermsMap {
			uniquePermsArr = append(uniquePermsArr, v)
		}

		// Step 4: Bulk select existing permissions
		permissionIdMap := map[string]int{}
		if len(uniquePermsArr) > 0 {
			values := ""
			args := []interface{}{}
			for i, perm := range uniquePermsArr {
				if i > 0 {
					values += ", "
				}
				values += fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3)
				args = append(args, perm[0], perm[1], perm[2])
			}
			selectQuery := fmt.Sprintf(`SELECT id, page_name, tab_name, action 
				FROM permissions 
				WHERE (page_name, tab_name, action) IN (%s)`, values)
			rows, err := db.Query(selectQuery, args...)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var id int
					var pageName, tabName, action string
					rows.Scan(&id, &pageName, &tabName, &action)
					key := fmt.Sprintf("%s|%s|%s", pageName, tabName, action)
					permissionIdMap[key] = id
				}
			}
		}

		// Step 5: Bulk insert missing permissions
		missingPerms := [][3]interface{}{}
		for _, perm := range uniquePermsArr {
			tabStr := ""
			if perm[1] != nil {
				tabStr = fmt.Sprint(perm[1])
			}
			if _, ok := permissionIdMap[fmt.Sprintf("%s|%s|%s", perm[0], tabStr, perm[2])]; !ok {
				missingPerms = append(missingPerms, perm)
			}
		}
		if len(missingPerms) > 0 {
			values := ""
			args := []interface{}{}
			for i, perm := range missingPerms {
				if i > 0 {
					values += ", "
				}
				values += fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3)
				args = append(args, perm[0], perm[1], perm[2])
			}
			insertQuery := fmt.Sprintf(`INSERT INTO permissions (page_name, tab_name, action) 
				VALUES %s 
				RETURNING id, page_name, tab_name, action`, values)
			rows, err := db.Query(insertQuery, args...)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var id int
					var pageName, tabName, action string
					rows.Scan(&id, &pageName, &tabName, &action)
					key := fmt.Sprintf("%s|%s|%s", pageName, tabName, action)
					permissionIdMap[key] = id
				}
			}
		}

		// Step 6: Deduplicate before bulk upsert role_permissions
		rolePermMap := map[string][]interface{}{}
		for _, p := range perms {
			tabStr := ""
			if p.Tab != nil {
				tabStr = *p.Tab
			}
			permission_id := permissionIdMap[fmt.Sprintf("%s|%s|%s", p.Page, tabStr, p.Action)]
			key := fmt.Sprintf("%d|%d", roleID, permission_id)
			if _, exists := rolePermMap[key]; !exists {
				rolePermMap[key] = []interface{}{roleID, permission_id, p.Allowed}
			}
		}

		rolePermValues := [][]interface{}{}
		for _, v := range rolePermMap {
			rolePermValues = append(rolePermValues, v)
		}

		if len(rolePermValues) > 0 {
			values := ""
			args := []interface{}{}
			for i, v := range rolePermValues {
				if i > 0 {
					values += ", "
				}
				values += fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3)
				args = append(args, v[0], v[1], v[2])
			}
			upsertQuery := fmt.Sprintf(`INSERT INTO role_permissions (role_id, permission_id, allowed) 
				VALUES %s 
				ON CONFLICT (role_id, permission_id) 
				DO UPDATE SET allowed = EXCLUDED.allowed`, values)
			_, err := db.Exec(upsertQuery, args...)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "results": perms})
	}
}

func GetRolePermissionsJson(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse request body
		// Example: user_id from middleware (set in context)
		userID := r.Context().Value("user_id")
		var req struct {
			UserID   string `json:"user_id"`
			RoleName string `json:"roleName"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"success":false,"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}
		// Prefer userID from context if available
		if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
			req.UserID = ctxUserID
		}
		if req.UserID == "" {
			http.Error(w, `{"success":false,"error":"user_id required"}`, http.StatusBadRequest)
			return
		}
		if req.RoleName == "" {
			http.Error(w, `{"success":false,"error":"roleName required"}`, http.StatusBadRequest)
			return
		}

		// Find role
		var roleID int
		err := db.QueryRow("SELECT id FROM roles WHERE name = $1", req.RoleName).Scan(&roleID)
		if err == sql.ErrNoRows {
			http.Error(w, `{"success":false,"error":"Role not found"}`, http.StatusNotFound)
			return
		} else if err != nil {
			http.Error(w, `{"success":false,"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}

		// Query permissions
		rows, err := db.Query(`
			   SELECT p.page_name, p.tab_name, p.action, rp.allowed
			   FROM role_permissions rp
			   JOIN permissions p ON rp.permission_id = p.id
			   WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')`,
			roleID)
		if err != nil {
			http.Error(w, `{"success":false,"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Build pages structure
		pages := make(map[string]interface{})
		for rows.Next() {
			var page, action string
			var tab sql.NullString
			var allowed bool

			if err := rows.Scan(&page, &tab, &action, &allowed); err != nil {
				http.Error(w, `{"success":false,"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
				return
			}

			// Ensure page object
			if _, ok := pages[page]; !ok {
				pages[page] = make(map[string]interface{})
			}
			pageObj := pages[page].(map[string]interface{})

			if !tab.Valid { // tab is NULL
				if _, ok := pageObj["pagePermissions"]; !ok {
					pageObj["pagePermissions"] = make(map[string]interface{})
				}
				pageObj["pagePermissions"].(map[string]interface{})[action] = allowed
			} else {
				if _, ok := pageObj["tabs"]; !ok {
					pageObj["tabs"] = make(map[string]interface{})
				}
				if _, ok := pageObj["tabs"].(map[string]interface{})[tab.String]; !ok {
					pageObj["tabs"].(map[string]interface{})[tab.String] = make(map[string]interface{})
				}
				pageObj["tabs"].(map[string]interface{})[tab.String].(map[string]interface{})[action] = allowed
			}

		}

		// Final response
		resp := struct {
			RoleName string                 `json:"roleName"`
			//    UserID   string                 `json:"user_id"`
			Pages    map[string]interface{} `json:"pages"`
		}{
			RoleName: req.RoleName,
			//    UserID:   req.UserID,
			Pages:    pages,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, `{"success":false,"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		}
	}
}

func UpdateRolePermissionsStatusByName(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get user_id from middleware/context
		userID := r.Context().Value("user_id")
		var req struct {
			UserID   string `json:"user_id"`
			RoleName string `json:"roleName"`
			Status   string `json:"status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		// Prefer userID from context if available
		if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
			req.UserID = ctxUserID
		}
		if req.UserID == "" || req.RoleName == "" || req.Status == "" {
			respondWithError(w, http.StatusBadRequest, "user_id, roleName and status are required")
			return
		}

		// Get role_id
		var roleID int
		err := db.QueryRow("SELECT id FROM roles WHERE name = $1", req.RoleName).Scan(&roleID)
		if err == sql.ErrNoRows {
			respondWithError(w, http.StatusNotFound, "Role not found")
			return
		} else if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Update status for all role_permissions for this role
		rows, err := db.Query(
			"UPDATE role_permissions SET status = $1 WHERE role_id = $2 RETURNING role_id, permission_id, allowed, status",
			req.Status, roleID,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		// Collect updated permissions
		updatedPermissions := []map[string]interface{}{}
		for rows.Next() {
			var  roleID, permissionID int
			var allowed bool
			var status string
			if err := rows.Scan(&roleID, &permissionID, &allowed, &status); err != nil {
				continue
			}
			updatedPermissions = append(updatedPermissions, map[string]interface{}{
				"role_id":       roleID,
				"permission_id": permissionID,
				"allowed":       allowed,
				"status":        status,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":           true,
			// "user_id":           req.UserID,
			"updatedPermissions": updatedPermissions,
		})
	}
}

// Handler: Get distinct role statuses, using user_id from middleware
func GetRolesStatus(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get user_id from middleware/context
		userID := r.Context().Value("user_id")
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			// Prefer userID from context if available
			if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
				req.UserID = ctxUserID
			}
		} else {
			// If body is not valid, still try to get userID from context
			if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
				req.UserID = ctxUserID
			}
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		// Fetch distinct role_id and status from role_permissions
		rows, err := db.Query("SELECT DISTINCT role_id, status FROM role_permissions")
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		rolesStatus := []map[string]interface{}{}
		for rows.Next() {
			var roleID int
			var status string
			if err := rows.Scan(&roleID, &status); err != nil {
				continue
			}
			// Fetch roleName for the roleId
			var roleName string
			err := db.QueryRow("SELECT name FROM roles WHERE id = $1", roleID).Scan(&roleName)
			if err == nil {
				rolesStatus = append(rolesStatus, map[string]interface{}{
					"roleName": roleName,
					"status":   status,
				})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			// "user_id":    req.UserID,
			"rolesStatus": rolesStatus,
		})
	}
}