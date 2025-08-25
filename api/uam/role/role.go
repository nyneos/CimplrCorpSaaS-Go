package role

// role.go: Handles role management logic and APIs.

import (
	// "CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
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

// Handler: Create role
func CreateRole(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name                string `json:"name"`
			RoleCode            string `json:"rolecode"`
			Description         string `json:"description"`
			OfficeStartTimeIST  string `json:"office_start_time_ist"`
			OfficeEndTimeIST    string `json:"office_end_time_ist"`
			CreatedBy           string `json:"created_by"`
			UserID              string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		if req.Name == "" || req.RoleCode == "" || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "name, rolecode, and user_id are required")
			return
		}
		   // Insert role (no business_unit_name)
		   _, err := db.Exec(
			   `INSERT INTO roles (name, rolecode, description, office_start_time_ist, office_end_time_ist, status, created_by) VALUES ($1, $2, $3, $4, $5, 'pending', $6)`,
			   req.Name,
			   req.RoleCode,
			   req.Description,
			   req.OfficeStartTimeIST,
			   req.OfficeEndTimeIST,
			   req.CreatedBy,
		   )
		   if err != nil {
			   respondWithError(w, http.StatusInternalServerError, err.Error())
			   return
		   }
		   // Scan result dynamically
		   // Use sql.Rows to get columns and scan
		   rows, err := db.Query(
			   `SELECT * FROM roles WHERE id = (SELECT currval(pg_get_serial_sequence('roles','id')))`,
		   )
		   if err != nil {
			   respondWithError(w, http.StatusInternalServerError, err.Error())
			   return
		   }
		   defer rows.Close()
		   cols, err := rows.Columns()
		   if err != nil {
			   respondWithError(w, http.StatusInternalServerError, err.Error())
			   return
		   }
		   vals := make([]interface{}, len(cols))
		   valPtrs := make([]interface{}, len(cols))
		   for i := range vals {
			   valPtrs[i] = &vals[i]
		   }
		   var roleMap map[string]interface{} = map[string]interface{}{}
		   if rows.Next() {
			   if err := rows.Scan(valPtrs...); err != nil {
				   respondWithError(w, http.StatusBadRequest, err.Error())
				   return
			   }
			   for i, col := range cols {
				   roleMap[col] = vals[i]
			   }
		   }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"role":    roleMap,
		})
	}
}

func GetRolesPageData(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            UserID string `json:"user_id"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
            respondWithError(w, http.StatusBadRequest, "Missing user_id in request body")
            return
        }

        // Get business units from context (set by middleware)
        // buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
        // if !ok || len(buNames) == 0 {
        //     respondWithError(w, http.StatusNotFound, "No accessible business units found")
        //     return
        // }

        // Get role_id from user_roles for user_id
        rolesPerms := map[string]interface{}{}
        var roleId int
        err := db.QueryRow("SELECT role_id FROM user_roles WHERE user_id = $1 LIMIT 1", req.UserID).Scan(&roleId)
        if err == nil {
            permRows, err := db.Query(`
                SELECT p.page_name, p.tab_name, p.action, rp.allowed
                FROM role_permissions rp
                JOIN permissions p ON rp.permission_id = p.id
                WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')
            `, roleId)
            if err == nil {
                defer permRows.Close()
                pagePermissions := map[string]interface{}{}
                tabs := map[string]map[string]interface{}{}
                for permRows.Next() {
                    var pageName, tabName, action string
                    var allowed interface{}
                    permRows.Scan(&pageName, &tabName, &action, &allowed)
                    if pageName != "roles" {
                        continue
                    }
                    if tabName == "" {
                        pagePermissions[action] = allowed
                    } else {
                        if _, ok := tabs[tabName]; !ok {
                            tabs[tabName] = map[string]interface{}{}
                        }
                        tabs[tabName][action] = allowed
                    }
                }
                rolesPerms["pagePermissions"] = pagePermissions
                rolesPerms["tabs"] = tabs
            }
        }

        // Get all roles
        rows, err := db.Query("SELECT * FROM roles")
        if err != nil {
            respondWithError(w, http.StatusInternalServerError, err.Error())
            return
        }
        defer rows.Close()
        cols, _ := rows.Columns()
        roleData := []map[string]interface{}{}
        for rows.Next() {
            vals := make([]interface{}, len(cols))
            valPtrs := make([]interface{}, len(cols))
            for i := range vals {
                valPtrs[i] = &vals[i]
            }
            rows.Scan(valPtrs...)
            rMap := map[string]interface{}{}
            for i, col := range cols {
                rMap[col] = vals[i]
            }
            // Map fields as needed
            role := map[string]interface{}{
                "id": rMap["id"],
                "name": rMap["name"],
                "role_code": rMap["role_code"],
                "description": rMap["description"],
                "startTime": rMap["office_start_time_ist"],
                "endTime": rMap["office_end_time_ist"],
                "createdAt": rMap["created_at"],
                "status": rMap["status"],
                "createdBy": rMap["created_by"],
                "approvedBy": rMap["approved_by"],
                "approveddate": rMap["approved_at"],
            }
            roleData = append(roleData, role)
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
            "permissions": rolesPerms,
            "roleData":    roleData,
        })
    }
}
