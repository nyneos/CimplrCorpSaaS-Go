package role

import (
	// "CimplrCorpSaas/api"
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

// Handler: Approve multiple roles
func ApproveMultipleRoles(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            UserID          string `json:"user_id"`
            RoleIds         []int  `json:"roleIds"`
            ApprovedBy      string `json:"approved_by"`
            ApprovalComment string `json:"approval_comment"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.RoleIds) == 0 || req.ApprovedBy == "" {
            respondWithError(w, http.StatusBadRequest, "user_id, roleIds and approved_by are required")
            return
        }
        // Middleware: check business units
        // Uncomment if you want to restrict by business units
        // buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
        // if !ok || len(buNames) == 0 {
        //     respondWithError(w, http.StatusNotFound, "No accessible business units found")
        //     return
        // }

        // Fetch current statuses
        rows, err := db.Query(`SELECT id, status FROM roles WHERE id = ANY($1)`, pq.Array(req.RoleIds))
        if err != nil {
            respondWithError(w, http.StatusInternalServerError, err.Error())
            return
        }
        defer rows.Close()
        toDelete := []int{}
        toApprove := []int{}
        for rows.Next() {
            var id int
            var status string
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
        // Delete roles
        if len(toDelete) > 0 {
            delRows, err := db.Query(`DELETE FROM roles WHERE id = ANY($1) RETURNING *`, pq.Array(toDelete))
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
                    roleMap := map[string]interface{}{}
                    for i, col := range cols {
                        roleMap[col] = vals[i]
                    }
                    results["deleted"] = append(results["deleted"].([]map[string]interface{}), roleMap)
                }
            }
        }
        // Approve roles
        if len(toApprove) > 0 {
            appRows, err := db.Query(`UPDATE roles SET status = 'approved', approved_by = $1, approved_at = NOW(), approval_comment = $2 WHERE id = ANY($3) RETURNING *`, req.ApprovedBy, req.ApprovalComment, pq.Array(toApprove))
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
                    roleMap := map[string]interface{}{}
                    for i, col := range cols {
                        roleMap[col] = vals[i]
                    }
                    results["approved"] = append(results["approved"].([]map[string]interface{}), roleMap)
                }
            }
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "deleted": results["deleted"], "approved": results["approved"]})
    }
}

// Handler: Delete role (soft delete)
func DeleteRole(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            UserID string `json:"user_id"`
            ID     int    `json:"id"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.ID == 0 {
            respondWithError(w, http.StatusBadRequest, "user_id and id are required")
            return
        }
        // Middleware: check business units
        // buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
        // if !ok || len(buNames) == 0 {
        //     respondWithError(w, http.StatusNotFound, "No accessible business units found")
        //     return
        // }
        rows, err := db.Query(
            "UPDATE roles SET status = 'Delete-Approval' WHERE id = $1 RETURNING *",
            req.ID,
        )
        if err != nil {
            respondWithError(w, http.StatusInternalServerError, err.Error())
            return
        }
        defer rows.Close()
        cols, _ := rows.Columns()
        if !rows.Next() {
            respondWithError(w, http.StatusNotFound, "Role not found")
            return
        }
        vals := make([]interface{}, len(cols))
        valPtrs := make([]interface{}, len(cols))
        for i := range vals {
            valPtrs[i] = &vals[i]
        }
        rows.Scan(valPtrs...)
        roleMap := map[string]interface{}{}
        for i, col := range cols {
            roleMap[col] = vals[i]
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "deleted": roleMap})
    }
}

// Handler: Reject multiple roles
func RejectMultipleRoles(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            UserID           string `json:"user_id"`
            RoleIds          []int  `json:"roleIds"`
            RejectedBy       string `json:"rejected_by"`
            RejectionComment string `json:"rejection_comment"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.RoleIds) == 0 || req.RejectedBy == "" {
            respondWithError(w, http.StatusBadRequest, "user_id, roleIds and rejected_by are required")
            return
        }
        // Middleware: check business units
        // buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
        // if !ok || len(buNames) == 0 {
        //     respondWithError(w, http.StatusNotFound, "No accessible business units found")
        //     return
        // }
        rows, err := db.Query(
            `UPDATE roles SET status = 'Rejected', approved_by = $1, approved_at = NOW(), approval_comment = $2 WHERE id = ANY($3) RETURNING *`,
            req.RejectedBy, req.RejectionComment, pq.Array(req.RoleIds),
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
            roleMap := map[string]interface{}{}
            for i, col := range cols {
                roleMap[col] = vals[i]
            }
            updated = append(updated, roleMap)
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
    }
}

// Handler: Get just role names
func GetJustRoles(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Optionally, get user_id from body for middleware
        var req struct {
            UserID string `json:"user_id"`
        }
        _ = json.NewDecoder(r.Body).Decode(&req) // Not required for this query

    rows, err := db.Query("SELECT DISTINCT name FROM roles WHERE status = 'approved' OR status = 'Approved'")
        if err != nil {
            respondWithError(w, http.StatusInternalServerError, err.Error())
            return
        }
        defer rows.Close()
        roleNames := []string{}
        for rows.Next() {
            var name string
            rows.Scan(&name)
            roleNames = append(roleNames, name)
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "roles": roleNames})
    }
}

func GetPendingRoles(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            UserID string `json:"user_id"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
            respondWithError(w, http.StatusBadRequest, "Missing user_id in request body")
            return
        }

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

        // Get all pending roles
        rows, err := db.Query("SELECT * FROM roles WHERE status IN ($1, $2, $3)", "pending", "Awaiting-Approval", "Delete-Approval")
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

// Handler: Update role
func UpdateRole(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req map[string]interface{}
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            respondWithError(w, http.StatusBadRequest, "Invalid request body")
            return
        }
        // Extract id and user_id safely
        idVal, ok := req["id"]
        userIDVal, okID := req["user_id"]
        if !ok || !okID {
            respondWithError(w, http.StatusBadRequest, "Missing id or user_id in request body")
            return
        }
        id := fmt.Sprint(idVal)
        userID := fmt.Sprint(userIDVal)
        if id == "" || userID == "" {
            respondWithError(w, http.StatusBadRequest, "Invalid id or user_id")
            return
        }
        // Middleware: check business units (uncomment if needed)
        // buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
        // if !ok || len(buNames) == 0 {
        //     respondWithError(w, http.StatusNotFound, "No accessible business units found")
        //     return
        // }
        // Prepare fields for update
        fields := map[string]interface{}{}
        for k, v := range req {
            if k == "id" || k == "user_id" {
                continue
            }
            if v != nil && fmt.Sprint(v) != "" {
                fields[k] = v
            }
        }
        // Always set status to Awaiting-Approval
        fields["status"] = "Awaiting-Approval"
        if len(fields) == 0 {
            respondWithError(w, http.StatusBadRequest, "No fields to update")
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
        // WHERE clause for id
        query := fmt.Sprintf(
            "UPDATE roles SET %s WHERE id = $%d RETURNING *",
            setClause, len(keys)+1,
        )
        values = append(values, id)
        // Execute query and fetch row(s)
        rows, err := db.Query(query, values...)
        if err != nil {
            respondWithError(w, http.StatusBadRequest, err.Error())
            return
        }
        defer rows.Close()
        cols, _ := rows.Columns()
        if !rows.Next() {
            respondWithError(w, http.StatusNotFound, "Role not found")
            return
        }
        vals := make([]interface{}, len(cols))
        valPtrs := make([]interface{}, len(cols))
        for i := range vals {
            valPtrs[i] = &vals[i]
        }
        rows.Scan(valPtrs...)
        roleMap := map[string]interface{}{}
        for i, col := range cols {
            roleMap[col] = vals[i]
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
            "success": true,
            "role":    roleMap,
        })
    }
}