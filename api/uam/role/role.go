package role

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/utils"

	"github.com/lib/pq"
)

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
	log.Println("[ERROR] role:", err)
	respondWithError(w, http.StatusInternalServerError, "Internal server error")
}

// Handler: Create role
func CreateRole(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name               string `json:"name"`
			RoleCode           string `json:"rolecode"`
			Description        string `json:"description"`
			OfficeStartTimeIST string `json:"office_start_time_ist"`
			OfficeEndTimeIST   string `json:"office_end_time_ist"`
			UserID             string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
		if req.Name == "" || req.RoleCode == "" || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "name, rolecode, and user_id are required")
			return
		}
		createdBy := api.GetUserEmailFromCtx(r.Context())
		if createdBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		// Uniqueness checks: ensure role name and role code are unique (exclude soft-deleted roles)
		var existingID string
		// check name uniqueness (case-insensitive)
		if err := db.QueryRow("SELECT id FROM roles WHERE LOWER(TRIM(name)) = LOWER(TRIM($1)) AND COALESCE(is_deleted, false) = false LIMIT 1", req.Name).Scan(&existingID); err == nil {
			respondWithError(w, http.StatusBadRequest, fmt.Sprintf("role name '%s' already exists (id=%s)", req.Name, existingID))
			return
		} else if err != sql.ErrNoRows {
			respondWithInternalError(w, err)
			return
		}
		// check role code uniqueness if supplied
		if strings.TrimSpace(req.RoleCode) != "" {
			if err := db.QueryRow("SELECT id FROM roles WHERE (rolecode = $1 OR role_code = $1) AND COALESCE(is_deleted, false) = false LIMIT 1", req.RoleCode).Scan(&existingID); err == nil {
				respondWithError(w, http.StatusBadRequest, fmt.Sprintf("role code '%s' already exists (id=%s)", req.RoleCode, existingID))
				return
			} else if err != sql.ErrNoRows {
				respondWithInternalError(w, err)
				return
			}
		}
		// Insert role and return the inserted row (set created_at)
		rows, err := db.Query(
			`INSERT INTO roles (name, rolecode, description, office_start_time_ist, office_end_time_ist, status, created_by, created_at)
			 VALUES ($1, $2, $3, $4, $5, 'pending', $6, NOW()) RETURNING *`,
			req.Name,
			req.RoleCode,
			req.Description,
			req.OfficeStartTimeIST,
			req.OfficeEndTimeIST,
			createdBy,
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			respondWithInternalError(w, err)
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
				respondWithInternalError(w, err)
				return
			}
			for i, col := range cols {
				roleMap[col] = vals[i]
			}

			// Normalize role code: prefer `rolecode` (legacy insert column) and
			// fall back to `role_code`. Also expose camelCase `roleCode` for
			// frontend convenience.
			var roleCodeVal interface{}
			if v, ok := roleMap["rolecode"]; ok && fmt.Sprint(v) != "" {
				roleCodeVal = v
			}
			if roleCodeVal == nil || fmt.Sprint(roleCodeVal) == "" {
				if v2, ok2 := roleMap["role_code"]; ok2 && fmt.Sprint(v2) != "" {
					roleCodeVal = v2
				}
			}
			if roleCodeVal != nil {
				roleMap["role_code"] = roleCodeVal
				roleMap["roleCode"] = fmt.Sprint(roleCodeVal)
			} else {
				// ensure keys exist (empty string) so frontend doesn't get undefined
				roleMap["role_code"] = ""
				roleMap["roleCode"] = ""
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
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
		// buNames := api.GetEntityNamesFromCtx(r.Context())
		// if len(buNames) == 0 {
		//     respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
		//     return
		// }

		// Get role_id from user_roles for user_id
		rolesPerms := map[string]interface{}{}
		var roleId int
		err := db.QueryRow("SELECT role_id FROM user_roles WHERE user_id = $1 AND COALESCE(is_deleted, false) = false LIMIT 1", req.UserID).Scan(&roleId)
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

		// Pagination
		pagination, err := utils.ExtractPagination(r)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Count total
		total, _ := utils.CountTotal(db, "SELECT COUNT(*) FROM roles WHERE COALESCE(is_deleted, false) = false")
		pagination.SetPaginationStats(total)

		// Get roles with pagination
		// Order by the latest of created_at, approved_at or edited_at (whichever is greatest)
		// so the most recently created/approved/edited roles appear first.
		rows, err := db.Query(
			`SELECT * FROM roles
								 WHERE COALESCE(is_deleted, false) = false
								 ORDER BY GREATEST(
									 COALESCE(created_at, '1970-01-01'::timestamp),
									 COALESCE(approved_at, '1970-01-01'::timestamp),
									 COALESCE(edited_at, '1970-01-01'::timestamp),
									 COALESCE(rejected_at, '1970-01-01'::timestamp)
								 ) DESC
								 LIMIT $1 OFFSET $2`,
			pagination.Limit, pagination.Offset)
		if err != nil {
			respondWithInternalError(w, err)
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
			// Normalize role_code: prefer `rolecode` then `role_code`. Also add
			// camelCase `roleCode` to match frontend expectations.
			var roleCodeVal interface{}
			if v, ok := rMap["rolecode"]; ok && fmt.Sprint(v) != "" {
				roleCodeVal = v
			}
			if roleCodeVal == nil || fmt.Sprint(roleCodeVal) == "" {
				if v2, ok2 := rMap["role_code"]; ok2 && fmt.Sprint(v2) != "" {
					roleCodeVal = v2
				}
			}

			role := map[string]interface{}{
				"id":                      rMap["id"],
				"name":                    rMap["name"],
				"role_code":               roleCodeVal,
				"roleCode":                fmt.Sprint(roleCodeVal),
				"description":             rMap["description"],
				"startTime":               rMap["office_start_time_ist"],
				"endTime":                 rMap["office_end_time_ist"],
				"createdAt":               rMap["created_at"],
				"editedBy":                rMap["edited_by"],
				"editedAt":                rMap["edited_at"],
				"status":                  rMap["status"],
				"createdBy":               rMap["created_by"],
				"roles_permission_status": rMap["roles_permission_status"],
				"approvedBy":              rMap["approved_by"],
				"approveddate":            rMap["approved_at"],
			}
			roleData = append(roleData, role)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"permissions": rolesPerms,
			"roleData":    roleData,
			"pagination":  pagination,
		})
	}
}

// Handler: Approve multiple roles
func ApproveMultipleRoles(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			RoleIds []string `json:"roleIds"`
			// ApprovedBy      string `json:"approved_by"`
			ApprovalComment string `json:"approval_comment,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.RoleIds) == 0 {
			respondWithError(w, http.StatusBadRequest, "user_id and roleIds are required")
			return
		}
		// Middleware: check business units
		// Uncomment if you want to restrict by business units
		// buNames := api.GetEntityNamesFromCtx(r.Context())
		// if len(buNames) == 0 {
		//     respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
		//     return
		// }

		// Fetch current statuses
		rows, err := db.Query(`SELECT id, status FROM roles WHERE id = ANY($1)`, pq.Array(req.RoleIds))
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		toDelete := []string{}
		toApprove := []string{}
		for rows.Next() {
			var id string
			var status string
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
		// Delete roles (Soft Delete)
		if len(toDelete) > 0 {
			delRows, err := db.Query(`UPDATE roles SET is_deleted = true, status = 'Deleted', edited_at = NOW() WHERE id = ANY($1) RETURNING *`, pq.Array(toDelete))
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
		approvedBy := api.GetUserEmailFromCtx(r.Context())
		if approvedBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		// Approve roles
		if len(toApprove) > 0 {
			appRows, err := db.Query(`UPDATE roles SET status = 'Approved', approved_by = $1, approved_at = NOW(), approval_comment = $2 WHERE id = ANY($3) RETURNING *`, approvedBy, req.ApprovalComment, pq.Array(toApprove))
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "deleted": results["deleted"], "approved": results["approved"]})
	}
}

// Handler: Delete role (soft delete)
func DeleteRole(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			ID     string   `json:"id"`
			Ids    []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || (strings.TrimSpace(req.ID) == "" && len(req.Ids) == 0) {
			respondWithError(w, http.StatusBadRequest, "user_id and id/ids are required")
			return
		}
		// Middleware: check business units
		// buNames := api.GetEntityNamesFromCtx(r.Context())
		// if len(buNames) == 0 {
		//     respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
		//     return
		// }
		editor := api.GetUserEmailFromCtx(r.Context())
		if editor == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		// determine target ids (single or bulk)
		targetIds := req.Ids
		if len(targetIds) == 0 && strings.TrimSpace(req.ID) != "" {
			targetIds = []string{req.ID}
		}

		rows, err := db.Query(
			"UPDATE roles SET status = 'Delete-Approval', edited_by = $1, edited_at = NOW() WHERE id = ANY($2) RETURNING *",
			editor, pq.Array(targetIds),
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		deleted := []map[string]interface{}{}
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
			deleted = append(deleted, roleMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "deleted": deleted})
	}
}

// Handler: Reject multiple roles
func RejectMultipleRoles(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			RoleIds []string `json:"roleIds"`
			// RejectedBy       string `json:"rejected_by"`
			RejectionComment string `json:"rejection_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.RoleIds) == 0 {
			respondWithError(w, http.StatusBadRequest, "user_id, roleIds are required")
			return
		}
		// Middleware: check business units
		// buNames := api.GetEntityNamesFromCtx(r.Context())
		// if len(buNames) == 0 {
		//     respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
		//     return
		// }
		rejectedBy := api.GetUserEmailFromCtx(r.Context())
		if rejectedBy == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		rows, err := db.Query(
			`UPDATE roles SET status = 'Rejected', rejected_by = $1, rejected_at = NOW(), rejection_comment = $2 WHERE id = ANY($3) RETURNING *`,
			rejectedBy, req.RejectionComment, pq.Array(req.RoleIds),
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
			roleMap := map[string]interface{}{}
			for i, col := range cols {
				roleMap[col] = vals[i]
			}
			updated = append(updated, roleMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}

// Handler: Get roles for dropdown (returns id and name)
func GetRolesForDropdown(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query("SELECT id, name FROM roles WHERE (status = 'approved' OR status = 'Approved') AND COALESCE(is_deleted, false) = false")
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		type RoleOption struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		roles := []RoleOption{}
		for rows.Next() {
			var opt RoleOption
			rows.Scan(&opt.ID, &opt.Name)
			roles = append(roles, opt)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "roles": roles})
	}
}

// Handler: Get just role names
func GetJustRoles(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			EntityName string `json:"entity_name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		// If entity_name supplied: only return roles that have at least one member
		// (via user_roles) whose users.business_unit_name matches that entity.
		var rows *sql.Rows
		var err error
		if req.EntityName != "" {
			rows, err = db.Query(`
				SELECT DISTINCT r.name
				FROM roles r
				WHERE (r.status = 'approved' OR r.status = 'Approved')
				  AND COALESCE(r.is_deleted, false) = false
				  AND EXISTS (
					SELECT 1
					FROM user_roles ur
					JOIN user_entity_mappings uem ON uem.user_id = ur.user_id
					WHERE ur.role_id = r.id
					  AND uem.entity_name = $1
					  AND COALESCE(ur.is_deleted, false) = false
				)
			`, req.EntityName)
		} else {
			rows, err = db.Query("SELECT DISTINCT name FROM roles WHERE (status = 'approved' OR status = 'Approved') AND COALESCE(is_deleted, false) = false")
		}
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		roleNames := []string{}
		for rows.Next() {
			var name string
			rows.Scan(&name)
			roleNames = append(roleNames, name)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "roles": roleNames})
	}
}

func GetJustRolesPERMISSIONapproved(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req) // Not required for this query

		rows, err := db.Query("SELECT DISTINCT name FROM roles WHERE (status = 'approved' OR status = 'Approved') AND (roles_permission_status = 'approved' OR roles_permission_status = 'Approved') AND COALESCE(is_deleted, false) = false")
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		roleNames := []string{}
		for rows.Next() {
			var name string
			rows.Scan(&name)
			roleNames = append(roleNames, name)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
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
		err := db.QueryRow("SELECT role_id FROM user_roles WHERE user_id = $1 AND COALESCE(is_deleted, false) = false LIMIT 1", req.UserID).Scan(&roleId)
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

		// Pagination for pending roles
		pagination, err := utils.ExtractPagination(r)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		totalPendingQuery := "SELECT COUNT(*) FROM roles WHERE status IN ($1, $2, $3) AND COALESCE(is_deleted, false) = false"
		totalPending, _ := utils.CountTotal(db, totalPendingQuery, "pending", constants.StatusCodeAwaitingApproval, constants.StatusCodeDeleteApproval)
		pagination.SetPaginationStats(totalPending)

		// Fetch paginated pending roles
		rows, err := db.Query("SELECT * FROM roles WHERE status IN ($1, $2, $3) AND COALESCE(is_deleted, false) = false ORDER BY id LIMIT $4 OFFSET $5", "pending", constants.StatusCodeAwaitingApproval, constants.StatusCodeDeleteApproval, pagination.Limit, pagination.Offset)
		if err != nil {
			respondWithInternalError(w, err)
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
				"id":                      rMap["id"],
				"name":                    rMap["name"],
				"role_code":               rMap["role_code"],
				"description":             rMap["description"],
				"startTime":               rMap["office_start_time_ist"],
				"endTime":                 rMap["office_end_time_ist"],
				"createdAt":               rMap["created_at"],
				"editedBy":                rMap["edited_by"],
				"editedAt":                rMap["edited_at"],
				"status":                  rMap["status"],
				"createdBy":               rMap["created_by"],
				"roles_permission_status": rMap["roles_permission_status"],
				"approvedBy":              rMap["approved_by"],
				"approveddate":            rMap["approved_at"],
			}
			roleData = append(roleData, role)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"permissions": rolesPerms,
			"roleData":    roleData,
			"pagination":  pagination,
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

		editor := api.GetUserEmailFromCtx(r.Context())
		if editor == "" {
			editor = userID
		}
		// Middleware: check business units (uncomment if needed)
		// buNames := api.GetEntityNamesFromCtx(r.Context())
		// if len(buNames) == 0 {
		//     respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
		//     return
		// }
		// Prepare fields for update
		// Map frontend keys to DB column names and ignore unknown keys to avoid SQL errors
		allowedMap := map[string][]string{
			"name":                    {"name"},
			"description":             {"description"},
			"startTime":               {"office_start_time_ist"},
			"endTime":                 {"office_end_time_ist"},
			"office_start_time_ist":   {"office_start_time_ist"},
			"office_end_time_ist":     {"office_end_time_ist"},
			"status":                  {"status"},
			"roles_permission_status": {"roles_permission_status"},
			// support multiple role code column names (keep both in sync)
			"roleCode":  {"role_code", "rolecode"},
			"role_code": {"role_code", "rolecode"},
			"rolecode":  {"role_code", "rolecode"},
		}

		fields := map[string]interface{}{}
		for k, v := range req {
			if k == "id" || k == "user_id" {
				continue
			}
			if v == nil || fmt.Sprint(v) == "" {
				continue
			}
			if dbCols, ok := allowedMap[k]; ok {
				for _, col := range dbCols {
					fields[col] = v
				}
			}
		}

		// Always set status to Awaiting-Approval unless caller explicitly set status
		if _, ok := fields["status"]; ok {
			fields["status"] = constants.StatusCodeAwaitingApproval
		}

		// Set edited_by and edited_at for auditing
		fields["edited_by"] = editor
		fields["edited_at"] = time.Now()
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
			respondWithInternalError(w, err)
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"role":    roleMap,
		})
	}
}
