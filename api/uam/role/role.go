package role

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Helper: send JSON error response (delegates to the CLAUDE.md standard envelope)
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	api.RespondEnvelopeError(w, status, errMsg, "")
}

// respondWithInternalError logs the real error internally and returns a generic
// 500 message to the client so DB internals are never exposed.
func respondWithInternalError(w http.ResponseWriter, err error) {
	log.Println("[ERROR] role:", err)
	respondWithError(w, http.StatusInternalServerError, constants.ErrInternalServer)
}

// Handler: Create role
func CreateRole(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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
		if err := pool.QueryRow(ctx, "SELECT id FROM roles WHERE LOWER(TRIM(name)) = LOWER(TRIM($1)) AND COALESCE(is_deleted, false) = false LIMIT 1", req.Name).Scan(&existingID); err == nil {
			respondWithError(w, http.StatusBadRequest, fmt.Sprintf("role name '%s' already exists (id=%s)", req.Name, existingID))
			return
		} else if !errors.Is(err, pgx.ErrNoRows) {
			respondWithInternalError(w, err)
			return
		}
		// check role code uniqueness if supplied
		if strings.TrimSpace(req.RoleCode) != "" {
			if err := pool.QueryRow(ctx, "SELECT id FROM roles WHERE (rolecode = $1 OR role_code = $1) AND COALESCE(is_deleted, false) = false LIMIT 1", req.RoleCode).Scan(&existingID); err == nil {
				respondWithError(w, http.StatusBadRequest, fmt.Sprintf("role code '%s' already exists (id=%s)", req.RoleCode, existingID))
				return
			} else if !errors.Is(err, pgx.ErrNoRows) {
				respondWithInternalError(w, err)
				return
			}
		}
		// Insert role and return the inserted row (set created_at)
		rows, err := pool.Query(ctx,
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
		var roleMap map[string]interface{} = map[string]interface{}{}
		if rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				respondWithInternalError(w, err)
				return
			}
			for i, col := range rows.FieldDescriptions() {
				roleMap[string(col.Name)] = vals[i]
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
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"role": roleMap,
		})
	}
}

func GetRolesPageData(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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

		rolesPerms := map[string]interface{}{}
		permRows, err := pool.Query(ctx, `
			SELECT p.page_name, p.tab_name, p.action,
			       bool_or(rp.allowed) AS allowed
			FROM role_permissions rp
			JOIN permissions p ON rp.permission_id = p.id
			JOIN user_roles ur ON rp.role_id = ur.role_id
			WHERE ur.user_id = $1
			  AND COALESCE(ur.is_deleted, false) = false
			  AND (rp.status = 'Approved' OR rp.status = 'approved')
			GROUP BY p.page_name, p.tab_name, p.action
		`, req.UserID)
		if err == nil {
			defer permRows.Close()
			pagePermissions := map[string]interface{}{}
			tabs := map[string]map[string]interface{}{}
			for permRows.Next() {
				var pageName, action string
				var tabName sql.NullString
				var allowed interface{}
				if err := permRows.Scan(&pageName, &tabName, &action, &allowed); err != nil {
					log.Printf("role.go GetRolesPageData: scan permission row failed: %v", err)
					continue
				}
				if pageName != "roles" {
					continue
				}
				if !tabName.Valid || tabName.String == "" {
					pagePermissions[action] = allowed
				} else {
					if _, ok := tabs[tabName.String]; !ok {
						tabs[tabName.String] = map[string]interface{}{}
					}
					tabs[tabName.String][action] = allowed
				}
			}
			rolesPerms["pagePermissions"] = pagePermissions
			rolesPerms["tabs"] = tabs
		}

		// Pagination
		pagination, err := utils.ExtractPagination(r)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Count total
		total, _ := utils.CountTotal(ctx, pool, "SELECT COUNT(*) FROM roles WHERE COALESCE(is_deleted, false) = false")
		pagination.SetPaginationStats(total)

		// Get roles with pagination
		// Order by the latest of created_at, approved_at or edited_at (whichever is greatest)
		// so the most recently created/approved/edited roles appear first.
		rows, err := pool.Query(ctx,
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
		roleData := []map[string]interface{}{}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				log.Printf("role.go GetRolesPageData: scan role row failed: %v", err)
				continue
			}
			rMap := map[string]interface{}{}
			for i, col := range rows.FieldDescriptions() {
				rMap[string(col.Name)] = vals[i]
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
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"permissions": rolesPerms,
			"roleData":    roleData,
			"pagination":  pagination,
		})
	}
}

// Handler: Approve multiple roles
func ApproveMultipleRoles(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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
		rows, err := pool.Query(ctx, `SELECT id, status FROM roles WHERE id = ANY($1)`, req.RoleIds)
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
			if err := rows.Scan(&id, &status); err != nil {
				log.Printf("role.go ApproveMultipleRoles: scan id/status row failed: %v", err)
			}
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
			delRows, err := pool.Query(ctx, `UPDATE roles SET is_deleted = true, status = 'Deleted', edited_at = NOW() WHERE id = ANY($1) RETURNING *`, toDelete)
			if err == nil {
				defer delRows.Close()
				for delRows.Next() {
					vals, err := delRows.Values()
					if err != nil {
						log.Printf("role.go ApproveMultipleRoles: scan deleted role row failed: %v", err)
						continue
					}
					roleMap := map[string]interface{}{}
					for i, col := range delRows.FieldDescriptions() {
						roleMap[string(col.Name)] = vals[i]
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
			appRows, err := pool.Query(ctx, `UPDATE roles SET status = 'Approved', approved_by = $1, approved_at = NOW(), approval_comment = $2 WHERE id = ANY($3) RETURNING *`, approvedBy, req.ApprovalComment, toApprove)
			if err == nil {
				defer appRows.Close()
				for appRows.Next() {
					vals, err := appRows.Values()
					if err != nil {
						log.Printf("role.go ApproveMultipleRoles: scan approved role row failed: %v", err)
						continue
					}
					roleMap := map[string]interface{}{}
					for i, col := range appRows.FieldDescriptions() {
						roleMap[string(col.Name)] = vals[i]
					}
					results["approved"] = append(results["approved"].([]map[string]interface{}), roleMap)
				}
			}
		}
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"deleted":  results["deleted"],
			"approved": results["approved"],
		})
	}
}

// Handler: Delete role (soft delete)
func DeleteRole(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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

		rows, err := pool.Query(ctx,
			"UPDATE roles SET status = 'Delete-Approval', edited_by = $1, edited_at = NOW() WHERE id = ANY($2) RETURNING *",
			editor, targetIds,
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		deleted := []map[string]interface{}{}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				log.Printf("role.go DeleteRole: scan deleted role row failed: %v", err)
				continue
			}
			roleMap := map[string]interface{}{}
			for i, col := range rows.FieldDescriptions() {
				roleMap[string(col.Name)] = vals[i]
			}
			deleted = append(deleted, roleMap)
		}
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"deleted": deleted})
	}
}

// Handler: Reject multiple roles
func RejectMultipleRoles(pool *pgxpool.Pool) http.HandlerFunc {
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
		rows, err := pool.Query(r.Context(),
			`UPDATE roles SET status = 'Rejected', rejected_by = $1, rejected_at = NOW(), rejection_comment = $2 WHERE id = ANY($3) RETURNING *`,
			rejectedBy, req.RejectionComment, req.RoleIds,
		)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		fieldDescs := rows.FieldDescriptions()
		cols := make([]string, len(fieldDescs))
		for i, fd := range fieldDescs {
			cols[i] = fd.Name
		}
		updated := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				log.Printf("role.go RejectMultipleRoles: scan updated role row failed: %v", err)
			}
			roleMap := map[string]interface{}{}
			for i, col := range cols {
				roleMap[col] = vals[i]
			}
			updated = append(updated, roleMap)
		}
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"updated": updated})
	}
}

// Handler: Get roles for dropdown (returns id and name)
func GetRolesForDropdown(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := pool.Query(r.Context(), "SELECT id, name FROM roles WHERE (status = 'approved' OR status = 'Approved') AND COALESCE(is_deleted, false) = false")
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
			if err := rows.Scan(&opt.ID, &opt.Name); err != nil {
				log.Printf("role.go GetRolesForDropdown: scan role option row failed: %v", err)
			}
			roles = append(roles, opt)
		}
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"roles": roles})
	}
}

// Handler: Get just role names
func GetJustRoles(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			EntityName string `json:"entity_name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		// If entity_name supplied: only return roles that have at least one member
		// (via user_roles) whose users.business_unit_name matches that entity.
		var rows pgx.Rows
		var err error
		if req.EntityName != "" {
			rows, err = pool.Query(r.Context(), `
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
			rows, err = pool.Query(r.Context(), "SELECT DISTINCT name FROM roles WHERE (status = 'approved' OR status = 'Approved') AND COALESCE(is_deleted, false) = false")
		}
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		roleNames := []string{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				log.Printf("role.go GetJustRoles: scan role name row failed: %v", err)
			}
			roleNames = append(roleNames, name)
		}
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"roles": roleNames})
	}
}

func GetJustRolesPERMISSIONapproved(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req) // Not required for this query

		rows, err := pool.Query(r.Context(), "SELECT DISTINCT name FROM roles WHERE (status = 'approved' OR status = 'Approved') AND (roles_permission_status = 'approved' OR roles_permission_status = 'Approved') AND COALESCE(is_deleted, false) = false")
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		roleNames := []string{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				log.Printf("role.go GetJustRolesPERMISSIONapproved: scan role name row failed: %v", err)
			}
			roleNames = append(roleNames, name)
		}
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"roles": roleNames})
	}
}

func GetPendingRoles(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Missing user_id in request body")
			return
		}

		rolesPerms := map[string]interface{}{}
		permRows, err := pool.Query(ctx, `
			SELECT p.page_name, p.tab_name, p.action,
			       bool_or(rp.allowed) AS allowed
			FROM role_permissions rp
			JOIN permissions p ON rp.permission_id = p.id
			JOIN user_roles ur ON rp.role_id = ur.role_id
			WHERE ur.user_id = $1
			  AND COALESCE(ur.is_deleted, false) = false
			  AND (rp.status = 'Approved' OR rp.status = 'approved')
			GROUP BY p.page_name, p.tab_name, p.action
		`, req.UserID)
		if err == nil {
			defer permRows.Close()
			pagePermissions := map[string]interface{}{}
			tabs := map[string]map[string]interface{}{}
			for permRows.Next() {
				var pageName, action string
				var tabName sql.NullString
				var allowed interface{}
				if err := permRows.Scan(&pageName, &tabName, &action, &allowed); err != nil {
					log.Printf("role.go GetPendingRoles: scan permission row failed: %v", err)
					continue
				}
				if pageName != "roles" {
					continue
				}
				if !tabName.Valid || tabName.String == "" {
					pagePermissions[action] = allowed
				} else {
					if _, ok := tabs[tabName.String]; !ok {
						tabs[tabName.String] = map[string]interface{}{}
					}
					tabs[tabName.String][action] = allowed
				}
			}
			rolesPerms["pagePermissions"] = pagePermissions
			rolesPerms["tabs"] = tabs
		}

		// Pagination for pending roles
		pagination, err := utils.ExtractPagination(r)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		totalPendingQuery := "SELECT COUNT(*) FROM roles WHERE status IN ($1, $2, $3) AND COALESCE(is_deleted, false) = false"
		totalPending, _ := utils.CountTotal(ctx, pool, totalPendingQuery, "pending", constants.StatusCodeAwaitingApproval, constants.StatusCodeDeleteApproval)
		pagination.SetPaginationStats(totalPending)

		// Fetch paginated pending roles
		rows, err := pool.Query(ctx, "SELECT * FROM roles WHERE status IN ($1, $2, $3) AND COALESCE(is_deleted, false) = false ORDER BY id LIMIT $4 OFFSET $5", "pending", constants.StatusCodeAwaitingApproval, constants.StatusCodeDeleteApproval, pagination.Limit, pagination.Offset)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		roleData := []map[string]interface{}{}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				log.Printf("role.go GetPendingRoles: scan role row failed: %v", err)
				continue
			}
			rMap := map[string]interface{}{}
			for i, col := range rows.FieldDescriptions() {
				rMap[string(col.Name)] = vals[i]
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
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"permissions": rolesPerms,
			"roleData":    roleData,
			"pagination":  pagination,
		})
	}
}

// Handler: Update role
func UpdateRole(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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
		rows, err := pool.Query(ctx, query, values...)
		if err != nil {
			respondWithInternalError(w, err)
			return
		}
		defer rows.Close()
		if !rows.Next() {
			respondWithError(w, http.StatusNotFound, "Role not found")
			return
		}
		vals, err := rows.Values()
		if err != nil {
			log.Printf("role.go UpdateRole: scan role row failed: %v", err)
		}
		roleMap := map[string]interface{}{}
		for i, col := range rows.FieldDescriptions() {
			roleMap[string(col.Name)] = vals[i]
		}
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"role": roleMap,
		})
	}
}
