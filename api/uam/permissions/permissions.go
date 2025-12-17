package permissions

import (
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"strings"

	"github.com/lib/pq"
)

func GetRolePermissionsJsonByRoleName(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RoleName string `json:"roleName"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"success":false,"error":"invalid request"}`, http.StatusBadRequest)
			return
		}

		var jsonResult []byte
		err := db.QueryRow(`
			WITH role_cte AS (
				SELECT id FROM public.roles WHERE LOWER(name) = LOWER($1) AND LOWER(status) = 'approved'
			),
			page_json AS (
				SELECT 
					p.page_name,
					jsonb_build_object(
						'pagePermissions',
							COALESCE((
								SELECT jsonb_object_agg(subp.action, COALESCE(subrp.allowed, false))
								FROM public.permissions subp
								LEFT JOIN public.role_permissions subrp
									ON subrp.permission_id = subp.id
									AND subrp.role_id = (SELECT id FROM role_cte)
									AND LOWER(subrp.status) = 'approved'
								WHERE subp.page_name = p.page_name
								  AND (subp.tab_name IS NULL OR subp.tab_name = '')
							), '{}'::jsonb),
						'tabs',
							COALESCE((
								SELECT jsonb_object_agg(tab_group.tab_name, tab_group.tab_actions)
								FROM (
									SELECT 
										subp2.tab_name,
										jsonb_object_agg(subp2.action, COALESCE(subrp2.allowed, false)) AS tab_actions
									FROM public.permissions subp2
									LEFT JOIN public.role_permissions subrp2
										ON subrp2.permission_id = subp2.id
										AND subrp2.role_id = (SELECT id FROM role_cte)
										AND LOWER(subrp2.status) = 'approved'
									WHERE subp2.page_name = p.page_name
									  AND subp2.tab_name IS NOT NULL
									  AND subp2.tab_name <> ''
									GROUP BY subp2.tab_name
								) AS tab_group
							), '{}'::jsonb)
					) AS page_data
				FROM public.permissions p
				GROUP BY p.page_name
			)
			SELECT COALESCE(jsonb_object_agg(page_name, page_data), '{}'::jsonb)
			FROM page_json;
		`, req.RoleName).Scan(&jsonResult)

		if err == sql.ErrNoRows {
			http.Error(w, `{"success":false,"error":"role not found"}`, http.StatusNotFound)
			return
		} else if err != nil {
			http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
			return
		}

		resp := map[string]interface{}{
			"success":  true,
			"roleName": req.RoleName,
			"pages":    json.RawMessage(jsonResult),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

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
			respondWithError(w, http.StatusBadRequest, "user_id, roleName and pages required")
			return
		}

		// Step 1: Get role_id
		var roleID int
		if err := db.QueryRow(`SELECT id FROM roles WHERE name = $1`, req.RoleName).Scan(&roleID); err != nil {
			respondWithError(w, http.StatusNotFound, "role not found")
			return
		}

		// Step 2: Flatten and normalize permissions
		type permKey struct {
			Page   string
			Tab    sql.NullString
			Action string
		}
		type permData struct {
			Key     permKey
			Allowed bool
		}
		var perms []permData
		permSet := make(map[string]struct{}) // to deduplicate quickly

		for page, pageObjRaw := range req.Pages {
			pageObj, ok := pageObjRaw.(map[string]interface{})
			if !ok {
				continue
			}

			// pagePermissions
			if ppRaw, ok := pageObj["pagePermissions"]; ok {
				if pp, ok := ppRaw.(map[string]interface{}); ok {
					for action, allowedRaw := range pp {
						key := fmt.Sprintf("%s||%s||%s", page, "", action)
						if _, exists := permSet[key]; exists {
							continue
						}
						perms = append(perms, permData{
							Key:     permKey{Page: page, Tab: sql.NullString{}, Action: action},
							Allowed: parseAllowed(allowedRaw),
						})
						permSet[key] = struct{}{}
					}
				}
			}

			// tabs
			if tabsRaw, ok := pageObj["tabs"]; ok {
				if tabs, ok := tabsRaw.(map[string]interface{}); ok {
					for tab, tabObjRaw := range tabs {
						if tabObj, ok := tabObjRaw.(map[string]interface{}); ok {
							for action, allowedRaw := range tabObj {
								key := fmt.Sprintf("%s||%s||%s", page, tab, action)
								if _, exists := permSet[key]; exists {
									continue
								}
								perms = append(perms, permData{
									Key:     permKey{Page: page, Tab: sql.NullString{String: tab, Valid: true}, Action: action},
									Allowed: parseAllowed(allowedRaw),
								})
								permSet[key] = struct{}{}
							}
						}
					}
				}
			}
		}

		if len(perms) == 0 {
			respondWithError(w, http.StatusBadRequest, "no valid permissions found")
			return
		}

		// Step 3: Bulk insert or fetch permission IDs using CTE (no duplicates created)
		tx, err := db.Begin()
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to begin transaction")
			return
		}
		defer tx.Rollback()

		pageNames := []string{}
		tabNames := []sql.NullString{}
		actions := []string{}
		for _, p := range perms {
			pageNames = append(pageNames, p.Key.Page)
			tabNames = append(tabNames, p.Key.Tab)
			actions = append(actions, p.Key.Action)
		}

		// Use UNNEST for bulk operation
		// rows, err := tx.Query(`
		// 	WITH input_data AS (
		// 		SELECT UNNEST($1::text[]) AS page_name,
		// 			   UNNEST($2::text[]) AS tab_name,
		// 			   UNNEST($3::text[]) AS action
		// 	),
		// 	inserted AS (
		// 		INSERT INTO public.permissions (page_name, tab_name, action)
		// 		SELECT page_name, NULLIF(tab_name, ''), action
		// 		FROM input_data
		// 		ON CONFLICT (page_name, tab_name, action) DO NOTHING
		// 		RETURNING id, page_name, tab_name, action
		// 	)
		// 	SELECT id, page_name, tab_name, action
		// 	FROM inserted
		// 	UNION
		// 	SELECT id, page_name, tab_name, action
		// 	FROM permissions
		// 	WHERE (page_name, tab_name, action) IN (
		// 		SELECT page_name, NULLIF(tab_name, ''), action FROM input_data
		// 	)
		// `, pq.Array(pageNames), pq.Array(nullStringToText(tabNames)), pq.Array(actions))
				rows, err := tx.Query(`
    WITH input_data AS (
        SELECT 
            UNNEST($1::text[]) AS page_name,
            UNNEST($2::text[]) AS tab_name,
            UNNEST($3::text[]) AS action
    ),
    inserted AS (
        INSERT INTO public.permissions (page_name, tab_name, action)
        SELECT 
            page_name, 
            NULLIF(NULLIF(tab_name, ''), 'NULL') AS tab_name,
            action
        FROM input_data
        ON CONFLICT (page_name, COALESCE(tab_name, ''), action) DO NOTHING
        RETURNING id, page_name, tab_name, action
    )
    SELECT id, page_name, tab_name, action
    FROM inserted
    UNION
    SELECT id, page_name, tab_name, action
    FROM public.permissions
    WHERE (page_name, COALESCE(tab_name, ''), action) IN (
        SELECT page_name, COALESCE(NULLIF(NULLIF(tab_name, ''), 'NULL'), ''), action FROM input_data
    )
`, pq.Array(pageNames), pq.Array(nullStringToText(tabNames)), pq.Array(actions))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "permission sync failed: "+err.Error())
			return
		}
		defer rows.Close()

		permissionIdMap := make(map[string]int)
		for rows.Next() {
			var id int
			var page, action string
			var tab sql.NullString
			rows.Scan(&id, &page, &tab, &action)
			key := fmt.Sprintf("%s||%s||%s", page, tab.String, action)
			permissionIdMap[key] = id
		}

		// Step 4: Bulk upsert role_permissions
		roleIDs := []int{}
		permIDs := []int{}
		alloweds := []bool{}

		for _, p := range perms {
			tabVal := ""
			if p.Key.Tab.Valid {
				tabVal = p.Key.Tab.String
			}
			key := fmt.Sprintf("%s||%s||%s", p.Key.Page, tabVal, p.Key.Action)
			if pid, ok := permissionIdMap[key]; ok {
				roleIDs = append(roleIDs, roleID)
				permIDs = append(permIDs, pid)
				alloweds = append(alloweds, p.Allowed)
			}
		}

		if len(roleIDs) > 0 {
			_, err = tx.Exec(`
			INSERT INTO public.role_permissions (role_id, permission_id, allowed, status)
			SELECT UNNEST($1::int[]), UNNEST($2::int[]), UNNEST($3::bool[]), 'pending'
			ON CONFLICT (role_id, permission_id)
			DO UPDATE SET
			  allowed = EXCLUDED.allowed,
			  status = CASE
			    WHEN role_permissions.allowed IS DISTINCT FROM EXCLUDED.allowed THEN 'pending'
			    ELSE role_permissions.status
			  END
			`, pq.Array(roleIDs), pq.Array(permIDs), pq.Array(alloweds))
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "role_permissions upsert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(); err != nil {
			respondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"count":   len(perms),
		})
	}
}

// helper: convert []sql.NullString -> []string
func nullStringToText(ns []sql.NullString) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		if n.Valid {
			out[i] = n.String
		} else {
			out[i] = ""
		}
	}
	return out
}

func parseAllowed(raw interface{}) bool {
	switch v := raw.(type) {
	case bool:
		return v

	case string:
		val := strings.TrimSpace(strings.ToLower(v))
		switch val {
		case "true", "t", "1", "yes", "y":
			return true
		case "false", "f", "0", "no", "n", "":
			return false
		default:
			// handle capitalized or mixed-case (e.g. "True", "TRUE")
			valUpper := strings.ToUpper(strings.TrimSpace(v))
			return valUpper == "TRUE" || valUpper == "T"
		}

	case float64:
		return v != 0

	case int:
		return v != 0

	case nil:
		return false

	default:
		return false
	}
}

func GetRolePermissionsJson(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value("user_id")

		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"success":false,"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}

		if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
			req.UserID = ctxUserID
		}
		if req.UserID == "" {
			http.Error(w, `{"success":false,"error":"user_id required"}`, http.StatusBadRequest)
			return
		}

		// Get roleName from active session
		roleName := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				roleName = s.Role
				break
			}
		}
		if roleName == "" {
			http.Error(w, `{"success":false,"error":"Role not found in session"}`, http.StatusUnauthorized)
			return
		}

		// Get role_id
		var roleID int
		if err := db.QueryRow(`SELECT id FROM roles WHERE name = $1`, roleName).Scan(&roleID); err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, `{"success":false,"error":"Role not found"}`, http.StatusNotFound)
			} else {
				http.Error(w, `{"success":false,"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			}
			return
		}

		query := `
		WITH rp_data AS (
			SELECT 
				p.page_name,
			p.tab_name,
			p.action,
			COALESCE(rp.allowed, false) AS allowed
			FROM role_permissions rp
			JOIN permissions p ON rp.permission_id = p.id
			JOIN roles r ON rp.role_id = r.id
			WHERE rp.role_id = $1
			  AND LOWER(rp.status) = 'approved'
			  AND LOWER(r.status) = 'approved'
		),
		action_json AS (
			SELECT 
				page_name,
				tab_name,
				jsonb_object_agg(action, allowed) AS actions
			FROM rp_data
			GROUP BY page_name, tab_name
		),
		page_json AS (
			SELECT 
				page_name,
				jsonb_build_object(
					'pagePermissions',
					COALESCE(
						(SELECT actions FROM action_json WHERE page_name = ad.page_name AND tab_name IS NULL),
						'{}'::jsonb
					),
					'tabs',
					COALESCE(
						(
							SELECT jsonb_object_agg(tab_name, actions)
							FROM action_json
							WHERE page_name = ad.page_name AND tab_name IS NOT NULL
						),
						'{}'::jsonb
					)
				) AS page_data
			FROM (SELECT DISTINCT page_name FROM rp_data) ad
		)
		SELECT jsonb_object_agg(page_name, page_data)
		FROM page_json;
		`

		var pagesJSON sql.NullString
		if err := db.QueryRow(query, roleID).Scan(&pagesJSON); err != nil {
			http.Error(w, `{"success":false,"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			return
		}

		resp := struct {
			RoleName string                 `json:"roleName"`
			Pages    map[string]interface{} `json:"pages"`
		}{
			RoleName: roleName,
			Pages:    map[string]interface{}{},
		}

		if pagesJSON.Valid {
			if err := json.Unmarshal([]byte(pagesJSON.String), &resp.Pages); err != nil {
				http.Error(w, `{"success":false,"error":"invalid json returned"}`, http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
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
		// Update roles.roles_permission_status to reflect the new aggregated status
		if _, err := db.Exec("UPDATE roles SET roles_permission_status = $1 WHERE id = $2", req.Status, roleID); err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to update role permission status: "+err.Error())
			return
		}
		// Collect updated permissions
		updatedPermissions := []map[string]interface{}{}
		for rows.Next() {
			var roleID, permissionID int
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
			"success": true,
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
			"success": true,
			// "user_id":    req.UserID,
			"rolesStatus": rolesStatus,
		})
	}
}

// func GetSidebarPermissions(db *sql.DB) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		var req struct {
// 			UserID string `json:"user_id"`
// 		}
// 		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
// 			http.Error(w, `{"success":false,"error":"user_id required"}`, http.StatusBadRequest)
// 			return
// 		}
// 		allPages := []string{
// 			"entity",
// 			"hierarchical",
// 			"masters",
// 			"dashboard",
// 			"cfo-dashboard",
// 			"fx-ops-dashboard",
// 			"bu-currency-exposure-dashboard",
// 			"hedging-dashboard",
// 			"dashboard-builder",
// 			"exposure-bucketing",
// 			"hedging-proposal",
// 			"roles",
// 			"permissions",
// 			"user-creation",
// 			"exposure-upload",
// 			"exposure-linkage",
// 			"fx-forward-booking",
// 			"forward-confirmation",
// 			"fx-cancellation",
// 			"settlement",
// 		}
// 		existing := make(map[string]struct{})
// 		existRows, err := db.Query(`
// 			SELECT DISTINCT page_name
// 			FROM public.permissions
// 			WHERE tab_name IS NULL
// 			  AND action = 'hasAccess'
// 			  AND page_name = ANY($1)
// 		`, pq.Array(allPages))
// 		if err != nil {
// 			http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
// 			return
// 		}
// 		defer existRows.Close()

// 		for existRows.Next() {
// 			var name string
// 			if err := existRows.Scan(&name); err == nil {
// 				existing[name] = struct{}{}
// 			}
// 		}
// 		var missing []string
// 		for _, page := range allPages {
// 			if _, ok := existing[page]; !ok {
// 				missing = append(missing, page)
// 			}
// 		}

// 		if len(missing) > 0 {
// 			tx, err := db.Begin()
// 			if err != nil {
// 				http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
// 				return
// 			}

// 			stmt, err := tx.Prepare(`
// 				INSERT INTO public.permissions (page_name, tab_name, action)
// 				VALUES ($1, NULL, 'hasAccess')
// 				ON CONFLICT (page_name, tab_name, action) DO NOTHING
// 			`)
// 			if err != nil {
// 				tx.Rollback()
// 				http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
// 				return
// 			}

// 			for _, page := range missing {
// 				if _, err := stmt.Exec(page); err != nil {
// 					tx.Rollback()
// 					http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
// 					return
// 				}
// 			}

// 			stmt.Close()
// 			if err := tx.Commit(); err != nil {
// 				http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
// 				return
// 			}
// 		}
// 		query := `
// 			SELECT p.page_name, p.action, rp.allowed
// 			FROM public.user_roles ur
// 			JOIN public.roles r ON ur.role_id = r.id
// 			JOIN public.role_permissions rp ON r.id = rp.role_id
// 			JOIN public.permissions p ON rp.permission_id = p.id
// 			WHERE ur.user_id = $1
// 			  AND LOWER(rp.status) = 'approved'
// 			  AND LOWER(r.status) = 'approved'
// 		`

// 		rows, err := db.Query(query, req.UserID)
// 		if err != nil {
// 			http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
// 			return
// 		}
// 		defer rows.Close()

// 		type PermissionRow struct {
// 			PageName string
// 			Action   string
// 			Allowed  bool
// 		}

// 		var results []PermissionRow
// 		for rows.Next() {
// 			var r PermissionRow
// 			if err := rows.Scan(&r.PageName, &r.Action, &r.Allowed); err != nil {
// 				http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
// 				return
// 			}
// 			results = append(results, r)
// 		}
// 		if err := rows.Err(); err != nil {
// 			http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
// 			return
// 		}

// 		pages := make(map[string]bool, len(allPages))
// 		for _, page := range allPages {
// 			pages[page] = false
// 		}
// 		for _, row := range results {
// 			if strings.EqualFold(row.Action, "hasAccess") && row.Allowed {
// 				pages[strings.ToLower(row.PageName)] = true
// 			}
// 		}
// 		resp := map[string]interface{}{
// 			"success": true,
// 			"pages":   pages,
// 		}
// 		w.Header().Set("Content-Type", "application/json")
// 		json.NewEncoder(w).Encode(resp)
// 	}
// }
func GetSidebarPermissions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			http.Error(w, `{"success":false,"error":"user_id required"}`, http.StatusBadRequest)
			return
		}

		allPages := []string{
			"entity", "hierarchical", "masters", "dashboard", "cfo-dashboard",
			"fx-ops-dashboard", "bu-currency-exposure-dashboard", "hedging-dashboard",
			"dashboard-builder", "exposure-bucketing", "hedging-proposal", "roles",
			"permissions", "user-creation", "exposure-upload", "exposure-linkage",
			"fx-forward-booking", "forward-confirmation", "fx-cancellation", "settlement",
		}

		_, _ = db.Exec(`
			INSERT INTO public.permissions (page_name, tab_name, action)
			SELECT p, '', 'hasAccess'
			FROM unnest($1::text[]) AS p
			ON CONFLICT (page_name, COALESCE(tab_name, ''), action) DO NOTHING
		`, pq.Array(allPages))

		rows, err := db.Query(`
			SELECT DISTINCT LOWER(p.page_name)
			FROM public.user_roles ur
			JOIN public.roles r ON ur.role_id = r.id
			JOIN public.role_permissions rp ON r.id = rp.role_id
			JOIN public.permissions p ON rp.permission_id = p.id
			WHERE ur.user_id = $1
			  AND LOWER(rp.status) = 'approved'
			  AND LOWER(r.status) = 'approved'
			  AND p.action = 'hasAccess'
			  AND rp.allowed = true
		`, req.UserID)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		pages := make(map[string]bool, len(allPages))
		for _, p := range allPages {
			pages[p] = false
		}

		for rows.Next() {
			var page string
			_ = rows.Scan(&page)
			pages[page] = true
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"pages":   pages,
		})
	}
}



