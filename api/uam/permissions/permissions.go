package permissions

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"
)

// ---------------------------------------------------------------------------
// Helper: send JSON error response
// ---------------------------------------------------------------------------

func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

// ---------------------------------------------------------------------------
// GetRolePermissionsJsonByRoleName — UNCHANGED
// ---------------------------------------------------------------------------

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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// ---------------------------------------------------------------------------
// UpsertRolePermissions — original logic UNCHANGED, only adds diff-staging
// at the end of the transaction to populate role_permission_requests.
// ---------------------------------------------------------------------------

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
		var roleID string
		if err := db.QueryRow(`SELECT id FROM roles WHERE name = $1`, req.RoleName).Scan(&roleID); err != nil {
			log.Print(err.Error())
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
		permSet := make(map[string]struct{})

		for page, pageObjRaw := range req.Pages {
			pageObj, ok := pageObjRaw.(map[string]interface{})
			if !ok {
				continue
			}
			if ppRaw, ok := pageObj["pagePermissions"]; ok {
				if pp, ok := ppRaw.(map[string]interface{}); ok {
					for action, allowedRaw := range pp {
						key := fmt.Sprintf(constants.FormatPipelineTripleAlt, page, "", action)
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
			if tabsRaw, ok := pageObj["tabs"]; ok {
				if tabs, ok := tabsRaw.(map[string]interface{}); ok {
					for tab, tabObjRaw := range tabs {
						if tabObj, ok := tabObjRaw.(map[string]interface{}); ok {
							for action, allowedRaw := range tabObj {
								key := fmt.Sprintf(constants.FormatPipelineTripleAlt, page, tab, action)
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

		// Step 3: Bulk insert or fetch permission IDs
		tx, err := db.Begin()
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrFailedToBeginTransaction)
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
			SELECT id, page_name, tab_name, action FROM inserted
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
			key := fmt.Sprintf(constants.FormatPipelineTripleAlt, page, tab.String, action)
			permissionIdMap[key] = id
		}
		rows.Close()

		// Step 4: Build role_permissions slices
		roleIDs := []string{}
		permIDs := []int{}
		alloweds := []bool{}

		for _, p := range perms {
			tabVal := ""
			if p.Key.Tab.Valid {
				tabVal = p.Key.Tab.String
			}
			key := fmt.Sprintf(constants.FormatPipelineTripleAlt, p.Key.Page, tabVal, p.Key.Action)
			if pid, ok := permissionIdMap[key]; ok {
				roleIDs = append(roleIDs, roleID)
				permIDs = append(permIDs, pid)
				alloweds = append(alloweds, p.Allowed)
			}
		}

		// Step 4a: Snapshot current allowed values BEFORE upsert (for diff)
		existingAllowed := make(map[int]sql.NullBool)
		if len(permIDs) > 0 {
			exRows, exErr := tx.Query(
				`SELECT permission_id, allowed FROM public.role_permissions WHERE role_id = $1 AND permission_id = ANY($2)`,
				roleID, pq.Array(permIDs),
			)
			if exErr == nil {
				for exRows.Next() {
					var pid int
					var allowed sql.NullBool
					exRows.Scan(&pid, &allowed)
					existingAllowed[pid] = allowed
				}
				exRows.Close()
			}
		}

		// Step 4b: Upsert role_permissions (original logic unchanged)
		if len(roleIDs) > 0 {
			_, err = tx.Exec(`
				INSERT INTO public.role_permissions (role_id, permission_id, allowed, status)
				SELECT UNNEST($1::text[]), UNNEST($2::int[]), UNNEST($3::bool[]), 'pending'
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

		// Step 4c: Stage ONLY changed rows into role_permission_requests
		var hasPendingRequests bool
		for i, pid := range permIDs {
			oldNullBool := existingAllowed[pid]
			newVal := alloweds[i]

			// Only insert if value actually changed or permission is brand new
			if !oldNullBool.Valid || oldNullBool.Bool != newVal {
				hasPendingRequests = true
				var oldValArg interface{}
				if oldNullBool.Valid {
					oldValArg = oldNullBool.Bool
				} // else nil → SQL NULL (brand new permission)

				_, insErr := tx.Exec(`
					INSERT INTO public.role_permission_requests
						(role_id, permission_id, old_value, new_value, status, submitted_by)
					VALUES ($1, $2, $3, $4, 'Pending', $5)
				`, roleIDs[i], pid, oldValArg, newVal, req.UserID)
				if insErr != nil {
					// Non-fatal: log but don't fail the whole upsert
					log.Printf("warn: failed to stage permission request role=%s perm=%d: %v", roleIDs[i], pid, insErr)
				}
			}
		}

		// Mark role as Pending if any requests were staged
		if hasPendingRequests {
			_, _ = tx.Exec(
				`UPDATE public.roles SET roles_permission_status = 'Pending' WHERE id = $1`,
				roleID,
			)
		}

		if err := tx.Commit(); err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"count":   len(perms),
		})
	}
}

// ---------------------------------------------------------------------------
// GetRolePermissionsJson — UNCHANGED
// ---------------------------------------------------------------------------

func GetRolePermissionsJson(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value("user_id")

		var req struct {
			UserID   string `json:"user_id"`
			RoleName string `json:"roleName,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"success":false,"error":constants.ErrInvalidRequestBody}`, http.StatusBadRequest)
			return
		}

		roleName := strings.TrimSpace(req.RoleName)
		if roleName == "" {
			if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
				req.UserID = ctxUserID
			}
			if req.UserID == "" {
				http.Error(w, `{"success":false,"error":constants.ErrUserIDRequired}`, http.StatusBadRequest)
				return
			}
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
		}

		var roleID string
		if err := db.QueryRow(`SELECT id FROM roles WHERE name = $1`, roleName).Scan(&roleID); err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, `{"success":false,"error":"Role not found"}`, http.StatusNotFound)
			} else {
				http.Error(w, `{"success":false,"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
			}
			return
		}

		query := `
			WITH page_json AS (
				SELECT
					p.page_name,
					jsonb_build_object(
						'pagePermissions',
							COALESCE((
								SELECT jsonb_object_agg(subp.action, COALESCE(subrp.allowed, false))
								FROM public.permissions subp
								LEFT JOIN public.role_permissions subrp
									ON subrp.permission_id = subp.id
									AND subrp.role_id = $1
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
										AND subrp2.role_id = $1
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// ---------------------------------------------------------------------------
// UpdateRolePermissionsStatusByName
// NOW powers: POST /uam/permissions/approve-reject
//
// Input:
//   { "user_id": "...", "roleName": "ADMIN" }          ← approve/reject ALL pending for a role
//   { "user_id": "...", "requestIds": [1,2,3] }        ← approve/reject specific requests
//   { "action": "Approved" | "approved" | "Rejected" | "rejected" }
//   { "comment": "optional note" }
//
// Action field is case-insensitive. Stored as title-case ("Approved"/"Rejected").
//
// Response: { success, action, actionedBy, count, changes: [{requestId, pageName, tabName?, action, oldValue, newValue}] }
// ---------------------------------------------------------------------------

func UpdateRolePermissionsStatusByName(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value("user_id")
		var req struct {
			UserID     string `json:"user_id"`
			RoleName   string `json:"roleName,omitempty"`
			RequestIDs []int  `json:"requestIds,omitempty"`
			Action     string `json:"action"`
			Comment    string `json:"comment,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidRequestBody)
			return
		}
		if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
			req.UserID = ctxUserID
		}

		// Normalise action to title-case, accept any casing
		action := normAction(req.Action)
		if req.UserID == "" || (action != "Approved" && action != "Rejected") {
			respondWithError(w, http.StatusBadRequest, "user_id and action (Approved|Rejected) required")
			return
		}
		if len(req.RequestIDs) == 0 && strings.TrimSpace(req.RoleName) == "" {
			respondWithError(w, http.StatusBadRequest, "provide requestIds or roleName")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrFailedToBeginTransaction)
			return
		}
		defer tx.Rollback()

		// Fetch pending target rows
		type targetRow struct {
			ID       int
			RoleID   string
			PermID   int
			NewValue bool
		}
		var targets []targetRow
		var fetchRows *sql.Rows

		if len(req.RequestIDs) > 0 {
			fetchRows, err = tx.Query(`
				SELECT id, role_id, permission_id, new_value
				FROM public.role_permission_requests
				WHERE id = ANY($1) AND LOWER(status) = 'pending'
			`, pq.Array(req.RequestIDs))
		} else {
			fetchRows, err = tx.Query(`
				SELECT rpr.id, rpr.role_id, rpr.permission_id, rpr.new_value
				FROM public.role_permission_requests rpr
				JOIN public.roles r ON r.id = rpr.role_id
				WHERE LOWER(r.name) = LOWER($1) AND LOWER(rpr.status) = 'pending'
			`, req.RoleName)
		}
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for fetchRows.Next() {
			var t targetRow
			fetchRows.Scan(&t.ID, &t.RoleID, &t.PermID, &t.NewValue)
			targets = append(targets, t)
		}
		fetchRows.Close()

		if len(targets) == 0 {
			respondWithError(w, http.StatusNotFound, "no pending requests found")
			return
		}

		ids := make([]int, len(targets))
		for i, t := range targets {
			ids[i] = t.ID
		}

		// Mark request rows as actioned
		_, err = tx.Exec(`
			UPDATE public.role_permission_requests
			SET status      = $1,
			    actioned_by = $2,
			    actioned_at = now(),
			    comment     = COALESCE(NULLIF($3, ''), comment)
			WHERE id = ANY($4)
		`, action, req.UserID, req.Comment, pq.Array(ids))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "request update failed: "+err.Error())
			return
		}

		// If Approved: apply new_value to role_permissions
		if action == "Approved" {
			rIDs := make([]string, len(targets))
			pIDs := make([]int, len(targets))
			nVals := make([]bool, len(targets))
			for i, t := range targets {
				rIDs[i] = t.RoleID
				pIDs[i] = t.PermID
				nVals[i] = t.NewValue
			}
			_, err = tx.Exec(`
				INSERT INTO public.role_permissions
					(role_id, permission_id, allowed, status, approved_by, approved_at)
				SELECT UNNEST($1::text[]), UNNEST($2::int[]), UNNEST($3::bool[]), 'approved', $4, now()
				ON CONFLICT (role_id, permission_id)
				DO UPDATE SET
					allowed     = EXCLUDED.allowed,
					status      = 'approved',
					approved_by = EXCLUDED.approved_by,
					approved_at = EXCLUDED.approved_at
			`, pq.Array(rIDs), pq.Array(pIDs), pq.Array(nVals), req.UserID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "role_permissions apply failed: "+err.Error())
				return
			}
		}

		// Recompute roles.roles_permission_status for each affected role
		affectedRoles := make(map[string]struct{})
		for _, t := range targets {
			affectedRoles[t.RoleID] = struct{}{}
		}
		for rid := range affectedRoles {
			var pendingCount int
			tx.QueryRow(`
				SELECT COUNT(*) FROM public.role_permission_requests
				WHERE role_id = $1 AND LOWER(status) = 'pending'
			`, rid).Scan(&pendingCount)

			newRoleStatus := action
			if pendingCount > 0 {
				newRoleStatus = "Pending"
			}
			tx.Exec(`UPDATE public.roles SET roles_permission_status = $1 WHERE id = $2`, newRoleStatus, rid)
		}

		if err := tx.Commit(); err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		// Build audit entries for response
		type auditEntry struct {
			RequestID int     `json:"requestId"`
			PageName  string  `json:"pageName"`
			TabName   *string `json:"tabName,omitempty"`
			Action    string  `json:"action"`
			OldValue  *bool   `json:"oldValue"` // null = brand new permission
			NewValue  bool    `json:"newValue"`
		}
		var audits []auditEntry
		auditRows, auditErr := db.Query(`
			SELECT rpr.id, p.page_name, p.tab_name, p.action, rpr.old_value, rpr.new_value
			FROM public.role_permission_requests rpr
			JOIN public.permissions p ON p.id = rpr.permission_id
			WHERE rpr.id = ANY($1)
		`, pq.Array(ids))
		if auditErr == nil {
			defer auditRows.Close()
			for auditRows.Next() {
				var ae auditEntry
				var tab sql.NullString
				var oldValue sql.NullBool
				auditRows.Scan(&ae.RequestID, &ae.PageName, &tab, &ae.Action, &oldValue, &ae.NewValue)
				if tab.Valid {
					ae.TabName = &tab.String
				}
				if oldValue.Valid {
					ae.OldValue = &oldValue.Bool
				}
				audits = append(audits, ae)
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"action":     action,
			"actionedBy": req.UserID,
			"count":      len(targets),
			"changes":    audits,
		})
	}
}

// ---------------------------------------------------------------------------
// GetRolesStatus
// NOW powers: POST /uam/permissions/status
//
// Input:
//   { "user_id": "...", "roleName": "ADMIN" (optional), "status": "Pending" (optional, default Pending) }
//
// Response: grouped by role, each with totalChanges and a changes[] array
// showing pageName, tabName, action, oldValue, newValue, who submitted, who actioned.
// ---------------------------------------------------------------------------

func GetRolesStatus(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value("user_id")
		var req struct {
			UserID   string `json:"user_id"`
			RoleName string `json:"roleName,omitempty"`
			Status   string `json:"status,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
			req.UserID = ctxUserID
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		// Default to Pending; accept any casing from caller
		statusFilter := strings.TrimSpace(req.Status)
		if statusFilter == "" {
			statusFilter = "Pending"
		}

		query := `
			SELECT
				rpr.id,
				r.name          AS role_name,
				p.page_name,
				p.tab_name,
				p.action,
				rpr.old_value,
				rpr.new_value,
				rpr.status,
				rpr.submitted_by,
				rpr.submitted_at,
				rpr.actioned_by,
				rpr.actioned_at,
				rpr.comment
			FROM public.role_permission_requests rpr
			JOIN public.roles       r ON r.id  = rpr.role_id
			JOIN public.permissions p ON p.id  = rpr.permission_id
			WHERE LOWER(rpr.status) = LOWER($1)
		`
		args := []interface{}{statusFilter}
		if strings.TrimSpace(req.RoleName) != "" {
			query += ` AND LOWER(r.name) = LOWER($2)`
			args = append(args, req.RoleName)
		}
		query += ` ORDER BY r.name, rpr.submitted_at DESC`

		rows, err := db.Query(query, args...)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		type ChangeRow struct {
			ID          int        `json:"id"`
			PageName    string     `json:"pageName"`
			TabName     *string    `json:"tabName,omitempty"`
			Action      string     `json:"action"`
			OldValue    *bool      `json:"oldValue"` // null = brand new permission
			NewValue    bool       `json:"newValue"`
			Status      string     `json:"status"`
			SubmittedBy *string    `json:"submittedBy,omitempty"`
			SubmittedAt *time.Time `json:"submittedAt,omitempty"`
			ActionedBy  *string    `json:"actionedBy,omitempty"`
			ActionedAt  *time.Time `json:"actionedAt,omitempty"`
			Comment     *string    `json:"comment,omitempty"`
		}
		type RoleGroup struct {
			RoleName     string      `json:"roleName"`
			TotalChanges int         `json:"totalChanges"`
			Changes      []ChangeRow `json:"changes"`
		}

		roleMap := make(map[string]*RoleGroup)
		roleOrder := []string{}

		for rows.Next() {
			var cr ChangeRow
			var roleName string
			var tab, submittedBy, actionedBy, comment sql.NullString
			var oldValue sql.NullBool
			var submittedAt, actionedAt sql.NullTime

			if err := rows.Scan(
				&cr.ID, &roleName, &cr.PageName, &tab, &cr.Action,
				&oldValue, &cr.NewValue, &cr.Status,
				&submittedBy, &submittedAt, &actionedBy, &actionedAt, &comment,
			); err != nil {
				continue
			}
			if tab.Valid {
				cr.TabName = &tab.String
			}
			if oldValue.Valid {
				cr.OldValue = &oldValue.Bool
			}
			if submittedBy.Valid {
				cr.SubmittedBy = &submittedBy.String
			}
			if submittedAt.Valid {
				cr.SubmittedAt = &submittedAt.Time
			}
			if actionedBy.Valid {
				cr.ActionedBy = &actionedBy.String
			}
			if actionedAt.Valid {
				cr.ActionedAt = &actionedAt.Time
			}
			if comment.Valid {
				cr.Comment = &comment.String
			}

			if _, ok := roleMap[roleName]; !ok {
				roleMap[roleName] = &RoleGroup{RoleName: roleName}
				roleOrder = append(roleOrder, roleName)
			}
			roleMap[roleName].Changes = append(roleMap[roleName].Changes, cr)
		}

		result := make([]*RoleGroup, 0, len(roleOrder))
		for _, name := range roleOrder {
			rg := roleMap[name]
			rg.TotalChanges = len(rg.Changes)
			result = append(result, rg)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":      true,
			"statusFilter": statusFilter,
			"roles":        result,
		})
	}
}

// ---------------------------------------------------------------------------
// GetSidebarPermissions — UNCHANGED
// ---------------------------------------------------------------------------

func GetSidebarPermissions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			http.Error(w, `{"success":false,"error":constants.ErrUserIDRequired}`, http.StatusBadRequest)
			return
		}

		query := `
			WITH pages AS (
				SELECT DISTINCT LOWER(TRIM(page_name)) AS page_name
				FROM public.permissions
				WHERE page_name IS NOT NULL AND TRIM(page_name) <> ''
			), user_allowed AS (
				SELECT LOWER(TRIM(p.page_name)) AS page_name,
					   bool_or(COALESCE(rp.allowed, false)) AS has_access
				FROM public.permissions p
				JOIN public.role_permissions rp ON rp.permission_id = p.id
				JOIN public.roles r ON r.id = rp.role_id
				JOIN public.user_roles ur ON ur.role_id = r.id AND ur.user_id = $1
				WHERE p.action = 'hasAccess'
				  AND (p.tab_name IS NULL OR TRIM(p.tab_name) = '')
				  AND LOWER(COALESCE(rp.status,'')) = 'approved'
				  AND LOWER(COALESCE(r.status,'')) = 'approved'
				GROUP BY LOWER(TRIM(p.page_name))
			)
			SELECT p.page_name, COALESCE(ua.has_access, false) AS has_access
			FROM pages p
			LEFT JOIN user_allowed ua ON ua.page_name = p.page_name
			ORDER BY p.page_name
		`

		rows, err := db.Query(query, req.UserID)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"success":false,"error":"%v"}`, err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		pages := map[string]bool{}
		for rows.Next() {
			var page sql.NullString
			var hasAccess bool
			if err := rows.Scan(&page, &hasAccess); err != nil {
				continue
			}
			if page.Valid {
				pages[strings.ToLower(strings.TrimSpace(page.String))] = hasAccess
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"pages":   pages,
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// nullStringToText converts []sql.NullString -> []string (empty string for NULL)
func nullStringToText(ns []sql.NullString) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		if n.Valid {
			out[i] = n.String
		}
	}
	return out
}

// parseAllowed coerces any JSON value to a bool
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

// normAction normalises action to title-case "Approved" or "Rejected".
// Accepts any casing: "approved", "APPROVED", "Approved" → "Approved"
func normAction(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "approved":
		return "Approved"
	case "rejected":
		return "Rejected"
	default:
		return s // let caller validation handle unknown values
	}
}

// ---------------------------------------------------------------------------
// GetAllPermissionRequests
// GET /uam/permissions/requests/all
//
// Returns every role_permission_request row (all statuses by default),
// joining users.employee_name for submitted_by and actioned_by so the
// frontend gets human-readable names not just IDs.
//
// Input (optional filters):
//   {
//     "user_id":   "...",
//     "status":    "Pending|Approved|Rejected"  // omit = all statuses
//     "roleName":  "ADMIN"                       // omit = all roles
//   }
//
// Response:
//   {
//     "success": true,
//     "statusFilter": "all",
//     "total": 42,
//     "requests": [
//       {
//         "id": 7,
//         "roleName": "ADMIN",
//         "pageName": "fx-forward-booking",
//         "tabName": null,
//         "action": "hasAccess",
//         "oldValue": false,
//         "newValue": true,
//         "status": "Approved",
//         "submittedById": "usr_abc",
//         "submittedByName": "Alice Sharma",
//         "submittedAt": "2025-04-20T10:30:00Z",
//         "actionedById": "usr_xyz",
//         "actionedByName": "Bob Verma",
//         "actionedAt": "2025-04-20T11:00:00Z",
//         "comment": "looks good"
//       }
//     ]
//   }
// ---------------------------------------------------------------------------

func GetAllPermissionRequests(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value("user_id")
		var req struct {
			UserID   string `json:"user_id"`
			Status   string `json:"status,omitempty"`   // empty = all
			RoleName string `json:"roleName,omitempty"` // empty = all roles
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
			req.UserID = ctxUserID
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		statusFilter := strings.TrimSpace(req.Status)

		// Build query — LEFT JOIN users twice for submitter and actioner names
		query := `
			SELECT
				rpr.id,
				r.name                                        AS role_name,
				p.page_name,
				p.tab_name,
				p.action,
				rpr.old_value,
				rpr.new_value,
				rpr.status,
				rpr.submitted_by,
				COALESCE(su.employee_name, rpr.submitted_by) AS submitted_by_name,
				rpr.submitted_at,
				rpr.actioned_by,
				COALESCE(au.employee_name, rpr.actioned_by)  AS actioned_by_name,
				rpr.actioned_at,
				rpr.comment
			FROM public.role_permission_requests rpr
			JOIN  public.roles       r  ON r.id  = rpr.role_id
			JOIN  public.permissions p  ON p.id  = rpr.permission_id
			LEFT JOIN public.users   su ON su.id = rpr.submitted_by
			LEFT JOIN public.users   au ON au.id = rpr.actioned_by
			WHERE 1=1
		`
		args := []interface{}{}
		argIdx := 1

		if statusFilter != "" {
			query += fmt.Sprintf(` AND LOWER(rpr.status) = LOWER($%d)`, argIdx)
			args = append(args, statusFilter)
			argIdx++
		}
		if strings.TrimSpace(req.RoleName) != "" {
			query += fmt.Sprintf(` AND LOWER(r.name) = LOWER($%d)`, argIdx)
			args = append(args, req.RoleName)
			argIdx++
		}
		query += ` ORDER BY rpr.submitted_at DESC`

		rows, err := db.Query(query, args...)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		type RequestRow struct {
			ID              int        `json:"id"`
			RoleName        string     `json:"roleName"`
			PageName        string     `json:"pageName"`
			TabName         *string    `json:"tabName,omitempty"`
			Action          string     `json:"action"`
			OldValue        *bool      `json:"oldValue"`
			NewValue        bool       `json:"newValue"`
			Status          string     `json:"status"`
			SubmittedByID   *string    `json:"submittedById,omitempty"`
			SubmittedByName *string    `json:"submittedByName,omitempty"`
			SubmittedAt     *time.Time `json:"submittedAt,omitempty"`
			ActionedByID    *string    `json:"actionedById,omitempty"`
			ActionedByName  *string    `json:"actionedByName,omitempty"`
			ActionedAt      *time.Time `json:"actionedAt,omitempty"`
			Comment         *string    `json:"comment,omitempty"`
		}

		var requests []RequestRow
		for rows.Next() {
			var rr RequestRow
			var tab, submittedByID, submittedByName, actionedByID, actionedByName, comment sql.NullString
			var oldValue sql.NullBool
			var submittedAt, actionedAt sql.NullTime

			if err := rows.Scan(
				&rr.ID, &rr.RoleName, &rr.PageName, &tab, &rr.Action,
				&oldValue, &rr.NewValue, &rr.Status,
				&submittedByID, &submittedByName, &submittedAt,
				&actionedByID, &actionedByName, &actionedAt,
				&comment,
			); err != nil {
				continue
			}
			if tab.Valid {
				rr.TabName = &tab.String
			}
			if oldValue.Valid {
				rr.OldValue = &oldValue.Bool
			}
			if submittedByID.Valid {
				rr.SubmittedByID = &submittedByID.String
			}
			if submittedByName.Valid {
				rr.SubmittedByName = &submittedByName.String
			}
			if submittedAt.Valid {
				rr.SubmittedAt = &submittedAt.Time
			}
			if actionedByID.Valid {
				rr.ActionedByID = &actionedByID.String
			}
			if actionedByName.Valid {
				rr.ActionedByName = &actionedByName.String
			}
			if actionedAt.Valid {
				rr.ActionedAt = &actionedAt.Time
			}
			if comment.Valid {
				rr.Comment = &comment.String
			}

			requests = append(requests, rr)
		}

		sfLabel := statusFilter
		if sfLabel == "" {
			sfLabel = "all"
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":      true,
			"statusFilter": sfLabel,
			"total":        len(requests),
			"requests":     requests,
		})
	}
}

// ---------------------------------------------------------------------------
// GetRolePermissionAuditTable
// POST /uam/permissions/requests/role-summary
//
// Returns ALL active role_permissions grouped as:
//   role → pages → tabs (optional) → permissions
// Each permission line has its current live state + latest audit trail
// (who submitted the change, who approved/rejected, old→new values).
//
// Drives from role_permissions (live state) so every active permission
// appears even if no request was ever staged for it yet.
//
// Input (optional filters):
//   { "user_id": "...", "roleName": "ADMIN" }   // omit roleName = all roles
//
// Response shape:
//   {
//     "success": true,
//     "totalRoles": 3,
//     "roles": [
//       {
//         "roleName": "ADMIN",
//         "rolesPermissionStatus": "Approved",
//         "totalPermissions": 42,
//         "pages": [
//           {
//             "pageName": "fx-forward-booking",
//             "pagePermissions": [
//               {
//                 "permissionId":    12,
//                 "action":          "hasAccess",
//                 "currentAllowed":  true,
//                 "currentStatus":   "approved",
//                 "latestRequestId": 7,
//                 "requestStatus":   "Approved",
//                 "oldValue":        false,
//                 "newValue":        true,
//                 "submittedById":   "usr_abc",
//                 "submittedByName": "Alice Sharma",
//                 "submittedAt":     "2025-04-20T10:30:00Z",
//                 "actionedById":    "usr_xyz",
//                 "actionedByName":  "Bob Verma",
//                 "actionedAt":      "2025-04-20T11:00:00Z",
//                 "comment":         "looks good"
//               }
//             ],
//             "tabs": {
//               "settlements": [
//                 { "permissionId": 15, "action": "canEdit", ... }
//               ]
//             }
//           }
//         ]
//       }
//     ]
//   }
// ---------------------------------------------------------------------------

func GetRolePermissionAuditTable(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value("user_id")
		var req struct {
			UserID   string `json:"user_id"`
			RoleName string `json:"roleName,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if ctxUserID, ok := userID.(string); ok && ctxUserID != "" {
			req.UserID = ctxUserID
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		// latest_req CTE: one row per (role_id, permission_id) — most recent request only
		query := `
			WITH latest_req AS (
				SELECT DISTINCT ON (role_id, permission_id)
					id,
					role_id,
					permission_id,
					old_value,
					new_value,
					status,
					submitted_by,
					submitted_at,
					actioned_by,
					actioned_at,
					comment
				FROM public.role_permission_requests
				ORDER BY role_id, permission_id, submitted_at DESC
			)
			SELECT
				ro.id                                               AS role_id,
				ro.name                                             AS role_name,
				COALESCE(ro.roles_permission_status, 'Pending')     AS role_perm_status,
				p.id                                                AS permission_id,
				p.page_name,
				COALESCE(p.tab_name, '')                            AS tab_name,
				p.action,
				COALESCE(rp.allowed, false)                         AS current_allowed,
				COALESCE(rp.status,  'none')                        AS current_status,
				lr.id                                               AS latest_request_id,
				lr.status                                           AS request_status,
				lr.old_value,
				lr.new_value,
				lr.submitted_by,
				COALESCE(su.employee_name, lr.submitted_by)         AS submitted_by_name,
				lr.submitted_at,
				lr.actioned_by,
				COALESCE(au.employee_name, lr.actioned_by)          AS actioned_by_name,
				lr.actioned_at,
				lr.comment
			FROM public.roles            ro
			JOIN  public.role_permissions rp  ON rp.role_id       = ro.id
			JOIN  public.permissions      p   ON p.id             = rp.permission_id
			LEFT JOIN latest_req          lr  ON lr.role_id       = ro.id
			                                 AND lr.permission_id = p.id
			LEFT JOIN public.users        su  ON su.id            = lr.submitted_by
			LEFT JOIN public.users        au  ON au.id            = lr.actioned_by
			WHERE 1=1
		`
		args := []interface{}{}
		if strings.TrimSpace(req.RoleName) != "" {
			query += ` AND LOWER(ro.name) = LOWER($1)`
			args = append(args, req.RoleName)
		}
		// Order matters for grouping: role → page → tab → action
		query += ` ORDER BY ro.name, p.page_name, COALESCE(p.tab_name,''), p.action`

		rows, err := db.Query(query, args...)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		// ---- in-memory grouping structs ----

		type PermLine struct {
			PermissionID    int        `json:"permissionId"`
			Action          string     `json:"action"`
			CurrentAllowed  bool       `json:"currentAllowed"`
			CurrentStatus   string     `json:"currentStatus"`
			LatestRequestID *int       `json:"latestRequestId,omitempty"`
			RequestStatus   *string    `json:"requestStatus,omitempty"`
			OldValue        *bool      `json:"oldValue,omitempty"`
			NewValue        *bool      `json:"newValue,omitempty"`
			SubmittedByID   *string    `json:"submittedById,omitempty"`
			SubmittedByName *string    `json:"submittedByName,omitempty"`
			SubmittedAt     *time.Time `json:"submittedAt,omitempty"`
			ActionedByID    *string    `json:"actionedById,omitempty"`
			ActionedByName  *string    `json:"actionedByName,omitempty"`
			ActionedAt      *time.Time `json:"actionedAt,omitempty"`
			Comment         *string    `json:"comment,omitempty"`
		}

		// PageEntry holds top-level (no tab) permissions and tab-grouped permissions
		type PageEntry struct {
			PageName        string                `json:"pageName"`
			PagePermissions []PermLine            `json:"pagePermissions"`
			Tabs            map[string][]PermLine `json:"tabs,omitempty"`
		}

		type RoleEntry struct {
			RoleName              string       `json:"roleName"`
			RolesPermissionStatus string       `json:"rolesPermissionStatus"`
			TotalPermissions      int          `json:"totalPermissions"`
			Pages                 []*PageEntry `json:"pages"`
		}

		// keyed maps to preserve insertion order via separate slices
		roleMap := make(map[string]*RoleEntry)
		roleOrder := []string{}
		pageMap := make(map[string]map[string]*PageEntry) // role→page
		pageOrder := make(map[string][]string)            // role→[]pageName (ordered)

		for rows.Next() {
			// raw scan targets
			var (
				roleID, roleName, rolePermStatus string
				permID                           int
				pageName, tabName, action        string
				currentAllowed                   bool
				currentStatus                    string
				latestReqID                      sql.NullInt64
				reqStatus                        sql.NullString
				oldValue, newValue               sql.NullBool
				submittedByID, submittedByName   sql.NullString
				submittedAt                      sql.NullTime
				actionedByID, actionedByName     sql.NullString
				actionedAt                       sql.NullTime
				comment                          sql.NullString
			)

			if err := rows.Scan(
				&roleID, &roleName, &rolePermStatus,
				&permID, &pageName, &tabName, &action,
				&currentAllowed, &currentStatus,
				&latestReqID, &reqStatus,
				&oldValue, &newValue,
				&submittedByID, &submittedByName, &submittedAt,
				&actionedByID, &actionedByName, &actionedAt,
				&comment,
			); err != nil {
				continue
			}

			// Build PermLine
			pl := PermLine{
				PermissionID:   permID,
				Action:         action,
				CurrentAllowed: currentAllowed,
				CurrentStatus:  currentStatus,
			}
			if latestReqID.Valid {
				v := int(latestReqID.Int64)
				pl.LatestRequestID = &v
			}
			if reqStatus.Valid {
				pl.RequestStatus = &reqStatus.String
			}
			if oldValue.Valid {
				pl.OldValue = &oldValue.Bool
			}
			if newValue.Valid {
				pl.NewValue = &newValue.Bool
			}
			if submittedByID.Valid {
				pl.SubmittedByID = &submittedByID.String
			}
			if submittedByName.Valid {
				pl.SubmittedByName = &submittedByName.String
			}
			if submittedAt.Valid {
				pl.SubmittedAt = &submittedAt.Time
			}
			if actionedByID.Valid {
				pl.ActionedByID = &actionedByID.String
			}
			if actionedByName.Valid {
				pl.ActionedByName = &actionedByName.String
			}
			if actionedAt.Valid {
				pl.ActionedAt = &actionedAt.Time
			}
			if comment.Valid {
				pl.Comment = &comment.String
			}

			// Ensure role exists
			if _, ok := roleMap[roleName]; !ok {
				roleMap[roleName] = &RoleEntry{
					RoleName:              roleName,
					RolesPermissionStatus: rolePermStatus,
					Pages:                 []*PageEntry{},
				}
				pageMap[roleName] = make(map[string]*PageEntry)
				pageOrder[roleName] = []string{}
				roleOrder = append(roleOrder, roleName)
			}

			// Ensure page exists under role
			if _, ok := pageMap[roleName][pageName]; !ok {
				pe := &PageEntry{
					PageName:        pageName,
					PagePermissions: []PermLine{},
					Tabs:            map[string][]PermLine{},
				}
				pageMap[roleName][pageName] = pe
				pageOrder[roleName] = append(pageOrder[roleName], pageName)
				roleMap[roleName].Pages = append(roleMap[roleName].Pages, pe)
			}

			pe := pageMap[roleName][pageName]

			// Slot into pagePermissions (no tab) or tabs
			if tabName == "" {
				pe.PagePermissions = append(pe.PagePermissions, pl)
			} else {
				pe.Tabs[tabName] = append(pe.Tabs[tabName], pl)
			}

			roleMap[roleName].TotalPermissions++
		}

		// Strip empty Tabs maps so JSON is clean
		for _, re := range roleMap {
			for _, pe := range re.Pages {
				if len(pe.Tabs) == 0 {
					pe.Tabs = nil
				}
			}
		}

		// Build ordered result slice
		result := make([]*RoleEntry, 0, len(roleOrder))
		for _, name := range roleOrder {
			result = append(result, roleMap[name])
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"totalRoles": len(result),
			"roles":      result,
		})
	}
}
