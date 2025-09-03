package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

type CashFlowCategoryRequest struct {
	CategoryName     string `json:"category_name"`
	CategoryType     string `json:"category_type"`
	ParentCategoryID string `json:"parent_category_id"`
	DefaultMapping   string `json:"default_mapping"`
	CashflowNature   string `json:"cashflow_nature"`
	UsageFlag        string `json:"usage_flag"`
	Description      string `json:"description"`
	Status           string `json:"status"`
	CategoryLevel    int    `json:"category_level"`
}

// ifaceToString converts a scanned pgx value (interface{}) to string safely
func ifaceToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprint(t)
	}
}

// ifaceToTimeString converts scanned time-like values to formatted string
func ifaceToTimeString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case time.Time:
		return t.Format("2006-01-02 15:04:05")
	case *time.Time:
		if t == nil {
			return ""
		}
		return t.Format("2006-01-02 15:04:05")
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

// ifaceToInt converts scanned numeric-like interface{} to int (safe)
func ifaceToInt(v interface{}) int {
	if v == nil {
		return 0
	}
	switch t := v.(type) {
	case int:
		return t
	case int8:
		return int(t)
	case int16:
		return int(t)
	case int32:
		return int(t)
	case int64:
		return int(t)
	case uint:
		return int(t)
	case uint8:
		return int(t)
	case uint16:
		return int(t)
	case uint32:
		return int(t)
	case uint64:
		return int(t)
	case float32:
		return int(t)
	case float64:
		return int(t)
	case []byte:
		s := string(t)
		var i int
		fmt.Sscan(s, &i)
		return i
	case string:
		var i int
		fmt.Sscan(t, &i)
		return i
	default:
		return 0
	}
}

// Helper: get file extension
func getFileExt(filename string) string {
	return strings.ToLower(filepath.Ext(filename))
}

// Helper: parse uploaded file into [][]string
func parseCashFlowCategoryFile(file multipart.File, ext string) ([][]string, error) {
	if ext == ".csv" {
		r := csv.NewReader(file)
		return r.ReadAll()
	}
	if ext == ".xlsx" || ext == ".xls" {
		f, err := excelize.OpenReader(file)
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	return nil, errors.New("unsupported file type")
}

// CreateAndSyncCashFlowCategories inserts categories into mastercashflowcategory and creates relationships
// Request shape: { "user_id": "...", "categories": [ { ... } ] }
func CreateAndSyncCashFlowCategories(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string                    `json:"user_id"`
			Categories []CashFlowCategoryRequest `json:"categories"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// validate session and get created_by
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()
		created := make([]map[string]interface{}, 0)
		// map index->category_id in case parent references are in same payload by id
		nameToID := make(map[string]string)

		for _, cat := range req.Categories {
			// basic validation
			if cat.CategoryName == "" || cat.CategoryType == "" || cat.DefaultMapping == "" || cat.CashflowNature == "" || cat.UsageFlag == "" || cat.Status == "" {
				created = append(created, map[string]interface{}{"success": false, "error": "Missing required fields", "category_name": cat.CategoryName})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{"success": false, "error": "Failed to start transaction: " + err.Error(), "category_name": cat.CategoryName})
				continue
			}

			var categoryID string
			// Insert into master table and return generated category_id
			insertSQL := `INSERT INTO mastercashflowcategory (
					category_name, category_type, parent_category_id, default_mapping, cashflow_nature, usage_flag, description, status, category_level
				) VALUES ($1,$2, NULLIF($3, '')::uuid, $4, $5, $6, $7, $8, $9) RETURNING category_id`
			err = tx.QueryRow(ctx, insertSQL,
				cat.CategoryName,
				cat.CategoryType,
				cat.ParentCategoryID,
				cat.DefaultMapping,
				cat.CashflowNature,
				cat.UsageFlag,
				cat.Description,
				cat.Status,
				cat.CategoryLevel,
				// createdBy,
			).Scan(&categoryID)
			if err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": err.Error(), "category_name": cat.CategoryName})
				continue
			}

			// insert audit action
			auditSQL := `INSERT INTO auditactioncashflowcategory (
				category_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditSQL, categoryID, nil, createdBy); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "Audit insert failed: " + err.Error(), "category_id": categoryID})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "Commit failed: " + err.Error(), "category_id": categoryID})
				continue
			}

			// record created
			created = append(created, map[string]interface{}{"success": true, "category_id": categoryID, "category_name": cat.CategoryName})
			nameToID[strings.ToLower(cat.CategoryName)] = categoryID
		}

		// Create relationships: iterate again and insert where parent provided
		relAdded := 0
		for _, cat := range req.Categories {
			childID := ""
			// find child id by name map (we stored by name), but prefer parent_category_id supplied
			if id := nameToID[strings.ToLower(cat.CategoryName)]; id != "" {
				childID = id
			}
			if childID == "" {
				continue
			}
			parentID := strings.TrimSpace(cat.ParentCategoryID)
			if parentID == "" {
				continue
			}
			// insert relationship if not exists
			ctx := context.Background()
			var exists bool
			err := pgxPool.QueryRow(ctx, `SELECT true FROM cashflowcategoryrelationships WHERE parent_category_id=$1 AND child_category_id=$2`, parentID, childID).Scan(&exists)
			if err == nil && exists {
				continue
			}
			// ignore error from select (treat as not exists)
			if _, err := pgxPool.Exec(ctx, `INSERT INTO cashflowcategoryrelationships (parent_category_id, child_category_id, status) VALUES ($1,$2,'Active')`, parentID, childID); err == nil {
				relAdded++
			}
		}

		// respond
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":             true,
			"created":             created,
			"relationships_added": relAdded,
		})
	}
}

func GetCashFlowCategoryHierarchyPGX(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Step 1: fetch categories with latest audit info
		query := `
			SELECT m.category_id, m.category_name, m.category_type, m.parent_category_id,
			       m.default_mapping, m.cashflow_nature, m.usage_flag, m.description, m.status,
			       m.old_category_name, m.old_category_type, m.old_parent_category_id,
			       m.old_default_mapping, m.old_cashflow_nature, m.old_usage_flag, m.old_description, m.old_status,
			       m.is_top_level_category, m.is_deleted, m.category_level, m.old_category_level,
			       a.processing_status, a.requested_by, a.requested_at, a.actiontype, a.action_id,
			       a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM mastercashflowcategory m
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, requested_at, actiontype, action_id,
				       checker_by, checker_at, checker_comment, reason
				FROM auditactioncashflowcategory a
				WHERE a.category_id = m.category_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE;
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		entityMap := map[string]map[string]interface{}{}
		typeIDs := []string{}
		hideIds := map[string]bool{}

		for rows.Next() {
			var (
				categoryID string
				// all scanned as interface{} then converted
				categoryNameI, categoryTypeI, parentCategoryIDI,
				defaultMappingI, cashflowNatureI, usageFlagI, descriptionI, statusI,
				oldCategoryNameI, oldCategoryTypeI, oldParentCategoryIDI, oldDefaultMappingI,
				oldCashflowNatureI, oldUsageFlagI, oldDescriptionI, oldStatusI,
				categoryLevelI, oldCategoryLevelI, processingStatusI,
				requestedByI, requestedAtI, actionTypeI, actionIDI,
				checkerByI, checkerAtI, checkerCommentI, reasonI interface{}
				isTopLevelCategory, isDeleted bool
			)

			if err := rows.Scan(
				&categoryID, &categoryNameI, &categoryTypeI, &parentCategoryIDI,
				&defaultMappingI, &cashflowNatureI, &usageFlagI, &descriptionI, &statusI,
				&oldCategoryNameI, &oldCategoryTypeI, &oldParentCategoryIDI,
				&oldDefaultMappingI, &oldCashflowNatureI, &oldUsageFlagI,
				&oldDescriptionI, &oldStatusI,
				&isTopLevelCategory, &isDeleted, &categoryLevelI, &oldCategoryLevelI,
				&processingStatusI, &requestedByI, &requestedAtI, &actionTypeI, &actionIDI,
				&checkerByI, &checkerAtI, &checkerCommentI, &reasonI,
			); err != nil {
				continue
			}

			entityMap[categoryID] = map[string]interface{}{
				"id":   categoryID,
				"name": ifaceToString(categoryNameI),
				"data": map[string]interface{}{
					"category_id":            categoryID,
					"category_name":          ifaceToString(categoryNameI),
					"category_type":          ifaceToString(categoryTypeI),
					"parent_category_id":     ifaceToString(parentCategoryIDI),
					"default_mapping":        ifaceToString(defaultMappingI),
					"cashflow_nature":        ifaceToString(cashflowNatureI),
					"usage_flag":             ifaceToString(usageFlagI),
					"description":            ifaceToString(descriptionI),
					"status":                 ifaceToString(statusI),
					"old_category_name":      ifaceToString(oldCategoryNameI),
					"old_category_type":      ifaceToString(oldCategoryTypeI),
					"old_parent_category_id": ifaceToString(oldParentCategoryIDI),
					"old_default_mapping":    ifaceToString(oldDefaultMappingI),
					"old_cashflow_nature":    ifaceToString(oldCashflowNatureI),
					"old_usage_flag":         ifaceToString(oldUsageFlagI),
					"old_description":        ifaceToString(oldDescriptionI),
					"old_status":             ifaceToString(oldStatusI),
					"is_top_level_category":  isTopLevelCategory,
					"is_deleted":             isDeleted,
					"category_level":         ifaceToInt(categoryLevelI),
					"old_category_level":     ifaceToInt(oldCategoryLevelI),
					"processing_status":      ifaceToString(processingStatusI),
					"action_type":            ifaceToString(actionTypeI),
					"action_id":              ifaceToString(actionIDI),
					"checker_by":             ifaceToString(checkerByI),
					"checker_at":             ifaceToTimeString(checkerAtI),
					"checker_comment":        ifaceToString(checkerCommentI),
					"reason":                 ifaceToString(reasonI),
				},
				"children": []interface{}{},
			}

			typeIDs = append(typeIDs, categoryID)

			if isDeleted && strings.ToUpper(ifaceToString(processingStatusI)) == "APPROVED" {
				hideIds[categoryID] = true
			}
		}

		// Step 2: fetch CREATE/EDIT/DELETE audit history in bulk
		if len(typeIDs) > 0 {
			auditQuery := `
				SELECT category_id, actiontype, requested_by, requested_at
				FROM auditactioncashflowcategory
				WHERE category_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')
				ORDER BY requested_at DESC;
			`
			auditRows, err := pgxPool.Query(ctx, auditQuery, typeIDs)
			if err == nil {
				defer auditRows.Close()
				auditMap := make(map[string]map[string]string)
				for auditRows.Next() {
					var cid, atype string
					var rbyI, ratI interface{}
					if err := auditRows.Scan(&cid, &atype, &rbyI, &ratI); err == nil {
						if auditMap[cid] == nil {
							auditMap[cid] = map[string]string{}
						}
						switch atype {
						case "CREATE":
							if auditMap[cid]["created_by"] == "" {
								auditMap[cid]["created_by"] = ifaceToString(rbyI)
								auditMap[cid]["created_at"] = ifaceToTimeString(ratI)
							}
						case "EDIT":
							if auditMap[cid]["edited_by"] == "" {
								auditMap[cid]["edited_by"] = ifaceToString(rbyI)
								auditMap[cid]["edited_at"] = ifaceToTimeString(ratI)
							}
						case "DELETE":
							if auditMap[cid]["deleted_by"] == "" {
								auditMap[cid]["deleted_by"] = ifaceToString(rbyI)
								auditMap[cid]["deleted_at"] = ifaceToTimeString(ratI)
							}
						}
					}
				}

				// merge into entityMap
				for cid, info := range auditMap {
					if entity, ok := entityMap[cid]; ok {
						data := entity["data"].(map[string]interface{})
						for k, v := range info {
							data[k] = v
						}
					}
				}
			}
		}

		// Step 3: fetch relationships
		relRows, err := pgxPool.Query(ctx, "SELECT parent_category_id, child_category_id FROM cashflowcategoryrelationships")
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()

		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}

		// Step 4: hide deleted+approved categories (and descendants)
		if len(hideIds) > 0 {
			getAllDescendants := func(start []string) []string {
				all := map[string]bool{}
				queue := append([]string{}, start...)
				for _, id := range start {
					all[id] = true
				}
				for len(queue) > 0 {
					cur := queue[0]
					queue = queue[1:]
					for _, child := range parentMap[cur] {
						if !all[child] {
							all[child] = true
							queue = append(queue, child)
						}
					}
				}
				res := []string{}
				for id := range all {
					res = append(res, id)
				}
				return res
			}
			// toHide := getAllDescendants(maps.Keys(hideIds))
			start := []string{}
			for id := range hideIds {
				start = append(start, id)
			}
			toHide := getAllDescendants(start)

			for _, id := range toHide {
				delete(entityMap, id)
			}
		}

		// Step 5: rebuild children arrays
		for _, e := range entityMap {
			e["children"] = []interface{}{}
		}
		for parentID, children := range parentMap {
			if entityMap[parentID] != nil {
				for _, childID := range children {
					if entityMap[childID] != nil {
						entityMap[parentID]["children"] = append(entityMap[parentID]["children"].([]interface{}), entityMap[childID])
					}
				}
			}
		}

		// Step 6: build final rows (preserve query order via typeIDs)
		rowsOut := []map[string]interface{}{}
		// children map: only include children that still exist in entityMap
		for _, id := range typeIDs {
			if entityMap[id] == nil {
				continue
			}
			data := entityMap[id]["data"].(map[string]interface{})
			// build children id slice
			childrenIDs := []string{}
			for _, c := range parentMap[id] {
				if entityMap[c] != nil {
					childrenIDs = append(childrenIDs, c)
				}
			}

			rec := map[string]interface{}{
				"category_id":            ifaceToString(data["category_id"]),
				"category_name":          ifaceToString(data["category_name"]),
				"category_type":          ifaceToString(data["category_type"]),
				"parent_category_id":     ifaceToString(data["parent_category_id"]),
				"default_mapping":        ifaceToString(data["default_mapping"]),
				"cashflow_nature":        ifaceToString(data["cashflow_nature"]),
				"usage_flag":             ifaceToString(data["usage_flag"]),
				"description":            ifaceToString(data["description"]),
				"status":                 ifaceToString(data["status"]),
				"old_category_name":      ifaceToString(data["old_category_name"]),
				"old_category_type":      ifaceToString(data["old_category_type"]),
				"old_parent_category_id": ifaceToString(data["old_parent_category_id"]),
				"old_default_mapping":    ifaceToString(data["old_default_mapping"]),
				"old_cashflow_nature":    ifaceToString(data["old_cashflow_nature"]),
				"old_usage_flag":         ifaceToString(data["old_usage_flag"]),
				"old_description":        ifaceToString(data["old_description"]),
				"old_status":             ifaceToString(data["old_status"]),
				"category_level":         ifaceToInt(data["category_level"]),
				"old_category_level":     ifaceToInt(data["old_category_level"]),
				"is_top_level_category":  data["is_top_level_category"],
				"is_deleted":             data["is_deleted"],
				"processing_status":      ifaceToString(data["processing_status"]),
				"requested_by":           ifaceToString(data["requested_by"]),
				"requested_at":           ifaceToTimeString(data["requested_at"]),
				"action_type":            ifaceToString(data["action_type"]),
				"action_id":              ifaceToString(data["action_id"]),
				"checker_by":             ifaceToString(data["checker_by"]),
				"checker_at":             ifaceToTimeString(data["checker_at"]),
				"checker_comment":        ifaceToString(data["checker_comment"]),
				"reason":                 ifaceToString(data["reason"]),
				// audit summary fields merged earlier (if present)
				"created_by": ifaceToString(data["created_by"]),
				"created_at": ifaceToString(data["created_at"]),
				"edited_by":  ifaceToString(data["edited_by"]),
				"edited_at":  ifaceToString(data["edited_at"]),
				"deleted_by": ifaceToString(data["deleted_by"]),
				"deleted_at": ifaceToString(data["deleted_at"]),
				"children":   childrenIDs,
			}
			rowsOut = append(rowsOut, rec)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": rowsOut})
	}
}

// FindParentCashFlowCategoryAtLevel returns the ancestor of a category at the requested level (pgx)
func FindParentCashFlowCategoryAtLevel(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Parse request
		var req struct {
			UserID string `json:"user_id"`
			Level  int    `json:"level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing or invalid user_id/level")
			return
		}

		// Validate user session
		validUser := false
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				validUser = true
				break
			}
		}
		if !validUser {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		parentLevel := req.Level - 1
		query := `
			SELECT m.category_name, m.category_id
			FROM mastercashflowcategory m
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactioncashflowcategory a
				WHERE a.category_id = m.category_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.category_level = $1
			  AND (m.is_deleted = false OR m.is_deleted IS NULL)
			  AND LOWER(m.status) = 'active'
		`

		rows, err := pgxPool.Query(context.Background(), query, parentLevel)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var name, id string
			if err := rows.Scan(&name, &id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			results = append(results, map[string]interface{}{"name": name, "id": id})
		}

		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "results": results})
	}
}

// GetCashFlowCategoryNamesWithID returns minimal id/name/type for all categories (requires user_id)
func GetCashFlowCategoryNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		// validate session
		valid := false
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()
		// select with latest audit processing_status and is_deleted to filter out deleted+approved entries
		rows, err := pgxPool.Query(ctx, `
			SELECT m.category_id, m.category_name, m.category_type, m.is_deleted, a.processing_status
			FROM mastercashflowcategory m
			LEFT JOIN LATERAL (
				SELECT processing_status FROM auditactioncashflowcategory a WHERE a.category_id = m.category_id ORDER BY requested_at DESC LIMIT 1
			) a ON TRUE
		`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		for rows.Next() {
			var id, name, ctype interface{}
			var isDeleted bool
			var processingStatus interface{}
			if err := rows.Scan(&id, &name, &ctype, &isDeleted, &processingStatus); err == nil {
				// hide when deleted and latest processing_status is APPROVED
				if isDeleted && strings.ToUpper(ifaceToString(processingStatus)) == "APPROVED" {
					continue
				}
				out = append(out, map[string]interface{}{"category_id": ifaceToString(id), "category_name": ifaceToString(name), "category_type": ifaceToString(ctype)})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "categories": out})
	}
}

// UpdateCashFlowCategoryBulk updates multiple cashflow categories with old_* preservation and audit via pgx
func UpdateCashFlowCategoryBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string `json:"user_id"`
			Categories []struct {
				CategoryID string                 `json:"category_id"`
				Fields     map[string]interface{} `json:"fields"`
				Reason     string                 `json:"reason"`
			} `json:"categories"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// get updated_by from session
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				updatedBy = s.Email
				break
			}
		}
		if updatedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		var results []map[string]interface{}
		var relationshipsAdded []map[string]interface{}

		ctx := context.Background()
		for _, cat := range req.Categories {
			if cat.CategoryID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "Missing category_id"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Failed to start transaction: " + err.Error(), "category_id": cat.CategoryID})
				continue
			}
			committed := false

			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()

				// fetch existing values FOR UPDATE
				var (
					existingCategoryNameI     interface{}
					existingCategoryTypeI     interface{}
					existingParentCategoryIDI interface{}
					existingDefaultMappingI   interface{}
					existingCashflowNatureI   interface{}
					existingUsageFlagI        interface{}
					existingDescriptionI      interface{}
					existingStatusI           interface{}
					existingCategoryLevelI    interface{}
					existingIsTopLevel        bool
					existingIsDeleted         bool
				)
				sel := `SELECT category_name, category_type, parent_category_id, default_mapping, cashflow_nature, usage_flag, description, status, is_top_level_category, is_deleted, category_level FROM mastercashflowcategory WHERE category_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, cat.CategoryID).Scan(&existingCategoryNameI, &existingCategoryTypeI, &existingParentCategoryIDI, &existingDefaultMappingI, &existingCashflowNatureI, &existingUsageFlagI, &existingDescriptionI, &existingStatusI, &existingIsTopLevel, &existingIsDeleted, &existingCategoryLevelI); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Failed to fetch existing category: " + err.Error(), "category_id": cat.CategoryID})
					return
				}

				// build dynamic update
				var sets []string
				var args []interface{}
				pos := 1
				parentProvided := ""
				for k, v := range cat.Fields {
					switch k {
					case "category_name":
						sets = append(sets, fmt.Sprintf("category_name=$%d, old_category_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCategoryNameI))
						pos += 2
					case "category_type":
						sets = append(sets, fmt.Sprintf("category_type=$%d, old_category_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCategoryTypeI))
						pos += 2
					case "parent_category_id":
						newParent := fmt.Sprint(v)
						parentProvided = newParent
						sets = append(sets, fmt.Sprintf("parent_category_id=$%d, old_parent_category_id=$%d", pos, pos+1))
						args = append(args, newParent, ifaceToString(existingParentCategoryIDI))
						pos += 2
					case "default_mapping":
						sets = append(sets, fmt.Sprintf("default_mapping=$%d, old_default_mapping=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDefaultMappingI))
						pos += 2
					case "cashflow_nature":
						sets = append(sets, fmt.Sprintf("cashflow_nature=$%d, old_cashflow_nature=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCashflowNatureI))
						pos += 2
					case "usage_flag":
						sets = append(sets, fmt.Sprintf("usage_flag=$%d, old_usage_flag=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingUsageFlagI))
						pos += 2
					case "description":
						sets = append(sets, fmt.Sprintf("description=$%d, old_description=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDescriptionI))
						pos += 2
					case "status":
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatusI))
						pos += 2
					case "category_level":
						// new integer level, preserve old_category_level
						var newLevel int
						switch t := v.(type) {
						case float64:
							newLevel = int(t)
						case int:
							newLevel = t
						case string:
							var tmp int
							fmt.Sscan(t, &tmp)
							newLevel = tmp
						default:
							newLevel = 0
						}
						sets = append(sets, fmt.Sprintf("category_level=$%d, old_category_level=$%d", pos, pos+1))
						args = append(args, newLevel, ifaceToInt(existingCategoryLevelI))
						pos += 2
					case "is_top_level_category":
						var newBool bool
						switch t := v.(type) {
						case bool:
							newBool = t
						case string:
							if strings.ToLower(t) == "true" {
								newBool = true
							} else {
								newBool = false
							}
						default:
							newBool = false
						}
						sets = append(sets, fmt.Sprintf("is_top_level_category=$%d", pos))
						args = append(args, newBool)
						pos++
					case "is_deleted":
						var newBool bool
						switch t := v.(type) {
						case bool:
							newBool = t
						case string:
							if strings.ToLower(t) == "true" {
								newBool = true
							} else {
								newBool = false
							}
						default:
							newBool = false
						}
						sets = append(sets, fmt.Sprintf("is_deleted=$%d", pos))
						args = append(args, newBool)
						pos++
					default:
						// ignore unknown keys
					}
				}

				var updatedCategoryID string
				if len(sets) > 0 {
					q := "UPDATE mastercashflowcategory SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE category_id=$%d RETURNING category_id", pos)
					args = append(args, cat.CategoryID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedCategoryID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": err.Error(), "category_id": cat.CategoryID})
						return
					}
				} else {
					updatedCategoryID = cat.CategoryID
				}

				// Insert audit action
				auditQuery := `INSERT INTO auditactioncashflowcategory (
					category_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, $2, $3, $4, $5, now())`
				if _, err := tx.Exec(ctx, auditQuery, updatedCategoryID, "EDIT", "PENDING_EDIT_APPROVAL", cat.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Category updated but audit log failed: " + err.Error(), "category_id": updatedCategoryID})
					return
				}

				// Sync relationships if parent_category_id provided
				if parentProvided != "" {
					var exists bool
					err := tx.QueryRow(ctx, `SELECT true FROM cashflowcategoryrelationships WHERE parent_category_id=$1 AND child_category_id=$2`, parentProvided, updatedCategoryID).Scan(&exists)
					if err == nil && exists {
						// already exists
					} else {
						if _, err := tx.Exec(ctx, `INSERT INTO cashflowcategoryrelationships (parent_category_id, child_category_id, status) VALUES ($1,$2,'Active')`, parentProvided, updatedCategoryID); err == nil {
							relationshipsAdded = append(relationshipsAdded, map[string]interface{}{"success": true, "parent_category_id": parentProvided, "child_category_id": updatedCategoryID})
						}
					}
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Transaction commit failed: " + err.Error(), "category_id": updatedCategoryID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "category_id": updatedCategoryID})
			}()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":            true,
			"results":            results,
			"relationshipsAdded": len(relationshipsAdded),
			"details":            relationshipsAdded,
		})
	}
}

// DeleteCashFlowCategory marks a category and its descendants as deleted and creates audit actions (pgx-only)
func DeleteCashFlowCategory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			IDs    []string `json:"ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		if req.UserID == "" || len(req.IDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Missing user_id or ids")
			return
		}

		// get requested_by from session
		requestedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()

		// Fetch relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_category_id, child_category_id FROM cashflowcategoryrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}

		// Traverse descendants
		getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				for _, child := range parentMap[current] {
					if !all[child] {
						all[child] = true
						queue = append(queue, child)
					}
				}
			}
			result := []string{}
			for id := range all {
				result = append(result, id)
			}
			return result
		}

		allToDelete := getAllDescendants(req.IDs)
		if len(allToDelete) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No category found to delete")
			return
		}

		// Start transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// Mark as deleted and RETURNING category_id
		rows, err := tx.Query(ctx, `UPDATE mastercashflowcategory SET is_deleted = true WHERE category_id = ANY($1) RETURNING category_id`, allToDelete)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		updated := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				updated = append(updated, id)
			}
		}

		if len(updated) == 0 {
			tx.Rollback(ctx)
			api.RespondWithError(w, http.StatusBadRequest, "No rows updated")
			return
		}

		// Insert audit actions for each updated id
		for _, cid := range updated {
			if _, err := tx.Exec(ctx, `INSERT INTO auditactioncashflowcategory (category_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now())`, cid, req.Reason, requestedBy); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}
		committed = true

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}

// BulkRejectCashFlowCategoryActions rejects audit actions for categories (and their descendants)
// using pgx: updates processing_status='REJECTED', sets checker_by, checker_at and checker_comment
func BulkRejectCashFlowCategoryActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CategoryIDs []string `json:"category_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// get checker_by from session
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()

		// Fetch relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_category_id, child_category_id FROM cashflowcategoryrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}

		// Traverse descendants
		getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				for _, child := range parentMap[current] {
					if !all[child] {
						all[child] = true
						queue = append(queue, child)
					}
				}
			}
			res := []string{}
			for id := range all {
				res = append(res, id)
			}
			return res
		}

		allToReject := getAllDescendants(req.CategoryIDs)
		if len(allToReject) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No categories found to reject")
			return
		}

		// Update audit rows to REJECTED and return affected rows
		query := `UPDATE auditactioncashflowcategory SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE category_id = ANY($3) RETURNING action_id, category_id`
		rows, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToReject)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var updated []map[string]interface{}
		for rows.Next() {
			var actionID, categoryID string
			if err := rows.Scan(&actionID, &categoryID); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": actionID, "category_id": categoryID})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		success := len(updated) > 0
		resp := map[string]interface{}{"success": success, "updated": updated}
		if !success {
			resp["message"] = "No rows updated"
		}
		json.NewEncoder(w).Encode(resp)
	}
}

// BulkApproveCashFlowCategoryActions approves audit actions for categories (and their descendants)
// using pgx: updates processing_status='APPROVED', sets checker_by, checker_at and checker_comment
// Returns the list of updated action_ids and category_ids along with action_type so callers can act accordingly.
func BulkApproveCashFlowCategoryActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CategoryIDs []string `json:"category_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// get checker_by from session
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()

		// Fetch relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_category_id, child_category_id FROM cashflowcategoryrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			if err := relRows.Scan(&parentID, &childID); err == nil {
				parentMap[parentID] = append(parentMap[parentID], childID)
			}
		}

		// Traverse descendants
		getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				for _, child := range parentMap[current] {
					if !all[child] {
						all[child] = true
						queue = append(queue, child)
					}
				}
			}
			res := []string{}
			for id := range all {
				res = append(res, id)
			}
			return res
		}

		allToApprove := getAllDescendants(req.CategoryIDs)
		if len(allToApprove) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No categories found to approve")
			return
		}

		// Update audit rows to APPROVED and return affected rows including action type
		query := `UPDATE auditactioncashflowcategory SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE category_id = ANY($3) RETURNING action_id, category_id, actiontype`
		rows, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToApprove)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var updated []map[string]interface{}
		for rows.Next() {
			var actionID, categoryID, actionType string
			if err := rows.Scan(&actionID, &categoryID, &actionType); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": actionID, "category_id": categoryID, "action_type": actionType})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		success := len(updated) > 0
		resp := map[string]interface{}{"success": success, "updated": updated}
		if !success {
			resp["message"] = "No rows updated"
		}
		json.NewEncoder(w).Encode(resp)
	}
}

// Handler: UploadCashFlowCategory (staging + mapping)
func UploadCashFlowCategory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get("Content-Type") == "application/json" {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in body")
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue("user_id")
			if userID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in form")
				return
			}
		}

		// Fetch user name from active sessions
		userName := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "User not found in active sessions")
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse multipart form")
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No files uploaded")
			return
		}
		batchIDs := make([]string, 0, len(files))
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fileHeader.Filename)
				return
			}
			ext := getFileExt(fileHeader.Filename)
			records, err := parseCashFlowCategoryFile(file, ext)
			file.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fileHeader.Filename)
				return
			}
			headerRow := records[0]
			dataRows := records[1:]
			batchID := uuid.New().String()
			batchIDs = append(batchIDs, batchID)
			colCount := len(headerRow)
			copyRows := make([][]interface{}, len(dataRows))
			for i, row := range dataRows {
				vals := make([]interface{}, colCount+1)
				vals[0] = batchID
				for j := 0; j < colCount; j++ {
					if j < len(row) {
						cell := strings.TrimSpace(row[j])
						if cell == "" {
							// use nil so pgx encodes it as SQL NULL (avoids uuid binary-encoding errors)
							vals[j+1] = nil
						} else {
							vals[j+1] = cell
						}
					} else {
						vals[j+1] = nil
					}
				}
				copyRows[i] = vals
			}
			columns := append([]string{"upload_batch_id"}, headerRow...)

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction: "+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			// Stage to input table
			_, err = tx.CopyFrom(
				ctx,
				pgx.Identifier{"input_cashflow_category"},
				columns,
				pgx.CopyFromRows(copyRows),
			)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// Read mapping
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_cashflow_category`)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error")
				return
			}
			mapping := make(map[string]string)
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					mapping[src] = tgt
				}
			}
			mapRows.Close()
			var srcCols []string
			var tgtCols []string
			// build columns in CSV header order so SELECT columns align with INSERT columns
			for _, h := range headerRow {
				if tgt, ok := mapping[h]; ok {
					srcCols = append(srcCols, h)
					tgtCols = append(tgtCols, tgt)
				} else {
					// missing mapping for a header column -> fail
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No mapping for source column: %s", h))
					return
				}
			}
			tgtColsStr := strings.Join(tgtCols, ", ")

			// Build SELECT expressions in order and handle parent_category_id (text -> uuid)
			var selectExprs []string
			for i, src := range srcCols {
				tgt := tgtCols[i]
				if strings.ToLower(tgt) == "parent_category_id" {
					// Avoid calling trim on a column that may already be uuid-typed.
					// Convert the column to text for the emptiness check, then cast to uuid when non-empty.
					selectExprs = append(selectExprs, fmt.Sprintf("CASE WHEN COALESCE(s.%s::text, '') = '' THEN NULL ELSE s.%s::uuid END AS %s", src, src, tgt))
				} else {
					selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
				}
			}
			srcColsStr := strings.Join(selectExprs, ", ")

			// Move to final table (resolve FKs via join)
			insertSQL := fmt.Sprintf(`
				INSERT INTO mastercashflowcategory (%s)
				SELECT %s
				FROM input_cashflow_category s
				WHERE s.upload_batch_id = $1
				RETURNING category_id
			`, tgtColsStr, srcColsStr)
			rows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newCategoryIDs []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil {
					newCategoryIDs = append(newCategoryIDs, id)
				}
			}
			rows.Close()

			// Bulk insert audit actions
			if len(newCategoryIDs) > 0 {
				auditSQL := `
					INSERT INTO auditactioncashflowcategory (
						category_id, actiontype, processing_status, reason, requested_by, requested_at
					)
					SELECT category_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now()
					FROM mastercashflowcategory
					WHERE category_id = ANY($2)
				`
				_, err = tx.Exec(ctx, auditSQL, userName, newCategoryIDs)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}

			// Bulk insert relationships (if parent_category_id is present)
			if len(newCategoryIDs) > 0 {
				relSQL := `
					INSERT INTO cashflowcategoryrelationships (parent_category_id, child_category_id, status)
					SELECT parent_category_id, category_id, 'Active'
					FROM mastercashflowcategory
					WHERE category_id = ANY($1) AND parent_category_id IS NOT NULL
				`
				_, err = tx.Exec(ctx, relSQL, newCategoryIDs)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert relationships: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"batch_ids": batchIDs,
			"message":   "All cash flow categories uploaded, mapped, synced, and audited",
		})
	}
}
