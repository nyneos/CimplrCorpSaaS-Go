package allMaster

import (
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type EntityMasterRequest struct {
	EntityName                string      `json:"entity_name"`
	ParentName                string      `json:"parentname"`
	IsTopLevelEntity          bool        `json:"is_top_level_entity"`
	Address                   string      `json:"address"`
	ContactPhone              string      `json:"contact_phone"`
	ContactEmail              string      `json:"contact_email"`
	RegistrationNumber        string      `json:"registration_number"`
	PanGST                    string      `json:"pan_gst"`
	LegalEntityIdentifier     string      `json:"legal_entity_identifier"`
	TaxIdentificationNumber   string      `json:"tax_identification_number"`
	DefaultCurrency           string      `json:"default_currency"`
	AssociatedBusinessUnits   string      `json:"associated_business_units"`
	ReportingCurrency         string      `json:"reporting_currency"`
	UniqueIdentifier          string      `json:"unique_identifier"`
	LegalEntityType           string      `json:"legal_entity_type"`
	FxTradingAuthority        string      `json:"fx_trading_authority"`
	InternalFxTradingLimit    string      `json:"internal_fx_trading_limit"`
	AssociatedTreasuryContact string      `json:"associated_treasury_contact"`
	IsDeleted                 bool        `json:"is_deleted"`
	ApprovalStatus            string      `json:"approval_status"`
	Level                     string      `json:"level"`
	Comments                  string      `json:"comments"`
	CompanyName               string      `json:"company_name"`
	CreatedBy                 string      `json:"created_by"`
	UpdatedBy                 string      `json:"updated_by"`
}

type EntityMasterBulkRequest struct {
	Entities []EntityMasterRequest `json:"entities"`
}

// Combined handler: creates entities and syncs relationships
func CreateAndSyncEntities(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                  `json:"user_id"`
			Entities []EntityMasterRequest   `json:"entities"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Invalid JSON"})
			return
		}
		// Get created_by from session
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		// if createdBy == "" {
		// 	w.WriteHeader(http.StatusBadRequest)
		// 	json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Invalid user_id or session"})
		// 	return
		// }
		// Insert entities
		entityIDs := make(map[string]string) // name -> id
		inserted := []map[string]interface{}{}
		for _, entity := range req.Entities {
			entityId := "E" + strings.ToUpper(uuid.New().String()[:8])
			query := `INSERT INTO masterentity (
				entity_id, entity_name, parentname, is_top_level_entity, address, contact_phone, contact_email, registration_number, pan_gst, legal_entity_identifier, tax_identification_number, default_currency, associated_business_units, reporting_currency, unique_identifier, legal_entity_type, fx_trading_authority, internal_fx_trading_limit, associated_treasury_contact, is_deleted, approval_status, level, comments, company_name, created_by, updated_by, created_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, now()
			) RETURNING entity_id`
			var newEntityID string
			err := db.QueryRow(query,
				entityId,
				entity.EntityName,
				entity.ParentName,
				entity.IsTopLevelEntity,
				entity.Address,
				entity.ContactPhone,
				entity.ContactEmail,
				entity.RegistrationNumber,
				entity.PanGST,
				entity.LegalEntityIdentifier,
				entity.TaxIdentificationNumber,
				entity.DefaultCurrency,
				entity.AssociatedBusinessUnits,
				entity.ReportingCurrency,
				entity.UniqueIdentifier,
				entity.LegalEntityType,
				entity.FxTradingAuthority,
				entity.InternalFxTradingLimit,
				entity.AssociatedTreasuryContact,
				false, // is_deleted
				"Pending", // approval_status
				entity.Level,
				entity.Comments,
				entity.CompanyName,
				createdBy,
				createdBy,
			).Scan(&newEntityID)
			if err != nil {
				inserted = append(inserted, map[string]interface{}{"success": false, "error": err.Error(), "entity_name": entity.EntityName})
				continue
			}
			entityIDs[entity.EntityName] = newEntityID
			inserted = append(inserted, map[string]interface{}{"success": true, "entity_id": newEntityID, "entity_name": entity.EntityName})
		}
		// Sync relationships
		relationshipsAdded := []map[string]interface{}{}
		for _, entity := range req.Entities {
			if entity.ParentName == "" || entityIDs[entity.ParentName] == "" || entityIDs[entity.EntityName] == "" {
				continue
			}
			parentId := entityIDs[entity.ParentName]
			childId := entityIDs[entity.EntityName]
			var exists int
			err := db.QueryRow(`SELECT 1 FROM entityrelationships WHERE parent_entity_id = $1 AND child_entity_id = $2`, parentId, childId).Scan(&exists)
			if err == sql.ErrNoRows {
				_, err := db.Exec(`INSERT INTO entityrelationships (parent_entity_id, child_entity_id, status) VALUES ($1, $2, $3)`, parentId, childId, "Active")
				if err == nil {
					relationshipsAdded = append(relationshipsAdded, map[string]interface{}{"parent_id": parentId, "child_id": childId})
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"entities": inserted,
			"relationshipsAdded": len(relationshipsAdded),
			"details": relationshipsAdded,
		})
	}
}

// GET handler to return entity hierarchy, excluding deleted entities and their descendants
func GetEntityHierarchy(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Fetch all entities
		entitiesRows, err := db.Query("SELECT * FROM masterentity")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		defer entitiesRows.Close()
		entities := []map[string]interface{}{}
		entityMap := map[string]map[string]interface{}{}
		deletedIds := map[string]bool{}
		for entitiesRows.Next() {
			var e = make(map[string]interface{})
			cols, _ := entitiesRows.Columns()
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range cols {
				valPtrs[i] = &vals[i]
			}
			entitiesRows.Scan(valPtrs...)
			for i, col := range cols {
				e[col] = vals[i]
			}
			entityID := e["entity_id"].(string)
			entityMap[entityID] = map[string]interface{}{
				"id": entityID,
				"name": e["entity_name"],
				"data": e,
				"children": []interface{}{},
			}
			if e["is_deleted"] == true {
				deletedIds[entityID] = true
			}
			entities = append(entities, e)
		}
		// Fetch relationships
		relRows, err := db.Query("SELECT * FROM entityrelationships")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		defer relRows.Close()
		relationships := []map[string]interface{}{}
		parentMap := map[string][]string{}
		for relRows.Next() {
			var rel = make(map[string]interface{})
			cols, _ := relRows.Columns()
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range cols {
				valPtrs[i] = &vals[i]
			}
			relRows.Scan(valPtrs...)
			for i, col := range cols {
				rel[col] = vals[i]
			}
			parentID := rel["parent_entity_id"].(string)
			childID := rel["child_entity_id"].(string)
			parentMap[parentID] = append(parentMap[parentID], childID)
			relationships = append(relationships, rel)
		}
		// Find all entity_ids that are deleted or descendants of deleted
	    	getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				children := parentMap[current]
				for _, child := range children {
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
		deletedList := []string{}
		for id := range deletedIds {
			deletedList = append(deletedList, id)
		}
		allDeleted := getAllDescendants(deletedList)
		// Remove deleted entities from entityMap
		for _, id := range allDeleted {
			delete(entityMap, id)
		}
		// Rebuild children arrays for remaining entities
		for _, entity := range entityMap {
			entity["children"] = []interface{}{}
		}
		for _, rel := range relationships {
			parentID := rel["parent_entity_id"].(string)
			childID := rel["child_entity_id"].(string)
			if entityMap[parentID] != nil && entityMap[childID] != nil {
				entityMap[parentID]["children"] = append(entityMap[parentID]["children"].([]interface{}), entityMap[childID])
			}
		}
		// Find top-level entities (not deleted, not child of any parent, or is_top_level_entity)
		topLevel := []interface{}{}
		for _, e := range entityMap {
			data := e["data"].(map[string]interface{})
			isTop := false
			if v, ok := data["is_top_level_entity"]; ok && v == true {
				isTop = true
			}
			isChild := false
			for _, rel := range relationships {
				if rel["child_entity_id"].(string) == e["id"].(string) {
					isChild = true
					break
				}
			}
			if isTop || !isChild {
				topLevel = append(topLevel, e)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(topLevel)
	}
}


// Approve entity (and descendants if Delete-Approval)
func ApproveEntity(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct { ID string `json:"id"`; Comments string `json:"comments"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "message": "Missing id in body" })
			return
		}
		id := req.ID
		var status string
		err := db.QueryRow(`SELECT approval_status FROM masterentity WHERE entity_id = $1`, id).Scan(&status)
		if err == sql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "message": "Entity not found" })
			return
		}
		if status == "Delete-Approval" {
			// Get all descendants
			relRows, _ := db.Query(`SELECT parent_entity_id, child_entity_id FROM entityrelationships`)
			defer relRows.Close()
			parentMap := map[string][]string{}
			for relRows.Next() {
				var parent, child string
				relRows.Scan(&parent, &child)
				parentMap[parent] = append(parentMap[parent], child)
			}
			// var getAllDescendants func([]string) []string
			getAllDescendants := func(ids []string) []string {
				all := map[string]bool{}
				queue := append([]string{}, ids...)
				for _, id := range ids { all[id] = true }
				for len(queue) > 0 {
					current := queue[0]
					queue = queue[1:]
					for _, child := range parentMap[current] {
						if !all[child] { all[child] = true; queue = append(queue, child) }
					}
				}
				result := []string{}
				for id := range all { result = append(result, id) }
				return result
			}
			allToApprove := getAllDescendants([]string{id})
			rows, err := db.Query(`UPDATE masterentity SET approval_status = 'Delete-Approved', is_deleted = true, comments = $2 WHERE entity_id = ANY($1) RETURNING *`, pq.Array(allToApprove), req.Comments)
			if err != nil { w.WriteHeader(http.StatusInternalServerError); json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "error": err.Error() }); return }
			defer rows.Close()
			if rows.Next() {
				cols, _ := rows.Columns(); vals := make([]interface{}, len(cols)); valPtrs := make([]interface{}, len(cols)); for i := range cols { valPtrs[i] = &vals[i] }
				rows.Scan(valPtrs...)
				entity := map[string]interface{}{}; for i, col := range cols { entity[col] = vals[i] }
				json.NewEncoder(w).Encode(map[string]interface{}{ "success": true, "entity": entity })
				return
			}
		} else {
	       rows, err := db.Query(`UPDATE masterentity SET approval_status = 'Approved', comments = $2 WHERE entity_id = $1 RETURNING *`, id, req.Comments)
	       if err != nil {
		       w.WriteHeader(http.StatusInternalServerError)
		       json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "error": err.Error() })
		       return
	       }
	       defer rows.Close()
	       if rows.Next() {
		       cols, _ := rows.Columns()
		       vals := make([]interface{}, len(cols))
		       valPtrs := make([]interface{}, len(cols))
		       for i := range cols { valPtrs[i] = &vals[i] }
		       scanErr := rows.Scan(valPtrs...)
		       if scanErr != nil {
			       w.WriteHeader(http.StatusInternalServerError)
			       json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "error": scanErr.Error() })
			       return
		       }
		       entity := map[string]interface{}{}
		       for i, col := range cols { entity[col] = vals[i] }
		       json.NewEncoder(w).Encode(map[string]interface{}{ "success": true, "entity": entity })
	       } else {
		       w.WriteHeader(http.StatusNotFound)
		       json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "message": "Entity not found" })
	       }
		}
	}
}

// Bulk reject entities (and descendants)
func RejectEntitiesBulk(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct { EntityIds []string `json:"entityIds"`; Comments string `json:"comments"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.EntityIds) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "message": "entityIds array required" })
			return
		}
		relRows, _ := db.Query(`SELECT parent_entity_id, child_entity_id FROM entityrelationships`)
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parent, child string
			relRows.Scan(&parent, &child)
			parentMap[parent] = append(parentMap[parent], child)
		}
		getAllDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids { all[id] = true }
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				for _, child := range parentMap[current] {
					if !all[child] { all[child] = true; queue = append(queue, child) }
				}
			}
			result := []string{}
			for id := range all { result = append(result, id) }
			return result
		}
		allToReject := getAllDescendants(req.EntityIds)
		rows, err := db.Query(`UPDATE masterentity SET approval_status = 'Rejected', comments = $2 WHERE entity_id = ANY($1) RETURNING *`, pq.Array(allToReject), req.Comments)
		if err != nil { w.WriteHeader(http.StatusInternalServerError); json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "error": err.Error() }); return }
		defer rows.Close()
		updated := []map[string]interface{}{}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]interface{}, len(cols)); valPtrs := make([]interface{}, len(cols)); for i := range cols { valPtrs[i] = &vals[i] }
			rows.Scan(valPtrs...)
			entity := map[string]interface{}{}; for i, col := range cols { entity[col] = vals[i] }
			updated = append(updated, entity)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{ "success": true, "updated": updated })
	}
}

// Update entity (always sets approval_status to Pending)
func UpdateEntity(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     string                 `json:"id"`
			Fields map[string]interface{} `json:"fields"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || len(req.Fields) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "message": "Missing id or fields in body" })
			return
		}
		setClause := ""
		args := []interface{}{}
		i := 1
		for k, v := range req.Fields {
			setClause += k + " = $" + strconv.Itoa(i) + ", "
			args = append(args, v)
			i++
		}
		setClause += "approval_status = 'Pending'"
		args = append(args, req.ID)
		query := "UPDATE masterentity SET " + setClause + " WHERE entity_id = $" + strconv.Itoa(i) + " RETURNING *"
	       rows, err := db.Query(query, args...)
	       if err != nil {
		       w.WriteHeader(http.StatusInternalServerError)
		       json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "error": err.Error() })
		       return
	       }
	       defer rows.Close()
	       if rows.Next() {
		       cols, _ := rows.Columns()
		       vals := make([]interface{}, len(cols))
		       valPtrs := make([]interface{}, len(cols))
		       for i := range cols { valPtrs[i] = &vals[i] }
		       scanErr := rows.Scan(valPtrs...)
		       if scanErr != nil {
			       w.WriteHeader(http.StatusInternalServerError)
			       json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "error": scanErr.Error() })
			       return
		       }
		       entity := map[string]interface{}{}
		       for i, col := range cols { entity[col] = vals[i] }
		       json.NewEncoder(w).Encode(map[string]interface{}{ "success": true, "entity": entity })
	       } else {
		       w.WriteHeader(http.StatusNotFound)
		       json.NewEncoder(w).Encode(map[string]interface{}{ "success": false, "message": "Entity not found" })
	       }
	}
}

// Get all entity names (approved and not deleted)
func GetAllEntityNames(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query("SELECT entity_name FROM masterentity WHERE (approval_status = 'Approved' OR approval_status = 'approved') AND is_deleted = false")
		if err != nil { w.WriteHeader(http.StatusInternalServerError); json.NewEncoder(w).Encode(map[string]interface{}{ "error": err.Error() }); return }
		defer rows.Close()
		var names []string
		for rows.Next() {
			var name string
			rows.Scan(&name)
			names = append(names, name)
		}
		json.NewEncoder(w).Encode(names)
	}
}

// Delete entity (and descendants if Delete-Approval)
func DeleteEntity(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            ID       string `json:"id"`
            Comments string `json:"comments"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Missing id in body"})
            return
        }
        // Fetch all relationships
        relRows, err := db.Query(`SELECT parent_entity_id, child_entity_id FROM entityrelationships`)
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
            return
        }
        defer relRows.Close()
        parentMap := map[string][]string{}
        for relRows.Next() {
            var parent, child string
            if err := relRows.Scan(&parent, &child); err == nil {
                parentMap[parent] = append(parentMap[parent], child)
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
        allToDelete := getAllDescendants([]string{req.ID})
        rows, err := db.Query(
            `UPDATE masterentity SET approval_status = 'Delete-Approval', comments = $2 WHERE entity_id = ANY($1) RETURNING *`,
            pq.Array(allToDelete), req.Comments,
        )
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
            return
        }
        defer rows.Close()
        updated := []map[string]interface{}{}
        cols, _ := rows.Columns()
        for rows.Next() {
            vals := make([]interface{}, len(cols))
            valPtrs := make([]interface{}, len(cols))
            for i := range cols {
                valPtrs[i] = &vals[i]
            }
            rows.Scan(valPtrs...)
            entity := map[string]interface{}{}
            for i, col := range cols {
                entity[col] = vals[i]
            }
            updated = append(updated, entity)
        }
        if len(updated) == 0 {
            w.WriteHeader(http.StatusNotFound)
            json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Entity not found"})
            return
        }
        json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
    }
}

// Find parent at level
func FindParentAtLevel(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var req struct {
            Level string `json:"level"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Level == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode([]string{})
            return
        }
        numericLevel, err := strconv.Atoi(req.Level)
        if err != nil || numericLevel <= 1 {
            json.NewEncoder(w).Encode([]string{})
            return
        }
        parentLevel := numericLevel - 1
        query := `SELECT entity_name FROM masterentity WHERE (TRIM(BOTH ' ' FROM level) = $1 OR TRIM(BOTH ' ' FROM level) = $2) AND (is_deleted = false OR is_deleted IS NULL)`
        rows, err := db.Query(query, strconv.Itoa(parentLevel), fmt.Sprintf("Level %d", parentLevel))
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            json.NewEncoder(w).Encode(map[string]interface{}{"error": err.Error()})
            return
        }
        defer rows.Close()
        var names []string
        for rows.Next() {
            var name string
            rows.Scan(&name)
            names = append(names, name)
        }
        json.NewEncoder(w).Encode(names)
    }
}

func GetRenderVarsHierarchical(db *sql.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
		// Get user_id from body
		var req struct { UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Missing user_id in body"})
			return
		}
		// Get role_id from user_roles
		var roleId int
		errRole := db.QueryRow("SELECT role_id FROM user_roles WHERE user_id = $1 LIMIT 1", req.UserID).Scan(&roleId)
		if errRole == sql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Role not found for user"})
			return
		} else if errRole != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": errRole.Error()})
			return
		}
		// Get permissions for hierarchical page
		query := `SELECT p.page_name, p.tab_name, p.action, rp.allowed FROM role_permissions rp JOIN permissions p ON rp.permission_id = p.id WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')`
		rows, errPerm := db.Query(query, roleId)
		if errPerm != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": errPerm.Error()})
			return
		}
        defer rows.Close()
        pages := map[string]map[string]interface{}{}
        for rows.Next() {
            var page, tab, action string
            var allowed bool
            rows.Scan(&page, &tab, &action, &allowed)
            if pages[page] == nil {
                pages[page] = map[string]interface{}{}
            }
            if tab == "" {
                if pages[page]["pagePermissions"] == nil {
                    pages[page]["pagePermissions"] = map[string]bool{}
                }
                pages[page]["pagePermissions"].(map[string]bool)[action] = allowed
            } else {
                if pages[page]["tabs"] == nil {
                    pages[page]["tabs"] = map[string]map[string]bool{}
                }
                if pages[page]["tabs"].(map[string]map[string]bool)[tab] == nil {
                    pages[page]["tabs"].(map[string]map[string]bool)[tab] = map[string]bool{}
                }
                pages[page]["tabs"].(map[string]map[string]bool)[tab][action] = allowed
            }
        }
        // Get entity hierarchy (reuse your GetEntityHierarchy logic)
        entitiesRows, err := db.Query("SELECT * FROM masterentity")
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
            return
        }
        defer entitiesRows.Close()
        entities := []map[string]interface{}{}
        entityMap := map[string]map[string]interface{}{}
        deletedIds := map[string]bool{}
        for entitiesRows.Next() {
            var e = make(map[string]interface{})
            cols, _ := entitiesRows.Columns()
            vals := make([]interface{}, len(cols))
            valPtrs := make([]interface{}, len(cols))
            for i := range cols {
                valPtrs[i] = &vals[i]
            }
            entitiesRows.Scan(valPtrs...)
            for i, col := range cols {
                e[col] = vals[i]
            }
            entityID := e["entity_id"].(string)
            entityMap[entityID] = map[string]interface{}{
                "id": entityID,
                "name": e["entity_name"],
                "data": e,
                "children": []interface{}{},
            }
            if e["is_deleted"] == true {
                deletedIds[entityID] = true
            }
            entities = append(entities, e)
        }
        relRows, err := db.Query("SELECT * FROM entityrelationships")
        if err != nil {
            w.WriteHeader(http.StatusInternalServerError)
            json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
            return
        }
        defer relRows.Close()
        relationships := []map[string]interface{}{}
        parentMap := map[string][]string{}
        for relRows.Next() {
            var rel = make(map[string]interface{})
            cols, _ := relRows.Columns()
            vals := make([]interface{}, len(cols))
            valPtrs := make([]interface{}, len(cols))
            for i := range cols {
                valPtrs[i] = &vals[i]
            }
            relRows.Scan(valPtrs...)
            for i, col := range cols {
                rel[col] = vals[i]
            }
            parentID := rel["parent_entity_id"].(string)
            childID := rel["child_entity_id"].(string)
            parentMap[parentID] = append(parentMap[parentID], childID)
            relationships = append(relationships, rel)
        }
        getAllDescendants := func(ids []string) []string {
            all := map[string]bool{}
            queue := append([]string{}, ids...)
            for _, id := range ids {
                all[id] = true
            }
            for len(queue) > 0 {
                current := queue[0]
                queue = queue[1:]
                children := parentMap[current]
                for _, child := range children {
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
        deletedList := []string{}
        for id := range deletedIds {
            deletedList = append(deletedList, id)
        }
        allDeleted := getAllDescendants(deletedList)
        for _, id := range allDeleted {
            delete(entityMap, id)
        }
        for _, entity := range entityMap {
            entity["children"] = []interface{}{}
        }
        for _, rel := range relationships {
            parentID := rel["parent_entity_id"].(string)
            childID := rel["child_entity_id"].(string)
            if entityMap[parentID] != nil && entityMap[childID] != nil {
                entityMap[parentID]["children"] = append(entityMap[parentID]["children"].([]interface{}), entityMap[childID])
            }
        }
        topLevel := []interface{}{}
        for _, e := range entityMap {
            data := e["data"].(map[string]interface{})
            isTop := false
            if v, ok := data["is_top_level_entity"]; ok && v == true {
                isTop = true
            }
            isChild := false
            for _, rel := range relationships {
                if rel["child_entity_id"].(string) == e["id"].(string) {
                    isChild = true
                    break
                }
            }
            if isTop || !isChild {
                topLevel = append(topLevel, e)
            }
        }
        json.NewEncoder(w).Encode(map[string]interface{}{
            "hierarchical": pages["hierarchical"],
            "pageData":     topLevel,
        })
    }
}
