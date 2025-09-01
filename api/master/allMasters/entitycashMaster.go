package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type CashEntityMasterRequest struct {
	EntityName               string `json:"entity_name"`
	EntityShortName          string `json:"entity_short_name"`
	EntityLevel              int    `json:"entity_level"`
	ParentEntityID           string `json:"parent_entity_id"`
	EntityRegistrationNumber string `json:"entity_registration_number"`
	Country                  string `json:"country"`
	BaseOperatingCurrency    string `json:"base_operating_currency"`
	TaxIdentificationNumber  string `json:"tax_identification_number"`
	AddressLine1             string `json:"address_line1"`
	AddressLine2             string `json:"address_line2"`
	City                     string `json:"city"`
	StateProvince            string `json:"state_province"`
	PostalCode               string `json:"postal_code"`
	ContactPersonName        string `json:"contact_person_name"`
	ContactPersonEmail       string `json:"contact_person_email"`
	ContactPersonPhone       string `json:"contact_person_phone"`
	ActiveStatus             string `json:"active_status"`
	IsTopLevelEntity         bool   `json:"is_top_level_entity"`
	IsDeleted                bool   `json:"is_deleted"`
}

// Request type for bulk entity cash update
type CashEntityUpdateRequest struct {
	EntityID                    string `json:"entity_id"`
	EntityName                  string `json:"entity_name"`
	OldEntityName               string `json:"old_entity_name"`
	EntityShortName             string `json:"entity_short_name"`
	OldEntityShortName          string `json:"old_entity_short_name"`
	EntityLevel                 int    `json:"entity_level"`
	OldEntityLevel              int    `json:"old_entity_level"`
	ParentEntityID              string `json:"parent_entity_id"`
	OldParentEntityID           string `json:"old_parent_entity_id"`
	EntityRegistrationNumber    string `json:"entity_registration_number"`
	OldEntityRegistrationNumber string `json:"old_entity_registration_number"`
	Country                     string `json:"country"`
	OldCountry                  string `json:"old_country"`
	BaseOperatingCurrency       string `json:"base_operating_currency"`
	OldBaseOperatingCurrency    string `json:"old_base_operating_currency"`
	TaxIdentificationNumber     string `json:"tax_identification_number"`
	OldTaxIdentificationNumber  string `json:"old_tax_identification_number"`
	AddressLine1                string `json:"address_line1"`
	OldAddressLine1             string `json:"old_address_line1"`
	AddressLine2                string `json:"address_line2"`
	OldAddressLine2             string `json:"old_address_line2"`
	City                        string `json:"city"`
	OldCity                     string `json:"old_city"`
	StateProvince               string `json:"state_province"`
	OldStateProvince            string `json:"old_state_province"`
	PostalCode                  string `json:"postal_code"`
	OldPostalCode               string `json:"old_postal_code"`
	ContactPersonName           string `json:"contact_person_name"`
	OldContactPersonName        string `json:"old_contact_person_name"`
	ContactPersonEmail          string `json:"contact_person_email"`
	OldContactPersonEmail       string `json:"old_contact_person_email"`
	ContactPersonPhone          string `json:"contact_person_phone"`
	OldContactPersonPhone       string `json:"old_contact_person_phone"`
	ActiveStatus                string `json:"active_status"`
	OldActiveStatus             string `json:"old_active_status"`
	IsTopLevelEntity            bool   `json:"is_top_level_entity"`
	IsDeleted                   bool   `json:"is_deleted"`
	Reason                      string `json:"reason"`
}

type CashEntityBulkRequest struct {
	Entities []CashEntityMasterRequest `json:"entities"`
	UserID   string                    `json:"user_id"`
}

func CreateAndSyncCashEntities(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CashEntityBulkRequest
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
		if createdBy == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Invalid user_id or session"})
			return
		}
		// Insert entities
		entityIDs := make(map[string]string) // name -> id
		inserted := []map[string]interface{}{}
		for _, entity := range req.Entities {
			// Generate custom entity ID: EC-XXXXXXXX (8 random alphanumeric)
			const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
			b := make([]byte, 8)
			for i := range b {
				b[i] = charset[int(uuid.New().ID())%len(charset)]
			}
			entityId := "EC-" + string(b)
			// query := `INSERT INTO masterentitycash (
			// 	entity_id, entity_name, entity_short_name, entity_level, parent_entity_id, entity_registration_number, country, base_operating_currency, tax_identification_number, address_line1, address_line2, city, state_province, postal_code, contact_person_name, contact_person_email, contact_person_phone, active_status, old_entity_name, old_entity_short_name, old_entity_level, old_parent_entity_id, old_entity_registration_number, old_country, old_base_operating_currency, old_tax_identification_number, old_address_line1, old_address_line2, old_city, old_state_province, old_postal_code, old_contact_person_name, old_contact_person_email, old_contact_person_phone, old_active_status, is_top_level_entity, is_deleted
			// ) VALUES (
			// 	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, COALESCE(NULLIF($18, ''), 'Inactive'), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $19, $20
			// ) RETURNING entity_id`
			query := `INSERT INTO masterentitycash (
	entity_id, entity_name, entity_short_name, entity_level, parent_entity_id, entity_registration_number, country, base_operating_currency, tax_identification_number, address_line1, address_line2, city, state_province, postal_code, contact_person_name, contact_person_email, contact_person_phone, active_status, is_top_level_entity, is_deleted
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, COALESCE(NULLIF($18, ''), 'Inactive'), $19, $20
) RETURNING entity_id`
			var newEntityID string
			err := db.QueryRow(query,
				entityId,
				entity.EntityName,
				entity.EntityShortName,
				entity.EntityLevel,
				entity.ParentEntityID,
				entity.EntityRegistrationNumber,
				entity.Country,
				entity.BaseOperatingCurrency,
				entity.TaxIdentificationNumber,
				entity.AddressLine1,
				entity.AddressLine2,
				entity.City,
				entity.StateProvince,
				entity.PostalCode,
				entity.ContactPersonName,
				entity.ContactPersonEmail,
				entity.ContactPersonPhone,
				entity.ActiveStatus,
				entity.IsTopLevelEntity,
				entity.IsDeleted,
			).Scan(&newEntityID)
			if err != nil {
				inserted = append(inserted, map[string]interface{}{"success": false, "error": err.Error(), "entity_name": entity.EntityName})
				continue
			}
			entityIDs[entity.EntityName] = newEntityID
			inserted = append(inserted, map[string]interface{}{"success": true, "entity_id": newEntityID, "entity_name": entity.EntityName})
			// Insert audit action
			auditQuery := `INSERT INTO auditactionentity (
				entity_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, $2, $3, $4, $5, now())`
			_, auditErr := db.Exec(auditQuery,
				newEntityID,
				"CREATE",
				"PENDING_APPROVAL",
				nil,
				createdBy,
			)
			if auditErr != nil {
				inserted = append(inserted, map[string]interface{}{
					"success":     false,
					"error":       "Entity created but audit log failed: " + auditErr.Error(),
					"entity_id":   newEntityID,
					"entity_name": entity.EntityName,
				})
			}
		}
		// Sync relationships
		relationshipsAdded := []map[string]interface{}{}
		for _, entity := range req.Entities {
			if entity.ParentEntityID == "" || entityIDs[entity.EntityName] == "" {
				continue
			}
			parentId := entity.ParentEntityID
			childId := entityIDs[entity.EntityName]
			// Insert relationship if not exists
			var exists int
			err := db.QueryRow(`SELECT 1 FROM cashentityrelationships WHERE parent_entity_id = $1 AND child_entity_id = $2`, parentId, childId).Scan(&exists)
			if err == sql.ErrNoRows {
				relQuery := `INSERT INTO cashentityrelationships (parent_entity_id, child_entity_id, status) VALUES ($1, $2, 'Active') RETURNING relationship_id`
				var relID int
				relErr := db.QueryRow(relQuery, parentId, childId).Scan(&relID)
				if relErr == nil {
					relationshipsAdded = append(relationshipsAdded, map[string]interface{}{"success": true, "relationship_id": relID, "parent_entity_id": parentId, "child_entity_id": childId})
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":            true,
			"entities":           inserted,
			"relationshipsAdded": len(relationshipsAdded),
			"details":            relationshipsAdded,
		})
	}
}

// GET handler to return cash entity hierarchy, excluding deleted entities and their descendants, with audit join
func GetCashEntityHierarchy(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Fetch all entities with audit join
		query := `
			SELECT m.entity_id, m.entity_name, m.entity_short_name, m.entity_level, m.parent_entity_id, m.entity_registration_number, m.country, m.base_operating_currency, m.tax_identification_number, m.address_line1, m.address_line2, m.city, m.state_province, m.postal_code, m.contact_person_name, m.contact_person_email, m.contact_person_phone, m.active_status, m.old_entity_name, m.old_entity_short_name, m.old_entity_level, m.old_parent_entity_id, m.old_entity_registration_number, m.old_country, m.old_base_operating_currency, m.old_tax_identification_number, m.old_address_line1, m.old_address_line2, m.old_city, m.old_state_province, m.old_postal_code, m.old_contact_person_name, m.old_contact_person_email, m.old_contact_person_phone, m.old_active_status, m.is_top_level_entity, m.is_deleted,
				   a.processing_status, a.requested_by, a.requested_at, a.actiontype, a.action_id, a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM masterentitycash m
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason
				FROM auditactionentity a
				WHERE a.entity_id = m.entity_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
		`
		rows, err := db.Query(query)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		defer rows.Close()

		entityMap := map[string]map[string]interface{}{}
		deletedIds := map[string]bool{}
		for rows.Next() {
			var (
				entityID                                                                               string
				entityName                                                                             sql.NullString
				entityShortName                                                                        sql.NullString
				entityLevel                                                                            sql.NullInt64
				parentEntityID                                                                         sql.NullString
				entityRegistrationNumber                                                               sql.NullString
				country                                                                                sql.NullString
				baseOperatingCurrency                                                                  sql.NullString
				taxIdentificationNumber                                                                sql.NullString
				addressLine1                                                                           sql.NullString
				addressLine2                                                                           sql.NullString
				city                                                                                   sql.NullString
				stateProvince                                                                          sql.NullString
				postalCode                                                                             sql.NullString
				contactPersonName                                                                      sql.NullString
				contactPersonEmail                                                                     sql.NullString
				contactPersonPhone                                                                     sql.NullString
				activeStatus                                                                           sql.NullString
				oldEntityName                                                                          sql.NullString
				oldEntityShortName                                                                     sql.NullString
				oldEntityLevel                                                                         sql.NullInt64
				oldParentEntityID                                                                      sql.NullString
				oldEntityRegistrationNumber                                                            sql.NullString
				oldCountry                                                                             sql.NullString
				oldBaseOperatingCurrency                                                               sql.NullString
				oldTaxIdentificationNumber                                                             sql.NullString
				oldAddressLine1                                                                        sql.NullString
				oldAddressLine2                                                                        sql.NullString
				oldCity                                                                                sql.NullString
				oldStateProvince                                                                       sql.NullString
				oldPostalCode                                                                          sql.NullString
				oldContactPersonName                                                                   sql.NullString
				oldContactPersonEmail                                                                  sql.NullString
				oldContactPersonPhone                                                                  sql.NullString
				oldActiveStatus                                                                        sql.NullString
				isTopLevelEntity, isDeleted                                                            bool
				processingStatus, requestedBy, actionType, actionID, checkerBy, checkerComment, reason sql.NullString
				requestedAt, checkerAt                                                                 sql.NullTime
			)
			err := rows.Scan(&entityID, &entityName, &entityShortName, &entityLevel, &parentEntityID, &entityRegistrationNumber, &country, &baseOperatingCurrency, &taxIdentificationNumber, &addressLine1, &addressLine2, &city, &stateProvince, &postalCode, &contactPersonName, &contactPersonEmail, &contactPersonPhone, &activeStatus, &oldEntityName, &oldEntityShortName, &oldEntityLevel, &oldParentEntityID, &oldEntityRegistrationNumber, &oldCountry, &oldBaseOperatingCurrency, &oldTaxIdentificationNumber, &oldAddressLine1, &oldAddressLine2, &oldCity, &oldStateProvince, &oldPostalCode, &oldContactPersonName, &oldContactPersonEmail, &oldContactPersonPhone, &oldActiveStatus, &isTopLevelEntity, &isDeleted,
				&processingStatus, &requestedBy, &requestedAt, &actionType, &actionID, &checkerBy, &checkerAt, &checkerComment, &reason)
			if err != nil {
				continue
			}
			// fetch CREATE/EDIT/DELETE history for audit info
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM auditactionentity 
								  WHERE entity_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') 
								  ORDER BY requested_at DESC`
			auditRows, auditErr := db.Query(auditDetailsQuery, entityID)
			if auditErr == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var atype string
					var rby sql.NullString
					var rat sql.NullTime
					if err := auditRows.Scan(&atype, &rby, &rat); err == nil {
						auditInfo := api.GetAuditInfo(atype, rby, rat)
						if atype == "CREATE" && createdBy == "" {
							createdBy = auditInfo.CreatedBy
							createdAt = auditInfo.CreatedAt
						} else if atype == "EDIT" && editedBy == "" {
							editedBy = auditInfo.EditedBy
							editedAt = auditInfo.EditedAt
						} else if atype == "DELETE" && deletedBy == "" {
							deletedBy = auditInfo.DeletedBy
							deletedAt = auditInfo.DeletedAt
						}
					}
				}
			}
			entityMap[entityID] = map[string]interface{}{
				"id":   entityID,
				"name": getNullString(entityName),
				"data": map[string]interface{}{
					"entity_id":                      entityID,
					"entity_name":                    getNullString(entityName),
					"entity_short_name":              getNullString(entityShortName),
					"entity_level":                   entityLevel.Int64,
					"parent_entity_id":               getNullString(parentEntityID),
					"entity_registration_number":     getNullString(entityRegistrationNumber),
					"country":                        getNullString(country),
					"base_operating_currency":        getNullString(baseOperatingCurrency),
					"tax_identification_number":      getNullString(taxIdentificationNumber),
					"address_line1":                  getNullString(addressLine1),
					"address_line2":                  getNullString(addressLine2),
					"city":                           getNullString(city),
					"state_province":                 getNullString(stateProvince),
					"postal_code":                    getNullString(postalCode),
					"contact_person_name":            getNullString(contactPersonName),
					"contact_person_email":           getNullString(contactPersonEmail),
					"contact_person_phone":           getNullString(contactPersonPhone),
					"active_status":                  getNullString(activeStatus),
					"old_entity_name":                getNullString(oldEntityName),
					"old_entity_short_name":          getNullString(oldEntityShortName),
					"old_entity_level":               oldEntityLevel.Int64,
					"old_parent_entity_id":           getNullString(oldParentEntityID),
					"old_entity_registration_number": getNullString(oldEntityRegistrationNumber),
					"old_country":                    getNullString(oldCountry),
					"old_base_operating_currency":    getNullString(oldBaseOperatingCurrency),
					"old_tax_identification_number":  getNullString(oldTaxIdentificationNumber),
					"old_address_line1":              getNullString(oldAddressLine1),
					"old_address_line2":              getNullString(oldAddressLine2),
					"old_city":                       getNullString(oldCity),
					"old_state_province":             getNullString(oldStateProvince),
					"old_postal_code":                getNullString(oldPostalCode),
					"old_contact_person_name":        getNullString(oldContactPersonName),
					"old_contact_person_email":       getNullString(oldContactPersonEmail),
					"old_contact_person_phone":       getNullString(oldContactPersonPhone),
					"old_active_status":              getNullString(oldActiveStatus),
					"is_top_level_entity":            isTopLevelEntity,
					"is_deleted":                     isDeleted,
					"processing_status":              getNullString(processingStatus),
					// "requested_by":                   getNullString(requestedBy),
					// "requested_at":                   getNullTime(requestedAt),
					"action_type":     getNullString(actionType),
					"action_id":       getNullString(actionID),
					"checker_at":      getNullTime(checkerAt),
					"checker_by":      getNullString(checkerBy),
					"checker_comment": getNullString(checkerComment),
					"reason":          getNullString(reason),
					"created_by":      createdBy,
					"created_at":      createdAt,
					"edited_by":       editedBy,
					"edited_at":       editedAt,
					"deleted_by":      deletedBy,
					"deleted_at":      deletedAt,
				},
				"children": []interface{}{},
			}
			if isDeleted {
				deletedIds[entityID] = true
			}
		}
		// Fetch relationships
		relRows, err := db.Query("SELECT parent_entity_id, child_entity_id FROM cashentityrelationships")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		defer relRows.Close()
		parentMap := map[string][]string{}
		for relRows.Next() {
			var parentID, childID string
			relRows.Scan(&parentID, &childID)
			parentMap[parentID] = append(parentMap[parentID], childID)
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
		for parentID, children := range parentMap {
			if entityMap[parentID] != nil {
				for _, childID := range children {
					if entityMap[childID] != nil {
						entityMap[parentID]["children"] = append(entityMap[parentID]["children"].([]interface{}), entityMap[childID])
					}
				}
			}
		}
		// Find top-level entities (not deleted, not child of any parent, or is_top_level_entity)
		topLevel := []interface{}{}
		childSet := map[string]bool{}
		for _, children := range parentMap {
			for _, childID := range children {
				childSet[childID] = true
			}
		}
		for _, e := range entityMap {
			data := e["data"].(map[string]interface{})
			isChild := childSet[e["id"].(string)]
			if data["is_top_level_entity"].(bool) || !isChild {
				topLevel = append(topLevel, e)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(topLevel)
	}
}

// Bulk update handler for entity cash master
func UpdateCashEntityBulk(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                    `json:"user_id"`
			Entities []CashEntityUpdateRequest `json:"entities"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Invalid JSON"})
			return
		}
		// Get updated_by from session
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				updatedBy = s.Email
				break
			}
		}
		if updatedBy == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Invalid user_id or session"})
			return
		}
		var results []map[string]interface{}
		relationshipsAdded := []map[string]interface{}{}
		for _, entity := range req.Entities {
			if entity.EntityID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "Missing entity_id"})
				continue
			}
			updateQuery := `UPDATE masterentitycash SET 
				entity_name=$1, old_entity_name=$2,
				entity_short_name=$3, old_entity_short_name=$4,
				entity_level=$5, old_entity_level=$6,
				parent_entity_id=$7, old_parent_entity_id=$8,
				entity_registration_number=$9, old_entity_registration_number=$10,
				country=$11, old_country=$12,
				base_operating_currency=$13, old_base_operating_currency=$14,
				tax_identification_number=$15, old_tax_identification_number=$16,
				address_line1=$17, old_address_line1=$18,
				address_line2=$19, old_address_line2=$20,
				city=$21, old_city=$22,
				state_province=$23, old_state_province=$24,
				postal_code=$25, old_postal_code=$26,
				contact_person_name=$27, old_contact_person_name=$28,
				contact_person_email=$29, old_contact_person_email=$30,
				contact_person_phone=$31, old_contact_person_phone=$32,
				active_status=$33, old_active_status=$34,
				is_top_level_entity=$35, is_deleted=$36
				WHERE entity_id=$37 RETURNING entity_id`
			var updatedEntityID string
			err := db.QueryRow(updateQuery,
				entity.EntityName, entity.OldEntityName,
				entity.EntityShortName, entity.OldEntityShortName,
				entity.EntityLevel, entity.OldEntityLevel,
				entity.ParentEntityID, entity.OldParentEntityID,
				entity.EntityRegistrationNumber, entity.OldEntityRegistrationNumber,
				entity.Country, entity.OldCountry,
				entity.BaseOperatingCurrency, entity.OldBaseOperatingCurrency,
				entity.TaxIdentificationNumber, entity.OldTaxIdentificationNumber,
				entity.AddressLine1, entity.OldAddressLine1,
				entity.AddressLine2, entity.OldAddressLine2,
				entity.City, entity.OldCity,
				entity.StateProvince, entity.OldStateProvince,
				entity.PostalCode, entity.OldPostalCode,
				entity.ContactPersonName, entity.OldContactPersonName,
				entity.ContactPersonEmail, entity.OldContactPersonEmail,
				entity.ContactPersonPhone, entity.OldContactPersonPhone,
				entity.ActiveStatus, entity.OldActiveStatus,
				entity.IsTopLevelEntity, entity.IsDeleted,
				entity.EntityID,
			).Scan(&updatedEntityID)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": err.Error(), "entity_id": entity.EntityID})
				continue
			}
			// Insert audit action
			auditQuery := `INSERT INTO auditactionentity (
				entity_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, $2, $3, $4, $5, now())`
			_, auditErr := db.Exec(auditQuery,
				updatedEntityID,
				"EDIT",
				"PENDING_EDIT_APPROVAL",
				entity.Reason,
				updatedBy,
			)
			if auditErr != nil {
				results = append(results, map[string]interface{}{
					"success":   false,
					"error":     "Entity updated but audit log failed: " + auditErr.Error(),
					"entity_id": updatedEntityID,
				})
				continue
			}
			// Sync relationships if parent_entity_id is present
			parentId := entity.ParentEntityID
			if parentId != "" {
				var exists int
				err := db.QueryRow(`SELECT 1 FROM cashentityrelationships WHERE parent_entity_id = $1 AND child_entity_id = $2`, parentId, updatedEntityID).Scan(&exists)
				if err == sql.ErrNoRows {
					relQuery := `INSERT INTO cashentityrelationships (parent_entity_id, child_entity_id, status) VALUES ($1, $2, 'Active') RETURNING relationship_id`
					var relID int
					relErr := db.QueryRow(relQuery, parentId, updatedEntityID).Scan(&relID)
					if relErr == nil {
						relationshipsAdded = append(relationshipsAdded, map[string]interface{}{"success": true, "relationship_id": relID, "parent_entity_id": parentId, "child_entity_id": updatedEntityID})
					}
				}
			}
			results = append(results, map[string]interface{}{
				"success":   true,
				"entity_id": updatedEntityID,
			})
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

// Delete cash entity (and descendants if Delete-Approval)
func DeleteCashEntity(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntityID string `json:"entity_id"`
			Reason   string `json:"reason"`
			UserID   string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.EntityID == "" || req.UserID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing entity_id or user_id in body")
			return
		}
		// Get requested_by from session
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
		// Fetch all relationships
		relRows, err := db.Query(`SELECT parent_entity_id, child_entity_id FROM cashentityrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
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
		allToDelete := getAllDescendants([]string{req.EntityID})
		// Mark all for delete approval
		rows, err := db.Query(
			`UPDATE masterentitycash SET is_deleted = true WHERE entity_id = ANY($1) RETURNING entity_id`,
			pq.Array(allToDelete),
		)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		updated := []string{}
		for rows.Next() {
			var eid string
			if err := rows.Scan(&eid); err == nil {
				updated = append(updated, eid)
			}
		}
		// Insert audit actions for all
		var auditErrors []string
		for _, eid := range updated {
			auditQuery := `INSERT INTO auditactionentity (
					entity_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now())`
			_, auditErr := db.Exec(auditQuery, eid, req.Reason, requestedBy)
			if auditErr != nil {
				auditErrors = append(auditErrors, eid+":"+auditErr.Error())
			}
		}
		w.Header().Set("Content-Type", "application/json")
		success := len(auditErrors) == 0 && len(updated) > 0
		resp := map[string]interface{}{
			"success": success,
			"updated": updated,
		}
		if len(auditErrors) > 0 {
			resp["audit_errors"] = auditErrors
		}
		if len(updated) == 0 {
			resp["message"] = "No entities found to delete"
		}
		json.NewEncoder(w).Encode(resp)
	}
}

// Bulk reject cash entity actions (and descendants)
func BulkRejectCashEntityActions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			EntityIDs []string `json:"entity_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.EntityIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		// Get checker_by from session
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
		// Fetch all relationships
		relRows, err := db.Query(`SELECT parent_entity_id, child_entity_id FROM cashentityrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
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
		allToReject := getAllDescendants(req.EntityIDs)
		// Update processing_status to 'REJECTED' in auditactionentity for all
		query := `UPDATE auditactionentity SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE entity_id = ANY($3) RETURNING action_id, entity_id`
		rows, err := db.Query(query, checkerBy, req.Comment, pq.Array(allToReject))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []map[string]interface{}
		for rows.Next() {
			var actionID, entityID string
			if err := rows.Scan(&actionID, &entityID); err == nil {
				updated = append(updated, map[string]interface{}{
					"action_id": actionID,
					"entity_id": entityID,
				})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		success := len(updated) > 0
		resp := map[string]interface{}{
			"success": success,
			"updated": updated,
		}
		if !success {
			resp["message"] = "No entities found to reject"
		}
		json.NewEncoder(w).Encode(resp)
	}
}

// Bulk approve cash entity actions (and descendants)
func BulkApproveCashEntityActions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			EntityIDs []string `json:"entity_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.EntityIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		// Get checker_by from session
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
		// Fetch all relationships
		relRows, err := db.Query(`SELECT parent_entity_id, child_entity_id FROM cashentityrelationships`)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
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
		// For each entity, check approval_status
		var allUpdated []map[string]interface{}
		var anyError error
		for _, eid := range req.EntityIDs {
			var status string
			err := db.QueryRow(`SELECT processing_status FROM auditactionentity WHERE entity_id = $1 ORDER BY requested_at DESC LIMIT 1`, eid).Scan(&status)
			if err == sql.ErrNoRows {
				continue
			} else if err != nil {
				anyError = err
				break
			}
			if status == "PENDING_DELETE_APPROVAL" {
				// Mark all descendants as deleted (do not approve them)
				descendants := getAllDescendants([]string{eid})
				rows, err := db.Query(`UPDATE masterentitycash SET is_deleted = true WHERE entity_id = ANY($1) RETURNING entity_id`, pq.Array(descendants))
				if err != nil {
					anyError = err
					break
				}
				defer rows.Close()
				for rows.Next() {
					var entityID string
					if err := rows.Scan(&entityID); err == nil {
						allUpdated = append(allUpdated, map[string]interface{}{
							"entity_id": entityID,
							"status":    "Marked Deleted",
						})
					}
				}
			} else {
				// Approve only this entity: update auditactionentity processing_status
				rows, err := db.Query(`UPDATE auditactionentity SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE entity_id = $3 AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id, entity_id`, checkerBy, req.Comment, eid)
				if err != nil {
					anyError = err
					break
				}
				defer rows.Close()
				for rows.Next() {
					var actionID, entityID string
					if err := rows.Scan(&actionID, &entityID); err == nil {
						allUpdated = append(allUpdated, map[string]interface{}{
							"entity_id": entityID,
							"action_id": actionID,
							"status":    "Approved",
						})
					}
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		success := len(allUpdated) > 0 && anyError == nil
		resp := map[string]interface{}{
			"success": success,
			"updated": allUpdated,
		}
		if anyError != nil {
			resp["error"] = anyError.Error()
		}
		if !success {
			resp["message"] = "No entities found to approve"
		}
		json.NewEncoder(w).Encode(resp)
	}
}

// Find parent cash entities at a given level
func FindParentCashEntityAtLevel(db *sql.DB) http.HandlerFunc {
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
		query := `SELECT entity_name FROM masterentitycash WHERE entity_level = $1 AND (is_deleted = false OR is_deleted IS NULL)`
		rows, err := db.Query(query, parentLevel)
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

// GET handler to fetch all entity_id, entity_name, entity_short_name for all cash entities, requiring user_id in body
func GetCashEntityNamesWithID(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		query := `
			SELECT m.entity_id, m.entity_name, m.entity_short_name
			FROM masterentitycash m
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionentity a
				WHERE a.entity_id = m.entity_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.active_status = 'Active' AND (m.is_deleted = false OR m.is_deleted IS NULL) AND a.processing_status = 'APPROVED'
		`
		rows, err := db.Query(query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		var anyError error
		for rows.Next() {
			var entityID, entityName, entityShortName string
			if err := rows.Scan(&entityID, &entityName, &entityShortName); err != nil {
				anyError = err
				break
			}
			results = append(results, map[string]interface{}{
				"entity_id":         entityID,
				"entity_name":       entityName,
				"entity_short_name": entityShortName,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		if anyError != nil {
			api.RespondWithError(w, http.StatusInternalServerError, anyError.Error())
			return
		}
		if results == nil {
			results = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	}
}
