package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"

	// "context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	// "strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

type CashEntityBulkRequest struct {
	Entities []CashEntityMasterRequest `json:"entities"`
	UserID   string                    `json:"user_id"`
}

func CreateAndSyncCashEntities(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CashEntityBulkRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		entityIDs := make(map[string]string) 
		inserted := []map[string]interface{}{}
		for _, entity := range req.Entities {
	
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
			err := pgxPool.QueryRow(r.Context(), query,
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
			if _, auditErr := pgxPool.Exec(r.Context(), auditQuery,
				newEntityID,
				"CREATE",
				"PENDING_APPROVAL",
				nil,
				createdBy,
			); auditErr != nil {
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
			err := pgxPool.QueryRow(r.Context(), `SELECT 1 FROM cashentityrelationships WHERE parent_entity_id = $1 AND child_entity_id = $2`, parentId, childId).Scan(&exists)
			if err == pgx.ErrNoRows {
				relQuery := `INSERT INTO cashentityrelationships (parent_entity_id, child_entity_id, status) VALUES ($1, $2, 'Active') RETURNING relationship_id`
				var relID int
				relErr := pgxPool.QueryRow(r.Context(), relQuery, parentId, childId).Scan(&relID)
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
func GetCashEntityHierarchy(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		defer rows.Close()

		entityMap := map[string]map[string]interface{}{}
		deletedIds := map[string]bool{}
		// hideIds holds entities that are deleted AND have processing_status = 'APPROVED'
		hideIds := map[string]bool{}
		for rows.Next() {
			var (
				entityID                                                                                                    string
				entityName                                                                                                  *string
				entityShortName                                                                                             *string
				entityLevel                                                                                                 *int64
				parentEntityID                                                                                              *string
				entityRegistrationNumber                                                                                    *string
				country                                                                                                     *string
				baseOperatingCurrency                                                                                       *string
				taxIdentificationNumber                                                                                     *string
				addressLine1                                                                                                *string
				addressLine2                                                                                                *string
				city                                                                                                        *string
				stateProvince                                                                                               *string
				postalCode                                                                                                  *string
				contactPersonName                                                                                           *string
				contactPersonEmail                                                                                          *string
				contactPersonPhone                                                                                          *string
				activeStatus                                                                                                *string
				oldEntityName                                                                                               *string
				oldEntityShortName                                                                                          *string
				oldEntityLevel                                                                                              *int64
				oldParentEntityID                                                                                           *string
				oldEntityRegistrationNumber                                                                                 *string
				oldCountry                                                                                                  *string
				oldBaseOperatingCurrency                                                                                    *string
				oldTaxIdentificationNumber                                                                                  *string
				oldAddressLine1                                                                                             *string
				oldAddressLine2                                                                                             *string
				oldCity                                                                                                     *string
				oldStateProvince                                                                                            *string
				oldPostalCode                                                                                               *string
				oldContactPersonName                                                                                        *string
				oldContactPersonEmail                                                                                       *string
				oldContactPersonPhone                                                                                       *string
				oldActiveStatus                                                                                             *string
				isTopLevelEntity, isDeleted                                                                                 bool
				processingStatusPtr, requestedByPtr, actionTypePtr, actionIDPtr, checkerByPtr, checkerCommentPtr, reasonPtr *string
				requestedAtPtr, checkerAtPtr                                                                                *time.Time
			)
			err := rows.Scan(&entityID, &entityName, &entityShortName, &entityLevel, &parentEntityID, &entityRegistrationNumber, &country, &baseOperatingCurrency, &taxIdentificationNumber, &addressLine1, &addressLine2, &city, &stateProvince, &postalCode, &contactPersonName, &contactPersonEmail, &contactPersonPhone, &activeStatus, &oldEntityName, &oldEntityShortName, &oldEntityLevel, &oldParentEntityID, &oldEntityRegistrationNumber, &oldCountry, &oldBaseOperatingCurrency, &oldTaxIdentificationNumber, &oldAddressLine1, &oldAddressLine2, &oldCity, &oldStateProvince, &oldPostalCode, &oldContactPersonName, &oldContactPersonEmail, &oldContactPersonPhone, &oldActiveStatus, &isTopLevelEntity, &isDeleted,
				&processingStatusPtr, &requestedByPtr, &requestedAtPtr, &actionTypePtr, &actionIDPtr, &checkerByPtr, &checkerAtPtr, &checkerCommentPtr, &reasonPtr)
			if err != nil {
				continue
			}
			// fetch CREATE/EDIT/DELETE history for audit info
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM auditactionentity
				  WHERE entity_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE')
				  ORDER BY requested_at DESC`
			auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, entityID)
			if auditErr == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var atype string
					var rbyPtr *string
					var ratPtr *time.Time
					if err := auditRows.Scan(&atype, &rbyPtr, &ratPtr); err == nil {
						auditInfo := api.GetAuditInfo(atype, rbyPtr, ratPtr)
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
				"id": entityID,
				"name": func() string {
					if entityName != nil {
						return *entityName
					}
					return ""
				}(),
				"data": map[string]interface{}{
					"entity_id": entityID,
					"entity_name": func() string {
						if entityName != nil {
							return *entityName
						}
						return ""
					}(),
					"entity_short_name": func() string {
						if entityShortName != nil {
							return *entityShortName
						}
						return ""
					}(),
					"entity_level": func() int64 {
						if entityLevel != nil {
							return *entityLevel
						}
						return 0
					}(),
					"parent_entity_id": func() string {
						if parentEntityID != nil {
							return *parentEntityID
						}
						return ""
					}(),
					"entity_registration_number": func() string {
						if entityRegistrationNumber != nil {
							return *entityRegistrationNumber
						}
						return ""
					}(),
					"country": func() string {
						if country != nil {
							return *country
						}
						return ""
					}(),
					"base_operating_currency": func() string {
						if baseOperatingCurrency != nil {
							return *baseOperatingCurrency
						}
						return ""
					}(),
					"tax_identification_number": func() string {
						if taxIdentificationNumber != nil {
							return *taxIdentificationNumber
						}
						return ""
					}(),
					"address_line1": func() string {
						if addressLine1 != nil {
							return *addressLine1
						}
						return ""
					}(),
					"address_line2": func() string {
						if addressLine2 != nil {
							return *addressLine2
						}
						return ""
					}(),
					"city": func() string {
						if city != nil {
							return *city
						}
						return ""
					}(),
					"state_province": func() string {
						if stateProvince != nil {
							return *stateProvince
						}
						return ""
					}(),
					"postal_code": func() string {
						if postalCode != nil {
							return *postalCode
						}
						return ""
					}(),
					"contact_person_name": func() string {
						if contactPersonName != nil {
							return *contactPersonName
						}
						return ""
					}(),
					"contact_person_email": func() string {
						if contactPersonEmail != nil {
							return *contactPersonEmail
						}
						return ""
					}(),
					"contact_person_phone": func() string {
						if contactPersonPhone != nil {
							return *contactPersonPhone
						}
						return ""
					}(),
					"active_status": func() string {
						if activeStatus != nil {
							return *activeStatus
						}
						return ""
					}(),
					"old_entity_name": func() string {
						if oldEntityName != nil {
							return *oldEntityName
						}
						return ""
					}(),
					"old_entity_short_name": func() string {
						if oldEntityShortName != nil {
							return *oldEntityShortName
						}
						return ""
					}(),
					"old_entity_level": func() int64 {
						if oldEntityLevel != nil {
							return *oldEntityLevel
						}
						return 0
					}(),
					"old_parent_entity_id": func() string {
						if oldParentEntityID != nil {
							return *oldParentEntityID
						}
						return ""
					}(),
					"old_entity_registration_number": func() string {
						if oldEntityRegistrationNumber != nil {
							return *oldEntityRegistrationNumber
						}
						return ""
					}(),
					"old_country": func() string {
						if oldCountry != nil {
							return *oldCountry
						}
						return ""
					}(),
					"old_base_operating_currency": func() string {
						if oldBaseOperatingCurrency != nil {
							return *oldBaseOperatingCurrency
						}
						return ""
					}(),
					"old_tax_identification_number": func() string {
						if oldTaxIdentificationNumber != nil {
							return *oldTaxIdentificationNumber
						}
						return ""
					}(),
					"old_address_line1": func() string {
						if oldAddressLine1 != nil {
							return *oldAddressLine1
						}
						return ""
					}(),
					"old_address_line2": func() string {
						if oldAddressLine2 != nil {
							return *oldAddressLine2
						}
						return ""
					}(),
					"old_city": func() string {
						if oldCity != nil {
							return *oldCity
						}
						return ""
					}(),
					"old_state_province": func() string {
						if oldStateProvince != nil {
							return *oldStateProvince
						}
						return ""
					}(),
					"old_postal_code": func() string {
						if oldPostalCode != nil {
							return *oldPostalCode
						}
						return ""
					}(),
					"old_contact_person_name": func() string {
						if oldContactPersonName != nil {
							return *oldContactPersonName
						}
						return ""
					}(),
					"old_contact_person_email": func() string {
						if oldContactPersonEmail != nil {
							return *oldContactPersonEmail
						}
						return ""
					}(),
					"old_contact_person_phone": func() string {
						if oldContactPersonPhone != nil {
							return *oldContactPersonPhone
						}
						return ""
					}(),
					"old_active_status": func() string {
						if oldActiveStatus != nil {
							return *oldActiveStatus
						}
						return ""
					}(),
					"is_top_level_entity": isTopLevelEntity,
					"is_deleted":          isDeleted,
					"processing_status": func() string {
						if processingStatusPtr != nil {
							return *processingStatusPtr
						}
						return ""
					}(),
					// "requested_by":                   func() string { if requestedByPtr != nil { return *requestedByPtr }; return "" }(),
					// "requested_at":                   func() string { if requestedAtPtr != nil { return requestedAtPtr.Format("2006-01-02 15:04:05") }; return "" }(),
					"action_type": func() string {
						if actionTypePtr != nil {
							return *actionTypePtr
						}
						return ""
					}(),
					"action_id": func() string {
						if actionIDPtr != nil {
							return *actionIDPtr
						}
						return ""
					}(),
					"checker_at": func() string {
						if checkerAtPtr != nil {
							return checkerAtPtr.Format("2006-01-02 15:04:05")
						}
						return ""
					}(),
					"checker_by": func() string {
						if checkerByPtr != nil {
							return *checkerByPtr
						}
						return ""
					}(),
					"checker_comment": func() string {
						if checkerCommentPtr != nil {
							return *checkerCommentPtr
						}
						return ""
					}(),
					"reason": func() string {
						if reasonPtr != nil {
							return *reasonPtr
						}
						return ""
					}(),
					"created_by": createdBy,
					"created_at": createdAt,
					"edited_by":  editedBy,
					"edited_at":  editedAt,
					"deleted_by": deletedBy,
					"deleted_at": deletedAt,
				},
				"children": []interface{}{},
			}
			if isDeleted && strings.ToUpper(func() string {
				if processingStatusPtr != nil {
					return *processingStatusPtr
				}
				return ""
			}()) == "APPROVED" {
				hideIds[entityID] = true
			}
		}
		// Fetch relationships
		relRows, err := pgxPool.Query(ctx, "SELECT parent_entity_id, child_entity_id FROM cashentityrelationships")
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
		// include hideIds as well (they are a subset of deletedIds but include for safety)
		for id := range hideIds {
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
func UpdateCashEntityBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			Entities []struct {
				EntityID string                 `json:"entity_id"`
				Fields   map[string]interface{} `json:"fields"`
				Reason   string                 `json:"reason"`
			} `json:"entities"`
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
			ctx := r.Context()
			tx, txErr := pgxPool.Begin(ctx)
			if txErr != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Failed to start transaction: " + txErr.Error(), "entity_id": entity.EntityID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
					if p := recover(); p != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "panic: " + fmt.Sprint(p), "entity_id": entity.EntityID})
					}
				}()

				// fetch existing values for old_* (use pointer types instead of sql.Null*)
				var exEntityName, exEntityShortName, exParentEntityID, exEntityRegistrationNumber, exCountry, exBaseCurrency, exTaxID, exAddress1, exAddress2, exCity, exState, exPostal, exContactName, exContactEmail, exContactPhone, exActiveStatus *string
				var exEntityLevel *int64
				var exIsTopLevel, exIsDeleted *bool
				sel := `SELECT entity_name, entity_short_name, entity_level, parent_entity_id, entity_registration_number, country, base_operating_currency, tax_identification_number, address_line1, address_line2, city, state_province, postal_code, contact_person_name, contact_person_email, contact_person_phone, active_status, is_top_level_entity, is_deleted FROM masterentitycash WHERE entity_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, entity.EntityID).Scan(&exEntityName, &exEntityShortName, &exEntityLevel, &exParentEntityID, &exEntityRegistrationNumber, &exCountry, &exBaseCurrency, &exTaxID, &exAddress1, &exAddress2, &exCity, &exState, &exPostal, &exContactName, &exContactEmail, &exContactPhone, &exActiveStatus, &exIsTopLevel, &exIsDeleted); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Failed to fetch existing entity: " + err.Error(), "entity_id": entity.EntityID})
					return
				}

				// build dynamic update
				var sets []string
				var args []interface{}
				pos := 1
				var parentProvided string
				for k, v := range entity.Fields {
					switch k {
					case "entity_name":
						sets = append(sets, fmt.Sprintf("entity_name=$%d, old_entity_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exEntityName != nil {
								return *exEntityName
							}
							return ""
						}())
						pos += 2
					case "entity_short_name":
						sets = append(sets, fmt.Sprintf("entity_short_name=$%d, old_entity_short_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exEntityShortName != nil {
								return *exEntityShortName
							}
							return ""
						}())
						pos += 2
					case "entity_level":
						var newLevel int64
						switch t := v.(type) {
						case float64:
							newLevel = int64(t)
						case int:
							newLevel = int64(t)
						case int64:
							newLevel = t
						case string:
							fmt.Sscan(t, &newLevel)
						default:
							newLevel = 0
						}
						sets = append(sets, fmt.Sprintf("entity_level=$%d, old_entity_level=$%d", pos, pos+1))
						args = append(args, newLevel, func() int64 {
							if exEntityLevel != nil {
								return *exEntityLevel
							}
							return int64(0)
						}())
						pos += 2
					case "parent_entity_id":
						newParent := fmt.Sprint(v)
						parentProvided = newParent
						sets = append(sets, fmt.Sprintf("parent_entity_id=$%d, old_parent_entity_id=$%d", pos, pos+1))
						args = append(args, newParent, func() string {
							if exParentEntityID != nil {
								return *exParentEntityID
							}
							return ""
						}())
						pos += 2
					case "entity_registration_number":
						sets = append(sets, fmt.Sprintf("entity_registration_number=$%d, old_entity_registration_number=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exEntityRegistrationNumber != nil {
								return *exEntityRegistrationNumber
							}
							return ""
						}())
						pos += 2
					case "country":
						sets = append(sets, fmt.Sprintf("country=$%d, old_country=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exCountry != nil {
								return *exCountry
							}
							return ""
						}())
						pos += 2
					case "base_operating_currency":
						sets = append(sets, fmt.Sprintf("base_operating_currency=$%d, old_base_operating_currency=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exBaseCurrency != nil {
								return *exBaseCurrency
							}
							return ""
						}())
						pos += 2
					case "tax_identification_number":
						sets = append(sets, fmt.Sprintf("tax_identification_number=$%d, old_tax_identification_number=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exTaxID != nil {
								return *exTaxID
							}
							return ""
						}())
						pos += 2
					case "address_line1":
						sets = append(sets, fmt.Sprintf("address_line1=$%d, old_address_line1=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exAddress1 != nil {
								return *exAddress1
							}
							return ""
						}())
						pos += 2
					case "address_line2":
						sets = append(sets, fmt.Sprintf("address_line2=$%d, old_address_line2=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exAddress2 != nil {
								return *exAddress2
							}
							return ""
						}())
						pos += 2
					case "city":
						sets = append(sets, fmt.Sprintf("city=$%d, old_city=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exCity != nil {
								return *exCity
							}
							return ""
						}())
						pos += 2
					case "state_province":
						sets = append(sets, fmt.Sprintf("state_province=$%d, old_state_province=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exState != nil {
								return *exState
							}
							return ""
						}())
						pos += 2
					case "postal_code":
						sets = append(sets, fmt.Sprintf("postal_code=$%d, old_postal_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exPostal != nil {
								return *exPostal
							}
							return ""
						}())
						pos += 2
					case "contact_person_name":
						sets = append(sets, fmt.Sprintf("contact_person_name=$%d, old_contact_person_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exContactName != nil {
								return *exContactName
							}
							return ""
						}())
						pos += 2
					case "contact_person_email":
						sets = append(sets, fmt.Sprintf("contact_person_email=$%d, old_contact_person_email=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exContactEmail != nil {
								return *exContactEmail
							}
							return ""
						}())
						pos += 2
					case "contact_person_phone":
						sets = append(sets, fmt.Sprintf("contact_person_phone=$%d, old_contact_person_phone=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exContactPhone != nil {
								return *exContactPhone
							}
							return ""
						}())
						pos += 2
					case "active_status":
						sets = append(sets, fmt.Sprintf("active_status=$%d, old_active_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exActiveStatus != nil {
								return *exActiveStatus
							}
							return ""
						}())
						pos += 2
					case "is_top_level_entity":
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
						sets = append(sets, fmt.Sprintf("is_top_level_entity=$%d", pos))
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

				var updatedEntityID string
				if len(sets) > 0 {
					q := "UPDATE masterentitycash SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE entity_id=$%d RETURNING entity_id", pos)
					args = append(args, entity.EntityID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedEntityID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": err.Error(), "entity_id": entity.EntityID})
						return
					}
				} else {
					updatedEntityID = entity.EntityID
				}

				// Insert audit action
				auditQuery := `INSERT INTO auditactionentity (
					entity_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, $2, $3, $4, $5, now())`
				if _, err := tx.Exec(ctx, auditQuery, updatedEntityID, "EDIT", "PENDING_EDIT_APPROVAL", entity.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Entity updated but audit log failed: " + err.Error(), "entity_id": updatedEntityID})
					return
				}

				// Sync relationships if parent_entity_id provided
				if parentProvided != "" {
					parentId := parentProvided
					if parentId != "" {
						var exists int
						err := pgxPool.QueryRow(ctx, `SELECT 1 FROM cashentityrelationships WHERE parent_entity_id = $1 AND child_entity_id = $2`, parentId, updatedEntityID).Scan(&exists)
						if err == pgx.ErrNoRows {
							relQuery := `INSERT INTO cashentityrelationships (parent_entity_id, child_entity_id, status) VALUES ($1, $2, 'Active') RETURNING relationship_id`
							var relID int
							relErr := pgxPool.QueryRow(ctx, relQuery, parentId, updatedEntityID).Scan(&relID)
							if relErr == nil {
								relationshipsAdded = append(relationshipsAdded, map[string]interface{}{"success": true, "relationship_id": relID, "parent_entity_id": parentId, "child_entity_id": updatedEntityID})
							}
						}
					}
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Transaction commit failed: " + err.Error(), "entity_id": updatedEntityID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "entity_id": updatedEntityID})
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

// Delete cash entity (and descendants if Delete-Approval)
func DeleteCashEntity(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_id, child_entity_id FROM cashentityrelationships`)
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
		rows, err := pgxPool.Query(ctx, `UPDATE masterentitycash SET is_deleted = true WHERE entity_id = ANY($1) RETURNING entity_id`, allToDelete)
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
			_, auditErr := pgxPool.Exec(ctx, auditQuery, eid, req.Reason, requestedBy)
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
func BulkRejectCashEntityActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		ctx := r.Context()
		// Fetch all relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_id, child_entity_id FROM cashentityrelationships`)
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
		rows, err := pgxPool.Query(ctx, query, checkerBy, req.Comment, allToReject)
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
func BulkApproveCashEntityActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		ctx := r.Context()
		// Fetch all relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_id, child_entity_id FROM cashentityrelationships`)
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
			err := pgxPool.QueryRow(ctx, `SELECT processing_status FROM auditactionentity WHERE entity_id = $1 ORDER BY requested_at DESC LIMIT 1`, eid).Scan(&status)
			if err == pgx.ErrNoRows {
				continue
			} else if err != nil {
				anyError = err
				break
			}
			if status == "PENDING_DELETE_APPROVAL" {
				// Mark all descendants as deleted (do not approve them)
				descendants := getAllDescendants([]string{eid})
				rows, err := pgxPool.Query(ctx, `UPDATE masterentitycash SET is_deleted = true WHERE entity_id = ANY($1) RETURNING entity_id`, descendants)
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

				// Also update the audit rows for these descendants: mark their delete actions as APPROVED
				auditRows, aerr := pgxPool.Query(ctx, `UPDATE auditactionentity SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE entity_id = ANY($3) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, entity_id`, checkerBy, req.Comment, descendants)
				if aerr != nil {
					anyError = aerr
					break
				}
				defer auditRows.Close()
				for auditRows.Next() {
					var actionID, entityID string
					if err := auditRows.Scan(&actionID, &entityID); err == nil {
						allUpdated = append(allUpdated, map[string]interface{}{
							"entity_id": entityID,
							"action_id": actionID,
							"status":    "Delete Approved",
						})
					}
				}
			} else {
				// Approve only this entity: update auditactionentity processing_status
				rows, err := pgxPool.Query(ctx, `UPDATE auditactionentity SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE entity_id = $3 AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id, entity_id`, checkerBy, req.Comment, eid)
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

func FindParentCashEntityAtLevel(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Parse request
		var req struct {
			UserID string `json:"user_id"`
			Level  int    `json:"level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   "Missing or invalid user_id/level",
			})
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
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   "Invalid user_id or session",
			})
			return
		}
		ctx := r.Context()

		parentLevel := req.Level - 1
		query := `
			SELECT m.entity_name, m.entity_id
			FROM masterentitycash m
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionentity a
				WHERE a.entity_id = m.entity_id
				  AND a.processing_status = 'APPROVED'
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.entity_level = $1
			  AND (m.is_deleted = false OR m.is_deleted IS NULL)
			  AND LOWER(m.active_status) = 'active'
		`

		rows, err := pgxPool.Query(ctx, query, parentLevel)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var name, id string
			if err := rows.Scan(&name, &id); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"error":   err.Error(),
				})
				return
			}
			results = append(results, map[string]interface{}{
				"name": name,
				"id":   id,
			})
		}

		// Check for iteration errors
		if err := rows.Err(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
			return
		}

		// Success response
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	}
}

// GET handler to fetch all entity_id, entity_name, entity_short_name for all cash entities, requiring user_id in body
func GetCashEntityNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		ctx := r.Context()
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
		rows, err := pgxPool.Query(ctx, query)
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

// UploadEntityCash handles staging and mapping for masterentitycash using pgxpool (no database/sql usage)
func UploadEntityCash(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		userName := ""
		for _, s := range auth.GetActiveSessions() {
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
		// collect relationships created across all batches
		relationshipsAdded := []map[string]interface{}{}

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fh.Filename)
				return
			}
			ext := getFileExt(fh.Filename)
			// reuse parseCashFlowCategoryFile for CSV/XLSX parsing which returns [][]string
			records, err := parseCashFlowCategoryFile(f, ext)
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
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

			headerNorm := make([]string, len(headerRow))
			for i, h := range headerRow {
				hn := strings.TrimSpace(h)
				hn = strings.Trim(hn, ", ")
				hn = strings.ToLower(hn)
				hn = strings.ReplaceAll(hn, " ", "_")
				hn = strings.Trim(hn, "\"'`")
				headerNorm[i] = hn
			}
			columns := append([]string{"upload_batch_id"}, headerNorm...)

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

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"input_entitycash"}, columns, pgx.CopyFromRows(copyRows)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// read mapping from upload_mapping_entity
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_entity`)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error")
				return
			}
			mapping := make(map[string]string)
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					key := strings.ToLower(strings.TrimSpace(src))
					key = strings.ReplaceAll(key, " ", "_")
					tt := strings.TrimSpace(tgt)
					tt = strings.Trim(tt, ", \"'`")
					tt = strings.ReplaceAll(tt, " ", "_")
					mapping[key] = tt
				}
			}
			mapRows.Close()

			var srcCols []string
			var tgtCols []string
			for i, h := range headerRow {
				key := headerNorm[i]
				if t, ok := mapping[key]; ok {
					srcCols = append(srcCols, key)
					tgtCols = append(tgtCols, t)
				} else {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No mapping for source column: %s", h))
					return
				}
			}

			tgtColsStr := strings.Join(tgtCols, ", ")
			var selectExprs []string
			for i, src := range srcCols {
				tgt := tgtCols[i]
				selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
			}
			srcColsStr := strings.Join(selectExprs, ", ")

			insertSQL := fmt.Sprintf(`INSERT INTO masterentitycash (%s) SELECT %s FROM input_entitycash s WHERE s.upload_batch_id = $1 RETURNING entity_id`, tgtColsStr, srcColsStr)
			rows2, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newIDs []string
			for rows2.Next() {
				var id string
				if err := rows2.Scan(&id); err == nil {
					newIDs = append(newIDs, id)
				}
			}
			rows2.Close()

			// Insert audit actions for created entities
			if len(newIDs) > 0 {
				auditSQL := `INSERT INTO auditactionentity (entity_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT entity_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM masterentitycash WHERE entity_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userName, newIDs); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}

			// Sync parent-child relationships for newly created entities (if parent_entity_id set)
			if len(newIDs) > 0 {
				relRows2, err := tx.Query(ctx, `SELECT entity_id, parent_entity_id FROM masterentitycash WHERE entity_id = ANY($1)`, newIDs)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch parent info: "+err.Error())
					return
				}
				for relRows2.Next() {
					var eid, parent string
					if err := relRows2.Scan(&eid, &parent); err != nil {
						continue
					}
					if parent == "" {
						continue
					}
					// check existence
					var exists int
					err = tx.QueryRow(ctx, `SELECT 1 FROM cashentityrelationships WHERE parent_entity_id=$1 AND child_entity_id=$2`, parent, eid).Scan(&exists)
					if err == pgx.ErrNoRows {
						var relID int
						err2 := tx.QueryRow(ctx, `INSERT INTO cashentityrelationships (parent_entity_id, child_entity_id, status) VALUES ($1, $2, 'Active') RETURNING relationship_id`, parent, eid).Scan(&relID)
						if err2 == nil {
							relationshipsAdded = append(relationshipsAdded, map[string]interface{}{"success": true, "relationship_id": relID, "parent_entity_id": parent, "child_entity_id": eid})
						}
					}
				}
				relRows2.Close()
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "batch_ids": batchIDs, "relationships_added": len(relationshipsAdded), "relationship_details": relationshipsAdded})
	}
}
