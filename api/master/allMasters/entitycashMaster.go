package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"compress/gzip"
	"database/sql"

	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	// "strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CashEntityMasterRequest struct {
	EntityName       string `json:"entity_name"`
	ParentEntityName string `json:"parent_entity_name"`
	EntityShortName  string `json:"entity_short_name"`
	EntityLevel      int    `json:"entity_level"`
	// ParentEntityName           string `json:"parent_entity_name"`
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
				createdBy = s.Name
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
			// 	entity_id, entity_name, entity_short_name, entity_level, parent_entity_name, entity_registration_number, country, base_operating_currency, tax_identification_number, address_line1, address_line2, city, state_province, postal_code, contact_person_name, contact_person_email, contact_person_phone, active_status, old_entity_name, old_entity_short_name, old_entity_level, old_parent_entity_name, old_entity_registration_number, old_country, old_base_operating_currency, old_tax_identification_number, old_address_line1, old_address_line2, old_city, old_state_province, old_postal_code, old_contact_person_name, old_contact_person_email, old_contact_person_phone, old_active_status, is_top_level_entity, is_deleted
			// ) VALUES (
			// 	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, COALESCE(NULLIF($18, ''), 'Inactive'), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $19, $20
			// ) RETURNING entity_id`
			query := `INSERT INTO masterentitycash (
	entity_id, entity_name, entity_short_name, entity_level, parent_entity_name, entity_registration_number, country, base_operating_currency, tax_identification_number, address_line1, address_line2, city, state_province, postal_code, contact_person_name, contact_person_email, contact_person_phone, active_status, is_top_level_entity, is_deleted
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, COALESCE(NULLIF($18, ''), 'Inactive'), $19, $20
) RETURNING entity_id`
			var newEntityID string
			err := pgxPool.QueryRow(r.Context(), query,
				entityId,
				entity.EntityName,
				entity.EntityShortName,
				entity.EntityLevel,
				entity.ParentEntityName,
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
	// Skip if parent is empty (top-level entity)
	if strings.TrimSpace(entity.ParentEntityName) == "" {
		continue
	}

	parentName := entity.ParentEntityName
	childName := entity.EntityName

	var exists int
	err := pgxPool.QueryRow(r.Context(),
		`SELECT 1 FROM cashentityrelationships WHERE parent_entity_name = $1 AND child_entity_name = $2`,
		parentName, childName,
	).Scan(&exists)

	if err == pgx.ErrNoRows {
		relQuery := `
			INSERT INTO cashentityrelationships (parent_entity_name, child_entity_name, status)
			VALUES ($1, $2, 'Active')
			RETURNING relationship_id
		`
		var relID int
		relErr := pgxPool.QueryRow(r.Context(), relQuery, parentName, childName).Scan(&relID)
		if relErr == nil {
			relationshipsAdded = append(relationshipsAdded, map[string]interface{}{
				"success":            true,
				"relationship_id":    relID,
				"parent_entity_name": parentName,
				"child_entity_name":  childName,
			})
		} else {
			relationshipsAdded = append(relationshipsAdded, map[string]interface{}{
				"success":            false,
				"error":              relErr.Error(),
				"parent_entity_name": parentName,
				"child_entity_name":  childName,
			})
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

func GetCashEntityHierarchy(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		start := time.Now()

		// === 1️⃣ Fetch entities with latest audit info
		entityQuery := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.entity_id)
					a.entity_id, a.processing_status, a.requested_by, a.requested_at,
					a.actiontype, a.action_id, a.checker_by, a.checker_at,
					a.checker_comment, a.reason
				FROM auditactionentity a
				ORDER BY a.entity_id, a.requested_at DESC
			)
			SELECT
				m.entity_id, m.entity_name, m.entity_short_name, m.entity_level, m.parent_entity_name,
				m.entity_registration_number, m.country, m.base_operating_currency, m.tax_identification_number,
				m.address_line1, m.address_line2, m.city, m.state_province, m.postal_code,
				m.contact_person_name, m.contact_person_email, m.contact_person_phone, m.active_status,
				m.old_entity_name, m.old_entity_short_name, m.old_entity_level, m.old_parent_entity_name,
				m.old_entity_registration_number, m.old_country, m.old_base_operating_currency,
				m.old_tax_identification_number, m.old_address_line1, m.old_address_line2,
				m.old_city, m.old_state_province, m.old_postal_code, m.old_contact_person_name,
				m.old_contact_person_email, m.old_contact_person_phone, m.old_active_status,
				m.is_top_level_entity, m.is_deleted,
				a.processing_status, a.requested_by, a.requested_at, a.actiontype, a.action_id,
				a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM masterentitycash m
			LEFT JOIN latest_audit a ON a.entity_id = m.entity_id;
		`

		rows, err := pgxPool.Query(ctx, entityQuery)
		if err != nil {
			http.Error(w, "query failed: "+err.Error(), 500)
			return
		}
		defer rows.Close()

		// === Struct with exported fields for JSON
		type EntityNode struct {
			ID       string                 `json:"id"`
			Name     string                 `json:"name"`
			Parent   string                 `json:"parent"`
			Data     map[string]interface{} `json:"data"`
			Children []*EntityNode          `json:"children"`
		}

		entityMap := make(map[string]*EntityNode)
		hideIDs := make(map[string]bool)
		allIDs := []string{}

		for rows.Next() {
			var (
				id, name, shortName, parentName, regNum, country, baseCur, taxID,
				addr1, addr2, city, state, postal, contactName, contactEmail, contactPhone,
				active, oldName, oldShort, oldParent, oldReg, oldCountry, oldBaseCur,
				oldTax, oldAddr1, oldAddr2, oldCity, oldState, oldPostal, oldContactName,
				oldContactEmail, oldContactPhone, oldActive, procStatus, reqBy, actType,
				actID, checkerBy, checkerComment, reason sql.NullString
				level, oldLevel  sql.NullInt64
				reqAt, checkerAt sql.NullTime
				isTop, isDel     bool
			)

			if err := rows.Scan(
				&id, &name, &shortName, &level, &parentName,
				&regNum, &country, &baseCur, &taxID,
				&addr1, &addr2, &city, &state, &postal,
				&contactName, &contactEmail, &contactPhone, &active,
				&oldName, &oldShort, &oldLevel, &oldParent, &oldReg, &oldCountry,
				&oldBaseCur, &oldTax, &oldAddr1, &oldAddr2, &oldCity, &oldState,
				&oldPostal, &oldContactName, &oldContactEmail, &oldContactPhone, &oldActive,
				&isTop, &isDel,
				&procStatus, &reqBy, &reqAt, &actType, &actID, &checkerBy, &checkerAt, &checkerComment, &reason,
			); err != nil {
				continue
			}

			if id.Valid {
				allIDs = append(allIDs, id.String)
			}

			data := map[string]interface{}{
				"entity_id":                      id.String,
				"entity_name":                    name.String,
				"entity_short_name":              shortName.String,
				"entity_level":                   level.Int64,
				"parent_entity_name":             parentName.String,
				"entity_registration_number":     regNum.String,
				"country":                        country.String,
				"base_operating_currency":        baseCur.String,
				"tax_identification_number":      taxID.String,
				"address_line1":                  addr1.String,
				"address_line2":                  addr2.String,
				"city":                           city.String,
				"state_province":                 state.String,
				"postal_code":                    postal.String,
				"contact_person_name":            contactName.String,
				"contact_person_email":           contactEmail.String,
				"contact_person_phone":           contactPhone.String,
				"active_status":                  active.String,
				"old_entity_name":                oldName.String,
				"old_entity_short_name":          oldShort.String,
				"old_entity_level":               oldLevel.Int64,
				"old_parent_entity_name":         oldParent.String,
				"old_entity_registration_number": oldReg.String,
				"old_country":                    oldCountry.String,
				"old_base_operating_currency":    oldBaseCur.String,
				"old_tax_identification_number":  oldTax.String,
				"old_address_line1":              oldAddr1.String,
				"old_address_line2":              oldAddr2.String,
				"old_city":                       oldCity.String,
				"old_state_province":             oldState.String,
				"old_postal_code":                oldPostal.String,
				"old_contact_person_name":        oldContactName.String,
				"old_contact_person_email":       oldContactEmail.String,
				"old_contact_person_phone":       oldContactPhone.String,
				"old_active_status":              oldActive.String,
				"is_top_level_entity":            isTop,
				"is_deleted":                     isDel,
				"processing_status":              procStatus.String,
				"action_type":                    actType.String,
				"action_id":                      actID.String,
				"checker_by":                     checkerBy.String,
				"checker_comment":                checkerComment.String,
				"reason":                         reason.String,
				"checker_at": func() string {
					if checkerAt.Valid {
						return checkerAt.Time.Format("2006-01-02 15:04:05")
					}
					return ""
				}(),
				"created_by": reqBy.String,
				"created_at": func() string {
					if reqAt.Valid {
						return reqAt.Time.Format("2006-01-02 15:04:05")
					}
					return ""
				}(),
			}

			node := &EntityNode{
				ID:       id.String,
				Name:     name.String,
				Parent:   parentName.String,
				Data:     data,
				Children: []*EntityNode{},
			}

			entityMap[id.String] = node

			// Only hide if deletion is fully approved
			if isDel && strings.EqualFold(procStatus.String, "APPROVED") {
				hideIDs[id.String] = true
			}
		}

		// === 2️⃣ Fetch audit history
		auditQuery := `
			SELECT entity_id, actiontype, requested_by, requested_at
			FROM auditactionentity
			WHERE entity_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')
			ORDER BY entity_id, requested_at DESC;
		`
		auditRows, err := pgxPool.Query(ctx, auditQuery, allIDs)
		if err == nil {
			defer auditRows.Close()
			for auditRows.Next() {
				var eid, atype, rby sql.NullString
				var rat sql.NullTime
				if err := auditRows.Scan(&eid, &atype, &rby, &rat); err == nil {
					if ent, ok := entityMap[eid.String]; ok {
						ts := ""
						if rat.Valid {
							ts = rat.Time.Format("2006-01-02 15:04:05")
						}
						switch atype.String {
						case "CREATE":
							ent.Data["created_by"] = rby.String
							ent.Data["created_at"] = ts
						case "EDIT":
							ent.Data["edited_by"] = rby.String
							ent.Data["edited_at"] = ts
						case "DELETE":
							ent.Data["deleted_by"] = rby.String
							ent.Data["deleted_at"] = ts
						}
					}
				}
			}
		}

		// === 3️⃣ Fetch hierarchy relationships
		relRows, err := pgxPool.Query(ctx, "SELECT parent_entity_name, child_entity_name FROM cashentityrelationships")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer relRows.Close()

		parentMap := make(map[string][]string)
		nameToID := map[string]string{}
		for _, e := range entityMap {
			nameToID[e.Name] = e.ID
		}

		for relRows.Next() {
			var parentName, childName string
			if err := relRows.Scan(&parentName, &childName); err == nil {
				pid, cid := nameToID[parentName], nameToID[childName]
				if pid != "" && cid != "" {
					parentMap[pid] = append(parentMap[pid], cid)
				}
			}
		}

		// === 4️⃣ Remove deleted + descendants
		getDescendants := func(ids []string) []string {
			all := map[string]bool{}
			queue := append([]string{}, ids...)
			for _, id := range ids {
				all[id] = true
			}
			for len(queue) > 0 {
				curr := queue[0]
				queue = queue[1:]
				for _, c := range parentMap[curr] {
					if !all[c] {
						all[c] = true
						queue = append(queue, c)
					}
				}
			}
			out := []string{}
			for k := range all {
				out = append(out, k)
			}
			return out
		}

		deletedList := []string{}
		for id := range hideIDs {
			deletedList = append(deletedList, id)
		}

		for _, d := range getDescendants(deletedList) {
			delete(entityMap, d)
		}

		// === 5️⃣ Build tree
		for pid, children := range parentMap {
			if parent, ok := entityMap[pid]; ok {
				for _, cid := range children {
					if child, ok := entityMap[cid]; ok {
						parent.Children = append(parent.Children, child)
					}
				}
			}
		}

		// === 6️⃣ Find top-level nodes
		childSet := map[string]bool{}
		for _, children := range parentMap {
			for _, c := range children {
				childSet[c] = true
			}
		}

		top := []*EntityNode{}
		for _, e := range entityMap {
			if e.Data["is_top_level_entity"].(bool) || !childSet[e.ID] {
				top = append(top, e)
			}
		}

		// === 7️⃣ Return compressed JSON
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()

		if err := json.NewEncoder(gz).Encode(top); err != nil {
			http.Error(w, "encode failed: "+err.Error(), 500)
			return
		}

		log.Printf("✅ Hierarchy built (%d entities, %d roots) in %v", len(entityMap), len(top), time.Since(start))
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
				updatedBy = s.Name
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
				var exEntityName, exEntityShortName, exParentEntityName, exEntityRegistrationNumber, exCountry, exBaseCurrency, exTaxID, exAddress1, exAddress2, exCity, exState, exPostal, exContactName, exContactEmail, exContactPhone, exActiveStatus *string
				var exEntityLevel *int64
				var exIsTopLevel, exIsDeleted *bool
				sel := `SELECT entity_name, entity_short_name, entity_level, parent_entity_name, entity_registration_number, country, base_operating_currency, tax_identification_number, address_line1, address_line2, city, state_province, postal_code, contact_person_name, contact_person_email, contact_person_phone, active_status, is_top_level_entity, is_deleted FROM masterentitycash WHERE entity_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, entity.EntityID).Scan(&exEntityName, &exEntityShortName, &exEntityLevel, &exParentEntityName, &exEntityRegistrationNumber, &exCountry, &exBaseCurrency, &exTaxID, &exAddress1, &exAddress2, &exCity, &exState, &exPostal, &exContactName, &exContactEmail, &exContactPhone, &exActiveStatus, &exIsTopLevel, &exIsDeleted); err != nil {
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
					case "parent_entity_name":
						newParent := fmt.Sprint(v)
						parentProvided = newParent
						sets = append(sets, fmt.Sprintf("parent_entity_name=$%d, old_parent_entity_name=$%d", pos, pos+1))
						args = append(args, newParent, func() string {
							if exParentEntityName != nil {
								return *exParentEntityName
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

				// Sync relationships if parent_entity_name provided
				if parentProvided != "" {
					parentId := parentProvided
					if parentId != "" {
						var exists int
						err := pgxPool.QueryRow(ctx, `SELECT 1 FROM cashentityrelationships WHERE parent_entity_name = $1 AND child_entity_name = $2`, parentId, updatedEntityID).Scan(&exists)
						if err == pgx.ErrNoRows {
							relQuery := `INSERT INTO cashentityrelationships (parent_entity_name, child_entity_name, status) VALUES ($1, $2, 'Active') RETURNING relationship_id`
							var relID int
							relErr := pgxPool.QueryRow(ctx, relQuery, parentId, updatedEntityID).Scan(&relID)
							if relErr == nil {
								relationshipsAdded = append(relationshipsAdded, map[string]interface{}{"success": true, "relationship_id": relID, "parent_entity_name": parentId, "child_entity_name": updatedEntityID})
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
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		// Fetch all relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_name, child_entity_name FROM cashentityrelationships`)
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
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		ctx := r.Context()
		// Fetch all relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_name, child_entity_name FROM cashentityrelationships`)
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
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		ctx := r.Context()
		// Fetch all relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_name, child_entity_name FROM cashentityrelationships`)
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
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.entity_level = $1
			  AND (m.is_deleted = false OR m.is_deleted IS NULL)
			  AND LOWER(m.active_status) = 'active'
			  AND COALESCE(LOWER(a.processing_status),'') = 'approved'
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

			// Sync parent-child relationships for newly created entities (if parent_entity_name set)
			if len(newIDs) > 0 {
				relRows2, err := tx.Query(ctx, `SELECT entity_id, parent_entity_name FROM masterentitycash WHERE entity_id = ANY($1)`, newIDs)
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
					err = tx.QueryRow(ctx, `SELECT 1 FROM cashentityrelationships WHERE parent_entity_name=$1 AND child_entity_name=$2`, parent, eid).Scan(&exists)
					if err == pgx.ErrNoRows {
						var relID int
						err2 := tx.QueryRow(ctx, `INSERT INTO cashentityrelationships (parent_entity_name, child_entity_name, status) VALUES ($1, $2, 'Active') RETURNING relationship_id`, parent, eid).Scan(&relID)
						if err2 == nil {
							relationshipsAdded = append(relationshipsAdded, map[string]interface{}{"success": true, "relationship_id": relID, "parent_entity_name": parent, "child_entity_name": eid})
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

func UploadEntitySimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startOverall := time.Now()
		ctx := r.Context()

		// Quick index ensure (non-fatal)
		_ = func() error {
			cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := pgxPool.Exec(cctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_masterentitycash_name ON masterentitycash (entity_name)`)
			if err != nil {
				log.Printf("[UploadEntitySimple] warn: ensure index failed: %v", err)
			}
			_, _ = pgxPool.Exec(cctx, `ANALYZE masterentitycash`)
			return nil
		}()

		// 1) identify user
		userID := r.FormValue("user_id")
		if userID == "" {
			var body struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			userID = body.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
			return
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

		// 2) parse multipart & file (supports CSV and XLSX via shared parser)
		if err := r.ParseMultipartForm(128 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "parse multipart: "+err.Error())
			return
		}
		f, fh, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "file required: "+err.Error())
			return
		}
		defer f.Close()

		records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
		if err != nil || len(records) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
			return
		}
		header := normalizeHeader(records[0])
		dataRows := records[1:]

		// allowed columns mapping (extend if needed)
		allowed := map[string]bool{
			"entity_name": true, "entity_short_name": true, "entity_level": true,
			"parent_entity_name": true, "entity_registration_number": true,
			"country": true, "base_operating_currency": true, "tax_identification_number": true,
			"address_line1": true, "address_line2": true, "city": true, "state_province": true, "postal_code": true,
			"contact_person_name": true, "contact_person_email": true, "contact_person_phone": true,
			"active_status": true, "is_top_level_entity": true,
		}

		copyCols := make([]string, 0, len(header))
		headerPos := make(map[string]int, len(header))
		for i, h := range header {
			headerPos[h] = i
			if allowed[h] {
				copyCols = append(copyCols, h)
			}
		}
		// require entity_name and entity_level
		if !(contains(copyCols, "entity_name") && contains(copyCols, "entity_level")) {
			api.RespondWithError(w, http.StatusBadRequest, "CSV/XLSX must include entity_name and entity_level")
			return
		}

		// 3) start transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		// session tuning
		if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = OFF"); err != nil {
			log.Printf("[UploadEntitySimple] warn: set synchronous_commit failed: %v", err)
		}
		if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '10min'"); err != nil {
			log.Printf("[UploadEntitySimple] warn: set statement_timeout failed: %v", err)
		}

		// create temp table like master (safe with ON COMMIT DROP)
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_me (LIKE masterentitycash INCLUDING DEFAULTS) ON COMMIT DROP`); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Temp table failed: "+err.Error())
			return
		}

		// Build copyRows in memory (simpler and supports XLSX)
		copyRows := make([][]interface{}, 0, len(dataRows))
		for _, rec := range dataRows {
			row := make([]interface{}, len(copyCols))
			for j, col := range copyCols {
				if pos, ok := headerPos[col]; ok && pos < len(rec) {
					cell := strings.TrimSpace(rec[pos])
					if cell == "" {
						row[j] = nil
					} else {
						row[j] = cell
					}
				} else {
					row[j] = nil
				}
			}
			copyRows = append(copyRows, row)
		}
		rowsCopied := len(copyRows)
		skipped := 0 
		tStart := time.Now()
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_me"}, copyCols, pgx.CopyFromRows(copyRows)); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "COPY failed: "+err.Error())
			return
		}
		log.Printf("[UploadEntitySimple] COPY rows=%d elapsed=%v skipped=%d", rowsCopied, time.Since(tStart), skipped)

		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_tmp_me_name ON tmp_me (entity_name)`); err != nil {
			log.Printf("[UploadEntitySimple] warn: create index tmp_me failed: %v", err)
		}

		// 4) insert new entities (anti-join by name)
		t1 := time.Now()

		if _, err := tx.Exec(ctx, `
INSERT INTO masterentitycash (
	entity_name, entity_short_name, entity_level, entity_registration_number,
	country, base_operating_currency, tax_identification_number,
	address_line1, address_line2, city, state_province, postal_code,
	contact_person_name, contact_person_email, contact_person_phone,
	active_status, is_top_level_entity, is_deleted, parent_entity_name
)
SELECT
	t.entity_name, t.entity_short_name, t.entity_level, t.entity_registration_number,
	t.country, t.base_operating_currency, t.tax_identification_number,
	t.address_line1, t.address_line2, t.city, t.state_province, t.postal_code,
	t.contact_person_name, t.contact_person_email, t.contact_person_phone,
	COALESCE(t.active_status, 'Inactive'),
	COALESCE(t.is_top_level_entity, false),
	false,
	t.parent_entity_name
FROM tmp_me t
LEFT JOIN masterentitycash m ON m.entity_name = t.entity_name
WHERE m.entity_name IS NULL;
`); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}
		log.Printf("[UploadEntitySimple] insert elapsed=%v", time.Since(t1))

		// 5) update existing entities if changed (by name)
		t2 := time.Now()
		if _, err := tx.Exec(ctx, `
UPDATE masterentitycash m
SET
	entity_short_name = t.entity_short_name,
	entity_level = t.entity_level,
	entity_registration_number = t.entity_registration_number,
	country = t.country,
	base_operating_currency = t.base_operating_currency,
	tax_identification_number = t.tax_identification_number,
	address_line1 = t.address_line1,
	address_line2 = t.address_line2,
	city = t.city,
	state_province = t.state_province,
	postal_code = t.postal_code,
	contact_person_name = t.contact_person_name,
	contact_person_email = t.contact_person_email,
	contact_person_phone = t.contact_person_phone,
	active_status = COALESCE(t.active_status, m.active_status),
	is_top_level_entity = COALESCE(t.is_top_level_entity, m.is_top_level_entity),
	
	parent_entity_name = t.parent_entity_name
FROM tmp_me t
WHERE m.entity_name = t.entity_name
AND (
	m.entity_short_name IS DISTINCT FROM t.entity_short_name OR
	m.entity_level IS DISTINCT FROM t.entity_level OR
	m.entity_registration_number IS DISTINCT FROM t.entity_registration_number OR
	m.country IS DISTINCT FROM t.country OR
	m.base_operating_currency IS DISTINCT FROM t.base_operating_currency OR
	m.tax_identification_number IS DISTINCT FROM t.tax_identification_number OR
	m.address_line1 IS DISTINCT FROM t.address_line1 OR
	m.address_line2 IS DISTINCT FROM t.address_line2 OR
	m.city IS DISTINCT FROM t.city OR
	m.state_province IS DISTINCT FROM t.state_province OR
	m.postal_code IS DISTINCT FROM t.postal_code OR
	m.contact_person_name IS DISTINCT FROM t.contact_person_name OR
	m.contact_person_email IS DISTINCT FROM t.contact_person_email OR
	m.contact_person_phone IS DISTINCT FROM t.contact_person_phone OR
	m.active_status IS DISTINCT FROM COALESCE(t.active_status, m.active_status) OR
	m.is_top_level_entity IS DISTINCT FROM COALESCE(t.is_top_level_entity, m.is_top_level_entity) OR
	
	m.parent_entity_name IS DISTINCT FROM t.parent_entity_name
);
`); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Update failed: "+err.Error())
			return
		}
		log.Printf("[UploadEntitySimple] update elapsed=%v", time.Since(t2))

		// 6) hierarchy sync (recalc levels for affected and their descendants up to depth 3)
		t3 := time.Now()
		if _, err := tx.Exec(ctx, `
WITH RECURSIVE affected AS (
  SELECT entity_id, entity_name, parent_entity_name, 0 AS lvl
  FROM masterentitycash
  WHERE entity_name IN (SELECT entity_name FROM tmp_me)

  UNION ALL

  SELECT c.entity_id, c.entity_name, c.parent_entity_name, CASE WHEN a.lvl + 1 > 3 THEN 3 ELSE a.lvl + 1 END
  FROM masterentitycash c
  JOIN affected a ON c.parent_entity_name = a.entity_name
  WHERE a.lvl < 3
)
UPDATE masterentitycash m
SET entity_level = a.lvl,
    is_top_level_entity = (a.lvl = 0)
FROM affected a
WHERE m.entity_id = a.entity_id;
`); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Hierarchy failed: "+err.Error())
			return
		}
		log.Printf("[UploadEntitySimple] hierarchy elapsed=%v", time.Since(t3))

		// 7) insert missing name-based relationships (parent_name, child_name)
		t4 := time.Now()
		if _, err := tx.Exec(ctx, `
INSERT INTO cashentityrelationships (parent_entity_name, child_entity_name, status)
SELECT DISTINCT p.entity_name, c.entity_name, 'Active'
FROM masterentitycash c
JOIN masterentitycash p ON c.parent_entity_name = p.entity_name
WHERE (p.entity_name IN (SELECT entity_name FROM tmp_me)
       OR c.entity_name IN (SELECT entity_name FROM tmp_me))
ON CONFLICT (parent_entity_name, child_entity_name) DO NOTHING;
`); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Relationships failed: "+err.Error())
			return
		}
		log.Printf("[UploadEntitySimple] relationships elapsed=%v", time.Since(t4))

		// 8) audit insert (CREATE for all affected names)
		t5 := time.Now()
		if _, err := tx.Exec(ctx, `
INSERT INTO auditactionentity(entity_id, actiontype, processing_status, requested_by, requested_at)
SELECT m.entity_id, 'CREATE', 'PENDING_APPROVAL', $1, now()
FROM masterentitycash m
WHERE m.entity_name IN (SELECT entity_name FROM tmp_me)
ON CONFLICT DO NOTHING;
`, userName); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}
		log.Printf("[UploadEntitySimple] audit elapsed=%v", time.Since(t5))

		// commit
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}
		tx = nil

		totalDur := time.Since(startOverall)
		resp := map[string]interface{}{
			"success":     true,
			"file":        fh.Filename,
			"rows":        rowsCopied,
			"skipped":     skipped,
			"duration_ms": totalDur.Milliseconds(),
			"batch_id":    uuid.New().String(),
		}
		log.Printf("[UploadEntitySimple] finished rows=%d skipped=%d total_ms=%d", rowsCopied, skipped, totalDur.Milliseconds())

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}
