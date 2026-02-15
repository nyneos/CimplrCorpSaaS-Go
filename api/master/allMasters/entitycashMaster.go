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

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyEntityCashError returns a user-friendly error message and HTTP status code
func getUserFriendlyEntityCashError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errMsg := err.Error()
	errLower := strings.ToLower(errMsg)

	// Unique constraint violations
	if strings.Contains(errLower, "unique_entity_name_not_deleted") {
		return "Entity name already exists and is not deleted. Please use a different name.", http.StatusOK
	}
	if strings.Contains(errLower, "idx_masterentitycash_name") {
		return "Entity name already exists. Please use a different name.", http.StatusOK
	}
	if strings.Contains(errLower, "uq_parent_child") {
		return "This parent-child entity relationship already exists.", http.StatusOK
	}

	// Check constraints
	if strings.Contains(errLower, "masterentitycash_entity_level_check") {
		return "Entity level must be 0, 1, 2, or 3.", http.StatusOK
	}
	if strings.Contains(errLower, "masterentitycash_old_entity_level_check") {
		return "Old entity level must be 0, 1, 2, or 3.", http.StatusOK
	}

	// Audit action check constraints
	if strings.Contains(errLower, "auditactionentity_actiontype_check") {
		return "Action type must be one of: CREATE, EDIT, DELETE.", http.StatusOK
	}
	if strings.Contains(errLower, "auditactionentity_processing_status_check") {
		return "Processing status must be one of: PENDING_APPROVAL, PENDING_EDIT_APPROVAL, PENDING_DELETE_APPROVAL, APPROVED, REJECTED, CANCELLED.", http.StatusOK
	}

	// Foreign key violations
	if strings.Contains(errLower, "foreign key") || strings.Contains(errLower, "fk_") || strings.Contains(errLower, "_fkey") {
		if strings.Contains(errLower, "entity_id") {
			return "Entity does not exist or has been deleted.", http.StatusOK
		}
		if strings.Contains(errLower, "parent_entity") {
			return "Parent entity does not exist or has been deleted.", http.StatusOK
		}
		return "Referenced record does not exist. Please check your input.", http.StatusOK
	}

	// Not null violations
	if strings.Contains(errLower, "not null") || strings.Contains(errLower, "null value") {
		if strings.Contains(errLower, "entity_name") {
			return "Entity name is required.", http.StatusOK
		}
		if strings.Contains(errLower, "entity_level") {
			return "Entity level is required.", http.StatusOK
		}
		if strings.Contains(errLower, "country") {
			return "Country is required.", http.StatusOK
		}
		if strings.Contains(errLower, "base_operating_currency") {
			return "Base operating currency is required.", http.StatusOK
		}
		if strings.Contains(errLower, "active_status") {
			return "Active status is required.", http.StatusOK
		}
		return "A required field is missing. Please check your input.", http.StatusOK
	}

	// Connection errors
	if strings.Contains(errLower, "connection") || strings.Contains(errLower, "timeout") {
		return fmt.Sprintf("%s: Database connection issue. Please try again.", context), http.StatusServiceUnavailable
	}

	// Default error
	return fmt.Sprintf("%s: %s", context, errMsg), http.StatusInternalServerError
}

type CashEntityMasterRequest struct {
	EntityName       string `json:"entity_name"`
	ParentEntityName string `json:"parent_entity_name"`
	EntityShortName  string `json:"entity_short_name"`
	EntityLevel      int    `json:"entity_level"`
	// ParentEntityName           string `json:"parent_entity_name"`
	EntityRegistrationNumber  string `json:"entity_registration_number"`
	Country                   string `json:"country"`
	BaseOperatingCurrency     string `json:"base_operating_currency"`
	TaxIdentificationNumber   string `json:"tax_identification_number"`
	AddressLine1              string `json:"address_line1"`
	AddressLine2              string `json:"address_line2"`
	City                      string `json:"city"`
	StateProvince             string `json:"state_province"`
	PostalCode                string `json:"postal_code"`
	ContactPersonName         string `json:"contact_person_name"`
	ContactPersonEmail        string `json:"contact_person_email"`
	ContactPersonPhone        string `json:"contact_person_phone"`
	ActiveStatus              string `json:"active_status"`
	IsTopLevelEntity          bool   `json:"is_top_level_entity"`
	IsDeleted                 bool   `json:"is_deleted"`
	PanGST                    string `json:"pan_gst"`
	LegalEntityIdentifier     string `json:"legal_entity_identifier"`
	LegalEntityType           string `json:"legal_entity_type"`
	ReportingCurrency         string `json:"reporting_currency"`
	FxTradingAuthority        string `json:"fx_trading_authority"`
	InternalFxTradingLimit    string `json:"internal_fx_trading_limit"`
	AssociatedTreasuryContact string `json:"associated_treasury_contact"`
	AssociatedBusinessUnits   string `json:"associated_business_units"`
	Comments                  string `json:"comments"`
	UniqueIdentifier          string `json:"unique_identifier"`
}

type CashEntityBulkRequest struct {
	Entities []CashEntityMasterRequest `json:"entities"`
	UserID   string                    `json:"user_id"`
}

func CreateAndSyncCashEntities(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CashEntityBulkRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		currCodes := api.GetCurrencyCodesFromCtx(ctx)
		accessibleEntityNames := api.GetEntityNamesFromCtx(ctx)

		entityIDs := make(map[string]string)
		inserted := []map[string]interface{}{}
		for _, entity := range req.Entities {
			// Validate currency
			if entity.BaseOperatingCurrency != "" && len(currCodes) > 0 && !api.IsCurrencyAllowed(ctx, entity.BaseOperatingCurrency) {
				inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized base_operating_currency: " + entity.BaseOperatingCurrency, "entity_name": entity.EntityName})
				continue
			}

			// Validate reporting currency if present
			if entity.ReportingCurrency != "" && len(currCodes) > 0 && !api.IsCurrencyAllowed(ctx, entity.ReportingCurrency) {
				inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized reporting_currency: " + entity.ReportingCurrency, "entity_name": entity.EntityName})
				continue
			}

			// Validate parent entity exists in user's accessible entities (not just approved)
			if entity.ParentEntityName != "" {
				parentAllowed := false
				for _, name := range accessibleEntityNames {
					if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(entity.ParentEntityName)) {
						parentAllowed = true
						break
					}
				}
				if !parentAllowed {
					inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "parent entity not in your accessible hierarchy: " + entity.ParentEntityName, "entity_name": entity.EntityName})
					continue
				}
			}

			// Let DB generate entity_id (use default). Insert all columns including newly-added ones.
			query := `INSERT INTO masterentitycash (
	entity_name, entity_short_name, entity_level, parent_entity_name, entity_registration_number, country, base_operating_currency, tax_identification_number, address_line1, address_line2, city, state_province, postal_code, contact_person_name, contact_person_email, contact_person_phone, active_status, pan_gst, legal_entity_identifier, legal_entity_type, reporting_currency, fx_trading_authority, internal_fx_trading_limit, associated_treasury_contact, associated_business_units, comments, unique_identifier, is_top_level_entity, is_deleted
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, COALESCE(NULLIF($17, ''), 'Inactive'), $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29
) RETURNING entity_id`
			var newEntityID string
			err := pgxPool.QueryRow(ctx, query,
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
				entity.PanGST,
				entity.LegalEntityIdentifier,
				entity.LegalEntityType,
				entity.ReportingCurrency,
				entity.FxTradingAuthority,
				entity.InternalFxTradingLimit,
				entity.AssociatedTreasuryContact,
				entity.AssociatedBusinessUnits,
				entity.Comments,
				entity.UniqueIdentifier,
				entity.IsTopLevelEntity,
				entity.IsDeleted,
			).Scan(&newEntityID)
			if err != nil {
				inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error(), "entity_name": entity.EntityName})
				continue
			}
			entityIDs[entity.EntityName] = newEntityID
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "entity_id": newEntityID, "entity_name": entity.EntityName})
			// Insert audit action
			auditQuery := `INSERT INTO auditactionentity (
				entity_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, $2, $3, $4, $5, now())`
			if _, auditErr := pgxPool.Exec(ctx, auditQuery,
				newEntityID,
				"CREATE",
				"PENDING_APPROVAL",
				nil,
				createdBy,
			); auditErr != nil {
				inserted = append(inserted, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   "Entity created but audit log failed: " + auditErr.Error(),
					"entity_id":            newEntityID,
					"entity_name":          entity.EntityName,
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
						constants.ValueSuccess: true,
						"relationship_id":      relID,
						"parent_entity_name":   parentName,
						"child_entity_name":    childName,
					})
				} else {
					relationshipsAdded = append(relationshipsAdded, map[string]interface{}{
						constants.ValueSuccess: false,
						constants.ValueError:   relErr.Error(),
						"parent_entity_name":   parentName,
						"child_entity_name":    childName,
					})
				}
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"entities":             inserted,
			"relationshipsAdded":   len(relationshipsAdded),
			"details":              relationshipsAdded,
		})
	}
}

func GetCashEntityHierarchy(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		start := time.Now()

		// Get user's accessible entities from context
		accessibleEntityIDs := api.GetEntityIDsFromCtx(ctx)
		// Debug: log accessible IDs and a small sample
		if len(accessibleEntityIDs) == 0 {
			api.RespondWithPayload(w, true, "No entities accessible with your current permissions", []map[string]interface{}{})
			return
		}
		sampleIDs := accessibleEntityIDs
		if len(sampleIDs) > 5 {
			sampleIDs = sampleIDs[:5]
		}
		log.Printf("[DEBUG] accessible entity ids count=%d sample=%v", len(accessibleEntityIDs), sampleIDs)

		// Quick DB checks to help debug mismatches between middleware IDs and stored rows
		var matchedCount int
		countQ := `SELECT count(*) FROM masterentitycash WHERE entity_id = ANY($1)`
		if err := pgxPool.QueryRow(ctx, countQ, accessibleEntityIDs).Scan(&matchedCount); err != nil {
			log.Printf("[DEBUG] failed to run count query for accessible IDs: %v", err)
		} else {
			log.Printf("[DEBUG] masterentitycash rows matching accessible ids: %d", matchedCount)
		}
		// Also fetch up to 5 matching entity_ids to show actual stored formats
		sampleQ := `SELECT entity_id FROM masterentitycash WHERE entity_id = ANY($1) LIMIT 5`
		if srows, serr := pgxPool.Query(ctx, sampleQ, accessibleEntityIDs); serr == nil {
			defer srows.Close()
			found := []string{}
			for srows.Next() {
				var eid sql.NullString
				if err := srows.Scan(&eid); err == nil && eid.Valid {
					found = append(found, eid.String)
				}
			}
			if len(found) > 0 {
				log.Printf("[DEBUG] sample entity_ids found in DB for your accessible ids: %v", found)
			} else {
				log.Printf("[DEBUG] no sample entity_ids found in DB for the provided accessible ids")
			}
		} else {
			log.Printf("[DEBUG] sample query failed: %v", serr)
		}

		// === 1️⃣ Fetch entities with latest audit info - FILTERED by user's accessible entities
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
				m.pan_gst, m.legal_entity_identifier, m.legal_entity_type, m.reporting_currency, m.fx_trading_authority, m.internal_fx_trading_limit, m.associated_treasury_contact, m.associated_business_units, m.comments, m.unique_identifier,
				m.old_entity_name, m.old_entity_short_name, m.old_entity_level, m.old_parent_entity_name,
				m.old_entity_registration_number, m.old_country, m.old_base_operating_currency,
				m.old_tax_identification_number, m.old_address_line1, m.old_address_line2,
				m.old_city, m.old_state_province, m.old_postal_code, m.old_contact_person_name,
				m.old_contact_person_email, m.old_contact_person_phone, m.old_active_status,
				m.old_pan_gst, m.old_legal_entity_identifier, m.old_legal_entity_type, m.old_reporting_currency, m.old_fx_trading_authority, m.old_internal_fx_trading_limit, m.old_associated_treasury_contact, m.old_associated_business_units, m.old_comments, m.old_unique_identifier,
				m.is_top_level_entity, m.is_deleted,
				a.processing_status, a.requested_by, a.requested_at, a.actiontype, a.action_id,
				a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM masterentitycash m
			LEFT JOIN latest_audit a ON a.entity_id = m.entity_id
			WHERE m.entity_id = ANY($1)
			ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, entityQuery, accessibleEntityIDs)
		if err != nil {
			http.Error(w, constants.ErrQueryFailed+err.Error(), 500)
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
				id, name, shortName, parentName, regNum, country, baseCur, taxID                                                                                                                                                            sql.NullString
				panGST, legalEntityIdentifier, legalEntityType, reportingCurrency, fxTradingAuthority, internalFxTradingLimit, associatedTreasuryContact, associatedBusinessUnits, comments, uniqueIdentifier                               sql.NullString
				addr1, addr2, city, state, postal, contactName, contactEmail, contactPhone                                                                                                                                                  sql.NullString
				active                                                                                                                                                                                                                      sql.NullString
				oldName, oldShort, oldParent, oldReg, oldCountry, oldBaseCur, oldTax, oldAddr1, oldAddr2, oldCity, oldState, oldPostal, oldContactName, oldContactEmail, oldContactPhone, oldActive                                         sql.NullString
				oldPanGST, oldLegalEntityIdentifier, oldLegalEntityType, oldReportingCurrency, oldFxTradingAuthority, oldInternalFxTradingLimit, oldAssociatedTreasuryContact, oldAssociatedBusinessUnits, oldComments, oldUniqueIdentifier sql.NullString
				procStatus, reqBy, actType, actID, checkerBy, checkerComment, reason                                                                                                                                                        sql.NullString
				level, oldLevel                                                                                                                                                                                                             sql.NullInt64
				reqAt, checkerAt                                                                                                                                                                                                            sql.NullTime
				isTop, isDel                                                                                                                                                                                                                bool
			)

			if err := rows.Scan(
				&id, &name, &shortName, &level, &parentName,
				&regNum, &country, &baseCur, &taxID,
				&addr1, &addr2, &city, &state, &postal, &contactName, &contactEmail, &contactPhone, &active,
				&panGST, &legalEntityIdentifier, &legalEntityType, &reportingCurrency, &fxTradingAuthority, &internalFxTradingLimit, &associatedTreasuryContact, &associatedBusinessUnits, &comments, &uniqueIdentifier,
				&oldName, &oldShort, &oldLevel, &oldParent, &oldReg, &oldCountry,
				&oldBaseCur, &oldTax, &oldAddr1, &oldAddr2, &oldCity, &oldState,
				&oldPostal, &oldContactName, &oldContactEmail, &oldContactPhone, &oldActive,
				&oldPanGST, &oldLegalEntityIdentifier, &oldLegalEntityType, &oldReportingCurrency, &oldFxTradingAuthority, &oldInternalFxTradingLimit, &oldAssociatedTreasuryContact, &oldAssociatedBusinessUnits, &oldComments, &oldUniqueIdentifier,
				&isTop, &isDel,
				&procStatus, &reqBy, &reqAt, &actType, &actID, &checkerBy, &checkerAt, &checkerComment, &reason,
			); err != nil {
				log.Printf("[DEBUG] rows.Scan failed: %v", err)
				continue
			}

			if id.Valid {
				allIDs = append(allIDs, id.String)
			}

			data := map[string]interface{}{
				"entity_id":                       id.String,
				"entity_name":                     name.String,
				"entity_short_name":               shortName.String,
				"entity_level":                    level.Int64,
				"parent_entity_name":              parentName.String,
				"entity_registration_number":      regNum.String,
				"country":                         country.String,
				"base_operating_currency":         baseCur.String,
				"tax_identification_number":       taxID.String,
				"pan_gst":                         panGST.String,
				"legal_entity_identifier":         legalEntityIdentifier.String,
				"legal_entity_type":               legalEntityType.String,
				"reporting_currency":              reportingCurrency.String,
				"fx_trading_authority":            fxTradingAuthority.String,
				"internal_fx_trading_limit":       internalFxTradingLimit.String,
				"associated_treasury_contact":     associatedTreasuryContact.String,
				"associated_business_units":       associatedBusinessUnits.String,
				"comments":                        comments.String,
				"unique_identifier":               uniqueIdentifier.String,
				"address_line1":                   addr1.String,
				"address_line2":                   addr2.String,
				"city":                            city.String,
				"state_province":                  state.String,
				"postal_code":                     postal.String,
				"contact_person_name":             contactName.String,
				"contact_person_email":            contactEmail.String,
				"contact_person_phone":            contactPhone.String,
				"active_status":                   active.String,
				"old_entity_name":                 oldName.String,
				"old_entity_short_name":           oldShort.String,
				"old_entity_level":                oldLevel.Int64,
				"old_parent_entity_name":          oldParent.String,
				"old_entity_registration_number":  oldReg.String,
				"old_country":                     oldCountry.String,
				"old_base_operating_currency":     oldBaseCur.String,
				"old_tax_identification_number":   oldTax.String,
				"old_address_line1":               oldAddr1.String,
				"old_address_line2":               oldAddr2.String,
				"old_city":                        oldCity.String,
				"old_state_province":              oldState.String,
				"old_postal_code":                 oldPostal.String,
				"old_contact_person_name":         oldContactName.String,
				"old_contact_person_email":        oldContactEmail.String,
				"old_contact_person_phone":        oldContactPhone.String,
				"old_active_status":               oldActive.String,
				"old_pan_gst":                     oldPanGST.String,
				"old_legal_entity_identifier":     oldLegalEntityIdentifier.String,
				"old_legal_entity_type":           oldLegalEntityType.String,
				"old_reporting_currency":          oldReportingCurrency.String,
				"old_fx_trading_authority":        oldFxTradingAuthority.String,
				"old_internal_fx_trading_limit":   oldInternalFxTradingLimit.String,
				"old_associated_treasury_contact": oldAssociatedTreasuryContact.String,
				"old_associated_business_units":   oldAssociatedBusinessUnits.String,
				"old_comments":                    oldComments.String,
				"old_unique_identifier":           oldUniqueIdentifier.String,
				"is_top_level_entity":             isTop,
				"is_deleted":                      isDel,
				"processing_status":               procStatus.String,
				"action_type":                     actType.String,
				"action_id":                       actID.String,
				"checker_by":                      checkerBy.String,
				"checker_comment":                 checkerComment.String,
				"reason":                          reason.String,
				"checker_at": func() string {
					if checkerAt.Valid {
						return checkerAt.Time.Format(constants.DateTimeFormat)
					}
					return ""
				}(),
				"created_by": reqBy.String,
				"created_at": func() string {
					if reqAt.Valid {
						return reqAt.Time.Format(constants.DateTimeFormat)
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
							ts = rat.Time.Format(constants.DateTimeFormat)
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

		unmatchedRels := []string{}
		for relRows.Next() {
			var parentName, childName string
			if err := relRows.Scan(&parentName, &childName); err == nil {
				pid, cid := nameToID[parentName], nameToID[childName]
				if pid != "" && cid != "" {
					parentMap[pid] = append(parentMap[pid], cid)
				} else {
					unmatchedRels = append(unmatchedRels, fmt.Sprintf("%s -> %s (pid=%s cid=%s)", parentName, childName, pid, cid))
				}
			} else {
				log.Printf("[DEBUG] relRows.Scan failed: %v", err)
			}
		}

		if len(unmatchedRels) > 0 {
			// Log up to 20 unmatched relationship rows to help debug name mismatches
			limit := 20
			if len(unmatchedRels) < limit {
				limit = len(unmatchedRels)
			}
			log.Printf("[DEBUG] unmatched relationships count=%d sample=%v", len(unmatchedRels), unmatchedRels[:limit])
		} else {
			log.Printf("[DEBUG] no unmatched relationships found in cashentityrelationships")
		}

		// Fallback: if there are entities with parent_entity_name set but
		// the relationship row is missing from `cashentityrelationships`,
		// attach them using the scanned parent name. This helps with
		// migrated data where parent_entity_name exists on the entity row
		// but the relationships table wasn't populated.
		// Fallback: attach using parent_entity_name when relationships row missing
		fallbackAdded := 0
		for _, e := range entityMap {
			pname := ""
			if v, ok := e.Data["parent_entity_name"]; ok {
				pname = strings.TrimSpace(fmt.Sprint(v))
			}
			if pname == "" {
				continue
			}
			pid := nameToID[pname]
			if pid == "" {
				// parent name not found among scanned entities
				continue
			}
			// ensure we don't duplicate the child in the parent's slice
			already := false
			for _, c := range parentMap[pid] {
				if c == e.ID {
					already = true
					break
				}
			}
			if !already {
				parentMap[pid] = append(parentMap[pid], e.ID)
				fallbackAdded++
			}
		}

		log.Printf("[DEBUG] parentMap size=%d nameToID size=%d fallbackAdded=%d", len(parentMap), len(nameToID), fallbackAdded)
		// Log up to 10 parent entries to inspect mapping (parentID -> childrenIDs)
		cnt := 0
		for pid, children := range parentMap {
			if cnt < 10 {
				log.Printf("[DEBUG] parentID=%s children=%v", pid, children)
			}
			cnt++
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
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
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrInvalidJSONShort})
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
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrInvalidSessionCapitalized})
			return
		}

		reqCtx := r.Context()
		var results []map[string]interface{}
		relationshipsAdded := []map[string]interface{}{}
		for _, entity := range req.Entities {
			if entity.EntityID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Missing entity_id"})
				continue
			}

			// Validate currency if being updated
			if val, ok := entity.Fields["base_operating_currency"]; ok {
				if valStr := fmt.Sprint(val); valStr != "" && !api.IsCurrencyAllowed(reqCtx, valStr) {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized base_operating_currency: " + valStr, "entity_id": entity.EntityID})
					continue
				}
			}

			// Validate reporting currency if being updated
			if val, ok := entity.Fields["reporting_currency"]; ok {
				if valStr := fmt.Sprint(val); valStr != "" && !api.IsCurrencyAllowed(reqCtx, valStr) {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid or unauthorized reporting_currency: " + valStr, "entity_id": entity.EntityID})
					continue
				}
			}

			// Validate parent if being updated
			if val, ok := entity.Fields["parent_entity_name"]; ok {
				parentName := strings.TrimSpace(fmt.Sprint(val))
				if parentName != "" {
					var parentExists bool
					parentCheckQ := `SELECT EXISTS(
						SELECT 1 FROM masterentitycash m
						LEFT JOIN LATERAL (
							SELECT processing_status FROM auditactionentity
							WHERE entity_id = m.entity_id
							ORDER BY requested_at DESC LIMIT 1
						) a ON TRUE
						WHERE m.entity_name = $1
						AND UPPER(a.processing_status) = 'APPROVED'
						AND UPPER(m.active_status) = 'ACTIVE'
					)`
					if err := pgxPool.QueryRow(reqCtx, parentCheckQ, parentName).Scan(&parentExists); err != nil || !parentExists {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "parent entity not found or not approved: " + parentName, "entity_id": entity.EntityID})
						continue
					}
				}
			}

			ctx := r.Context()
			tx, txErr := pgxPool.Begin(ctx)
			if txErr != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxStartFailed + txErr.Error(), "entity_id": entity.EntityID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
					if p := recover(); p != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "panic: " + fmt.Sprint(p), "entity_id": entity.EntityID})
					}
				}()

				// fetch existing values for old_* (use pointer types instead of sql.Null*)
				var exEntityName, exEntityShortName, exParentEntityName, exEntityRegistrationNumber, exCountry, exBaseCurrency, exTaxID, exAddress1, exAddress2, exCity, exState, exPostal, exContactName, exContactEmail, exContactPhone, exActiveStatus *string
				var exEntityLevel *int64
				var exIsTopLevel, exIsDeleted *bool
				var exPanGST, exLegalEntityIdentifier, exLegalEntityType, exReportingCurrency, exFxTradingAuthority, exInternalFxTradingLimit, exAssociatedTreasuryContact, exAssociatedBusinessUnits, exComments, exUniqueIdentifier *string
				sel := `SELECT entity_name, entity_short_name, entity_level, parent_entity_name, entity_registration_number, country, base_operating_currency, tax_identification_number, address_line1, address_line2, city, state_province, postal_code, contact_person_name, contact_person_email, contact_person_phone, active_status, is_top_level_entity, is_deleted, pan_gst, legal_entity_identifier, legal_entity_type, reporting_currency, fx_trading_authority, internal_fx_trading_limit, associated_treasury_contact, associated_business_units, comments, unique_identifier FROM masterentitycash WHERE entity_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, entity.EntityID).Scan(&exEntityName, &exEntityShortName, &exEntityLevel, &exParentEntityName, &exEntityRegistrationNumber, &exCountry, &exBaseCurrency, &exTaxID, &exAddress1, &exAddress2, &exCity, &exState, &exPostal, &exContactName, &exContactEmail, &exContactPhone, &exActiveStatus, &exIsTopLevel, &exIsDeleted, &exPanGST, &exLegalEntityIdentifier, &exLegalEntityType, &exReportingCurrency, &exFxTradingAuthority, &exInternalFxTradingLimit, &exAssociatedTreasuryContact, &exAssociatedBusinessUnits, &exComments, &exUniqueIdentifier); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Failed to fetch existing entity: " + err.Error(), "entity_id": entity.EntityID})
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
					case "pan_gst":
						sets = append(sets, fmt.Sprintf("pan_gst=$%d, old_pan_gst=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exPanGST != nil {
								return *exPanGST
							}
							return ""
						}())
						pos += 2
					case "legal_entity_identifier":
						sets = append(sets, fmt.Sprintf("legal_entity_identifier=$%d, old_legal_entity_identifier=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exLegalEntityIdentifier != nil {
								return *exLegalEntityIdentifier
							}
							return ""
						}())
						pos += 2
					case "legal_entity_type":
						sets = append(sets, fmt.Sprintf("legal_entity_type=$%d, old_legal_entity_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exLegalEntityType != nil {
								return *exLegalEntityType
							}
							return ""
						}())
						pos += 2
					case "reporting_currency":
						sets = append(sets, fmt.Sprintf("reporting_currency=$%d, old_reporting_currency=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exReportingCurrency != nil {
								return *exReportingCurrency
							}
							return ""
						}())
						pos += 2
					case "fx_trading_authority":
						sets = append(sets, fmt.Sprintf("fx_trading_authority=$%d, old_fx_trading_authority=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exFxTradingAuthority != nil {
								return *exFxTradingAuthority
							}
							return ""
						}())
						pos += 2
					case "internal_fx_trading_limit":
						sets = append(sets, fmt.Sprintf("internal_fx_trading_limit=$%d, old_internal_fx_trading_limit=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exInternalFxTradingLimit != nil {
								return *exInternalFxTradingLimit
							}
							return ""
						}())
						pos += 2
					case "associated_treasury_contact":
						sets = append(sets, fmt.Sprintf("associated_treasury_contact=$%d, old_associated_treasury_contact=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exAssociatedTreasuryContact != nil {
								return *exAssociatedTreasuryContact
							}
							return ""
						}())
						pos += 2
					case "associated_business_units":
						sets = append(sets, fmt.Sprintf("associated_business_units=$%d, old_associated_business_units=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exAssociatedBusinessUnits != nil {
								return *exAssociatedBusinessUnits
							}
							return ""
						}())
						pos += 2
					case "comments":
						sets = append(sets, fmt.Sprintf("comments=$%d, old_comments=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exComments != nil {
								return *exComments
							}
							return ""
						}())
						pos += 2
					case "unique_identifier":
						sets = append(sets, fmt.Sprintf("unique_identifier=$%d, old_unique_identifier=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exUniqueIdentifier != nil {
								return *exUniqueIdentifier
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
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error(), "entity_id": entity.EntityID})
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
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Entity updated but audit log failed: " + err.Error(), "entity_id": updatedEntityID})
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
								relationshipsAdded = append(relationshipsAdded, map[string]interface{}{constants.ValueSuccess: true, "relationship_id": relID, "parent_entity_name": parentId, "child_entity_name": updatedEntityID})
							}
						}
					}
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Transaction commit failed: " + err.Error(), "entity_id": updatedEntityID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "entity_id": updatedEntityID})
			}()
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"results":              results,
			"relationshipsAdded":   len(relationshipsAdded),
			"details":              relationshipsAdded,
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		// Fetch all relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_name, child_entity_name FROM cashentityrelationships`)
		if err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to fetch entity relationships")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to mark entities for deletion")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		success := len(auditErrors) == 0 && len(updated) > 0
		resp := map[string]interface{}{
			constants.ValueSuccess: success,
			"updated":              updated,
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		ctx := r.Context()
		// Fetch all relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_name, child_entity_name FROM cashentityrelationships`)
		if err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to fetch entity relationships for bulk reject")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to reject entity actions")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		success := len(updated) > 0
		resp := map[string]interface{}{
			constants.ValueSuccess: success,
			"updated":              updated,
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		ctx := r.Context()
		// Fetch all relationships
		relRows, err := pgxPool.Query(ctx, `SELECT parent_entity_name, child_entity_name FROM cashentityrelationships`)
		if err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to fetch entity relationships for bulk approve")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
					errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to mark entities as deleted")
					if statusCode == http.StatusOK {
						w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
						json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
					} else {
						api.RespondWithError(w, statusCode, errMsg)
					}
					return
				}
				defer rows.Close()
				for rows.Next() {
					var entityID string
					if err := rows.Scan(&entityID); err == nil {
						allUpdated = append(allUpdated, map[string]interface{}{
							"entity_id":         entityID,
							constants.KeyStatus: "Marked Deleted",
						})
					}
				}

				// Also update the audit rows for these descendants: mark their delete actions as APPROVED
				auditRows, aerr := pgxPool.Query(ctx, `UPDATE auditactionentity SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE entity_id = ANY($3) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, entity_id`, checkerBy, req.Comment, descendants)
				if aerr != nil {
					errMsg, statusCode := getUserFriendlyEntityCashError(aerr, "Failed to approve delete actions")
					if statusCode == http.StatusOK {
						w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
						json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
					} else {
						api.RespondWithError(w, statusCode, errMsg)
					}
					return
				}
				defer auditRows.Close()
				for auditRows.Next() {
					var actionID, entityID string
					if err := auditRows.Scan(&actionID, &entityID); err == nil {
						allUpdated = append(allUpdated, map[string]interface{}{
							"entity_id":         entityID,
							"action_id":         actionID,
							constants.KeyStatus: "Delete Approved",
						})
					}
				}
			} else {
				// Approve only this entity: update auditactionentity processing_status
				rows, err := pgxPool.Query(ctx, `UPDATE auditactionentity SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE entity_id = $3 AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id, entity_id`, checkerBy, req.Comment, eid)
				if err != nil {
					errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to approve entity action")
					if statusCode == http.StatusOK {
						w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
						json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
					} else {
						api.RespondWithError(w, statusCode, errMsg)
					}
					return
				}
				defer rows.Close()
				for rows.Next() {
					var actionID, entityID string
					if err := rows.Scan(&actionID, &entityID); err == nil {
						allUpdated = append(allUpdated, map[string]interface{}{
							"entity_id":         entityID,
							"action_id":         actionID,
							constants.KeyStatus: "Approved",
						})
					}
				}
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		success := len(allUpdated) > 0 && anyError == nil
		resp := map[string]interface{}{
			constants.ValueSuccess: success,
			"updated":              allUpdated,
		}
		if anyError != nil {
			resp[constants.ValueError] = anyError.Error()
		}
		if !success {
			resp["message"] = "No entities found to approve"
		}
		json.NewEncoder(w).Encode(resp)
	}
}

func FindParentCashEntityAtLevel(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)

		// Parse request
		var req struct {
			UserID string `json:"user_id"`
			Level  int    `json:"level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{
				constants.ValueSuccess: false,
				constants.ValueError:   "Missing or invalid user_id/level",
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
				constants.ValueSuccess: false,
				constants.ValueError:   constants.ErrInvalidSessionCapitalized,
			})
			return
		}
		ctx := r.Context()

		// Get user's accessible entities from context
		accessibleEntityIDs := api.GetEntityIDsFromCtx(ctx)
		if len(accessibleEntityIDs) == 0 {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				constants.ValueSuccess: true,
				"results":              []map[string]interface{}{},
			})
			return
		}

		parentLevel := req.Level - 1
		// Only return entities at the requested level that user has access to, are approved and active
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
			  AND m.entity_id = ANY($2)
			  AND (m.is_deleted = false OR m.is_deleted IS NULL)
			  AND LOWER(m.active_status) = 'active'
			  AND COALESCE(LOWER(a.processing_status),'') = 'approved'
			ORDER BY m.entity_name
		`

		rows, err := pgxPool.Query(ctx, query, parentLevel, accessibleEntityIDs)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{
				constants.ValueSuccess: false,
				constants.ValueError:   err.Error(),
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
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Error during result iteration")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				w.WriteHeader(statusCode)
				json.NewEncoder(w).Encode(map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   errMsg,
				})
			}
			return
		}

		// Success response
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"results":              results,
		})
	}
}

// GET handler to fetch all entity_id, entity_name, entity_short_name for user's accessible cash entities
func GetCashEntityNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		ctx := r.Context()

		// Get user's accessible entities from context (hierarchy-based filtering)
		accessibleEntityIDs := api.GetEntityIDsFromCtx(ctx)
		if len(accessibleEntityIDs) == 0 {
			api.RespondWithPayload(w, true, "No entities accessible with your current permissions", []map[string]interface{}{})
			return
		}

		// Only return entities user has access to (from their hierarchy)
		query := `
						SELECT m.entity_id, m.entity_name, m.entity_short_name, m.unique_identifier
						FROM masterentitycash m
						WHERE m.entity_id = ANY($1)
							AND m.active_status = 'Active' 
							AND (m.is_deleted = false OR m.is_deleted IS NULL)
						ORDER BY m.entity_name
				`
		rows, err := pgxPool.Query(ctx, query, accessibleEntityIDs)
		if err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to fetch entity names")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		var anyError error
		for rows.Next() {
			var entityID, entityName string
			var entityShortName sql.NullString
			var uniqueIdentifier sql.NullString
			if err := rows.Scan(&entityID, &entityName, &entityShortName, &uniqueIdentifier); err != nil {
				anyError = err
				break
			}
			shortName := ""
			if entityShortName.Valid {
				shortName = entityShortName.String
			}
			uid := ""
			if uniqueIdentifier.Valid {
				uid = uniqueIdentifier.String
			}
			results = append(results, map[string]interface{}{
				"entity_id":         entityID,
				"entity_name":       entityName,
				"entity_short_name": shortName,
				"unique_identifier": uid,
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		if anyError != nil {
			api.RespondWithError(w, http.StatusInternalServerError, anyError.Error())
			return
		}
		if results == nil {
			results = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"results":              results,
		})
	}
}

// UploadEntityCash handles staging and mapping for masterentitycash using pgxpool (no database/sql usage)
func UploadEntityCash(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get(constants.ContentTypeText) == constants.ContentTypeJSON {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in body")
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue(constants.KeyUserID)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseMultipartForm)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
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
				errMsg, statusCode := getUserFriendlyEntityCashError(err, constants.ErrTxStartFailed)
				if statusCode == http.StatusOK {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
				} else {
					api.RespondWithError(w, statusCode, errMsg)
				}
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()

			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"input_entitycash"}, columns, pgx.CopyFromRows(copyRows)); err != nil {
				errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to stage data")
				if statusCode == http.StatusOK {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
				} else {
					api.RespondWithError(w, statusCode, errMsg)
				}
				return
			}

			// read mapping from upload_mapping_entity
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_entity`)
			if err != nil {
				errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to fetch column mapping")
				if statusCode == http.StatusOK {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
				} else {
					api.RespondWithError(w, statusCode, errMsg)
				}
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
					// Prevent allowing a CSV to override DB-generated entity_id
					if strings.ToLower(t) == "entity_id" {
						// skip entity_id mapping so DB default generates it
						continue
					}
					srcCols = append(srcCols, key)
					tgtCols = append(tgtCols, t)
				} else {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf(constants.ErrNoMappingForSourceColumn, h))
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
				errMsg, statusCode := getUserFriendlyEntityCashError(err, "Final insert error")
				if statusCode == http.StatusOK {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
				} else {
					api.RespondWithError(w, statusCode, errMsg)
				}
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
					errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to insert audit actions")
					if statusCode == http.StatusOK {
						w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
						json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
					} else {
						api.RespondWithError(w, statusCode, errMsg)
					}
					return
				}
			}

			// Sync parent-child relationships for newly created entities (if parent_entity_name set)
			if len(newIDs) > 0 {
				relRows2, err := tx.Query(ctx, `SELECT entity_id, parent_entity_name FROM masterentitycash WHERE entity_id = ANY($1)`, newIDs)
				if err != nil {
					errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to fetch parent info")
					if statusCode == http.StatusOK {
						w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
						json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
					} else {
						api.RespondWithError(w, statusCode, errMsg)
					}
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
							relationshipsAdded = append(relationshipsAdded, map[string]interface{}{constants.ValueSuccess: true, "relationship_id": relID, "parent_entity_name": parent, "child_entity_name": eid})
						}
					}
				}
				relRows2.Close()
			}

			if err := tx.Commit(ctx); err != nil {
				errMsg, statusCode := getUserFriendlyEntityCashError(err, constants.ErrCommitFailedCapitalized)
				if statusCode == http.StatusOK {
					w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
				} else {
					api.RespondWithError(w, statusCode, errMsg)
				}
				return
			}
			committed = true
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "batch_ids": batchIDs, "relationships_added": len(relationshipsAdded), "relationship_details": relationshipsAdded})
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
		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			var body struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			userID = body.UserID
		}
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
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
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		// 2) parse multipart & file (supports CSV and XLSX via shared parser)
		if err := r.ParseMultipartForm(128 << 20); err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to parse multipart form")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		f, fh, err := r.FormFile("file")
		if err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, constants.ErrFileUploadFailed)
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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

		// Get user's accessible entities and currencies from context
		ctx = r.Context()
		currCodes := api.GetCurrencyCodesFromCtx(ctx)
		accessibleEntityNames := api.GetEntityNamesFromCtx(ctx)

		// allowed columns mapping (extend if needed)
		allowed := map[string]bool{
			"entity_name": true, "entity_short_name": true, "entity_level": true,
			"parent_entity_name": true, "entity_registration_number": true,
			"country": true, "base_operating_currency": true, "tax_identification_number": true,
			"address_line1": true, "address_line2": true, "city": true, "state_province": true, "postal_code": true,
			"contact_person_name": true, "contact_person_email": true, "contact_person_phone": true,
			"active_status": true, "is_top_level_entity": true,
			// newly supported columns
			"pan_gst": true, "legal_entity_identifier": true, "legal_entity_type": true,
			"reporting_currency": true, "fx_trading_authority": true, "internal_fx_trading_limit": true,
			"associated_treasury_contact": true, "associated_business_units": true, "comments": true, "unique_identifier": true,
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, constants.ErrTxStartFailed)
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Failed to create temp table")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}

		// Build copyRows in memory (simpler and supports XLSX)
		copyRows := make([][]interface{}, 0, len(dataRows))
		skipped := 0
		for rowIdx, rec := range dataRows {
			// Validate currency if present
			currencyCol := headerPos["base_operating_currency"]
			if currencyCol < len(rec) {
				currency := strings.TrimSpace(rec[currencyCol])
				if currency != "" && len(currCodes) > 0 && !api.IsCurrencyAllowed(ctx, currency) {
					log.Printf("[UploadEntitySimple] Row %d skipped: invalid currency %s", rowIdx+2, currency)
					skipped++
					continue
				}
			}

			// Validate parent entity if present
			parentCol := headerPos["parent_entity_name"]
			if parentCol < len(rec) {
				parentName := strings.TrimSpace(rec[parentCol])
				if parentName != "" && len(accessibleEntityNames) > 0 {
					parentAllowed := false
					for _, name := range accessibleEntityNames {
						if strings.EqualFold(strings.TrimSpace(name), parentName) {
							parentAllowed = true
							break
						}
					}
					if !parentAllowed {
						log.Printf("[UploadEntitySimple] Row %d skipped: parent entity not in accessible hierarchy: %s", rowIdx+2, parentName)
						skipped++
						continue
					}
				}
			}

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
		tStart := time.Now()
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_me"}, copyCols, pgx.CopyFromRows(copyRows)); err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "COPY failed")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
	active_status, is_top_level_entity, is_deleted, parent_entity_name, unique_identifier
)
SELECT
	t.entity_name, t.entity_short_name, t.entity_level, t.entity_registration_number,
	t.country, t.base_operating_currency, t.tax_identification_number,
	t.address_line1, t.address_line2, t.city, t.state_province, t.postal_code,
	t.contact_person_name, t.contact_person_email, t.contact_person_phone,
	COALESCE(t.active_status, 'Inactive'),
	COALESCE(t.is_top_level_entity, false),
	false,
	t.parent_entity_name, t.unique_identifier
FROM tmp_me t
LEFT JOIN masterentitycash m ON m.entity_name = t.entity_name
WHERE m.entity_name IS NULL;
`); err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Insert failed")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, constants.ErrUpdateFailed)
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Hierarchy sync failed")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, "Relationships sync failed")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
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
			errMsg, statusCode := getUserFriendlyEntityCashError(err, constants.ErrAuditInsertFailed)
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		log.Printf("[UploadEntitySimple] audit elapsed=%v", time.Since(t5))

		// commit
		if err := tx.Commit(ctx); err != nil {
			errMsg, statusCode := getUserFriendlyEntityCashError(err, constants.ErrCommitFailedCapitalized)
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		tx = nil

		totalDur := time.Since(startOverall)
		resp := map[string]interface{}{
			constants.ValueSuccess: true,
			"file":                 fh.Filename,
			"rows":                 rowsCopied,
			"skipped":              skipped,
			"duration_ms":          totalDur.Milliseconds(),
			"batch_id":             uuid.New().String(),
		}
		log.Printf("[UploadEntitySimple] finished rows=%d skipped=%d total_ms=%d", rowsCopied, skipped, totalDur.Milliseconds())

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}
