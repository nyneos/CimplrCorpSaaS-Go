package allMaster

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/lib/pq"
)

type BankMasterRequest struct {
	BankName              string `json:"bank_name"`
	BankShortName         string `json:"bank_short_name"`
	SwiftBicCode          string `json:"swift_bic_code"`
	CountryOfHeadquarters string `json:"country_of_headquarters"`
	ConnectivityType      string `json:"connectivity_type"`
	ActiveStatus          string `json:"active_status"`
	ContactPersonName     string `json:"contact_person_name"`
	ContactPersonEmail    string `json:"contact_person_email"`
	ContactPersonPhone    string `json:"contact_person_phone"`
	AddressLine1          string `json:"address_line1"`
	AddressLine2          string `json:"address_line2"`
	City                  string `json:"city"`
	StateProvince         string `json:"state_province"`
	PostalCode            string `json:"postal_code"`
	UserID                string `json:"user_id"`
}

func CreateBankMaster(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BankMasterRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		userID := req.UserID
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing user_id in body")
			return
		}
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "User session not found or email missing")
			return
		}
		// Basic validation (add more as needed)
		if req.BankName == "" || req.CountryOfHeadquarters == "" || req.ConnectivityType == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required bank details")
			return
		}
		tx, txErr := db.Begin()
		if txErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction: "+txErr.Error())
			return
		}
		var bankID string
		query := `INSERT INTO masterbank (
			bank_name, bank_short_name, swift_bic_code, country_of_headquarters, connectivity_type, active_status,
			contact_person_name, contact_person_email, contact_person_phone, address_line1, address_line2, city,
			state_province, postal_code
		) VALUES (
			$1, $2, $3, $4, $5, COALESCE(NULLIF($6, ''), 'Inactive'),
			$7, $8, $9, $10, $11, $12, $13, $14
		) RETURNING bank_id`
		err := tx.QueryRow(query,
			req.BankName,
			req.BankShortName,
			req.SwiftBicCode,
			req.CountryOfHeadquarters,
			req.ConnectivityType,
			req.ActiveStatus,
			req.ContactPersonName,
			req.ContactPersonEmail,
			req.ContactPersonPhone,
			req.AddressLine1,
			req.AddressLine2,
			req.City,
			req.StateProvince,
			req.PostalCode,
		).Scan(&bankID)
		if err != nil {
			tx.Rollback()
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		auditQuery := `INSERT INTO auditactionbank (
			bank_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, $2, $3, $4, $5, now())`
		_, auditErr := tx.Exec(auditQuery,
			bankID,
			"CREATE",
			"PENDING_APPROVAL",
			nil,
			createdBy,
		)
		if auditErr != nil {
			tx.Rollback()
			api.RespondWithError(w, http.StatusInternalServerError, "Bank created but audit log failed: "+auditErr.Error())
			return
		}
		if commitErr := tx.Commit(); commitErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction commit failed: "+commitErr.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"bank_id": bankID,
		})
	}
}

func GetAllBankMaster(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := `
			SELECT m.bank_id, m.bank_name, m.bank_short_name, m.swift_bic_code, m.country_of_headquarters, m.connectivity_type, m.active_status,
				   m.contact_person_name, m.contact_person_email, m.contact_person_phone, m.address_line1, m.address_line2, m.city,
				   m.state_province, m.postal_code,
				   m.old_bank_name, m.old_bank_short_name, m.old_swift_bic_code, m.old_country_of_headquarters, m.old_connectivity_type, m.old_active_status,
				   m.old_contact_person_name, m.old_contact_person_email, m.old_contact_person_phone, m.old_address_line1, m.old_address_line2, m.old_city,
				   m.old_state_province, m.old_postal_code
			FROM masterbank m
		`

		rows, err := db.Query(query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var banks []map[string]interface{}
		var anyError error
		for rows.Next() {
			var (
				bankID, bankName, countryOfHQ, connectivityType, activeStatus string
				bankShortName, swiftBicCode                                   sql.NullString
				contactPersonName, contactPersonEmail, contactPersonPhone     sql.NullString
				addressLine1, addressLine2, city, stateProvince, postalCode   sql.NullString
				oldBankName, oldBankShortName, oldSwiftBicCode                sql.NullString
				oldCountryOfHQ, oldConnectivityType, oldActiveStatus          sql.NullString
				oldContactPersonName, oldContactPersonEmail                   sql.NullString
				oldContactPersonPhone, oldAddressLine1, oldAddressLine2       sql.NullString
				oldCity, oldStateProvince, oldPostalCode                      sql.NullString
			)

			if err := rows.Scan(
				&bankID, &bankName, &bankShortName, &swiftBicCode, &countryOfHQ, &connectivityType, &activeStatus,
				&contactPersonName, &contactPersonEmail, &contactPersonPhone, &addressLine1, &addressLine2, &city, &stateProvince, &postalCode,
				&oldBankName, &oldBankShortName, &oldSwiftBicCode, &oldCountryOfHQ, &oldConnectivityType, &oldActiveStatus,
				&oldContactPersonName, &oldContactPersonEmail, &oldContactPersonPhone, &oldAddressLine1, &oldAddressLine2, &oldCity, &oldStateProvince, &oldPostalCode,
			); err != nil {
				anyError = err
				break
			}

			// ...existing code...
			auditQuery := `SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason 
						   FROM auditactionbank WHERE bank_id = $1 ORDER BY requested_at DESC LIMIT 1`
			var processingStatus, requestedBy, actionType, actionID, checkerBy, checkerComment, reason sql.NullString
			var requestedAt, checkerAt sql.NullTime
			_ = db.QueryRow(auditQuery, bankID).Scan(&processingStatus, &requestedBy, &requestedAt, &actionType, &actionID, &checkerBy, &checkerAt, &checkerComment, &reason)

			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM auditactionbank 
								  WHERE bank_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') 
								  ORDER BY requested_at DESC`
			auditRows, auditErr := db.Query(auditDetailsQuery, bankID)
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
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

			banks = append(banks, map[string]interface{}{
				"bank_id":                     bankID,
				"bank_name":                   bankName,
				"bank_short_name":             getNullString(bankShortName),
				"swift_bic_code":              getNullString(swiftBicCode),
				"country_of_headquarters":     countryOfHQ,
				"connectivity_type":           connectivityType,
				"active_status":               activeStatus,
				"contact_person_name":         getNullString(contactPersonName),
				"contact_person_email":        getNullString(contactPersonEmail),
				"contact_person_phone":        getNullString(contactPersonPhone),
				"address_line1":               getNullString(addressLine1),
				"address_line2":               getNullString(addressLine2),
				"city":                        getNullString(city),
				"state_province":              getNullString(stateProvince),
				"postal_code":                 getNullString(postalCode),
				"old_bank_name":               getNullString(oldBankName),
				"old_bank_short_name":         getNullString(oldBankShortName),
				"old_swift_bic_code":          getNullString(oldSwiftBicCode),
				"old_country_of_headquarters": getNullString(oldCountryOfHQ),
				"old_connectivity_type":       getNullString(oldConnectivityType),
				"old_active_status":           getNullString(oldActiveStatus),
				"old_contact_person_name":     getNullString(oldContactPersonName),
				"old_contact_person_email":    getNullString(oldContactPersonEmail),
				"old_contact_person_phone":    getNullString(oldContactPersonPhone),
				"old_address_line1":           getNullString(oldAddressLine1),
				"old_address_line2":           getNullString(oldAddressLine2),
				"old_city":                    getNullString(oldCity),
				"old_state_province":          getNullString(oldStateProvince),
				"old_postal_code":             getNullString(oldPostalCode),
				"processing_status":           getNullString(processingStatus),
				"action_type":                 getNullString(actionType),
				"action_id":                   getNullString(actionID),
				"checker_at":                  getNullTime(checkerAt),
				"checker_by":                  getNullString(checkerBy),
				"checker_comment":             getNullString(checkerComment),
				"reason":                      getNullString(reason),
				"created_by":                  createdBy,
				"created_at":                  createdAt,
				"edited_by":                   editedBy,
				"edited_at":                   editedAt,
				"deleted_by":                  deletedBy,
				"deleted_at":                  deletedAt,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		if anyError != nil {
			api.RespondWithError(w, http.StatusInternalServerError, anyError.Error())
			return
		}
		if banks == nil {
			banks = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    banks,
		})
	}
}

// GET handler to fetch all bank_id, bank_name (bank_short_name) for all banks, requiring user_id in body
func GetBankNamesWithID(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		query := `
			SELECT m.bank_id, m.bank_name, m.bank_short_name
			FROM masterbank m
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionbank a
				WHERE a.bank_id = m.bank_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.active_status = 'Active' AND a.processing_status = 'APPROVED'
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
			var bankID, bankName, bankShortName string
			if err := rows.Scan(&bankID, &bankName, &bankShortName); err != nil {
				anyError = err
				break
			}
			results = append(results, map[string]interface{}{
				"bank_id":         bankID,
				"bank_name":       bankName,
				"bank_short_name": getNullString(sql.NullString{String: bankShortName, Valid: bankShortName != ""}),
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

// Bulk update handler for bank master
func UpdateBankMasterBulk(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Banks  []struct {
				BankID string                 `json:"bank_id"`
				Fields map[string]interface{} `json:"fields"`
			} `json:"banks"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		userID := req.UserID
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				updatedBy = s.Email
				break
			}
		}
		if updatedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		var results []map[string]interface{}
		for _, bank := range req.Banks {
			tx, txErr := db.Begin()
			if txErr != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Failed to start transaction: " + txErr.Error(), "bank_id": bank.BankID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback()
					}
					if p := recover(); p != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "panic: " + fmt.Sprint(p), "bank_id": bank.BankID})
					}
				}()

				// fetch existing values
				var exBankName, exBankShortName, exSwift, exCountry, exConnectivity, exActive sql.NullString
				var exContactName, exContactEmail, exContactPhone sql.NullString
				var exAddr1, exAddr2, exCity, exState, exPostal sql.NullString
				sel := `SELECT bank_name, bank_short_name, swift_bic_code, country_of_headquarters, connectivity_type, active_status, contact_person_name, contact_person_email, contact_person_phone, address_line1, address_line2, city, state_province, postal_code FROM masterbank WHERE bank_id=$1 FOR UPDATE`
				if err := tx.QueryRow(sel, bank.BankID).Scan(&exBankName, &exBankShortName, &exSwift, &exCountry, &exConnectivity, &exActive, &exContactName, &exContactEmail, &exContactPhone, &exAddr1, &exAddr2, &exCity, &exState, &exPostal); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Failed to fetch existing bank: " + err.Error(), "bank_id": bank.BankID})
					return
				}

				// build dynamic update
				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range bank.Fields {
					switch k {
					case "bank_name":
						sets = append(sets, fmt.Sprintf("bank_name=$%d, old_bank_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exBankName.String)
						pos += 2
					case "bank_short_name":
						sets = append(sets, fmt.Sprintf("bank_short_name=$%d, old_bank_short_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exBankShortName.String)
						pos += 2
					case "swift_bic_code":
						sets = append(sets, fmt.Sprintf("swift_bic_code=$%d, old_swift_bic_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exSwift.String)
						pos += 2
					case "country_of_headquarters":
						sets = append(sets, fmt.Sprintf("country_of_headquarters=$%d, old_country_of_headquarters=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exCountry.String)
						pos += 2
					case "connectivity_type":
						sets = append(sets, fmt.Sprintf("connectivity_type=$%d, old_connectivity_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exConnectivity.String)
						pos += 2
					case "active_status":
						sets = append(sets, fmt.Sprintf("active_status=$%d, old_active_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exActive.String)
						pos += 2
					case "contact_person_name":
						sets = append(sets, fmt.Sprintf("contact_person_name=$%d, old_contact_person_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exContactName.String)
						pos += 2
					case "contact_person_email":
						sets = append(sets, fmt.Sprintf("contact_person_email=$%d, old_contact_person_email=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exContactEmail.String)
						pos += 2
					case "contact_person_phone":
						sets = append(sets, fmt.Sprintf("contact_person_phone=$%d, old_contact_person_phone=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exContactPhone.String)
						pos += 2
					case "address_line1":
						sets = append(sets, fmt.Sprintf("address_line1=$%d, old_address_line1=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exAddr1.String)
						pos += 2
					case "address_line2":
						sets = append(sets, fmt.Sprintf("address_line2=$%d, old_address_line2=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exAddr2.String)
						pos += 2
					case "city":
						sets = append(sets, fmt.Sprintf("city=$%d, old_city=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exCity.String)
						pos += 2
					case "state_province":
						sets = append(sets, fmt.Sprintf("state_province=$%d, old_state_province=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exState.String)
						pos += 2
					case "postal_code":
						sets = append(sets, fmt.Sprintf("postal_code=$%d, old_postal_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exPostal.String)
						pos += 2
					default:
						// ignore unknown
					}
				}

				var updatedBankID string
				if len(sets) > 0 {
					q := "UPDATE masterbank SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE bank_id=$%d RETURNING bank_id", pos)
					args = append(args, bank.BankID)
					if err := tx.QueryRow(q, args...).Scan(&updatedBankID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": err.Error(), "bank_id": bank.BankID})
						return
					}
				} else {
					updatedBankID = bank.BankID
				}

				// audit
				auditQuery := `INSERT INTO auditactionbank (
					bank_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, $2, $3, $4, $5, now())`
				if _, err := tx.Exec(auditQuery, updatedBankID, "EDIT", "PENDING_EDIT_APPROVAL", nil, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Bank updated but audit log failed: " + err.Error(), "bank_id": updatedBankID})
					return
				}

				if err := tx.Commit(); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Transaction commit failed: " + err.Error(), "bank_id": updatedBankID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "bank_id": updatedBankID})
			}()
		}
		w.Header().Set("Content-Type", "application/json")
		finalSuccess := api.IsBulkSuccess(results)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": finalSuccess,
			"results": results,
		})
	}
}

// Bulk delete handler for bank master audit actions
func BulkDeleteBankAudit(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			BankIDs []string `json:"bank_ids"`
			Reason  string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BankIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		sessions := auth.GetActiveSessions()
		requestedBy := ""
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
		var results []string
		for _, bankID := range req.BankIDs {
			query := `INSERT INTO auditactionbank (
				bank_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now()) RETURNING action_id`
			var actionID string
			err := db.QueryRow(query, bankID, req.Reason, requestedBy).Scan(&actionID)
			if err == nil {
				results = append(results, actionID)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"created": results,
		})
	}
}

// Bulk reject audit actions for bank master
func BulkRejectBankAuditActions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ActionIDs []string `json:"action_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ActionIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		sessions := auth.GetActiveSessions()
		checkerBy := ""
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
		query := `UPDATE auditactionbank SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id,bank_id`
		rows, err := db.Query(query, checkerBy, req.Comment, pq.Array(req.ActionIDs))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, bankID string
			rows.Scan(&id, &bankID)
			updated = append(updated, id, bankID)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"updated": updated,
		})
	}
}

// Bulk approve audit actions for bank master
func BulkApproveBankAuditActions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ActionIDs []string `json:"action_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ActionIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		sessions := auth.GetActiveSessions()
		checkerBy := ""
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
		// First, delete records with processing_status = 'PENDING_DELETE_APPROVAL' for the given action_ids
		delQuery := `DELETE FROM auditactionbank WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, bank_id`
		delRows, delErr := db.Query(delQuery, pq.Array(req.ActionIDs))
		var deleted []string
		var bankIDsToDelete []string
		if delErr == nil {
			defer delRows.Close()
			for delRows.Next() {
				var id, bankID string
				delRows.Scan(&id, &bankID)
				deleted = append(deleted, id, bankID)
				bankIDsToDelete = append(bankIDsToDelete, bankID)
			}
		}
		// Delete corresponding banks from masterbank
		if len(bankIDsToDelete) > 0 {
			delBankQuery := `DELETE FROM masterbank WHERE bank_id = ANY($1)`
			_, _ = db.Exec(delBankQuery, pq.Array(bankIDsToDelete))
		}

		// Then, approve the rest
		query := `UPDATE auditactionbank SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id,bank_id`
		rows, err := db.Query(query, checkerBy, req.Comment, pq.Array(req.ActionIDs))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, bankID string
			rows.Scan(&id, &bankID)
			updated = append(updated, id, bankID)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"updated": updated,
			"deleted": deleted,
		})
	}
}
