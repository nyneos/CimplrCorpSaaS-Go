package allMaster

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"time"

	// "mime/multipart"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

func CreateBankMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		ctx := r.Context()
		tx, txErr := pgxPool.Begin(ctx)
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
		err := tx.QueryRow(ctx, query,
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
			tx.Rollback(ctx)
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		auditQuery := `INSERT INTO auditactionbank (
			bank_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, $2, $3, $4, $5, now())`
		_, auditErr := tx.Exec(ctx, auditQuery,
			bankID,
			"CREATE",
			"PENDING_APPROVAL",
			nil,
			createdBy,
		)
		if auditErr != nil {
			tx.Rollback(ctx)
			api.RespondWithError(w, http.StatusInternalServerError, "Bank created but audit log failed: "+auditErr.Error())
			return
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction commit failed: "+commitErr.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"bank_id": bankID,
		})
	}
}

func GetAllBankMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		query := `
			SELECT m.bank_id, m.bank_name, m.bank_short_name, m.swift_bic_code, m.country_of_headquarters, m.connectivity_type, m.active_status,
			       m.contact_person_name, m.contact_person_email, m.contact_person_phone, m.address_line1, m.address_line2, m.city,
			       m.state_province, m.postal_code,
			       m.old_bank_name, m.old_bank_short_name, m.old_swift_bic_code, m.old_country_of_headquarters, m.old_connectivity_type, m.old_active_status,
			       m.old_contact_person_name, m.old_contact_person_email, m.old_contact_person_phone, m.old_address_line1, m.old_address_line2, m.old_city,
			       m.old_state_province, m.old_postal_code
			FROM masterbank m
			WHERE COALESCE(m.is_deleted, false) = false
		`

		rows, err := pgxPool.Query(ctx, query)
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
				bankShortName, swiftBicCode                                   *string
				contactPersonName, contactPersonEmail, contactPersonPhone     *string
				addressLine1, addressLine2, city, stateProvince, postalCode   *string
				oldBankName, oldBankShortName, oldSwiftBicCode                *string
				oldCountryOfHQ, oldConnectivityType, oldActiveStatus          *string
				oldContactPersonName, oldContactPersonEmail                   *string
				oldContactPersonPhone, oldAddressLine1, oldAddressLine2       *string
				oldCity, oldStateProvince, oldPostalCode                      *string
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
			var processingStatusPtr, requestedByPtr, actionTypePtr, actionIDPtr, checkerByPtr, checkerCommentPtr, reasonPtr *string
			var requestedAtPtr, checkerAtPtr *time.Time
			_ = pgxPool.QueryRow(ctx, auditQuery, bankID).Scan(&processingStatusPtr, &requestedByPtr, &requestedAtPtr, &actionTypePtr, &actionIDPtr, &checkerByPtr, &checkerAtPtr, &checkerCommentPtr, &reasonPtr)

			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM auditactionbank 
								  WHERE bank_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') 
								  ORDER BY requested_at DESC`
			auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, bankID)
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
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

			banks = append(banks, map[string]interface{}{
				"bank_id":   bankID,
				"bank_name": bankName,
				"bank_short_name": func() string {
					if bankShortName != nil {
						return *bankShortName
					}
					return ""
				}(),
				"swift_bic_code": func() string {
					if swiftBicCode != nil {
						return *swiftBicCode
					}
					return ""
				}(),
				"country_of_headquarters": countryOfHQ,
				"connectivity_type":       connectivityType,
				"active_status":           activeStatus,
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
				"old_bank_name": func() string {
					if oldBankName != nil {
						return *oldBankName
					}
					return ""
				}(),
				"old_bank_short_name": func() string {
					if oldBankShortName != nil {
						return *oldBankShortName
					}
					return ""
				}(),
				"old_swift_bic_code": func() string {
					if oldSwiftBicCode != nil {
						return *oldSwiftBicCode
					}
					return ""
				}(),
				"old_country_of_headquarters": func() string {
					if oldCountryOfHQ != nil {
						return *oldCountryOfHQ
					}
					return ""
				}(),
				"old_connectivity_type": func() string {
					if oldConnectivityType != nil {
						return *oldConnectivityType
					}
					return ""
				}(),
				"old_active_status": func() string {
					if oldActiveStatus != nil {
						return *oldActiveStatus
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
				"processing_status": func() string {
					if processingStatusPtr != nil {
						return *processingStatusPtr
					}
					return ""
				}(),
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
func GetBankNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			WHERE m.active_status = 'Active' AND a.processing_status = 'APPROVED' AND COALESCE(m.is_deleted, false) = false
		`
		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, query)
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
				"bank_id":   bankID,
				"bank_name": bankName,
				"bank_short_name": func() string {
					if bankShortName != "" {
						return bankShortName
					}
					return ""
				}(),
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

// UploadBank handles multipart uploads for banks and stages them using pgxpool (no database/sql usage)
func UploadBank(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
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
		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fh.Filename)
				return
			}
			ext := getFileExt(fh.Filename)
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

			// normalize header names to match staging table column names
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

			// stage
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"input_bank_table"}, columns, pgx.CopyFromRows(copyRows))
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// read mapping
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_bank`)
			if err != nil {
				tx.Rollback(ctx)
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

			insertSQL := fmt.Sprintf(`
				INSERT INTO masterbank (%s)
				SELECT %s
				FROM input_bank_table s
				WHERE s.upload_batch_id = $1
				RETURNING bank_id
			`, tgtColsStr, srcColsStr)
			rows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newIDs []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil {
					newIDs = append(newIDs, id)
				}
			}
			rows.Close()

			if len(newIDs) > 0 {
				auditSQL := `INSERT INTO auditactionbank (bank_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT bank_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM masterbank WHERE bank_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userName, newIDs); err != nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "batch_ids": batchIDs})
	}
}

// Bulk update handler for bank master
func UpdateBankMasterBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		ctx := r.Context()
		for _, bank := range req.Banks {
			tx, txErr := pgxPool.Begin(ctx)
			if txErr != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Failed to start transaction: " + txErr.Error(), "bank_id": bank.BankID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
					if p := recover(); p != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "panic: " + fmt.Sprint(p), "bank_id": bank.BankID})
					}
				}()

				// fetch existing values (use pointer types instead of database/sql Null types)
				var exBankName, exBankShortName, exSwift, exCountry, exConnectivity, exActive *string
				var exContactName, exContactEmail, exContactPhone *string
				var exAddr1, exAddr2, exCity, exState, exPostal *string
				sel := `SELECT bank_name, bank_short_name, swift_bic_code, country_of_headquarters, connectivity_type, active_status, contact_person_name, contact_person_email, contact_person_phone, address_line1, address_line2, city, state_province, postal_code FROM masterbank WHERE bank_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, bank.BankID).Scan(&exBankName, &exBankShortName, &exSwift, &exCountry, &exConnectivity, &exActive, &exContactName, &exContactEmail, &exContactPhone, &exAddr1, &exAddr2, &exCity, &exState, &exPostal); err != nil {
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
						args = append(args, fmt.Sprint(v), func() string {
							if exBankName != nil {
								return *exBankName
							}
							return ""
						}())
						pos += 2
					case "bank_short_name":
						sets = append(sets, fmt.Sprintf("bank_short_name=$%d, old_bank_short_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exBankShortName != nil {
								return *exBankShortName
							}
							return ""
						}())
						pos += 2
					case "swift_bic_code":
						sets = append(sets, fmt.Sprintf("swift_bic_code=$%d, old_swift_bic_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exSwift != nil {
								return *exSwift
							}
							return ""
						}())
						pos += 2
					case "country_of_headquarters":
						sets = append(sets, fmt.Sprintf("country_of_headquarters=$%d, old_country_of_headquarters=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exCountry != nil {
								return *exCountry
							}
							return ""
						}())
						pos += 2
					case "connectivity_type":
						sets = append(sets, fmt.Sprintf("connectivity_type=$%d, old_connectivity_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exConnectivity != nil {
								return *exConnectivity
							}
							return ""
						}())
						pos += 2
					case "active_status":
						sets = append(sets, fmt.Sprintf("active_status=$%d, old_active_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exActive != nil {
								return *exActive
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
					case "address_line1":
						sets = append(sets, fmt.Sprintf("address_line1=$%d, old_address_line1=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exAddr1 != nil {
								return *exAddr1
							}
							return ""
						}())
						pos += 2
					case "address_line2":
						sets = append(sets, fmt.Sprintf("address_line2=$%d, old_address_line2=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), func() string {
							if exAddr2 != nil {
								return *exAddr2
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
					default:
						// ignore unknown
					}
				}

				var updatedBankID string
				if len(sets) > 0 {
					q := "UPDATE masterbank SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE bank_id=$%d RETURNING bank_id", pos)
					args = append(args, bank.BankID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedBankID); err != nil {
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
				if _, err := tx.Exec(ctx, auditQuery, updatedBankID, "EDIT", "PENDING_EDIT_APPROVAL", nil, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Bank updated but audit log failed: " + err.Error(), "bank_id": updatedBankID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
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
func BulkDeleteBankAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			err := pgxPool.QueryRow(r.Context(), query, bankID, req.Reason, requestedBy).Scan(&actionID)
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
func BulkRejectBankAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			BankIDs []string `json:"bank_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BankIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields: provide bank_ids")
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
		query := `UPDATE auditactionbank SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE bank_id = ANY($3) RETURNING action_id,bank_id`
		rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, pq.Array(req.BankIDs))
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
func BulkApproveBankAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			BankIDs []string `json:"bank_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BankIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields: provide bank_ids")
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
		// First, delete records with processing_status = 'PENDING_DELETE_APPROVAL' for the given bank_ids
		delQuery := `DELETE FROM auditactionbank WHERE bank_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, bank_id`
		delRows, delErr := pgxPool.Query(r.Context(), delQuery, pq.Array(req.BankIDs))
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
			// soft-delete: mark rows as deleted instead of physical delete
			_, _ = pgxPool.Exec(r.Context(), `UPDATE masterbank SET is_deleted = true WHERE bank_id = ANY($1)`, pq.Array(bankIDsToDelete))
		}

		// Then, approve the rest by bank_ids
		query := `UPDATE auditactionbank SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE bank_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id,bank_id`
		rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, pq.Array(req.BankIDs))
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
