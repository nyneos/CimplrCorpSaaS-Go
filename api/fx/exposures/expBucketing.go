package exposures

// expBucketing.go: Handles exposure bucketing logic and APIs.

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/lib/pq"
)

// // Helper: send JSON error response
// func respondWithError(w http.ResponseWriter, status int, errMsg string) {
// 	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
// 	w.WriteHeader(status)
// 	json.NewEncoder(w).Encode(map[string]interface{}{
// 		constants.ValueSuccess: false,
// 		constants.ValueError:   errMsg,
// 	})
// }

// Handler: Update exposure headers, line items, bucketing, hedging proposal
func UpdateExposureHeadersLineItemsBucketing(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string                 `json:"user_id"`
			ExposureHeaderID string                 `json:"exposure_header_id"`
			HeaderFields     map[string]interface{} `json:"headerFields"`
			LineItemFields   map[string]interface{} `json:"lineItemFields"`
			BucketingFields  map[string]interface{} `json:"bucketingFields"`
			HedgingFields    map[string]interface{} `json:"hedgingFields"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ExposureHeaderID == "" {
			respondWithError(w, http.StatusBadRequest, "Invalid request body or exposure_header_id missing")
			return
		}

		// // Middleware: Validate session
		// activeSessions := auth.GetActiveSessions()
		// found := false
		// for _, session := range activeSessions {
		// 	if session.UserID == req.UserID {
		// 		found = true
		// 		break
		// 	}
		// }
		// if !found {
		// 	respondWithError(w, http.StatusUnauthorized, "Unauthorized: invalid session")
		// 	return
		// }

		// Middleware: Get business units from context
		// buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		// if !ok {
		// 	respondWithError(w, http.StatusInternalServerError, "Business units not found in context")
		// 	return
		// }

		updated := map[string]interface{}{}

		// Ensure exposure_bucketing row exists
		if len(req.BucketingFields) > 0 {
			var exists int
			err := db.QueryRow("SELECT 1 FROM exposure_bucketing WHERE exposure_header_id = $1", req.ExposureHeaderID).Scan(&exists)
			if err == sql.ErrNoRows {
				_, err := db.Exec("INSERT INTO exposure_bucketing (exposure_header_id) VALUES ($1)", req.ExposureHeaderID)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "Failed to create exposure_bucketing row")
					return
				}
			}
		}

		// Ensure hedging_proposal row exists
		if len(req.HedgingFields) > 0 {
			var exists int
			err := db.QueryRow("SELECT 1 FROM hedging_proposal WHERE exposure_header_id = $1", req.ExposureHeaderID).Scan(&exists)
			if err == sql.ErrNoRows {
				_, err := db.Exec("INSERT INTO hedging_proposal (exposure_header_id) VALUES ($1)", req.ExposureHeaderID)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "Failed to create hedging_proposal row")
					return
				}
			}
		}

		// Update exposure_headers
		if len(req.HeaderFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range req.HeaderFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
				values = append(values, v)
				i++
			}
			values = append(values, req.ExposureHeaderID)
			query := fmt.Sprintf("UPDATE exposure_headers SET %s WHERE exposure_header_id = $%d RETURNING *", strings.Join(setParts, ", "), len(values))
			rows, err := db.Query(query, values...)
			if err == nil {
				cols, _ := rows.Columns()
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					rows.Scan(valPtrs...)
					rowMap := map[string]interface{}{}
					for i, col := range cols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					updated["header"] = rowMap
				}
				rows.Close()
				db.Exec("UPDATE exposure_bucketing SET status = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID)
			}
		}

		// Update exposure_line_items
		if len(req.LineItemFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range req.LineItemFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
				values = append(values, v)
				i++
			}
			values = append(values, req.ExposureHeaderID)
			query := fmt.Sprintf("UPDATE exposure_line_items SET %s WHERE exposure_header_id = $%d RETURNING *", strings.Join(setParts, ", "), len(values))
			rows, err := db.Query(query, values...)
			if err == nil {
				cols, _ := rows.Columns()
				lineItems := []map[string]interface{}{}
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					rows.Scan(valPtrs...)
					rowMap := map[string]interface{}{}
					for i, col := range cols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					lineItems = append(lineItems, rowMap)
				}
				updated["lineItems"] = lineItems
				rows.Close()
				db.Exec("UPDATE exposure_bucketing SET status = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID)
			}
		}

		// Update exposure_bucketing
		if len(req.BucketingFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range req.BucketingFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
				values = append(values, v)
				i++
			}
			values = append(values, req.ExposureHeaderID)
			query := fmt.Sprintf("UPDATE exposure_bucketing SET %s WHERE exposure_header_id = $%d RETURNING *", strings.Join(setParts, ", "), len(values))
			rows, err := db.Query(query, values...)
			if err == nil {
				cols, _ := rows.Columns()
				bucketing := []map[string]interface{}{}
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					rows.Scan(valPtrs...)
					rowMap := map[string]interface{}{}
					for i, col := range cols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					bucketing = append(bucketing, rowMap)
				}
				updated["bucketing"] = bucketing
				rows.Close()
				db.Exec("UPDATE exposure_bucketing SET status_bucketing = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID)
			}
		}

		// Update hedging_proposal
		if len(req.HedgingFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range req.HedgingFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
				values = append(values, v)
				i++
			}
			values = append(values, req.ExposureHeaderID)
			query := fmt.Sprintf("UPDATE hedging_proposal SET %s WHERE exposure_header_id = $%d RETURNING *", strings.Join(setParts, ", "), len(values))
			rows, err := db.Query(query, values...)
			if err == nil {
				cols, _ := rows.Columns()
				hedging := []map[string]interface{}{}
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					valPtrs := make([]interface{}, len(cols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					rows.Scan(valPtrs...)
					rowMap := map[string]interface{}{}
					for i, col := range cols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					hedging = append(hedging, rowMap)
				}
				updated["hedging"] = hedging
				rows.Close()
				db.Exec("UPDATE hedging_proposal SET status = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID)
			}
		}

		if len(updated) == 0 {
			respondWithError(w, http.StatusNotFound, "No records updated")
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"updated":              updated,
		})
	}
}

// Handler: Get exposure headers, line items, and bucketing for accessible business units
func GetExposureHeadersLineItemsBucketing(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}

		// Validate session
		// activeSessions := auth.GetActiveSessions()
		// var session *auth.UserSession
		// for _, s := range activeSessions {
		// 	if s.UserID == req.UserID {
		// 		session = s
		// 		break
		// 	}
		// }
		// if session == nil {
		// 	respondWithError(w, http.StatusNotFound, "Session expired or not found. Please login again.")
		// 	return
		// }

		// Get business units from context (set by middleware)
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Ensure all exposure_header_id are present in exposure_bucketing
		_, _ = db.Exec(`INSERT INTO exposure_bucketing (exposure_header_id)
			SELECT exposure_header_id
			FROM exposure_headers
			WHERE entity = ANY($1)
			  AND (approval_status = 'approved' OR approval_status = 'Approved')
			  AND exposure_header_id NOT IN (
				SELECT exposure_header_id FROM exposure_bucketing
			  )`, pq.Array(buNames))

		// Join exposure_headers, exposure_line_items, exposure_bucketing
		rows, err := db.Query(`SELECT h.*, l.*, b.*
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			LEFT JOIN exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id
			WHERE h.entity = ANY($1)
			  AND (h.approval_status = 'approved' OR h.approval_status = 'Approved')`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch joined exposures")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		pageData := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			pageData = append(pageData, rowMap)
		}

		// Fetch permissions for 'exposure-bucketing' page for this role
		exposureBucketingPerms := map[string]interface{}{}
		var roleId int
		err = db.QueryRow("SELECT role_id FROM user_roles WHERE user_id = $1 LIMIT 1", req.UserID).Scan(&roleId)
		if err == nil {
			permRows, err := db.Query(`
				SELECT p.page_name, p.tab_name, p.action, rp.allowed
				FROM role_permissions rp
				JOIN permissions p ON rp.permission_id = p.id
				WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')
			`, roleId)
			if err == nil {
				defer permRows.Close()
				for permRows.Next() {
					var pageName, tabName, action string
					var allowed bool
					if err := permRows.Scan(&pageName, &tabName, &action, &allowed); err == nil {
						if pageName != constants.ExposureBucketing {
							continue
						}
						if exposureBucketingPerms[constants.ExposureBucketing] == nil {
							exposureBucketingPerms[constants.ExposureBucketing] = map[string]interface{}{}
						}
						perms := exposureBucketingPerms[constants.ExposureBucketing].(map[string]interface{})
						if tabName == "" {
							if perms["pagePermissions"] == nil {
								perms["pagePermissions"] = map[string]interface{}{}
							}
							perms["pagePermissions"].(map[string]interface{})[action] = allowed
						} else {
							if perms["tabs"] == nil {
								perms["tabs"] = map[string]interface{}{}
							}
							if perms["tabs"].(map[string]interface{})[tabName] == nil {
								perms["tabs"].(map[string]interface{})[tabName] = map[string]interface{}{}
							}
							perms["tabs"].(map[string]interface{})[tabName].(map[string]interface{})[action] = allowed
						}
					}
				}
			}
		}

		resp := map[string]interface{}{
			"buAccessible": buNames,
			"pageData":     pageData,
		}
		if perms, ok := exposureBucketingPerms[constants.ExposureBucketing]; ok {
			resp[constants.ExposureBucketing] = perms
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

func ApproveBucketingStatus(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposure_header_ids"`
			Comments          string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "exposure_header_ids and user_id are required")
			return
		}

		// Get updatedBy from session
		var updatedBy string
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				updatedBy = s.Name // or s.Email
				break
			}
		}
		if updatedBy == "" {
			respondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		rows, err := db.Query(
			`UPDATE exposure_bucketing
             SET status_bucketing = 'Approved', updated_by = $2, comments = $3, updated_at = NOW()
             WHERE exposure_header_id = ANY($1)
             RETURNING *`,
			pq.Array(req.ExposureHeaderIds), updatedBy, req.Comments,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		approved := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			rows.Scan(valPtrs...)
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			approved = append(approved, rowMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"Approved":             approved,
		})
	}
}

func RejectBucketingStatus(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposure_header_ids"`
			Comments          string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "exposure_header_ids and user_id are required")
			return
		}

		// Get updatedBy from session
		var updatedBy string
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				updatedBy = s.Name // or s.Email
				break
			}
		}
		if updatedBy == "" {
			respondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		rows, err := db.Query(
			`UPDATE exposure_bucketing
             SET status_bucketing = 'Rejected', updated_by = $2, comments = $3, updated_at = NOW()
             WHERE exposure_header_id = ANY($1)
             RETURNING *`,
			pq.Array(req.ExposureHeaderIds), updatedBy, req.Comments,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		rejected := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			rows.Scan(valPtrs...)
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			rejected = append(rejected, rowMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"Rejected":             rejected,
		})
	}
}
