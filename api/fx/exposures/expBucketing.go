package exposures

// expBucketing.go: Handles exposure bucketing logic and APIs.

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/lib/pq"
)

var allowedExposureHeaderFields = map[string]string{
	"additional_header_details": "additional_header_details",
	"amount_in_doc_curr":        "amount_in_doc_curr",
	"amount_in_local_currency":  "amount_in_local_currency",
	"company_code":              "company_code",
	"counterparty_code":         "counterparty_code",
	"counterparty_name":         "counterparty_name",
	"counterparty_type":         "counterparty_type",
	"currency":                  "currency",
	"document_date":             "value_date",
	"document_id":               "document_id",
	"effective_due_date":        "effective_due_date",
	"entity":                    "entity",
	"exposure_type":             "exposure_type",
	"net_due_date":              "net_due_date",
	"net_value":                 "net_value",
	"payment_to_vendor":         "payment_to_vendor",
	"posting_date":              "posting_date",
	"source":                    "source",
	"status":                    "status",
	"total_open_amount":         "total_open_amount",
	"total_original_amount":     "total_original_amount",
	"value_date":                "value_date",
}

var allowedExposureLineItemFields = map[string]string{
	"additional_line_details":  "additional_line_details",
	"amount_in_doc_curr":       "amount_in_doc_curr",
	"amount_in_local_currency": "amount_in_local_currency",
	"company_code":             "company_code",
	"counterparty_code":        "counterparty_code",
	"counterparty_name":        "counterparty_name",
	"counterparty_type":        "counterparty_type",
	"currency":                 "currency",
	"document_date":            "document_date",
	"document_id":              "document_id",
	"effective_due_date":       "effective_due_date",
	"line_item_amount":         "line_item_amount",
	"net_due_date":             "net_due_date",
	"net_value":                "net_value",
	"payment_to_vendor":        "payment_to_vendor",
	"posting_date":             "posting_date",
	"quantity":                 "quantity",
	"source":                   "source",
	"status":                   "status",
	"unit_price":               "unit_price",
}

var allowedExposureBucketingFields = map[string]string{
	"comments":         "comments",
	"month_1":          "month_1",
	"month_2":          "month_2",
	"month_3":          "month_3",
	"month_4":          "month_4",
	"month_4_6":        "month_4_6",
	"month_6plus":      "month_6plus",
	"old_month1":       "old_month1",
	"old_month2":       "old_month2",
	"old_month3":       "old_month3",
	"old_month4":       "old_month4",
	"old_month4to6":    "old_month4to6",
	"old_month6plus":   "old_month6plus",
	"status_bucketing": "status_bucketing",
}

var allowedHedgingProposalFields = map[string]string{
	"comments":       "comments",
	"status_hedging": "status_hedging",
}

var jsonUpdateColumns = map[string]bool{
	"additional_header_details": true,
	"additional_line_details":   true,
}

func normalizeExposureUpdateValue(column string, value interface{}) (interface{}, error) {
	if !jsonUpdateColumns[column] || value == nil {
		return value, nil
	}

	switch value.(type) {
	case string, []byte:
		return value, nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return encoded, nil
	}
}

func buildExposureUpdateParts(fields map[string]interface{}, allowed map[string]string) ([]string, []interface{}, error) {
	if len(fields) == 0 {
		return nil, nil, nil
	}

	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	setParts := make([]string, 0, len(keys))
	values := make([]interface{}, 0, len(keys))
	usedColumns := make(map[string]string, len(keys))

	for index, key := range keys {
		column, ok := allowed[key]
		if !ok {
			return nil, nil, fmt.Errorf("unsupported field %q", key)
		}
		if previousKey, exists := usedColumns[column]; exists {
			return nil, nil, fmt.Errorf("duplicate fields %q and %q target the same column", previousKey, key)
		}

		normalizedValue, err := normalizeExposureUpdateValue(column, fields[key])
		if err != nil {
			return nil, nil, fmt.Errorf("invalid value for %q: %w", key, err)
		}

		setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, column, index+1))
		values = append(values, normalizedValue)
		usedColumns[column] = key
	}

	return setParts, values, nil
}

func queryExposureUpdateRows(ctx context.Context, db *sql.DB, table string, setParts []string, values []interface{}, exposureHeaderID string) ([]map[string]interface{}, error) {
	if len(setParts) == 0 {
		return nil, nil
	}

	args := append(append([]interface{}{}, values...), exposureHeaderID)
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE exposure_header_id = $%d RETURNING *",
		table,
		strings.Join(setParts, ", "),
		len(args),
	)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := []map[string]interface{}{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			return nil, err
		}
		rowMap := map[string]interface{}{}
		for i, col := range cols {
			rowMap[col] = parseDBValue(col, vals[i])
		}
		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

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
			// legacy/alternate payload key used by some clients
			Fields        map[string]interface{} `json:"fields"`
			HedgingFields map[string]interface{} `json:"hedgingFields"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ExposureHeaderID == "" {
			respondWithError(w, http.StatusBadRequest, "Invalid request body or exposure_header_id missing")
			return
		}

		if len(req.BucketingFields) == 0 && len(req.Fields) > 0 {
			req.BucketingFields = req.Fields
		}

		headerSetParts, headerValues, err := buildExposureUpdateParts(req.HeaderFields, allowedExposureHeaderFields)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		lineItemSetParts, lineItemValues, err := buildExposureUpdateParts(req.LineItemFields, allowedExposureLineItemFields)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		bucketingSetParts, bucketingValues, err := buildExposureUpdateParts(req.BucketingFields, allowedExposureBucketingFields)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		hedgingSetParts, hedgingValues, err := buildExposureUpdateParts(req.HedgingFields, allowedHedgingProposalFields)
		if err != nil {
			respondWithError(w, http.StatusBadRequest, err.Error())
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
		if len(bucketingSetParts) > 0 {
			var exists int
			err := db.QueryRowContext(r.Context(), "SELECT 1 FROM exposure_bucketing WHERE exposure_header_id = $1", req.ExposureHeaderID).Scan(&exists)
			if err == sql.ErrNoRows {
				_, err := db.ExecContext(r.Context(), "INSERT INTO exposure_bucketing (exposure_header_id) VALUES ($1)", req.ExposureHeaderID)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "Failed to create exposure_bucketing row")
					return
				}
			}
		}

		// Ensure hedging_proposal row exists
		if len(hedgingSetParts) > 0 {
			var exists int
			err := db.QueryRowContext(r.Context(), "SELECT 1 FROM hedging_proposal WHERE exposure_header_id = $1", req.ExposureHeaderID).Scan(&exists)
			if err == sql.ErrNoRows {
				_, err := db.ExecContext(r.Context(), "INSERT INTO hedging_proposal (exposure_header_id) VALUES ($1)", req.ExposureHeaderID)
				if err != nil {
					respondWithError(w, http.StatusInternalServerError, "Failed to create hedging_proposal row")
					return
				}
			}
		}

		// Update exposure_headers
		if len(headerSetParts) > 0 {
			headerRows, err := queryExposureUpdateRows(r.Context(), db, "exposure_headers", headerSetParts, headerValues, req.ExposureHeaderID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update exposure_headers")
				return
			}
			if len(headerRows) > 0 {
				updated["header"] = headerRows[0]
				db.ExecContext(r.Context(), "UPDATE exposure_bucketing SET status = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID)
			}
		}

		// Update exposure_line_items
		if len(lineItemSetParts) > 0 {
			lineItemRows, err := queryExposureUpdateRows(r.Context(), db, "exposure_line_items", lineItemSetParts, lineItemValues, req.ExposureHeaderID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update exposure_line_items")
				return
			}
			if len(lineItemRows) > 0 {
				updated["lineItems"] = lineItemRows
				db.ExecContext(r.Context(), "UPDATE exposure_bucketing SET status = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID)
			}
		}

		// Update exposure_bucketing
		if len(bucketingSetParts) > 0 {
			bucketingRows, err := queryExposureUpdateRows(r.Context(), db, "exposure_bucketing", bucketingSetParts, bucketingValues, req.ExposureHeaderID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update exposure_bucketing")
				return
			}
			if len(bucketingRows) > 0 {
				updated["bucketing"] = bucketingRows
				db.ExecContext(r.Context(), "UPDATE exposure_bucketing SET status_bucketing = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID)
			}
		}

		// Update hedging_proposal
		if len(hedgingSetParts) > 0 {
			hedgingRows, err := queryExposureUpdateRows(r.Context(), db, "hedging_proposal", hedgingSetParts, hedgingValues, req.ExposureHeaderID)
			if err != nil {
				respondWithError(w, http.StatusInternalServerError, "Failed to update hedging_proposal")
				return
			}
			if len(hedgingRows) > 0 {
				updated["hedging"] = hedgingRows
				db.ExecContext(r.Context(), "UPDATE hedging_proposal SET status = 'pending' WHERE exposure_header_id = $1", req.ExposureHeaderID)
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
		_, _ = db.ExecContext(r.Context(), `INSERT INTO exposure_bucketing (exposure_header_id)
			SELECT exposure_header_id
			FROM exposure_headers
			WHERE entity = ANY($1)
			  AND (approval_status = 'approved' OR approval_status = 'Approved')
			  AND exposure_header_id NOT IN (
				SELECT exposure_header_id FROM exposure_bucketing
			  )`, pq.Array(buNames))

		// Join exposure_headers, exposure_line_items, exposure_bucketing
		rows, err := db.QueryContext(r.Context(), `SELECT h.*, l.*, b.*
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
		err = db.QueryRowContext(r.Context(), "SELECT role_id FROM user_roles WHERE user_id = $1 LIMIT 1", req.UserID).Scan(&roleId)
		if err == nil {
			permRows, err := db.QueryContext(r.Context(), `
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

		rows, err := db.QueryContext(r.Context(),
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

		rows, err := db.QueryContext(r.Context(),
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
