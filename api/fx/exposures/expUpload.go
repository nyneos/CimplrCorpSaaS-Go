package exposures

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/xuri/excelize/v2"

	"github.com/lib/pq"
)

func UploadExposure(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
		// ...other exposure fields...
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	activeSessions := auth.GetActiveSessions()
	found := false
	for _, session := range activeSessions {
		if session.UserID == req.UserID {
			found = true
			break
		}
	}

	if !found {
		http.Error(w, "Unauthorized: invalid session", http.StatusUnauthorized)
		return
	}

	buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
	if !ok {
		http.Error(w, "Business units not found in context", http.StatusInternalServerError)
		return
	}
	responseNames, _ := json.Marshal(buNames)
	// Handle the exposure upload logic here
	w.Write(responseNames)
}

// Helper: send JSON error response and log
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

// Helper: check if string in slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func NormalizeDate(dateStr string) string {
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return ""
	}

	// Normalize spaces
	dateStr = regexp.MustCompile(`\s+`).ReplaceAllString(dateStr, " ")

	// Try common layouts first (preserve original behavior)
	layouts := []string{
		// ISO formats
		"2006-01-02",
		"2006/01/02",
		"2006.01.02",
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",

		// DD-MM-YYYY formats
		"02-01-2006",
		"02/01/2006",
		"02.01.2006",
		"02-01-2006 15:04:05",
		"02/01/2006 15:04:05",
		"02.01.2006 15:04:05",

		// MM-DD-YYYY formats
		"01-02-2006",
		"01/02/2006",
		"01.02.2006",
		"01-02-2006 15:04:05",
		"01/02/2006 15:04:05",
		"01.02.2006 15:04:05",

		// Text month formats
		"02-Jan-2006",
		"02-Jan-06",
		"2-Jan-2006",
		"2-Jan-06",
		"02-Jan-2006 15:04:05",
		"02 Jan 2006",
		"2 Jan 2006",
		"02 Jan 06",
		"2 Jan 06",
		"Jan 02, 2006",
		"Jan 2, 2006",
		"January 02, 2006",
		"January 2, 2006",

		// Single digit day/month formats
		"2-1-2006",
		"2/1/2006",
		"2.1.2006",
		"1-2-2006",
		"1/2/2006",
		"1.2.2006",

		// Short year formats
		"02-01-06",
		"02/01/06",
		"02.01.06",
		"01-02-06",
		"01/02/06",
		"01.02.06",
		"2-1-06",
		"2/1/06",
		"1-2-06",
		"1/2-06",

		// compact
		"20060102",
	}

	for _, l := range layouts {
		if t, err := time.Parse(l, dateStr); err == nil {
			if t.Year() < 1900 || t.Year() > 9999 {
				continue
			}
			return t.Format("2006-01-02")
		}
	}

	// If the string is purely numeric try several heuristics:
	// - YYYYMMDD (8 digits)
	// - Unix timestamp (seconds / ms / us / ns)
	// - Excel serial (days since 1899-12-30)
	digits := true
	for _, r := range dateStr {
		if r < '0' || r > '9' {
			digits = false
			break
		}
	}

	if digits {
		// YYYYMMDD
		if len(dateStr) == 8 {
			if y, err := strconv.Atoi(dateStr[0:4]); err == nil {
				if m, err := strconv.Atoi(dateStr[4:6]); err == nil {
					if d, err := strconv.Atoi(dateStr[6:8]); err == nil {
						if y >= 1900 && y <= 9999 {
							return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
						}
					}
				}
			}
		}

		if v, err := strconv.ParseInt(dateStr, 10, 64); err == nil {
			var t time.Time
			switch {
			case v >= 1e17:
				// nanoseconds since epoch
				t = time.Unix(0, v)
			case v >= 1e14:
				// microseconds -> ns
				t = time.Unix(0, v*1000)
			case v >= 1e11:
				// milliseconds -> ns
				t = time.Unix(0, v*1000000)
			case v >= 1e9:
				// seconds
				t = time.Unix(v, 0)
			default:
				// Treat as Excel serial date (days since 1899-12-30)
				base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
				t = base.AddDate(0, 0, int(v))
			}
			if t.Year() >= 1900 && t.Year() <= 9999 {
				return t.Format("2006-01-02")
			}
		}
	}

	return ""
}

// Helper: parse DB value to correct Go type
func parseDBValue(col string, val interface{}) interface{} {
	if val == nil {
		return nil
	}
	switch t := val.(type) {
	case time.Time:
		y := t.Year()
		if y >= 0 && y <= 9999 {
			return t.Format(time.RFC3339)
		}
		return fmt.Sprintf("%v", t)
	case *time.Time:
		if t == nil {
			return ""
		}
		y := t.Year()
		if y >= 0 && y <= 9999 {
			return t.Format(time.RFC3339)
		}
		return fmt.Sprintf("%v", t)
	}

	if b, ok := val.([]byte); ok {
		// JSON fields
		if col == "additional_header_details" || col == "additional_line_details" {
			var obj interface{}
			if err := json.Unmarshal(b, &obj); err == nil {
				return obj
			}
		}
		// Numeric fields
		numericFields := map[string]bool{
			"amount_in_local_currency": true,
			"line_item_amount":         true,
			"quantity":                 true,
			"total_open_amount":        true,
			"total_original_amount":    true,
			"unit_price":               true,
		}
		if numericFields[col] {
			s := string(b)
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
		return string(b)
	}
	return val
}

// Handler
func EditExposureHeadersLineItemsJoined(db *sql.DB) http.HandlerFunc {
	// ...existing code...
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     string                 `json:"id"` // exposure_header_id
			Fields map[string]interface{} `json:"fields"`
			UserID string                 `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
			return
		}

		// Get buNames from context
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusInternalServerError, "Business units not found in context")
			return
		}

		// Get joined row
		joinRow := db.QueryRow(`
			SELECT h.exposure_header_id, h.entity
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			WHERE h.exposure_header_id = $1
		`, req.ID)

		var exposureHeaderID, entity string
		if err := joinRow.Scan(&exposureHeaderID, &entity); err != nil {
			respondWithError(w, http.StatusNotFound, "Row not found: "+err.Error())
			return
		}

		// Validate entity is in buNames
		if !contains(buNames, entity) {
			respondWithError(w, http.StatusForbidden, "Forbidden: entity not accessible")
			return
		}

		// Get columns for each table
		headerColsRes, _ := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'exposure_headers'`)
		lineColsRes, _ := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'exposure_line_items'`)
		defer headerColsRes.Close()
		defer lineColsRes.Close()

		var headerCols, lineCols []string
		for headerColsRes.Next() {
			var col string
			headerColsRes.Scan(&col)
			headerCols = append(headerCols, col)
		}
		for lineColsRes.Next() {
			var col string
			lineColsRes.Scan(&col)
			lineCols = append(lineCols, col)
		}

		headerFields := make(map[string]interface{})
		lineFields := make(map[string]interface{})
		for k, v := range req.Fields {
			if contains(headerCols, k) {
				headerFields[k] = v
			}
			if contains(lineCols, k) {
				lineFields[k] = v
			}
		}
		if val, ok := headerFields["document_date"]; ok {
	headerFields["value_date"] = val
	delete(headerFields, "document_date")
}
		// Update header if needed
		if len(headerFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range headerFields {
				setParts = append(setParts, fmt.Sprintf("%s = $%d", k, i))
				values = append(values, v)
				i++
			}
			setParts = append(setParts, "approval_status = 'Pending'")
			values = append(values, exposureHeaderID)

			query := fmt.Sprintf(
				"UPDATE exposure_headers SET %s WHERE exposure_header_id = $%d",
				strings.Join(setParts, ", "),
				len(values),
			)
			if _, err := db.Exec(query, values...); err != nil {
				respondWithError(w, http.StatusInternalServerError, "Header update failed: "+err.Error())
				return
			}
		}

		// Update line item if needed
		if len(lineFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range lineFields {
				setParts = append(setParts, fmt.Sprintf("%s = $%d", k, i))
				values = append(values, v)
				i++
			}
			values = append(values, exposureHeaderID)

			query := fmt.Sprintf(
				"UPDATE exposure_line_items SET %s WHERE exposure_header_id = $%d",
				strings.Join(setParts, ", "),
				len(values),
			)
			if _, err := db.Exec(query, values...); err != nil {
				respondWithError(w, http.StatusInternalServerError, "Line item update failed: "+err.Error())
				return
			}
		}

		// Only return the updated fields
		// Fetch full joined row and parse fields
		rows, err := db.Query(`
			SELECT h.*, l.*
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			WHERE h.exposure_header_id = $1
		`, exposureHeaderID)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Fetch failed: "+err.Error())
			return
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		results := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				respondWithError(w, http.StatusInternalServerError, "Row scan failed: "+err.Error())
				return
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				pv := parseDBValue(col, vals[i])
				if pv == nil {
					if col == "additional_header_details" || col == "additional_line_details" {
						rowMap[col] = map[string]interface{}{}
					} else {
						pv = ""
					}
				} else if s, ok := pv.(string); ok && s == "" {
					pv = ""
				}
				// If column already set and new value is empty, preserve the existing value
				if _, exists := rowMap[col]; exists && pv == "" {
					continue
				}
				rowMap[col] = pv
			}
			results = append(results, rowMap)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    results,
		})
	}
}

// Handler: GetExposureHeadersLineItems - returns joined exposure_headers and exposure_line_items filtered by entity
func GetExposureHeadersLineItems(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}

		// Get user's business unit name
		var userBu string
		err := db.QueryRow("SELECT business_unit_name FROM users WHERE id = $1", req.UserID).Scan(&userBu)
		if err != nil || userBu == "" {
			respondWithError(w, http.StatusNotFound, "User not found or has no business unit assigned")
			return
		}

		// Get root entity id
		var rootEntityId string
		err = db.QueryRow(
			"SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
			userBu,
		).Scan(&rootEntityId)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "Business unit entity not found")
			return
		}

		// Recursive CTE to get all descendant entity_names
		rows, err := db.Query(`
			WITH RECURSIVE descendants AS (
				SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
				UNION ALL
				SELECT me.entity_id, me.entity_name
				FROM masterEntity me
				INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
				INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
				WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
			)
			SELECT entity_name FROM descendants
		`, rootEntityId)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		defer rows.Close()
		var buNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				buNames = append(buNames, name)
			}
		}
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}

		// Join exposure_headers and exposure_line_items filtered by entity
		joinRows, err := db.Query(`
			SELECT h.*, l.*
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			WHERE h.entity = ANY($1)
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch exposure headers/line items")
			return
		}
		defer joinRows.Close()
		joinCols, _ := joinRows.Columns()
		joinData := []map[string]interface{}{}
		for joinRows.Next() {
			vals := make([]interface{}, len(joinCols))
			valPtrs := make([]interface{}, len(joinCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := joinRows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range joinCols {
				pv := parseDBValue(col, vals[i])
				if pv == nil {
					if col == "additional_header_details" || col == "additional_line_details" {
						rowMap[col] = map[string]interface{}{}
					} else {
						pv = ""
					}
				} else if s, ok := pv.(string); ok && s == "" {
					pv = ""
				}
									// If column already set and new value is empty, preserve the existing value
					if _, exists := rowMap[col]; exists && pv == "" {
						continue
					}
					rowMap[col] = pv
			}
				// Normalize date-like fields for frontend (DD-MM-YYYY)
				for k, v := range rowMap {
					lk := strings.ToLower(k)
					if strings.Contains(lk, "date") || strings.HasSuffix(lk, "_at") {
						switch tv := v.(type) {
						case string:
							if nd := NormalizeDate(tv); nd != "" {
								// Try to parse the normalized date and reformat to dd-mm-yyyy
								var perr error
								layouts := []string{"2006-01-02", time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05"}
								parsedOK := false
								var parsed time.Time
								for _, l := range layouts {
									parsed, perr = time.Parse(l, nd)
									if perr == nil {
										rowMap[k] = parsed.Format("02-01-2006")
										parsedOK = true
										break
									}
								}
								if !parsedOK {
									// Fallback: if nd looks like yyyy-mm-dd, swap to dd-mm-yyyy
									parts := strings.Split(nd, "-")
									if len(parts) == 3 && len(parts[0]) == 4 {
										rowMap[k] = parts[2] + "-" + parts[1] + "-" + parts[0]
									} else {
										rowMap[k] = nd
									}
								}
							}
						case time.Time:
							rowMap[k] = tv.Format("02-01-2006")
						case *time.Time:
							if tv != nil {
								rowMap[k] = tv.Format("02-01-2006")
							} else {
								rowMap[k] = ""
							}
						}
					}
				}
				joinData = append(joinData, rowMap)
		}

		// Fetch permissions for 'exposure-upload' page for this role
		exposureUploadPerms := map[string]interface{}{}
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
						if pageName != "exposure-upload" {
							continue
						}
						if exposureUploadPerms["exposure-upload"] == nil {
							exposureUploadPerms["exposure-upload"] = map[string]interface{}{}
						}
						perms := exposureUploadPerms["exposure-upload"].(map[string]interface{})
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
			"pageData":     joinData,
		}
		if perms, ok := exposureUploadPerms["exposure-upload"]; ok {
			resp["exposure-upload"] = perms
		}
		// Debug: log sizes so we can see why clients receive empty responses
		log.Printf("[DEBUG] GetExposureHeadersLineItems: buAccessible=%d, pageData=%d", len(buNames), len(joinData))
		w.Header().Set("Content-Type", "application/json")
		b, jerr := json.Marshal(resp)
		if jerr != nil {
			respondWithError(w, http.StatusInternalServerError, "JSON marshal failed: "+jerr.Error())
			return
		}
		if _, werr := w.Write(b); werr != nil {
			log.Printf("[ERROR] Failed to write response: %v", werr)
		}
	}
}

// Handler: GetPendingApprovalHeadersLineItems - returns joined exposure_headers and exposure_line_items filtered by entity and approval_status pending
func GetPendingApprovalHeadersLineItems(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}

		// Validate session
		activeSessions := auth.GetActiveSessions()
		var session *auth.UserSession
		for _, s := range activeSessions {
			if s.UserID == req.UserID {
				session = s
				break
			}
		}
		if session == nil {
			respondWithError(w, http.StatusNotFound, "Session expired or not found. Please login again.")
			return
		}

		// Get user's business unit name
		var userBu string
		err := db.QueryRow("SELECT business_unit_name FROM users WHERE id = $1", req.UserID).Scan(&userBu)
		if err != nil || userBu == "" {
			respondWithError(w, http.StatusNotFound, "User not found or has no business unit assigned")
			return
		}

		// Get root entity id
		var rootEntityId string
		err = db.QueryRow(
			"SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
			userBu,
		).Scan(&rootEntityId)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "Business unit entity not found")
			return
		}

		// Recursive CTE to get all descendant entity_names
		rows, err := db.Query(`
			WITH RECURSIVE descendants AS (
				SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
				UNION ALL
				SELECT me.entity_id, me.entity_name
				FROM masterEntity me
				INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
				INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
				WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
			)
			SELECT entity_name FROM descendants
		`, rootEntityId)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		defer rows.Close()
		var buNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				buNames = append(buNames, name)
			}
		}
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}

		// Join exposure_headers and exposure_line_items filtered by entity and approval_status pending
		joinRows, err := db.Query(`
			SELECT h.*, l.*
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			WHERE h.entity = ANY($1) AND h.approval_status NOT IN ('Approved', 'approved')
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch pending approval headers/line items")
			return
		}
		defer joinRows.Close()
		joinCols, _ := joinRows.Columns()
		joinData := []map[string]interface{}{}
		for joinRows.Next() {
			vals := make([]interface{}, len(joinCols))
			valPtrs := make([]interface{}, len(joinCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := joinRows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range joinCols {
				pv := parseDBValue(col, vals[i])
				if pv == nil {
					if col == "additional_header_details" || col == "additional_line_details" {
						rowMap[col] = map[string]interface{}{}
					} else {
						pv = ""
					}
				} else if s, ok := pv.(string); ok && s == "" {
					pv = ""
				}
				
					// If column already set and new value is empty, preserve the existing value
					if _, exists := rowMap[col]; exists && pv == "" {
						continue
					}
					rowMap[col] = pv
			}
				// Normalize date-like fields for frontend (YYYY-MM-DD)
				for k, v := range rowMap {
					lk := strings.ToLower(k)
					if strings.Contains(lk, "date") || strings.HasSuffix(lk, "_at") {
						switch tv := v.(type) {
						case string:
							if nd := NormalizeDate(tv); nd != "" {
								// Try to parse the normalized date and reformat to dd-mm-yyyy
								var perr error
								layouts := []string{"2006-01-02", time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05"}
								parsedOK := false
								var parsed time.Time
								for _, l := range layouts {
									parsed, perr = time.Parse(l, nd)
									if perr == nil {
										rowMap[k] = parsed.Format("02-01-2006")
										parsedOK = true
										break
									}
								}
								if !parsedOK {
									// Fallback: if nd looks like yyyy-mm-dd, swap to dd-mm-yyyy
									parts := strings.Split(nd, "-")
									if len(parts) == 3 && len(parts[0]) == 4 {
										rowMap[k] = parts[2] + "-" + parts[1] + "-" + parts[0]
									} else {
										rowMap[k] = nd
									}
								}
							}
						case time.Time:
							rowMap[k] = tv.Format("02-01-2006")
						case *time.Time:
							if tv != nil {
								rowMap[k] = tv.Format("02-01-2006")
							} else {
								rowMap[k] = ""
							}
						}
					}
				}
				joinData = append(joinData, rowMap)
		}
		// Fetch permissions for 'exposure-upload' page for this role
		exposureUploadPerms := map[string]interface{}{}
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
						if pageName != "exposure-upload" {
							continue
						}
						if exposureUploadPerms["exposure-upload"] == nil {
							exposureUploadPerms["exposure-upload"] = map[string]interface{}{}
						}
						perms := exposureUploadPerms["exposure-upload"].(map[string]interface{})
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
			"pageData":     joinData,
		}
		if perms, ok := exposureUploadPerms["exposure-upload"]; ok {
			resp["exposure-upload"] = perms
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// Handler: deleteExposureHeaders - marks headers for delete approval
func DeleteExposureHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposureHeaderIds"`
			DeleteComment     string   `json:"delete_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "exposureHeaderIds and user_id are required")
			return
		}

		// Get session info
		var requestedBy string
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Name // or s.Email
				break
			}
		}
		if requestedBy == "" {
			respondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		// Mark for delete approval first
		res, err := db.Exec(
			`UPDATE exposure_headers SET approval_status = 'Delete-Approval', delete_comment = $1, requested_by = $2 WHERE exposure_header_id = ANY($3::uuid[])`,
			req.DeleteComment,
			requestedBy,
			pq.Array(req.ExposureHeaderIds),
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		count, _ := res.RowsAffected()
		if count == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "No matching exposure_headers found",
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("%d exposure_header(s) marked for delete approval", count),
		})
	}
}

// Handler: rejectMultipleExposureHeaders - rejects multiple headers
func RejectMultipleExposureHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposureHeaderIds"`
			RejectionComment  string   `json:"rejection_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "exposureHeaderIds and user_id are required")
			return
		}

		// Get session info
		var rejectedBy string
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				rejectedBy = s.Name // or s.Email
				break
			}
		}
		if rejectedBy == "" {
			respondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		rows, err := db.Query(
			`UPDATE exposure_headers SET approval_status = 'Rejected', rejected_by = $1, rejection_comment = $2, rejected_at = NOW() WHERE exposure_header_id = ANY($3::uuid[]) RETURNING *`,
			rejectedBy,
			req.RejectionComment,
			pq.Array(req.ExposureHeaderIds),
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
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			rejected = append(rejected, rowMap)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"rejected": rejected,
		})
	}
}

// Handler: approveMultipleExposureHeaders - approves multiple headers and handles business logic
func ApproveMultipleExposureHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposureHeaderIds"`
			ApprovalComment   string   `json:"approval_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "exposureHeaderIds and user_id are required")
			return
		}

		// Get session info for ApprovedBy
		var approvedBy string
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				approvedBy = s.Name // or s.Email
				break
			}
		}
		if approvedBy == "" {
			respondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		tx, err := db.Begin()
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback()
		// Fetch current statuses and details
		rows, err := tx.Query(`
			SELECT exposure_header_id, approval_status, exposure_type, status, document_id, total_original_amount, total_open_amount, additional_header_details
			FROM exposure_headers WHERE exposure_header_id = ANY($1::uuid[])`, pq.Array(req.ExposureHeaderIds))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		type headerRow struct {
			ExposureHeaderId    string
			ApprovalStatus      string
			ExposureType        string
			Status              string
			DocumentId          string
			TotalOriginalAmount float64
			TotalOpenAmount     float64
			AdditionalDetails   []byte
		}
		headers := []headerRow{}
		for rows.Next() {
			var h headerRow
			if err := rows.Scan(
				&h.ExposureHeaderId,
				&h.ApprovalStatus,
				&h.ExposureType,
				&h.Status,
				&h.DocumentId,
				&h.TotalOriginalAmount,
				&h.TotalOpenAmount,
				&h.AdditionalDetails,
			); err == nil {
				headers = append(headers, h)
			}
		}
		toDelete := []string{}
		toApprove := []string{}
		skipped := []string{}
		for _, h := range headers {
			status := strings.ToLower(h.ApprovalStatus)
			if strings.Contains(status, "delete") {
				toDelete = append(toDelete, h.ExposureHeaderId)
			} else if status == "pending" || status == "rejected" {
				toApprove = append(toApprove, h.ExposureHeaderId)
			} else if status == "approved" {
				skipped = append(skipped, h.ExposureHeaderId)
			}
		}
		results := map[string]interface{}{
			"deleted":  []map[string]interface{}{},
			"approved": []map[string]interface{}{},
			"rolled":   []map[string]interface{}{},
			"skipped":  skipped,
		}
		// Handle delete-approval: delete header and line items
		if len(toDelete) > 0 {
			errors := []string{}
			// Delete from exposure_rollover_log
			if _, err := tx.Exec(`DELETE FROM exposure_rollover_log WHERE child_header_id = ANY($1::uuid[]) OR parent_header_id = ANY($1::uuid[])`, pq.Array(toDelete)); err != nil {
				errors = append(errors, "exposure_rollover_log: "+err.Error())
			}
			// Delete from exposure_bucketing
			if _, err := tx.Exec(`DELETE FROM exposure_bucketing WHERE exposure_header_id = ANY($1::uuid[])`, pq.Array(toDelete)); err != nil {
				errors = append(errors, "exposure_bucketing: "+err.Error())
			}
			// Delete from hedging_proposal
			if _, err := tx.Exec(`DELETE FROM hedging_proposal WHERE exposure_header_id = ANY($1::uuid[])`, pq.Array(toDelete)); err != nil {
				errors = append(errors, "hedging_proposal: "+err.Error())
			}
			// Log deleted headers before deletion
			delRows, errDel := tx.Query(`DELETE FROM exposure_headers WHERE exposure_header_id = ANY($1::uuid[]) RETURNING *`, pq.Array(toDelete))
			deletedHeaders := []map[string]interface{}{}
			if errDel != nil {
				errors = append(errors, "exposure_headers: "+errDel.Error())
			} else {
				delCols, _ := delRows.Columns()
				for delRows.Next() {
					vals := make([]interface{}, len(delCols))
					valPtrs := make([]interface{}, len(delCols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					if err := delRows.Scan(valPtrs...); err != nil {
						continue
					}
					rowMap := map[string]interface{}{}
					for i, col := range delCols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					deletedHeaders = append(deletedHeaders, rowMap)
				}
				delRows.Close()
			}
			// Delete from exposure_line_items
			if _, err := tx.Exec(`DELETE FROM exposure_line_items WHERE exposure_header_id = ANY($1::uuid[])`, pq.Array(toDelete)); err != nil {
				errors = append(errors, "exposure_line_items: "+err.Error())
			}
			results["deleted"] = deletedHeaders
			// Reverse rollover if needed
			for _, h := range deletedHeaders {
				var parentDocNo string
				var typeStr string
				var addDetails map[string]interface{}
				if v, ok := h["exposure_type"].(string); ok {
					typeStr = strings.ToLower(v)
				}
				if v, ok := h["additional_header_details"].(map[string]interface{}); ok {
					addDetails = v
				}
				if typeStr == "lc" && addDetails != nil {
					if lc, ok := addDetails["input_letters_of_credit"].(map[string]interface{}); ok {
						if linked, ok := lc["linked_po_so_number"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "grn" && addDetails != nil {
					if grn, ok := addDetails["input_grn"].(map[string]interface{}); ok {
						if linked, ok := grn["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "creditors" && addDetails != nil {
					if cred, ok := addDetails["input_creditors"].(map[string]interface{}); ok {
						if linked, ok := cred["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "debitors" && addDetails != nil {
					if deb, ok := addDetails["input_debitors"].(map[string]interface{}); ok {
						if linked, ok := deb["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				}
				if parentDocNo != "" {
					var parentId string
					err := tx.QueryRow(`SELECT exposure_header_id FROM exposure_headers WHERE document_id = $1 LIMIT 1`, parentDocNo).Scan(&parentId)
					if err == nil {
						amt := 0.0
						if v, ok := h["total_open_amount"].(float64); ok {
							amt = math.Abs(v)
						} else if v, ok := h["total_original_amount"].(float64); ok {
							amt = math.Abs(v)
						}
						_, _ = tx.Exec(`UPDATE exposure_headers SET total_open_amount = total_open_amount + $1, status = 'Open' WHERE exposure_header_id = $2`, amt, parentId)
					}
				}
			}
		}

		// Approve remaining headers
		if len(toApprove) > 0 {
			appRows, _ := tx.Query(`UPDATE exposure_headers SET approval_status = 'Approved', approved_by = $1, approval_comment = $2, approved_at = NOW() WHERE exposure_header_id = ANY($3::uuid[]) RETURNING *`, approvedBy, req.ApprovalComment, pq.Array(toApprove))
			approvedHeaders := []map[string]interface{}{}
			appCols, _ := appRows.Columns()
			for appRows.Next() {
				vals := make([]interface{}, len(appCols))
				valPtrs := make([]interface{}, len(appCols))
				for i := range vals {
					valPtrs[i] = &vals[i]
				}
				if err := appRows.Scan(valPtrs...); err != nil {
					continue
				}
				rowMap := map[string]interface{}{}
				for i, col := range appCols {
					rowMap[col] = parseDBValue(col, vals[i])
				}
				approvedHeaders = append(approvedHeaders, rowMap)
			}
			appRows.Close()
			results["approved"] = approvedHeaders
			// Rollover logic
			for _, h := range approvedHeaders {
				var parentDocNo string
				var typeStr string
				var addDetails map[string]interface{}
				if v, ok := h["exposure_type"].(string); ok {
					typeStr = strings.ToLower(v)
				}
				if v, ok := h["additional_header_details"].(map[string]interface{}); ok {
					addDetails = v
				}
				if typeStr == "lc" && addDetails != nil {
					if lc, ok := addDetails["input_letters_of_credit"].(map[string]interface{}); ok {
						if linked, ok := lc["linked_po_so_number"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "grn" && addDetails != nil {
					if grn, ok := addDetails["input_grn"].(map[string]interface{}); ok {
						if linked, ok := grn["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "creditors" && addDetails != nil {
					if cred, ok := addDetails["input_creditors"].(map[string]interface{}); ok {
						if linked, ok := cred["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "debitors" && addDetails != nil {
					if deb, ok := addDetails["input_debitors"].(map[string]interface{}); ok {
						if linked, ok := deb["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				}
				if parentDocNo != "" {
					var parentId string
					err := tx.QueryRow(`SELECT exposure_header_id FROM exposure_headers WHERE document_id = $1 LIMIT 1`, parentDocNo).Scan(&parentId)
					if err == nil {
						amt := 0.0
						if v, ok := h["total_original_amount"].(float64); ok {
							amt = math.Abs(v)
						}
						_, _ = tx.Exec(`UPDATE exposure_headers SET total_open_amount = total_open_amount - $1, status = 'Rolled' WHERE exposure_header_id = $2`, amt, parentId)
						_, _ = tx.Exec(`UPDATE exposure_headers SET status = 'Rolled' WHERE exposure_header_id = $1`, h["exposure_header_id"])
						_, _ = tx.Exec(`INSERT INTO exposure_rollover_log (parent_header_id, child_header_id, rollover_amount, rollover_date, created_at) VALUES ($1, $2, $3, CURRENT_DATE, NOW())`, parentId, h["exposure_header_id"], amt)
						if rolled, ok := results["rolled"].([]map[string]interface{}); ok {
							rolled = append(rolled, h)
							results["rolled"] = rolled
						}
					}
				}
			}
		}
		tx.Commit()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"deleted":  results["deleted"],
			"approved": results["approved"],
			"rolled":   results["rolled"],
			"skipped":  results["skipped"],
		})
	}
}

// Handler: BatchUploadStagingData - handles batch upload for staging tables
func BatchUploadStagingData(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse multipart form
		err := r.ParseMultipartForm(32 << 20) // 32MB max
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid multipart form: "+err.Error())
			return
		}
		userID := r.FormValue("user_id")
		if userID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}
		// Get buNames from context (from middleware)
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		// Supported file fields
		fileFields := []struct {
			Field     string
			DataType  string
			TableName string
		}{
			{"input_grn", "grn", "input_grn"},
			{"input_letters_of_credit", "LC", "input_letters_of_credit"},
			{"input_purchase_orders", "PO", "input_purchase_orders"},
			{"input_sales_orders", "SO", "input_sales_orders"},
			{"input_creditors", "creditors", "input_creditors"},
			{"input_debitors", "debitors", "input_debitors"},
		}
		results := []map[string]interface{}{}
		absorptionErrors := []string{}
		for _, field := range fileFields {
			files := r.MultipartForm.File[field.Field]
			for _, fh := range files {
				filename := fh.Filename
				tempFile, err := os.CreateTemp("", "upload-*")
				// if err != nil {
				// 	results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to save file"})
				// 	continue
				// }
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to open file"})
					tempFile.Close()
					os.Remove(tempFile.Name())
					continue
				}
				for _, fh := range files {
					filename := fh.Filename
					tempFile, err := os.CreateTemp("", "upload-*")
					if err != nil {
						results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to save file"})
						continue
					}
					f, err := fh.Open()
					if err != nil {
						results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to open file"})
						tempFile.Close()
						os.Remove(tempFile.Name())
						continue
					}
					io.Copy(tempFile, f)
					f.Close()
					tempFile.Close()
					var dataArr []map[string]interface{}
					ext := filepath.Ext(filename)
					if ext == ".csv" {
						file, _ := os.Open(tempFile.Name())
						reader := csv.NewReader(file)
						headers, err := reader.Read()
						if err != nil {
							results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to read CSV headers"})
							file.Close()
							os.Remove(tempFile.Name())
							continue
						}
						// Clean headers - remove BOM and trim whitespace
						for i, h := range headers {
							// Remove UTF-8 BOM if present
							h = strings.TrimPrefix(h, "\ufeff")
							// Remove other invisible characters and trim
							h = strings.TrimSpace(h)
							headers[i] = h
						}
						for {
							row, err := reader.Read()
							if err != nil {
								break
							}
							obj := map[string]interface{}{}
							for i, h := range headers {
								if h != "" { // Skip empty column names
									obj[h] = row[i]
								}
							}
							dataArr = append(dataArr, obj)
						}
						file.Close()
					} else if ext == ".xlsx" || ext == ".xls" {
						xl, err := excelize.OpenFile(tempFile.Name())
						if err != nil {
							results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to read Excel file"})
							os.Remove(tempFile.Name())
							continue
						}
						sheet := xl.GetSheetName(0)
						rows, err := xl.GetRows(sheet)
						if err != nil || len(rows) < 1 {
							results = append(results, map[string]interface{}{"filename": filename, "error": "No data in Excel file"})
							os.Remove(tempFile.Name())
							continue
						}
						headers := rows[0]
						// Clean headers - remove BOM and trim whitespace
						for i, h := range headers {
							// Remove UTF-8 BOM if present
							h = strings.TrimPrefix(h, "\ufeff")
							// Remove other invisible characters and trim
							h = strings.TrimSpace(h)
							headers[i] = h
						}
						for _, row := range rows[1:] {
							obj := map[string]interface{}{}
							for i, h := range headers {
								if h != "" { // Skip empty column names
									if i < len(row) {
										obj[h] = row[i]
									} else {
										obj[h] = nil
									}
								}
							}
							dataArr = append(dataArr, obj)
						}
					} else {
						results = append(results, map[string]interface{}{"filename": filename, "error": "Unsupported file type"})
						os.Remove(tempFile.Name())
						continue
					}
					var buCol string
					switch field.DataType {
					case "LC":
						buCol = "applicant_name"
					case "PO", "SO":
						buCol = "entity"
					case "creditors", "debitors", "grn":
						buCol = "company"
					}
					invalidRows := []map[string]interface{}{}
					for _, row := range dataArr {
						if buCol != "" {
							buVal, _ := row[buCol].(string)
							buVal = strings.TrimSpace(buVal)

							// Check if buVal matches any of the allowed business units (case-insensitive and trimmed)
							found := false
							for _, allowedBU := range buNames {
								if strings.EqualFold(buVal, strings.TrimSpace(allowedBU)) {
									found = true
									break
								}
							}

							if !found {
								ref := "(no ref)"
								for _, k := range []string{"reference_no", "document_no", "system_lc_number", "bank_reference"} {
									if v, ok := row[k].(string); ok && v != "" {
										ref = v
										break
									}
								}
								invalidRows = append(invalidRows, map[string]interface{}{
									"reference":     ref,
									"business_unit": buVal,
									"field":         buCol,
								})
							}
						}
					}
					if len(invalidRows) > 0 {
						allowedBUs := strings.Join(buNames, ", ")
						errorMsg := fmt.Sprintf("Access denied: %d row(s) contain business units not authorized for your account. Your account has access to: [%s]. Please verify the '%s' field values in your file.",
							len(invalidRows), allowedBUs, buCol)
						results = append(results, map[string]interface{}{
							"filename":             filename,
							"error":                errorMsg,
							"invalidRows":          invalidRows,
							"allowedBusinessUnits": buNames,
							"validationField":      buCol,
						})
						os.Remove(tempFile.Name())
						continue
					}
					// Get table columns to validate against
					tableColumnsRes, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = $1`, field.TableName)
					var validColumns map[string]bool = make(map[string]bool)
					if err == nil {
						defer tableColumnsRes.Close()
						for tableColumnsRes.Next() {
							var colName string
							if err := tableColumnsRes.Scan(&colName); err == nil {
								validColumns[colName] = true
							}
						}
					}

					uploadBatchId := uuid.New().String()
					insertedRows := 0
					for i, row := range dataArr {
						row["upload_batch_id"] = uploadBatchId
						row["row_number"] = i + 1

						// Normalize date fields
						for k, v := range row {
							if vStr, ok := v.(string); ok && vStr != "" {
								// Check if field name suggests it's a date field
								lowerK := strings.ToLower(k)
								if strings.Contains(lowerK, "date") ||
									strings.Contains(lowerK, "due") ||
									strings.Contains(lowerK, "maturity") ||
									strings.Contains(lowerK, "expiry") ||
									strings.Contains(lowerK, "valid") ||
									strings.Contains(lowerK, "created") ||
									strings.Contains(lowerK, "updated") ||
									strings.Contains(lowerK, "issued") ||
									strings.Contains(lowerK, "received") ||
									strings.Contains(lowerK, "payment") && strings.Contains(lowerK, "date") {
									if normalized := NormalizeDate(vStr); normalized != "" {
										row[k] = normalized
									}
								}
							}
						}

						keys := []string{}
						vals := []interface{}{}
						placeholders := []string{}
						idx := 1
						for k, v := range row {
							// Skip columns that don't exist in the table
							if len(validColumns) > 0 && !validColumns[k] {
								continue
							}
							keys = append(keys, k)
							vals = append(vals, v)
							placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
							idx++
						}
						if len(keys) == 0 {
							continue // Skip rows with no valid columns
						}
						query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", field.TableName, strings.Join(keys, ", "), strings.Join(placeholders, ", "))
						_, err := db.Exec(query, vals...)
						if err != nil {
							results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to insert row: " + err.Error()})
							db.Exec(fmt.Sprintf("DELETE FROM %s WHERE upload_batch_id = $1", field.TableName), uploadBatchId)
							os.Remove(tempFile.Name())
							break
						}
						insertedRows++
					}
					mappingRows, err := db.Query(`SELECT source_column_name, target_table_name, target_field_name FROM upload_mappings WHERE exposure_type = $1 ORDER BY target_table_name, target_field_name`, field.DataType)
					insertedFinalRows := 0
					if err == nil {
						defer mappingRows.Close()
						var mappings []struct {
							SourceCol   string
							TargetTable string
							TargetField string
						}
						for mappingRows.Next() {
							var src, tgtTable, tgtField string
							mappingRows.Scan(&src, &tgtTable, &tgtField)
							mappings = append(mappings, struct {
								SourceCol   string
								TargetTable string
								TargetField string
							}{src, tgtTable, tgtField})
						}
						stagedRows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s WHERE upload_batch_id = $1`, field.TableName), uploadBatchId)
						if err == nil {
							defer stagedRows.Close()
							stagedCols, _ := stagedRows.Columns()
							for stagedRows.Next() {
								stagedVals := make([]interface{}, len(stagedCols))
								stagedPtrs := make([]interface{}, len(stagedCols))
								for i := range stagedVals {
									stagedPtrs[i] = &stagedVals[i]
								}
								stagedRows.Scan(stagedPtrs...)
								staged := map[string]interface{}{}
								for i, col := range stagedCols {
									staged[col] = stagedVals[i]
								}
								header := map[string]interface{}{}
								headerDetails := map[string]interface{}{}
								for _, m := range mappings {
									if m.TargetTable == "exposure_headers" {
										var val interface{}
										switch m.SourceCol {
										case field.DataType:
											val = field.DataType
										case "Open":
											val = "Open"
										case "true":
											val = true
										case field.TableName:
											val = staged
										default:
											val = staged[m.SourceCol]
										}
										if m.TargetField == "additional_header_details" {
											headerDetails[m.SourceCol] = val
										} else {
											if strings.Contains(m.TargetField, "amount") {
												switch v := val.(type) {
												case float64:
													val = math.Abs(v)
												case string:
													if f, err := strconv.ParseFloat(v, 64); err == nil {
														val = math.Abs(f)
													}
												}
											}
											header[m.TargetField] = val
										}
									}
								}
								header["additional_header_details"] = headerDetails
								headerKeys := []string{}
								headerVals := []interface{}{}
								headerPlaceholders := []string{}
								idx := 1
								for k, v := range header {
									headerKeys = append(headerKeys, k)
									// Marshal map[string]interface{} to JSON for DB
									if m, ok := v.(map[string]interface{}); ok {
										jsonVal, err := json.Marshal(m)
										if err != nil {
											absorptionErrors = append(absorptionErrors, fmt.Sprintf("header marshal error for file %s, key %s: %v", filename, k, err))
											headerVals = append(headerVals, nil)
										} else {
											headerVals = append(headerVals, jsonVal)
										}
									} else {
										headerVals = append(headerVals, v)
									}
									headerPlaceholders = append(headerPlaceholders, fmt.Sprintf("$%d", idx))
									idx++
								}
								headerInsert := fmt.Sprintf("INSERT INTO exposure_headers (%s) VALUES (%s) RETURNING exposure_header_id", strings.Join(headerKeys, ", "), strings.Join(headerPlaceholders, ", "))
								var exposureHeaderId string
								err := db.QueryRow(headerInsert, headerVals...).Scan(&exposureHeaderId)
								if err != nil {
									absorptionErrors = append(absorptionErrors, fmt.Sprintf("header insert error for file %s: %v", filename, err))
									continue
								}
								insertedFinalRows++
								line := map[string]interface{}{}
								lineDetails := map[string]interface{}{}
								for _, m := range mappings {
									if m.TargetTable == "exposure_line_items" {
										var val interface{}
										switch m.SourceCol {
										case field.DataType:
											val = field.DataType
										case "1":
											val = 1
										case field.TableName:
											val = staged
										default:
											val = staged[m.SourceCol]
										}
										if m.TargetField == "additional_line_details" {
											lineDetails[m.SourceCol] = val
										} else {
											if strings.Contains(m.TargetField, "amount") {
												switch v := val.(type) {
												case float64:
													val = math.Abs(v)
												case string:
													if f, err := strconv.ParseFloat(v, 64); err == nil {
														val = math.Abs(f)
													}
												}
											}
											line[m.TargetField] = val
										}
									}
								}
								// Marshal lineDetails if needed
								if len(lineDetails) > 0 {
									jsonVal, err := json.Marshal(lineDetails)
									if err == nil {
										line["additional_line_details"] = jsonVal
									} else {
										absorptionErrors = append(absorptionErrors, fmt.Sprintf("line details marshal error for file %s: %v", filename, err))
										line["additional_line_details"] = nil
									}
								}
								line["exposure_header_id"] = exposureHeaderId
								delete(line, "linked_exposure_header_id")
								lineKeys := []string{}
								lineVals := []interface{}{}
								linePlaceholders := []string{}
								idx = 1
								for k, v := range line {
									lineKeys = append(lineKeys, k)
									// Marshal map[string]interface{} to JSON for DB
									if m, ok := v.(map[string]interface{}); ok {
										jsonVal, err := json.Marshal(m)
										if err != nil {
											absorptionErrors = append(absorptionErrors, fmt.Sprintf("line marshal error for file %s, key %s: %v", filename, k, err))
											lineVals = append(lineVals, nil)
										} else {
											lineVals = append(lineVals, jsonVal)
										}
									} else {
										lineVals = append(lineVals, v)
									}
									linePlaceholders = append(linePlaceholders, fmt.Sprintf("$%d", idx))
									idx++
								}
								lineInsert := fmt.Sprintf("INSERT INTO exposure_line_items (%s) VALUES (%s)", strings.Join(lineKeys, ", "), strings.Join(linePlaceholders, ", "))
								_, err = db.Exec(lineInsert, lineVals...)
								if err != nil {
									absorptionErrors = append(absorptionErrors, fmt.Sprintf("line item insert error for file %s: %v", filename, err))
								}
							}
						}
					}
					success := insertedFinalRows > 0
					msg := "Batch uploaded to staging table only"
					if success {
						msg = "Batch absorbed into exposures"
					}
					results = append(results, map[string]interface{}{
						"success":           success,
						"filename":          filename,
						"message":           msg,
						"uploadBatchId":     uploadBatchId,
						"insertedRows":      insertedRows,
						"insertedFinalRows": insertedFinalRows,
					})
					os.Remove(tempFile.Name())
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		var allErrors []string
		// Collect errors from absorptionErrors
		if len(absorptionErrors) > 0 {
			allErrors = append(allErrors, absorptionErrors...)
		}
		// Collect errors from results
		for _, res := range results {
			if errMsg, ok := res["error"].(string); ok && errMsg != "" {
				allErrors = append(allErrors, errMsg)
			}
			if success, ok := res["success"].(bool); ok && !success {
				// If not already in errors, add generic error
				if errMsg, ok := res["error"].(string); !ok || errMsg == "" {
					allErrors = append(allErrors, fmt.Sprintf("File %v failed to process.", res["filename"]))
				}
			}
		}
		if len(results) == 0 {
			allErrors = append(allErrors, "No files found.")
		}
		if len(allErrors) > 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   strings.Join(allErrors, "; "),
			})
			return
		}
		// All successful
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    results,
		})

	}
}
