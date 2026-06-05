package exposures

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/fx/auditutil"
	"CimplrCorpSaas/api/utils"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/xuri/excelize/v2"

	"CimplrCorpSaas/api/constants"

	"github.com/lib/pq"

	"CimplrCorpSaas/internal/logger"
)

var ErrExposureFileAlreadyUploaded = errors.New("exposure file already uploaded")

const duplicateExposureUploadMessage = "This exposure file was already uploaded earlier. Please upload a different file."

func existingExposureUploadKeys(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT upload_s3_key
		FROM exposure_headers
		WHERE COALESCE(TRIM(upload_s3_key), '') <> ''
		  AND COALESCE(is_deleted, false) = false
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]string, 0)
	for rows.Next() {
		var key string
		if scanErr := rows.Scan(&key); scanErr != nil {
			return nil, scanErr
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func ensureUniqueExposureUpload(ctx context.Context, db *sql.DB, fileBytes []byte) error {
	keys, err := existingExposureUploadKeys(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to check duplicate exposure upload: %w", err)
	}
	duplicateKey, err := s3storage.FindDuplicateObjectKey(ctx, fileBytes, keys)
	if err != nil {
		return fmt.Errorf("failed to compare exposure upload content: %w", err)
	}
	if duplicateKey != "" {
		return ErrExposureFileAlreadyUploaded
	}
	return nil
}

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
	logger.LogError("%s", errMsg)
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: false,
		constants.ValueError:   errMsg,
	})
}

func fetchExposurePermissions(db *sql.DB, userID string) map[string]interface{} {
	result := map[string]interface{}{}
	var roleId int
	if err := db.QueryRow("SELECT role_id FROM user_roles WHERE user_id = $1 LIMIT 1", userID).Scan(&roleId); err != nil {
		return result
	}
	rows, err := db.Query(`
		SELECT p.page_name, p.tab_name, p.action, rp.allowed
		FROM role_permissions rp
		JOIN permissions p ON rp.permission_id = p.id
		WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')
	`, roleId)
	if err != nil {
		return result
	}
	defer rows.Close()
	for rows.Next() {
		var pageName, tabName, action string
		var allowed bool
		if err := rows.Scan(&pageName, &tabName, &action, &allowed); err != nil {
			continue
		}
		if pageName != constants.ExposureUpload {
			continue
		}
		if result[constants.ExposureUpload] == nil {
			result[constants.ExposureUpload] = map[string]interface{}{}
		}
		perms := result[constants.ExposureUpload].(map[string]interface{})
		if tabName == "" {
			if perms["pagePermissions"] == nil {
				perms["pagePermissions"] = map[string]interface{}{}
			}
			perms["pagePermissions"].(map[string]interface{})[action] = allowed
		} else {
			if perms["tabs"] == nil {
				perms["tabs"] = map[string]interface{}{}
			}
			tabs := perms["tabs"].(map[string]interface{})
			if tabs[tabName] == nil {
				tabs[tabName] = map[string]interface{}{}
			}
			tabs[tabName].(map[string]interface{})[action] = allowed
		}
	}
	return result
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
	return utils.NormalizeDateString(dateStr)
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

func resolveUploadMappingValue(staged map[string]interface{}, sourceCol, dataType, tableName string) (interface{}, bool) {
	switch sourceCol {
	case dataType:
		return dataType, true
	case tableName:
		return staged, true
	case "1":
		return 1, true
	case "true":
		return true, true
	case "false":
		return false, true
	case "Open", "Pending", "Approved", "Rejected", "Delete-Approval":
		return sourceCol, true
	}

	val, ok := staged[sourceCol]
	return val, ok
}

func hasNonEmptyUploadValue(v interface{}) bool {
	switch t := v.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(t) != ""
	case []byte:
		return strings.TrimSpace(string(t)) != ""
	default:
		return true
	}
}

func exposureHeadersHasUploadBatchID(ctx context.Context, db *sql.DB) bool {
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_name = 'exposure_headers' AND column_name = 'upload_batch_id'
		)
	`).Scan(&exists); err != nil {
		logger.LogError("[FX-DOWNLOAD] failed to detect exposure_headers.upload_batch_id: %v", err)
		return false
	}
	return exists
}

func lookupExposureUploadS3Key(ctx context.Context, db *sql.DB, recordID string) (sql.NullString, error) {
	var uploadS3Key sql.NullString
	trimmedRecordID := strings.TrimSpace(recordID)
	if trimmedRecordID == "" {
		return sql.NullString{}, sql.ErrNoRows
	}

	err := db.QueryRowContext(ctx, `
		SELECT upload_s3_key
		FROM exposure_headers
		WHERE exposure_header_id::text = $1
		LIMIT 1
	`, trimmedRecordID).Scan(&uploadS3Key)
	if err == nil && strings.TrimSpace(uploadS3Key.String) != "" {
		return uploadS3Key, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return sql.NullString{}, err
	}

	err = db.QueryRowContext(ctx, `
		SELECT upload_s3_key
		FROM exposure_headers
		WHERE document_id = $1
		ORDER BY created_at DESC
		LIMIT 1
	`, trimmedRecordID).Scan(&uploadS3Key)
	if err == nil && strings.TrimSpace(uploadS3Key.String) != "" {
		return uploadS3Key, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return sql.NullString{}, err
	}
	return sql.NullString{}, sql.ErrNoRows
}

// Handler
func EditExposureHeadersLineItemsJoined(db *sql.DB) http.HandlerFunc {
	// ...existing code...
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     string                 `json:"id"` // exposure_header_id
			Fields map[string]interface{} `json:"fields"`
			UserID string                 `json:"user_id"`
			Reason string                 `json:"reason"`
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
		headerColsRes, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'exposure_headers'`)
		if err != nil {
			logger.LogError("failed to query header columns: %v", err)
			respondWithError(w, http.StatusInternalServerError, "failed to read table metadata")
			return
		}
		defer headerColsRes.Close()

		lineColsRes, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'exposure_line_items'`)
		if err != nil {
			logger.LogError("failed to query line columns: %v", err)
			respondWithError(w, http.StatusInternalServerError, "failed to read table metadata")
			return
		}
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
		oldValues := auditutil.FetchRowSnapshot(r.Context(), db, constants.ExposureHeaders, "exposure_header_id", exposureHeaderID)
		// Update header if needed
		if len(headerFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range headerFields {
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
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
				setParts = append(setParts, fmt.Sprintf(constants.FormatSQLColumnArg, k, i))
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 results,
		})
		newValues := req.Fields
		if len(results) > 0 {
			newValues = results[0]
		}
		auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: exposureHeaderID, ActionType: "EDIT", Status: constants.StatusPendingEditApproval, Reason: strings.TrimSpace(req.Reason), RequestedBy: auditutil.Actor(req.UserID), OldValues: oldValues, NewValues: newValues})
	}
}

// normalizeDateFields - shared helper, replaces the inline date loop in both handlers
func normalizeDateFields(rowMap map[string]interface{}) {
	for k, v := range rowMap {
		lk := strings.ToLower(k)
		if !strings.Contains(lk, "date") && !strings.HasSuffix(lk, "_at") {
			continue
		}
		switch tv := v.(type) {
		case string:
			if nd := NormalizeDate(tv); nd != "" {
				layouts := []string{constants.DateFormat, time.RFC3339, constants.DateFormatISO, constants.DateTimeFormat}
				parsed := false
				for _, l := range layouts {
					if t, err := time.Parse(l, nd); err == nil {
						rowMap[k] = t.Format(constants.DateFormatAlt)
						parsed = true
						break
					}
				}
				if !parsed {
					parts := strings.Split(nd, "-")
					if len(parts) == 3 && len(parts[0]) == 4 {
						rowMap[k] = parts[2] + "-" + parts[1] + "-" + parts[0]
					} else {
						rowMap[k] = nd
					}
				}
			}
		case time.Time:
			rowMap[k] = tv.Format(constants.DateFormatAlt)
		case *time.Time:
			if tv != nil {
				rowMap[k] = tv.Format(constants.DateFormatAlt)
			} else {
				rowMap[k] = ""
			}
		}
	}
}

// Handler: GetExposureHeadersLineItems
func GetExposureHeadersLineItems(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}

		ctx := r.Context()
		buNames := api.GetEntityNamesFromCtx(ctx)
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		joinRows, err := db.QueryContext(ctx, `
			SELECT
				h.exposure_header_id, h.company_code, h.entity, h.entity1, h.entity2, h.entity3,
				h.exposure_type, h.document_id, h.document_date, h.counterparty_type,
				h.counterparty_code, h.counterparty_name, h.currency,
				h.total_original_amount, h.total_open_amount, h.value_date,
				h.status, h.is_active, h.created_at, h.updated_at,
				h.approval_status, h.approval_comment, h.approved_by,
				h.delete_comment, h.requested_by, h.rejection_comment,
				h.approved_at, h.rejected_by, h.rejected_at,
				h.amount_in_local_currency, h.posting_date, h.text,
				h.gl_account, h.reference, h.additional_header_details,
				h.exposure_category, h.exposure_creation_status, h.batch_id,
				h.upload_s3_key,
				l.line_item_id, l.line_number, l.product_id, l.product_description,
				l.quantity, l.unit_of_measure, l.unit_price, l.line_item_amount,
				l.plant_code, l.delivery_date, l.payment_terms, l.inco_terms,
				l.additional_line_details, l.created_at AS line_created_at
			FROM exposure_headers h
			JOIN exposure_line_items l ON l.exposure_header_id = h.exposure_header_id
			WHERE h.entity = ANY($1)
			  AND h.exposure_creation_status = 'Approved'
			  AND h.is_deleted IS NOT TRUE
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
						rowMap[col] = ""
					}
					continue
				}
				if s, ok := pv.(string); ok && s == "" {
					if _, exists := rowMap[col]; exists {
						continue
					}
				}
				rowMap[col] = pv
			}
			normalizeDateFields(rowMap)
			joinData = append(joinData, rowMap)
		}

		resp := map[string]interface{}{
			"buAccessible": buNames,
			"pageData":     joinData,
		}
		logger.LogInfo("[DEBUG] GetExposureHeadersLineItems: buAccessible=%d, pageData=%d", len(buNames), len(joinData))
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		b, jerr := json.Marshal(resp)
		if jerr != nil {
			respondWithError(w, http.StatusInternalServerError, "JSON marshal failed: "+jerr.Error())
			return
		}
		if _, werr := w.Write(b); werr != nil {
			logger.LogError("Failed to write response: %v", werr)
		}
	}
}

// Handler: GetPendingApprovalHeadersLineItems
func GetPendingApprovalHeadersLineItems(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}

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

		ctx := r.Context()
		buNames := api.GetEntityNamesFromCtx(ctx)
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		joinRows, err := db.QueryContext(ctx, `
			SELECT
				h.exposure_header_id, h.company_code, h.entity, h.entity1, h.entity2, h.entity3,
				h.exposure_type, h.document_id, h.document_date, h.counterparty_type,
				h.counterparty_code, h.counterparty_name, h.currency,
				h.total_original_amount, h.total_open_amount, h.value_date,
				h.status, h.is_active, h.created_at, h.updated_at,
				h.approval_status, h.approval_comment, h.approved_by,
				h.delete_comment, h.requested_by, h.rejection_comment,
				h.approved_at, h.rejected_by, h.rejected_at,
				h.amount_in_local_currency, h.posting_date, h.text,
				h.gl_account, h.reference, h.additional_header_details,
				h.exposure_category, h.exposure_creation_status, h.batch_id,
				h.upload_s3_key,
				l.line_item_id, l.line_number, l.product_id, l.product_description,
				l.quantity, l.unit_of_measure, l.unit_price, l.line_item_amount,
				l.plant_code, l.delivery_date, l.payment_terms, l.inco_terms,
				l.additional_line_details, l.created_at AS line_created_at
			FROM exposure_headers h
			JOIN exposure_line_items l ON l.exposure_header_id = h.exposure_header_id
			WHERE h.entity = ANY($1)
			  AND h.exposure_creation_status = 'Approved'
			  AND h.is_deleted IS NOT TRUE
			  AND upper(h.approval_status) <> 'APPROVED'
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
						rowMap[col] = ""
					}
					continue
				}
				if s, ok := pv.(string); ok && s == "" {
					if _, exists := rowMap[col]; exists {
						continue
					}
				}
				rowMap[col] = pv
			}
			normalizeDateFields(rowMap)
			joinData = append(joinData, rowMap)
		}

		resp := map[string]interface{}{
			"buAccessible": buNames,
			"pageData":     joinData,
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
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
			Reason            string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrExposureHeaderIDsUserID)
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
			respondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		// Mark for delete approval first
		deleteComment := req.DeleteComment
		if strings.TrimSpace(deleteComment) == "" {
			deleteComment = req.Reason
		}

		res, err := db.Exec(
			`UPDATE exposure_headers
			 SET approval_status = 'PENDING_DELETE_APPROVAL',
			     delete_comment = $1,
			     requested_by = $2
			 WHERE exposure_header_id = ANY($3::uuid[])
			   AND COALESCE(is_deleted, false) = false`,
			deleteComment,
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
				constants.ValueSuccess: false,
				"message":              "No matching exposure_headers found",
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"message":              fmt.Sprintf("%d exposure_header(s) marked for delete approval", count),
		})
		for _, id := range req.ExposureHeaderIds {
			auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: id, ActionType: "DELETE", Status: constants.StatusPendingDeleteApproval, Reason: deleteComment, RequestedBy: requestedBy, OldValues: nil, NewValues: nil})
		}
	}
}

// Handler: rejectMultipleExposureHeaders - rejects multiple headers
func RejectMultipleExposureHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIds []string `json:"exposureHeaderIds"`
			RejectionComment  string   `json:"rejection_comment"`
			Comments          string   `json:"comments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrExposureHeaderIDsUserID)
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
			respondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		rejectionComment := strings.TrimSpace(req.RejectionComment)
		if rejectionComment == "" {
			rejectionComment = strings.TrimSpace(req.Comments)
		}

		oldValuesByID := make(map[string]map[string]interface{}, len(req.ExposureHeaderIds))
		for _, id := range req.ExposureHeaderIds {
			oldValuesByID[id] = auditutil.FetchRowSnapshot(r.Context(), db, constants.ExposureHeaders, "exposure_header_id", id)
		}

		rows, err := db.Query(
			`UPDATE exposure_headers
			 SET approval_status = 'REJECTED',
			     rejected_by = $1,
			     rejection_comment = $2,
			     rejected_at = NOW()
			 WHERE exposure_header_id = ANY($3::uuid[])
			   AND COALESCE(is_deleted, false) = false
			 RETURNING *`,
			rejectedBy,
			rejectionComment,
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
			if id, ok := rowMap["exposure_header_id"]; ok {
				parentID := fmt.Sprint(id)
				auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: parentID, Status: constants.StatusRejected, CheckerBy: rejectedBy, Comment: rejectionComment})
				auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: parentID, ActionType: "REJECT", Status: constants.StatusRejected, Reason: rejectionComment, RequestedBy: rejectedBy, OldValues: oldValuesByID[parentID], NewValues: rowMap})
			}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"rejected":             rejected,
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
			respondWithError(w, http.StatusBadRequest, constants.ErrExposureHeaderIDsUserID)
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
			respondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		approvalComment := strings.TrimSpace(req.ApprovalComment)

		oldValuesByID := make(map[string]map[string]interface{}, len(req.ExposureHeaderIds))
		for _, id := range req.ExposureHeaderIds {
			oldValuesByID[id] = auditutil.FetchRowSnapshot(r.Context(), db, constants.ExposureHeaders, "exposure_header_id", id)
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
			if status == "pending_delete_approval" || strings.Contains(status, "delete-approval") {
				toDelete = append(toDelete, h.ExposureHeaderId)
			} else if status == "pending" || status == "rejected" || status == "pending_approval" || status == "pending_edit_approval" {
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
		var approvalErr error
		// Handle delete approval as a soft delete on the master row.
		if len(toDelete) > 0 {
			delRows, errDel := tx.Query(`
				UPDATE exposure_headers
				SET approval_status = 'APPROVED',
				    approved_by = $1,
				    approval_comment = $2,
				    approved_at = NOW(),
				    is_deleted = TRUE,
				    deleted_at = NOW(),
				    deleted_by = $1
				WHERE exposure_header_id = ANY($3::uuid[])
				  AND COALESCE(is_deleted, false) = false
				RETURNING *
			`, approvedBy, approvalComment, pq.Array(toDelete))
			deletedHeaders := []map[string]interface{}{}
			if errDel != nil {
				approvalErr = errDel
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
			appRows, err := tx.Query(`UPDATE exposure_headers SET approval_status = 'Approved', approved_by = $1, approval_comment = $2, approved_at = NOW() WHERE exposure_header_id = ANY($3::uuid[]) RETURNING *`, approvedBy, approvalComment, pq.Array(toApprove))
			if err != nil {
				logger.LogError("[WARN] approving exposure headers failed: %v", err)
				approvalErr = err
			} else {
				defer appRows.Close()
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
		}

		if approvalErr != nil {
			respondWithError(w, http.StatusInternalServerError, approvalErr.Error())
			return
		}

		if len(toDelete) == 0 && len(toApprove) == 0 {
			respondWithError(w, http.StatusBadRequest, "No eligible exposure headers found for approval")
			return
		}

		if err := tx.Commit(); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for _, row := range results["approved"].([]map[string]interface{}) {
			id := fmt.Sprint(row["exposure_header_id"])
			// Update the existing pending audit row in place instead of creating a new APPROVE row.
			auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: id, Status: constants.StatusApproved, CheckerBy: approvedBy, Comment: approvalComment})
		}
		for _, row := range results["deleted"].([]map[string]interface{}) {
			id := fmt.Sprint(row["exposure_header_id"])
			// Update the existing pending audit row in place instead of creating a new APPROVE row.
			auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableExposure, ParentColumn: "exposure_header_id", ParentID: id, Status: constants.StatusApproved, CheckerBy: approvedBy, Comment: approvalComment})
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"deleted":              results["deleted"],
			"approved":             results["approved"],
			"rolled":               results["rolled"],
			"skipped":              results["skipped"],
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
		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}

		// Validate session
		sessions := auth.GetActiveSessions()
		var session *auth.UserSession
		for _, s := range sessions {
			if s.UserID == userID {
				session = s
				break
			}
		}
		if session == nil {
			respondWithError(w, http.StatusNotFound, "Session expired or not found. Please login again.")
			return
		}

		// Prefer prevalidation context if available, otherwise fallback to DB-approved entity lookup
		ctx := r.Context()
		buNames := api.GetEntityNamesFromCtx(ctx)
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		results, absorptionErrors, err := processBatchUploadStagingData(ctx, db, r, buNames, session)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		var allErrors []string
		// Collect errors from absorptionErrors
		if len(absorptionErrors) > 0 {
			allErrors = append(allErrors, absorptionErrors...)
		}
		// Collect errors from results
		for _, res := range results {
			if errMsg, ok := res[constants.ValueError].(string); ok && errMsg != "" {
				allErrors = append(allErrors, errMsg)
			}
			if success, ok := res[constants.ValueSuccess].(bool); ok && !success {
				// If not already in errors, add generic error
				if errMsg, ok := res[constants.ValueError].(string); !ok || errMsg == "" {
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
				constants.ValueSuccess: false,
				constants.ValueError:   strings.Join(allErrors, "; "),
			})
			return
		}
		// All successful
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 results,
		})

	}
}

func GetExposureDownloadURL(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RecordID string `json:"record_id"`
			UserID   string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		recordID := strings.TrimSpace(req.RecordID)
		if recordID == "" {
			respondWithError(w, http.StatusBadRequest, "record_id is required")
			return
		}
		uploadS3Key, err := lookupExposureUploadS3Key(r.Context(), db, recordID)
		if err != nil {
			if err == sql.ErrNoRows {
				respondWithError(w, http.StatusNotFound, "record not found")
				return
			}
			respondWithError(w, http.StatusInternalServerError, "failed to fetch record")
			return
		}
		s3Key := strings.TrimSpace(uploadS3Key.String)
		if !uploadS3Key.Valid || s3Key == "" {
			respondWithError(w, http.StatusNotFound, "no file available for download")
			return
		}
		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), s3Key, 15*time.Minute)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to generate download url")
			return
		}
		recordExposureDownloadAudit(r.Context(), db, recordID, exposureDownloadActor(r.Context(), req.UserID), s3Key)
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data": map[string]interface{}{
				"download_url": downloadURL,
			},
		})
	}
}

func normalizeBulkIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func exposureDownloadActor(ctx context.Context, userID string) string {
	requestedBy := strings.TrimSpace(api.RequestedByFromCtx(ctx, userID))
	if requestedBy == "" {
		requestedBy = strings.TrimSpace(auditutil.ActorFromContext(ctx))
	}
	if requestedBy == "" {
		requestedBy = strings.TrimSpace(userID)
	}
	return requestedBy
}

func recordExposureDownloadAudit(ctx context.Context, db *sql.DB, exposureHeaderID, requestedBy, uploadS3Key string) {
	auditutil.RecordDownload(ctx, db, auditutil.DownloadParams{TableName: auditutil.TableExposureDownloads, ParentColumn: "exposure_header_id", ParentID: exposureHeaderID, RequestedBy: requestedBy, UploadS3Key: uploadS3Key, ExtraColumns: nil})
}

func writeBulkDownloadResponse(w http.ResponseWriter, files []map[string]string, failedIDs []string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	if len(files) == 0 {
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: false,
			"message":              "no downloadable files found",
			"data": map[string]interface{}{
				"files":      []map[string]string{},
				"failed_ids": failedIDs,
			},
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: true,
		"data": map[string]interface{}{
			"files":      files,
			"failed_ids": failedIDs,
		},
	})
}

func GetExposureBulkDownloadURL(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UploadBatchIDs []string `json:"upload_batch_ids"`
			RecordIDs      []string `json:"record_ids"`
			UserID         string   `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		recordIDs := normalizeBulkIDs(req.RecordIDs)
		if len(recordIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "record_ids is required")
			return
		}

		ctx := r.Context()
		files := make([]map[string]string, 0, len(recordIDs))
		failedIDs := make([]string, 0)
		requestedBy := exposureDownloadActor(ctx, req.UserID)

		for _, recordID := range recordIDs {
			uploadS3Key, err := lookupExposureUploadS3Key(ctx, db, recordID)
			if err != nil {
				failedIDs = append(failedIDs, recordID)
				continue
			}

			s3Key := strings.TrimSpace(uploadS3Key.String)
			if !uploadS3Key.Valid || s3Key == "" {
				failedIDs = append(failedIDs, recordID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, s3Key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, recordID)
				continue
			}

			files = append(files, map[string]string{
				"record_id":    recordID,
				"download_url": downloadURL,
			})
			recordExposureDownloadAudit(ctx, db, recordID, requestedBy, s3Key)
		}

		writeBulkDownloadResponse(w, files, failedIDs)
	}
}

func processBatchUploadStagingData(ctx context.Context, db *sql.DB, r *http.Request, buNames []string, session *auth.UserSession) ([]map[string]interface{}, []string, error) {
	uploadedBy := "user"
	if session != nil {
		uploadedBy = strings.TrimSpace(session.Name)
		if uploadedBy == "" {
			uploadedBy = strings.TrimSpace(session.UserID)
		}
	}

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
			func() {
				filename := fh.Filename
				var s3Key string
				var uploadedNewObject bool
				var tx *sql.Tx
				var txCommitted bool

				tempFile, err := os.CreateTemp("", "upload-*")
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: constants.ErrFailedToOpenFile})
					if tempFile != nil {
						tempFile.Close()
						os.Remove(tempFile.Name())
					}
					return
				}

				tempPath := tempFile.Name()
				defer os.Remove(tempPath)
				defer func() {
					if tx != nil && !txCommitted {
						_ = tx.Rollback()
					}
					if uploadedNewObject && !txCommitted && s3Key != "" {
						if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
							logger.LogError("[FX-STAGING-S3] cleanup failed for key=%s: %v", s3Key, cleanupErr)
						}
					}
				}()

				f, err := fh.Open()
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: constants.ErrFailedToOpenFile})
					tempFile.Close()
					return
				}
				if _, err := io.Copy(tempFile, f); err != nil {
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: constants.ErrFailedToOpenFile})
					f.Close()
					tempFile.Close()
					return
				}
				f.Close()
				tempFile.Close()

				var dataArr []map[string]interface{}
				ext := filepath.Ext(filename)
				if ext == ".csv" {
					file, err := os.Open(tempPath)
					if err != nil {
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: constants.ErrFailedToOpenFile})
						return
					}
					reader := csv.NewReader(file)
					headers, err := reader.Read()
					if err != nil {
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: constants.ErrFailedToReadCSVHeaders})
						file.Close()
						return
					}
					for i, h := range headers {
						h = strings.TrimPrefix(h, constants.ErrInvalidJSONBOM)
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
							if h != "" {
								obj[h] = row[i]
							}
						}
						dataArr = append(dataArr, obj)
					}
					file.Close()
				} else if ext == ".xlsx" || ext == ".xls" {
					xl, err := excelize.OpenFile(tempPath)
					if err != nil {
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to read Excel file"})
						return
					}
					sheet := xl.GetSheetName(0)
					rows, err := xl.GetRows(sheet)
					if err != nil || len(rows) < 1 {
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "No data in Excel file"})
						return
					}
					headers := rows[0]
					for i, h := range headers {
						h = strings.TrimPrefix(h, constants.ErrInvalidJSONBOM)
						h = strings.TrimSpace(h)
						headers[i] = h
					}
					for _, row := range rows[1:] {
						obj := map[string]interface{}{}
						for i, h := range headers {
							if h != "" {
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
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: constants.ErrUnsupportedFileType})
					return
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
					if buCol == "" {
						continue
					}
					buVal, _ := row[buCol].(string)
					buVal = strings.TrimSpace(buVal)

					found := false
					for _, allowedBU := range buNames {
						if strings.EqualFold(buVal, strings.TrimSpace(allowedBU)) {
							found = true
							break
						}
					}
					if found {
						continue
					}

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
				if len(invalidRows) > 0 {
					allowedBUs := strings.Join(buNames, ", ")
					errorMsg := fmt.Sprintf("Access denied: %d row(s) contain business units not authorized for your account. Your account has access to: [%s]. Please verify the '%s' field values in your file.",
						len(invalidRows), allowedBUs, buCol)
					results = append(results, map[string]interface{}{
						"filename":             filename,
						constants.ValueError:   errorMsg,
						"invalidRows":          invalidRows,
						"allowedBusinessUnits": buNames,
						"validationField":      buCol,
					})
					return
				}

				fileBytes, err := os.ReadFile(tempPath)
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: constants.ErrFailedToOpenFile})
					return
				}
				if s3storage.IsS3UploadEnabled() {
					if err := ensureUniqueExposureUpload(ctx, db, fileBytes); err != nil {
						message := err.Error()
						if errors.Is(err, ErrExposureFileAlreadyUploaded) {
							message = duplicateExposureUploadMessage
						}
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: message})
						return
					}
				}

				exposureTypeFolder, err := batchStagingExposureTypeFolder(field.DataType)
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: err.Error()})
					return
				}
				s3Key = s3storage.BuildUploadedS3Key("fx/exposures", exposureTypeFolder, filename, uploadedBy, time.Now().UTC())
				logger.LogInfo("[FX-STAGING] UPLOAD START: filename=%s, field.DataType=%s, s3Key=%s, S3Enabled=%v",
					filename, field.DataType, s3Key, s3storage.IsS3UploadEnabled())
				if s3storage.IsS3UploadEnabled() {
					if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, s3storage.DetectContentType(fileBytes)); err != nil {
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to upload file to S3: " + err.Error()})
						return
					}
					uploadedNewObject = true
					logger.LogInfo("[FX-STAGING] S3 UPLOAD SUCCESS: s3Key=%s", s3Key)
				} else {
					logger.LogInfo("[FX-STAGING] S3 DISABLED - file will not be uploaded to S3!")
				}

				tableColumnsRes, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = $1`, field.TableName)
				validColumns := make(map[string]bool)
				if err == nil {
					defer tableColumnsRes.Close()
					for tableColumnsRes.Next() {
						var colName string
						if err := tableColumnsRes.Scan(&colName); err == nil {
							validColumns[colName] = true
						}
					}
				}

				tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to start DB transaction: " + err.Error()})
					return
				}

				hasHeaderUploadBatchID := false
				if err := tx.QueryRowContext(ctx, `
					SELECT EXISTS (
						SELECT 1
						FROM information_schema.columns
						WHERE table_name = 'exposure_headers' AND column_name = 'upload_batch_id'
					)
				`).Scan(&hasHeaderUploadBatchID); err != nil {
					logger.LogError("[FX-STAGING] failed to detect exposure_headers.upload_batch_id; proceeding without it: %v", err)
					hasHeaderUploadBatchID = false
				}

				persistedS3Key := ""
				if uploadedNewObject {
					persistedS3Key = s3Key
				}

				uploadBatchId := uuid.New().String()
				insertedRows := 0
				createdExposureIDs := []string{}
				for i, row := range dataArr {
					row["upload_batch_id"] = uploadBatchId
					row["row_number"] = i + 1

					for k, v := range row {
						if vStr, ok := v.(string); ok && vStr != "" {
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
						if len(validColumns) > 0 && !validColumns[k] {
							continue
						}
						keys = append(keys, k)
						vals = append(vals, v)
						placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
						idx++
					}
					if len(keys) == 0 {
						continue
					}
					query := fmt.Sprintf(constants.ErrInsertFailed, field.TableName, strings.Join(keys, ", "), strings.Join(placeholders, ", "))
					if _, err := tx.ExecContext(ctx, query, vals...); err != nil {
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to insert row: " + err.Error()})
						return
					}
					insertedRows++
				}

				mappingRows, err := tx.QueryContext(ctx, `
					SELECT source_column_name, target_table_name, target_field_name
					FROM upload_mappings
					WHERE LOWER(TRIM(exposure_type)) = LOWER(TRIM($1))
					ORDER BY target_table_name, target_field_name
				`, field.DataType)
				insertedFinalRows := 0
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to fetch mapping rows: " + err.Error()})
					return
				}
				var mappings []struct {
					SourceCol   string
					TargetTable string
					TargetField string
				}
				for mappingRows.Next() {
					var src, tgtTable, tgtField string
					if err := mappingRows.Scan(&src, &tgtTable, &tgtField); err != nil {
						mappingRows.Close()
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to scan mapping row: " + err.Error()})
						return
					}
					mappings = append(mappings, struct {
						SourceCol   string
						TargetTable string
						TargetField string
					}{src, tgtTable, tgtField})
				}
				if err := mappingRows.Err(); err != nil {
					mappingRows.Close()
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed while reading mapping rows: " + err.Error()})
					return
				}
				mappingRows.Close()
				if len(mappings) == 0 {
					results = append(results, map[string]interface{}{
						"filename":           filename,
						constants.ValueError: fmt.Sprintf("No upload_mappings found for exposure type %q", field.DataType),
					})
					return
				}

				hasHeaderMapping := false
				hasLineItemMapping := false
				for _, m := range mappings {
					if m.TargetTable == "exposure_headers" {
						hasHeaderMapping = true
					}
					if m.TargetTable == "exposure_line_items" {
						hasLineItemMapping = true
					}
				}
				if !hasHeaderMapping || !hasLineItemMapping {
					missingTables := []string{}
					if !hasHeaderMapping {
						missingTables = append(missingTables, "exposure_headers")
					}
					if !hasLineItemMapping {
						missingTables = append(missingTables, "exposure_line_items")
					}
					results = append(results, map[string]interface{}{
						"filename":           filename,
						constants.ValueError: fmt.Sprintf("upload_mappings for exposure type %q are incomplete; missing target table mappings for %s", field.DataType, strings.Join(missingTables, ", ")),
					})
					return
				}

				stagedRows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s WHERE upload_batch_id = $1`, field.TableName), uploadBatchId)
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to fetch staged rows: " + err.Error()})
					return
				}
				stagedCols, _ := stagedRows.Columns()
				stagedData := make([]map[string]interface{}, 0)
				for stagedRows.Next() {
					stagedVals := make([]interface{}, len(stagedCols))
					stagedPtrs := make([]interface{}, len(stagedCols))
					for i := range stagedVals {
						stagedPtrs[i] = &stagedVals[i]
					}
					if err := stagedRows.Scan(stagedPtrs...); err != nil {
						stagedRows.Close()
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to scan staged row: " + err.Error()})
						return
					}
					staged := map[string]interface{}{}
					for i, col := range stagedCols {
						staged[col] = stagedVals[i]
					}
					stagedData = append(stagedData, staged)
				}
				if err := stagedRows.Err(); err != nil {
					stagedRows.Close()
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed while reading staged rows: " + err.Error()})
					return
				}
				stagedRows.Close()
				if len(stagedData) == 0 {
					results = append(results, map[string]interface{}{
						"filename":           filename,
						constants.ValueError: fmt.Sprintf("No staged rows were found in %s for upload_batch_id %s", field.TableName, uploadBatchId),
					})
					return
				}

				for _, staged := range stagedData {
					header := map[string]interface{}{}
					headerDetails := map[string]interface{}{}
					parseAbsFloat := func(v interface{}) (float64, bool) {
						switch t := v.(type) {
						case float64:
							return math.Abs(t), true
						case float32:
							return math.Abs(float64(t)), true
						case int:
							return math.Abs(float64(t)), true
						case int64:
							return math.Abs(float64(t)), true
						case string:
							s := strings.TrimSpace(t)
							if s == "" {
								return 0, false
							}
							f, err := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
							if err != nil {
								return 0, false
							}
							return math.Abs(f), true
						case []byte:
							s := strings.TrimSpace(string(t))
							if s == "" {
								return 0, false
							}
							f, err := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
							if err != nil {
								return 0, false
							}
							return math.Abs(f), true
						default:
							return 0, false
						}
					}
					pickAmount := func(candidates ...interface{}) (float64, bool) {
						for _, c := range candidates {
							if v, ok := parseAbsFloat(c); ok {
								return v, true
							}
						}
						return 0, false
					}
					for _, m := range mappings {
						if m.TargetTable != "exposure_headers" {
							continue
						}
						val, ok := resolveUploadMappingValue(staged, m.SourceCol, field.DataType, field.TableName)
						if !ok {
							continue
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
					if !hasNonEmptyUploadValue(header["exposure_type"]) {
						header["exposure_type"] = field.DataType
					}
					if !hasNonEmptyUploadValue(header["status"]) {
						header["status"] = "Open"
					}
					if !hasNonEmptyUploadValue(header["approval_status"]) {
						header["approval_status"] = "Pending"
					}
					if !hasNonEmptyUploadValue(header["exposure_creation_status"]) {
						header["exposure_creation_status"] = "Approved"
					}
					if !hasNonEmptyUploadValue(header["is_active"]) {
						header["is_active"] = true
					}

					amountVal, hasAmount := pickAmount(
						header["total_original_amount"],
						header["total_open_amount"],
						header["line_item_amount"],
						header["amount_in_doc_curr"],
						header["amount_in_local_currency"],
						header["net_value"],
						header["payment_to_vendor"],
						staged["amount_in_doc_curr"],
						staged["amount_in_local_currency"],
						staged["net_value"],
						staged["payment_to_vendor"],
						staged["amount"],
					)
					if !hasAmount {
						amountVal = 0
					}
					if _, ok := parseAbsFloat(header["total_original_amount"]); !ok {
						header["total_original_amount"] = amountVal
					}
					if _, ok := parseAbsFloat(header["total_open_amount"]); !ok {
						header["total_open_amount"] = amountVal
					}

					header["additional_header_details"] = headerDetails
					header["upload_s3_key"] = persistedS3Key
					logger.LogInfo("[FX-STAGING] About to insert into exposure_headers with upload_s3_key=%s, upload_batch_id=%s", persistedS3Key, uploadBatchId)
					if hasHeaderUploadBatchID {
						header["upload_batch_id"] = uploadBatchId
					}

					headerKeys := []string{}
					headerVals := []interface{}{}
					headerPlaceholders := []string{}
					idx := 1
					for k, v := range header {
						headerKeys = append(headerKeys, k)
						if m, ok := v.(map[string]interface{}); ok {
							jsonVal, err := json.Marshal(m)
							if err != nil {
								absorptionErrors = append(absorptionErrors, fmt.Sprintf("header marshal error for file %s, key %s: %v", filename, k, err))
								results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: absorptionErrors[len(absorptionErrors)-1]})
								return
							}
							headerVals = append(headerVals, jsonVal)
						} else {
							headerVals = append(headerVals, v)
						}
						headerPlaceholders = append(headerPlaceholders, fmt.Sprintf("$%d", idx))
						idx++
					}

					headerInsert := fmt.Sprintf("INSERT INTO exposure_headers (%s) VALUES (%s) RETURNING exposure_header_id", strings.Join(headerKeys, ", "), strings.Join(headerPlaceholders, ", "))
					logger.LogInfo("[FX-STAGING] INSERT QUERY: %s", headerInsert)
					logger.LogInfo("[FX-STAGING] INSERT VALUES count=%d, first few: %v", len(headerVals), headerVals)
					logger.LogInfo("[FX-STAGING] Trying INSERT into exposure_headers now...")
					var exposureHeaderId string
					err := tx.QueryRowContext(ctx, headerInsert, headerVals...).Scan(&exposureHeaderId)
					if err != nil {
						absorptionErrors = append(absorptionErrors, fmt.Sprintf("header insert error for file %s: %v", filename, err))
						logger.LogError("[FX-STAGING] INSERT FAILED: %v", err)
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: absorptionErrors[len(absorptionErrors)-1]})
						return
					}
					headerMetaSetParts := []string{}
					headerMetaVals := []interface{}{}
					metaIdx := 1
					if strings.TrimSpace(persistedS3Key) != "" {
						headerMetaSetParts = append(headerMetaSetParts, fmt.Sprintf("upload_s3_key = $%d", metaIdx))
						headerMetaVals = append(headerMetaVals, persistedS3Key)
						metaIdx++
					}
					if hasHeaderUploadBatchID && strings.TrimSpace(uploadBatchId) != "" {
						headerMetaSetParts = append(headerMetaSetParts, fmt.Sprintf("upload_batch_id = $%d", metaIdx))
						headerMetaVals = append(headerMetaVals, uploadBatchId)
						metaIdx++
					}
					if len(headerMetaSetParts) > 0 {
						headerMetaVals = append(headerMetaVals, exposureHeaderId)
						headerMetaUpdate := fmt.Sprintf(
							"UPDATE exposure_headers SET %s WHERE exposure_header_id = $%d",
							strings.Join(headerMetaSetParts, ", "),
							metaIdx,
						)
						if _, err := tx.ExecContext(ctx, headerMetaUpdate, headerMetaVals...); err != nil {
							absorptionErrors = append(absorptionErrors, fmt.Sprintf("header upload metadata update error for file %s: %v", filename, err))
							logger.LogError("[FX-STAGING] upload metadata update failed: %v", err)
							results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: absorptionErrors[len(absorptionErrors)-1]})
							return
						}
					}
					if strings.TrimSpace(persistedS3Key) != "" {
						var storedUploadS3Key string
						if err := tx.QueryRowContext(ctx, `
							SELECT COALESCE(upload_s3_key, '')
							FROM exposure_headers
							WHERE exposure_header_id = $1
						`, exposureHeaderId).Scan(&storedUploadS3Key); err != nil {
							absorptionErrors = append(absorptionErrors, fmt.Sprintf("header upload key verification error for file %s: %v", filename, err))
							logger.LogError("[FX-STAGING] upload key verification failed: %v", err)
							results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: absorptionErrors[len(absorptionErrors)-1]})
							return
						}
						if strings.TrimSpace(storedUploadS3Key) != strings.TrimSpace(persistedS3Key) {
							absorptionErrors = append(absorptionErrors, fmt.Sprintf("header upload key verification failed for file %s: persisted key is empty or mismatched", filename))
							logger.LogInfo("[FX-STAGING] upload key verification mismatch: expected=%q actual=%q", persistedS3Key, storedUploadS3Key)
							results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: absorptionErrors[len(absorptionErrors)-1]})
							return
						}
					}
					logger.LogInfo("[FX-STAGING] INSERT SUCCESS: exposureHeaderId=%s, insertedFinalRows=%d", exposureHeaderId, insertedFinalRows+1)
					insertedFinalRows++

					line := map[string]interface{}{}
					lineDetails := map[string]interface{}{}
					for _, m := range mappings {
						if m.TargetTable != "exposure_line_items" {
							continue
						}
						val, ok := resolveUploadMappingValue(staged, m.SourceCol, field.DataType, field.TableName)
						if !ok {
							continue
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
					if len(lineDetails) > 0 {
						jsonVal, err := json.Marshal(lineDetails)
						if err == nil {
							line["additional_line_details"] = jsonVal
						} else {
							absorptionErrors = append(absorptionErrors, fmt.Sprintf("line details marshal error for file %s: %v", filename, err))
							results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: absorptionErrors[len(absorptionErrors)-1]})
							return
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
						if m, ok := v.(map[string]interface{}); ok {
							jsonVal, err := json.Marshal(m)
							if err != nil {
								absorptionErrors = append(absorptionErrors, fmt.Sprintf("line marshal error for file %s, key %s: %v", filename, k, err))
								results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: absorptionErrors[len(absorptionErrors)-1]})
								return
							}
							lineVals = append(lineVals, jsonVal)
						} else {
							lineVals = append(lineVals, v)
						}
						linePlaceholders = append(linePlaceholders, fmt.Sprintf("$%d", idx))
						idx++
					}
					lineInsert := fmt.Sprintf("INSERT INTO exposure_line_items (%s) VALUES (%s)", strings.Join(lineKeys, ", "), strings.Join(linePlaceholders, ", "))
					if _, err = tx.ExecContext(ctx, lineInsert, lineVals...); err != nil {
						absorptionErrors = append(absorptionErrors, fmt.Sprintf("line item insert error for file %s: %v", filename, err))
						results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: absorptionErrors[len(absorptionErrors)-1]})
						return
					}
					createdExposureIDs = append(createdExposureIDs, exposureHeaderId)
				}

				logger.LogError("[FX-STAGING] FINAL RESULT: filename=%s, insertedRows=%d, insertedFinalRows=%d, absorptionErrors=%v", filename, insertedRows, insertedFinalRows, absorptionErrors)
				success := insertedFinalRows > 0 && len(absorptionErrors) == 0
				msg := fmt.Sprintf("Batch uploaded to staging table only (staged: %d rows, absorbed: %d rows, errors: %d)", insertedRows, insertedFinalRows, len(absorptionErrors))
				if success {
					msg = fmt.Sprintf("Batch absorbed into exposures (%d records created, upload_s3_key: %s)", insertedFinalRows, s3Key)
				}
				if len(absorptionErrors) > 0 {
					msg = "Upload failed: " + absorptionErrors[0]
					logger.LogError("[FX-STAGING] ERROR DETAIL: %v", absorptionErrors)
				}
				logger.LogInfo("[FX-STAGING] About to COMMIT transaction...")
				if err := tx.Commit(); err != nil {
					logger.LogError("[FX-STAGING] COMMIT FAILED: %v", err)
					results = append(results, map[string]interface{}{"filename": filename, constants.ValueError: "Failed to commit staged exposure upload: " + err.Error()})
					return
				}
				logger.LogInfo("[FX-STAGING] COMMIT SUCCESS for filename=%s", filename)
				txCommitted = true
				for _, exposureHeaderID := range createdExposureIDs {
					newValues := auditutil.FetchRowSnapshot(ctx, db, constants.ExposureHeaders, "exposure_header_id", exposureHeaderID)
					auditutil.RecordAction(ctx, db, auditutil.ActionParams{
						TableName:    auditutil.TableExposure,
						ParentColumn: "exposure_header_id",
						ParentID:     exposureHeaderID,
						ActionType:   "CREATE",
						Status:       constants.StatusPendingApproval,
						Reason:       "Uploaded file: " + filename,
						RequestedBy:  uploadedBy,
						OldValues:    nil,
						NewValues:    newValues,
					})
				}
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: success,
					"filename":             filename,
					"message":              msg,
					"uploadBatchId":        uploadBatchId,
					"insertedRows":         insertedRows,
					"insertedFinalRows":    insertedFinalRows,
				})
			}()
		}
	}

	return results, absorptionErrors, nil
}

func batchStagingExposureTypeFolder(dataType string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(dataType))
	switch normalized {
	case "po":
		return "po", nil
	case "so":
		return "so", nil
	case "lc":
		return "lc", nil
	case "creditors", "creditor":
		return "creditor", nil
	case "debitors", "debitor":
		return "debitor", nil
	case "grn":
		return "grn", nil
	default:
		return "", fmt.Errorf("invalid exposure type '%s': expected one of PO, LC, SO, creditor, debitor, grn", dataType)
	}
}

func fxExposureS3Bucket() string {
	if b := strings.TrimSpace(os.Getenv("BANK_STMT_S3_BUCKET")); b != "" {
		return b
	}
	return "cimplr"
}

func fxExposureS3Region() string {
	if r := strings.TrimSpace(os.Getenv("BANK_STMT_S3_REGION")); r != "" {
		return r
	}
	return "ap-south-1"
}

func fxExposureS3PresignExpiry() time.Duration {
	expiryDuration := 7 * 24 * time.Hour
	if envExpiry := strings.TrimSpace(os.Getenv("BANK_STMT_URL_EXPIRY_HOURS")); envExpiry != "" {
		if hours, parseErr := strconv.Atoi(envExpiry); parseErr == nil && hours > 0 {
			expiryDuration = time.Duration(hours) * time.Hour
			max := 7 * 24 * time.Hour
			if expiryDuration > max {
				expiryDuration = max
			}
		}
	}
	return expiryDuration
}
