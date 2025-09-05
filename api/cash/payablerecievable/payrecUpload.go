package payablerecievable

import (
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// TODO: ADD Old Value Concept for Audit Logs
// TODO: dont ask entiyty_id or any id from user, fetch from session

// Helper: fetch audit info for payables/receivables
func getAuditInfoPayable(ctx context.Context, pgxPool *pgxpool.Pool, payableID string) (createdBy, createdAt, createdStatus, editedBy, editedAt, editedStatus, deletedBy, deletedAt, deletedStatus string) {
	auditDetailsQuery := `SELECT actiontype, requested_by, requested_at, processing_status FROM auditactionpayable WHERE payable_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
	auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, payableID)
	if auditErr == nil {
		defer auditRows.Close()
		for auditRows.Next() {
			var atype, status string
			var rbyPtr *string
			var ratPtr *time.Time
			if err := auditRows.Scan(&atype, &rbyPtr, &ratPtr, &status); err == nil {
				by := ""
				at := ""
				if rbyPtr != nil {
					by = *rbyPtr
				}
				if ratPtr != nil {
					at = ratPtr.Format("2006-01-02 15:04:05")
				}
				if atype == "CREATE" && createdBy == "" {
					createdBy = by
					createdAt = at
					createdStatus = status
				} else if atype == "EDIT" && editedBy == "" {
					editedBy = by
					editedAt = at
					editedStatus = status
				} else if atype == "DELETE" && deletedBy == "" {
					deletedBy = by
					deletedAt = at
					deletedStatus = status
				}
			}
		}
	}
	return
}

func getAuditInfoReceivable(ctx context.Context, pgxPool *pgxpool.Pool, receivableID string) (createdBy, createdAt, createdStatus, editedBy, editedAt, editedStatus, deletedBy, deletedAt, deletedStatus string) {
	auditDetailsQuery := `SELECT actiontype, requested_by, requested_at, processing_status FROM auditactionreceivable WHERE receivable_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
	auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, receivableID)
	if auditErr == nil {
		defer auditRows.Close()
		for auditRows.Next() {
			var atype, status string
			var rbyPtr *string
			var ratPtr *time.Time
			if err := auditRows.Scan(&atype, &rbyPtr, &ratPtr, &status); err == nil {
				by := ""
				at := ""
				if rbyPtr != nil {
					by = *rbyPtr
				}
				if ratPtr != nil {
					at = ratPtr.Format("2006-01-02 15:04:05")
				}
				if atype == "CREATE" && createdBy == "" {
					createdBy = by
					createdAt = at
					createdStatus = status
				} else if atype == "EDIT" && editedBy == "" {
					editedBy = by
					editedAt = at
					editedStatus = status
				} else if atype == "DELETE" && deletedBy == "" {
					deletedBy = by
					deletedAt = at
					deletedStatus = status
				}
			}
		}
	}
	return
}

// Helper: get file extension
func getFileExt(filename string) string {
	return strings.ToLower(filepath.Ext(filename))
}

// Helper: parse uploaded file into [][]string
func parseUploadFile(file multipart.File, ext string) ([][]string, error) {
	if ext == ".csv" {
		r := csv.NewReader(file)
		return r.ReadAll()
	}
	if ext == ".xlsx" || ext == ".xls" {
		f, err := excelize.OpenReader(file)
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	return nil, errors.New("unsupported file type")
}

// Helper: normalize date string to YYYY-MM-DD
func normalizeDate(dateStr string) string {
	layouts := []string{"2006-01-02", "02-01-2006", "01/02/2006", "2 Jan 2006", "2006/01/02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t.Format("2006-01-02")
		}
	}
	return dateStr // fallback, let DB error if invalid
}

// Handler: UploadPayRec (for payables/receivables)
func UploadPayRec(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get("Content-Type") == "application/json" {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				http.Error(w, "user_id required in body", http.StatusBadRequest)
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue("user_id")
			if userID == "" {
				http.Error(w, "user_id required in form", http.StatusBadRequest)
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
			http.Error(w, "User not found in active sessions", http.StatusUnauthorized)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, "Failed to parse multipart form", http.StatusBadRequest)
			return
		}
		if len(r.MultipartForm.File) == 0 {
			http.Error(w, "No files uploaded", http.StatusBadRequest)
			return
		}

		// Read mapping
		mapRows, err := pgxPool.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_input_transactions`)
		if err != nil {
			http.Error(w, "Mapping error", http.StatusInternalServerError)
			return
		}
		mapping := make(map[string]string)
		for mapRows.Next() {
			var src, tgt string
			if err := mapRows.Scan(&src, &tgt); err == nil {
				mapping[src] = tgt
			}
		}
		mapRows.Close()

		batchIDs := make([]string, 0)
		for txType, files := range r.MultipartForm.File {
			txTypeUpper := strings.ToUpper(txType)
			for _, fileHeader := range files {
				file, err := fileHeader.Open()
				if err != nil {
					http.Error(w, "Failed to open file: "+fileHeader.Filename, http.StatusBadRequest)
					return
				}
				ext := getFileExt(fileHeader.Filename)
				records, err := parseUploadFile(file, ext)
				file.Close()
				if err != nil || len(records) < 2 {
					http.Error(w, "Invalid or empty file: "+fileHeader.Filename, http.StatusBadRequest)
					return
				}
				headerRow := records[0]
				dataRows := records[1:]
				batchID := uuid.New().String()
				batchIDs = append(batchIDs, batchID)
				colCount := len(headerRow)
				copyRows := make([][]interface{}, len(dataRows))
				for i, row := range dataRows {
					vals := make([]interface{}, colCount+2) // +2 for batchID and transaction_type
					vals[0] = batchID
					vals[1] = txTypeUpper
					for j := 0; j < colCount; j++ {
						val := ""
						if j < len(row) {
							val = row[j]
						}
						// Normalize date columns
						if mapping[headerRow[j]] == "invoice_date" || mapping[headerRow[j]] == "due_date" {
							val = normalizeDate(val)
						}
						vals[j+2] = val
					}
					copyRows[i] = vals
				}
				columns := append([]string{"upload_batch_id", "transaction_type"}, headerRow...)
				_, err = pgxPool.CopyFrom(
					ctx,
					pgx.Identifier{"input_transactions"},
					columns,
					pgx.CopyFromRows(copyRows),
				)
				if err != nil {
					http.Error(w, "Failed to stage data: "+err.Error(), http.StatusInternalServerError)
					return
				}

				// Move to final table(s)
				if txTypeUpper == "PAYABLE" {
					// Insert into payables and get payable_ids
					rows, err := pgxPool.Query(ctx, `
						INSERT INTO payables (entity_id, vendor_id, invoice_number, invoice_date, due_date, amount, currency_code)
						SELECT entity_id, vendor_id, invoice_number, invoice_date::date, due_date::date, amount::numeric, currency_code
						FROM input_transactions WHERE upload_batch_id = $1
						RETURNING payable_id
					`, batchID)
					if err != nil {
						http.Error(w, "Final insert error (payables): "+err.Error(), http.StatusInternalServerError)
						return
					}
					var payableIDs []string
					for rows.Next() {
						var payableID string
						if err := rows.Scan(&payableID); err == nil {
							payableIDs = append(payableIDs, payableID)
						}
					}
					rows.Close()
					if len(payableIDs) > 0 {
						// Bulk insert audit logs
						var auditValues []string
						for _, pid := range payableIDs {
							auditValues = append(auditValues, fmt.Sprintf("('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())", pid, userName))
						}
						auditSQL := "INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES " + strings.Join(auditValues, ",")
						_, auditErr := pgxPool.Exec(ctx, auditSQL)
						if auditErr != nil {
							http.Error(w, "Audit log error (payables): "+auditErr.Error(), http.StatusInternalServerError)
							return
						}
					}
				} else if txTypeUpper == "RECEIVABLE" {
					// Insert into receivables and get receivable_ids
					rows, err := pgxPool.Query(ctx, `
						INSERT INTO receivables (entity_id, customer_id, invoice_number, invoice_date, due_date, invoice_amount, currency_code)
						SELECT entity_id, customer_id, invoice_number, invoice_date::date, due_date::date, invoice_amount::numeric, currency_code
						FROM input_transactions WHERE upload_batch_id = $1
						RETURNING receivable_id
					`, batchID)
					if err != nil {
						http.Error(w, "Final insert error (receivables): "+err.Error(), http.StatusInternalServerError)
						return
					}
					var receivableIDs []string
					for rows.Next() {
						var receivableID string
						if err := rows.Scan(&receivableID); err == nil {
							receivableIDs = append(receivableIDs, receivableID)
						}
					}
					rows.Close()
					if len(receivableIDs) > 0 {
						// Bulk insert audit logs
						var auditValues []string
						for _, rid := range receivableIDs {
							auditValues = append(auditValues, fmt.Sprintf("('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())", rid, userName))
						}
						auditSQL := "INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES " + strings.Join(auditValues, ",")
						_, auditErr := pgxPool.Exec(ctx, auditSQL)
						if auditErr != nil {
							http.Error(w, "Audit log error (receivables): "+auditErr.Error(), http.StatusInternalServerError)
							return
						}
					}
				} else {
					http.Error(w, "Unknown transaction_type: "+txTypeUpper, http.StatusBadRequest)
					return
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "All transactions uploaded and processed",
		})
	}
}

// Handler: GetAllPayableReceivable
func GetAllPayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		type Payable struct {
			PayableID    string  `json:"payable_id"`
			EntityID     string  `json:"entity_id"`
			VendorID     string  `json:"vendor_id"`
			InvoiceNo    string  `json:"invoice_number"`
			InvoiceDate  string  `json:"invoice_date"`
			DueDate      string  `json:"due_date"`
			Amount       float64 `json:"amount"`
			CurrencyCode string  `json:"currency_code"`
			Status       string  `json:"status"`
			CreatedBy    string  `json:"created_by"`
			CreatedAt    string  `json:"created_at"`
			EditedBy     string  `json:"edited_by"`
			EditedAt     string  `json:"edited_at"`
			DeletedBy    string  `json:"deleted_by"`
			DeletedAt    string  `json:"deleted_at"`
		}
		type Receivable struct {
			ReceivableID string  `json:"receivable_id"`
			EntityID     string  `json:"entity_id"`
			CustomerID   string  `json:"customer_id"`
			InvoiceNo    string  `json:"invoice_number"`
			InvoiceDate  string  `json:"invoice_date"`
			DueDate      string  `json:"due_date"`
			Amount       float64 `json:"invoice_amount"`
			CurrencyCode string  `json:"currency_code"`
			Status       string  `json:"status"`
			CreatedBy    string  `json:"created_by"`
			CreatedAt    string  `json:"created_at"`
			EditedBy     string  `json:"edited_by"`
			EditedAt     string  `json:"edited_at"`
			DeletedBy    string  `json:"deleted_by"`
			DeletedAt    string  `json:"deleted_at"`
		}

		// 1. Fetch all payables
		payableRows, err := pgxPool.Query(ctx, `SELECT payable_id, entity_id, vendor_id, invoice_number, invoice_date, due_date, amount, currency_code FROM payables`)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		defer payableRows.Close()
		var payables []Payable
		var payableIDs []string
		for payableRows.Next() {
			var p Payable
			var invoiceDate, dueDate *time.Time
			if err := payableRows.Scan(&p.PayableID, &p.EntityID, &p.VendorID, &p.InvoiceNo, &invoiceDate, &dueDate, &p.Amount, &p.CurrencyCode); err != nil {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
				return
			}
			p.InvoiceDate = ""
			if invoiceDate != nil {
				p.InvoiceDate = invoiceDate.Format("2006-01-02")
			}
			p.DueDate = ""
			if dueDate != nil {
				p.DueDate = dueDate.Format("2006-01-02")
			}
			payables = append(payables, p)
			payableIDs = append(payableIDs, p.PayableID)
		}

		// 2. Fetch all receivables
		receivableRows, err := pgxPool.Query(ctx, `SELECT receivable_id, entity_id, customer_id, invoice_number, invoice_date, due_date, invoice_amount, currency_code FROM receivables`)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		defer receivableRows.Close()
		var receivables []Receivable
		var receivableIDs []string
		for receivableRows.Next() {
			var rcv Receivable
			var invoiceDate, dueDate *time.Time
			if err := receivableRows.Scan(&rcv.ReceivableID, &rcv.EntityID, &rcv.CustomerID, &rcv.InvoiceNo, &invoiceDate, &dueDate, &rcv.Amount, &rcv.CurrencyCode); err != nil {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
				return
			}
			rcv.InvoiceDate = ""
			if invoiceDate != nil {
				rcv.InvoiceDate = invoiceDate.Format("2006-01-02")
			}
			rcv.DueDate = ""
			if dueDate != nil {
				rcv.DueDate = dueDate.Format("2006-01-02")
			}
			receivables = append(receivables, rcv)
			receivableIDs = append(receivableIDs, rcv.ReceivableID)
		}

		// 3. Batch fetch audit logs for payables
		auditPayableMap := make(map[string]map[string]string)
		if len(payableIDs) > 0 {
			query := `SELECT payable_id, actiontype, requested_by, requested_at, processing_status FROM auditactionpayable WHERE payable_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')`
			rows, err := pgxPool.Query(ctx, query, payableIDs)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var pid, atype, requestedBy, status string
					var requestedAt *time.Time
					_ = rows.Scan(&pid, &atype, &requestedBy, &requestedAt, &status)
					if _, ok := auditPayableMap[pid]; !ok {
						auditPayableMap[pid] = make(map[string]string)
					}
					if atype == "CREATE" {
						auditPayableMap[pid]["created_by"] = requestedBy
						if requestedAt != nil {
							auditPayableMap[pid]["created_at"] = requestedAt.Format("2006-01-02 15:04:05")
						}
						auditPayableMap[pid]["created_status"] = status
					} else if atype == "EDIT" {
						auditPayableMap[pid]["edited_by"] = requestedBy
						if requestedAt != nil {
							auditPayableMap[pid]["edited_at"] = requestedAt.Format("2006-01-02 15:04:05")
						}
						auditPayableMap[pid]["edited_status"] = status
					} else if atype == "DELETE" {
						auditPayableMap[pid]["deleted_by"] = requestedBy
						if requestedAt != nil {
							auditPayableMap[pid]["deleted_at"] = requestedAt.Format("2006-01-02 15:04:05")
						}
						auditPayableMap[pid]["deleted_status"] = status
					}
				}
			}
		}
		for i := range payables {
			audit := auditPayableMap[payables[i].PayableID]
			if audit != nil {
				payables[i].CreatedBy = audit["created_by"]
				payables[i].CreatedAt = audit["created_at"]
				payables[i].EditedBy = audit["edited_by"]
				payables[i].EditedAt = audit["edited_at"]
				payables[i].DeletedBy = audit["deleted_by"]
				payables[i].DeletedAt = audit["deleted_at"]
				// Set status: prefer DELETE, then EDIT, then CREATE
				if audit["deleted_status"] != "" {
					payables[i].Status = audit["deleted_status"]
				} else if audit["edited_status"] != "" {
					payables[i].Status = audit["edited_status"]
				} else {
					payables[i].Status = audit["created_status"]
				}
			}
		}

		// 4. Batch fetch audit logs for receivables
		auditReceivableMap := make(map[string]map[string]string)
		if len(receivableIDs) > 0 {
			query := `SELECT receivable_id, actiontype, requested_by, requested_at, processing_status FROM auditactionreceivable WHERE receivable_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')`
			rows, err := pgxPool.Query(ctx, query, receivableIDs)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var rid, atype, requestedBy, status string
					var requestedAt *time.Time
					_ = rows.Scan(&rid, &atype, &requestedBy, &requestedAt, &status)
					if _, ok := auditReceivableMap[rid]; !ok {
						auditReceivableMap[rid] = make(map[string]string)
					}
					if atype == "CREATE" {
						auditReceivableMap[rid]["created_by"] = requestedBy
						if requestedAt != nil {
							auditReceivableMap[rid]["created_at"] = requestedAt.Format("2006-01-02 15:04:05")
						}
						auditReceivableMap[rid]["created_status"] = status
					} else if atype == "EDIT" {
						auditReceivableMap[rid]["edited_by"] = requestedBy
						if requestedAt != nil {
							auditReceivableMap[rid]["edited_at"] = requestedAt.Format("2006-01-02 15:04:05")
						}
						auditReceivableMap[rid]["edited_status"] = status
					} else if atype == "DELETE" {
						auditReceivableMap[rid]["deleted_by"] = requestedBy
						if requestedAt != nil {
							auditReceivableMap[rid]["deleted_at"] = requestedAt.Format("2006-01-02 15:04:05")
						}
						auditReceivableMap[rid]["deleted_status"] = status
					}
				}
			}
		}
		for i := range receivables {
			audit := auditReceivableMap[receivables[i].ReceivableID]
			if audit != nil {
				receivables[i].CreatedBy = audit["created_by"]
				receivables[i].CreatedAt = audit["created_at"]
				receivables[i].EditedBy = audit["edited_by"]
				receivables[i].EditedAt = audit["edited_at"]
				receivables[i].DeletedBy = audit["deleted_by"]
				receivables[i].DeletedAt = audit["deleted_at"]
				// Set status: prefer DELETE, then EDIT, then CREATE
				if audit["deleted_status"] != "" {
					receivables[i].Status = audit["deleted_status"]
				} else if audit["edited_status"] != "" {
					receivables[i].Status = audit["edited_status"]
				} else {
					receivables[i].Status = audit["created_status"]
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"payables":    payables,
				"receivables": receivables,
			},
		})
	}
}

// Bulk delete handler for payables audit actions
func BulkDeletePayableAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			PayableIDs []string `json:"payable_ids"`
			Reason     string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.PayableIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid JSON or missing fields"})
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
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid user_id or session"})
			return
		}
		var results []string
		for _, pid := range req.PayableIDs {
			query := `INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now()) RETURNING action_id`
			var actionID string
			err := pgxPool.QueryRow(r.Context(), query, pid, req.Reason, requestedBy).Scan(&actionID)
			if err == nil {
				results = append(results, actionID)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "created": results})
	}
}

// Bulk reject audit actions for payables
func BulkRejectPayableAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			PayableIDs []string `json:"payable_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.PayableIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid JSON or missing fields"})
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
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid user_id or session"})
			return
		}
		// For each payable_id, find the latest action_id
		var actionIDs []string
		for _, pid := range req.PayableIDs {
			var actionID string
			err := pgxPool.QueryRow(r.Context(), `SELECT action_id FROM auditactionpayable WHERE payable_id = $1 ORDER BY requested_at DESC LIMIT 1`, pid).Scan(&actionID)
			if err == nil && actionID != "" {
				actionIDs = append(actionIDs, actionID)
			}
		}
		if len(actionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "No valid actions found for provided payable_ids"})
			return
		}
		query := `UPDATE auditactionpayable SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id,payable_id`
		rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, actionIDs)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, pid string
			rows.Scan(&id, &pid)
			updated = append(updated, id, pid)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}

// Bulk approve audit actions for payables
func BulkApprovePayableAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			PayableIDs []string `json:"payable_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.PayableIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid JSON or missing fields"})
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
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid user_id or session"})
			return
		}
		// For each payable_id, find the latest action_id
		var actionIDs []string
		for _, pid := range req.PayableIDs {
			var actionID string
			err := pgxPool.QueryRow(r.Context(), `SELECT action_id FROM auditactionpayable WHERE payable_id = $1 ORDER BY requested_at DESC LIMIT 1`, pid).Scan(&actionID)
			if err == nil && actionID != "" {
				actionIDs = append(actionIDs, actionID)
			}
		}
		if len(actionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "No valid actions found for provided payable_ids"})
			return
		}
		// First, delete records with processing_status = 'PENDING_DELETE_APPROVAL' for the found action_ids
		delQuery := `DELETE FROM auditactionpayable WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, payable_id`
		delRows, delErr := pgxPool.Query(r.Context(), delQuery, actionIDs)
		var deleted []string
		var payableIDsToDelete []string
		if delErr == nil {
			defer delRows.Close()
			for delRows.Next() {
				var id, pid string
				delRows.Scan(&id, &pid)
				deleted = append(deleted, id, pid)
				payableIDsToDelete = append(payableIDsToDelete, pid)
			}
		}
		// Delete corresponding payables from payables
		if len(payableIDsToDelete) > 0 {
			delPayableQuery := `DELETE FROM payables WHERE payable_id = ANY($1)`
			_, _ = pgxPool.Exec(r.Context(), delPayableQuery, payableIDsToDelete)
		}
		// Then, approve the rest
		query := `UPDATE auditactionpayable SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id,payable_id`
		rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, actionIDs)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, pid string
			rows.Scan(&id, &pid)
			updated = append(updated, id, pid)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated, "deleted": deleted})
	}
}

// Repeat the same for receivables
func BulkDeleteReceivableAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			ReceivableIDs []string `json:"receivable_ids"`
			Reason        string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ReceivableIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid JSON or missing fields"})
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
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid user_id or session"})
			return
		}
		var results []string
		for _, rid := range req.ReceivableIDs {
			query := `INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now()) RETURNING action_id`
			var actionID string
			err := pgxPool.QueryRow(r.Context(), query, rid, req.Reason, requestedBy).Scan(&actionID)
			if err == nil {
				results = append(results, actionID)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "created": results})
	}
}

func BulkRejectReceivableAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			ReceivableIDs []string `json:"receivable_ids"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ReceivableIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid JSON or missing fields"})
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
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid user_id or session"})
			return
		}
		// For each receivable_id, find the latest action_id
		var actionIDs []string
		for _, rid := range req.ReceivableIDs {
			var actionID string
			err := pgxPool.QueryRow(r.Context(), `SELECT action_id FROM auditactionreceivable WHERE receivable_id = $1 ORDER BY requested_at DESC LIMIT 1`, rid).Scan(&actionID)
			if err == nil && actionID != "" {
				actionIDs = append(actionIDs, actionID)
			}
		}
		if len(actionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "No valid actions found for provided receivable_ids"})
			return
		}
		query := `UPDATE auditactionreceivable SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id,receivable_id`
		rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, actionIDs)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, rid string
			rows.Scan(&id, &rid)
			updated = append(updated, id, rid)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}

func BulkApproveReceivableAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			ReceivableIDs []string `json:"receivable_ids"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ReceivableIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid JSON or missing fields"})
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
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid user_id or session"})
			return
		}
		// For each receivable_id, find the latest action_id
		var actionIDs []string
		for _, rid := range req.ReceivableIDs {
			var actionID string
			err := pgxPool.QueryRow(r.Context(), `SELECT action_id FROM auditactionreceivable WHERE receivable_id = $1 ORDER BY requested_at DESC LIMIT 1`, rid).Scan(&actionID)
			if err == nil && actionID != "" {
				actionIDs = append(actionIDs, actionID)
			}
		}
		if len(actionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "No valid actions found for provided receivable_ids"})
			return
		}
		// First, delete records with processing_status = 'PENDING_DELETE_APPROVAL' for the found action_ids
		delQuery := `DELETE FROM auditactionreceivable WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, receivable_id`
		delRows, delErr := pgxPool.Query(r.Context(), delQuery, actionIDs)
		var deleted []string
		var receivableIDsToDelete []string
		if delErr == nil {
			defer delRows.Close()
			for delRows.Next() {
				var id, rid string
				delRows.Scan(&id, &rid)
				deleted = append(deleted, id, rid)
				receivableIDsToDelete = append(receivableIDsToDelete, rid)
			}
		}
		// Delete corresponding receivables from receivables
		if len(receivableIDsToDelete) > 0 {
			delReceivableQuery := `DELETE FROM receivables WHERE receivable_id = ANY($1)`
			_, _ = pgxPool.Exec(r.Context(), delReceivableQuery, receivableIDsToDelete)
		}
		// Then, approve the rest
		query := `UPDATE auditactionreceivable SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id,receivable_id`
		rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, actionIDs)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, rid string
			rows.Scan(&id, &rid)
			updated = append(updated, id, rid)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated, "deleted": deleted})
	}
}
