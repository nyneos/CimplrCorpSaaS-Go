package payablerecievable

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// helpers used by bulk update flow
func nullifyEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

type sqlNullString struct {
	Valid bool
	S     string
}

func (n *sqlNullString) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		n.S = ""
		return nil
	}
	switch t := v.(type) {
	case string:
		n.Valid = true
		n.S = t
	case []byte:
		n.Valid = true
		n.S = string(t)
	default:
		n.Valid = true
		n.S = fmt.Sprint(v)
	}
	return nil
}

func (n sqlNullString) ValueOrZero() interface{} {
	if n.Valid {
		return n.S
	}
	return ""
}

type sqlNullFloat struct {
	Valid bool
	F     float64
}

func (n *sqlNullFloat) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		n.F = 0
		return nil
	}
	switch t := v.(type) {
	case float64:
		n.Valid = true
		n.F = t
	case int64:
		n.Valid = true
		n.F = float64(t)
	case []byte:
		s := string(t)
		if s == "" {
			n.Valid = false
			n.F = 0
			return nil
		}
		var err error
		n.F, err = strconv.ParseFloat(s, 64)
		if err != nil {
			n.Valid = false
			n.F = 0
			return nil
		}
		n.Valid = true
	case string:
		if t == "" {
			n.Valid = false
			n.F = 0
			return nil
		}
		var err error
		n.F, err = strconv.ParseFloat(t, 64)
		if err != nil {
			n.Valid = false
			n.F = 0
			return nil
		}
		n.Valid = true
	default:
		n.Valid = true
		n.F = 0
	}
	return nil
}

func (n sqlNullFloat) ValueOrZero() interface{} {
	if n.Valid {
		return n.F
	}
	return nil
}

type sqlNullTime struct {
	Valid bool
	T     time.Time
}

func (n *sqlNullTime) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		return nil
	}
	switch t := v.(type) {
	case time.Time:
		n.Valid = true
		n.T = t
	case []byte:
		s := string(t)
		if s == "" {
			n.Valid = false
			return nil
		}
		if parsed, err := time.Parse(constants.DateFormat, s); err == nil {
			n.Valid = true
			n.T = parsed
		} else {
			n.Valid = false
		}
	case string:
		if t == "" {
			n.Valid = false
			return nil
		}
		if parsed, err := time.Parse(constants.DateFormat, t); err == nil {
			n.Valid = true
			n.T = parsed
		} else {
			n.Valid = false
		}
	default:
		n.Valid = false
	}
	return nil
}

func (n sqlNullTime) ValueOrZero() interface{} {
	if n.Valid {
		return n.T
	}
	return nil
}
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
					at = ratPtr.Format(constants.DateTimeFormat)
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
					at = ratPtr.Format(constants.DateTimeFormat)
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
	return nil, errors.New(constants.ErrUnsupportedFileType)
}

// Helper: normalize date string to YYYY-MM-DD
func normalizeDate(dateStr string) string {
	layouts := []string{constants.DateFormat, constants.DateFormatAlt, "01/02/2006", "2 Jan 2006", "2006/01/02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t.Format(constants.DateFormat)
		}
	}
	return dateStr // fallback, let DB error if invalid
}

// Handler: UploadPayRec (for payables/receivables)
func UploadPayRec(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get(constants.ContentTypeText) == constants.ContentTypeJSON {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				http.Error(w, "user_id required in body", http.StatusBadRequest)
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue(constants.KeyUserID)
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
			http.Error(w, constants.ErrInvalidSession, http.StatusUnauthorized)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, constants.ErrFailedToParseMultipartForm, http.StatusBadRequest)
			return
		}
		if len(r.MultipartForm.File) == 0 {
			http.Error(w, constants.ErrNoFilesUploaded, http.StatusBadRequest)
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
							auditValues = append(auditValues, fmt.Sprintf(constants.FormatInsertAuditLog, pid, userName))
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
							auditValues = append(auditValues, fmt.Sprintf(constants.FormatInsertAuditLog, rid, userName))
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
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"message":              "All transactions uploaded and processed",
		})
	}
}

func GetAllPayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		type Payable struct {
			PayableID        string  `json:"payable_id"`
			EntityName       string  `json:"entity_name"`
			CounterpartyName string  `json:"counterparty_name"`
			InvoiceNo        string  `json:"invoice_number"`
			InvoiceDate      string  `json:"invoice_date"`
			DueDate          string  `json:"due_date"`
			Amount           float64 `json:"amount"`
			CurrencyCode     string  `json:"currency_code"`
			// old values
			OldEntityName   string  `json:"old_entity_name"`
			OldCounterparty string  `json:"old_counterparty_name"`
			OldInvoiceNo    string  `json:"old_invoice_number"`
			OldInvoiceDate  string  `json:"old_invoice_date"`
			OldDueDate      string  `json:"old_due_date"`
			OldAmount       float64 `json:"old_amount"`
			OldCurrencyCode string  `json:"old_currency_code"`
			Status          string  `json:"status"`
			CreatedBy       string  `json:"created_by"`
			CreatedAt       string  `json:"created_at"`
			EditedBy        string  `json:"edited_by"`
			EditedAt        string  `json:"edited_at"`
			DeletedBy       string  `json:"deleted_by"`
			DeletedAt       string  `json:"deleted_at"`
		}
		type Receivable struct {
			ReceivableID     string  `json:"receivable_id"`
			EntityName       string  `json:"entity_name"`
			CounterpartyName string  `json:"counterparty_name"`
			InvoiceNo        string  `json:"invoice_number"`
			InvoiceDate      string  `json:"invoice_date"`
			DueDate          string  `json:"due_date"`
			Amount           float64 `json:"invoice_amount"`
			CurrencyCode     string  `json:"currency_code"`
			// old values
			OldEntityName   string  `json:"old_entity_name"`
			OldCounterparty string  `json:"old_counterparty_name"`
			OldInvoiceNo    string  `json:"old_invoice_number"`
			OldInvoiceDate  string  `json:"old_invoice_date"`
			OldDueDate      string  `json:"old_due_date"`
			OldAmount       float64 `json:"old_invoice_amount"`
			OldCurrencyCode string  `json:"old_currency_code"`
			Status          string  `json:"status"`
			CreatedBy       string  `json:"created_by"`
			CreatedAt       string  `json:"created_at"`
			EditedBy        string  `json:"edited_by"`
			EditedAt        string  `json:"edited_at"`
			DeletedBy       string  `json:"deleted_by"`
			DeletedAt       string  `json:"deleted_at"`
		}

		// 1. Fetch all payables (new table tr_payables)
		payableRows, err := pgxPool.Query(ctx, `SELECT payable_id, entity_name, counterparty_name, invoice_number, invoice_date, due_date, amount, currency_code, old_entity_name, old_counterparty_name, old_invoice_number, old_invoice_date, old_due_date, old_amount, old_currency_code FROM tr_payables WHERE is_deleted != TRUE`)
		if err != nil {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": err.Error()})
			return
		}
		defer payableRows.Close()
		var payables []Payable
		var payableIDs []string
		for payableRows.Next() {
			var p Payable
			var invoiceDate, dueDate *time.Time
			var oldEntityPtr, oldCounterPtr, oldInvoicePtr *string
			var oldInvoiceDate, oldDueDate *time.Time
			var oldAmountPtr *float64
			var oldCurrencyPtr *string
			if err := payableRows.Scan(&p.PayableID, &p.EntityName, &p.CounterpartyName, &p.InvoiceNo, &invoiceDate, &dueDate, &p.Amount, &p.CurrencyCode, &oldEntityPtr, &oldCounterPtr, &oldInvoicePtr, &oldInvoiceDate, &oldDueDate, &oldAmountPtr, &oldCurrencyPtr); err != nil {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": err.Error()})
				return
			}
			// populate old fields safely
			if oldEntityPtr != nil {
				p.OldEntityName = *oldEntityPtr
			} else {
				p.OldEntityName = ""
			}
			if oldCounterPtr != nil {
				p.OldCounterparty = *oldCounterPtr
			} else {
				p.OldCounterparty = ""
			}
			if oldInvoicePtr != nil {
				p.OldInvoiceNo = *oldInvoicePtr
			} else {
				p.OldInvoiceNo = ""
			}
			if oldInvoiceDate != nil {
				p.OldInvoiceDate = oldInvoiceDate.Format(constants.DateFormat)
			} else {
				p.OldInvoiceDate = ""
			}
			if oldDueDate != nil {
				p.OldDueDate = oldDueDate.Format(constants.DateFormat)
			} else {
				p.OldDueDate = ""
			}
			if oldAmountPtr != nil {
				p.OldAmount = *oldAmountPtr
			} else {
				p.OldAmount = 0
			}
			if oldCurrencyPtr != nil {
				p.OldCurrencyCode = *oldCurrencyPtr
			} else {
				p.OldCurrencyCode = ""
			}
			p.InvoiceDate = ""
			if invoiceDate != nil {
				p.InvoiceDate = invoiceDate.Format(constants.DateFormat)
			}
			p.DueDate = ""
			if dueDate != nil {
				p.DueDate = dueDate.Format(constants.DateFormat)
			}
			payables = append(payables, p)
			payableIDs = append(payableIDs, p.PayableID)
		}

		// 2. Fetch all receivables (new table tr_receivables)
		receivableRows, err := pgxPool.Query(ctx, `SELECT receivable_id, entity_name, counterparty_name, invoice_number, invoice_date, due_date, invoice_amount, currency_code, old_entity_name, old_counterparty_name, old_invoice_number, old_invoice_date, old_due_date, old_invoice_amount, old_currency_code FROM tr_receivables WHERE is_deleted != TRUE`)
		if err != nil {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": err.Error()})
			return
		}
		defer receivableRows.Close()
		var receivables []Receivable
		var receivableIDs []string
		for receivableRows.Next() {
			var rcv Receivable
			var invoiceDate, dueDate *time.Time
			var oldEntityPtr, oldCounterPtr, oldInvoicePtr *string
			var oldInvoiceDate, oldDueDate *time.Time
			var oldAmountPtr *float64
			var oldCurrencyPtr *string
			if err := receivableRows.Scan(&rcv.ReceivableID, &rcv.EntityName, &rcv.CounterpartyName, &rcv.InvoiceNo, &invoiceDate, &dueDate, &rcv.Amount, &rcv.CurrencyCode, &oldEntityPtr, &oldCounterPtr, &oldInvoicePtr, &oldInvoiceDate, &oldDueDate, &oldAmountPtr, &oldCurrencyPtr); err != nil {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": err.Error()})
				return
			}
			if oldEntityPtr != nil {
				rcv.OldEntityName = *oldEntityPtr
			} else {
				rcv.OldEntityName = ""
			}
			if oldCounterPtr != nil {
				rcv.OldCounterparty = *oldCounterPtr
			} else {
				rcv.OldCounterparty = ""
			}
			if oldInvoicePtr != nil {
				rcv.OldInvoiceNo = *oldInvoicePtr
			} else {
				rcv.OldInvoiceNo = ""
			}
			if oldInvoiceDate != nil {
				rcv.OldInvoiceDate = oldInvoiceDate.Format(constants.DateFormat)
			} else {
				rcv.OldInvoiceDate = ""
			}
			if oldDueDate != nil {
				rcv.OldDueDate = oldDueDate.Format(constants.DateFormat)
			} else {
				rcv.OldDueDate = ""
			}
			if oldAmountPtr != nil {
				rcv.OldAmount = *oldAmountPtr
			} else {
				rcv.OldAmount = 0
			}
			if oldCurrencyPtr != nil {
				rcv.OldCurrencyCode = *oldCurrencyPtr
			} else {
				rcv.OldCurrencyCode = ""
			}
			rcv.InvoiceDate = ""
			if invoiceDate != nil {
				rcv.InvoiceDate = invoiceDate.Format(constants.DateFormat)
			}
			rcv.DueDate = ""
			if dueDate != nil {
				rcv.DueDate = dueDate.Format(constants.DateFormat)
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
							auditPayableMap[pid]["created_at"] = requestedAt.Format(constants.DateTimeFormat)
						}
						auditPayableMap[pid]["created_status"] = status
					} else if atype == "EDIT" {
						auditPayableMap[pid]["edited_by"] = requestedBy
						if requestedAt != nil {
							auditPayableMap[pid]["edited_at"] = requestedAt.Format(constants.DateTimeFormat)
						}
						auditPayableMap[pid]["edited_status"] = status
					} else if atype == "DELETE" {
						auditPayableMap[pid]["deleted_by"] = requestedBy
						if requestedAt != nil {
							auditPayableMap[pid]["deleted_at"] = requestedAt.Format(constants.DateTimeFormat)
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
							auditReceivableMap[rid]["created_at"] = requestedAt.Format(constants.DateTimeFormat)
						}
						auditReceivableMap[rid]["created_status"] = status
					} else if atype == "EDIT" {
						auditReceivableMap[rid]["edited_by"] = requestedBy
						if requestedAt != nil {
							auditReceivableMap[rid]["edited_at"] = requestedAt.Format(constants.DateTimeFormat)
						}
						auditReceivableMap[rid]["edited_status"] = status
					} else if atype == "DELETE" {
						auditReceivableMap[rid]["deleted_by"] = requestedBy
						if requestedAt != nil {
							auditReceivableMap[rid]["deleted_at"] = requestedAt.Format(constants.DateTimeFormat)
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data": map[string]interface{}{
				"payables":    payables,
				"receivables": receivables,
			},
		})
	}
}

// BulkRequestDeleteTransactions inserts DELETE audit actions for mixed transaction ids (payable or receivable)
func BulkRequestDeleteTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string   `json:"user_id"`
			TransactionIDs []string `json:"transaction_ids"`
			Reason         string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.TransactionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidJSON})
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidSession})
			return
		}

		txIDsPay := []string{}
		txIDsRec := []string{}
		for _, id := range req.TransactionIDs {
			if strings.HasPrefix(id, constants.ErrPrefixPayable) {
				txIDsPay = append(txIDsPay, id)
			} else if strings.HasPrefix(id, constants.ErrPrefixReceivable) {
				txIDsRec = append(txIDsRec, id)
			}
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to begin tx"})
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		insPay := `INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now()) RETURNING action_id`
		reasonArg := interface{}(nil)
		if strings.TrimSpace(req.Reason) != "" {
			reasonArg = req.Reason
		}
		for _, id := range txIDsPay {
			var actionID string
			if err := tx.QueryRow(ctx, insPay, id, reasonArg, requestedBy).Scan(&actionID); err == nil {
				// nop: collected if needed
			}
		}

		insRec := `INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now()) RETURNING action_id`
		for _, id := range txIDsRec {
			var actionID string
			if err := tx.QueryRow(ctx, insRec, id, reasonArg, requestedBy).Scan(&actionID); err == nil {
				// nop
			}
		}

		if err := tx.Commit(ctx); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to commit"})
			return
		}
		committed = true

		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "message": "delete requests created"})
	}
}

// BulkRejectTransactions rejects latest audit actions for mixed transaction ids (payable or receivable)
func BulkRejectTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string   `json:"user_id"`
			TransactionIDs []string `json:"transaction_ids"`
			Comment        string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.TransactionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidJSON})
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidSession})
			return
		}

		// separate ids
		payIDs := []string{}
		recIDs := []string{}
		for _, id := range req.TransactionIDs {
			if strings.HasPrefix(id, constants.ErrPrefixPayable) {
				payIDs = append(payIDs, id)
			} else if strings.HasPrefix(id, constants.ErrPrefixReceivable) {
				recIDs = append(recIDs, id)
			}
		}

		ctx := r.Context()
		var actionIDs []string

		// For payables: find latest action_id per payable and add to list
		if len(payIDs) > 0 {
			for _, pid := range payIDs {
				var aid string
				if err := pgxPool.QueryRow(ctx, `SELECT action_id FROM auditactionpayable WHERE payable_id = $1 ORDER BY requested_at DESC LIMIT 1`, pid).Scan(&aid); err == nil && aid != "" {
					actionIDs = append(actionIDs, aid)
				}
			}
		}

		// For receivables
		if len(recIDs) > 0 {
			for _, rid := range recIDs {
				var aid string
				if err := pgxPool.QueryRow(ctx, `SELECT action_id FROM auditactionreceivable WHERE receivable_id = $1 ORDER BY requested_at DESC LIMIT 1`, rid).Scan(&aid); err == nil && aid != "" {
					actionIDs = append(actionIDs, aid)
				}
			}
		}

		if len(actionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "no valid actions found for provided ids"})
			return
		}
		commentArg := interface{}(nil)
		if strings.TrimSpace(req.Comment) != "" {
			commentArg = req.Comment
		}
		if _, err := pgxPool.Exec(ctx, `UPDATE auditactionpayable SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3)`, checkerBy, commentArg, actionIDs); err != nil {
			// ignore error, try receivable update
		}
		if _, err := pgxPool.Exec(ctx, `UPDATE auditactionreceivable SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3)`, checkerBy, commentArg, actionIDs); err != nil {
			// ignore
		}

		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "rejected_count": len(actionIDs)})
	}
}

// BulkApproveTransactions approves latest audit actions for mixed transaction ids (payable or receivable)
func BulkApproveTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string   `json:"user_id"`
			TransactionIDs []string `json:"transaction_ids"`
			Comment        string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.TransactionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidJSON})
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidSession})
			return
		}

		// separate ids
		payIDs := []string{}
		recIDs := []string{}
		for _, id := range req.TransactionIDs {
			if strings.HasPrefix(id, constants.ErrPrefixPayable) {
				payIDs = append(payIDs, id)
			} else if strings.HasPrefix(id, constants.ErrPrefixReceivable) {
				recIDs = append(recIDs, id)
			}
		}

		ctx := r.Context()
		// Get latest action ids for provided transaction ids
		var actionIDs []string
		for _, pid := range payIDs {
			var aid string
			if err := pgxPool.QueryRow(ctx, `SELECT action_id FROM auditactionpayable WHERE payable_id = $1 ORDER BY requested_at DESC LIMIT 1`, pid).Scan(&aid); err == nil && aid != "" {
				actionIDs = append(actionIDs, aid)
			}
		}
		for _, rid := range recIDs {
			var aid string
			if err := pgxPool.QueryRow(ctx, `SELECT action_id FROM auditactionreceivable WHERE receivable_id = $1 ORDER BY requested_at DESC LIMIT 1`, rid).Scan(&aid); err == nil && aid != "" {
				actionIDs = append(actionIDs, aid)
			}
		}

		if len(actionIDs) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "no valid actions found for provided ids"})
			return
		}
		// First, remove PENDING_DELETE_APPROVAL audit actions and collect their target transaction ids
		var payableIDsToDelete []string
		var receivableIDsToDelete []string

		delPayQuery := `DELETE FROM auditactionpayable WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, payable_id`
		delPayRows, err := pgxPool.Query(ctx, delPayQuery, actionIDs)
		if err != nil {
			log.Printf("[WARN] failed to delete auditactionpayable rows: %v", err)
		} else {
			defer delPayRows.Close()
			for delPayRows.Next() {
				var aid, pid string
				if err := delPayRows.Scan(&aid, &pid); err == nil {
					payableIDsToDelete = append(payableIDsToDelete, pid)
				}
			}
		}

		delRecQuery := `DELETE FROM auditactionreceivable WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, receivable_id`
		delRecRows, err := pgxPool.Query(ctx, delRecQuery, actionIDs)
		if err != nil {
			log.Printf("[WARN] failed to delete auditactionreceivable rows: %v", err)
		} else {
			defer delRecRows.Close()
			for delRecRows.Next() {
				var aid, rid string
				if err := delRecRows.Scan(&aid, &rid); err == nil {
					receivableIDsToDelete = append(receivableIDsToDelete, rid)
				}
			}
		}

		// Mark canonical rows as deleted (soft-delete) instead of hard delete
		if len(payableIDsToDelete) > 0 {
			if _, err := pgxPool.Exec(ctx, `UPDATE tr_payables SET is_deleted = TRUE, updated_at = now() WHERE payable_id = ANY($1)`, payableIDsToDelete); err != nil {
				// log or ignore; continue
			}
		}
		if len(receivableIDsToDelete) > 0 {
			if _, err := pgxPool.Exec(ctx, `UPDATE tr_receivables SET is_deleted = TRUE, updated_at = now() WHERE receivable_id = ANY($1)`, receivableIDsToDelete); err != nil {
				// log or ignore
			}
		}

		// Approve the remaining actions
		commentArg := interface{}(nil)
		if strings.TrimSpace(req.Comment) != "" {
			commentArg = req.Comment
		}
		if _, err := pgxPool.Exec(ctx, `UPDATE auditactionpayable SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL'`, checkerBy, commentArg, actionIDs); err != nil {
			// ignore
		}
		if _, err := pgxPool.Exec(ctx, `UPDATE auditactionreceivable SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL'`, checkerBy, commentArg, actionIDs); err != nil {
			// ignore
		}

		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "approved_count": len(actionIDs)})
	}
}

func BulkCreateTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string                   `json:"user_id"`
			Items  []map[string]interface{} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidJSONShort})
			return
		}
		if req.UserID == "" || len(req.Items) == 0 {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "user_id and items are required"})
			return
		}

		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": constants.ErrInvalidSession})
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to begin tx"})
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		var createdPayables []string
		var createdReceivables []string
		var payableActionIDs []string
		var receivableActionIDs []string

		for idx, itm := range req.Items {
			tRaw, ok := itm["transaction_type"]
			if !ok {
				tx.Rollback(ctx)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": fmt.Sprintf("item %d missing transaction_type", idx)})
				return
			}
			txType := strings.ToUpper(fmt.Sprint(tRaw))

			entityName := fmt.Sprint(itm["entity_name"])         // required
			counterparty := fmt.Sprint(itm["counterparty_name"]) // required
			invoiceNumber := fmt.Sprint(itm["invoice_number"])   // required
			invDateStr := fmt.Sprint(itm["invoice_date"])        // optional
			dueDateStr := fmt.Sprint(itm["due_date"])            // optional
			currency := fmt.Sprint(itm["currency_code"])         // required

			amountF := 0.0
			if v, ok := itm["amount"]; ok {
				switch vv := v.(type) {
				case float64:
					amountF = vv
				case float32:
					amountF = float64(vv)
				case int:
					amountF = float64(vv)
				case int64:
					amountF = float64(vv)
				case string:
					fmt.Sscan(vv, &amountF)
				}
			}
			if entityName == "" || counterparty == "" || invoiceNumber == "" || currency == "" {
				tx.Rollback(ctx)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": fmt.Sprintf("item %d missing required fields", idx)})
				return
			}

			invDate := normalizeDate(invDateStr)
			dueDate := normalizeDate(dueDateStr)
			var invDateVal, dueDateVal interface{}
			if strings.TrimSpace(invDate) != "" {
				if t, e := time.Parse(constants.DateFormat, invDate); e == nil {
					invDateVal = t
				}
			}
			if strings.TrimSpace(dueDate) != "" {
				if t, e := time.Parse(constants.DateFormat, dueDate); e == nil {
					dueDateVal = t
				}
			}

			if txType == "PAYABLE" {
				var pid string
				q := `INSERT INTO tr_payables (entity_name, counterparty_name, invoice_number, invoice_date, due_date, amount, currency_code) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING payable_id`
				if err := tx.QueryRow(ctx, q, entityName, counterparty, invoiceNumber, invDateVal, dueDateVal, amountF, currency).Scan(&pid); err != nil {
					tx.Rollback(ctx)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": fmt.Sprintf("failed to insert payable item %d: %v", idx, err)})
					return
				}
				createdPayables = append(createdPayables, pid)
				var actionID string
				auditQ := `INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',NULL,$2,now()) RETURNING action_id`
				if err := tx.QueryRow(ctx, auditQ, pid, userName).Scan(&actionID); err != nil {
					tx.Rollback(ctx)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": fmt.Sprintf("failed to create audit for payable %s: %v", pid, err)})
					return
				}
				payableActionIDs = append(payableActionIDs, actionID)

			} else if txType == "RECEIVABLE" {
				var rid string
				q := `INSERT INTO tr_receivables (entity_name, counterparty_name, invoice_number, invoice_date, due_date, invoice_amount, currency_code) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING receivable_id`
				if err := tx.QueryRow(ctx, q, entityName, counterparty, invoiceNumber, invDateVal, dueDateVal, amountF, currency).Scan(&rid); err != nil {
					tx.Rollback(ctx)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": fmt.Sprintf("failed to insert receivable item %d: %v", idx, err)})
					return
				}
				createdReceivables = append(createdReceivables, rid)
				var actionID string
				auditQ := `INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',NULL,$2,now()) RETURNING action_id`
				if err := tx.QueryRow(ctx, auditQ, rid, userName).Scan(&actionID); err != nil {
					tx.Rollback(ctx)
					json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": fmt.Sprintf("failed to create audit for receivable %s: %v", rid, err)})
					return
				}
				receivableActionIDs = append(receivableActionIDs, actionID)

			} else {
				tx.Rollback(ctx)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": fmt.Sprintf("item %d unknown transaction_type: %s", idx, txType)})
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to commit"})
			return
		}
		committed = true

		resp := map[string]interface{}{constants.ValueSuccess: true}
		if len(createdPayables) > 0 {
			resp["created_payables"] = createdPayables
			resp["payable_action_ids"] = payableActionIDs
		}
		if len(createdReceivables) > 0 {
			resp["created_receivables"] = createdReceivables
			resp["receivable_action_ids"] = receivableActionIDs
		}

		json.NewEncoder(w).Encode(resp)
	}
}

func UpdateTransaction(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string                 `json:"user_id"`
			ID     string                 `json:"id"`
			Fields map[string]interface{} `json:"fields"`
			Reason string                 `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if req.UserID == "" || req.ID == "" || len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "user_id, id and fields are required")
			return
		}

		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSession)
			return
		}

		id := strings.TrimSpace(req.ID)
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		var actionID string

		if strings.HasPrefix(id, constants.ErrPrefixPayable) {
			// Update single payable using old_ pattern
			sel := `SELECT entity_name, counterparty_name, invoice_number, invoice_date, due_date, amount, currency_code FROM tr_payables WHERE payable_id=$1 FOR UPDATE`
			var curEntity, curCounter, curInvoice sqlNullString
			var curInvoiceDate, curDueDate sqlNullTime
			var curAmount sqlNullFloat
			var curCurrency sqlNullString
			if err := tx.QueryRow(ctx, sel, id).Scan(&curEntity, &curCounter, &curInvoice, &curInvoiceDate, &curDueDate, &curAmount, &curCurrency); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch payable: "+err.Error())
				return
			}

			sets := []string{}
			args := []interface{}{}
			pos := 1
			addStr := func(col, oldcol string, val interface{}, cur sqlNullString) {
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
				args = append(args, nullifyEmpty(fmt.Sprint(val)))
				args = append(args, cur.ValueOrZero())
				pos += 2
			}
			addDate := func(col, oldcol string, val interface{}, cur sqlNullTime) {
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
				if val == nil {
					args = append(args, nil)
				} else if s, ok := val.(string); ok && strings.TrimSpace(s) != "" {
					if t, e := time.Parse(constants.DateFormat, normalizeDate(s)); e == nil {
						args = append(args, t)
					} else {
						args = append(args, nil)
					}
				} else {
					args = append(args, nil)
				}
				args = append(args, cur.ValueOrZero())
				pos += 2
			}
			addFloat := func(col, oldcol string, val interface{}, cur sqlNullFloat) {
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
				if val == nil {
					args = append(args, nil)
				} else {
					switch vv := val.(type) {
					case float64:
						args = append(args, vv)
					case float32:
						args = append(args, float64(vv))
					case int:
						args = append(args, float64(vv))
					case int64:
						args = append(args, float64(vv))
					case string:
						var out float64
						fmt.Sscan(vv, &out)
						args = append(args, out)
					default:
						args = append(args, nil)
					}
				}
				args = append(args, cur.ValueOrZero())
				pos += 2
			}

			for k, v := range req.Fields {
				switch k {
				case "entity_name":
					addStr("entity_name", "old_entity_name", v, curEntity)
				case "counterparty_name":
					addStr("counterparty_name", "old_counterparty_name", v, curCounter)
				case "invoice_number":
					addStr("invoice_number", "old_invoice_number", v, curInvoice)
				case "invoice_date":
					addDate("invoice_date", "old_invoice_date", v, curInvoiceDate)
				case "due_date":
					addDate("due_date", "old_due_date", v, curDueDate)
				case "amount":
					addFloat("amount", "old_amount", v, curAmount)
				case "currency_code":
					addStr("currency_code", "old_currency_code", v, curCurrency)
				default:
					// ignore unknown
				}
			}
			if len(sets) == 0 {
				api.RespondWithResult(w, false, "no valid fields to update")
				tx.Rollback(ctx)
				return
			}
			q := "UPDATE tr_payables SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE payable_id=$%d RETURNING payable_id", pos)
			args = append(args, id)
			var updatedID string
			if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to update payable: "+err.Error())
				return
			}

			auditQ := `INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now()) RETURNING action_id`
			reasonArg := interface{}(nil)
			if strings.TrimSpace(req.Reason) != "" {
				reasonArg = req.Reason
			}
			if err := tx.QueryRow(ctx, auditQ, updatedID, reasonArg, userName).Scan(&actionID); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to create audit for payable: "+err.Error())
				return
			}

		} else if strings.HasPrefix(id, constants.ErrPrefixReceivable) {
			sel := `SELECT entity_name, counterparty_name, invoice_number, invoice_date, due_date, invoice_amount, currency_code FROM tr_receivables WHERE receivable_id=$1 FOR UPDATE`
			var curEntity, curCounter, curInvoice sqlNullString
			var curInvoiceDate, curDueDate sqlNullTime
			var curAmount sqlNullFloat
			var curCurrency sqlNullString
			if err := tx.QueryRow(ctx, sel, id).Scan(&curEntity, &curCounter, &curInvoice, &curInvoiceDate, &curDueDate, &curAmount, &curCurrency); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch receivable: "+err.Error())
				return
			}

			sets := []string{}
			args := []interface{}{}
			pos := 1
			addStr := func(col, oldcol string, val interface{}, cur sqlNullString) {
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
				args = append(args, nullifyEmpty(fmt.Sprint(val)))
				args = append(args, cur.ValueOrZero())
				pos += 2
			}
			addDate := func(col, oldcol string, val interface{}, cur sqlNullTime) {
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
				if val == nil {
					args = append(args, nil)
				} else if s, ok := val.(string); ok && strings.TrimSpace(s) != "" {
					if t, e := time.Parse(constants.DateFormat, normalizeDate(s)); e == nil {
						args = append(args, t)
					} else {
						args = append(args, nil)
					}
				} else {
					args = append(args, nil)
				}
				args = append(args, cur.ValueOrZero())
				pos += 2
			}
			addFloat := func(col, oldcol string, val interface{}, cur sqlNullFloat) {
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, col, pos, oldcol, pos+1))
				if val == nil {
					args = append(args, nil)
				} else {
					switch vv := val.(type) {
					case float64:
						args = append(args, vv)
					case float32:
						args = append(args, float64(vv))
					case int:
						args = append(args, float64(vv))
					case int64:
						args = append(args, float64(vv))
					case string:
						var out float64
						fmt.Sscan(vv, &out)
						args = append(args, out)
					default:
						args = append(args, nil)
					}
				}
				args = append(args, cur.ValueOrZero())
				pos += 2
			}

			for k, v := range req.Fields {
				switch k {
				case "entity_name":
					addStr("entity_name", "old_entity_name", v, curEntity)
				case "counterparty_name":
					addStr("counterparty_name", "old_counterparty_name", v, curCounter)
				case "invoice_number":
					addStr("invoice_number", "old_invoice_number", v, curInvoice)
				case "invoice_date":
					addDate("invoice_date", "old_invoice_date", v, curInvoiceDate)
				case "due_date":
					addDate("due_date", "old_due_date", v, curDueDate)
				case "invoice_amount":
					addFloat("invoice_amount", "old_invoice_amount", v, curAmount)
				case "currency_code":
					addStr("currency_code", "old_currency_code", v, curCurrency)
				default:
					// ignore unknown
				}
			}
			if len(sets) == 0 {
				api.RespondWithResult(w, false, "no valid fields to update")
				tx.Rollback(ctx)
				return
			}
			q := "UPDATE tr_receivables SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE receivable_id=$%d RETURNING receivable_id", pos)
			args = append(args, id)
			var updatedID string
			if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to update receivable: "+err.Error())
				return
			}

			auditQ := `INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now()) RETURNING action_id`
			reasonArg := interface{}(nil)
			if strings.TrimSpace(req.Reason) != "" {
				reasonArg = req.Reason
			}
			if err := tx.QueryRow(ctx, auditQ, updatedID, reasonArg, userName).Scan(&actionID); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to create audit for receivable: "+err.Error())
				return
			}

		} else {
			api.RespondWithError(w, http.StatusBadRequest, "unknown id prefix")
			tx.Rollback(ctx)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		committed = true

		api.RespondWithPayload(w, true, "", map[string]string{"id": id, "action_id": actionID})
	}
}
