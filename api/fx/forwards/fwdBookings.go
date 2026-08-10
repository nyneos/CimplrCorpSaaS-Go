package forwards

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/fx/auditutil"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"CimplrCorpSaas/internal/ctxutil"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"CimplrCorpSaas/api/constants"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

var ErrForwardBookingFileAlreadyUploaded = errors.New("forward booking file already uploaded")

const duplicateForwardBookingUploadMessage = "This forward booking file was already uploaded earlier. Please upload a different file."

var ErrForwardConfirmationFileAlreadyUploaded = errors.New("forward confirmation file already uploaded")

const duplicateForwardConfirmationUploadMessage = "This forward confirmation file was already uploaded earlier. Please upload a different file."

func decodeForwardDownloadID(raw string) string {
	id := strings.TrimSpace(raw)
	if id == "" {
		return ""
	}
	decoded, err := base64.StdEncoding.DecodeString(id)
	if err != nil {
		return id
	}
	decodedID := strings.TrimSpace(string(decoded))
	if decodedID == "" {
		return id
	}
	for _, r := range decodedID {
		if r < 32 || r == 127 {
			return id
		}
	}
	return decodedID
}

func forwardDownloadKeyExpr(downloadType string) string {
	switch strings.ToUpper(strings.TrimSpace(downloadType)) {
	case "BOOKING":
		return "COALESCE(NULLIF(upload_s3_key, ''), '')"
	case "CONFIRMATION":
		return "COALESCE(NULLIF(confirmation_upload_s3_key, ''), '')"
	default:
		return "COALESCE(NULLIF(confirmation_upload_s3_key, ''), NULLIF(upload_s3_key, ''), '')"
	}
}

func forwardDownloadAuditType(downloadType string, s3Key string) string {
	switch strings.ToUpper(strings.TrimSpace(downloadType)) {
	case "CONFIRMATION":
		return "CONFIRMATION"
	case "BOOKING":
		return "BOOKING"
	default:
		if strings.Contains(strings.ToLower(s3Key), "confirmation") {
			return "CONFIRMATION"
		}
		return "BOOKING"
	}
}

func existingForwardBookingUploadKeys(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT upload_s3_key
		FROM forward_bookings
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

func ensureUniqueForwardBookingUpload(ctx context.Context, pool *pgxpool.Pool, fileBytes []byte) error {
	keys, err := existingForwardBookingUploadKeys(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to check duplicate forward booking upload: %w", err)
	}
	duplicateKey, err := s3storage.FindDuplicateObjectKey(ctx, fileBytes, keys)
	if err != nil {
		return fmt.Errorf("failed to compare forward booking upload content: %w", err)
	}
	if duplicateKey != "" {
		return ErrForwardBookingFileAlreadyUploaded
	}
	return nil
}

func existingForwardConfirmationUploadKeys(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT confirmation_upload_s3_key
		FROM forward_bookings
		WHERE COALESCE(TRIM(confirmation_upload_s3_key), '') <> ''
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

func ensureUniqueForwardConfirmationUpload(ctx context.Context, pool *pgxpool.Pool, fileBytes []byte) error {
	keys, err := existingForwardConfirmationUploadKeys(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to check duplicate forward confirmation upload: %w", err)
	}
	duplicateKey, err := s3storage.FindDuplicateObjectKey(ctx, fileBytes, keys)
	if err != nil {
		return fmt.Errorf("failed to compare forward confirmation upload content: %w", err)
	}
	if duplicateKey != "" {
		return ErrForwardConfirmationFileAlreadyUploaded
	}
	return nil
}

func NormalizeDate(dateStr string) string {
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return ""
	}

	layouts := []string{
		constants.DateFormat,
		constants.DateFormatAlt,
		"2006/01/02",
		"02/01/2006",
		"2006.01.02",
		"02.01.2006",
		time.RFC3339,
		constants.DateTimeFormat,
		constants.DateFormatISO,
	}

	layouts = append(layouts, []string{
		constants.DateFormatDash,
		"02-Jan-06",
		"2-Jan-2006",
		"2-Jan-06",
		"02-Jan-2006 15:04:05",
	}...)

	for _, l := range layouts {
		if t, err := time.Parse(l, dateStr); err == nil {
			return t.Format(constants.DateFormat)
		}
	}

	return ""
}

func normalizeOrderType(orderType string) string {
	// Normalize input
	orderType = strings.TrimSpace(orderType)
	orderType = strings.ToLower(orderType)

	// Define mappings
	buyAliases := map[string]bool{
		"buy":      true,
		"purchase": true,
		"b":        true, // optional shorthand
	}

	sellAliases := map[string]bool{
		"sell": true,
		"sale": true,
		"s":    true, // optional shorthand
	}

	// Check mapping
	if buyAliases[orderType] {
		return "Buy"
	}
	if sellAliases[orderType] {
		return "Sell"
	}

	// Unknown type — keep original formatting
	// You may want to return "" or "Invalid" instead if you want strict validation
	return orderType
}

func forwardUploadUserName(userID string) string {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "user"
	}
	for _, session := range auth.GetActiveSessions() {
		if session.UserID == userID {
			if name := strings.TrimSpace(session.Name); name != "" {
				return name
			}
			break
		}
	}
	return userID
}

// Handler: AddForwardBookingManualEntry
func AddForwardBookingManualEntry(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                      string      `json:"user_id"`
			InternalReferenceID         string      `json:"internal_reference_id"`
			EntityLevel0                string      `json:"entity_level_0"`
			EntityLevel1                string      `json:"entity_level_1"`
			EntityLevel2                string      `json:"entity_level_2"`
			EntityLevel3                string      `json:"entity_level_3"`
			LocalCurrency               string      `json:"local_currency"`
			OrderType                   string      `json:"order_type"`
			TransactionType             string      `json:"transaction_type"`
			Counterparty                string      `json:"counterparty"`
			ModeOfDelivery              string      `json:"mode_of_delivery"`
			DeliveryPeriod              string      `json:"delivery_period"`
			AddDate                     string      `json:"add_date"`
			SettlementDate              string      `json:"settlement_date"`
			MaturityDate                string      `json:"maturity_date"`
			DeliveryDate                string      `json:"delivery_date"`
			CurrencyPair                string      `json:"currency_pair"`
			BaseCurrency                string      `json:"base_currency"`
			QuoteCurrency               string      `json:"quote_currency"`
			BookingAmount               json.Number `json:"booking_amount"`
			ValueType                   string      `json:"value_type"`
			ActualValueBaseCurrency     json.Number `json:"actual_value_base_currency"`
			SpotRate                    json.Number `json:"spot_rate"`
			ForwardPoints               json.Number `json:"forward_points"`
			BankMargin                  json.Number `json:"bank_margin"`
			TotalRate                   json.Number `json:"total_rate"`
			ValueQuoteCurrency          json.Number `json:"value_quote_currency"`
			InterveningRateQuoteToLocal json.Number `json:"intervening_rate_quote_to_local"`
			ValueLocalCurrency          json.Number `json:"value_local_currency"`
			InternalDealer              string      `json:"internal_dealer"`
			CounterpartyDealer          string      `json:"counterparty_dealer"`
			Remarks                     string      `json:"remarks"`
			Narration                   string      `json:"narration"`
			TransactionTimestamp        string      `json:"transaction_timestamp"`
		}

		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&req); err != nil || req.UserID == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		missing := make([]string, 0)
		for _, f := range []struct{ name, value string }{
			{"internal_reference_id", req.InternalReferenceID},
			{"entity_level_0", req.EntityLevel0},
			{"order_type", req.OrderType},
			{"transaction_type", req.TransactionType},
			{"counterparty", req.Counterparty},
			{"currency_pair", req.CurrencyPair},
			{"value_type", req.ValueType},
			{"mode_of_delivery", req.ModeOfDelivery},
			{"internal_dealer", req.InternalDealer},
			{"counterparty_dealer", req.CounterpartyDealer},
		} {
			if strings.TrimSpace(f.value) == "" {
				missing = append(missing, f.name)
			}
		}
		if amt, err := req.BookingAmount.Float64(); err != nil || amt <= 0 {
			missing = append(missing, "booking_amount")
		}
		if len(missing) > 0 {
			respondEnvelopeError(w, http.StatusBadRequest,
				"missing or invalid required field(s): "+strings.Join(missing, ", "))
			return
		}
		// We no longer enforce a strict magnitude limit here; DB has unlimited numeric precision.
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondEnvelopeError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Check entity_level_0 access
		found := false
		for _, bu := range buNames {
			if bu == req.EntityLevel0 {
				found = true
				break
			}
		}
		if !found {
			respondEnvelopeError(w, http.StatusForbidden, "You do not have access to this business unit")
			return
		}
		createRow := forwardBookingRow{
			InternalReferenceID:         req.InternalReferenceID,
			EntityLevel0:                req.EntityLevel0,
			EntityLevel1:                req.EntityLevel1,
			EntityLevel2:                req.EntityLevel2,
			EntityLevel3:                req.EntityLevel3,
			LocalCurrency:               req.LocalCurrency,
			OrderType:                   req.OrderType,
			TransactionType:             req.TransactionType,
			Counterparty:                req.Counterparty,
			ModeOfDelivery:              req.ModeOfDelivery,
			DeliveryPeriod:              req.DeliveryPeriod,
			AddDate:                     req.AddDate,
			SettlementDate:              req.SettlementDate,
			MaturityDate:                req.MaturityDate,
			DeliveryDate:                req.DeliveryDate,
			CurrencyPair:                req.CurrencyPair,
			BaseCurrency:                req.BaseCurrency,
			QuoteCurrency:               req.QuoteCurrency,
			BookingAmount:               jsonNumberToFloatPtr(req.BookingAmount),
			ValueType:                   req.ValueType,
			ActualValueBaseCurrency:     jsonNumberToFloatPtr(req.ActualValueBaseCurrency),
			SpotRate:                    jsonNumberToFloatPtr(req.SpotRate),
			ForwardPoints:               jsonNumberToFloatPtr(req.ForwardPoints),
			BankMargin:                  jsonNumberToFloatPtr(req.BankMargin),
			TotalRate:                   jsonNumberToFloatPtr(req.TotalRate),
			ValueQuoteCurrency:          jsonNumberToFloatPtr(req.ValueQuoteCurrency),
			InterveningRateQuoteToLocal: jsonNumberToFloatPtr(req.InterveningRateQuoteToLocal),
			ValueLocalCurrency:          jsonNumberToFloatPtr(req.ValueLocalCurrency),
			InternalDealer:              req.InternalDealer,
			CounterpartyDealer:          req.CounterpartyDealer,
			Remarks:                     req.Remarks,
			Narration:                   req.Narration,
			TransactionTimestamp:        req.TransactionTimestamp,
			ProcessingStatus:            "pending",
		}
		if !runtime.Enforce(r.Context(), w, r, pool, runtime.EnforceInput{
			EventCode:           common.TriggerPreCreate,
			ModuleCode:          common.ModuleFX,
			SubModule:           "FORWARD_BOOKING",
			EntityCode:          req.EntityLevel0,
			ActorUserID:         req.UserID,
			HandlerName:         "AddForwardBookingManualEntry",
			APIPath:             "/fx/forwards/manual-entry",
			DefaultBlockMessage: "Forward booking create blocked by policy",
			Fields:              buildForwardBookingPolicyFields(createRow),
		}) {
			return
		}
		query := `INSERT INTO forward_bookings (
		       internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period, add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency, booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin, total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency, internal_dealer, counterparty_dealer, remarks, narration, transaction_timestamp, processing_status
	       ) VALUES (
		       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34
	       ) RETURNING internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period, add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency, booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin, total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency, internal_dealer, counterparty_dealer, remarks, narration, transaction_timestamp, processing_status`
		// helper to pass numeric values as strings (preserves precision for Postgres numeric)
		numOrNil := func(n json.Number) interface{} {
			if n == "" {
				return nil
			}
			return n.String()
		}

		values := []interface{}{
			req.InternalReferenceID,
			req.EntityLevel0,
			req.EntityLevel1,
			req.EntityLevel2,
			req.EntityLevel3,
			req.LocalCurrency,
			req.OrderType,
			req.TransactionType,
			req.Counterparty,
			req.ModeOfDelivery,
			req.DeliveryPeriod,
			req.AddDate,
			req.SettlementDate,
			req.MaturityDate,
			req.DeliveryDate,
			req.CurrencyPair,
			req.BaseCurrency,
			req.QuoteCurrency,
			numOrNil(req.BookingAmount),
			req.ValueType,
			numOrNil(req.ActualValueBaseCurrency),
			numOrNil(req.SpotRate),
			numOrNil(req.ForwardPoints),
			numOrNil(req.BankMargin),
			numOrNil(req.TotalRate),
			numOrNil(req.ValueQuoteCurrency),
			numOrNil(req.InterveningRateQuoteToLocal),
			numOrNil(req.ValueLocalCurrency),
			req.InternalDealer,
			req.CounterpartyDealer,
			req.Remarks,
			req.Narration,
			req.TransactionTimestamp,
			"pending",
		}
		row := pool.QueryRow(r.Context(), query, values...)
		cols := []string{"internal_reference_id", "entity_level_0", "entity_level_1", "entity_level_2", "entity_level_3", "local_currency", "order_type", "transaction_type", "counterparty", "mode_of_delivery", "delivery_period", "add_date", "settlement_date", "maturity_date", "delivery_date", "currency_pair", "base_currency", "quote_currency", "booking_amount", "value_type", "actual_value_base_currency", "spot_rate", "forward_points", "bank_margin", "total_rate", "value_quote_currency", "intervening_rate_quote_to_local", "value_local_currency", "internal_dealer", "counterparty_dealer", "remarks", "narration", "transaction_timestamp", "processing_status"}
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := row.Scan(valPtrs...); err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		result := make(map[string]interface{})
		for i, col := range cols {
			result[col] = vals[i]
		}
		var systemTransactionID string
		_ = pool.QueryRow(r.Context(), `SELECT system_transaction_id FROM forward_bookings WHERE internal_reference_id = $1 AND entity_level_0 = $2 LIMIT 1`, req.InternalReferenceID, req.EntityLevel0).Scan(&systemTransactionID)
		if strings.TrimSpace(systemTransactionID) != "" {
			actor := auditutil.Actor(req.UserID)
			auditutil.RecordActionPGX(r.Context(), pool, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: systemTransactionID, ActionType: constants.AuditActionCreate, Status: constants.StatusPendingApproval, Reason: "", RequestedBy: actor, OldValues: nil, NewValues: result})
			triggerForwardBookingNotif(r.Context(), pool, routeForwardManualEntry, "CREATE", actor, "pending", []string{systemTransactionID})
			dmsjobs.FireDmsEvent(pool, "FX", "FORWARD_BOOKING", "POST_CREATE", []string{systemTransactionID}, actor)
		}
		respondEnvelopeSuccess(w, "Forward booking created successfully", result)
	}
}

// Handler: GetEntityRelevantForwardBookings
func GetEntityRelevantForwardBookings(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondEnvelopeError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT *
			FROM forward_bookings
			WHERE entity_level_0 = ANY($1)
			  AND COALESCE(is_deleted, false) = false
		`, buNames)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		fieldDescs := rows.FieldDescriptions()
		cols := make([]string, len(fieldDescs))
		for i, fd := range fieldDescs {
			cols[i] = fd.Name
		}
		var data []map[string]interface{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err == nil {
				rowMap := make(map[string]interface{})
				for i, col := range cols {
					v := normalizeMTMRowValue(vals[i])
					switch val := v.(type) {
					case nil:
						rowMap[col] = nil
					case []byte:
						// Try to convert []byte to string, float, or UUID
						s := string(val)
						// Try float
						if f, err := strconv.ParseFloat(s, 64); err == nil {
							rowMap[col] = f
						} else {
							rowMap[col] = s
						}
					case int64:
						rowMap[col] = val
					case float64:
						rowMap[col] = val
					case bool:
						rowMap[col] = val
					case string:
						rowMap[col] = val
					case pgtype.Numeric:
						if !val.Valid {
							rowMap[col] = nil
						} else if f, err := val.Float64Value(); err == nil && f.Valid {
							rowMap[col] = f.Float64
						} else {
							rowMap[col] = fmt.Sprintf("%v", val)
						}
					default:
						rowMap[col] = fmt.Sprintf("%v", val)
					}
				}
				data = append(data, rowMap)
			}
		}
		rows.Close()
		respondEnvelopeSuccess(w, "Success", data)
	}
}

// Handler: UploadForwardBookingsMulti
// Multi-file upload for forward bookings (CSV/Excel)
// Accepts multipart/form-data with files, uses user_id from middleware, checks buName access

func UploadForwardBookingsMulti(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse multipart form
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}
		files := r.MultipartForm.File["files"]
		if len(files) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondEnvelopeError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		if !runtime.Enforce(r.Context(), w, r, pool, runtime.EnforceInput{
			EventCode:           common.TriggerPreUpload,
			ModuleCode:          common.ModuleFX,
			SubModule:           "FORWARD_BOOKING",
			ActorUserID:         r.FormValue(constants.KeyUserID),
			HandlerName:         "UploadForwardBookingsMulti",
			APIPath:             "/fx/forwards/upload-multi",
			DefaultBlockMessage: "Forward booking upload blocked by policy",
			Fields: map[string]interface{}{
				"file_count": len(files),
			},
		}) {
			return
		}
		// Delegate to service
		ctx := r.Context()
		results, err := processUploadForwardBookings(ctx, pool, r, buNames)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		uploadedBy := forwardUploadUserName(r.FormValue(constants.KeyUserID))
		var uploadedIDs []string
		for _, result := range results {
			s3Key, _ := result["upload_s3_key"].(string)
			if strings.TrimSpace(s3Key) == "" {
				continue
			}
			if inserted, ok := result["inserted"].(int); ok && inserted > 0 {
				ids := fetchBookingIDsByUploadKey(ctx, pool, s3Key)
				uploadedIDs = append(uploadedIDs, ids...)
				triggerForwardBookingNotif(ctx, pool, routeForwardUploadMulti, "UPLOAD", uploadedBy, "pending", ids)
			}
		}
		if len(uploadedIDs) > 0 {
			dmsjobs.FireDmsEvent(pool, "FX", "FORWARD_BOOKING", "POST_UPLOAD", uploadedIDs, uploadedBy)
		}
		respondEnvelopeSuccess(w, "Forward bookings uploaded successfully", map[string]interface{}{
			"results": results,
		})
	}
}

// processUploadForwardBookings contains all business logic for the forward bookings upload.
// It is called by the thin UploadForwardBookingsMulti controller.
func processUploadForwardBookings(ctx context.Context, pool *pgxpool.Pool, r *http.Request, buNames []string) ([]map[string]interface{}, error) {
	files := r.MultipartForm.File["files"]
	results := []map[string]interface{}{}
	uploadedBy := forwardUploadUserName(r.FormValue(constants.KeyUserID))
	for _, fileHeader := range files {
		file, err := fileHeader.Open()
		if err != nil {
			results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToOpenFile, "details": err.Error()})
			continue
		}
		// Read all file bytes for parsing, hashing, and deferred S3 upload.
		fileBytes, _ := io.ReadAll(file)
		file.Close()
		if s3storage.IsS3UploadEnabled() {
			if err := ensureUniqueForwardBookingUpload(ctx, pool, fileBytes); err != nil {
				message := err.Error()
				if errors.Is(err, ErrForwardBookingFileAlreadyUploaded) {
					message = duplicateForwardBookingUploadMessage
				}
				results = append(results, map[string]interface{}{
					"filename":             fileHeader.Filename,
					"inserted":             0,
					"errors":               []map[string]interface{}{},
					"invalidRows":          []map[string]interface{}{},
					"message":              message,
					constants.ValueSuccess: false,
				})
				continue
			}
		}
		fileExt := strings.ToLower(filepath.Ext(fileHeader.Filename))
		s3Key := s3storage.BuildUploadedS3Key("fx/forwards/bookings", "", fileHeader.Filename, uploadedBy, time.Now().UTC())
		ext := fileExt
		var rows []map[string]interface{}
		if ext == ".csv" {
			reader := csv.NewReader(bytes.NewReader(fileBytes))
			headers, err := reader.Read()
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToReadCSVHeaders, "details": err.Error()})
				continue
			}
			for i, h := range headers {
				headers[i] = strings.TrimSpace(strings.TrimPrefix(h, constants.ErrInvalidJSONBOM))
			}
			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					continue
				}
				row := make(map[string]interface{})
				for i, h := range headers {
					row[h] = record[i]
				}
				rows = append(rows, row)
			}
		} else if ext == ".xls" || ext == ".xlsx" {
			tmpFile, err := os.CreateTemp("", constants.ErrExposureUploadFilenamePattern)
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToCreateTempFile, "details": err.Error()})
				continue
			}
			_, err = tmpFile.Write(fileBytes)
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToCopyFile, "details": err.Error()})
				tmpFile.Close()
				continue
			}
			tmpFile.Close()
			f, err := excelize.OpenFile(tmpFile.Name())
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToParseExcelFile, "details": err.Error()})
				continue
			}
			sheetName := f.GetSheetName(0)
			rowsData, err := f.GetRows(sheetName)
			if err != nil || len(rowsData) < 2 {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrNoDataToUpload})
				continue
			}
			headers := rowsData[0]
			for _, record := range rowsData[1:] {
				row := make(map[string]interface{})
				for i, h := range headers {
					if i < len(record) {
						row[h] = record[i]
					} else {
						row[h] = nil
					}
				}
				rows = append(rows, row)
			}
			os.Remove(tmpFile.Name())
		} else {
			results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrUnsupportedFileType})
			continue
		}
		if len(rows) == 0 {
			results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrNoDataToUpload})
			continue
		}
		invalidRows := []map[string]interface{}{}
		for i, row := range rows {
			entityLevel0, _ := row["entity_level_0"].(string)
			if !contains(buNames, entityLevel0) {
				invalidRows = append(invalidRows, map[string]interface{}{"row": i + 1, "entity": entityLevel0})
			}
			if ot, ok := row["order_type"].(string); ok {
				row["order_type"] = normalizeOrderType(ot)
			}

			for k, v := range row {
				// Normalize dates
				if isDateColumn(k) {
					if v == nil || v == "" {
						row[k] = nil
						continue
					}
					norm := NormalizeDate(fmt.Sprint(v))
					if norm == "" {
						row[k] = nil
					} else {
						row[k] = norm
					}
					continue
				}

				lower := strings.ToLower(k)
				if lower == "booking_amount" || lower == "actual_value_base_currency" || lower == "spot_rate" || lower == "forward_points" || lower == "bank_margin" || lower == "total_rate" || lower == "value_quote_currency" || lower == "intervening_rate_quote_to_local" || lower == "value_local_currency" {
					s := fmt.Sprint(v)
					s = strings.TrimSpace(s)
					if s == "" {
						row[k] = nil
					} else {
						if f, err := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64); err == nil {
							row[k] = f
						} else {
							row[k] = v
						}
					}
				}
			}
		}
		if len(invalidRows) > 0 {
			results = append(results, map[string]interface{}{
				"filename":      fileHeader.Filename,
				"upload_s3_key": "",
				"inserted":      0,
				"errors":        []map[string]interface{}{},
				"invalidRows":   invalidRows,
			})
			continue
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			results = append(results, map[string]interface{}{
				"filename":      fileHeader.Filename,
				"upload_s3_key": "",
				"inserted":      0,
				"errors":        []map[string]interface{}{{constants.ValueError: constants.ErrFailedToStartDBTransaction, "details": err.Error()}},
				"invalidRows":   invalidRows,
			})
			continue
		}
		defer tx.Rollback(ctx)

		uploadS3Key := ""
		s3Uploaded := false
		if s3storage.IsS3UploadEnabled() {
			contentType := s3storage.DetectContentType(fileBytes)
			if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
				results = append(results, map[string]interface{}{
					"filename":      fileHeader.Filename,
					"upload_s3_key": "",
					"inserted":      0,
					"errors":        []map[string]interface{}{{constants.ValueError: constants.ErrFailedToUploadFileToS3, "details": err.Error()}},
					"invalidRows":   invalidRows,
				})
				continue
			}
			uploadS3Key = s3Key
			s3Uploaded = true
		}

		successCount := 0
		errorRows := []map[string]interface{}{}
		for i, row := range rows {
			query := `INSERT INTO forward_bookings (
				internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period, add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency, booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin, total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency, internal_dealer, counterparty_dealer, remarks, narration, transaction_timestamp, processing_status, upload_s3_key
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35
			)`
			values := []interface{}{
				row["internal_reference_id"], row["entity_level_0"], row["entity_level_1"], row["entity_level_2"], row["entity_level_3"], row["local_currency"], row["order_type"], row["transaction_type"], row["counterparty"], row["mode_of_delivery"], row["delivery_period"], row["add_date"], row["settlement_date"], row["maturity_date"], row["delivery_date"], row["currency_pair"], row["base_currency"], row["quote_currency"], row["booking_amount"], row["value_type"], row["actual_value_base_currency"], row["spot_rate"], row["forward_points"], row["bank_margin"], row["total_rate"], row["value_quote_currency"], row["intervening_rate_quote_to_local"], row["value_local_currency"], row["internal_dealer"], row["counterparty_dealer"], row["remarks"], row["narration"], row["transaction_timestamp"], "pending", uploadS3Key,
			}
			_, err := tx.Exec(ctx, query, values...)
			if err != nil {
				errorRows = append(errorRows, map[string]interface{}{"row": i + 1, constants.ValueError: "Database insert error", "details": err.Error()})
				break
			} else {
				successCount++
			}
		}
		if len(errorRows) > 0 {
			if s3Uploaded {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					fmt.Printf("[forward-upload] failed to cleanup S3 object for %s after insert failure: %v\n", fileHeader.Filename, cleanupErr)
				}
			}
			results = append(results, map[string]interface{}{
				"filename":      fileHeader.Filename,
				"upload_s3_key": "",
				"inserted":      0,
				"errors":        errorRows,
				"invalidRows":   invalidRows,
			})
			continue
		}
		if err := tx.Commit(ctx); err != nil {
			if s3Uploaded {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					fmt.Printf("[forward-upload] failed to cleanup S3 object for %s after commit failure: %v\n", fileHeader.Filename, cleanupErr)
				}
			}
			results = append(results, map[string]interface{}{
				"filename":      fileHeader.Filename,
				"upload_s3_key": "",
				"inserted":      0,
				"errors":        []map[string]interface{}{{constants.ValueError: constants.ErrFailedToCommitTransaction, "details": err.Error()}},
				"invalidRows":   invalidRows,
			})
			continue
		}
		results = append(results, map[string]interface{}{
			"filename":             fileHeader.Filename,
			"upload_s3_key":        uploadS3Key,
			"inserted":             successCount,
			"errors":               errorRows,
			"invalidRows":          invalidRows,
			constants.ValueSuccess: successCount > 0,
		})
		recordForwardUploadAudit(ctx, pool, uploadS3Key, uploadedBy)
	}
	return results, nil
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}

func recordForwardUploadAudit(ctx context.Context, pool *pgxpool.Pool, uploadS3Key, uploadedBy string) {
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	uploadedBy = strings.TrimSpace(uploadedBy)
	if uploadS3Key == "" || uploadedBy == "" {
		return
	}
	rows, err := pool.Query(ctx, `SELECT system_transaction_id FROM forward_bookings WHERE upload_s3_key = $1`, uploadS3Key)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var systemTransactionID string
		if err := rows.Scan(&systemTransactionID); err == nil {
			auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: systemTransactionID, ActionType: constants.AuditActionCreate, Status: constants.StatusPendingApproval, Reason: "", RequestedBy: uploadedBy, OldValues: nil, NewValues: map[string]interface{}{"upload_s3_key": uploadS3Key}})
		}
	}
}

// Handler: UploadForwardConfirmationsMulti

func UploadForwardConfirmationsMulti(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse multipart form
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}
		files := r.MultipartForm.File["files"]
		if len(files) == 0 {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondEnvelopeError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		// Delegate to service
		ctx := r.Context()
		results, err := processUploadForwardConfirmations(ctx, pool, r, buNames)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		uploadedBy := forwardUploadUserName(r.FormValue(constants.KeyUserID))
		for _, result := range results {
			s3Key, _ := result["upload_s3_key"].(string)
			if strings.TrimSpace(s3Key) == "" {
				continue
			}
			if updated, ok := result["updated"].(int); ok && updated > 0 {
				triggerForwardConfirmationNotif(ctx, pool, routeForwardUploadConfirmations, "UPLOAD", uploadedBy, "pending", fetchBookingIDsByConfirmationUploadKey(ctx, pool, s3Key))
			}
		}
		respondEnvelopeSuccess(w, "Forward confirmations uploaded successfully", map[string]interface{}{
			"results": results,
		})
	}
}

func processUploadForwardConfirmations(ctx context.Context, pool *pgxpool.Pool, r *http.Request, buNames []string) ([]map[string]interface{}, error) {
	files := r.MultipartForm.File["files"]
	results := []map[string]interface{}{}
	uploadedBy := forwardUploadUserName(r.FormValue(constants.KeyUserID))
	for _, fileHeader := range files {
		file, err := fileHeader.Open()
		if err != nil {
			results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToOpenFile, "details": err.Error()})
			continue
		}
		// Read all file bytes for parsing, hashing, and deferred S3 upload.
		fileBytes, _ := io.ReadAll(file)
		file.Close()
		if s3storage.IsS3UploadEnabled() {
			if err := ensureUniqueForwardConfirmationUpload(ctx, pool, fileBytes); err != nil {
				message := err.Error()
				if errors.Is(err, ErrForwardConfirmationFileAlreadyUploaded) {
					message = duplicateForwardConfirmationUploadMessage
				}
				results = append(results, map[string]interface{}{
					"filename":             fileHeader.Filename,
					"updated":              0,
					"errors":               []map[string]interface{}{},
					"invalidRows":          []map[string]interface{}{},
					"message":              message,
					constants.ValueSuccess: false,
				})
				continue
			}
		}
		fileExt := strings.ToLower(filepath.Ext(fileHeader.Filename))
		s3Key := s3storage.BuildUploadedS3Key("fx/forwards/confirmations", "", fileHeader.Filename, uploadedBy, time.Now().UTC())
		ext := fileExt
		var rows []map[string]interface{}
		if ext == ".csv" {
			reader := csv.NewReader(bytes.NewReader(fileBytes))
			headers, err := reader.Read()
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToReadCSVHeaders, "details": err.Error()})
				continue
			}
			for i, h := range headers {
				headers[i] = strings.TrimSpace(strings.TrimPrefix(h, constants.ErrInvalidJSONBOM))
			}
			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					continue
				}
				row := make(map[string]interface{})
				for i, h := range headers {
					row[h] = record[i]
				}
				rows = append(rows, row)
			}
		} else if ext == ".xls" || ext == ".xlsx" {
			tmpFile, err := os.CreateTemp("", constants.ErrExposureUploadFilenamePattern)
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToCreateTempFile, "details": err.Error()})
				continue
			}
			_, err = tmpFile.Write(fileBytes)
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToCopyFile, "details": err.Error()})
				tmpFile.Close()
				continue
			}
			tmpFile.Close()
			f, err := excelize.OpenFile(tmpFile.Name())
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToParseExcelFile, "details": err.Error()})
				continue
			}
			sheetName := f.GetSheetName(0)
			rowsData, err := f.GetRows(sheetName)
			if err != nil || len(rowsData) < 2 {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrNoDataToUpload})
				continue
			}
			headers := rowsData[0]
			for _, record := range rowsData[1:] {
				row := make(map[string]interface{})
				for i, h := range headers {
					if i < len(record) {
						row[h] = record[i]
					} else {
						row[h] = nil
					}
				}
				rows = append(rows, row)
			}
			os.Remove(tmpFile.Name())
		} else {
			results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrUnsupportedFileType})
			continue
		}
		if len(rows) == 0 {
			results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrNoDataToUpload})
			continue
		}
		invalidRows := []map[string]interface{}{}
		for i, row := range rows {
			entityLevel0, _ := row["entity_level_0"].(string)
			if !contains(buNames, entityLevel0) {
				invalidRows = append(invalidRows, map[string]interface{}{"row": i + 1, "entity": entityLevel0})
			}
			// Normalize bank_confirmation_date if provided
			if v, ok := row["bank_confirmation_date"]; ok {
				if v == nil || v == "" {
					row["bank_confirmation_date"] = nil
				} else {
					n := NormalizeDate(fmt.Sprint(v))
					if n == "" {
						row["bank_confirmation_date"] = nil
					} else {
						row["bank_confirmation_date"] = n
					}
				}
			}
		}
		if len(invalidRows) > 0 {
			results = append(results, map[string]interface{}{
				"filename":      fileHeader.Filename,
				"upload_s3_key": "",
				"updated":       0,
				"errors":        []map[string]interface{}{},
				"invalidRows":   invalidRows,
			})
			continue
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			results = append(results, map[string]interface{}{
				"filename":      fileHeader.Filename,
				"upload_s3_key": "",
				"updated":       0,
				"errors":        []map[string]interface{}{{constants.ValueError: constants.ErrFailedToStartDBTransaction, "details": err.Error()}},
				"invalidRows":   invalidRows,
			})
			continue
		}
		defer tx.Rollback(ctx)

		uploadS3Key := ""
		s3Uploaded := false
		if s3storage.IsS3UploadEnabled() {
			contentType := s3storage.DetectContentType(fileBytes)
			if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
				results = append(results, map[string]interface{}{
					"filename":      fileHeader.Filename,
					"upload_s3_key": "",
					"updated":       0,
					"errors":        []map[string]interface{}{{constants.ValueError: constants.ErrFailedToUploadFileToS3, "details": err.Error()}},
					"invalidRows":   invalidRows,
				})
				continue
			}
			uploadS3Key = s3Key
			s3Uploaded = true
		}

		successCount := 0
		errorRows := []map[string]interface{}{}
		for i, row := range rows {
			updateQuery := `UPDATE forward_bookings SET
				status = 'Confirmed',
				bank_transaction_id = $1,
				swift_unique_id = $2,
				bank_confirmation_date = $3,
				processing_status = 'pending',
				confirmation_upload_s3_key = $6
			WHERE internal_reference_id = $4 AND status = 'Pending Confirmation' AND entity_level_0 = $5`
			updateValues := []interface{}{
				row["bank_transaction_id"],
				row["swift_unique_id"],
				row["bank_confirmation_date"],
				row["internal_reference_id"],
				row["entity_level_0"],
				uploadS3Key,
			}
			res, err := tx.Exec(ctx, updateQuery, updateValues...)
			if err != nil {
				errorRows = append(errorRows, map[string]interface{}{"row": i + 1, constants.ValueError: "Failed to update record", "details": err.Error()})
				break
			}
			rowsAffected := res.RowsAffected()
			if rowsAffected == 0 {
				errorRows = append(errorRows, map[string]interface{}{"row": i + 1, constants.ValueError: "No matching record found or already confirmed"})
				break
			}
			successCount++
		}
		if len(errorRows) > 0 {
			if s3Uploaded {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					fmt.Printf("[forward-confirmation-upload] failed to cleanup S3 object for %s after update failure: %v\n", fileHeader.Filename, cleanupErr)
				}
			}
			results = append(results, map[string]interface{}{
				"filename":      fileHeader.Filename,
				"upload_s3_key": "",
				"updated":       0,
				"errors":        errorRows,
				"invalidRows":   invalidRows,
			})
			continue
		}
		if err := tx.Commit(ctx); err != nil {
			if s3Uploaded {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					fmt.Printf("[forward-confirmation-upload] failed to cleanup S3 object for %s after commit failure: %v\n", fileHeader.Filename, cleanupErr)
				}
			}
			results = append(results, map[string]interface{}{
				"filename":      fileHeader.Filename,
				"upload_s3_key": "",
				"updated":       0,
				"errors":        []map[string]interface{}{{constants.ValueError: constants.ErrFailedToCommitTransaction, "details": err.Error()}},
				"invalidRows":   invalidRows,
			})
			continue
		}
		results = append(results, map[string]interface{}{
			"filename":             fileHeader.Filename,
			"upload_s3_key":        uploadS3Key,
			"updated":              successCount,
			"errors":               errorRows,
			"invalidRows":          invalidRows,
			constants.ValueSuccess: successCount > 0,
		})
		recordForwardConfirmationUploadAudit(ctx, pool, uploadS3Key, uploadedBy)
	}
	return results, nil
}

func recordForwardConfirmationUploadAudit(ctx context.Context, pool *pgxpool.Pool, uploadS3Key, uploadedBy string) {
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	uploadedBy = strings.TrimSpace(uploadedBy)
	if uploadS3Key == "" || uploadedBy == "" {
		return
	}
	rows, err := pool.Query(ctx, `SELECT system_transaction_id FROM forward_bookings WHERE confirmation_upload_s3_key = $1`, uploadS3Key)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var systemTransactionID string
		if err := rows.Scan(&systemTransactionID); err == nil {
			auditutil.RecordActionPGX(ctx, pool, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: systemTransactionID, ActionType: constants.AuditActionCreate, Status: constants.StatusPendingEditApproval, Reason: "", RequestedBy: uploadedBy, OldValues: nil, NewValues: map[string]interface{}{"upload_s3_key": uploadS3Key}})
		}
	}
}

func UploadBankForwardBookingsMulti(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse multipart form
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrFailedToParseForm+err.Error())
			return
		}
		// Validate user_id
		if userID := r.FormValue(constants.KeyUserID); userID == "" {
			respondEnvelopeError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}
		// Delegate to service
		ctx := r.Context()
		results, batchID, err := processUploadBankForwardBookings(ctx, pool, r)
		if err != nil {
			respondEnvelopeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		respondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"batch_id": batchID,
			"results":  results,
		})
	}
}

func GetForwardDownloadURL(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RecordID            string `json:"record_id"`
			InternalReferenceID string `json:"internal_reference_id"`
			SystemTransactionID string `json:"system_transaction_id"`
			DownloadType        string `json:"download_type"`
			UploadBatchID       string `json:"upload_batch_id"`
			UploadS3Key         string `json:"upload_s3_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		recordID := decodeForwardDownloadID(req.InternalReferenceID)
		if recordID == "" {
			recordID = decodeForwardDownloadID(req.RecordID)
		}
		systemTransactionID := decodeForwardDownloadID(req.SystemTransactionID)
		downloadType := strings.ToUpper(strings.TrimSpace(req.DownloadType))
		uploadBatchID := decodeForwardDownloadID(req.UploadBatchID)

		if recordID == "" && systemTransactionID == "" && uploadBatchID == "" {
			respondWithError(w, http.StatusBadRequest, "system_transaction_id, internal_reference_id, or upload_batch_id is required")
			return
		}

		var uploadS3Key sql.NullString
		if directS3Key := strings.TrimSpace(req.UploadS3Key); directS3Key != "" {
			uploadS3Key = sql.NullString{String: directS3Key, Valid: true}
		}

		if systemTransactionID != "" {
			selectExpr := forwardDownloadKeyExpr(downloadType)
			err := pool.QueryRow(r.Context(), fmt.Sprintf(`
				SELECT %s
				FROM forward_bookings
				WHERE system_transaction_id = $1
				LIMIT 1
			`, selectExpr), systemTransactionID).Scan(&uploadS3Key)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				respondWithError(w, http.StatusInternalServerError, "failed to fetch forward booking")
				return
			}
		}

		if strings.TrimSpace(uploadS3Key.String) == "" && recordID != "" {
			selectExpr := forwardDownloadKeyExpr(downloadType)
			err := pool.QueryRow(r.Context(), fmt.Sprintf(`
				SELECT %s
				FROM forward_bookings
				WHERE internal_reference_id = $1
				ORDER BY add_date DESC
				LIMIT 1
			`, selectExpr), recordID).Scan(&uploadS3Key)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				respondWithError(w, http.StatusInternalServerError, "failed to fetch forward booking")
				return
			}
		}

		if strings.TrimSpace(uploadS3Key.String) == "" && uploadBatchID != "" {
			err := pool.QueryRow(r.Context(), `
				SELECT COALESCE(NULLIF(upload_s3_key, ''), '')
				FROM staging_bank_forward
				WHERE upload_batch_id = $1
				ORDER BY add_date DESC
				LIMIT 1
			`, uploadBatchID).Scan(&uploadS3Key)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					respondWithError(w, http.StatusNotFound, "record not found")
					return
				}
				respondWithError(w, http.StatusInternalServerError, "failed to fetch upload batch")
				return
			}
		}

		s3Key := strings.TrimSpace(uploadS3Key.String)
		if s3Key == "" {
			respondWithError(w, http.StatusNotFound, "no file available for download")
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), s3Key, 15*time.Minute)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to generate download url")
			return
		}
		if systemTransactionID == "" && recordID != "" {
			_ = pool.QueryRow(r.Context(), `SELECT system_transaction_id FROM forward_bookings WHERE internal_reference_id = $1 LIMIT 1`, recordID).Scan(&systemTransactionID)
		}
		if systemTransactionID != "" {
			auditutil.RecordDownloadPGX(r.Context(), pool, auditutil.DownloadParams{TableName: auditutil.TableForwardDownloads, ParentColumn: "system_transaction_id", ParentID: systemTransactionID, RequestedBy: auditutil.ActorFromContext(r.Context()), UploadS3Key: s3Key, ExtraColumns: map[string]string{"download_type": forwardDownloadAuditType(downloadType, s3Key)}})
		}

		respondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"download_url": downloadURL,
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
		id = decodeForwardDownloadID(id)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func writeBulkDownloadResponse(w http.ResponseWriter, files []map[string]string, failedIDs []string) {
	if len(files) == 0 {
		respondEnvelopeFailureWithData(w, http.StatusOK, "no downloadable files found", map[string]interface{}{
			"files":      []map[string]string{},
			"failed_ids": failedIDs,
		})
		return
	}

	respondEnvelopeSuccess(w, "Success", map[string]interface{}{
		"files":      files,
		"failed_ids": failedIDs,
	})
}

func GetForwardBulkDownloadURL(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			InternalReferenceIDs []string `json:"internal_reference_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		ids := normalizeBulkIDs(req.InternalReferenceIDs)
		if len(ids) == 0 {
			respondWithError(w, http.StatusBadRequest, "internal_reference_ids is required")
			return
		}

		ctx := r.Context()
		files := make([]map[string]string, 0, len(ids))
		failedIDs := make([]string, 0)

		for _, referenceID := range ids {
			var uploadS3Key sql.NullString
			err := pool.QueryRow(ctx, `
				SELECT COALESCE(NULLIF(upload_s3_key, ''), '')
				FROM forward_bookings
				WHERE internal_reference_id = $1
				ORDER BY add_date DESC
				LIMIT 1
			`, referenceID).Scan(&uploadS3Key)
			if err != nil {
				failedIDs = append(failedIDs, referenceID)
				continue
			}

			s3Key := strings.TrimSpace(uploadS3Key.String)
			if s3Key == "" {
				failedIDs = append(failedIDs, referenceID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, s3Key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, referenceID)
				continue
			}

			files = append(files, map[string]string{
				"internal_reference_id": referenceID,
				"download_url":          downloadURL,
			})
			var systemTransactionID string
			_ = pool.QueryRow(ctx, `SELECT system_transaction_id FROM forward_bookings WHERE internal_reference_id = $1 LIMIT 1`, referenceID).Scan(&systemTransactionID)
			auditutil.RecordDownloadPGX(ctx, pool, auditutil.DownloadParams{TableName: auditutil.TableForwardDownloads, ParentColumn: "system_transaction_id", ParentID: systemTransactionID, RequestedBy: auditutil.ActorFromContext(ctx), UploadS3Key: s3Key, ExtraColumns: map[string]string{"download_type": "BOOKING"}})
		}

		writeBulkDownloadResponse(w, files, failedIDs)
	}
}

func GetForwardConfirmationBulkDownloadURL(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			InternalReferenceIDs []string `json:"internal_reference_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		ids := normalizeBulkIDs(req.InternalReferenceIDs)
		if len(ids) == 0 {
			respondWithError(w, http.StatusBadRequest, "internal_reference_ids is required")
			return
		}

		ctx := r.Context()
		files := make([]map[string]string, 0, len(ids))
		failedIDs := make([]string, 0)

		for _, referenceID := range ids {
			var uploadS3Key sql.NullString
			err := pool.QueryRow(ctx, `
				SELECT COALESCE(NULLIF(confirmation_upload_s3_key, ''), '')
				FROM forward_bookings
				WHERE internal_reference_id = $1
				ORDER BY add_date DESC
				LIMIT 1
			`, referenceID).Scan(&uploadS3Key)
			if err != nil {
				failedIDs = append(failedIDs, referenceID)
				continue
			}

			s3Key := strings.TrimSpace(uploadS3Key.String)
			if s3Key == "" {
				failedIDs = append(failedIDs, referenceID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, s3Key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, referenceID)
				continue
			}

			files = append(files, map[string]string{
				"internal_reference_id": referenceID,
				"download_url":          downloadURL,
			})
			var systemTransactionID string
			_ = pool.QueryRow(ctx, `SELECT system_transaction_id FROM forward_bookings WHERE internal_reference_id = $1 LIMIT 1`, referenceID).Scan(&systemTransactionID)
			auditutil.RecordDownloadPGX(ctx, pool, auditutil.DownloadParams{TableName: auditutil.TableForwardDownloads, ParentColumn: "system_transaction_id", ParentID: systemTransactionID, RequestedBy: auditutil.ActorFromContext(ctx), UploadS3Key: s3Key, ExtraColumns: map[string]string{"download_type": "CONFIRMATION"}})
		}

		writeBulkDownloadResponse(w, files, failedIDs)
	}
}

func GetForwardBankBulkDownloadURL(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UploadBatchIDs []string `json:"upload_batch_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		ids := normalizeBulkIDs(req.UploadBatchIDs)
		if len(ids) == 0 {
			respondWithError(w, http.StatusBadRequest, "upload_batch_ids is required")
			return
		}

		ctx := r.Context()
		files := make([]map[string]string, 0, len(ids))
		failedIDs := make([]string, 0)

		for _, uploadBatchID := range ids {
			var uploadS3Key sql.NullString
			err := pool.QueryRow(ctx, `
				SELECT COALESCE(NULLIF(upload_s3_key, ''), '')
				FROM staging_bank_forward
				WHERE upload_batch_id = $1
				ORDER BY add_date DESC
				LIMIT 1
			`, uploadBatchID).Scan(&uploadS3Key)
			if err != nil {
				failedIDs = append(failedIDs, uploadBatchID)
				continue
			}

			s3Key := strings.TrimSpace(uploadS3Key.String)
			if s3Key == "" {
				failedIDs = append(failedIDs, uploadBatchID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, s3Key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, uploadBatchID)
				continue
			}

			files = append(files, map[string]string{
				"upload_batch_id": uploadBatchID,
				"download_url":    downloadURL,
			})
		}

		writeBulkDownloadResponse(w, files, failedIDs)
	}
}

// processUploadBankForwardBookings contains all business logic for the bank forward upload.
// It is called by the thin UploadBankForwardBookingsMulti controller.
func processUploadBankForwardBookings(ctx context.Context, pool *pgxpool.Pool, r *http.Request) ([]map[string]interface{}, string, error) {
	uploadBatchID := uuid.New().String()
	// Get staging table columns
	stagingCols := []string{}
	colRows, err := pool.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_name = 'staging_bank_forward'`)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch staging table columns: %w", err)
	}
	defer colRows.Close()
	for colRows.Next() {
		var col string
		if err := colRows.Scan(&col); err == nil {
			stagingCols = append(stagingCols, col)
		}
	}
	results := []map[string]interface{}{}
	uploadedBy := forwardUploadUserName(r.FormValue(constants.KeyUserID))
	// Loop over file arrays (e.g., forward_axis, forward_hdfc)
	for arrayName, files := range r.MultipartForm.File {
		bankIdentifier := strings.ToUpper(strings.Replace(arrayName, "forward_", "", 1))
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToOpenFile, "details": err.Error()})
				continue
			}
			// Read all file bytes for parsing, hashing, and deferred S3 upload.
			fileBytes, _ := io.ReadAll(file)
			file.Close()
			if s3storage.IsS3UploadEnabled() {
				if err := ensureUniqueForwardBookingUpload(ctx, pool, fileBytes); err != nil {
					message := err.Error()
					if errors.Is(err, ErrForwardBookingFileAlreadyUploaded) {
						message = duplicateForwardBookingUploadMessage
					}
					results = append(results, map[string]interface{}{
						"filename":             fileHeader.Filename,
						"upload_s3_key":        "",
						"staging_inserted":     0,
						"staging_errors":       []map[string]interface{}{},
						"bookings_inserted":    0,
						"booking_errors":       []map[string]interface{}{},
						"message":              message,
						constants.ValueSuccess: false,
					})
					continue
				}
			}
			fileExt := strings.ToLower(filepath.Ext(fileHeader.Filename))
			s3Key := s3storage.BuildUploadedS3Key("fx/forwards/bank", "", fileHeader.Filename, uploadedBy, time.Now().UTC())
			ext := fileExt
			var rows []map[string]interface{}
			if ext == ".csv" {
				reader := csv.NewReader(bytes.NewReader(fileBytes))
				headers, err := reader.Read()
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToReadCSVHeaders, "details": err.Error()})
					continue
				}
				for i, h := range headers {
					headers[i] = strings.TrimSpace(strings.TrimPrefix(h, constants.ErrInvalidJSONBOM))
				}
				for {
					record, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						continue
					}
					row := make(map[string]interface{})
					for i, h := range headers {
						row[h] = record[i]
					}
					rows = append(rows, row)
				}
			} else if ext == ".xls" || ext == ".xlsx" {
				tmpFile, err := os.CreateTemp("", constants.ErrExposureUploadFilenamePattern)
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToCreateTempFile, "details": err.Error()})
					continue
				}
				_, err = tmpFile.Write(fileBytes)
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToCopyFile, "details": err.Error()})
					tmpFile.Close()
					continue
				}
				tmpFile.Close()
				f, err := excelize.OpenFile(tmpFile.Name())
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrFailedToParseExcelFile, "details": err.Error()})
					continue
				}
				sheetName := f.GetSheetName(0)
				rowsData, err := f.GetRows(sheetName)
				if err != nil || len(rowsData) < 2 {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrNoDataToUpload})
					continue
				}
				headers := rowsData[0]
				for _, record := range rowsData[1:] {
					row := make(map[string]interface{})
					for i, h := range headers {
						if i < len(record) {
							row[h] = record[i]
						} else {
							row[h] = nil
						}
					}
					rows = append(rows, row)
				}
				os.Remove(tmpFile.Name())
			} else {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrUnsupportedFileType})
				continue
			}
			if len(rows) == 0 {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, constants.ValueError: constants.ErrNoDataToUpload})
				continue
			}
			// Get all mappings for this bank before S3/upload persistence work.
			mappings := []map[string]interface{}{}
			mapRows, err := pool.Query(ctx, `SELECT * FROM fwd_mapping_bank_forward WHERE bank_identifier = $1 AND is_active = true`, bankIdentifier)
			if err != nil {
				results = append(results, map[string]interface{}{
					"filename":          fileHeader.Filename,
					"upload_s3_key":     "",
					"staging_inserted":  0,
					"staging_errors":    []map[string]interface{}{},
					"bookings_inserted": 0,
					"booking_errors":    []map[string]interface{}{{constants.ValueError: "Failed to fetch mapping: " + err.Error()}},
				})
				continue
			}
			mapFieldDescs := mapRows.FieldDescriptions()
			cols := make([]string, len(mapFieldDescs))
			for i, fd := range mapFieldDescs {
				cols[i] = fd.Name
			}
			for mapRows.Next() {
				vals := make([]interface{}, len(cols))
				valPtrs := make([]interface{}, len(cols))
				for i := range vals {
					valPtrs[i] = &vals[i]
				}
				if err := mapRows.Scan(valPtrs...); err == nil {
					m := map[string]interface{}{}
					for i, col := range cols {
						m[col] = vals[i]
					}
					mappings = append(mappings, m)
				}
			}
			mapRows.Close()

			tx, err := pool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					"filename":          fileHeader.Filename,
					"upload_s3_key":     "",
					"staging_inserted":  0,
					"staging_errors":    []map[string]interface{}{{constants.ValueError: constants.ErrFailedToStartDBTransaction, "details": err.Error()}},
					"bookings_inserted": 0,
					"booking_errors":    []map[string]interface{}{},
				})
				continue
			}

			uploadS3Key := ""
			s3Uploaded := false
			if s3storage.IsS3UploadEnabled() {
				contentType := s3storage.DetectContentType(fileBytes)
				if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
					_ = tx.Rollback(ctx)
					results = append(results, map[string]interface{}{
						"filename":          fileHeader.Filename,
						"upload_s3_key":     "",
						"staging_inserted":  0,
						"staging_errors":    []map[string]interface{}{{constants.ValueError: constants.ErrFailedToUploadFileToS3, "details": err.Error()}},
						"bookings_inserted": 0,
						"booking_errors":    []map[string]interface{}{},
					})
					continue
				}
				uploadS3Key = s3Key
				s3Uploaded = true
			}

			// Insert into staging_bank_forward
			stagingSuccess := 0
			stagingErrors := []map[string]interface{}{}
			for i, row := range rows {
				insertObj := map[string]interface{}{
					"upload_batch_id":     uploadBatchID,
					"bank_identifier":     bankIdentifier,
					"bank_reference_id":   getStringOrUUID(row["bank_reference_id"]),
					"row_number":          i + 1,
					"source_reference_id": row["source_reference_id"],
					"raw_data":            marshalJSON(row),
					"processing_status":   "Pending",
					"upload_s3_key":       uploadS3Key,
				}
				// Add other columns from stagingCols
				for _, col := range stagingCols {
					if _, ok := insertObj[col]; ok {
						continue
					}
					value := row[col]
					// For date columns, normalize and set NULL if empty/invalid
					if isDateColumn(col) {
						if value == nil || value == "" {
							insertObj[col] = nil
						} else {
							norm := NormalizeDate(fmt.Sprint(value))
							if norm == "" {
								insertObj[col] = nil
							} else {
								insertObj[col] = norm
							}
						}
					} else if value != nil {
						insertObj[col] = value
					}
				}
				// Build dynamic insert
				fields := []string{}
				placeholders := []string{}
				values := []interface{}{}
				idx := 1
				for k, v := range insertObj {
					fields = append(fields, fmt.Sprintf("\"%s\"", k))
					placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
					values = append(values, v)
					idx++
				}
				_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO staging_bank_forward (%s) VALUES (%s)", strings.Join(fields, ", "), strings.Join(placeholders, ", ")), values...)
				if err != nil {
					stagingErrors = append(stagingErrors, map[string]interface{}{"row": i + 1, constants.ValueError: err.Error()})
					break
				}
				stagingSuccess++
			}
			if len(stagingErrors) > 0 {
				_ = tx.Rollback(ctx)
				if s3Uploaded {
					if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
						fmt.Printf("[bank-forward-upload] failed to cleanup S3 object for %s after staging failure: %v\n", fileHeader.Filename, cleanupErr)
					}
				}
				results = append(results, map[string]interface{}{
					"filename":          fileHeader.Filename,
					"upload_s3_key":     "",
					"staging_inserted":  0,
					"staging_errors":    stagingErrors,
					"bookings_inserted": 0,
					"booking_errors":    []map[string]interface{}{},
				})
				continue
			}
			// Mapping and insert into forward_bookings
			bookingSuccess := 0
			bookingErrors := []map[string]interface{}{}
			for i, r := range rows {
				booking := map[string]interface{}{}
				unmapped := map[string]interface{}{}
				for key, val := range r {
					found := false
					for _, m := range mappings {
						if m["source_field"] == key && m["target_table"] == "forward_bookings" {
							// Transformation logic can be added here
							booking[m["target_field"].(string)] = val
							found = true
							break
						}
					}
					if !found {
						unmapped[key] = val
					}
				}
				booking["additional_bank_details"] = marshalJSON(unmapped)
				// Required fields fallback
				booking["entity_level_0"] = getStringOrDefault(booking["entity_level_0"], r["entity_level_0"])
				booking["maturity_date"] = getStringOrDefault(booking["maturity_date"], r["maturity_date"])
				booking["currency_pair"] = getStringOrDefault(booking["currency_pair"], r["currency_pair"])
				booking["order_type"] = getStringOrDefault(booking["order_type"], r["order_type"])
				booking["booking_amount"] = getStringOrDefault(booking["booking_amount"], r["booking_amount"])
				booking["total_rate"] = getStringOrDefault(booking["total_rate"], r["total_rate"])
				booking["processing_status"] = "pending"
				booking["upload_s3_key"] = uploadS3Key
				for bk := range booking {
					legacyCol := strings.ToLower(strings.TrimSpace(bk))
					if strings.Contains(legacyCol, "upload") && strings.Contains(legacyCol, "link") {
						delete(booking, bk)
					}
				}
				// Normalize date fields in booking before insert
				for k, v := range booking {
					if isDateColumn(k) {
						if v == nil || v == "" {
							booking[k] = nil
						} else {
							norm := NormalizeDate(fmt.Sprint(v))
							if norm == "" {
								booking[k] = nil
							} else {
								booking[k] = norm
							}
						}
					}
				}
				// Build dynamic insert
				fields := []string{}
				placeholders := []string{}
				values := []interface{}{}
				idx := 1
				for k, v := range booking {
					fields = append(fields, k)
					placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
					values = append(values, v)
					idx++
				}
				_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO forward_bookings (%s) VALUES (%s)", strings.Join(fields, ", "), strings.Join(placeholders, ", ")), values...)
				if err != nil {
					bookingErrors = append(bookingErrors, map[string]interface{}{"row": i + 1, constants.ValueError: err.Error()})
					break
				}
				bookingSuccess++
			}
			if len(bookingErrors) > 0 {
				_ = tx.Rollback(ctx)
				if s3Uploaded {
					if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
						fmt.Printf("[bank-forward-upload] failed to cleanup S3 object for %s after booking failure: %v\n", fileHeader.Filename, cleanupErr)
					}
				}
				results = append(results, map[string]interface{}{
					"filename":          fileHeader.Filename,
					"upload_s3_key":     "",
					"staging_inserted":  0,
					"staging_errors":    []map[string]interface{}{},
					"bookings_inserted": 0,
					"booking_errors":    bookingErrors,
				})
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				if s3Uploaded {
					if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
						fmt.Printf("[bank-forward-upload] failed to cleanup S3 object for %s after commit failure: %v\n", fileHeader.Filename, cleanupErr)
					}
				}
				results = append(results, map[string]interface{}{
					"filename":          fileHeader.Filename,
					"upload_s3_key":     "",
					"staging_inserted":  0,
					"staging_errors":    []map[string]interface{}{},
					"bookings_inserted": 0,
					"booking_errors":    []map[string]interface{}{{constants.ValueError: constants.ErrFailedToCommitTransaction, "details": err.Error()}},
				})
				continue
			}
			results = append(results, map[string]interface{}{
				"filename":          fileHeader.Filename,
				"upload_s3_key":     uploadS3Key,
				"staging_inserted":  stagingSuccess,
				"staging_errors":    stagingErrors,
				"bookings_inserted": bookingSuccess,
				"booking_errors":    bookingErrors,
			})
		}
	}
	return results, uploadBatchID, nil
}

// Helper: marshal map to JSON
func marshalJSON(m map[string]interface{}) []byte {
	b, _ := json.Marshal(m)
	return b
}

// Helper: get string or generate UUID
func getStringOrUUID(val interface{}) string {
	s, ok := val.(string)
	if ok && s != "" {
		return s
	}
	return uuid.New().String()
}

// Helper: get string or fallback
func getStringOrDefault(a interface{}, b interface{}) interface{} {
	if a != nil && a != "" {
		return a
	}
	return b
}

// Helper: check if column is a date column
func isDateColumn(col string) bool {
	dateCols := []string{"trade_date", "booking_date", "expiry_date", "delivery_from_date", "delivery_to_date", "option_start_date", "maturity_date", "processed_at", "loaded_at", "add_date", "settlement_date", "delivery_date", "bank_confirmation_date", "transaction_timestamp"}
	for _, d := range dateCols {
		if strings.EqualFold(col, d) {
			return true
		}
	}
	return false
}
