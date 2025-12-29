package allMaster

import (
	"CimplrCorpSaas/api"
	middlewares "CimplrCorpSaas/api/middlewares"
	"encoding/json"
	"fmt"
	"time"

	// "log"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
)

// getUserFriendlyCurrencyError converts database errors into user-friendly messages
func getUserFriendlyCurrencyError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errMsg := strings.ToLower(err.Error())

	// Unique constraint violations
	if strings.Contains(errMsg, "unique") || strings.Contains(errMsg, "duplicate") {
		if strings.Contains(errMsg, "currency_code") || strings.Contains(errMsg, "mastercurrency_currency_code_key") {
			return "Currency code already exists. Please use a different code.", http.StatusOK
		}
		return "This currency already exists in the system.", http.StatusOK
	}

	// Check constraint violations
	if strings.Contains(errMsg, "check constraint") || strings.Contains(errMsg, "violates check") {
		if strings.Contains(errMsg, "decimal_places") {
			return "Decimal places must be between 0 and 4.", http.StatusOK
		}
		if strings.Contains(errMsg, "old_decimal_places") {
			return "Old decimal places value is invalid (must be 0-4).", http.StatusOK
		}
		if strings.Contains(errMsg, "actiontype") {
			return "Invalid action type. Must be CREATE, EDIT, or DELETE.", http.StatusOK
		}
		if strings.Contains(errMsg, "processing_status") {
			return "Invalid processing status. Must be PENDING_APPROVAL, PENDING_EDIT_APPROVAL, PENDING_DELETE_APPROVAL, APPROVED, REJECTED, or CANCELLED.", http.StatusOK
		}
		return "Data validation failed. Please check your input values.", http.StatusOK
	}

	// Foreign key violations
	if strings.Contains(errMsg, "foreign key") || strings.Contains(errMsg, "violates foreign key constraint") {
		if strings.Contains(errMsg, "currency_id") {
			return "Currency does not exist or has been deleted.", http.StatusOK
		}
		return "Referenced record does not exist.", http.StatusOK
	}

	// NOT NULL violations
	if strings.Contains(errMsg, "null value") || strings.Contains(errMsg, "violates not-null constraint") {
		if strings.Contains(errMsg, "currency_code") {
			return "Currency code is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "currency_name") {
			return "Currency name is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "country") {
			return "Country is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "decimal_places") {
			return "Decimal places is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "status") {
			return "Status is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "actiontype") {
			return "Action type is required.", http.StatusOK
		}
		if strings.Contains(errMsg, "processing_status") {
			return "Processing status is required.", http.StatusOK
		}
		return "Required field is missing.", http.StatusOK
	}

	// Connection/timeout errors
	if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "network") {
		return "Database connection issue. Please try again.", http.StatusServiceUnavailable
	}

	// Default with context
	if context != "" {
		return context + ": " + err.Error(), http.StatusInternalServerError
	}
	return err.Error(), http.StatusInternalServerError
}

type CurrencyMasterRequest struct {
	Status        string `json:"status"`
	CurrencyCode  string `json:"currency_code"`
	CurrencyName  string `json:"currency_name"`
	Country       string `json:"country"`
	Symbol        string `json:"symbol"`
	DecimalPlaces int    `json:"decimal_places"`
	// UserID        string `json:"user_id"`
}
type CurrencyMasterUpdateRequest struct {
	CurrencyID       string `json:"currency_id"`
	Status           string `json:"status"`
	DecimalPlaces    int    `json:"decimal_places"`
	UserID           string `json:"user_id"`
	OLDStatus        string `json:"old_status"`
	OldDecimalPlaces int    `json:"old_decimal_places"`
	Reason           string `json:"reason"`
}

// helper usage in this file expects pointer types from query results when possible
func CreateCurrencyMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                  `json:"user_id"`
			Currency []CurrencyMasterRequest `json:"currency"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		// Get pre-validated context values
		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		createdBy := session.Name
		var results []map[string]interface{}
		for _, cur := range req.Currency {
			if len(cur.CurrencyCode) != 3 {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.FormatFieldError("currency_code", "must be exactly 3 characters"),
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			if cur.CurrencyName == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.FormatMissingFieldError("currency_name"),
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			if cur.Country == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.FormatMissingFieldError("country"),
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			if cur.DecimalPlaces < 0 || cur.DecimalPlaces > 4 {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.FormatFieldError("decimal_places", "must be between 0 and 4"),
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			if cur.Status == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.FormatMissingFieldError("status"),
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			ctx := r.Context()
			tx, txErr := pgxPool.Begin(ctx)
			if txErr != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrTransactionFailed,
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			var currencyID string
			query := `INSERT INTO mastercurrency (
				currency_code, currency_name, country, symbol, decimal_places, status
			) VALUES ($1, $2, $3, $4, $5, $6) RETURNING currency_id`
			err := tx.QueryRow(ctx, query,
				strings.ToUpper(cur.CurrencyCode),
				cur.CurrencyName,
				cur.Country,
				cur.Symbol,
				cur.DecimalPlaces,
				cur.Status,
			).Scan(&currencyID)
			if err != nil {
				tx.Rollback(ctx)
				errMsg := constants.ErrCurrencyCreateFailed
				if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
					errMsg = constants.FormatError(constants.ErrCurrencyAlreadyExists, cur.CurrencyCode)
				}
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   errMsg,
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			auditQuery := `INSERT INTO auditactioncurrency (
				currency_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, $2, $3, $4, $5, now())`
			_, auditErr := tx.Exec(ctx, auditQuery,
				currencyID,
				"CREATE",
				"PENDING_APPROVAL",
				nil,
				createdBy,
			)
			if auditErr != nil {
				tx.Rollback(ctx)
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrAuditLogFailed,
					"currency_id":          currencyID,
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   constants.ErrTransactionCommitFailed,
					"currency_id":          currencyID,
					"currency_code":        cur.CurrencyCode,
				})
				continue
			}
			results = append(results, map[string]interface{}{
				constants.ValueSuccess: true,
				"currency_id":          currencyID,
				"currency_code":        cur.CurrencyCode,
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		finalSuccess := api.IsBulkSuccess(results)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: finalSuccess,
			"results":              results,
		})
	}
}

// GET handler to fetch all currency records
func GetAllCurrencyMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// --- Query: mastercurrency + latest audit info per currency (for ordering) ---
		query := `
			SELECT 
				m.currency_id,
				m.currency_code,
				m.currency_name,
				m.country,
				m.symbol,
				m.decimal_places,
				m.status,
				m.old_decimal_places,
				m.old_status,
				a.requested_at AS laterequested_at
			FROM mastercurrency m
			LEFT JOIN LATERAL (
				SELECT requested_at , checker_at
				FROM auditactioncurrency
				WHERE currency_id = m.currency_id
				ORDER BY requested_at DESC
				LIMIT 1
		) a ON TRUE
		ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC;
	`

	rows, err := pgxPool.Query(ctx, query)
	if err != nil {
		errMsg, statusCode := getUserFriendlyCurrencyError(err, "Failed to fetch currency data")
		if statusCode == http.StatusOK {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
		} else {
			api.RespondWithError(w, statusCode, errMsg)
		}
		return
	}
	defer rows.Close()

	var currencies []map[string]interface{}
	var anyError error

	for rows.Next() {
		var (
			currencyID, currencyCode, currencyName, country, symbol, status, oldStatus string
			decimalPlaces, oldDecimalPlaces                                            int
			latestRequestedAt                                                          *time.Time
		)
			err := rows.Scan(
				&currencyID, &currencyCode, &currencyName, &country, &symbol,
				&decimalPlaces, &status, &oldDecimalPlaces, &oldStatus, &latestRequestedAt,
			)
			if err != nil {
				anyError = fmt.Errorf("%s: %v", constants.ErrDatabaseScanFailed, err)
				break
			}

			// --- Get audit info for this currency ---
			auditQuery := `
				SELECT processing_status, requested_by, requested_at, actiontype, action_id, 
				       checker_by, checker_at, checker_comment, reason
				FROM auditactioncurrency
				WHERE currency_id = $1
				ORDER BY requested_at DESC
				LIMIT 1
			`
			var processingStatusPtr, requestedByPtr, actionTypePtr, actionIDPtr, checkerByPtr, checkerCommentPtr, reasonPtr *string
			var requestedAtPtr, checkerAtPtr *time.Time
			_ = pgxPool.QueryRow(ctx, auditQuery, currencyID).Scan(
				&processingStatusPtr, &requestedByPtr, &requestedAtPtr, &actionTypePtr, &actionIDPtr,
				&checkerByPtr, &checkerAtPtr, &checkerCommentPtr, &reasonPtr,
			)

			// --- Historical audit details (CREATE / EDIT / DELETE) ---
			auditDetailsQuery := `
				SELECT actiontype, requested_by, requested_at
				FROM auditactioncurrency
				WHERE currency_id = $1
				  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
				ORDER BY requested_at DESC
			`
			auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, currencyID)
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
			if auditErr == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var actionType string
					var requestedByPtr *string
					var requestedAtPtr *time.Time
					auditRows.Scan(&actionType, &requestedByPtr, &requestedAtPtr)
					auditInfo := api.GetAuditInfo(actionType, requestedByPtr, requestedAtPtr)
					switch actionType {
					case "CREATE":
						if createdBy == "" {
							createdBy = auditInfo.CreatedBy
							createdAt = auditInfo.CreatedAt
						}
					case "EDIT":
						if editedBy == "" {
							editedBy = auditInfo.EditedBy
							editedAt = auditInfo.EditedAt
						}
					case "DELETE":
						if deletedBy == "" {
							deletedBy = auditInfo.DeletedBy
							deletedAt = auditInfo.DeletedAt
						}
					}
				}
			}

			currencies = append(currencies, map[string]interface{}{
				"currency_id":        currencyID,
				"currency_code":      currencyCode,
				"currency_name":      currencyName,
				"country":            country,
				"symbol":             symbol,
				"decimal_places":     decimalPlaces,
				"old_decimal_places": oldDecimalPlaces,
				constants.KeyStatus:  status,
				"old_status":         oldStatus,
				"processing_status":  ifaceToString(processingStatusPtr),
				"action_type":        ifaceToString(actionTypePtr),
				"action_id":          ifaceToString(actionIDPtr),
				"checker_at":         ifaceToTimeString(checkerAtPtr),
				"checker_by":         ifaceToString(checkerByPtr),
				"checker_comment":    ifaceToString(checkerCommentPtr),
				"reason":             ifaceToString(reasonPtr),
				"created_by":         createdBy,
				"created_at":         createdAt,
				"edited_by":          editedBy,
				"edited_at":          editedAt,
				"deleted_by":         deletedBy,
				"deleted_at":         deletedAt,
				// "latest_requested_at": ifaceToTimeString(latestRequestedAt),
			})
		}

		if anyError != nil {
			api.RespondWithError(w, http.StatusInternalServerError, anyError.Error())
			return
		}
		if currencies == nil {
			currencies = make([]map[string]interface{}, 0)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 currencies,
		})
	}
}

func UpdateCurrencyMasterBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			Currency []struct {
				CurrencyID string                 `json:"currency_id"`
				Fields     map[string]interface{} `json:"fields"`
				Reason     string                 `json:"reason"`
			} `json:"currency"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSON)
			return
		}

		// Get pre-validated context values
		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		updatedBy := session.Name
		var results []map[string]interface{}
		for _, cur := range req.Currency {
			ctx := r.Context()
			tx, txErr := pgxPool.Begin(ctx)
			if txErr != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTransactionFailed, "currency_id": cur.CurrencyID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
					if p := recover(); p != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "panic: " + fmt.Sprint(p), "currency_id": cur.CurrencyID})
					}
				}()

				// fetch existing values (for old_* capture)
				var exDecimal *int64
				var exStatus *string
				sel := `SELECT decimal_places, status FROM mastercurrency WHERE currency_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, cur.CurrencyID).Scan(&exDecimal, &exStatus); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCurrencyNotFound, "currency_id": cur.CurrencyID})
					return
				}

				// build dynamic update
				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range cur.Fields {
					switch k {
					case "decimal_places":
						// JSON numbers decode as float64
						var newDec int64
						switch t := v.(type) {
						case float64:
							newDec = int64(t)
						case int:
							newDec = int64(t)
						case int64:
							newDec = t
						case string:
							// attempt parse
							// ignore parse errors and treat as 0
							var tmp int64
							fmt.Sscan(t, &tmp)
							newDec = tmp
						default:
							newDec = 0
						}
						oldVal := int64(0)
						if exDecimal != nil {
							oldVal = *exDecimal
						}
						sets = append(sets, fmt.Sprintf("decimal_places=$%d, old_decimal_places=$%d", pos, pos+1))
						args = append(args, newDec, oldVal)
						pos += 2
					case constants.KeyStatus:
						newStatus := fmt.Sprint(v)
						oldVal := ""
						if exStatus != nil {
							oldVal = *exStatus
						}
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, newStatus, oldVal)
						pos += 2
					case "currency_code":
						sets = append(sets, fmt.Sprintf("currency_code=$%d", pos))
						args = append(args, strings.ToUpper(fmt.Sprint(v)))
						pos++
					case "currency_name":
						sets = append(sets, fmt.Sprintf("currency_name=$%d", pos))
						args = append(args, fmt.Sprint(v))
						pos++
					case "country":
						sets = append(sets, fmt.Sprintf("country=$%d", pos))
						args = append(args, fmt.Sprint(v))
						pos++
					case "symbol":
						sets = append(sets, fmt.Sprintf("symbol=$%d", pos))
						args = append(args, fmt.Sprint(v))
						pos++
					default:
						// ignore unknown
					}
				}

				var currencyID, currencyCode string
				if len(sets) > 0 {
					q := "UPDATE mastercurrency SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE currency_id=$%d RETURNING currency_id, currency_code", pos)
					args = append(args, cur.CurrencyID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&currencyID, &currencyCode); err != nil {
						results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCurrencyUpdateFailed, "currency_id": cur.CurrencyID})
						return
					}
				} else {
					currencyID = cur.CurrencyID
				}

				auditQuery := `INSERT INTO auditactioncurrency (
					currency_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, $2, $3, $4, $5, now())`
				if _, err := tx.Exec(ctx, auditQuery, currencyID, "EDIT", "PENDING_EDIT_APPROVAL", cur.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditLogFailed, "currency_id": currencyID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTransactionCommitFailed, "currency_id": currencyID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "currency_id": currencyID, "currency_code": currencyCode})
			}()
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		finalSuccess := api.IsBulkSuccess(results)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: finalSuccess,
			"results":              results,
		})
	}
}
func BulkRejectAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CurrencyIDs []string `json:"currency_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.CurrencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMissingRequiredField)
			return
		}

		// Get pre-validated context values
		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		checkerBy := session.Name
		query := `UPDATE auditactioncurrency SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE currency_id = ANY($3) RETURNING action_id,currency_id`
		rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, pq.Array(req.CurrencyIDs))
		if err != nil {
			errMsg, statusCode := getUserFriendlyCurrencyError(err, "Failed to reject currency actions")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, currencyID string
			rows.Scan(&id, &currencyID)
			updated = append(updated, id, currencyID)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"updated":              updated,
		})
	}
}

func BulkApproveAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CurrencyIDs []string `json:"currency_ids"`
			Comment     string   `json:"comment"`
		}

		// --- Step 1: Validate input ---
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.CurrencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMissingRequiredField)
			return
		}

		// --- Step 2: Get pre-validated context values ---
		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		checkerBy := session.Name

		ctx := r.Context()

		// --- Step 3: Delete records pending approval ---
		delQuery := `
			DELETE FROM auditactioncurrency
			WHERE currency_id = ANY($1)
			  AND processing_status = 'PENDING_DELETE_APPROVAL'
			RETURNING action_id, currency_id
		`
		delRows, delErr := pgxPool.Query(ctx, delQuery, pq.Array(req.CurrencyIDs))
		var deleted []string
		var currencyIDsToDelete []string
		if delErr == nil {
			defer delRows.Close()
			for delRows.Next() {
				var actionID, currencyID string
				if err := delRows.Scan(&actionID, &currencyID); err == nil {
					deleted = append(deleted, actionID, currencyID)
					currencyIDsToDelete = append(currencyIDsToDelete, currencyID)
				}
			}
		}

		// --- Step 4: Delete corresponding currencies from mastercurrency (HARD DELETE) ---
		if len(currencyIDsToDelete) > 0 {
			delCurQuery := `DELETE FROM mastercurrency WHERE currency_id = ANY($1)`
			if _, err := pgxPool.Exec(ctx, delCurQuery, pq.Array(currencyIDsToDelete)); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrDatabaseDeleteFailed)
				return
			}
		}

		// --- Step 5: Approve the remaining audit actions ---
		approveQuery := `
			UPDATE auditactioncurrency
			SET processing_status = 'APPROVED',
			    checker_by = $1,
			    checker_at = now(),
			    checker_comment = $2
			WHERE currency_id = ANY($3)
			  AND processing_status != 'PENDING_DELETE_APPROVAL'
			RETURNING action_id, currency_id
		`
		rows, err := pgxPool.Query(ctx, approveQuery, checkerBy, req.Comment, pq.Array(req.CurrencyIDs))
		if err != nil {
			errMsg, statusCode := getUserFriendlyCurrencyError(err, "Failed to approve currency actions")
			if statusCode == http.StatusOK {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "error": errMsg})
			} else {
				api.RespondWithError(w, statusCode, errMsg)
			}
			return
		}
		defer rows.Close()

		var updated []string
		for rows.Next() {
			var actionID, currencyID string
			if scanErr := rows.Scan(&actionID, &currencyID); scanErr == nil {
				updated = append(updated, actionID, currencyID)
			}
		}

		// --- Step 6: Send response ---
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"updated":              updated,
			"deleted":              deleted,
		})
	}
}

func BulkDeleteCurrencyAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CurrencyIDs []string `json:"currency_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.CurrencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrMissingRequiredField)
			return
		}

		// Get pre-validated context values
		session := middlewares.GetSessionFromContext(r.Context())
		if session == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}
		requestedBy := session.Name
		ctx := r.Context()
		var results []string
		for _, currencyID := range req.CurrencyIDs {
			query := `INSERT INTO auditactioncurrency (
				currency_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now()) RETURNING action_id`
			var actionID string
			err := pgxPool.QueryRow(ctx, query, currencyID, req.Reason, requestedBy).Scan(&actionID)
			if err == nil {
				results = append(results, actionID)
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"created":              results,
		})
	}
}

// GET handler to fetch currency_code where status is 'Active' and processing_status is 'APPROVED'
func GetActiveApprovedCurrencyCodes(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		query := `
			SELECT m.currency_code ,m.decimal_places
			FROM mastercurrency m
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactioncurrency a
				WHERE a.currency_id = m.currency_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			WHERE m.status = 'Active' AND a.processing_status = 'APPROVED'
		`
		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			errMsg, statusCode := getUserFriendlyCurrencyError(err, "Failed to fetch active currency codes")
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
			var code string
			var decimalPlace int
			if err := rows.Scan(&code, &decimalPlace); err != nil {
				anyError = err
				break
			}
			results = append(results, map[string]interface{}{
				"currency_code": code,
				"decimal_place": decimalPlace,
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

// Helper functions for type conversion
