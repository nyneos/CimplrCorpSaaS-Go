package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/json"
	"fmt"
	"time"

	// "log"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
)

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
	CurrencyID string `json:"currency_id"`
	Status     string `json:"status"`
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		userID := req.UserID
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				createdBy = s.Name
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		var results []map[string]interface{}
		for _, cur := range req.Currency {
			if len(cur.CurrencyCode) != 3 || cur.CurrencyName == "" || cur.Country == "" || cur.DecimalPlaces < 0 || cur.DecimalPlaces > 4 || cur.Status == "" {
				results = append(results, map[string]interface{}{
					"success":       false,
					"error":         "Invalid currency details",
					"currency_code": cur.CurrencyCode,
				})
				continue
			}
			ctx := r.Context()
			tx, txErr := pgxPool.Begin(ctx)
			if txErr != nil {
				results = append(results, map[string]interface{}{
					"success":       false,
					"error":         "Failed to start transaction: " + txErr.Error(),
					"currency_code": cur.CurrencyCode,
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
				results = append(results, map[string]interface{}{
					"success":       false,
					"error":         err.Error(),
					"currency_code": cur.CurrencyCode,
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
					"success":       false,
					"error":         "Currency created but audit log failed: " + auditErr.Error(),
					"currency_id":   currencyID,
					"currency_code": cur.CurrencyCode,
				})
				continue
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				results = append(results, map[string]interface{}{
					"success":       false,
					"error":         "Transaction commit failed: " + commitErr.Error(),
					"currency_id":   currencyID,
					"currency_code": cur.CurrencyCode,
				})
				continue
			}
			results = append(results, map[string]interface{}{
				"success":       true,
				"currency_id":   currencyID,
				"currency_code": cur.CurrencyCode,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		finalSuccess := api.IsBulkSuccess(results)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": finalSuccess,
			"results": results,
		})
	}
}

// GET handler to fetch all currency records
func GetAllCurrencyMaster(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		query := `
			SELECT m.currency_id, m.currency_code, m.currency_name, m.country, m.symbol, m.decimal_places, m.status, m.old_decimal_places, m.old_status
			FROM mastercurrency m
		`
		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var currencies []map[string]interface{}
		var anyError error
		for rows.Next() {
			var (
				currencyID, currencyCode, currencyName, country, symbol, status, oldStatus string
				decimalPlaces, oldDecimalPlaces                                            int
			)
			err := rows.Scan(&currencyID, &currencyCode, &currencyName, &country, &symbol, &decimalPlaces, &status, &oldDecimalPlaces, &oldStatus)
			if err != nil {
				anyError = err
				break
			}
			// ...existing code...
			auditQuery := `SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason FROM auditactioncurrency WHERE currency_id = $1 ORDER BY requested_at DESC LIMIT 1`
			var processingStatusPtr, requestedByPtr, actionTypePtr, actionIDPtr, checkerByPtr, checkerCommentPtr, reasonPtr *string
			var requestedAtPtr, checkerAtPtr *time.Time
			_ = pgxPool.QueryRow(ctx, auditQuery, currencyID).Scan(&processingStatusPtr, &requestedByPtr, &requestedAtPtr, &actionTypePtr, &actionIDPtr, &checkerByPtr, &checkerAtPtr, &checkerCommentPtr, &reasonPtr)

			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM auditactioncurrency WHERE currency_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
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
					if actionType == "CREATE" && createdBy == "" {
						createdBy = auditInfo.CreatedBy
						createdAt = auditInfo.CreatedAt
					} else if actionType == "EDIT" && editedBy == "" {
						editedBy = auditInfo.EditedBy
						editedAt = auditInfo.EditedAt
					} else if actionType == "DELETE" && deletedBy == "" {
						deletedBy = auditInfo.DeletedBy
						deletedAt = auditInfo.DeletedAt
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
				"status":             status,
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
			})
		}
		w.Header().Set("Content-Type", "application/json")
		if anyError != nil {
			api.RespondWithError(w, http.StatusInternalServerError, anyError.Error())
			return
		}
		if currencies == nil {
			currencies = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    currencies,
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
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		userID := req.UserID
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				updatedBy = s.Name
				break
			}
		}
		if updatedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		var results []map[string]interface{}
		for _, cur := range req.Currency {
			ctx := r.Context()
			tx, txErr := pgxPool.Begin(ctx)
			if txErr != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Failed to start transaction: " + txErr.Error(), "currency_id": cur.CurrencyID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
					if p := recover(); p != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "panic: " + fmt.Sprint(p), "currency_id": cur.CurrencyID})
					}
				}()

				// fetch existing values (for old_* capture)
				var exDecimal *int64
				var exStatus *string
				sel := `SELECT decimal_places, status FROM mastercurrency WHERE currency_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, cur.CurrencyID).Scan(&exDecimal, &exStatus); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Failed to fetch existing currency: " + err.Error(), "currency_id": cur.CurrencyID})
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
					case "status":
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
						results = append(results, map[string]interface{}{"success": false, "error": err.Error(), "currency_id": cur.CurrencyID})
						return
					}
				} else {
					currencyID = cur.CurrencyID
				}

				auditQuery := `INSERT INTO auditactioncurrency (
					currency_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, $2, $3, $4, $5, now())`
				if _, err := tx.Exec(ctx, auditQuery, currencyID, "EDIT", "PENDING_EDIT_APPROVAL", cur.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Currency updated but audit log failed: " + err.Error(), "currency_id": currencyID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Transaction commit failed: " + err.Error(), "currency_id": currencyID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "currency_id": currencyID, "currency_code": currencyCode})
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
func BulkRejectAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CurrencyIDs []string `json:"currency_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.CurrencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields: provide currency_ids")
			return
		}
		sessions := auth.GetActiveSessions()
		checkerBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
	query := `UPDATE auditactioncurrency SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE currency_id = ANY($3) RETURNING action_id,currency_id`
	rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, pq.Array(req.CurrencyIDs))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, currencyID string
			rows.Scan(&id, &currencyID)
			updated = append(updated, id, currencyID)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"updated": updated,
		})
	}
}

// BulkApproveAuditActions sets processing_status to APPROVED for multiple actions
func BulkApproveAuditActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CurrencyIDs []string `json:"currency_ids"`
			Comment     string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.CurrencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields: provide currency_ids")
			return
		}
		sessions := auth.GetActiveSessions()
		checkerBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		// First, delete records with processing_status = 'PENDING_DELETE_APPROVAL' for the given action_ids
	delQuery := `DELETE FROM auditactioncurrency WHERE currency_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, currency_id`
	delRows, delErr := pgxPool.Query(r.Context(), delQuery, pq.Array(req.CurrencyIDs))
		var deleted []string
		var currencyIDsToDelete []string
		if delErr == nil {
			defer delRows.Close()
			for delRows.Next() {
				var id, currencyID string
				delRows.Scan(&id, &currencyID)
				deleted = append(deleted, id, currencyID)
				currencyIDsToDelete = append(currencyIDsToDelete, currencyID)
			}
		}
		// Delete corresponding currencies from mastercurrency
		if len(currencyIDsToDelete) > 0 {
			// soft-delete: mark rows as deleted
			_, _ = pgxPool.Exec(r.Context(), `UPDATE mastercurrency SET is_deleted = true WHERE currency_id = ANY($1)`, pq.Array(currencyIDsToDelete))
		}

		// Then, approve the rest
	query := `UPDATE auditactioncurrency SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE currency_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id,currency_id`
	rows, err := pgxPool.Query(r.Context(), query, checkerBy, req.Comment, pq.Array(req.CurrencyIDs))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, currencyID string
			rows.Scan(&id, &currencyID)
			updated = append(updated, id, currencyID)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"updated": updated,
			"deleted": deleted,
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
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.CurrencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		sessions := auth.GetActiveSessions()
		requestedBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"created": results,
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
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
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
