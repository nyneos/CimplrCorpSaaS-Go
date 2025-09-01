package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/lib/pq"
)

// "database/sql"

// Error response helper
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

type CurrencyMasterRequest struct {
	Status        string `json:"status"`
	CurrencyCode  string `json:"currency_code"`
	CurrencyName  string `json:"currency_name"`
	Country       string `json:"country"`
	Symbol        string `json:"symbol"`
	DecimalPlaces int    `json:"decimal_places"`
	UserID        string `json:"user_id"`
}
type CurrencyMasterUpdateRequest struct {
	CurrencyID string `json:"currency_id"`
	Status     string `json:"status"`
	// CurrencyCode  string `json:"currency_code"`
	// CurrencyName  string `json:"currency_name"`
	// Country       string `json:"country"`
	// Symbol        string `json:"symbol"`
	DecimalPlaces    int    `json:"decimal_places"`
	UserID           string `json:"user_id"`
	OLDStatus        string `json:"old_status"`
	OldDecimalPlaces int    `json:"old_decimal_places"`
	Reason           string `json:"reason"`
}

// Helper functions for handling sql.NullString and sql.NullTime

func getNullString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func getNullTime(nt sql.NullTime) string {
	if nt.Valid {
		return nt.Time.Format("2006-01-02 15:04:05")
	}
	return ""
}
func CreateCurrencyMaster(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                  `json:"user_id"`
			Currency []CurrencyMasterRequest `json:"currency"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		userID := req.UserID
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
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
			tx, txErr := db.Begin()
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
			err := tx.QueryRow(query,
				strings.ToUpper(cur.CurrencyCode),
				cur.CurrencyName,
				cur.Country,
				cur.Symbol,
				cur.DecimalPlaces,
				cur.Status,
			).Scan(&currencyID)
			if err != nil {
				tx.Rollback()
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
			_, auditErr := tx.Exec(auditQuery,
				currencyID,
				"CREATE",
				"PENDING_APPROVAL",
				nil,
				createdBy,
			)
			if auditErr != nil {
				tx.Rollback()
				results = append(results, map[string]interface{}{
					"success":       false,
					"error":         "Currency created but audit log failed: " + auditErr.Error(),
					"currency_id":   currencyID,
					"currency_code": cur.CurrencyCode,
				})
				continue
			}
			if commitErr := tx.Commit(); commitErr != nil {
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
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	}
}

// GET handler to fetch all currency records
func GetAllCurrencyMaster(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := `
			SELECT m.currency_id, m.currency_code, m.currency_name, m.country, m.symbol, m.decimal_places, m.status, m.old_decimal_places, m.old_status
			FROM mastercurrency m
		`
		rows, err := db.Query(query)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var currencies []map[string]interface{}
		for rows.Next() {
			var (
				currencyID, currencyCode, currencyName, country, symbol, status, oldStatus string
				decimalPlaces, oldDecimalPlaces                                            int
			)
			err := rows.Scan(&currencyID, &currencyCode, &currencyName, &country, &symbol, &decimalPlaces, &status, &oldDecimalPlaces, &oldStatus)
			if err != nil {
				continue
			}
			// Fetch latest audit record for this currency (for status, checker, etc.)
			auditQuery := `SELECT processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason FROM auditactioncurrency WHERE currency_id = $1 ORDER BY requested_at DESC LIMIT 1`
			var processingStatus, requestedBy, actionType, actionID, checkerBy, checkerComment, reason sql.NullString
			var requestedAt, checkerAt sql.NullTime
			_ = db.QueryRow(auditQuery, currencyID).Scan(&processingStatus, &requestedBy, &requestedAt, &actionType, &actionID, &checkerBy, &checkerAt, &checkerComment, &reason)

			// Fetch CREATE, EDIT, DELETE audit records for created/edited/deleted info
			auditDetailsQuery := `SELECT actiontype, requested_by, requested_at FROM auditactioncurrency WHERE currency_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
			auditRows, auditErr := db.Query(auditDetailsQuery, currencyID)
			var createdBy, createdAt, editedBy, editedAt, deletedBy, deletedAt string
			if auditErr == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var actionType string
					var requestedBy sql.NullString
					var requestedAt sql.NullTime
					auditRows.Scan(&actionType, &requestedBy, &requestedAt)
					auditInfo := api.GetAuditInfo(actionType, requestedBy, requestedAt)
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
				"processing_status":  getNullString(processingStatus),
				// "requested_by":       getNullString(requestedBy),
				// "requested_at":       getNullTime(requestedAt),
				"action_type":     getNullString(actionType),
				"action_id":       getNullString(actionID),
				"checker_at":      getNullTime(checkerAt),
				"checker_by":      getNullString(checkerBy),
				"checker_comment": getNullString(checkerComment),
				"reason":          getNullString(reason),
				"created_by":      createdBy,
				"created_at":      createdAt,
				"edited_by":       editedBy,
				"edited_at":       editedAt,
				"deleted_by":      deletedBy,
				"deleted_at":      deletedAt,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		if currencies == nil {
			currencies = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    currencies,
		})
	}
}

func UpdateCurrencyMasterBulk(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string                        `json:"user_id"`
			Currency []CurrencyMasterUpdateRequest `json:"currency"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		var results []map[string]interface{}
		for _, cur := range req.Currency {
			tx, txErr := db.Begin()
			if txErr != nil {
				results = append(results, map[string]interface{}{
					"success": false,
					"error":   "Failed to start transaction: " + txErr.Error(),
					// "currency_code": cur.CurrencyCode,
				})
				continue
			}
			query := `UPDATE mastercurrency SET old_decimal_places=$1, old_status=$2, status=$3, decimal_places=$4 WHERE currency_id=$5 RETURNING currency_id, currency_code`
			var currencyID, currencyCode string
			err := tx.QueryRow(query,
				cur.OldDecimalPlaces,
				cur.OLDStatus,
				cur.Status,
				cur.DecimalPlaces,
				// updatedBy,
				// strings.ToUpper(cur.CurrencyCode),
				cur.CurrencyID,
			).Scan(&currencyID, &currencyCode)
			if err != nil {
				tx.Rollback()
				results = append(results, map[string]interface{}{
					"success":       false,
					"error":         err.Error(),
					"currency_code": currencyCode,
				})
				continue
			}
			auditQuery := `INSERT INTO auditactioncurrency (
				currency_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, $2, $3, $4, $5, now())`
			_, auditErr := tx.Exec(auditQuery,
				currencyID,
				"EDIT",
				"PENDING_EDIT_APPROVAL",
				cur.Reason,
				updatedBy,
			)
			if auditErr != nil {
				tx.Rollback()
				results = append(results, map[string]interface{}{
					"success":       false,
					"error":         "Currency updated but audit log failed: " + auditErr.Error(),
					"currency_id":   currencyID,
					"currency_code": currencyCode,
				})
				continue
			}
			if commitErr := tx.Commit(); commitErr != nil {
				results = append(results, map[string]interface{}{
					"success":       false,
					"error":         "Transaction commit failed: " + commitErr.Error(),
					"currency_id":   currencyID,
					"currency_code": currencyCode,
				})
				continue
			}
			results = append(results, map[string]interface{}{
				"success":       true,
				"currency_id":   currencyID,
				"currency_code": currencyCode,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	}
}
func BulkRejectAuditActions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ActionIDs []string `json:"action_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ActionIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
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
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		query := `UPDATE auditactioncurrency SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id,currency_id`
		rows, err := db.Query(query, checkerBy, req.Comment, pq.Array(req.ActionIDs))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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
func BulkApproveAuditActions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ActionIDs []string `json:"action_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ActionIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
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
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		// First, delete records with processing_status = 'PENDING_DELETE_APPROVAL' for the given action_ids
		delQuery := `DELETE FROM auditactioncurrency WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, currency_id`
		delRows, delErr := db.Query(delQuery, pq.Array(req.ActionIDs))
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
			delCurQuery := `DELETE FROM mastercurrency WHERE currency_id = ANY($1)`
			_, _ = db.Exec(delCurQuery, pq.Array(currencyIDsToDelete))
		}

		// Then, approve the rest
		query := `UPDATE auditactioncurrency SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id,currency_id`
		rows, err := db.Query(query, checkerBy, req.Comment, pq.Array(req.ActionIDs))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
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

func BulkDeleteCurrencyAudit(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			CurrencyIDs []string `json:"currency_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.CurrencyIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
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
			respondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		var results []string
		for _, currencyID := range req.CurrencyIDs {
			query := `INSERT INTO auditactioncurrency (
                currency_id, actiontype, processing_status, reason, requested_by, requested_at
            ) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now()) RETURNING action_id`
			var actionID string
			err := db.QueryRow(query, currencyID, req.Reason, requestedBy).Scan(&actionID)
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
func GetActiveApprovedCurrencyCodes(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		rows, err := db.Query(query)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var code string
			var decimalPlace int
			if err := rows.Scan(&code, &decimalPlace); err == nil {
				results = append(results, map[string]interface{}{
					"currency_code": code,
					"decimal_place": decimalPlace,
				})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if results == nil {
			results = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	}
}
