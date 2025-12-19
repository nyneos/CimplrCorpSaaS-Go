package accountingworkbench

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SettingRequest represents a setting configuration
type SettingRequest struct {
	SettingKey   string `json:"setting_key"`
	SettingValue string `json:"setting_value"`
	SettingType  string `json:"setting_type"`
	Description  string `json:"description,omitempty"`
	IsActive     bool   `json:"is_active"`
	UpdatedBy    string `json:"updated_by"`
}

// SettingResponse represents a setting in the response
type SettingResponse struct {
	SettingID    int       `json:"setting_id"`
	SettingKey   string    `json:"setting_key"`
	SettingValue string    `json:"setting_value"`
	SettingType  string    `json:"setting_type"`
	Description  string    `json:"description,omitempty"`
	IsActive     bool      `json:"is_active"`
	UpdatedAt    time.Time `json:"updated_at"`
	UpdatedBy    string    `json:"updated_by"`
	CreatedAt    time.Time `json:"created_at"`
}

// CreateSetting creates a new accounting setting (no approval workflow)
func CreateSetting(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SettingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Validate required fields
		if req.SettingKey == "" || req.SettingValue == "" || req.SettingType == "" {
			http.Error(w, "setting_key, setting_value, and setting_type are required", http.StatusBadRequest)
			return
		}

		// Validate setting_type
		validTypes := map[string]bool{
			"ROUNDING":        true,
			"ACCOUNT_MAPPING": true,
			"THRESHOLD":       true,
			"PRECISION":       true,
			"FORMULA":         true,
		}
		if !validTypes[req.SettingType] {
			http.Error(w, "setting_type must be one of: ROUNDING, ACCOUNT_MAPPING, THRESHOLD, PRECISION, FORMULA", http.StatusBadRequest)
			return
		}

		// Normalize setting_key to uppercase
		req.SettingKey = strings.ToUpper(req.SettingKey)

		ctx := context.Background()
		tx, err := pool.Begin(ctx)
		if err != nil {
			log.Printf("Error starting transaction: %v", err)
			http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback(ctx)

		query := `
			INSERT INTO investment.accounting_setting 
			(setting_key, setting_value, setting_type, description, is_active, updated_by)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING setting_id, created_at, updated_at
		`

		var settingID int
		var createdAt, updatedAt time.Time
		err = tx.QueryRow(ctx, query,
			req.SettingKey,
			req.SettingValue,
			req.SettingType,
			req.Description,
			req.IsActive,
			req.UpdatedBy,
		).Scan(&settingID, &createdAt, &updatedAt)

		if err != nil {
			if strings.Contains(err.Error(), "duplicate key") {
				http.Error(w, "Setting with this key already exists", http.StatusConflict)
				return
			}
			log.Printf("Error creating setting: %v", err)
			http.Error(w, "Failed to create setting: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			log.Printf("Error committing transaction: %v", err)
			http.Error(w, "Failed to commit transaction", http.StatusInternalServerError)
			return
		}

		response := SettingResponse{
			SettingID:    settingID,
			SettingKey:   req.SettingKey,
			SettingValue: req.SettingValue,
			SettingType:  req.SettingType,
			Description:  req.Description,
			IsActive:     req.IsActive,
			UpdatedAt:    updatedAt,
			UpdatedBy:    req.UpdatedBy,
			CreatedAt:    createdAt,
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Setting created successfully",
			"data":    response,
		})
	}
}

// UpdateSetting updates an existing setting (no approval workflow)
func UpdateSetting(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		settingKey := r.URL.Query().Get("setting_key")
		if settingKey == "" {
			http.Error(w, constants.ErrSettingKeyRequired, http.StatusBadRequest)
			return
		}

		var req SettingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		ctx := context.Background()
		tx, err := pool.Begin(ctx)
		if err != nil {
			log.Printf("Error starting transaction: %v", err)
			http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback(ctx)

		// Build dynamic update query
		updateFields := []string{}
		args := []interface{}{}
		argCount := 1

		if req.SettingValue != "" {
			updateFields = append(updateFields, "setting_value = $"+strconv.Itoa(argCount))
			args = append(args, req.SettingValue)
			argCount++
		}

		if req.SettingType != "" {
			updateFields = append(updateFields, "setting_type = $"+strconv.Itoa(argCount))
			args = append(args, req.SettingType)
			argCount++
		}

		if req.Description != "" {
			updateFields = append(updateFields, "description = $"+strconv.Itoa(argCount))
			args = append(args, req.Description)
			argCount++
		}

		updateFields = append(updateFields, "is_active = $"+strconv.Itoa(argCount))
		args = append(args, req.IsActive)
		argCount++

		if req.UpdatedBy != "" {
			updateFields = append(updateFields, "updated_by = $"+strconv.Itoa(argCount))
			args = append(args, req.UpdatedBy)
			argCount++
		}

		updateFields = append(updateFields, "updated_at = NOW()")

		if len(updateFields) == 1 { // Only updated_at
			http.Error(w, "No fields to update", http.StatusBadRequest)
			return
		}

		// Add WHERE clause
		args = append(args, strings.ToUpper(settingKey))
		whereClause := "$" + strconv.Itoa(argCount)

		query := `
			UPDATE investment.accounting_setting 
			SET ` + strings.Join(updateFields, ", ") + `
			WHERE setting_key = ` + whereClause + `
			RETURNING setting_id, setting_key, setting_value, setting_type, description, is_active, updated_at, updated_by, created_at
		`

		var response SettingResponse
		err = tx.QueryRow(ctx, query, args...).Scan(
			&response.SettingID,
			&response.SettingKey,
			&response.SettingValue,
			&response.SettingType,
			&response.Description,
			&response.IsActive,
			&response.UpdatedAt,
			&response.UpdatedBy,
			&response.CreatedAt,
		)

		if err == sql.ErrNoRows {
			http.Error(w, constants.ErrSettingNotFound, http.StatusNotFound)
			return
		}
		if err != nil {
			log.Printf("Error updating setting: %v", err)
			http.Error(w, "Failed to update setting: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			log.Printf("Error committing transaction: %v", err)
			http.Error(w, "Failed to commit transaction", http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Setting updated successfully",
			"data":    response,
		})
	}
}

// GetSettings retrieves all settings or filters by type
func GetSettings(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		settingType := r.URL.Query().Get("setting_type")
		isActiveStr := r.URL.Query().Get("is_active")

		ctx := context.Background()

		// Build query
		query := `
			SELECT setting_id, setting_key, setting_value, setting_type, 
			       COALESCE(description, '') as description, is_active, 
			       updated_at, COALESCE(updated_by, '') as updated_by, created_at
			FROM investment.accounting_setting
			WHERE 1=1
		`
		args := []interface{}{}
		argCount := 1

		if settingType != "" {
			query += " AND setting_type = $" + strconv.Itoa(argCount)
			args = append(args, settingType)
			argCount++
		}

		if isActiveStr != "" {
			isActive, err := strconv.ParseBool(isActiveStr)
			if err == nil {
				query += " AND is_active = $" + strconv.Itoa(argCount)
				args = append(args, isActive)
				argCount++
			}
		}

		query += " ORDER BY setting_type, setting_key"

		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			log.Printf("Error fetching settings: %v", err)
			http.Error(w, "Failed to fetch settings", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		settings := []SettingResponse{}
		for rows.Next() {
			var setting SettingResponse
			err := rows.Scan(
				&setting.SettingID,
				&setting.SettingKey,
				&setting.SettingValue,
				&setting.SettingType,
				&setting.Description,
				&setting.IsActive,
				&setting.UpdatedAt,
				&setting.UpdatedBy,
				&setting.CreatedAt,
			)
			if err != nil {
				log.Printf("Error scanning setting: %v", err)
				continue
			}
			settings = append(settings, setting)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"count":   len(settings),
			"data":    settings,
		})
	}
}

// GetSettingByKey retrieves a single setting by key
func GetSettingByKey(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		settingKey := r.URL.Query().Get("setting_key")
		if settingKey == "" {
			http.Error(w, constants.ErrSettingKeyRequired, http.StatusBadRequest)
			return
		}

		ctx := context.Background()
		query := `
			SELECT setting_id, setting_key, setting_value, setting_type, 
			       COALESCE(description, '') as description, is_active, 
			       updated_at, COALESCE(updated_by, '') as updated_by, created_at
			FROM investment.accounting_setting
			WHERE setting_key = $1
		`

		var setting SettingResponse
		err := pool.QueryRow(ctx, query, strings.ToUpper(settingKey)).Scan(
			&setting.SettingID,
			&setting.SettingKey,
			&setting.SettingValue,
			&setting.SettingType,
			&setting.Description,
			&setting.IsActive,
			&setting.UpdatedAt,
			&setting.UpdatedBy,
			&setting.CreatedAt,
		)

		if err == sql.ErrNoRows {
			http.Error(w, constants.ErrSettingNotFound, http.StatusNotFound)
			return
		}
		if err != nil {
			log.Printf("Error fetching setting: %v", err)
			http.Error(w, "Failed to fetch setting", http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    setting,
		})
	}
}

// DeleteSetting soft-deletes a setting (just deactivates it)
func DeleteSetting(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		settingKey := r.URL.Query().Get("setting_key")
		if settingKey == "" {
			http.Error(w, constants.ErrSettingKeyRequired, http.StatusBadRequest)
			return
		}

		ctx := context.Background()
		query := `
			UPDATE investment.accounting_setting 
			SET is_active = FALSE, updated_at = NOW()
			WHERE setting_key = $1
			RETURNING setting_id
		`

		var settingID int
		err := pool.QueryRow(ctx, query, strings.ToUpper(settingKey)).Scan(&settingID)

		if err == sql.ErrNoRows {
			http.Error(w, constants.ErrSettingNotFound, http.StatusNotFound)
			return
		}
		if err != nil {
			log.Printf("Error deleting setting: %v", err)
			http.Error(w, "Failed to delete setting", http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Setting deactivated successfully",
		})
	}
}
