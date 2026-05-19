package travel

import (
	"CimplrCorpSaas/api/constants"
	"database/sql"
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/internal/logger")

// CreatePackageHandler accepts a JSON body for a travel package and
// inserts it into the `travel.packages` table. If a package with the same
// id already exists it will be updated.
func CreatePackageHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid json payload", http.StatusBadRequest)
			return
		}

		idVal, ok := payload["id"].(string)
		if !ok || idVal == "" {
			http.Error(w, "missing id in payload", http.StatusBadRequest)
			return
		}

		// Store the full JSON payload in package_json
		pkgBytes, err := json.Marshal(payload)
		if err != nil {
			logger.LogError("failed to marshal payload: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		stmt := `INSERT INTO travel.packages (id, package_json) VALUES ($1, $2)
                 ON CONFLICT (id) DO UPDATE SET package_json = EXCLUDED.package_json, updated_at = now()`

		if _, err := db.Exec(stmt, idVal, pkgBytes); err != nil {
			logger.LogError("failed to upsert package %s: %v", idVal, err)
			http.Error(w, "failed to save package", http.StatusInternalServerError)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusCreated)
		w.Write(pkgBytes)
	}
}

// GetPackageHandler supports two modes:
// - GET /...?id=<id>   -> returns JSON for the package with given id
// - GET /              -> returns a list of packages (recent first, up to 100)
func GetPackageHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.URL.Query().Get("id")
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)

		if id != "" {
			var pkgBytes []byte
			err := db.QueryRow(`SELECT package_json FROM travel.packages WHERE id = $1 AND COALESCE(is_deleted, false) = false`, id).Scan(&pkgBytes)
			if err == sql.ErrNoRows {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				logger.LogError("failed to fetch package %s: %v", id, err)
				http.Error(w, "failed to fetch package", http.StatusInternalServerError)
				return
			}
			w.Write(pkgBytes)
			return
		}

		rows, err := db.Query(`SELECT id, package_json FROM travel.packages WHERE COALESCE(is_deleted, false) = false ORDER BY created_at DESC LIMIT 100`)
		if err != nil {
			logger.LogError("failed to list packages: %v", err)
			http.Error(w, "failed to list packages", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		for rows.Next() {
			var id string
			var pkgBytes []byte
			if err := rows.Scan(&id, &pkgBytes); err != nil {
				logger.LogError("row scan error: %v", err)
				continue
			}
			var obj map[string]interface{}
			if err := json.Unmarshal(pkgBytes, &obj); err != nil {
				// if unmarshal fails, include raw json under "package_json"
				obj = map[string]interface{}{"id": id, "package_json": string(pkgBytes)}
			} else {
				// ensure id field exists and is correct
				obj["id"] = id
			}
			out = append(out, obj)
		}

		if err := rows.Err(); err != nil {
			logger.LogError("rows error: %v", err)
		}

		if err := json.NewEncoder(w).Encode(out); err != nil {
			logger.LogError("failed to write response: %v", err)
		}
	}
}

// DeletePackageHandler deletes a package by id (soft delete). Supports:
// - DELETE /...?id=<id>
// - POST /cash/package/delete with JSON body {"id":"..."} (for clients that can't send DELETE)
func DeletePackageHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// accept DELETE or POST (for compatibility)
		if r.Method != http.MethodDelete && r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.URL.Query().Get("id")
		if id == "" {
			// try body
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "missing id", http.StatusBadRequest)
				return
			}
			if v, ok := body["id"].(string); ok {
				id = v
			}
		}

		if id == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}

		// Soft delete: set is_deleted = true
		res, err := db.Exec(`UPDATE travel.packages SET is_deleted = true, updated_at = now() WHERE id = $1`, id)
		if err != nil {
			logger.LogError("failed to soft delete package %s: %v", id, err)
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			http.NotFound(w, r)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"deleted": true, "id": id})
	}
}

