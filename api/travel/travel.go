package travel

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
)

// CreatePackageHandler accepts a JSON body for a travel package and
// inserts it into the `travel.packages` table. If a package with the same
// id already exists it will be updated.
func CreatePackageHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.Error(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			api.Error(w, http.StatusBadRequest, "invalid json payload")
			return
		}

		idVal, ok := payload["id"].(string)
		if !ok || idVal == "" {
			api.Error(w, http.StatusBadRequest, "missing id in payload")
			return
		}

		// Store the full JSON payload in package_json
		pkgBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("failed to marshal payload: %v", err)
			api.Error(w, http.StatusInternalServerError, "internal error")
			return
		}

		stmt := `INSERT INTO travel.packages (id, package_json) VALUES ($1, $2)
                 ON CONFLICT (id) DO UPDATE SET package_json = EXCLUDED.package_json, updated_at = now()`

		if _, err := db.Exec(stmt, idVal, pkgBytes); err != nil {
			log.Printf("failed to upsert package %s: %v", idVal, err)
			api.Error(w, http.StatusInternalServerError, "failed to save package")
			return
		}

		api.Success(w, http.StatusCreated, json.RawMessage(pkgBytes), "")
	}
}

// GetPackageHandler supports two modes:
// - GET /...?id=<id>   -> returns JSON for the package with given id
// - GET /              -> returns a list of packages (recent first, up to 100)
func GetPackageHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			api.Error(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		id := r.URL.Query().Get("id")

		if id != "" {
			var pkgBytes []byte
			err := db.QueryRow(`SELECT package_json FROM travel.packages WHERE id = $1`, id).Scan(&pkgBytes)
			if err == sql.ErrNoRows {
				api.Error(w, http.StatusNotFound, "404 page not found")
				return
			}
			if err != nil {
				log.Printf("failed to fetch package %s: %v", id, err)
				api.Error(w, http.StatusInternalServerError, "failed to fetch package")
				return
			}
			api.Success(w, http.StatusOK, json.RawMessage(pkgBytes), "")
			return
		}

		rows, err := db.Query(`SELECT id, package_json FROM travel.packages ORDER BY created_at DESC LIMIT 100`)
		if err != nil {
			log.Printf("failed to list packages: %v", err)
			api.Error(w, http.StatusInternalServerError, "failed to list packages")
			return
		}
		defer rows.Close()

		var out []map[string]interface{}
		for rows.Next() {
			var id string
			var pkgBytes []byte
			if err := rows.Scan(&id, &pkgBytes); err != nil {
				log.Printf("row scan error: %v", err)
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
			log.Printf("rows error: %v", err)
		}

		api.Success(w, http.StatusOK, out, "")
	}
}

// DeletePackageHandler deletes a package by id. Supports:
// - DELETE /...?id=<id>
// - POST /cash/package/delete with JSON body {"id":"..."} (for clients that can't send DELETE)
func DeletePackageHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// accept DELETE or POST (for compatibility)
		if r.Method != http.MethodDelete && r.Method != http.MethodPost {
			api.Error(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		id := r.URL.Query().Get("id")
		if id == "" {
			// try body
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				api.Error(w, http.StatusBadRequest, "missing id")
				return
			}
			if v, ok := body["id"].(string); ok {
				id = v
			}
		}

		if id == "" {
			api.Error(w, http.StatusBadRequest, "missing id")
			return
		}

		res, err := db.Exec(`DELETE FROM travel.packages WHERE id = $1`, id)
		if err != nil {
			log.Printf("failed to delete package %s: %v", id, err)
			api.Error(w, http.StatusInternalServerError, "failed to delete")
			return
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			api.Error(w, http.StatusNotFound, "404 page not found")
			return
		}

		api.Success(w, http.StatusOK, map[string]any{"deleted": true, "id": id}, "")
	}
}
