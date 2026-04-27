package travel

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CreatePackageHandler accepts a JSON body for a travel package and
// inserts it into the `travel.packages` table. If a package with the same
// id already exists it will be updated.
func CreatePackageHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			log.Printf("failed to marshal payload: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		stmt := `INSERT INTO travel.packages (id, package_json) VALUES ($1, $2)
                 ON CONFLICT (id) DO UPDATE SET package_json = EXCLUDED.package_json, updated_at = now()`

		if _, err := pgxPool.Exec(r.Context(), stmt, idVal, pkgBytes); err != nil {
			log.Printf("failed to upsert package %s: %v", idVal, err)
			http.Error(w, "failed to save package", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write(pkgBytes)
	}
}

// GetPackageHandler supports two modes:
// - GET /...?id=<id>   -> returns JSON for the package with given id
// - GET /              -> returns a list of packages (recent first, up to 100)
func GetPackageHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := r.URL.Query().Get("id")
		w.Header().Set("Content-Type", "application/json")

		if id != "" {
			var pkgBytes []byte
			err := pgxPool.QueryRow(r.Context(), `SELECT package_json FROM travel.packages WHERE id = $1`, id).Scan(&pkgBytes)
			if err == pgx.ErrNoRows {
				http.NotFound(w, r)
				return
			}
			if err != nil {
				log.Printf("failed to fetch package %s: %v", id, err)
				http.Error(w, "failed to fetch package", http.StatusInternalServerError)
				return
			}
			w.Write(pkgBytes)
			return
		}

		rows, err := pgxPool.Query(r.Context(), `SELECT id, package_json FROM travel.packages ORDER BY created_at DESC LIMIT 100`)
		if err != nil {
			log.Printf("failed to list packages: %v", err)
			http.Error(w, "failed to list packages", http.StatusInternalServerError)
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

		if err := json.NewEncoder(w).Encode(out); err != nil {
			log.Printf("failed to write response: %v", err)
		}
	}
}

// DeletePackageHandler deletes a package by id. Supports:
// - DELETE /...?id=<id>
// - POST /cash/package/delete with JSON body {"id":"..."} (for clients that can't send DELETE)
func DeletePackageHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
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

		res, err := pgxPool.Exec(r.Context(), `DELETE FROM travel.packages WHERE id = $1`, id)
		if err != nil {
			log.Printf("failed to delete package %s: %v", id, err)
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		rows := res.RowsAffected()
		if rows == 0 {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"deleted": true, "id": id})
	}
}
