package emailmessages

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	emailcommon "CimplrCorpSaas/api/email/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

func cimplrTransformedLocalBase() string {
	if v := strings.TrimSpace(os.Getenv("CIMPLR_TRANSFORMED_LOCAL_DIR")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("EMAIL_TRANSFORMED_LOCAL_DIR")); v != "" {
		return v
	}
	return "./transformed"
}

func isUnderTransformedLocalBase(absPath string) bool {
	base, err := filepath.Abs(cimplrTransformedLocalBase())
	if err != nil {
		return false
	}
	full, err := filepath.Abs(absPath)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(base, full)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

// HandleTransformResultDownloadLocal returns a LOCAL destination file stored on
// the Cimplr Go host as content_base64 so the UI can trigger a browser download.
func HandleTransformResultDownloadLocal(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			ResultID string `json:"result_id"`
			UserID   string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "Invalid request body")
			return
		}
		resultID := strings.TrimSpace(req.ResultID)
		if resultID == "" {
			emailcommon.RespondBadRequest(w, "result_id is required")
			return
		}
		_, _, _, _ = emailcommon.RequestIdentity(r, "", "")

		var destType, location, filename string
		err := pool.QueryRow(r.Context(), `
			SELECT COALESCE(NULLIF(destination_type, ''), 'S3'),
			       COALESCE(output_location, ''),
			       COALESCE(output_filename, '')
			FROM email_svc.transformation_results
			WHERE result_id = $1::uuid
			LIMIT 1
		`, resultID).Scan(&destType, &location, &filename)
		if err != nil {
			emailcommon.RespondNotFound(w, "transformation result not found")
			return
		}
		if strings.ToUpper(strings.TrimSpace(destType)) != "LOCAL" {
			emailcommon.RespondBadRequest(w, "result is not a LOCAL destination file")
			return
		}
		location = strings.TrimSpace(location)
		if location == "" {
			emailcommon.RespondNotFound(w, "local file path is empty")
			return
		}
		if !isUnderTransformedLocalBase(location) {
			emailcommon.RespondBadRequest(w, "local file path is outside allowed directory")
			return
		}
		raw, err := os.ReadFile(location)
		if err != nil {
			emailcommon.RespondNotFound(w, "local file not found on server: "+err.Error())
			return
		}
		name := strings.TrimSpace(filename)
		if name == "" {
			name = filepath.Base(location)
		}
		emailcommon.RespondPayload(w, "transform-results/download-local", map[string]interface{}{
			"filename":       name,
			"content_base64": base64.StdEncoding.EncodeToString(raw),
			"byte_size":      len(raw),
			"path":           location,
		})
	}
}
