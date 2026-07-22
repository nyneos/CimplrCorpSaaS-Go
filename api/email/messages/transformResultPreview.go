package emailmessages

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	emailcommon "CimplrCorpSaas/api/email/common"
	"CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const transformPreviewMaxBytes = 512 * 1024 // 512KB

// HandleTransformResultPreviewContent returns text content of original or transformed file
// so the UI can show an in-app preview without relying on S3 CORS.
func HandleTransformResultPreviewContent(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}

		var req struct {
			ResultID string `json:"result_id"`
			Which    string `json:"which"` // original | transformed
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "Invalid request body")
			return
		}
		resultID := strings.TrimSpace(req.ResultID)
		which := strings.ToLower(strings.TrimSpace(req.Which))
		if resultID == "" {
			emailcommon.RespondBadRequest(w, "result_id is required")
			return
		}
		if which != "original" && which != "transformed" {
			emailcommon.RespondBadRequest(w, "which must be original or transformed")
			return
		}

		var originalKey, transformedKey, filename, destType, outputLocation, outputFilename string
		err := pool.QueryRow(r.Context(), `
			SELECT COALESCE(ma.s3_key, ''),
			       COALESCE(tr.transformed_s3_key, ''),
			       COALESCE(ma.filename, ''),
			       COALESCE(NULLIF(tr.destination_type, ''), 'S3'),
			       COALESCE(tr.output_location, ''),
			       COALESCE(tr.output_filename, '')
			FROM email_svc.transformation_results tr
			JOIN email_svc.message_attachment ma ON ma.attachment_id = tr.attachment_id
			WHERE tr.result_id = $1::uuid
			LIMIT 1
		`, resultID).Scan(&originalKey, &transformedKey, &filename, &destType, &outputLocation, &outputFilename)
		if err != nil {
			emailcommon.RespondNotFound(w, "transformation result not found")
			return
		}

		var raw []byte
		label := filename
		s3Key := originalKey

		if which == "transformed" {
			label = "transformed"
			if strings.TrimSpace(outputFilename) != "" {
				label = outputFilename
			} else if transformedKey != "" {
				label = filepath.Base(transformedKey)
			}

			if strings.EqualFold(destType, "LOCAL") && strings.TrimSpace(outputLocation) != "" {
				if !isUnderTransformedLocalBase(outputLocation) {
					emailcommon.RespondBadRequest(w, "local file path is outside allowed directory")
					return
				}
				raw, err = os.ReadFile(outputLocation)
				if err != nil {
					emailcommon.RespondNotFound(w, "local file not found: "+err.Error())
					return
				}
			} else if strings.EqualFold(destType, "API") && strings.TrimSpace(outputFilename) != "" {
				rt := mailruntime.NewRuntime()
				if !rt.Ready() {
					emailcommon.RespondInternal(w, "email service not configured")
					return
				}
				readOut, readErr := rt.ReadAPIInbox(r.Context(), mailruntime.ReadAPIInboxRequest{
					Filename: outputFilename,
					Folder:   apiInboxFolderFromLocation(outputLocation),
				})
				if readErr != nil {
					emailcommon.RespondNotFound(w, "API inbox file not found: "+readErr.Error())
					return
				}
				raw, err = base64.StdEncoding.DecodeString(readOut.ContentBase64)
				if err != nil {
					emailcommon.RespondInternal(w, "Failed to decode API inbox file: "+err.Error())
					return
				}
			} else {
				s3Key = transformedKey
				if strings.TrimSpace(s3Key) == "" {
					emailcommon.RespondNotFound(w, which+" file not available")
					return
				}
				raw, err = s3storage.GetObjectBytes(r.Context(), s3Key)
				if err != nil {
					emailcommon.RespondInternal(w, "Failed to read file: "+err.Error())
					return
				}
			}
		} else {
			if strings.TrimSpace(s3Key) == "" {
				emailcommon.RespondNotFound(w, which+" file not available")
				return
			}
			raw, err = s3storage.GetObjectBytes(r.Context(), s3Key)
			if err != nil {
				emailcommon.RespondInternal(w, "Failed to read file: "+err.Error())
				return
			}
		}

		truncated := false
		if len(raw) > transformPreviewMaxBytes {
			raw = raw[:transformPreviewMaxBytes]
			truncated = true
		}

		content := string(raw)
		if !utf8.ValidString(content) {
			// Binary / non-UTF8 — show a short hex-ish notice rather than garbage
			emailcommon.RespondPayload(w, "transform-results/preview-content", map[string]interface{}{
				"which":      which,
				"filename":   label,
				"s3_key":     s3Key,
				"content":    "[Binary file — preview as text is not available. Use Download.]",
				"truncated": truncated,
				"is_binary":  true,
				"byte_size":  len(raw),
			})
			return
		}

		// Pretty-print JSON when possible
		trimmed := strings.TrimSpace(content)
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			var pretty interface{}
			if json.Unmarshal([]byte(trimmed), &pretty) == nil {
				if b, err := json.MarshalIndent(pretty, "", "  "); err == nil {
					content = string(b)
				}
			}
		}

		emailcommon.RespondPayload(w, "transform-results/preview-content", map[string]interface{}{
			"which":      which,
			"filename":   label,
			"s3_key":     s3Key,
			"content":    content,
			"truncated": truncated,
			"is_binary":  false,
			"byte_size":  len(raw),
		})
	}
}

func apiInboxFolderFromLocation(outputLocation string) string {
	loc := strings.ToLower(strings.TrimSpace(outputLocation))
	if strings.Contains(loc, "test-receive-2") {
		return "api-inbox-2"
	}
	return "api-inbox"
}
