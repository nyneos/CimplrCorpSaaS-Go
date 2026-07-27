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

	"github.com/jackc/pgx/v5/pgxpool"
)

const transformPreviewMaxBytes = 512 * 1024       // 512KB text previews
const transformSpreadsheetMaxBytes = 4 * 1024 * 1024 // 4MB spreadsheet / binary previews

func isSpreadsheetFilename(name string) bool {
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(name), "."))
	// ".excel" is used by some transform destinations; content is typically CSV/XLSX.
	return ext == "xlsx" || ext == "xls" || ext == "csv" || ext == "excel"
}

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

			// Prefer S3 whenever any delivery stored there (S3+API / S3+SFTP mixes).
			s3FromDelivery := ""
			s3NameFromDelivery := ""
			drows, derr := pool.Query(r.Context(), `
				SELECT COALESCE(transformed_s3_key, ''),
				       COALESCE(output_location, ''),
				       COALESCE(output_filename, '')
				FROM email_svc.transformation_result_deliveries
				WHERE result_id = $1::uuid
				  AND UPPER(COALESCE(destination_type, '')) = 'S3'
				  AND UPPER(COALESCE(status, 'SUCCESS')) = 'SUCCESS'
				ORDER BY created_at ASC
			`, resultID)
			if derr == nil {
				defer drows.Close()
				for drows.Next() {
					var key, loc, name string
					if scanErr := drows.Scan(&key, &loc, &name); scanErr != nil {
						continue
					}
					key = strings.TrimSpace(key)
					if key == "" {
						key = strings.TrimSpace(loc)
					}
					if key == "" {
						continue
					}
					s3FromDelivery = key
					s3NameFromDelivery = strings.TrimSpace(name)
					break
				}
			}
			if s3FromDelivery != "" {
				transformedKey = s3FromDelivery
				if s3NameFromDelivery != "" {
					label = s3NameFromDelivery
				} else {
					label = filepath.Base(s3FromDelivery)
				}
			}

			if strings.TrimSpace(transformedKey) != "" {
				s3Key = transformedKey
				raw, err = s3storage.GetObjectBytes(r.Context(), s3Key)
				if err != nil {
					emailcommon.RespondInternal(w, "Failed to read file: "+err.Error())
					return
				}
			} else if strings.EqualFold(destType, "LOCAL") && strings.TrimSpace(outputLocation) != "" {
				if !isUnderTransformedLocalBase(outputLocation) {
					emailcommon.RespondBadRequest(w, "local file path is outside allowed directory")
					return
				}
				raw, err = os.ReadFile(outputLocation)
				if err != nil {
					emailcommon.RespondNotFound(w, "local file not found: "+err.Error())
					return
				}
			} else {
				// API / SFTP (and mixes without S3) are store-only — no preview bytes.
				emailcommon.RespondNotFound(w, "preview not available for this destination (no S3 copy)")
				return
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
		maxBytes := transformPreviewMaxBytes
		if isSpreadsheetFilename(label) {
			maxBytes = transformSpreadsheetMaxBytes
		}
		if len(raw) > maxBytes {
			raw = raw[:maxBytes]
			truncated = true
		}

		// Spreadsheets / CSV: return base64 so the UI can render Excel grid preview
		// the same way as Email DMS (FilePreviewModal + parseExcel).
		if isSpreadsheetFilename(label) {
			emailcommon.RespondPayload(w, "transform-results/preview-content", map[string]interface{}{
				"which":          which,
				"filename":       label,
				"s3_key":         s3Key,
				"content":        "",
				"content_base64": base64.StdEncoding.EncodeToString(raw),
				"truncated":     truncated,
				"is_binary":      true,
				"is_spreadsheet": true,
				"byte_size":      len(raw),
			})
			return
		}

		content := string(raw)
		if !utf8.ValidString(content) {
			// Binary / non-UTF8 — return base64 so UI can still offer download-style blob preview
			emailcommon.RespondPayload(w, "transform-results/preview-content", map[string]interface{}{
				"which":          which,
				"filename":       label,
				"s3_key":         s3Key,
				"content":        "[Binary file — preview as text is not available. Use Download.]",
				"content_base64": base64.StdEncoding.EncodeToString(raw),
				"truncated":     truncated,
				"is_binary":      true,
				"is_spreadsheet": false,
				"byte_size":      len(raw),
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
			"which":          which,
			"filename":       label,
			"s3_key":         s3Key,
			"content":        content,
			"content_base64": "",
			"truncated":     truncated,
			"is_binary":      false,
			"is_spreadsheet": false,
			"byte_size":      len(raw),
		})
	}
}
