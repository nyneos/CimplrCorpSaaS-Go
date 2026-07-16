package emailmessages

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"
	"CimplrCorpSaas/api/utils/s3storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

const transformBulkDownloadMax = 50

// HandleTransformResultsBulkDownload streams a ZIP of selected transformed files.
// Body: { "ids": ["result-uuid", ...] }
func HandleTransformResultsBulkDownload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}

		var req struct {
			IDs []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}

		ids := make([]string, 0, len(req.IDs))
		seen := map[string]struct{}{}
		for _, id := range req.IDs {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
		if len(ids) == 0 {
			http.Error(w, "Please select at least one row to download.", http.StatusBadRequest)
			return
		}
		if len(ids) > transformBulkDownloadMax {
			http.Error(w, fmt.Sprintf("Select at most %d files per download.", transformBulkDownloadMax), http.StatusBadRequest)
			return
		}

		userID, userEmail, _, _ := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)

		query := `
			SELECT tr.result_id::text,
			       COALESCE(tr.transformed_s3_key, ''),
			       COALESCE(ma.filename, 'file'),
			       COALESCE(r.rule_name, 'rule'),
			       COALESCE(r.mapping_name, '')
			FROM email_svc.transformation_results tr
			JOIN email_svc.message_attachment ma ON ma.attachment_id = tr.attachment_id
			JOIN email_svc.message m ON m.message_id = ma.message_id
			LEFT JOIN email_svc.transformation_rules r ON r.rule_id = tr.rule_id
			WHERE tr.result_id = ANY($1::uuid[])
			  AND COALESCE(tr.transformed_s3_key, '') <> ''
			  AND COALESCE(tr.status, 'SUCCESS') = 'SUCCESS'
			` + emailcommon.ActiveInboxMessageSQL + `
			AND (
				$2::boolean
				OR EXISTS (
					SELECT 1
					FROM email_svc.inbox_config i
					WHERE i.is_deleted = false
					  AND i.processing_status = 'APPROVED'
					  AND i.is_active = true
					  AND (
						  i.owner_user_id = $3
						  OR ($4 <> '' AND LOWER(i.mailbox_address) = LOWER($4))
						  OR EXISTS (
							  SELECT 1 FROM email_svc.inbox_members im
							  WHERE im.inbox_id = i.inbox_id AND im.user_id = $3
						  )
					  )
					  AND (
						  (m.inbox_id IS NOT NULL AND i.inbox_id = m.inbox_id)
						  OR LOWER(COALESCE(m.envelope_from, '')) = LOWER(i.mailbox_address)
						  OR LOWER(i.mailbox_address) = ANY (
							  SELECT LOWER(unnest(COALESCE(m.envelope_to, ARRAY[]::text[])))
						  )
					  )
				)
				OR (
					m.processing_status = 'MANUAL_UPLOAD'
					AND EXISTS (
						SELECT 1 FROM email_svc.processing_log upl_self
						WHERE upl_self.message_id = m.message_id
						  AND upl_self.step = 'UPLOAD_EML'
						  AND (
							  upl_self.detail->>'uploaded_by' = $3
							  OR ($4 <> '' AND upl_self.detail->>'uploaded_by' = $4)
						  )
					)
				)
			)`

		rows, err := pool.Query(r.Context(), query, ids, admin, userID, userEmail)
		if err != nil {
			http.Error(w, "Failed to load files: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type fileRow struct {
			ResultID string
			S3Key    string
			Filename string
			RuleName string
			Mapping  string
		}
		files := make([]fileRow, 0)
		for rows.Next() {
			var f fileRow
			if err := rows.Scan(&f.ResultID, &f.S3Key, &f.Filename, &f.RuleName, &f.Mapping); err != nil {
				continue
			}
			files = append(files, f)
		}
		if len(files) == 0 {
			http.Error(w, "No files available for download.", http.StatusNotFound)
			return
		}

		zipName := fmt.Sprintf("transformed_files_%s.zip", time.Now().Format("20060102_150405"))
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, zipName))

		zw := zip.NewWriter(w)
		defer zw.Close()

		usedNames := map[string]int{}
		added := 0
		for _, f := range files {
			data, err := s3storage.GetObjectBytes(r.Context(), f.S3Key)
			if err != nil {
				continue
			}
			ext := filepath.Ext(f.S3Key)
			if ext == "" {
				ext = filepath.Ext(f.Filename)
			}
			base := strings.TrimSuffix(filepath.Base(f.Filename), filepath.Ext(f.Filename))
			if base == "" || base == "." {
				base = "file"
			}
			rulePart := sanitizeZipName(f.RuleName)
			if rulePart == "" {
				rulePart = "rule"
			}
			name := fmt.Sprintf("%s__%s%s", base, rulePart, ext)
			if n, ok := usedNames[name]; ok {
				usedNames[name] = n + 1
				name = fmt.Sprintf("%s__%s_%d%s", base, rulePart, n+1, ext)
			} else {
				usedNames[name] = 1
			}

			fw, err := zw.Create(name)
			if err != nil {
				continue
			}
			if _, err := fw.Write(data); err != nil {
				continue
			}
			added++
		}
		if added == 0 {
			http.Error(w, "No files available for download.", http.StatusNotFound)
			return
		}
	}
}

func sanitizeZipName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) > 40 {
		out = out[:40]
	}
	return out
}
