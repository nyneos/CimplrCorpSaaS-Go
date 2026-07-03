package additionalfiles

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	s3storage "CimplrCorpSaas/api/utils/s3storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

const packageAdditionalFolder = "additional files"

type MainPackageFile struct {
	FileName    string
	UploadS3Key string
}

type PackageZipOptions struct {
	ModuleLabel               string
	IDField                   string
	LoadMain                  func(ctx context.Context, pool *pgxpool.Pool, rowID string) (*MainPackageFile, error)
	ResolveAdditionalParentID func(ctx context.Context, pool *pgxpool.Pool, rowID string) (string, error)
}

type packageZipRequest struct {
	UserID string   `json:"user_id"`
	IDs    []string `json:"ids"`
}

func NewPackageZipHandler(pool *pgxpool.Pool, cfg Config, opts PackageZipOptions) http.HandlerFunc {
	idField := strings.TrimSpace(opts.IDField)
	if idField == "" {
		idField = cfg.ParentIDField
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req packageZipRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidJSONPrefix+err.Error(), http.StatusBadRequest)
			return
		}
		rowIDs := trimPackageIDs(req.IDs)
		if len(rowIDs) == 0 {
			http.Error(w, fmt.Sprintf("%s is required", idField), http.StatusBadRequest)
			return
		}

		rows := collectPackageRows(r.Context(), pool, cfg, opts, rowIDs)
		rows = preparePackageRows(r.Context(), rows)
		if packageRowsFileCount(rows) == 0 {
			http.Error(w, "no downloadable files found", http.StatusNotFound)
			return
		}
		recordPackageDownloadAudits(r.Context(), pool, cfg, req.UserID, api.ClientIPFromRequest(r), rows)

		zipName := packageZipName(rowIDs, opts.ModuleLabel)
		w.Header().Set(constants.ContentTypeText, "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", zipName))
		w.WriteHeader(http.StatusOK)

		zw := zip.NewWriter(w)
		writePackageRows(zw, rows)
		_ = zw.Close()
	}
}

type packageRow struct {
	RowID string
	Files []packageFile
}

type packageFile struct {
	FileName         string
	StoredFileName   string
	S3Key            string
	FileID           string
	AuditParentID    string
	IsAdditionalFile bool
	Body             []byte
}

func collectPackageRows(ctx context.Context, pool *pgxpool.Pool, cfg Config, opts PackageZipOptions, rowIDs []string) []packageRow {
	rows := make([]packageRow, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		row := packageRow{RowID: rowID}
		additionalParentID := rowID

		if opts.LoadMain != nil {
			if mainFile, err := opts.LoadMain(ctx, pool, rowID); err == nil && mainFile != nil && strings.TrimSpace(mainFile.UploadS3Key) != "" {
				name := mainFile.FileName
				if strings.TrimSpace(name) == "" {
					name = objectBaseName(mainFile.UploadS3Key)
				}
				row.Files = append(row.Files, packageFile{
					FileName:       name,
					StoredFileName: name,
					S3Key:          mainFile.UploadS3Key,
					AuditParentID:  rowID,
				})
			}
		}

		if opts.ResolveAdditionalParentID != nil {
			if parentID, err := opts.ResolveAdditionalParentID(ctx, pool, rowID); err == nil && strings.TrimSpace(parentID) != "" {
				additionalParentID = strings.TrimSpace(parentID)
			}
		}

		if cfg.List != nil {
			if files, err := cfg.List(ctx, pool, additionalParentID); err == nil {
				for _, file := range files {
					key := strings.TrimSpace(file.UploadS3Key)
					if key == "" {
						continue
					}
					name := file.StoredFileName
					if strings.TrimSpace(name) == "" {
						name = objectBaseName(key)
					}
					row.Files = append(row.Files, packageFile{
						FileName:         packageAdditionalFolder + "/" + name,
						StoredFileName:   name,
						S3Key:            key,
						FileID:           file.FileID,
						AuditParentID:    additionalParentID,
						IsAdditionalFile: true,
					})
				}
			}
		}

		rows = append(rows, row)
	}
	return rows
}

func preparePackageRows(ctx context.Context, rows []packageRow) []packageRow {
	prepared := make([]packageRow, 0, len(rows))
	for _, row := range rows {
		files := make([]packageFile, 0, len(row.Files))
		for _, file := range row.Files {
			body, err := s3storage.GetObjectBytes(ctx, strings.TrimSpace(file.S3Key))
			if err != nil {
				continue
			}
			file.Body = body
			files = append(files, file)
		}
		row.Files = files
		prepared = append(prepared, row)
	}
	return prepared
}

func recordPackageDownloadAudits(ctx context.Context, pool *pgxpool.Pool, cfg Config, userID, requestedIP string, rows []packageRow) {
	if !auditEnabled(cfg) {
		return
	}
	performedBy := requestedByOrFallback(ctx, userID)
	requestedAt := time.Now().UTC()
	for _, row := range rows {
		for _, file := range row.Files {
			parentID := strings.TrimSpace(file.AuditParentID)
			if parentID == "" {
				parentID = row.RowID
			}
			payload := MainUploadAuditPayload{
				FileID:      strings.TrimSpace(file.FileID),
				FileName:    strings.TrimSpace(file.StoredFileName),
				UploadS3Key: strings.TrimSpace(file.S3Key),
				UploadedBy:  performedBy,
				UploadedAt:  requestedAt,
				RequestedIP: requestedIP,
			}
			if file.IsAdditionalFile {
				record := FileRecord{
					FileID:         payload.FileID,
					StoredFileName: payload.FileName,
					UploadS3Key:    payload.UploadS3Key,
				}
				if err := recordDownloadAudit(ctx, pool, cfg, parentID, record, downloadActorInfo{RequestedBy: performedBy, RequestedIP: requestedIP}); err != nil && !isUndefinedTableError(err) {
					continue
				}
			}
			_ = recordMainDownloadAuditSafely(ctx, pool, cfg, parentID, payload)
		}
	}
}

func writePackageRows(zw *zip.Writer, rows []packageRow) {
	for _, row := range rows {
		writePackageRow(zw, row)
	}
}

func writePackageRow(zw *zip.Writer, row packageRow) {
	used := map[string]int{}
	rowFolder := safeZipSegment(row.RowID)
	for _, file := range row.Files {
		parts := strings.Split(file.FileName, "/")
		for i := range parts {
			parts[i] = safeZipFileName(parts[i])
		}
		path := uniqueZipPath(used, rowFolder+"/"+strings.Join(parts, "/"))
		_ = writePreparedZipFile(zw, path, file.Body)
	}
}

func writePreparedZipFile(zw *zip.Writer, zipPath string, body []byte) bool {
	writer, err := zw.Create(zipPath)
	if err != nil {
		return false
	}
	_, err = writer.Write(body)
	return err == nil
}

func packageRowsFileCount(rows []packageRow) int {
	count := 0
	for _, row := range rows {
		count += len(row.Files)
	}
	return count
}

func trimPackageIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		candidate := strings.TrimSpace(id)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	return out
}

func packageZipName(rowIDs []string, moduleLabel string) string {
	if len(rowIDs) == 1 {
		return safeZipSegment(rowIDs[0]) + ".zip"
	}
	label := strings.TrimSpace(moduleLabel)
	if label == "" {
		label = "Documents"
	}
	return safeZipFileName(label) + ".zip"
}

var unsafeZipNameChars = regexp.MustCompile(`[<>:"\\|?*\x00-\x1F]`)

func safeZipSegment(value string) string {
	cleaned := strings.TrimSpace(unsafeZipNameChars.ReplaceAllString(value, "_"))
	cleaned = strings.NewReplacer("/", "_", "\\", "_").Replace(cleaned)
	cleaned = strings.Trim(cleaned, ". ")
	if cleaned == "" {
		return "record"
	}
	return cleaned
}

func safeZipFileName(value string) string {
	name := strings.TrimSpace(filepath.Base(value))
	name = unsafeZipNameChars.ReplaceAllString(name, "_")
	name = strings.Trim(name, ". ")
	if name == "" {
		return "file"
	}
	return name
}

func objectBaseName(key string) string {
	return safeZipFileName(strings.TrimSpace(key))
}

func uniqueZipPath(used map[string]int, path string) string {
	if used[path] == 0 {
		used[path] = 1
		return path
	}
	used[path]++
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	return fmt.Sprintf("%s (%d)%s", base, used[path], ext)
}
