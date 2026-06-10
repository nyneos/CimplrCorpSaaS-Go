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
		if packageRowsFileCount(rows) == 0 {
			http.Error(w, "no downloadable files found", http.StatusNotFound)
			return
		}

		zipName := packageZipName(rowIDs, opts.ModuleLabel)
		w.Header().Set(constants.ContentTypeText, "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", zipName))
		w.WriteHeader(http.StatusOK)

		zw := zip.NewWriter(w)
		writePackageRows(r.Context(), zw, rows)
		_ = zw.Close()
	}
}

type packageRow struct {
	RowID string
	Files []packageFile
}

type packageFile struct {
	FileName string
	S3Key    string
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
				row.Files = append(row.Files, packageFile{FileName: name, S3Key: mainFile.UploadS3Key})
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
					row.Files = append(row.Files, packageFile{FileName: packageAdditionalFolder + "/" + name, S3Key: key})
				}
			}
		}

		rows = append(rows, row)
	}
	return rows
}

func writePackageRows(ctx context.Context, zw *zip.Writer, rows []packageRow) {
	for _, row := range rows {
		writePackageRow(ctx, zw, row)
	}
}

func writePackageRow(ctx context.Context, zw *zip.Writer, row packageRow) {
	used := map[string]int{}
	rowFolder := safeZipSegment(row.RowID)
	for _, file := range row.Files {
		parts := strings.Split(file.FileName, "/")
		for i := range parts {
			parts[i] = safeZipFileName(parts[i])
		}
		path := uniqueZipPath(used, rowFolder+"/"+strings.Join(parts, "/"))
		_ = writeS3ZipFile(ctx, zw, path, file.S3Key)
	}
}

func writeS3ZipFile(ctx context.Context, zw *zip.Writer, zipPath, s3Key string) bool {
	body, err := s3storage.GetObjectBytes(ctx, strings.TrimSpace(s3Key))
	if err != nil {
		return false
	}
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
