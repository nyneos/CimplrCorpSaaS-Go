package bulkuploadaudit

import (
	"context"
	"time"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusCompleted = "COMPLETED"
	StatusPartial   = "PARTIAL"
	StatusFailed    = "FAILED"

	auditTable = "cimplrcorpsaas.master_bulk_upload_audit"
)

// Entry holds the data recorded for one bulk upload invocation.
type Entry struct {
	ModuleKey        string
	OriginalFileName string
	StoredFileName   string
	UploadS3Key      string
	ContentType      string
	FileSize         int64
	TotalRows        int
	InsertedCount    int
	ErrorCount       int
	Status           string
	UploadedBy       string
	UploadedAt       time.Time
	FileHash         string
}

func ExistsByHash(ctx context.Context, pool *pgxpool.Pool, moduleKey, fileHash string) (bool, error) {
	if fileHash == "" {
		return false, nil
	}
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM `+auditTable+`
			WHERE module_key = $1 AND file_hash = $2
		)`, moduleKey, fileHash).Scan(&exists)
	return exists, err
}

// StatusFor returns COMPLETED, PARTIAL, or FAILED based on counts.
func StatusFor(inserted, errors int) string {
	switch {
	case inserted > 0 && errors == 0:
		return StatusCompleted
	case inserted > 0:
		return StatusPartial
	default:
		return StatusFailed
	}
}

// Record writes one row to master_bulk_upload_audit.
// It is called after the main transaction has committed, so it uses the pool
// directly. Failures are logged but do not propagate — audit must not block
// the upload response.
func Record(ctx context.Context, pool *pgxpool.Pool, e Entry) {
	if e.UploadedAt.IsZero() {
		e.UploadedAt = time.Now().UTC()
	}
	if e.Status == "" {
		e.Status = StatusFor(e.InsertedCount, e.ErrorCount)
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO `+auditTable+` (
			module_key,
			original_file_name,
			stored_file_name,
			upload_s3_key,
			content_type,
			file_size,
			total_rows,
			inserted_count,
			error_count,
			status,
			uploaded_by,
			uploaded_at,
			file_hash
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		e.ModuleKey,
		e.OriginalFileName,
		nullText(e.StoredFileName),
		nullText(e.UploadS3Key),
		nullText(e.ContentType),
		nullInt64(e.FileSize),
		nullInt(e.TotalRows),
		nullInt(e.InsertedCount),
		nullInt(e.ErrorCount),
		e.Status,
		nullText(e.UploadedBy),
		e.UploadedAt,
		nullText(e.FileHash),
	)
	if err != nil {
		logger.LogError("master_bulk_upload_audit: module=%s file=%s: %v", e.ModuleKey, e.OriginalFileName, err)
	}
}

func nullText(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullInt(n int) interface{} {
	if n == 0 {
		return nil
	}
	return n
}

func nullInt64(n int64) interface{} {
	if n == 0 {
		return nil
	}
	return n
}
