package uploadutil

import (
	"CimplrCorpSaas/api/utils/s3storage"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrFileAlreadyUploaded = errors.New("investment file already uploaded")

func EnsureUniqueMultipartUpload(ctx context.Context, pool *pgxpool.Pool, headers []*multipart.FileHeader, query string, args ...interface{}) error {
	if len(headers) == 0 || headers[0] == nil {
		return nil
	}

	file, err := headers[0].Open()
	if err != nil {
		return fmt.Errorf("failed to open upload file: %w", err)
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read upload file: %w", err)
	}

	keys, err := existingUploadKeys(ctx, pool, query, args...)
	if err != nil {
		return err
	}

	duplicateKey, err := s3storage.FindDuplicateObjectKey(ctx, body, keys)
	if err != nil {
		return fmt.Errorf("failed to compare uploaded file: %w", err)
	}
	if duplicateKey != "" {
		return ErrFileAlreadyUploaded
	}
	return nil
}

func existingUploadKeys(ctx context.Context, pool *pgxpool.Pool, query string, args ...interface{}) ([]string, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing upload keys: %w", err)
	}
	defer rows.Close()

	keys := make([]string, 0)
	for rows.Next() {
		var key sql.NullString
		if scanErr := rows.Scan(&key); scanErr != nil {
			return nil, fmt.Errorf("failed to scan existing upload key: %w", scanErr)
		}
		trimmed := strings.TrimSpace(key.String)
		if key.Valid && trimmed != "" {
			keys = append(keys, trimmed)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate existing upload keys: %w", err)
	}

	return keys, nil
}
