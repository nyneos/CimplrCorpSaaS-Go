package dmsjobs

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/services/docsvc"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type generatedPackage struct {
	S3Key          string
	OutputFilename string
	FileSize       int64
	StorageBackend string
}

// fetchGeneratedBytes reads a generated object regardless of which S3 bucket
// it lives in — Main's own (MAIN_S3, pre-migration and LOCAL-fallback rows)
// or Document-Service's own (DOCSVC_S3). Every read path for DMS-generated
// content branches through this rather than assuming one backend.
func fetchGeneratedBytes(ctx context.Context, key, storageBackend string) ([]byte, error) {
	if storageBackend == "DOCSVC_S3" {
		return docsvc.NewFromEnv().DownloadDocument(ctx, key)
	}
	return s3storage.GetObjectBytes(ctx, key)
}

func ruleWantsZipPackage(ctx context.Context, pool *pgxpool.Pool, versionID string) (bool, error) {
	var count int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM dms_svc.generation_rule_destination
		WHERE version_id = $1::uuid
		  AND is_enabled = true
		  AND is_deleted = false
		  AND package_mode = 'ZIP'`, versionID).Scan(&count)
	return count > 0, err
}

// ensureRunZip downloads every generated object once, creates one reusable ZIP,
// uploads it once, and records it. Every ZIP destination references this object.
func ensureRunZip(
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, ruleName string,
	docs []generatedDoc,
) (generatedPackage, error) {
	var existing generatedPackage
	err := pool.QueryRow(ctx, `
		SELECT s3_key, output_filename, file_size, storage_backend
		FROM dms_svc.generated_package
		WHERE run_id = $1::uuid AND package_type = 'ZIP'`, runID,
	).Scan(&existing.S3Key, &existing.OutputFilename, &existing.FileSize, &existing.StorageBackend)
	if err == nil {
		return existing, nil
	}
	if err != pgx.ErrNoRows {
		return generatedPackage{}, fmt.Errorf("load existing ZIP: %w", err)
	}

	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	usedNames := make(map[string]int)
	for _, doc := range docs {
		body, getErr := fetchGeneratedBytes(ctx, doc.S3Key, doc.StorageBackend)
		if getErr != nil {
			_ = writer.Close()
			return generatedPackage{}, fmt.Errorf("download %s for ZIP: %w", doc.S3Key, getErr)
		}
		name := filenameForDoc(doc, ruleName)
		if n := usedNames[name]; n > 0 {
			dot := strings.LastIndex(name, ".")
			if dot > 0 {
				name = fmt.Sprintf("%s_%d%s", name[:dot], n+1, name[dot:])
			} else {
				name = fmt.Sprintf("%s_%d", name, n+1)
			}
		}
		usedNames[filenameForDoc(doc, ruleName)]++
		entry, createErr := writer.Create(name)
		if createErr != nil {
			_ = writer.Close()
			return generatedPackage{}, fmt.Errorf("create ZIP entry %s: %w", name, createErr)
		}
		if _, writeErr := entry.Write(body); writeErr != nil {
			_ = writer.Close()
			return generatedPackage{}, fmt.Errorf("write ZIP entry %s: %w", name, writeErr)
		}
	}
	if err := writer.Close(); err != nil {
		return generatedPackage{}, fmt.Errorf("close ZIP: %w", err)
	}

	sum := sha256.Sum256(buffer.Bytes())
	checksum := hex.EncodeToString(sum[:])
	storageBackend := "MAIN_S3"
	s3Key, storeErr := docsvc.NewFromEnv().UploadDocument(ctx, "dms/packages", buffer.Bytes(), "application/zip", "zip")
	if storeErr != nil {
		api.LogInfo("[DMS] ZIP store via document-service unavailable, falling back to Main S3: %v", storeErr)
		s3Key = s3storage.BuildModuleS3Key("dms", "packages", checksum, "zip")
		if err := s3storage.PutObjectToS3(ctx, s3Key, buffer.Bytes(), "application/zip"); err != nil {
			return generatedPackage{}, fmt.Errorf("upload ZIP: %w", err)
		}
	} else {
		storageBackend = "DOCSVC_S3"
	}
	base := sanitizeOutputName(ruleName)
	if base == "" {
		base = "documents"
	}
	filename := base + "_" + runID[:8] + ".zip"
	if _, err := pool.Exec(ctx, `
		INSERT INTO dms_svc.generated_package
			(run_id, package_type, s3_key, output_filename, file_size, checksum, storage_backend)
		VALUES ($1::uuid, 'ZIP', $2, $3, $4, $5, $6)
		ON CONFLICT (run_id, package_type) DO NOTHING`,
		runID, s3Key, filename, len(buffer.Bytes()), checksum, storageBackend); err != nil {
		return generatedPackage{}, fmt.Errorf("record ZIP: %w", err)
	}
	return generatedPackage{
		S3Key:          s3Key,
		OutputFilename: filename,
		FileSize:       int64(len(buffer.Bytes())),
		StorageBackend: storageBackend,
	}, nil
}
