package s3storage

import (
	"CimplrCorpSaas/api/constants"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const (
	storageDefaultBucket   = "cimplr"
	storageDefaultPrefix   = "bankstatements/"
	storageDefaultRegion   = "ap-south-1"
	storageDefaultBaseURL  = "https://cimplr.s3.ap-south-1.amazonaws.com/"
	contentHashMetadataKey = "content-sha256"
)

func storageBucket() string {
	if b := strings.TrimSpace(os.Getenv("BANK_STMT_S3_BUCKET")); b != "" {
		return b
	}
	return storageDefaultBucket
}

func storageRegion() string {
	if r := strings.TrimSpace(os.Getenv("BANK_STMT_S3_REGION")); r != "" {
		return r
	}
	return storageDefaultRegion
}

func storageBaseURL() string {
	if u := strings.TrimSpace(os.Getenv("BANK_STMT_S3_BASE_URL")); u != "" {
		u = strings.TrimSuffix(u, "/")
		return u + "/"
	}
	return storageDefaultBaseURL
}

// storageRootPrefix returns the root folder for uploads.
// Env override: BANK_STMT_ROOT_PREFIX (trailing slash optional).
func storageRootPrefix() string {
	if p := strings.TrimSpace(os.Getenv("BANK_STMT_ROOT_PREFIX")); p != "" {
		return ensureTrailingSlash(p)
	}
	return storageDefaultPrefix
}

// modulePrefix picks the folder for a module based on BANK_STMT_PREFIX_MAP.
// Format: "moduleA=folderA;moduleB=folderB". Falls back to storageRootPrefix.
func modulePrefix(module string) string {
	defaultPrefix := storageRootPrefix()
	m := strings.TrimSpace(strings.ToLower(module))
	if m == "" {
		return defaultPrefix
	}

	mapStr := strings.TrimSpace(os.Getenv("BANK_STMT_PREFIX_MAP"))
	if mapStr != "" {
		pairs := strings.Split(mapStr, ";")
		for _, p := range pairs {
			p = strings.TrimSpace(p)
			if p == "" || !strings.Contains(p, "=") {
				continue
			}
			kv := strings.SplitN(p, "=", 2)
			key := strings.TrimSpace(strings.ToLower(kv[0]))
			val := strings.TrimSpace(kv[1])
			if key == m && val != "" {
				return ensureTrailingSlash(val)
			}
		}
	}

	return defaultPrefix
}

func ensureTrailingSlash(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

func sanitizePathSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(" ", "_", "/", "_", "\\", "_")
	return replacer.Replace(s)
}

func sanitizeFilenameComponent(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "file"
	}
	var builder strings.Builder
	builder.Grow(len(s))
	lastUnderscore := false
	for _, r := range s {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_':
			builder.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore {
				builder.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	cleaned := strings.Trim(builder.String(), "-_")
	if cleaned == "" {
		return "file"
	}
	return cleaned
}

func safeObjectName(name string) string {
	name = strings.TrimSpace(filepath.Base(name))
	if name == "" || name == "." || name == "/" {
		return "download.bin"
	}
	return name
}

// BuildUploadedFilename returns a file name in the format
// originalname-uploader-timestamp.ext using a filesystem-safe basename.
func BuildUploadedFilename(originalFilename, uploadedBy string, uploadedAt time.Time) string {
	baseName := strings.TrimSpace(filepath.Base(originalFilename))
	ext := strings.TrimSpace(strings.ToLower(filepath.Ext(baseName)))
	nameOnly := strings.TrimSpace(strings.TrimSuffix(baseName, filepath.Ext(baseName)))
	if nameOnly == "" {
		nameOnly = "file"
	}
	if ext == "" {
		ext = ".bin"
	}
	if uploadedAt.IsZero() {
		uploadedAt = time.Now().UTC()
	}
	loc, _ := time.LoadLocation("Asia/Kolkata")
	timestamp := uploadedAt.In(loc).Format("02 Jan 2006, 03:04 PM MST")
	return fmt.Sprintf("%s-%s-%s%s",
		sanitizeFilenameComponent(nameOnly),
		sanitizeFilenameComponent(uploadedBy),
		timestamp,
		ext,
	)
}

// BuildUploadedS3Key preserves the existing folder structure while using the
// standard uploaded filename format for the final object name.
func BuildUploadedS3Key(folder, subject, originalFilename, uploadedBy string, uploadedAt time.Time) string {
	storedFileName := BuildUploadedFilename(originalFilename, uploadedBy, uploadedAt)
	return BuildNamedS3Key(folder, subject, storedFileName)
}

func ContentHashHex(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func storagePrefixEnvVar(module string) string {
	moduleKey := strings.ToLower(strings.TrimSpace(module))
	switch moduleKey {
	case "bankbalance":
		return "BANK_BALANCE_S3_PREFIX"
	case "bankstatement":
		return "BANK_STATEMENT_S3_PREFIX"
	case "projection":
		return "PROJECTION_S3_PREFIX"
	case "payables", "receivables":
		return "TRANSACTION_S3_PREFIX"
	}

	normalized := strings.NewReplacer(" ", "_", "-", "_").Replace(moduleKey)
	return strings.ToUpper(normalized) + "_S3_PREFIX"
}

func moduleDefaultPrefix(module string) string {
	switch strings.ToLower(strings.TrimSpace(module)) {
	case "bankstatement":
		return "cash/bankstatements/"
	case "bankbalance":
		return "cash/bank-balance/"
	case "rule-master":
		return "cash/rule-master/"
	case "entitylogo":
		return "Entity logo/"
	case "projection":
		return "cash/projections/"
	case "payables":
		return "cash/transactions/payables/"
	case "receivables":
		return "cash/transactions/receivables/"
	case "fx-exposure":
		return "fx/exposures/"
	case "fx-exposure-bucketing":
		return "fx/exposures/exposure bucketing/"
	case "fx-pending-exposure-bucketing":
		return "fx/exposures/pending exposure bucketing/"
	case "fx-forward":
		return "fx/forwards/"
	case "fx-mtm":
		return "fx/mtm/"
	case "fund-planning":
		return "cash/fund-planning/"
	case "limit-utilization":
		return "cash/limit utilization/"
	case "limit-position":
		return "cash/limit position/"
	case "sweep-planning":
		return "cash/sweep-planning/"
	case "sweep-initiation":
		return "cash/sweep-initiation/"
	case "investment-onboarding":
		return "investment/onboarding center/"
	case "investment-confirmation":
		return "investment/confirmation/"
	case "investment-fvo":
		return "investment/financial closing work bench/"
	// allMasters
	case "master-bank":
		return "masters/bank/"
	case "master-currency":
		return "masters/currency/"
	case "master-bank-account":
		return "masters/bank-account/"
	case "master-counterparty":
		return "masters/counterparty/"
	case "master-gl-account":
		return "masters/gl-account/"
	case "master-cashflow-category":
		return "masters/cashflow-category/"
	case "master-costprofit-center":
		return "masters/costprofit-center/"
	case "master-payable-receivable":
		return "masters/payable-receivable/"
	case "master-entity-cash":
		return "masters/entity-cash/"
	case "master-entity":
		return "masters/entity/"
	// investmentMasters
	case "master-amc":
		return "masters/amc/"
	case "master-scheme":
		return "masters/scheme/"
	case "master-dp":
		return "masters/dp/"
	case "master-demat":
		return "masters/demat/"
	case "master-folio":
		return "masters/folio/"
	case "master-interest-type":
		return "masters/interest-type/"
	case "master-penalty-structure":
		return "masters/penalty-structure/"
	case "master-compounding-frequency":
		return "masters/compounding-frequency/"
	case "master-tds-plan":
		return "masters/tds-plan/"
	case "master-calendar":
		return "masters/calendar/"
	case "master-day-count-convention":
		return "masters/day-count-convention/"
	case "master-bank-config":
		return "masters/bank-config/"
	case "master-bank-rate-card":
		return "masters/bank-rate-card/"
	default:
		return ""
	}
}

func normalizePrefix(p string) string {
	return ensureTrailingSlash(strings.Trim(strings.TrimSpace(p), "/"))
}

func transactionModulePrefix(base, module string) string {
	base = strings.Trim(strings.TrimSpace(base), "/")
	moduleKey := strings.ToLower(strings.TrimSpace(module))
	if base == "" {
		return moduleDefaultPrefix(moduleKey)
	}
	if moduleKey != "payables" && moduleKey != "receivables" {
		return ensureTrailingSlash(base)
	}

	lowerBase := strings.ToLower(base)
	if lowerBase == moduleKey || strings.HasSuffix(lowerBase, "/"+moduleKey) {
		return ensureTrailingSlash(base)
	}

	return ensureTrailingSlash(base + "/" + moduleKey)
}

// GetStoragePrefix returns the S3 folder prefix for a given module.
func GetStoragePrefix(module string) string {
	moduleKey := strings.ToLower(strings.TrimSpace(module))
	if prefix := moduleDefaultPrefix(moduleKey); prefix != "" {
		return prefix
	}
	return modulePrefix(moduleKey)
}

// BuildS3Key builds an S3 key with a custom folder prefix.
// If folder is empty, file goes to root of bucket.
func BuildS3Key(folder, subject, fileHash, fileExt string) string {
	folder = strings.Trim(strings.TrimSpace(folder), "/")
	if folder != "" {
		folder = folder + "/"
	}
	ext := strings.TrimSpace(fileExt)
	if ext == "" {
		ext = ".bin"
	}
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	subjectSafe := sanitizePathSegment(subject)
	if subjectSafe == "unknown" {
		return fmt.Sprintf("%s%s%s", folder, fileHash, ext)
	}
	return fmt.Sprintf("%s%s/%s%s", folder, subjectSafe, fileHash, ext)
}

// BuildNamedS3Key builds an S3 key using the provided object name while
// preserving the existing folder and subject structure.
func BuildNamedS3Key(folder, subject, objectName string) string {
	folder = strings.Trim(strings.TrimSpace(folder), "/")
	if folder != "" {
		folder = folder + "/"
	}
	name := safeObjectName(objectName)
	subjectSafe := sanitizePathSegment(subject)
	if subjectSafe == "unknown" {
		return folder + name
	}
	return fmt.Sprintf("%s%s/%s", folder, subjectSafe, name)
}

// BuildModuleS3Key builds an S3 object key under the module's folder.
func BuildModuleS3Key(module, subject, fileHash, fileExt string) string {
	prefix := GetStoragePrefix(module)
	ext := strings.TrimSpace(fileExt)
	if ext == "" {
		ext = ".bin"
	}
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	subjectSafe := sanitizePathSegment(subject)
	return fmt.Sprintf("%s%s/%s%s", prefix, subjectSafe, fileHash, ext)
}

// IsS3UploadEnabled reads env var BANK_STMT_S3_ENABLED to determine whether to
// upload files to S3. Defaults to true when unset.
func IsS3UploadEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("BANK_STMT_S3_ENABLED")))
	if v == "" {
		return true
	}
	return v == "1" || v == "true" || v == "yes"
}

func DetectContentType(data []byte) string {
	if len(data) == 0 {
		return "application/octet-stream"
	}
	if len(data) > 512 {
		return http.DetectContentType(data[:512])
	}
	return http.DetectContentType(data)
}

func contentTypeFromExtension(key string, detected string) string {
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(key)))
	switch ext {
	case ".csv":
		return "text/csv"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".json":
		return constants.ContentTypeJSON
	case ".txt":
		return "text/plain"
	case ".pdf":
		return "application/pdf"
	default:
		if strings.TrimSpace(detected) == "" {
			return "application/octet-stream"
		}
		return detected
	}
}

func contentDispositionForKey(key string) string {
	name := safeObjectName(key)
	return fmt.Sprintf("attachment; filename=%q", name)
}

func newS3Client(ctx context.Context) (*s3.Client, string, error) {
	bucket := storageBucket()
	region := storageRegion()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, "", fmt.Errorf("load AWS config: %w", err)
	}
	return s3.NewFromConfig(cfg), bucket, nil
}

func presignExpiry(expiry time.Duration) time.Duration {
	expiryDuration := expiry
	if expiryDuration <= 0 {
		expiryDuration = 7 * 24 * time.Hour
		if envExpiry := strings.TrimSpace(os.Getenv("BANK_STMT_URL_EXPIRY_HOURS")); envExpiry != "" {
			if hours, parseErr := strconv.Atoi(envExpiry); parseErr == nil && hours > 0 {
				expiryDuration = time.Duration(hours) * time.Hour
			}
		}
	}
	max := 7 * 24 * time.Hour
	if expiryDuration > max {
		expiryDuration = max
	}
	return expiryDuration
}

func PutObjectToS3(ctx context.Context, key string, body []byte, contentType string) error {
	client, bucket, err := newS3Client(ctx)
	if err != nil {
		return err
	}
	contentType = contentTypeFromExtension(key, contentType)
	contentDisposition := contentDispositionForKey(key)
	metadata := map[string]string{
		contentHashMetadataKey: ContentHashHex(body),
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:             aws.String(bucket),
		Key:                aws.String(key),
		Body:               bytes.NewReader(body),
		ContentType:        aws.String(contentType),
		ContentDisposition: aws.String(contentDisposition),
		Metadata:           metadata,
	})
	if err != nil {
		return fmt.Errorf("upload to s3 (bucket %s, key %s): %w", bucket, key, err)
	}
	return nil
}

func UploadFirstMultipartFile(ctx context.Context, module, uploadedBy string, headers []*multipart.FileHeader) (string, error) {
	if len(headers) == 0 || headers[0] == nil {
		return "", nil
	}

	header := headers[0]
	file, err := header.Open()
	if err != nil {
		return "", fmt.Errorf("open upload file: %w", err)
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("read upload file: %w", err)
	}

	uploadedAt := time.Now().UTC()
	s3Key := BuildUploadedS3Key(GetStoragePrefix(module), "", header.Filename, uploadedBy, uploadedAt)
	if err := PutObjectToS3(ctx, s3Key, body, DetectContentType(body)); err != nil {
		return "", err
	}

	return s3Key, nil
}

func CollectMultipartFiles(form *multipart.Form, keys ...string) []*multipart.FileHeader {
	if form == nil {
		return nil
	}
	files := make([]*multipart.FileHeader, 0)
	for _, key := range keys {
		files = append(files, form.File[key]...)
	}
	return files
}

func isMissingObjectError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := strings.TrimSpace(apiErr.ErrorCode())
		return code == "NotFound" || code == "NoSuchKey"
	}
	return false
}

func objectContentHash(ctx context.Context, key string) (string, error) {
	client, bucket, err := newS3Client(ctx)
	if err != nil {
		return "", err
	}
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	if head.Metadata != nil {
		if hash := strings.TrimSpace(head.Metadata[contentHashMetadataKey]); hash != "" {
			return hash, nil
		}
	}
	obj, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	defer obj.Body.Close()
	body, err := io.ReadAll(obj.Body)
	if err != nil {
		return "", fmt.Errorf("read s3 object %s from bucket %s: %w", key, bucket, err)
	}
	return ContentHashHex(body), nil
}

func FindDuplicateObjectKey(ctx context.Context, body []byte, keys []string) (string, error) {
	expectedHash := ContentHashHex(body)
	seen := make(map[string]struct{}, len(keys))
	for _, rawKey := range keys {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		hash, err := objectContentHash(ctx, key)
		if err != nil {
			if isMissingObjectError(err) {
				continue
			}
			return "", err
		}
		if hash == expectedHash {
			return key, nil
		}
	}
	return "", nil
}

func GetDownloadPresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	client, bucket, err := newS3Client(ctx)
	if err != nil {
		return "", err
	}
	contentType := contentTypeFromExtension(key, "")
	contentDisposition := contentDispositionForKey(key)
	presignClient := s3.NewPresignClient(client)
	presignedReq, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket:                     aws.String(bucket),
		Key:                        aws.String(key),
		ResponseContentType:        aws.String(contentType),
		ResponseContentDisposition: aws.String(contentDisposition),
	}, s3.WithPresignExpires(presignExpiry(expiry)))
	if err != nil {
		return "", fmt.Errorf("failed to generate pre-signed URL: %w", err)
	}

	return presignedReq.URL, nil
}

func GetInlinePresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	client, bucket, err := newS3Client(ctx)
	if err != nil {
		return "", err
	}
	contentType := contentTypeFromExtension(key, "")
	contentDisposition := fmt.Sprintf("inline; filename=%q", safeObjectName(key))
	presignClient := s3.NewPresignClient(client)
	presignedReq, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket:                     aws.String(bucket),
		Key:                        aws.String(key),
		ResponseContentType:        aws.String(contentType),
		ResponseContentDisposition: aws.String(contentDisposition),
	}, s3.WithPresignExpires(presignExpiry(expiry)))
	if err != nil {
		return "", fmt.Errorf("failed to generate inline pre-signed URL: %w", err)
	}

	return presignedReq.URL, nil
}

// UploadToS3 is kept as a temporary compatibility wrapper during migration.
func UploadToS3(ctx context.Context, key string, body []byte, contentType string) (string, error) {
	if err := PutObjectToS3(ctx, key, body, contentType); err != nil {
		return "", err
	}
	return GetDownloadPresignedURL(ctx, key, 0)
}

func DeleteFromS3(ctx context.Context, key string) error {
	client, bucket, err := newS3Client(ctx)
	if err != nil {
		return err
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("delete s3 object %s from bucket %s: %w", key, bucket, err)
	}
	return nil
}
