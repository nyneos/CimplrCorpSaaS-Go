package bankstatement

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	storageDefaultBucket  = "cimplr"
	storageDefaultPrefix  = "bankstatements/"
	storageDefaultRegion  = "ap-south-1"
	storageDefaultBaseURL = "https://cimplr.s3.ap-south-1.amazonaws.com/"
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
	case "projection":
		return "cash/projections/"
	case "payables":
		return "cash/transactions/payables/"
	case "receivables":
		return "cash/transactions/receivables/"
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
	return fmt.Sprintf("%s%s/%s%s", folder, subjectSafe, fileHash, ext)
}

// buildModuleS3Key builds an S3 object key under the module's folder.
// Kept for backward compatibility with existing callers.
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

// isS3UploadEnabled reads env var BANK_STMT_S3_ENABLED to determine whether to
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
		return "application/json"
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
	name := strings.TrimSpace(filepath.Base(key))
	if name == "" || name == "." || name == "/" {
		name = "download.bin"
	}
	// Use attachment so browsers save with the correct extension instead of guessing.
	return fmt.Sprintf("attachment; filename=%q", name)
}

func UploadToS3(ctx context.Context, key string, body []byte, contentType string) (string, error) {
	bucket := storageBucket()
	region := storageRegion()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return "", fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	contentType = contentTypeFromExtension(key, contentType)
	contentDisposition := contentDispositionForKey(key)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:             aws.String(bucket),
		Key:                aws.String(key),
		Body:               bytes.NewReader(body),
		ContentType:        aws.String(contentType),
		ContentDisposition: aws.String(contentDisposition),
	})
	if err != nil {
		return "", fmt.Errorf("upload to s3 (bucket %s, key %s): %w", bucket, key, err)
	}

	// Generate pre-signed URL. AWS SigV4 presigned URLs max at 7 days; we clamp to that.
	presignClient := s3.NewPresignClient(client)
	expiryDuration := 7 * 24 * time.Hour // 7 days (AWS limit for presign)
	if envExpiry := strings.TrimSpace(os.Getenv("BANK_STMT_URL_EXPIRY_HOURS")); envExpiry != "" {
		if hours, parseErr := strconv.Atoi(envExpiry); parseErr == nil && hours > 0 {
			expiryDuration = time.Duration(hours) * time.Hour
			max := 7 * 24 * time.Hour
			if expiryDuration > max {
				expiryDuration = max
			}
		}
	}

	presignedReq, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket:                     aws.String(bucket),
		Key:                        aws.String(key),
		ResponseContentType:        aws.String(contentType),
		ResponseContentDisposition: aws.String(contentDisposition),
	}, s3.WithPresignExpires(expiryDuration))
	if err != nil {
		return "", fmt.Errorf("failed to generate pre-signed URL: %w", err)
	}

	return presignedReq.URL, nil
}

func DeleteFromS3(ctx context.Context, key string) error {
	bucket := storageBucket()
	region := storageRegion()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("delete s3 object %s from bucket %s: %w", key, bucket, err)
	}
	return nil
}
