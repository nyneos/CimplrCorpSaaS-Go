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

// buildModuleS3Key builds an S3 object key under the module's folder.
func buildModuleS3Key(module, subject, fileHash, fileExt string) string {
	prefix := modulePrefix(module)
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
func isS3UploadEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("BANK_STMT_S3_ENABLED")))
	if v == "" {
		return true
	}
	return v == "1" || v == "true" || v == "yes"
}

func detectContentType(data []byte) string {
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

func uploadToS3(ctx context.Context, key string, body []byte, contentType string) (string, error) {
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
