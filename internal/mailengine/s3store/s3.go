package s3store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"

	"CimplrCorpSaas/internal/mailengine/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func bucket() string {
	if b := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_BUCKET")); b != "" {
		return b
	}
	return "cimplr"
}

func region() string {
	if r := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_REGION")); r != "" {
		return r
	}
	return "ap-south-1"
}

func rawPrefix() string {
	p := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_PREFIX"))
	if p == "" {
		p = "email/inbound/raw/"
	}
	return ensureSlash(p)
}

func parsedPrefix() string {
	p := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_PARSED_PREFIX"))
	if p == "" {
		p = "email/inbound/parsed/"
	}
	return ensureSlash(p)
}

func attachPrefix() string {
	p := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_ATTACH_PREFIX"))
	if p == "" {
		p = "email/inbound/attachments/"
	}
	return ensureSlash(p)
}

func ensureSlash(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if !strings.HasSuffix(s, "/") {
		return s + "/"
	}
	return s
}

func client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region()))
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

func GetObjectBytes(ctx context.Context, key string) ([]byte, error) {
	c, err := client(ctx)
	if err != nil {
		return nil, err
	}
	obj, err := c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket()),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}

func PutObject(ctx context.Context, key string, body []byte, contentType string) error {
	c, err := client(ctx)
	if err != nil {
		return err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	_, err = c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket()),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	return err
}

func PutParsedJSON(ctx context.Context, storageID string, parsed model.ParsedEmail) (string, error) {
	key := strings.TrimSuffix(parsedPrefix(), "/") + "/" + storageID + ".json"
	b, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return "", err
	}
	if err := PutObject(ctx, key, b, "application/json"); err != nil {
		return "", err
	}
	return key, nil
}

func attachmentObjectKey(attachPrefix, storageID, filename string, content []byte) string {
	safeName := filepath.Base(strings.TrimSpace(filename))
	if safeName == "" || safeName == "." {
		safeName = "attachment"
	}
	hash := sha256.Sum256(content)
	hashHex := hex.EncodeToString(hash[:])
	return strings.TrimSuffix(attachPrefix, "/") + "/" + storageID + "/" + hashHex[:16] + "_" + safeName
}

// PutAttachmentStable stores attachment bytes at a deterministic key (overwrite-safe).
func PutAttachmentStable(ctx context.Context, storageID, filename string, data []byte, contentType string) (string, string, error) {
	key := attachmentObjectKey(attachPrefix(), storageID, filename, data)
	hash := sha256.Sum256(data)
	hashHex := hex.EncodeToString(hash[:])
	if err := PutObject(ctx, key, data, contentType); err != nil {
		return "", "", err
	}
	return key, hashHex, nil
}

func PutAttachment(ctx context.Context, messageID, filename string, data []byte, contentType string) (string, string, error) {
	return PutAttachmentStable(ctx, messageID, filename, data, contentType)
}

func ListNewRawKeys(ctx context.Context, afterKey string, limit int32) ([]string, error) {
	c, err := client(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10000
	}

	prefix := rawPrefix()
	var keys []string
	var continuation *string

	for {
		out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket()),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuation,
			MaxKeys:           aws.Int32(100),
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			k := *obj.Key
			if !strings.HasSuffix(strings.ToLower(k), ".eml") {
				continue
			}
			if afterKey != "" && k <= afterKey {
				continue
			}
			keys = append(keys, k)
			if int32(len(keys)) >= limit {
				return keys, nil
			}
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		continuation = out.NextContinuationToken
	}

	return keys, nil
}

func RawPrefix() string { return rawPrefix() }
func Bucket() string    { return bucket() }
func Region() string    { return region() }
