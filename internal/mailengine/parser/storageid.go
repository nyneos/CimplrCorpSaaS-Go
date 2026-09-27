package parser

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// StorageIDFromRawKey returns a deterministic id for parsed JSON + attachment keys
// derived from the canonical raw .eml S3 path (same mail → same id → idempotent S3 writes).
func StorageIDFromRawKey(s3RawKey string) string {
	s3RawKey = strings.TrimSpace(s3RawKey)
	if s3RawKey == "" {
		return newMessageID()
	}
	sum := sha256.Sum256([]byte(strings.ToLower(s3RawKey)))
	return hex.EncodeToString(sum[:16])
}
