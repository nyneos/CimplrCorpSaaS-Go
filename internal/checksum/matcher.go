package checksum

import (
    "crypto/sha256"
    "encoding/hex"
    "errors"
)

// ChecksumMatcher verifies the integrity of deployed code against licenses.
type ChecksumMatcher struct {
    expectedChecksum string
}

// NewChecksumMatcher creates a new ChecksumMatcher with the expected checksum.
func NewChecksumMatcher(expectedChecksum string) *ChecksumMatcher {
    return &ChecksumMatcher{expectedChecksum: expectedChecksum}
}

// Match checks if the provided data's checksum matches the expected checksum.
func (cm *ChecksumMatcher) Match(data []byte) (bool, error) {
    if cm.expectedChecksum == "" {
        return false, errors.New("expected checksum is not set")
    }

    hash := sha256.New()
    hash.Write(data)
    computedChecksum := hex.EncodeToString(hash.Sum(nil))

    return computedChecksum == cm.expectedChecksum, nil
}