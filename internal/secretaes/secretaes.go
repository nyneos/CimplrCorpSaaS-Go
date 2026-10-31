package secretaes

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

const prefix = "enc1:"

var (
	keyOnce     sync.Once
	cachedKey   []byte
	keyErr      error
	warnOnce    sync.Once
	keyMissing  bool
)

// Seal encrypts plaintext with AES-GCM when SECRETS_ENC_KEY is set (base64 32-byte key).
// Returns "enc1:" + base64(nonce|ciphertext). Empty input stays empty.
// If the key is unset, returns plaintext unchanged and logs a one-time warning.
func Seal(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	if strings.HasPrefix(plaintext, prefix) {
		return plaintext, nil
	}
	key, err := loadKey()
	if err != nil {
		return "", err
	}
	if key == nil {
		warnPlaintextOnce()
		return plaintext, nil
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("secretaes: cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("secretaes: gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("secretaes: nonce: %w", err)
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return prefix + base64.StdEncoding.EncodeToString(sealed), nil
}

// Open decrypts Seal output. Values without the enc1: prefix are treated as
// legacy plaintext. Empty input stays empty. Missing key with ciphertext fails.
func Open(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, prefix) {
		return value, nil
	}
	key, err := loadKey()
	if err != nil {
		return "", err
	}
	if key == nil {
		return "", fmt.Errorf("secretaes: ciphertext present but SECRETS_ENC_KEY is unset")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, prefix))
	if err != nil {
		return "", fmt.Errorf("secretaes: decode: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("secretaes: cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("secretaes: gcm: %w", err)
	}
	if len(raw) < gcm.NonceSize() {
		return "", fmt.Errorf("secretaes: ciphertext too short")
	}
	nonce, ct := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", fmt.Errorf("secretaes: decrypt: %w", err)
	}
	return string(pt), nil
}

// MustOpen is Open that returns the input unchanged on decrypt failure after logging.
// Prefer Open when the caller can surface errors; use MustOpen for poller best-effort paths.
func MustOpen(value string) string {
	out, err := Open(value)
	if err != nil {
		log.Printf("secretaes: open failed: %v", err)
		return ""
	}
	return out
}

func loadKey() ([]byte, error) {
	keyOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv("SECRETS_ENC_KEY"))
		if raw == "" {
			keyMissing = true
			return
		}
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			keyErr = fmt.Errorf("secretaes: SECRETS_ENC_KEY must be base64: %w", err)
			return
		}
		if len(decoded) != 32 {
			keyErr = fmt.Errorf("secretaes: SECRETS_ENC_KEY must decode to 32 bytes, got %d", len(decoded))
			return
		}
		cachedKey = decoded
	})
	if keyErr != nil {
		return nil, keyErr
	}
	if keyMissing {
		return nil, nil
	}
	return cachedKey, nil
}

func warnPlaintextOnce() {
	warnOnce.Do(func() {
		log.Printf("secretaes: SECRETS_ENC_KEY unset — storing credentials as plaintext (backward compatible)")
	})
}
