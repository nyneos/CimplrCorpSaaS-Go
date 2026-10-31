package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	mfaMaxAttempts   = 5
	mfaLockDuration  = 15 * time.Minute
	mfaSecretPrefix  = "enc1:"
	headerSessionID  = "X-Session-Id"
	mfaSecretKeyName = "MFA_SECRET_KEY"
)

type mfaAttemptState struct {
	failures    int
	lockedUntil time.Time
	lastCounter int64
}

var (
	mfaAttemptsMu sync.Mutex
	mfaAttempts   = map[string]*mfaAttemptState{}
)

var errMFALocked = errors.New("too many invalid codes, try again later")

func requireSessionToken() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("AUTH_REQUIRE_SESSION_TOKEN"))) {
	case "false", "0", "no", "off":
		return false
	default:
		return true
	}
}

func callerSession(r *http.Request, _ string) *UserSession {
	sid := strings.TrimSpace(r.Header.Get(headerSessionID))
	if sid == "" || (requireSessionToken() && sid == "") {
		return nil
	}
	s := SessionByID(sid)
	if !s.LoggedIn() {
		return nil
	}
	return s
}

func pendingSession(mfaToken, _ string) *UserSession {
	if globalAuthService == nil {
		return nil
	}
	t := strings.TrimSpace(mfaToken)
	if t == "" {
		return nil
	}
	a := globalAuthService
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.users[t]
}

func checkTOTP(userID, secret, code string) (bool, error) {
	mfaAttemptsMu.Lock()
	st := mfaAttempts[userID]
	if st == nil {
		st = &mfaAttemptState{}
		mfaAttempts[userID] = st
	}
	if time.Now().Before(st.lockedUntil) {
		mfaAttemptsMu.Unlock()
		return false, errMFALocked
	}
	mfaAttemptsMu.Unlock()

	counter, ok := matchTOTP(secret, strings.TrimSpace(code))

	mfaAttemptsMu.Lock()
	defer mfaAttemptsMu.Unlock()
	if !ok || counter <= st.lastCounter {
		st.failures++
		if st.failures >= mfaMaxAttempts {
			st.failures = 0
			st.lockedUntil = time.Now().Add(mfaLockDuration)
			return false, errMFALocked
		}
		return false, nil
	}
	st.failures = 0
	st.lastCounter = counter
	return true, nil
}

func matchTOTP(secret, code string) (int64, bool) {
	if len(code) != totpDigits {
		return 0, false
	}
	now := time.Now().Unix() / totpPeriod
	for i := -totpSkew; i <= totpSkew; i++ {
		t := now + int64(i)
		if hmac.Equal([]byte(generateTOTPCode(secret, t)), []byte(code)) {
			return t, true
		}
	}
	return 0, false
}

func mfaSecretKey() []byte {
	raw := strings.TrimSpace(os.Getenv(mfaSecretKeyName))
	if raw == "" {
		return nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(key) != 32 {
		return nil
	}
	return key
}

// mfaSecretKeyConfigured is true when MFA_SECRET_KEY is present and is a valid
// 32-byte base64 key. Setup and verify fail closed when this is false.
func mfaSecretKeyConfigured() bool {
	return mfaSecretKey() != nil
}

func refuseMFASecretKeyMissing(w http.ResponseWriter) {
	authEnvelopeError(w, http.StatusServiceUnavailable,
		"MFA is unavailable: MFA_SECRET_KEY is not configured",
		"MFA_SECRET_KEY_MISSING")
}

func sealMFASecret(secret string) (string, error) {
	key := mfaSecretKey()
	if key == nil {
		return "", errors.New("mfa secret key not configured")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, []byte(secret), nil)
	return mfaSecretPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

func openMFASecret(stored string) (string, error) {
	if !strings.HasPrefix(stored, mfaSecretPrefix) {
		return stored, nil
	}
	key := mfaSecretKey()
	if key == nil {
		return "", errors.New("mfa secret key not configured")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, mfaSecretPrefix))
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", errors.New("invalid mfa secret")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
