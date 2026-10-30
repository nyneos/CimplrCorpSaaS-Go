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
	return strings.EqualFold(strings.TrimSpace(os.Getenv("AUTH_REQUIRE_SESSION_TOKEN")), "true")
}

// callerSession resolves the logged-in session making the request. With a
// session header the header decides; otherwise the body user_id is used unless
// the token is required.
func callerSession(r *http.Request, bodyUserID string) *UserSession {
	if sid := strings.TrimSpace(r.Header.Get(headerSessionID)); sid != "" {
		s := SessionByID(sid)
		if !s.LoggedIn() {
			return nil
		}
		return s
	}
	if requireSessionToken() {
		return nil
	}
	return LoggedInSessionForUser(strings.TrimSpace(bodyUserID))
}

// pendingSession resolves the MFA-pending session for verify: by mfa_token when
// sent, else by user_id unless the token is required.
func pendingSession(mfaToken, userID string) *UserSession {
	if globalAuthService == nil {
		return nil
	}
	a := globalAuthService
	a.mu.Lock()
	defer a.mu.Unlock()
	if t := strings.TrimSpace(mfaToken); t != "" {
		return a.users[t]
	}
	if requireSessionToken() {
		return nil
	}
	return a.userPointers[strings.TrimSpace(userID)]
}

// checkTOTP validates a code once per time step with attempt limiting.
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

// sealMFASecret encrypts the TOTP secret when MFA_SECRET_KEY is configured.
func sealMFASecret(secret string) (string, error) {
	key := mfaSecretKey()
	if key == nil {
		return secret, nil
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

// openMFASecret returns the plain TOTP secret for a stored value.
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
