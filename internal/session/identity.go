package session

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const HeaderSessionID = "X-Session-Id"

var (
	ErrInvalidSession  = errors.New(constants.ErrInvalidSession)
	ErrSessionRequired = errors.New(constants.ErrInvalidSession)
	ErrUserIDRequired  = errors.New(constants.ErrUserIDRequired)
)

func RequireToken() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("AUTH_REQUIRE_SESSION_TOKEN"))) {
	case "false", "0", "no", "off":
		return false
	default:
		return true
	}
}

func HeaderSession(r *http.Request) (s *auth.UserSession, present bool) {
	sid := strings.TrimSpace(r.Header.Get(HeaderSessionID))
	if sid == "" {
		return nil, false
	}
	s = auth.SessionByID(sid)
	if !s.LoggedIn() {
		return nil, true
	}
	return s, true
}

func Identify(r *http.Request, body []byte) (*auth.UserSession, []byte, error) {
	s, present := HeaderSession(r)
	if !present && RequireToken() {
		return nil, body, ErrSessionRequired
	}
	if !present {
		return nil, body, ErrSessionRequired
	}
	if s == nil {
		return nil, body, ErrInvalidSession
	}
	_ = ClaimedUserID(r, body)
	body = BindUserID(r, body, s.UserID)
	return s, body, nil
}

func ClaimedUserID(r *http.Request, body []byte) string {
	if id := jsonUserID(body); id != "" {
		return id
	}
	if isMultipart(r) {
		if r.MultipartForm == nil {
			if body != nil {
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
			_ = r.ParseMultipartForm(32 << 20)
			if body != nil {
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
		}
		if r.MultipartForm != nil {
			if v := r.MultipartForm.Value[constants.KeyUserID]; len(v) > 0 && v[0] != "" {
				return v[0]
			}
		}
	} else if body != nil && !looksJSON(body) && r.PostForm == nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		_ = r.ParseForm()
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	if r.PostForm != nil {
		if v := r.PostForm.Get(constants.KeyUserID); v != "" {
			return v
		}
	}
	return r.URL.Query().Get(constants.KeyUserID)
}

func BindUserID(r *http.Request, body []byte, userID string) []byte {
	q := r.URL.Query()
	if q.Has(constants.KeyUserID) {
		q.Set(constants.KeyUserID, userID)
		r.URL.RawQuery = q.Encode()
	}
	if r.Form != nil && r.Form.Has(constants.KeyUserID) {
		r.Form.Set(constants.KeyUserID, userID)
	}
	if r.PostForm != nil && r.PostForm.Has(constants.KeyUserID) {
		r.PostForm.Set(constants.KeyUserID, userID)
	}
	if r.MultipartForm != nil {
		if _, ok := r.MultipartForm.Value[constants.KeyUserID]; ok {
			r.MultipartForm.Value[constants.KeyUserID] = []string{userID}
		}
	}
	if !looksJSON(body) {
		return body
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	enc, _ := json.Marshal(userID)
	if cur, ok := m[constants.KeyUserID]; ok && bytes.Equal(cur, enc) {
		return body
	}
	m[constants.KeyUserID] = enc
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	r.ContentLength = int64(len(out))
	r.Header.Set("Content-Length", fmt.Sprint(len(out)))
	return out
}

func jsonUserID(body []byte) string {
	if !looksJSON(body) {
		return ""
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(body, &m) != nil {
		return ""
	}
	raw, ok := m[constants.KeyUserID]
	if !ok {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return strings.TrimSpace(s)
	}
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		return n.String()
	}
	return ""
}

func looksJSON(b []byte) bool {
	t := bytes.TrimSpace(b)
	return len(t) > 0 && t[0] == '{'
}

func isMultipart(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get(constants.ContentTypeText)), constants.ContentTypeMultipart)
}
