package dbutil

import (
	"net"
	"net/url"
	"os"
	"strings"
)

// PostgresDSN builds a libpq URL with URL-escaped credentials and
// EffectiveSSLMode (default require). Extra query params may be supplied.
func PostgresDSN(extra ...url.Values) string {
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	return BuildPostgresDSN(user, pass, host, port, name, EffectiveSSLMode(host), extra...)
}

// BuildPostgresDSN encodes user/password via url.UserPassword and sets sslmode.
func BuildPostgresDSN(user, pass, host, port, name, sslMode string, extra ...url.Values) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, pass),
		Host:   net.JoinHostPort(host, port),
		Path:   "/" + name,
	}
	q := url.Values{}
	if strings.TrimSpace(sslMode) == "" {
		sslMode = "require"
	}
	q.Set("sslmode", sslMode)
	for _, e := range extra {
		for k, vs := range e {
			for _, v := range vs {
				q.Set(k, v)
			}
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}
