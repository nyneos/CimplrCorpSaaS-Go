package dbutil

import (
	"os"
	"strings"
)

// EffectiveSSLMode returns DB_SSLMODE when set; otherwise "require" for AWS RDS
// (RDS rejects non-TLS clients with pg_hba / "no encryption") and "disable" for
// typical local Postgres.
func EffectiveSSLMode(host string) string {
	if m := strings.TrimSpace(os.Getenv("DB_SSLMODE")); m != "" {
		return m
	}
	h := strings.ToLower(strings.TrimSpace(host))
	if strings.Contains(h, "rds.amazonaws.com") {
		return "require"
	}
	return "disable"
}
