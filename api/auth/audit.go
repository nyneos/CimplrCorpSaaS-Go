package auth

import (
	"context"
	"time"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LogSecurityEvent writes a security-relevant event to both the audit_logs table
// and the file-based logger. Non-blocking: errors are logged but never returned.
func LogSecurityEvent(ctx context.Context, pool *pgxpool.Pool, userID, eventType, detail, clientIP string) {
	// DB insert (best-effort)
	if pool != nil {
		_, err := pool.Exec(ctx,
			`INSERT INTO audit_logs (user_id, event_type, detail, ip_address, created_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			nullIfEmpty(userID), eventType, detail, clientIP, time.Now(),
		)
		if err != nil && logger.GlobalLogger != nil {
			logger.GlobalLogger.LogAudit("[AUDIT_DB_ERR] " + err.Error())
		}
	}

	// File logger
	if logger.GlobalLogger != nil {
		msg := "[SECURITY] " + eventType
		if userID != "" {
			msg += " user=" + userID
		}
		if detail != "" {
			msg += " detail=" + detail
		}
		if clientIP != "" {
			msg += " ip=" + clientIP
		}
		logger.GlobalLogger.LogAudit(msg)
	}
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
