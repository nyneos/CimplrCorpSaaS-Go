package auth

import (
	"context"
	"database/sql"
	"time"

	"CimplrCorpSaas/internal/logger"
)

// LogSecurityEvent writes a security-relevant event to both the audit_logs table
// and the file-based logger. Non-blocking: errors are logged but never returned.
func LogSecurityEvent(db *sql.DB, userID, eventType, detail, clientIP string) {
	// DB insert (best-effort)
	if db != nil {
		_, err := db.ExecContext(context.Background(),
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
