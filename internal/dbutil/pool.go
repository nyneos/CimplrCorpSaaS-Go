package dbutil

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Default per-module pool size. Supabase free/small plans have ~90
// max_connections total (many reserved for Supabase internals). One
// AppManager process starts ~10 module servers, each with its own pool —
// uncapped defaults exhaust the DB and return:
//   FATAL: remaining connection slots are reserved for roles with the SUPERUSER attribute
const (
	defaultMaxConns        = int32(4)
	defaultMinConns        = int32(0)
	defaultMaxConnIdleTime = 2 * time.Minute
	defaultMaxConnLifetime = 30 * time.Minute
	defaultHealthCheck     = 1 * time.Minute
)

func NewTracedPool(ctx context.Context, dsn, service string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	applyPoolLimits(cfg, service)
	ApplyPoolerSafeQueryMode(cfg)
	return pgxpool.NewWithConfig(ctx, cfg)
}

func applyPoolLimits(cfg *pgxpool.Config, service string) {
	maxConns := defaultMaxConns
	if v := strings.TrimSpace(os.Getenv("DB_POOL_MAX_CONNS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConns = int32(n)
		}
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = defaultMinConns
	cfg.MaxConnIdleTime = defaultMaxConnIdleTime
	cfg.MaxConnLifetime = defaultMaxConnLifetime
	cfg.HealthCheckPeriod = defaultHealthCheck
	if service != "" {
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["application_name"] = "cimplr-" + service
	}
}

// ApplyPoolerSafeQueryMode is required for Supabase/PgBouncer transaction
// pooler (port 6543). Named prepared-statement caching yields SQLSTATE 42P05
// ("prepared statement already exists"). Startup GUCs such as
// statement_timeout yield SQLSTATE 08P01 ("unsupported startup parameter").
//
// Uses QueryExecModeExec — pgx's PgBouncer-safe extended protocol — not
// SimpleProtocol. SimpleProtocol inlines ''::date and 500s list/dashboard
// queries that pass empty date filters.
func ApplyPoolerSafeQueryMode(cfg *pgxpool.Config) {
	if cfg == nil {
		return
	}
	if !usesTransactionPooler(cfg) {
		return
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.ConnConfig.StatementCacheCapacity = 0
	cfg.ConnConfig.DescriptionCacheCapacity = 0
	if cfg.ConnConfig.RuntimeParams != nil {
		for _, key := range []string{
			"statement_timeout",
			"lock_timeout",
			"idle_in_transaction_session_timeout",
			"idle_session_timeout",
		} {
			delete(cfg.ConnConfig.RuntimeParams, key)
		}
	}
}

func usesTransactionPooler(cfg *pgxpool.Config) bool {
	// Only transaction pooling (6543) needs Exec mode. Session pooler / direct
	// Postgres on 5432 supports prepared statements — leave pgx defaults.
	return cfg.ConnConfig.Port == 6543
}
