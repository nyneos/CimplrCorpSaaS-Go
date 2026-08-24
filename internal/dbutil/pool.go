package dbutil

import (
	"context"

	// "CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewTracedPool(ctx context.Context, dsn, service string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	// cfg.ConnConfig.Tracer = logger.NewDBTracer(service)
	return pgxpool.NewWithConfig(ctx, cfg)
}
