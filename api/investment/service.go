package investment

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"

	"github.com/jackc/pgx/v5/pgxpool"
)

// InvestmentService is a lightweight service wrapper for investment-related workers
type InvestmentService struct {
	cfg  map[string]interface{}
	pool *pgxpool.Pool
	db   *sql.DB
}

// NewInvestmentService constructs an InvestmentService and accepts a pgx pool instance and sql.DB.
func NewInvestmentService(cfg map[string]interface{}, pool *pgxpool.Pool, db *sql.DB) serviceiface.Service {
	return &InvestmentService{cfg: cfg, pool: pool, db: db}
}

func (s *InvestmentService) Name() string {
	return "investment"
}

func (s *InvestmentService) Start() error {
	go StartInvestmentService(s.pool, s.db)
	return nil
}

func (s *InvestmentService) Stop() error {
	// Implement stop logic if needed in the future
	return nil
}
