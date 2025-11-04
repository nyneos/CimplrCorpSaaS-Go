package investment

import (
	"CimplrCorpSaas/internal/serviceiface"

	"github.com/jackc/pgx/v5/pgxpool"
)

// InvestmentService is a lightweight service wrapper for investment-related workers
type InvestmentService struct {
	cfg  map[string]interface{}
	pool *pgxpool.Pool
}

// NewInvestmentService constructs an InvestmentService and accepts a pgx pool instance.
func NewInvestmentService(cfg map[string]interface{}, pool *pgxpool.Pool) serviceiface.Service {
	return &InvestmentService{cfg: cfg, pool: pool}
}

func (s *InvestmentService) Name() string {
	return "investment"
}

func (s *InvestmentService) Start() error {
	go StartInvestmentService(s.pool)
	return nil
}

func (s *InvestmentService) Stop() error {
	// Implement stop logic if needed in the future
	return nil
}
