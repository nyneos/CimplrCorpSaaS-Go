package investment

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"

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
	port := "7143"
	if s.cfg != nil {
		if v, ok := s.cfg["port"]; ok {
			switch t := v.(type) {
			case string:
				port = t
			case int:
				port = fmt.Sprintf("%d", t)
			case float64:
				port = fmt.Sprintf("%.0f", t)
			}
		}
	}
	go StartInvestmentService(s.pool, s.db, port)
	return nil
}

func (s *InvestmentService) Stop() error {
	return shutdownInvestmentService()
}
