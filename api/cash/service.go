package cash

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CashService struct {
	config map[string]interface{}
	db     *sql.DB
	pool   *pgxpool.Pool
}

func NewCashService(cfg map[string]interface{}, pool *pgxpool.Pool, db *sql.DB) serviceiface.Service {
	return &CashService{config: cfg, db: db, pool: pool}
}

func (s *CashService) Name() string {
	return "cash"
}

func (s *CashService) Start() error {
	port := "6143"
	if s.config != nil {
		if v, ok := s.config["port"]; ok {
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
	go StartCashService(s.pool, s.db, port)
	return nil
}

func (s *CashService) Stop() error {
	// Implement stop logic if needed
	return nil
}
