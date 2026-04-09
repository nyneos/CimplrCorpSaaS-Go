package uam

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type UAMService struct {
	config map[string]interface{}
	db     *sql.DB
	pool   *pgxpool.Pool
}

func NewUAMService(cfg map[string]interface{}, pool *pgxpool.Pool, db *sql.DB) serviceiface.Service {
	return &UAMService{config: cfg, db: db, pool: pool}
}

func (s *UAMService) Name() string {
	return "uam"
}

func (s *UAMService) Start() error {
	port := "5143"
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
	go StartUAMService(s.pool, s.db, port)
	return nil
}

func (s *UAMService) Stop() error {
	// Implement stop logic if needed
	return nil
}
