package dash

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DashService struct {
	config map[string]interface{}
	db     *sql.DB
	pool   *pgxpool.Pool
}

func NewDashService(cfg map[string]interface{}, pool *pgxpool.Pool, db *sql.DB) serviceiface.Service {
	return &DashService{config: cfg, db: db, pool: pool}
}

func (s *DashService) Name() string {
	return "dash"
}

func (s *DashService) Start() error {
	port := "4143"
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
	go StartDashService(s.pool, s.db, port)
	return nil
}

func (s *DashService) Stop() error {
	// Implement stop logic if needed
	return nil
}
