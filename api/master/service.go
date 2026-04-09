package master

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MasterService struct {
	config map[string]interface{}
	db     *sql.DB
	pool   *pgxpool.Pool
}

func NewMasterService(cfg map[string]interface{}, pool *pgxpool.Pool, db *sql.DB) serviceiface.Service {
	return &MasterService{config: cfg, db: db, pool: pool}
}

func (s *MasterService) Name() string {
	return "master"
}

func (s *MasterService) Start() error {
	port := "2143"
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
	go StartMasterService(s.pool, s.db, port)
	return nil
}

func (s *MasterService) Stop() error {
	// Implement stop logic if needed
	return nil
}
