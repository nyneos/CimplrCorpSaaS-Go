package dash

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
)


type DashService struct {
	config map[string]interface{}
	db     *sql.DB
}

func NewDashService(cfg map[string]interface{}, db *sql.DB) serviceiface.Service {
	return &DashService{config: cfg, db: db}
}

func (s *DashService) Name() string {
	return "dash"
}

func (s *DashService) Start() error {
	go StartDashService(s.db)
	return nil
}

func (s *DashService) Stop() error {
	// Implement stop logic if needed
	return nil
}
