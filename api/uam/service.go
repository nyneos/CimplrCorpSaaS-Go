package uam

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
)

type UAMService struct {
	config map[string]interface{}
	db     *sql.DB
}

func NewUAMService(cfg map[string]interface{}, db *sql.DB) serviceiface.Service {
	return &UAMService{config: cfg, db: db}
}

func (s *UAMService) Name() string {
	return "uam"
}

func (s *UAMService) Start() error {
	go StartUAMService(s.db)
	return nil
}

func (s *UAMService) Stop() error {
	// Implement stop logic if needed
	return nil
}
