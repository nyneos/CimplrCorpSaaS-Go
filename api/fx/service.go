package fx

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
)

type FXService struct {
	config map[string]interface{}
	db     *sql.DB
}

func NewFXService(cfg map[string]interface{}, db *sql.DB) serviceiface.Service {
	return &FXService{config: cfg, db: db}
}

func (s *FXService) Name() string {
	return "fx"
}

func (s *FXService) Start() error {
	go StartFXService(s.db)
	return nil
}

func (s *FXService) Stop() error {
	// Implement stop logic if needed
	return nil
}
