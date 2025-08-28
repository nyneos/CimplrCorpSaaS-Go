package cash

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
)

type CashService struct {
	config map[string]interface{}
	db     *sql.DB
}

func NewCashService(cfg map[string]interface{}, db *sql.DB) serviceiface.Service {
	return &CashService{config: cfg, db: db}
}

func (s *CashService) Name() string {
	return "cash"
}

func (s *CashService) Start() error {
	go StartCashService(s.db)
	return nil
}

func (s *CashService) Stop() error {
	// Implement stop logic if needed
	return nil
}
