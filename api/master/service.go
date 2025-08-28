package master

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
)

type MasterService struct {
	config map[string]interface{}
	db     *sql.DB
}

func NewMasterService(cfg map[string]interface{}, db *sql.DB) serviceiface.Service {
	return &MasterService{config: cfg, db: db}
}

func (s *MasterService) Name() string {
	return "master"
}

func (s *MasterService) Start() error {
	go StartMasterService(s.db)
	return nil
}

func (s *MasterService) Stop() error {
	// Implement stop logic if needed
	return nil
}
