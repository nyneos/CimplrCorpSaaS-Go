package master

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"
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
	go StartMasterService(s.db, port)
	return nil
}

func (s *MasterService) Stop() error {
	// Implement stop logic if needed
	return nil
}
