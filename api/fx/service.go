package fx

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"
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
	port := "3143"
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
	go StartFXService(s.db, port)
	return nil
}

func (s *FXService) Stop() error {
	return shutdownFXService()
}
