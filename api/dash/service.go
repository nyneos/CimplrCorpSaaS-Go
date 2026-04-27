package dash

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"
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
	go StartDashService(s.db, port)
	return nil
}

func (s *DashService) Stop() error {
	return shutdownDashService()
}
