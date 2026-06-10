package cash

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"
	"os"
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
	port := os.Getenv("CASH_PORT")
	if port == "" {
		port = "6143"
	}
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
	go StartCashService(port)
	return nil
}

func (s *CashService) Stop() error {
	// Implement stop logic if needed
	return nil
}
