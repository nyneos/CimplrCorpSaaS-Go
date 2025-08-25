package cash

import "CimplrCorpSaas/internal/serviceiface"

type CashService struct {
	config map[string]interface{}
}

func NewCashService(cfg map[string]interface{}) serviceiface.Service {
	return &CashService{config: cfg}
}

func (s *CashService) Name() string {
	return "cash"
}

func (s *CashService) Start() error {
	go StartCashService()
	return nil
}

func (s *CashService) Stop() error {
	// Implement stop logic if needed
	return nil
}
