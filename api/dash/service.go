package dash

import "CimplrCorpSaas/internal/serviceiface"

type DashService struct {
	config map[string]interface{}
}

func NewDashService(cfg map[string]interface{}) serviceiface.Service {
	return &DashService{config: cfg}
}

func (s *DashService) Name() string {
	return "dash"
}

func (s *DashService) Start() error {
	go StartDashService()
	return nil
}

func (s *DashService) Stop() error {
	// Implement stop logic if needed
	return nil
}
