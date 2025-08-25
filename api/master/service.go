package master

import "CimplrCorpSaas/internal/serviceiface"

type MasterService struct {
	config map[string]interface{}
}

func NewMasterService(cfg map[string]interface{}) serviceiface.Service {
	return &MasterService{config: cfg}
}

func (s *MasterService) Name() string {
	return "master"
}

func (s *MasterService) Start() error {
	go StartMasterService()
	return nil
}

func (s *MasterService) Stop() error {
	// Implement stop logic if needed
	return nil
}
