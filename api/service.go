package api

import "CimplrCorpSaas/internal/serviceiface"

type GatewayService struct {
	config map[string]interface{}
}

func NewGatewayService(cfg map[string]interface{}) serviceiface.Service {
	return &GatewayService{config: cfg}
}

func (s *GatewayService) Name() string {
	return "gateway"
}

func (s *GatewayService) Start() error {
	go StartGateway()
	return nil
}

func (s *GatewayService) Stop() error {
	// Implement stop logic if needed
	return nil
}
