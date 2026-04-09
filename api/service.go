package api

import (
	"CimplrCorpSaas/internal/serviceiface"
	"fmt"
)

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
	port := "8081"
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
	pathPrefix := "/cimplrapigateway"
	if s.config != nil {
		if v, ok := s.config["path_prefix"]; ok {
			if p, ok := v.(string); ok && p != "" {
				pathPrefix = p
			}
		}
	}
	StartGateway(port, pathPrefix)
	return nil
}

func (s *GatewayService) Stop() error {
	// Implement stop logic if needed
	return nil
}
