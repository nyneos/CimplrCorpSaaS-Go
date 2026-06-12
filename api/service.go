package api

import (
	"CimplrCorpSaas/internal/serviceiface"
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"CimplrCorpSaas/internal/logger"
)

type GatewayService struct {
	config map[string]interface{}
	server *http.Server
	done   chan struct{}
	mu     sync.Mutex
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

	server, cert, key := NewGatewayServer(port, pathPrefix)
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.server = server
	s.done = make(chan struct{})
	done := s.done
	s.mu.Unlock()

	go func() {
		defer close(done)
		var err error
		if cert != "" && key != "" {
			err = server.ServeTLS(ln, cert, key)
		} else {
			logger.LogInfo("TLS_CERT or TLS_KEY not set; starting HTTP on %s", server.Addr)
			err = server.Serve(ln)
		}
		if err != nil && err != http.ErrServerClosed {
			logger.LogError("Gateway server failed: %v", err)
		}
	}()
	return nil
}

func (s *GatewayService) Stop() error {
	s.mu.Lock()
	server := s.server
	done := s.done
	s.server = nil
	s.done = nil
	s.mu.Unlock()

	if server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := server.Shutdown(ctx)
	if done != nil {
		<-done
	}
	return err
}
