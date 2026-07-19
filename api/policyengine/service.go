package policyengine

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/serviceiface"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PolicyEngineService struct {
	config    map[string]interface{}
	pool      *pgxpool.Pool
	server    *http.Server
	ownedPool *pgxpool.Pool
	done      chan struct{}
	mu        sync.Mutex
}

func NewPolicyEngineService(cfg map[string]interface{}, pool *pgxpool.Pool) serviceiface.Service {
	return &PolicyEngineService{config: cfg, pool: pool}
}

func (s *PolicyEngineService) Name() string { return "policyengine" }

func (s *PolicyEngineService) Start() error {
	port := "8185"
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

	server, ownedPool, ownsPool, err := NewPolicyEngineServer(s.pool, port)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		if ownsPool {
			ownedPool.Close()
		}
		return err
	}

	s.mu.Lock()
	s.server = server
	if ownsPool {
		s.ownedPool = ownedPool
	}
	s.done = make(chan struct{})
	done := s.done
	s.mu.Unlock()

	go func() {
		defer close(done)
		logger.LogInfo("Policy Engine Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("Policy Engine Service failed: %v", err)
		}
	}()
	return nil
}

func (s *PolicyEngineService) Stop() error {
	s.mu.Lock()
	server := s.server
	ownedPool := s.ownedPool
	done := s.done
	s.server = nil
	s.ownedPool = nil
	s.done = nil
	s.mu.Unlock()

	if server == nil {
		if ownedPool != nil {
			ownedPool.Close()
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := server.Shutdown(ctx)
	if done != nil {
		<-done
	}
	if ownedPool != nil {
		ownedPool.Close()
	}
	return err
}
