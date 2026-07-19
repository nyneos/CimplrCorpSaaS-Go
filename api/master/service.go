package master

import (
	"CimplrCorpSaas/internal/serviceiface"
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MasterService struct {
	config map[string]interface{}
	server *http.Server
	pool   *pgxpool.Pool
	done   chan struct{}
	mu     sync.Mutex
}

func NewMasterService(cfg map[string]interface{}) serviceiface.Service {
	return &MasterService{config: cfg}
}

func (s *MasterService) Name() string {
	return "master"
}

func (s *MasterService) Start() error {
	port := "2143"
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

	server, pool, err := NewMasterServer(port)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		pool.Close()
		return err
	}

	s.mu.Lock()
	s.server = server
	s.pool = pool
	s.done = make(chan struct{})
	done := s.done
	s.mu.Unlock()

	go func() {
		defer close(done)
		logger.LogInfo("Master Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("Master Service failed: %v", err)
		}
	}()
	return nil
}

func (s *MasterService) Stop() error {
	s.mu.Lock()
	server := s.server
	pool := s.pool
	done := s.done
	s.server = nil
	s.pool = nil
	s.done = nil
	s.mu.Unlock()

	if server == nil {
		if pool != nil {
			pool.Close()
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := server.Shutdown(ctx)
	if done != nil {
		<-done
	}
	if pool != nil {
		pool.Close()
	}
	return err
}
