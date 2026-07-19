package cash

import (
	"CimplrCorpSaas/internal/serviceiface"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CashService struct {
	config map[string]interface{}
	server *http.Server
	pool   *pgxpool.Pool
	done   chan struct{}
	mu     sync.Mutex
}

func NewCashService(cfg map[string]interface{}) serviceiface.Service {
	return &CashService{config: cfg}
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

	server, pool, err := NewCashServer(port)
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
		logger.LogInfo("Cash Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("Cash Service failed: %v", err)
		}
	}()
	return nil
}

func (s *CashService) Stop() error {
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
