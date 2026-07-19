package investment

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

// InvestmentService is a lightweight service wrapper for investment-related workers
type InvestmentService struct {
	cfg    map[string]interface{}
	pool   *pgxpool.Pool
	server *http.Server
	done   chan struct{}
	mu     sync.Mutex
}

// NewInvestmentService constructs an InvestmentService and accepts a pgx pool instance.
func NewInvestmentService(cfg map[string]interface{}, pool *pgxpool.Pool) serviceiface.Service {
	return &InvestmentService{cfg: cfg, pool: pool}
}

func (s *InvestmentService) Name() string {
	return "investment"
}

func (s *InvestmentService) Start() error {
	port := "7143"
	if s.cfg != nil {
		if v, ok := s.cfg["port"]; ok {
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

	server := NewInvestmentServer(s.pool, port)
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
		logger.LogInfo("Investment Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("Investment service failed: %v", err)
		}
	}()
	return nil
}

func (s *InvestmentService) Stop() error {
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
