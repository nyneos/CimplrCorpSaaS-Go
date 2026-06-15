package uam

import (
	"CimplrCorpSaas/internal/serviceiface"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type UAMService struct {
	config map[string]interface{}
	db     *sql.DB
	server *http.Server
	pool   *pgxpool.Pool
	done   chan struct{}
	mu     sync.Mutex
}

func NewUAMService(cfg map[string]interface{}, db *sql.DB) serviceiface.Service {
	return &UAMService{config: cfg, db: db}
}

func (s *UAMService) Name() string {
	return "uam"
}

func (s *UAMService) Start() error {
	port := "5143"
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

	server, pool, err := NewUAMServer(s.db, port)
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
		logger.LogInfo("UAM Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("UAM Service failed: %v", err)
		}
	}()
	return nil
}

func (s *UAMService) Stop() error {
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
