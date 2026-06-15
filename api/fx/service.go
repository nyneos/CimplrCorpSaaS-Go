package fx

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

type FXService struct {
	config map[string]interface{}
	db     *sql.DB
	server *http.Server
	pool   *pgxpool.Pool
	done   chan struct{}
	mu     sync.Mutex
}

func NewFXService(cfg map[string]interface{}, db *sql.DB) serviceiface.Service {
	return &FXService{config: cfg, db: db}
}

func (s *FXService) Name() string {
	return "fx"
}

func (s *FXService) Start() error {
	port := "3143"
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

	server, pool, err := NewFXServer(s.db, port)
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
		logger.LogInfo("FX Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("FX Service failed: %v", err)
		}
	}()
	return nil
}

func (s *FXService) Stop() error {
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
