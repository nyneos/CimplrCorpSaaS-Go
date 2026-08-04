package dms

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

type DmsService struct {
	config    map[string]interface{}
	pool      *pgxpool.Pool
	server    *http.Server
	ownedPool *pgxpool.Pool
	done      chan struct{}
	mu        sync.Mutex
}

func NewDmsService(cfg map[string]interface{}, pool *pgxpool.Pool) serviceiface.Service {
	return &DmsService{config: cfg, pool: pool}
}

func (s *DmsService) Name() string { return "dms" }

func (s *DmsService) Start() error {
	port := "8186"
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

	server, ownedPool, ownsPool, err := NewDmsServer(s.pool, port)
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
		logger.LogInfo("DMS Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("DMS Service failed: %v", err)
		}
	}()
	return nil
}

func (s *DmsService) Stop() error {
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
