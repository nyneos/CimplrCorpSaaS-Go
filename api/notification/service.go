package notification

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

type NotificationService struct {
	config    map[string]interface{}
	pool      *pgxpool.Pool
	db        *sql.DB
	server    *http.Server
	ownedPool *pgxpool.Pool
	done      chan struct{}
	mu        sync.Mutex
}

func NewNotificationService(cfg map[string]interface{}, pool *pgxpool.Pool, db *sql.DB) serviceiface.Service {
	return &NotificationService{config: cfg, pool: pool, db: db}
}

func (s *NotificationService) Name() string {
	return "notification"
}

func (s *NotificationService) Start() error {
	port := "9111"
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

	server, ownedPool, ownsPool, err := NewNotificationServer(s.pool, s.db, port)
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
		logger.LogInfo("Notification Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("Notification Service failed: %v", err)
		}
	}()
	return nil
}

func (s *NotificationService) Stop() error {
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
