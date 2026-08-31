package fdMonthEndClosing

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

// FdMonthEndClosingService implements serviceiface.Service, shaped exactly
// like api/email/service.go's EmailService. It is NOT currently registered in
// internal/appmanager/manager.go's service registry — see the doc comment on
// NewFdMonthEndClosingServer in fdMonthEndClosing.go for why the live routes
// are mounted on the shared "investment" service's mux instead (via
// api/investment/routes.go). This type is kept so the module can be promoted
// to its own registered service/port later (mirroring how "email" is
// registered) without restructuring the handler code.
type FdMonthEndClosingService struct {
	config map[string]interface{}
	pool   *pgxpool.Pool
	server *http.Server
	done   chan struct{}
	mu     sync.Mutex
}

// NewFdMonthEndClosingService constructs the service wrapper. cfg follows the
// same shape as every other serviceiface.Service factory in this repo
// (optional "port" key as string/int/float64).
func NewFdMonthEndClosingService(cfg map[string]interface{}, pool *pgxpool.Pool) serviceiface.Service {
	return &FdMonthEndClosingService{config: cfg, pool: pool}
}

func (s *FdMonthEndClosingService) Name() string { return serviceName }

func (s *FdMonthEndClosingService) Start() error {
	port := "7144"
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

	if s.pool == nil {
		return fmt.Errorf("fd-month-end-closing pgxpool is not configured")
	}

	server := NewFdMonthEndClosingServer(s.pool, port)
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
		logger.LogInfo("FD Month End Closing Service started on :%s", port)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.LogError("FD Month End Closing service failed: %v", err)
		}
	}()
	return nil
}

func (s *FdMonthEndClosingService) Stop() error {
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
