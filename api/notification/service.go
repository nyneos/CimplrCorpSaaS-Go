package notification

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type NotificationService struct {
	config map[string]interface{}
	pool   *pgxpool.Pool
	db     *sql.DB
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
	go StartNotificationService(s.pool, s.db, port)
	return nil
}

func (s *NotificationService) Stop() error {
	return shutdownNotificationService()
}
