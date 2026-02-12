package notification

import (
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"

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
	go StartNotificationService(s.pool, s.db)
	return nil
}

func (s *NotificationService) Stop() error {
	return nil
}
