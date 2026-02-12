package notification

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

const notificationPort = "9111"

func StartNotificationService(pool *pgxpool.Pool, db *sql.DB) {
	mux := http.NewServeMux()

	if pool == nil {
		user := os.Getenv("DB_USER")
		pass := os.Getenv("DB_PASSWORD")
		host := os.Getenv("DB_HOST")
		port := os.Getenv("DB_PORT")
		name := os.Getenv("DB_NAME")
		if user != "" && pass != "" && host != "" && port != "" && name != "" {
			dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, name)
			var err error
			pool, err = pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Fatalf("failed to connect to pgxpool DB: %v", err)
			}
		}
	}

	log.Printf("Notification Service started on :%s", notificationPort)
	if err := http.ListenAndServe(":"+notificationPort, mux); err != nil {
		log.Fatalf("Notification Service failed: %v", err)
	}
}
