package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"CimplrCorpSaas/internal/appmanager"
)

func initDB() (*sql.DB, error) {
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	connStr := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		user, pass, host, port, name,
	)
	return sql.Open("postgres", connStr)
}

func main() {
	_ = godotenv.Overload("../.env")

	db, err := initDB()
	if err != nil {
		log.Fatal("failed to connect to DB:", err)
	}

	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	if err := db.PingContext(dbCtx); err != nil {
		log.Fatalf("sql.DB ping failed: %v", err)
	}
	appmanager.SetDB(db)

	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	sslMode := os.Getenv("DB_SSLMODE")
	if sslMode == "" {
		sslMode = "disable"
	}
	pgxConnStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s&connect_timeout=10&pool_max_conns=30&pool_min_conns=2&statement_timeout=30000",
		user, pass, host, port, name, sslMode,
	)

	pgxCtx, pgxCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pgxCancel()

	pgxPool, err := pgxpool.New(pgxCtx, pgxConnStr)
	if err != nil {
		log.Fatal("failed to create pgx pool:", err)
	}
	defer pgxPool.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()

	if err := pgxPool.Ping(pingCtx); err != nil {
		log.Fatalf("pgx pool ping failed — check DB config: %v", err)
	}
	log.Printf("Worker DB connected: host=%s port=%s db=%s sslmode=%s", host, port, name, sslMode)

	appmanager.SetPgxPool(pgxPool)

	manager := appmanager.NewAppManager()
	servicesCfg, err := appmanager.LoadServiceSequence("./services.yaml")
	if err != nil {
		log.Fatal("failed to load service sequence:", err)
	}
	servicesCfg = appmanager.SelectServiceConfigs(servicesCfg, "logger", "cron")

	manager.AutoRegisterServices(servicesCfg)

	if err := manager.StartAll(); err != nil {
		log.Fatal("failed to start worker services:", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	if err := manager.StopAll(); err != nil {
		log.Fatal("failed to stop worker services:", err)
	}
}
