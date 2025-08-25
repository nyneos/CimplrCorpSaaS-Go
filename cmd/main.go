package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/internal/appmanager"
)

func InitDB() (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		"postgres.sgryutycrupiuhovmbfo",
		"NyneOS@1234$",
		"aws-0-ap-south-1.pooler.supabase.com",
		"5432",
		"postgres",
	)

	return sql.Open("postgres", connStr)
}

func main() {
	// Initialize DB for Auth
	db, err := auth.InitDB()
	if err != nil {
		log.Fatal("failed to connect to DB:", err)
	}
	appmanager.SetDB(db)

	manager := appmanager.NewAppManager()

	// Load service configs from YAML
	servicesCfg, err := appmanager.LoadServiceSequence("services.yaml")
	if err != nil {
		log.Fatal("failed to load service sequence:", err)
	}

	// Automatically register all services
	manager.AutoRegisterServices(servicesCfg)

	// Start all services
	if err := manager.StartAll(); err != nil {
		log.Fatal("failed to start:", err)
	}

	// --- Wire AuthService to Gateway ---
	authSvcIface := manager.GetServiceByName("auth")
	if authSvcIface == nil {
		log.Fatal("Auth service not found in manager")
	}
	realAuthSvc, ok := authSvcIface.(*auth.AuthService)
	if !ok {
		log.Fatal("Auth service type assertion failed")
	}
	api.SetAuthService(realAuthSvc)

	// Graceful shutdown handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// Stop all services
	if err := manager.StopAll(); err != nil {
		log.Fatal("failed to stop:", err)
	}
}
