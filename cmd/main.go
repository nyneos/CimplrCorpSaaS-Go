package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/cash"
	"CimplrCorpSaas/api/dash"
	"CimplrCorpSaas/api/fx"
	"CimplrCorpSaas/api/investment"
	"CimplrCorpSaas/api/master"
	"CimplrCorpSaas/api/notification"
	"CimplrCorpSaas/api/uam"
	"CimplrCorpSaas/internal/appmanager"
	"CimplrCorpSaas/internal/config"
)

// InitDB loads DB config from env vars
func InitDB() (*sql.DB, error) {
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
	if envPath, err := config.ResolveExistingFile(".env"); err == nil {
		if err := godotenv.Load(envPath); err != nil {
			log.Printf("warning: failed to load env file %s: %v", envPath, err)
		} else {
			log.Printf("loaded env from %s", envPath)
		}
	} else {
		log.Printf("warning: %v", err)
	}

	fmt.Println("ENV CHECK:")
	fmt.Println("  DB_USER:", os.Getenv("DB_USER"))
	fmt.Println("  DB_HOST:", os.Getenv("DB_HOST"))
	fmt.Println("  DB_PORT:", os.Getenv("DB_PORT"))
	fmt.Println("  DB_NAME:", os.Getenv("DB_NAME"))

	if os.Getenv("DB_PASSWORD") != "" {
		fmt.Println("  DB_PASSWORD: [SET]")
	} else {
		fmt.Println("  DB_PASSWORD: [NOT SET!]")
	}

	db, err := InitDB()
	if err != nil {

		log.Fatal("failed to connect to DB:", err)
	}
	appmanager.SetDB(db)

	// Initialize pgx pool for better performance
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	pgxConnStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, pass, host, port, name,
	)

	ctx := context.Background()
	pgxPool, err := pgxpool.New(ctx, pgxConnStr)
	if err != nil {
		log.Fatal("failed to create pgx pool:", err)
	}
	defer pgxPool.Close()
	appmanager.SetPgxPool(pgxPool)

	// Initialize Redis
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	if redisHost == "" {
		redisHost = "localhost"
	}
	if redisPort == "" {
		redisPort = "6379"
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", redisHost, redisPort),
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("WARNING: Redis connection failed: %v (continuing with limited state sharing)", err)
	}
	appmanager.SetRedisClient(redisClient)
	api.SetRedisClient(redisClient)
	defer redisClient.Close()

	manager := appmanager.NewAppManager()

	// Load service configs from YAML
	servicesPath, err := config.ResolveExistingFile("services.yaml")
	if err != nil {
		log.Fatal("failed to resolve service sequence:", err)
	}

	servicesCfg, err := appmanager.LoadServiceSequence(servicesPath)
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
	realAuthSvc.SetRedisClient(redisClient)
	api.SetAuthService(realAuthSvc)
	
	mux := http.NewServeMux()
	api.RegisterGatewayRoutes(mux)
	cash.RegisterCashRoutes(mux, db, pgxPool)
	fx.RegisterFXRoutes(mux, db, pgxPool)
	dash.RegisterDashRoutes(mux, db, pgxPool)
	uam.RegisterUAMRoutes(mux, db, pgxPool)
	master.RegisterMasterRoutes(mux, db, pgxPool)
	investment.RegisterInvestmentRoutes(mux, db, pgxPool)
	notification.RegisterNotificationRoutes(mux, db, pgxPool)

	go func() {
		if err := api.ServeGateway(mux); err != nil {
			log.Fatalf("Gateway server failed: %v", err)
		}
	}()

	// Graceful shutdown handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// Stop all services
	if err := manager.StopAll(); err != nil {
		log.Fatal("failed to stop:", err)
	}

	// Close pgx pool if initialized
	if appmanager.GetPgxPool() != nil {
		appmanager.GetPgxPool().Close()
	}
}
