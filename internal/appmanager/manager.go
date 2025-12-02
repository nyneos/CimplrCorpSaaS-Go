package appmanager

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/cash"
	"CimplrCorpSaas/api/dash"
	"CimplrCorpSaas/api/fx"
	"CimplrCorpSaas/api/investment"
	"CimplrCorpSaas/api/master"
	"CimplrCorpSaas/api/uam"

	// "CimplrCorpSaas/internal/jobs"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/resource"
	"CimplrCorpSaas/internal/serviceiface"

	"database/sql"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"
)

var AuthDB *sql.DB
var db *sql.DB
var pgxPool *pgxpool.Pool

func SetDB(database *sql.DB) {
	db = database
	AuthDB = database
}

func SetPgxPool(pool *pgxpool.Pool) {
	pgxPool = pool
}

// GetDB returns the database connection
func GetDB() *sql.DB {
	return db
}

// GetPgxPool returns the pgx pool connection
func GetPgxPool() *pgxpool.Pool {
	return pgxPool
}

var serviceConstructors = map[string]func(map[string]interface{}) serviceiface.Service{
	"logger": func(cfg map[string]interface{}) serviceiface.Service {
		return logger.NewLoggerService(cfg)
	},
	"resourcemanager": func(cfg map[string]interface{}) serviceiface.Service {
		return resource.NewResourceManagerService(cfg)
	},
	"fx": func(cfg map[string]interface{}) serviceiface.Service {
		return fx.NewFXService(cfg, db) // Pass db here
	},
	"dash": func(cfg map[string]interface{}) serviceiface.Service {
		return dash.NewDashService(cfg, db) // Pass db here
	},
	"cash": func(cfg map[string]interface{}) serviceiface.Service {
		return cash.NewCashService(cfg, db) // Pass db here
	},
	"uam": func(cfg map[string]interface{}) serviceiface.Service {
		return uam.NewUAMService(cfg, db)
	},
	"master": func(cfg map[string]interface{}) serviceiface.Service {
		return master.NewMasterService(cfg, db)
	},
	"investment": func(cfg map[string]interface{}) serviceiface.Service {
		// return investment.NewInvestmentService(cfg, pgxPool)
		return investment.NewInvestmentService(cfg, pgxPool, db)
	},
	"gateway": func(cfg map[string]interface{}) serviceiface.Service {
		return api.NewGatewayService(cfg)
	},
	"auth": func(cfg map[string]interface{}) serviceiface.Service {
		var maxUsers int
		var sessionTimeout int
		var maxLoginAttempts int
		var accountLockDuration int
		var sessionCleanerPeriod int

		toInt := func(v interface{}) int {
			switch t := v.(type) {
			case int:
				return t
			case int64:
				return int(t)
			case float64:
				return int(t)
			case string:
				var parsed int
				if _, err := fmt.Sscanf(t, "%d", &parsed); err == nil {
					return parsed
				}
			}
			return 0
		}

		if cfg != nil {
			if v, ok := cfg["max_users"]; ok && v != nil {
				maxUsers = toInt(v)
			}
			if v, ok := cfg["session_timeout"]; ok && v != nil {
				sessionTimeout = toInt(v)
			}
			if v, ok := cfg["max_login_attempts"]; ok && v != nil {
				maxLoginAttempts = toInt(v)
			}
			if v, ok := cfg["account_lock_duration"]; ok && v != nil {
				accountLockDuration = toInt(v)
			}
			if v, ok := cfg["session_cleaner_period"]; ok && v != nil {
				sessionCleanerPeriod = toInt(v)
			}
		}
		return auth.NewAuthService(AuthDB, maxUsers, sessionTimeout, maxLoginAttempts, accountLockDuration, sessionCleanerPeriod)
	},
}

// ------------------- MANAGER -------------------

type AppManager struct {
	services []serviceiface.Service
	mu       sync.Mutex
}

func NewAppManager() *AppManager {
	return &AppManager{
		services: make([]serviceiface.Service, 0),
	}
}

func (am *AppManager) RegisterService(s serviceiface.Service) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.services = append(am.services, s)
}

func (am *AppManager) StartAll() error {
	am.mu.Lock()
	defer am.mu.Unlock()

	// First pass: start all except Resourcemanager
	for _, service := range am.services {
		if service.Name() == "resourcemanager" {
			continue
		}
		fmt.Println("Starting service:", service.Name())
		if err := service.Start(); err != nil {
			return fmt.Errorf("failed to start service %s: %w", service.Name(), err)
		}
	}

	// Now start resourcemanager (after heartbeat is wired)
	for _, service := range am.services {
		if service.Name() == "resourcemanager" {
			fmt.Println("Starting service:", service.Name())
			if err := service.Start(); err != nil {
				return fmt.Errorf("failed to start service %s: %w", service.Name(), err)
			}
		}
	}
	return nil
}

func (am *AppManager) StopAll() error {
	am.mu.Lock()
	defer am.mu.Unlock()
	for i := len(am.services) - 1; i >= 0; i-- {
		svc := am.services[i]
		if err := svc.Stop(); err != nil {
			return fmt.Errorf("failed to stop service %s: %w", svc.Name(), err)
		}
	}
	return nil
}

// ------------------- YAML CONFIG -------------------

type ServiceSequencer struct {
	Services []ServiceConfig `yaml:"services"`
}

type ServiceConfig struct {
	Name       string                 `yaml:"name"`
	StartOrder int                    `yaml:"start_order"`
	Config     map[string]interface{} `yaml:"config"`
}

func LoadServiceSequence(path string) ([]ServiceConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var seq ServiceSequencer
	if err := yaml.Unmarshal(data, &seq); err != nil {
		return nil, err
	}

	// sort by start_order
	sort.Slice(seq.Services, func(i, j int) bool {
		return seq.Services[i].StartOrder < seq.Services[j].StartOrder
	})

	return seq.Services, nil
}

func (am *AppManager) AutoRegisterServices(configs []ServiceConfig) {
	for _, svc := range configs {
		if constructor, ok := serviceConstructors[svc.Name]; ok {
			service := constructor(svc.Config)
			am.RegisterService(service)
			if svc.Name == "auth" {
				if realAuthSvc, ok := service.(*auth.AuthService); ok {
					api.SetAuthService(realAuthSvc)
					auth.SetGlobalAuthService(realAuthSvc)
				}
			}
		}
	}

	for _, svc := range am.services {
		if l, ok := svc.(*logger.LoggerService); ok {
			logger.SetGlobalLogger(l)
			break
		}
	}
}

// func (am *AppManager) WireServices() {
// 	// var hb *heartbeat.HeartbeatService
// 	// var rm *resource.ResourceManager
// 	for _, svc := range am.services {
// 		switch s := svc.(type) {
// 		case *heartbeat.HeartbeatService:
// 			hb = s
// 		case *resource.ResourceManager:
// 			rm = s
// 		}
// 	}
// 	// if rm != nil && hb != nil {
// 	// 	rm.SetHeartbeat(hb)
// 	// }
// }

// Each service (fx, dash, cash, uam, gateway) should be started independently as goroutines
// in their respective files, similar to how logger and resource manager are started.
/*
Example services.yaml:
services:
	- name: logger
		start_order: 1
		config:
			enabled: true
	- name: resourcemanager
		start_order: 2
		config:
			enabled: true
	- name: fx
		start_order: 3
		config:
			port: 3143
	- name: dash
		start_order: 4
		config:
			port: 4143
	- name: cash
		start_order: 5
		config:
			port: 6143
	- name: uam
		start_order: 6
		config:
			port: 5143
	- name: gateway
		start_order: 7
		config:
			port: 8080
*/

func (am *AppManager) GetServiceByName(name string) serviceiface.Service {
	for _, svc := range am.services {
		if svc.Name() == name {
			return svc
		}
	}
	return nil
}

