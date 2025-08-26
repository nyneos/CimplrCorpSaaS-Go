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
	"CimplrCorpSaas/api/master"
	"CimplrCorpSaas/api/uam"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/resource"
	"CimplrCorpSaas/internal/serviceiface"
	"database/sql"

	"gopkg.in/yaml.v3"
)

var AuthDB *sql.DB
var db *sql.DB

func SetDB(database *sql.DB) {
	db = database
	AuthDB = database
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
		   // Import master package at top: "CimplrCorpSaas/api/master"
		   return master.NewMasterService(cfg, db)
	   },
	   "gateway": func(cfg map[string]interface{}) serviceiface.Service {
		   return api.NewGatewayService(cfg)
	   },
	   "auth": func(cfg map[string]interface{}) serviceiface.Service {
		   maxUsers := 2 // or get from cfg
		   return auth.NewAuthService(AuthDB, maxUsers)
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
