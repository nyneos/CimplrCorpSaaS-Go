package resource

import (
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/serviceiface"
	"fmt"
	"sync"
	"time"
)

type ResourceManager struct {
	resources         map[string]interface{}
	mu                sync.RWMutex
	stopChan          chan struct{}
	heartbeatInterval time.Duration
}

func NewResourceManagerService(cfg map[string]interface{}) serviceiface.Service {
	interval := 5 * time.Second // default
	if val, ok := cfg["heartbeat_interval"]; ok {
		fmt.Println("Configuring heartbeat interval:", val)
		switch v := val.(type) {
		case string:
			if d, err := time.ParseDuration(v); err == nil {
				interval = d
			}
		case float64:
			interval = time.Duration(v) * time.Second
		}
	}
	return &ResourceManager{
		resources:         make(map[string]interface{}),
		stopChan:          make(chan struct{}),
		heartbeatInterval: interval,
	}
}

func (rm *ResourceManager) Name() string { return "resourcemanager" }

func (rm *ResourceManager) Start() error {
	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit("ResourceManager started")
	}
	go rm.heartbeatLoop()
	return nil
}

func (rm *ResourceManager) Stop() error {
	close(rm.stopChan)
	return nil
}

func (rm *ResourceManager) heartbeatLoop() {
	ticker := time.NewTicker(rm.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rm.stopChan:
			return
		case <-ticker.C:
			fmt.Println("ResourceManager: Heartbeat check at", time.Now())
			if logger.GlobalLogger != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("heartbeat check at %v", time.Now()))
			}
		}
	}
}

func (rm *ResourceManager) AddResource(key string, resource interface{}) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.resources[key] = resource
}

func (rm *ResourceManager) GetResource(key string) (interface{}, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	resource, exists := rm.resources[key]
	return resource, exists
}

func (rm *ResourceManager) RemoveResource(key string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.resources, key)
}

func (rm *ResourceManager) ListResources() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	keys := make([]string, 0, len(rm.resources))
	for key := range rm.resources {
		keys = append(keys, key)
	}
	return keys
}
