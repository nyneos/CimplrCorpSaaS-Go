package schemacache

import (
	"sync"
	"time"
)

const ttl = 5 * time.Minute

type columnsEntry struct {
	cols    map[string]bool
	expires time.Time
}

type existsEntry struct {
	exists  bool
	expires time.Time
}

var (
	mu      sync.RWMutex
	columns = map[string]columnsEntry{}
	tables  = map[string]existsEntry{}
)

func Columns(key string, load func() (map[string]bool, error)) (map[string]bool, error) {
	now := time.Now()
	mu.RLock()
	entry, ok := columns[key]
	mu.RUnlock()
	if ok && now.Before(entry.expires) {
		return entry.cols, nil
	}
	cols, err := load()
	if err != nil {
		return nil, err
	}
	mu.Lock()
	columns[key] = columnsEntry{cols: cols, expires: now.Add(ttl)}
	mu.Unlock()
	return cols, nil
}

func TableExists(key string, load func() (bool, error)) bool {
	now := time.Now()
	mu.RLock()
	entry, ok := tables[key]
	mu.RUnlock()
	if ok && now.Before(entry.expires) {
		return entry.exists
	}
	exists, err := load()
	if err != nil {
		return false
	}
	mu.Lock()
	tables[key] = existsEntry{exists: exists, expires: now.Add(ttl)}
	mu.Unlock()
	return exists
}
