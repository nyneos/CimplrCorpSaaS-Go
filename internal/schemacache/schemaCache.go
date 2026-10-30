// Package schemacache keeps short-lived copies of information_schema lookups so
// handlers that adapt to legacy column layouts do not query the catalog per request.
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

// Columns returns the cached column set for key, calling load on a miss or after expiry.
// Errors are not cached. The returned map is shared and must be treated as read-only.
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

// TableExists returns the cached existence flag for key, calling load on a miss or after expiry.
// A lookup error reports false and is not cached.
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
