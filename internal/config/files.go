package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// ResolveExistingFile finds a file across common launch locations so the app
// can be started from the repo root, cmd/, or a built binary directory.
func ResolveExistingFile(path string) (string, error) {
	seen := make(map[string]struct{})
	tried := make([]string, 0, 4)

	addCandidate := func(candidate string) (string, bool) {
		if candidate == "" {
			return "", false
		}

		absPath, err := filepath.Abs(candidate)
		if err == nil {
			candidate = absPath
		}

		if _, ok := seen[candidate]; ok {
			return "", false
		}
		seen[candidate] = struct{}{}
		tried = append(tried, candidate)

		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			return candidate, true
		}

		return "", false
	}

	if resolved, ok := addCandidate(path); ok {
		return resolved, nil
	}

	if !filepath.IsAbs(path) {
		if resolved, ok := addCandidate(filepath.Join("..", path)); ok {
			return resolved, nil
		}
	}

	if exePath, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exePath)

		if resolved, ok := addCandidate(filepath.Join(exeDir, path)); ok {
			return resolved, nil
		}

		if !filepath.IsAbs(path) {
			if resolved, ok := addCandidate(filepath.Join(exeDir, "..", path)); ok {
				return resolved, nil
			}
		}
	}

	return "", fmt.Errorf("file %q not found; tried %v", path, tried)
}
