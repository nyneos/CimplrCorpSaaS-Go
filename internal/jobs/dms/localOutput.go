package dmsjobs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const localKeyPrefix = "local:"

// LocalOutputRoot returns the directory for LOCAL destination files.
// Override with DMS_LOCAL_OUTPUT_DIR; default is ./dms_local_output under the Go process cwd.
func LocalOutputRoot() string {
	if v := strings.TrimSpace(os.Getenv("DMS_LOCAL_OUTPUT_DIR")); v != "" {
		return v
	}
	return "dms_local_output"
}

// WriteLocalDmsFile writes bytes under LocalOutputRoot()/runID/filename and returns
// a relative path (for DB) and an absolute path.
func WriteLocalDmsFile(runID, filename string, data []byte) (relPath, absPath string, err error) {
	runID = strings.TrimSpace(runID)
	filename = sanitizeLocalFilename(filename)
	if runID == "" {
		return "", "", fmt.Errorf("run_id required for local DMS output")
	}
	if filename == "" {
		filename = "document.bin"
	}
	root := LocalOutputRoot()
	dir := filepath.Join(root, runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("mkdir local DMS output: %w", err)
	}
	absPath = filepath.Join(dir, filename)
	if err := os.WriteFile(absPath, data, 0o644); err != nil {
		return "", "", fmt.Errorf("write local DMS file: %w", err)
	}
	relPath = filepath.ToSlash(filepath.Join(root, runID, filename))
	return relPath, absPath, nil
}

// LocalStorageKey encodes a relative filesystem path into generated_document.s3_key
// when the artifact is local-only (no S3 object).
func LocalStorageKey(relPath string) string {
	relPath = strings.TrimSpace(strings.TrimPrefix(relPath, localKeyPrefix))
	return localKeyPrefix + filepath.ToSlash(relPath)
}

// IsLocalStorageKey reports whether s3_key points at a local filesystem artifact.
func IsLocalStorageKey(s3Key string) bool {
	return strings.HasPrefix(strings.TrimSpace(s3Key), localKeyPrefix)
}

// ResolveLocalStoragePath maps a local: key or a relative local_path to an absolute file path.
func ResolveLocalStoragePath(keyOrPath string) (string, error) {
	raw := strings.TrimSpace(keyOrPath)
	raw = strings.TrimPrefix(raw, localKeyPrefix)
	raw = filepath.Clean(filepath.FromSlash(raw))
	if raw == "." || raw == "" || strings.HasPrefix(raw, "..") {
		return "", fmt.Errorf("invalid local DMS path")
	}
	abs := raw
	if !filepath.IsAbs(abs) {
		var err error
		if abs, err = filepath.Abs(raw); err != nil {
			return "", err
		}
	}
	rootAbs, err := filepath.Abs(LocalOutputRoot())
	if err != nil {
		return "", err
	}
	// Absolute or relative, the resolved file must sit inside the output root.
	rel, err := filepath.Rel(rootAbs, abs)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("local DMS path outside output root")
	}
	return abs, nil
}

func sanitizeLocalFilename(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, string(os.PathSeparator), "_")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, "..", "_")
	return name
}
