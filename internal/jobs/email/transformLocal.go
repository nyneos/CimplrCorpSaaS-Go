package emailjobs

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var unsafeLocalNameChars = regexp.MustCompile(`[^a-zA-Z0-9._@+-]+`)

// cimplrLocalBaseDir is where destination_type=LOCAL files are written on the
// Cimplr Go host (not the email-service machine). Default ./transformed next to
// the process cwd (same idea as ./logs).
func cimplrLocalBaseDir() string {
	if v := strings.TrimSpace(os.Getenv("CIMPLR_TRANSFORMED_LOCAL_DIR")); v != "" {
		return v
	}
	// Prefer Cimplr-specific env; fall back to legacy name if ops already set it on Go.
	if v := strings.TrimSpace(os.Getenv("EMAIL_TRANSFORMED_LOCAL_DIR")); v != "" {
		return v
	}
	return "./transformed"
}

func sanitizeLocalPathSegment(name string) string {
	name = strings.TrimSpace(name)
	name = unsafeLocalNameChars.ReplaceAllString(name, "_")
	name = strings.Trim(name, "._-")
	if name == "" {
		return "shared"
	}
	if len(name) > 80 {
		name = name[:80]
	}
	return name
}

func buildLocalOutputFilename(prefix string, appendDatetime bool, fileExt string, at time.Time) string {
	ext := strings.ToLower(strings.TrimSpace(fileExt))
	if ext == "" {
		ext = ".json"
	}
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	base := sanitizeLocalPathSegment(prefix)
	if base == "shared" && strings.TrimSpace(prefix) == "" {
		base = "transformed"
	}
	if appendDatetime {
		base = base + "_" + at.Format("20060102_150405")
	}
	return base + ext
}

// localOwnerKey picks which SaaS user "owns" the LOCAL folder.
// Prefer rule creator, then approver, else shared — so multi-user tenants
// do not dump everyone's files into one flat directory.
func localOwnerKey(createdBy, approvedBy string) string {
	if strings.TrimSpace(createdBy) != "" {
		return sanitizeLocalPathSegment(createdBy)
	}
	if strings.TrimSpace(approvedBy) != "" {
		return sanitizeLocalPathSegment(approvedBy)
	}
	return "shared"
}

// putLocalOnCimplr writes transformed bytes under:
//
//	{CIMPLR_TRANSFORMED_LOCAL_DIR}/{owner}/{optional_subfolder}/{filename}
//
// Creating missing directories. Returns absolute path + filename.
func putLocalOnCimplr(dest ruleDestination, fileExt string, body []byte) (absPath, filename string, err error) {
	owner := localOwnerKey(dest.CreatedBy, dest.ApprovedBy)
	filename = buildLocalOutputFilename(dest.OutputNamePrefix, dest.AppendDatetime, fileExt, time.Now())

	base, err := filepath.Abs(cimplrLocalBaseDir())
	if err != nil {
		return "", "", fmt.Errorf("resolve local base dir: %w", err)
	}

	dir := filepath.Join(base, owner)
	sub := strings.Trim(strings.TrimSpace(dest.LocalFolder), "/")
	if sub != "" {
		// Only allow relative subfolder segments (no absolute / escape).
		clean := filepath.Clean(filepath.FromSlash(sub))
		if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || filepath.IsAbs(clean) {
			return "", "", fmt.Errorf("invalid local_folder %q", dest.LocalFolder)
		}
		dir = filepath.Join(dir, clean)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("mkdir local dir: %w", err)
	}
	full := filepath.Join(dir, filename)
	// Ensure final path stays under base (path traversal guard).
	absFull, err := filepath.Abs(full)
	if err != nil {
		return "", "", err
	}
	rel, err := filepath.Rel(base, absFull)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", "", fmt.Errorf("refusing to write outside local base dir")
	}
	if err := os.WriteFile(absFull, body, 0o644); err != nil {
		return "", "", fmt.Errorf("write local file: %w", err)
	}
	return absFull, filename, nil
}

// IsPathUnderCimplrLocalBase returns true if absPath is under the configured LOCAL root.
func IsPathUnderCimplrLocalBase(absPath string) bool {
	base, err := filepath.Abs(cimplrLocalBaseDir())
	if err != nil {
		return false
	}
	full, err := filepath.Abs(absPath)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(base, full)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}
