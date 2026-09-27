package transformrules

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
)

// testReceiveToFolder is a dummy partner API for locally testing an "API"
// transform-rule destination end to end, without a real external partner.
// Point a rule's api_url at this instead:
//
//	http://localhost:8183/email/transform-rules/test-receive
//	http://localhost:8183/email/transform-rules/test-receive-2
//
// This used to be served by the now-retired standalone CIMPLR-Email-Service
// (on :8182); it moved here with the rest of the mail engine so any
// destination row still pointing at it keeps working.
func testReceiveToFolder(w http.ResponseWriter, r *http.Request, subfolder, logLabel string) {
	if r.Method != http.MethodPost {
		writeTestReceiveErr(w, "method not allowed — use POST", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		writeTestReceiveErr(w, "expected multipart form with field file: "+err.Error(), http.StatusBadRequest)
		return
	}
	file, hdr, err := r.FormFile("file")
	if err != nil {
		writeTestReceiveErr(w, "multipart field 'file' is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		writeTestReceiveErr(w, "failed to read upload: "+err.Error(), http.StatusInternalServerError)
		return
	}

	base := strings.TrimSpace(os.Getenv("EMAIL_TRANSFORMED_LOCAL_DIR"))
	if base == "" {
		base = "./transformed"
	}
	dir := filepath.Join(base, subfolder)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		writeTestReceiveErr(w, "mkdir "+subfolder+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	name := filepath.Base(strings.TrimSpace(hdr.Filename))
	if name == "" || name == "." {
		name = fmt.Sprintf("upload_%s.bin", time.Now().Format("20060102_150405"))
	}
	full := filepath.Join(dir, name)
	if err := os.WriteFile(full, body, 0o644); err != nil {
		writeTestReceiveErr(w, "write file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	abs, _ := filepath.Abs(full)
	logger.LogInfo("[mail] %s: saved %s (%d bytes)", logLabel, abs, len(body))
	writeTestReceiveJSON(w, map[string]interface{}{
		"ok":       true,
		"filename": name,
		"path":     abs,
		"bytes":    len(body),
		"folder":   subfolder,
	})
}

func testReceiveHandler(w http.ResponseWriter, r *http.Request) {
	testReceiveToFolder(w, r, "api-inbox", "transform-rules/test-receive")
}

func testReceive2Handler(w http.ResponseWriter, r *http.Request) {
	testReceiveToFolder(w, r, "api-inbox-2", "transform-rules/test-receive-2")
}

func writeTestReceiveJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeTestReceiveErr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "error": msg})
}
