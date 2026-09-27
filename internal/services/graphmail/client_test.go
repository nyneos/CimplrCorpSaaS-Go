package graphmail

import (
	"strings"
	"testing"
	"time"
)

func TestInboxMessagesPathEncodesOData(t *testing.T) {
	path := folderMessagesPath("hardik.mishra@nyneos.com", "inbox", "receivedDateTime", time.Date(2026, 7, 2, 8, 0, 37, 0, time.UTC), 25)
	if strings.Contains(path, " asc") || strings.Contains(path, " ge ") {
		t.Fatalf("path has unencoded OData whitespace: %q", path)
	}
	if !strings.Contains(path, "receivedDateTime+gt+") {
		t.Fatalf("expected gt filter in path: %q", path)
	}
	if !strings.Contains(path, "hardik.mishra@nyneos.com") {
		t.Fatalf("expected mailbox in path: %q", path)
	}
}
