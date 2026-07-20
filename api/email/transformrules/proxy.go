package transformrules

import (
	"encoding/json"
	"net/http"
	"time"

	emailcommon "CimplrCorpSaas/api/email/common"
	"CimplrCorpSaas/internal/transformtool"
)

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

func handleListMappings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		emailcommon.RespondMethodNotAllowed(w)
		return
	}

	toolURL := transformtool.BaseURL()

	resp, err := httpClient.Get(toolURL + "/tftoolapi/mappings/summary")
	if err != nil {
		emailcommon.RespondInternal(w, "Failed to connect to transformation tool")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		emailcommon.RespondInternal(w, "Transformation tool returned non-OK status")
		return
	}

	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		emailcommon.RespondInternal(w, "Failed to parse mappings from transformation tool")
		return
	}

	// Tool returns { data: [...], page, pageSize, total }. Forward rows for the UI.
	mappings, _ := raw["data"].([]interface{})
	if mappings == nil {
		mappings = []interface{}{}
	}
	emailcommon.RespondList(w, "transform-rules-mappings", mappings, len(mappings))
}
