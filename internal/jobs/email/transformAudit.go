package emailjobs

import (
	"regexp"
	"strings"
)

var apiStatusRe = regexp.MustCompile(`status=(\d{3})`)

// deliveryAuditFromOutcome builds normalized delivery fields for processing_log.
func deliveryAuditFromOutcome(destType, location, errMsg string) map[string]interface{} {
	dt := strings.ToUpper(strings.TrimSpace(destType))
	if dt == "" {
		dt = "S3"
	}
	out := map[string]interface{}{
		"destination_type": dt,
	}
	if strings.TrimSpace(errMsg) != "" {
		out["delivery_status"] = "FAIL"
		out["delivery_detail"] = errMsg
		return out
	}
	loc := strings.TrimSpace(location)
	switch dt {
	case "API":
		if m := apiStatusRe.FindStringSubmatch(loc); len(m) == 2 {
			out["delivery_status"] = m[1]
			out["http_status"] = m[1]
		} else if loc != "" {
			out["delivery_status"] = "OK"
		}
	case "SFTP":
		if strings.HasPrefix(strings.ToLower(loc), "sftp://") {
			out["delivery_status"] = "OK"
			out["delivery_detail"] = loc
		}
	default: // S3, LOCAL
		if loc != "" {
			out["delivery_status"] = "OK"
			out["delivery_detail"] = loc
		}
	}
	return out
}
