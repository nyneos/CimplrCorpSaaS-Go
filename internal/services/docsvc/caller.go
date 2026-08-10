package docsvc

import (
	"os"
	"strings"
)

// CallerContext is attached to every Document-Service request for billing/metering.
type CallerContext struct {
	CallerService       string
	CallerDeploymentURL string
	Actor               string
}

func DefaultCallerContext(actor string) CallerContext {
	return CallerContext{
		CallerService:       callerServiceName(),
		CallerDeploymentURL: callerDeploymentURL(),
		Actor:               strings.TrimSpace(actor),
	}
}

func callerServiceName() string {
	if v := strings.TrimSpace(os.Getenv("CIMPLR_SERVICE_NAME")); v != "" {
		return v
	}
	return "cimplr-main"
}

func callerDeploymentURL() string {
	if v := strings.TrimRight(strings.TrimSpace(os.Getenv("CIMPLR_DEPLOYMENT_URL")), "/"); v != "" {
		return v
	}
	port := strings.TrimSpace(os.Getenv("GATEWAY_PORT"))
	if port == "" {
		port = "8081"
	}
	return "http://localhost:" + port
}

func (c CallerContext) fields() map[string]any {
	return map[string]any{
		"caller_service":        strings.TrimSpace(c.CallerService),
		"caller_deployment_url": strings.TrimSpace(c.CallerDeploymentURL),
		"actor":                 strings.TrimSpace(c.Actor),
	}
}

func mergeCaller(body map[string]any, caller CallerContext) map[string]any {
	if body == nil {
		body = map[string]any{}
	}
	for k, v := range caller.fields() {
		if s, ok := v.(string); ok && s != "" {
			body[k] = s
		}
	}
	return body
}
