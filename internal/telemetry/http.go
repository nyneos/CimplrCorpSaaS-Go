package telemetry

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// WrapClient wraps an http.Client with OpenTelemetry instrumentation.
// This ensures that outbound HTTP requests made by this client will
// have spans created and trace context injected into their headers.
func WrapClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}

	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	// Wrap the transport with otelhttp
	client.Transport = otelhttp.NewTransport(transport)
	return client
}
