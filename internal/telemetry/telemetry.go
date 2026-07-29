package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"google.golang.org/grpc/credentials"
)

// InitTelemetry initializes OpenTelemetry with Better Stack integration.
// It configures OTLP trace and metric exporters based on environment variables.
func InitTelemetry(ctx context.Context) (func(context.Context) error, error) {
	// 1. Setup Resource
	res, err := newResource()
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// 2. Setup Propagators
	// We want to extract and inject trace context using W3C Trace Context and Baggage
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	// Setup shutdown functions to execute when application stops
	var shutdownFuncs []func(context.Context) error
	shutdown := func(shutdownCtx context.Context) error {
		var errs []string
		for i := len(shutdownFuncs) - 1; i >= 0; i-- {
			if err := shutdownFuncs[i](shutdownCtx); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("telemetry shutdown errors: %s", strings.Join(errs, ", "))
		}
		return nil
	}

	// If Better Stack token is missing, we don't start the exporters,
	// but we can still initialize no-op/local traces if needed.
	token := os.Getenv("BETTERSTACK_SOURCE_TOKEN")
	if token == "" {
		fmt.Println("[Telemetry] BETTERSTACK_SOURCE_TOKEN not set. Exporters will not be initialized.")
		return shutdown, nil
	}

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "in-otel.logs.betterstack.com"
	}
	// Depending on user input, it might include https:// prefix or not. The OTLP HTTP exporter requires the host.
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", token),
	}

	// 3. Setup Tracer Provider
	tracerProvider, err := newTracerProvider(ctx, res, endpoint, headers)
	if err != nil {
		return shutdown, fmt.Errorf("failed to create tracer provider: %w", err)
	}
	otel.SetTracerProvider(tracerProvider)
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)

	// 4. Setup Meter Provider
	meterProvider, err := newMeterProvider(ctx, res, endpoint, headers)
	if err != nil {
		return shutdown, fmt.Errorf("failed to create meter provider: %w", err)
	}
	otel.SetMeterProvider(meterProvider)
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)

	return shutdown, nil
}

func newResource() (*resource.Resource, error) {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "cimplrcorpsaas"
	}
	serviceVersion := os.Getenv("OTEL_SERVICE_VERSION")
	if serviceVersion == "" {
		serviceVersion = "1.0.0"
	}
	environment := os.Getenv("OTEL_ENVIRONMENT")
	if environment == "" {
		environment = "production"
	}

	return resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
			semconv.DeploymentEnvironment(environment),
		),
	)
}

func newTracerProvider(ctx context.Context, res *resource.Resource, endpoint string, headers map[string]string) (*sdktrace.TracerProvider, error) {
	// Initialize OTLP gRPC trace exporter
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, "")),
		otlptracegrpc.WithHeaders(headers),
	)
	if err != nil {
		return nil, err
	}

	// Sampling configuration
	var sampler sdktrace.Sampler
	if strings.EqualFold(os.Getenv("DEVEL_MODE"), "true") {
		sampler = sdktrace.AlwaysSample()
	} else {
		// Use TraceIDRatioBased or ParentBased depending on environment variables.
		// For now, if no specific environment var dictates differently, ParentBased(AlwaysSample) is a sensible default.
		sampler = sdktrace.ParentBased(sdktrace.AlwaysSample())
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter, sdktrace.WithBatchTimeout(time.Second*5))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	return tp, nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource, endpoint string, headers map[string]string) (*sdkmetric.MeterProvider, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, "")),
		otlpmetricgrpc.WithHeaders(headers),
	)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(15*time.Second))),
	)
	return mp, nil
}
