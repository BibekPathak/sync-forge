package observability

import (
	"context"
	"net/http"
	"strings"
	"time"

	promcli "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitMetrics builds a Prometheus-backed OTel meter provider for a service.
// Returns an HTTP handler serving /metrics and a shutdown func.
func InitMetrics(ctx context.Context, service string) (http.Handler, func(), error) {
	exporter, err := prometheus.New(
		prometheus.WithRegisterer(promcli.DefaultRegisterer),
	)
	if err != nil {
		return nil, nil, err
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(service),
	)

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
	)
	otel.SetMeterProvider(provider)

	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	}
	return promhttp.Handler(), shutdown, nil
}

// InitTracing configures the OpenTelemetry trace provider. When endpoint is
// empty a no-op provider is installed, so tracing is a pure opt-in feature
// (enabled by OTEL_EXPORTER_OTLP_ENDPOINT). Returns a shutdown func.
func InitTracing(ctx context.Context, service, endpoint string) (func(), error) {
	noop := func() {}
	if endpoint == "" {
		otel.SetTracerProvider(sdktrace.NewTracerProvider())
		return noop, nil
	}
	// OTLP over HTTP routes each signal to a path under the endpoint; the
	// exporter posts traces to {endpoint}/v1/traces. Append the signal path
	// when the configured endpoint does not already carry one, so a bare
	// "http://collector:4318" reaches the receiver instead of 404ing.
	endpoint = strings.TrimSuffix(endpoint, "/")
	if !strings.HasSuffix(endpoint, "/v1/traces") {
		endpoint += "/v1/traces"
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return noop, err
	}
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(service),
	)
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(shutdownCtx)
	}, nil
}

// ServiceMetrics centralizes the counters/histograms every service records.
type ServiceMetrics struct {
	Meter        metric.Meter
	HTTPRequests metric.Int64Counter
	HTTPDuration metric.Float64Histogram
	HTTPInflight metric.Int64UpDownCounter
}

func NewServiceMetrics(meter metric.Meter) (*ServiceMetrics, error) {
	requests, err := meter.Int64Counter("http_requests_total",
		metric.WithDescription("Total HTTP requests handled"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("http_request_duration_seconds",
		metric.WithDescription("HTTP request latency in seconds"))
	if err != nil {
		return nil, err
	}
	inflight, err := meter.Int64UpDownCounter("http_inflight_requests",
		metric.WithDescription("Currently in-flight HTTP requests"))
	if err != nil {
		return nil, err
	}
	return &ServiceMetrics{
		Meter:        meter,
		HTTPRequests: requests,
		HTTPDuration: duration,
		HTTPInflight: inflight,
	}, nil
}
