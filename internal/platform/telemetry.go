// Observability helpers wrapping OpenTelemetry (M2 Problem 6).
//
// Traces: OTLP/HTTP exporter to OTEL_EXPORTER_OTLP_ENDPOINT (default
// http://tempo.observability:4318). Metrics: Prometheus exporter served at
// /metrics on the same port as /health by HTTPMiddleware — no extra port to
// publish; Prometheus just scrapes the existing service port.
package platform

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	serviceName string
	meter       = otel.Meter("sixtributed")
	promHandler http.Handler

	requestsTotal = mustCounter("http_server_requests_total",
		"Requests handled, by service, path and HTTP status code.")
	requestDuration = mustHistogram("http_server_request_duration_ms",
		"Request duration in milliseconds.")
	outboundDuration = mustHistogram("outbound_duration_ms",
		"Outbound call duration in milliseconds.")
	pollSuccessTotal = mustCounter("aggregator_poll_success_total",
		"Successful upstream polls, by source.")
	pollErrorTotal = mustCounter("aggregator_poll_error_total",
		"Failed upstream polls, by source.")
	fireBuilderLag = mustGauge("fire_builder_lag_seconds",
		"now - max(last_update) of fire clusters processed.")
	coastalBreakerState = mustGauge("coastal_breaker_state",
		"Circuit breaker state: 0 closed, 1 half-open, 2 open.")
	coastalViolationsTotal = mustCounter("coastal_violations_total",
		"Rate-limited coastal sensor requests rejected by the breaker.")
	fieldReportDuplicates = mustCounter("field_reports_duplicates_total",
		"Duplicate field reports dropped by report-consumer.")
)

func mustCounter(name, desc string) metric.Int64Counter {
	counter, err := meter.Int64Counter(name, metric.WithDescription(desc))
	if err != nil {
		panic(err) // invalid instrument name is a programming error
	}
	return counter
}

func mustHistogram(name, desc string) metric.Float64Histogram {
	histogram, err := meter.Float64Histogram(name, metric.WithDescription(desc),
		metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}
	return histogram
}

func mustGauge(name, desc string) metric.Int64Gauge {
	gauge, err := meter.Int64Gauge(name, metric.WithDescription(desc))
	if err != nil {
		panic(err)
	}
	return gauge
}

func attr(pairs ...string) metric.MeasurementOption {
	attrs := make([]attribute.KeyValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		attrs = append(attrs, attribute.String(pairs[i], pairs[i+1]))
	}
	return metric.WithAttributes(attrs...)
}

// InitTracer installs a global TracerProvider exporting spans via OTLP/HTTP.
// Returns a shutdown func that flushes pending spans.
func InitTracer(service string) (func(), error) {
	ctx := context.Background()
	serviceName = service
	endpoint := Env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo.observability:4318")
	insecure := !strings.HasPrefix(endpoint, "https://")
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")),
		otlptracehttp.WithInsecure(),
	)
	if insecure {
		// already configured above; https uses default TLS
	}
	if err != nil {
		return func() {}, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL, semconv.ServiceName(service)))
	if err != nil {
		return func() {}, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return func() { _ = tp.Shutdown(context.Background()) }, nil
}

// InitMetrics installs a global MeterProvider backed by the Prometheus
// exporter. HTTPMiddleware serves the collector at /metrics.
func InitMetrics(service string) (func(), error) {
	exporter, err := prometheus.New()
	if err != nil {
		return func() {}, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(resource.NewSchemaless(semconv.ServiceName(service))),
	)
	otel.SetMeterProvider(mp)
	promHandler = promhttp.Handler()
	return func() { _ = mp.Shutdown(context.Background()) }, nil
}

// HTTPMiddleware combines CorrelationMiddleware (correlation id + access log)
// with otelhttp tracing and requests_total / duration recording. The server
// span covers the whole request; correlation middleware runs inside it.
func HTTPMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	traced := otelhttp.NewHandler(
		CorrelationMiddleware(logger, next), "http.server",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			if promHandler != nil {
				promHandler.ServeHTTP(w, r)
			} else {
				http.NotFound(w, r)
			}
			return
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		started := time.Now()
		traced.ServeHTTP(rec, r)
		if path := r.URL.Path; path != "/health" { // don't count probe scrapes
			labels := attr("service", serviceName, "path", path, "code", strconv.Itoa(rec.status))
			requestsTotal.Add(r.Context(), 1, labels)
			requestDuration.Record(r.Context(),
				float64(time.Since(started).Milliseconds()), labels)
		}
	})
}

// statusRecorder captures the status code handlers write.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = http.StatusOK, true
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// OutboundSpan starts a client span for an outbound call.
func OutboundSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return otel.Tracer("sixtributed").Start(ctx, "outbound "+operation)
}

// Metric hooks for services without a natural instrumentation point.
// Additive: call from wherever the value is already known.

func RecordPollSuccess(source string) {
	pollSuccessTotal.Add(context.Background(), 1, attr("source", source))
}

func RecordPollError(source string) {
	pollErrorTotal.Add(context.Background(), 1, attr("source", source))
}

func SetFireBuilderLag(seconds float64) {
	fireBuilderLag.Record(context.Background(), int64(seconds))
}

func SetCoastalBreakerState(state int64) {
	coastalBreakerState.Record(context.Background(), state)
}

func RecordCoastalViolation() {
	coastalViolationsTotal.Add(context.Background(), 1)
}

func RecordFieldReportDuplicate() {
	fieldReportDuplicates.Add(context.Background(), 1)
}
