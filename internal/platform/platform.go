package platform

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const correlationKey contextKey = "correlation_id"

func Env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func EnvInt(key string, fallback int) int {
	value, err := strconv.Atoi(Env(key, strconv.Itoa(fallback)))
	if err != nil {
		return fallback
	}
	return value
}

func EnvDurationSeconds(key string, fallback time.Duration) time.Duration {
	value, err := strconv.Atoi(Env(key, strconv.Itoa(int(fallback/time.Second))))
	if err != nil {
		return fallback
	}
	return time.Duration(value) * time.Second
}

func CorrelationID(ctx context.Context) string {
	if value, ok := ctx.Value(correlationKey).(string); ok && value != "" {
		return value
	}
	return ""
}

func WithCorrelationID(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, correlationKey, value)
}

func CorrelationMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := r.Header.Get("X-Correlation-ID")
		if correlationID == "" {
			correlationID = uuid.NewString()
		}
		ctx := WithCorrelationID(r.Context(), correlationID)
		w.Header().Set("X-Correlation-ID", correlationID)
		started := time.Now()
		next.ServeHTTP(w, r.WithContext(ctx))
		logger.Info("request_complete", "correlation_id", correlationID, "method", r.Method,
			"path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
	})
}

func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func Logger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("service", service)
}

func Outbound(logger *slog.Logger, ctx context.Context, operation string, fn func() error) error {
	started := time.Now()
	err := fn()
	logger.Info("outbound_call", "correlation_id", CorrelationID(ctx), "operation", operation,
		"duration_ms", time.Since(started).Milliseconds(), "error", err)
	return err
}
