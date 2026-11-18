package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/kafka"
	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
)

type HazardEvent struct {
	HazardID   string         `json:"hazard_id"`
	HazardType string         `json:"hazard_type"`
	Severity   string         `json:"severity"`
	AreaName   string         `json:"area_name"`
	OccurredAt time.Time      `json:"occurred_at"`
	Attributes map[string]any `json:"attributes"`
}

// correlation_id lives at the envelope top level, next to event_type (M1 bug fix).
type envelope struct {
	EventType     string      `json:"event_type"`
	CorrelationID string      `json:"correlation_id"`
	Event         HazardEvent `json:"event"`
}

func main() {
	service := platform.Env("CONSUMER_NAME", "consumer")
	logger := platform.Logger(service)
	shutdownTracer, err := platform.InitTracer(service)
	if err != nil {
		logger.Error("tracer_init", "error", err)
	} else {
		defer shutdownTracer()
	}
	shutdownMetrics, err := platform.InitMetrics(service)
	if err != nil {
		logger.Error("metrics_init", "error", err)
	} else {
		defer shutdownMetrics()
	}
	notifier := service == "notifier"

	var db *pgxpool.Pool
	if notifier {
		var err error
		db, err = pgxpool.New(
			context.Background(),
			platform.Env("NOTIFIER_POSTGRES_DSN",
				"postgres://postgres:postgres@localhost:5432/notifications?sslmode=disable"),
		)
		if err != nil {
			logger.Error("postgres_pool", "error", err)
			return
		}
		defer db.Close()
		_, _ = db.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS notifications (
			hazard_id TEXT PRIMARY KEY, severity TEXT NOT NULL, hazard_type TEXT NOT NULL,
			area_name TEXT NOT NULL, occurred_at TIMESTAMPTZ NOT NULL, critical BOOLEAN NOT NULL,
			correlation_id TEXT NOT NULL, notified_at TIMESTAMPTZ NOT NULL
		)`)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		platform.JSON(w, http.StatusOK, map[string]string{"status": "ok", "service": service})
	})
	if notifier {
		mux.HandleFunc("/internal/notifications", func(w http.ResponseWriter, r *http.Request) {
			limit := 100
			if raw := r.URL.Query().Get("limit"); raw != "" {
				if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 1000 {
					limit = parsed
				}
			}
			rows, err := db.Query(r.Context(), `SELECT hazard_id, severity, hazard_type, area_name,
				occurred_at, critical, correlation_id, notified_at
				FROM notifications ORDER BY notified_at DESC LIMIT $1`, limit)
			if err != nil {
				platform.JSON(w, http.StatusInternalServerError,
					map[string]string{"error": "store unavailable"})
				return
			}
			defer rows.Close()
			items := []map[string]any{}
			for rows.Next() {
				var (
					hazardID, severity, hazardType, areaName, correlationID string
					occurredAt, notifiedAt                                  time.Time
					critical                                                bool
				)
				if err := rows.Scan(&hazardID, &severity, &hazardType, &areaName,
					&occurredAt, &critical, &correlationID, &notifiedAt); err != nil {
					continue
				}
				items = append(items, map[string]any{
					"hazard_id": hazardID, "severity": severity, "hazard_type": hazardType,
					"area_name": areaName, "occurred_at": occurredAt.UTC().Format(time.RFC3339),
					"critical": critical, "correlation_id": correlationID,
					"notified_at": notifiedAt.UTC().Format(time.RFC3339),
				})
			}
			platform.JSON(w, http.StatusOK, map[string]any{"items": items})
		})
	}
	go func() {
		_ = http.ListenAndServe(":"+platform.Env("HEALTH_PORT", "8083"), platform.HTTPMiddleware(logger, mux))
	}()

	brokers := strings.Split(platform.Env("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	for {
		group, err := kafka.NewConsumerGroup(brokers, service+".group", []string{"hazard.events"})
		if err != nil {
			logger.Error("consumer_group", "error", err)
			time.Sleep(2 * time.Second)
			continue
		}
		logger.Info("started", "brokers", brokers, "group", service+".group", "topic", "hazard.events")
		err = group.Consume(context.Background(), func(_ context.Context, msg kafka.Message) error {
			return handle(service, notifier, db, logger, msg.Value)
		})
		_ = group.Close()
		if err != nil {
			logger.Error("consumer_cycle", "error", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func handle(service string, notifier bool, db *pgxpool.Pool, logger *slog.Logger, body []byte) error {
	var event envelope
	if err := json.Unmarshal(body, &event); err != nil {
		// bad envelope: log and commit; aggregator owns poison/unprocessed handling
		logger.Error("invalid_event", "error", err)
		return nil
	}
	if notifier && db != nil {
		critical := event.Event.Severity == "AWAS" ||
			(event.Event.HazardType == "COASTAL" &&
				event.Event.Attributes["critical"] == true)
		tag, err := db.Exec(context.Background(), `INSERT INTO notifications(
			hazard_id, severity, hazard_type, area_name, occurred_at, critical, correlation_id, notified_at
			) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (hazard_id) DO NOTHING`,
			event.Event.HazardID, event.Event.Severity, event.Event.HazardType,
			event.Event.AreaName, event.Event.OccurredAt, critical,
			event.CorrelationID, time.Now().UTC(),
		)
		if err != nil {
			return err // offset not committed; retried after reconnect
		}
		logger.Info("event_processed", "event_type", event.EventType,
			"hazard_id", event.Event.HazardID, "correlation_id", event.CorrelationID,
			"critical", critical, "duplicate", tag.RowsAffected() == 0)
		return nil
	}
	logger.Info("event_processed", "event_type", event.EventType,
		"hazard_id", event.Event.HazardID, "correlation_id", event.CorrelationID)
	return nil
}
