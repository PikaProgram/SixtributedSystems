package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/kafka"
	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
)

type reportEnvelope struct {
	ReportID      string          `json:"report_id"`
	ReceivedAt    string          `json:"received_at"`
	Payload       json.RawMessage `json:"payload"`
	CorrelationID string          `json:"correlation_id"`
}

type stats struct {
	total      atomic.Int64
	duplicates atomic.Int64
}

func main() {
	logger := platform.Logger("report-consumer")
	shutdownTracer, err := platform.InitTracer("report-consumer")
	if err != nil {
		logger.Error("tracer_init", "error", err)
	} else {
		defer shutdownTracer()
	}
	shutdownMetrics, err := platform.InitMetrics("report-consumer")
	if err != nil {
		logger.Error("metrics_init", "error", err)
	} else {
		defer shutdownMetrics()
	}
	st := &stats{}
	db, err := pgxpool.New(context.Background(), platform.Env("REPORT_POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/field_reports?sslmode=disable"))
	for err != nil {
		logger.Error("postgres_pool", "error", err)
		time.Sleep(2 * time.Second)
		db, err = pgxpool.New(context.Background(), platform.Env("REPORT_POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/field_reports?sslmode=disable"))
	}
	defer db.Close()
	if _, err := db.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS field_reports (
		report_id TEXT PRIMARY KEY,
		received_at TIMESTAMPTZ NOT NULL,
		payload JSONB NOT NULL,
		processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		correlation_id TEXT NOT NULL
	)`); err != nil {
		logger.Error("create_table", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		platform.JSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "report-consumer"})
	})
	mux.HandleFunc("/internal/reports/stats", func(w http.ResponseWriter, _ *http.Request) {
		platform.JSON(w, http.StatusOK, map[string]int64{
			"total":      st.total.Load(),
			"duplicates": st.duplicates.Load(),
		})
	})
	go func() {
		if err := http.ListenAndServe(":"+platform.Env("HEALTH_PORT", "8083"), platform.HTTPMiddleware(logger, mux)); err != nil {
			logger.Error("health_server", "error", err)
		}
	}()

	brokers := platform.EnvCSV("KAFKA_BROKERS", "localhost:9092")
	group, err := kafka.NewConsumerGroup(brokers, platform.Env("REPORT_CONSUMER_GROUP", "report-consumer"), []string{"field-reports"})
	if err != nil {
		logger.Error("consumer_group", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	for {
		err := group.Consume(ctx, func(ctx context.Context, msg kafka.Message) error {
			var report reportEnvelope
			if err := json.Unmarshal(msg.Value, &report); err != nil {
				logger.Error("invalid_report", "error", err) // poison message: commit and skip
				return nil
			}
			processedAt := time.Now().UTC()
			receivedAt, err := time.Parse(time.RFC3339Nano, report.ReceivedAt)
			if err != nil {
				receivedAt = processedAt
			}
			tag, err := db.Exec(ctx, `INSERT INTO field_reports
				(report_id, received_at, payload, processed_at, correlation_id)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (report_id) DO NOTHING`,
				report.ReportID, receivedAt, report.Payload, processedAt, report.CorrelationID)
			if err != nil {
				logger.Error("insert_failed", "error", err, "correlation_id", report.CorrelationID)
				return err // offset not committed; redelivered
			}
			if tag.RowsAffected() == 0 {
				dupes := st.duplicates.Add(1)
				platform.RecordFieldReportDuplicate()
				logger.Info("duplicate_report", "report_id", report.ReportID,
					"duplicates", dupes, "correlation_id", report.CorrelationID)
			} else {
				total := st.total.Add(1)
				logger.Info("report_persisted", "report_id", report.ReportID,
					"total", total, "correlation_id", report.CorrelationID)
			}
			return nil
		})
		if err != nil {
			logger.Error("consumer_cycle", "error", err)
		}
		if ctx.Err() != nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
}
