package main

import (
	"context"
	"log/slog"

	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/kafka"
	"github.com/astolfo/sixtributedsystems/internal/platform"
)

const (
	kafkaTopic = "hazard.events"
)

type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}
type SeismicEvent struct {
	EventID          string          `json:"event_id"`
	Magnitude        float64         `json:"magnitude"`
	DepthKM          float64         `json:"depth_km"`
	EpicenterLat     float64         `json:"epicenter_lat"`
	EpicenterLon     float64         `json:"epicenter_lon"`
	RegionName       string          `json:"region_name"`
	OccurredAt       time.Time       `json:"occurred_at"`
	PotentialTsunami bool            `json:"potential_tsunami"`
	Extras           json.RawMessage `json:"-"`
}
type VolcanicReport struct {
	ReportID         string          `json:"report_id"`
	VolcanoID        string          `json:"volcano_id"`
	AlertLevel       string          `json:"alert_level"`
	EruptionCount24h int             `json:"eruption_count_24h"`
	AshColumnHeightM float64         `json:"ash_column_height_m"`
	ReportedAt       time.Time       `json:"reported_at"`
	ConfidenceLevel  *float64        `json:"confidence_level,omitempty"`
	Extras           json.RawMessage `json:"-"`
}
type VolcanoReference struct {
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}
type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}
type sourceStatus struct {
	LastSuccess *time.Time `json:"last_success,omitempty"`
	LastError   *time.Time `json:"last_error,omitempty"`
	Error       string     `json:"error,omitempty"`
}
type aggregator struct {
	logger   *slog.Logger
	kafka    *kafka.Producer
	ring     *ring
	stores   *storeClient
	unproc   *unprocessedStore
	leader   *leader
	statuses struct {
		sync.RWMutex
		bmkg, pvmbg sourceStatus
	}
	bmkgURL, pvmbgURL, bmkgKey, pvmbgToken string
	interval, timeout                      time.Duration
	bucket                                 time.Duration
	volcanoes                              map[string]VolcanoReference
}

func main() {
	logger := platform.Logger("aggregator")
	shutdownTracer, errTracer := platform.InitTracer("aggregator")
	if errTracer != nil {
		logger.Error("tracer_init", "error", errTracer)
	} else {
		defer shutdownTracer()
	}
	shutdownMetrics, errMetrics := platform.InitMetrics("aggregator")
	if errMetrics != nil {
		logger.Error("metrics_init", "error", errMetrics)
	} else {
		defer shutdownMetrics()
	}
	ctx := context.Background()

	unproc, err := newUnprocessedStore(ctx, platform.Env(
		"AGGREGATOR_POSTGRES_DSN",
		"postgres://postgres:postgres@localhost:5433/aggregator?sslmode=disable",
	))
	if err != nil {
		logger.Error("postgres_pool", "error", err)
		return
	}
	defer unproc.Close()

	brokers := strings.Split(platform.Env("KAFKA_BROKERS", "localhost:9092"), ",")
	volcanoes, err := loadVolcanoReferences(
		platform.Env("VOLCANO_REFERENCE_PATH", "data/volcanoes.json"),
	)
	if err != nil {
		logger.Error("volcano_reference_load", "error", err)
		return
	}
	namespace := platform.Env("POD_NAMESPACE", "default")
	ring := newRing(logger)
	go ring.watch(namespace, platform.Env("STORE_LABEL_SELECTOR", "role=primary,app=store"))

	l := &leader{}
	a := &aggregator{
		logger: logger, kafka: kafka.NewProducer(brokers), ring: ring, unproc: unproc,
		stores: newStoreClient(logger), leader: l, volcanoes: volcanoes,
		bmkgURL:  platform.Env("BMKG_URL", "http://localhost:8081"),
		pvmbgURL: platform.Env("PVMBG_URL", "http://localhost:8082"),
		bmkgKey:  platform.Env("BMKG_API_KEY", "example-bmkg-key"),
		pvmbgToken: platform.Env(
			"PVMBG_TOKEN",
			"example-pvmbg-token",
		),
		interval: platform.EnvDurationSeconds("POLL_INTERVAL_SECONDS", 3*time.Second),
		timeout:  platform.EnvDurationSeconds("SOURCE_TIMEOUT_SECONDS", 4*time.Second),
		bucket:   platform.EnvDurationSeconds("ANALYTICS_BUCKET", time.Hour),
	}

	// only the leader polls; pollers run in the leader ctx, cancelled on lease loss
	startLeaderElection(logger, l, namespace, func(ctx context.Context) {
		go a.pollBMKG(ctx)
		go a.pollPVMBG(ctx)
		go a.runRetries(ctx)
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("GET /api/status", a.status)
	audit := http.NewServeMux()
	audit.HandleFunc("GET /internal/audit/events", a.auditEvents)
	audit.HandleFunc("GET /internal/audit/checksum", a.auditChecksum)
	audit.HandleFunc("GET /internal/audit/location", a.auditLocation)
	audit.HandleFunc("GET /internal/audit/unprocessed", a.auditUnprocessed)
	mux.Handle("GET /internal/audit/", auditToken(platform.Env("AUDIT_TOKEN", ""), audit))
	mux.HandleFunc("GET /internal/analytics", a.analytics)
	admin := http.NewServeMux()
	admin.HandleFunc("POST /internal/admin/replay", a.adminReplay)
	admin.HandleFunc("GET /internal/admin/replay/stats", a.adminReplayStats)
	mux.Handle("/internal/admin/", auditToken(platform.Env("AUDIT_TOKEN", ""), admin))

	port := platform.Env("AGGREGATOR_PORT", "8090")
	logger.Info("starting", "port", port)
	if err := http.ListenAndServe(
		":"+port,
		platform.HTTPMiddleware(logger, mux),
	); err != nil {
		logger.Error("http_server", "error", err)
	}
}

func lampiranError(w http.ResponseWriter, status int, code, message string) {
	platform.JSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}

func (a *aggregator) health(w http.ResponseWriter, _ *http.Request) {
	platform.JSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "aggregator"})
}

func (a *aggregator) status(w http.ResponseWriter, _ *http.Request) {
	a.statuses.RLock()
	defer a.statuses.RUnlock()
	platform.JSON(w, http.StatusOK, map[string]any{
		"bmkg": a.statuses.bmkg, "pvmbg": a.statuses.pvmbg,
		"leader": a.leader.IsLeader(),
	})
}

func (a *aggregator) recordSuccess(source string) {
	platform.RecordPollSuccess(source)
	now := time.Now().UTC()
	a.statuses.Lock()
	defer a.statuses.Unlock()
	if source == "bmkg" {
		a.statuses.bmkg.LastSuccess = &now
	} else {
		a.statuses.pvmbg.LastSuccess = &now
	}
}

func (a *aggregator) recordError(source string, err error) {
	platform.RecordPollError(source)
	now := time.Now().UTC()
	a.statuses.Lock()
	defer a.statuses.Unlock()
	if source == "bmkg" {
		a.statuses.bmkg.LastError = &now
		a.statuses.bmkg.Error = err.Error()
	} else {
		a.statuses.pvmbg.LastError = &now
		a.statuses.pvmbg.Error = err.Error()
	}
}

func loadVolcanoReferences(path string) (map[string]VolcanoReference, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read volcano references: %w", err)
	}
	references := map[string]VolcanoReference{}
	if err := json.Unmarshal(raw, &references); err != nil {
		return nil, fmt.Errorf("parse volcano references: %w", err)
	}
	return references, nil
}

func (a *aggregator) volcano(id string) (string, float64, float64) {
	reference, ok := a.volcanoes[id]
	if !ok {
		return id, 0, 0
	}
	return reference.Name, reference.Latitude, reference.Longitude
}

// stableID derives a deterministic hazard_id from source and source_ref_id.
func stableID(source, ref string) string {
	sum := sha256.Sum256([]byte(source + ":" + ref))
	return hex.EncodeToString(sum[:])
}

var errStoreWrite = errors.New("store write failed")
