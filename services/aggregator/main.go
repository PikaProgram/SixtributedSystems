package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rabbitmq/amqp091-go"
)

type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}
type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"`
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
}
type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
	ConfidenceLevel  *float64  `json:"confidence_level,omitempty"`
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
type logger interface {
	Info(string, ...any)
	Error(string, ...any)
}
type aggregator struct {
	db       *pgxpool.Pool
	broker   *amqp091.Channel
	logger   logger
	statuses struct {
		sync.RWMutex
		bmkg, pvmbg sourceStatus
	}
	bmkgURL, pvmbgURL, bmkgKey, pvmbgToken string
	interval, timeout                      time.Duration
	volcanoes                              map[string]VolcanoReference
}

func main() {
	logger := platform.Logger("aggregator")
	ctx := context.Background()
	dsn := platform.Env(
		"POSTGRES_DSN",
		"postgres://postgres:postgres@localhost:5432/canonical?sslmode=disable",
	)
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("postgres_pool", "error", err)
		return
	}
	defer db.Close()
	_, _ = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS hazard_events (
        hazard_id TEXT PRIMARY KEY, source TEXT NOT NULL, source_ref_id TEXT NOT NULL,
        hazard_type TEXT NOT NULL, severity TEXT NOT NULL, area_name TEXT NOT NULL,
        latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
        occurred_at TIMESTAMPTZ NOT NULL, ingested_at TIMESTAMPTZ NOT NULL,
        attributes JSONB NOT NULL DEFAULT '{}', UNIQUE(source, source_ref_id)
    )`)
	_, _ = db.Exec(ctx, `CREATE INDEX IF NOT EXISTS hazard_events_type_time_idx
        ON hazard_events(hazard_type, occurred_at DESC)`)
	broker, _ := amqp091.Dial(platform.Env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
	var channel *amqp091.Channel
	if broker != nil {
		channel, _ = broker.Channel()
		if channel != nil {
			_ = channel.ExchangeDeclare("hazard.events", "topic", true, false, false, false, nil)
		}
	}
	volcanoes, err := loadVolcanoReferences(
		platform.Env("VOLCANO_REFERENCE_PATH", "data/volcanoes.json"),
	)
	if err != nil {
		logger.Error("volcano_reference_load", "error", err)
		return
	}
	a := &aggregator{
		db: db, broker: channel, logger: logger, volcanoes: volcanoes,
		bmkgURL:    platform.Env("BMKG_URL", "http://localhost:8081"),
		pvmbgURL:   platform.Env("PVMBG_URL", "http://localhost:8082"),
		bmkgKey:    platform.Env("BMKG_API_KEY", "example-bmkg-key"),
		pvmbgToken: platform.Env("PVMBG_TOKEN", "example-pvmbg-token"),
		interval:   platform.EnvDurationSeconds("POLL_INTERVAL_SECONDS", 3*time.Second),
		timeout:    platform.EnvDurationSeconds("SOURCE_TIMEOUT_SECONDS", 4*time.Second),
	}
	go a.pollBMKG(ctx)
	go a.pollPVMBG(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.health)
	mux.HandleFunc("/api/hazards", a.hazards)
	mux.HandleFunc("/api/status", a.status)
	logger.Info("starting", "port", platform.Env("AGGREGATOR_PORT", "8090"))
	_ = http.ListenAndServe(
		":"+platform.Env("AGGREGATOR_PORT", "8090"),
		platform.CorrelationMiddleware(logger, mux),
	)
}

func (a *aggregator) get(
	ctx context.Context,
	url string,
	headers map[string]string,
	out any,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	started := time.Now()
	res, err := http.DefaultClient.Do(req)
	a.logger.Info(
		"outbound_call",
		"operation",
		url,
		"duration_ms",
		time.Since(started).Milliseconds(),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return fmt.Errorf("upstream %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(res.Body).Decode(out)
}
func (a *aggregator) pollBMKG(ctx context.Context) {
	cursor := time.Unix(0, 0)
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		cursor = a.fetchBMKG(ctx, cursor)
		<-ticker.C
	}
}
func (a *aggregator) fetchBMKG(ctx context.Context, cursor time.Time) time.Time {
	requestCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	var events []SeismicEvent
	var warnings []TsunamiWarning
	since := cursor.Format(time.RFC3339)
	err := a.get(
		requestCtx,
		a.bmkgURL+"/seismic-events?since="+since,
		map[string]string{"X-BMKG-Key": a.bmkgKey},
		&events,
	)
	if err == nil {
		err = a.get(
			requestCtx,
			a.bmkgURL+"/tsunami-warnings?since="+since,
			map[string]string{"X-BMKG-Key": a.bmkgKey},
			&warnings,
		)
	}
	if err != nil {
		a.recordError("bmkg", err)
		return cursor
	}
	warningMap := map[string]TsunamiWarning{}
	for _, w := range warnings {
		warningMap[w.RelatedEventID] = w
	}
	latest := cursor
	for _, event := range events {
		if event.OccurredAt.After(latest) {
			latest = event.OccurredAt
		}
		attrs := map[string]any{
			"magnitude":         event.Magnitude,
			"depth_km":          event.DepthKM,
			"potential_tsunami": event.PotentialTsunami,
		}
		severity := severityFor(event, warningMap[event.EventID])
		if warning, ok := warningMap[event.EventID]; ok {
			attrs["tsunami_warning"] = warning
		}
		_ = a.storeAndPublish(ctx, HazardEvent{
			HazardID: stableID("BMKG", event.EventID), Source: "BMKG", SourceRefID: event.EventID,
			HazardType: "SEISMIC", Severity: severity, AreaName: event.RegionName,
			Latitude: event.EpicenterLat, Longitude: event.EpicenterLon,
			OccurredAt: event.OccurredAt, IngestedAt: time.Now().UTC(), Attributes: attrs,
		})
	}
	a.recordSuccess("bmkg")
	return latest
}
func severityFor(event SeismicEvent, warning TsunamiWarning) string {
	switch warning.ThreatLevel {
	case "Awas":
		return "AWAS"
	case "Siaga":
		return "SIAGA"
	case "Waspada":
		return "WASPADA"
	}
	if event.Magnitude < 5 {
		return "NORMAL"
	}
	if event.Magnitude < 6.5 {
		return "WASPADA"
	}
	return "SIAGA"
}
func (a *aggregator) pollPVMBG(ctx context.Context) {
	cursor := time.Unix(0, 0)
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		requestCtx, cancel := context.WithTimeout(ctx, a.timeout)
		var reports []VolcanicReport
		err := a.get(
			requestCtx,
			a.pvmbgURL+"/volcanic-reports?since="+cursor.Format(time.RFC3339),
			map[string]string{"Authorization": "Bearer " + a.pvmbgToken},
			&reports,
		)
		cancel()
		if err != nil {
			a.recordError("pvmbg", err)
		} else {
			for _, r := range reports {
				if r.ReportedAt.After(cursor) {
					cursor = r.ReportedAt
				}
				attrs := map[string]any{
					"eruption_count_24h":  r.EruptionCount24h,
					"ash_column_height_m": r.AshColumnHeightM,
				}
				if r.ConfidenceLevel != nil {
					attrs["confidence_level"] = *r.ConfidenceLevel
				}
				name, lat, lon := a.volcano(r.VolcanoID)
				_ = a.storeAndPublish(ctx, HazardEvent{
					HazardID: stableID(
						"PVMBG",
						r.ReportID,
					),
					Source:      "PVMBG",
					SourceRefID: r.ReportID,
					HazardType:  "VOLCANIC",
					Severity:    strings.ToUpper(r.AlertLevel),
					AreaName:    name,
					Latitude:    lat,
					Longitude:   lon,
					OccurredAt:  r.ReportedAt,
					IngestedAt:  time.Now().UTC(),
					Attributes:  attrs,
				})
			}
			a.recordSuccess("pvmbg")
		}
		<-ticker.C
	}
}
func stableID(source, ref string) string {
	sum := sha256.Sum256([]byte(source + ":" + ref))
	return hex.EncodeToString(sum[:])
}
func (a *aggregator) storeAndPublish(ctx context.Context, event HazardEvent) error {
	attrs, _ := json.Marshal(event.Attributes)
	query := `INSERT INTO hazard_events(
        hazard_id, source, source_ref_id, hazard_type, severity, area_name,
        latitude, longitude, occurred_at, ingested_at, attributes
				) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
				ON CONFLICT(source,source_ref_id) DO UPDATE SET
        hazard_id=EXCLUDED.hazard_id, hazard_type=EXCLUDED.hazard_type,
        severity=EXCLUDED.severity, area_name=EXCLUDED.area_name,
        latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
        occurred_at=EXCLUDED.occurred_at, ingested_at=EXCLUDED.ingested_at,
        attributes=EXCLUDED.attributes RETURNING hazard_id`
	tag := ""
	err := a.db.QueryRow(ctx, query,
		event.HazardID, event.Source, event.SourceRefID, event.HazardType,
		event.Severity, event.AreaName, event.Latitude, event.Longitude,
		event.OccurredAt, event.IngestedAt, attrs,
	).Scan(&tag)
	if err != nil {
		return err
	}
	if a.broker != nil {
		payload, _ := json.Marshal(map[string]any{
			"event_type":     "hazard.created",
			"correlation_id": platform.CorrelationID(ctx),
			"event":          event,
		})
		err = a.broker.PublishWithContext(
			ctx,
			"hazard.events",
			"hazard.created",
			false,
			false,
			amqp091.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp091.Persistent,
				Body:         payload,
			},
		)
	}
	return err
}
func (a *aggregator) recordSuccess(source string) {
	now := time.Now().UTC()
	a.statuses.Lock()
	defer a.statuses.Unlock()
	if source == "bmkg" {
		a.statuses.bmkg.LastSuccess = &now
	} else {
		a.statuses.pvmbg.LastSuccess = &now
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
func (a *aggregator) recordError(source string, err error) {
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
func (a *aggregator) health(w http.ResponseWriter, _ *http.Request) {
	platform.JSON(w, 200, map[string]string{"status": "ok", "service": "aggregator"})
}
func (a *aggregator) status(w http.ResponseWriter, _ *http.Request) {
	a.statuses.RLock()
	defer a.statuses.RUnlock()
	platform.JSON(w, 200, map[string]any{"bmkg": a.statuses.bmkg, "pvmbg": a.statuses.pvmbg})
}
func (a *aggregator) hazards(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		fmt.Sscanf(raw, "%d", &limit)
	}
	args := []any{limit}
	query := `SELECT hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes FROM hazard_events`
	if source := r.URL.Query().Get("source"); source != "" {
		query += ` WHERE source = $2`
		args = append(args, strings.ToUpper(source))
	}
	query += ` ORDER BY occurred_at DESC LIMIT $1`
	rows, err := a.db.Query(r.Context(), query, args...)
	if err != nil {
		platform.JSON(w, 500, map[string]string{"error": "store unavailable"})
		return
	}
	defer rows.Close()
	events := []HazardEvent{}
	for rows.Next() {
		var e HazardEvent
		var attrs []byte
		if err := rows.Scan(
			&e.HazardID, &e.Source, &e.SourceRefID, &e.HazardType, &e.Severity,
			&e.AreaName, &e.Latitude, &e.Longitude, &e.OccurredAt, &e.IngestedAt, &attrs,
		); err != nil {
			continue
		}
		_ = json.Unmarshal(attrs, &e.Attributes)
		events = append(events, e)
	}
	result := map[string]any{"events": events, "count": len(events)}
	a.statuses.RLock()
	pvmbg := a.statuses.pvmbg
	a.statuses.RUnlock()
	if pvmbg.LastError != nil &&
		(pvmbg.LastSuccess == nil || pvmbg.LastError.After(*pvmbg.LastSuccess)) {
		result["stale_since"] = pvmbg.LastError
	}
	platform.JSON(w, 200, result)
}
