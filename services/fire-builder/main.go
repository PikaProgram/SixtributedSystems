// fire-builder consumes detections.raw and clusters VIIRS hotspot detections
// into fires (≤1.0 km, ≤360 min, chained transitively; cluster >= 2 = fire).
// All state lives in its own Postgres (FIRE_POSTGRES_DSN); every message is
// processed in one transaction and the Kafka offset is committed only after
// the store persist + hazard.created publish succeeded, so redelivery after a
// crash is idempotent (detection_key upsert + store ON CONFLICT DO NOTHING).
//
// shortcut: fire-builder has no shard ring, so HazardEvents are POSTed to a
// single store unit (env STORE_URL); if stores are sharded, wire a ring here.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/kafka"
	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	rawTopic      = "detections.raw"
	hazardTopic   = "hazard.events"
	sourceName    = "SATELIT"
	storeTimeout  = 5 * time.Second
	graceMins     = 30.0
	minClusterLen = 2
)

type logger = *slog.Logger

type builder struct {
	db        *pgxpool.Pool
	kafkaProd *kafka.Producer
	logger    logger
	client    *http.Client
	storeURL  string

	lateDetections atomic.Int64
	activeFires    atomic.Int64
	closedFires    atomic.Int64
}

type rawMessage struct {
	Type       string          `json:"type"`
	Offset     int64           `json:"offset"`
	ReceivedAt string          `json:"received_at"`
	Detection  json.RawMessage `json:"detection"`
}

type province struct {
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func main() {
	logger := platform.Logger("fire-builder")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, platform.Env(
		"FIRE_POSTGRES_DSN",
		"postgres://postgres:postgres@localhost:5433/fire_builder?sslmode=disable",
	))
	for err != nil {
		logger.Error("postgres_pool", "error", err)
		time.Sleep(2 * time.Second)
		db, err = pgxpool.New(ctx, platform.Env(
			"FIRE_POSTGRES_DSN",
			"postgres://postgres:postgres@localhost:5433/fire_builder?sslmode=disable",
		))
	}
	defer db.Close()
	for _, stmt := range schema {
		if _, err := db.Exec(ctx, stmt); err != nil {
			logger.Error("schema_create", "error", err)
			os.Exit(1)
		}
	}

	provinces, err := loadProvinces(platform.Env("PROVINCE_REFERENCE_PATH", "/data/provinces.json"))
	if err != nil {
		logger.Error("province_reference_load", "error", err)
		os.Exit(1)
	}

	b := &builder{
		db:        db,
		kafkaProd: kafka.NewProducer(platform.EnvCSV("KAFKA_BROKERS", "localhost:9092")),
		logger:    logger,
		client:    &http.Client{Timeout: storeTimeout},
		storeURL:  strings.TrimRight(platform.Env("STORE_URL", "http://localhost:8100"), "/"),
	}
	b.refreshCounters(ctx)

	go b.serveHTTP()

	brokers := platform.EnvCSV("KAFKA_BROKERS", "localhost:9092")
	for {
		group, err := kafka.NewConsumerGroup(brokers,
			platform.Env("FIRE_BUILDER_GROUP", "fire-builder"), []string{rawTopic})
		if err != nil {
			logger.Error("consumer_group", "error", err)
		} else {
			logger.Info("started", "brokers", brokers, "group", "fire-builder", "topic", rawTopic)
			err = group.Consume(ctx, func(ctx context.Context, msg kafka.Message) error {
				return b.handle(ctx, msg.Value, provinces)
			})
			_ = group.Close()
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Error("consumer_cycle", "error", err)
		}
		time.Sleep(2 * time.Second)
	}
}

var schema = []string{
	`CREATE TABLE IF NOT EXISTS detections (
		detection_key TEXT PRIMARY KEY, satellite TEXT NOT NULL,
		acq_date TEXT NOT NULL, acq_time TEXT NOT NULL,
		latitude DOUBLE PRECISION NOT NULL, longitude DOUBLE PRECISION NOT NULL,
		confidence TEXT NOT NULL DEFAULT '', frp_mw DOUBLE PRECISION NOT NULL DEFAULT 0,
		fire_id TEXT NOT NULL DEFAULT '', received_at TIMESTAMPTZ NOT NULL,
		acquired_at TIMESTAMPTZ, payload JSONB)`,
	`CREATE TABLE IF NOT EXISTS fires (
		fire_id TEXT PRIMARY KEY, centroid_lat DOUBLE PRECISION NOT NULL,
		centroid_lon DOUBLE PRECISION NOT NULL, first_seen TIMESTAMPTZ NOT NULL,
		last_update TIMESTAMPTZ NOT NULL, status TEXT NOT NULL CHECK (status IN ('active','closed')),
		detection_count INT NOT NULL, total_frp_mw DOUBLE PRECISION NOT NULL,
		max_confidence TEXT NOT NULL DEFAULT '', satellites JSONB NOT NULL DEFAULT '[]')`,
	`CREATE TABLE IF NOT EXISTS fire_edges (
		fire_id TEXT NOT NULL, other_key TEXT NOT NULL, PRIMARY KEY (fire_id, other_key))`,
	`CREATE TABLE IF NOT EXISTS late_detections (
		detection_key TEXT PRIMARY KEY, reason TEXT NOT NULL, received_at TIMESTAMPTZ NOT NULL)`,
	`CREATE INDEX IF NOT EXISTS detections_acquired_idx ON detections(acquired_at)`,
}

func loadProvinces(path string) ([]province, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read province references: %w", err)
	}
	var refs []province
	if err := json.Unmarshal(raw, &refs); err != nil {
		return nil, fmt.Errorf("parse province references: %w", err)
	}
	if len(refs) == 0 {
		return nil, errors.New("province references empty")
	}
	return refs, nil
}

func (b *builder) serveHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		platform.JSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "fire-builder"})
	})
	mux.HandleFunc("GET /internal/fire/stats", func(w http.ResponseWriter, _ *http.Request) {
		platform.JSON(w, http.StatusOK, map[string]any{
			"late_count":   b.lateDetections.Load(),
			"active_fires": b.activeFires.Load(),
			"closed_fires": b.closedFires.Load(),
		})
	})
	port := platform.Env("PORT", "8130")
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		b.logger.Error("http_server", "error", err)
	}
}

func (b *builder) refreshCounters(ctx context.Context) {
	var active, closed, late int64
	_ = b.db.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE status = 'active'),
		COUNT(*) FILTER (WHERE status = 'closed') FROM fires`).Scan(&active, &closed)
	_ = b.db.QueryRow(ctx, `SELECT COUNT(*) FROM late_detections`).Scan(&late)
	b.activeFires.Store(active)
	b.closedFires.Store(closed)
	b.lateDetections.Store(late)
}

// handle processes one detections.raw message inside one Postgres transaction.
func (b *builder) handle(ctx context.Context, body []byte, provinces []province) error {
	var raw rawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		b.logger.Error("invalid_message", "error", err) // poison: commit and skip
		return nil
	}
	d, ok, dropReason := parseDetection(raw.Detection)
	if !ok {
		b.logger.Warn("detection_dropped", "reason", dropReason, "offset", raw.Offset)
		return nil // contract violation is not a pipeline failure
	}
	tx, err := b.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	fireID, err := b.applyDetection(ctx, tx, d, raw.Type, provinces)
	if err != nil {
		return err
	}
	if fireID != "" {
		if err := b.persistAndPublish(ctx, tx, fireID, provinces); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// parseDetection decodes and validates one detection payload; low-confidence
// detections are dropped per the fire definition.
func parseDetection(payload json.RawMessage) (detection, bool, string) {
	var m struct {
		Satellite  string      `json:"satellite"`
		AcqDate    string      `json:"acq_date"`
		AcqTime    string      `json:"acq_time"`
		Latitude   float64     `json:"latitude"`
		Longitude  float64     `json:"longitude"`
		Confidence string      `json:"confidence"`
		FRPMW      json.Number `json:"frp_mw"`
	}
	if err := json.Unmarshal(payload, &m); err != nil {
		return detection{}, false, "malformed_json"
	}
	if strings.EqualFold(m.Confidence, "low") || strings.EqualFold(m.Confidence, "l") {
		return detection{}, false, "confidence_low"
	}
	if m.Satellite == "" || m.AcqDate == "" || m.AcqTime == "" ||
		m.Latitude == 0 && m.Longitude == 0 {
		return detection{}, false, "missing_identity_fields"
	}
	t := parseAcqTime(m.AcqDate, m.AcqTime)
	frp, _ := m.FRPMW.Float64()
	return detection{
		Satellite: m.Satellite, AcqDate: m.AcqDate, AcqTime: m.AcqTime,
		Latitude: m.Latitude, Longitude: m.Longitude,
		Confidence: strings.ToUpper(strings.TrimSpace(m.Confidence)),
		FRPMW:      frp,
		Key:        detectionKey(m.Satellite, m.AcqDate, m.AcqTime, m.Latitude, m.Longitude),
		Time:       t,
	}, true, ""
}

// applyDetection inserts/updates/retracts the detection and re-derives its
// fire cluster; returns the fire id whose HazardEvent changed ("" = none).
func (b *builder) applyDetection(
	ctx context.Context, tx pgx.Tx, d detection, eventType string, provinces []province,
) (string, error) {
	var existingFire string
	err := tx.QueryRow(ctx,
		`SELECT fire_id FROM detections WHERE detection_key = $1`, d.Key).Scan(&existingFire)
	notFound := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !notFound {
		return "", err
	}

	switch eventType {
	case "retract":
		if notFound {
			return "", nil // unknown detection: nothing to retract
		}
		if _, err := tx.Exec(ctx, `DELETE FROM detections WHERE detection_key = $1`, d.Key); err != nil {
			return "", err
		}
		return existingFire, nil
	case "update":
		if !notFound {
			// replace fields and re-derive links: remove, then re-add below
			if _, err := tx.Exec(ctx, `DELETE FROM detections WHERE detection_key = $1`, d.Key); err != nil {
				return "", err
			}
		}
		return b.insertDetection(ctx, tx, d, provinces)
	default: // detection (and unknown types treated as new detections)
		if !notFound {
			return "", nil // duplicate key: deduped, idempotent under redelivery
		}
		return b.insertDetection(ctx, tx, d, provinces)
	}
}

// insertDetection stores the detection, links it to all detections within
// 1 km / 360 min, unions the clusters, and returns the affected fire id.
func (b *builder) insertDetection(
	ctx context.Context, tx pgx.Tx, d detection, provinces []province,
) (string, error) {
	var acquired any
	if d.Time.OK {
		acquired = time.Unix(d.Time.Unix, 0).UTC()
	}
	if _, err := tx.Exec(ctx, `INSERT INTO detections(
		detection_key, satellite, acq_date, acq_time, latitude, longitude,
		confidence, frp_mw, fire_id, received_at, acquired_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'',now(),$9)
	ON CONFLICT (detection_key) DO NOTHING`,
		d.Key, d.Satellite, d.AcqDate, d.AcqTime, d.Latitude, d.Longitude,
		d.Confidence, d.FRPMW, acquired,
	); err != nil {
		return "", err
	}

	// rebuild the full link graph from persisted state: the cluster of the
	// new detection is its transitive closure under (<=1km, <=360min).
	rows, err := tx.Query(ctx, `SELECT detection_key, latitude, longitude,
		acq_date, acq_time, acquired_at FROM detections`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	type node struct {
		key      string
		lat, lon float64
		acqDate  string
		acqTime  string
		acquired timeUTC
		hasAcq   bool
	}
	nodes := []node{}
	for rows.Next() {
		var n node
		var acq *time.Time
		if err := rows.Scan(&n.key, &n.lat, &n.lon, &n.acqDate, &n.acqTime, &acq); err != nil {
			return "", err
		}
		if acq != nil {
			n.acquired = timeUTC{Unix: acq.Unix(), OK: true}
			n.hasAcq = true
		}
		if !n.hasAcq { // fall back to acq_date+acq_time columns
			n.acquired = parseAcqTime(n.acqDate, n.acqTime)
			n.hasAcq = n.acquired.OK
		}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	uf := newUnionFind()
	for _, n := range nodes {
		uf.ensure(n.key)
	}
	edges := [][2]string{}
	for i := range len(nodes) {
		for j := i + 1; j < len(nodes); j++ {
			a, c := nodes[i], nodes[j]
			if !a.acquired.OK || !c.acquired.OK {
				continue
			}
			dt := a.acquired.Unix - c.acquired.Unix
			if dt < 0 {
				dt = -dt
			}
			if dt > int64(linkWindowMins*60) {
				continue
			}
			if haversineKM(a.lat, a.lon, c.lat, c.lon) <= linkRadiusKM {
				uf.union(a.key, c.key)
				edges = append(edges, [2]string{a.key, c.key})
			}
		}
	}

	clusterKeys := []string{}
	root := uf.find(d.Key)
	for _, n := range nodes {
		if uf.find(n.key) == root {
			clusterKeys = append(clusterKeys, n.key)
		}
	}

	// find the cluster the new detection joined ("" fire id when singleton)
	fireID := ""
	if len(clusterKeys) >= minClusterLen {
		fireID = fireSourceRefID(clusterKeys)
	}

	// assign every node its cluster's fire id and write chain edges
	for _, n := range nodes {
		keys := uf.clusterOf(n.key)
		nFire := ""
		if len(keys) >= minClusterLen {
			nFire = fireSourceRefID(keys)
		}
		if _, err := tx.Exec(ctx, `UPDATE detections SET fire_id = $2 WHERE detection_key = $1`,
			n.key, nFire); err != nil {
			return "", err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM fire_edges`); err != nil {
		return "", err
	}
	for _, e := range edges {
		if _, err := tx.Exec(ctx, `INSERT INTO fire_edges(fire_id, other_key)
			VALUES ($1,$2),($2,$1) ON CONFLICT DO NOTHING`, e[0], e[1]); err != nil {
			return "", err
		}
	}

	// upsert/delete fires rows per cluster
	byKey := map[string]detection{d.Key: d}
	for _, n := range nodes {
		if n.key == d.Key {
			continue
		}
		byKey[n.key] = detection{
			Key: n.key, Latitude: n.lat, Longitude: n.lon,
			AcqDate: n.acqDate, AcqTime: n.acqTime, Time: n.acquired,
		}
	}
	seenFires := map[string]bool{}
	for _, n := range nodes {
		keys := uf.clusterOf(n.key)
		if len(keys) < minClusterLen {
			continue
		}
		fid := fireSourceRefID(keys)
		if seenFires[fid] {
			continue
		}
		seenFires[fid] = true
		if err := b.upsertFire(ctx, tx, fid, keys, byKey); err != nil {
			return "", err
		}
	}
	// clusters that dissolved below 2 members: their old fire rows close
	if err := b.closeOrphanFires(ctx, tx, seenFires); err != nil {
		return "", err
	}

	// late detection handling: if this detection is older than the fire's
	// cutoff it must not reopen a closed fire; it is recorded and excluded.
	late, err := b.handleLateDetection(ctx, tx, d, fireID)
	if err != nil {
		return "", err
	}
	if late {
		return "", nil
	}
	return fireID, nil
}

// clusterOf returns the sorted keys of the component containing key.
func (uf *unionFind) clusterOf(key string) []string {
	root := uf.find(key)
	out := []string{}
	for k := range uf.parent {
		if uf.find(k) == root {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func (b *builder) upsertFire(
	ctx context.Context, tx pgx.Tx, fireID string, keys []string, byKey map[string]detection,
) error {
	st := statsFor(keys, byKey)
	if !st.HasTime {
		// no acquisition time anywhere in the cluster: can't form a valid fire
		return nil
	}
	sats, _ := json.Marshal(st.Satellites)
	_, err := tx.Exec(ctx, `INSERT INTO fires(
		fire_id, centroid_lat, centroid_lon, first_seen, last_update, status,
		detection_count, total_frp_mw, max_confidence, satellites)
	VALUES ($1,$2,$3,$4,now(),'active',$5,$6,$7,$8)
	ON CONFLICT (fire_id) DO UPDATE SET centroid_lat = EXCLUDED.centroid_lat,
		centroid_lon = EXCLUDED.centroid_lon, last_update = now(), status = 'active',
		detection_count = EXCLUDED.detection_count, total_frp_mw = EXCLUDED.total_frp_mw,
		max_confidence = EXCLUDED.max_confidence, satellites = EXCLUDED.satellites`,
		fireID, st.CentroidLat, st.CentroidLon,
		time.Unix(st.OccurredAtUnix, 0).UTC(),
		st.DetectionCount, st.TotalFRPMW, st.MaxConfidence, sats,
	)
	return err
}

// closeOrphanFires marks fires no longer backed by a >= 2 cluster as closed.
func (b *builder) closeOrphanFires(ctx context.Context, tx pgx.Tx, alive map[string]bool) error {
	rows, err := tx.Query(ctx, `SELECT fire_id, status FROM fires`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var orphans []string
	var closedNow []string
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			return err
		}
		if !alive[id] {
			orphans = append(orphans, id)
			if status == "active" {
				closedNow = append(closedNow, id)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range orphans {
		if _, err := tx.Exec(ctx,
			`UPDATE fires SET status = 'closed', last_update = now() WHERE fire_id = $1`, id); err != nil {
			return err
		}
		// closed fires keep no detections assigned to them anymore
		if _, err := tx.Exec(ctx,
			`UPDATE detections SET fire_id = '' WHERE fire_id = $1`, id); err != nil {
			return err
		}
	}
	if len(closedNow) > 0 {
		b.logger.Info("fires_closed", "count", len(closedNow))
	}
	return nil
}

// handleLateDetection: a detection whose acq time is older than
// (fire.last_update - 360min - 30min grace) must not reopen a closed fire;
// it is recorded in late_detections and reported in /internal/fire/stats.
func (b *builder) handleLateDetection(
	ctx context.Context, tx pgx.Tx, d detection, fireID string,
) (bool, error) {
	if fireID == "" || !d.Time.OK {
		return false, nil
	}
	var lastUpdate time.Time
	var status string
	err := tx.QueryRow(ctx,
		`SELECT last_update, status FROM fires WHERE fire_id = $1`, fireID,
	).Scan(&lastUpdate, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	cutoff := lastUpdate.Add(-time.Duration(linkWindowMins+graceMins) * time.Minute)
	if status == "closed" && time.Unix(d.Time.Unix, 0).UTC().Before(cutoff) {
		if _, err := tx.Exec(ctx, `INSERT INTO late_detections(detection_key, reason, received_at)
			VALUES ($1,$2,now()) ON CONFLICT (detection_key) DO NOTHING`,
			d.Key, "older_than_last_update_minus_window_and_grace"); err != nil {
			return false, err
		}
		// remove it from the cluster it briefly joined
		if _, err := tx.Exec(ctx, `DELETE FROM detections WHERE detection_key = $1`, d.Key); err != nil {
			return false, err
		}
		b.lateDetections.Add(1)
		return true, nil
	}
	return false, nil
}

// persistAndPublish builds the HazardEvent for fireID and POSTs it to the
// store, then publishes hazard.created; both must succeed before the caller
// commits the Kafka offset (at-least-once + idempotent upsert).
func (b *builder) persistAndPublish(
	ctx context.Context, tx pgx.Tx, fireID string, provinces []province,
) error {
	var (
		centroidLat, centroidLon, totalFRP float64
		firstSeen, lastUpdate              time.Time
		status                             string
		detectionCount                     int
		maxConfidence                      string
		satellitesRaw                      []byte
	)
	err := tx.QueryRow(ctx, `SELECT centroid_lat, centroid_lon, first_seen, last_update,
		status, detection_count, total_frp_mw, max_confidence, satellites
		FROM fires WHERE fire_id = $1`, fireID,
	).Scan(&centroidLat, &centroidLon, &firstSeen, &lastUpdate, &status,
		&detectionCount, &totalFRP, &maxConfidence, &satellitesRaw)
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx,
		`SELECT detection_key, acquired_at FROM detections WHERE fire_id = $1 ORDER BY detection_key`, fireID)
	if err != nil {
		return err
	}
	defer rows.Close()
	keys := []string{}
	var occurredAt time.Time
	for rows.Next() {
		var k string
		var acq *time.Time
		if err := rows.Scan(&k, &acq); err != nil {
			return err
		}
		keys = append(keys, k)
		if acq != nil && (occurredAt.IsZero() || acq.Before(occurredAt)) {
			occurredAt = *acq
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	satellites := []string{}
	_ = json.Unmarshal(satellitesRaw, &satellites)

	// retracted-away fires and below-threshold clusters are not HazardEvents
	if status != "active" || len(keys) < minClusterLen {
		return nil
	}

	event := HazardEvent{
		HazardID:    stableID(sourceName, fireID),
		Source:      sourceName,
		SourceRefID: fireID,
		HazardType:  "WILDFIRE",
		Severity:    severityFor(detectionCount, totalFRP),
		AreaName:    nearestProvince(centroidLat, centroidLon, provinces),
		Latitude:    centroidLat,
		Longitude:   centroidLon,
		OccurredAt:  occurredAt,
		IngestedAt:  time.Now().UTC(),
		Attributes: map[string]any{
			"detection_count": detectionCount,
			"satellites":      satellites,
			"max_confidence":  maxConfidence,
			"total_frp_mw":    totalFRP,
			"detection_keys":  keys,
		},
	}
	if err := b.storeEvent(ctx, event); err != nil {
		return err
	}
	envelope, _ := json.Marshal(map[string]any{
		"event_type":     "hazard.created",
		"correlation_id": platform.NewCorrelationID(),
		"event":          event,
	})
	return b.kafkaProd.Publish(ctx, hazardTopic, []byte(event.HazardID), envelope, nil)
}

func (b *builder) storeEvent(ctx context.Context, event HazardEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, b.storeURL+"/internal/store/events", strings.NewReader(string(body)),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return platform.Outbound(b.logger, ctx, "store_put_event", func() error {
		res, err := b.client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusCreated {
			return nil // 200 exists / 201 created, both idempotent successes
		}
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return fmt.Errorf("store %s: %s", res.Status, strings.TrimSpace(string(raw)))
	})
}

// nearestProvince returns the province whose reference point is closest to
// the fire centroid (static BNPB reference table).
func nearestProvince(lat, lon float64, provinces []province) string {
	best, bestDist := "", math.MaxFloat64
	for _, p := range provinces {
		if d := haversineKM(lat, lon, p.Latitude, p.Longitude); d < bestDist {
			best, bestDist = p.Name, d
		}
	}
	return best
}

func stableID(source, ref string) string {
	sum := sha256.Sum256([]byte(source + ":" + ref))
	return hex.EncodeToString(sum[:])
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
