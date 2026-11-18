// feed-ingestor connects to the assistant's Feed Satelit SSE stream and
// forwards every detection/update/retract event to Kafka topic detections.raw.
// The last successfully published offset is tracked in its own Postgres
// (committed AFTER the Kafka publish) so reconnects resume the pipeline
// without gaps; the feed itself guarantees monotonic offsets.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/kafka"
	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	rawTopic    = "detections.raw"
	idleTimeout = 60 * time.Second // no data on the stream for 60s -> reconnect
	backoffBase = 1 * time.Second
	backoffMax  = 30 * time.Second
)

type logger = *slog.Logger

type ingestor struct {
	logger   logger
	client   *http.Client
	baseURL  string
	token    string
	db       *pgxpool.Pool
	producer *kafka.Producer

	lastPublished atomic.Int64
	connected     atomic.Bool
	lastData      atomic.Int64 // unix nanos of last line read off the stream
	shapeWarned   atomic.Bool  // warn once when a payload deviates from the contract
}

// streamEvent is one parsed SSE event ready for Kafka.
type streamEvent struct {
	Offset    int64
	Type      string // detection | update | retract | heartbeat
	Detection json.RawMessage
	Key       string // satellite|acq_date|acq_time|latitude|longitude (raw literals)
}

func main() {
	logger := platform.Logger("feed-ingestor")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := platform.Env(
		"INGESTOR_POSTGRES_DSN",
		"postgres://postgres:postgres@localhost:5433/ingestor?sslmode=disable",
	)
	var db *pgxpool.Pool
	var err error
	for {
		db, err = pgxpool.New(ctx, dsn)
		if err == nil {
			break
		}
		logger.Error("postgres_pool", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	defer db.Close()
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS ingest_offset (
		single_row INT PRIMARY KEY DEFAULT 1 CHECK (single_row = 1),
		last_offset BIGINT NOT NULL DEFAULT -1
	)`); err != nil {
		logger.Error("schema_create", "error", err)
		os.Exit(1)
	}

	ing := &ingestor{
		logger:   logger,
		client:   &http.Client{Timeout: 0}, // SSE streams indefinitely; per-request ctx bounds it
		baseURL:  strings.TrimRight(platform.Env("SATELLITE_URL", "https://satellite.naufarrel.dev"), "/"),
		token:    platform.Env("SATELLITE_TOKEN", ""),
		db:       db,
		producer: kafka.NewProducer(platform.EnvCSV("KAFKA_BROKERS", "localhost:9092")),
	}

	go ing.serveHTTP()

	from := ing.initialOffset(ctx)
	logger.Info("starting", "base_url", ing.baseURL, "from_offset", from)
	ing.run(ctx, from)
	logger.Info("stopped") // SIGTERM/SIGINT -> graceful exit code 0
}

func (ing *ingestor) serveHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		platform.JSON(w, http.StatusOK, map[string]any{
			"status": "ok", "service": "feed-ingestor", "connected": ing.connected.Load(),
		})
	})
	mux.HandleFunc("GET /internal/ingestor/stats", func(w http.ResponseWriter, _ *http.Request) {
		platform.JSON(w, http.StatusOK, map[string]any{
			"last_published_offset": ing.lastPublished.Load(),
			"connected":             ing.connected.Load(),
		})
	})
	port := platform.Env("PORT", "8120")
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		ing.logger.Error("http_server", "error", err)
	}
}

// initialOffset: stored offset > SSE_START_OFFSET env > latest_offset from /v1/session.
func (ing *ingestor) initialOffset(ctx context.Context) int64 {
	var last int64
	err := ing.db.QueryRow(ctx, `SELECT last_offset FROM ingest_offset WHERE single_row = 1`).Scan(&last)
	if err == nil && last >= 0 {
		ing.lastPublished.Store(last)
		return last
	}
	if raw := platform.Env("SSE_START_OFFSET", ""); raw != "" {
		if v, perr := strconv.ParseInt(raw, 10, 64); perr == nil && v >= 0 {
			return v // negative values are "unset" sentinels
		} else if perr != nil {
			ing.logger.Error("bad_sse_start_offset", "value", raw)
		}
	}
	var session struct {
		LatestOffset json.Number `json:"latest_offset"`
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := ing.get(reqCtx, "/v1/session", &session); err != nil {
		ing.logger.Error("session_fetch", "error", err)
		return 0
	}
	latest, _ := session.LatestOffset.Int64()
	return latest
}

func (ing *ingestor) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ing.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+ing.token)
	res, err := ing.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return fmt.Errorf("feed %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(res.Body).Decode(out)
}

// run streams forever with jittered exponential backoff 1s->30s between
// reconnects; the backoff only grows across consecutive failed attempts.
func (ing *ingestor) run(ctx context.Context, from int64) {
	attempt := 0
	for {
		published, established, err := ing.stream(ctx, from)
		if ctx.Err() != nil {
			return
		}
		if published > from {
			from = published
		}
		if established {
			attempt = 0
		} else {
			attempt++
		}
		wait := backoff(attempt)
		ing.logger.Warn("reconnect", "from_offset", from, "backoff_ms", wait.Milliseconds(), "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func backoff(attempt int) time.Duration {
	d := backoffBase << min(attempt, 5) // 1s..32s, clamped to 30s
	if d > backoffMax {
		d = backoffMax
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)))
}

// stream opens one SSE connection from=offset and dispatches events until the
// connection drops, the idle watchdog fires (no data for 60s), or ctx ends.
// It returns the last offset successfully published to Kafka.
func (ing *ingestor) stream(ctx context.Context, from int64) (published int64, established bool, err error) {
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	endpoint := fmt.Sprintf("%s/v1/stream?from=%d", ing.baseURL, from)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return published, false, err
	}
	req.Header.Set("Authorization", "Bearer "+ing.token)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Last-Event-ID", strconv.FormatInt(from, 10)) // feed honors this too
	res, err := ing.client.Do(req)
	if err != nil {
		return published, false, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return published, false, fmt.Errorf("feed %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	established = true
	ing.connected.Store(true)
	defer ing.connected.Store(false)
	ing.lastData.Store(time.Now().UnixNano())

	// idle watchdog: cancels the request when no line arrives for idleTimeout
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-reqCtx.Done():
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, ing.lastData.Load())) > idleTimeout {
					ing.logger.Warn("stream_idle", "timeout_seconds", idleTimeout.Seconds())
					cancel()
					return
				}
			}
		}
	}()

	lastSeen := from
	var publishErr error
	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var (
		name string
		data bytes.Buffer
	)
	dispatch := func() {
		defer func() {
			data.Reset()
			name = ""
		}()
		if data.Len() == 0 || publishErr != nil {
			return
		}
		ev, perr := parseEvent(name, data.Bytes())
		if perr != nil {
			ing.logger.Error("event_parse", "error", perr)
			return
		}
		if ev.Type == "heartbeat" {
			return // heartbeat only resets the liveness timer
		}
		if ev.Offset > lastSeen+1 {
			// feed guarantees no gaps; surface it but keep going
			ing.logger.Warn("offset_gap", "expected", lastSeen+1, "got", ev.Offset)
		}
		if err := ing.publish(reqCtx, ev); err != nil {
			// abort the stream; run() reconnects from the stored offset
			ing.logger.Error("publish_failed", "offset", ev.Offset, "error", err)
			publishErr = err
			cancel()
			return
		}
		lastSeen = ev.Offset
		published = ev.Offset
		ing.lastPublished.Store(ev.Offset)
	}
	for scanner.Scan() {
		ing.lastData.Store(time.Now().UnixNano())
		line := scanner.Text()
		switch {
		case line == "":
			dispatch()
		case strings.HasPrefix(line, "event:"):
			name = strings.TrimSpace(line[6:])
		case strings.HasPrefix(line, "data:"):
			data.WriteString(strings.TrimPrefix(line[5:], " "))
			data.WriteString("\n")
		}
		if publishErr != nil || reqCtx.Err() != nil {
			break
		}
	}
	if publishErr != nil {
		return published, established, publishErr
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return published, established, err
	}
	return published, established, reqCtx.Err()
}

// publish builds the Kafka value, publishes with the detection key so the same
// detection always lands on the same partition, and only then commits the
// offset to Postgres.
func (ing *ingestor) publish(ctx context.Context, ev streamEvent) error {
	value, err := json.Marshal(map[string]any{
		"type":        ev.Type,
		"offset":      ev.Offset,
		"received_at": time.Now().UTC().Format(time.RFC3339Nano),
		"detection":   ev.Detection,
	})
	if err != nil {
		return err
	}
	key := ev.Key
	if key == "" {
		key = fmt.Sprintf("offset-%d", ev.Offset)
	}
	if err := ing.producer.Publish(ctx, rawTopic, []byte(key), value, nil); err != nil {
		return err
	}
	_, err = ing.db.Exec(ctx, `INSERT INTO ingest_offset(single_row, last_offset) VALUES (1, $1)
		ON CONFLICT (single_row) DO UPDATE SET last_offset = EXCLUDED.last_offset`, ev.Offset)
	return err
}

// parseEvent extracts offset/type/detection from one SSE data payload. The
// contract is {offset, detection:{satellite, acq_date, acq_time, latitude,
// longitude, confidence, frp_mw}}; flattened payloads and shapes missing an
// identity field are forwarded as raw JSON per the assignment.
func parseEvent(name string, data []byte) (streamEvent, error) {
	ev := streamEvent{Type: name}
	if ev.Type == "" {
		ev.Type = "detection"
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		// non-JSON payload: forward raw, offset unknown
		ev.Detection = json.RawMessage(data)
		return ev, nil
	}
	if n, ok := m["offset"].(json.Number); ok {
		ev.Offset, _ = n.Int64()
	}
	if t, ok := m["type"].(string); ok && name == "" {
		ev.Type = t
	}
	det, ok := m["detection"]
	if !ok {
		det = m // flattened shape: detection fields live at the top level
	}
	detMap, isObj := det.(map[string]any)
	if !isObj {
		if det == nil {
			ev.Detection = json.RawMessage("null")
			return ev, nil
		}
		ev.Detection = mustMarshal(det)
		return ev, nil
	}
	for _, f := range []string{"satellite", "acq_date", "acq_time", "latitude", "longitude"} {
		if _, ok := detMap[f]; !ok {
			// shape differs from the contract: forward raw and let the
			// builder decide; log once so the deviation is visible
			if !ingShapeWarned() {
				slog.Warn("detection_shape_deviation", "fields", mustMarshal(detMap))
			}
			ev.Detection = mustMarshal(det)
			return ev, nil
		}
	}
	clean := map[string]any{
		"satellite": detMap["satellite"], "acq_date": detMap["acq_date"],
		"acq_time": detMap["acq_time"], "latitude": detMap["latitude"],
		"longitude": detMap["longitude"],
	}
	if c, ok := detMap["confidence"]; ok {
		clean["confidence"] = c
	}
	if f, ok := detMap["frp_mw"]; ok {
		clean["frp_mw"] = f
	}
	ev.Detection = mustMarshal(clean)
	ev.Key = detectionKey(detMap)
	return ev, nil
}

// ingShapeWarned logs the first payload deviation once per process.
func ingShapeWarned() bool { return shapeWarned.Swap(true) }

var shapeWarned atomic.Bool

func detectionKey(m map[string]any) string {
	parts := make([]string, 0, 5)
	for _, f := range []string{"satellite", "acq_date", "acq_time", "latitude", "longitude"} {
		v := rawString(m[f])
		if v == "" {
			return ""
		}
		parts = append(parts, v)
	}
	return strings.Join(parts, "|")
}

// rawString keeps numeric literals exactly as the feed wrote them (no rounding).
func rawString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}

func mustMarshal(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}
