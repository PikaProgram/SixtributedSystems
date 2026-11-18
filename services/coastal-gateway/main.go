package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/kafka"
	"github.com/astolfo/sixtributedsystems/internal/platform"
)

const (
	kafkaTopic   = "hazard.events"
	sourceName   = "SENSOR_PANTAI"
	coastalType  = "COASTAL"
	storeTimeout = 5 * time.Second
	backoffBase  = 1 * time.Second
	backoffMax   = 60 * time.Second
)

var errBreakerOpen = errors.New("circuit breaker open")

type logger = *slog.Logger

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

type station struct {
	Province string  `json:"province"`
	Lat      float64 `json:"latitude"`
	Lon      float64 `json:"longitude"`
}

type cachedObservation struct {
	raw map[string]any
	at  time.Time
	id  string
}

type parsedObservation struct {
	raw                    map[string]any
	id, stationID, quality string
	sea, wave              float64
	at                     time.Time
	critical               bool
}

// breaker implements the decided policy: 3 consecutive failures OR 30s of
// calls each slower than 3s opens it; OPEN sends nothing for 30s; then one
// HALF-OPEN probe call decides between CLOSED and OPEN again.
type breaker struct {
	logger           logger
	now              func() time.Time
	failureThreshold int
	slowThreshold    time.Duration
	slowWindow       time.Duration
	openDuration     time.Duration

	mu       sync.Mutex
	state    breakerState
	fails    int
	slow     []time.Time // end times of recent slow calls, kept within slowWindow
	openedAt time.Time
	probe    bool
}

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

func (s breakerState) String() string {
	switch s {
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

func newBreaker(logger logger) *breaker {
	return &breaker{
		logger:           logger,
		now:              time.Now,
		failureThreshold: platform.EnvInt("BREAKER_FAILURE_THRESHOLD", 3),
		slowThreshold:    platform.EnvDurationSeconds("BREAKER_SLOW_CALL_SECONDS", 3*time.Second),
		slowWindow:       platform.EnvDurationSeconds("BREAKER_SLOW_WINDOW_SECONDS", 30*time.Second),
		openDuration:     platform.EnvDurationSeconds("BREAKER_OPEN_SECONDS", 30*time.Second),
	}
}

// before gates one call: OPEN blocks with errBreakerOpen (no request reaches
// the sensor) until openDuration elapses, then admits exactly one probe.
func (b *breaker) before(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	switch b.state {
	case stateOpen:
		if now.Sub(b.openedAt) < b.openDuration {
			return errBreakerOpen
		}
		b.transition(ctx, stateHalfOpen, now)
		b.probe = true
		return nil
	case stateHalfOpen:
		if b.probe {
			return errBreakerOpen
		}
		b.probe = true
	}
	return nil
}

// after records one call outcome and applies the transition policy.
func (b *breaker) after(ctx context.Context, success bool, duration time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	slow := !success || duration > b.slowThreshold
	switch b.state {
	case stateHalfOpen:
		b.probe = false
		if success && !slow {
			b.transition(ctx, stateClosed, now)
			return
		}
		b.trip(ctx, now)
	case stateClosed:
		if !slow {
			b.fails = 0
			b.slow = nil
			return
		}
		if !success {
			b.fails++
			if b.fails >= b.failureThreshold {
				b.trip(ctx, now)
				return
			}
		}
		if duration > b.slowThreshold {
			b.slow = append(b.slow, now)
			cutoff := now.Add(-b.slowWindow)
			i := 0
			for i < len(b.slow) && b.slow[i].Before(cutoff) {
				i++
			}
			b.slow = b.slow[i:]
			if len(b.slow) > 0 && now.Sub(b.slow[0]) >= b.slowWindow {
				b.trip(ctx, now)
			}
		}
	default: // OPEN: racing probe reply, nothing to do
	}
}

func (b *breaker) trip(ctx context.Context, now time.Time) {
	b.fails = 0
	b.slow = nil
	b.transition(ctx, stateOpen, now)
}

func (b *breaker) transition(ctx context.Context, to breakerState, now time.Time) {
	if b.state == to {
		return
	}
	from := b.state
	b.state = to
	if to == stateOpen {
		b.openedAt = now
	}
	platform.SetCoastalBreakerState(int64(to)) // 0 closed, 1 half-open, 2 open
	b.logger.Info("breaker_transition", "correlation_id", platform.CorrelationID(ctx),
		"from", from.String(), "to", to.String())
}

func (b *breaker) stateString() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.String()
}

type gateway struct {
	logger    logger
	client    *http.Client
	sensorURL string
	token     string
	storeURL  string
	kafka     *kafka.Producer

	interval time.Duration
	timeout  time.Duration
	attempt  int

	brk *breaker

	mu         sync.Mutex
	seen       map[string]bool // shortcut: unbounded for process lifetime; fine for the exam window
	violations map[string]int

	cacheMu   sync.RWMutex
	cache     []cachedObservation
	updatedAt time.Time
	lastErr   string

	stationsMu sync.RWMutex
	stations   map[string]station
}

func main() {
	logger := platform.Logger("coastal-gateway")
	shutdownTracer, err := platform.InitTracer("coastal-gateway")
	if err != nil {
		logger.Error("tracer_init", "error", err)
	} else {
		defer shutdownTracer()
	}
	shutdownMetrics, err := platform.InitMetrics("coastal-gateway")
	if err != nil {
		logger.Error("metrics_init", "error", err)
	} else {
		defer shutdownMetrics()
	}
	g := &gateway{
		logger:     logger,
		client:     &http.Client{Timeout: storeTimeout},
		sensorURL:  strings.TrimRight(platform.Env("SENSOR_BASE_URL", "http://localhost:8080"), "/"),
		token:      platform.Env("PANTAI_AUTH_TOKEN", ""),
		storeURL:   strings.TrimRight(platform.Env("STORE_URL", "http://localhost:8100"), "/"),
		kafka:      kafka.NewProducer(platform.EnvCSV("KAFKA_BROKERS", "localhost:9092")),
		interval:   platform.EnvDurationSeconds("POLL_INTERVAL_SECONDS", 3*time.Second),
		timeout:    platform.EnvDurationSeconds("SOURCE_TIMEOUT_SECONDS", 4*time.Second),
		brk:        newBreaker(logger),
		seen:       map[string]bool{},
		violations: map[string]int{},
		stations:   map[string]station{},
	}
	go g.run(context.Background())
	port := platform.Env("PORT", "8110")
	logger.Info("starting", "port", port)
	if err := http.ListenAndServe(
		":"+port,
		platform.HTTPMiddleware(logger, g.routes()),
	); err != nil {
		logger.Error("http_server", "error", err)
	}
}

func (g *gateway) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", g.health)
	mux.HandleFunc("GET /internal/coastal/stats", g.stats)
	mux.HandleFunc("GET /internal/coastal/observations", g.observations)
	return mux
}

func (g *gateway) run(ctx context.Context) {
	ctx = platform.PollerCorrelationID("coastal-gateway")
	g.refreshStations(ctx)
	for {
		sleep := g.interval
		err := g.pollOnce(ctx)
		switch {
		case err == nil:
			g.resetBackoff()
		case errors.Is(err, errBreakerOpen):
			// OPEN sends no requests; wake at the normal interval only.
		default:
			sleep = g.nextBackoff()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

// jittered exponential backoff 1s→60s across consecutive poll failures.
func (g *gateway) nextBackoff() time.Duration {
	d := backoffBase << g.attempt
	if d <= 0 || d > backoffMax {
		d = backoffMax
	}
	g.attempt++
	d = d/2 + time.Duration(rand.Int64N(int64(d)))
	if d > backoffMax {
		d = backoffMax
	}
	return d
}

func (g *gateway) resetBackoff() { g.attempt = 0 }

func (g *gateway) pollOnce(ctx context.Context) error {
	if err := g.brk.before(ctx); err != nil {
		g.setLastErr(err.Error())
		return err
	}
	var (
		collected []cachedObservation
		critical  []HazardEvent
		normal    []HazardEvent
		delivered = map[string]bool{}
		fetchErr  error
		cursor    string
		pageFetch int
	)
	started := time.Now()
	for {
		pageCtx, cancel := context.WithTimeout(ctx, g.timeout)
		items, next, err := g.fetchObservations(pageCtx, cursor)
		cancel()
		if err != nil {
			fetchErr = err
			break
		}
		pageFetch++
		for _, raw := range items {
			p, bad := validateObservation(raw)
			if len(bad) > 0 {
				g.countViolations(bad)
				g.logger.Error("contract_violation", "correlation_id", platform.CorrelationID(ctx),
					"observation_id", p.id, "fields", strings.Join(bad, ","))
				continue
			}
			if delivered[p.id] {
				continue
			}
			delivered[p.id] = true
			event, ok := g.toEvent(ctx, p)
			if !ok {
				continue
			}
			collected = append(collected, cachedObservation{raw: p.raw, at: p.at, id: p.id})
			if p.critical {
				critical = append(critical, event)
			} else {
				normal = append(normal, event)
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	g.brk.after(ctx, fetchErr == nil, time.Since(started))
	if fetchErr != nil {
		if len(collected) > 0 {
			g.setCache(collected)
		}
		g.setLastErr(fetchErr.Error())
		return fetchErr
	}
	g.cacheMu.Lock()
	g.lastErr = ""
	g.updatedAt = time.Now().UTC()
	if len(collected) > 0 {
		sort.Slice(collected, func(i, j int) bool {
			if !collected[i].at.Equal(collected[j].at) {
				return collected[i].at.Before(collected[j].at)
			}
			return collected[i].id < collected[j].id
		})
		g.cache = collected
	}
	g.cacheMu.Unlock()
	// priority path: critical events first so field-team alerting is fastest
	for _, event := range critical {
		g.deliver(ctx, event, true)
	}
	for _, event := range normal {
		g.deliver(ctx, event, false)
	}
	return nil
}

func (g *gateway) deliver(ctx context.Context, event HazardEvent, isCritical bool) {
	g.mu.Lock()
	dup := g.seen[event.SourceRefID]
	g.mu.Unlock()
	if dup {
		return
	}
	if err := g.storeEvent(ctx, event); err != nil {
		g.logger.Error("store_failed", "correlation_id", platform.CorrelationID(ctx),
			"hazard_id", event.HazardID, "error", err)
		return // retried next cycle; the store POST is idempotent
	}
	envelope, _ := json.Marshal(map[string]any{
		"event_type":     "hazard.created",
		"correlation_id": platform.CorrelationID(ctx),
		"event":          event,
	})
	var pubErr error
	if g.kafka != nil {
		pubErr = g.kafka.Publish(ctx, kafkaTopic, []byte(event.HazardID), envelope, nil)
	}
	if pubErr != nil {
		g.logger.Error("publish_failed", "correlation_id", platform.CorrelationID(ctx),
			"hazard_id", event.HazardID, "error", pubErr)
		return // retried next cycle; store says "exists" and publish is re-attempted
	}
	if isCritical {
		g.logger.Info("alert_dispatched", "correlation_id", platform.CorrelationID(ctx),
			"hazard_id", event.HazardID, "observation_id", event.SourceRefID,
			"severity", event.Severity, "area_name", event.AreaName)
	}
	g.mu.Lock()
	g.seen[event.SourceRefID] = true
	g.mu.Unlock()
}

func (g *gateway) fetchObservations(ctx context.Context, cursor string) ([]json.RawMessage, string, error) {
	endpoint := g.sensorURL + "/v1/observations?limit=500"
	if cursor != "" {
		endpoint += "&cursor=" + url.QueryEscape(cursor)
	}
	var page struct {
		Items      []json.RawMessage `json:"items"`
		NextCursor *string           `json:"next_cursor"`
	}
	if err := g.get(ctx, endpoint, &page); err != nil {
		return nil, "", err
	}
	next := ""
	if page.NextCursor != nil {
		next = *page.NextCursor
	}
	return page.Items, next, nil
}

func (g *gateway) get(ctx context.Context, endpoint string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+g.token)
	res, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return fmt.Errorf("sensor %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (g *gateway) storeEvent(ctx context.Context, event HazardEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, g.storeURL+"/internal/store/events", bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return platform.Outbound(g.logger, ctx, "store_put_event", func() error {
		res, err := g.client.Do(req)
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

// validateObservation shunts contract violations instead of storing them.
func validateObservation(raw json.RawMessage) (parsedObservation, []string) {
	var obs map[string]any
	if err := json.Unmarshal(raw, &obs); err != nil {
		return parsedObservation{}, []string{"malformed_json"}
	}
	var bad []string
	p := parsedObservation{raw: obs}
	p.id, _ = obs["observation_id"].(string)
	if p.id == "" {
		bad = append(bad, "observation_id")
	}
	p.stationID, _ = obs["station_id"].(string)
	if p.stationID == "" {
		bad = append(bad, "station_id")
	}
	p.sea, _ = obs["sea_level_anomaly_m"].(float64)
	if _, ok := obs["sea_level_anomaly_m"].(float64); !ok {
		bad = append(bad, "sea_level_anomaly_m")
	}
	p.wave, _ = obs["wave_height_m"].(float64)
	if _, ok := obs["wave_height_m"].(float64); !ok {
		bad = append(bad, "wave_height_m")
	}
	atRaw, _ := obs["observed_at"].(string)
	at, err := time.Parse(time.RFC3339, atRaw)
	if err != nil {
		bad = append(bad, "observed_at")
	} else {
		p.at = at
	}
	p.quality, _ = obs["quality"].(string)
	switch p.quality {
	case "ok", "degraded", "bad":
	default:
		bad = append(bad, "quality")
	}
	p.critical, _ = obs["critical"].(bool)
	if _, ok := obs["critical"].(bool); !ok {
		bad = append(bad, "critical")
	}
	return p, bad
}

func (g *gateway) countViolations(keys []string) {
	g.mu.Lock()
	for _, k := range keys {
		g.violations[k]++
	}
	g.mu.Unlock()
	platform.RecordCoastalViolation()
}

func severityFor(critical bool, seaLevelAnomalyM, waveHeightM float64) string {
	if critical {
		return "AWAS"
	}
	if seaLevelAnomalyM >= 0.5 || waveHeightM >= 3.0 {
		return "SIAGA"
	}
	return "WASPADA"
}

func (g *gateway) toEvent(ctx context.Context, p parsedObservation) (HazardEvent, bool) {
	st, ok := g.station(ctx, p.stationID)
	if !ok {
		g.countViolations([]string{"station_unknown"})
		g.logger.Error("contract_violation", "correlation_id", platform.CorrelationID(ctx),
			"observation_id", p.id, "fields", "station_unknown")
		return HazardEvent{}, false
	}
	return HazardEvent{
		HazardID:    stableID(sourceName, p.id),
		Source:      sourceName,
		SourceRefID: p.id,
		HazardType:  coastalType,
		Severity:    severityFor(p.critical, p.sea, p.wave),
		AreaName:    st.Province,
		Latitude:    st.Lat,
		Longitude:   st.Lon,
		OccurredAt:  p.at,
		IngestedAt:  time.Now().UTC(),
		Attributes: map[string]any{
			"station_id":          p.stationID,
			"sea_level_anomaly_m": p.sea,
			"wave_height_m":       p.wave,
			"quality":             p.quality,
			"critical":            p.critical,
		},
	}, true
}

func (g *gateway) station(ctx context.Context, id string) (station, bool) {
	g.stationsMu.RLock()
	st, ok := g.stations[id]
	g.stationsMu.RUnlock()
	if ok {
		return st, true
	}
	g.refreshStations(ctx)
	g.stationsMu.RLock()
	st, ok = g.stations[id]
	g.stationsMu.RUnlock()
	return st, ok
}

func (g *gateway) refreshStations(ctx context.Context) {
	refreshCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	var page struct {
		Items []struct {
			StationID string  `json:"station_id"`
			Province  string  `json:"province"`
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"items"`
	}
	if err := g.get(refreshCtx, g.sensorURL+"/v1/stations", &page); err != nil {
		g.logger.Error("stations_refresh", "correlation_id", platform.CorrelationID(ctx), "error", err)
		return
	}
	fetched := make(map[string]station, len(page.Items))
	for _, it := range page.Items {
		fetched[it.StationID] = station{Province: it.Province, Lat: it.Latitude, Lon: it.Longitude}
	}
	g.stationsMu.Lock()
	g.stations = fetched
	g.stationsMu.Unlock()
}

func (g *gateway) setCache(items []cachedObservation) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].at.Equal(items[j].at) {
			return items[i].at.Before(items[j].at)
		}
		return items[i].id < items[j].id
	})
	g.cacheMu.Lock()
	g.cache = items
	g.updatedAt = time.Now().UTC()
	g.cacheMu.Unlock()
}

func (g *gateway) setLastErr(msg string) {
	g.cacheMu.Lock()
	g.lastErr = msg
	g.cacheMu.Unlock()
}

func (g *gateway) stale() bool {
	g.cacheMu.RLock()
	lastErr := g.lastErr
	g.cacheMu.RUnlock()
	return g.brk.stateString() != "closed" || lastErr != ""
}

func (g *gateway) health(w http.ResponseWriter, _ *http.Request) {
	platform.JSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "coastal-gateway",
		"role":    "sensor",
		"breaker": g.brk.stateString(),
	})
}

func (g *gateway) stats(w http.ResponseWriter, _ *http.Request) {
	g.mu.Lock()
	violations := make(map[string]int, len(g.violations))
	for k, v := range g.violations {
		violations[k] = v
	}
	g.mu.Unlock()
	g.cacheMu.RLock()
	var age int64
	if !g.updatedAt.IsZero() {
		age = int64(time.Since(g.updatedAt).Seconds())
	}
	g.cacheMu.RUnlock()
	platform.JSON(w, http.StatusOK, map[string]any{
		"violations":            violations,
		"breaker":               g.brk.stateString(),
		"last_good_age_seconds": age,
		"stale_serving":         g.stale(),
	})
}

// observations never blocks on the upstream sensor: it reads the in-memory
// last-good cache only, so it answers well under 50ms even during outages.
func (g *gateway) observations(w http.ResponseWriter, _ *http.Request) {
	g.cacheMu.RLock()
	items := make([]map[string]any, len(g.cache))
	for i, c := range g.cache {
		items[i] = c.raw
	}
	updatedAt := g.updatedAt
	g.cacheMu.RUnlock()
	resp := map[string]any{"items": items}
	if !updatedAt.IsZero() {
		resp["generated_at"] = updatedAt.Format(time.RFC3339)
	}
	if g.stale() {
		resp["stale"] = true
	}
	platform.JSON(w, http.StatusOK, resp)
}

func stableID(source, ref string) string {
	sum := sha256.Sum256([]byte(source + ":" + ref))
	return hex.EncodeToString(sum[:])
}
