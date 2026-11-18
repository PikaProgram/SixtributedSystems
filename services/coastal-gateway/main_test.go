package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func obs(id string, sea, wave float64, quality string, critical bool) map[string]any {
	return map[string]any{
		"observation_id":      id,
		"station_id":          "stn-padang-01",
		"observed_at":         "2026-11-12T02:43:00Z",
		"sea_level_anomaly_m": sea,
		"wave_height_m":       wave,
		"quality":             quality,
		"critical":            critical,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestGateway(sensor *httptest.Server, store *httptest.Server) *gateway {
	if store == nil {
		store = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"status":"created"}`))
		}))
	}
	g := &gateway{
		logger:     discardLogger(),
		client:     &http.Client{Timeout: 2 * time.Second},
		sensorURL:  sensor.URL,
		token:      "test-token",
		storeURL:   store.URL,
		interval:   time.Second,
		timeout:    2 * time.Second,
		brk:        newBreaker(discardLogger()),
		seen:       map[string]bool{},
		violations: map[string]int{},
		stations: map[string]station{
			"stn-padang-01": {Province: "Sumatera Barat", Lat: -0.9492, Lon: 100.3543},
		},
	}
	g.brk.slowWindow = time.Minute // keep slow-call tracking deterministic in unit tests
	return g
}

func newSensor(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	return s
}

// sensorServing encodes page as the /v1/observations response, requiring the
// bearer token; when seen is non-nil it records each served page.
func sensorServing(page map[string]any, seen *[]map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if seen != nil {
			*seen = append(*seen, page)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	}
}

func storedEvents(t *testing.T, store *httptest.Server) []HazardEvent {
	t.Helper()
	res, err := http.Get(store.URL + "/dump")
	if err != nil {
		t.Fatalf("dump store: %v", err)
	}
	defer res.Body.Close()
	var events []HazardEvent
	if err := json.NewDecoder(res.Body).Decode(&events); err != nil {
		t.Fatalf("decode dump: %v", err)
	}
	return events
}

func TestBreakerConsecutiveFailures(t *testing.T) {
	ctx := context.Background()
	b := newBreaker(discardLogger())
	b.failureThreshold = 3
	b.slowWindow = time.Minute
	now := time.Now()
	b.now = func() time.Time { return now }

	for range 3 {
		if err := b.before(ctx); err != nil {
			t.Fatalf("call blocked: %v", err)
		}
		b.after(ctx, false, time.Millisecond)
	}
	if got := b.stateString(); got != "open" {
		t.Fatalf("state after 3 failures = %q, want open", got)
	}
	if err := b.before(ctx); err == nil {
		t.Fatal("OPEN must send no requests (before should block)")
	}

	// advance past openDuration: exactly one probe allowed
	now = now.Add(b.openDuration + time.Second)
	if err := b.before(ctx); err != nil {
		t.Fatalf("half-open probe blocked: %v", err)
	}
	if err := b.before(ctx); err == nil {
		t.Fatal("half-open must allow only one probe call")
	}
	b.after(ctx, true, time.Millisecond)
	if got := b.stateString(); got != "closed" {
		t.Fatalf("successful probe should close breaker, got %q", got)
	}

	// failed probe in half-open reopens immediately
	for range 3 {
		if err := b.before(ctx); err != nil {
			t.Fatalf("call blocked: %v", err)
		}
		b.after(ctx, false, time.Millisecond)
	}
	if got := b.stateString(); got != "open" {
		t.Fatalf("state = %q, want open", got)
	}
	now = now.Add(b.openDuration + time.Second)
	if err := b.before(ctx); err != nil {
		t.Fatalf("probe blocked: %v", err)
	}
	b.after(ctx, false, time.Millisecond)
	if got := b.stateString(); got != "open" {
		t.Fatalf("failed probe should reopen, got %q", got)
	}
}

func TestBreakerSlowCalls(t *testing.T) {
	ctx := context.Background()
	b := newBreaker(discardLogger())
	b.slowThreshold = 3 * time.Second
	b.slowWindow = 30 * time.Second
	now := time.Now()
	b.now = func() time.Time { return now }

	// two slow-but-successful calls 15s apart: window not yet full
	for range 2 {
		if err := b.before(ctx); err != nil {
			t.Fatalf("call blocked: %v", err)
		}
		b.after(ctx, true, 4*time.Second)
		now = now.Add(15 * time.Second)
	}
	if got := b.stateString(); got != "closed" {
		t.Fatalf("state = %q, want closed before window fills", got)
	}
	// third call completes 30s after the first slow one: window full → OPEN
	if err := b.before(ctx); err != nil {
		t.Fatalf("call blocked: %v", err)
	}
	b.after(ctx, true, 4*time.Second)
	if got := b.stateString(); got != "open" {
		t.Fatalf("state = %q, want open after 30s of slow calls", got)
	}
}

func TestSeverityMapping(t *testing.T) {
	cases := []struct {
		critical        bool
		sea, wave, want string
	}{
		{true, "0", "0", "AWAS"},
		{true, "9", "9", "AWAS"},
		{false, "0.5", "0", "SIAGA"},
		{false, "0.1", "3.0", "SIAGA"},
		{false, "0.49", "2.99", "WASPADA"},
		{false, "0", "0", "WASPADA"},
	}
	for _, c := range cases {
		sea := parseFloat(t, c.sea)
		wave := parseFloat(t, c.wave)
		if got := severityFor(c.critical, sea, wave); got != c.want {
			t.Errorf("severityFor(critical=%v, sea=%v, wave=%v) = %q, want %q",
				c.critical, sea, wave, got, c.want)
		}
	}
}

func parseFloat(t *testing.T, raw string) float64 {
	t.Helper()
	var f float64
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return f
}

func TestViolationShunting(t *testing.T) {
	bad := func(mutate func(map[string]any)) map[string]any {
		o := obs("obs-bad", 0.1, 1.0, "ok", false)
		mutate(o)
		return o
	}
	page := map[string]any{
		"items": []any{
			obs("obs-ok", 0.1, 1.0, "ok", false),
			map[string]any{ // missing observation_id
				"station_id": "stn-padang-01", "observed_at": "2026-11-12T02:43:00Z",
				"sea_level_anomaly_m": 0.1, "wave_height_m": 1.0, "quality": "ok", "critical": false,
			},
			bad(func(o map[string]any) { o["sea_level_anomaly_m"] = "high" }),
			bad(func(o map[string]any) { o["observed_at"] = "12/11/2026" }),
			bad(func(o map[string]any) { o["quality"] = "excellent" }),
			bad(func(o map[string]any) { o["critical"] = "yes" }),
		},
	}
	sensor := newSensor(t, sensorServing(page, nil))
	store := newDumpStore()
	g := newTestGateway(sensor, store)

	if err := g.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	events := storedEvents(t, store)
	if len(events) != 1 || events[0].SourceRefID != "obs-ok" {
		t.Fatalf("stored events = %+v, want only obs-ok", events)
	}
	want := map[string]int{
		"observation_id": 1, "sea_level_anomaly_m": 1, "observed_at": 1,
		"quality": 1, "critical": 1,
	}
	if len(g.violations) != len(want) {
		t.Fatalf("violations = %v, want %v", g.violations, want)
	}
	for k, n := range want {
		if g.violations[k] != n {
			t.Errorf("violations[%s] = %d, want %d", k, g.violations[k], n)
		}
	}
}

func TestStoreIdempotentUpsert(t *testing.T) {
	events := map[string]bool{}
	var mu sync.Mutex
	store := newSensor(t, func(w http.ResponseWriter, r *http.Request) {
		var e HazardEvent
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		exists := events[e.SourceRefID]
		events[e.SourceRefID] = true
		mu.Unlock()
		if exists {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"exists"}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"created"}`))
	})
	page := map[string]any{
		"items": []any{obs("obs-1", 0.1, 1.0, "ok", false)},
	}
	g := newTestGateway(newSensor(t, sensorServing(page, nil)), store)
	ctx := context.Background()

	if err := g.pollOnce(ctx); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	// redeliver the same observation with the dedupe marker lost: the store
	// answers 200 exists and the gateway must treat it as success
	g.mu.Lock()
	g.seen = map[string]bool{}
	g.mu.Unlock()
	if err := g.pollOnce(ctx); err != nil {
		t.Fatalf("repoll: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 || !events["obs-1"] {
		t.Fatalf("store keys = %v, want only obs-1", events)
	}
}

func TestStaleServing(t *testing.T) {
	page := map[string]any{
		"items": []any{obs("obs-1", 0.1, 1.0, "ok", false)},
	}
	healthy := newSensor(t, sensorServing(page, nil))
	g := newTestGateway(healthy, nil)
	ctx := context.Background()

	if err := g.pollOnce(ctx); err != nil {
		t.Fatalf("prime poll: %v", err)
	}
	g.cacheMu.RLock()
	cached := len(g.cache)
	g.cacheMu.RUnlock()
	if cached != 1 {
		t.Fatalf("cache = %d items, want 1", cached)
	}
	if g.stale() {
		t.Fatal("cache freshly updated should not be stale")
	}

	// upstream starts failing: breaker opens after 3 polls, cache survives
	g.sensorURL = newSensor(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}).URL
	for range 3 {
		_ = g.pollOnce(ctx)
	}
	if got := g.brk.stateString(); got != "open" {
		t.Fatalf("breaker = %q, want open", got)
	}
	w := httptest.NewRecorder()
	g.observations(w, httptest.NewRequest(http.MethodGet, "/internal/coastal/observations", nil))
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["stale"] != true {
		t.Fatalf("stale marker missing: %v", resp)
	}
	items, _ := resp["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("cached items = %d, want 1", len(items))
	}
}

func TestPaginationAndDedupe(t *testing.T) {
	page2 := map[string]any{
		"items": []any{obs("obs-2", 0.6, 1.0, "ok", false)},
	}
	page1 := map[string]any{
		"items":       []any{obs("obs-1", 0.1, 1.0, "ok", false), obs("obs-2", 0.6, 1.0, "ok", false)},
		"next_cursor": "cursor-2",
	}
	calls := 0
	sensor := newSensor(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			sensorServing(page1, nil)(w, r)
			return
		}
		if got := r.URL.Query().Get("cursor"); got != "cursor-2" {
			t.Errorf("second page cursor = %q, want cursor-2", got)
		}
		sensorServing(page2, nil)(w, r)
	})
	g := newTestGateway(sensor, newDumpStore())
	if err := g.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	if calls != 2 {
		t.Fatalf("sensor calls = %d, want 2 (pagination stopped at null next_cursor)", calls)
	}
	events := storedEvents(t, newDumpStore()) // placeholder, replaced below
	_ = events
}

func TestPaginationStoresEachObservationOnce(t *testing.T) {
	page2 := map[string]any{
		"items": []any{obs("obs-2", 0.6, 1.0, "ok", false)},
	}
	page1 := map[string]any{
		"items":       []any{obs("obs-1", 0.1, 1.0, "ok", false), obs("obs-2", 0.6, 1.0, "ok", false)},
		"next_cursor": "cursor-2",
	}
	calls := 0
	sensor := newSensor(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			sensorServing(page1, nil)(w, r)
			return
		}
		sensorServing(page2, nil)(w, r)
	})
	store := newDumpStore()
	g := newTestGateway(sensor, store)
	if err := g.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	events := storedEvents(t, store)
	if len(events) != 2 {
		t.Fatalf("stored events = %d, want 2 (obs-2 delivered once)", len(events))
	}
}

func TestCriticalFirst(t *testing.T) {
	var order []string
	var mu sync.Mutex
	store := newSensor(t, func(w http.ResponseWriter, r *http.Request) {
		var e HazardEvent
		_ = json.NewDecoder(r.Body).Decode(&e)
		mu.Lock()
		order = append(order, e.SourceRefID)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	})
	page := map[string]any{
		"items": []any{
			obs("obs-normal", 0.1, 1.0, "ok", false),
			obs("obs-critical", 0.2, 1.0, "ok", true),
		},
	}
	g := newTestGateway(newSensor(t, sensorServing(page, nil)), store)
	if err := g.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "obs-critical" {
		t.Fatalf("delivery order = %v, want critical first", order)
	}
}

func TestObservationsNeverBlocks(t *testing.T) {
	g := newTestGateway(newSensor(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second) // upstream hangs; endpoint must not wait
	}), nil)
	g.cache = []cachedObservation{{
		raw: obs("obs-1", 0.1, 1.0, "ok", false),
		at:  time.Now().Add(-time.Minute), id: "obs-1",
	}}
	g.updatedAt = time.Now().Add(-time.Minute)
	g.lastErr = "sensor down"
	started := time.Now()
	w := httptest.NewRecorder()
	g.observations(w, httptest.NewRequest(http.MethodGet, "/internal/coastal/observations", nil))
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("observations took %v, want <50ms", elapsed)
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["stale"] != true {
		t.Fatal("stale marker missing")
	}
}

func TestStatsShape(t *testing.T) {
	g := newTestGateway(newSensor(t, sensorServing(map[string]any{"items": []any{}}, nil)), nil)
	g.violations["quality"] = 2
	w := httptest.NewRecorder()
	g.stats(w, httptest.NewRequest(http.MethodGet, "/internal/coastal/stats", nil))
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"violations", "breaker", "last_good_age_seconds", "stale_serving"} {
		if _, ok := resp[key]; !ok {
			t.Errorf("stats missing key %q: %v", key, resp)
		}
	}
	if got := resp["breaker"]; got != "closed" {
		t.Errorf("breaker = %v, want closed", got)
	}
}

func newDumpStore() *httptest.Server {
	var events []HazardEvent
	var mu sync.Mutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dump" {
			mu.Lock()
			defer mu.Unlock()
			_ = json.NewEncoder(w).Encode(events)
			return
		}
		var e HazardEvent
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
}
