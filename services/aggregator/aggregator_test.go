package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/kafka"
)

// fakeStore mimics the store unit API for unit tests: POST events, GET events
// with cursor pagination, GET location.
type fakeStore struct {
	mu      sync.Mutex
	name    string
	role    string
	newest  *time.Time
	events  []HazardEvent
	servers []*httptest.Server
}

func newFakeStore(name string) *fakeStore { return &fakeStore{name: name} }

// setRole overrides the role reported by /health and applied by the
// /internal/store/role handler (shard controller tests).
func (f *fakeStore) setRole(role string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.role = role
}

func (f *fakeStore) add(events ...HazardEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, events...)
}

func (f *fakeStore) sorted() []HazardEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]HazardEvent(nil), f.events...)
	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j]) })
	return out
}

func (f *fakeStore) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		events := f.sorted()
		switch {
		case r.URL.Path == "/internal/store/events" && r.Method == http.MethodPost:
			var e HazardEvent
			if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			f.add(e)
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/internal/store/events":
			limit := 100
			fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
			items := events
			if raw := r.URL.Query().Get("cursor"); raw != "" {
				cursor, err := decodeCursor(raw)
				if err != nil {
					http.Error(w, "bad cursor", http.StatusBadRequest)
					return
				}
				after := HazardEvent{IngestedAt: cursor.IngestedAt, HazardID: cursor.HazardID}
				var rest []HazardEvent
				for _, e := range items {
					if less(after, e) {
						rest = append(rest, e)
					}
				}
				items = rest
			}
			nextCursor := (*string)(nil)
			if len(items) > limit {
				items = items[:limit]
				last := items[len(items)-1]
				cursor := encodeCursor(auditCursor{IngestedAt: last.IngestedAt, HazardID: last.HazardID})
				nextCursor = &cursor
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "next_cursor": nextCursor})
		case r.URL.Path == "/internal/store/location":
			hazardID := r.URL.Query().Get("hazard_id")
			for _, e := range events {
				if e.HazardID == hazardID {
					_ = json.NewEncoder(w).Encode(map[string]any{
						"hazard_id": hazardID,
						"locations": []locationEntry{{Node: "test-node", Unit: f.name, Role: "primary"}},
					})
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/health":
			role := "primary"
			f.mu.Lock()
			if f.role != "" {
				role = f.role
			}
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "role": role})
		case r.URL.Path == "/internal/store/role" && r.Method == http.MethodPost:
			var body struct {
				Role string `json:"role"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			f.mu.Lock()
			f.role = body.Role
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "changed", "role": body.Role})
		case r.URL.Path == "/internal/store/stats":
			var newest time.Time
			f.mu.Lock()
			for _, e := range f.events {
				if e.IngestedAt.After(newest) {
					newest = e.IngestedAt
				}
			}
			if f.newest != nil {
				newest = *f.newest
			}
			count := len(f.events)
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"count": count,
				"newest": newest,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	f.servers = append(f.servers, srv)
	return srv
}

func (f *fakeStore) unit() unit {
	return unit{Name: f.name, IP: strings.TrimPrefix(f.servers[len(f.servers)-1].URL, "http://")}
}

func mkEvent(id string, at time.Time) HazardEvent {
	return HazardEvent{
		HazardID: id, Source: "BMKG", SourceRefID: "ref-" + id,
		HazardType: "SEISMIC", Severity: "NORMAL", AreaName: "Test",
		OccurredAt: at, IngestedAt: at, Attributes: map[string]any{},
	}
}

// ringWith builds a ring from a fixed unit list (client-go unavailable in
// unit tests).
func ringWith(t *testing.T, units ...unit) *ring {
	t.Helper()
	r := newRing(nilLogger())
	r.setUnits(units)
	return r
}

func nilLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRingConsistentHashing(t *testing.T) {
	// without SHARD_COUNT no shard is known and ownership must be empty
	r0 := newRing(nilLogger())
	if _, ok := r0.ownerShard(eventKey("BMKG", "e1")); ok {
		t.Fatal("empty shard ring must have no owner shard")
	}

	t.Setenv("SHARD_COUNT", "3")
	primary := unit{Name: "store-primary-0-0", IP: "10.0.0.1", Shard: "shard-0", Replica: "primary"}
	standby := unit{Name: "store-standby-0-0", IP: "10.0.0.2", Shard: "shard-0", Replica: "standby"}
	r := newRing(nilLogger())
	r.setUnits([]unit{primary, standby,
		{Name: "store-primary-1-0", IP: "10.0.0.3", Shard: "shard-1", Replica: "primary"},
		{Name: "store-primary-2-0", IP: "10.0.0.4", Shard: "shard-2", Replica: "primary"}})

	// same key always maps to the same shard
	s1, ok := r.ownerShard(eventKey("BMKG", "e1"))
	if !ok {
		t.Fatal("expected owner shard for non-empty ring")
	}
	s2, _ := r.ownerShard(eventKey("BMKG", "e1"))
	if s1 != s2 {
		t.Fatalf("unstable shard mapping: %s vs %s", s1, s2)
	}

	// ownership is static: dropping a pod must not move any key
	before := map[string]string{}
	for i := range 200 {
		key := eventKey("BMKG", fmt.Sprintf("key-%d", i))
		shard, _ := r.ownerShard(key)
		before[key] = shard
	}
	r.setUnits([]unit{
		{Name: "store-primary-1-0", IP: "10.0.0.3", Shard: "shard-1", Replica: "primary"},
		{Name: "store-primary-2-0", IP: "10.0.0.4", Shard: "shard-2", Replica: "primary"}})
	for key, shard := range before {
		after, _ := r.ownerShard(key)
		if after != shard {
			t.Fatalf("key %s moved from %s to %s when a pod left", key, shard, after)
		}
	}

	// vnode distribution: with vnodes every shard owns some keys
	counts := map[string]int{}
	for i := range 300 {
		key := eventKey("BMKG", fmt.Sprintf("dist-%d", i))
		shard, _ := r.ownerShard(key)
		counts[shard]++
	}
	if len(counts) != 3 {
		t.Fatalf("expected all 3 shards to own keys, got %v", counts)
	}
}

func TestRingCandidateOrder(t *testing.T) {
	t.Setenv("SHARD_COUNT", "2")
	r := newRing(nilLogger())
	primary := unit{Name: "store-primary-0-0", IP: "10.0.0.1", Shard: "shard-0", Replica: "primary"}
	standby := unit{Name: "store-standby-0-0", IP: "10.0.0.2", Shard: "shard-0", Replica: "standby"}
	r.setUnits([]unit{standby, primary})

	// primary comes first in the candidate order
	cands := r.candidates("shard-0")
	if len(cands) != 2 || cands[0].Name != primary.Name {
		t.Fatalf("expected primary first, got %v", cands)
	}

	// failing over: with the primary gone the standby is the only candidate
	r.setUnits([]unit{standby})
	cands = r.candidates("shard-0")
	if len(cands) != 1 || cands[0].Name != standby.Name {
		t.Fatalf("expected only the standby, got %v", cands)
	}

	// all() returns every replica for fan-out reads
	r.setUnits([]unit{standby, primary})
	if got := len(r.all()); got != 2 {
		t.Fatalf("expected 2 units in fan-out, got %d", got)
	}
}

func TestRingEmpty(t *testing.T) {
	r := ringWith(t)
	if _, ok := r.ownerShard("x"); ok {
		t.Fatal("empty ring must have no owner shard")
	}
}

func TestAuditEventsMergePaginate(t *testing.T) {
	logger := nilLogger()
	s1, s2 := newFakeStore("shard-1"), newFakeStore("shard-2")
	base := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	// interleave ingested_at across units to force a real merge
	s1.add(mkEvent("e1", base), mkEvent("e5", base.Add(4*time.Minute)))
	s2.add(mkEvent("e2", base.Add(1*time.Minute)), mkEvent("e3", base.Add(2*time.Minute)),
		mkEvent("e4", base.Add(3*time.Minute)))
	s1.start(t)
	s2.start(t)
	r := ringWith(t, s1.unit(), s2.unit())
	a := &aggregator{logger: logger, stores: newStoreClient(logger), ring: r}

	// page 1: limit 2
	req := httptest.NewRequest(http.MethodGet, "/internal/audit/events?limit=2", nil)
	rec := httptest.NewRecorder()
	a.auditEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var page struct {
		Items       []HazardEvent `json:"items"`
		NextCursor  *string       `json:"next_cursor"`
		Partial     bool          `json:"partial"`
		Unavailable []string      `json:"unavailable_units"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 ||
		page.Items[0].HazardID != "e1" || page.Items[1].HazardID != "e2" {
		t.Fatalf("bad page 1: %+v", page.Items)
	}
	if page.NextCursor == nil {
		t.Fatal("expected next_cursor on page 1")
	}

	// page 2: limit 2
	req = httptest.NewRequest(
		http.MethodGet, "/internal/audit/events?limit=2&cursor="+*page.NextCursor, nil)
	rec = httptest.NewRecorder()
	a.auditEvents(rec, req)
	var page2 struct {
		Items      []HazardEvent `json:"items"`
		NextCursor *string       `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page2); err != nil {
		t.Fatal(err)
	}
	if len(page2.Items) != 2 ||
		page2.Items[0].HazardID != "e3" || page2.Items[1].HazardID != "e4" {
		t.Fatalf("bad page 2: %+v", page2.Items)
	}
	if page2.NextCursor == nil {
		t.Fatal("expected next_cursor on page 2 (one item remains)")
	}

	// page 3: last item, null cursor
	req = httptest.NewRequest(
		http.MethodGet, "/internal/audit/events?limit=2&cursor="+*page2.NextCursor, nil)
	rec = httptest.NewRecorder()
	a.auditEvents(rec, req)
	var page3 struct {
		Items      []HazardEvent `json:"items"`
		NextCursor *string       `json:"next_cursor"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &page3)
	if len(page3.Items) != 1 || page3.Items[0].HazardID != "e5" || page3.NextCursor != nil {
		t.Fatalf("bad page 3: %+v", page3.Items)
	}
}

func TestAuditEventsPartialWhenUnitKilled(t *testing.T) {
	logger := nilLogger()
	s1, s2 := newFakeStore("shard-1"), newFakeStore("shard-2")
	base := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	s1.add(mkEvent("e1", base))
	s2.add(mkEvent("e2", base.Add(time.Minute)))
	s1.start(t)
	s2.start(t)
	r := ringWith(t, s1.unit(), s2.unit())
	a := &aggregator{logger: logger, stores: newStoreClient(logger), ring: r}

	// kill shard-2's server mid-request-set
	s2.servers[0].Close()

	req := httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil)
	rec := httptest.NewRecorder()
	a.auditEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("partial unit outage must not 500, got %d", rec.Code)
	}
	var page struct {
		Items       []HazardEvent `json:"items"`
		Partial     bool          `json:"partial"`
		Unavailable []string      `json:"unavailable_units"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if !page.Partial {
		t.Fatal("expected partial=true")
	}
	if len(page.Unavailable) != 1 || page.Unavailable[0] != "shard-2" {
		t.Fatalf("expected unavailable_units=[shard-2], got %v", page.Unavailable)
	}
	if len(page.Items) != 1 || page.Items[0].HazardID != "e1" {
		t.Fatalf("expected only reachable unit's items, got %+v", page.Items)
	}
}

// A unit whose pod is gone entirely (not-ready or deleted: no IP, absent
// from the live set) must still be probed and reported unavailable — its
// shard's data must not silently disappear from audit responses (Lampiran A).
func TestAuditEventsPartialWhenPodGone(t *testing.T) {
	logger := nilLogger()
	t.Setenv("SHARD_COUNT", "1")
	s1 := newFakeStore("store-primary-0-0")
	s1.start(t)
	s1.add(mkEvent("e1", time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)))
	r := newRing(nilLogger())
	live := s1.unit()
	live.Shard, live.Replica = "shard-0", "primary"
	r.setUnits([]unit{live})
	// standby's pod vanished from the API server; the ring only knows it
	// through the expected set built by rebuild().
	r.setExpected([]unit{
		{Name: live.Name, IP: live.IP, Shard: "shard-0", Replica: "primary"},
		{Name: "store-standby-0-0", Shard: "shard-0", Replica: "standby"},
	})
	a := &aggregator{logger: logger, stores: newStoreClient(logger), ring: r}

	req := httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil)
	rec := httptest.NewRecorder()
	a.auditEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("lost unit must not 500, got %d", rec.Code)
	}
	var page struct {
		Items       []HazardEvent `json:"items"`
		Partial     bool          `json:"partial"`
		Unavailable []string      `json:"unavailable_units"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if !page.Partial {
		t.Fatal("expected partial=true when a whole unit is gone")
	}
	if len(page.Unavailable) != 1 || page.Unavailable[0] != "store-standby-0-0" {
		t.Fatalf("expected unavailable_units=[store-standby-0-0], got %v", page.Unavailable)
	}
	if len(page.Items) != 1 {
		t.Fatalf("expected surviving unit's items, got %+v", page.Items)
	}

	// checksum must agree: 200 with partial, not 500.
	req = httptest.NewRequest(http.MethodGet, "/internal/audit/checksum?source=BMKG", nil)
	rec = httptest.NewRecorder()
	a.auditChecksum(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("checksum must stay 200 during unit loss, got %d", rec.Code)
	}
	var sum struct {
		Partial     bool     `json:"partial"`
		Unavailable []string `json:"unavailable_units"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &sum); err != nil {
		t.Fatal(err)
	}
	if !sum.Partial || len(sum.Unavailable) != 1 {
		t.Fatalf("expected checksum partial=true with one unavailable unit, got %v", sum)
	}

	// writes never target the lost unit: candidates stay live-only.
	if cands := r.candidates("shard-0"); len(cands) != 1 {
		t.Fatalf("expected candidates to exclude the lost unit, got %v", cands)
	}
}

func TestAuditChecksum(t *testing.T) {
	logger := nilLogger()
	s1, s2 := newFakeStore("shard-1"), newFakeStore("shard-2")
	base := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	refs := []string{}
	for _, id := range []string{"ref-delta", "ref-alpha", "ref-mike", "ref-bravo", "ref-charlie"} {
		refs = append(refs, id)
	}
	// spread across units; two share a ref to test dedup? No—refs are unique keys
	e1 := mkEvent("e1", base)
	e1.SourceRefID = "ref-delta"
	e2 := mkEvent("e2", base.Add(time.Second))
	e2.SourceRefID = "ref-alpha"
	e3 := mkEvent("e3", base.Add(2*time.Second))
	e3.SourceRefID = "ref-mike"
	s1.add(e1, e2)
	s2.add(e3)
	s1.start(t)
	s2.start(t)
	r := ringWith(t, s1.unit(), s2.unit())
	a := &aggregator{logger: logger, stores: newStoreClient(logger), ring: r}

	req := httptest.NewRequest(http.MethodGet, "/internal/audit/checksum?source=BMKG", nil)
	rec := httptest.NewRecorder()
	a.auditChecksum(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Source    string `json:"source"`
		Count     int    `json:"count"`
		Checksum  string `json:"checksum"`
		Algorithm string `json:"algorithm"`
		Partial   bool   `json:"partial"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Count != 3 || out.Algorithm != "sha256" {
		t.Fatalf("bad checksum response: %+v", out)
	}
	// manual computation: sorted byte-ascending, newline-joined, sha256
	sorted := []string{"ref-alpha", "ref-delta", "ref-mike"}
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	expected := hex.EncodeToString(sum[:])
	if out.Checksum != expected {
		t.Fatalf("checksum mismatch: got %s want %s", out.Checksum, expected)
	}
}

func TestAuditLocation(t *testing.T) {
	logger := nilLogger()
	s1, s2 := newFakeStore("shard-1"), newFakeStore("shard-2")
	base := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	s1.add(mkEvent("hz-1", base))
	s1.start(t)
	s2.start(t)
	r := ringWith(t, s1.unit(), s2.unit())
	a := &aggregator{logger: logger, stores: newStoreClient(logger), ring: r}

	req := httptest.NewRequest(http.MethodGet, "/internal/audit/location?hazard_id=hz-1", nil)
	rec := httptest.NewRecorder()
	a.auditLocation(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var out struct {
		HazardID  string          `json:"hazard_id"`
		Locations []locationEntry `json:"locations"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.HazardID != "hz-1" || len(out.Locations) != 1 || out.Locations[0].Unit != "shard-1" {
		t.Fatalf("bad location: %+v", out)
	}

	// unknown hazard id → 404
	req = httptest.NewRequest(http.MethodGet, "/internal/audit/location?hazard_id=nope", nil)
	rec = httptest.NewRecorder()
	a.auditLocation(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAuditToken(t *testing.T) {
	handler := auditToken("secret-token", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	// no token
	req := httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "unauthorized" || body.Error.Message != "token tidak valid" {
		t.Fatalf("bad lampiran error body: %s", rec.Body.String())
	}
	// wrong token
	req = httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong token, got %d", rec.Code)
	}
	// correct token
	req = httptest.NewRequest(http.MethodGet, "/internal/audit/events", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid token, got %d", rec.Code)
	}
}

func TestParseFailures(t *testing.T) {
	// non-JSON → non_json_payload
	_, err := parseBMKGEvents([]byte("not json at all {"))
	if err == nil || !strings.Contains(err.Error(), reasonNonJSON) {
		t.Fatalf("expected non_json_payload, got %v", err)
	}
	// wrong type (magnitude as string) → schema_incompatible
	_, err = parseBMKGEvents([]byte(`[{"event_id":"e1","magnitude":"six","occurred_at":"2026-01-01T00:00:00Z"}]`))
	if err == nil || !strings.Contains(err.Error(), reasonSchema) {
		t.Fatalf("expected schema_incompatible for wrong type, got %v", err)
	}
	// missing required field → schema_incompatible
	_, err = parseBMKGEvents([]byte(`[{"magnitude":5.1}]`))
	if err == nil || !strings.Contains(err.Error(), reasonSchema) {
		t.Fatalf("expected schema_incompatible for missing field, got %v", err)
	}
	// unknown fields tolerated
	events, err := parseBMKGEvents([]byte(
		`[{"event_id":"e1","occurred_at":"2026-01-01T00:00:00Z","brand_new_field":42}]`))
	if err != nil || len(events) != 1 {
		t.Fatalf("unknown field must be tolerated, got err=%v events=%d", err, len(events))
	}
	// PVMBG same rules
	if _, err = parsePVMBGReports([]byte("junk")); err == nil ||
		!strings.Contains(err.Error(), reasonNonJSON) {
		t.Fatalf("pvmbg non-json: %v", err)
	}
	if _, err = parsePVMBGReports([]byte(`[{"report_id":"r1"}]`)); err == nil ||
		!strings.Contains(err.Error(), reasonSchema) {
		t.Fatalf("pvmbg missing required: %v", err)
	}
	// raw payload path
	if _, err = parseRawPayload([]byte("nope")); err == nil {
		t.Fatal("raw payload non-json must fail")
	}
	event, err := parseRawPayload([]byte(
		`{"hazard_id":"h","source":"BMKG","source_ref_id":"r","occurred_at":"2026-01-01T00:00:00Z"}`))
	if err != nil || event.HazardID != "h" {
		t.Fatalf("raw payload valid: %v %+v", err, event)
	}
}

func TestUnprocessedInsertAndList(t *testing.T) {
	// bypass Postgres: test the insert/list contract via in-memory fakes is
	// not possible without a DB; instead assert the row mapping structurally.
	msg := unprocessedMessage{
		MessageID: "msg-31c9", Source: "PVMBG",
		ReceivedAt: time.Date(2026, 11, 12, 2, 40, 11, 0, time.UTC),
		Reason:     "schema_incompatible: field 'level' berubah dari number menjadi string",
		Retryable:  false, Attempts: 3, Status: "parked",
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"message_id", "source", "received_at", "reason", "retryable", "attempts", "status",
	} {
		if _, ok := back[key]; !ok {
			t.Fatalf("unprocessed row missing field %q", key)
		}
	}
	if msg.Status != "parked" || msg.Retryable {
		t.Fatal("non-retryable rows must be parked")
	}
}

func TestAnalyticsBucketing(t *testing.T) {
	base := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	event := mkEvent("e1", base.Add(30*time.Minute))
	event.HazardType = "SEISMIC"
	bucketKey := event.OccurredAt.Truncate(time.Hour).UTC().Format(time.RFC3339)
	if bucketKey != "2026-11-12T02:00:00Z" {
		t.Fatalf("bad bucket: %s", bucketKey)
	}
}

// replayTestPG returns a DSN for a live test Postgres; tests skip when none
// is provided (CI runs the container from scripts/test-deps.sh).
func replayTestPG(t *testing.T) *unprocessedStore {
	t.Helper()
	dsn := os.Getenv("AGGREGATOR_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("AGGREGATOR_TEST_POSTGRES_DSN not set; skipping Postgres-backed replay test")
	}
	ctx := context.Background()
	s, err := newUnprocessedStore(ctx, dsn)
	if err != nil {
		t.Fatalf("postgres connect: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM unprocessed_messages`); err != nil {
		t.Fatalf("clear table: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// validPayload is a canonical hazard payload the shared parseRawPayload accepts.
func validPayload(hazardID string) string {
	return fmt.Sprintf(
		`{"hazard_id":%q,"source":"PVMBG","source_ref_id":"ref-%s","hazard_type":"VOLCANIC",`+
			`"severity":"SIAGA","area_name":"Merapi","latitude":-7.54,"longitude":110.44,`+
			`"occurred_at":"2026-11-12T02:30:00Z","ingested_at":"2026-11-12T02:31:00Z"}`,
		hazardID, hazardID)
}

// brokenPayload is a poison row: non-JSON, stays parked forever.
const brokenPayload = `not json at all {`

func TestReplayRecoversWithoutDuplicates(t *testing.T) {
	logger := nilLogger()
	s1 := newFakeStore("shard-1")
	s1.start(t)
	r := ringWith(t, s1.unit())
	unproc := replayTestPG(t)
	a := &aggregator{
		logger: logger, stores: newStoreClient(logger), ring: r, unproc: unproc,
		kafka: kafka.NewProducer([]string{"localhost:19092"}), // not reached in this test
	}
	ctx := context.Background()

	// three parked rows: two valid-after-fix (replayable), one permanently broken
	_ = unproc.insert(ctx, "PVMBG", validPayload("h1"), "schema_incompatible: x", false)
	_ = unproc.insert(ctx, "PVMBG", validPayload("h2"), "schema_incompatible: x", false)
	_ = unproc.insert(ctx, "PVMBG", brokenPayload, "schema_incompatible: y", false)

	// replay everything via the admin handler
	req := httptest.NewRequest(http.MethodPost, "/internal/admin/replay",
		strings.NewReader(`{"all":true,"source":"PVMBG"}`))
	rec := httptest.NewRecorder()
	a.adminReplay(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var summary struct {
		Selected, Recovered, Failed int
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Selected != 3 || summary.Recovered != 2 || summary.Failed != 1 {
		t.Fatalf("bad replay summary: %+v", summary)
	}

	// recovered rows are gone from the audit listing; broken one remains parked
	rows, err := unproc.list(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 remaining row (broken), got %d: %+v", len(rows), rows)
	}
	if rows[0].Status != "parked" || rows[0].Attempts != 2 {
		t.Fatalf("broken row must stay parked with attempts incremented, got %+v", rows[0])
	}

	// exactly two hazard events persisted, one per source_ref_id
	if got := len(s1.events); got != 2 {
		t.Fatalf("expected 2 persisted hazard events, got %d", got)
	}
	ids := map[string]int{}
	for _, e := range s1.events {
		ids[e.SourceRefID]++
	}
	for _, ref := range []string{"ref-h1", "ref-h2"} {
		if ids[ref] != 1 {
			t.Fatalf("expected exactly 1 event for %s, got %d", ref, ids[ref])
		}
	}

	// replaying the same selection again: no second hazard event
	req = httptest.NewRequest(http.MethodPost, "/internal/admin/replay",
		strings.NewReader(`{"all":true,"source":"PVMBG"}`))
	rec = httptest.NewRecorder()
	a.adminReplay(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if got := len(s1.events); got != 2 {
		t.Fatalf("duplicate replay created extra events: %d", got)
	}
	rows, _ = unproc.list(ctx, "", 100)
	if len(rows) != 1 || rows[0].Attempts != 3 {
		t.Fatalf("repeat replay must only bump the broken row, got %+v", rows)
	}
}

func TestReplayOrderPerSourceByReceivedAt(t *testing.T) {
	logger := nilLogger()
	s1 := newFakeStore("shard-1")
	s1.start(t)
	r := ringWith(t, s1.unit())
	unproc := replayTestPG(t)
	a := &aggregator{
		logger: logger, stores: newStoreClient(logger), ring: r, unproc: unproc,
		kafka: kafka.NewProducer([]string{"localhost:19092"}),
	}
	ctx := context.Background()
	// insert out of order: h3 first but with the oldest received_at
	_ = unproc.insert(ctx, "PVMBG", validPayload("h3"), "schema: x", false)
	_ = unproc.insert(ctx, "PVMBG", validPayload("h1"), "schema: x", false)
	_ = unproc.insert(ctx, "PVMBG", validPayload("h2"), "schema: x", false)
	// rewrite received_at to force a deterministic oldest-first order
	for i, id := range []string{"h3", "h1", "h2"} {
		at := time.Date(2026, 11, 12, 2, i, 0, 0, time.UTC)
		if _, err := unproc.pool.Exec(ctx,
			`UPDATE unprocessed_messages SET received_at = $1 WHERE raw_payload LIKE '%ref-h%' || $2`,
			at, id); err != nil {
			t.Fatal(err)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/internal/admin/replay",
		strings.NewReader(`{"all":true,"source":"PVMBG"}`))
	rec := httptest.NewRecorder()
	a.adminReplay(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	// persisted in received_at order: h3 (00m), h1 (01m), h2 (02m)
	want := []string{"ref-h3", "ref-h1", "ref-h2"}
	if len(s1.events) != len(want) {
		t.Fatalf("expected %d events, got %d", len(want), len(s1.events))
	}
	for i, ref := range want {
		if s1.events[i].SourceRefID != ref {
			t.Fatalf("event %d = %s, want %s (per-source received_at order broken)",
				i, s1.events[i].SourceRefID, ref)
		}
	}
}

func TestReplayByIDAndStats(t *testing.T) {
	logger := nilLogger()
	s1 := newFakeStore("shard-1")
	s1.start(t)
	r := ringWith(t, s1.unit())
	unproc := replayTestPG(t)
	a := &aggregator{
		logger: logger, stores: newStoreClient(logger), ring: r, unproc: unproc,
		kafka: kafka.NewProducer([]string{"localhost:19092"}),
	}
	ctx := context.Background()
	_ = unproc.insert(ctx, "PVMBG", validPayload("h1"), "schema: x", false)
	_ = unproc.insert(ctx, "PVMBG", validPayload("h2"), "schema: x", false)
	rows, err := unproc.list(ctx, "", 100)
	if err != nil || len(rows) != 2 {
		t.Fatalf("seed rows: %v %d", err, len(rows))
	}
	target := rows[0].MessageID
	wantRef := "ref-h1"
	if rows[0].RawPayload != validPayload("h1") {
		wantRef = "ref-h2"
	}

	// stats endpoint counts by status
	req := httptest.NewRequest(http.MethodGet, "/internal/admin/replay/stats", nil)
	rec := httptest.NewRecorder()
	a.adminReplayStats(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("stats status %d", rec.Code)
	}
	var stats struct {
		ByStatus map[string]int `json:"by_status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats.ByStatus["parked"] != 2 {
		t.Fatalf("expected 2 parked in stats, got %+v", stats.ByStatus)
	}

	// replay one row by id
	req = httptest.NewRequest(http.MethodPost, "/internal/admin/replay",
		strings.NewReader(`{"ids":["`+target+`"]}`))
	rec = httptest.NewRecorder()
	a.adminReplay(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if len(s1.events) != 1 || s1.events[0].SourceRefID != wantRef {
		t.Fatalf("replay by id persisted wrong event: %+v", s1.events)
	}
}

func TestReplayAuthAndValidation(t *testing.T) {
	logger := nilLogger()
	a := &aggregator{logger: logger}

	// missing ids/all -> 400
	req := httptest.NewRequest(http.MethodPost, "/internal/admin/replay",
		strings.NewReader(`{"source":"PVMBG"}`))
	rec := httptest.NewRecorder()
	a.adminReplay(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty selection, got %d", rec.Code)
	}

	// unauthenticated through the token middleware -> 401
	handler := auditToken("secret", http.HandlerFunc(a.adminReplayStats))
	req = httptest.NewRequest(http.MethodGet, "/internal/admin/replay/stats", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/internal/admin/replay", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", rec.Code)
	}
}

func TestShardControllerFailover(t *testing.T) {
	logger := nilLogger()
	t.Setenv("SHARD_COUNT", "2")
	r := newRing(logger)

	// shard-0: primary dead (server closed), standby alive and labelled
	// primary (the shard controller must promote it).
	dead := newFakeStore("store-primary-0-0")
	deadSrv := dead.start(t)
	deadSrv.Close()
	alive := newFakeStore("store-standby-0-0")
	aliveSrv := alive.start(t)
	alive.setRole("standby")

	r.setUnits([]unit{
		{Name: "store-primary-0-0", IP: strings.TrimPrefix(deadSrv.URL, "http://"), Shard: "shard-0", Replica: "primary"},
		{Name: "store-standby-0-0", IP: strings.TrimPrefix(aliveSrv.URL, "http://"), Shard: "shard-0", Replica: "standby"},
		{Name: "store-primary-1-0", IP: "127.0.0.1:1", Shard: "shard-1", Replica: "primary"},
	})

	a := &aggregator{logger: logger, stores: newStoreClient(logger), ring: r}
	a.reconcileShards(context.Background())

	if alive.role != "primary" {
		t.Fatalf("expected surviving standby promoted to primary, role=%q", alive.role)
	}
}

func TestShardControllerSplitBrain(t *testing.T) {
	logger := nilLogger()
	t.Setenv("SHARD_COUNT", "1")
	r := newRing(nilLogger())

	fresh := newFakeStore("store-primary-0-0")
	freshSrv := fresh.start(t)
	fresh.setRole("primary")
	fresh.add(mkEvent("newer", time.Now().UTC().Add(time.Minute)))
	stale := newFakeStore("store-standby-0-0")
	staleSrv := stale.start(t)
	stale.add(mkEvent("older", time.Now().UTC().Add(-time.Minute)))
	stale.setRole("primary") // promoted while the original primary was down

	// both answer primary -> split brain; the freshest must survive.
	r.setUnits([]unit{
		{Name: "store-primary-0-0", IP: strings.TrimPrefix(freshSrv.URL, "http://"), Shard: "shard-0", Replica: "primary"},
		{Name: "store-standby-0-0", IP: strings.TrimPrefix(staleSrv.URL, "http://"), Shard: "shard-0", Replica: "standby"},
	})

	a := &aggregator{logger: logger, stores: newStoreClient(logger), ring: r}
	a.reconcileShards(context.Background())

	if stale.role != "standby" {
		t.Fatalf("expected stale unit demoted, role=%q", stale.role)
	}
	if fresh.role != "primary" {
		t.Fatalf("expected fresh unit to remain primary, role=%q", fresh.role)
	}
}
