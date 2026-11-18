package main

import (
	"testing"
)

func det(sat, date, hm string, lat, lon float64, conf string, frp float64) detection {
	key := detectionKey(sat, date, hm, lat, lon)
	return detection{
		Satellite: sat, AcqDate: date, AcqTime: hm,
		Latitude: lat, Longitude: lon, Confidence: conf, FRPMW: frp,
		Key: key, Time: parseAcqTime(date, hm),
	}
}

const day = "2026-11-12"

// linkedAndClustered checks the pure clustering rules without Postgres: pair
// linking plus transitive chaining through union-find.
func clusterOfAll(dets []detection) [][]string {
	uf := newUnionFind()
	for _, d := range dets {
		uf.ensure(d.Key)
	}
	for i := range dets {
		for j := i + 1; j < len(dets); j++ {
			if linked(dets[i], dets[j]) {
				uf.union(dets[i].Key, dets[j].Key)
			}
		}
	}
	return uf.clusters()
}

func TestPairWithin1km360minFormsFire(t *testing.T) {
	// ~30m apart, 10 minutes apart
	dets := []detection{
		det("N20", day, "0243", -2.5123, 113.8044, "n", 5),
		det("N20", day, "0253", -2.5125, 113.8047, "n", 7),
	}
	clusters := clusterOfAll(dets)
	if len(clusters) != 1 || len(clusters[0]) != 2 {
		t.Fatalf("want 1 fire of 2 detections, got %v", clusters)
	}
	st := statsFor(clusters[0], map[string]detection{dets[0].Key: dets[0], dets[1].Key: dets[1]})
	if got := severityFor(st.DetectionCount, st.TotalFRPMW); got != "WASPADA" {
		t.Fatalf("severity = %s, want WASPADA", got)
	}
}

func TestTooFarAloneNotFire(t *testing.T) {
	// 2 detections ~2km apart: not linked, two singletons (no fire)
	dets := []detection{
		det("N20", day, "0243", -2.5123, 113.8044, "n", 5),
		det("N20", day, "0243", -2.5302, 113.8044, "n", 5),
	}
	clusters := clusterOfAll(dets)
	if len(clusters) != 2 || len(clusters[0]) != 1 {
		t.Fatalf("want 2 singletons, got %v", clusters)
	}
}

func TestTooOldAloneNotFire(t *testing.T) {
	// same point, 370 minutes apart: outside the 360 min window
	dets := []detection{
		det("N20", day, "0200", -2.5123, 113.8044, "n", 5),
		det("N20", day, "0810", -2.5123, 113.8044, "n", 5),
	}
	clusters := clusterOfAll(dets)
	if len(clusters) != 2 {
		t.Fatalf("want 2 singletons, got %v", clusters)
	}
}

func TestChainTransitiveSameFire(t *testing.T) {
	// A-B-C: each hop ~600m / 60min apart, but A-C is ~1.2km apart; all one fire.
	dets := []detection{
		det("N20", day, "0200", -2.51230, 113.80440, "n", 5),
		det("N20", day, "0300", -2.51230, 113.80950, "n", 5),
		det("N20", day, "0400", -2.51230, 113.81460, "n", 5),
	}
	if d := haversineKM(dets[0].Latitude, dets[0].Longitude, dets[2].Latitude, dets[2].Longitude); d <= linkRadiusKM {
		t.Fatalf("A-C should exceed 1km, got %.3fkm", d)
	}
	clusters := clusterOfAll(dets)
	if len(clusters) != 1 || len(clusters[0]) != 3 {
		t.Fatalf("want one chained fire of 3, got %v", clusters)
	}
}

func TestTwentyDetectionsAWAS(t *testing.T) {
	dets := []detection{}
	for i := range 20 {
		// each ~200m from the last, 5 minutes apart: one chained cluster
		dets = append(dets, det("N20", day, "0200",
			-2.5123+float64(i)*0.0018, 113.8044, "n", 1))
	}
	clusters := clusterOfAll(dets)
	if len(clusters) != 1 || len(clusters[0]) != 20 {
		t.Fatalf("want one fire of 20, got %d clusters", len(clusters))
	}
	st := statsFor(clusters[0], byKey(dets))
	if got := severityFor(st.DetectionCount, st.TotalFRPMW); got != "AWAS" {
		t.Fatalf("severity = %s, want AWAS", got)
	}
}

func TestMultiSatelliteSameFire(t *testing.T) {
	dets := []detection{
		det("N20", day, "0243", -2.5123, 113.8044, "n", 5),
		det("N21", day, "0253", -2.5125, 113.8047, "h", 40),
	}
	clusters := clusterOfAll(dets)
	if len(clusters) != 1 {
		t.Fatalf("want 1 fire, got %v", clusters)
	}
	st := statsFor(clusters[0], byKey(dets))
	if len(st.Satellites) != 2 || st.MaxConfidence != "h" {
		t.Fatalf("satellites = %v, max_confidence = %q", st.Satellites, st.MaxConfidence)
	}
	if got := severityFor(st.DetectionCount, st.TotalFRPMW); got != "SIAGA" {
		t.Fatalf("severity = %s, want SIAGA (FRP 45 >= 30)", got)
	}
}

func TestDuplicateKeyDeduped(t *testing.T) {
	a := det("N20", day, "0243", -2.5123, 113.8044, "n", 5)
	b := det("N20", day, "0243", -2.5123, 113.8044, "n", 5)
	if a.Key != b.Key {
		t.Fatalf("identical detections must share a key: %q vs %q", a.Key, b.Key)
	}
	// the builder dedupes by key (ON CONFLICT DO NOTHING); verify key shape
	want := "N20|" + day + "|0243|-2.5123|113.8044"
	if a.Key != want {
		t.Fatalf("key = %q, want %q", a.Key, want)
	}
}

func TestLowConfidenceDropped(t *testing.T) {
	for _, conf := range []string{"low", "l", "Low"} {
		_, ok, reason := parseDetection([]byte(`{"satellite":"N20","acq_date":"` + day +
			`","acq_time":"0243","latitude":-2.5,"longitude":113.8,"confidence":"` + conf + `","frp_mw":5}`))
		if ok {
			t.Fatalf("confidence %q must be dropped", conf)
		}
		if reason != "confidence_low" {
			t.Fatalf("reason = %q, want confidence_low", reason)
		}
	}
}

func TestSourceRefIDStableAcrossReprocessing(t *testing.T) {
	keys := []string{
		detectionKey("N21", day, "0253", -2.5125, 113.8047),
		detectionKey("N20", day, "0243", -2.5123, 113.8044),
	}
	id1 := fireSourceRefID(keys)
	id2 := fireSourceRefID([]string{keys[1], keys[0]}) // different input order
	if id1 != id2 {
		t.Fatalf("source_ref_id not stable across order: %q vs %q", id1, id2)
	}
	// reprocessing the same detections from PG reproduces the same keys
	reprocessed := []string{
		detectionKey("N20", day, "0243", -2.5123, 113.8044),
		detectionKey("N21", day, "0253", -2.5125, 113.8047),
	}
	if id1 != fireSourceRefID(reprocessed) {
		t.Fatalf("source_ref_id not stable across reprocessing")
	}
	if len(id1) != len("fire-")+6 {
		t.Fatalf("id shape = %q, want fire-<6 hex>", id1)
	}
}

func TestSeverityBoundaries(t *testing.T) {
	cases := []struct {
		count int
		frp   float64
		want  string
	}{
		{1, 0, "WASPADA"},
		{2, 29.9, "WASPADA"},
		{2, 30, "SIAGA"},
		{6, 0, "SIAGA"},
		{19, 99.9, "SIAGA"},
		{20, 0, "AWAS"},
		{2, 100, "AWAS"},
	}
	for _, c := range cases {
		if got := severityFor(c.count, c.frp); got != c.want {
			t.Fatalf("severityFor(%d, %v) = %s, want %s", c.count, c.frp, got, c.want)
		}
	}
}

func TestParseAcqTime(t *testing.T) {
	cases := []struct {
		date, hm string
		ok       bool
	}{
		{day, "0243", true},
		{day, "243", true}, // 3-digit HHMM variant zero-padded
		{day, "0000", true},
		{day, "2400", false},
		{day, "0260", false},
		{"2026-13-01", "0243", false},
		{"bad-date", "0243", false},
	}
	for _, c := range cases {
		if got := parseAcqTime(c.date, c.hm); got.OK != c.ok {
			t.Fatalf("parseAcqTime(%q, %q).OK = %v, want %v", c.date, c.hm, got.OK, c.ok)
		}
	}
	// 2026-11-12T02:43 UTC
	if got := parseAcqTime(day, "0243").Unix; got != 1794451380 {
		t.Fatalf("unix = %d, want 1794451380", got)
	}
}

func byKey(dets []detection) map[string]detection {
	m := map[string]detection{}
	for _, d := range dets {
		m[d.Key] = d
	}
	return m
}
