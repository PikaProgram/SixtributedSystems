package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Clustering model (pure, no I/O) so the algorithm is unit-testable.
//
// A detection is identified by satellite|acq_date|acq_time|latitude|longitude.
// Two detections link when geodesic distance <= 1.0 km AND |Δt| <= 360 min.
// Linking is transitive (chained); a connected component of size >= 2 is a
// fire. Union-find over fire_edges keeps membership; the fire's stable
// source_ref_id is sha256 over the SORTED member detection keys, so
// reprocessing the same cluster yields the same id.

const (
	linkRadiusKM    = 1.0
	linkWindowMins  = 360.0
	earthRadiusKM   = 6371.0088
	detectionFields = 5
)

// detection is one parsed detections.raw message.
type detection struct {
	Satellite  string
	AcqDate    string
	AcqTime    string
	Latitude   float64
	Longitude  float64
	Confidence string
	FRPMW      float64
	Key        string
	Time       timeUTC
}

// timeUTC is a detection acquisition time; acq_time is UTC HHMM (or HMM) per VIIRS.
type timeUTC struct {
	Unix int64 // seconds since epoch; 0 means "unknown" and never links
	OK   bool
}

// parseAcqTime joins acq_date (YYYY-MM-DD) and acq_time (HHMM, possibly 3 digits).
func parseAcqTime(date, hm string) timeUTC {
	hm = strings.TrimSpace(hm)
	for len(hm) < 4 {
		hm = "0" + hm
	}
	if len(hm) > 4 || len(date) != 10 {
		return timeUTC{}
	}
	var y, mo, d, h, mi int
	if _, err := fmt.Sscanf(date, "%4d-%2d-%2d", &y, &mo, &d); err != nil {
		return timeUTC{}
	}
	if _, err := fmt.Sscanf(hm, "%2d%2d", &h, &mi); err != nil {
		return timeUTC{}
	}
	if mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 {
		return timeUTC{}
	}
	secs := daysFromCivil(y, mo, d)*86400 + int64(h*3600+mi*60)
	return timeUTC{Unix: secs, OK: true}
}

// daysFromCivil converts a civil date to days since 1970-01-01 (Howard Hinnant's algorithm).
func daysFromCivil(y, m, d int) int64 {
	y -= boolToInt(m <= 2)
	era := int64(y) / 400
	yoe := int64(y) - era*400
	mp := int64(m) - 3 // month position in [Mar=0..Feb=11]
	if m <= 2 {
		mp = int64(m) + 9
	}
	doy := (153*mp+2)/5 + int64(d) - 1
	return era*146097 + yoe*365 + yoe/4 - yoe/100 + doy - 719468
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// detectionKey joins the identity fields raw, no rounding.
func detectionKey(satellite, acqDate, acqTime string, lat, lon float64) string {
	return satellite + "|" + acqDate + "|" + acqTime + "|" +
		rawFloat(lat) + "|" + rawFloat(lon)
}

// rawFloat renders a coordinate with the shortest representation that
// round-trips (same as Go's JSON encoder); the builder receives the feed's
// value over a JSON round-trip through float64 anyway, so this preserves
// effective precision without trailing-zero games.
func rawFloat(v float64) string {
	return string(strconv.AppendFloat(make([]byte, 0, 32), v, 'g', -1, 64))
}

// haversine returns the geodesic distance between two points in kilometers.
func haversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	rad := math.Pi / 180
	φ1, φ2 := lat1*rad, lat2*rad
	dφ := (lat2 - lat1) * rad
	dλ := (lon2 - lon1) * rad
	a := math.Sin(dφ/2)*math.Sin(dφ/2) +
		math.Cos(φ1)*math.Cos(φ2)*math.Sin(dλ/2)*math.Sin(dλ/2)
	return 2 * earthRadiusKM * math.Asin(math.Sqrt(a))
}

// linked reports whether two detections are directly connected.
func linked(a, b detection) bool {
	if !a.Time.OK || !b.Time.OK {
		return false
	}
	dt := a.Time.Unix - b.Time.Unix
	if dt < 0 {
		dt = -dt
	}
	if dt > int64(linkWindowMins*60) {
		return false
	}
	return haversineKM(a.Latitude, a.Longitude, b.Latitude, b.Longitude) <= linkRadiusKM
}

// unionFind over detection keys (fire_edges chain membership).
type unionFind struct {
	parent map[string]string
}

func newUnionFind() *unionFind { return &unionFind{parent: map[string]string{}} }

func (uf *unionFind) find(x string) string {
	for uf.parent[x] != x {
		uf.parent[x] = uf.parent[uf.parent[x]] // path halving
		x = uf.parent[x]
	}
	return x
}

func (uf *unionFind) ensure(x string) {
	if _, ok := uf.parent[x]; !ok {
		uf.parent[x] = x
	}
}

func (uf *unionFind) union(a, b string) {
	ra, rb := uf.find(a), uf.find(b)
	if ra != rb {
		uf.parent[rb] = ra
	}
}

// clusters returns all connected components with >= 1 member, each as sorted keys.
func (uf *unionFind) clusters() [][]string {
	members := map[string][]string{}
	for k := range uf.parent {
		r := uf.find(k)
		members[r] = append(members[r], k)
	}
	out := make([][]string, 0, len(members))
	for _, m := range members {
		sort.Strings(m)
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// fireSourceRefID derives the stable fire id: "fire-" + first 6 hex of sha256
// over the SORTED detection keys joined by newline.
func fireSourceRefID(keys []string) string {
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return "fire-" + hex.EncodeToString(sum[:])[:6]
}

// fireStats aggregates one cluster into the HazardEvent fields.
type fireStats struct {
	DetectionCount int
	Satellites     []string
	MaxConfidence  string
	TotalFRPMW     float64
	DetectionKeys  []string
	CentroidLat    float64
	CentroidLon    float64
	OccurredAtUnix int64
	HasTime        bool
}

// statsFor computes centroid, earliest time, satellites, max confidence and
// total FRP over the cluster's detections.
func statsFor(keys []string, byKey map[string]detection) fireStats {
	var s fireStats
	s.DetectionKeys = append([]string(nil), keys...)
	sort.Strings(s.DetectionKeys)
	var latSum, lonSum float64
	for _, k := range keys {
		d, ok := byKey[k]
		if !ok {
			continue // member loaded from PG lacks its payload; counted via DB below
		}
		s.DetectionCount++
		latSum += d.Latitude
		lonSum += d.Longitude
		s.Satellites = appendUnique(s.Satellites, d.Satellite)
		s.MaxConfidence = maxConfidence(s.MaxConfidence, d.Confidence)
		s.TotalFRPMW += d.FRPMW
		if d.Time.OK && (!s.HasTime || d.Time.Unix < s.OccurredAtUnix) {
			s.OccurredAtUnix = d.Time.Unix
			s.HasTime = true
		}
	}
	if s.DetectionCount > 0 {
		s.CentroidLat = latSum / float64(s.DetectionCount)
		s.CentroidLon = lonSum / float64(s.DetectionCount)
	}
	return s
}

// severityFor: AWAS if >= 20 detections or total FRP >= 100 MW; SIAGA if
// >= 6 detections or total FRP >= 30 MW; else WASPADA.
func severityFor(count int, totalFRP float64) string {
	switch {
	case count >= 20 || totalFRP >= 100:
		return "AWAS"
	case count >= 6 || totalFRP >= 30:
		return "SIAGA"
	default:
		return "WASPADA"
	}
}

// maxConfidence ranks nominal VIIRS confidence: h > n > l (anything else sorts lowest).
func maxConfidence(a, b string) string {
	if rank(b) > rank(a) {
		return b
	}
	return a
}

func rank(c string) int {
	switch strings.ToLower(strings.TrimSpace(c)) {
	case "h":
		return 3
	case "n", "nominal":
		return 2
	case "l", "low":
		return 1
	default:
		return 0
	}
}

func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}
