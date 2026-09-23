package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
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
type service struct {
	mu       sync.RWMutex
	events   []SeismicEvent
	warnings []TsunamiWarning
	key      string
	delay    time.Duration
}

func main() {
	logger := platform.Logger("bmkg-mock")
	now := time.Now().UTC()
	events := make([]SeismicEvent, 0, 20)
	for i := range 20 {
		events = append(events, SeismicEvent{
			EventID: fmt.Sprintf("seismic-%02d", i+1), Magnitude: 4.5 + float64(i%5)*0.6,
			DepthKM: 8 + float64(i), EpicenterLat: -6.2 + float64(i)*0.03,
			EpicenterLon: 106.7 + float64(i)*0.04, RegionName: fmt.Sprintf("Region-%02d", i+1),
			OccurredAt: now.Add(-time.Duration(20-i) * time.Hour), PotentialTsunami: i%3 == 0,
		})
	}
	warnings := []TsunamiWarning{
		{
			WarningID:        "warning-01",
			RelatedEventID:   "seismic-16",
			ThreatLevel:      "Awas",
			AffectedZones:    []string{"Coastal Zone A"},
			EstimatedArrival: now.Add(2 * time.Hour),
		},
		{
			WarningID:        "warning-02",
			RelatedEventID:   "seismic-04",
			ThreatLevel:      "Waspada",
			AffectedZones:    []string{"Coastal Zone B"},
			EstimatedArrival: now.Add(3 * time.Hour),
		},
	}
	s := &service{
		events: events, warnings: warnings, key: platform.Env("BMKG_API_KEY", "example-bmkg-key"),
		delay: time.Duration(platform.EnvInt("BMKG_DELAY_MS", 100)) * time.Millisecond,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/seismic-events", s.seismic)
	mux.HandleFunc("/tsunami-warnings", s.warningsHandler)
	logger.Info("starting", "port", platform.Env("BMKG_PORT", "8081"))
	_ = http.ListenAndServe(
		":"+platform.Env("BMKG_PORT", "8081"),
		platform.CorrelationMiddleware(logger, mux),
	)
}
func (s *service) generate(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for now := range ticker.C {
		s.mu.Lock()
		id := fmt.Sprintf("seismic-live-%d", now.UnixNano())
		s.events = append(s.events, SeismicEvent{
			EventID:          id,
			Magnitude:        5.2,
			DepthKM:          12,
			EpicenterLat:     -6.3,
			EpicenterLon:     106.8,
			RegionName:       "Live Region",
			OccurredAt:       now.UTC(),
			PotentialTsunami: false,
		})
		s.mu.Unlock()
	}
}
func (s *service) auth(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("X-BMKG-Key") != s.key {
		platform.JSON(w, 401, map[string]string{"error": "invalid BMKG credential"})
		return false
	}
	return true
}
func (s *service) health(w http.ResponseWriter, _ *http.Request) {
	platform.JSON(w, 200, map[string]string{"status": "ok", "service": "bmkg-mock"})
}
func since(r *http.Request) (time.Time, error) {
	raw := r.URL.Query().Get("since")
	if raw == "" {
		return time.Unix(0, 0), nil
	}
	return time.Parse(time.RFC3339, raw)
}
func (s *service) seismic(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	cutoff, err := since(r)
	if err != nil {
		platform.JSON(w, 400, map[string]string{"error": "since must be RFC3339"})
		return
	}
	time.Sleep(s.delay)
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []SeismicEvent{}
	for _, e := range s.events {
		if e.OccurredAt.After(cutoff) {
			out = append(out, e)
		}
	}
	platform.JSON(w, 200, out)
}
func (s *service) warningsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	cutoff, err := since(r)
	if err != nil {
		platform.JSON(w, 400, map[string]string{"error": "since must be RFC3339"})
		return
	}
	time.Sleep(s.delay)
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []TsunamiWarning{}
	for _, warning := range s.warnings {
		if warning.EstimatedArrival.After(cutoff) {
			out = append(out, warning)
		}
	}
	platform.JSON(w, 200, out)
}
