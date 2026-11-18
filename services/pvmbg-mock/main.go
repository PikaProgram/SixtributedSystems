package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
	ConfidenceLevel  *float64  `json:"confidence_level,omitempty"`
}

type toggleRequest struct {
	Enabled bool `json:"enabled"`
}
type service struct {
	mu                 sync.RWMutex
	reports            []VolcanicReport
	token              string
	outage             bool
	schemaVersion      bool
	schemaMode         string
	logger             *slog.Logger
	minDelay, maxDelay time.Duration
}

// M2 Problem 3.2: modes emulate upstream schema drift; serve-time so stored
// and freshly generated records both reflect the current mode.
func applySchemaMode(report VolcanicReport, mode string) map[string]any {
	data, _ := json.Marshal(report)
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	switch mode {
	case "rename_field": // alert_level -> level
		out["level"] = out["alert_level"]
		delete(out, "alert_level")
	case "type_change": // string level -> numeric code
		out["alert_level"] = map[string]int{
			"Normal": 1, "Waspada": 2, "Siaga": 3, "Awas": 4,
		}[report.AlertLevel]
	case "required_missing":
		delete(out, "alert_level")
	}
	return out
}

func main() {
	logger := platform.Logger("pvmbg-mock")
	shutdownTracer, err := platform.InitTracer("pvmbg-mock")
	if err != nil {
		logger.Error("tracer_init", "error", err)
	} else {
		defer shutdownTracer()
	}
	shutdownMetrics, err := platform.InitMetrics("pvmbg-mock")
	if err != nil {
		logger.Error("metrics_init", "error", err)
	} else {
		defer shutdownMetrics()
	}
	now := time.Now().UTC()
	reports := make([]VolcanicReport, 0, 20)
	levels := []string{"Normal", "Waspada", "Siaga", "Awas"}
	for i := range 20 {
		reports = append(reports, VolcanicReport{
			ReportID: fmt.Sprintf(
				"report-%02d",
				i+1,
			),
			VolcanoID:        fmt.Sprintf("volcano-%02d", i%4+1),
			AlertLevel:       levels[i%len(levels)],
			EruptionCount24h: i % 6,
			AshColumnHeightM: float64(
				300 + i*125,
			),
			ReportedAt: now.Add(-time.Duration(20-i) * time.Hour),
		})
	}
	s := &service{
		reports: reports, token: platform.Env("PVMBG_TOKEN", "example-pvmbg-token"),
		schemaMode: "baseline", logger: logger,
		minDelay: time.Duration(platform.EnvInt("PVMBG_DELAY_MIN_MS", 500)) * time.Millisecond,
		maxDelay: time.Duration(platform.EnvInt("PVMBG_DELAY_MAX_MS", 3000)) * time.Millisecond,
	}
	go s.generate(platform.EnvDurationSeconds("EVENT_GENERATION_INTERVAL_SECONDS", 10*time.Second))
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/volcanic-reports", s.reportsHandler)
	mux.HandleFunc("/admin/schema-version", s.schemaHandler)
	mux.HandleFunc("/admin/outage", s.outageHandler)
	logger.Info("starting", "port", platform.Env("PVMBG_PORT", "8082"))
	_ = http.ListenAndServe(
		":"+platform.Env("PVMBG_PORT", "8082"),
		platform.HTTPMiddleware(logger, mux),
	)
}
func (s *service) generate(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for now := range ticker.C {
		s.mu.Lock()
		s.reports = append(s.reports, VolcanicReport{
			ReportID:         fmt.Sprintf("report-live-%d", now.UnixNano()),
			VolcanoID:        "volcano-01",
			AlertLevel:       "Waspada",
			EruptionCount24h: 1,
			AshColumnHeightM: 500,
			ReportedAt:       now.UTC(),
		})
		s.mu.Unlock()
	}
}

func (s *service) auth(w http.ResponseWriter, r *http.Request) bool {
	expected := "Bearer " + s.token
	if r.Header.Get("Authorization") != expected {
		platform.JSON(
			w,
			http.StatusUnauthorized,
			map[string]string{"error": "invalid PVMBG credential"},
		)
		return false
	}
	return true
}
func (s *service) health(w http.ResponseWriter, _ *http.Request) {
	platform.JSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "pvmbg-mock"})
}
func (s *service) reportsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	s.mu.RLock()
	outage, schema, mode := s.outage, s.schemaVersion, s.schemaMode
	reports := append([]VolcanicReport(nil), s.reports...)
	s.mu.RUnlock()
	if outage {
		platform.JSON(w, http.StatusServiceUnavailable, map[string]string{"error": "PVMBG outage"})
		return
	}
	delay := s.minDelay
	if s.maxDelay > s.minDelay {
		delay += time.Duration(rand.Int64N(int64(s.maxDelay-s.minDelay) + 1))
	}
	time.Sleep(delay)
	cutoff, err := since(r)
	if err != nil {
		platform.JSON(w, 400, map[string]string{"error": "since must be RFC3339"})
		return
	}
	out := make([]map[string]any, 0)
	for _, report := range reports {
		if report.ReportedAt.After(cutoff) {
			if schema {
				value := 0.65 + rand.Float64()*0.34
				report.ConfidenceLevel = &value
			}
			out = append(out, applySchemaMode(report, mode))
		}
	}
	platform.JSON(w, 200, out)
}
func (s *service) schemaHandler(w http.ResponseWriter, r *http.Request) {
	// admin endpoints are ops/tester facing: no auth
	if r.Method == http.MethodGet {
		s.mu.RLock()
		mode := s.schemaMode
		s.mu.RUnlock()
		platform.JSON(w, 200, map[string]string{"mode": mode})
		return
	}
	var req struct {
		Mode    string `json:"mode"`
		Enabled *bool  `json:"enabled"` // legacy M1 toggle, orthogonal to mode
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		platform.JSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.Enabled != nil {
		s.mu.Lock()
		s.schemaVersion = *req.Enabled
		s.mu.Unlock()
	}
	mode := req.Mode
	if mode == "" {
		mode = "baseline"
	}
	switch mode {
	case "baseline", "rename_field", "type_change", "required_missing":
	default:
		platform.JSON(w, 400, map[string]string{"error": "mode must be baseline, rename_field, type_change, or required_missing"})
		return
	}
	s.mu.Lock()
	previous := s.schemaMode
	s.schemaMode = mode
	s.mu.Unlock()
	if previous != mode {
		s.logger.Info("schema mode changed", "previous", previous, "mode", mode)
	}
	platform.JSON(w, 200, map[string]any{"mode": mode, "schema_version": s.schemaVersion})
}
func (s *service) outageHandler(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	var req toggleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		platform.JSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	s.mu.Lock()
	s.outage = req.Enabled
	s.mu.Unlock()
	platform.JSON(w, 200, map[string]any{"outage": req.Enabled})
}
func since(r *http.Request) (time.Time, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("since"))
	if raw == "" {
		return time.Unix(0, 0), nil
	}
	return time.Parse(time.RFC3339, raw)
}
