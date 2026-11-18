package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

// Problem 5.5: scope-based shedding. field-team (hazard:raw) bypasses; media
// (hazard:summary) goes through a per-client_id token bucket. The concurrency
// semaphore in limit() applies to both classes; shedding is class-based only.
type shedBucket struct {
	tokens float64
	last   time.Time
}

type shedder struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	buckets map[string]*shedBucket
}

func newShedder(perMinute, burst int) *shedder {
	return &shedder{
		rate:    float64(perMinute) / 60,
		burst:   float64(burst),
		buckets: map[string]*shedBucket{},
	}
}

func (sh *shedder) allow(clientID string) bool {
	now := time.Now()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	b, ok := sh.buckets[clientID]
	if !ok {
		b = &shedBucket{tokens: sh.burst, last: now}
		sh.buckets[clientID] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * sh.rate
	if b.tokens > sh.burst {
		b.tokens = sh.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// shed returns false and writes the 429 Lampiran A error body when the
// request class is shed. Called after JWT validation in each handler.
func (s *service) shed(w http.ResponseWriter, r *http.Request, claims map[string]any) bool {
	if s.hasScope(claims, "hazard:raw") { // field-team bypasses shedding
		return true
	}
	clientID, _ := claims["client_id"].(string)
	if s.mediaShed.allow(clientID) {
		return true
	}
	platform.JSON(w, 429, map[string]any{
		"error": map[string]string{"code": "rate_limited", "message": "terlalu banyak permintaan"},
	})
	return false
}

func (s *service) hasScope(claims map[string]any, want string) bool {
	values, ok := claims["scopes"].([]any)
	if !ok {
		return false
	}
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// Problem 3.6: field report ingestion. Publish-only, no downstream processing
// in the request path; duplicates are deduplicated downstream in
// report-consumer by report_id.
type fieldReportRequest struct {
	ReportID string          `json:"report_id"`
	Payload  json.RawMessage `json:"payload"`
}

func (s *service) fieldReports(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.claims(r)
	if !ok {
		platform.JSON(w, 401, map[string]any{
			"error": map[string]string{"code": "unauthorized", "message": "token tidak valid"},
		})
		return
	}
	if !s.shed(w, r, claims) {
		return
	}
	var body fieldReportRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && r.Body != http.NoBody {
		platform.JSON(w, 400, map[string]any{
			"error": map[string]string{"code": "bad_request", "message": "body tidak valid"},
		})
		return
	}
	reportID := r.Header.Get("Idempotency-Key")
	if reportID == "" {
		reportID = body.ReportID
	}
	if reportID == "" {
		platform.JSON(w, 400, map[string]any{
			"error": map[string]string{"code": "bad_request", "message": "report_id wajib diisi melalui header Idempotency-Key atau body report_id"},
		})
		return
	}
	if len(body.Payload) == 0 {
		body.Payload = []byte(`{}`)
	}
	event, err := json.Marshal(map[string]any{
		"report_id":      reportID,
		"received_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"payload":        json.RawMessage(body.Payload),
		"correlation_id": platform.CorrelationID(r.Context()),
	})
	if err != nil {
		platform.JSON(w, 503, map[string]any{
			"error": map[string]string{"code": "unavailable", "message": "layanan tidak tersedia"},
		})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.producer.Publish(ctx, "field-reports", []byte(reportID), event, nil); err != nil {
		platform.JSON(w, 503, map[string]any{
			"error": map[string]string{"code": "unavailable", "message": "layanan tidak tersedia"},
		})
		return
	}
	platform.JSON(w, 202, map[string]string{"status": "accepted", "report_id": reportID})
}
