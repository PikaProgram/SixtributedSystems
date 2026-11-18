package main

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

// dashboard serves the embedded analytics dashboard page.
func (s *service) dashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(dashboardHTML))
}

// analytics proxies the aggregator's /internal/analytics to the dashboard.
// The audit token is attached server-side so browser clients never see it;
// the response JSON is streamed through unchanged.
func (s *service) analytics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.aggregatorURL+"/internal/analytics", nil)
	req.Header.Set("X-Correlation-ID", platform.CorrelationID(r.Context()))
	req.Header.Set("Authorization", "Bearer "+s.auditToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		lampiranUnavailable(w)
		return
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		lampiranUnavailable(w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(w, res.Body)
}

// lampiranUnavailable returns the Lampiran A error body when the aggregator
// (and therefore the audit analytics) cannot be reached.
func lampiranUnavailable(w http.ResponseWriter) {
	platform.JSON(w, http.StatusServiceUnavailable, map[string]any{
		"error": map[string]string{"code": "store_unavailable", "message": "layanan analitik tidak tersedia"},
	})
}
