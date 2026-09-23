package main

import (
	"context"
	"encoding/json"
	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/golang-jwt/jwt/v5"
	"io"
	"net/http"
	"strings"
	"time"
)

type service struct {
	key           []byte
	aggregatorURL string
	slots         chan struct{}
}

func main() {
	logger := platform.Logger("client-api")
	s := &service{
		key:           []byte(platform.Env("JWT_SIGNING_KEY", "development-signing-key-change-me")),
		aggregatorURL: platform.Env("AGGREGATOR_URL", "http://localhost:8090"),
		slots:         make(chan struct{}, platform.EnvInt("CLIENT_MAX_CONCURRENCY", 64)),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/api/hazards", s.hazards)
	_ = http.ListenAndServe(
		":"+platform.Env("CLIENT_API_PORT", "8080"),
		platform.CorrelationMiddleware(logger, s.limit(mux)),
	)
}
func (s *service) limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
			next.ServeHTTP(w, r)
		default:
			platform.JSON(w, 429, map[string]string{"error": "capacity exceeded"})
		}
	})
}
func (s *service) health(w http.ResponseWriter, _ *http.Request) {
	platform.JSON(w, 200, map[string]string{"status": "ok", "service": "client-api"})
}
func (s *service) claims(r *http.Request) (jwt.MapClaims, bool) {
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || parts[0] != "Bearer" {
		return nil, false
	}
	token, err := jwt.Parse(parts[1], func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, jwt.ErrSignatureInvalid
		}
		return s.key, nil
	})
	if err != nil || !token.Valid {
		return nil, false
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	return claims, ok
}
func (s *service) hazards(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.claims(r)
	if !ok {
		platform.JSON(w, 401, map[string]string{"error": "invalid access token"})
		return
	}
	raw := false
	if values, ok := claims["scopes"].([]any); ok {
		for _, value := range values {
			if value == "hazard:raw" {
				raw = true
			}
		}
	}
	if r.URL.Query().Get("raw") == "true" && !raw {
		platform.JSON(w, 403, map[string]string{"error": "raw scope required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.aggregatorURL+"/api/hazards", nil)
	req.Header.Set("X-Correlation-ID", platform.CorrelationID(r.Context()))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		platform.JSON(w, 503, map[string]string{"error": "aggregator unavailable"})
		return
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		platform.JSON(w, 503, map[string]string{"error": "aggregator unavailable"})
		return
	}
	if raw {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}
	var envelope struct {
		Events []map[string]any `json:"events"`
		Count  int              `json:"count"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		platform.JSON(w, 502, map[string]string{"error": "invalid aggregator response"})
		return
	}
	for _, event := range envelope.Events {
		delete(event, "latitude")
		delete(event, "longitude")
		delete(event, "attributes")
		delete(event, "source_ref_id")
	}
	platform.JSON(w, 200, envelope)
}
