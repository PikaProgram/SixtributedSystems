package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/golang-jwt/jwt/v5"
)

type client struct {
	ID, Secret string
	Scopes     []string
	Refresh    bool
}
type service struct {
	mu         sync.Mutex
	clients    map[string]client
	refresh    map[string]time.Time
	signingKey []byte
	accessTTL  time.Duration
}
type tokenRequest struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}
type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func main() {
	logger := platform.Logger("auth-service")
	s := &service{
		clients: map[string]client{
			"media": {
				ID: platform.Env(
					"MEDIA_CLIENT_ID",
					"media",
				),
				Secret: platform.Env("MEDIA_CLIENT_SECRET", "media-secret"),
				Scopes: []string{"hazard:summary"},
			},
			"field-team": {
				ID: platform.Env(
					"FIELD_CLIENT_ID",
					"field-team",
				),
				Secret:  platform.Env("FIELD_CLIENT_SECRET", "field-secret"),
				Scopes:  []string{"hazard:raw"},
				Refresh: true,
			},
			"internal-ops": {
				ID:     platform.Env("INTERNAL_CLIENT_ID", "internal-ops"),
				Secret: platform.Env("INTERNAL_CLIENT_SECRET", "internal-secret"),
				Scopes: []string{"hazard:raw"},
			},
		},
		refresh:    map[string]time.Time{},
		signingKey: []byte(platform.Env("JWT_SIGNING_KEY", "development-signing-key-change-me")),
		accessTTL:  time.Duration(platform.EnvInt("ACCESS_TOKEN_TTL_SECONDS", 60)) * time.Second,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/token", s.token)
	mux.HandleFunc("/refresh", s.refreshToken)
	logger.Info("starting", "port", platform.Env("AUTH_PORT", "8091"))
	_ = http.ListenAndServe(
		":"+platform.Env("AUTH_PORT", "8091"),
		platform.CorrelationMiddleware(logger, mux),
	)
}

func (s *service) health(w http.ResponseWriter, _ *http.Request) {
	platform.JSON(w, 200, map[string]string{"status": "ok", "service": "auth-service"})
}

func (s *service) token(w http.ResponseWriter, r *http.Request) {
	var req tokenRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		platform.JSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	c, ok := s.clients[req.ClientID]
	if !ok || c.Secret != req.ClientSecret {
		platform.JSON(w, 401, map[string]string{"error": "invalid client credentials"})
		return
	}
	result := map[string]any{
		"access_token": s.issue(c),
		"token_type":   "Bearer",
		"expires_in":   int64(s.accessTTL / time.Second),
	}
	if c.Refresh {
		result["refresh_token"] = s.newRefresh()
	}
	platform.JSON(w, 200, result)
}
func (s *service) issue(c client) string {
	now := time.Now()
	claims := jwt.MapClaims{"sub": c.ID, "client_id": c.ID, "scopes": c.Scopes,
		"iat": now.Unix(), "exp": now.Add(s.accessTTL).Unix(), "jti": randomString()}
	token, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.signingKey)
	return token
}
func (s *service) newRefresh() string {
	buf := make([]byte, 32)
	_, _ = rand.Read(buf)
	raw := base64.RawURLEncoding.EncodeToString(buf)
	sum := sha256.Sum256([]byte(raw))
	s.mu.Lock()
	s.refresh[hex.EncodeToString(sum[:])] = time.Now().Add(24 * time.Hour)
	s.mu.Unlock()
	return raw
}
func (s *service) refreshToken(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		platform.JSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	sum := sha256.Sum256([]byte(req.RefreshToken))
	key := hex.EncodeToString(sum[:])
	s.mu.Lock()
	expires, ok := s.refresh[key]
	if ok {
		delete(s.refresh, key)
	}
	s.mu.Unlock()
	if !ok || time.Now().After(expires) {
		platform.JSON(w, 401, map[string]string{"error": "invalid or expired refresh token"})
		return
	}
	c := s.clients[platform.Env("FIELD_CLIENT_ID", "field-team")]
	platform.JSON(w, 200, map[string]any{"access_token": s.issue(c), "token_type": "Bearer",
		"expires_in": int64(s.accessTTL / time.Second), "refresh_token": s.newRefresh()})
}
func randomString() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return base64.RawURLEncoding.EncodeToString(buf)
}
