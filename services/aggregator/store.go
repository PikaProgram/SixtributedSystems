package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

const storeTimeout = 5 * time.Second

var errNotFound = errors.New("not found")

// storeClient calls the sharded store HTTP API. It is stateless; the ring
// supplies the units.
type storeClient struct {
	logger *slog.Logger
	client *http.Client
}

func newStoreClient(logger *slog.Logger) *storeClient {
	return &storeClient{
		logger: logger,
		client: &http.Client{Timeout: storeTimeout},
	}
}

// call performs one request against a unit; non-2xx and transport failures map
// to retryable store-unavailable errors.
func (s *storeClient) call(
	ctx context.Context,
	u unit,
	method, path string,
	body any,
	out any,
) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	if u.IP == "" {
		// Expected-but-lost unit: no pod, no IP. Fail fast with a clear
		// error instead of dialing an empty host.
		return fmt.Errorf("%w: unit %s not reachable (no IP)", errStoreWrite, u.Name)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.base()+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return platform.Outbound(s.logger, ctx, "store_"+method+"_"+path, func() error {
		res, callErr := s.client.Do(req)
		if callErr != nil {
			return fmt.Errorf("%w: %v", errStoreWrite, callErr)
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		if res.StatusCode >= 500 || res.StatusCode == http.StatusServiceUnavailable {
			return fmt.Errorf("%w: unit %s: %s", errStoreWrite, u.Name, res.Status)
		}
		if res.StatusCode >= 300 {
			return fmt.Errorf("store %s: %s", res.Status, string(raw))
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(raw, out)
	})
}

// persist writes a hazard event to the shard owning its key, trying the
// shard's units in candidate order (primary first). A 503 not_primary or a
// down primary simply advances to the next candidate (usually the standby,
// after the shard controller has promoted it).
func (s *storeClient) persist(ctx context.Context, r *ring, event HazardEvent) error {
	shard, ok := r.ownerShard(eventKey(event.Source, event.SourceRefID))
	if !ok {
		// No shard ring (dev): fall back to any live unit.
		units := r.all()
		if len(units) == 0 {
			return fmt.Errorf("%w: no store units available", errStoreWrite)
		}
		return s.call(ctx, units[0], http.MethodPost, "/internal/store/events", event, nil)
	}
	candidates := r.candidates(shard)
	if len(candidates) == 0 {
		return fmt.Errorf("%w: no live units for shard %s", errStoreWrite, shard)
	}
	var lastErr error
	for _, u := range candidates {
		if err := s.call(ctx, u, http.MethodPost, "/internal/store/events", event, nil); err != nil {
			lastErr = err
			continue // try the next candidate (primary down / not_primary)
		}
		return nil
	}
	return lastErr
}

// unitHealth returns one unit's /health: role plus identity.
func (s *storeClient) unitHealth(ctx context.Context, u unit) (map[string]string, error) {
	var out map[string]string
	err := s.call(ctx, u, http.MethodGet, "/health", nil, &out)
	return out, err
}

// unitStats returns one unit's /internal/store/stats (row count + newest row).
func (s *storeClient) unitStats(ctx context.Context, u unit) (map[string]any, error) {
	var out map[string]any
	err := s.call(ctx, u, http.MethodGet, "/internal/store/stats", nil, &out)
	return out, err
}

// setUnitRole asks one unit to flip its role (shard-controller failover).
func (s *storeClient) setUnitRole(ctx context.Context, u unit, role string) error {
	body := map[string]string{"role": role}
	return s.call(ctx, u, http.MethodPost, "/internal/store/role", body, nil)
}

// fanOutEvents probes every expected unit's /internal/store/events (live
// units answer; lost units fail fast and are reported unavailable) and
// returns the per-unit pages plus the names of unreachable units.
func (s *storeClient) fanOutEvents(
	ctx context.Context,
	r *ring,
	source, since string,
	limit int,
) ([]storeEventsPage, []string) {
	units := r.expected()
	pages := make([]storeEventsPage, len(units))
	var mu sync.Mutex
	unavailable := []string{}
	var wg sync.WaitGroup
	for i := range units {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			page, err := s.unitEvents(ctx, units[i], source, since, limit)
			if err != nil {
				s.logger.Error("store_fanout", "unit", units[i].Name, "error", err)
				mu.Lock()
				unavailable = append(unavailable, units[i].Name)
				mu.Unlock()
				return
			}
			pages[i] = page
		}(i)
	}
	wg.Wait()
	return pages, unavailable
}

// unitEvents fetches one page of events from a single unit.
func (s *storeClient) unitEvents(
	ctx context.Context,
	u unit,
	source, since string,
	limit int,
) (storeEventsPage, error) {
	query := fmt.Sprintf("/internal/store/events?limit=%d", limit)
	if source != "" {
		query += "&source=" + source
	}
	if since != "" {
		query += "&since=" + since
	}
	var page storeEventsPage
	err := s.call(ctx, u, http.MethodGet, query, nil, &page)
	return page, err
}

// location asks one unit where a hazard_id lives; present=false on the
// unit's 404, err on transport or other failures.
func (s *storeClient) location(ctx context.Context, u unit, hazardID string) (storeLocation, bool, error) {
	var loc storeLocation
	path := "/internal/store/location?hazard_id=" + url.QueryEscape(hazardID)
	err := s.rawCall(ctx, u, http.MethodGet, path, nil, &loc)
	if errors.Is(err, errNotFound) {
		return loc, false, nil
	}
	return loc, err == nil, err
}

// rawCall is call() but maps the unit's 404 error body to errNotFound.
func (s *storeClient) rawCall(
	ctx context.Context, u unit, method, path string, body, out any,
) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.base()+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return platform.Outbound(s.logger, ctx, "store_"+method+"_"+path, func() error {
		res, callErr := s.client.Do(req)
		if callErr != nil {
			return fmt.Errorf("%w: %v", errStoreWrite, callErr)
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		if res.StatusCode == http.StatusNotFound {
			return errNotFound
		}
		if res.StatusCode >= 300 {
			return fmt.Errorf("store %s: %s", res.Status, string(raw))
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(raw, out)
	})
}

type storeEventsPage struct {
	Items      []HazardEvent `json:"items"`
	NextCursor *string       `json:"next_cursor"`
}

type locationEntry struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

type storeLocation struct {
	HazardID  string          `json:"hazard_id"`
	Locations []locationEntry `json:"locations"`
}
