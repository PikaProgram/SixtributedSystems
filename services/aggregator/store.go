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

// persist writes a hazard event to the unit owning its (source, ref) key.
// It returns an errStoreWrite for the unprocessed table when no unit is reachable.
func (s *storeClient) persist(ctx context.Context, r *ring, event HazardEvent) error {
	u, ok := r.owner(eventKey(event.Source, event.SourceRefID))
	if !ok {
		return fmt.Errorf("%w: no store units available", errStoreWrite)
	}
	return s.call(ctx, u, http.MethodPost, "/internal/store/events", event, nil)
}

// fanOutEvents calls every reachable unit's /internal/store/events and returns
// the per-unit pages plus the names of unreachable units.
func (s *storeClient) fanOutEvents(
	ctx context.Context,
	r *ring,
	source, since string,
	limit int,
) ([]storeEventsPage, []string) {
	units := r.all()
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
