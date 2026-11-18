package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

const (
	auditDefaultLimit = 100
	auditMaxLimit     = 1000
	// hard cap on store pages followed per unit; guards against a store that
	// keeps returning a non-null cursor
	maxUnitPages = 10000
)

// auditCursor is the opaque cursor: base64 of the sort key of the last item.
type auditCursor struct {
	IngestedAt time.Time `json:"i"`
	HazardID   string    `json:"h"`
}

func encodeCursor(c auditCursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(raw string) (auditCursor, error) {
	var c auditCursor
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}

// less orders hazard events by (ingested_at, hazard_id) ascending.
func less(a, b HazardEvent) bool {
	if !a.IngestedAt.Equal(b.IngestedAt) {
		return a.IngestedAt.Before(b.IngestedAt)
	}
	return a.HazardID < b.HazardID
}

// mergePages k-way merges the per-unit pages (each already sorted ascending).
func mergePages(pages []storeEventsPage) []HazardEvent {
	var merged []HazardEvent
	for _, page := range pages {
		merged = append(merged, page.Items...)
	}
	sort.Slice(merged, func(i, j int) bool { return less(merged[i], merged[j]) })
	return merged
}

func (a *aggregator) auditEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := auditDefaultLimit
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			lampiranError(w, http.StatusBadRequest, "invalid_limit", "limit tidak valid")
			return
		}
		limit = min(parsed, auditMaxLimit)
	}
	source := strings.ToUpper(query.Get("source"))
	since := query.Get("since")
	if since != "" {
		if _, err := time.Parse(time.RFC3339, since); err != nil {
			lampiranError(w, http.StatusBadRequest, "invalid_since", "since tidak valid")
			return
		}
	}
	after := auditCursor{}
	if raw := query.Get("cursor"); raw != "" {
		cursor, err := decodeCursor(raw)
		if err != nil {
			lampiranError(w, http.StatusBadRequest, "invalid_cursor", "cursor tidak valid")
			return
		}
		after = cursor
	}
	// fetch one extra item per unit so hasMore is decidable after the merge
	pages, unavailable := a.stores.fanOutEvents(r.Context(), a.ring, source, since, limit+1)
	items := mergePages(pages)
	filtered := make([]HazardEvent, 0, len(items))
	afterKey := HazardEvent{IngestedAt: after.IngestedAt, HazardID: after.HazardID}
	for _, event := range items {
		if after.HazardID != "" && !less(afterKey, event) {
			continue
		}
		filtered = append(filtered, event)
	}
	hasMore := len(filtered) > limit
	if hasMore {
		filtered = filtered[:limit]
	}
	var nextCursor any
	if hasMore {
		last := filtered[len(filtered)-1]
		nextCursor = encodeCursor(auditCursor{IngestedAt: last.IngestedAt, HazardID: last.HazardID})
	}
	platform.JSON(w, http.StatusOK, map[string]any{
		"items":             filtered,
		"next_cursor":       nextCursor,
		"partial":           len(unavailable) > 0,
		"unavailable_units": unavailable,
	})
}

// unitRefs collects all source_ref_ids for a source from one unit, following
// the unit's own cursor pagination.
func (a *aggregator) unitRefs(
	ctx context.Context,
	u unit,
	source string,
	refs *map[string]struct{},
	mu *sync.Mutex,
) error {
	cursor := ""
	for range maxUnitPages {
		query := fmt.Sprintf(
			"/internal/store/events?limit=%d&source=%s",
			auditMaxLimit, url.QueryEscape(source),
		)
		if cursor != "" {
			query += "&cursor=" + url.QueryEscape(cursor)
		}
		var page storeEventsPage
		if err := a.stores.call(ctx, u, http.MethodGet, query, nil, &page); err != nil {
			return err
		}
		mu.Lock()
		for _, event := range page.Items {
			(*refs)[event.SourceRefID] = struct{}{}
		}
		mu.Unlock()
		if page.NextCursor == nil || *page.NextCursor == "" {
			return nil
		}
		cursor = *page.NextCursor
	}
	return nil
}

func (a *aggregator) auditChecksum(w http.ResponseWriter, r *http.Request) {
	source := strings.ToUpper(r.URL.Query().Get("source"))
	if source == "" {
		lampiranError(w, http.StatusBadRequest, "missing_source", "source wajib diisi")
		return
	}
	refs := map[string]struct{}{}
	var mu sync.Mutex
	unavailable := []string{}
	units := a.ring.all()
	var wg sync.WaitGroup
	for _, u := range units {
		wg.Add(1)
		go func(u unit) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), storeTimeout)
			defer cancel()
			if err := a.unitRefs(ctx, u, source, &refs, &mu); err != nil {
				a.logger.Error("store_checksum", "unit", u.Name, "error", err)
				mu.Lock()
				unavailable = append(unavailable, u.Name)
				mu.Unlock()
			}
		}(u)
	}
	wg.Wait()
	sorted := make([]string, 0, len(refs))
	for ref := range refs {
		sorted = append(sorted, ref)
	}
	sort.Strings(sorted) // byte-ascending for ASCII ref ids
	digest := sha256.New()
	digest.Write([]byte(strings.Join(sorted, "\n")))
	platform.JSON(w, http.StatusOK, map[string]any{
		"source":            source,
		"count":             len(sorted),
		"checksum":          hex.EncodeToString(digest.Sum(nil)),
		"algorithm":         "sha256",
		"as_of":             time.Now().UTC().Format(time.RFC3339),
		"partial":           len(unavailable) > 0,
		"unavailable_units": unavailable,
	})
}

func (a *aggregator) auditLocation(w http.ResponseWriter, r *http.Request) {
	hazardID := r.URL.Query().Get("hazard_id")
	if hazardID == "" {
		lampiranError(w, http.StatusBadRequest, "missing_hazard_id", "hazard_id wajib diisi")
		return
	}
	var found *storeLocation
	partial := false
	unavailable := []string{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, u := range a.ring.all() {
		wg.Add(1)
		go func(u unit) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), storeTimeout)
			defer cancel()
			loc, present, err := a.stores.location(ctx, u, hazardID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				a.logger.Error("store_location", "unit", u.Name, "error", err)
				unavailable = append(unavailable, u.Name)
				partial = true
				return
			}
			if present && len(loc.Locations) > 0 && found == nil {
				found = &loc
			}
		}(u)
	}
	wg.Wait()
	if found == nil {
		lampiranError(w, http.StatusNotFound, "not_found", "hazard_id tidak ditemukan")
		return
	}
	platform.JSON(w, http.StatusOK, map[string]any{
		"hazard_id":         hazardID,
		"locations":         found.Locations,
		"partial":           partial,
		"unavailable_units": unavailable,
	})
}

func (a *aggregator) auditUnprocessed(w http.ResponseWriter, r *http.Request) {
	limit := auditDefaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			lampiranError(w, http.StatusBadRequest, "invalid_limit", "limit tidak valid")
			return
		}
		limit = min(parsed, auditMaxLimit)
	}
	source := strings.ToUpper(r.URL.Query().Get("source"))
	items, err := a.unproc.list(r.Context(), source, limit)
	if err != nil {
		a.logger.Error("unprocessed_list", "error", err)
		lampiranError(w, http.StatusServiceUnavailable, "store_unavailable", "database tidak tersedia")
		return
	}
	platform.JSON(w, http.StatusOK, map[string]any{
		"items":       items,
		"next_cursor": nil,
	})
}
