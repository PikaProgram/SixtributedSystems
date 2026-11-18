package main

import (
	"net/http"
	"sort"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

// analytics buckets hazard events by type, severity, area, and time.
func (a *aggregator) analytics(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	from := query.Get("from")
	to := query.Get("to")
	if from == "" || to == "" {
		lampiranError(w, http.StatusBadRequest, "missing_range", "from dan to wajib diisi")
		return
	}
	fromTime, err := time.Parse(time.RFC3339, from)
	if err != nil {
		lampiranError(w, http.StatusBadRequest, "invalid_from", "from tidak valid")
		return
	}
	toTime, err := time.Parse(time.RFC3339, to)
	if err != nil {
		lampiranError(w, http.StatusBadRequest, "invalid_to", "to tidak valid")
		return
	}
	pages, unavailable := a.stores.fanOutEvents(r.Context(), a.ring, "", from, auditMaxLimit)
	// analytics reads every event in range, not one audit page: re-fetch per
	// unit following that unit's own cursor pagination would need since+to
	// support in the store; one page per unit is the M2 contract (default page
	// limit 1000).
	items := mergePages(pages)
	bucket := a.bucket
	if bucket <= 0 {
		bucket = time.Hour
	}
	byType := map[string]int{}
	bySeverity := map[string]int{}
	byArea := map[string]int{}
	timeseries := map[string]map[string]int{}
	for _, event := range items {
		if event.OccurredAt.Before(fromTime) || event.OccurredAt.After(toTime) {
			continue
		}
		byType[event.HazardType]++
		bySeverity[event.Severity]++
		byArea[event.AreaName]++
		bucketKey := event.OccurredAt.Truncate(bucket).UTC().Format(time.RFC3339)
		if timeseries[bucketKey] == nil {
			timeseries[bucketKey] = map[string]int{}
		}
		timeseries[bucketKey][event.HazardType]++
	}
	buckets := make([]string, 0, len(timeseries))
	for key := range timeseries {
		buckets = append(buckets, key)
	}
	sort.Strings(buckets)
	series := make([]map[string]any, 0, len(buckets))
	for _, key := range buckets {
		entry := map[string]any{"bucket": key}
		for hazardType, count := range timeseries[key] {
			entry[hazardType] = count
		}
		series = append(series, entry)
	}
	platform.JSON(w, http.StatusOK, map[string]any{
		"by_type":           byType,
		"by_severity":       bySeverity,
		"by_area":           byArea,
		"timeseries":        series,
		"partial":           len(unavailable) > 0,
		"unavailable_units": unavailable,
	})
}
