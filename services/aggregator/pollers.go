package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

// unprocessed reasons (Problem 3): unknown fields are tolerated; missing
// required fields, wrong types, or non-JSON payloads are schema-incompatible
// and parked. Transient upstream failures are retryable.
const (
	reasonNonJSON  = "non_json_payload: body is not valid JSON"
	reasonSchema   = "schema_incompatible"
	reasonUpstream = "transient_upstream_error"
	reasonStore    = "store_unavailable"
)

// parseBMKGEvents is the testable parse step for the BMKG seismic feed:
// a bare JSON array of seismic events. Unknown fields tolerated; wrong types
// or missing required fields are schema-incompatible (parked).
func parseBMKGEvents(raw []byte) ([]SeismicEvent, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%s", reasonNonJSON)
	}
	var events []SeismicEvent
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&events); err != nil {
		return nil, fmt.Errorf("%s: %v", reasonSchema, err)
	}
	for _, e := range events {
		if e.EventID == "" || e.OccurredAt.IsZero() {
			return nil, fmt.Errorf(
				"%s: missing required field in seismic event %q", reasonSchema, e.EventID)
		}
	}
	return events, nil
}

// parsePVMBGReports is the testable parse step for the PVMBG volcanic feed.
func parsePVMBGReports(raw []byte) ([]VolcanicReport, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%s", reasonNonJSON)
	}
	var reports []VolcanicReport
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&reports); err != nil {
		return nil, fmt.Errorf("%s: %v", reasonSchema, err)
	}
	for _, r := range reports {
		if r.ReportID == "" || r.ReportedAt.IsZero() {
			return nil, fmt.Errorf(
				"%s: missing required field in volcanic report %q", reasonSchema, r.ReportID)
		}
	}
	return reports, nil
}

// parseRawPayload re-drives a stored raw payload during unprocessed retries.
func parseRawPayload(raw []byte) (*HazardEvent, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%s", reasonNonJSON)
	}
	var event HazardEvent
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&event); err != nil {
		return nil, fmt.Errorf("%s: %v", reasonSchema, err)
	}
	if event.HazardID == "" || event.Source == "" || event.SourceRefID == "" ||
		event.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%s: missing required hazard field", reasonSchema)
	}
	return &event, nil
}

// fetchJSON performs an authenticated GET of a JSON body.
func (a *aggregator) fetchJSON(
	ctx context.Context,
	url string,
	headers map[string]string,
	out any,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return platform.Outbound(a.logger, ctx, "upstream "+url, func() error {
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode >= 300 {
			return fmt.Errorf("upstream %s", res.Status)
		}
		return json.NewDecoder(res.Body).Decode(out)
	})
}

// recordUpstreamFailure stores a transient upstream error as a retryable
// unprocessed row (no payload to re-drive; rows park after maxAttempts).
func (a *aggregator) recordUpstreamFailure(ctx context.Context, source string, err error) {
	if insertErr := a.unproc.insert(
		ctx, source, "", reasonUpstream+": "+err.Error(), true,
	); insertErr != nil {
		a.logger.Error("unprocessed_insert", "source", source, "error", insertErr)
	}
}

// recordParseFailure stores an unparsable payload as a parked row.
func (a *aggregator) recordParseFailure(ctx context.Context, source string, raw []byte, err error) {
	if insertErr := a.unproc.insert(ctx, source, string(raw), err.Error(), false); insertErr != nil {
		a.logger.Error("unprocessed_insert", "source", source, "error", insertErr)
	}
}

// processEvent persists to the owning store, then publishes to Kafka. Store
// failures land in the unprocessed table (Problem 3) and do not publish.
func (a *aggregator) processEvent(ctx context.Context, event HazardEvent) error {
	payload, _ := json.Marshal(event)
	err := a.stores.persist(ctx, a.ring, event)
	if err != nil {
		a.logger.Error("store_persist", "hazard_id", event.HazardID, "error", err)
		if insertErr := a.unproc.insert(
			ctx, event.Source, string(payload), reasonStore+": "+err.Error(), true,
		); insertErr != nil {
			a.logger.Error("unprocessed_insert", "source", event.Source, "error", insertErr)
		}
		return err
	}
	envelope, _ := json.Marshal(map[string]any{
		"event_type":     "hazard.created",
		"correlation_id": platform.CorrelationID(ctx),
		"event":          event,
	})
	return platform.Outbound(a.logger, ctx, "kafka_publish", func() error {
		return a.kafka.Publish(
			ctx, kafkaTopic, []byte(event.HazardID), envelope,
			map[string]string{"correlation_id": platform.CorrelationID(ctx)},
		)
	})
}

// pollerFenced: non-leaders skip poll cycles; the flag is checked every
// iteration so a lost lease stops writes immediately.
func (a *aggregator) pollerFenced() bool { return !a.leader.IsLeader() }

func (a *aggregator) pollBMKG(ctx context.Context) {
	cursor := time.Unix(0, 0)
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		if !a.pollerFenced() {
			cursor = a.fetchBMKG(ctx, cursor)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *aggregator) fetchBMKG(ctx context.Context, cursor time.Time) time.Time {
	requestCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	var events []SeismicEvent
	var warnings []TsunamiWarning
	since := cursor.Format(time.RFC3339)
	err := a.fetchJSON(
		requestCtx,
		a.bmkgURL+"/seismic-events?since="+since,
		map[string]string{"X-BMKG-Key": a.bmkgKey},
		&events,
	)
	if err != nil {
		a.recordUpstreamFailure(requestCtx, "BMKG", err)
		a.recordError("bmkg", err)
		return cursor
	}
	if err == nil {
		err = a.fetchJSON(
			requestCtx,
			a.bmkgURL+"/tsunami-warnings?since="+since,
			map[string]string{"X-BMKG-Key": a.bmkgKey},
			&warnings,
		)
		if err != nil {
			a.recordUpstreamFailure(requestCtx, "BMKG", err)
			a.recordError("bmkg", err)
			return cursor
		}
	}
	warningMap := map[string]TsunamiWarning{}
	for _, w := range warnings {
		warningMap[w.RelatedEventID] = w
	}
	latest := cursor
	for _, event := range events {
		if event.OccurredAt.After(latest) {
			latest = event.OccurredAt
		}
		attrs := map[string]any{
			"magnitude":         event.Magnitude,
			"depth_km":          event.DepthKM,
			"potential_tsunami": event.PotentialTsunami,
		}
		severity := severityFor(event, warningMap[event.EventID])
		if warning, ok := warningMap[event.EventID]; ok {
			attrs["tsunami_warning"] = warning
		}
		hazard := HazardEvent{
			HazardID: stableID("BMKG", event.EventID), Source: "BMKG",
			SourceRefID: event.EventID, HazardType: "SEISMIC", Severity: severity,
			AreaName: event.RegionName, Latitude: event.EpicenterLat,
			Longitude: event.EpicenterLon, OccurredAt: event.OccurredAt,
			IngestedAt: time.Now().UTC(), Attributes: attrs,
		}
		// continue on store errors: the event is parked for retry
		_ = a.processEvent(ctx, hazard)
	}
	a.recordSuccess("bmkg")
	return latest
}

func severityFor(event SeismicEvent, warning TsunamiWarning) string {
	switch warning.ThreatLevel {
	case "Awas":
		return "AWAS"
	case "Siaga":
		return "SIAGA"
	case "Waspada":
		return "WASPADA"
	}
	if event.Magnitude < 5 {
		return "NORMAL"
	}
	if event.Magnitude < 6.5 {
		return "WASPADA"
	}
	return "SIAGA"
}

func (a *aggregator) pollPVMBG(ctx context.Context) {
	cursor := time.Unix(0, 0)
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		if !a.pollerFenced() {
			cursor = a.fetchPVMBG(ctx, cursor)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *aggregator) fetchPVMBG(ctx context.Context, cursor time.Time) time.Time {
	requestCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	var reports []VolcanicReport
	err := a.fetchJSON(
		requestCtx,
		a.pvmbgURL+"/volcanic-reports?since="+cursor.Format(time.RFC3339),
		map[string]string{"Authorization": "Bearer " + a.pvmbgToken},
		&reports,
	)
	if err != nil {
		a.recordUpstreamFailure(requestCtx, "PVMBG", err)
		a.recordError("pvmbg", err)
		return cursor
	}
	latest := cursor
	for _, report := range reports {
		if report.ReportedAt.After(latest) {
			latest = report.ReportedAt
		}
		attrs := map[string]any{
			"eruption_count_24h":  report.EruptionCount24h,
			"ash_column_height_m": report.AshColumnHeightM,
		}
		if report.ConfidenceLevel != nil {
			attrs["confidence_level"] = *report.ConfidenceLevel
		}
		name, lat, lon := a.volcano(report.VolcanoID)
		hazard := HazardEvent{
			HazardID: stableID("PVMBG", report.ReportID), Source: "PVMBG",
			SourceRefID: report.ReportID, HazardType: "VOLCANIC",
			Severity: strings.ToUpper(report.AlertLevel), AreaName: name,
			Latitude: lat, Longitude: lon, OccurredAt: report.ReportedAt,
			IngestedAt: time.Now().UTC(), Attributes: attrs,
		}
		_ = a.processEvent(ctx, hazard)
	}
	a.recordSuccess("pvmbg")
	return latest
}
