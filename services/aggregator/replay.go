package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

// errNoPayload marks upstream-transient rows: they never carried a payload,
// so there is nothing to re-drive.
var errNoPayload = errors.New("no payload to re-drive")

// replayRequest selects parked/retrying rows: explicit message ids, or all
// rows (optionally of one source).
type replayRequest struct {
	IDs    []string `json:"ids"`
	All    bool     `json:"all"`
	Source string   `json:"source"`
}

// replayMessage re-drives one row through the same re-drive path the
// background retry loop uses: the shared parseRawPayload for the stored
// canonical payload, then the idempotent store upsert (unique on
// (source, source_ref_id), so replays cannot duplicate). No row in
// unprocessed has ever been published (publish happens only after a
// successful persist), so nothing is missed by not re-publishing. Per-source
// ordering holds because callers process rows in received_at order and the
// latest upsert per key wins.
func (a *aggregator) replayMessage(ctx context.Context, m unprocessedMessage) error {
	if m.RawPayload == "" {
		return errNoPayload
	}
	event, err := parseRawPayload([]byte(m.RawPayload))
	if err != nil {
		return err
	}
	return a.stores.persist(ctx, a.ring, *event)
}

// adminReplay re-runs failed messages after the system is adapted to a schema
// change (Problem 3.4). Recovered rows are deleted; rows that fail again get
// attempts++ and stay parked/retrying.
func (a *aggregator) adminReplay(w http.ResponseWriter, r *http.Request) {
	var req replayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		lampiranError(w, http.StatusBadRequest, "bad_request", "body harus JSON replay")
		return
	}
	if !req.All && len(req.IDs) == 0 {
		lampiranError(w, http.StatusBadRequest, "bad_request", "butuh \"ids\" atau \"all\": true")
		return
	}
	rows, err := a.unproc.forReplay(r.Context(), req.IDs, req.Source)
	if err != nil {
		lampiranError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	recovered, failed := 0, 0
	for _, m := range rows {
		err := a.replayMessage(r.Context(), m)
		if markErr := a.unproc.markAttempt(r.Context(), m.MessageID, err == nil); markErr != nil {
			a.logger.Error("replay_mark", "message_id", m.MessageID, "error", markErr)
		}
		if err == nil {
			recovered++
		} else {
			failed++
		}
	}
	platform.JSON(w, http.StatusOK, map[string]any{
		"selected": len(rows), "recovered": recovered, "failed": failed,
	})
}

// adminReplayStats reports current DLQ counts by status.
func (a *aggregator) adminReplayStats(w http.ResponseWriter, r *http.Request) {
	counts, err := a.unproc.statusCounts(r.Context())
	if err != nil {
		lampiranError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	platform.JSON(w, http.StatusOK, map[string]any{"by_status": counts})
}
