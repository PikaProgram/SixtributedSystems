// Store service: one instance per shard, paired with a co-located Postgres
// whose data lives under DATA_DIR (default /data/pg, hostPath PV mount; the
// volume belongs to the Postgres container, not this process).
// Env: POSTGRES_DSN, ROLE=primary|standby, PEER_DSN (peer's POSTGRES_DSN),
// NODE_NAME (pass via downward API spec.nodeName), UNIT_NAME, PORT.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

type store struct {
	db           *pgxpool.Pool
	logger       *slog.Logger
	nodeName     string
	unitName     string
	promoteAfter time.Duration
	isPrimary    atomic.Bool
}

const eventColumns = `hazard_id,source,source_ref_id,hazard_type,severity,area_name,
    latitude,longitude,occurred_at,ingested_at,attributes`

const schema = `CREATE TABLE IF NOT EXISTS hazard_events (
    hazard_id TEXT PRIMARY KEY, source TEXT NOT NULL, source_ref_id TEXT NOT NULL,
    hazard_type TEXT NOT NULL, severity TEXT NOT NULL, area_name TEXT NOT NULL,
    latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
    occurred_at TIMESTAMPTZ NOT NULL, ingested_at TIMESTAMPTZ NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}', UNIQUE(source, source_ref_id))`

func main() {
	logger := platform.Logger("store")
	shutdownTracer, err := platform.InitTracer("store")
	if err != nil {
		logger.Error("tracer_init", "error", err)
	} else {
		defer shutdownTracer()
	}
	shutdownMetrics, err := platform.InitMetrics("store")
	if err != nil {
		logger.Error("metrics_init", "error", err)
	} else {
		defer shutdownMetrics()
	}
	ctx := context.Background()
	dsn := platform.Env(
		"POSTGRES_DSN",
		"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
	)
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("postgres_pool", "error", err)
		return
	}
	defer db.Close()
	if _, err := db.Exec(ctx, schema); err != nil {
		logger.Error("schema_create", "error", err)
		return
	}
	if _, err := db.Exec(ctx,
		`CREATE INDEX IF NOT EXISTS hazard_events_ingested_idx ON hazard_events(ingested_at, hazard_id)`); err != nil {
		logger.Error("index_create", "error", err)
		return
	}
	hostname, _ := os.Hostname()
	s := &store{
		db:           db,
		logger:       logger,
		nodeName:     platform.Env("NODE_NAME", hostname),
		unitName:     platform.Env("UNIT_NAME", hostname),
		promoteAfter: platform.EnvDurationSeconds("STANDBY_PROMOTE_SECONDS", 10*time.Second),
	}
	role := strings.ToLower(platform.Env("ROLE", "primary"))
	s.isPrimary.Store(role == "primary")
	if role == "standby" {
		if peer := platform.Env("PEER_DSN", ""); peer != "" {
			go s.replicate(ctx, peer)
		} else {
			logger.Error("standby_without_peer", "hint", "set PEER_DSN to the primary's POSTGRES_DSN")
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/store/events", s.putEvent)
	mux.HandleFunc("GET /internal/store/events", s.listEvents)
	mux.HandleFunc("GET /internal/store/checksum", s.checksum)
	mux.HandleFunc("GET /internal/store/location", s.location)
	mux.HandleFunc("GET /internal/store/stats", s.stats)
	mux.HandleFunc("GET /health", s.health)
	port := platform.Env("PORT", "8100")
	logger.Info("starting", "port", port, "role", s.role(), "node", s.nodeName, "unit", s.unitName)
	_ = http.ListenAndServe(":"+port, platform.HTTPMiddleware(logger, mux))
}

func (s *store) role() string {
	if s.isPrimary.Load() {
		return "primary"
	}
	return "standby"
}

func (s *store) fail(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	s.logger.Error("request_failed", "correlation_id", platform.CorrelationID(r.Context()),
		"code", code, "error", message)
	platform.JSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}

func scanEvent(rows pgx.Rows) (HazardEvent, error) {
	var e HazardEvent
	var attrs []byte
	if err := rows.Scan(
		&e.HazardID, &e.Source, &e.SourceRefID, &e.HazardType, &e.Severity,
		&e.AreaName, &e.Latitude, &e.Longitude, &e.OccurredAt, &e.IngestedAt, &attrs,
	); err != nil {
		return e, err
	}
	e.Attributes = map[string]any{}
	_ = json.Unmarshal(attrs, &e.Attributes)
	return e, nil
}

func (s *store) putEvent(w http.ResponseWriter, r *http.Request) {
	if !s.isPrimary.Load() {
		s.fail(w, r, http.StatusServiceUnavailable, "not_primary",
			"standby node does not accept writes; retry against the current primary")
		return
	}
	var e HazardEvent
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		s.fail(w, r, http.StatusBadRequest, "bad_request", "invalid HazardEvent JSON: "+err.Error())
		return
	}
	attrs, _ := json.Marshal(e.Attributes)
	var inserted string
	err := s.db.QueryRow(r.Context(), `INSERT INTO hazard_events(
        hazard_id, source, source_ref_id, hazard_type, severity, area_name,
        latitude, longitude, occurred_at, ingested_at, attributes
    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT(source, source_ref_id) DO NOTHING RETURNING hazard_id`,
		e.HazardID, e.Source, e.SourceRefID, e.HazardType, e.Severity, e.AreaName,
		e.Latitude, e.Longitude, e.OccurredAt, e.IngestedAt, attrs,
	).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		platform.JSON(w, http.StatusOK, map[string]string{"status": "exists"})
		return
	}
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	platform.JSON(w, http.StatusCreated, map[string]string{"status": "created"})
}

func decodeCursor(raw string) (time.Time, string, error) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, "", err
	}
	var parts []string
	if err := json.Unmarshal(decoded, &parts); err != nil || len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("invalid cursor payload")
	}
	at, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", err
	}
	return at, parts[1], nil
}

func encodeCursor(at time.Time, hazardID string) string {
	payload, _ := json.Marshal([]string{at.Format(time.RFC3339Nano), hazardID})
	return base64.StdEncoding.EncodeToString(payload)
}

func (s *store) listEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			s.fail(w, r, http.StatusBadRequest, "bad_request", "limit must be a positive integer")
			return
		}
		limit = min(parsed, 1000)
	}
	where, args := "", []any{limit}
	add := func(condition string, values ...any) {
		if where == "" {
			where = " WHERE "
		} else {
			where += " AND "
		}
		where += condition
		args = append(args, values...)
	}
	if raw := q.Get("since"); raw != "" {
		since, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			s.fail(w, r, http.StatusBadRequest, "bad_request", "since must be ISO8601 (RFC3339)")
			return
		}
		add(fmt.Sprintf("ingested_at >= $%d", len(args)+1), since)
	}
	if raw := q.Get("cursor"); raw != "" {
		at, hazardID, err := decodeCursor(raw)
		if err != nil {
			s.fail(w, r, http.StatusBadRequest, "bad_request", "invalid cursor: "+err.Error())
			return
		}
		add(fmt.Sprintf("(ingested_at, hazard_id) > ($%d, $%d)", len(args)+1, len(args)+2), at, hazardID)
	}
	rows, err := s.db.Query(r.Context(),
		`SELECT `+eventColumns+` FROM hazard_events`+where+
			` ORDER BY ingested_at ASC, hazard_id ASC LIMIT $1`, args...)
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	defer rows.Close()
	items := []HazardEvent{}
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			continue
		}
		items = append(items, e)
	}
	var nextCursor *string
	if len(items) == limit {
		last := items[len(items)-1]
		cursor := encodeCursor(last.IngestedAt, last.HazardID)
		nextCursor = &cursor
	}
	platform.JSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nextCursor})
}

func (s *store) checksum(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	query := `SELECT source_ref_id FROM hazard_events`
	args := []any{}
	if source != "" {
		query += ` WHERE source = $1`
		args = append(args, source)
	}
	rows, err := s.db.Query(r.Context(), query, args...)
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	defer rows.Close()
	refs := []string{}
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err == nil {
			refs = append(refs, ref)
		}
	}
	sort.Strings(refs) // byte order, independent of the database collation
	sum := sha256.Sum256([]byte(strings.Join(refs, "\n")))
	platform.JSON(w, http.StatusOK, map[string]any{
		"source":    source,
		"count":     len(refs),
		"checksum":  hex.EncodeToString(sum[:]),
		"algorithm": "sha256",
		"as_of":     time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *store) location(w http.ResponseWriter, r *http.Request) {
	hazardID := r.URL.Query().Get("hazard_id")
	var exists bool
	if err := s.db.QueryRow(r.Context(),
		`SELECT EXISTS(SELECT 1 FROM hazard_events WHERE hazard_id = $1)`, hazardID,
	).Scan(&exists); err != nil {
		s.fail(w, r, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	if !exists {
		s.fail(w, r, http.StatusNotFound, "not_found", "no event with hazard_id "+hazardID)
		return
	}
	role := "replica"
	if s.isPrimary.Load() {
		role = "primary"
	}
	platform.JSON(w, http.StatusOK, map[string]any{
		"hazard_id": hazardID,
		"locations": []map[string]string{
			{"node": s.nodeName, "unit": s.unitName, "role": role},
		},
	})
}

func (s *store) stats(w http.ResponseWriter, r *http.Request) {
	var count int64
	var oldest, newest *time.Time
	err := s.db.QueryRow(r.Context(),
		`SELECT count(*), min(ingested_at), max(ingested_at) FROM hazard_events`,
	).Scan(&count, &oldest, &newest)
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	platform.JSON(w, http.StatusOK, map[string]any{"count": count, "oldest": oldest, "newest": newest})
}

func (s *store) health(w http.ResponseWriter, _ *http.Request) {
	platform.JSON(w, http.StatusOK, map[string]string{
		"status": "ok", "role": s.role(), "node": s.nodeName, "unit": s.unitName,
	})
}

// replicate pulls new rows from the peer (primary) into the local store; the
// equivalent of streaming replication done at the application level, so the
// service works against any Postgres it does not own the process of.
// shortcut: rows whose ingested_at precedes the watermark are never re-pulled;
// safe because the aggregator stamps ingested_at at ingest time.
func (s *store) replicate(ctx context.Context, peerDSN string) {
	peer, err := pgxpool.New(ctx, peerDSN)
	if err != nil {
		s.logger.Error("peer_pool", "error", err)
		return
	}
	defer peer.Close()
	var watermark time.Time
	var hazardWatermark string
	lastPrimaryOk := time.Now()
	caughtUp := false
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if s.isPrimary.Load() {
			return // promoted; stop syncing to avoid overwriting local writes
		}
		batch := []HazardEvent{}
		rows, err := peer.Query(ctx,
			`SELECT `+eventColumns+` FROM hazard_events
             WHERE (ingested_at, hazard_id) > ($1, $2)
             ORDER BY ingested_at ASC, hazard_id ASC LIMIT 1000`,
			watermark, hazardWatermark)
		if err == nil {
			for rows.Next() {
				e, scanErr := scanEvent(rows)
				if scanErr != nil {
					continue
				}
				batch = append(batch, e)
			}
			rows.Close()
			for _, e := range batch {
				attrs, _ := json.Marshal(e.Attributes)
				if _, err = s.db.Exec(ctx, `INSERT INTO hazard_events(
                    hazard_id, source, source_ref_id, hazard_type, severity, area_name,
                    latitude, longitude, occurred_at, ingested_at, attributes
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                ON CONFLICT(source, source_ref_id) DO UPDATE SET
                    hazard_id=EXCLUDED.hazard_id, hazard_type=EXCLUDED.hazard_type,
                    severity=EXCLUDED.severity, area_name=EXCLUDED.area_name,
                    latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
                    occurred_at=EXCLUDED.occurred_at, ingested_at=EXCLUDED.ingested_at,
                    attributes=EXCLUDED.attributes`,
					e.HazardID, e.Source, e.SourceRefID, e.HazardType, e.Severity, e.AreaName,
					e.Latitude, e.Longitude, e.OccurredAt, e.IngestedAt, attrs); err != nil {
					break
				}
			}
		}
		if err == nil && len(batch) > 0 && batch[len(batch)-1].HazardID != "" {
			last := batch[len(batch)-1]
			watermark, hazardWatermark = last.IngestedAt, last.HazardID
		}
		if err != nil {
			s.logger.Error("replication_pull", "error", err)
		} else {
			lastPrimaryOk = time.Now()
			caughtUp = len(batch) < 1000
			if len(batch) > 0 {
				s.logger.Info("replicated", "count", len(batch), "caught_up", caughtUp)
			}
		}
		if !s.isPrimary.Load() && caughtUp && time.Since(lastPrimaryOk) > s.promoteAfter {
			if s.isPrimary.CompareAndSwap(false, true) {
				s.logger.Info("promoted", "reason", "primary unreachable", "unreachable_for_ms",
					time.Since(lastPrimaryOk).Milliseconds())
				return
			}
		}
	}
}
