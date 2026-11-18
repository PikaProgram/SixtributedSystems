package main

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// unprocessedMessage is one row of the aggregator's DLQ table.
type unprocessedMessage struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	RawPayload string    `json:"-"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	Status     string    `json:"status"` // retrying | parked
}

const maxAttempts = 5

// unprocessedStore is the aggregator-local Postgres backing
// unprocessed_messages (Problem 3).
type unprocessedStore struct {
	pool *pgxpool.Pool
}

func newUnprocessedStore(ctx context.Context, dsn string) (*unprocessedStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	_, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS unprocessed_messages (
        message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        source TEXT NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        raw_payload TEXT NOT NULL,
        reason TEXT NOT NULL,
        retryable BOOLEAN NOT NULL,
        attempts INT NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'retrying'
    )`)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &unprocessedStore{pool: pool}, nil
}

func (u *unprocessedStore) Close() { u.pool.Close() }

// insert records an unparsable/unpersistable message; retryable=false parks it
// immediately, retryable=true starts the exponential retry loop.
func (u *unprocessedStore) insert(
	ctx context.Context, source, rawPayload, reason string, retryable bool,
) error {
	status := "parked"
	if retryable {
		status = "retrying"
	}
	_, err := u.pool.Exec(ctx, `INSERT INTO unprocessed_messages
        (source, received_at, raw_payload, reason, retryable, attempts, status)
        VALUES ($1, now(), $2, $3, $4, 1, $5)`,
		source, rawPayload, reason, retryable, status)
	return err
}

// retryPass bumps attempts on due retrying rows, parking those that exceed
// maxAttempts, and returns the due rows so callers can re-drive them.
func (u *unprocessedStore) dueRetries(ctx context.Context) ([]unprocessedMessage, error) {
	rows, err := u.pool.Query(ctx, `SELECT message_id, source, received_at, raw_payload,
            reason, retryable, attempts, status
        FROM unprocessed_messages
        WHERE status = 'retrying'
          AND now() >= received_at + power(2, attempts) * interval '1 second'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var due []unprocessedMessage
	for rows.Next() {
		var m unprocessedMessage
		if err := rows.Scan(&m.MessageID, &m.Source, &m.ReceivedAt, &m.RawPayload,
			&m.Reason, &m.Retryable, &m.Attempts, &m.Status); err != nil {
			continue
		}
		due = append(due, m)
	}
	return due, rows.Err()
}

// markAttempt increments attempts; parked once attempts exceeds maxAttempts.
func (u *unprocessedStore) markAttempt(ctx context.Context, messageID string, succeeded bool) error {
	if succeeded {
		_, err := u.pool.Exec(ctx,
			`DELETE FROM unprocessed_messages WHERE message_id = $1`, messageID)
		return err
	}
	_, err := u.pool.Exec(ctx, `UPDATE unprocessed_messages
        SET attempts = attempts + 1,
            status = CASE WHEN attempts + 1 > $2 THEN 'parked' ELSE status END
        WHERE message_id = $1`, messageID, maxAttempts)
	return err
}

// list returns parked/retrying rows for the audit endpoint.
func (u *unprocessedStore) list(ctx context.Context, source string, limit int) ([]unprocessedMessage, error) {
	query := `SELECT message_id, source, received_at, raw_payload, reason, retryable,
            attempts, status FROM unprocessed_messages`
	args := []any{}
	if source != "" {
		query += ` WHERE source = $1`
		args = append(args, source)
	}
	query += ` ORDER BY received_at DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)
	rows, err := u.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []unprocessedMessage{}
	for rows.Next() {
		var m unprocessedMessage
		if err := rows.Scan(&m.MessageID, &m.Source, &m.ReceivedAt, &m.RawPayload,
			&m.Reason, &m.Retryable, &m.Attempts, &m.Status); err != nil {
			continue
		}
		items = append(items, m)
	}
	return items, rows.Err()
}

// forReplay selects parked/retrying rows to re-drive, oldest first so
// per-source received_at order (and thus source_ref_id ordering) holds.
// ids filters to explicit messages when non-empty; source filters by feed.
func (u *unprocessedStore) forReplay(ctx context.Context, ids []string, source string) ([]unprocessedMessage, error) {
	query := `SELECT message_id, source, received_at, raw_payload, reason, retryable,
        attempts, status FROM unprocessed_messages`
	args := []any{}
	if len(ids) > 0 {
		query += ` WHERE message_id = ANY($1)`
		args = append(args, ids)
	} else if source != "" {
		query += ` WHERE source = $1`
		args = append(args, source)
	}
	query += ` ORDER BY source, received_at ASC`
	rows, err := u.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []unprocessedMessage{}
	for rows.Next() {
		var m unprocessedMessage
		if err := rows.Scan(&m.MessageID, &m.Source, &m.ReceivedAt, &m.RawPayload,
			&m.Reason, &m.Retryable, &m.Attempts, &m.Status); err != nil {
			continue
		}
		items = append(items, m)
	}
	return items, rows.Err()
}

// statusCounts returns row counts keyed by status for the replay stats endpoint.
func (u *unprocessedStore) statusCounts(ctx context.Context) (map[string]int, error) {
	rows, err := u.pool.Query(ctx,
		`SELECT status, count(*) FROM unprocessed_messages GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			continue
		}
		counts[status] = n
	}
	return counts, rows.Err()
}

// runRetries drives the exponential retry loop for retrying rows: rows whose
// backoff (2^attempts seconds since received_at) elapsed are re-parsed from
// their raw payload, re-persisted, and deleted on success; parked after
// maxAttempts failures.
func (a *aggregator) runRetries(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		due, err := a.unproc.dueRetries(ctx)
		if err != nil {
			a.logger.Error("unprocessed_due", "error", err)
			continue
		}
		for _, m := range due {
			if m.RawPayload == "" {
				// upstream-transient row has no payload to re-drive; count the attempt
				_ = a.unproc.markAttempt(ctx, m.MessageID, false)
				continue
			}
			event, err := parseRawPayload([]byte(m.RawPayload))
			if err != nil {
				_ = a.unproc.markAttempt(ctx, m.MessageID, false)
				continue
			}
			if err := a.stores.persist(ctx, a.ring, *event); err != nil {
				_ = a.unproc.markAttempt(ctx, m.MessageID, false)
				continue
			}
			_ = a.unproc.markAttempt(ctx, m.MessageID, true)
		}
	}
}
