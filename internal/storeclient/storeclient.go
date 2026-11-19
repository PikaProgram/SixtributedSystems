// Package storeclient is the sharded-store write path shared by the two
// writers that do not run the aggregator's ring (fire-builder,
// coastal-gateway). The chart fronts all shard primaries with one
// store-writers Service, so the first attempt may land on the wrong shard
// (422 wrong_shard) or on a surviving standby (503 not_primary); the store's
// error body names the shard, and the writer retries the owning shard via
// its per-pod headless DNS (store-primary-N-0.store-pg-N /
// store-standby-N-0.store-pg-N, see the sixtributed chart).
package storeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
)

// Writer routes HazardEvent writes to the owning shard.
type Writer struct {
	baseURL string
	logger  *slog.Logger
	client  *http.Client
}

// NewWriter creates a writer for baseURL (usually the store-writers
// Service URL).
func NewWriter(logger *slog.Logger, baseURL string) *Writer {
	return &Writer{
		baseURL: baseURL,
		logger:  logger,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

type routeError struct {
	msg         string
	owningShard string // wrong_shard (422)
	shard       string // not_primary (503)
}

func (e *routeError) Error() string { return e.msg }

// Put persists one HazardEvent, idempotently (200 exists / 201 created both
// mean success). Routing errors are retried against the named shard;
// non-routing errors are returned to the caller, which owns its own retry
// loop (Kafka redelivery / poll cycle). Bounded: each pod URL is tried at
// most twice (a not_primary answer can flip to primary within the shard
// controller's interval) after the initial service-URL attempt.
func (w *Writer) Put(ctx context.Context, event any) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	counts := map[string]int{}
	url := w.baseURL
	var lastErr error
	for {
		counts[url]++
		lastErr = w.putOnce(ctx, url, body)
		if lastErr == nil {
			return nil
		}
		next := w.nextURL(lastErr, url)
		if next == "" || counts[next] >= 2 {
			return lastErr
		}
		url = next
	}
}

// nextURL picks the next URL to try after an error from origin:
//   - wrong_shard (422): the owning shard's primary pod
//   - not_primary (503): the same shard's other replica (the answering pod
//     is a standby, so a live primary exists on the other pod)
//   - transport failure on a pod URL: the same shard's other replica
//   - anything else: no retry ("" )
//
// The Put loop enforces a per-URL attempt cap, so a stale answer (e.g. a
// promoted pod that has not flipped /health yet) cannot loop forever.
func (w *Writer) nextURL(err error, origin string) string {
	if r, ok := err.(*routeError); ok {
		if r.owningShard != "" {
			return podURL(r.owningShard, "primary")
		}
		if r.shard != "" {
			return podURL(r.shard, otherReplica(replicaOf(origin)))
		}
		return ""
	}
	if shard, ok := podShardName(origin); ok {
		// transport failure on a pod URL: try the other replica of the same
		// shard.
		return podURL(shard, otherReplica(replicaOf(origin)))
	}
	return ""
}

// podURL returns the headless per-pod URL of one shard replica; the chart
// gives each shard's pods stable DNS names (store-primary-N-0.store-pg-N /
// store-standby-N-0.store-pg-N, N is the shard's numeric index).
func podURL(shard, replica string) string {
	return "http://store-" + replica + "-" + shard + ".store-pg-" + shard + ":8100"
}

// podShardName extracts the shard name (shard-N) from a per-pod URL;
// ok=false for the load-balanced service URL.
func podShardName(origin string) (string, bool) {
	idx := strings.Index(origin, "shard-")
	if idx < 0 {
		return "", false
	}
	end := idx + len("shard-")
	for end < len(origin) && origin[end] >= '0' && origin[end] <= '9' {
		end++
	}
	if end == idx+len("shard-") {
		return "", false
	}
	return origin[idx:end], true
}

func replicaOf(origin string) string {
	s := origin[len("http://store-"):]
	if len(s) > len("primary-") && s[:len("primary-")] == "primary-" {
		return "primary"
	}
	return "standby"
}

func otherReplica(r string) string {
	if r == "primary" {
		return "standby"
	}
	return "primary"
}

func (w *Writer) putOnce(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/internal/store/events", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return platform.Outbound(w.logger, ctx, "store_put_event", func() error {
		res, err := w.client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusCreated {
			return nil
		}
		return parseError(res.StatusCode, raw)
	})
}

func parseError(status int, raw []byte) error {
	var envelope struct {
		Error struct {
			Code        string `json:"code"`
			OwningShard string `json:"owning_shard"`
			Shard       string `json:"shard"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err == nil && envelope.Error.Code != "" {
		return &routeError{
			msg:         fmt.Sprintf("store %d %s", status, envelope.Error.Code),
			owningShard: envelope.Error.OwningShard,
			shard:       envelope.Error.Shard,
		}
	}
	return fmt.Errorf("store %d: %s", status, string(raw))
}

// retryCandidates returns the URL to try next after an error: the shard
// named by the store (both replicas, primary first), or — when the error
// carries no shard (transport failure) and the shard count is known — the
// first shard's pods. A nil result means "do not retry".
func retryCandidates(err error, origin string, shardCount int) []string {
	r, ok := err.(*routeError)
	if !ok {
		if shardCount <= 0 {
			return nil
		}
		return podURLs(0)
	}
	shard := r.owningShard
	if shard == "" {
		shard = r.shard
	}
	if shard == "" {
		return nil
	}
	n, _ := parseShardIndex(shard)
	cands := podURLs(n)
	if r.owningShard == "" {
		// not_primary: the answering pod is the shard's own pod; it may be
		// the promoted survivor.
		cands = append([]string{origin}, cands...)
	}
	return cands
}

// podURLs returns the per-pod URLs of one shard (shard-<n>'s primary and
// standby headless DNS names).
func podURLs(n int) []string {
	shard := fmt.Sprintf("shard-%d", n)
	return []string{
		"http://store-primary-" + shard + ".store-pg-" + shard + ":8100",
		"http://store-standby-" + shard + ".store-pg-" + shard + ":8100",
	}
}

func parseShardIndex(shard string) (int, bool) {
	const prefix = "shard-"
	if len(shard) <= len(prefix) || shard[:len(prefix)] != prefix {
		return 0, false
	}
	n := 0
	for _, c := range shard[len(prefix):] {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
