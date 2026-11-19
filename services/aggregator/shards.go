package main

import (
	"context"
	"sort"
	"time"
)

const shardControllerInterval = 10 * time.Second

// runShardController keeps each shard's role state consistent. It runs only
// on the leader (spawned from the leader-election hook), every
// shardControllerInterval, and does exactly one of per shard:
//
//   - no reachable primary  -> promote the best reachable unit (a shard
//     whose primary node died must still accept writes)
//   - two reachable primaries -> resolve split-brain: keep the freshest
//     (max newest ingested_at; tie: the pod labelled primary, then name),
//     demote the other (it resumes standby replication; its rows are kept,
//     replication only upserts)
//   - exactly one primary -> nothing
//
// Pods keep their original labels after a flip; the candidate order
// (primary label first) therefore degrades gracefully: a stale label costs
// one 503 round-trip, persist() then walks to the live primary.
func (a *aggregator) runShardController(ctx context.Context) {
	ticker := time.NewTicker(shardControllerInterval)
	defer ticker.Stop()
	for {
		a.reconcileShards(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *aggregator) reconcileShards(ctx context.Context) {
	for _, shard := range a.ring.shards() {
		units := a.ring.candidates(shard)
		var primaries, others []unit
		for _, u := range units {
			callCtx, cancel := context.WithTimeout(ctx, storeTimeout)
			health, err := a.stores.unitHealth(callCtx, u)
			cancel()
			if err != nil {
				a.logger.Error("shard_health", "shard", shard, "unit", u.Name, "error", err)
				continue // unreachable; excluded from role math
			}
			if health["role"] == "primary" {
				primaries = append(primaries, u)
			} else {
				others = append(others, u)
			}
		}
		switch {
		case len(primaries) == 0:
			if len(others) == 0 {
				continue
			}
			best := a.bestUnit(ctx, append([]unit(nil), others...))
			if best == (unit{}) {
				continue
			}
			a.flipRole(ctx, best, "primary", "shard without reachable primary")
		case len(primaries) > 1:
			best := a.bestUnit(ctx, primaries)
			for _, u := range primaries {
				if u.Name != best.Name {
					a.flipRole(ctx, u, "standby", "split-brain: multiple primaries in shard "+shard)
				}
			}
		}
	}
}

// bestUnit ranks candidates by (newest ingested_at desc, replica label
// primary first, name asc).
func (a *aggregator) bestUnit(ctx context.Context, units []unit) unit {
	type scored struct {
		unit     unit
		newest   time.Time
		newestOK bool
		labeled  bool
	}
	scores := make([]scored, 0, len(units))
	for _, u := range units {
		callCtx, cancel := context.WithTimeout(ctx, storeTimeout)
		stats, err := a.stores.unitStats(callCtx, u)
		cancel()
		s := scored{unit: u, labeled: u.Replica == "primary"}
		if err == nil {
			if raw, ok := stats["newest"].(string); ok && raw != "" {
				if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
					s.newest, s.newestOK = t, true
				}
			}
		}
		scores = append(scores, s)
	}
	sort.Slice(scores, func(i, j int) bool {
		a, b := scores[i], scores[j]
		if a.newestOK != b.newestOK {
			return a.newestOK
		}
		if a.newestOK && !a.newest.Equal(b.newest) {
			return a.newest.After(b.newest)
		}
		if a.labeled != b.labeled {
			return a.labeled
		}
		return a.unit.Name < b.unit.Name
	})
	if len(scores) == 0 {
		return unit{}
	}
	return scores[0].unit
}

func (a *aggregator) flipRole(ctx context.Context, u unit, role, reason string) {
	callCtx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()
	if err := a.stores.setUnitRole(callCtx, u, role); err != nil {
		a.logger.Error("shard_role_flip", "unit", u.Name, "role", role, "reason", reason, "error", err)
		return
	}
	a.logger.Info("shard_role_flip", "unit", u.Name, "role", role, "reason", reason)
}
