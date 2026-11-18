# Scenario Scripts — Trigger Commands, Expected Results, Dashboards (P7.2)

One section per scenario across P1–P5. All scripts live in `scripts/`.
Sources `scripts/scenarios.sh` for kubectl-scoped scenarios; standalone scripts run directly.

## Prerequisites

- `kubectl` context pointing at the cluster; namespace default `sistem-informasi-kebencanaan-bnpb`.
- Tokens in env: `AUDIT_TOKEN` (aggregator audit endpoints), `SATELLITE_TOKEN` (Feed Satelit), `PANTAI_AUTH_TOKEN` (coastal sensor).
- `jq` installed.
- Grafana dashboards scrape each service's `/metrics` (Prometheus datasource).

---

## P1 — Node failure / store recovery

### `kill_worker_node` (scenarios.sh)
- **Command:** `source scripts/scenarios.sh && kill_worker_node`
- **Trigger:** manual — poweroff node-b VM, wipe `/var/lib/rancher/k3s/storage/*`, reboot.
- **Expected:** store pod reschedules; hazard data re-reachable via the surviving peer; aggregator shows no partial audit pages once the unit returns.
- **Watch:** `kubectl get pods -o wide -w`, Grafana panel `up{service="store"}`, aggregator logs `store_checksum`.

### `clock_skew <node-host> <delta>` (scenarios.sh)
- **Command:** `clock_skew node-b-host +45`
- **Expected:** node clock shifted by ±delta seconds; system continues ingesting; report discusses ordering effects.
- **Watch:** node kernel logs, aggregator `status` endpoint (`bmkg.last_error`).

### `freeze_vm` (scenarios.sh)
- **Command:** `freeze_vm` (prints the manual step)
- **Trigger:** manual — suspend the VM of the node running the leader aggregator pod.
- **Expected:** leader lease fails over to another aggregator replica within the lease timeout; duplicates suppressed by `hazard_id` upsert.
- **Watch:** `aggregator` leader field in `GET /status`, Grafana `fire_builder_lag_seconds`.

---

## P2 — Scale / rollout / throughput

### `scale_aggregator <n>` (scenarios.sh)
- **Command:** `scale_aggregator 5`
- **Expected:** replicas scale to n; ring rebalances; no dropped events.
- **Watch:** `kubectl get pods -l app=aggregator`, Grafana `http_server_requests_total`.

### `rollout_aggregator` (scenarios.sh)
- **Command:** `rollout_aggregator`
- **Expected:** rolling image swap to `sixtributed/aggregator:m2-next`, `maxUnavailable 0 / maxSurge 1`, zero 5xx during rollout; PodDisruptionBudget keeps ≥2 of 3.
- **Watch:** `kubectl rollout status`, Grafana p95 `histogram_quantile(0.95, sum(rate(http_server_request_duration_ms_bucket[5m])) by (le))`.

### load tests (scripts/load-test/)
- **Command:** `scripts/load-test/distributed.sh` (P2.6 scale mid-run), `scripts/load-test/distributed.sh --throughput-compare` (P2.7), `scripts/load-test/sustained.sh` if present (sustained profile).
- **Expected:** P2.6: replicas 3→5→2 mid-run with no lost requests. P2.7: prints throughput ratio 1 vs 3 replicas.
- **Watch:** per-pod k6 JSON summaries in job logs; Grafana request-rate panel.

---

## P3 — DLQ / replay

### `scripts/replay.sh`
- **Command:** `AUDIT_TOKEN=... scripts/replay.sh --all` / `--source PVMBG` / `--ids <id1,id2>`
- **Expected:** parked/retrying messages replayed; `GET /internal/admin/replay/stats` shows the queue draining to zero.
- **Watch:** aggregator logs `admin_replay`, Grafana `http_server_requests_total` for the admin paths.

---

## P4 — Satellite feed (surge, catch-up, recovery equivalence)

### `scripts/surge-test.sh` (P4.5)
- **Command:** `SATELLITE_TOKEN=... AUDIT_TOKEN=... AGGREGATOR_URL=http://aggregator:8090 scripts/surge-test.sh`
  - `TRIGGER=manual` to pause and wait for the assistant-side exam window; `SPEED=60` is the target speed.
- **Trigger:** exam/practice speed bump on the Feed Satelit (assistant-controlled, same idea as `PANTAI_PROFILE` on the coastal sensor); if the feed exposes no speed control from the group token, the script prints document-only instructions and detects the surge via `latest_offset` growth.
- **Expected:** script follows `GET /v1/session` `latest_offset` (Bearer `SATELLITE_TOKEN`), waits for the surge to end (offset stops advancing), then polls the WILDFIRE event count every 5s until stable for 3 polls. Prints `time_to_caught_up_seconds=` — **must be ≤60** (criterion 4), else exit 1. Also prints the latency panel query strings (below).
- **Watch:** Grafana `fire_builder_lag_seconds` (ingest backlog during the surge), p95 panel:
  `histogram_quantile(0.95, sum(rate(http_server_request_duration_ms_bucket[5m])) by (le))`

### `scripts/compare-reference.sh <reference.csv>` (P4.6)
- **Command:** `AUDIT_TOKEN=... scripts/compare-reference.sh reference.csv`
- **Expected:** dumps all WILDFIRE events via `GET /internal/audit/events?source=SATELIT` with cursor pagination into CSV (`source_ref_id,detection_keys,attributes`), compares row-by-row with the assistant reference, prints `missing=N extra=N mismatched=N`; **exit 0 iff identical** (criterion 6). Report any diff cause (e.g. late-detection window boundary).
- **Watch:** script's own per-page progress log; audit `partial: true` warnings mean a store unit was down during the dump.

### `scripts/compare-reference.sh --recovery <reference.csv>` (P4.5 criterion 5)
- **Command:** start the feed streaming, then: `AUDIT_TOKEN=... scripts/compare-reference.sh --recovery reference.csv`
- **Expected:** kills `fire-builder` mid-stream (scale to 0), waits `RECOVERY_WAIT=10`s, restarts it, lets it drain the backlog, dumps and compares. Result **identical to the clean run** — no lost fires, no duplicates (upsert on `hazard_id`, offset resume from PG).
- **Watch:** `kubectl get pods -l app=fire-builder -w`, `fire_builder_lag_seconds` spike then fall to baseline, `detections.raw` topic consumer lag.

---

## P5 — Coastal sensor failure (P5.5)

All in `scripts/scenarios.sh` (source it first: `source scripts/scenarios.sh`). The sensor's own failure modes are driven by the tester-agent image: `scenario_list` shows available names, `scenario_run practice <name>` fires one.

### `sensor_down [seconds]`
- **Command:** `sensor_down 60`
- **Trigger:** `kubectl scale deploy sensor-pantai --replicas=0`, wait, restore + `rollout status`.
- **Expected:** coastal-gateway breaker transitions closed → open (3 consecutive failures), requests stop hitting the dead sensor ("berhenti mengetuk pintu yang tidak pernah dibuka"), `/internal/coastal/observations` keeps serving from the last-good cache (`stale_serving: true`). After restore, breaker half-opens → closes.
- **Watch:** coastal-gateway logs `breaker_transition`; Grafana `coastal_breaker_state` (0 closed / 1 half-open / 2 open).

### `sensor_slow [profile]`
- **Command:** `sensor_slow slow`
- **Trigger:** runs `scenario_run practice <profile>` when the tester-agent lists it; otherwise patches sensor-pantai CPU limit to `5m` to force slow responses (restore command printed by the script).
- **Expected:** breaker trips on 30s of slow responses; gateway serves stale-but-valid observations under the 50ms budget.
- **Watch:** Grafana `coastal_breaker_state`, `outbound_duration_ms` p99 (coastal-gateway), `coastal_violations_total`.

### `sensor_storm_check [seconds]`
- **Command:** `sensor_storm_check 60`
- **Trigger:** wraps `sensor_down` and samples `GET /internal/coastal/stats` before/during/after (in-cluster curl pod — endpoint is internal, no auth).
- **Expected:** prints breaker state at each phase and the diff of `violations` counters; request counter to the sensor stays flat once the breaker opens — **no request storm** (criterion: breaker back-off instead of hammering).
- **Watch:** the script's own output; Grafana `coastal_violations_total`, coastal-gateway access logs (poll attempts should stop while open).

### `scenario_list` / `scenario_run practice <name>` / `scenario_exists <name>`
- **Command:** `scenario_list`, `scenario_exists slow`, `scenario_run practice sensor_lies`
- **Expected:** lists/fires the tester-agent's built-in failure scenarios (the assistant-provided sensor image's practice modes). Use `scenario_exists` to gate wrappers like `sensor_slow`.
- **Watch:** `tester_logs` — `kubectl logs -n tester-agent -l app=tester-agent --tail=100 -f` for the scenario's own narrative output.

### `coastal_stats`
- **Command:** `coastal_stats`
- **Expected:** raw JSON from `/internal/coastal/stats`: `breaker`, `violations`, `last_good_age_seconds`, `stale_serving`.
- **Watch:** n/a (raw probe).

---

## Game-day quick reference (P5.6)

See `docs/game-day.md` for the run-of-show combining `scenario_list`, `scenario_run`, `rollout_aggregator`, and `audit_checksum <token>`.
