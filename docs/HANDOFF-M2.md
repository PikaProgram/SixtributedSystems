# Handoff: SixtributedSystems M2 (IF4031 Tugas Besar Milestone 2)

You are continuing Milestone 2 of a distributed-systems course project (Go, module github.com/astolfo/sixtributedsystems). Hard deadline: 20 Nov 2026. The full spec is in "IF4031 - Tugas Besar - M2.md" (repo root). The settled architecture and full task tree are tracked in the session `todo` list; the report prose is docs/report-m2.md.

## State

Live `todo` list (10 items) is the source of truth; 7/10 done as of this writing.

Done:
- All M2 code complete and green: `go build ./...`, `go vet ./...`, `go test ./...` (aggregator, coastal-gateway, fire-builder suites pass)
- `helm lint infra/helm/sixtributed -f infra/helm/values-prod.yaml` passes; default values render without node pinning
- P7.3 smoke deploy on kind 2-node `smoke` cluster was green once (23/23 pods, BMKG event flow verified) — but **/tmp was wiped since** (WSL restart): images, kind/kubectl binaries, and the cluster are all gone. `scripts/build-and-load-images.sh` now supports `KIND_CLUSTER=smoke` for the load step (k3s ssh loop unchanged).
- Report (docs/report-m2.md) corrected to match the implementation: static shard ring, primary+standby pods per shard, shard controller, storeclient retry, aggregator DLQ ownership, field-reports NOT written to store.

Remaining:
1. **Rebuild + redeploy smoke** (kind): `docker build` 11 images (repo-root context), `KIND_CLUSTER=smoke bash scripts/build-and-load-images.sh`, recreate kind cluster if needed, `helm upgrade --install` (namespace `sistem-informasi-kebencanaan-bnpb`), apply `infra/manifests/sensor-pantai.yaml`, verify 6 store pods (3 shards x primary/standby), ring sees 3 shards x 2 units.
2. **Node-kill drill**: failover writes, partial-marked reads, shard-1 promotion by shard controller (≤10s).
3. **P8.2 evidence captures** per `[BUKTI: ...]` placeholders in the report (terminal blocks, not screenshots — decided).

## Key contracts (settled, do not redesign)

- Sharded store: services/store (port 8100), **3 shards x (primary + standby) pods** via per-shard StatefulSets `store-primary-N` / `store-standby-N`, one Postgres sidecar per pod on hostPath /var/sixtributed/store-N. Static vnode ring over shard names (SHARD_COUNT=3) in internal/vnode; every writer (aggregator persist(), fire-builder, coastal-gateway via internal/storeclient) hashes `source:source_ref_id` → shard, retries the shard's pod DNS on 422/503. Primary writes sync to standby's PG before answering; standby catches up by watermark `(ingested_at,hazard_id)` and auto-promotes when the primary is unreachable. Aggregator shard controller (leader-only, 10s) promotes/demotes via `/internal/store/role`; node pinning only when `store.nodes` is non-empty (values-prod sets node-a/node-b).
- Leader election: K8s Lease aggregator-leader (15s/5s); only leader polls BMKG/PVMBG.
- Kafka (Redpanda single-broker) replaced RabbitMQ: topics hazard.events, detections.raw, field-reports, fire.clusters. internal/kafka wrapper (kafka-go).
- DLQ: aggregator's own PG `unprocessed_messages` (retryable/parked), replay POST /internal/admin/replay, audit /internal/audit/unprocessed.
- Field reports: client-api POST /field-reports → 202 after Kafka publish; report-consumer dedupes on report_id PK.
- Feed: services/feed-ingestor SSE w/ Last-Event-ID + offset in own PG → detections.raw; services/fire-builder union-find 1km/360min, 30min lateness grace, stable fire-<hex6> ids; late_count via /internal/fire/stats.
- Coastal: services/coastal-gateway breaker (3 fails/30s slow → open 30s → half-open 1 probe), last-good cache, contract-violation shunting; notifier consumer persists critical notifications (dedup by hazard_id).
- Observability: OTel wired in all mains, /metrics on health port, kube-prom-stack+Loki+Tempo values at infra/helm/observability/values-observability.yaml.
- Scripts: scripts/bootstrap-cluster.sh, build-and-load-images.sh, create-vms.sh, install-observability.sh, scenarios.sh, surge-test.sh, compare-reference.sh, replay.sh, load-test/{api.js,distributed.sh}. docs/: report-m2.md, scenarios.md, game-day.md, topology.md, demo-script.md.

## Known gaps (honest, for the report's batasan section)

- Feed Satelit live probe returned 401 (dummy token `change-me-satellite-token`); real group token goes into the ingestor Secret at deploy.
- No vnode splitting under extreme skew; fire-builder/coastal-gateway are single-writer per pod (no shard ring of their own — they rely on storeclient retry).
- docker-compose M1 flow still in README alongside the M2 flow.

## Suggested skills

- `ponytail` — default coding discipline for this session; keep it on.
- `grilling` — only if the user reopens design questions.
- `code-review` — if the user asks to review the branch before committing.

## First actions

1. Recreate kind 2-node cluster `smoke` (re-download kind/kubectl to /tmp if needed).
2. `KIND_CLUSTER=smoke bash scripts/build-and-load-images.sh` (builds from repo root, loads into kind).
3. `helm upgrade --install sixtributed infra/helm/sixtributed -n sistem-informasi-kebencanaan-bnpb --create-namespace` (default values; no node pinning on kind), apply `infra/manifests/sensor-pantai.yaml`.
4. Verify 6 store pods + ring (`/internal/audit/*` with AUDIT_TOKEN), then node-kill drill + evidence captures into the report.
