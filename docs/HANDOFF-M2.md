# Handoff: SixtributedSystems M2 (IF4031 Tugas Besar Milestone 2)

You are continuing Milestone 2 of a distributed-systems course project (Go, module github.com/astolfo/sixtributedsystems). Hard deadline: 20 Nov 2026. The full spec is in "IF4031 - Tugas Besar - M2.md" (repo root). The settled architecture and full task tree are tracked in the session `todo` list; the report prose is docs/report-m2.md.

## State: 41/43 todo items DONE

All code phases complete and verified green:
- `go build ./...`, `go vet ./internal/... ./services/...`, `gofmt` — clean
- `go test ./...` — aggregator, coastal-gateway, fire-builder suites pass
- `helm lint infra/helm/sixtributed -f infra/helm/values-prod.yaml` — passes; `helm template` renders all 15 workloads with role labels, resources, correct `:m2` tags

## Remaining 2 items

1. **P7.3 — end-to-end smoke deploy** (DONE 2026-11-19). kind cluster `smoke` (2 nodes) recreated after /tmp wipe; all 11 `:m2` images rebuilt (repo-root context, golang:1.26) and loaded; chart deployed with `helm upgrade --install` + smoke secret overrides; sensor-pantai applied (Running). Smoke criterion met: 23/23 pods healthy, BMKG event flows mock→aggregator→store→`/internal/audit/events` (`count:8, partial:false`). Fixes made during smoke (all committed): aux-postgres template for aggregator/fire-builder/report-consumer/feed-ingestor (chart had DSNs but no Postgres services), RBAC Role for Lease+Endpoints/EndpointSlices, `POD_NAMESPACE`/`STORE_LABEL_SELECTOR` env, store Service selectors `app: store-unit`, store `ROLE=primary`, DSN hostname alignment, fire-builder/feed-ingestor ports, report-consumer `/health` route, client-api `KAFKA_BROKERS`, AUDIT_TOKEN wiring. NOTE: /tmp binaries wipe on restart — rerun downloads of /tmp/kind + /tmp/kubectl if the WSL restarts.
   - Was mid-way: `docker build -t sixtributed/<svc>:m2 -f services/<svc>/Dockerfile .` for all 11 services (bg job may still be running — check `docker images | grep sixtributed`). NOTE: Dockerfiles MUST be built from REPO ROOT context (`-f services/x/Dockerfile .`), and were just sed-bumped to `golang:1.26` (go.mod requires 1.26; golang:1.25 image fails with GOTOOLCHAIN=local). services/coastal-gateway/Dockerfile was created fresh from the aggregator pattern (no volcano COPY).
   - Then: `/tmp/kind load docker-image sixtributed/<svc>:m2 --name smoke` for each, then deploy: Redpanda + app chart. Chart expects namespace sistem-informasi-kebencanaan-bnpb. Sensor-pantai manifest: infra/manifests/sensor-pantai.yaml (unmodifiable image).
   - Smoke criterion: all pods healthy, one BMKG event flows mock→aggregator→store→`/internal/audit/events` (Bearer $AUDIT_TOKEN).
   - Values gotchas already fixed: fire-builder/report-consumer/feed-ingestor/store images were tagged `:m3` in values.yaml — now `:m2`. `infra/helm/values-prod.yaml` lives at infra/helm/ root (chart refs resolve relative to CWD).
2. **P8.2 — evidence captures** (blocker-free but requires the running cluster from P7.3): screenshots/logs per criterion into docs/report-m2.md `[BUKTI: ...]` placeholders. List is in the report, per problem (checksum after node kill, k6 results, 60x surge, breaker timeline, DLQ replay, audit dashboard partial banner).

## Key contracts (settled, do not redesign)

- Sharded store: services/store (port 8100), one Postgres sidecar per unit on hostPath /var/sixtributed/store-N, vnode ring in aggregator via client-go watch, sync peer replication + promote-on-loss, audit fan-out in aggregator with partial/unavailable_units.
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
- fire-builder posts to single STORE_URL (no ring access) — documented shortcut in code.
- No vnode splitting under extreme skew; fire-builder single-writer.
- docker-compose M1 flow still in README alongside the M2 flow.

## Suggested skills

- `ponytail` — default coding discipline for this session; keep it on.
- `grilling` — only if the user reopens design questions.
- `code-review` — if the user asks to review the branch before committing.

## First actions

1. Check bg build job / `docker images` — finish building the 11 `:m2` images (repo-root context!).
2. Load into kind, helm upgrade --install sixtributed, apply sensor-pantai, verify pods, run the audit smoke.
3. Mark P7.3 done in todo; then P8.2 captures.
