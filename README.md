# SixtributedSystems

Milestone 1 disaster-coordination proof of concept connecting BMKG, PVMBG, and BNPB.

## Current state

Implemented services:

- BMKG mock: seismic events and tsunami warnings.
- PVMBG mock: volcanic reports, runtime schema changes, and outage control.
- Aggregator: polling, canonical mapping, PostgreSQL JSONB storage, and RabbitMQ publishing.
- Auth Service: separate Media, Field Team, and Internal Ops identities.
- Client API: JWT validation, scope enforcement, and summary/raw projections.
- Dashboard and notifier consumers: independent RabbitMQ subscribers.
- Traefik, PostgreSQL, and RabbitMQ orchestration through Docker Compose.

The implementation status audit is available in [`docs/Implementation_Status_Report.md`](docs/Implementation_Status_Report.md).

## Requirements

- Go 1.25+
- Docker Desktop with Compose
- curl
- jq for token examples
- k6 for load testing

## Start

```bash
cp .env.example .env
docker compose -f infra/docker-compose.yml up --build -d
docker compose -f infra/docker-compose.yml ps
```

Endpoints:

| Component | Address |
|---|---|
| Client API through Traefik | `http://localhost:8080` |
| BMKG mock | `http://localhost:8081` |
| PVMBG mock | `http://localhost:8082` |
| Aggregator API | `http://localhost:8090` |
| Auth Service | `http://localhost:8091` |
| RabbitMQ management | `http://localhost:15672` |

Configuration and credentials are read from `.env`. See [`.env.example`](.env.example).

## Health checks

```bash
curl http://localhost:8081/health
curl http://localhost:8082/health
curl http://localhost:8090/health
curl http://localhost:8091/health
curl http://localhost:8080/health
```

## Source APIs

BMKG requires its own API key:

```bash
curl -H "X-BMKG-Key: $BMKG_API_KEY" \
  'http://localhost:8081/seismic-events?since=1970-01-01T00:00:00Z'
```

PVMBG requires its own Bearer token:

```bash
curl -H "Authorization: Bearer $PVMBG_TOKEN" \
  'http://localhost:8082/volcanic-reports?since=1970-01-01T00:00:00Z'
```

The BMKG credential must not work against PVMBG, and vice versa.

## Downstream authentication

Request a Media token:

```bash
MEDIA_TOKEN=$(curl -sS -X POST http://localhost:8091/token \
  -H 'Content-Type: application/json' \
  -d '{"client_id":"media","client_secret":"media-secret"}' \
  | jq -r .access_token)
```

Read summary data:

```bash
curl -H "Authorization: Bearer $MEDIA_TOKEN" \
  http://localhost:8080/api/hazards
```

Media cannot request raw fields. A request with `?raw=true` returns `403` without the raw scope.

Field Team and Internal Ops use separate credentials and receive raw fields. Field Team access tokens expire after 60 seconds by default and use the `/refresh` endpoint with the issued refresh token.

## Demo controls

Enable the PVMBG schema change without restarting services:

```bash
curl -X POST http://localhost:8082/admin/schema-version \
  -H "Authorization: Bearer $PVMBG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true}'
```

New canonical reports include `attributes.confidence_level`.

Enable and disable PVMBG outage:

```bash
curl -X POST http://localhost:8082/admin/outage \
  -H "Authorization: Bearer $PVMBG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true}'

curl -X POST http://localhost:8082/admin/outage \
  -H "Authorization: Bearer $PVMBG_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"enabled":false}'
```

BMKG polling continues while PVMBG is unavailable. Aggregator status and canonical reads expose the latest source state.

## Pub/sub

Aggregator publishes `hazard.created` to RabbitMQ exchange `hazard.events`. The independent queues are:

- `dashboard.queue`
- `notifier.queue`

Stop and restart one consumer:

```bash
docker compose -f infra/docker-compose.yml stop dashboard-consumer
docker compose -f infra/docker-compose.yml start dashboard-consumer
```

The other consumer and Aggregator continue operating while the first consumer is stopped.

## Load test

```bash
k6 run scripts/load-test/sustained.js
```

The script runs 50 virtual users for 60 seconds and records latency, controlled `429` responses, and non-controlled errors. To demonstrate slow PVMBG behavior, set these values in `.env` before starting:

```text
PVMBG_DELAY_MIN_MS=3000
PVMBG_DELAY_MAX_MS=3000
```

## Development guide

Format and verify all Go code:

```bash
gofmt -w $(find services internal -name '*.go' -type f)
go test ./...
go vet ./...
```

Build or restart one service independently:

```bash
docker compose -f infra/docker-compose.yml build aggregator
docker compose -f infra/docker-compose.yml up -d aggregator
```

Service ownership rules:

- Only Aggregator accesses the Canonical Store directly.
- Client API communicates with Aggregator over HTTP.
- Source business logic stays inside its owning service.
- Shared code is limited to technical helpers such as logging and correlation IDs.
- Add consumers through RabbitMQ queues; do not add producer-to-consumer calls.

Stop the stack:

```bash
docker compose -f infra/docker-compose.yml down
```

## Deployment (Milestone 2)

Kubernetes deployment on a 2-node k3s cluster with Redpanda, via the
`infra/helm/sixtributed` Helm chart. Topology declaration:
[`docs/topology.md`](docs/topology.md).

1. Build all images and load them into both nodes:

```bash
./scripts/build-and-load-images.sh
```

2. Bootstrap the cluster (installs k3s server on node-a, joins node-b as
   agent) and deploy the chart:

```bash
./scripts/bootstrap-cluster.sh
```

Both scripts are idempotent and parameterized by `NODE_A_IP`, `NODE_B_IP`
(bootstrap) and `K3S_NODES` (image loading). Secrets are placeholders in
`infra/helm/values-prod.yaml` — fill them in before the demo.

3. Verify:

```bash
kubectl get pods -n sistem-informasi-kebencanaan-bnpb
```

## Running from scratch (full M2 flow)

From a clean machine, in order:

1. Create the two VMs (WSL2 + QEMU/KVM) and bootstrap k3s on them:

   ```bash
   ./scripts/create-vms.sh
   ```

   Produces node-a (server, ssh forward `127.0.0.1:2221`) and node-b (agent,
   `127.0.0.1:2222`) and installs node-a's kubeconfig into `~/.kube/config`.
   With a different VM provider (Multipass/VirtualBox), create the VMs manually
   as declared in [`docs/topology.md`](docs/topology.md).

2. Build every service image and load it into both nodes:

   ```bash
   ./scripts/build-and-load-images.sh
   ```

3. Install k3s server + agent (skipped if `create-vms.sh` already did it) and
   deploy the Helm chart:

   ```bash
   ./scripts/bootstrap-cluster.sh
   ```

   Idempotent; parameterized by `NODE_A_IP`, `NODE_B_IP`, `SSH_USER`. Secrets
   are placeholders in `infra/helm/values-prod.yaml` — fill them in first.

4. Deploy the coastal sensor (sensor-pantai):

   ```bash
   kubectl apply -f infra/manifests/sensor-pantai.yaml
   ```

5. Install the observability stack (kube-prometheus-stack, Loki, Tempo):

   ```bash
   ./scripts/install-observability.sh
   ```

6. **Demo step** — install the Tester Agent:

   ```bash
   kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
   ```

   Runs as DaemonSet in namespace `tester-agent`. List practice scenarios with
   `kubectl exec -n tester-agent daemonset/tester-agent -- /app list` and run
   one with `... -- /app run practice <name>`.

## Topology declaration

Full declaration (node table, node types, physical hosts, CPU/RAM,
distribution): [`docs/topology.md`](docs/topology.md). Inline summary:

| Node   | Type | Role       | CPU    | RAM   | IP            |
| ------ | ---- | ---------- | ------ | ----- | ------------- |
| node-a | VM   | k3s server | 4 vCPU | 8 GiB | 192.168.56.10 |
| node-b | VM   | k3s agent  | 4 vCPU | 8 GiB | 192.168.56.11 |

k3s, 2 VM nodes, all workloads in namespace
`sistem-informasi-kebencanaan-bnpb` with required `role` labels.

## Configuration variables

All variables live in [`.env.example`](.env.example) (copy to `.env`). The
Milestone 1 variables drive the Docker Compose stack; the Milestone 2 block is
mirrored into `infra/helm/values-prod.yaml` / `infra/helm/sixtributed/values.yaml`.

| Variable | Purpose |
|---|---|
| `TRAEFIK_PORT` | Host port for the Traefik entrypoint (8080) |
| `BMKG_PORT` | Host port for the BMKG mock (8081) |
| `PVMBG_PORT` | Host port for the PVMBG mock (8082) |
| `AGGREGATOR_PORT` | Host port for the Aggregator API (8090) |
| `CLIENT_API_PORT` | Host port for the Client API (8080) |
| `AUTH_PORT` | Host port for the Auth Service (8091) |
| `BMKG_API_KEY` | API key the Aggregator presents to the BMKG mock |
| `PVMBG_TOKEN` | Bearer token the Aggregator presents to the PVMBG mock |
| `JWT_SIGNING_KEY` | Secret used to sign downstream access tokens |
| `ACCESS_TOKEN_TTL_SECONDS` | Access token lifetime (default 60) |
| `MEDIA_CLIENT_ID` / `MEDIA_CLIENT_SECRET` | Media persona credentials (summary projections only) |
| `FIELD_CLIENT_ID` / `FIELD_CLIENT_SECRET` | Field Team persona credentials (raw fields, short TTL, refresh) |
| `INTERNAL_CLIENT_ID` / `INTERNAL_CLIENT_SECRET` | Internal Ops persona credentials (raw fields) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Canonical Postgres bootstrap credentials |
| `POSTGRES_DSN` | Aggregator's DSN to the canonical Postgres |
| `AUTH_POSTGRES_DSN` | Auth Service Postgres DSN |
| `RABBITMQ_URL` | AMQP connection URL for pub/sub |
| `POLL_INTERVAL_SECONDS` | Aggregator polling interval per source |
| `SOURCE_TIMEOUT_SECONDS` | Per-request timeout against a source mock |
| `BMKG_DELAY_MS` | Artificial latency added by the BMKG mock |
| `PVMBG_DELAY_MIN_MS` / `PVMBG_DELAY_MAX_MS` | Artificial latency range of the PVMBG mock |
| `EVENT_GENERATION_INTERVAL_SECONDS` | How often the mocks synthesize new events |
| `SEED_RECORD_COUNT` | Records seeded into the mocks at startup |
| `KAFKA_BROKERS` | Redpanda broker list used by Kafka consumers (M2) |
| `STORE_URL` | Base URL of the sharded store for cross-store reads (M2) |
| `AGGREGATOR_POSTGRES_DSN` | Aggregator's own Postgres DSN in the k3s deployment (M2) |
| `AUDIT_TOKEN` | Bearer token guarding `/internal/audit/*` and `/internal/admin/*` (M2) |
| `LEASE_NAME` | Leader-election lease name for the Aggregator (M2) |
| `POD_NAMESPACE` | Namespace used for leader election (M2) |
| `STORE_VNODES` | Virtual nodes per store unit in the consistent-hashing ring (M2) |
| `ANALYTICS_BUCKET` | Time-bucket size for the analytics dashboard (M2) |
| `SATELLITE_TOKEN` | Bearer token accepted by the feed-ingestor satellite endpoint (M2) |
| `SSE_START_OFFSET` | Where feed-ingestor starts reading the SSE stream (M2) |
| `INGESTOR_POSTGRES_DSN` | feed-ingestor Postgres DSN (M2) |
| `FIRE_POSTGRES_DSN` | fire-builder Postgres DSN (M2) |
| `PROVINCE_REFERENCE_PATH` | Path to the province reference data mounted into fire-builder (M2) |
| `SENSOR_BASE_URL` | Base URL of the sensor-pantai service (M2) |
| `PANTAI_AUTH_TOKEN` | Bearer token for the sensor-pantai API (M2) |
| `REPORT_POSTGRES_DSN` | report-consumer Postgres DSN for field reports (M2) |
| `NOTIFIER_POSTGRES_DSN` | notifier consumer Postgres DSN (M2) |
| `MEDIA_RATE_LIMIT` | Requests/second allowed for Media tokens (load shedding) |

## Test scenarios

How to trigger each scenario: [`docs/scenarios.md`](docs/scenarios.md).
One-line list:

- **Worker node kill + store checksum** — power off node-b's VM, wipe its
  store hostPath, reboot; verify data integrity with
  `GET /internal/audit/checksum`.
- **Aggregator failover** — delete the leader aggregator pod; a standby
  replica takes over the lease (`LEASE_NAME`).
- **DLQ replay** — fail messages with a schema change, then
  `./scripts/replay.sh --all` (uses `AUDIT_TOKEN`).
- **Satellite feed burst** — feed-ingestor ingests the live SSE feed; observe
  lag and recovery on the dashboard.
- **Sensor pantai failure** — stop or fail the sensor; the coastal pipeline
  degrades without losing already-accepted events.
- **Clock skew / VM freeze / aggregator scale & rollout** — helpers in
  `scripts/scenarios.sh` (source it, then run `clock_skew`, `freeze_vm`,
  `kill_worker_node`, `scale_aggregator`, `rollout_aggregator`, `audit_checksum`).

## Audit API

`GET /internal/audit/*` on the Aggregator is guarded by a bearer token: send
`Authorization: Bearer $AUDIT_TOKEN`. Without it, or when `AUDIT_TOKEN` is
empty, requests get `401 unauthorized`. Endpoints:

- `GET /internal/audit/events` — paged canonical events across all store units.
- `GET /internal/audit/checksum?source=BMKG` — per-source integrity checksum.
- `GET /internal/audit/location?hazard_id=...` — where a hazard is stored.
- `GET /internal/audit/unprocessed` — parked/retrying (DLQ) messages.

Example from a machine with port-forward access:

```bash
curl -sS -H "Authorization: Bearer $AUDIT_TOKEN" \
  http://localhost:8090/internal/audit/checksum?source=BMKG
```

The same token guards the admin replay endpoints used by
[`scripts/replay.sh`](scripts/replay.sh).

## Dashboards

- **Analytics dashboard** — served by the Client API at `GET /dashboard`
  (embedded page; data from `/api/analytics`). Access it through a port-forward:

  ```bash
  kubectl port-forward -n sistem-informasi-kebencanaan-bnpb svc/client-api 8080:8080
  # open http://localhost:8080/dashboard
  ```

- **Metrics dashboards (Grafana)** — installed by
  [`scripts/install-observability.sh`](scripts/install-observability.sh):

  ```bash
  kubectl port-forward -n observability svc/kube-prometheus-stack-grafana 3000:80
  # open http://localhost:3000 — user: admin, password: admin
  ```

## Video demo

The 30–45 minute recording plan mapped to the spec's Cakupan Wajib is in
[`docs/demo-script.md`](docs/demo-script.md).
