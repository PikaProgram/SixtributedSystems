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
