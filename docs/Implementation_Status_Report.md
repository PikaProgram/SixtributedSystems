# Milestone 1 Implementation Status Report

## Audit basis

This report cross-references the implementation against `docs/Spesifikasi_Milestone_1.md`, including the common service rules, source contracts, canonical mapping rules, client permissions, and P1–P5 acceptance criteria.

Status labels:

- **Correct:** implemented and verified in code or runtime.
- **Corrected:** a gap was found during this audit and fixed.
- **Partial:** implementation exists, but the required demonstration or evidence is not yet complete.
- **Missing:** not implemented.

## Current project state

The repository contains a Go implementation with separate services and Dockerfiles:

- BMKG mock
- PVMBG mock
- Aggregator
- Auth Service
- Client-Facing API
- Dashboard consumer
- Notifier consumer
- PostgreSQL
- RabbitMQ
- Traefik

The Compose stack has been built and started successfully. Current runtime services are healthy, and both RabbitMQ consumer queues have one active consumer.

Static verification currently passes:

```text
go test ./...
go vet ./...
docker compose -f infra/docker-compose.yml config
```

Runtime verification currently passes:

```text
BMKG health:       HTTP 200
PVMBG health:      HTTP 200
Aggregator health: HTTP 200
Auth health:       HTTP 200
```

## Common implementation requirements

| Requirement | Status | Evidence / notes |
|---|---|---|
| Network communication between services | Correct | REST calls connect mocks, Aggregator, Auth, and Client API; RabbitMQ connects producer and consumers. |
| Storage owned by one service | Correct | Aggregator owns Canonical Store access; Client API queries Aggregator HTTP API. |
| Independent Docker container per service | Correct | Separate Dockerfiles and Compose services exist for application services. Infrastructure uses PostgreSQL, RabbitMQ, and Traefik containers. |
| Independent service lifecycle | Partial | Compose supports independent rebuild/restart. This is documented and was exercised for Aggregator. Full evidence for every container still needs a demo capture. |
| Separate BMKG/PVMBG credentials | Correct | BMKG uses `X-BMKG-Key`; PVMBG uses a separate Bearer token. Cross-use returns `401`. |
| Secrets from environment | Correct | `.env.example` exists; service credentials are read from environment variables. Development fallback values remain in code and must be replaced in the final demo environment. |
| Health endpoints | Corrected | BMKG, PVMBG, Aggregator, Auth, and Client API expose health endpoints. Consumers now expose configurable health ports `8083` and `8084`. |
| Structured logs | Correct | Services use JSON structured logs. |
| Correlation ID propagation | Partial | HTTP middleware generates/propagates correlation IDs and outbound Aggregator calls log latency. RabbitMQ event envelopes currently receive an empty correlation ID from background polling, and consumer logs can therefore show `correlation_id:null`. This should be corrected before final report evidence. |
| Outbound latency logging | Correct | Aggregator records source call duration; Client API has the outbound call path but does not yet emit a dedicated latency log field. |

## Source contracts

### BMKG

| Requirement | Status | Evidence / notes |
|---|---|---|
| `GET /seismic-events?since=` | Correct | Implemented and authenticated. |
| `GET /tsunami-warnings?since=` | Correct | Implemented as separate endpoint. |
| `GET /health` | Correct | Runtime HTTP 200 verified. |
| At least 20 seed records | Correct | 20 historical records are created at startup. |
| Fixed 50–150 ms delay | Correct | Default is 100 ms and configurable. |
| `X-BMKG-Key` authentication | Correct | Invalid credentials return `401`. |
| At least one new event per 10 seconds | Corrected | Periodic live event generation was added using `EVENT_GENERATION_INTERVAL_SECONDS`. Runtime query returned 22 records after the audit interval. |
| Tsunami warnings only for tsunami-capable events | Corrected | Seed warning references were aligned with events whose `potential_tsunami` is true. |

### PVMBG

| Requirement | Status | Evidence / notes |
|---|---|---|
| `GET /volcanic-reports?since=` | Correct | Implemented and authenticated. |
| `POST /admin/schema-version` | Correct | Runtime toggle implemented. |
| `POST /admin/outage` | Correct | Runtime outage toggle implemented. |
| `GET /health` | Correct | Runtime HTTP 200 verified. |
| At least 20 seed records | Correct | 20 historical records are created at startup. |
| Random 500 ms–3 second delay | Correct | Configurable min/max delay implemented. |
| Separate Bearer credential | Correct | Invalid credentials return `401`. |
| Runtime `confidence_level` introduction | Correct | Field is omitted initially and added after schema toggle without restart. |
| At least one new report per 10 seconds | Corrected | Periodic live report generation was added using `EVENT_GENERATION_INTERVAL_SECONDS`. Runtime query returned 22 records after the audit interval. |
| Admin controls remain available during outage | Correct | Outage is checked only on data endpoint; admin handlers remain available. |

## Canonical mapping

| Mapping requirement | Status | Evidence / notes |
|---|---|---|
| Stable BNPB-generated `hazard_id` | Correct | SHA-256 source/reference identity. |
| BMKG source and seismic type | Correct | Implemented. |
| BMKG region and coordinates | Correct | Implemented. |
| BMKG severity thresholds | Correct | Magnitude thresholds and warning-level precedence implemented. |
| Awas warning maps to AWAS | Correct | Implemented. |
| Tsunami warning data in `attributes` | Correct | Related warning object is preserved. |
| PVMBG source and volcanic type | Correct | Implemented. |
| Static volcano reference mapping | Corrected | Mapping now supports both `volcano-01`…`04` and seeded `volcano-1`…`4` identifiers. Runtime records map to Merapi, Semeru, Agung, and Sinabung coordinates. |
| PVMBG alert-level mapping | Correct | Uppercase canonical severity is generated. |
| Dynamic fields in `attributes` | Partial | `confidence_level` is preserved. Decoder currently uses typed structs rather than a generic unknown-field map, so arbitrary future unknown fields are not preserved. This must be fixed for strict P1 compliance. |
| JSONB flexible storage | Correct | `attributes JSONB` stores dynamic values without migration. |
| Existing records remain readable after schema change | Corrected | Persistence changed from `DO NOTHING` to an upsert so existing source records can gain newly available attributes. |

## Problem 1: Schema evolution

| Criterion | Status | Evidence / notes |
|---|---|---|
| Normal old-schema ingestion | Correct | Aggregator maps both source types into PostgreSQL. |
| Runtime schema trigger without restart | Correct | PVMBG schema endpoint toggles runtime state. |
| Aggregator remains operational | Correct | Runtime schema test was performed without restarting Aggregator. |
| Unknown-field policy declared | Partial | README declares additive evolution conceptually, but implementation currently only preserves explicitly modeled fields. Generic unknown-field preservation is still missing. |
| Supported/unsupported evolution documented | Partial | README contains a basic policy but not a complete report-grade explanation with evidence. |

## Problem 2: Concurrency, availability, graceful degradation

| Criterion | Status | Evidence / notes |
|---|---|---|
| Independent BMKG/PVMBG pollers | Correct | Separate goroutines and cursors. |
| PVMBG slow delay does not block BMKG polling | Correct | Logs show BMKG calls continuing while PVMBG takes up to 3 seconds. |
| Bounded concurrency | Corrected | Client API now has a configurable semaphore and returns HTTP 429 when capacity is exhausted. |
| 50+ sustained load for 60 seconds | Partial | k6 script exists, but the required 60-second run and report metrics have not yet been captured in this repository. |
| BMKG-only p95 below 300 ms | Partial | BMKG delay and independent polling support the scenario, but no recorded p95 result is committed. |
| PVMBG outage | Correct | Runtime outage returns fast `503` at PVMBG and Aggregator keeps BMKG alive. |
| Stale volcanic data or explicit unavailable status | Partial | Aggregator now exposes `stale_since` when PVMBG has a newer error than its last success. Client API currently forwards the Aggregator envelope but does not explicitly document/filter this field in a dedicated source response. |
| Recovery without restart | Correct | PVMBG outage was enabled and disabled successfully while services remained running. |
| Circuit breaker | Missing | The plan requested a circuit breaker, but the current Aggregator only has timeout/error tracking. |

## Problem 3: Authentication and trust

| Criterion | Status | Evidence / notes |
|---|---|---|
| BMKG credential rejected by PVMBG | Correct | Runtime `401` verified. |
| PVMBG credential rejected by BMKG | Correct | Separate header formats and credentials are enforced. |
| Media summary-only access | Correct | Server-side projection removes raw fields. |
| Media raw request rejected | Correct | `?raw=true` returns `403`. |
| Field Team short-lived access token | Correct | Default TTL is 60 seconds and configurable. |
| Field Team refresh without login | Correct | Opaque refresh token flow implemented. |
| Old refresh token rejected after rotation | Correct | Refresh tokens are deleted on use. |
| Three distinct client identities | Correct | Media, Field Team, and Internal Ops credentials are independently configured. |
| No committed secret | Partial | `.env.example` is present and no real secret was found. Development fallback credentials are embedded as defaults in source and should be removed or clearly limited to local demo defaults. |

## Problem 4: Microservices and flexible storage

| Criterion | Status | Evidence / notes |
|---|---|---|
| Separate containers | Correct | Compose contains independent services and Dockerfiles. |
| Independent rebuild/restart demonstration | Partial | Aggregator was rebuilt independently while other services remained running. A complete captured scenario is still needed. |
| Dynamic JSONB attributes without migration | Correct | PostgreSQL JSONB is used. |
| Old/new records coexist | Corrected | Upsert preserves and refreshes records when new attributes appear. Runtime records were observed with `confidence_level`. |
| Canonical Store direct access only by Aggregator | Correct | Client API only calls Aggregator HTTP. |
| Service boundary rationale in report | Missing | Needs report prose covering polling, aggregation/storage, auth, client API, broker, and consumer boundaries. |
| Two storage alternatives compared | Missing | Needs report prose comparing PostgreSQL JSONB with MongoDB or a document store. |

## Problem 5: Pub/sub fan-out

| Criterion | Status | Evidence / notes |
|---|---|---|
| Aggregator publishes to RabbitMQ | Correct | Durable topic exchange and persistent messages are implemented. |
| Two independent consumers | Corrected | Consumers now bind correctly to `hazard.events` with routing key `hazard.created`; both queues report one active consumer and logs show event processing. |
| Consumer outage does not block producer | Partial | Durable queues and manual acknowledgements are implemented, but a complete stop/restart evidence capture is still needed. |
| Recovery behavior documented | Partial | README describes queue persistence, but exact observed redelivery evidence is not recorded. |
| Add consumer without Producer change | Correct | Consumer image can be launched with a new queue/name configuration. |
| At-least-once semantics | Partial | Persistent messages and acknowledgements are implemented. Consumer side-effect deduplication storage is not implemented, so idempotency is only conceptual. |

## Corrections implemented during this audit

1. Added BMKG periodic live-event generation.
2. Added PVMBG periodic live-report generation.
3. Corrected BMKG tsunami-warning fixture references.
4. Corrected volcano identifier compatibility for seeded records.
5. Initialized the Canonical Store table before polling.
6. Changed Canonical Store persistence to upsert source records so new schema attributes can be retained.
7. Added Aggregator source filtering and `stale_since` metadata.
8. Added Client API bounded concurrency with controlled HTTP 429 responses.
9. Added consumer health endpoints and RabbitMQ reconnect behavior.
10. Corrected RabbitMQ consumer exchange/queue binding and verified both consumers process events.
11. Ran `gofmt` over every Go source file after the final source edits.

## Remaining work before claiming full specification compliance

1. Decode source JSON into a generic map plus typed known fields so arbitrary unknown fields are preserved in `attributes`.
2. Add an Aggregator circuit breaker with explicit open/half-open state and configurable thresholds.
3. Add consumer-owned processed-event tracking to make at-least-once processing idempotent in behavior, not only by convention.
4. Add report-grade evidence scripts/results for k6 sustained load, p95/p99 metrics, outage fallback, independent rebuild, and consumer restart/redelivery.
5. Add the required P1–P5 report rationale, alternatives, trade-offs, assumptions, and evidence references.
6. Remove or clearly isolate source-code development credential fallbacks before submission.

## Verification snapshot

The latest static checks pass:

```text
go test ./...
go vet ./...
docker compose -f infra/docker-compose.yml config
```

The latest runtime snapshot showed:

- BMKG and PVMBG each returning 22 records after the live-generation interval.
- Aggregator canonical store containing 46 events.
- RabbitMQ `dashboard.queue` and `notifier.queue` each with one active consumer.
- Both consumers logging `event_processed` for `hazard.created` messages.
