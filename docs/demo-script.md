# Video Demo Script (P8.3)

30–45 minute recording, mapped to the spec's **Cakupan Wajib**:

1. Eksekusi cURL/Postman dengan pengukuran `time_total`, Dasbor Analitik/Metrik, CLI/GUI basis data (psql/DBeaver) — sections 3, 4, 5.
2. Log Tester Agent (`kubectl logs`) saat skenario ujian dijalankan — section 6.
3. Pembuktian P1–P5 (node mati, failover Aggregator, DLQ replay, Feed Satelit, ketahanan Sensor Pantai) — sections 6 and 7.
4. Setiap anggota tampil menjelaskan bagian arsitektur/kode yang ditanganinya — section 2.

Namespace: `sistem-informasi-kebencanaan-bnpb` (`$NS`). Helper functions come
from `source scripts/scenarios.sh`. Before recording: fill
`infra/helm/values-prod.yaml` secrets, set `PANTAI_PROFILE=demo` in
`infra/manifests/sensor-pantai.yaml`, have `AUDIT_TOKEN` exported, and run one
full pass of this script as rehearsal.

## 1. Arsitektur walkthrough (5 min)

- **Record:** `docs/architecture.drawio` (or the rendered diagram) +
  `kubectl get pods -o wide -n $NS` alongside `docs/topology.md`, then a live
  walk of the request path: sensor/mock → aggregator → Redpanda → consumers →
  client-api.
- **Script/endpoint:** `./scripts/bootstrap-cluster.sh` (already applied —
  don't re-run); `kubectl get pods -o wide -n $NS`.
- **Evidence:** diagram matches running pods; each pod's `role` label visible
  (`kubectl get pods -n $NS --show-labels`); topology table matches
  `kubectl get nodes`.

## 2. Kode walkthrough + pembagian tugas (10 min)

- **Record:** each member presents the service(s) they own, one screen of code
  per service, ~1.5 min each: aggregator (polling, canonical mapping,
  leader election, audit API), store (sharding, replication, checksum),
  feed-ingestor (SSE, backpressure), fire-builder (province join), coastal-gateway
  (sensor pantai), client-api (auth scopes, analytics dashboard), consumers
  (dashboard, notifier, report), mocks (BMKG, PVMBG).
- **Script/endpoint:** no commands; show `services/<name>/` and
  `infra/helm/sixtributed/templates/` for the owned service.
- **Evidence:** every member is on camera or voice-over (Cakupan Wajib #4);
  keep the file tree visible so the grader can map code to person.

## 3. cURL/Postman dengan time_total (5 min)

- **Record:** terminal with `curl -w` timing for each endpoint, one after
  another, showing `time_total` per request.
- **Script/endpoint:**
  ```bash
  # token via auth-service (through ingress or port-forward)
  curl -sS -X POST http://localhost:8080/auth/token -H 'Content-Type: application/json' \
    -d '{"client_id":"media","client_secret":"media-secret"}' | jq -r .access_token

  curl -w '\ntime_total: %{time_total}s\n' -H "Authorization: Bearer $MEDIA_TOKEN" \
    http://localhost:8080/api/hazards
  curl -w '\ntime_total: %{time_total}s\n' -H "Authorization: Bearer $AUDIT_TOKEN" \
    http://localhost:8090/internal/audit/events?limit=5
  curl -w '\ntime_total: %{time_total}s\n' -H "Authorization: Bearer $SATELLITE_TOKEN" \
    <satellite-feed-endpoint>
  ```
- **Evidence:** latency numbers visible on screen per endpoint; mention expected
  magnitudes (mock delays: BMKG ~100 ms, PVMBG 500–3000 ms).

## 4. Dasbor analitik + metrik Grafana (5 min)

- **Record:** browser, both dashboards side by side while generating load.
- **Script/endpoint:**
  ```bash
  kubectl port-forward -n $NS svc/client-api 8080:8080     # http://localhost:8080/dashboard
  kubectl port-forward -n observability svc/kube-prometheus-stack-grafana 3000:80  # admin/admin
  k6 run scripts/load-test/sustained.js                    # generate traffic first
  ```
- **Evidence:** analytics dashboard aggregates per hazard type with counts that
  match the Audit API; Grafana shows latency, error rate, throughput panels;
  point out the "belum lengkap / partial" badge behavior is shown in section 6
  after the node kill.

## 5. psql / DBeaver pada data store (3 min)

- **Record:** psql in one store pod and (optionally) DBeaver connected through a
  port-forward, showing the same hazard rows the dashboard reported.
- **Script/endpoint:**
  ```bash
  kubectl exec -it store-0 -n $NS -c postgres -- psql -U postgres -d aggregator \
    -c "select count(*), source from hazard_events group by source;"
  kubectl port-forward -n $NS pod/store-0 5433:5432   # DBeaver → localhost:5433
  ```
- **Evidence:** row counts per shard; hash distribution visible across
  store-0/1/2; numbers consistent with the dashboard and Audit API.

## 6. Tester Agent logs + skenario (12 min)

- **Record:** split screen — `kubectl logs -n tester-agent -l app=tester-agent -f`
  on one side, your terminal on the other. Announce each P-number before
  triggering it.

  a. **Node kill + checksum (P1.4), ~4 min.** `kill_worker_node` (manual:
     power off node-b VM, wipe `/var/sixtributed/store-*` on it, reboot).
     While it is down: dashboard stays up with the partial/incomplete marker
     (`unavailable_units`), cluster re-replicates; after recovery run
     `audit_checksum $AUDIT_TOKEN` — checksums match pre-kill values.
  b. **Aggregator failover (P2.x), ~2 min.** Delete the leader pod
     (`kubectl delete pod <leader-aggregator-pod> -n $NS`); lease
     `aggregator-leader` moves to a standby, ingestion resumes; show leader
     change in logs/events (`kubectl get lease -n $NS`).
  c. **DLQ replay (P3.4), ~2 min.** Enable a PVMBG schema change to park
     messages, then `./scripts/replay.sh --all` with `AUDIT_TOKEN` exported;
     show parked count going to zero
     (`GET /internal/audit/unprocessed`).
  d. **Feed satelit live (P4), ~2 min.** Watch feed-ingestor ingest the live
     SSE stream; show lag metric climbing and dropping back to ≤60 s after the
     burst on the Grafana panel; confirm no dropped records.
  e. **Sensor pantai failure (P5), ~2 min.** Kill the sensor-pantai pod;
     coastal pipeline degrades gracefully, already-accepted events survive
     restart; sensor comes back and resumes.

- **Script/endpoint:** `scripts/scenarios.sh` helpers, `scripts/replay.sh`,
  `/internal/audit/*` with `Authorization: Bearer $AUDIT_TOKEN`.
- **Evidence:** Tester Agent log lines for each scenario (Cakupan Wajib #2),
  checksum equality before/after, `partial: true` badge on the dashboard,
  replay counter output.

## 7. Load test 10k VU + scaling + version swap (5 min)

- **Record:** run the distributed load test, then scale, then swap the version,
  narrating latency/error panels on Grafana the whole time.
- **Script/endpoint:**
  ```bash
  ./scripts/load-test/distributed.sh                      # 10000 VUs across k6 pods
  ./scripts/load-test/api.js                              # in-cluster k6 variant
  kubectl scale deploy aggregator -n $NS --replicas=5     # scale out mid-run
  # version swap (P2.7):
  kubectl set image deploy/aggregator aggregator=sixtributed/aggregator:m2-next -n $NS
  kubectl rollout status deploy/aggregator -n $NS
  ```
- **Evidence:** throughput and p95 latency at 10k VUs on screen; scaling
  changes the latency curve; zero-downtime swap visible in Grafana
  (no error spike during rollout).

## Timeline check

| Section | Minutes |
|---|---|
| 1. Arsitektur | 5 |
| 2. Kode + anggota | 10 |
| 3. cURL time_total | 5 |
| 4. Dasbor | 5 |
| 5. psql/DBeaver | 3 |
| 6. Skenario | 12 |
| 7. Load test | 5 |
| **Total** | **45** |
