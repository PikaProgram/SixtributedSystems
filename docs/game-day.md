# Game Day Playbook — P5.6 (Reliability of the Critical Coastal Alert Path)

Target: **99.5% of critical coastal observations are delivered in ≤ 5 s, over
any 10-minute window, during game day.**

## 1. Reliability target definition

| Item | Value |
| --- | --- |
| Scope | end-to-end critical coastal observation path: coastal capture → store → broker → aggregator → notifier → recipient |
| Success | notification for the observation exists (notifier) ≤ 5 s after the observation was captured |
| Window | any rolling 10-minute window during game day |
| Target | ≥ 99.5% of critical coastal observations in that window succeed |

Anything not counted as a failure: observations the Tester Agent deliberately
suppresses via chaos scenarios (node kill, VM freeze, clock skew inside its
allowed recovery window) — those are counted separately as "chaos-induced",
reported but not counted against the 99.5% figure.

## 2. How to measure it

Two independent counters, joined on observation id:

1. **Sent count — notifier**
   `kubectl exec -n sistem-informasi-kebencanaan-bnpb deploy/notifier -- wget -qO- localhost:8080/internal/notifications`
   (returns recent notification records with timestamps).

2. **Received count — coastal**
   `kubectl exec -n sistem-informasi-kebencanaan-bnpb deploy/api -- wget -qO- localhost:8080/internal/coastal/stats`
   (returns counts of coastal observations, including critical ones).

   > ⚠ The exact pod/selector names above are placeholders — confirm with
   > `kubectl get pods -n sistem-informasi-kebencanaan-bnpb -o wide` before game day
   > and record the real ones in the run sheet.

Compute for each 10-minute window (use the game-day spreadsheet or a script):

```
success_rate = |critical observations delivered ≤5s| / |critical observations|
```

3. **p95 latency from Grafana** (cross-check against the counters):
   - Port-forward Grafana: `kubectl port-forward -n observability svc/kube-prometheus-stack-grafana 3000:80`
   - Dashboard → Prometheus → query, p95 of capture→delivery duration metric
     exported by notifier/aggregator (metric name per service docs).
   - Record p95 every 5 minutes. p95 ≤ 5 s is the proxy signal between counter samples.

## 3. Recovery window — no-touch policy

- Every failure has a **recovery window**: after triggering a chaos scenario,
  hands off for **T_recovery minutes** (default: 5) before declaring the
  scenario failed or intervening manually.
- During the recovery window **nobody touches the cluster**: no restarts, no
  `kubectl delete pod`, no scaling changes. Let the system self-heal (or not).
- Only after the window expires may the scenario owner intervene, and the
  intervention itself is recorded in the run sheet.
- Reason: this measures actual resilience; premature intervention masks real
  weaknesses we need to report on.

## 4. Roles & escalation

| Role | Person | Responsibility |
| --- | --- | --- |
| Dashboard watcher | _fill in_ | Watches Grafana (Prometheus/Loki/Tempo) and records p95 + error rates every 5 min; never runs scenarios. |
| Scenario runner | _fill in_ | Executes scenarios (below) one at a time, announces each in the team channel, waits out the recovery window, records result. |
| Incident lead | _fill in_ | Only person allowed to intervene after recovery window; decides pass/fail per scenario; owns the run sheet. |
| Comms/scribe | _fill in_ | Timestamps everything, keeps run sheet updated, collects final numbers for the report. |

Escalation path: scenario runner → incident lead (any failure past recovery
window) → abort game day (reliability target unrecoverable: ≥ 3 consecutive
failed windows or total outage > 10 min).

## 5. Scenario checklist

Each Tester-Agent practice scenario (listed by `scenario_list` in
`scripts/scenarios.sh`, i.e. `kubectl exec -n tester-agent daemonset/tester-agent -- /app list`)
maps to a local helper function in `scripts/scenarios.sh`. Names marked
`[placeholder]` must be confirmed against `/app list` output on game-day
morning and corrected in this table.

| # | Tester-Agent scenario (`/app list`) [placeholder] | scripts/scenarios.sh function | Chaos target |
| --- | --- | --- | --- |
| 1 | practice — node loss | `kill_worker_node` | node-b VM poweroff + store data wipe |
| 2 | practice — leader freeze | `freeze_vm` | suspend node-a VM |
| 3 | practice — clock skew | `clock_skew node-b-host +45` | +45 s skew on node-b |
| 4 | practice — aggregator scale-down | `scale_aggregator 0` (then `scale_aggregator 2`) | aggregator replicas 0→2 |
| 5 | practice — aggregator rollout | `rollout_aggregator` | rolling version swap (P2.8) |
| 6 | practice — audit checksum | `audit_checksum <token>` | audit endpoint integrity check (P5.x) |
| 7 | practice — custom run | `scenario_run practice <name>` | any scenario by name |
| 8 | (logs follow-up) | `tester_logs` | inspect tester-agent output after each run |

Run order on game day: 6 → 7 (baseline, no chaos) → 4 → 5 → 1 → 2 → 3, with a
fresh 10-minute clean window between each chaos scenario.
