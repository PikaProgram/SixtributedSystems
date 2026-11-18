#!/usr/bin/env bash
# Distributed k6 runner for the aggregator client-api (P2.6 scale test, P2.7 throughput compare).
#
# Strategy: k6 runs in-cluster as one Indexed Job with N parallel pods (default 3),
# each pod an independent k6 process with VUS/N virtual users against the same
# stages shape. Per-pod JSON summaries are pulled from pod logs and aggregated.
#
# Usage:
#   ./distributed.sh                    # P2.6: scale 3 -> 5 -> 2 mid-run
#   ./distributed.sh --throughput-compare  # P2.7: same load at 1 vs 3 replicas, print ratio
#
# Parameters (env):
#   NAMESPACE, AGGREGATOR_DEPLOY, TARGET_URL, TOKEN_MEDIA, TOKEN_FIELD,
#   VUS (default 10000), DURATION (seconds, default 120), SEGMENTS (default 3),
#   K6_IMAGE (default grafana/k6:latest)
set -euo pipefail

cd "$(dirname "$0")"

NAMESPACE=${NAMESPACE:-default}
AGGREGATOR_DEPLOY=${AGGREGATOR_DEPLOY:-aggregator}
TARGET_URL=${TARGET_URL:-http://aggregator}
VUS=${VUS:-10000}
DURATION=${DURATION:-120}   # seconds; api.js appends 's'
SEGMENTS=${SEGMENTS:-3}
K6_IMAGE=${K6_IMAGE:-grafana/k6:latest}
NS=(-n "$NAMESPACE")

MODE=p26
[[ "${1:-}" == "--throughput-compare" ]] && MODE=p27

TS=$(date +%Y%m%d-%H%M%S)
RESULTS="results/$TS"
mkdir -p "$RESULTS"
MARKER="__K6_SUMMARY_JSON__"

log() { echo "[$(date +%H:%M:%S)] $*"; }

current_replicas() { kubectl "${NS[@]}" get deploy "$AGGREGATOR_DEPLOY" -o jsonpath='{.spec.replicas}'; }

install_script_config() {
  kubectl "${NS[@]}" create configmap k6-api-script --from-file=api.js \
    --dry-run=client -o yaml | kubectl "${NS[@]}" apply -f -
}

# Launch one k6 Job with SEGMENTS parallel pods; wait for completion; pull summaries.
run_k6() {
  local vus_per=$(( (VUS + SEGMENTS - 1) / SEGMENTS ))
  kubectl "${NS[@]}" delete job k6-api --ignore-not-found --wait=true
  kubectl "${NS[@]}" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: k6-api
  namespace: $NAMESPACE
spec:
  parallelism: $SEGMENTS
  completions: $SEGMENTS
  backoffLimit: 0
  template:
    metadata:
      labels:
        job-name: k6-api
    spec:
      restartPolicy: Never
      # spread pods across nodes (nodeSelector cannot spread; affinity can)
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  job-name: k6-api
              topologyKey: kubernetes.io/hostname
      containers:
        - name: k6
          image: $K6_IMAGE
          command: ["sh", "-c", "k6 run --summary-export=/tmp/summary.json /scripts/api.js; echo $MARKER; cat /tmp/summary.json"]
          env:
            - name: PUBLIC_URL
              value: $TARGET_URL
            - name: VUS
              value: "$vus_per"
            - name: DURATION
              value: "${DURATION}s"
            - name: TOKEN_MEDIA
              value: "$TOKEN_MEDIA"
            - name: TOKEN_FIELD
              value: "$TOKEN_FIELD"
          resources:
            requests: { cpu: "2", memory: "1Gi" }
            limits: { cpu: "4", memory: "2Gi" }
          volumeMounts:
            - name: script
              mountPath: /scripts
      volumes:
        - name: script
          configMap:
            name: k6-api-script
EOF

  local timeout=$(( DURATION + 90 + 300 ))
  kubectl "${NS[@]}" wait --for=condition=complete job/k6-api "--timeout=${timeout}s" \
    || log "WARN: job did not reach Complete within ${timeout}s (continuing, collecting what exists)"

  kubectl "${NS[@]}" get events --sort-by=.lastTimestamp > "$RESULTS/events-$(date +%H%M%S).txt" 2>/dev/null || true
  mkdir -p "$RESULTS/logs" "$RESULTS/summaries"
  local i=0
  for pod in $(kubectl "${NS[@]}" get pods -l job-name=k6-api -o jsonpath='{.items[*].metadata.name}'); do
    kubectl "${NS[@]}" logs "$pod" > "$RESULTS/logs/$pod.log" 2>&1 || true
    # JSON blob starts after the marker line
    sed -n "/$MARKER/,\$p" "$RESULTS/logs/$pod.log" | tail -n +2 > "$RESULTS/summaries/pod-$i.json" || true
    [[ -s "$RESULTS/summaries/pod-$i.json" ]] || log "WARN: no summary JSON from pod $pod"
    i=$(( i + 1 ))
  done
  ls "$RESULTS"/summaries/pod-*.json 2>/dev/null | wc -l
}

# Aggregate per-pod k6 summary exports: prints one JSON line.
aggregate() {
  local files
  files=$(ls "$RESULTS"/summaries/pod-*.json 2>/dev/null) || true
  [[ -n "$files" ]] || { echo '{"error":"no summaries"}'; return; }
  jq -s '{
    total_reqs:        ([.[].metrics.http_reqs.values.count // 0] | add),
    duration_s:        ([.[].state.testRunDurationMs // 0] | max / 1000),
    non_controlled:    (([.[] | (.metrics.non_controlled_errors.values.rate // 0) * (.metrics.http_reqs.values.count // 0)] | add)
                        / ([.[].metrics.http_reqs.values.count // 0] | add // 1)),
    controlled_429:    ([.[].metrics.controlled_429.values.count // 0] | add),
    p95_ms:            ([.[].metrics.http_req_duration.values."p(95)" // 0] | max)
  }' $files
}

# P2.6: start load, +30s scale to 5, +30s scale to 2, ride out, collect.
run_p26() {
  local orig
  orig=$(current_replicas 2>/dev/null || echo 3)
  log "P2.6 sequence: baseline replicas=$orig, $SEGMENTS k6 pods x $(( (VUS + SEGMENTS - 1) / SEGMENTS )) VUs"

  install_script_config
  run_k6 &
  local k6_pid=$!
  sleep 30
  log "scaling $AGGREGATOR_DEPLOY -> 5"
  kubectl "${NS[@]}" scale deploy "$AGGREGATOR_DEPLOY" --replicas=5
  sleep 30
  log "scaling $AGGREGATOR_DEPLOY -> 2"
  kubectl "${NS[@]}" scale deploy "$AGGREGATOR_DEPLOY" --replicas=2
  wait "$k6_pid"
  kubectl "${NS[@]}" scale deploy "$AGGREGATOR_DEPLOY" --replicas="$orig"

  local agg
  agg=$(aggregate)
  echo "$agg" | jq . > "$RESULTS/aggregated.json"
  log "metrics: $agg"

  local rate
  rate=$(echo "$agg" | jq '.non_controlled // 1')
  if jq -e '.non_controlled < 0.01' >/dev/null <<< "$agg"; then
    echo "VERDICT P2.6: PASS — non-controlled error rate $(echo "$agg" | jq '.non_controlled') < 1% (controlled 429s: $(echo "$agg" | jq '.controlled_429'))"
  else
    echo "VERDICT P2.6: FAIL — non-controlled error rate $rate >= 1%"
  fi
}

# P2.7: identical load against 1 replica, then 3; print throughput ratio.
run_p27() {
  local orig
  orig=$(current_replicas 2>/dev/null || echo 3)
  install_script_config

  for replicas in 1 3; do
    log "throughput run: $AGGREGATOR_DEPLOY=$replicas replica(s)"
    kubectl "${NS[@]}" scale deploy "$AGGREGATOR_DEPLOY" --replicas="$replicas"
    sleep 10   # let the deployment settle before load
    run_k6
    mv "$RESULTS/summaries" "$RESULTS/summaries-$replicas"
    mkdir -p "$RESULTS/summaries"
  done
  kubectl "${NS[@]}" scale deploy "$AGGREGATOR_DEPLOY" --replicas="$orig"

  local t1 t3 ratio
  t1=$(jq -s '{reqs: ([.[].metrics.http_reqs.values.count // 0] | add), dur: ([.[].state.testRunDurationMs // 0] | max / 1000)}' "$RESULTS"/summaries-1/pod-*.json)
  t3=$(jq -s '{reqs: ([.[].metrics.http_reqs.values.count // 0] | add), dur: ([.[].state.testRunDurationMs // 0] | max / 1000)}' "$RESULTS"/summaries-3/pod-*.json)
  ratio=$(jq -n --argjson a "$t1" --argjson b "$t3" '($b.reqs / $b.dur) / ($a.reqs / $a.dur)' 2>/dev/null || echo 0)
  echo "{\"single\":$t1,\"three\":$t3,\"ratio\":$ratio}" > "$RESULTS/throughput-compare.json"
  log "1 replica: $t1 | 3 replicas: $t3 | ratio: $ratio"

  if jq -e '. >= 2' <<< "$ratio" >/dev/null; then
    echo "VERDICT P2.7: PASS — throughput ratio $ratio >= 2.0"
  else
    echo "VERDICT P2.7: FAIL — throughput ratio $ratio < 2.0"
  fi
}

if [[ "$MODE" == "p27" ]]; then run_p27; else run_p26; fi
echo "results in $RESULTS/"
