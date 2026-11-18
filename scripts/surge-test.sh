#!/usr/bin/env bash
# P4.5: satellite feed surge (speed bump) test.
#
# Waits for a feed surge to end (latest_offset stops advancing), then polls the
# WILDFIRE HazardEvent count via the aggregator audit API every POLL_SECONDS
# until it stops growing for STABLE_POLLS consecutive polls. Caught-up time is
# measured from surge end; exits non-zero if not caught up within
# CAUGHT_UP_DEADLINE seconds.
#
# Usage:
#   scripts/surge-test.sh                # auto: try exam start, else document-only
#   TRIGGER=manual scripts/surge-test.sh # print instructions, wait for operator Enter
#
# Env:
#   SATELLITE_URL      (default https://satellite.naufarrel.dev)
#   SATELLITE_TOKEN    (required)
#   AUDIT_TOKEN        (required — aggregator /internal/audit)
#   AGGREGATOR_URL     (default http://aggregator:8090; port-forward URL enables direct curl)
#   NAMESPACE          (default sistem-informasi-kebencanaan-bnpb)
#   SPEED              (default 60)
#   POLL_SECONDS       (default 5)
#   CAUGHT_UP_DEADLINE (default 60)
#   STABLE_POLLS       (default 3)
#   SURGE_END_POLLS    (default 2 — consecutive polls without offset growth end the surge)
set -euo pipefail

SATELLITE_URL="${SATELLITE_URL:-https://satellite.naufarrel.dev}"
: "${SATELLITE_TOKEN:?SATELLITE_TOKEN env var required}"
: "${AUDIT_TOKEN:?AUDIT_TOKEN env var required}"
AGGREGATOR_URL="${AGGREGATOR_URL:-http://aggregator:8090}"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
SPEED="${SPEED:-60}"
POLL_SECONDS="${POLL_SECONDS:-5}"
CAUGHT_UP_DEADLINE="${CAUGHT_UP_DEADLINE:-60}"
STABLE_POLLS="${STABLE_POLLS:-3}"
SURGE_END_POLLS="${SURGE_END_POLLS:-2}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Aggregator audit API: ephemeral in-cluster curl pod unless AGGREGATOR_URL is
# host-reachable (port-forward).
agg_get() {
  local path=$1
  if [[ "$AGGREGATOR_URL" == http://localhost* || "$AGGREGATOR_URL" == http://127.0.0.1* ]]; then
    curl -sS -m 10 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGGREGATOR_URL$path"
  else
    kubectl run surge-curl --rm -i --restart=Never --image=curlimages/curl \
      -n "$NAMESPACE" </dev/null -- \
      -sS -m 10 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGGREGATOR_URL$path"
  fi
}

latest_offset() {
  curl -sS -m 10 -H "Authorization: Bearer $SATELLITE_TOKEN" "$SATELLITE_URL/v1/session" |
    jq -r '.latest_offset // 0'
}

# Fires (source_ref_id count) across all store units; "partial" while a unit is down.
fire_count() {
  agg_get "/internal/audit/checksum?source=SATELIT" |
    jq -r 'if .partial then "partial" else (.count | tostring) end'
}

trigger_surge() {
  log "attempting exam start (speed=$SPEED) at $SATELLITE_URL/v1/exam/start"
  local code
  code=$(curl -sS -m 10 -o /dev/null -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $SATELLITE_TOKEN" -H 'Content-Type: application/json' \
    -d "{\"speed\":$SPEED}" "$SATELLITE_URL/v1/exam/start" || echo 000)
  if [[ "$code" == 2* ]]; then
    log "exam started (speed=$SPEED)"
    return 0
  fi
  log "feed exposes no speed control from here (HTTP $code) — document-only mode"
  cat <<EOF
  The feed speed is controlled assistant-side (exam profile), the same way
  PANTAI_PROFILE controls the coastal sensor. Ask the assistant to start the
  exam window at speed $SPEED, or wait for the scheduled one. This script
  detects the surge via latest_offset growth and measures catch-up from the
  moment the surge ends (offset stops advancing).
EOF
  if [[ "${TRIGGER:-auto}" == manual && -t 0 ]]; then
    read -r -p "Press Enter once the surge is visibly under way..."
  fi
}

wait_surge_end() {
  local prev cur stable=0
  prev=$(latest_offset)
  log "surge running: latest_offset=$prev; waiting for it to end"
  while (( stable < SURGE_END_POLLS )); do
    sleep "$POLL_SECONDS"
    cur=$(latest_offset)
    if (( cur == prev )); then
      stable=$((stable + 1))
    else
      stable=0
      log "offset $prev -> $cur"
    fi
    prev=$cur
  done
  log "surge ended at offset=$prev"
}

wait_caught_up() {
  local t_end=$1 prev cur stable=0 elapsed
  prev=$(fire_count)
  log "polling WILDFIRE event count (start=$prev) every ${POLL_SECONDS}s"
  while :; do
    sleep "$POLL_SECONDS"
    elapsed=$(( $(date +%s) - t_end ))
    cur=$(fire_count)
    if [[ "$cur" == partial ]]; then
      log "store unit(s) unavailable; retrying"
      continue
    fi
    if (( cur == prev )); then
      stable=$((stable + 1))
    else
      stable=0
      log "count $prev -> $cur (elapsed ${elapsed}s)"
    fi
    prev=$cur
    if (( stable >= STABLE_POLLS )); then
      log "caught up: time-to-caught-up=${elapsed}s (deadline ${CAUGHT_UP_DEADLINE}s)"
      echo "time_to_caught_up_seconds=$elapsed"
      return 0
    fi
    if (( elapsed >= CAUGHT_UP_DEADLINE )); then
      log "NOT caught up within ${CAUGHT_UP_DEADLINE}s (count still $cur, stable $stable/$STABLE_POLLS)"
      return 1
    fi
  done
}

command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

log "session: $(curl -sS -m 10 -H "Authorization: Bearer $SATELLITE_TOKEN" \
  "$SATELLITE_URL/v1/session" | jq -c '{group, profile, speed, latest_offset}')"

trigger_surge
wait_surge_end
t_end=$(date +%s)

if wait_caught_up "$t_end"; then
  cat <<'EOF'
Latency panels (Grafana, Prometheus datasource; metrics on each service's /metrics):
  ingest backlog (lag):        fire_builder_lag_seconds            (fire-builder)
  p95 request duration:        histogram_quantile(0.95, sum(rate(http_server_request_duration_ms_bucket[5m])) by (le))
EOF
  exit 0
fi
exit 1
