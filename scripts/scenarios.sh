#!/usr/bin/env bash
# Game-day / practice scenario helpers for Milestone 2 (P5.5, P2.4).
# Usage: source this file or run each function manually. All kubectl-scoped.

NS=sistem-informasi-kebencanaan-bnpb

scenario_list() {
  kubectl exec -n tester-agent daemonset/tester-agent -- /app list
}

scenario_run() {
  # e.g. scenario_run practice <name>
  kubectl exec -n tester-agent daemonset/tester-agent -- /app run "$@"
}

tester_logs() {
  kubectl logs -n tester-agent -l app=tester-agent --tail=100 -f
}

# P2.4: clock skew on a node (requires host access to the VM)
clock_skew() {
  local node_host=$1 delta=$2  # e.g. clock_skew node-b-host +45
  ssh "$node_host" "sudo date -s \"${delta} seconds\""
}

# P2.5: freeze the leader node's VM (manual step at demo; documented for checklist)
freeze_vm() { echo "MANUAL: suspend the VM of the node running the leader aggregator pod"; }

kill_worker_node() {
  # P1.4: kill node-b worker + wipe its store data
  echo "MANUAL: poweroff node-b VM, then wipe hostPath dir /var/lib/rancher/k3s/storage/* on it, reboot"
}

scale_aggregator() {
  kubectl scale deploy aggregator -n "$NS" --replicas="$1"
}

rollout_aggregator() {
  kubectl set image deploy/aggregator aggregator=sixtributed/aggregator:m2-next -n "$NS"
  kubectl rollout status deploy/aggregator -n "$NS"
}

audit_checksum() {
  local token=$1 source=${2:-}
  kubectl run curl --rm -i --restart=Never --image=curlimages/curl -n "$NS" -- \
    -s -H "Authorization: Bearer $token" "http://aggregator:8090/internal/audit/checksum?source=$source"
}

coastal_stats() {
  # P5.5 helper: breaker state + violation counters from coastal-gateway.
  # Run in-cluster (no auth on /internal/coastal/stats; service DNS only).
  kubectl run coastal-curl --rm -i --restart=Never --image=curlimages/curl -n "$NS" </dev/null -- \
    -s "http://coastal-gateway:8110/internal/coastal/stats"
}

# P5.5: sensor down — scale sensor-pantai to 0, watch breaker open + stale
# serving, then restore. $1 = outage seconds (default 60).
sensor_down() {
  local out_secs=${1:-60}
  echo "== before: $(coastal_stats)"
  kubectl scale deploy sensor-pantai -n "$NS" --replicas=0
  echo "sensor-pantai scaled to 0; outage ${out_secs}s — watch coastal-gateway logs"
  sleep "$out_secs"
  echo "== during: $(coastal_stats)"
  kubectl scale deploy sensor-pantai -n "$NS" --replicas=1
  kubectl rollout status deploy/sensor-pantai -n "$NS"
  echo "== after: $(coastal_stats)"
}

# P5.5: sensor slow/degraded. Two supported paths:
#   1. If the assistant feed exposes a slow profile, switch it with a patch:
#        sensor_slow slow
#   2. Otherwise simulate slowness by throttling the sensor container (pause +
#     cpu limit) — the breaker should trip on 30s-of-slow responses.
# Falls back to the tester-agent practice scenario when the name exists.
sensor_slow() {
  local profile=${1:-slow}
  if scenario_exists "$profile"; then
    scenario_run practice "$profile"
    return
  fi
  echo "no tester scenario '$profile'; patching sensor-pantai resources to force slow responses"
  kubectl patch deploy sensor-pantai -n "$NS" --type=strategic \
    -p '{"spec":{"template":{"spec":{"containers":[{"name":"sensor","resources":{"limits":{"cpu":"5m"}}}]}}}}'
  echo "watch breaker trip (30s slow window): $(coastal_stats)"
  echo "restore with: kubectl patch deploy sensor-pantai -n $NS --type=strategic -p '{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"sensor\",\"resources\":{\"limits\":{\"cpu\":\"500m\"}}}]}}}}'"
}

scenario_exists() {
  scenario_list 2>/dev/null | grep -qw "$1"
}

# P5.5: no request storm — poll coastal-gateway /internal/coastal/stats around
# a failure window and print the breaker state + violation counter diff, so the
# report can show the breaker stopped hammering a dead sensor.
sensor_storm_check() {
  local out_secs=${1:-60}
  local before during after
  before=$(coastal_stats)
  echo "== before: $before"
  sensor_down "$out_secs" >/dev/null
  during=$(coastal_stats)
  echo "== during: $during"
  after=$(coastal_stats)
  echo "== after: $after"
  echo "violations diff (before -> after):"
  diff <(jq -S '.violations' <<<"$before") <(jq -S '.violations' <<<"$after") || true
  echo "breaker states: before=$(jq -r .breaker <<<"$before") during=$(jq -r .breaker <<<"$during") after=$(jq -r .breaker <<<"$after")"
}

"$@"
