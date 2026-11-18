#!/usr/bin/env bash
# P4.6: dump WILDFIRE HazardEvents via the aggregator audit API and compare
# against the assistant's reference CSV (crash-recovery equivalence, criteria
# 5 and 6 of Problem 4).
#
# Usage:
#   scripts/compare-reference.sh <reference.csv>            # dump + compare
#   scripts/compare-reference.sh --recovery <reference.csv> # kill fire-builder mid-stream,
#                                                           # restart after 10s, dump + compare
#   scripts/compare-reference.sh --dump-only [out.csv]      # just dump our output
#
# CSV columns: source_ref_id, detection_keys, attributes (JSON strings,
# jq @csv quoting). Row identity = source_ref_id (first field, no comma).
# Exit 0 iff no missing/extra/mismatched rows.
#
# For --recovery: start the dump scenario with the feed actively flowing, then
# run this script — it kills fire-builder mid-stream, restarts it after
# RECOVERY_WAIT seconds, lets it drain the backlog, dumps the recovered output
# and compares. Result must equal the clean run exactly (no lost, no duplicate
# fires).
#
# Env: AUDIT_TOKEN (required), AGGREGATOR_URL (default http://aggregator:8090),
#      NAMESPACE (default sistem-informasi-kebencanaan-bnpb),
#      RECOVERY_WAIT (default 10), PAGE_LIMIT (default 500)
set -euo pipefail

: "${AUDIT_TOKEN:?AUDIT_TOKEN env var required}"
AGGREGATOR_URL="${AGGREGATOR_URL:-http://aggregator:8090}"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
RECOVERY_WAIT="${RECOVERY_WAIT:-10}"
PAGE_LIMIT="${PAGE_LIMIT:-500}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

agg_get() {
  local path=$1
  if [[ "$AGGREGATOR_URL" == http://localhost* || "$AGGREGATOR_URL" == http://127.0.0.1* ]]; then
    curl -sS -m 30 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGGREGATOR_URL$path"
  else
    kubectl run compare-curl --rm -i --restart=Never --image=curlimages/curl \
      -n "$NAMESPACE" </dev/null -- \
      -sS -m 30 -H "Authorization: Bearer $AUDIT_TOKEN" "$AGGREGATOR_URL$path"
  fi
}

# Follow next_cursor pagination until exhausted; emit CSV rows.
dump_events() {
  local out=$1 cursor="" page path
  : > "$out"
  while :; do
    path="/internal/audit/events?source=SATELIT&limit=$PAGE_LIMIT${cursor:+&cursor=$cursor}"
    page=$(agg_get "$path")
    if jq -e '.partial' <<<"$page" >/dev/null; then
      log "warning: some store units unavailable; page may be incomplete"
    fi
    jq -r '.items[] | [
      .source_ref_id,
      (.attributes.detection_keys // [] | tojson),
      (.attributes // {} | tojson)
    ] | @csv' <<<"$page" >> "$out"
    cursor=$(jq -r '.next_cursor // empty' <<<"$page")
    [[ -z "$cursor" ]] && break
    log "fetched next page (cursor ${cursor:0:12}...)"
  done
  log "dumped $(wc -l < "$out") rows to $out"
}

# missing  = rows in the reference but not in our dump
# extra    = rows in our dump but not in the reference
# mismatch = same source_ref_id, different detection_keys/attributes
compare_csv() {
  local ref=$1 got=$2
  awk -F',' '
    NR==FNR { ref[$1]=$0; next }
    {
      if ($1 in ref) { if (ref[$1] != $0) mismatched++; delete ref[$1] }
      else extra++
    }
    END {
      for (k in ref) missing++
      printf "missing=%d extra=%d mismatched=%d\n", missing, extra, mismatched
      exit (missing == 0 && extra == 0 && mismatched == 0) ? 0 : 1
    }' "$ref" "$got"
}

run_recovery() {
  log "killing fire-builder mid-stream (feed must be actively flowing)"
  kubectl scale deploy fire-builder -n "$NAMESPACE" --replicas=0
  log "fire-builder down; restarting in ${RECOVERY_WAIT}s"
  sleep "$RECOVERY_WAIT"
  kubectl scale deploy fire-builder -n "$NAMESPACE" --replicas=1
  kubectl rollout status deploy/fire-builder -n "$NAMESPACE" --timeout=180s
  log "fire-builder recovered; letting it drain the backlog before dumping"
}

command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

mode=compare
[[ "${1:-}" == "--recovery" ]] && { mode=recovery; shift; }
[[ "${1:-}" == "--dump-only" ]] && { mode=dump; shift; }

case "$mode" in
  dump)
    out="${1:-wildfire-events-$(date +%Y%m%d-%H%M%S).csv}"
    dump_events "$out"
    ;;
  compare | recovery)
    ref="${1:?usage: compare-reference.sh [--recovery|--dump-only] <reference.csv>}"
    [[ -f "$ref" ]] || { echo "reference CSV not found: $ref" >&2; exit 2; }
    [[ "$mode" == recovery ]] && run_recovery
    got=$(mktemp)
    trap 'rm -f "$got"' EXIT
    dump_events "$got"
    echo "comparing with reference $ref"
    compare_csv "$ref" "$got" && log "IDENTICAL — recovery equivalence holds" ||
      { log "DIFFERENCES found (see counts above)"; exit 1; }
    ;;
esac
