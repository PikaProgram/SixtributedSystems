#!/usr/bin/env bash
# Replay failed (parked/retrying) aggregator messages after a schema fix
# (Problem 3.4). Uses the audit token like the audit endpoints.
#
#   scripts/replay.sh --all
#   scripts/replay.sh --all --source PVMBG
#   scripts/replay.sh --ids 11111111-2222-3333-4444-555555555555,aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
#
# Env: AUDIT_TOKEN (required), AGGREGATOR_URL (default http://localhost:8090).
set -euo pipefail

AGGREGATOR_URL="${AGGREGATOR_URL:-http://localhost:8090}"
: "${AUDIT_TOKEN:?AUDIT_TOKEN env var required}"

body="{}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      if [[ "$body" == "{}" || "$body" == *'"all":false'* ]]; then
        body='{"all":true}'
      elif [[ "$body" == *'"ids"'* ]]; then
        echo "error: --all cannot be combined with --ids" >&2; exit 2
      else
        body="${body/%\}/, \"all\":true}"
      fi
      shift
      ;;
    --source)
      src="$2"; shift 2
      if [[ "$body" == "{}" ]]; then
        body="{\"source\":\"$src\"}"
      else
        body="${body/%\}/, \"source\":\"$src\"}"
      fi
      ;;
    --ids)
      # comma-separated message ids -> JSON array
      IFS=',' read -ra idarr <<<"$2"; shift 2
      if [[ "$body" == *'"all":true'* ]]; then
        echo "error: --ids cannot be combined with --all" >&2; exit 2
      fi
      json="["
      for i in "${!idarr[@]}"; do
        [[ $i -gt 0 ]] && json+=","
        json+="\"${idarr[$i]}\""
      done
      json+="]"
      body="{\"ids\":$json}"
      ;;
    *)
      echo "usage: replay.sh [--all] [--source PVMBG] [--ids id1,id2]" >&2; exit 2;;
  esac
done

if [[ "$body" == *'"all":true'* && "$body" != *'"ids"'* ]]; then
  : # all(+source) is fine as-is
elif [[ "$body" != *'"ids"'* && "$body" == *'"source"'* ]]; then
  echo "error: --source requires --all (use --ids for explicit messages)" >&2; exit 2
fi

echo "POST $AGGREGATOR_URL/internal/admin/replay  body=$body"
curl -sS -X POST "$AGGREGATOR_URL/internal/admin/replay" \
  -H "Authorization: Bearer $AUDIT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$body"
echo
echo "GET $AGGREGATOR_URL/internal/admin/replay/stats"
curl -sS "$AGGREGATOR_URL/internal/admin/replay/stats" \
  -H "Authorization: Bearer $AUDIT_TOKEN"
echo
