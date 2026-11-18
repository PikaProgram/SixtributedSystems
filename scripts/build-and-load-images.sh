#!/usr/bin/env bash
# Build every service image and load it into each k3s node's containerd.
#
# Env:
#   K3S_NODES  space-separated ssh destinations of k3s nodes (default "vagrant@192.168.56.10 vagrant@192.168.56.11")
#   TAG        image tag (default m2)

set -euo pipefail

K3S_NODES="${K3S_NODES:-vagrant@192.168.56.10 vagrant@192.168.56.11}"
TAG="${TAG:-m2}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

for dockerfile in "$ROOT"/services/*/Dockerfile; do
  svc="$(basename "$(dirname "$dockerfile")")"
  image="sixtributed/${svc}:${TAG}"
  echo "==> [${svc}] docker build -t ${image}"
  docker build -t "$image" -f "$dockerfile" "$ROOT"
  for node in $K3S_NODES; do
    echo "==> [${svc}] load ${image} -> ${node}"
    docker save "$image" | ssh "$node" "sudo k3s ctr images import -"
  done
  echo "==> [${svc}] done"
done

echo "All images built and loaded on: ${K3S_NODES}"
