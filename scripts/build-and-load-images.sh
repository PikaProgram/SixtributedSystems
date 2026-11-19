#!/usr/bin/env bash
# Build every service image and load it into each k3s node's containerd.
#
# Env:
#   K3S_NODES  space-separated ssh destinations of k3s nodes (default "vagrant@192.168.56.10 vagrant@192.168.56.11")
#   KIND_CLUSTER  if set, load images into this kind cluster instead of k3s nodes (K3S_NODES ignored)
#   TAG        image tag (default m2)

set -euo pipefail

KIND_CLUSTER="${KIND_CLUSTER:-}"
K3S_NODES="${K3S_NODES:-vagrant@192.168.56.10 vagrant@192.168.56.11}"
TAG="${TAG:-m2}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

for dockerfile in "$ROOT"/services/*/Dockerfile; do
  svc="$(basename "$(dirname "$dockerfile")")"
  image="sixtributed/${svc}:${TAG}"
  echo "==> [${svc}] docker build -t ${image}"
  docker build -t "$image" -f "$dockerfile" "$ROOT"
  if [[ -n "$KIND_CLUSTER" ]]; then
    echo "==> [${svc}] kind load ${image} -> ${KIND_CLUSTER}"
    kind load docker-image "$image" --name "$KIND_CLUSTER"
  else
    for node in $K3S_NODES; do
      echo "==> [${svc}] load ${image} -> ${node}"
      docker save "$image" | ssh "$node" "sudo k3s ctr images import -"
    done
  fi
  echo "==> [${svc}] done"
done

if [[ -n "$KIND_CLUSTER" ]]; then
  echo "All images built and loaded into kind cluster: ${KIND_CLUSTER}"
else
  echo "All images built and loaded on: ${K3S_NODES}"
fi
