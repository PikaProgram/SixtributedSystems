#!/usr/bin/env bash
# Install the observability stack into the k3s cluster (namespace: observability):
#   kube-prometheus-stack  (prometheus-community)  — Prometheus + Grafana + Alertmanager + exporters
#   loki-stack             (grafana)               — Loki single-binary + promtail
#   tempo                  (grafana)               — trace backend
# Idempotent: helm upgrade --install is safe to re-run. Requires kubectl + helm.
set -euo pipefail

NS=observability
PROM_CHART_VERSION=62.7.0   # kube-prometheus-stack, pin for reproducibility

step() { echo "==> $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl not found"
command -v helm >/dev/null || die "helm not found (install: https://helm.sh/docs/intro/install/)"

step "Adding helm repos"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

step "Creating namespace $NS"
kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

step "Installing kube-prometheus-stack (Prometheus, Grafana, Alertmanager)"
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace "$NS" \
  --version "$PROM_CHART_VERSION" \
  --set grafana.adminPassword=admin \
  --set prometheus.prometheusSpec.retention=24h

step "Installing loki-stack (Loki single-binary + promtail)"
helm upgrade --install loki-stack grafana/loki-stack \
  --namespace "$NS" \
  --set loki.isDefault=false \
  --set promtail.enabled=true \
  --set loki.persistence.enabled=false

step "Installing tempo"
helm upgrade --install tempo grafana/tempo \
  --namespace "$NS" \
  --set persistence.enabled=false \
  --set tempo.retention=24h

step "Waiting for pods"
kubectl rollout status -n "$NS" deployment/kube-prometheus-stack-grafana --timeout=300s || true
kubectl get pods -n "$NS"

cat <<'EOF'

==> Observability stack installed.
==> Grafana port-forward (run in a separate terminal, leave open):
      kubectl port-forward -n observability svc/kube-prometheus-stack-grafana 3000:80
    Then open http://localhost:3000 — user: admin, password: admin
    Datasources: Prometheus (kube-prometheus-stack, default), Loki (add URL
    http://loki-stack.observability.svc.cluster.local:3100), Tempo (add URL
    http://tempo.observability.svc.cluster.local:3100).
EOF
