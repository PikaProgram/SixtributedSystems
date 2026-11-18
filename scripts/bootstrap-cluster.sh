#!/usr/bin/env bash
# Idempotent bootstrap of the 2-node k3s cluster and the M2 deployment.
#
# Documents (does not create) the two VMs, installs k3s server + agent, then
# deploys the sixtributed Helm chart.
#
# Env:
#   NODE_A_IP  k3s server node IP   (default 192.168.56.10)
#   NODE_B_IP  k3s agent node IP    (default 192.168.56.11)
#   SSH_USER   ssh user on both VMs (default vagrant)

set -euo pipefail

NODE_A_IP="${NODE_A_IP:-192.168.56.10}"
NODE_B_IP="${NODE_B_IP:-192.168.56.11}"
SSH_USER="${SSH_USER:-vagrant}"
NS="sistem-informasi-kebencanaan-bnpb"
SSH_A="ssh ${SSH_USER}@${NODE_A_IP}"
SSH_B="ssh ${SSH_USER}@${NODE_B_IP}"

echo "==> Step 0: VMs (documented, not created here)"
cat <<EOF
    node-a: VM, server,   ${NODE_A_IP}, hosted on HOST_A (see docs/topology.md)
    node-b: VM, agent,    ${NODE_B_IP}, hosted on HOST_B (see docs/topology.md)
Create these VMs with your VM provider (e.g. Multipass/VirtualBox) before running.
EOF

echo "==> Step 1: install k3s server on node-a (${NODE_A_IP})"
${SSH_A} "curl -sfL https://get.k3s.io | sh -s - server --node-ip ${NODE_A_IP} --write-kubeconfig-mode 644"

echo "==> Step 2: fetch node token from node-a"
TOKEN="$(${SSH_A} "sudo cat /var/lib/rancher/k3s/server/node-token")"
echo "    token: ${TOKEN:0:8}... (truncated)"

echo "==> Step 3: join node-b (${NODE_B_IP}) as agent"
${SSH_B} "curl -sfL https://get.k3s.io | K3S_URL=https://${NODE_A_IP}:6443 K3S_TOKEN=${TOKEN} sh -s - agent --node-ip ${NODE_B_IP}"

echo "==> Step 4: namespace + deploy"
kubectl get namespace "$NS" >/dev/null 2>&1 && echo "    namespace ${NS} exists" \
  || { echo "    creating namespace ${NS}"; kubectl create namespace "$NS"; }
echo "    helm upgrade --install sixtributed ./infra/helm/sixtributed -f infra/helm/values-prod.yaml -n ${NS}"
helm upgrade --install sixtributed ./infra/helm/sixtributed \
  -f infra/helm/values-prod.yaml \
  --namespace "$NS" \
  --create-namespace

echo "==> Done. Verify with: kubectl get pods -n ${NS}"
