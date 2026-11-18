#!/usr/bin/env bash
# Provision two k3s VMs locally (WSL2 + QEMU/KVM) and bootstrap the cluster:
#   node-a: k3s server  (ssh forwarded to 127.0.0.1:2221)
#   node-b: k3s agent   (ssh forwarded to 127.0.0.1:2222)
# Then copy node-a's kubeconfig to ~/.kube/config with the server IP rewritten.
#
# Idempotent: re-running skips finished steps. No sudo is executed inside this
# script; when apt is needed it prints the exact command for you to run.
set -euo pipefail

VMS_DIR="$PWD/.vms"
IMG="$VMS_DIR/noble-server-cloudimg-amd64.img"
IMG_URL="https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
NODE_A_FWD="2221"
NODE_B_FWD="2222"
CPU=2
RAM_MB=2560        # 2.5 GB
DISK_GB=20
SEED_A="$VMS_DIR/seed-a"
SEED_B="$VMS_DIR/seed-b"

step() { echo "==> $*"; }

die() { echo "ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------- preflight
step "Preflight"
[ -e /dev/kvm ] || die "/dev/kvm not found. Nested virtualization/KVM is unavailable on this machine; this script cannot proceed (do not run under software emulation)."
egrep -q '(vmx|svm)' /proc/cpuinfo || die "CPU virtualization extensions not visible in /proc/cpuinfo; nested KVM unavailable."
grep -qi wsl /proc/version || echo "WARN: not running under WSL (continuing anyway, expected WSL2)."

TOTAL_KB=$(awk '/MemTotal/{print $2}' /proc/meminfo)
[ "$TOTAL_KB" -ge 5500000 ] || die "Need ~5.5GB free RAM for 2 VMs + k3s on 8GB host; MemTotal=${TOTAL_KB}kB."

if ! command -v qemu-system-x86_64 >/dev/null 2>&1; then
  echo
  echo "qemu-system-x86_64 is missing. Install it (needs sudo, run this yourself):"
  echo "    sudo apt-get update && sudo apt-get install -y qemu-system-x86 qemu-utils cloud-image-utils"
  echo "then re-run this script."
  exit 1
fi
step "qemu-system-x86_64: $(qemu-system-x86_64 --version | head -1)"

mkdir -p "$VMS_DIR"

# ---------------------------------------------------------------- image
if [ -f "$IMG" ]; then
  step "Cloud image already downloaded: $IMG"
else
  step "Downloading Ubuntu 24.04 cloud image"
  curl -fL --retry 3 -o "$IMG" "$IMG_URL"
fi

# ---------------------------------------------------------------- disk images
for n in a b; do
  disk="$VMS_DIR/node-$n.qcow2"
  if [ -f "$disk" ]; then
    step "node-$n disk already exists: $disk"
  else
    step "Creating node-$n disk (${DISK_GB}G, backing on cloud image)"
    qemu-img create -f qcow2 -b "$IMG" -F qcow2 "$disk" "${DISK_GB}G"
  fi
done

# ---------------------------------------------------------------- cloud-init
make_seed() { # $1=seed dir $2=hostname
  local seed=$1 host=$2
  if [ -f "$seed/user-data" ] && [ -f "$seed/meta-data" ]; then
    step "cloud-init seed for $host already present"
    return
  fi
  step "Writing cloud-init seed for $host ($ip)"
  mkdir -p "$seed"
  cat > "$seed/meta-data" <<EOF
instance-id: $host
local-hostname: $host
EOF
  cat > "$seed/user-data" <<EOF
#cloud-config
hostname: $host
manage_etc_hosts: true
users:
  - name: ubuntu
    plain_text_passwd: ubuntu
    lock_passwd: false
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    groups: [adm, sudo]
ssh_pwauth: true
chpasswd:
  expire: false
  users:
    - name: ubuntu
      password: ubuntu
packages: [curl]
runcmd:
  - sed -i 's/^#\?Port .*/Port 22/' /etc/ssh/sshd_config
  - systemctl restart ssh || systemctl restart sshd
EOF
  cloud-localds "$seed/seed.iso" "$seed/user-data" "$seed/meta-data" \
    || die "cloud-localds failed (install cloud-image-utils)"
}
make_seed "$SEED_A" "node-a"
make_seed "$SEED_B" "node-b"

# ---------------------------------------------------------------- VM launch
vm_running() { # $1=pidfile
  [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null
}

start_vm() { # $1=a|b $2=ssh_fwd_port $3=seeddir
  local name="node-$1" fwd=$2 seed=$3
  local pidf="$VMS_DIR/$name.pid"
  if vm_running "$pidf"; then
    step "$name already running (pid $(cat "$pidf"))"
  else
    step "Starting $name ($CPU vCPU, ${RAM_MB}MB, ssh->127.0.0.1:$fwd)"
    qemu-system-x86_64 \
      -enable-kvm -cpu host -smp "$CPU" -m "$RAM_MB" \
      -drive "file=$VMS_DIR/$name.qcow2,format=qcow2,if=virtio" \
      -drive "file=$seed/seed.iso,format=raw,if=virtio" \
      -netdev "user,id=net0,hostfwd=tcp::${fwd}-:22" \
      -device virtio-net-pci,netdev=net0 \
      -display none -daemonize -pidfile "$pidf" \
      -serial "file:$VMS_DIR/$name-console.log"
  fi
}
# NOTE: qemu user-mode networking is per-VM isolated; inside the VM each node
# gets 10.0.2.15. node-b reaches node-a's k3s API at that same in-VM address
# (K3S_URL below), not via the host-forwarded ports (those are ssh only).
start_vm a "$NODE_A_FWD" "$SEED_A"
start_vm b "$NODE_B_FWD" "$SEED_B"

ssh_run() { # $1=port $2=command...
  local port=$1; shift
  sshpass -p ubuntu ssh -p "$port" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=10 ubuntu@127.0.0.1 "$@"
}

step "Waiting for SSH (this can take a few minutes on first boot)"
for port in "$NODE_A_FWD" "$NODE_B_FWD"; do
  for i in $(seq 1 60); do
    if command -v sshpass >/dev/null 2>&1 && ssh_run "$port" true 2>/dev/null; then
      step "ssh up on 127.0.0.1:$port"; break
    fi
    sleep 10
  done
  ssh_run "$port" true 2>/dev/null || die "SSH on port $port never came up; check $VMS_DIR/node-*-console.log"
done

# ---------------------------------------------------------------- k3s install
NODE_A_ADDR="10.0.2.15"   # node-b reaches node-a over qemu user-net

k3s_installed() { # $1=port
  ssh_run "$1" 'sudo systemctl is-active k3s 2>/dev/null || sudo systemctl is-active k3s-agent 2>/dev/null' 2>/dev/null | grep -q active
}

if k3s_installed "$NODE_A_FWD"; then
  step "node-a: k3s already running, skipping install"
else
  step "node-a: installing k3s server"
  ssh_run "$NODE_A_FWD" "curl -sfL https://get.k3s.io | sudo sh -s - server \
    --write-kubeconfig-mode 644 \
    --node-external-ip $NODE_A_ADDR \
    --node-ip $NODE_A_ADDR --flannel-iface eth0 --bind-address $NODE_A_ADDR"
fi
step "node-a: $(ssh_run "$NODE_A_FWD" 'sudo kubectl get nodes -o wide 2>/dev/null || kubectl get nodes -o wide')"

if k3s_installed "$NODE_B_FWD"; then
  step "node-b: k3s-agent already running, skipping install"
else
  step "node-b: fetching node token from node-a and installing k3s agent"
  TOKEN=$(ssh_run "$NODE_A_FWD" "sudo cat /var/lib/rancher/k3s/server/node-token") \
    || die "could not read node token from node-a"
  ssh_run "$NODE_B_FWD" "curl -sfL https://get.k3s.io | sudo K3S_URL=https://$NODE_A_ADDR:6443 sh -s - agent --token '$TOKEN'"
fi

# ---------------------------------------------------------------- kubeconfig
step "Fetching kubeconfig from node-a"
ssh_run "$NODE_A_FWD" "sudo cat /etc/rancher/k3s/k3s.yaml" > "$VMS_DIR/kubeconfig.raw"
mkdir -p "$HOME/.kube"
sed "s#https://127.0.0.1:6443#https://$NODE_A_ADDR:6443#" "$VMS_DIR/kubeconfig.raw" > "$HOME/.kube/config"
chmod 600 "$HOME/.kube/config"
step "Wrote ~/.kube/config (server https://$NODE_A_ADDR:6443)"

step "Done. Next: kubectl get nodes   (from WSL, k3s API is inside the VMs)"
