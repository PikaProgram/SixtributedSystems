# Topology Declaration (Milestone 2)

Required by the M2 spec ("Deklarasi Topologi"). Fill in the placeholders with
your real environment before the demo.

## Nodes

| Node   | Type | Role              | Physical host | CPU | RAM  | IP              |
| ------ | ---- | ----------------- | ------------- | --- | ---- | --------------- |
| node-a | VM   | k3s server        | HOST_A (fill in, e.g. laptop / lab desktop) | 4 vCPU | 8 GiB | 192.168.56.10 |
| node-b | VM   | k3s agent         | HOST_B (fill in)                            | 4 vCPU | 8 GiB | 192.168.56.11 |

- Node count: **2**
- Node types: both are **VMs** (not native), so the Tester Agent can reboot /
  destroy a node to test role=store data loss.
- Physical hosts: node-a runs on HOST_A, node-b runs on HOST_B. If both VMs run
  on the same physical machine, state that explicitly.
- CPU/RAM per host: as listed above; adjust to match the actual machines.
- Kubernetes distribution: k3s (server on node-a, agent on node-b), Traefik
  ingress as shipped by k3s.
- Workloads: all application components run in namespace
  `sistem-informasi-kebencanaan-bnpb` with the required `role` labels
  (aggregator, broker, consumer, api, mock, other).
