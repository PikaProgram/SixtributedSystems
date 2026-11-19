package main

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/astolfo/sixtributedsystems/internal/vnode"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const storePort = "8100"

// unit is one store pod (primary or standby of a shard).
type unit struct {
	Name    string
	IP      string // pod IP; host:port in tests
	Shard   string // shard-N; "" in tests/dev
	Replica string // primary|standby
}

func (u unit) base() string {
	if strings.Contains(u.IP, ":") { // already host:port
		return "http://" + u.IP
	}
	return "http://" + u.IP + ":" + storePort
}

// ring is the aggregator's view of the store units:
//
//   - ownership is a STATIC consistent-hash ring over the shard names
//     (shard-0..N-1, N = SHARD_COUNT). Shard names are the stable storage
//     homes (P1 k1/k2): keys move between shards only when the shard set
//     itself changes, never when pods come and go.
//   - each shard's candidate units are the live pods labelled with that
//     shard (primary first); failover is a pod-level concern handled by
//     persist() walking the candidates.
type ring struct {
	logger *slog.Logger
	mu     sync.RWMutex
	units  []unit // all live units (both replicas)
	shardNames []string
	vnodeKeys      []uint64
	vnodeShardIndex []int
	vnodes         int
}

func newRing(logger *slog.Logger) *ring {
	r := &ring{logger: logger, vnodes: platform.EnvInt("STORE_VNODES", 100)}
	if n := platform.EnvInt("SHARD_COUNT", 0); n > 0 {
		names := make([]string, 0, n)
		for i := 0; i < n; i++ {
			names = append(names, fmt.Sprintf("shard-%d", i))
		}
		r.buildShardRing(names)
	}
	return r
}

// buildShardRing rebuilds the static vnode ring over the given shard names.
// Caller holds r.mu.
func (r *ring) buildShardRing(shards []string) {
	sort.Strings(shards)
	type vn struct {
		key   uint64
		index int
	}
	nodes := make([]vn, 0, len(shards)*r.vnodes)
	for i := range shards {
		for v := range r.vnodes {
			nodes = append(nodes, vn{key: vnode.ShardNameHash(shards[i], v), index: i})
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].key < nodes[j].key })
	r.shardNames = shards
	r.vnodeKeys = make([]uint64, len(nodes))
	r.vnodeShardIndex = make([]int, len(nodes))
	for i, n := range nodes {
		r.vnodeKeys[i] = n.key
		r.vnodeShardIndex[i] = n.index
	}
}

// ownerShard returns the shard owning key, or ok=false when no shard is known.
func (r *ring) ownerShard(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.shardNames) == 0 {
		return "", false
	}
	h := vnode.KeyHash(key)
	i := sort.Search(len(r.vnodeKeys), func(i int) bool { return r.vnodeKeys[i] >= h })
	if i == len(r.vnodeKeys) {
		i = 0
	}
	return r.shardNames[r.vnodeShardIndex[i]], true
}

// shards returns the currently known shard names (sorted).
func (r *ring) shards() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.shardNames...)
}

// candidates returns the live units of a shard, primary first, then by name
// for a deterministic failover order.
func (r *ring) candidates(shard string) []unit {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []unit
	for _, u := range r.units {
		if u.Shard != shard {
			continue
		}
		out = append(out, u)
	}
	rank := func(u unit) int { // primary first, deterministic
		if u.Replica == "primary" {
			return 0
		}
		return 1
	}
	sort.Slice(out, func(i, j int) bool {
		if rank(out[i]) != rank(out[j]) {
			return rank(out[i]) < rank(out[j])
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// all returns every live unit (both replicas), for fan-out reads. Reads are
// duplicated across replicas and de-duplicated by the caller.
func (r *ring) all() []unit {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]unit(nil), r.units...)
}

// setUnits installs the current pod set. Ownership is static (built in
// newRing from SHARD_COUNT); only the per-shard candidate lists change.
func (r *ring) setUnits(units []unit) {
	sort.Slice(units, func(i, j int) bool { return units[i].Name < units[j].Name })
	r.mu.Lock()
	r.units = units
	r.mu.Unlock()
}

// eventKey is the stable routing key shared with the store units: the same
// value must hash to the same shard on both sides (P1 k1/k2: one home per
// key, no duplication between nodes).
func eventKey(source, sourceRefID string) string {
	return source + ":" + sourceRefID
}

// collectFromPods adds one unit per ready store pod, shard/replica from the
// pod labels.
func collectFromPods(list *corev1.PodList, units map[string]unit) {
	for _, pod := range list.Items {
		if pod.Status.PodIP == "" {
			continue
		}
		ready := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}
		if !ready {
			continue
		}
		units[pod.Name] = unit{
			Name:    pod.Name,
			IP:      pod.Status.PodIP,
			Shard:   pod.Labels["shard"],
			Replica: pod.Labels["replica"],
		}
	}
}

// rebuild installs the currently observed pod set.
func (r *ring) rebuild(list *corev1.PodList) {
	units := map[string]unit{}
	collectFromPods(list, units)
	ordered := make([]unit, 0, len(units))
	for _, u := range units {
		ordered = append(ordered, u)
	}
	r.setUnits(ordered)
	r.logger.Info("ring_rebuild", "units", len(ordered))
}

// watch keeps the ring in sync with store pods via a shared informer.
func (r *ring) watch(namespace, selector string) {
	config, err := rest.InClusterConfig()
	if err != nil {
		r.logger.Error("k8s_config", "error", err)
		return
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		r.logger.Error("k8s_client", "error", err)
		return
	}
	parsed, err := labels.Parse(selector)
	if err != nil {
		r.logger.Error("store_label_selector", "error", err)
		return
	}
	factory := informers.NewSharedInformerFactoryWithOptions(
		client, 0, informers.WithNamespace(namespace),
	)
	podInformer := factory.Core().V1().Pods().Informer()
	podLister := corelisters.NewPodLister(podInformer.GetIndexer())
	rebuild := func() {
		items, err := podLister.Pods(namespace).List(parsed)
		if err != nil {
			return
		}
		deref := make([]corev1.Pod, 0, len(items))
		for _, item := range items {
			deref = append(deref, *item)
		}
		r.rebuild(&corev1.PodList{Items: deref})
	}
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { rebuild() },
		UpdateFunc: func(any, any) { rebuild() },
		DeleteFunc: func(any) { rebuild() },
	}
	_, _ = podInformer.AddEventHandler(handler)
	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	for _, synced := range factory.WaitForCacheSync(stop) {
		if !synced {
			r.logger.Error("ring_cache_sync_failed")
			return
		}
	}
	rebuild()
	<-stop
}
