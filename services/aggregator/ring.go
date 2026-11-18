package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/astolfo/sixtributedsystems/internal/platform"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const storePort = "8100"

// unit is one primary store replica behind the vnode ring.
type unit struct {
	Name string
	IP   string // pod IP; host:port in tests
}

func (u unit) base() string {
	if strings.Contains(u.IP, ":") { // already host:port
		return "http://" + u.IP
	}
	return "http://" + u.IP + ":" + storePort
}

// ring is a consistent-hash vnode ring over the primary store units.
type ring struct {
	logger         *slog.Logger
	mu             sync.RWMutex
	units          []unit
	vnodeKeys      []uint64
	vnodeUnitIndex []int
	vnodes         int
}

func newRing(logger *slog.Logger) *ring {
	return &ring{logger: logger, vnodes: platform.EnvInt("STORE_VNODES", 100)}
}

func vnodeHash(unitName string, vnode int) uint64 {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", unitName, vnode)))
	return binary.BigEndian.Uint64(sum[:8])
}

func eventKey(source, sourceRefID string) string {
	return source + ":" + sourceRefID
}

func keyHash(key string) uint64 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

// setUnits atomically rebuilds the ring for the given unit list.
func (r *ring) setUnits(units []unit) {
	sort.Slice(units, func(i, j int) bool { return units[i].Name < units[j].Name })
	type vnode struct {
		key   uint64
		index int
	}
	vnodes := make([]vnode, 0, len(units)*r.vnodes)
	for i, u := range units {
		for v := range r.vnodes {
			vnodes = append(vnodes, vnode{key: vnodeHash(u.Name, v), index: i})
		}
	}
	sort.Slice(vnodes, func(i, j int) bool { return vnodes[i].key < vnodes[j].key })
	keys := make([]uint64, len(vnodes))
	indexes := make([]int, len(vnodes))
	for i, vn := range vnodes {
		keys[i] = vn.key
		indexes[i] = vn.index
	}
	r.mu.Lock()
	r.units = units
	r.vnodeKeys = keys
	r.vnodeUnitIndex = indexes
	r.mu.Unlock()
}

// owner returns the unit owning key, or ok=false when the ring is empty.
func (r *ring) owner(key string) (unit, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.units) == 0 {
		return unit{}, false
	}
	h := keyHash(key)
	i := sort.Search(len(r.vnodeKeys), func(i int) bool { return r.vnodeKeys[i] >= h })
	if i == len(r.vnodeKeys) {
		i = 0
	}
	return r.units[r.vnodeUnitIndex[i]], true
}

func (r *ring) all() []unit {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]unit(nil), r.units...)
}

// unitName derives the store unit name for an endpoint object: the owning
// Service (one dedicated store service per shard), falling back to the
// kubernetes.io/service-name label.
func sliceUnitName(slice *discoveryv1.EndpointSlice) string {
	for _, owner := range slice.OwnerReferences {
		if owner.Kind == "Service" && owner.Name != "" {
			return owner.Name
		}
	}
	return slice.Labels[discoveryv1.LabelServiceName]
}

// collectFromEndpoints adds one unit per Endpoints object with ready addresses.
func collectFromEndpoints(list *corev1.EndpointsList, units map[string]unit) {
	for _, ep := range list.Items {
		for _, subset := range ep.Subsets {
			for _, address := range subset.Addresses {
				if address.IP != "" {
					units[ep.Name] = unit{Name: ep.Name, IP: address.IP}
					break
				}
			}
		}
	}
}

// collectFromSlices adds one unit per EndpointSlice with a ready serving endpoint.
func collectFromSlices(list *discoveryv1.EndpointSliceList, units map[string]unit) {
	for _, slice := range list.Items {
		name := sliceUnitName(&slice)
		if name == "" {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			ready := endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready
			serving := endpoint.Conditions.Serving == nil || *endpoint.Conditions.Serving
			if !ready || !serving {
				continue
			}
			for _, address := range endpoint.Addresses {
				if address != "" {
					units[name] = unit{Name: name, IP: address}
					break
				}
			}
		}
	}
}

func listEndpoints(list *corelisters.EndpointsLister, namespace string, selector labels.Selector) *corev1.EndpointsList {
	items, err := (*list).Endpoints(namespace).List(selector)
	if err != nil {
		return &corev1.EndpointsList{}
	}
	deref := make([]corev1.Endpoints, 0, len(items))
	for _, item := range items {
		deref = append(deref, *item)
	}
	return &corev1.EndpointsList{Items: deref}
}

func listSlices(list *discoverylisters.EndpointSliceLister, namespace string, selector labels.Selector) *discoveryv1.EndpointSliceList {
	items, err := (*list).EndpointSlices(namespace).List(selector)
	if err != nil {
		return &discoveryv1.EndpointSliceList{}
	}
	deref := make([]discoveryv1.EndpointSlice, 0, len(items))
	for _, item := range items {
		deref = append(deref, *item)
	}
	return &discoveryv1.EndpointSliceList{Items: deref}
}

// rebuild merges endpoints and slices into the ring's unit set.
func (r *ring) rebuild(
	endpoints *corev1.EndpointsList,
	slices *discoveryv1.EndpointSliceList,
) {
	units := map[string]unit{}
	collectFromEndpoints(endpoints, units)
	collectFromSlices(slices, units)
	list := make([]unit, 0, len(units))
	for _, u := range units {
		list = append(list, u)
	}
	r.setUnits(list)
	r.logger.Info("ring_rebuild", "units", len(list))
}

// watch keeps the ring in sync with store endpoints via shared informers.
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
	endpointInformer := factory.Core().V1().Endpoints().Informer()
	sliceInformer := factory.Discovery().V1().EndpointSlices().Informer()
	epLister := corelisters.NewEndpointsLister(endpointInformer.GetIndexer())
	sliceLister := discoverylisters.NewEndpointSliceLister(sliceInformer.GetIndexer())
	rebuild := func() {
		r.rebuild(
			listEndpoints(&epLister, namespace, parsed),
			listSlices(&sliceLister, namespace, parsed),
		)
	}
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { rebuild() },
		UpdateFunc: func(any, any) { rebuild() },
		DeleteFunc: func(any) { rebuild() },
	}
	_, _ = endpointInformer.AddEventHandler(handler)
	_, _ = sliceInformer.AddEventHandler(handler)
	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	synced := factory.WaitForCacheSync(stop)
	for _, ok := range synced {
		if !ok {
			r.logger.Error("ring_cache_sync_failed")
			return
		}
	}
	rebuild()
	<-stop
}
