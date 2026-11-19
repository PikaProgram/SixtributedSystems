// Package vnode implements the consistent-hash (virtual node) ring used to
// assign storage keys to store shards. Both the aggregator (write routing)
// and the store units (wrong-shard rejection) MUST derive ownership from this
// package so they always agree on where a key belongs.
package vnode

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// KeyHash hashes a storage key (source + ":" + source_ref_id) to a stable
// uint64.
func KeyHash(key string) uint64 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

// ShardNameHash hashes one virtual node: shard name + its vnode index.
func ShardNameHash(name string, vnode int) uint64 {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", name, vnode)))
	return binary.BigEndian.Uint64(sum[:8])
}

// Ring maps keys to shard names over a set of virtual nodes.
type Ring struct {
	shards  []string
	keys    []uint64
	owners  []int // index into shards
}

// New builds a ring over the given shard names, each contributing vnodes
// virtual nodes. Shard names are expected to be stable (e.g. shard-0..N-1);
// the ring content never changes after construction, so the value can be
// shared across goroutines without a lock.
func New(shardNames []string, vnodes int) *Ring {
	names := append([]string(nil), shardNames...)
	sort.Strings(names)
	type vn struct {
		key   uint64
		index int
	}
	nodes := make([]vn, 0, len(names)*vnodes)
	for i := range len(names) {
		for v := range vnodes {
			nodes = append(nodes, vn{key: ShardNameHash(names[i], v), index: i})
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].key < nodes[j].key })
	r := &Ring{
		shards: names,
		keys:   make([]uint64, len(nodes)),
		owners: make([]int, len(nodes)),
	}
	for i, n := range nodes {
		r.keys[i] = n.key
		r.owners[i] = n.index
	}
	return r
}

// Owner returns the shard name owning key, or ok=false for an empty ring.
func (r *Ring) Owner(key uint64) (string, bool) {
	if len(r.keys) == 0 {
		return "", false
	}
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= key })
	if i == len(r.keys) {
		i = 0
	}
	return r.shards[r.owners[i]], true
}
