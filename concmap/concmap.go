// Package concmap implements a concurrent map. The interface of the map mimics
// one of sync.Map.
package concmap

import (
	"sync"
	"sync/atomic"

	"github.com/OneOfOne/cmap/hashers"
	"github.com/decillion/go-hmap"
)

const (
	iniMapCap  = 1 << 4
	minMapSize = 1 << 6
	lowerBound = 2
	defaultLF  = 4
	upperBound = 6
)

var hasher = hashers.TypeHasher32

// Map is a concurrent that can be safely accessed by multiple goroutines.
// It is strongly discouraged to use keys of non-built-in types because
// the current hash function does not perform well on such keys.
type Map struct {
	hm atomic.Value // *hmap.Map
	mu sync.Mutex
}

// NewMap returns an empty map. Map must be created via this function.
func NewMap() (m *Map) {
	m = &Map{}
	h := hmap.NewMap(iniMapCap, hasher)
	m.hm.Store(h)
	return
}

// Load returns the value stored in the map for a key, or nil if no value is
// present. The ok result indicates whether value was found in the map.
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	h := m.hm.Load().(*hmap.Map)
	return h.Load(key)
}

// Store sets the value for a key.
func (m *Map) Store(key, value interface{}) {
	m.mu.Lock()
	h := m.hm.Load().(*hmap.Map)
	h.Store(key, value)
	resizeMap(m)
	m.mu.Unlock()
}

// LoadOrStore returns the existing value for the key if present. Otherwise,
// it stores and returns the given value. The loaded result is true if the
// value was loaded, false if stored.
func (m *Map) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
	h := m.hm.Load().(*hmap.Map)
	actual, loaded = h.Load(key)
	if loaded {
		return
	}

	m.mu.Lock()
	h = m.hm.Load().(*hmap.Map)
	actual, loaded = h.Load(key)
	if loaded {
		m.mu.Unlock()
		return
	}
	h.Store(key, value)
	m.mu.Unlock()
	// Do not resize here because this path is expensive.
	return value, false
}

// Delete deletes the value for a key.
func (m *Map) Delete(key interface{}) {
	m.mu.Lock()
	h := m.hm.Load().(*hmap.Map)
	h.Delete(key)
	resizeMap(m)
	m.mu.Unlock()
}

// Range calls f sequentially for each key and value present in the map. If f
// returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the
// Map's contents: no key will be visited more than once, but if the value for
// any key is stored or deleted concurrently, Range may reflect any mapping for
// that key from any point during the Range call.
func (m *Map) Range(f func(key, value interface{}) bool) {
	h := m.hm.Load().(*hmap.Map)
	h.Range(f)
}

func shouldResize(m *Map) bool {
	return false
}

// resizeMap resizes the map if needed.
func resizeMap(m *Map) {
	h := m.hm.Load().(*hmap.Map)
	buckets := hmap.NumOfBuckets(h)
	entries := hmap.NumOfEntries(h)
	deleted := hmap.NumOfDeleted(h)
	realEntries := entries - deleted
	loadFactor := float32(entries) / float32(buckets)

	if entries < minMapSize {
		return
	}
	tooSmallBuckets := loadFactor > upperBound
	tooLargeBuckets := loadFactor < lowerBound && entries > minMapSize
	tooManyDeleted := entries < 2*deleted
	shouldResize := tooSmallBuckets || tooLargeBuckets || tooManyDeleted
	if shouldResize {
		newCapacity := max(realEntries/defaultLF, iniMapCap)
		newHMap := hmap.NewMap(newCapacity, hasher)
		m.hm.Store(newHMap)
	}
}

func max(x, y uint32) uint32 {
	if x < y {
		return y
	}
	return x
}
