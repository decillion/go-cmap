package concmap

import (
	"sync"
	"sync/atomic"

	"github.com/OneOfOne/cmap/hashers"
	hmap "github.com/decillion/go-hmap"
)

const (
	iniMapCapacity   = 1 << 4
	minimumMapSize   = 1 << 6
	lowerThreshold   = 2
	mediumLoadFactor = 4
	upperThreshold   = 6
)

var hasher = hashers.TypeHasher32

type Map struct {
	hm atomic.Value // *hmap.Map
	mu sync.Mutex
}

func NewMap() (m *Map) {
	m = &Map{}
	h := hmap.NewMap(iniMapCapacity, hasher)
	m.hm.Store(h)
	return
}

func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	h := m.hm.Load().(*hmap.Map)
	return h.Load(key)
}

func (m *Map) Store(key, value interface{}) {
	m.mu.Lock()
	h := m.hm.Load().(*hmap.Map)
	h.Store(key, value)
	resizeMap(m)
	m.mu.Unlock()
}

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

func (m *Map) Delete(key interface{}) {
	m.mu.Lock()
	h := m.hm.Load().(*hmap.Map)
	h.Delete(key)
	resizeMap(m)
	m.mu.Unlock()
}

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

	if entries < minimumMapSize {
		return
	}
	tooSmallBuckets := loadFactor > upperThreshold
	tooLargeBuckets := loadFactor < lowerThreshold && entries > minimumMapSize
	tooManyDeleted := entries < 2*deleted
	shouldResize := tooSmallBuckets || tooLargeBuckets || tooManyDeleted
	if shouldResize {
		newCapacity := max(realEntries/mediumLoadFactor, iniMapCapacity)
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
