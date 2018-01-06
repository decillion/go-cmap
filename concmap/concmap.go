package concmap

import (
	"sync"
	"sync/atomic"

	"github.com/OneOfOne/cmap/hashers"
	hmap "github.com/decillion/go-hmap"
)

const (
	iniMapCapacity = 1 << 8
	// iniMapCapacity = 1 << 4
)

type Map struct {
	hm atomic.Value // *hmap.Map
	mu sync.Mutex
}

func NewMap() (m *Map) {
	m = &Map{}
	h := hmap.NewMap(iniMapCapacity, hashers.TypeHasher32)
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

	if shouldResize(m) {
		resizeMap(m)
	}
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

	if shouldResize(m) {
		resizeMap(m)
	}
	m.mu.Unlock()
}

func (m *Map) Range(f func(key, value interface{}) bool) {
	h := m.hm.Load().(*hmap.Map)
	h.Range(f)
}

func shouldResize(m *Map) bool {
	return false
}

func resizeMap(m *Map) {

}
