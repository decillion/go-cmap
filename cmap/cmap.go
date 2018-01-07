// Package cmap implements a resizable concurrency-aware hash map based on
// hmap.Map. Only keys of built-in types are supported.
package cmap

import (
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

// Map is a hash map. A single update operation and multiple read operations
// can be executed concurrently on the map, while multiple update operations
// cannot. In other words, only update operations need an external
// synchronization. Store and Delete are update operations and Load and Range
// are read operations.
type Map struct {
	hm atomic.Value // *hmap.Map
}

// NewMap returns an empty hash map.
func NewMap() (m *Map) {
	m = &Map{}
	h := hmap.NewMap(iniMapCap, hasher)
	m.hm.Store(h)
	return
}

// Load returns the value associated with the given key and true if the key
// exists. Otherwise, it returns nil and false.
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	h := m.hm.Load().(*hmap.Map)
	return h.Load(key)
}

// Store sets the given value to the given key.
func (m *Map) Store(key, value interface{}) {
	h := m.hm.Load().(*hmap.Map)
	h.Store(key, value)
	resizeIfNeeded(m)
}

// Delete removes the given key and its associated value.
func (m *Map) Delete(key interface{}) {
	h := m.hm.Load().(*hmap.Map)
	h.Delete(key)
	resizeIfNeeded(m)
}

// Range iteratively applies the given function to each key-value pair until
// the function returns false.
func (m *Map) Range(f func(key, value interface{}) bool) {
	h := m.hm.Load().(*hmap.Map)
	h.Range(f)
}

func shouldResize(m *Map) bool {
	return false
}

func resizeIfNeeded(m *Map) {
	h := m.hm.Load().(*hmap.Map)

	entries := hmap.NumOfEntries(h)
	if entries < minMapSize {
		return
	}
	buckets := hmap.NumOfBuckets(h)
	deleted := hmap.NumOfDeleted(h)
	realEntries := entries - deleted
	loadFactor := float32(entries) / float32(buckets)

	tooSmallBuckets := loadFactor > upperBound
	tooLargeBuckets := loadFactor < lowerBound
	tooManyDeleted := entries < 2*deleted
	shouldResize := tooSmallBuckets || tooLargeBuckets || tooManyDeleted

	if shouldResize {
		newCapacity := max(realEntries/defaultLF, iniMapCap)
		newMap := hmap.NewMap(newCapacity, hasher)
		oldMap := m.hm.Load().(*hmap.Map)
		oldMap.Range(func(k, v interface{}) bool {
			if v != deleted {
				newMap.Store(k, v)
			}
			return true
		})
		m.hm.Store(newMap)
	}
}

func max(x, y uint32) uint32 {
	if x < y {
		return y
	}
	return x
}
