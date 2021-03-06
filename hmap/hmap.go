// Package hmap implements a non-resizable concurrency-aware hash map.
package hmap

import (
	"sync/atomic"
	"unsafe"
)

// Map is a non-resizable hash map. A single update operation and multiple read
// operations can be executed concurrently on the map, while multiple update
// operations cannot. In other words, only update operations need an external
// synchronization.
//
// Store and Delete are update operations and Load and Range are read
// operations. StatBuckets and StatEntries are considered to be write
// operations, while they do not modify the map.
type Map struct {
	hasher        func(key interface{}) (hash uint32)
	buckets       []*bucket
	numOfEntries  uint
	numOfDeleted  uint
	largestBucket uint
}

type bucket struct {
	first        unsafe.Pointer // *entry
	numOfEntries uint
}

type entry struct {
	key   interface{}
	value unsafe.Pointer // *interface{}
	next  unsafe.Pointer // *entry
}

var (
	deleted  = unsafe.Pointer(new(interface{}))
	terminal = unsafe.Pointer(new(interface{}))
)

func (b *bucket) loadFirst() (first *entry) {
	return (*entry)(atomic.LoadPointer(&b.first))
}

func (b *bucket) storeFirst(first *entry) {
	atomic.StorePointer(&b.first, unsafe.Pointer(first))
}

func (e *entry) loadValue() (value interface{}) {
	return *(*interface{})(atomic.LoadPointer(&e.value))
}

func (e *entry) storeValue(value interface{}) {
	atomic.StorePointer(&e.value, unsafe.Pointer(&value))
}

func (e *entry) loadNext() (next *entry) {
	return (*entry)(atomic.LoadPointer(&e.next))
}

func (e *entry) storeNext(next *entry) {
	atomic.StorePointer(&e.next, unsafe.Pointer(next))
}

// StatBuckets returns the number of buckets and the number of keys in the
// largest bucket.
func (m *Map) StatBuckets() (capacity, largest uint) {
	return uint(len(m.buckets)), m.largestBucket
}

// StatEntries returns the number of keys physically existing in the map and
// the number of logically deleted keys.
func (m *Map) StatEntries() (mapSize, deleted uint) {
	return m.numOfEntries, m.numOfDeleted
}

// NewMap returns an empty hash map that maintain the given number of buckets.
// The function hasher is used to hash keys.
func NewMap(capacity uint, hasher func(key interface{}) uint32) (m *Map) {
	buckets := make([]*bucket, capacity)
	for i := uint(0); i < capacity; i++ {
		buckets[i] = &bucket{}
		sentinel := &entry{key: terminal}
		buckets[i].storeFirst(sentinel)
	}
	return &Map{hasher: hasher, buckets: buckets}
}

// findEntry returns the bucket and the entry with the given key and true if
// the key exists. Otherwise, it returns the bucket with the given key, the
// sentinel entry, and false.
func (m *Map) findEntry(key interface{}) (b *bucket, e *entry, ok bool) {
	i := m.hasher(key) % uint32(len(m.buckets))
	b = m.buckets[i]
	e = b.loadFirst()

	for e.key != key && e.key != terminal {
		e = e.loadNext()
	}
	if e.key == key {
		return b, e, true
	}
	return b, e, false
}

// Load returns the value associated with the given key and true if the key
// exists. Otherwise, it returns nil and false.
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	if _, e, ok := m.findEntry(key); ok {
		if v := e.loadValue(); v != deleted {
			return v, true
		}
	}
	return nil, false
	// The linearization point of Load should be taken as the folowing:
	// 1. If ok == true, take the point of e.loadValue();
	// 2. If ok != true, take the point of the invocation of Load.
}

// Store sets the given value to the given key.
func (m *Map) Store(key, value interface{}) {
	if b, e, ok := m.findEntry(key); ok {
		if v := e.loadValue(); v == deleted {
			m.numOfDeleted--
		}
		e.storeValue(value) // linearization point
	} else {
		m.numOfEntries++
		b.numOfEntries++
		if b.numOfEntries > m.largestBucket {
			m.largestBucket++
		}
		newEntry := &entry{key: key}
		newEntry.storeValue(value)
		newEntry.storeNext(b.loadFirst())
		b.storeFirst(newEntry) // linearization point
	}
}

// Delete logically removes the given key and its associated value.
func (m *Map) Delete(key interface{}) {
	if _, e, ok := m.findEntry(key); ok {
		if v := e.loadValue(); v != deleted {
			m.numOfDeleted++
		}
		e.storeValue(deleted) // linearization point
	}
}

// Range iteratively applies the given function to each key-value pair until
// the function returns false.
func (m *Map) Range(f func(key, value interface{}) bool) {
	for _, b := range m.buckets {
		for e := b.loadFirst(); e.key != terminal; e = e.loadNext() {
			v := e.loadValue()
			if v == deleted {
				continue
			}
			f(e.key, v)
		}
	}
}
