// Package hmap implements a non-resizable concurrency-aware hash map. The
// package is intended to used as a building block of other packages.
package hmap

import (
	"sync/atomic"
	"unsafe"
)

// Map is a non-resizable hash map. A single update operation and multiple read
// operations can be executed concurrently on the map, while multiple update
// operations cannot. In other words, only update operations need an external
// synchronization. Store and Delete are update operations and Load and Range
// are read operations.
type Map struct {
	hasher       func(key interface{}) (hash uint32)
	buckets      []*bucket
	numOfBuckets uint32
	numOfEntries uint32
}

// NumOfBuckets returns the number of buckets in the map.
func NumOfBuckets(m *Map) uint32 {
	return m.numOfBuckets
}

// NumOfEntries returns the number of keys in the map.
func NumOfEntries(m *Map) uint32 {
	return m.numOfEntries
}

type bucket struct {
	first unsafe.Pointer
}

type entry struct {
	key   interface{}
	value unsafe.Pointer // *interface{}
	next  unsafe.Pointer // *entry
}

var deleted = unsafe.Pointer(new(interface{}))

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

// findEntry returns the bucket and the entry with the given key and true if
// the key exists. Otherwise, it returns the bucket with the given key, the
// entry at the end of the bucket (nil if no entry) and false.
func (m *Map) findEntry(key interface{}) (b *bucket, e *entry, ok bool) {
	i := m.hasher(key) % m.numOfBuckets
	b = m.buckets[i]
	e = b.loadFirst()
	if e == nil {
		return b, nil, false
	}

	for e.key != key && e.loadNext() != nil {
		e = e.loadNext()
	}
	if e.key == key {
		return b, e, true
	}
	return b, e, false
}

// NewMap returns an empty hash map that maintain the number cap of buckets.
// The map uses the given hash function internally.
func NewMap(cap uint32, hasher func(key interface{}) uint32) (m *Map) {
	buckets := make([]*bucket, cap)
	for i := uint32(0); i < cap; i++ {
		buckets[i] = &bucket{}
	}

	return &Map{
		numOfBuckets: cap,
		hasher:       hasher,
		buckets:      buckets,
	}
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
}

// Store sets the given value to the given key.
func (m *Map) Store(key, value interface{}) {
	if b, e, ok := m.findEntry(key); ok {
		e.storeValue(value)
	} else {
		m.numOfEntries++
		newEntry := &entry{key: key}
		newEntry.storeValue(value)

		if e == nil {
			b.storeFirst(newEntry)
		} else {
			e.storeNext(newEntry)
		}
	}
}

// Delete logically removes the given key and its associated value.
func (m *Map) Delete(key interface{}) {
	if b, e, ok := m.findEntry(key); ok {
		m.numOfEntries--
		e.storeValue(deleted) // logical delete

		if b.loadFirst() == e {
			b.storeFirst(e.loadNext())
		} else {
			prev := b.loadFirst()
			curr := prev.loadNext()
			for curr != e {
				prev = curr
				curr = curr.loadNext()
			}
			next := curr.loadNext()
			prev.storeNext(next)
		}
	}
}

// Range iteratively applies the given function to each key-value pair until
// the function returns false.
func (m *Map) Range(f func(key, value interface{}) bool) {
	for _, b := range m.buckets {
		for e := b.loadFirst(); e != nil; e = e.loadNext() {
			v := e.loadValue()
			if v == deleted {
				continue
			}
			f(e.key, v)
		}
	}
}
