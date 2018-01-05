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
// synchronization. Store and Delete are update operations and Load is a read
// operation.
type Map struct {
	NumOfBuckets uint
	NumOfEntries uint
	hashFun      func(key interface{}) (hash uint)
	buckets      []*entry
}

type entry struct {
	key   interface{}
	value unsafe.Pointer // *interface{}
	next  unsafe.Pointer // *entry
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

// findEntry returns the entry with the given key and true if the key exists.
// Otherwise, it returns the entry at the end of the current bucket and false.
func (m *Map) findEntry(key interface{}) (e *entry, ok bool) {
	e = m.buckets[m.hashFun(key)%m.NumOfBuckets]
	for e.key != key && e.next != nil {
		e = e.loadNext()
	}
	if e.key == key {
		return e, true
	}
	return e, false
}

// NewMap returns an empty hash map that maintain the number cap of buckets.
func NewMap(cap uint) (m *Map) {
	return &Map{
		NumOfBuckets: cap,
		hashFun:      fnvHasher,
		buckets:      make([]*entry, cap),
	}
}

// Load returns the value associated with the given key and true if the key
// exists. Otherwise, it returns nil and false.
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	if e, ok := m.findEntry(key); ok {
		if v := e.loadValue(); v != e {
			// v == e means the key is logically deleted.
			return v, true
		}
	}
	return nil, false
}

// Store sets the given value to the given key.
func (m *Map) Store(key, value interface{}) {
	if e, ok := m.findEntry(key); ok {
		e.storeValue(value)
	} else {
		m.NumOfEntries++
		newEntry := &entry{key: key}
		newEntry.storeValue(value)
		e.storeNext(newEntry)
	}
}

// Delete logically removes the given key and its associated value.
func (m *Map) Delete(key interface{}) {
	if e, ok := m.findEntry(key); ok {
		e.storeValue(e) // logical delete
	}
}

// Range iteratively applies the given function to each key-value pair until
// the function returns false.
func (m *Map) Range(f func(key, value interface{}) bool) {
	for _, b := range m.buckets {
		for e := b; e.next != nil; e = e.loadNext() {
			f(e.key, e.loadValue())
		}
	}
}
