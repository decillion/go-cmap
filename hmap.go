// Package hmap implements a non-resizable concurrency-aware hash map. The
// package is intended to used as a building block of other packages.
package hmap

import (
	"unsafe"
)

// Map is a non-resizable hash map. A single update operation and multiple read
// operations can be executed concurrently on the map, while multiple update
// operations cannot. In other words, only update operations need an external
// synchronization. Store and Delete are update operations and Load is a read
// operation.
type Map struct {
	NumOfBuckets  uint // the number of buckets
	NumOfEntries  uint // the number of entries
	MaxBucketSize uint // the size of the largest bucket
	hashFun       func(key interface{}) (hash uint32)
	buckets       []bucket
}

type bucket struct {
	size  uint
	first unsafe.Pointer // *entry
}

type entry struct {
	key   interface{}
	value interface{}
	next  unsafe.Pointer // *entry
}

// NewMap returns an empty hash map that maintain the given number of buckets.
func NewMap(numOfBuckets uint) (m *Map) {
	return nil
}

// Load returns the value associated with the given key and true if the key
// exists. Otherwise, it returns nil and false.
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	return nil, false
}

// Store sets the given value to the given key.
func (m *Map) Store(key, value interface{}) {

}

// Delete logically removes the given key and its associated value.
func (m *Map) Delete(key interface{}) {

}

// Range iteratively applies the given function to each key-value pair until
// the function returns false.
func (m *Map) Range(f func(key, value interface{}) bool) {

}
