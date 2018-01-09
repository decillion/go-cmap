package hmap_test

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/OneOfOne/cmap/hashers"
	"github.com/decillion/go-hmap"
)

const (
	capacity = 1 << 8
)

type mapIface interface {
	Load(key interface{}) (value interface{}, ok bool)
	Store(key, value interface{})
	Delete(key interface{})
	Range(func(key, value interface{}) bool)
}

type mapOp string

const (
	Load   = mapOp("Load")
	Store  = mapOp("Store")
	Delete = mapOp("Delete")
)

var mapOps = [...]mapOp{Load, Store, Delete}

type mapCall struct {
	op   mapOp
	k, v interface{}
}

func (c mapCall) apply(m mapIface) (interface{}, bool) {
	switch c.op {
	case Load:
		return m.Load(c.k)
	case Store:
		m.Store(c.k, c.v)
		return nil, false
	case Delete:
		m.Delete(c.k)
		return nil, false
	default:
		panic("invalid mapOp")
	}
}

type mapResult struct {
	v  interface{}
	ok bool
}

func randValue(r *rand.Rand) interface{} {
	b := make([]byte, r.Intn(4))
	for i := range b {
		b[i] = 'a' + byte(rand.Intn(26))
	}
	return string(b)
}

func (mapCall) Generate(r *rand.Rand, size int) reflect.Value {
	c := mapCall{op: mapOps[rand.Intn(len(mapOps))], k: randValue(r)}
	switch c.op {
	case Store:
		c.v = randValue(r)
	}
	return reflect.ValueOf(c)
}

func applyCalls(m mapIface, calls []mapCall) (results []mapResult, final map[interface{}]interface{}) {
	for _, c := range calls {
		v, ok := c.apply(m)
		results = append(results, mapResult{v, ok})
	}
	final = make(map[interface{}]interface{})
	m.Range(func(k, v interface{}) bool {
		final[k] = v
		return true
	})
	return results, final
}

func applyHashMap(calls []mapCall) ([]mapResult, map[interface{}]interface{}) {
	return applyCalls(hmap.NewMap(1<<10, hashers.TypeHasher32), calls)
}

func applyBuiltIn(calls []mapCall) ([]mapResult, map[interface{}]interface{}) {
	return applyCalls(NewBuiltIn(), calls)
}

func TestMachesBuiltInMap(t *testing.T) {
	if err := quick.CheckEqual(applyHashMap, applyBuiltIn, nil); err != nil {
		t.Error(err)
	}
}

type BuiltIn struct {
	b map[interface{}]interface{}
}

func NewBuiltIn() (m *BuiltIn) {
	return &BuiltIn{
		b: make(map[interface{}]interface{}),
	}
}

func (m *BuiltIn) Load(key interface{}) (value interface{}, ok bool) {
	value, ok = m.b[key]
	return
}

func (m *BuiltIn) Store(key, value interface{}) {
	m.b[key] = value
}

func (m *BuiltIn) Delete(key interface{}) {
	delete(m.b, key)
}

func (m *BuiltIn) Range(f func(key, value interface{}) bool) {
	for k, v := range m.b {
		if !f(k, v) {
			return
		}
	}
}
