package cmap_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/decillion/go-cmap"
)

const (
	entries = 1 << 10
)

type bench struct {
	setup func(*testing.B, mapIface)
	perG  func(b *testing.B, pb *testing.PB, m mapIface)
}

func benchMap(b *testing.B, bench bench) {
	for _, m := range [...]mapIface{&sync.Map{}, &cmap.Map{}} {
		b.Run(fmt.Sprintf("%T", m), func(b *testing.B) {
			if _, ok := m.(*cmap.Map); ok {
				m = cmap.NewMap(cmap.DefaultHasher)
			}
			if bench.setup != nil {
				bench.setup(b, m)
			}
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				bench.perG(b, pb, m)
			})
		})
	}
}

func Benchmark_ReadMostly_StableKeys(b *testing.B) {
	benchMap(b, bench{
		setup: func(_ *testing.B, m mapIface) {
			for i := 0; i < entries; i++ {
				m.Store(i, struct{}{})
			}
		},

		perG: func(b *testing.B, pb *testing.PB, m mapIface) {
			var id uint32
			for pb.Next() {
				key := int(atomic.AddUint32(&id, 1)) % entries
				if key%20 == 0 {
					m.Store(key, struct{}{})
				} else if key%20 == 10 {
					m.Delete(key)
				} else {
					m.Load(key)
				}
			}
		},
	})
}

func Benchmark_ReadMostly_UnstableKey(b *testing.B) {
	benchMap(b, bench{
		setup: func(_ *testing.B, m mapIface) {
			for i := 0; i < entries; i++ {
				m.Store(i, struct{}{})
			}
		},

		perG: func(b *testing.B, pb *testing.PB, m mapIface) {
			var id uint32
			var newestKey uint32 = entries
			var oldestKey uint32
			for pb.Next() {
				i := int(atomic.AddUint32(&id, 1)) % entries
				if i%20 == 0 {
					key := int(atomic.AddUint32(&newestKey, 1))
					m.Store(key, struct{}{})
				} else if i%20 == 10 {
					key := int(atomic.AddUint32(&oldestKey, 1)) - 1
					m.Delete(key)
				} else {
					offset := int(atomic.LoadUint32(&oldestKey))
					m.Load(i + offset)
				}
			}
		},
	})
}
