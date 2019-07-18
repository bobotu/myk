package art

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/coocood/badger/skl"
	"github.com/coocood/badger/y"
)

const N = 1000000

func BenchmarkArtSet(b *testing.B) {
	es := genEntries(N)
	test := []int{10, 100, 1000, 10000, 100000, 1000000}
	for _, t := range test {
		b.Run(fmt.Sprintf("ART-set-%d", t), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if t >= 100000 {
					b.StopTimer()
					runtime.GC()
					b.StartTimer()
				}
				tree := New()
				for _, e := range es[:t] {
					tree.Put(e.k[:], e.v)
				}
			}
		})
	}
}

func BenchmarkArtConSet(b *testing.B) {
	G := runtime.GOMAXPROCS(0)
	es := make([][]*entry, G)
	for i := range es {
		es[i] = genEntriesWithSeed(int64(i), N/G)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		start := make(chan struct{})
		var wg sync.WaitGroup
		tree := New()
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g1 int) {
				<-start
				for _, e := range es[g1] {
					tree.Put(e.k[:], e.v)
				}
				wg.Done()
			}(g)
		}
		runtime.GC()
		b.StartTimer()
		close(start)
		wg.Wait()
	}
}

func BenchmarkArtGet(b *testing.B) {
	es := genEntries(N)
	test := []int{10, 100, 1000, 10000, 100000, 1000000}
	for _, t := range test {
		tree := New()
		for _, e := range es[:t] {
			tree.Put(e.k[:], e.v)
		}
		keys := shuffleKeys(es[:t])

		b.Run(fmt.Sprintf("ART-get-%d", t), func(b *testing.B) {
			benchmarkArtGet(b, tree, keys)
		})
	}
}

func benchmarkArtGet(b *testing.B, t *ART, keys [][19]byte) {
	N := len(keys)
	var sink []byte
	for i := 0; i < b.N; i++ {
		sink, _ = t.Get(keys[i%N][:])
	}
	_ = sink
}

func BenchmarkMapSet(b *testing.B) {
	es := genEntries(N)
	test := []int{10, 100, 1000, 10000, 100000, 1000000}
	for _, t := range test {
		b.Run(fmt.Sprintf("Map-set-%d", t), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if t >= 100000 {
					b.StopTimer()
					runtime.GC()
					b.StartTimer()
				}
				m := make(map[[19]byte][]byte)
				for _, e := range es[:t] {
					m[e.k] = e.v
				}
			}
		})
	}
}

func BenchmarkMapConSet(b *testing.B) {
	G := runtime.GOMAXPROCS(0)
	es := make([][]*entry, G)
	for i := range es {
		es[i] = genEntriesWithSeed(int64(i), N/G)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		start := make(chan struct{})
		var wg sync.WaitGroup
		m := make(map[[19]byte][]byte)
		var lock sync.Mutex
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g1 int) {
				<-start
				for _, e := range es[g1] {
					lock.Lock()
					m[e.k] = e.v
					lock.Unlock()
				}
				wg.Done()
			}(g)
		}
		runtime.GC()
		b.StartTimer()
		close(start)
		wg.Wait()
	}
}

func BenchmarkMapGet(b *testing.B) {
	es := genEntries(N)
	test := []int{10, 100, 1000, 10000, 100000, 1000000}
	for _, t := range test {
		m := make(map[[19]byte][]byte)
		for _, e := range es[:t] {
			m[e.k] = e.v
		}
		keys := shuffleKeys(es[:t])

		b.Run(fmt.Sprintf("Map-get-%d", t), func(b *testing.B) {
			benchmarkMapGet(b, m, keys)
		})
	}
}

func benchmarkMapGet(b *testing.B, m map[[19]byte][]byte, keys [][19]byte) {
	N := len(keys)
	var sink []byte
	for i := 0; i < b.N; i++ {
		sink = m[keys[i%N]]
	}
	_ = sink
}

func BenchmarkBSklSet(b *testing.B) {
	es := genEntries(N)
	test := []int{10, 100, 1000, 10000, 100000, 1000000}
	for _, t := range test {
		b.Run(fmt.Sprintf("bSKL-set-%d", t), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var list *skl.Skiplist
				if t >= 100000 {
					b.StopTimer()
					runtime.GC()
					list = skl.NewSkiplist(int64(t * skl.MaxNodeSize))
					b.StartTimer()
				} else {
					list = skl.NewSkiplist(int64(t * skl.MaxNodeSize))
				}
				for _, e := range es[:t] {
					list.Put(e.k[:], y.ValueStruct{Value: e.v})
				}
			}
		})
	}
}

func BenchmarkBSklConSet(b *testing.B) {
	G := runtime.GOMAXPROCS(0)
	es := make([][]*entry, G)
	for i := range es {
		es[i] = genEntriesWithSeed(int64(i), N/G)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		start := make(chan struct{})
		var wg sync.WaitGroup
		list := skl.NewSkiplist(int64(N * skl.MaxNodeSize))
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g1 int) {
				<-start
				for _, e := range es[g1] {
					list.Put(e.k[:], y.ValueStruct{Value: e.v})
				}
				wg.Done()
			}(g)
		}
		runtime.GC()
		b.StartTimer()
		close(start)
		wg.Wait()
	}
}

func BenchmarkBSklGet(b *testing.B) {
	es := genEntries(N)
	test := []int{10, 100, 1000, 10000, 100000, 1000000}
	for _, t := range test {
		list := skl.NewSkiplist(int64(t * skl.MaxNodeSize))
		for _, e := range es[:t] {
			list.Put(e.k[:], y.ValueStruct{Value: e.v})
		}
		keys := shuffleKeys(es[:t])

		b.Run(fmt.Sprintf("bSKL-get-%d", t), func(b *testing.B) {
			benchmarkBSKLGet(b, list, keys)
		})
	}
}

func benchmarkBSKLGet(b *testing.B, list *skl.Skiplist, keys [][19]byte) {
	N := len(keys)
	var sink y.ValueStruct
	for i := 0; i < b.N; i++ {
		sink = list.Get(keys[i%N][:])
	}
	_ = sink
}
