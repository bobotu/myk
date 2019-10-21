package surf

import (
	"testing"

	"github.com/coocood/bbloom"
)

func BenchmarkBuild(b *testing.B) {
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		builder := NewBuilder(2, HashSuffix, 8, 0)
		builder.Build(handles, handles, 24)
	}
}

func BenchmarkGet(b *testing.B) {
	builder := NewBuilder(2, NoneSuffix, 4, 0)
	var surf SuRF
	surf.Unmarshal(builder.bulk(handles).Marshal())

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _ = surf.Get(handlesRnd[n%len(handles)][:])
	}
}

func BenchmarkIteratorSeek(b *testing.B) {
	builder := NewBuilder(2, RealSuffix, 0, 4)
	var surf SuRF
	surf.Unmarshal(builder.bulk(handles).Marshal())
	it := surf.NewIterator()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		it.Seek(handlesRnd[n%len(handles)][:])
	}
}

func BenchmarkIteratorNext(b *testing.B) {
	builder := NewBuilder(2, RealSuffix, 0, 4)
	var surf SuRF
	surf.Unmarshal(builder.bulk(handles).Marshal())
	it := surf.NewIterator()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if !it.Valid() {
			it.SeekToFirst()
		}
		it.Next()
	}
}

func BenchmarkBloomGet(b *testing.B) {
	bloom := bbloom.New(float64(len(handles)), 0.01)
	for _, k := range handles {
		bloom.Add(k)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = bloom.Has(handlesRnd[n%len(handles)][:])
	}
}
