package surf

import (
	"testing"
)

func BenchmarkSuRF(b *testing.B) {
	suite := new(SuRFTestSuite)
	suite.SetupSuite()
	b.ResetTimer()

	b.Run("Build", func(b *testing.B) {
		benchmarkBuild(suite, b)
	})
	b.Run("Get", func(b *testing.B) {
		benchmarkGet(suite, b)
	})
	b.Run("IteratorSeek", func(b *testing.B) {
		benchmarkIteratorSeek(suite, b)
	})
	b.Run("IteratorNext", func(b *testing.B) {
		benchmarkIteratorNext(suite, b)
	})
}

func benchmarkBuild(suite *SuRFTestSuite, b *testing.B) {
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		builder := NewBuilder(2, 8, 0)
		builder.Build(suite.handles, suite.handles, 24)
	}
}

func benchmarkGet(suite *SuRFTestSuite, b *testing.B) {
	builder := NewBuilder(2, 0, 0)
	var surf SuRF
	surf.Unmarshal(builder.bulk(suite.handles).Marshal())

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _ = surf.Get(suite.handlesRnd[n%len(suite.handles)][:])
	}
}

func benchmarkIteratorSeek(suite *SuRFTestSuite, b *testing.B) {
	builder := NewBuilder(2, 0, 4)
	var surf SuRF
	surf.Unmarshal(builder.bulk(suite.handles).Marshal())
	it := surf.NewIterator()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		it.Seek(suite.handlesRnd[n%len(suite.handles)][:])
	}
}

func benchmarkIteratorNext(suite *SuRFTestSuite, b *testing.B) {
	builder := NewBuilder(2, 0, 4)
	var surf SuRF
	surf.Unmarshal(builder.bulk(suite.handles).Marshal())
	it := surf.NewIterator()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if !it.Valid() {
			it.SeekToFirst()
		}
		it.Next()
	}
}
