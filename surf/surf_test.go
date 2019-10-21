package surf

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
)

func u16ToBytes(v uint16) []byte {
	var b [2]byte
	endian.PutUint16(b[:], v)
	return b[:]
}

var (
	intKeys      [][]byte
	intKeysTrunc [][]byte
	handles      [][]byte
	handlesRnd   [][19]byte

	datasets map[string][][]byte
)

func truncateSuffixes(keys [][]byte) [][]byte {
	result := make([][]byte, 0, len(keys))
	commonPrefixLen := 0
	for i := 0; i < len(keys); i++ {
		if i == 0 {
			commonPrefixLen = getCommonPrefixLen(keys[i], keys[i+1])
		} else if i == len(keys)-1 {
			commonPrefixLen = getCommonPrefixLen(keys[i-1], keys[i])
		} else {
			commonPrefixLen = getCommonPrefixLen(keys[i-1], keys[i])
			b := getCommonPrefixLen(keys[i], keys[i+1])
			if b > commonPrefixLen {
				commonPrefixLen = b
			}
		}

		if commonPrefixLen < len(keys[i]) {
			result = append(result, keys[i][:commonPrefixLen+1])
		} else {
			k := make([]byte, 0, len(keys[i])+1)
			k = append(k, keys[i]...)
			result = append(result, append(k, labelTerminator))
		}
	}

	return result
}

func truncateKey(k []byte) []byte {
	if k[len(k)-1] == labelTerminator {
		k = k[:len(k)-1]
	}
	return k
}

func getCommonPrefixLen(a, b []byte) int {
	l := 0
	for l < len(a) && l < len(b) && a[l] == b[l] {
		l++
	}
	return l
}

func initTexture() {
	intKeys = make([][]byte, 0, 1000000)
	for i := 0; i < 1000000; i++ {
		intKeys = append(intKeys, []byte(strconv.Itoa(i)))
	}
	sort.Slice(intKeys, func(i, j int) bool {
		return bytes.Compare(intKeys[i], intKeys[j]) < 0
	})
	intKeysTrunc = truncateSuffixes(intKeys)

	handles = make([][]byte, 0, 3000000)
	rnd := rand.New(rand.NewSource(0xdeadbeaf))
	for i := 0; i < 3000000; i++ {
		k := tablecodec.EncodeRowKeyWithHandle(rnd.Int63n(15)+1, rnd.Int63())
		handles = append(handles, k)
	}
	sort.Slice(handles, func(i, j int) bool {
		return bytes.Compare(handles[i], handles[j]) < 0
	})
	handlesRnd = make([][19]byte, len(handles))
	p := rand.New(rand.NewSource(0xdeadbeaf)).Perm(len(handles))
	for i, idx := range p {
		copy(handlesRnd[i][:], handles[idx])
	}

	datasets = make(map[string][][]byte)
	datasets["handles"] = handles
	var wg sync.WaitGroup
	filepath.Walk("../dataset", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !strings.HasSuffix(info.Name(), ".txt.bz2") {
				return
			}

			f, err := os.Open(path)
			if err != nil {
				panic(err)
			}
			sc := bufio.NewScanner(bzip2.NewReader(f))

			var data [][]byte
			for sc.Scan() {
				data = append(data, append([]byte{}, sc.Bytes()...))
			}
			sort.Slice(data, func(i, j int) bool {
				return bytes.Compare(data[i], data[j]) < 0
			})
			datasets[strings.TrimSuffix(info.Name(), ".txt.bz2")] = data
		}()

		return nil
	})
	wg.Wait()
}

func TestMain(m *testing.M) {
	initTexture()
	os.Exit(m.Run())
}

func TestSingleKey(t *testing.T) {
	builder := NewBuilder(2, MixedSuffix, 2, 2)
	s := builder.Build([][]byte{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, [][]byte{{1, 2}}, 10)
	v, ok := s.Get([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
	require.True(t, ok)
	require.Equal(t, []byte{1, 2}, v)
}

func TestTableRowKeyWithVaryTid(t *testing.T) {
	for _, n := range []int{10000, 100000, 500000, 1000000} {
		for _, x := range []int{2, 5, 10, 50, 100} {
			func(n, x int) {
				t.Run(fmt.Sprintf("%d/%d", n, x), func(t *testing.T) {
					t.Parallel()

					rnd := rand.New(rand.NewSource(0xdeadbeaf))
					handles := rnd.Perm(3 * n)

					a := make([][]byte, 0, n)
					b := make([][]byte, 0, n*2)
					for i := 0; i < n; i += 3 {
						tid := int64(i % x)
						a = append(a, tablecodec.EncodeRowKeyWithHandle(tid, int64(handles[i])))
						b = append(b, tablecodec.EncodeRowKeyWithHandle(tid, int64(handles[i+1])))
						b = append(b, tablecodec.EncodeRowKeyWithHandle(tid, int64(handles[i+2])))
					}
					sort.Slice(a, func(i, j int) bool {
						return bytes.Compare(a[i], a[j]) < 0
					})

					surf := NewBuilder(10, MixedSuffix, 4, 4).Build(a, a, 48)

					for _, k := range a {
						v, ok := surf.Get(k)
						require.Equal(t, k[:len(v)], v)
						require.True(t, ok)
					}

					var fp int
					for _, k := range b {
						if _, ok := surf.Get(k); ok {
							fp++
						}
					}
					t.Logf("[n: %d, x: %d], fp: %f%% (size: %d)", n, x, float64(fp)/float64(len(b))*100.0, surf.MarshalSize())
				})
			}(n, x)
		}
	}
}

func TestWithDatasets(t *testing.T) {
	for name, data := range datasets {
		for _, n := range []int{100000, 500000, 1000000} {
			func(data [][]byte, n int, name string) {
				t.Run(fmt.Sprintf("%s/%d", name, n), func(t *testing.T) {
					t.Parallel()
					keys := append([][]byte{}, data[:n]...)
					sort.Slice(keys, func(i, j int) bool {
						return bytes.Compare(keys[i], keys[j]) < 0
					})

					surf := NewBuilder(3, MixedSuffix, 4, 4).Build(keys, keys, 48)

					for _, k := range keys {
						v, ok := surf.Get(k)
						require.Equal(t, k[:3], v)
						require.True(t, ok)
					}

					var fp int
					for _, k := range data[n:] {
						if _, ok := surf.Get(k); ok {
							fp++
						}
					}
					t.Logf("[data: %s, n: %d], fp: %f%% (size: %d)", name, n, float64(fp)/float64(len(data[n:]))*100.0, surf.MarshalSize())
				})
			}(data[:n*3], n, name)
		}
	}
}

func TestKeysExist(t *testing.T) {
	suffixLens := []uint32{1, 3, 7, 8, 16, 31, 48, 64}
	suffixes := []SuffixType{NoneSuffix, HashSuffix, RealSuffix, MixedSuffix}

	for _, sl := range suffixLens {
		for _, sf := range suffixes {
			if sf == MixedSuffix && sl >= 48 {
				continue
			}

			t.Run(fmt.Sprintf("suffix=%s,suffixLen=%d", sf, sl), func(t *testing.T) {
				builder := NewBuilder(2, sf, sl, sl)
				t.Parallel()
				surf := builder.bulk(handles)
				for i, k := range handles {
					val, ok := surf.Get(k)
					if !ok {
						t.Logf("%d %v", i, k)
					}
					require.True(t, ok)
					require.EqualValues(t, u16ToBytes(uint16(i)), val)
				}
			})
		}
	}
}

func TestMarshal(t *testing.T) {
	builder := NewBuilder(2, MixedSuffix, 7, 5)
	surf := builder.bulk(intKeys)
	buf := surf.Marshal()
	var surf1 SuRF
	surf1.Unmarshal(buf)

	surf.checkEquals(t, &surf1)
	for i, k := range intKeys {
		val, ok := surf1.Get(k)
		require.Equal(t, u16ToBytes(uint16(i)), val)
		require.True(t, ok)
	}
}

func TestIterator(t *testing.T) {
	for name, data := range datasets {
		func(name string, data [][]byte) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				builder := NewBuilder(2, NoneSuffix, 0, 0)
				surf := builder.bulk(data)
				it := surf.NewIterator()

				var i int
				for it.SeekToFirst(); it.Valid(); it.Next() {
					require.Truef(t, bytes.HasPrefix(data[i], it.Key()), "%d", i)
					require.Equal(t, u16ToBytes(uint16(i)), it.Value())
					i++
				}
			})
		}(name, data)
	}
}

func TestIteratorReverse(t *testing.T) {
	for name, data := range datasets {
		func(name string, data [][]byte) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				builder := NewBuilder(2, NoneSuffix, 0, 0)
				surf := builder.bulk(data)
				it := surf.NewIterator()

				i := len(data) - 1
				for it.SeekToLast(); it.Valid(); it.Prev() {
					require.True(t, bytes.HasPrefix(data[i], it.Key()))
					require.Equal(t, u16ToBytes(uint16(i)), it.Value())
					i--
				}
			})
		}(name, data)
	}
}

func TestIteratorSeekExist(t *testing.T) {
	for name, data := range datasets {
		func(name string, data [][]byte) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				builder := NewBuilder(2, NoneSuffix, 0, 0)
				surf := builder.bulk(data)
				it := surf.NewIterator()

				for i, k := range data {
					it.Seek(k)
					require.True(t, bytes.HasPrefix(data[i], it.Key()))
					require.Equal(t, u16ToBytes(uint16(i)), it.Value())
				}
			})
		}(name, data)
	}
}

func TestIteratorSeekAbsence(t *testing.T) {
	keys := make([][]byte, 10)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i * 10))
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	truc := truncateSuffixes(keys)
	builder := NewBuilder(2, RealSuffix, 0, 4)
	it := builder.bulk(keys).NewIterator()

	for i := 0; i < 100; i++ {
		key := []byte(strconv.Itoa(i))
		idx := sort.Search(len(keys), func(i int) bool {
			return bytes.Compare(keys[i], key) >= 0
		})

		fp := it.Seek(key)
		if idx == len(keys) || !bytes.Equal(truncateKey(truc[idx]), it.Key()) {
			require.Truef(t, fp, "%d", i)
			require.Equal(t, truncateKey(truc[idx-1]), it.Key())
			require.Equal(t, u16ToBytes(uint16(idx-1)), it.Value())
		} else {
			require.Equal(t, u16ToBytes(uint16(idx)), it.Value())
		}
	}

	largeThanMax := append([]byte{}, keys[len(keys)-1]...)
	largeThanMax[0]++
	fp := it.Seek(largeThanMax)
	require.False(t, it.Valid())
	require.False(t, fp)

	smallThanMin := append([]byte{}, keys[0]...)
	smallThanMin[0]--
	fp = it.Seek(smallThanMin)
	require.False(t, fp)
	require.True(t, it.Valid())
	require.Equal(t, truncateKey(truc[0]), it.Key())
}

func (v *rankVector) checkEquals(t *testing.T, o *rankVector) {
	require.Equal(t, v.numBits, o.numBits)
	require.Equal(t, v.lutSize(), o.lutSize())
	if v.numBits != 0 {
		require.Equal(t, v.bits, o.bits)
	}
	require.Equal(t, v.rankLut, o.rankLut)
}

func (v *selectVector) checkEquals(t *testing.T, o *selectVector) {
	require.Equal(t, v.numBits, o.numBits)
	require.Equal(t, v.numOnes, o.numOnes)
	require.Equal(t, v.lutSize(), o.lutSize())
	require.Equal(t, v.bits, o.bits)
	require.Equal(t, v.selectLut, o.selectLut)
}

func (v *suffixVector) checkEquals(t *testing.T, o *suffixVector) {
	require.Equal(t, v.numBits, o.numBits)
	if v.numBits != 0 {
		require.Equal(t, v.bits, o.bits)
	}
	require.Equal(t, v.suffixType, o.suffixType)
	require.Equal(t, v.hashSuffixLen, o.hashSuffixLen)
	require.Equal(t, v.realSuffixLen, o.realSuffixLen)
}

func (v *valueVector) checkEquals(t *testing.T, o *valueVector) {
	require.Equal(t, v.bytes, o.bytes)
	require.Equal(t, v.valueSize, o.valueSize)
}

func (v *labelVector) checkEquals(t *testing.T, o *labelVector) {
	require.Equal(t, v.labels, o.labels)
}

func (ld *loudsDense) checkEquals(t *testing.T, o *loudsDense) {
	require.Equal(t, ld.height, o.height)
	ld.labelVec.checkEquals(t, &o.labelVec.rankVector)
	ld.hasChildVec.checkEquals(t, &o.hasChildVec.rankVector)
	ld.isPrefixVec.checkEquals(t, &o.isPrefixVec.rankVector)
	ld.suffixes.checkEquals(t, &o.suffixes)
	ld.values.checkEquals(t, &o.values)
}

func (ls *loudsSparse) checkEquals(t *testing.T, o *loudsSparse) {
	require.Equal(t, ls.height, o.height)
	require.Equal(t, ls.startLevel, o.startLevel)
	require.Equal(t, ls.denseChildCount, o.denseChildCount)
	require.Equal(t, ls.denseNodeCount, o.denseNodeCount)
	ls.labelVec.checkEquals(t, &o.labelVec)
	ls.hasChildVec.checkEquals(t, &o.hasChildVec.rankVector)
	ls.loudsVec.checkEquals(t, &o.loudsVec)
	ls.suffixes.checkEquals(t, &o.suffixes)
	ls.values.checkEquals(t, &o.values)
}

func (s *SuRF) checkEquals(t *testing.T, o *SuRF) {
	s.ld.checkEquals(t, &o.ld)
	s.ls.checkEquals(t, &o.ls)
}

func (b *Builder) bulk(keys [][]byte) *SuRF {
	vals := make([][]byte, 0, len(keys))
	for i := range keys {
		vals = append(vals, u16ToBytes(uint16(i)))
	}
	return b.Build(keys, vals, 30)
}
