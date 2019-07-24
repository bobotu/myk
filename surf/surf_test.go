package surf

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
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
)

func initTexture() {
	intKeys = make([][]byte, 0, 1000000)
	for i := 0; i < 1000000; i++ {
		intKeys = append(intKeys, []byte(strconv.Itoa(i)))
	}
	sort.Slice(intKeys, func(i, j int) bool {
		return bytes.Compare(intKeys[i], intKeys[j]) < 0
	})
	intKeysTrunc = truncateSuffixes(intKeys)

	handles = make([][]byte, 0, 1000000)
	rnd := rand.New(rand.NewSource(0xdeadbeaf))
	for i := 0; i < 1000000; i++ {
		k := tablecodec.EncodeRowKeyWithHandle(rnd.Int63n(15)+1, rnd.Int63())
		handles = append(handles, k)
	}
	handlesRnd = make([][19]byte, len(handles))
	p := rand.New(rand.NewSource(0xdeadbeaf)).Perm(len(handles))
	for i, idx := range p {
		copy(handlesRnd[i][:], handles[idx])
	}
}

func TestMain(m *testing.M) {
	initTexture()
	os.Exit(m.Run())
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
				surf := builder.bulk(intKeys)
				for i, k := range intKeys {
					val, ok := surf.Get(k)
					require.EqualValues(t, u16ToBytes(uint16(i)), val)
					require.True(t, ok)
				}
			})
		}
	}
}

func TestEmptySuRF(t *testing.T) {
	builder := NewBuilder(2, HashSuffix, 2, 0)
	surf := builder.Finish(20)
	_, ok := surf.Get([]byte{1, 2})
	require.False(t, ok)
	it := surf.NewIterator()
	it.SeekToFirst()
	require.False(t, it.Valid())
	it.Seek([]byte{1, 2, 3, 4})
	require.False(t, it.Valid())
	it.SeekToLast()
	require.False(t, it.Valid())
	require.False(t, surf.HasRange([]byte{}, []byte{12}))

	buf := surf.Marshal()
	var surf1 SuRF
	surf1.Unmarshal(buf)
	_, ok = surf1.Get([]byte{1, 2})
	require.False(t, ok)
	it = surf.NewIterator()
	it.SeekToFirst()
	require.False(t, it.Valid())
	it.Seek([]byte{1, 2, 3, 4})
	require.False(t, it.Valid())
	it.SeekToLast()
	require.False(t, it.Valid())
	require.False(t, surf1.HasRange([]byte{}, []byte{12}))
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
	builder := NewBuilder(2, NoneSuffix, 0, 0)
	surf := builder.bulk(intKeys)
	it := surf.NewIterator()

	var i int
	for it.SeekToFirst(); it.Valid(); it.Next() {
		require.Equal(t, truncateKey(intKeysTrunc[i]), it.Key())
		require.Equal(t, u16ToBytes(uint16(i)), it.Value())
		i++
	}
}

func TestIteratorReverse(t *testing.T) {
	builder := NewBuilder(2, NoneSuffix, 0, 0)
	surf := builder.bulk(intKeys)
	it := surf.NewIterator()

	i := len(intKeys) - 1
	for it.SeekToLast(); it.Valid(); it.Prev() {
		require.Equal(t, truncateKey(intKeysTrunc[i]), it.Key(), i)
		require.Equal(t, u16ToBytes(uint16(i)), it.Value())
		i--
	}
}

func TestIteratorSeek(t *testing.T) {
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
			require.True(t, fp)
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
	for i, k := range keys {
		b.Add(k, u16ToBytes(uint16(i)))
	}
	return b.Finish(20)
}
