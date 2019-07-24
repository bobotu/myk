package art

import (
	"context"
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
)

func putAndCheck(t *testing.T, a *ART, keys [][]byte) {
	for _, k := range keys {
		a.Put(k, k)
	}

	for _, k := range keys {
		v, ok := a.Get(k)
		require.True(t, ok)
		require.Equal(t, k, v)
	}
}

func sleep() {
	time.Sleep(10 * time.Millisecond)
}

func TestBasic(t *testing.T) {
	a := New()
	k, v := []byte("hello"), []byte("world")
	a.Put(k, v)
	v1, ok := a.Get(k)
	require.True(t, ok)
	require.Equal(t, v, v1)

	a.Delete([]byte("foobar"))
	v1, ok = a.Get(k)
	require.True(t, ok)
	require.Equal(t, v, v1)

	a.Delete(k)
	v1, ok = a.Get(k)
	require.False(t, ok)
}

func TestMoreKeys(t *testing.T) {
	a := New()
	es := genEntries(500000)
	for _, e := range es {
		a.Put(e.k[:], e.v)
	}
	for _, e := range es {
		v, ok := a.Get(e.k[:])
		require.True(t, ok)
		require.Equal(t, e.v, v)
	}
}

func TestPrefixLeaf(t *testing.T) {
	a := New()
	keys := [][]byte{
		{1},
		{1, 2, 3, 4},
		{1, 2},
		{1, 2, 3, 4, 5},
		{1, 2, 3},
		{2, 3, 4},
		{2, 3, 5},
		{2, 3},
		{3, 1},
		{3, 2, 3, 7, 5},
		{3, 2, 3, 4, 5},
		{3, 2},
	}

	putAndCheck(t, a, keys)
}

func TestEmptyKey(t *testing.T) {
	a := New()
	a.Put([]byte{}, []byte("empty"))
	v, ok := a.Get([]byte{})
	require.True(t, ok)
	require.Equal(t, []byte("empty"), v)

	a.Put(nil, []byte("nil"))
	v, ok = a.Get(nil)
	require.True(t, ok)
	require.Equal(t, []byte("nil"), v)

	v, ok = a.Get([]byte{})
	require.True(t, ok)
	require.Equal(t, []byte("nil"), v)
}

func TestExpandLeaf(t *testing.T) {
	a := New()
	keys := [][]byte{
		[]byte("abcdefghijklmn"),
		[]byte("abcdefghijklmnopq"),
		[]byte("abcdefg"),
		[]byte("abcdefghijklmn123"),
		[]byte("abcdefghijklmo123"),

		[]byte("deanthropomorphic"),
		[]byte("deanthropomorphism"),
		[]byte("deanthropomorphization"),
		[]byte("deanthropomorphize"),
	}

	putAndCheck(t, a, keys)
}

func TestCompressPath(t *testing.T) {
	a := New()

	k21 := []byte{2, 1}
	k12 := []byte{1, 2}
	k125 := []byte{1, 2, 5}
	k1237 := []byte{1, 2, 3, 7}
	k12345 := []byte{1, 2, 3, 4, 5}
	k12346 := []byte{1, 2, 3, 4, 6}

	a.Put(k21, k21)
	a.Put(k12, k12)
	a.Put(k125, k125)
	a.Put(k1237, k1237)
	a.Put(k12345, k12345)
	a.Put(k12346, k12346)

	a.Delete(k1237)
	a.Delete(k125)

	v, ok := a.Get(k12345)
	require.True(t, ok)
	require.Equal(t, k12345, v)

	v, ok = a.Get(k12346)
	require.True(t, ok)
	require.Equal(t, k12346, v)

	a.Delete(k21)
	v, ok = a.Get(k12)
	require.True(t, ok)
	require.Equal(t, k12, v)

	a.Delete(k12345)
	v, ok = a.Get(k12346)
	require.True(t, ok)
	require.Equal(t, k12346, v)

	a.Delete(k12)
	v, ok = a.Get(k12)
	require.False(t, ok)

	leaf := (*leaf)(unsafe.Pointer(a.root.firstChild()))
	require.Equal(t, uint8(typeLeaf), leaf.nodeType)
	require.Equal(t, k12346, leaf.key())
}

func TestGrowAndShrink(t *testing.T) {
	a := New()
	var keys [][]byte
	for i := 0; i < 256; i++ {
		keys = append(keys, []byte{byte(i)})
	}

	putAndCheck(t, a, keys[:4])
	require.Equal(t, uint8(typeNode4), a.root.nodeType)

	putAndCheck(t, a, keys[:16])
	require.Equal(t, uint8(typeNode16), a.root.nodeType)

	putAndCheck(t, a, keys[:48])
	require.Equal(t, uint8(typeNode48), a.root.nodeType)

	putAndCheck(t, a, keys)
	require.Equal(t, uint8(typeNode256), a.root.nodeType)

	for i := 0; i <= 256-node256MinSize; i++ {
		a.Delete([]byte{byte(i)})
	}
	require.Equal(t, uint8(typeNode48), a.root.nodeType)

	for i := 256 - node256MinSize + 1; i <= 256-node48MinSize; i++ {
		a.Delete([]byte{byte(i)})
	}
	require.Equal(t, uint8(typeNode16), a.root.nodeType)

	for i := 256 - node48MinSize + 1; i <= 256-node16MinSize; i++ {
		a.Delete([]byte{byte(i)})
	}
	require.Equal(t, uint8(typeNode4), a.root.nodeType)

	for i := 256 - node16MinSize + 1; i < 256; i++ {
		a.Delete([]byte{byte(i)})
	}
	require.Equal(t, uint8(typeNode4), a.root.nodeType)
}

func TestGetWhenPathExpand(t *testing.T) {
	// case 1: expand leaf node.
	a := New()
	a.Put([]byte{1, 2, 3, 4}, []byte{1, 2, 3, 4})
	err := failpoint.Enable("github.com/bobotu/myk/art/get-before-rLock-fp", "pause")
	require.Nil(t, err)
	go func() {
		sleep()
		a.Put([]byte{1, 2, 3}, []byte{1, 2, 3})
		err := failpoint.Disable("github.com/bobotu/myk/art/get-before-rLock-fp")
		require.Nil(t, err)
	}()
	v, ok := a.Get([]byte{1, 2, 3})
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3}, v)

	// case 2: split prefix.
	err = failpoint.Enable("github.com/bobotu/myk/art/get-before-rLock-fp", "1*return(0)->pause")
	require.Nil(t, err)
	go func() {
		sleep()
		a.Put([]byte{1, 2, 1, 2}, []byte{1, 2, 1, 2})
		err := failpoint.Disable("github.com/bobotu/myk/art/get-before-rLock-fp")
		require.Nil(t, err)
	}()
	v, ok = a.Get([]byte{1, 2, 1, 2})
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 1, 2}, v)

	// case 3: split prefix and pause after prefix check. Get should handle the structure modification happened during pause.
	a = New()
	a.Put([]byte{1, 2, 3, 4, 5}, []byte{1, 2, 3, 4, 5})
	a.Put([]byte{1, 2, 3, 4, 6}, []byte{1, 2, 3, 4, 6})
	err = failpoint.Enable("github.com/bobotu/myk/art/get-after-checkPrefix-fp", "1*return(0)->pause")
	require.Nil(t, err)
	go func() {
		sleep()
		a.Put([]byte{1, 2, 1, 2}, []byte{1, 2, 1, 2})
		err := failpoint.Disable("github.com/bobotu/myk/art/get-after-checkPrefix-fp")
		require.Nil(t, err)
	}()
	v, ok = a.Get([]byte{1, 2, 3, 4, 5})
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3, 4, 5}, v)
}

func TestGetWhenNodeObsoleted(t *testing.T) {
	var a *ART
	triggerGrow := func() {
		sleep()
		for i := 0; i < 5; i++ {
			a.Put([]byte{byte(i)}, []byte{byte(i)})
		}
		err := failpoint.Disable("github.com/bobotu/myk/art/get-before-rLock-fp")
		require.Nil(t, err)
	}

	a = New()
	err := failpoint.Enable("github.com/bobotu/myk/art/get-before-rLock-fp", "pause")
	require.Nil(t, err)
	go triggerGrow()

	v, ok := a.Get([]byte{4})
	require.True(t, ok)
	require.Equal(t, []byte{4}, v)

	a = New()
	a.Put([]byte{1, 2}, []byte{1, 2})
	a.Put([]byte{1, 2, 3}, []byte{1, 2, 3})
	err = failpoint.Enable("github.com/bobotu/myk/art/get-before-rLock-fp", "1*return(0)->pause")
	require.Nil(t, err)
	go triggerGrow()

	v, ok = a.Get([]byte{1, 2})
	require.True(t, ok)
	require.Equal(t, []byte{1, 2}, v)
}

func TestGetWhenNodeCompressed(t *testing.T) {
	var a *ART
	genTree := func() *ART {
		a := New()
		keys := [][]byte{
			{1, 2, 3, 4, 5},
			{1, 2, 3, 4, 6, 7},
			{1, 2, 3, 4, 6, 8},
		}
		putAndCheck(t, a, keys)
		return a
	}
	triggerCompress := func(fp string) {
		sleep()
		a.Delete([]byte{1, 2, 3, 4, 5})
		err := failpoint.Disable(fp)
		require.Nil(t, err)
	}

	// case 1: get will paused before check prefix, and a delete operation will compress the node at paused level.
	// Ideally, get should detect the conflict and return the correct value.
	a = genTree()
	err := failpoint.Enable("github.com/bobotu/myk/art/get-before-checkPrefix-fp", "2*return(0)->pause")
	require.Nil(t, err)
	go triggerCompress("github.com/bobotu/myk/art/get-before-checkPrefix-fp")

	v, ok := a.Get([]byte{1, 2, 3, 4, 6, 8})
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3, 4, 6, 8}, v)

	// case 2: get will pause after check prefix, and a delete operation will compress the node at paused level.
	// This case emulate normal node read-write conflict, get should return the correct result.
	a = genTree()
	err = failpoint.Enable("github.com/bobotu/myk/art/get-after-checkPrefix-fp", "2*return(0)->pause")
	require.Nil(t, err)
	go triggerCompress("github.com/bobotu/myk/art/get-after-checkPrefix-fp")

	v, ok = a.Get([]byte{1, 2, 3, 4, 6, 8})
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3, 4, 6, 8}, v)

	// case 3: get with an key which doesn't exist in target tree. After get resume from failpoint
	// the stored prefix will be updated due to path compression, and key will match with the updated prefix.
	// In this case, get should detect this kind of incorrect match and return key not found.
	a = genTree()
	err = failpoint.Enable("github.com/bobotu/myk/art/get-before-checkPrefix-fp", "2*return(0)->pause")
	require.Nil(t, err)
	go triggerCompress("github.com/bobotu/myk/art/get-before-checkPrefix-fp")

	v, ok = a.Get([]byte{1, 2, 3, 4, 6, 2, 3, 4, 6, 8})
	require.False(t, ok)
}

func TestWriteWhenPathExpand(t *testing.T) {
	inTrigger := false
	putTestCtx = failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return !inTrigger
	})

	test := func(fp string) {
		a := New()
		a.Put([]byte{1, 2, 3, 4}, []byte{1, 2, 3, 4})
		a.Put([]byte{1, 2, 3, 6}, []byte{1, 2, 3, 6})
		err := failpoint.Enable(fp, "1*return(0)->1*return(1000)->off")
		require.Nil(t, err)
		go func() {
			sleep()
			a.Put([]byte{1, 2, 1, 0}, []byte{1, 2, 1, 0})
			err := failpoint.Disable(fp)
			require.Nil(t, err)
		}()
		putAndCheck(t, a, [][]byte{{1, 2, 3, 5}})
	}

	test("github.com/bobotu/myk/art/set-before-prefixMismatch-fp")
	test("github.com/bobotu/myk/art/set-after-prefixMismatch-fp")
	test("github.com/bobotu/myk/art/set-before-incr-depth-fp")
}

type entry struct {
	k [19]byte
	v []byte
}

func genEntriesWithSeed(seed int64, n int) []*entry {
	result := make([]*entry, 0, n)
	rnd := rand.New(rand.NewSource(seed))
	for i := 0; i < n; i++ {
		k := tablecodec.EncodeRowKeyWithHandle(rnd.Int63n(15)+1, rnd.Int63())
		e := &entry{v: k}
		copy(e.k[:], k)
		result = append(result, e)
	}
	return result
}

func genEntries(n int) []*entry {
	return genEntriesWithSeed(0, n)
}

func shuffleKeys(es []*entry) [][19]byte {
	N := len(es)
	p := rand.New(rand.NewSource(0xdeadbeaf)).Perm(N)
	keys := make([][19]byte, N)
	for i := range keys {
		copy(keys[i][:], es[p[i]].k[:])
	}
	return keys
}
