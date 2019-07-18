package art

import (
	"context"
	"math/rand"
	"testing"
	"time"
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/tablecodec"
)

func Test(t *testing.T) { TestingT(t) }

type ArtSuite struct{}

var _ = Suite(&ArtSuite{})

func (s *ArtSuite) putAndCheck(c *C, t *ART, keys [][]byte) {
	for _, k := range keys {
		t.Put(k, k)
	}

	for _, k := range keys {
		v, ok := t.Get(k)
		c.Assert(ok, IsTrue)
		c.Assert(v, BytesEquals, k)
	}
}

func (s *ArtSuite) sleep() {
	time.Sleep(10 * time.Millisecond)
}

func (s *ArtSuite) TestBasic(c *C) {
	t := New()
	k, v := []byte("hello"), []byte("world")
	t.Put(k, v)
	v1, ok := t.Get(k)
	c.Assert(ok, IsTrue)
	c.Assert(v1, BytesEquals, v)

	t.Delete([]byte("foobar"))
	v1, ok = t.Get(k)
	c.Assert(ok, IsTrue)
	c.Assert(v1, BytesEquals, v)

	t.Delete(k)
	v1, ok = t.Get(k)
	c.Assert(ok, IsFalse)
}

func (s *ArtSuite) TestMoreKeys(c *C) {
	t := New()
	es := genEntries(500000)
	for _, e := range es {
		t.Put(e.k[:], e.v)
	}
	for _, e := range es {
		v, ok := t.Get(e.k[:])
		c.Assert(ok, IsTrue)
		c.Assert(v, BytesEquals, e.v)
	}
}

func (s *ArtSuite) TestPrefixLeaf(c *C) {
	t := New()
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

	s.putAndCheck(c, t, keys)
}

func (s *ArtSuite) TestEmptyKey(c *C) {
	t := New()
	t.Put([]byte{}, []byte("empty"))
	v, ok := t.Get([]byte{})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte("empty"))

	t.Put(nil, []byte("nil"))
	v, ok = t.Get(nil)
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte("nil"))
	v, ok = t.Get([]byte{})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte("nil"))
}

func (s *ArtSuite) TestExpandLeaf(c *C) {
	t := New()
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

	s.putAndCheck(c, t, keys)
}

func (s *ArtSuite) TestCompressPath(c *C) {
	t := New()

	k21 := []byte{2, 1}
	k12 := []byte{1, 2}
	k125 := []byte{1, 2, 5}
	k1237 := []byte{1, 2, 3, 7}
	k12345 := []byte{1, 2, 3, 4, 5}
	k12346 := []byte{1, 2, 3, 4, 6}

	t.Put(k21, k21)
	t.Put(k12, k12)
	t.Put(k125, k125)
	t.Put(k1237, k1237)
	t.Put(k12345, k12345)
	t.Put(k12346, k12346)

	t.Delete(k1237)
	t.Delete(k125)

	v, ok := t.Get(k12345)
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, k12345)

	v, ok = t.Get(k12346)
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, k12346)

	t.Delete(k21)
	v, ok = t.Get(k12)
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, k12)

	t.Delete(k12345)
	v, ok = t.Get(k12346)
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, k12346)

	t.Delete(k12)
	v, ok = t.Get(k12)
	c.Assert(ok, IsFalse)

	leaf := (*leaf)(unsafe.Pointer(t.root.firstChild()))
	c.Assert(leaf.nodeType, Equals, uint8(typeLeaf))
	c.Assert(leaf.key(), BytesEquals, k12346)
}

func (s *ArtSuite) TestGrowAndShrink(c *C) {
	t := New()
	var keys [][]byte
	for i := 0; i < 256; i++ {
		keys = append(keys, []byte{byte(i)})
	}

	s.putAndCheck(c, t, keys[:4])
	c.Assert(t.root.nodeType, Equals, uint8(typeNode4))

	s.putAndCheck(c, t, keys[:16])
	c.Assert(t.root.nodeType, Equals, uint8(typeNode16))

	s.putAndCheck(c, t, keys[:48])
	c.Assert(t.root.nodeType, Equals, uint8(typeNode48))

	s.putAndCheck(c, t, keys)
	c.Assert(t.root.nodeType, Equals, uint8(typeNode256))

	for i := 0; i <= 256-node256MinSize; i++ {
		t.Delete([]byte{byte(i)})
	}
	c.Assert(t.root.nodeType, Equals, uint8(typeNode48))

	for i := 256 - node256MinSize + 1; i <= 256-node48MinSize; i++ {
		t.Delete([]byte{byte(i)})
	}
	c.Assert(t.root.nodeType, Equals, uint8(typeNode16))

	for i := 256 - node48MinSize + 1; i <= 256-node16MinSize; i++ {
		t.Delete([]byte{byte(i)})
	}
	c.Assert(t.root.nodeType, Equals, uint8(typeNode4))

	for i := 256 - node16MinSize + 1; i < 256; i++ {
		t.Delete([]byte{byte(i)})
	}
	c.Assert(t.root.nodeType, Equals, uint8(typeNode4))
}

func (s *ArtSuite) TestGetWhenPathExpand(c *C) {
	// case 1: expand leaf node.
	t := New()
	t.Put([]byte{1, 2, 3, 4}, []byte{1, 2, 3, 4})
	err := failpoint.Enable("github.com/bobotu/myk/art/get-before-rLock-fp", "pause")
	c.Assert(err, IsNil)
	go func() {
		s.sleep()
		t.Put([]byte{1, 2, 3}, []byte{1, 2, 3})
		err := failpoint.Disable("github.com/bobotu/myk/art/get-before-rLock-fp")
		c.Assert(err, IsNil)
	}()
	v, ok := t.Get([]byte{1, 2, 3})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte{1, 2, 3})

	// case 2: split prefix.
	err = failpoint.Enable("github.com/bobotu/myk/art/get-before-rLock-fp", "1*return(0)->pause")
	c.Assert(err, IsNil)
	go func() {
		s.sleep()
		t.Put([]byte{1, 2, 1, 2}, []byte{1, 2, 1, 2})
		err := failpoint.Disable("github.com/bobotu/myk/art/get-before-rLock-fp")
		c.Assert(err, IsNil)
	}()
	v, ok = t.Get([]byte{1, 2, 1, 2})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte{1, 2, 1, 2})

	// case 3: split prefix and pause after prefix check. Get should handle the structure modification happened during pause.
	t = New()
	t.Put([]byte{1, 2, 3, 4, 5}, []byte{1, 2, 3, 4, 5})
	t.Put([]byte{1, 2, 3, 4, 6}, []byte{1, 2, 3, 4, 6})
	err = failpoint.Enable("github.com/bobotu/myk/art/get-after-checkPrefix-fp", "1*return(0)->pause")
	c.Assert(err, IsNil)
	go func() {
		s.sleep()
		t.Put([]byte{1, 2, 1, 2}, []byte{1, 2, 1, 2})
		err := failpoint.Disable("github.com/bobotu/myk/art/get-after-checkPrefix-fp")
		c.Assert(err, IsNil)
	}()
	v, ok = t.Get([]byte{1, 2, 3, 4, 5})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte{1, 2, 3, 4, 5})
}

func (s *ArtSuite) TestGetWhenNodeObsoleted(c *C) {
	var t *ART
	triggerGrow := func() {
		s.sleep()
		for i := 0; i < 5; i++ {
			t.Put([]byte{byte(i)}, []byte{byte(i)})
		}
		err := failpoint.Disable("github.com/bobotu/myk/art/get-before-rLock-fp")
		c.Assert(err, IsNil)
	}

	t = New()
	err := failpoint.Enable("github.com/bobotu/myk/art/get-before-rLock-fp", "pause")
	c.Assert(err, IsNil)
	go triggerGrow()

	v, ok := t.Get([]byte{4})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte{4})

	t = New()
	t.Put([]byte{1, 2}, []byte{1, 2})
	t.Put([]byte{1, 2, 3}, []byte{1, 2, 3})
	err = failpoint.Enable("github.com/bobotu/myk/art/get-before-rLock-fp", "1*return(0)->pause")
	c.Assert(err, IsNil)
	go triggerGrow()

	v, ok = t.Get([]byte{1, 2})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte{1, 2})
}

func (s *ArtSuite) TestGetWhenNodeCompressed(c *C) {
	var t *ART
	genTree := func() *ART {
		t := New()
		keys := [][]byte{
			{1, 2, 3, 4, 5},
			{1, 2, 3, 4, 6, 7},
			{1, 2, 3, 4, 6, 8},
		}
		s.putAndCheck(c, t, keys)
		return t
	}
	triggerCompress := func(fp string) {
		s.sleep()
		t.Delete([]byte{1, 2, 3, 4, 5})
		err := failpoint.Disable(fp)
		c.Assert(err, IsNil)
	}

	// case 1: get will paused before check prefix, and a delete operation will compress the node at paused level.
	// Ideally, get should detect the conflict and return the correct value.
	t = genTree()
	err := failpoint.Enable("github.com/bobotu/myk/art/get-before-checkPrefix-fp", "2*return(0)->pause")
	c.Assert(err, IsNil)
	go triggerCompress("github.com/bobotu/myk/art/get-before-checkPrefix-fp")

	v, ok := t.Get([]byte{1, 2, 3, 4, 6, 8})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte{1, 2, 3, 4, 6, 8})

	// case 2: get will pause after check prefix, and a delete operation will compress the node at paused level.
	// This case emulate normal node read-write conflict, get should return the correct result.
	t = genTree()
	err = failpoint.Enable("github.com/bobotu/myk/art/get-after-checkPrefix-fp", "2*return(0)->pause")
	c.Assert(err, IsNil)
	go triggerCompress("github.com/bobotu/myk/art/get-after-checkPrefix-fp")

	v, ok = t.Get([]byte{1, 2, 3, 4, 6, 8})
	c.Assert(ok, IsTrue)
	c.Assert(v, BytesEquals, []byte{1, 2, 3, 4, 6, 8})

	// case 3: get with an key which doesn't exist in target tree. After get resume from failpoint
	// the stored prefix will be updated due to path compression, and key will match with the updated prefix.
	// In this case, get should detect this kind of incorrect match and return key not found.
	t = genTree()
	err = failpoint.Enable("github.com/bobotu/myk/art/get-before-checkPrefix-fp", "2*return(0)->pause")
	c.Assert(err, IsNil)
	go triggerCompress("github.com/bobotu/myk/art/get-before-checkPrefix-fp")

	v, ok = t.Get([]byte{1, 2, 3, 4, 6, 2, 3, 4, 6, 8})
	c.Assert(ok, IsFalse)
}

func (s *ArtSuite) TestWriteWhenPathExpand(c *C) {
	inTrigger := false
	putTestCtx = failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return !inTrigger
	})

	test := func(fp string) {
		t := New()
		t.Put([]byte{1, 2, 3, 4}, []byte{1, 2, 3, 4})
		t.Put([]byte{1, 2, 3, 6}, []byte{1, 2, 3, 6})
		err := failpoint.Enable(fp, "1*return(0)->1*return(1000)->off")
		c.Assert(err, IsNil)
		go func() {
			s.sleep()
			t.Put([]byte{1, 2, 1, 0}, []byte{1, 2, 1, 0})
			err := failpoint.Disable(fp)
			c.Assert(err, IsNil)
		}()
		s.putAndCheck(c, t, [][]byte{{1, 2, 3, 5}})
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
