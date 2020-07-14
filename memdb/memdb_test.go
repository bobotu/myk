package memdb

import (
	"encoding/binary"
	"testing"

	. "github.com/pingcap/check"
)

type testMemDBSuite struct{}

var _ = Suite(testMemDBSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (db *memdb) get4Test(key uint32) (uint32, bool) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(key))
	v, ok := db.Get(buf[:])
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint32(v), true
}

func (db *memdb) set4Test(key, value uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(key))
	db.Set(buf[:], buf[:])
}

func (db *memdb) del4Test(key uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(key))
	db.delete(buf[:])
}

func (s testMemDBSuite) TestSimpleGetSet(c *C) {
	db := newMemDB()

	const cnt = 10000
	for i := uint32(0); i < cnt; i++ {
		db.set4Test(i, i)
	}
	for i := uint32(0); i < cnt; i++ {
		v, ok := db.get4Test(i)
		c.Assert(ok, IsTrue)
		c.Assert(v, Equals, i)
	}
	for i := uint32(0); i < cnt; i++ {
		if i%3 == 0 {
			db.del4Test(i)
		}
	}
	for i := uint32(0); i < cnt; i++ {
		v, ok := db.get4Test(i)
		if i%3 == 0 {
			c.Assert(ok, IsFalse)
		} else {
			c.Assert(ok, IsTrue)
			c.Assert(v, Equals, i)
		}
	}
}
