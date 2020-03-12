package trb

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

func TestSimpleCommit(t *testing.T) {
	tree := NewTree()
	keys := rand.Perm(1000000)

	txn := tree.Begin()
	for _, k := range keys {
		txn.Insert(i2b(k), i2b(k))
	}
	txn.Commit()

	for _, k := range keys {
		result, ok := tree.Get(i2b(k))
		if !ok || b2i(result) != k {
			t.Fatalf("failed to get %d", k)
		}
	}
}

func TestSimpleRollback(t *testing.T) {
	tree := NewTree()
	commit := []int{0, 2, 4, 6, 8, 10}
	txn := tree.Begin()
	for _, k := range commit {
		txn.Insert(i2b(k), i2b(k))
	}
	txn.Commit()

	rollback := []int{1, 3, 5, 7, 9}
	txn = tree.Begin()
	for _, k := range rollback {
		txn.Insert(i2b(k), i2b(k))
	}

	for _, k := range commit {
		result, ok := txn.Get(i2b(k))
		if !ok || b2i(result) != k {
			t.Fatalf("failed to get %d", k)
		}
	}
	for _, k := range rollback {
		result, ok := txn.Get(i2b(k))
		if !ok || b2i(result) != k {
			t.Fatalf("failed to get %d", k)
		}
	}
	txn.Rollback()

	for _, k := range commit {
		result, ok := tree.Get(i2b(k))
		if !ok || b2i(result) != k {
			t.Fatalf("failed to get %d", k)
		}
	}

	for _, k := range rollback {
		_, ok := tree.Get(i2b(k))
		if ok {
			t.Fatalf("get rollbacked key %d", k)
		}
	}
}

func i2b(u int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(u))
	return buf
}

func b2i(buf []byte) int {
	return int(binary.BigEndian.Uint64(buf))
}

const (
	keySize   = 16
	valueSize = 128
)

// func BenchmarkLargeIndex(b *testing.B) {
// 	buf := make([][valueSize]byte, 10000000)
// 	for i := range buf {
// 		binary.LittleEndian.PutUint32(buf[i][:], uint32(i))
// 	}
// 	tree := NewTree()
// 	b.ResetTimer()

// 	txn := tree.Begin()
// 	for i := range buf {
// 		txn.Insert(buf[i][:keySize], buf[i][:])
// 	}
// 	txn.Commit()
// }

func BenchmarkPut(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	tree := NewTree()
	b.ResetTimer()

	txn := tree.Begin()
	for i := range buf {
		txn.Insert(buf[i][:keySize], buf[i][:])
		// txn.Commit()
	}
}

func TestDebug(t *testing.T) {
	buf := make([][valueSize]byte, 100000)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	tree := NewTree()

	txn := tree.Begin()
	for i := range buf {
		txn.Insert(buf[i][:keySize], buf[i][:])
		// txn.Commit()
	}
}

func BenchmarkPutRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	tree := NewTree()
	b.ResetTimer()

	txn := tree.Begin()
	for i := range buf {
		txn.Insert(buf[i][:keySize], buf[i][:])
		// txn.Commit()
	}
}

func BenchmarkGet(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.BigEndian.PutUint32(buf[i][:], uint32(i))
	}

	tree := NewTree()
	b.ResetTimer()

	txn := tree.Begin()
	for i := range buf {
		txn.Insert(buf[i][:keySize], buf[i][:])
	}
	txn.Commit()

	b.ResetTimer()
	for i := range buf {
		tree.Get(buf[i][:keySize])
	}
}

func BenchmarkGetRandom(b *testing.B) {
	buf := make([][valueSize]byte, b.N)
	for i := range buf {
		binary.LittleEndian.PutUint32(buf[i][:], uint32(rand.Int()))
	}

	tree := NewTree()
	b.ResetTimer()

	txn := tree.Begin()
	for i := range buf {
		txn.Insert(buf[i][:keySize], buf[i][:])
	}
	txn.Commit()

	b.ResetTimer()
	for i := range buf {
		tree.Get(buf[i][:keySize])
	}
}
