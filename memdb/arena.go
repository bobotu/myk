package memdb

import (
	"math"
	"unsafe"
)

const (
	alignMask = 1<<32 - 8 // 29 bit 1 and 3 bit 0.

	nullBlockOffset = math.MaxUint32
	maxBlockSize    = 128 << 20
	initBlockSize   = 4 * 1024
)

var nullAddr = arenaAddr{math.MaxUint32, math.MaxUint32}

type arenaAddr struct {
	idx uint32
	off uint32
}

func (addr arenaAddr) isNull() bool {
	return addr == nullAddr
}

type arena struct {
	blockSize int
	blocks    []arenaBlock
	nullNode  memdbNode
}

func (a *arena) init() {
	a.nullNode = memdbNode{
		up:    nullAddr,
		left:  nullAddr,
		right: nullAddr,
		vptr:  nullAddr,
	}
}

func (a *arena) getNode(addr arenaAddr) *memdbNode {
	if addr.isNull() {
		return &a.nullNode
	}

	return (*memdbNode)(unsafe.Pointer(&a.blocks[addr.idx].buf[addr.off]))
}

func (a *arena) allocNode(key []byte) (arenaAddr, *memdbNode) {
	nodeSize := 8*4 + 2 + 1 + len(key)
	addr, mem := a.alloc(nodeSize)
	n := (*memdbNode)(unsafe.Pointer(&mem[0]))
	n.klen = uint16(len(key))
	copy(n.getKey(), key)
	return addr, n
}

func (a *arena) freeNode(addr arenaAddr) {
	// TODO: we can reuse node's space.
}

func (a *arena) alloc(size int) (arenaAddr, []byte) {
	if size > maxBlockSize {
		panic("alloc size is larger than max block size")
	}

	if len(a.blocks) == 0 {
		a.enlarge(size, initBlockSize)
	}

	addr, data := a.allocInLastBlock(size)
	if !addr.isNull() {
		return addr, data
	}

	a.enlarge(size, a.blockSize<<1)
	return a.allocInLastBlock(size)
}

func (a *arena) enlarge(allocSize, blockSize int) {
	a.blockSize = blockSize
	for a.blockSize <= allocSize {
		a.blockSize <<= 1
	}
	// Size will never larger than maxBlockSize.
	if a.blockSize > maxBlockSize {
		a.blockSize = maxBlockSize
	}
	a.blocks = append(a.blocks, newArenaBlock(a.blockSize))
}

func (a *arena) allocInLastBlock(size int) (arenaAddr, []byte) {
	idx := len(a.blocks) - 1
	offset, data := a.blocks[idx].alloc(size)
	if offset == nullBlockOffset {
		return nullAddr, nil
	}
	return arenaAddr{uint32(idx), offset}, data
}

type arenaBlock struct {
	buf    []byte
	length int
}

func newArenaBlock(blockSize int) arenaBlock {
	return arenaBlock{
		buf: make([]byte, blockSize),
	}
}

func (a *arenaBlock) alloc(size int) (uint32, []byte) {
	// We must align the allocated address for node
	// to make runtime.checkptrAlignment happy.
	offset := (a.length + 7) & alignMask
	newLen := offset + size
	if newLen > len(a.buf) {
		return nullBlockOffset, nil
	}
	a.length = newLen
	return uint32(offset), a.buf[offset : offset+size]
}