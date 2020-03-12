package trb

import (
	"math"
	"math/bits"
	"sync"
)

type nodeAddr uint64

const (
	offsetMask = uint64(1)<<6 - 1
	isRedMask  = uint64(1) << 6
	nullMask   = uint64(1) << 7

	nullNodeAddr = nodeAddr(nullMask)
)

func newNodeAddr(index, offset int) nodeAddr {
	return nodeAddr(uint64(index)<<8 | uint64(offset))
}

func (a nodeAddr) index() int {
	return int(a >> 8)
}

func (a nodeAddr) offset() int {
	return int(uint64(a) & offsetMask)
}

func (a nodeAddr) isRed() bool {
	return uint64(a)&isRedMask != 0
}

func (a *nodeAddr) setRed(red bool) {
	if red {
		*a = nodeAddr(uint64(*a) | isRedMask)
	} else {
		*a = nodeAddr(uint64(*a) & ^isRedMask)
	}
}

func (a nodeAddr) isNull() bool {
	return uint64(a)&nullMask != 0
}

var nodesPool = sync.Pool{
	New: func() interface{} { return make([]node, 64) },
}

type page struct {
	stateMap    uint64
	allocMap    uint64
	deletionMap uint64
	nodes       []node
}

func newPage() page {
	return page{
		stateMap:    math.MaxUint64,
		allocMap:    math.MaxUint64,
		deletionMap: math.MaxUint64,
		nodes:       nodesPool.Get().([]node),
	}
}

func (p *page) free() {
	nodesPool.Put(p.nodes)
}

func (p *page) commit() {
	free := p.stateMap ^ p.deletionMap
	p.stateMap = (p.stateMap & p.allocMap) | free
	p.deletionMap = p.stateMap
	p.allocMap = p.stateMap
}

func (p *page) rollback() {
	p.deletionMap = p.stateMap
	p.allocMap = p.stateMap
}

func (p *page) isDirty(offset int) bool {
	return ((p.stateMap^p.allocMap)>>offset)&1 == 1
}

func (p *page) getNode(offset int) *node {
	return &p.nodes[offset]
}

func (p *page) allocNode() int {
	if p.allocMap == 0 {
		return -1
	}
	offset := bits.TrailingZeros64(p.allocMap & -p.allocMap)
	p.allocMap &= p.allocMap - 1
	return offset
}

func (p *page) freeNode(offset int) {
	p.deletionMap |= uint64(1) << offset
}

const wordSize = 64

type nodeHeap struct {
	pages    []page
	dirtyMap []uint64
	freeMap  []uint64
}

func newHeap() nodeHeap {
	a := nodeHeap{
		pages:    make([]page, 0, 4),
		dirtyMap: make([]uint64, 0, 4),
		freeMap:  make([]uint64, 0, 4),
	}
	a.addPage()
	return a
}

func (h *nodeHeap) addPage() {
	h.pages = append(h.pages, newPage())
	if len(h.pages)/wordSize == len(h.dirtyMap) {
		h.dirtyMap = append(h.dirtyMap, 0)
		h.freeMap = append(h.freeMap, 0)
	}
	h.markPageFree(len(h.pages)-1, true)
}

func (h *nodeHeap) allocNode() nodeAddr {
	i := len(h.freeMap) - 1
	base := i * wordSize
	m := h.freeMap[i]
	for m != 0 {
		index := base + bits.TrailingZeros64(m&-m)
		p := &h.pages[index]
		off := p.allocNode()
		if off >= 0 {
			h.markPageDirty(index)
			if p.allocMap == 0 {
				h.freeMap[i] &= m - 1
			}
			return newNodeAddr(index, off)
		}
		m &= m - 1
		h.freeMap[i] = m
	}
	h.addPage()
	index := len(h.pages) - 1
	offset := h.pages[index].allocNode()
	h.markPageDirty(index)
	return newNodeAddr(index, offset)
}

func (h *nodeHeap) freeNode(addr nodeAddr) {
	h.markPageDirty(addr.index())
	h.pages[addr.index()].freeNode(addr.offset())
}

func (a *nodeHeap) getNode(addr nodeAddr) *node {
	if addr.isNull() {
		return nil
	}
	return a.pages[addr.index()].getNode(addr.offset())
}

func (h *nodeHeap) getNodeForUpdate(addr nodeAddr) (*node, nodeAddr) {
	if h.isNodeDirty(addr) {
		return h.getNode(addr), addr
	}

	newAddr := h.allocNode()
	newNode := h.getNode(newAddr)
	oldNode := h.getNode(addr)
	*newNode = *oldNode
	h.freeNode(addr)
	return newNode, newAddr
}

func (h *nodeHeap) isNodeDirty(addr nodeAddr) bool {
	return h.pages[addr.index()].isDirty(addr.offset())
}

func (h *nodeHeap) markPageDirty(index int) {
	wordOff := index / wordSize
	bitsOff := index % wordSize
	h.dirtyMap[wordOff] |= uint64(1) << bitsOff
}

func (h *nodeHeap) markPageFree(index int, isFree bool) {
	wordOff := index / wordSize
	bitsOff := index % wordSize
	mask := uint64(1) << bitsOff
	if isFree {
		h.freeMap[wordOff] |= mask
	} else {
		h.freeMap[wordOff] &= ^mask
	}
}

func (h *nodeHeap) commit() {
	for i, bs := range h.dirtyMap {
		if bs == 0 {
			continue
		}

		base := i * wordSize
		for bs != 0 {
			off := bits.TrailingZeros64(bs & -bs)
			p := &h.pages[base+off]
			p.commit()
			h.markPageFree(base+off, p.allocNode() != 0)
			bs &= bs - 1
		}

		h.dirtyMap[i] = 0
	}
}

func (h *nodeHeap) rollback() {
	for i, bs := range h.dirtyMap {
		if bs == 0 {
			continue
		}

		base := i * wordSize
		for bs != 0 {
			off := bits.TrailingZeros64(bs & -bs)
			p := &h.pages[base+off]
			p.rollback()
			h.markPageFree(base+off, p.allocNode() != 0)
			bs &= bs - 1
		}

		h.dirtyMap[i] = 0
	}
}
