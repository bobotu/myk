package art

import (
	"bytes"
	"math/bits"
	"reflect"
	"unsafe"
)

const (
	// if anyone create a new node without type it will be typeInvalid (0).
	typeInvalid = iota
	typeNode4
	typeNode16
	typeNode48
	typeNode256
	typeLeaf
	typeDummy
)

// checkPrefix assume maxPrefixLen is 8 to do a little hack.
// If you want to change it please take care of checkPrefix function.
const maxPrefixLen = 8

const (
	node16MinSize  = 4
	node48MinSize  = 13
	node256MinSize = 38
)

// node is the basic data of each node. Embedded into node{4, 16, 48, 256}.
type node struct {
	nodeType uint8

	// numChildren is number of children except prefixLeaf.
	numChildren uint8

	// prefixLen and prefix is the optimistic path compression.
	prefixLen uint32

	// version is the optimistic lock.
	version uint64

	// prefixLeaf store value of key which is prefix of other keys.
	// eg. [1]'s value will store here when [1, 0] exist.
	prefixLeaf *leaf

	prefix [maxPrefixLen]byte
}

func (n *node) isFull() bool {
	switch n.nodeType {
	case typeNode4:
		return n.numChildren == 4
	case typeNode16:
		return n.numChildren == 16
	case typeNode48:
		return n.numChildren == 48
	case typeNode256:
		return false
	}

	panic("unreachable code")
}

type node4 struct {
	node
	keys     [4]byte
	children [4]*node
}

func newNode4() *node4 {
	n := new(node4)
	n.nodeType = typeNode4
	return n
}

func (n *node4) toNode() *node {
	return (*node)(unsafe.Pointer(n))
}

type node16 struct {
	node
	keys     [16]byte
	children [16]*node
}

func newNode16() *node16 {
	n := new(node16)
	n.nodeType = typeNode16
	return n
}

func (n *node16) toNode() *node {
	return (*node)(unsafe.Pointer(n))
}

const (
	node48EmptySlots = 0xffff000000000000
	node48GrowSlots  = 0xffffffff00000000
)

type node48 struct {
	node
	index    [256]int8
	children [48]*node
	slots    uint64
}

func newNode48() *node48 {
	n := new(node48)
	n.nodeType = typeNode48
	n.slots = node48EmptySlots
	return n
}

func (n *node48) toNode() *node {
	return (*node)(unsafe.Pointer(n))
}

func (n *node48) allocSlot() int {
	idx := 48 - bits.Len64(^n.slots)
	n.slots |= uint64(1) << (48 - uint(idx) - 1)
	return idx
}

func (n *node48) freeSlot(idx int) {
	n.slots &= ^(uint64(1) << (48 - uint(idx) - 1))
}

type node256 struct {
	node
	children [256]*node
}

func newNode256() *node256 {
	n := new(node256)
	n.nodeType = typeNode256
	return n
}

func (n *node256) toNode() *node {
	return (*node)(unsafe.Pointer(n))
}

type leaf struct {
	nodeType uint8
}

func newLeaf(key []byte, value []byte) *leaf {
	mem := make([]byte, 1+4+4+len(key)+len(value))
	mem[0] = byte(typeLeaf)
	cursor := 1
	*(*uint32)(unsafe.Pointer(&mem[cursor])) = uint32(len(key))
	cursor += 4
	copy(mem[cursor:], key)
	cursor += len(key)
	*(*uint32)(unsafe.Pointer(&mem[cursor])) = uint32(len(value))
	cursor += 4
	copy(mem[cursor:], value)
	return (*leaf)(unsafe.Pointer(&mem[0]))
}

func (l *leaf) toNode() *node {
	return (*node)(unsafe.Pointer(l))
}

func (l *leaf) key() []byte {
	var k []byte
	kl := int(*(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(l)) + 1)))

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	hdr.Data = uintptr(unsafe.Pointer(uintptr(unsafe.Pointer(l)) + 5))
	hdr.Len = kl
	hdr.Cap = kl

	return k
}

func (l *leaf) value() []byte {
	var v []byte
	kl := int(*(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(l)) + 1)))
	vl := int(*(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(l)) + uintptr(kl+5))))

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	hdr.Data = uintptr(unsafe.Pointer(uintptr(unsafe.Pointer(l)) + uintptr(kl+9)))
	hdr.Len = vl
	hdr.Cap = vl

	return v
}

func (l *leaf) match(key []byte) bool {
	return bytes.Equal(l.key(), key)
}

func (n *node) insertChild(key byte, child *node) {
	switch n.nodeType {
	case typeNode4:
		(*node4)(unsafe.Pointer(n)).insertChild(key, child)
	case typeNode16:
		(*node16)(unsafe.Pointer(n)).insertChild(key, child)
	case typeNode48:
		(*node48)(unsafe.Pointer(n)).insertChild(key, child)
	case typeNode256:
		(*node256)(unsafe.Pointer(n)).insertChild(key, child)
	default:
		panic("unreachable code")
	}
}

func (n *node4) insertChild(key byte, child *node) {
	n.keys[n.numChildren] = key
	n.children[n.numChildren] = child
	n.numChildren++
}

func (n *node16) insertChild(key byte, child *node) {
	n.keys[n.numChildren] = key
	n.children[n.numChildren] = child
	n.numChildren++
}

func (n *node48) insertChild(key byte, child *node) {
	pos := n.allocSlot()
	n.children[pos] = child
	n.index[key] = int8(pos + 1)
	n.numChildren++
}

func (n *node256) insertChild(key byte, child *node) {
	n.children[key] = child
	n.numChildren++
}

func (n *node) growAndInsert(key byte, child *node, nodeLoc **node) {
	switch n.nodeType {
	case typeNode4:
		(*node4)(unsafe.Pointer(n)).growAndInsert(key, child, nodeLoc)
	case typeNode16:
		(*node16)(unsafe.Pointer(n)).growAndInsert(key, child, nodeLoc)
	case typeNode48:
		(*node48)(unsafe.Pointer(n)).growAndInsert(key, child, nodeLoc)
	default:
		panic("unreachable code")
	}
}

func copyNode(newNode, n *node) {
	newNode.numChildren = n.numChildren
	newNode.prefixLen = n.prefixLen
	newNode.prefix = n.prefix
	newNode.prefixLeaf = n.prefixLeaf
}

func (n *node4) growAndInsert(key byte, child *node, nodeLoc **node) {
	newNode := newNode16()
	copy(newNode.keys[:], n.keys[:])
	copy(newNode.children[:], n.children[:])
	copyNode(newNode.toNode(), n.toNode())
	newNode.insertChild(key, child)
	*nodeLoc = newNode.toNode()
}

func (n *node16) growAndInsert(key byte, child *node, nodeLoc **node) {
	newNode := newNode48()
	copy(newNode.children[:], n.children[:])
	newNode.slots = node48GrowSlots
	for idx, k := range n.keys {
		newNode.index[k] = int8(idx) + 1
	}
	copyNode(newNode.toNode(), n.toNode())
	newNode.insertChild(key, child)
	*nodeLoc = newNode.toNode()
}

func (n *node48) growAndInsert(key byte, child *node, nodeLoc **node) {
	newNode := newNode256()
	for i := range newNode.children {
		if idx := n.index[i]; idx > 0 {
			newNode.children[i] = n.children[idx-1]
		}
	}
	copyNode(newNode.toNode(), n.toNode())
	newNode.insertChild(key, child)
	*nodeLoc = newNode.toNode()
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	} else {
		return b
	}
}

func (l *leaf) updateOrExpand(key []byte, value []byte, depth uint32, nodeLoc **node) {
	if l.match(key) {
		*nodeLoc = newLeaf(key, value).toNode()
		return
	}

	var (
		missPos   uint32
		keyLen    = uint32(len(key))
		lkey      = l.key()
		lkeyLen   = uint32(len(lkey))
		prefixLen = min(keyLen, lkeyLen)
		newNode   = newNode4()
	)
	for missPos = depth; missPos < prefixLen; missPos++ {
		if lkey[missPos] != key[missPos] {
			break
		}
	}
	newNode.prefixLen = missPos - depth
	copy(newNode.prefix[:], key[depth:missPos])

	if missPos == lkeyLen {
		newNode.prefixLeaf = l
	} else {
		newNode.insertChild(lkey[missPos], l.toNode())
	}
	if missPos == keyLen {
		newNode.prefixLeaf = newLeaf(key, value)
	} else {
		newNode.insertChild(key[missPos], newLeaf(key, value).toNode())
	}
	*nodeLoc = newNode.toNode()
}

func (n *node) updatePrefixLeaf(key []byte, value []byte) {
	n.prefixLeaf = newLeaf(key, value)
}

func (n *node) removeChild(i int) {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		copy(n4.keys[i:], n4.keys[i+1:])
		copy(n4.children[i:], n4.children[i+1:])
		n4.numChildren--
		n4.children[n4.numChildren] = nil
	case typeNode16:
		n16 := (*node16)(unsafe.Pointer(n))
		copy(n16.keys[i:], n16.keys[i+1:])
		copy(n16.children[i:], n16.children[i+1:])
		n16.numChildren--
		n16.children[n16.numChildren] = nil
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		pos := int(n48.index[i] - 1)
		n48.index[i] = 0
		n48.children[pos] = nil
		n48.freeSlot(pos)
		n48.numChildren--
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		n256.children[i] = nil
		n256.numChildren--
	}
}

func (n *node) shouldShrink(parent *node) bool {
	switch n.nodeType {
	case typeNode4:
		if parent.nodeType == typeDummy {
			return false
		}
		if n.prefixLeaf == nil {
			return n.numChildren <= 2
		}
		return n.numChildren <= 1
	case typeNode16:
		return n.numChildren <= node16MinSize
	case typeNode48:
		return n.numChildren <= node48MinSize
	case typeNode256:
		// 256 will overflow to 0. But node256 never have 0 children,
		// so 0 simply means 256.
		return n.numChildren > 0 && n.numChildren <= node256MinSize
	default:
		panic("unreachable code.")
	}
}

func (n *node) removeChildAndShrink(key byte, nodeLoc **node) bool {
	switch n.nodeType {
	case typeNode4:
		return (*node4)(unsafe.Pointer(n)).removeChildAndShrink(key, nodeLoc)
	case typeNode16:
		return (*node16)(unsafe.Pointer(n)).removeChildAndShrink(key, nodeLoc)
	case typeNode48:
		return (*node48)(unsafe.Pointer(n)).removeChildAndShrink(key, nodeLoc)
	case typeNode256:
		return (*node256)(unsafe.Pointer(n)).removeChildAndShrink(key, nodeLoc)
	default:
		panic("unreachable code")
	}
}

func (n *node4) removeChildAndShrink(key byte, nodeLoc **node) bool {
	if n.prefixLeaf != nil {
		*nodeLoc = n.prefixLeaf.toNode()
		return true
	}

	if n.numChildren == 1 {
		*nodeLoc = newNode4().toNode()
		return true
	}

	if n.keys[0] == key {
		return n.compressChild(1, nodeLoc)
	}
	return n.compressChild(0, nodeLoc)
}

func (n *node4) compressChild(idx int, nodeLoc **node) bool {
	child := n.children[idx]
	if child.nodeType != typeLeaf {
		if !child.lock() {
			return false
		}
		prefixLen := n.prefixLen
		if prefixLen < maxPrefixLen {
			n.prefix[prefixLen] = n.keys[idx]
			prefixLen++
		}
		if prefixLen < maxPrefixLen {
			subPrefixLen := min(child.prefixLen, maxPrefixLen-prefixLen)
			copy(n.prefix[prefixLen:], child.prefix[:subPrefixLen])
			prefixLen += subPrefixLen
		}

		var tmp [maxPrefixLen]byte
		copy(tmp[:], n.prefix[:min(prefixLen, maxPrefixLen)])
		child.prefix = tmp
		child.prefixLen += n.prefixLen + 1
		child.unlock()
	}
	*nodeLoc = child
	return true
}

func (n *node16) removeChildAndShrink(key byte, nodeLoc **node) bool {
	newNode := newNode4()
	idx := 0
	for i := 0; i < int(n.numChildren); i++ {
		if n.keys[i] != key {
			newNode.keys[idx] = n.keys[i]
			newNode.children[idx] = n.children[i]
			idx++
		}
	}
	copyNode(newNode.toNode(), n.toNode())
	newNode.numChildren = node16MinSize - 1
	*nodeLoc = newNode.toNode()
	return true
}

func (n *node48) removeChildAndShrink(key byte, nodeLoc **node) bool {
	newNode := newNode16()
	idx := 0
	for i := 0; i < 256; i++ {
		if i != int(key) && n.index[i] != 0 {
			newNode.keys[idx] = uint8(i)
			newNode.children[idx] = n.children[n.index[i]-1]
			idx++
		}
	}
	copyNode(newNode.toNode(), n.toNode())
	newNode.numChildren = node48MinSize - 1
	*nodeLoc = newNode.toNode()
	return true
}

func (n *node256) removeChildAndShrink(key byte, nodeLoc **node) bool {
	newNode := newNode48()
	for i := 0; i < 256; i++ {
		if i != int(key) && n.children[i] != nil {
			pos := newNode.allocSlot()
			newNode.index[i] = int8(pos) + 1
			newNode.children[pos] = n.children[i]
		}
	}
	copyNode(newNode.toNode(), n.toNode())
	newNode.numChildren = node256MinSize - 1
	*nodeLoc = newNode.toNode()
	return true
}

func (n *node) shouldCompress(parent *node) bool {
	if n.nodeType == typeNode4 {
		return n.numChildren == 1 && parent.nodeType != typeDummy
	}
	return false
}

func (n *node) checkPrefix(key []byte, depth uint32) (uint32, bool) {
	if n.prefixLen == 0 {
		return depth, true
	}
	nextPos := depth + n.prefixLen
	if uint32(len(key)) < nextPos {
		return 0, false
	}

	var keyBuf [8]byte
	copy(keyBuf[:], key[depth:nextPos])
	k := *(*uint64)(unsafe.Pointer(&keyBuf[0]))
	p := *(*uint64)(unsafe.Pointer(&n.prefix[0]))

	return nextPos, k == p
}

func (n *node) findChild(key byte) (child *node, nodeLoc **node, position int) {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		for i := 0; i < int(n4.numChildren); i++ {
			if n4.keys[i] == key {
				return n4.children[i], &n4.children[i], i
			}
		}
	case typeNode16:
		n16 := (*node16)(unsafe.Pointer(n))
		if i := bytes.IndexByte(n16.keys[:], key); i >= 0 && i < int(n.numChildren) {
			return n16.children[i], &n16.children[i], i
		}
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		if idx := n48.index[key]; idx > 0 {
			return n48.children[idx-1], &n48.children[idx-1], int(key)
		}
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		return n256.children[key], &n256.children[key], int(key)
	}

	// Not found.
	return nil, nil, 0
}

func (n *node) insertSplitPrefix(key, fullKey []byte, value []byte, depth uint32, prefixLen uint32, nodeLoc **node) {
	newNode := newNode4()
	if depth := depth + prefixLen; uint32(len(key)) == depth {
		newNode.prefixLeaf = newLeaf(key, value)
	} else {
		newNode.insertChild(key[depth], newLeaf(key, value).toNode())
	}

	newNode.prefixLen = prefixLen
	copy(newNode.prefix[:min(maxPrefixLen, prefixLen)], n.prefix[:])

	// search require memory after n.prefixLen is zero, so copy prefix into an empty temp slice.
	var tmp [maxPrefixLen]byte
	var pos byte
	if n.prefixLen <= maxPrefixLen {
		pos = n.prefix[prefixLen]
		n.prefixLen -= prefixLen + 1
		copy(tmp[:n.prefixLen], n.prefix[prefixLen+1:])
	} else {
		pos = fullKey[depth+prefixLen]
		off := depth + prefixLen + 1
		n.prefixLen -= prefixLen + 1
		copy(tmp[:], fullKey[off:off+n.prefixLen])
	}
	newNode.insertChild(pos, n)
	n.prefix = tmp

	*nodeLoc = newNode.toNode()
}

func (n *node) fullKey(version uint64) ([]byte, bool) {
	curr := n
	for {
		if curr.prefixLeaf != nil {
			l := curr.prefixLeaf
			if !curr.rUnlock(version) {
				return nil, false
			}
			return l.key(), true
		}

		next := curr.firstChild()
		if !curr.lockCheck(version) {
			return nil, false
		}

		if next.nodeType == typeLeaf {
			l := (*leaf)(unsafe.Pointer(next))
			if !curr.rUnlock(version) {
				return nil, false
			}
			return l.key(), true
		}

		v, ok := next.rLock()
		if !ok {
			return nil, false
		}

		curr = next
		version = v
	}
}

// TODO: maybe we can tweak bytes comparison in this function.
func (n *node) prefixMismatch(key []byte, depth uint32, parent *node, version, parentVersion uint64) (uint32, []byte, bool) {
	if n.prefixLen <= maxPrefixLen {
		l := min(uint32(len(key))-depth, n.prefixLen)
		var idx uint32
		for idx = 0; idx < l; idx++ {
			if n.prefix[idx] != key[depth+idx] {
				break
			}
		}
		return idx, nil, true
	}

	// as prefix exceed maxPrefixLen bytes is omitted, we must load the full key from any leaf.
	var (
		fullKey []byte
		ok      bool
	)
	for {
		if !n.lockCheck(version) || !parent.lockCheck(parentVersion) {
			return 0, nil, false
		}
		if ok {
			break
		}
		fullKey, ok = n.fullKey(version)
	}

	i, l := depth, min(uint32(len(key)), depth+n.prefixLen)
	for ; i < l; i++ {
		if key[i] != fullKey[i] {
			break
		}
	}
	return i - depth, fullKey, true
}
