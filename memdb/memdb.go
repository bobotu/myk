package memdb

import (
	"bytes"
	"reflect"
	"unsafe"
)

type KeyFlags uint8

const (
	// bit 1 => red, bit 0 => black
	nodeColorBit  uint8 = 0x80
	nodeFlagsMark       = nodeColorBit - 1
)

type nodeAllocator struct {
	arena
	nullNode memdbNode
}

func (a *nodeAllocator) init() {
	a.nullNode = memdbNode{
		up:    nullAddr,
		left:  nullAddr,
		right: nullAddr,
		vptr:  nullAddr,
	}
}

func (a *nodeAllocator) getNode(addr arenaAddr) *memdbNode {
	if addr.isNull() {
		return &a.nullNode
	}

	return (*memdbNode)(unsafe.Pointer(&a.blocks[addr.idx].buf[addr.off]))
}

func (a *nodeAllocator) allocNode(key []byte) (arenaAddr, *memdbNode) {
	nodeSize := 8*4 + 2 + 1 + len(key)
	addr, mem := a.alloc(nodeSize)
	n := (*memdbNode)(unsafe.Pointer(&mem[0]))
	n.klen = uint16(len(key))
	copy(n.getKey(), key)
	return addr, n
}

func (a *nodeAllocator) freeNode(addr arenaAddr) {
	// TODO: we can reuse node's space.
}

type memdbNode struct {
	up    arenaAddr
	left  arenaAddr
	right arenaAddr
	vptr  arenaAddr
	klen  uint16
	flags uint8
}

func (n *memdbNode) isRed() bool {
	return n.flags&nodeColorBit != 0
}

func (n *memdbNode) isBlack() bool {
	return !n.isRed()
}

func (n *memdbNode) setRed() {
	n.flags |= nodeColorBit
}

func (n *memdbNode) setBlack() {
	n.flags &= ^nodeColorBit
}

func (n *memdbNode) getKey() []byte {
	var ret []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	hdr.Data = uintptr(unsafe.Pointer(&n.flags)) + 1
	hdr.Len = int(n.klen)
	hdr.Cap = int(n.klen)
	return ret
}

type memdb struct {
	root      arenaAddr
	allocator nodeAllocator
	vlog      memdbVlog
}

func newMemDB() *memdb {
	db := new(memdb)
	db.allocator.init()
	db.root = nullAddr
	return db
}

func (db *memdb) Get(key []byte) ([]byte, bool) {
	_, xn := db.tranverse(key, false)
	if xn == nil {
		return nil, false
	}
	if xn.vptr.isNull() {
		// A flag only key, act as value not exists
		return nil, false
	}
	return db.vlog.getValue(xn.vptr), true
}

func (db *memdb) Set(key, value []byte) {
	x, xn := db.tranverse(key, true)
	xn.vptr = db.vlog.appendValue(x, xn.vptr, value)
}

// tranverse search for and if not found and insert is true, will add a new node in.
// Returns a pointer to the new node, or the node found.
func (db *memdb) tranverse(key []byte, insert bool) (arenaAddr, *memdbNode) {
	var (
		x          = db.root
		y          = nullAddr
		z          = nullAddr
		xn, yn, zn *memdbNode
		found      = false
	)

	// walk x down the tree
	for !x.isNull() && !found {
		y = x
		xn = db.allocator.getNode(x)
		cmp := bytes.Compare(key, xn.getKey())
		if cmp < 0 {
			x = xn.left
		} else if cmp > 0 {
			x = xn.right
		} else {
			found = true
		}
	}

	if found || !insert {
		if x.isNull() {
			xn = nil
		}
		return x, xn
	}

	z, zn = db.allocator.allocNode(key)
	yn = db.allocator.getNode(y)
	zn.up = y

	if y.isNull() {
		db.root = z
	} else {
		cmp := bytes.Compare(zn.getKey(), yn.getKey())
		if cmp < 0 {
			yn.left = z
		} else {
			yn.right = z
		}
	}

	zn.left = nullAddr
	zn.right = nullAddr

	// colour this new node red
	zn.setRed()

	// Having added a red node, we must now walk back up the tree balancing it,
	// by a series of rotations and changing of colours
	x = z
	xn = zn

	a := db.allocator

	// While we are not at the top and our parent node is red
	// N.B. Since the root node is garanteed black, then we
	// are also going to stop if we are the child of the root

	for x != db.root {
		xupn := a.getNode(xn.up)
		if xupn.isBlack() {
			break
		}

		xgrandupn := a.getNode(xupn.up)
		// if our parent is on the left side of our grandparent
		if xn.up == xgrandupn.left {
			// get the right side of our grandparent (uncle?)
			y = xgrandupn.right
			yn = a.getNode(y)
			if yn.isRed() {
				// make our parent black
				xupn.setBlack()
				// make our uncle black
				yn.setBlack()
				// make our grandparent red
				xgrandupn.setRed()
				// now consider our grandparent
				x = xupn.up
				xn = xgrandupn
			} else {
				// if we are on the right side of our parent
				if x == xupn.right {
					// Move up to our parent
					x = xn.up
					xn = xupn
					db.leftRotate(x, xn)
					xupn = a.getNode(xn.up)
					xgrandupn = a.getNode(xupn.up)
				}

				xupn.setBlack()
				xgrandupn.setRed()
				db.rightRotate(xupn.up, xgrandupn)
			}
		} else {
			// everything here is the same as above, but exchanging left for right
			y = xgrandupn.left
			yn = a.getNode(y)
			if yn.isRed() {
				xupn.setBlack()
				yn.setBlack()
				xgrandupn.setRed()

				x = xupn.up
				xn = xgrandupn
			} else {
				if x == xupn.left {
					x = xn.up
					xn = xupn
					db.rightRotate(x, xn)
					xupn = a.getNode(xn.up)
					xgrandupn = a.getNode(xupn.up)
				}

				xupn.setBlack()
				xgrandupn.setRed()
				db.leftRotate(xupn.up, xgrandupn)
			}
		}
	}

	// Set the root node black
	a.getNode(db.root).setBlack()

	return z, zn
}

//
// Rotate our tree thus:-
//
//             X        leftRotate(X)--->           Y
//           /   \                                /   \
//          A     Y     <---rightRotate(Y)       X     C
//              /   \                          /   \
//             B     C                        A     B
//
// N.B. This does not change the ordering.
//
// We assume that neither X or Y is NULL
//

func (db *memdb) leftRotate(x arenaAddr, xn *memdbNode) {
	y := xn.right
	yn := db.allocator.getNode(y)

	// Turn Y's left subtree into X's right subtree (move B)
	xn.right = yn.left

	// If B is not null, set it's parent to be X
	if !yn.left.isNull() {
		db.allocator.getNode(yn.left).up = x
	}

	// Set Y's parent to be what X's parent was
	yn.up = xn.up

	// if X was the root
	if xn.up.isNull() {
		db.root = y
	} else {
		xupn := db.allocator.getNode(xn.up)
		// Set X's parent's left or right pointer to be Y
		if x == xupn.left {
			xupn.left = y
		} else {
			xupn.right = y
		}
	}

	// Put X on Y's left
	yn.left = x
	// Set X's parent to be Y
	xn.up = y
}

func (db *memdb) rightRotate(y arenaAddr, yn *memdbNode) {
	x := yn.left
	xn := db.allocator.getNode(x)

	// Turn X's right subtree into Y's left subtree (move B)
	yn.left = xn.right

	// If B is not null, set it's parent to be Y
	if !xn.right.isNull() {
		db.allocator.getNode(xn.right).up = y
	}

	// Set X's parent to be what Y's parent was
	xn.up = yn.up

	// if Y was the root
	if yn.up.isNull() {
		db.root = x
	} else {
		yupn := db.allocator.getNode(yn.up)
		// Set Y's parent's left or right pointer to be X
		if y == yupn.left {
			yupn.left = x
		} else {
			yupn.right = x
		}
	}

	// Put Y on X's right
	xn.right = y
	// Set Y's parent to be X
	yn.up = x
}

func (db *memdb) delete(key []byte) {
	x, xn := db.tranverse(key, false)
	if x.isNull() {
		return
	}
	db.deleteNode(x, xn)
}

func (db *memdb) deleteNode(z arenaAddr, zn *memdbNode) {
	var (
		x, y   arenaAddr
		xn, yn *memdbNode
	)

	if zn.left.isNull() || zn.right.isNull() {
		y = z
		yn = zn
	} else {
		y, yn = db.successor(z, zn)
	}

	if !yn.left.isNull() {
		x = yn.left
		xn = db.allocator.getNode(x)
	} else {
		x = yn.right
		xn = db.allocator.getNode(x)
	}

	xn.up = yn.up

	if yn.up.isNull() {
		db.root = x
	} else {
		yupn := db.allocator.getNode(yn.up)
		if y == yupn.left {
			yupn.left = x
		} else {
			yupn.right = x
		}
	}

	needFix := yn.isBlack()

	if y != z {
		db.replaceNode(y, yn, z, zn)
	}

	if needFix {
		db.deleteNodeFix(x, xn)
	}

	db.allocator.freeNode(z)
}

func (db *memdb) replaceNode(x arenaAddr, xn *memdbNode, y arenaAddr, yn *memdbNode) {
	if !yn.up.isNull() {
		yupn := db.allocator.getNode(yn.up)
		if y == yupn.left {
			yupn.left = x
		} else {
			yupn.right = x
		}
	} else {
		db.root = x
	}
	xn.up = yn.up

	if !yn.left.isNull() {
		db.allocator.getNode(yn.left).up = x
	}
	xn.left = yn.left

	if !yn.right.isNull() {
		db.allocator.getNode(yn.right).up = x
	}
	xn.right = yn.right

	if yn.isBlack() {
		xn.setBlack()
	} else {
		xn.setRed()
	}
}

func (db *memdb) deleteNodeFix(x arenaAddr, xn *memdbNode) {
	a := db.allocator
	for x != db.root && xn.isBlack() {
		xupn := a.getNode(xn.up)
		if x == xupn.left {
			w := xupn.right
			wn := a.getNode(w)
			if wn.isRed() {
				wn.setBlack()
				xupn.setRed()
				db.leftRotate(xn.up, xupn)
				w = xupn.right
				wn = a.getNode(w)
			}

			if a.getNode(wn.left).isBlack() && a.getNode(wn.right).isBlack() {
				wn.setRed()
				x = xn.up
				xn = xupn
			} else {
				if a.getNode(wn.right).isBlack() {
					a.getNode(wn.left).setBlack()
					wn.setRed()
					db.rightRotate(w, wn)
					xupn = a.getNode(xn.up)
					w = xupn.right
					wn = a.getNode(w)
				}

				if xupn.isBlack() {
					wn.setBlack()
				} else {
					wn.setRed()
				}
				xupn.setBlack()
				a.getNode(wn.right).setBlack()
				db.leftRotate(xn.up, xupn)
				x = db.root
				xn = a.getNode(x)
			}
		} else {
			w := xupn.left
			wn := a.getNode(w)
			if wn.isRed() {
				wn.setBlack()
				xupn.setRed()
				db.rightRotate(xn.up, xupn)
				w = xupn.left
				wn = a.getNode(w)
			}

			if a.getNode(wn.right).isBlack() && a.getNode(wn.left).isBlack() {
				wn.setRed()
				x = xn.up
				xn = xupn
			} else {
				if a.getNode(wn.left).isBlack() {
					a.getNode(wn.right).setBlack()
					wn.setRed()
					db.leftRotate(w, wn)
					xupn = a.getNode(xn.up)
					w = xupn.left
					wn = a.getNode(w)
				}

				if xupn.isBlack() {
					wn.setBlack()
				} else {
					wn.setRed()
				}
				xupn.setBlack()
				a.getNode(wn.left).setBlack()
				db.rightRotate(xn.up, xupn)
				x = db.root
				xn = a.getNode(x)
			}
		}
	}
	xn.setBlack()
}

func (db *memdb) successor(x arenaAddr, xn *memdbNode) (y arenaAddr, yn *memdbNode) {
	if !xn.right.isNull() {
		// If right is not NULL then go right one and
		// then keep going left until we find a node with
		// no left pointer.

		y = xn.right
		yn = db.allocator.getNode(y)
		for !yn.left.isNull() {
			y = yn.left
			yn = db.allocator.getNode(y)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// left of its parent (or the root) and then return the
	// parent.

	y = xn.up
	for !y.isNull() {
		yn = db.allocator.getNode(y)
		if x != yn.right {
			break
		}
		x = y
		y = yn.up
	}
	return
}

func (db *memdb) predecessor(x arenaAddr, xn *memdbNode) (y arenaAddr, yn *memdbNode) {
	if !xn.left.isNull() {
		// If left is not NULL then go left one and
		// then keep going right until we find a node with
		// no right pointer.

		y = xn.left
		yn = db.allocator.getNode(y)
		for !yn.right.isNull() {
			y = yn.right
			yn = db.allocator.getNode(y)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// right of its parent (or the root) and then return the
	// parent.

	y = xn.up
	for !y.isNull() {
		yn = db.allocator.getNode(y)
		if x != yn.left {
			break
		}
		x = y
		y = yn.up
	}
	return
}
