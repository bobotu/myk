package trb

import "bytes"

type Tree struct {
	h    nodeHeap
	d    dataStore
	root nodeAddr
}

func NewTree() *Tree {
	return &Tree{
		h:    newHeap(),
		d:    newDataStore(),
		root: nullNodeAddr,
	}
}

func (t *Tree) Get(key []byte) ([]byte, bool) {
	return t.get(t.root, key)
}

func (t *Tree) get(root nodeAddr, key []byte) ([]byte, bool) {
	curr := root
	for curr != nullNodeAddr {
		n := t.h.getNode(curr)
		cmp := bytes.Compare(key, t.d.getKey(n.data))
		if cmp < 0 {
			curr = n.left
		} else if cmp > 0 {
			curr = n.right
		} else {
			return t.d.getValue(n.data), true
		}
	}
	return nil, false
}

type node struct {
	left  nodeAddr
	right nodeAddr

	// TODO: replace with data ptr
	data dataAddr
}

func (t *Tree) insert(addr nodeAddr, key []byte, data dataAddr) (nodeAddr, bool) {
	if !addr.isNull() {
		n := t.h.getNode(addr)
		cmp := bytes.Compare(key, t.d.getKey(n.data))
		newNode, newAddr := t.h.getNodeForUpdate(addr)
		if cmp < 0 {
			newLeft, isNew := t.insert(n.left, key, data)
			newNode.left = newLeft
			if isNew {
				return t.balance(newAddr), isNew
			}
			return newAddr, isNew
		}
		if cmp > 0 {
			newRight, isNew := t.insert(n.right, key, data)
			newNode.right = newRight
			if isNew {
				return t.balance(newAddr), isNew
			}
			return newAddr, isNew
		}
		newNode.data = data
		return newAddr, false
	}

	newAddr := t.h.allocNode()
	newNode := t.h.getNode(newAddr)
	*newNode = node{
		left:  nullNodeAddr,
		right: nullNodeAddr,
		data:  data,
	}
	newAddr.setRed(true)

	return newAddr, true
}

func (t *Tree) balance(addr nodeAddr) nodeAddr {
	if addr.isRed() {
		return addr
	}
	n := t.h.getNode(addr)

	if !n.left.isNull() && n.left.isRed() {
		left := t.h.getNode(n.left)
		if !left.left.isNull() && left.left.isRed() {
			n, nAddr := t.h.getNodeForUpdate(addr)

			newRightAddr := t.h.allocNode()
			newRight := t.h.getNode(newRightAddr)
			*newRight = node{
				left:  left.right,
				right: n.right,
				data:  n.data,
			}
			newRightAddr.setRed(true)

			_, newLeftAddr := t.h.getNodeForUpdate(left.left)
			newLeftAddr.setRed(true)
			t.h.freeNode(n.left)

			*n = node{
				left:  newLeftAddr,
				right: newRightAddr,
				data:  left.data,
			}
			nAddr.setRed(true)
			return nAddr
		}

		if !left.right.isNull() && left.right.isRed() {
			n, nAddr := t.h.getNodeForUpdate(addr)
			leftright := t.h.getNode(left.right)

			newRightAddr := t.h.allocNode()
			newRight := t.h.getNode(newRightAddr)
			*newRight = node{
				left:  leftright.right,
				right: n.right,
				data:  n.data,
			}
			newRightAddr.setRed(false)

			leftrightAddr := left.right
			newLeft, newLeftAddr := t.h.getNodeForUpdate(n.left)
			newLeft.right = leftright.left
			newLeftAddr.setRed(false)
			t.h.freeNode(leftrightAddr)

			*n = node{
				left:  newLeftAddr,
				right: newRightAddr,
				data:  leftright.data,
			}
			nAddr.setRed(true)
			return nAddr
		}
	}

	if !n.right.isNull() && n.right.isRed() {
		right := t.h.getNode(n.right)
		if !right.left.isNull() && right.left.isRed() {
			n, nAddr := t.h.getNodeForUpdate(addr)
			rightleft := t.h.getNode(right.left)

			newLeftAddr := t.h.allocNode()
			newLeft := t.h.getNode(newLeftAddr)
			*newLeft = node{
				left:  n.left,
				right: rightleft.left,
				data:  n.data,
			}
			newLeftAddr.setRed(false)

			rightleftAddr := right.left
			newRight, newRightAddr := t.h.getNodeForUpdate(n.right)
			newRight.left = rightleft.right
			newRightAddr.setRed(false)
			t.h.freeNode(rightleftAddr)

			*n = node{
				left:  newLeftAddr,
				right: newRightAddr,
				data:  rightleft.data,
			}
			nAddr.setRed(true)
			return nAddr
		}

		if !right.right.isNull() && right.right.isRed() {
			n, nAddr := t.h.getNodeForUpdate(addr)

			newLeftAddr := t.h.allocNode()
			newLeft := t.h.getNode(newLeftAddr)
			*newLeft = node{
				left:  n.left,
				right: right.left,
				data:  n.data,
			}
			newLeftAddr.setRed(false)

			_, newRightAddr := t.h.getNodeForUpdate(right.right)
			newRightAddr.setRed(false)
			t.h.freeNode(n.right)

			*n = node{
				left:  newLeftAddr,
				right: newRightAddr,
				data:  right.data,
			}
			nAddr.setRed(true)
			return nAddr
		}
	}

	return addr
}

type Txn struct {
	t         *Tree
	root      nodeAddr
	storeTail dataAddr
}

func (t *Tree) Begin() *Txn {
	return &Txn{
		t:         t,
		root:      t.root,
		storeTail: t.d.getTail(),
	}
}

func (t *Txn) Insert(key, val []byte) {
	data := t.t.d.append(key, val)
	newRoot, _ := t.t.insert(t.root, key, data)
	newRoot.setRed(false)
	t.root = newRoot
}

func (t *Txn) Get(key []byte) ([]byte, bool) {
	return t.t.get(t.root, key)
}

func (t *Txn) Commit() {
	t.t.h.commit()
	t.t.d.setTail(t.storeTail)
	t.t.root = t.root
}

func (t *Txn) Rollback() {
	t.t.h.rollback()
	t.t.d.truncate(t.storeTail)
}
