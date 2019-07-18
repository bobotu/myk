// Package art implements the Adaptive Radix Tree, and use Optimistic Locking method to achieve thread safe
// concurrent operation. The ART is described in "V. Leis, A. Kemper, and T. Neumann. The adaptive radix tree: ARTful
// indexing for main-memory databases. In ICDE, 2013". The Optimistic Syncing is described in "V. Leis, et al.,
// The ART of Practical Synchronization, in DaMoN, 2016".
package art

import (
	"context"
	"unsafe"

	"github.com/pingcap/failpoint"
)

// ART implements the Adaptive Radix Tree with Optimistic Locking.
// It looks like a KV data structure, which use byte slice as key.
// It support thread safe concurrent update and query.
type ART struct {
	dummy node
	root  *node
}

// OpFunc is ART query callback function.
// If OpFunc return true the current query will terminate immediately.
type OpFunc func(key []byte, value []byte) (end bool)

// New create a new empty ART.
func New() *ART {
	return &ART{
		dummy: node{nodeType: typeDummy},
		root:  newNode4().toNode(),
	}
}

// Get lookup this tree, and return the value associate with the given key.
// This operation is thread safe.
func (t *ART) Get(key []byte) ([]byte, bool) {
	for {
		if value, ex, ok := t.root.search(key, 0, &t.dummy, t.dummy.waitUnlock()); ok {
			return value, ex
		}
	}
}

// Put put the given key and value into this tree, or replace exist key's value.
// This operation is thread safe.
func (t *ART) Put(key []byte, value []byte) {
	for {
		if t.root.insert(key, value, 0, &t.dummy, t.dummy.waitUnlock(), &t.root) {
			return
		}
	}
}

// Delete delete the given key and it's value from this tree.
// This operation is thread safe.
func (t *ART) Delete(key []byte) {
	for {
		if t.root.remove(key, 0, &t.dummy, t.dummy.waitUnlock(), &t.root) {
			return
		}
	}
}

//go:norace
func (n *node) search(key []byte, depth uint32, parent *node, parentVersion uint64) ([]byte, bool, bool) {
	var (
		version  uint64
		ok       bool
		currNode = n
		keyLen   = uint32(len(key))
	)

	for {
		failpoint.Inject("get-before-rLock-fp", func() {})
		if version, ok = currNode.rLock(); !ok {
			return nil, false, false
		}
		if !parent.rUnlock(parentVersion) {
			return nil, false, false
		}

		failpoint.Inject("get-before-checkPrefix-fp", func() {})
		if depth, ok = currNode.checkPrefix(key, depth); !ok {
			return nil, false, currNode.rUnlock(version)
		}
		failpoint.Inject("get-after-checkPrefix-fp", func() {})

		var nextNode *node
		if depth == keyLen {
			nextNode = (*node)(unsafe.Pointer(currNode.prefixLeaf))
		} else if depth < keyLen {
			nextNode, _, _ = currNode.findChild(key[depth])
		}

		if !currNode.lockCheck(version) {
			return nil, false, false
		}

		if nextNode == nil {
			return nil, false, true
		}

		if nextNode.nodeType == typeLeaf {
			l := (*leaf)(unsafe.Pointer(nextNode))
			var (
				value []byte
				ex    bool
			)
			if l.match(key) {
				value = l.value()
				ex = true
			}
			return value, ex, true
		}

		depth += 1
		parent = currNode
		parentVersion = version
		currNode = nextNode
	}
}

// context used by failpoint in tests.
var putTestCtx context.Context

//go:norace
func (n *node) insert(key []byte, value []byte, depth uint32, parent *node, parentVersion uint64, nodeLoc **node) bool {
	var (
		version  uint64
		ok       bool
		nextNode *node
		nextLoc  **node
		currNode = n
	)

	for {
		if version, ok = currNode.rLock(); !ok {
			return false
		}

		failpoint.InjectContext(putTestCtx, "set-before-prefixMismatch-fp", func() {})
		p, fullKey, ok := currNode.prefixMismatch(key, depth, parent, version, parentVersion)
		if !ok {
			return false
		}
		failpoint.InjectContext(putTestCtx, "set-after-prefixMismatch-fp", func() {})

		// split current node due to prefix mismatch.
		if p != currNode.prefixLen {
			// update parent node, so lock it first.
			if !parent.upgradeToLock(parentVersion) {
				return false
			}
			if !currNode.upgradeToLockWithNode(version, parent) {
				return false
			}

			currNode.insertSplitPrefix(key, fullKey, value, depth, p, nodeLoc)

			currNode.unlock()
			parent.unlock()
			return true
		}
		failpoint.InjectContext(putTestCtx, "set-before-incr-depth-fp", func() {})
		depth += currNode.prefixLen

		if depth == uint32(len(key)) {
			if !currNode.upgradeToLock(version) {
				return false
			}
			// only modify current node, rUnlock parent.
			if !parent.rUnlockWithNode(parentVersion, currNode) {
				return false
			}

			currNode.updatePrefixLeaf(key, value)

			currNode.unlock()
			return true
		}

		nextNode, nextLoc, _ = currNode.findChild(key[depth])
		if !currNode.lockCheck(version) {
			return false
		}

		// no exist key, insert it directly.
		if nextNode == nil {
			if currNode.isFull() {
				if !parent.upgradeToLock(parentVersion) {
					return false
				}
				if !currNode.upgradeToLockWithNode(version, parent) {
					return false
				}

				currNode.growAndInsert(key[depth], newLeaf(key, value).toNode(), nodeLoc)

				currNode.unlockObsolete()
				parent.unlock()
			} else {
				if !currNode.upgradeToLock(version) {
					return false
				}
				if !parent.rUnlockWithNode(parentVersion, currNode) {
					return false
				}

				currNode.insertChild(key[depth], newLeaf(key, value).toNode())

				currNode.unlock()
			}
			return true
		}

		// step to next level.

		if !parent.rUnlock(parentVersion) {
			return false
		}

		if nextNode.nodeType == typeLeaf {
			if !currNode.upgradeToLock(version) {
				return false
			}

			l := (*leaf)(unsafe.Pointer(nextNode))
			l.updateOrExpand(key, value, depth+1, nextLoc)

			currNode.unlock()
			return true
		}

		depth += 1
		parent = currNode
		parentVersion = version
		nodeLoc = nextLoc
		currNode = nextNode
	}
}

//go:norace
func (n *node) remove(key []byte, depth uint32, parent *node, parentVersion uint64, nodeLoc **node) bool {
	var (
		version  uint64
		ok       bool
		currNode = n
	)

	for {
		if version, ok = currNode.rLock(); !ok {
			return false
		}
		if !parent.rUnlock(parentVersion) {
			return false
		}

		if depth, ok = currNode.checkPrefix(key, depth); !ok {
			return currNode.rUnlock(version)
		}

		// remove prefixLeaf, maybe compress current node.
		if depth == uint32(len(key)) {
			l := currNode.prefixLeaf
			if !currNode.lockCheck(version) {
				return false
			}
			if l == nil || !l.match(key) {
				return currNode.rUnlock(version)
			}

			// compress single way node, maybe restart.
			if currNode.shouldCompress(parent) {
				if !parent.upgradeToLock(parentVersion) {
					return false
				}
				if !currNode.upgradeToLockWithNode(version, parent) {
					return false
				}

				n4 := (*node4)(unsafe.Pointer(currNode))
				ok := n4.compressChild(0, nodeLoc)

				if !ok {
					currNode.unlock()
				} else {
					currNode.unlockObsolete()
				}
				parent.unlock()
				return ok
			}

			if !currNode.upgradeToLock(version) {
				return false
			}
			currNode.prefixLeaf = nil
			currNode.unlock()
			return true
		}

		if depth > uint32(len(key)) {
			return currNode.rUnlock(version)
		}

		nextNode, nextLoc, idx := currNode.findChild(key[depth])
		if !currNode.lockCheck(version) {
			return false
		}

		// not found.
		if nextNode == nil {
			return true
		}

		if nextNode.nodeType == typeLeaf {
			l := (*leaf)(unsafe.Pointer(nextNode))
			if !l.match(key) {
				return !currNode.rUnlock(version)
			}
			if currNode.shouldShrink(parent) {
				if !parent.upgradeToLock(parentVersion) {
					return false
				}
				if !currNode.upgradeToLockWithNode(version, parent) {
					return false
				}

				ok := currNode.removeChildAndShrink(key[depth], nodeLoc)

				if !ok {
					currNode.unlock()
				} else {
					currNode.unlockObsolete()
				}
				parent.unlock()
				return ok
			}
			if !currNode.upgradeToLock(version) {
				return false
			}
			currNode.removeChild(idx)
			currNode.unlock()
			return true
		}

		depth += 1
		parent = currNode
		parentVersion = version
		nodeLoc = nextLoc
		currNode = nextNode
	}
}
