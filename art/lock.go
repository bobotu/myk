package art

import (
	"runtime"
	"sync/atomic"
)

const spinCount = 30

func (n *node) rLock() (uint64, bool) {
	v := n.waitUnlock()
	return v, v&1 != 1
}

func (n *node) lockCheck(version uint64) bool {
	return n.rUnlock(version)
}

func (n *node) rUnlock(version uint64) bool {
	return version == atomic.LoadUint64(&n.version)
}

func (n *node) rUnlockWithNode(version uint64, lockedNode *node) bool {
	if version != atomic.LoadUint64(&n.version) {
		lockedNode.unlock()
		return false
	}
	return true
}

func (n *node) upgradeToLock(version uint64) bool {
	return atomic.CompareAndSwapUint64(&n.version, version, version+2)
}

func (n *node) upgradeToLockWithNode(version uint64, lockedNode *node) bool {
	if !atomic.CompareAndSwapUint64(&n.version, version, version+2) {
		lockedNode.unlock()
		return false
	}
	return true
}

func (n *node) lock() bool {
	for {
		version, ok := n.rLock()
		if !ok {
			return false
		}
		if n.upgradeToLock(version) {
			break
		}
	}

	return true
}

func (n *node) unlock() {
	atomic.AddUint64(&n.version, 2)
}

func (n *node) unlockObsolete() {
	atomic.AddUint64(&n.version, 3)
}

func (n *node) waitUnlock() uint64 {
	v := atomic.LoadUint64(&n.version)
	count := spinCount
	for v&2 == 2 {
		if count <= 0 {
			runtime.Gosched()
			count = spinCount
		}
		count--
		v = atomic.LoadUint64(&n.version)
	}
	return v
}
