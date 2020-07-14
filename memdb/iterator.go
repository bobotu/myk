package memdb

import "bytes"

type memdbIterator struct {
	db      *memdb
	curr    arenaAddr
	currn   *memdbNode
	start   Key
	end     Key
	reverse bool
}

func (db *memdb) Iter(k Key, upperBound Key) (*memdbIterator, error) {
	i := &memdbIterator{
		db:    db,
		start: k,
		end:   upperBound,
	}
	if len(i.start) == 0 {
		i.seekToFirst()
	} else {
		i.seek(i.start)
	}
	return i, nil
}

func (db *memdb) IterReverse(k Key) (*memdbIterator, error) {
	i := &memdbIterator{
		db:      db,
		end:     k,
		reverse: true,
	}
	if len(i.end) == 0 {
		i.seekToLast()
	} else {
		i.seek(i.end)
	}
	return i, nil
}

func (i *memdbIterator) Valid() bool {
	return !i.curr.isNull()
}

func (i *memdbIterator) Key() Key {
	return i.currn.getKey()
}

func (i *memdbIterator) Value() []byte {
	return i.db.vlog.getValue(i.currn.vptr)
}

func (i *memdbIterator) Next() error {
	if i.reverse {
		i.curr, i.currn = i.db.predecessor(i.curr, i.currn)
	} else {
		i.curr, i.currn = i.db.successor(i.curr, i.currn)
	}
	return nil
}

func (i *memdbIterator) Close() {}

func (i *memdbIterator) seekToFirst() {
	y := nullAddr
	x := i.db.root
	var yn *memdbNode

	for !x.isNull() {
		y = x
		yn = i.db.allocator.getNode(y)
		x = yn.left
	}

	i.curr = y
	i.currn = yn
}

func (i *memdbIterator) seekToLast() {
	y := nullAddr
	x := i.db.root
	var yn *memdbNode

	for !x.isNull() {
		y = x
		yn = i.db.allocator.getNode(y)
		x = yn.right
	}

	i.curr = y
	i.currn = yn
}

func (i *memdbIterator) seek(key Key) {
	var (
		y   = nullAddr
		x   = i.db.root
		yn  *memdbNode
		cmp int
	)

	for !x.isNull() {
		y = x
		yn = i.db.allocator.getNode(y)
		cmp = bytes.Compare(key, yn.getKey())

		if cmp < 0 {
			x = yn.left
		} else if cmp > 0 {
			x = yn.right
		} else {
			break
		}
	}

	if !i.reverse {
		if cmp > 0 {
			// Move to next
			i.curr, i.currn = i.db.successor(y, yn)
			return
		}
		i.curr = y
		i.currn = yn
		return
	}

	if cmp <= 0 {
		i.curr, i.currn = i.db.predecessor(y, yn)
		return
	}
	i.curr = y
	i.currn = yn
}
