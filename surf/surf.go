package surf

import (
	"bytes"
	"io"
	"unsafe"
)

type SuRF struct {
	ld loudsDense
	ls loudsSparse
}

func (s *SuRF) Get(key []byte) ([]byte, bool) {
	cont, value, ok := s.ld.Get(key)
	if !ok || cont < 0 {
		return value, ok
	}
	return s.ls.Get(key, uint32(cont))
}

func (s *SuRF) HasRange(start, end []byte) bool {
	if s.ld.height == 0 && s.ls.height == 0 {
		return false
	}
	it := s.NewIterator()
	it.denseIter.seek(start)
	if !it.denseIter.valid {
		return false
	}
	if !it.denseIter.isComplete() {
		if !it.denseIter.searchComp {
			it.passToSparse()
			it.sparseIter.seek(start)
			if !it.sparseIter.valid {
				it.incrDenseIter()
			}
		} else if !it.denseIter.leftComp {
			it.passToSparse()
			it.sparseIter.moveToLeftMostKey()
		}
	}

	if !it.Valid() {
		return false
	}
	cmp := it.compare(end)
	if cmp == couldBePositive {
		return true
	}
	return cmp < 0
}

func (s *SuRF) MemSize() uint32 {
	return uint32(unsafe.Sizeof(*s)) + s.ld.MemSize() + s.ls.MemSize()
}

func (s *SuRF) MarshalSize() int64 {
	return s.ld.MarshalSize() + s.ls.MarshalSize() + s.ld.values.MarshalSize() + s.ls.values.MarshalSize()
}

func (s *SuRF) MarshalNoValueSize() int64 {
	return s.ld.MarshalSize() + s.ls.MarshalSize()
}

func (s *SuRF) Marshal() []byte {
	w := bytes.NewBuffer(make([]byte, 0, s.MarshalSize()))
	_ = s.WriteTo(w)
	return w.Bytes()
}

func (s *SuRF) WriteTo(w io.Writer) error {
	if err := s.ld.WriteTo(w); err != nil {
		return err
	}
	if err := s.ls.WriteTo(w); err != nil {
		return err
	}
	if err := s.ld.values.WriteTo(w); err != nil {
		return err
	}
	return s.ls.values.WriteTo(w)
}

func (s *SuRF) Unmarshal(b []byte) {
	b = s.ld.Unmarshal(b)
	b = s.ls.Unmarshal(b)
	b = s.ld.values.Unmarshal(b)
	s.ls.values.Unmarshal(b)
}

type Iterator struct {
	denseIter  denseIter
	sparseIter sparseIter
	keyBuf     []byte
}

func (s *SuRF) NewIterator() *Iterator {
	iter := new(Iterator)
	iter.denseIter.init(&s.ld)
	iter.sparseIter.init(&s.ls)
	return iter
}

func (it *Iterator) Valid() bool {
	return it.denseIter.valid && (it.denseIter.isComplete() || it.sparseIter.valid)
}

func (it *Iterator) Next() {
	if it.incrSparseIter() {
		return
	}
	it.incrDenseIter()
}

func (it *Iterator) Prev() {
	if it.decrSparseIter() {
		return
	}
	it.decrDenseIter()
}

func (it *Iterator) Seek(key []byte) bool {
	var fp bool
	it.Reset()

	if it.sparseIter.ls.height == 0 && it.denseIter.ld.height == 0 {
		return false
	}

	fp = it.denseIter.seek(key)
	if !it.denseIter.valid || it.denseIter.isComplete() {
		return fp
	}

	if !it.denseIter.searchComp {
		it.passToSparse()
		fp = it.sparseIter.seek(key)
		if !it.sparseIter.valid {
			it.incrDenseIter()
		}
		return fp
	} else if !it.denseIter.leftComp {
		it.passToSparse()
		it.sparseIter.moveToLeftMostKey()
		return fp
	}

	panic("invalid state")
}

func (it *Iterator) SeekToFirst() {
	it.Reset()
	if it.denseIter.ld.height > 0 {
		it.denseIter.setToFirstInRoot()
		it.denseIter.moveToLeftMostKey()
		if it.denseIter.leftComp {
			return
		}
		it.passToSparse()
		it.sparseIter.moveToLeftMostKey()
	} else if it.sparseIter.ls.height > 0 {
		it.sparseIter.setToFirstInRoot()
		it.sparseIter.moveToLeftMostKey()
	}
}

func (it *Iterator) SeekToLast() {
	it.Reset()
	if it.denseIter.ld.height > 0 {
		it.denseIter.setToLastInRoot()
		it.denseIter.moveToRightMostKey()
		if it.denseIter.rightComp {
			return
		}
		it.passToSparse()
		it.sparseIter.moveToRightMostKey()
	} else if it.sparseIter.ls.height > 0 {
		it.sparseIter.setToLastInRoot()
		it.sparseIter.moveToRightMostKey()
	}
}

func (it *Iterator) Key() []byte {
	if it.denseIter.isComplete() {
		return it.denseIter.key()
	}
	it.keyBuf = append(it.keyBuf[:0], it.denseIter.key()...)
	return append(it.keyBuf, it.sparseIter.key()...)
}

func (it *Iterator) Value() []byte {
	if it.denseIter.isComplete() {
		return it.denseIter.value()
	}
	return it.sparseIter.value()
}

func (it *Iterator) Reset() {
	it.denseIter.reset()
	it.sparseIter.reset()
}

func (it *Iterator) passToSparse() {
	it.sparseIter.startNodeID = it.denseIter.sendOutNodeID
}

func (it *Iterator) incrDenseIter() bool {
	if !it.denseIter.valid {
		return false
	}

	it.denseIter.next()
	if !it.denseIter.valid {
		return false
	}
	if it.denseIter.leftComp {
		return true
	}

	it.passToSparse()
	it.sparseIter.moveToLeftMostKey()
	return true
}

func (it *Iterator) incrSparseIter() bool {
	if !it.sparseIter.valid {
		return false
	}
	it.sparseIter.next()
	return it.sparseIter.valid
}

func (it *Iterator) decrDenseIter() bool {
	if !it.denseIter.valid {
		return false
	}

	it.denseIter.prev()
	if !it.denseIter.valid {
		return false
	}
	if it.denseIter.rightComp {
		return true
	}

	it.passToSparse()
	it.sparseIter.moveToRightMostKey()
	return true
}

func (it *Iterator) decrSparseIter() bool {
	if !it.sparseIter.valid {
		return false
	}
	it.sparseIter.prev()
	return it.sparseIter.valid
}

func (it *Iterator) compare(key []byte) int {
	cmp := it.denseIter.compare(key)
	if it.denseIter.isComplete() || cmp != 0 {
		return cmp
	}
	return it.sparseIter.compare(key)
}
