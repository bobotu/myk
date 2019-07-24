package surf

import (
	"bytes"
	"io"
	"unsafe"
)

const (
	denseFanout      = 256
	denseRankBlkSize = 512
)

type loudsDense struct {
	labelVec    rankVectorDense
	hasChildVec rankVectorDense
	isPrefixVec rankVectorDense
	suffixes    suffixVector
	values      valueVector

	// height is dense end level.
	height uint32
}

func (ld *loudsDense) init(builder *Builder) *loudsDense {
	ld.height = builder.sparseStartLevel

	numBitsPerLevel := make([]uint32, 0, ld.height)
	for level := 0; uint32(level) < ld.height; level++ {
		n := len(builder.ldLabels[level]) * wordSize
		numBitsPerLevel = append(numBitsPerLevel, uint32(n))
	}

	ld.labelVec.init(builder.ldLabels, numBitsPerLevel, 0, ld.height)
	ld.hasChildVec.init(builder.ldHasChild, numBitsPerLevel, 0, ld.height)
	ld.isPrefixVec.init(builder.ldIsPrefix, builder.nodeCounts, 0, ld.height)

	if builder.suffixType != NoneSuffix {
		hashLen := builder.hashSuffixLen
		realLen := builder.realSuffixLen
		suffixLen := hashLen + realLen
		numSuffixBitsPerLevel := make([]uint32, ld.height)
		for i := range numSuffixBitsPerLevel {
			numSuffixBitsPerLevel[i] = builder.suffixCounts[i] * suffixLen
		}
		ld.suffixes.init(builder.suffixType, hashLen, realLen, builder.suffixes, numSuffixBitsPerLevel, 0, ld.height)
	}

	ld.values.init(builder.values, builder.valueSize, 0, ld.height)

	return ld
}

func (ld *loudsDense) Get(key []byte) (sparseNode int64, value []byte, ok bool) {
	var nodeID, pos, level uint32
	for level = 0; level < ld.height; level++ {
		pos = nodeID * denseFanout
		if level >= uint32(len(key)) {
			if ld.isPrefixVec.IsSet(nodeID) {
				valPos := ld.suffixPos(pos, true)
				if ok = ld.suffixes.CheckEquality(valPos, key, level+1); ok {
					value = ld.values.Get(valPos)
				}
			}
			return -1, value, ok
		}
		pos += uint32(key[level])

		if !ld.labelVec.IsSet(pos) {
			return -1, nil, false
		}

		if !ld.hasChildVec.IsSet(pos) {
			valPos := ld.suffixPos(pos, false)
			if ok = ld.suffixes.CheckEquality(valPos, key, level+1); ok {
				value = ld.values.Get(valPos)

			}
			return -1, value, ok
		}

		nodeID = ld.childNodeID(pos)
	}

	return int64(nodeID), nil, true
}

func (ld *loudsDense) MemSize() uint32 {
	return uint32(unsafe.Sizeof(*ld)) + ld.labelVec.MemSize() +
		ld.hasChildVec.MemSize() + ld.isPrefixVec.MemSize() + ld.suffixes.MemSize()
}

func (ld *loudsDense) MarshalSize() int64 {
	return align(ld.rawMarshalSize())
}

func (ld *loudsDense) rawMarshalSize() int64 {
	return 4 + ld.labelVec.MarshalSize() + ld.hasChildVec.MarshalSize() + ld.isPrefixVec.MarshalSize() + ld.suffixes.MarshalSize()
}

func (ld *loudsDense) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], ld.height)

	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if err := ld.labelVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.hasChildVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.isPrefixVec.WriteTo(w); err != nil {
		return err
	}
	if err := ld.suffixes.WriteTo(w); err != nil {
		return err
	}

	padding := ld.MarshalSize() - ld.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (ld *loudsDense) Unmarshal(buf []byte) []byte {
	ld.height = endian.Uint32(buf)
	buf1 := buf[4:]
	buf1 = ld.labelVec.Unmarshal(buf1)
	buf1 = ld.hasChildVec.Unmarshal(buf1)
	buf1 = ld.isPrefixVec.Unmarshal(buf1)
	buf1 = ld.suffixes.Unmarshal(buf1)

	sz := align(int64(len(buf) - len(buf1)))
	return buf[sz:]
}

func (ld *loudsDense) childNodeID(pos uint32) uint32 {
	return ld.hasChildVec.Rank(pos)
}

func (ld *loudsDense) suffixPos(pos uint32, isPrefix bool) uint32 {
	nodeID := pos / denseFanout
	suffixPos := ld.labelVec.Rank(pos) - ld.hasChildVec.Rank(pos) + ld.isPrefixVec.Rank(nodeID) - 1

	// Correct off by one error when current have a leaf node at label 0.
	// Otherwise suffixPos will point to that leaf node's suffix.
	if isPrefix && ld.labelVec.IsSet(pos) && !ld.hasChildVec.IsSet(pos) {
		suffixPos--
	}
	return suffixPos
}

func (ld *loudsDense) nextPos(pos uint32) uint32 {
	return pos + ld.labelVec.DistanceToNextSetBit(pos)
}

func (ld *loudsDense) prevPos(pos uint32) (uint32, bool) {
	dist := ld.labelVec.DistanceToPrevSetBit(pos)
	if pos <= dist {
		return 0, true
	}
	return pos - dist, false
}

type denseIter struct {
	valid         bool
	searchComp    bool
	leftComp      bool
	rightComp     bool
	ld            *loudsDense
	sendOutNodeID uint32
	keyLen        uint32
	keyBuf        []byte
	posInTrie     []uint32
	atPrefixKey   bool
}

func (it *denseIter) next() {
	if it.ld.height == 0 {
		return
	}
	if it.atPrefixKey {
		it.atPrefixKey = false
		it.moveToLeftMostKey()
		return
	}

	pos := it.posInTrie[it.keyLen-1]
	nextPos := it.ld.nextPos(pos)

	for nextPos/denseFanout > pos/denseFanout {
		it.keyLen--
		if it.keyLen == 0 {
			it.valid = false
			return
		}
		pos = it.posInTrie[it.keyLen-1]
		nextPos = it.ld.nextPos(pos)
	}
	it.set(it.keyLen-1, nextPos)
	it.moveToLeftMostKey()
}

func (it *denseIter) prev() {
	if it.ld.height == 0 {
		return
	}
	if it.atPrefixKey {
		it.atPrefixKey = false
		it.keyLen--
	}
	pos := it.posInTrie[it.keyLen-1]
	prevPos, out := it.ld.prevPos(pos)
	if out {
		it.valid = false
		return
	}

	for prevPos/denseFanout < pos/denseFanout {
		nodeID := pos / denseFanout
		if it.ld.isPrefixVec.IsSet(nodeID) {
			it.atPrefixKey = true
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return
		}

		it.keyLen--
		if it.keyLen == 0 {
			it.valid = false
			return
		}
		pos = it.posInTrie[it.keyLen-1]
		prevPos, out = it.ld.prevPos(pos)
		if out {
			it.valid = false
			return
		}
	}
	it.set(it.keyLen-1, prevPos)
	it.moveToRightMostKey()
}

func (it *denseIter) seek(key []byte) bool {
	var nodeID, pos uint32
	for level := uint32(0); level < it.ld.height; level++ {
		pos = nodeID * denseFanout
		if level >= uint32(len(key)) {
			it.append(it.ld.nextPos(pos - 1))
			if it.ld.isPrefixVec.IsSet(nodeID) {
				it.atPrefixKey = true
			} else {
				it.moveToLeftMostKey()
			}
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return true
		}

		pos += uint32(key[level])
		it.append(pos)

		if !it.ld.labelVec.IsSet(pos) {
			it.next()
			return false
		}

		if !it.ld.hasChildVec.IsSet(pos) {
			return it.compareSuffixGreaterThan(key, pos, level+1)
		}

		nodeID = it.ld.childNodeID(pos)
	}

	it.sendOutNodeID = nodeID
	it.valid, it.searchComp, it.leftComp, it.rightComp = true, false, true, true
	return true
}

func (it *denseIter) key() []byte {
	l := it.keyLen
	if it.atPrefixKey {
		l--
	}
	return it.keyBuf[:l]
}

func (it *denseIter) value() []byte {
	valPos := it.ld.suffixPos(it.posInTrie[it.keyLen-1], it.atPrefixKey)
	return it.ld.values.Get(valPos)
}

func (it *denseIter) isComplete() bool {
	return it.searchComp && (it.leftComp && it.rightComp)
}

func (it *denseIter) init(ld *loudsDense) {
	it.ld = ld
	it.keyBuf = make([]byte, ld.height)
	it.posInTrie = make([]uint32, ld.height)
}

func (it *denseIter) reset() {
	it.valid = false
	it.keyLen = 0
	it.atPrefixKey = false
}

func (it *denseIter) append(pos uint32) {
	it.keyBuf[it.keyLen] = byte(pos % denseFanout)
	it.posInTrie[it.keyLen] = pos
	it.keyLen++
}

func (it *denseIter) set(level, pos uint32) {
	it.keyBuf[level] = byte(pos % denseFanout)
	it.posInTrie[level] = pos
}

func (it *denseIter) moveToLeftMostKey() {
	level := it.keyLen - 1
	pos := it.posInTrie[level]
	if !it.ld.hasChildVec.IsSet(pos) {
		it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
		return
	}

	for level < it.ld.height-1 {
		nodeID := it.ld.childNodeID(pos)
		if it.ld.isPrefixVec.IsSet(nodeID) {
			it.append(it.ld.nextPos(nodeID*denseFanout - 1))
			it.atPrefixKey = true
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return
		}

		pos = it.ld.nextPos(nodeID*denseFanout - 1)
		it.append(pos)

		// If trie branch terminates
		if !it.ld.hasChildVec.IsSet(pos) {
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return
		}

		level++
	}
	it.sendOutNodeID = it.ld.childNodeID(pos)
	it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, false, true
}

func (it *denseIter) moveToRightMostKey() {
	level := it.keyLen - 1
	pos := it.posInTrie[level]
	if !it.ld.hasChildVec.IsSet(pos) {
		it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
		return
	}

	var out bool
	for level < it.ld.height-1 {
		nodeID := it.ld.childNodeID(pos)
		pos, out = it.ld.prevPos((nodeID + 1) * denseFanout)
		if out {
			it.valid = false
			return
		}
		it.append(pos)

		// If trie branch terminates
		if !it.ld.hasChildVec.IsSet(pos) {
			it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
			return
		}

		level++
	}
	it.sendOutNodeID = it.ld.childNodeID(pos)
	it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, false
}

func (it *denseIter) setToFirstInRoot() {
	if it.ld.labelVec.IsSet(0) {
		it.posInTrie[0] = 0
		it.keyBuf[0] = 0
	} else {
		it.posInTrie[0] = it.ld.nextPos(0)
		it.keyBuf[0] = byte(it.posInTrie[0])
	}
	it.keyLen++
}

func (it *denseIter) setToLastInRoot() {
	it.posInTrie[0], _ = it.ld.prevPos(denseFanout)
	it.keyBuf[0] = byte(it.posInTrie[0])
	it.keyLen++
}

func (it *denseIter) compareSuffixGreaterThan(key []byte, pos, level uint32) bool {
	cmp := it.ld.suffixes.Compare(key, it.ld.suffixPos(pos, false), level)
	if cmp < 0 {
		it.next()
		return false
	}
	it.valid, it.searchComp, it.leftComp, it.rightComp = true, true, true, true
	return cmp == couldBePositive
}

func (it *denseIter) compare(key []byte) int {
	if it.atPrefixKey && (it.keyLen-1) < uint32(len(key)) {
		return -1
	}
	itKey := it.key()
	if len(itKey) > len(key) {
		return 1
	}
	cmp := bytes.Compare(itKey, key[:len(itKey)])
	if cmp != 0 {
		return cmp
	}
	if it.isComplete() {
		suffixPos := it.ld.suffixPos(it.posInTrie[it.keyLen-1], it.atPrefixKey)
		return it.ld.suffixes.Compare(key, suffixPos, it.keyLen)
	}
	return cmp
}
