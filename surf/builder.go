package surf

import (
	"bytes"
	"fmt"
)

// SuffixType is SuRF's suffix type.
type SuffixType uint8

const (
	// NoneSuffix means don't store suffix for keys.
	NoneSuffix SuffixType = iota
	// HashSuffix means store a small hash of keys.
	HashSuffix
	// RealSuffix means store a prefix of keys.
	RealSuffix
	// MixedSuffix means store a small hash with prefix of keys.
	MixedSuffix
)

func (st SuffixType) String() string {
	switch st {
	case HashSuffix:
		return "Hash"
	case RealSuffix:
		return "Real"
	case MixedSuffix:
		return "Mixed"
	default:
		return "None"
	}
}

const labelTerminator = 0xff

// Builder is a SuRF builder.
type Builder struct {
	sparseStartLevel uint32
	valueSize        uint32
	totalCount       int

	// LOUDS-Sparse bitvecs
	lsLabels    [][]byte
	lsHasChild  [][]uint64
	lsLoudsBits [][]uint64

	// LOUDS-Dense bitvecs
	ldLabels   [][]uint64
	ldHasChild [][]uint64
	ldIsPrefix [][]uint64

	// suffix
	suffixType    SuffixType
	hashSuffixLen uint32
	realSuffixLen uint32
	suffixes      [][]uint64
	suffixCounts  []uint32
	values        [][]byte
	valueCounts   []uint32

	nodeCounts           []uint32
	isLastItemTerminator []bool

	pendingKey   []byte
	pendingValue []byte
}

// NewBuilder returns a new builder with value size and suffix settings.
func NewBuilder(valueSize uint32, suffixType SuffixType, hashSuffixLen, realSuffixLen uint32) *Builder {
	switch suffixType {
	case HashSuffix:
		realSuffixLen = 0
	case RealSuffix:
		hashSuffixLen = 0
	case NoneSuffix:
		realSuffixLen = 0
		hashSuffixLen = 0
	}

	return &Builder{
		valueSize:     valueSize,
		suffixType:    suffixType,
		hashSuffixLen: hashSuffixLen,
		realSuffixLen: realSuffixLen,
	}
}

// Add add key and value to builder.
func (b *Builder) Add(key []byte, value []byte) {
	if bytes.Compare(b.pendingKey, key) >= 0 {
		panic(fmt.Sprintf("new added key %v >= prev key %v ", key, b.pendingKey))
	}
	b.totalCount++
	b.processPendingKey(key)
	b.pendingKey = append(b.pendingKey[:0], key...)
	b.pendingValue = value
}

// Finish build a new SuRF based on added kvs.
func (b *Builder) Finish(bitsPerKeyHint int) *SuRF {
	b.processPendingKey([]byte{})
	b.determineCutoffLevel(bitsPerKeyHint)
	b.buildDense()

	surf := new(SuRF)
	surf.ld.init(b)
	surf.ls.init(b)
	return surf
}

func (b *Builder) processPendingKey(curr []byte) {
	if len(b.pendingKey) == 0 {
		return
	}
	level := b.skipCommonPrefix(b.pendingKey)
	level = b.insertKeyIntoTrieUntilUnique(b.pendingKey, curr, level)
	b.insertSuffix(b.pendingKey, level)
	b.insertValue(b.pendingValue, level)
}

func (b *Builder) buildDense() {
	var level uint32
	for level = 0; level < b.sparseStartLevel; level++ {
		b.initDenseVectors(level)
		if b.numItems(level) == 0 {
			continue
		}

		var nodeID uint32
		if b.isTerminator(level, 0) {
			setBit(b.ldIsPrefix[level], 0)
		} else {
			b.setLabelAndHasChildVec(level, nodeID, 0)
		}

		var pos uint32
		numItems := b.numItems(level)
		for pos = 1; pos < numItems; pos++ {
			if b.isStartOfNode(level, pos) {
				nodeID++
				if b.isTerminator(level, pos) {
					setBit(b.ldIsPrefix[level], nodeID)
					continue
				}
			}
			b.setLabelAndHasChildVec(level, nodeID, pos)
		}
	}
}

func (b *Builder) setLabelAndHasChildVec(level, nodeID, pos uint32) {
	label := b.lsLabels[level][pos]
	setBit(b.ldLabels[level], nodeID*denseFanout+uint32(label))
	if readBit(b.lsHasChild[level], pos) {
		setBit(b.ldHasChild[level], nodeID*denseFanout+uint32(label))
	}
}

func (b *Builder) initDenseVectors(level uint32) {
	vecLength := b.nodeCounts[level] * (denseFanout / wordSize)
	prefixVecLen := b.nodeCounts[level] / wordSize
	if b.nodeCounts[level]%wordSize != 0 {
		prefixVecLen++
	}

	b.ldLabels = append(b.ldLabels, make([]uint64, vecLength))
	b.ldHasChild = append(b.ldHasChild, make([]uint64, vecLength))
	b.ldIsPrefix = append(b.ldIsPrefix, make([]uint64, prefixVecLen))
}

func (b *Builder) isStartOfNode(level, pos uint32) bool {
	return readBit(b.lsLoudsBits[level], pos)
}

func (b *Builder) isTerminator(level, pos uint32) bool {
	label := b.lsLabels[level][pos]
	return (label == labelTerminator) && !readBit(b.lsHasChild[level], pos)
}

func (b *Builder) suffixLen() uint32 {
	return b.hashSuffixLen + b.realSuffixLen
}

func (b *Builder) treeHeight() uint32 {
	return uint32(len(b.nodeCounts))
}

func (b *Builder) numItems(level uint32) uint32 {
	return uint32(len(b.lsLabels[level]))
}

func (b *Builder) isLevelEmpty(level uint32) bool {
	return level >= b.treeHeight() || len(b.lsLabels[level]) == 0
}

func (b *Builder) determineCutoffLevel(bitsPerKeyHint int) {
	height := b.treeHeight()
	if height == 0 {
		return
	}

	sizeHint := uint64(b.totalCount * bitsPerKeyHint)
	suffixAndValueSize := uint64(b.totalCount) * uint64(b.suffixLen())
	var level uint32
	for level = height - 1; level > 0; level-- {
		ds := b.denseSizeNoSuffix(level)
		ss := b.sparseSizeNoSuffix(level)
		sz := ds + ss + suffixAndValueSize
		if sz <= sizeHint {
			break
		}
	}
	b.sparseStartLevel = level
}

func (b *Builder) denseSizeNoSuffix(level uint32) uint64 {
	var total uint64
	for l := 0; uint32(l) < level; l++ {
		total += uint64(2 * denseFanout * b.nodeCounts[l])
		if l > 0 {
			total += uint64(b.nodeCounts[l-1])
		}
	}
	return total
}

func (b *Builder) sparseSizeNoSuffix(level uint32) uint64 {
	var total uint64
	height := b.treeHeight()
	for l := level; l < height; l++ {
		n := uint64(len(b.lsLabels[l]))
		total += n*8 + 2*n
	}
	return total
}

func (b *Builder) insertSuffix(key []byte, level uint32) {
	if level >= b.treeHeight() {
		b.addLevel()
	}
	suffix := constructSuffix(key, level, b.suffixType, b.realSuffixLen, b.hashSuffixLen)

	suffixLen := b.suffixLen()
	suffixLevel := level - 1
	pos := b.suffixCounts[suffixLevel] * suffixLen
	if pos == uint32(len(b.suffixes[suffixLevel])*wordSize) {
		b.suffixes[suffixLevel] = append(b.suffixes[suffixLevel], 0)
	}
	wordID := pos / wordSize
	offset := pos % wordSize
	remain := wordSize - offset
	if suffixLen <= remain {
		shift := remain - suffixLen
		b.suffixes[suffixLevel][wordID] += suffix << shift
	} else {
		left := suffix >> (suffixLen - remain)
		right := suffix << (wordSize - (suffixLen - remain))
		b.suffixes[suffixLevel][wordID] += left
		b.suffixes[suffixLevel] = append(b.suffixes[suffixLevel], right)
	}
	b.suffixCounts[suffixLevel]++
}

func (b *Builder) insertValue(value []byte, level uint32) {
	valueLevel := level - 1
	b.values[valueLevel] = append(b.values[valueLevel], value[:b.valueSize]...)
	b.valueCounts[valueLevel]++
}

func (b *Builder) skipCommonPrefix(key []byte) uint32 {
	var level uint32
	for level < uint32(len(key)) && b.isCharCommonPrefix(key[level], level) {
		setBit(b.lsHasChild[level], b.numItems(level)-1)
		level++
	}
	return level
}

func (b *Builder) isCharCommonPrefix(c byte, level uint32) bool {
	return (level < b.treeHeight()) && (!b.isLastItemTerminator[level]) &&
		(c == b.lsLabels[level][len(b.lsLabels[level])-1])
}

func (b *Builder) insertKeyIntoTrieUntilUnique(key, nextKey []byte, level uint32) uint32 {
	var isStartOfNode bool
	if b.isLevelEmpty(level) {
		// If it is the start of level, the louds bit needs to be set.
		isStartOfNode = true
	}

	b.insertByte(key[level], level, isStartOfNode, false)
	level++

	// build suffix.
	if level > uint32(len(nextKey)) || !bytes.Equal(key[:level], nextKey[:level]) {
		return level
	}

	isStartOfNode = true
	for level < uint32(len(key)) && level < uint32(len(nextKey)) && key[level] == nextKey[level] {
		b.insertByte(key[level], level, isStartOfNode, false)
		level++
	}

	// The last byte inserted makes key unique in the trie.
	if level < uint32(len(key)) {
		b.insertByte(key[level], level, isStartOfNode, false)
	} else {
		b.insertByte(labelTerminator, level, isStartOfNode, true)
	}
	level++
	return level
}

func (b *Builder) insertByte(c byte, level uint32, isStartOfNode, isTerm bool) {
	if level >= b.treeHeight() {
		b.addLevel()
	}

	if level > 0 {
		setBit(b.lsHasChild[level-1], b.numItems(level-1)-1)
	}

	b.lsLabels[level] = append(b.lsLabels[level], c)
	if isStartOfNode {
		setBit(b.lsLoudsBits[level], b.numItems(level)-1)
		b.nodeCounts[level]++
	}
	b.isLastItemTerminator[level] = isTerm

	b.moveToNextItemSlot(level)
}

func (b *Builder) moveToNextItemSlot(level uint32) {
	if b.numItems(level)%wordSize == 0 {
		b.lsHasChild[level] = append(b.lsHasChild[level], 0)
		b.lsLoudsBits[level] = append(b.lsLoudsBits[level], 0)
	}
}

func (b *Builder) addLevel() {
	b.lsLabels = append(b.lsLabels, []byte{})
	b.lsHasChild = append(b.lsHasChild, []uint64{})
	b.lsLoudsBits = append(b.lsLoudsBits, []uint64{})
	b.suffixes = append(b.suffixes, []uint64{})
	b.suffixCounts = append(b.suffixCounts, 0)
	b.values = append(b.values, []byte{})
	b.valueCounts = append(b.valueCounts, 0)

	b.nodeCounts = append(b.nodeCounts, 0)
	b.isLastItemTerminator = append(b.isLastItemTerminator, false)

	level := b.treeHeight() - 1
	b.lsHasChild[level] = append(b.lsHasChild[level], 0)
	b.lsLoudsBits[level] = append(b.lsLoudsBits[level], 0)
}
