package surf

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSparseInt(t *testing.T) {
	suffixLens := []uint32{1, 3, 7, 8, 13}
	for _, sl := range suffixLens {
		builder := NewBuilder(2, RealSuffix, 0, sl)
		for i, k := range intKeys {
			builder.Add(k, u16ToBytes(uint16(i)))
		}
		builder.Finish(0)
		checkSparse(t, builder, intKeys, intKeysTrunc)
	}
}

func TestDenseInt(t *testing.T) {
	suffixLens := []uint32{1, 3, 7, 8, 13}
	for _, sl := range suffixLens {
		builder := NewBuilder(2, RealSuffix, 0, sl)
		for i, k := range intKeys {
			builder.Add(k, u16ToBytes(uint16(i)))
		}
		builder.sparseStartLevel = builder.treeHeight()
		builder.buildDense()
		builder.Finish(9999999)
		checkDense(t, builder, intKeys, intKeysTrunc)
	}
}

func checkSparse(t *testing.T, builder *Builder, keys, keysTrunc [][]byte) {
	for level := 0; uint32(level) < builder.treeHeight(); level++ {
		var pos, suffixBitPos uint32
		pos--
		for i := range keysTrunc {
			if level >= len(keysTrunc[i]) {
				continue
			}
			if prefixMatchInTrunc(keysTrunc, i-1, i, level+1) {
				continue
			}
			pos++

			label := keysTrunc[i][level]
			require.True(t, builder.lsLabels[level][pos] == label)

			hasChild := readBit(builder.lsHasChild[level], pos)
			samePrefixInPrevKey := prefixMatchInTrunc(keysTrunc, i-1, i, level+1)
			samePrefixInNextKey := prefixMatchInTrunc(keysTrunc, i, i+1, level+1)
			require.Equal(t, samePrefixInNextKey || samePrefixInPrevKey, hasChild)

			loudsBit := readBit(builder.lsLoudsBits[level], pos)
			if pos == 0 {
				require.True(t, loudsBit)
			} else {
				require.Equal(t, !prefixMatchInTrunc(keysTrunc, i-1, i, level), loudsBit)
			}

			if !hasChild {
				suffixLen := builder.suffixLen()
				var bitpos uint32
				if (uint32(len(keys[i])-level)-1)*8 >= suffixLen {
					for bitpos = 0; bitpos < suffixLen; bitpos++ {
						byteID := bitpos / 8
						byteOff := bitpos % 8
						byteMask := byte(0x80)
						byteMask >>= byteOff
						var expectedSuffixBit bool
						if level+1+int(byteID) < len(keys[i]) {
							expectedSuffixBit = keys[i][level+1+int(byteID)]&byteMask != 0
						}
						storedSuffixBit := readBit(builder.suffixes[level], suffixBitPos)
						require.Equal(t, expectedSuffixBit, storedSuffixBit)
						suffixBitPos++
					}
				} else {
					for bitpos = 0; bitpos < suffixLen; bitpos++ {
						storedSuffixBit := readBit(builder.suffixes[level], suffixBitPos)
						require.False(t, storedSuffixBit)
						suffixBitPos++
					}
				}
			}
		}
	}
}

func checkDense(t *testing.T, builder *Builder, keys, keysTrunc [][]byte) {
	for level := 0; uint32(level) < builder.sparseStartLevel; level++ {
		var prevLabel byte
		nodeNum := -1
		for i := 0; i < len(builder.lsLabels[level]); i++ {
			isNodeStart := readBit(builder.lsLoudsBits[level], uint32(i))
			if isNodeStart {
				nodeNum++
			}

			label := builder.lsLabels[level][i]
			existInNode := readBit(builder.ldLabels[level], uint32(nodeNum*denseFanout+int(label)))
			hasChildSparse := readBit(builder.lsHasChild[level], uint32(i))
			hasChildDense := readBit(builder.ldHasChild[level], uint32(nodeNum*denseFanout+int(label)))

			if isNodeStart {
				isPrefixKey := readBit(builder.ldIsPrefix[level], uint32(nodeNum))
				require.Equal(t, label == labelTerminator && !hasChildSparse, isPrefixKey)
				prevLabel = label
				continue
			}

			require.True(t, existInNode)
			require.Equal(t, hasChildSparse, hasChildDense)

			if isNodeStart {
				if nodeNum > 0 {
					for k := prevLabel + 1; uint(k) < denseFanout; k++ {
						existInNode = readBit(builder.ldLabels[level], uint32((nodeNum-1)*denseFanout+int(k)))
						require.False(t, existInNode)
						hasChildDense = readBit(builder.ldHasChild[level], uint32((nodeNum-1)*denseFanout+int(k)))
						require.False(t, hasChildDense)
					}
				}
				for k := 0; k < int(label); k++ {
					existInNode = readBit(builder.ldLabels[level], uint32(nodeNum*denseFanout+int(k)))
					require.False(t, existInNode)
					hasChildDense = readBit(builder.ldHasChild[level], uint32(nodeNum*denseFanout+int(k)))
					require.False(t, hasChildDense)
				}
			} else {
				for k := prevLabel + 1; k < label; k++ {
					existInNode = readBit(builder.ldLabels[level], uint32(nodeNum*denseFanout+int(k)))
					require.False(t, existInNode)
					hasChildDense = readBit(builder.ldHasChild[level], uint32(nodeNum*denseFanout+int(k)))
					require.False(t, hasChildDense)
				}
			}
			prevLabel = label
		}
	}
}

func prefixMatchInTrunc(keys [][]byte, i, j, depth int) bool {
	if i < 0 || i >= len(keys) {
		return false
	}
	if j < 0 || j >= len(keys) {
		return false
	}
	if depth <= 0 {
		return true
	}
	if len(keys[i]) < depth || len(keys[j]) < depth {
		return false
	}
	return bytes.Equal(keys[i][:depth], keys[j][:depth])
}

func truncateSuffixes(keys [][]byte) [][]byte {
	result := make([][]byte, 0, len(keys))
	commonPrefixLen := 0
	for i := 0; i < len(keys); i++ {
		if i == 0 {
			commonPrefixLen = getCommonPrefixLen(keys[i], keys[i+1])
		} else if i == len(keys)-1 {
			commonPrefixLen = getCommonPrefixLen(keys[i-1], keys[i])
		} else {
			commonPrefixLen = getCommonPrefixLen(keys[i-1], keys[i])
			b := getCommonPrefixLen(keys[i], keys[i+1])
			if b > commonPrefixLen {
				commonPrefixLen = b
			}
		}

		if commonPrefixLen < len(keys[i]) {
			result = append(result, keys[i][:commonPrefixLen+1])
		} else {
			k := make([]byte, 0, len(keys[i])+1)
			k = append(k, keys[i]...)
			result = append(result, append(k, labelTerminator))
		}
	}

	return result
}

func truncateKey(k []byte) []byte {
	if k[len(k)-1] == labelTerminator {
		k = k[:len(k)-1]
	}
	return k
}

func getCommonPrefixLen(a, b []byte) int {
	l := 0
	for l < len(a) && l < len(b) && a[l] == b[l] {
		l++
	}
	return l
}
