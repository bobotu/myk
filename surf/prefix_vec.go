package surf

import (
	"bytes"
	"io"
)

type prefixVec struct {
	hasPrefixVec  rankVectorSparse
	prefixOffsets []byte
	prefixData    []byte
}

func (v *prefixVec) Init(hasPrefixBits [][]uint64, numNodesPerLevel []uint32, prefixes [][][]byte) {
	v.hasPrefixVec.Init(hasPrefixBits, numNodesPerLevel)

	var offset uint32
	for _, level := range prefixes {
		for _, prefix := range level {
			var buf [4]byte
			endian.PutUint32(buf[:], offset)
			v.prefixOffsets = append(v.prefixOffsets, buf[:]...)
			offset += uint32(len(prefix))
			v.prefixData = append(v.prefixData, prefix...)
		}
	}
}

func (v *prefixVec) CheckPrefix(key []byte, depth uint32, nodeID uint32) (uint32, bool) {
	prefix := v.GetPrefix(nodeID)
	if len(prefix) == 0 {
		return 0, true
	}

	if int(depth)+len(prefix) > len(key) {
		return 0, false
	}
	if !bytes.Equal(key[depth:depth+uint32(len(prefix))], prefix) {
		return 0, false
	}
	return uint32(len(prefix)), true
}

func (v *prefixVec) GetPrefix(nodeID uint32) []byte {
	if !v.hasPrefixVec.IsSet(nodeID) {
		return nil
	}

	prefixID := v.hasPrefixVec.Rank(nodeID) - 1
	start := endian.Uint32(v.prefixOffsets[prefixID*4:])
	end := uint32(len(v.prefixData))
	if int((prefixID+1)*4) < len(v.prefixOffsets) {
		end = endian.Uint32(v.prefixOffsets[(prefixID+1)*4:])
	}
	return v.prefixData[start:end]
}

func (v *prefixVec) WriteTo(w io.Writer) error {
	if err := v.hasPrefixVec.WriteTo(w); err != nil {
		return err
	}

	var length [8]byte
	endian.PutUint32(length[:4], uint32(len(v.prefixOffsets)))
	endian.PutUint32(length[4:], uint32(len(v.prefixData)))

	if _, err := w.Write(length[:]); err != nil {
		return err
	}
	if _, err := w.Write(v.prefixOffsets); err != nil {
		return err
	}
	if _, err := w.Write(v.prefixData); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *prefixVec) Unmarshal(b []byte) []byte {
	buf1 := v.hasPrefixVec.Unmarshal(b)
	var cursor int64
	offsetsLen := int64(endian.Uint32(buf1[cursor:]))
	cursor += 4
	dataLen := int64(endian.Uint32(buf1[cursor:]))
	cursor += 4

	v.prefixOffsets = buf1[cursor : cursor+offsetsLen]
	cursor += offsetsLen
	v.prefixData = buf1[cursor : cursor+dataLen]

	return b[v.MarshalSize():]
}

func (v *prefixVec) rawMarshalSize() int64 {
	return v.hasPrefixVec.MarshalSize() + 8 + int64(len(v.prefixOffsets)+len(v.prefixData))
}

func (v *prefixVec) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}
