package surf

import (
	"bytes"
	"io"
	"sort"
)

type labelVector struct {
	labels []byte
}

func (v *labelVector) Init(labelsPerLevel [][]byte, startLevel, endLevel uint32) {
	numBytes := 1
	for l := startLevel; l < endLevel; l++ {
		numBytes += len(labelsPerLevel[l])
	}
	v.labels = make([]byte, numBytes)

	var pos uint32
	for l := startLevel; l < endLevel; l++ {
		copy(v.labels[pos:], labelsPerLevel[l])
		pos += uint32(len(labelsPerLevel[l]))
	}
}

func (v *labelVector) GetLabel(pos uint32) byte {
	return v.labels[pos]
}

func (v *labelVector) Search(k byte, start, size uint32) (uint32, bool) {
	if size > 1 && v.labels[start] == labelTerminator {
		start++
		size--
	}

	end := start + size
	if end > uint32(len(v.labels)) {
		end = uint32(len(v.labels))
	}
	result := bytes.IndexByte(v.labels[start:end], k)
	if result < 0 {
		return start, false
	}
	return start + uint32(result), true
}

func (v *labelVector) SearchGreaterThan(label byte, pos, size uint32) (uint32, bool) {
	if size > 1 && v.labels[pos] == labelTerminator {
		pos++
		size--
	}

	result := sort.Search(int(size), func(i int) bool { return v.labels[pos+uint32(i)] > label })
	if uint32(result) == size {
		return pos + uint32(result) - 1, false
	}
	return pos + uint32(result), true
}

func (v *labelVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *labelVector) rawMarshalSize() int64 {
	return 4 + int64(len(v.labels))
}

func (v *labelVector) WriteTo(w io.Writer) error {
	var bs [4]byte
	endian.PutUint32(bs[:], uint32(len(v.labels)))
	if _, err := w.Write(bs[:]); err != nil {
		return err
	}
	if _, err := w.Write(v.labels); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *labelVector) Unmarshal(buf []byte) []byte {
	l := endian.Uint32(buf)
	v.labels = buf[4 : 4+l]
	return buf[align(int64(4+l)):]
}
