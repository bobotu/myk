package surf

import (
	"io"

	"github.com/dgryski/go-farm"
)

const hashShift = 7

// Max suffix_len_ = 64 bits
// For kReal suffixes, if the stored key is not long enough to provide
// suffix_len_ suffix bits, its suffix field is cleared (i.e., all 0's)
// to indicate that there is no suffix info associated with the key.
type suffixVector struct {
	bitVector
	suffixType    SuffixType
	hashSuffixLen uint32
	realSuffixLen uint32
}

func (v *suffixVector) Init(suffixType SuffixType, hashLen, realLen uint32, bitsPerLevel [][]uint64, numBitsPerLevel []uint32) *suffixVector {
	v.bitVector.init(bitsPerLevel, numBitsPerLevel)
	v.suffixType = suffixType
	v.hashSuffixLen = hashLen
	v.realSuffixLen = realLen
	return v
}

func (v *suffixVector) CheckEquality(idx uint32, key []byte, level uint32) bool {
	if v.suffixType == NoneSuffix {
		return true
	}
	if idx*v.suffixLen() >= v.numBits {
		return false
	}

	suffix := v.read(idx)
	if v.suffixType == RealSuffix {
		if suffix == 0 {
			return true
		}
		if uint32(len(key)) < level || (uint32(len(key))-level)*8 < v.realSuffixLen {
			return false
		}
	}
	expected := constructSuffix(key, level, v.suffixType, v.realSuffixLen, v.hashSuffixLen)
	return suffix == expected
}

const couldBePositive = 2

func (v *suffixVector) Compare(key []byte, idx, level uint32) int {
	if idx*v.suffixLen() >= v.numBits || v.suffixType == NoneSuffix || v.suffixType == HashSuffix {
		return couldBePositive
	}

	suffix := v.read(idx)
	if v.suffixType == MixedSuffix {
		suffix = extractRealSuffix(suffix, v.realSuffixLen)
	}
	expected := constructRealSuffix(key, level, v.realSuffixLen)

	if suffix == 0 && expected == 0 {
		return couldBePositive
	} else if suffix == 0 || suffix < expected {
		return -1
	} else if suffix == expected {
		return couldBePositive
	} else {
		return 1
	}
}

func (v *suffixVector) MarshalSize() int64 {
	return align(v.rawMarshalSize())
}

func (v *suffixVector) rawMarshalSize() int64 {
	return 4 + 1 + 4 + 4 + int64(v.bitsSize())
}

func (v *suffixVector) WriteTo(w io.Writer) error {
	var buf [4]byte
	endian.PutUint32(buf[:], v.numBits)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write([]byte{byte(v.suffixType)}); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.hashSuffixLen)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	endian.PutUint32(buf[:], v.realSuffixLen)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(u64SliceToBytes(v.bits)); err != nil {
		return err
	}

	padding := v.MarshalSize() - v.rawMarshalSize()
	var zeros [8]byte
	_, err := w.Write(zeros[:padding])
	return err
}

func (v *suffixVector) Unmarshal(buf []byte) []byte {
	var cursor int64
	v.numBits = endian.Uint32(buf)
	cursor += 4
	v.suffixType = SuffixType(buf[cursor])
	cursor += 1
	v.hashSuffixLen = endian.Uint32(buf[cursor:])
	cursor += 4
	v.realSuffixLen = endian.Uint32(buf[cursor:])
	cursor += 4
	if v.suffixType != NoneSuffix {
		bitsSize := int64(v.bitsSize())
		v.bits = bytesToU64Slice(buf[cursor : cursor+bitsSize])
		cursor += bitsSize
	}
	cursor = align(cursor)
	return buf[cursor:]
}

func (v *suffixVector) read(idx uint32) uint64 {
	suffixLen := v.suffixLen()
	bitPos := idx * suffixLen
	wordOff := bitPos / wordSize
	bitsOff := bitPos % wordSize
	result := (v.bits[wordOff] << bitsOff) >> (wordSize - suffixLen)
	if bitsOff+suffixLen > wordSize {
		result += v.bits[wordOff+1] >> (2*wordSize - bitsOff - suffixLen)
	}
	return result
}

func (v *suffixVector) suffixLen() uint32 {
	return v.hashSuffixLen + v.realSuffixLen
}

func constructSuffix(key []byte, level uint32, suffixType SuffixType, realSuffixLen, hashSuffixLen uint32) uint64 {
	switch suffixType {
	case HashSuffix:
		return constructHashSuffix(key, hashSuffixLen)
	case RealSuffix:
		return constructRealSuffix(key, level, realSuffixLen)
	case MixedSuffix:
		return constructMixedSuffix(key, level, realSuffixLen, hashSuffixLen)
	default:
		return 0
	}
}

func constructHashSuffix(key []byte, hashSuffixLen uint32) uint64 {
	fp := farm.Fingerprint64(key)
	fp <<= wordSize - hashSuffixLen - hashShift
	fp >>= wordSize - hashSuffixLen
	return fp
}

func constructRealSuffix(key []byte, level, realSuffixLen uint32) uint64 {
	klen := uint32(len(key))
	if klen < level || (klen-level)*8 < realSuffixLen {
		return 0
	}

	var suffix uint64
	nbytes := realSuffixLen / 8
	if nbytes > 0 {
		suffix += uint64(key[level])
		for i := 1; uint32(i) < nbytes; i++ {
			suffix <<= 8
			suffix += uint64(key[i])
		}
	}

	off := realSuffixLen % 8
	if off > 0 {
		suffix <<= off
		remain := uint64(key[level+nbytes])
		remain >>= 8 - off
		suffix += remain
	}

	return suffix
}

func constructMixedSuffix(key []byte, level, realSuffixLen, hashSuffixLen uint32) uint64 {
	hs := constructHashSuffix(key, hashSuffixLen)
	rs := constructRealSuffix(key, level, realSuffixLen)
	return (hs << realSuffixLen) | rs
}

func extractRealSuffix(suffix uint64, suffixLen uint32) uint64 {
	mask := (uint64(1) << suffixLen) - 1
	return suffix & mask
}
