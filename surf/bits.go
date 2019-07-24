package surf

import (
	"encoding/binary"
	"math/bits"
	"reflect"
	"unsafe"
)

const (
	wordSize     = 64
	popcountSize = wordSize
	popcountMask = popcountSize - 1

	msbMask = 0x8000000000000000
)

var endian = binary.LittleEndian

func select64(x uint64, k int) int {
	if k > bits.OnesCount64(x) {
		return -1
	}

	loc := -1
	var testbits uint32
	for testbits = 32; testbits > 0; testbits >>= 1 {
		cnt := bits.OnesCount64(x >> testbits)
		if k > cnt {
			x &= (uint64(1) << testbits) - 1
			loc += int(testbits)
			k -= cnt
		} else {
			x >>= testbits
		}
	}
	return loc + k
}

func popcountBlock(bs []uint64, off, nbits uint32) uint32 {
	if nbits == 0 {
		return 0
	}

	lastWord := (nbits - 1) / popcountSize
	var i, p uint32

	for i = 0; i < lastWord; i++ {
		p += uint32(bits.OnesCount64(bs[off+i]))
	}
	last := bs[off+lastWord] >> (63 - ((nbits - 1) & popcountMask))
	return p + uint32(bits.OnesCount64(last))
}

func readBit(bs []uint64, pos uint32) bool {
	wordOff := pos / wordSize
	bitsOff := pos % wordSize
	return bs[wordOff]&(msbMask>>bitsOff) != 0
}

func setBit(bs []uint64, pos uint32) {
	wordOff := pos / wordSize
	bitsOff := pos % wordSize
	bs[wordOff] |= msbMask >> bitsOff
}

func align(off int64) int64 {
	return (off + 7) & ^int64(7)
}

func u64SliceToBytes(u []uint64) []byte {
	if len(u) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u) * 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u[0]))
	return b
}

func bytesToU64Slice(b []byte) []uint64 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint64
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}
func u32SliceToBytes(u []uint32) []byte {
	if len(u) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u[0]))
	return b
}

func bytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}
