package memdb

import "unsafe"

type memdbVlog struct {
	arena
}

type memdbVlogHdr struct {
	nodeAddr arenaAddr
	oldValue arenaAddr
	valueLen uint32
}

const memdbVlogHdrSize = 8 + 8 + 4

func (l *memdbVlog) appendValue(nodeAddr arenaAddr, oldvalue arenaAddr, value []byte) arenaAddr {
	size := memdbVlogHdrSize + len(value)
	addr, mem := l.alloc(size)

	copy(mem, value)
	hdr := (*memdbVlogHdr)(unsafe.Pointer(&mem[len(value)]))
	hdr.nodeAddr = nodeAddr
	hdr.oldValue = oldvalue
	hdr.valueLen = uint32(len(value))
	addr.off += uint32(size)

	return addr
}

func (l *memdbVlog) getValue(addr arenaAddr) []byte {
	hdrOff := addr.off - memdbVlogHdrSize
	block := l.blocks[addr.idx].buf
	hdr := (*memdbVlogHdr)(unsafe.Pointer(&block[hdrOff]))
	valueOff := hdrOff - hdr.valueLen
	return block[valueOff:hdrOff]
}
