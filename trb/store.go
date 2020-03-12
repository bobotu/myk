package trb

import (
	"encoding/binary"
	"sync"
)

type dataAddr struct {
	index  uint32
	offset uint32
}

const chunkSize = 4096

type chunk struct {
	tail uint32
	data [4096]byte
}

var codec = binary.LittleEndian

func (c *chunk) append(key, val []byte) int64 {
	sz := 4 + len(key) + len(val)
	if int(chunkSize-c.tail) < sz {
		return -1
	}

	off := c.tail
	codec.PutUint16(c.data[c.tail:], uint16(len(key)))
	c.tail += 2
	codec.PutUint16(c.data[c.tail:], uint16(len(val)))
	c.tail += 2
	copy(c.data[c.tail:], key)
	c.tail += uint32(len(key))
	copy(c.data[c.tail:], val)
	c.tail += uint32(len(val))
	return int64(off)
}

func (c *chunk) getKey(offset uint32) []byte {
	len := codec.Uint16(c.data[offset:])
	return c.data[offset+4 : offset+4+uint32(len)]
}

func (c *chunk) getValue(offset uint32) []byte {
	klen := codec.Uint16(c.data[offset:])
	len := codec.Uint16(c.data[offset+2:])
	return c.data[offset+4+uint32(klen) : offset+4+uint32(len+klen)]
}

func (c *chunk) truncate(tail uint32) {
	c.tail = tail
}

var chunkPool = sync.Pool{
	New: func() interface{} { return new(chunk) },
}

type dataStore struct {
	tail   dataAddr
	chunks []*chunk
}

func newDataStore() dataStore {
	return dataStore{
		chunks: []*chunk{chunkPool.Get().(*chunk)},
	}
}

func (d *dataStore) append(key, val []byte) dataAddr {
	index := d.tail.index
	offset := d.chunks[index].append(key, val)
	if offset < 0 {
		d.addChunk()
		index = d.tail.index
		offset = d.chunks[index].append(key, val)
	}
	return dataAddr{index, uint32(offset)}
}

func (d *dataStore) addChunk() {
	if int(d.tail.index) == len(d.chunks)-1 {
		d.chunks = append(d.chunks, chunkPool.Get().(*chunk))
	}
	d.tail = dataAddr{d.tail.index + 1, 0}
}

func (d *dataStore) getKey(addr dataAddr) []byte {
	return d.chunks[addr.index].getKey(addr.offset)
}

func (d *dataStore) getValue(addr dataAddr) []byte {
	return d.chunks[addr.index].getValue(addr.offset)
}

func (d *dataStore) getTail() dataAddr {
	return d.tail
}

func (d *dataStore) setTail(tail dataAddr) {
	d.tail = tail
}

func (d *dataStore) truncate(pos dataAddr) {
	d.chunks[pos.index].truncate(pos.offset)
	for _, c := range d.chunks[pos.index+1:] {
		c.truncate(0)
	}
}
