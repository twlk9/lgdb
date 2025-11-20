package sstable

import (
	"encoding/binary"
)

// BlockBuilder builds data blocks for SSTables using Pebble format
type BlockBuilder struct {
	buffer          []byte
	restarts        []uint32
	numEntries      int
	lastKey         []byte
	finished        bool
	restartInterval int
	blockSize       int
	minEntries      int
}

// NewBlockBuilder creates a new block builder
func NewBlockBuilder(blockSize, restartInterval, minEntries int) *BlockBuilder {
	if restartInterval == 0 {
		restartInterval = RestartInterval
	}
	if minEntries == 0 {
		minEntries = 4
	}
	return &BlockBuilder{
		buffer:          make([]byte, 0, blockSize),
		restarts:        make([]uint32, 0),
		restartInterval: restartInterval,
		blockSize:       blockSize,
		minEntries:      minEntries,
	}
}

// Add adds a key-value pair to the block using Pebble format
func (b *BlockBuilder) Add(key, value []byte) {
	if b.finished {
		panic("Cannot add to finished block")
	}

	// Calculate shared prefix with last key
	var shared int
	if len(b.lastKey) > 0 {
		shared = sharedPrefixLen(b.lastKey, key)
	}
	unshared := len(key) - shared

	// Add restart point if this is the first entry or every restartInterval entries
	if b.numEntries%b.restartInterval == 0 {
		b.restarts = append(b.restarts, uint32(len(b.buffer)))
		shared = 0
		unshared = len(key)
	}

	// Pebble block entry format:
	// varint(shared_key_length) +
	// varint(unshared_key_length) +
	// varint(value_length) +
	// unshared_key_data +
	// value_data
	// fmt.Printf("SHARED KEY LENGTH: %d\n", shared)
	// Encode shared key length
	b.buffer = appendUvarint(b.buffer, uint64(shared))

	// Encode unshared key length
	b.buffer = appendUvarint(b.buffer, uint64(unshared))

	// Encode value length
	b.buffer = appendUvarint(b.buffer, uint64(len(value)))

	// Append unshared key data
	b.buffer = append(b.buffer, key[shared:]...)

	// Append value data
	b.buffer = append(b.buffer, value...)

	// Update last key using buffer pool
	if cap(b.lastKey) < len(key) {
		b.lastKey = make([]byte, len(key))
	} else {
		b.lastKey = b.lastKey[:len(key)]
	}
	copy(b.lastKey, key)

	b.numEntries++
}

// Finish finalizes the block and returns the encoded data using Pebble format
func (b *BlockBuilder) Finish() []byte {
	if b.finished {
		panic("Block already finished")
	}

	// Ensure we have at least one restart point
	if len(b.restarts) == 0 {
		b.restarts = append(b.restarts, 0)
	}

	// Pebble block trailer format:
	// restart_point_0 (4 bytes, little-endian) +
	// restart_point_1 (4 bytes, little-endian) +
	// ...
	// restart_count (4 bytes, little-endian)

	// Append restart points using stack-allocated array (avoids race condition with buffer pool)
	var restartBytes [4]byte

	for _, restart := range b.restarts {
		binary.LittleEndian.PutUint32(restartBytes[:], restart)
		b.buffer = append(b.buffer, restartBytes[:]...)
	}

	// Append number of restart points
	binary.LittleEndian.PutUint32(restartBytes[:], uint32(len(b.restarts)))
	b.buffer = append(b.buffer, restartBytes[:]...)

	b.finished = true
	return b.buffer
}

// IsFull returns true if the block is full and has enough entries
func (b *BlockBuilder) IsFull() bool {
	return len(b.buffer) > b.blockSize && b.numEntries > b.minEntries
}

// EstimatedSize returns the estimated size of the block when finished
func (b *BlockBuilder) EstimatedSize() int {
	return len(b.buffer)
}

// IsEmpty returns true if the block is empty
func (b *BlockBuilder) IsEmpty() bool {
	return b.numEntries == 0
}

// Reset resets the block builder for reuse
func (b *BlockBuilder) Reset() {
	b.buffer = b.buffer[:0]
	b.restarts = b.restarts[:0]
	b.numEntries = 0
	b.lastKey = nil
	b.finished = false
}

// NumEntries returns the number of entries in the block
func (b *BlockBuilder) NumEntries() int {
	return b.numEntries
}

// Helper functions

// sharedPrefixLen returns the length of the shared prefix between two byte slices
// Uses optimized 8-byte-at-a-time comparison for better performance
func sharedPrefixLen(a, b []byte) int {
	asUint64 := func(data []byte, i int) uint64 {
		return binary.LittleEndian.Uint64(data[i:])
	}
	var shared int
	n := min(len(a), len(b))

	// Compare 8 bytes at a time while we have at least 8 bytes remaining
	for shared < n-7 && asUint64(a, shared) == asUint64(b, shared) {
		shared += 8
	}

	// Compare remaining bytes one by one
	for shared < n && a[shared] == b[shared] {
		shared++
	}
	return shared
}

// appendUvarint appends a varint to the buffer
func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}
