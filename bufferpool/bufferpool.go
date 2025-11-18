package bufferpool

import (
	"sync"
)

const (
	smallBufferSize = 4096
	largeBufferSize = 32768
)

// BufferPool provides reusable byte slices to reduce allocations.
// It maintains two pools for different buffer sizes to optimize memory usage.
type BufferPool struct {
	small sync.Pool // For buffers <= smallBufferSize
	large sync.Pool // For buffers > smallBufferSize and <= largeBufferSize
}

// NewBufferPool creates a new buffer pool with predefined size classes.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		small: sync.Pool{
			New: func() any {
				// Slices returned from the pool have zero length but a pre-allocated capacity.
				return make([]byte, 0, smallBufferSize)
			},
		},
		large: sync.Pool{
			New: func() any {
				return make([]byte, 0, largeBufferSize)
			},
		},
	}
}

// Get returns a byte slice with at least the requested capacity.
// The returned slice will have a length equal to the requested size.
func (p *BufferPool) Get(size int) []byte {
	var buf []byte
	if size <= smallBufferSize {
		buf = p.small.Get().([]byte)
	} else if size <= largeBufferSize {
		buf = p.large.Get().([]byte)
	} else {
		// For requests larger than the largest pool size, allocate a new slice.
		// This buffer will not be returned to the pool.
		return make([]byte, size)
	}

	// If the buffer from the pool has less capacity than required,
	// allocate a new one. The undersized buffer is discarded and will be GC'd.
	if cap(buf) < size {
		return make([]byte, size)
	}

	// Reuse the buffer by slicing it to the desired length.
	return buf[:size]
}

// Put returns a byte slice to the appropriate pool for reuse.
// Buffers with capacities that don't match the pool sizes are discarded.
func (p *BufferPool) Put(buf []byte) {
	// Reset the slice length to 0, preserving its capacity.
	buf = buf[:0]

	// Place the buffer back into the correct pool based on its capacity.
	// Buffers that don't match the pool's capacity are left for the GC.
	switch cap(buf) {
	case smallBufferSize:
		p.small.Put(buf)
	case largeBufferSize:
		p.large.Put(buf)
	}
}

// Global buffer pool instance
var globalBufferPool = NewBufferPool()

// GetBuffer returns a byte slice from the global pool.
func GetBuffer(size int) []byte {
	return globalBufferPool.Get(size)
}

// PutBuffer returns a byte slice to the global pool.
func PutBuffer(buf []byte) {
	globalBufferPool.Put(buf)
}
