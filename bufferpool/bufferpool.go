package bufferpool

import (
	"sync"
)

// BufferPool provides reusable byte slices to reduce allocations
type BufferPool struct {
	small sync.Pool // For buffers <= 4KB
	large sync.Pool // For buffers > 4KB
}

// NewBufferPool creates a new buffer pool
func NewBufferPool() *BufferPool {
	return &BufferPool{
		small: sync.Pool{
			New: func() any {
				buf := make([]byte, 0, 4096) // 4KB initial capacity
				return &buf
			},
		},
		large: sync.Pool{
			New: func() any {
				buf := make([]byte, 0, 32768) // 32KB initial capacity
				return &buf
			},
		},
	}
}

// Get returns a byte slice with at least the requested capacity
func (p *BufferPool) Get(size int) []byte {
	if size <= 4096 {
		bufPtr := p.small.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) >= size {
			*bufPtr = buf[:size] // Modify the slice in-place via pointer
			return *bufPtr
		}
		// Buffer too small, return to pool and allocate new one
		p.small.Put(bufPtr)
		return make([]byte, size)
	} else {
		bufPtr := p.large.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) >= size {
			*bufPtr = buf[:size] // Modify the slice in-place via pointer
			return *bufPtr
		}
		// Buffer too small, return to pool and allocate new one
		p.large.Put(bufPtr)
		return make([]byte, size)
	}
}

// Put returns a byte slice to the pool for reuse
func (p *BufferPool) Put(buf []byte) {
	if cap(buf) <= 4096 {
		// Reset length to 0 and put the pointer back
		buf = buf[:0]
		p.small.Put(&buf)
	} else if cap(buf) <= 32768 {
		// Reset length to 0 and put the pointer back
		buf = buf[:0]
		p.large.Put(&buf)
	}
	// If buffer is too large, let it be garbage collected
}

// Global buffer pool instance
var globalBufferPool = NewBufferPool()

// GetBuffer returns a byte slice from the global pool
func GetBuffer(size int) []byte {
	return globalBufferPool.Get(size)
}

// PutBuffer returns a byte slice to the global pool
func PutBuffer(buf []byte) {
	globalBufferPool.Put(buf)
}
