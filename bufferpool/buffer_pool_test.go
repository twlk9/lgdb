package bufferpool

import (
	"testing"
)

func TestBufferPool(t *testing.T) {
	pool := NewBufferPool()

	// Test getting small buffer
	buf1 := pool.Get(1024)
	if len(buf1) != 1024 {
		t.Errorf("Expected buffer length 1024, got %d", len(buf1))
	}
	if cap(buf1) < 1024 {
		t.Errorf("Expected buffer capacity >= 1024, got %d", cap(buf1))
	}

	// Test getting large buffer
	buf2 := pool.Get(8192)
	if len(buf2) != 8192 {
		t.Errorf("Expected buffer length 8192, got %d", len(buf2))
	}
	if cap(buf2) < 8192 {
		t.Errorf("Expected buffer capacity >= 8192, got %d", cap(buf2))
	}

	// Test returning buffers to pool
	pool.Put(buf1)
	pool.Put(buf2)

	// Test buffer reuse
	buf3 := pool.Get(512) // Should reuse buf1
	if cap(buf3) < 512 {
		t.Errorf("Expected buffer capacity >= 512, got %d", cap(buf3))
	}

	buf4 := pool.Get(4096) // Should reuse buf2
	if cap(buf4) < 4096 {
		t.Errorf("Expected buffer capacity >= 4096, got %d", cap(buf4))
	}
}

func TestGlobalBufferPool(t *testing.T) {
	// Test global pool functions
	buf1 := GetBuffer(2048)
	if len(buf1) != 2048 {
		t.Errorf("Expected buffer length 2048, got %d", len(buf1))
	}

	buf2 := GetBuffer(16384)
	if len(buf2) != 16384 {
		t.Errorf("Expected buffer length 16384, got %d", len(buf2))
	}

	// Return buffers
	PutBuffer(buf1)
	PutBuffer(buf2)

	// Test reuse
	buf3 := GetBuffer(1024)
	if cap(buf3) < 1024 {
		t.Errorf("Expected buffer capacity >= 1024, got %d", cap(buf3))
	}

	PutBuffer(buf3)
}

func TestBufferPoolOversized(t *testing.T) {
	pool := NewBufferPool()

	// Test very large buffer (should not be pooled)
	buf := pool.Get(64 * 1024) // 64KB
	if len(buf) != 64*1024 {
		t.Errorf("Expected buffer length 64KB, got %d", len(buf))
	}

	// Return it (should not be pooled)
	pool.Put(buf)

	// Next small allocation should not reuse the large buffer
	buf2 := pool.Get(1024)
	if cap(buf2) > 32768 {
		t.Errorf("Expected small buffer, got capacity %d", cap(buf2))
	}
}

func BenchmarkBufferPoolSmall(b *testing.B) {
	pool := NewBufferPool()

	for b.Loop() {
		buf := pool.Get(1024)
		pool.Put(buf)
	}
}

func BenchmarkBufferPoolLarge(b *testing.B) {
	pool := NewBufferPool()

	for b.Loop() {
		buf := pool.Get(16384)
		pool.Put(buf)
	}
}

func BenchmarkDirectAllocationSmall(b *testing.B) {
	for b.Loop() {
		_ = make([]byte, 1024)
	}
}

func BenchmarkDirectAllocationLarge(b *testing.B) {
	for b.Loop() {
		_ = make([]byte, 16384)
	}
}
