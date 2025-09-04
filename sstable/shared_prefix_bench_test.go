package sstable

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/bits"
	"testing"
	"unsafe"
)

// Generate realistic key distributions for benchmarking
func generateKeys(numKeys int, keySize int, commonPrefixLen int) [][]byte {
	keys := make([][]byte, numKeys)

	// Generate a common prefix
	commonPrefix := make([]byte, commonPrefixLen)
	rand.Read(commonPrefix)

	for i := range keys {
		key := make([]byte, keySize)
		// Copy common prefix
		copy(key, commonPrefix)
		// Fill remaining bytes with random data
		rand.Read(key[commonPrefixLen:])
		keys[i] = key
	}

	return keys
}

// Benchmark comparing all implementations with different key sizes
func BenchmarkSharedPrefixLenSizeComparison(b *testing.B) {
	testCases := []struct {
		name            string
		keySize         int
		commonPrefixLen int
	}{
		{"short_keys", 16, 8},
		{"medium_keys", 50, 20},
		{"long_keys", 100, 40},
		{"very_long_keys", 500, 200},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			keys := generateKeys(1000, tc.keySize, tc.commonPrefixLen)

			b.Run("current", func(b *testing.B) {
				benchmarkImpl(b, keys, sharedPrefixLen)
			})

			b.Run("gemini_unsafe", func(b *testing.B) {
				benchmarkImpl(b, keys, sharedPrefixLenGeminiUnsafe)
			})

			b.Run("naive", func(b *testing.B) {
				benchmarkImpl(b, keys, sharedPrefixLenNaive)
			})
		})
	}
}

func benchmarkImpl(b *testing.B, keys [][]byte, impl func([]byte, []byte) int) {
	var sum int
	j := 0

	for b.Loop() {
		next := j + 1
		if next >= len(keys) {
			next = 0
		}
		sum += impl(keys[j], keys[next])
		j = next
	}
	b.StopTimer()
	b.Logf("average prefix length: %d", sum/b.N)
}

// Benchmark sharedPrefixLen with different key characteristics
func BenchmarkSharedPrefixLen(b *testing.B) {
	testCases := []struct {
		name            string
		keySize         int
		commonPrefixLen int
		numKeys         int
	}{
		{"small_keys_short_prefix", 20, 4, 1000},
		{"small_keys_med_prefix", 20, 10, 1000},
		{"medium_keys_short_prefix", 50, 4, 1000},
		{"medium_keys_med_prefix", 50, 20, 1000},
		{"medium_keys_long_prefix", 50, 35, 1000},
		{"large_keys_short_prefix", 100, 4, 1000},
		{"large_keys_med_prefix", 100, 40, 1000},
		{"large_keys_long_prefix", 100, 80, 1000},
		{"very_large_keys", 500, 200, 1000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			keys := generateKeys(tc.numKeys, tc.keySize, tc.commonPrefixLen)

			b.ResetTimer()
			var sum int
			j := 0
			for b.Loop() {
				next := j + 1
				if next >= len(keys) {
					next = 0
				}
				sum += sharedPrefixLen(keys[j], keys[next])
				j = next
			}
			b.StopTimer()

			b.ReportMetric(float64(sum)/float64(b.N), "avg_prefix_len")
		})
	}
}

// Benchmark comparing against simple byte-by-byte (current implementation)
func BenchmarkSharedPrefixLenComparison(b *testing.B) {
	// Test with realistic database keys - medium size with moderate shared prefixes
	keys := generateKeys(1000, 50, 15)

	b.Run("current", func(b *testing.B) {
		var sum int
		j := 0
		for b.Loop() {
			next := j + 1
			if next >= len(keys) {
				next = 0
			}
			sum += sharedPrefixLen(keys[j], keys[next])
			j = next
		}
		b.Logf("average prefix length: %d", sum/b.N)
	})

	b.Run("optimized", func(b *testing.B) {
		var sum int
		j := 0
		for b.Loop() {
			next := j + 1
			if next >= len(keys) {
				next = 0
			}
			sum += sharedPrefixLenOptimized(keys[j], keys[next])
			j = next
		}
		b.Logf("average prefix length: %d", sum/b.N)
	})

	b.Run("gemini_unsafe", func(b *testing.B) {
		var sum int
		j := 0
		for b.Loop() {
			next := j + 1
			if next >= len(keys) {
				next = 0
			}
			sum += sharedPrefixLenGeminiUnsafe(keys[j], keys[next])
			j = next
		}
		b.Logf("average prefix length: %d", sum/b.N)
	})

	b.Run("naive", func(b *testing.B) {
		var sum int
		j := 0
		for b.Loop() {
			next := j + 1
			if next >= len(keys) {
				next = 0
			}
			sum += sharedPrefixLenNaive(keys[j], keys[next])
			j = next
		}
		b.Logf("average prefix length: %d", sum/b.N)
	})
}

// Optimized version using 8-byte comparison (to test against current)
func sharedPrefixLenOptimized(a, b []byte) int {
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

// Gemini's unsafe pointer-based implementation with XOR + TrailingZeros64
func sharedPrefixLenGeminiUnsafe(a, b []byte) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	// Pointers to the underlying byte arrays.
	// This is safe because we're not modifying the data, just reading from it.
	p_a := unsafe.Pointer(&a[0])
	p_b := unsafe.Pointer(&b[0])

	var shared int
	n := min(len(a), len(b))

	// Fast path: compare 64 bits at a time using pointer arithmetic.
	// This avoids slice bounds checks within the loop.
	n_u64 := n >> 3 // n / 8
	for range n_u64 {
		// Use unsafe.Pointer to get a pointer to the current 8 bytes.
		// unsafe.Pointer can be cast to *uint64.
		v_a := *(*uint64)(unsafe.Pointer(uintptr(p_a) + uintptr(shared)))
		v_b := *(*uint64)(unsafe.Pointer(uintptr(p_b) + uintptr(shared)))

		if v_a != v_b {
			// If they differ, find the first differing byte using bits.TrailingZeros64.
			// This is an extremely fast way to find the position of the first set bit.
			return shared + bits.TrailingZeros64(v_a^v_b)>>3 // (v_a XOR v_b) / 8
		}
		shared += 8
	}

	// Slow path: compare remaining bytes one by one.
	for shared < n {
		if a[shared] != b[shared] {
			return shared
		}
		shared++
	}

	return n
}

// Very simple naive implementation for comparison
func sharedPrefixLenNaive(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// Test correctness of optimized implementation
func TestSharedPrefixLenOptimizedCorrectness(t *testing.T) {
	testCases := []struct {
		a, b     []byte
		expected int
	}{
		{[]byte("hello"), []byte("help"), 3},
		{[]byte("test"), []byte("test"), 4},
		{[]byte(""), []byte("test"), 0},
		{[]byte("test"), []byte(""), 0},
		{[]byte("a"), []byte("b"), 0},
		{[]byte("same"), []byte("different"), 0},
		// Test cases with various lengths to verify 8-byte boundary handling
		{[]byte("12345678abcd"), []byte("12345678efgh"), 8},
		{[]byte("1234567890123456"), []byte("1234567890123456"), 16},
		{[]byte("1234567890123456x"), []byte("1234567890123456y"), 16},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			result := sharedPrefixLenOptimized(tc.a, tc.b)
			naive := sharedPrefixLenNaive(tc.a, tc.b)
			current := sharedPrefixLen(tc.a, tc.b)
			gemini := sharedPrefixLenGeminiUnsafe(tc.a, tc.b)

			if result != tc.expected {
				t.Errorf("optimized: expected %d, got %d for %q vs %q", tc.expected, result, tc.a, tc.b)
			}
			if naive != tc.expected {
				t.Errorf("naive: expected %d, got %d for %q vs %q", tc.expected, naive, tc.a, tc.b)
			}
			if current != tc.expected {
				t.Errorf("current: expected %d, got %d for %q vs %q", tc.expected, current, tc.a, tc.b)
			}
			if gemini != tc.expected {
				t.Errorf("gemini: expected %d, got %d for %q vs %q", tc.expected, gemini, tc.a, tc.b)
			}

			// All implementations should agree
			if result != naive || naive != current || current != gemini {
				t.Errorf("implementations disagree: optimized=%d, naive=%d, current=%d, gemini=%d for %q vs %q",
					result, naive, current, gemini, tc.a, tc.b)
			}
		})
	}
}
