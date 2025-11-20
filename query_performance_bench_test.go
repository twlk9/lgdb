package lgdb

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// BenchmarkRangeQueryPerformance tests range query performance with optimized setup
func BenchmarkRangeQueryPerformance(b *testing.B) {
	tests := []struct {
		name      string
		numKeys   int
		rangeSize int
	}{
		{"Small range (10 keys)", 10000, 10},
		{"Medium range (100 keys)", 10000, 100},
		{"Large range (1000 keys)", 10000, 1000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			opts := DefaultOptions()
			// Use async writes for fast setup

			bdb := newBenchmarkDB(b, opts)
			defer bdb.close()

			// Generate test data
			bdb.generateKeys(tt.numKeys, 16)
			bdb.generateValues(tt.numKeys, 100)

			// Fast population using NoSync
			b.StopTimer()
			for i := 0; i < tt.numKeys; i++ {
				if err := bdb.db.PutWithOptions(bdb.keys[i], bdb.values[i], NoSync); err != nil {
					b.Fatal(err)
				}
			}
			// Wait for background syncs to complete
			time.Sleep(200 * time.Millisecond)
			b.StartTimer()

			// Benchmark actual range queries
			for i := 0; b.Loop(); i++ {
				start := i % (tt.numKeys - tt.rangeSize)
				startKey := bdb.keys[start]

				iter := bdb.db.NewIterator(nil)
				count := 0
				// Count exactly rangeSize keys starting from startKey
				iter.Seek(startKey)
				for count < tt.rangeSize && iter.Valid() {
					count++
					iter.Next()
				}
				iter.Close()

				if count != tt.rangeSize {
					b.Errorf("Expected %d keys, got %d", tt.rangeSize, count)
				}
			}
		})
	}
}

// BenchmarkGetPerformance tests single key lookup performance
func BenchmarkGetPerformance(b *testing.B) {
	tests := []struct {
		name    string
		numKeys int
		pattern string
	}{
		{"Sequential access", 10000, "sequential"},
		{"Random access", 10000, "random"},
		{"Missing keys", 10000, "missing"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			opts := DefaultOptions()

			bdb := newBenchmarkDB(b, opts)
			defer bdb.close()

			bdb.generateKeys(tt.numKeys, 16)
			bdb.generateValues(tt.numKeys, 100)

			// Fast population
			b.StopTimer()
			for i := 0; i < tt.numKeys; i++ {
				if err := bdb.db.PutWithOptions(bdb.keys[i], bdb.values[i], NoSync); err != nil {
					b.Fatal(err)
				}
			}
			time.Sleep(200 * time.Millisecond)
			b.StartTimer()

			// Benchmark Get operations
			for i := 0; b.Loop(); i++ {
				var key []byte
				switch tt.pattern {
				case "sequential":
					key = bdb.keys[i%tt.numKeys]
				case "random":
					key = bdb.keys[(i*7919)%tt.numKeys] // Prime for pseudo-random
				case "missing":
					key = fmt.Appendf(nil, "missing-key-%d", i)
				}

				value, err := bdb.db.Get(key)
				if tt.pattern != "missing" && err != nil {
					b.Errorf("Unexpected error: %v", err)
				}
				if tt.pattern == "missing" && err == nil {
					b.Errorf("Expected error for missing key, got value: %s", value)
				}
			}
		})
	}
}

// BenchmarkIteratorOverhead tests iterator creation and basic operations
func BenchmarkIteratorOverhead(b *testing.B) {
	opts := DefaultOptions()

	bdb := newBenchmarkDB(b, opts)
	defer bdb.close()

	// Small dataset for focused iterator testing
	numKeys := 1000
	bdb.generateKeys(numKeys, 16)
	bdb.generateValues(numKeys, 100)

	// Populate database (not counted in benchmark)
	for i := range numKeys {
		if err := bdb.db.PutWithOptions(bdb.keys[i], bdb.values[i], NoSync); err != nil {
			b.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	b.ResetTimer()

	b.Run("Iterator creation", func(b *testing.B) {
		for b.Loop() {
			iter := bdb.db.NewIterator(nil)
			iter.Close()
		}
	})

	b.Run("Iterator seek", func(b *testing.B) {
		iter := bdb.db.NewIterator(nil)
		defer iter.Close()

		for i := 0; b.Loop(); i++ {
			key := bdb.keys[i%numKeys]
			iter.Seek(key)
		}
	})

	b.Run("Iterator next", func(b *testing.B) {
		iter := bdb.db.NewIterator(nil)
		defer iter.Close()

		// Start at beginning
		iter.Seek(bdb.keys[0])

		for b.Loop() {
			if !iter.Valid() {
				iter.Seek(bdb.keys[0]) // Reset to beginning
			}
			iter.Next()
		}
	})
}

// TestRangeQueryCorrectness ensures range queries work correctly after profiling
func TestRangeQueryCorrectness(t *testing.T) {
	opts := DefaultOptions()

	bdb := newBenchmarkDB(&testing.B{}, opts)
	defer bdb.close()

	// Small test dataset
	numKeys := 100
	bdb.generateKeys(numKeys, 16)
	bdb.generateValues(numKeys, 50)

	// Populate with async writes
	for i := range numKeys {
		if err := bdb.db.PutWithOptions(bdb.keys[i], bdb.values[i], NoSync); err != nil {
			t.Fatal(err)
		}
	}

	// Test various range sizes
	rangeSizes := []int{1, 5, 10, 25, 50}

	for _, rangeSize := range rangeSizes {
		t.Run(fmt.Sprintf("Range size %d", rangeSize), func(t *testing.T) {
			start := 10 // Start from key 10
			startKey := bdb.keys[start]

			iter := bdb.db.NewIterator(nil)
			defer iter.Close()

			count := 0
			var keys [][]byte
			// Count exactly rangeSize keys starting from startKey
			iter.Seek(startKey)
			for count < rangeSize && iter.Valid() {
				k := make([]byte, len(iter.Key()))
				copy(k, iter.Key())
				keys = append(keys, k)
				count++
				iter.Next()
			}

			if count != rangeSize {
				t.Errorf("Range size %d: expected %d keys, got %d", rangeSize, rangeSize, count)
			}

			// Verify keys are in order
			for i := 1; i < len(keys); i++ {
				if bytes.Compare(keys[i-1], keys[i]) >= 0 {
					t.Errorf("Keys not in order: %s >= %s", keys[i-1], keys[i])
				}
			}
		})
	}
}
