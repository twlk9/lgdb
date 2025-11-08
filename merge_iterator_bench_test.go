package lgdb

import (
	"fmt"
	"testing"

	"github.com/twlk9/lgdb/keys"
	"github.com/twlk9/lgdb/memtable"
)

// BenchmarkMergeIteratorSeekSmallInputs measures merge iterator performance with a few iterators
func BenchmarkMergeIteratorSeekSmallInputs(b *testing.B) {
	numIterators := 5
	keysPerIterator := 1000
	totalKeys := numIterators * keysPerIterator

	// Create multiple memtables with different key ranges
	memtables := make([]*memtable.MemTable, numIterators)
	for m := range numIterators {
		memtables[m] = memtable.NewMemtable(1024 * 1024 * 10) // 10MB buffer
		// Fill with non-overlapping key ranges
		for i := range keysPerIterator {
			globalIdx := m*keysPerIterator + i
			key := fmt.Appendf(nil, "key%016d", globalIdx)
			encodedKey := keys.NewEncodedKey(key, 0, keys.KindSet)
			value := fmt.Appendf(nil, "value%016d", globalIdx)
			memtables[m].Put(encodedKey, value)
		}
	}

	// Reset timer after setup
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// Create merge iterator
		mi := NewMergeIterator(nil, false, 0)
		for _, mt := range memtables {
			iter := mt.NewIterator()
			mi.AddIterator(iter)
		}

		// Seek to different positions and iterate
		seekPositions := []int{0, totalKeys / 4, totalKeys / 2, totalKeys * 3 / 4}
		for _, pos := range seekPositions {
			target := fmt.Appendf(nil, "key%016d", pos)
			targetKey := keys.NewEncodedKey(target, 0, keys.KindSet)
			mi.Seek(targetKey)

			// Iterate through some entries
			for j := 0; j < 100 && mi.Valid(); j++ {
				_ = mi.Key()
				_ = mi.Value()
				mi.Next()
			}
		}

		mi.Close()
	}
}

// BenchmarkMergeIteratorSeekLargeInputs measures merge iterator performance with many iterators
func BenchmarkMergeIteratorSeekLargeInputs(b *testing.B) {
	numIterators := 40
	keysPerIterator := 500
	totalKeys := numIterators * keysPerIterator

	// Create multiple memtables with different key ranges
	memtables := make([]*memtable.MemTable, numIterators)
	for m := range numIterators {
		memtables[m] = memtable.NewMemtable(1024 * 1024 * 10) // 10MB buffer
		// Fill with non-overlapping key ranges
		for i := range keysPerIterator {
			globalIdx := m*keysPerIterator + i
			key := fmt.Appendf(nil, "key%016d", globalIdx)
			encodedKey := keys.NewEncodedKey(key, 0, keys.KindSet)
			value := fmt.Appendf(nil, "value%016d", globalIdx)
			memtables[m].Put(encodedKey, value)
		}
	}

	// Reset timer after setup
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// Create merge iterator
		mi := NewMergeIterator(nil, false, 0)
		for _, mt := range memtables {
			iter := mt.NewIterator()
			mi.AddIterator(iter)
		}

		// Seek to different positions and iterate
		seekPositions := []int{0, totalKeys / 4, totalKeys / 2, totalKeys * 3 / 4}
		for _, pos := range seekPositions {
			target := fmt.Appendf(nil, "key%016d", pos)
			targetKey := keys.NewEncodedKey(target, 0, keys.KindSet)
			mi.Seek(targetKey)

			// Iterate through some entries
			for j := 0; j < 100 && mi.Valid(); j++ {
				_ = mi.Key()
				_ = mi.Value()
				mi.Next()
			}
		}

		mi.Close()
	}
}

// BenchmarkMergeIteratorFullScanSmallInputs measures full scan performance with few iterators
func BenchmarkMergeIteratorFullScanSmallInputs(b *testing.B) {
	numIterators := 5
	keysPerIterator := 1000

	// Create multiple memtables with different key ranges
	memtables := make([]*memtable.MemTable, numIterators)
	for m := range numIterators {
		memtables[m] = memtable.NewMemtable(1024 * 1024 * 10) // 10MB buffer
		// Fill with non-overlapping key ranges
		for i := range keysPerIterator {
			globalIdx := m*keysPerIterator + i
			key := fmt.Appendf(nil, "key%016d", globalIdx)
			encodedKey := keys.NewEncodedKey(key, 0, keys.KindSet)
			value := fmt.Appendf(nil, "value%016d", globalIdx)
			memtables[m].Put(encodedKey, value)
		}
	}

	// Reset timer after setup
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// Create merge iterator
		mi := NewMergeIterator(nil, false, 0)
		for _, mt := range memtables {
			iter := mt.NewIterator()
			mi.AddIterator(iter)
		}

		// Full scan from beginning
		mi.SeekToFirst()
		for mi.Valid() {
			_ = mi.Key()
			_ = mi.Value()
			mi.Next()
		}

		mi.Close()
	}
}

// BenchmarkMergeIteratorFullScanLargeInputs measures full scan performance with many iterators
func BenchmarkMergeIteratorFullScanLargeInputs(b *testing.B) {
	numIterators := 40
	keysPerIterator := 500

	// Create multiple memtables with different key ranges
	memtables := make([]*memtable.MemTable, numIterators)
	for m := range numIterators {
		memtables[m] = memtable.NewMemtable(1024 * 1024 * 10) // 10MB buffer
		// Fill with non-overlapping key ranges
		for i := range keysPerIterator {
			globalIdx := m*keysPerIterator + i
			key := fmt.Appendf(nil, "key%016d", globalIdx)
			encodedKey := keys.NewEncodedKey(key, 0, keys.KindSet)
			value := fmt.Appendf(nil, "value%016d", globalIdx)
			memtables[m].Put(encodedKey, value)
		}
	}

	// Reset timer after setup
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// Create merge iterator
		mi := NewMergeIterator(nil, false, 0)
		for _, mt := range memtables {
			iter := mt.NewIterator()
			mi.AddIterator(iter)
		}

		// Full scan from beginning
		mi.SeekToFirst()
		for mi.Valid() {
			_ = mi.Key()
			_ = mi.Value()
			mi.Next()
		}

		mi.Close()
	}
}

// BenchmarkMergeIteratorFindMinimum benchmarks the findMinimumIterator operation
// This is the critical path for each Next() operation
func BenchmarkMergeIteratorFindMinimum(b *testing.B) {
	numIterators := 40
	keysPerIterator := 100

	// Create multiple memtables
	memtables := make([]*memtable.MemTable, numIterators)
	iterators := make([]Iterator, numIterators)
	for m := range numIterators {
		memtables[m] = memtable.NewMemtable(1024 * 1024 * 10) // 10MB buffer
		for i := range keysPerIterator {
			globalIdx := m*keysPerIterator + i
			key := fmt.Appendf(nil, "key%016d", globalIdx)
			encodedKey := keys.NewEncodedKey(key, 0, keys.KindSet)
			value := fmt.Appendf(nil, "value%016d", globalIdx)
			memtables[m].Put(encodedKey, value)
		}
		iterators[m] = memtables[m].NewIterator()
		iterators[m].SeekToFirst()
	}

	mi := NewMergeIterator(nil, false, 0)
	mi.iterators = iterators

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// Benchmark the findMinimumIterator operation
		_, _ = mi.findMinimumIterator()
	}
}

// BenchmarkMergeIteratorWithDuplicates measures performance when dealing with duplicate keys across iterators
func BenchmarkMergeIteratorWithDuplicates(b *testing.B) {
	numIterators := 10
	keysPerIterator := 1000

	// Create multiple memtables with overlapping keys (simulating compaction scenario)
	memtables := make([]*memtable.MemTable, numIterators)
	for m := range numIterators {
		memtables[m] = memtable.NewMemtable(1024 * 1024 * 10) // 10MB buffer
		// Fill with overlapping key ranges
		for i := range keysPerIterator {
			key := fmt.Appendf(nil, "key%016d", i) // Same keys across all memtables
			// Use different sequence numbers for each memtable
			encodedKey := keys.NewEncodedKey(key, uint64(numIterators-m), keys.KindSet)
			value := fmt.Appendf(nil, "value%016d", m*1000+i)
			memtables[m].Put(encodedKey, value)
		}
	}

	// Reset timer after setup
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		// Create merge iterator
		mi := NewMergeIterator(nil, false, 0)
		for _, mt := range memtables {
			iter := mt.NewIterator()
			mi.AddIterator(iter)
		}

		// Full scan - merge iterator must deduplicate keys
		mi.SeekToFirst()
		count := 0
		for mi.Valid() && count < 2000 {
			_ = mi.Key()
			_ = mi.Value()
			mi.Next()
			count++
		}

		mi.Close()
	}
}
