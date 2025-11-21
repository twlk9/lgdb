package lgdb

import (
	"fmt"
	"testing"
)

func BenchmarkIterationWithRangeDeletes(b *testing.B) {
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.Path = dir
	// Disable background work to make the benchmark more deterministic.
	opts.L0CompactionTrigger = 1000
	opts.L0StopWritesTrigger = 1001

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	const numKeys = 10000
	const numDeletes = 1000

	// Write a large number of keys.
	for i := range numKeys {
		key := fmt.Sprintf("key%05d", i)
		val := fmt.Sprintf("val%05d", i)
		if err := db.Put([]byte(key), []byte(val)); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	// Create many small, non-overlapping range deletions.
	for i := range numDeletes {
		startIdx := i * (numKeys / numDeletes)
		startKey := fmt.Sprintf("key%05d", startIdx)
		endKey := fmt.Sprintf("key%05d", startIdx+1)
		if err := db.DeleteRange([]byte(startKey), []byte(endKey)); err != nil {
			b.Fatalf("DeleteRange failed: %v", err)
		}
	}

	for b.Loop() {
		iter := db.NewIterator(nil)
		count := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			// Accessing the key is enough to trigger the logic.
			_ = iter.Key()
			count++
		}
		iter.Close()
	}
}
