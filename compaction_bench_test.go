package lgdb

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
)

// BenchmarkCompactionL0ToL1 measures performance of L0->L1 compaction
func BenchmarkCompactionL0ToL1(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 512 * 1024 // 512KB memtable
	opts.L0CompactionTrigger = 2      // Trigger compaction at 2 L0 files

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Generate test data
	count := 10000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i)
	}

	// Pre-fill database to create multiple L0 files
	for i := 0; i < count*3; i++ {
		if err := db.Put(keys[i%count], values[i%count]); err != nil {
			b.Fatal(err)
		}
	}

	// Wait for compaction to complete
	db.WaitForCompaction()

	b.ReportAllocs()

	for b.Loop() {
		// Write data that triggers compaction
		for i := range count {
			if err := db.Put(keys[i], values[i]); err != nil {
				b.Fatal(err)
			}
		}
		// Wait for compaction to complete
		db.WaitForCompaction()
	}
}

// BenchmarkCompactionWithDeletions measures compaction performance with delete tombstones
func BenchmarkCompactionWithDeletions(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 512 * 1024
	opts.L0CompactionTrigger = 2

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Generate test data
	count := 10000
	keys := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
	}

	// Pre-fill database
	values := make([][]byte, count)
	for i := range count {
		values[i] = fmt.Appendf(nil, "value%0100d", i)
	}

	for i := 0; i < count*2; i++ {
		if err := db.Put(keys[i%count], values[i%count]); err != nil {
			b.Fatal(err)
		}
	}
	db.WaitForCompaction()

	b.ReportAllocs()

	for b.Loop() {
		// Delete half the keys, then write them back (creates tombstones)
		for i := 0; i < count/2; i++ {
			if err := db.Delete(keys[i]); err != nil {
				b.Fatal(err)
			}
		}
		for i := 0; i < count/2; i++ {
			if err := db.Put(keys[i], values[i]); err != nil {
				b.Fatal(err)
			}
		}
		db.WaitForCompaction()
	}
}

// BenchmarkCompactionMultiLevel measures compaction performance across multiple levels
func BenchmarkCompactionMultiLevel(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 256 * 1024 // Smaller buffer to trigger more compactions
	opts.L0CompactionTrigger = 2

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Generate test data
	count := 5000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i)
	}

	// Pre-fill database with more data to trigger multi-level compaction
	for i := 0; i < count*5; i++ {
		if err := db.Put(keys[i%count], values[i%count]); err != nil {
			b.Fatal(err)
		}
	}
	db.WaitForCompaction()

	b.ReportAllocs()

	for b.Loop() {
		// Write a large amount of data to trigger multi-level compaction
		for i := 0; i < count*2; i++ {
			if err := db.Put(keys[i%count], values[i%count]); err != nil {
				b.Fatal(err)
			}
		}
		db.WaitForCompaction()
	}
}

// BenchmarkCompactionMemtableFlush measures memtable flush performance
func BenchmarkCompactionMemtableFlush(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 256 * 1024

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Generate test data
	count := 2000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i)
	}

	b.ReportAllocs()

	for b.Loop() {
		// Fill up memtable and trigger flush
		for i := range count {
			if err := db.Put(keys[i], values[i]); err != nil {
				b.Fatal(err)
			}
		}
		db.WaitForCompaction()
	}
}
