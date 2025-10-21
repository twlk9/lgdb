package lgdb

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkConcurrentPuts measures throughput with concurrent write operations
func BenchmarkConcurrentPuts(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 10 * 1024 * 1024 // Large buffer to reduce flushes

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Prepare test data
	count := 10000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i)
	}

	numGoroutines := 4
	opsPerGoroutine := b.N / numGoroutines

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				key := keys[(goroutineID*opsPerGoroutine+i)%count]
				value := values[(goroutineID*opsPerGoroutine+i)%count]
				if err := db.Put(key, value); err != nil {
					b.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkConcurrentGets measures throughput with concurrent read operations
func BenchmarkConcurrentGets(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Prepare test data
	count := 10000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i)
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}

	numGoroutines := 4
	opsPerGoroutine := b.N / numGoroutines

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				key := keys[(goroutineID*opsPerGoroutine+i)%count]
				_, err := db.Get(key)
				if err != nil && err != ErrNotFound {
					b.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkConcurrentMixed measures throughput with mixed read/write operations
func BenchmarkConcurrentMixed(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 10 * 1024 * 1024

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Prepare test data
	count := 10000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i)
	}

	// Pre-populate with initial data
	for i := range count {
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}

	numGoroutines := 4
	opsPerGoroutine := b.N / numGoroutines

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				keyIdx := (goroutineID*opsPerGoroutine + i) % count
				if i%3 == 0 {
					// 1 in 3 writes
					if err := db.Put(keys[keyIdx], values[keyIdx]); err != nil {
						b.Error(err)
					}
				} else {
					// 2 in 3 reads
					_, err := db.Get(keys[keyIdx])
					if err != nil && err != ErrNotFound {
						b.Error(err)
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkConcurrentDeletes measures throughput with concurrent delete operations
func BenchmarkConcurrentDeletes(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 10 * 1024 * 1024

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Prepare test data - use more keys than needed to avoid contention
	count := b.N * 2
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i)
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}

	numGoroutines := 4
	opsPerGoroutine := b.N / numGoroutines

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	var deleteCount int64
	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for range opsPerGoroutine {
				keyIdx := int(atomic.AddInt64(&deleteCount, 1)) - 1
				if keyIdx < count {
					if err := db.Delete(keys[keyIdx]); err != nil {
						b.Error(err)
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkConcurrentRangeScans measures throughput with concurrent range scans
func BenchmarkConcurrentRangeScans(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Prepare test data
	count := 10000
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range count {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i)
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}

	numGoroutines := 4
	scansPerGoroutine := b.N / numGoroutines

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := range scansPerGoroutine {
				startIdx := (goroutineID*scansPerGoroutine + i) % (count - 100)
				endIdx := startIdx + 100
				iter := db.NewIterator(nil)
				if iter == nil {
					b.Error("failed to create iterator")
					return
				}
				iter.Seek(keys[startIdx])
				for iter.Valid() && iter.Key()[0] <= keys[endIdx][0] {
					iter.Next()
				}
				iter.Close()
			}
		}(g)
	}
	wg.Wait()
}
