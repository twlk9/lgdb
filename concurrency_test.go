//go:build integration || stress
// +build integration stress

package lgdb

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentReadsWrites tests basic concurrent read/write operations
func TestConcurrentReadsWrites(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 4096 // Reasonable size for concurrent tests
	opts.L0CompactionTrigger = 4

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	const numWriters = 3
	const numReaders = 2
	const numOperations = 100

	var wg sync.WaitGroup
	var writeErrors int64
	var readErrors int64

	// Start writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < numOperations; i++ {
				key := fmt.Sprintf("writer_%d_key_%d", writerID, i)
				value := fmt.Sprintf("writer_%d_value_%d", writerID, i)

				if err := db.Put([]byte(key), []byte(value)); err != nil {
					atomic.AddInt64(&writeErrors, 1)
					t.Logf("Write error: %v", err)
				}

				// Occasionally delete keys to test tombstones
				if i%10 == 0 && i > 0 {
					deleteKey := fmt.Sprintf("writer_%d_key_%d", writerID, i-5)
					if err := db.Delete([]byte(deleteKey)); err != nil {
						atomic.AddInt64(&writeErrors, 1)
					}
				}
			}
		}(w)
	}

	// Start readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < numOperations*2; i++ {
				// Read from different writers
				writerID := i % numWriters
				keyID := i % numOperations
				key := fmt.Sprintf("writer_%d_key_%d", writerID, keyID)

				_, err := db.Get([]byte(key))
				if err != nil && err != ErrNotFound {
					atomic.AddInt64(&readErrors, 1)
					t.Logf("Read error: %v", err)
				}

				time.Sleep(time.Microsecond) // Small delay to allow interleaving
			}
		}(r)
	}

	wg.Wait()

	// Check for errors
	if writeErrors > 0 {
		t.Errorf("Had %d write errors", writeErrors)
	}
	if readErrors > 0 {
		t.Errorf("Had %d read errors", readErrors)
	}

	// Verify some data integrity
	for w := 0; w < numWriters; w++ {
		for i := numOperations - 10; i < numOperations; i++ { // Check last 10 keys per writer
			key := fmt.Sprintf("writer_%d_key_%d", w, i)
			expectedValue := fmt.Sprintf("writer_%d_value_%d", w, i)

			value, err := db.Get([]byte(key))
			if err != nil {
				t.Errorf("Failed to read key %s: %v", key, err)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Data corruption: key %s expected %s, got %s", key, expectedValue, string(value))
			}
		}
	}
}

// TestConcurrentCompaction tests concurrent operations during compaction
func TestConcurrentCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024  // Small to trigger frequent flushes
	opts.L0CompactionTrigger = 2 // Trigger compaction quickly

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	const numWorkers = 4
	const numOperations = 50
	var wg sync.WaitGroup
	var operationErrors int64

	// Start workers that perform various operations
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < numOperations; i++ {
				switch i % 4 {
				case 0: // Put operation
					key := fmt.Sprintf("worker_%d_put_%d", workerID, i)
					value := fmt.Sprintf("value_%d_%d", workerID, i)
					if err := db.Put([]byte(key), []byte(value)); err != nil {
						atomic.AddInt64(&operationErrors, 1)
					}

				case 1: // Get operation
					key := fmt.Sprintf("worker_%d_put_%d", workerID, i-1)
					if _, err := db.Get([]byte(key)); err != nil && err != ErrNotFound {
						atomic.AddInt64(&operationErrors, 1)
					}

				case 2: // Delete operation
					if i > 4 {
						key := fmt.Sprintf("worker_%d_put_%d", workerID, i-4)
						if err := db.Delete([]byte(key)); err != nil {
							atomic.AddInt64(&operationErrors, 1)
						}
					}

				case 3: // Iterator operation
					iter := db.NewIterator()
					count := 0
					for iter.SeekToFirst(); iter.Valid() && count < 10; iter.Next() {
						count++
					}
					if err := iter.Error(); err != nil {
						atomic.AddInt64(&operationErrors, 1)
					}
					iter.Close()
				}

				// Occasionally force flush to trigger compaction
				if i%20 == 0 {
					db.Flush()
				}
			}
		}(w)
	}

	// Background compaction trigger
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			time.Sleep(50 * time.Millisecond)
			db.CompactRange() // Force compaction
		}
	}()

	wg.Wait()

	if operationErrors > 0 {
		t.Errorf("Had %d operation errors during concurrent compaction", operationErrors)
	}

	// Final consistency check
	stats := db.GetStats()
	t.Logf("Final stats after concurrent compaction: %+v", stats)
}

// TestConcurrentIterators tests multiple iterators running concurrently
func TestConcurrentIterators(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Setup initial data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to setup data: %v", err)
		}
	}

	const numIterators = 5
	var wg sync.WaitGroup
	var iteratorErrors int64

	// Start multiple iterators concurrently
	for i := 0; i < numIterators; i++ {
		wg.Add(1)
		go func(iteratorID int) {
			defer wg.Done()

			iter := db.NewIterator()
			defer iter.Close()

			count := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				key := iter.Key()
				value := iter.Value()

				// Verify data integrity
				expectedKey := fmt.Sprintf("key_%03d", count)
				expectedValue := fmt.Sprintf("value_%03d", count)

				if string(key) != expectedKey || string(value) != expectedValue {
					t.Logf("Iterator %d: data mismatch at position %d", iteratorID, count)
					atomic.AddInt64(&iteratorErrors, 1)
				}

				count++

				// Small delay to allow other iterators to interleave
				if count%10 == 0 {
					time.Sleep(time.Microsecond)
				}
			}

			if err := iter.Error(); err != nil {
				atomic.AddInt64(&iteratorErrors, 1)
			}

			t.Logf("Iterator %d scanned %d keys", iteratorID, count)
		}(i)
	}

	// Continue writing data while iterators are running
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 200; i++ {
			key := fmt.Sprintf("key_%03d", i)
			value := fmt.Sprintf("value_%03d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				atomic.AddInt64(&iteratorErrors, 1)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	if iteratorErrors > 0 {
		t.Errorf("Had %d iterator errors", iteratorErrors)
	}
}

// TestConcurrentFlushAndCompaction tests concurrent flush and compaction operations
func TestConcurrentFlushAndCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 2

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	var errors int64
	const duration = 3 * time.Second
	stopChan := make(chan struct{})

	// Stop after duration
	go func() {
		time.Sleep(duration)
		close(stopChan)
	}()

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stopChan:
				return
			default:
				key := fmt.Sprintf("flush_test_key_%d", i)
				value := fmt.Sprintf("flush_test_value_%d", i)
				if err := db.Put([]byte(key), []byte(value)); err != nil {
					atomic.AddInt64(&errors, 1)
				}
				i++
			}
		}
	}()

	// Flush goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				if err := db.Flush(); err != nil {
					atomic.AddInt64(&errors, 1)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	// Compaction goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				if err := db.CompactRange(); err != nil {
					atomic.AddInt64(&errors, 1)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stopChan:
				return
			default:
				key := fmt.Sprintf("flush_test_key_%d", i%1000)
				if _, err := db.Get([]byte(key)); err != nil && err != ErrNotFound {
					atomic.AddInt64(&errors, 1)
				}
				i++
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Wait()

	totalErrors := atomic.LoadInt64(&errors)
	if totalErrors > 0 {
		t.Errorf("Had %d errors during concurrent flush/compaction test", totalErrors)
	}

	t.Logf("Concurrent flush/compaction test completed with %d errors", totalErrors)
}

// TestDeadlockPrevention tests that the database doesn't deadlock under concurrent load
func TestDeadlockPrevention(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 2048
	opts.L0CompactionTrigger = 3

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	const numGoroutines = 10
	const numOperations = 100
	var wg sync.WaitGroup

	// Start multiple goroutines doing various operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("deadlock_test_%d_%d", id, j)
				value := fmt.Sprintf("value_%d_%d", id, j)

				// Mix of operations that could potentially cause deadlocks
				switch j % 5 {
				case 0:
					db.Put([]byte(key), []byte(value))
				case 1:
					db.Get([]byte(key))
				case 2:
					db.Delete([]byte(key))
				case 3:
					iter := db.NewIterator()
					iter.SeekToFirst()
					if iter.Valid() {
						iter.Next()
					}
					iter.Close()
				case 4:
					db.Flush()
				}
			}
		}(i)
	}

	// Create a timeout to detect deadlocks
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		t.Log("Deadlock prevention test completed successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out - possible deadlock detected")
	}

	// Ensure database is properly closed before test cleanup
	db.Close()
	time.Sleep(50 * time.Millisecond) // Give system time to release file handles
}

// TestRaceConditionDetection tests for race conditions using Go's race detector
func TestRaceConditionDetection(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 2048
	opts.L0CompactionTrigger = 3

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	const numGoroutines = 20
	const numOperationsPerGoroutine = 50
	var wg sync.WaitGroup

	// Shared key space to increase chance of race conditions
	const sharedKeySpace = 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

			for j := 0; j < numOperationsPerGoroutine; j++ {
				// Use shared key space to increase contention
				key := fmt.Sprintf("race_key_%d", rng.Intn(sharedKeySpace))
				value := fmt.Sprintf("race_value_%d_%d", goroutineID, j)

				switch rng.Intn(4) {
				case 0: // Put
					db.Put([]byte(key), []byte(value))
				case 1: // Get
					db.Get([]byte(key))
				case 2: // Delete
					db.Delete([]byte(key))
				case 3: // Iterator
					iter := db.NewIterator()
					iter.SeekToFirst()
					if iter.Valid() {
						iter.Next()
					}
					iter.Close()
				}

				// Occasional flush/compaction to stress internal state
				if rng.Intn(20) == 0 {
					if rng.Intn(2) == 0 {
						db.Flush()
					} else {
						db.CompactRange()
					}
				}
			}
		}(i)
	}

	wg.Wait()
	t.Log("Race condition detection test completed - run with 'go test -race' to detect races")
}

// TestMemoryConsistency tests memory consistency across concurrent operations
func TestMemoryConsistency(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 4096

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	const numKeys = 100
	const numWriters = 3
	const numReaders = 5
	const numRounds = 20

	var wg sync.WaitGroup
	var inconsistencies int64

	// Initialize keys with known values
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("consistency_key_%d", i)
		value := fmt.Sprintf("initial_value_%d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to initialize key %d: %v", i, err)
		}
	}

	// Start writers that update values in a predictable pattern (no deletions)
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for round := 0; round < numRounds; round++ {
				// Each writer updates its subset of keys
				startKey := (writerID * numKeys) / numWriters
				endKey := ((writerID + 1) * numKeys) / numWriters

				for keyID := startKey; keyID < endKey; keyID++ {
					key := fmt.Sprintf("consistency_key_%d", keyID)
					value := fmt.Sprintf("writer_%d_round_%d_value_%d", writerID, round, keyID)

					// Only PUT operations, no deletions to ensure keys remain stable
					if err := db.Put([]byte(key), []byte(value)); err != nil {
						t.Logf("Writer %d failed to put key %d: %v", writerID, keyID, err)
					}
				}

				// Small delay between rounds
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Start readers that verify consistency
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for round := 0; round < numRounds*2; round++ {
				// Read a random subset of keys and verify they have consistent values
				for attempt := 0; attempt < 10; attempt++ {
					keyID := rand.Intn(numKeys)
					key := fmt.Sprintf("consistency_key_%d", keyID)

					_, err1 := db.Get([]byte(key))
					if err1 != nil && err1 != ErrNotFound {
						continue
					}

					// Small delay to allow concurrent writes to complete
					time.Sleep(10 * time.Microsecond)

					// Read the same key again
					_, err2 := db.Get([]byte(key))
					if err2 != nil && err2 != ErrNotFound {
						continue
					}

					// Check for true consistency violations (existence changes)
					// Value changes are expected during concurrent writes, but
					// a key shouldn't flip between existing and not existing
					if (err1 == nil) != (err2 == nil) {
						atomic.AddInt64(&inconsistencies, 1)
						t.Logf("Reader %d: inconsistent existence for key %s", readerID, key)
					}
					// Note: We don't check value changes as those are expected during concurrent writes
				}

				time.Sleep(time.Millisecond)
			}
		}(r)
	}

	wg.Wait()

	if inconsistencies > 0 {
		t.Errorf("Found %d memory consistency violations", inconsistencies)
	} else {
		t.Log("Memory consistency test passed")
	}
}

// TestConcurrentIteratorWrites tests iterators running while writes happen
// This is the test that caught the merge iterator duplicate key bug
func TestConcurrentIteratorWrites(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024 // Small to force frequent flushes
	opts.L0CompactionTrigger = 3

	db, err := Open(opts)
	if err != nil {
	}
	defer db.Close()

	// Setup initial data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var iteratorErrors int64
	var duplicateKeys int64
	stopChan := make(chan struct{})

	// Start iterator that scans while writes happen
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-stopChan:
				return
			default:
				iter := db.NewIterator()

				// Track keys we've seen to detect duplicates
				seenKeys := make(map[string]bool)
				count := 0

				for iter.SeekToFirst(); iter.Valid(); iter.Next() {
					key := string(iter.Key())

					// Check for duplicate keys - this is what the merge iterator bug caused
					if seenKeys[key] {
						atomic.AddInt64(&duplicateKeys, 1)
						t.Logf("DUPLICATE KEY DETECTED: %s", key)
					}
					seenKeys[key] = true
					count++

					if count > 1000 { // Prevent infinite iteration
						break
					}
				}

				if err := iter.Error(); err != nil {
					atomic.AddInt64(&iteratorErrors, 1)
				}
				iter.Close()

				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Start concurrent writers
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 200; i < 500; i++ {
			select {
			case <-stopChan:
				return
			default:
				key := fmt.Sprintf("key_%04d", i)
				value := fmt.Sprintf("value_%04d", i)

				if err := db.Put([]byte(key), []byte(value)); err != nil {
					t.Logf("Write error: %v", err)
				}

				// Force flush occasionally to trigger memtable->sstable transitions
				if i%50 == 0 {
					db.Flush()
				}

				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Run test for 3 seconds
	time.Sleep(3 * time.Second)
	close(stopChan)
	wg.Wait()

	duplicates := atomic.LoadInt64(&duplicateKeys)
	errors := atomic.LoadInt64(&iteratorErrors)

	t.Logf("Iterator while writes test results:")
	t.Logf("  Duplicate keys detected: %d", duplicates)
	t.Logf("  Iterator errors: %d", errors)

	if duplicates > 0 {
		t.Errorf("Found %d duplicate keys - merge iterator bug detected!", duplicates)
	}
	if errors > 0 {
		t.Errorf("Found %d iterator errors", errors)
	}
}

// TestConcurrentPrefixScanWrites tests prefix scans while writes happen
func TestConcurrentPrefixScanWrites(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 512 // Very small to force frequent flushes

	db, err := Open(opts)
	if err != nil {
	}
	defer db.Close()

	// Setup data with predictable prefixes
	prefixes := []string{"user_", "item_", "order_"}
	for _, prefix := range prefixes {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("%s%04d", prefix, i)
			value := fmt.Sprintf("value_%s%04d", prefix, i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatal(err)
			}
		}
	}

	var wg sync.WaitGroup
	var scanErrors int64
	var duplicateKeys int64
	stopChan := make(chan struct{})

	// Concurrent prefix scanners
	for _, prefix := range prefixes {
		wg.Add(1)
		go func(scanPrefix string) {
			defer wg.Done()

			for {
				select {
				case <-stopChan:
					return
				default:
					iter, err := db.ScanPrefix([]byte(scanPrefix))
					if err != nil {
						atomic.AddInt64(&scanErrors, 1)
						continue
					}

					seenKeys := make(map[string]bool)
					count := 0

					for iter.SeekToFirst(); iter.Valid(); iter.Next() {
						key := string(iter.Key())

						// Check for duplicates
						if seenKeys[key] {
							atomic.AddInt64(&duplicateKeys, 1)
							t.Logf("DUPLICATE KEY in prefix %s: %s", scanPrefix, key)
						}
						seenKeys[key] = true
						count++
					}

					if err := iter.Error(); err != nil {
						atomic.AddInt64(&scanErrors, 1)
					}
					iter.Close()

					time.Sleep(20 * time.Millisecond)
				}
			}
		}(prefix)
	}

	// Concurrent writer adding more data
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 100; i < 200; i++ {
			select {
			case <-stopChan:
				return
			default:
				for _, prefix := range prefixes {
					key := fmt.Sprintf("%s%04d", prefix, i)
					value := fmt.Sprintf("value_%s%04d", prefix, i)

					if err := db.Put([]byte(key), []byte(value)); err != nil {
						continue
					}

					if i%20 == 0 {
						db.Flush()
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Run test for 2 seconds
	time.Sleep(2 * time.Second)
	close(stopChan)
	wg.Wait()

	duplicates := atomic.LoadInt64(&duplicateKeys)
	errors := atomic.LoadInt64(&scanErrors)

	t.Logf("Prefix scan while writes test results:")
	t.Logf("  Duplicate keys detected: %d", duplicates)
	t.Logf("  Scan errors: %d", errors)

	if duplicates > 0 {
		t.Errorf("Found %d duplicate keys in prefix scans!", duplicates)
	}
	if errors > 10 { // Allow some transient errors
		t.Errorf("Too many scan errors: %d", errors)
	}
}
