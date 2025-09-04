//go:build stress
// +build stress

package lgdb

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Helper functions for stress tests
func setupTestDB(t *testing.T) (string, *DB) {
	t.Helper()
	tempDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tempDir
	opts.WriteBufferSize = 1024
	opts.Sync = false
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return tempDir, db
}

func cleanupTestDB(t *testing.T, tempDir string, db *DB) {
	t.Helper()
	if db != nil {
		t.Logf("Closing database at %s", tempDir)
		if err := db.Close(); err != nil {
			t.Logf("Warning: error closing database: %v", err)
		} else {
			t.Logf("Database closed successfully")
		}

		// Give a moment for any lingering file handles to close
		time.Sleep(10 * time.Millisecond)

		// Attempt to remove temp directory with retry
		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			// The Go testing framework handles TempDir cleanup automatically,
			// but if cleanup fails, we can try to help by removing files manually
			if err := removeAllFiles(tempDir); err != nil {
				if i == maxRetries-1 {
					t.Logf("Warning: failed to clean up temp directory after %d attempts: %v", maxRetries, err)
				} else {
					// Wait briefly and retry
					time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
				}
			} else {
				break // Success
			}
		}
	}
}

// removeAllFiles attempts to remove all files in a directory recursively
func removeAllFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	return nil
}

// Move stress tests from concurrency_test.go here
func TestStressTest(t *testing.T) {
	tempDir, db := setupTestDB(t)
	defer cleanupTestDB(t, tempDir, db)

	// Stress test configuration
	duration := 5 * time.Second
	numGoroutines := 10
	keyRange := 1000

	var (
		totalOps    int64
		totalErrors int64
		wg          sync.WaitGroup
	)

	start := time.Now()
	deadline := start.Add(duration)

	// Start worker goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(int64(workerID)))
			for time.Now().Before(deadline) {
				key := []byte(fmt.Sprintf("stress_key_%d", r.Intn(keyRange)))
				value := []byte(fmt.Sprintf("stress_value_%d_%d", workerID, r.Int()))

				// Random operation
				switch r.Intn(3) {
				case 0: // PUT
					err := db.Put(key, value)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					}
				case 1: // GET
					_, err := db.Get(key)
					if err != nil && err != ErrNotFound {
						atomic.AddInt64(&totalErrors, 1)
					}
				case 2: // DELETE
					err := db.Delete(key)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					}
				}

				atomic.AddInt64(&totalOps, 1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	opsPerSecond := float64(totalOps) / elapsed.Seconds()
	errorRate := float64(totalErrors) / float64(totalOps) * 100

	t.Logf("Stress test completed:")
	t.Logf("  Total operations: %d", totalOps)
	t.Logf("  Operations/second: %.0f", opsPerSecond)
	t.Logf("  Total errors: %d", totalErrors)
	t.Logf("  Error rate: %.2f%%", errorRate)

	// Verify database is still functional
	stats := db.GetStats()
	t.Logf("Final database stats: %+v", stats)

	// Error rate should be very low
	if errorRate > 5.0 {
		t.Errorf("Error rate too high: %.2f%%", errorRate)
	}
}

func TestHighContentionStress(t *testing.T) {
	tempDir, db := setupTestDB(t)
	defer cleanupTestDB(t, tempDir, db)

	// High contention test - many goroutines hitting same keys
	duration := 3 * time.Second
	numGoroutines := 50
	hotKeys := 5 // Small number of keys for high contention

	var (
		totalOps    int64
		totalErrors int64
		wg          sync.WaitGroup
	)

	start := time.Now()
	deadline := start.Add(duration)

	// Start worker goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(int64(workerID)))
			for time.Now().Before(deadline) {
				// High contention - all workers hit same small set of keys
				keyID := r.Intn(hotKeys)
				key := []byte(fmt.Sprintf("hot_key_%d", keyID))
				value := []byte(fmt.Sprintf("hot_value_%d_%d", workerID, r.Int()))

				// Random operation with bias toward writes
				switch r.Intn(10) {
				case 0, 1, 2, 3, 4, 5: // PUT (60% of operations)
					err := db.Put(key, value)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					}
				case 6, 7, 8: // GET (30% of operations)
					_, err := db.Get(key)
					if err != nil && err != ErrNotFound {
						atomic.AddInt64(&totalErrors, 1)
					}
				case 9: // DELETE (10% of operations)
					err := db.Delete(key)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					}
				}

				atomic.AddInt64(&totalOps, 1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	opsPerSecond := float64(totalOps) / elapsed.Seconds()
	errorRate := float64(totalErrors) / float64(totalOps) * 100

	t.Logf("High contention stress test results:")
	t.Logf("  Goroutines: %d", numGoroutines)
	t.Logf("  Hot keys: %d", hotKeys)
	t.Logf("  Total operations: %d", totalOps)
	t.Logf("  Operations/second: %.0f", opsPerSecond)
	t.Logf("  Total errors: %d", totalErrors)
	t.Logf("  Error rate: %.2f%%", errorRate)

	// Verify all hot keys exist and have values
	for i := 0; i < hotKeys; i++ {
		key := []byte(fmt.Sprintf("hot_key_%d", i))
		value, err := db.Get(key)
		if err == ErrNotFound {
			// It's OK if some keys don't exist due to deletes
			continue
		}
		if err != nil {
			t.Errorf("Error reading hot key %d: %v", i, err)
		}
		if len(value) == 0 {
			t.Errorf("Hot key %d has empty value", i)
		}
	}

	// Error rate should be very low even under contention
	if errorRate > 5.0 {
		t.Errorf("Error rate too high under contention: %.2f%%", errorRate)
	}
}

func TestConcurrentCompactionStress(t *testing.T) {
	tempDir, db := setupTestDB(t)
	defer cleanupTestDB(t, tempDir, db)

	// Phase 1: Populate database to trigger compaction
	t.Log("Phase 1: Populating database to trigger compaction...")
	keyRange := 1000
	for i := 0; i < keyRange; i++ {
		key := []byte(fmt.Sprintf("setup_key_%04d", i))
		value := []byte(fmt.Sprintf("setup_value_%04d", i))
		err := db.Put(key, value)
		if err != nil {
		}

		// Force flush every 100 keys to create multiple L0 files
		if i%100 == 99 {
			db.Flush()
		}
	}

	// Check that we have multiple L0 files to trigger compaction
	stats := db.GetStats()
	levels := stats["levels"].(map[string]int)
	t.Logf("After setup: L0 files = %d", levels["level_0_files"])

	if levels["level_0_files"] < 3 {
		// Force more flushes to ensure compaction will be triggered
		for i := 0; i < 200; i++ {
			key := []byte(fmt.Sprintf("extra_key_%04d", i))
			value := []byte(fmt.Sprintf("extra_value_%04d", i))
			db.Put(key, value)
		}
		db.Flush()
	}

	// Phase 2: Start light concurrent operations while compaction runs
	t.Log("Phase 2: Starting light concurrent operations...")
	duration := 3 * time.Second
	numReaders := 5 // Light read load
	numWriters := 2 // Very light write load

	var (
		totalReads  int64
		totalWrites int64
		totalErrors int64
		wg          sync.WaitGroup
		done        = make(chan struct{})
	)

	// Stop after duration
	go func() {
		time.Sleep(duration)
		close(done)
	}()

	// Light reader load
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(readerID)))

			for {
				select {
				case <-done:
					return
				default:
					// Read from setup data
					keyID := r.Intn(keyRange)
					key := []byte(fmt.Sprintf("setup_key_%04d", keyID))

					_, err := db.Get(key)
					if err != nil && err != ErrNotFound {
						atomic.AddInt64(&totalErrors, 1)
					}
					atomic.AddInt64(&totalReads, 1)

					// Slow down reads to not overwhelm
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i)
	}

	// Very light writer load
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
					// Write new keys (not conflicting with setup data)
					key := []byte(fmt.Sprintf("concurrent_key_%d_%d", writerID, atomic.LoadInt64(&totalWrites)))
					value := []byte(fmt.Sprintf("concurrent_value_%d", writerID))

					err := db.Put(key, value)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
					}
					atomic.AddInt64(&totalWrites, 1)

					// Very slow writes to not interfere with compaction
					time.Sleep(50 * time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()

	// Phase 3: Let compaction finish and verify
	t.Log("Phase 3: Waiting for compaction to complete...")
	db.WaitForCompaction()

	// Get final stats
	finalStats := db.GetStats()
	finalLevels := finalStats["levels"].(map[string]int)

	reads := atomic.LoadInt64(&totalReads)
	writes := atomic.LoadInt64(&totalWrites)
	errors := atomic.LoadInt64(&totalErrors)
	errorRate := float64(errors) / float64(reads+writes) * 100

	t.Logf("Concurrent compaction stress test results:")
	t.Logf("  Setup keys: %d", keyRange)
	t.Logf("  Concurrent reads: %d", reads)
	t.Logf("  Concurrent writes: %d", writes)
	t.Logf("  Total errors: %d", errors)
	t.Logf("  Error rate: %.2f%%", errorRate)
	t.Logf("  Final L0 files: %d", finalLevels["level_0_files"])
	t.Logf("  Final L1 files: %d", finalLevels["level_1_files"])

	// Verify data integrity - check some setup keys still exist
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("setup_key_%04d", i*100))
		expectedValue := []byte(fmt.Sprintf("setup_value_%04d", i*100))

		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Setup key %s not found after compaction: %v", key, err)
		} else if string(value) != string(expectedValue) {
			t.Errorf("Setup key %s has wrong value: got %s, expected %s", key, value, expectedValue)
		}
	}

	// Error rate should be very low
	if errorRate > 5.0 {
		t.Errorf("Error rate too high during compaction: %.2f%%", errorRate)
	}

	// Compaction should have moved data to L1 (some L0 increase is expected from concurrent writes)
	if finalLevels["level_1_files"] == 0 {
		t.Errorf("No L1 files created - compaction may not have run")
	}

	// L0 files shouldn't grow excessively
	if finalLevels["level_0_files"] > levels["level_0_files"]+5 {
		t.Errorf("L0 files grew too much: before=%d, after=%d",
			levels["level_0_files"], finalLevels["level_0_files"])
	}
}

func getCompactionCount(stats map[string]interface{}) int {
	// Try to extract compaction count from stats
	// This is a placeholder - adjust based on actual stats structure
	if fileNum, ok := stats["file_number"].(int); ok {
		return fileNum / 10 // Rough estimate
	}
	return 0
}
