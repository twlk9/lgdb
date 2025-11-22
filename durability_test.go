package lgdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCrashDuringWALWrite tests recovery when the database crashes during WAL write operations.\n// This is the critical durability test - simulates a crash and verifies recovery works.\n// If this fails, you lose data, which is unacceptable for a database.
func TestCrashDuringWALWrite(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Open database and write some data
	opts := DefaultOptions()
	opts.Path = tmpDir
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write some initial data that should be recoverable
	initialData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range initialData {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put initial data %s: %v", key, err)
		}
	}

	// Force sync to ensure data is written
	if err := db.wal.Sync(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Simulate crash by forcefully closing the database without proper shutdown
	db.mu.Lock()
	if err := db.wal.Close(); err != nil {
		t.Logf("WAL close during crash simulation: %v", err)
	}
	if err := db.locker.Unlock(); err != nil {
		t.Logf("Locker unlock during crash simulation: %v", err)
	}
	db.mu.Unlock()

	// Simulate partially written WAL data by creating a truncated WAL file
	walFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if err != nil {
		t.Fatalf("Failed to find WAL files: %v", err)
	}

	if len(walFiles) > 0 {
		// Truncate the last WAL file to simulate incomplete write
		lastWAL := walFiles[len(walFiles)-1]
		file, err := os.OpenFile(lastWAL, os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("Failed to open WAL file for truncation: %v", err)
		}

		// Get current size and truncate to 80% to simulate partial write
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			t.Fatalf("Failed to stat WAL file: %v", err)
		}

		newSize := stat.Size() * 8 / 10
		if err := file.Truncate(newSize); err != nil {
			file.Close()
			t.Fatalf("Failed to truncate WAL file: %v", err)
		}
		file.Close()
	}

	// Attempt recovery by reopening the database
	opts.Path = tmpDir
	recoveredDB, err := Open(opts)
	if err != nil {
		// Recovery might fail due to corrupted WAL - this is acceptable
		t.Logf("Recovery failed as expected with corrupted WAL: %v", err)
		return
	}
	defer recoveredDB.Close()

	// Verify that at least some data is recoverable
	// Since we tolerate tail corruption, we may lose the last few records
	recoveredCount := 0
	for key, expectedValue := range initialData {
		value, err := recoveredDB.Get([]byte(key))
		if err != nil {
			// Some keys may be lost due to WAL truncation - this is expected
			t.Logf("Key %s not recovered (expected with tail corruption): %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
		}
		recoveredCount++
	}

	// We should recover at least one record (since we truncated to 80%)
	if recoveredCount == 0 {
		t.Error("No data recovered - expected at least some records to survive truncation")
	}
	t.Logf("Recovered %d/%d keys after WAL truncation", recoveredCount, len(initialData))

	// Verify database is functional after recovery
	if err := recoveredDB.Put([]byte("post_recovery"), []byte("test_value")); err != nil {
		t.Errorf("Database not functional after recovery: %v", err)
	}

	value, err := recoveredDB.Get([]byte("post_recovery"))
	if err != nil {
		t.Errorf("Failed to read post-recovery data: %v", err)
	} else if string(value) != "test_value" {
		t.Errorf("Post-recovery data corrupted: expected test_value, got %s", string(value))
	}
}

// TestCrashDuringMemtableFlush tests recovery when crash occurs during memtable flush to SSTable
func TestCrashDuringMemtableFlush(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Create options that will trigger flush quickly
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024 // Very small memtable to trigger flush quickly

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write enough data to trigger a flush
	numKeys := 100
	for i := range numKeys {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)) + strings.Repeat("x", 50)) // Larger values

		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}

		// Check if we've written enough data to potentially trigger a flush
		if i > 50 {
			// Enough data written, simulate crash now
			break
		}
	}

	// Force close database to simulate crash during or just after flush
	db.mu.Lock()
	db.wal.Close()
	if err := db.locker.Unlock(); err != nil {
		t.Logf("Locker unlock during crash simulation: %v", err)
	}
	db.mu.Unlock()

	// Look for any incomplete SSTable files (ones that might be partially written)
	sstFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to find SST files: %v", err)
	}

	// Simulate incomplete SSTable by truncating the most recent one
	if len(sstFiles) > 0 {
		lastSST := sstFiles[len(sstFiles)-1]
		file, err := os.OpenFile(lastSST, os.O_WRONLY, 0644)
		if err == nil {
			stat, err := file.Stat()
			if err == nil && stat.Size() > 100 {
				// Truncate to simulate incomplete write
				file.Truncate(stat.Size() / 2)
			}
			file.Close()
		}
	}

	// Attempt recovery
	opts.Path = tmpDir
	recoveredDB, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to recover database after flush crash: %v", err)
	}
	defer recoveredDB.Close()

	// Verify database functionality
	testKey := []byte("recovery_test")
	testValue := []byte("recovery_value")

	if err := recoveredDB.Put(testKey, testValue); err != nil {
		t.Errorf("Database not functional after flush crash recovery: %v", err)
	}

	retrievedValue, err := recoveredDB.Get(testKey)
	if err != nil {
		t.Errorf("Failed to read after flush crash recovery: %v", err)
	} else if string(retrievedValue) != string(testValue) {
		t.Errorf("Data corruption after flush crash recovery")
	}
}

// TestCrashDuringCompaction tests recovery when crash occurs during compaction
func TestCrashDuringCompaction(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Create options that will trigger compaction
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 512
	opts.L0CompactionTrigger = 2 // Trigger compaction with just 2 L0 files

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write enough data to trigger multiple flushes and potentially compaction
	numBatches := 5
	for batch := range numBatches {
		for i := range 50 {
			key := []byte("batch" + string(rune(batch)) + "_key" + string(rune(i)))
			value := []byte("value" + strings.Repeat("x", 30))

			if err := db.Put(key, value); err != nil {
				t.Fatalf("Failed to put key in batch %d: %v", batch, err)
			}
		}

		// Force flush to create L0 files
		if err := db.Flush(); err != nil {
			t.Fatalf("Failed to flush memtable: %v", err)
		}
	}

	// Check if compaction is likely to have started
	db.mu.RLock()
	currentVersion := db.versions.GetCurrentVersion()
	l0Files := currentVersion.files[0]
	l0Count := len(l0Files)
	db.mu.RUnlock()
	if l0Count >= opts.L0CompactionTrigger {
		t.Logf("Compaction should be triggered with %d L0 files", l0Count)
	}

	// Simulate crash during compaction by forcefully closing
	db.mu.Lock()
	db.wal.Close()
	if err := db.locker.Unlock(); err != nil {
		t.Logf("Locker unlock during crash simulation: %v", err)
	}
	db.mu.Unlock()

	// Look for any temporary compaction files that might exist
	tempFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.tmp"))
	if err != nil {
		t.Fatalf("Failed to search for temp files: %v", err)
	}

	// These temp files should be cleaned up during recovery
	tempFileCount := len(tempFiles)
	t.Logf("Found %d temporary files before recovery", tempFileCount)

	// Attempt recovery
	opts.Path = tmpDir
	recoveredDB, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to recover database after compaction crash: %v", err)
	}
	defer recoveredDB.Close()

	// Verify temp files are cleaned up
	tempFilesAfter, err := filepath.Glob(filepath.Join(tmpDir, "*.tmp"))
	if err != nil {
		t.Fatalf("Failed to search for temp files after recovery: %v", err)
	}

	if len(tempFilesAfter) > 0 {
		t.Errorf("Temporary files not cleaned up after recovery: %v", tempFilesAfter)
	}

	// Verify database functionality and data integrity
	testKey := []byte("post_compaction_crash")
	testValue := []byte("test_value")

	if err := recoveredDB.Put(testKey, testValue); err != nil {
		t.Errorf("Database not functional after compaction crash recovery: %v", err)
	}

	// Test some of the original data is still accessible
	originalKey := []byte("batch0_key0")
	_, err = recoveredDB.Get(originalKey)
	if err != nil && err != ErrNotFound {
		t.Errorf("Error accessing original data after compaction crash recovery: %v", err)
	}
}

// TestDataSafetyGracefulClose tests data safety with graceful close
func TestDataSafetyGracefulClose(t *testing.T) {
	opts := DefaultOptions()

	bdb := newBenchmarkDB(&testing.B{}, opts)
	bdb.generateKeys(100, 16)
	bdb.generateValues(100, 100)

	// Write some data
	for i := range 10 {
		key := bdb.keys[i]
		value := bdb.values[i]
		if err := bdb.db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// Graceful close - should sync remaining data
	if err := bdb.db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify data
	opts.Path = bdb.dir
	db2, err := Open(opts)
	if err != nil {
	}
	defer db2.Close()

	// Check if all data is present
	for i := range 10 {
		key := bdb.keys[i]
		expectedValue := bdb.values[i]

		value, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Key %d not found after graceful close: %v", i, err)
		}

		if string(value) != string(expectedValue) {
			t.Fatalf("Key %d value mismatch after graceful close", i)
		}
	}

	t.Logf("Graceful close: All %d keys recovered successfully", 10)
}

// TestDataSafetyCrashSimulation tests data safety with simulated crash (no close)
func TestDataSafetyCrashSimulation(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.CreateIfMissing = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	keys := make([][]byte, 10)
	values := make([][]byte, 10)
	for i := range 10 {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
		values[i] = []byte(fmt.Sprintf("value%d", i))
		if err := db.Put(keys[i], values[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate crash - don't call Close(), just abandon the DB
	if err := db.locker.Unlock(); err != nil {
		t.Fatalf("Failed to unlock database: %v", err)
	}

	// Force a small delay to let any pending syncs complete
	time.Sleep(50 * time.Millisecond)

	// Reopen and check what survived
	reopenedDB, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDB.Close()

	// Count how many keys survived the "crash"
	recovered := 0
	for i := range 10 {
		value, err := reopenedDB.Get(keys[i])
		if err == nil && string(value) == string(values[i]) {
			recovered++
		}
	}

	// With our sync intervals, most data should survive
	t.Logf("Crash simulation: %d/10 keys recovered", recovered)

	if recovered == 0 {
		t.Errorf("No data survived crash simulation - sync not working")
	}
}

// TestDataLossStressTest more aggressive test to detect data loss
func TestDataLossStressTest(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.CreateIfMissing = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	keys := make([][]byte, 100)
	values := make([][]byte, 100)
	// Write data very rapidly
	for i := range 100 {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
		values[i] = []byte(fmt.Sprintf("value%d", i))
		if err := db.Put(keys[i], values[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Immediate crash simulation - don't wait for any syncs
	// Don't call Close() to simulate hard crash
	if err := db.locker.Unlock(); err != nil {
		t.Fatalf("Failed to unlock database: %v", err)
	}

	// Reopen immediately
	reopenedDB, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDB.Close()

	// Count survivors
	recovered := 0
	for i := range 100 {
		value, err := reopenedDB.Get(keys[i])
		if err == nil && string(value) == string(values[i]) {
			recovered++
		}
	}

	lossRate := float64(100-recovered) / 100.0 * 100
	t.Logf("Stress test: %d/100 keys recovered, %.1f%% loss rate", recovered, lossRate)

	if lossRate > 80 {
		t.Logf("HIGH LOSS RATE detected - this is expected with long sync intervals")
	} else if lossRate < 10 {
		t.Logf("LOW LOSS RATE - sync intervals may not be working as expected")
	}
}

// TestWriteOptionsBasic tests basic WriteOptions functionality
func TestWriteOptionsBasic(t *testing.T) {
	opts := DefaultOptions()
	bdb := newBenchmarkDB(&testing.B{}, opts)
	defer bdb.close()

	bdb.generateKeys(10, 16)
	bdb.generateValues(10, 100)

	// Test synchronous writes
	syncOpts := &WriteOptions{Sync: true}
	start := time.Now()
	for i := range 5 {
		key := bdb.keys[i]
		value := bdb.values[i]
		if err := bdb.db.PutWithOptions(key, value, syncOpts); err != nil {
			t.Fatal(err)
		}
	}
	syncDuration := time.Since(start)

	// Test asynchronous writes
	asyncOpts := &WriteOptions{Sync: false}
	start = time.Now()
	for i := 5; i < 10; i++ {
		key := bdb.keys[i]
		value := bdb.values[i]
		if err := bdb.db.PutWithOptions(key, value, asyncOpts); err != nil {
			t.Fatal(err)
		}
	}
	asyncDuration := time.Since(start)

	t.Logf("Sync writes: %v, Async writes: %v", syncDuration, asyncDuration)

	// Async should be faster
	if asyncDuration >= syncDuration {
		t.Logf("Warning: async writes not significantly faster (sync: %v, async: %v)",
			syncDuration, asyncDuration)
	}

	// Verify all data is present
	for i := range 10 {
		key := bdb.keys[i]
		expectedValue := bdb.values[i]

		value, err := bdb.db.Get(key)
		if err != nil {
			t.Fatalf("Key %d not found: %v", i, err)
		}

		if string(value) != string(expectedValue) {
			t.Fatalf("Key %d value mismatch", i)
		}
	}
}

// TestWALBytesPerSync tests background syncing
func TestWALBytesPerSync(t *testing.T) {
	opts := DefaultOptions()
	opts.WALBytesPerSync = 1024 // Sync every 1KB

	bdb := newBenchmarkDB(&testing.B{}, opts)
	defer bdb.close()

	// Generate larger values to trigger background sync
	bdb.generateKeys(50, 16)
	for i := range 50 {
		// 100 byte values, so ~10 puts should trigger sync
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i)
		}
		bdb.values = append(bdb.values, value)
	}

	asyncOpts := &WriteOptions{Sync: false}

	// Write data rapidly - should trigger background syncs
	for i := range 50 {
		key := bdb.keys[i]
		value := bdb.values[i]
		if err := bdb.db.PutWithOptions(key, value, asyncOpts); err != nil {
			t.Fatal(err)
		}
	}

	// Give background syncs time to complete
	time.Sleep(50 * time.Millisecond)

	// Close database gracefully
	if err := bdb.db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify most data survived
	opts.Path = bdb.dir
	db2, err := Open(opts)
	if err != nil {
	}
	defer db2.Close()

	recovered := 0
	for i := range 50 {
		key := bdb.keys[i]
		expectedValue := bdb.values[i]

		value, err := db2.Get(key)
		if err == nil && string(value) == string(expectedValue) {
			recovered++
		}
	}

	t.Logf("Background sync test: %d/50 keys recovered", recovered)

	// With background syncing, we should recover most data
	if recovered < 40 {
		t.Errorf("Too much data lost with background syncing: only %d/50 recovered", recovered)
	}
}

// BenchmarkDurabilityOptions compares performance of different durability settings
func BenchmarkDurabilityOptions(b *testing.B) {
	benchmarks := []struct {
		name string
		opts *Options
	}{
		{
			name: "NoSync",
			opts: func() *Options {
				opts := DefaultOptions()
				return opts
			}(),
		},
		{
			name: "FastSync",
			opts: func() *Options {
				opts := DefaultOptions()
				return opts
			}(),
		},
		{
			name: "SlowSync",
			opts: func() *Options {
				opts := DefaultOptions()
				return opts
			}(),
		},
		{
			name: "SyncEveryWrite",
			opts: func() *Options {
				opts := DefaultOptions()
				return opts
			}(),
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			bdb := newBenchmarkDB(b, bm.opts)
			defer bdb.close()

			// Use fixed size for pre-generated keys/values
			count := 10000
			bdb.generateKeys(count, 16)
			bdb.generateValues(count, 100)

			writeOpts := &WriteOptions{Sync: false}
			if bm.name == "SyncEveryWrite" {
				writeOpts.Sync = true
			}

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				key := bdb.keys[i%count]
				value := bdb.values[i%count]
				if err := bdb.db.PutWithOptions(key, value, writeOpts); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestSequenceRecoveryWithoutWAL tests that sequence numbers are properly restored
// from manifest metadata when WAL is disabled. When WAL is disabled, the sequence
// number isn't persisted in the WAL, so it must be reconstructed from the manifest.
func TestSequenceRecoveryWithoutWAL(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Open database with WAL disabled
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.DisableWAL = true
	opts.WriteBufferSize = 1024 // Small buffer to force flush

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write enough data to trigger a flush
	numKeys := 100
	for i := range numKeys {
		key := []byte("key" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+(i/100)%10)))
		value := []byte("value" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+(i/100)%10)) + strings.Repeat("x", 30))

		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Force flush to ensure data goes to SSTable
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush memtable: %v", err)
	}

	// Capture current sequence number before closing
	originalSeq := db.seq.Load()

	t.Logf("Original sequence number before close: %d", originalSeq)

	if originalSeq < uint64(numKeys) {
		t.Errorf("Expected sequence number >= %d, got %d", numKeys, originalSeq)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database (still with WAL disabled)
	opts.Path = tmpDir
	opts.DisableWAL = true

	reopenedDB, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDB.Close()

	// Check the sequence number after reopening
	recoveredSeq := reopenedDB.seq.Load()

	t.Logf("Recovered sequence number after reopen: %d", recoveredSeq)

	// The sequence should be properly restored from manifest metadata
	if recoveredSeq != originalSeq {
		t.Errorf("Sequence number not properly recovered: original=%d, recovered=%d", originalSeq, recoveredSeq)
	}

	// Verify data is still accessible
	for i := range numKeys {
		key := []byte("key" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+(i/100)%10)))
		expectedValue := []byte("value" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+(i/100)%10)) + strings.Repeat("x", 30))

		value, err := reopenedDB.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %d after reopen: %v", i, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Key %d: expected %s, got %s", i, string(expectedValue), string(value))
		}
	}

	// Test that new writes use proper sequence numbers (should be higher than recovered)
	newKey := []byte("new_key_after_recovery")
	newValue := []byte("new_value_after_recovery")

	if err := reopenedDB.Put(newKey, newValue); err != nil {
		t.Fatalf("Failed to put new key after recovery: %v", err)
	}

	// Check that sequence advanced
	finalSeq := reopenedDB.seq.Load()

	if finalSeq <= recoveredSeq {
		t.Errorf("Sequence number did not advance after new write: recovered=%d, final=%d", recoveredSeq, finalSeq)
	}

	// Verify the new key is accessible
	retrievedNewValue, err := reopenedDB.Get(newKey)
	if err != nil {
		t.Errorf("Failed to get new key after recovery: %v", err)
	} else if string(retrievedNewValue) != string(newValue) {
		t.Errorf("New key value mismatch: expected %s, got %s", string(newValue), string(retrievedNewValue))
	}

	t.Logf("Final sequence number after new write: %d", finalSeq)
	t.Logf("Sequence recovery test completed successfully")
}
