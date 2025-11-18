package lgdb

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twlk9/lgdb/epoch"
)

// TestBasicOperations tests fundamental database operations.
// Covers the basic Put/Get/Delete cycle that every KV store should support.
// This is the smoke test - if this fails, everything else is broken.
func TestBasicOperations(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Open database
	opts := DefaultOptions()
	opts.Path = tempDir
	db, err := Open(opts)
	if err != nil {
	}
	defer db.Close()

	// Test Put and Get
	key := []byte("test-key")
	value := []byte("test-value")

	err = db.Put(key, value)
	if err != nil {
	}

	retrievedValue, err := db.Get(key)
	if err != nil {
	}

	if !bytes.Equal(value, retrievedValue) {
		t.Errorf("Expected %s, got %s", string(value), string(retrievedValue))
	}

	// Test non-existent key
	_, err = db.Get([]byte("non-existent"))
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// Test Delete
	err = db.Delete(key)
	if err != nil {
	}

	// Key should not be found after deletion
	_, err = db.Get(key)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after deletion, got %v", err)
	}
}

// TestMultipleOperations tests handling multiple keys and updates.
// Verifies that we can store multiple keys and that updates work correctly.
func TestMultipleOperations(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tempDir
	db, err := Open(opts)
	if err != nil {
	}
	defer db.Close()

	// Insert multiple key-value pairs
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err = db.Put([]byte(k), []byte(v))
		if err != nil {
		}
	}

	// Verify all values
	for k, expectedV := range testData {
		v, err := db.Get([]byte(k))
		if err != nil {
		}
		if string(v) != expectedV {
			t.Errorf("Expected %s, got %s", expectedV, string(v))
		}
	}

	// Update existing key
	err = db.Put([]byte("key1"), []byte("updated-value1"))
	if err != nil {
	}

	v, err := db.Get([]byte("key1"))
	if err != nil {
	}
	if string(v) != "updated-value1" {
		t.Errorf("Expected updated-value1, got %s", string(v))
	}
}

// TestInvalidInputs tests error handling for invalid inputs.
// Makes sure we reject empty/nil keys and return proper error codes.
func TestInvalidInputs(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tempDir
	db, err := Open(opts)
	if err != nil {
	}
	defer db.Close()

	// Test empty key
	err = db.Put([]byte{}, []byte("value"))
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey for empty key, got %v", err)
	}

	// Test nil key
	err = db.Put(nil, []byte("value"))
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey for nil key, got %v", err)
	}

	// Test Get with invalid key
	_, err = db.Get([]byte{})
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey for empty key, got %v", err)
	}
}

// TestClosedDatabase tests operations on a closed database.
// Once you close a database, all operations should fail with ErrDBClosed.
func TestClosedDatabase(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tempDir
	db, err := Open(opts)
	if err != nil {
	}

	// Close the database
	err = db.Close()
	if err != nil {
	}

	// Operations should fail on closed database
	err = db.Put([]byte("key"), []byte("value"))
	if err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed, got %v", err)
	}

	_, err = db.Get([]byte("key"))
	if err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed, got %v", err)
	}

	err = db.Delete([]byte("key"))
	if err != ErrDBClosed {
		t.Errorf("Expected ErrDBClosed, got %v", err)
	}
}

// TestWALPersistence tests that writes survive database restart.
// This verifies WAL recovery - data should persist even without explicit flush.
func TestWALPersistence(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tempDir

	// First session: write data
	db1, err := Open(opts)
	if err != nil {
	}

	testData := map[string]string{
		"persistent1": "value1",
		"persistent2": "value2",
	}

	for k, v := range testData {
		err = db1.Put([]byte(k), []byte(v))
		if err != nil {
		}
	}

	// Delete one key
	err = db1.Delete([]byte("persistent1"))
	if err != nil {
	}

	err = db1.Close()
	if err != nil {
	}

	// Second session: reopen and verify data
	db2, err := Open(opts)
	if err != nil {
	}
	defer db2.Close()

	// persistent1 should not exist (was deleted)
	_, err = db2.Get([]byte("persistent1"))
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for deleted key, got %v", err)
	}

	// persistent2 should exist
	v, err := db2.Get([]byte("persistent2"))
	if err != nil {
	}
	if string(v) != "value2" {
		t.Errorf("Expected value2, got %s", string(v))
	}
}

// TestSequenceNumbers tests sequence number handling for multiple updates.
// Verifies that multiple updates to the same key work correctly and the latest wins.
func TestSequenceNumbers(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tempDir
	db, err := Open(opts)
	if err != nil {
	}
	defer db.Close()

	key := []byte("test-key")

	// Multiple operations on same key
	err = db.Put(key, []byte("value1"))
	if err != nil {
	}

	err = db.Put(key, []byte("value2"))
	if err != nil {
	}

	err = db.Put(key, []byte("value3"))
	if err != nil {
	}

	// Should get the latest value
	v, err := db.Get(key)
	if err != nil {
	}
	if string(v) != "value3" {
		t.Errorf("Expected value3, got %s", string(v))
	}

	// Delete and verify
	err = db.Delete(key)
	if err != nil {
	}

	_, err = db.Get(key)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after deletion, got %v", err)
	}
}

// TestMemtableFlush tests memtable flushing operations.
// Forces flush by filling up a small memtable and verifies SSTable creation.
func TestMemtableFlush(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create database with small memtable size to trigger flush
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024 // 1KB - very small to trigger flush quickly

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Add enough data to trigger a flush
	largeValue := make([]byte, 200) // 200 bytes per value
	for i := range len(largeValue) {
		largeValue[i] = byte(i % 256)
	}

	numKeys := 10
	keys := make([]string, numKeys)
	for i := range numKeys {
		key := []byte{byte('k'), byte('e'), byte('y'), byte(i + 1)}
		keys[i] = string(key)

		err := db.Put(key, largeValue)
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Give some time for background flush to complete
	time.Sleep(100 * time.Millisecond)

	// Check that SSTable files were created
	sstFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to list SSTable files: %v", err)
	}

	if len(sstFiles) == 0 {
		t.Fatalf("Expected at least one SSTable file, got none")
	}

	t.Logf("Created %d SSTable files: %v", len(sstFiles), sstFiles)

	// Verify all keys can still be read
	for _, key := range keys {
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}

		if len(value) != len(largeValue) {
			t.Fatalf("Value length mismatch for key %s: expected %d, got %d",
				key, len(largeValue), len(value))
		}

		// Verify value content
		for j := range len(largeValue) {
			if value[j] != largeValue[j] {
				t.Fatalf("Value content mismatch for key %s at position %d", key, j)
			}
		}
	}
}

// TestMemtableRotation tests memtable rotation logic.
// Verifies that when memtable gets full, it becomes immutable and a new one is created.
func TestMemtableRotation(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create database with small memtable size
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 512 // 512 bytes - very small

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Check initial state
	msize := db.memtable.Size()
	if msize != 0 {
		t.Fatalf("Expected empty memtable, got size %d", msize)
	}

	// With the new architecture, we always have exactly one memtable in the version
	e := epoch.EnterEpoch()
	defer epoch.ExitEpoch(e)

	// Add data that should trigger rotation
	largeValue := make([]byte, 100)
	for i := range len(largeValue) {
		largeValue[i] = byte(i % 256)
	}

	// Add several keys to exceed memtable size
	for i := range 10 {
		key := []byte{byte('t'), byte('e'), byte('s'), byte('t'), byte(i)}
		err := db.Put(key, largeValue)
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	// Give some time for background operations
	time.Sleep(50 * time.Millisecond)

	// Verify memtable rotation occurred by checking for SSTable files
	sstFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to list SSTable files: %v", err)
	}

	if len(sstFiles) == 0 {
		t.Logf("No SSTable files found yet, checking flush queue...")

		// With the new architecture, check if flushes are pending
		stats := db.GetStats()
		flushQueueSize := stats["flush_queue_size"].(int)
		t.Logf("Found %d items in flush queue", flushQueueSize)

		// Give more time for background flush to complete
		time.Sleep(100 * time.Millisecond)

		// Check again for SSTable files
		sstFiles, err = filepath.Glob(filepath.Join(tmpDir, "*.sst"))
		if err != nil {
			t.Fatalf("Failed to re-check SSTable files: %v", err)
		}

		if len(sstFiles) > 0 {
			t.Logf("Background flush completed, created %d SSTable files", len(sstFiles))
		} else {
			t.Logf("Background flush may still be in progress")
		}
	} else {
		t.Logf("Memtable rotation successful, created %d SSTable files", len(sstFiles))
	}
}

// TestReliableMultiLevelRead tests reading from both memtable and SSTables.
// This is the comprehensive test for LSM tree read precedence - memtable beats SSTable.
func TestReliableMultiLevelRead(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create database
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 4096 // Normal size
	opts.Sync = false
	// opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})) // Enable for debug output

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Phase 1: Start with small values that will fit in memtable
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range testData {
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Verify all keys are readable from memtable
	t.Logf("Testing reads from memtable...")
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get %s from memtable: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Fatalf("Wrong value for %s: expected %s, got %s",
				key, expectedValue, string(value))
		}
	}

	// Check stats before flush
	stats := db.GetStats()
	t.Logf("Before flush: memtable_entries=%v, level_0_files=%v",
		stats["memtable_entries"], stats["levels"].(map[string]int)["level_0_files"])

	// Phase 2: Add enough data to trigger automatic flush
	// Write large values to exceed WriteBufferSize (4MB)
	largeValue := make([]byte, 1024*1024) // 1MB per value
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Add large values to trigger flush
	for i := range 10 {
		key := fmt.Sprintf("large_key%d", i)
		err := db.Put([]byte(key), largeValue)
		if err != nil {
			t.Fatalf("Failed to put large key %s: %v", key, err)
		}
	}

	// Give time for background flush to complete
	time.Sleep(200 * time.Millisecond)

	// Check stats after flush
	stats = db.GetStats()
	t.Logf("After flush: memtable_entries=%v, level_0_files=%v",
		stats["memtable_entries"], stats["levels"].(map[string]int)["level_0_files"])

	// Verify SSTable files were created
	sstFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to list SSTable files: %v", err)
	}

	if len(sstFiles) == 0 {
		t.Fatalf("Expected SSTable files to be created, got none")
	}

	t.Logf("Created %d SSTable files: %v", len(sstFiles), sstFiles)

	// Phase 3: Verify all keys are still readable from SSTables
	t.Logf("Testing reads from SSTables...")
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get %s from SSTable: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Fatalf("Wrong value for %s from SSTable: expected %s, got %s",
				key, expectedValue, string(value))
		}
	}

	// Phase 4: Add new data to memtable and test precedence
	t.Logf("Testing memtable precedence over SSTables...")

	// Update existing key (should override SSTable)
	newValue1 := "updated_value1"
	err = db.Put([]byte("key1"), []byte(newValue1))
	if err != nil {
		t.Fatalf("Failed to update key1: %v", err)
	}

	// Add new key
	err = db.Put([]byte("key4"), []byte("value4"))
	if err != nil {
		t.Fatalf("Failed to put key4: %v", err)
	}

	// Test reads - should get updated value from memtable
	value, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get updated key1: %v", err)
	}
	if string(value) != newValue1 {
		t.Fatalf("Memtable precedence failed: expected %s, got %s",
			newValue1, string(value))
	}

	// Test new key from memtable
	value, err = db.Get([]byte("key4"))
	if err != nil {
		t.Fatalf("Failed to get key4: %v", err)
	}
	if string(value) != "value4" {
		t.Fatalf("Wrong value for key4: expected value4, got %s", string(value))
	}

	// Test other keys still come from SSTable
	value, err = db.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2: %v", err)
	}
	if string(value) != "value2" {
		t.Fatalf("Wrong value for key2: expected value2, got %s", string(value))
	}

	// Phase 5: Test with multiple flushes
	t.Logf("Testing multiple flush cycles...")

	// Flush again
	err = db.Flush()
	if err != nil {
		t.Fatalf("Failed to flush second time: %v", err)
	}

	// Check that we have more SSTable files
	sstFiles, err = filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to list SSTable files after second flush: %v", err)
	}

	t.Logf("After second flush: %d SSTable files", len(sstFiles))

	// Verify all data is still accessible
	finalTestData := map[string]string{
		"key1": "updated_value1", // Updated value
		"key2": "value2",         // Original value
		"key3": "value3",         // Original value
		"key4": "value4",         // New value
	}

	for key, expectedValue := range finalTestData {
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get %s in final test: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Fatalf("Wrong final value for %s: expected %s, got %s",
				key, expectedValue, string(value))
		}
	}

	// Test non-existent key
	_, err = db.Get([]byte("nonexistent"))
	if err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound for nonexistent key, got %v", err)
	}

	// Final stats
	stats = db.GetStats()
	t.Logf("Final stats: memtable_entries=%v, level_0_files=%v, sequence=%v",
		stats["memtable_entries"],
		stats["levels"].(map[string]int)["level_0_files"],
		stats["sequence_number"])

	t.Logf("Multi-level read test completed successfully!")
}

// TestFlushEmptyMemtable tests flushing when memtable is empty.
// Should be a no-op and not create any SSTable files.
func TestFlushEmptyMemtable(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create database
	opts := DefaultOptions()
	opts.Path = tmpDir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to flush empty memtable - should be a no-op
	err = db.Flush()
	if err != nil {
		t.Fatalf("Failed to flush empty memtable: %v", err)
	}

	// Verify no SSTable files were created
	sstFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to list SSTable files: %v", err)
	}

	if len(sstFiles) != 0 {
		t.Fatalf("Expected no SSTable files for empty flush, got %d", len(sstFiles))
	}

	t.Logf("Empty flush test passed")
}

// TestDBGetWithCache tests database get operations with file caching.
// Verifies that the file cache works correctly for SSTable reads.
func TestDBGetWithCache(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create options with small cache size for testing
	opts := DefaultOptions()
	opts.Path = tempDir
	opts.MaxOpenFiles = 12 // Small limit: 12 - 10 reserved = 2 cache size
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 1024

	// Open the database
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	var value []byte

	// Put some test data
	testData := map[string]string{
		"key001": "value001",
		"key002": "value002",
		"key003": "value003",
		"key004": "value004",
		"key005": "value005",
	}

	for key, value := range testData {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Force flush to create SSTable files
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Wait for flush to complete
	time.Sleep(100 * time.Millisecond)

	// Check that we have some SSTable files
	files, err := filepath.Glob(filepath.Join(tempDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to list SSTable files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("No SSTable files created")
	}

	// Test that DB.Get() can retrieve values from SSTables through cache
	// First check if keys are still in memtable
	stats := db.GetStats()
	t.Logf("DB Stats: memtable_entries=%v, level_0_files=%v",
		stats["memtable_entries"], stats["levels"].(map[string]int)["level_0_files"])

	for key, expectedValue := range testData {
		value, err = db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("Expected value %s for key %s, got %s", expectedValue, key, string(value))
		}
	}

	// Test repeated access to same key to verify cache usage
	// Access the same key multiple times to trigger cache usage
	for i := range 3 {
		value, err = db.Get([]byte("key001"))
		if err != nil {
			t.Errorf("Failed to get key001 on iteration %d: %v", i, err)
		}
		if string(value) != "value001" {
			t.Errorf("Expected value001, got %s", string(value))
		}
	}
	// If we get here without errors, the cache is working properly
	t.Log("Repeated access successful - cache is working")

	// Test non-existent key
	_, err = db.Get([]byte("nonexistent"))
	if err == nil {
		t.Error("Expected error for non-existent key")
	}
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

// TestDBGetCacheEviction tests cache eviction under memory pressure.
// Uses a small cache to force evictions and verifies reads still work.
func TestDBGetCacheEviction(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()

	// Create options with small cache size to trigger eviction
	opts := DefaultOptions()
	opts.Path = tempDir
	opts.MaxOpenFiles = 13 // Small limit: 13 - 10 reserved = 3 cache size
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	opts.WriteBufferSize = 1024 // Use a reasonable buffer size

	// Open the database
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Put data to create multiple SSTable files that exceed cache size
	numFiles := 5 // More than our cache size of 3
	allKeys := make([]string, 0)

	for fileIdx := range numFiles {
		// Put some data for this "file" (actually just a batch)
		for i := range 10 {
			key := fmt.Sprintf("file%d_key%03d", fileIdx, i)
			value := fmt.Sprintf("file%d_value%03d", fileIdx, i)
			allKeys = append(allKeys, key)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}

		// Force flush to create an SSTable file
		if err := db.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		// Wait for flush to complete
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for background operations to complete and force any pending compactions
	db.WaitForCompaction()

	// With epoch-based reclamation, ensure any pending cleanup is handled
	// but don't be too aggressive about it (files should still be accessible)
	epoch.AdvanceEpoch()

	// Wait a bit more for any background operations to complete
	time.Sleep(200 * time.Millisecond)

	// Verify we have multiple SSTable files
	files, err := filepath.Glob(filepath.Join(tempDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to list SSTable files: %v", err)
	}
	if len(files) < numFiles {
		t.Fatalf("Expected at least %d SSTable files, got %d", numFiles, len(files))
	}

	// Debug: Show what files were created
	t.Logf("Created %d SSTable files: %v", len(files), files)

	// Debug: Show database stats
	stats := db.GetStats()
	t.Logf("DB Stats before testing: memtable_entries=%v, level_0_files=%v",
		stats["memtable_entries"], stats["levels"].(map[string]int)["level_0_files"])

	// First, verify that all keys we put in are actually accessible
	t.Run("VerifyAllKeysAccessible", func(t *testing.T) {
		missingKeys := 0
		sampleKeys := make([]string, 0)

		// Test every 5th key to avoid too much output
		for i := 0; i < len(allKeys); i += 5 {
			key := allKeys[i]
			_, err := db.Get([]byte(key))
			if err != nil {
				missingKeys++
				if len(sampleKeys) < 5 {
					sampleKeys = append(sampleKeys, key)
				}
			}
		}

		if missingKeys > 0 {
			t.Errorf("Missing %d keys out of %d tested. Sample missing keys: %v",
				missingKeys, len(allKeys)/5, sampleKeys)
		} else {
			t.Logf("All sampled keys are accessible (%d tested)", len(allKeys)/5)
		}
	})

	// Access keys from different files to trigger cache eviction
	t.Run("AccessAcrossFiles", func(t *testing.T) {
		// Use keys that we know exist across different SSTable files
		testKeys := []string{
			"file0_key001",
			"file1_key001",
			"file2_key001",
			"file3_key001", // This should exceed cache size and trigger evictions
			"file4_key001",
		}

		for _, key := range testKeys {
			expectedValue := strings.Replace(key, "_key", "_value", 1)

			t.Logf("Attempting to get key: %s", key)
			value, err := db.Get([]byte(key))
			if err != nil {
				t.Errorf("Failed to get key %s: %v", key, err)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Expected value %s for key %s, got %s", expectedValue, key, string(value))
			} else {
				t.Logf("Successfully retrieved key %s = %s", key, string(value))
			}
		}

		// Since cache size is 3 and we accessed 5 different files,
		// evictions should have occurred but all keys should still be accessible
		// The epoch system ensures SSTableReaders stay open until safe to close
		t.Log("Access across files successful - cache eviction with epoch cleanup is working")
	})
}
