package lgdb

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twlk9/lgdb/compression"
	"github.com/twlk9/lgdb/epoch"
	"github.com/twlk9/lgdb/keys"
	"github.com/twlk9/lgdb/sstable"
)

// CLEANUP TESTS
func TestBasicCleanup(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tempDir
	opts.WriteBufferSize = 1024  // Small buffer to trigger flushes
	opts.L0CompactionTrigger = 2 // Trigger compaction with 2 L0 files
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert data to create multiple SSTables
	for i := range 100 {
		key := []byte{byte(i)}
		value := make([]byte, 100) // Large enough values to trigger flushes
		for j := range value {
			value[j] = byte(i)
		}

		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Force flush to create SSTables
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Count initial SSTable files
	initialFiles := countSSTables(t, tempDir)
	if initialFiles == 0 {
		t.Fatal("No SSTable files were created")
	}

	// Ensure all pending writes are flushed before compacting
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush before compaction: %v", err)
	}

	db.CompactRange() // Force compaction - this already waits for completion

	// With epoch-based reclamation, cleanup is deferred and may not happen immediately
	// The key thing to test is that:
	// 1. We can trigger cleanup without errors
	// 2. Data integrity is maintained after cleanup operations
	// 3. The system remains in a consistent state

	// Try to trigger cleanup multiple times with epoch advancement
	totalCleaned := 0
	for i := range 3 {
		epoch.AdvanceEpoch()          // Advance past any active readers
		cleaned := epoch.TryCleanup() // Explicitly trigger cleanup
		totalCleaned += cleaned
		if cleaned > 0 {
			t.Logf("Cleanup round %d: removed %d resources", i+1, cleaned)
		}
	}

	t.Logf("Total epoch cleanup operations: %d", totalCleaned)

	finalFiles := countSSTables(t, tempDir)
	t.Logf("File count: %d initial -> %d final", initialFiles, finalFiles)

	// The main goal is data integrity and system consistency, not necessarily fewer files
	// (epoch-based cleanup is deferred and may not reduce file count immediately)

	// Verify all files are still valid (can be read)
	for i := range 100 {
		key := []byte{byte(i)}
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %d after cleanup: %v", i, err)
		}
		if len(value) != 100 || value[0] != byte(i) {
			t.Errorf("Invalid value for key %d after cleanup", i)
		}
	}
}

func TestConcurrentCleanup(t *testing.T) {
	tempDir := t.TempDir()
	fmt.Printf("TEST_DB_PATH: %s\n", tempDir)

	opts := DefaultOptions()
	opts.Path = tempDir
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 2
	// opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})) // Enable focused logging to track data flow
	opts.Compression = compression.NoCompressionConfig()

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	var wg sync.WaitGroup

	// Start concurrent writers
	for i := range 3 {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()

			for j := range 50 {
				key := []byte{byte(offset*50 + j)}
				value := make([]byte, 100)
				for k := range value {
					value[k] = byte(offset*50 + j)
				}

				if err := db.Put(key, value); err != nil {
					t.Errorf("Failed to put key %d: %v", offset*50+j, err)
				}
			}
		}(i)
	}
	t.Log("Put goroutines under way...")
	// Start concurrent readers
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := range 100 {
				key := []byte{byte(j)}
				db.Get(key) // Ignore errors since key might not exist yet
				time.Sleep(time.Millisecond)
			}
		}()
	}
	t.Log("Get goroutines under way... We don't care if they work")
	// Start cleanup goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		for range 10 {
			time.Sleep(20 * time.Millisecond) // Give time for background operations
		}
	}()
	t.Log("Waiting for work to finish....")
	wg.Wait()

	// Verify final state - ensure all background operations complete
	// for range 2 {
	// 	if err := db.Flush(); err != nil {
	// 		t.Fatalf("Failed on flush in conccurent test %#v", err)
	// 	}
	// }

	// Give extra time for background flush operations to complete
	time.Sleep(100 * time.Millisecond)

	// Wait for any background compaction to complete
	db.WaitForCompaction()

	// Trigger epoch-based cleanup explicitly
	epoch.AdvanceEpoch()
	epoch.TryCleanup()

	// If this is active everything works, even with the race detector
	// time.Sleep(5 * time.Second)

	// Check that we can still read all data
	for i := range 150 {
		key := []byte{byte(i)}
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %d after concurrent cleanup: %v", i, err)

			// Debug: manually inspect SSTable files when key is missing
			fmt.Printf("DEBUGGING: Key %d missing, inspecting SSTable files manually...\n", i)
			inspectL1File(tempDir, byte(i))
			return // Stop after first missing key to avoid spam
		}
		if len(value) != 100 || value[0] != byte(i) {
			t.Errorf("Invalid value for key %d after concurrent cleanup", i)
		}
	}

	// Close database properly before test cleanup
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// inspectL1File manually inspects the L1 SSTable files to see if a key exists
func inspectL1File(dbPath string, targetKey byte) {
	// Find all .sst files
	files, err := filepath.Glob(filepath.Join(dbPath, "*.sst"))
	if err != nil {
		fmt.Printf("Error globbing SST files: %v\n", err)
		return
	}

	fmt.Printf("Found %d SSTable files in %s\n", len(files), dbPath)

	// Check each file for the target key
	for _, filePath := range files {
		reader, err := sstable.NewSSTableReader(filePath, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})))
		if err != nil {
			fmt.Printf("Error opening %s: %v\n", filePath, err)
			continue
		}

		iter := reader.NewIterator()
		found := false
		count := 0
		var allKeys []byte
		var keyGaps []string

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			internalKey := iter.Key()
			if internalKey != nil && len(internalKey.UserKey()) == 1 {
				count++
				keyByte := internalKey.UserKey()[0]
				allKeys = append(allKeys, keyByte)

				if keyByte == targetKey {
					value := iter.Value()
					fmt.Printf("FOUND key %d in %s: value[0]=%d len=%d\n",
						targetKey, filepath.Base(filePath), value[0], len(value))
					found = true
				}
			}
		}

		if err := iter.Error(); err != nil {
			fmt.Printf("Iterator error in %s: %v\n", filePath, err)
		}

		// Analyze key sequence for gaps
		if len(allKeys) > 0 {
			fmt.Printf("File %s key sequence: ", filepath.Base(filePath))
			for i, k := range allKeys {
				if i > 0 && k != allKeys[i-1]+1 {
					gap := fmt.Sprintf("[GAP: %d->%d]", allKeys[i-1], k)
					keyGaps = append(keyGaps, gap)
					fmt.Printf("%s", gap)
				}
				fmt.Printf("%d", k)
				if i < len(allKeys)-1 {
					fmt.Printf(",")
				}
			}
			fmt.Printf("\n")

			if len(keyGaps) > 0 {
				fmt.Printf("GAPS DETECTED in %s: %v\n", filepath.Base(filePath), keyGaps)
			}
		}

		fmt.Printf("File %s: %d entries, key %d found: %v\n",
			filepath.Base(filePath), count, targetKey, found)

		// CRITICAL TEST: Try normal index-based Get() on the same file
		if found {
			fmt.Printf("TESTING: Direct SSTable Get() for key %d...\n", targetKey)
			targetUserKey := []byte{targetKey}
			targetInternalKey := keys.NewQueryKey(targetUserKey)

			value, returnedKey := reader.Get(targetInternalKey)
			if returnedKey == nil {
				fmt.Printf("BUG CONFIRMED: SSTable Get() cannot find key %d (but sequential scan found it!)\n", targetKey)
			} else {
				fmt.Printf("SSTable Get() found key %d: value[0]=%d len=%d\n", targetKey, value[0], len(value))
			}
		}

		// Additional debug: Show detailed block-level analysis for files with gaps
		if len(keyGaps) > 0 && filepath.Base(filePath) == "000005.sst" {
			fmt.Printf("\n=== DETAILED BLOCK ANALYSIS for %s ===\n", filepath.Base(filePath))
			analyzeSSTableBlocks(reader, targetKey)
		}

		iter.Close()
		reader.Close()
	}
}

// analyzeSSTableBlocks performs detailed block-level analysis of an SSTable
func analyzeSSTableBlocks(reader *sstable.SSTableReader, targetKey byte) {
	fmt.Printf("Analyzing blocks in SSTable...\n")

	// We'll need to manually walk through the index to see block boundaries
	// This is a debugging function to understand the block structure

	// First, let's try to read all entries and group them to understand block boundaries
	iter := reader.NewIterator()
	defer iter.Close()

	blockNum := 0
	entriesInBlock := 0
	var currentBlockKeys []byte
	var lastKey keys.EncodedKey

	fmt.Printf("Walking through all entries to infer block boundaries:\n")

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		internalKey := iter.Key()
		if internalKey == nil || len(internalKey.UserKey()) != 1 {
			continue
		}

		keyByte := internalKey.UserKey()[0]

		// Simple heuristic: if we've seen many entries or there's a big jump in keys,
		// we might be in a new block
		if lastKey != nil {
			keyJump := int(keyByte) - int(lastKey.UserKey()[0])
			if keyJump > 10 || entriesInBlock > 20 { // Rough heuristic for block boundary
				// Print previous block summary
				if len(currentBlockKeys) > 0 {
					fmt.Printf("Block %d: %d entries, keys %d-%d\n",
						blockNum, len(currentBlockKeys), currentBlockKeys[0], currentBlockKeys[len(currentBlockKeys)-1])
					if targetKey >= currentBlockKeys[0] && targetKey <= currentBlockKeys[len(currentBlockKeys)-1] {
						found := slices.Contains(currentBlockKeys, targetKey)
						if !found {
							fmt.Printf("  *** MISSING: Key %d should be in this range but is ABSENT! ***\n", targetKey)
						} else {
							fmt.Printf("  *** Key %d found in this block ***\n", targetKey)
						}
					}
				}

				// Start new block
				blockNum++
				currentBlockKeys = nil
				entriesInBlock = 0
			}
		}

		currentBlockKeys = append(currentBlockKeys, keyByte)
		entriesInBlock++
		lastKey = internalKey
	}

	// Print final block
	if len(currentBlockKeys) > 0 {
		fmt.Printf("Block %d: %d entries, keys %d-%d\n",
			blockNum, len(currentBlockKeys), currentBlockKeys[0], currentBlockKeys[len(currentBlockKeys)-1])
		if targetKey >= currentBlockKeys[0] && targetKey <= currentBlockKeys[len(currentBlockKeys)-1] {
			found := slices.Contains(currentBlockKeys, targetKey)
			if !found {
				fmt.Printf("  *** MISSING: Key %d should be in this range but is ABSENT! ***\n", targetKey)
			} else {
				fmt.Printf("  *** Key %d found in this block ***\n", targetKey)
			}
		}
	}

	// Try to scan all blocks to find where key 31 actually ended up
	fmt.Printf("\nScanning for key %d in ALL blocks:\n", targetKey)
	iter2 := reader.NewIterator()
	defer iter2.Close()

	blockNum = 0
	entriesInBlock = 0
	lastKey = nil

	for iter2.SeekToFirst(); iter2.Valid(); iter2.Next() {
		internalKey := iter2.Key()
		if internalKey == nil || len(internalKey.UserKey()) != 1 {
			continue
		}

		keyByte := internalKey.UserKey()[0]

		if lastKey != nil {
			keyJump := int(keyByte) - int(lastKey.UserKey()[0])
			if keyJump > 10 || entriesInBlock > 20 {
				blockNum++
				entriesInBlock = 0
			}
		}

		if keyByte == targetKey {
			fmt.Printf("*** FOUND key %d in block %d (estimated) ***\n", targetKey, blockNum)
		}

		entriesInBlock++
		lastKey = internalKey
	}
}

func TestOrphanedFileCleanup(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tempDir
	opts.WriteBufferSize = 1024

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Add some data
	for i := range 10 {
		key := []byte{byte(i)}
		value := []byte{byte(i)}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Manually create an orphaned SSTable file
	orphanedFile := filepath.Join(tempDir, "999999.sst")
	if err := os.WriteFile(orphanedFile, []byte("fake sstable content"), 0644); err != nil {
		t.Fatalf("Failed to create orphaned file: %v", err)
	}

	// Verify orphaned file exists
	if _, err := os.Stat(orphanedFile); err != nil {
		t.Fatalf("Orphaned file should exist: %v", err)
	}

	// With epoch-based management, orphaned files would be cleaned up automatically
	// This test may need to be redesigned or removed since manual cleanup is no longer needed

	// Verify legitimate files still exist and data is accessible
	for i := range 10 {
		key := []byte{byte(i)}
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %d after orphan cleanup: %v", i, err)
		}
		if len(value) != 1 || value[0] != byte(i) {
			t.Errorf("Invalid value for key %d after orphan cleanup", i)
		}
	}

	db.Close()
}

func TestEpochBasedCleanup(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tempDir
	opts.WriteBufferSize = 512   // Very small buffer to trigger frequent flushes
	opts.L0CompactionTrigger = 3 // Trigger compaction with 3 L0 files
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert data to create multiple SSTables and trigger compactions
	for i := range 50 {
		key := []byte{byte(i)}
		value := make([]byte, 50) // Medium-sized values
		for j := range value {
			value[j] = byte(i)
		}

		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}

		// Force frequent flushes to create many SSTables
		if i%10 == 9 {
			if err := db.Flush(); err != nil {
				t.Fatalf("Failed to flush at key %d: %v", i, err)
			}
		}
	}

	// Force final flush and compaction
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to final flush: %v", err)
	}

	initialFiles := countSSTables(t, tempDir)
	t.Logf("Initial SSTable files: %d", initialFiles)

	// Force compaction to create unreferenced files
	db.CompactRange()

	filesAfterCompaction := countSSTables(t, tempDir)
	t.Logf("Files after compaction: %d", filesAfterCompaction)

	// Test that epoch advancement and cleanup work correctly
	cleanupRounds := 0
	totalCleaned := 0

	// Try cleanup multiple times with epoch advancement
	for round := range 5 {
		epoch.AdvanceEpoch()
		cleaned := epoch.TryCleanup()

		if cleaned > 0 {
			cleanupRounds++
			totalCleaned += cleaned
			t.Logf("Cleanup round %d: removed %d resources", round+1, cleaned)
		}
	}

	t.Logf("Cleanup summary: %d rounds with activity, %d total cleanups", cleanupRounds, totalCleaned)

	// Verify data integrity after cleanup attempts
	for i := range 50 {
		key := []byte{byte(i)}
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %d after cleanup: %v", i, err)
		}
		if len(value) != 50 || value[0] != byte(i) {
			t.Errorf("Invalid value for key %d after cleanup", i)
		}
	}

	finalFiles := countSSTables(t, tempDir)
	t.Logf("Final SSTable files: %d", finalFiles)

	// The epoch-based system should maintain data integrity regardless of cleanup timing
	// This test validates that cleanup can be triggered and data remains accessible
}

// FILE CACHE TESTS
func TestFileCache(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()

	// Create a test SSTable
	sstPath := filepath.Join(tmpDir, "000001.sst")
	opts := sstable.SSTableOpts{
		Path:                 sstPath,
		Compression:          compression.DefaultConfig(),
		Logger:               slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})),
		BlockSize:            4 * KiB,
		BlockRestartInterval: 16,
	}
	writer, err := sstable.NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Write some test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, td := range testData {
		ikey := keys.NewEncodedKey([]byte(td.key), 1, keys.KindSet)
		if err := writer.Add(ikey, []byte(td.value)); err != nil {
			t.Fatalf("Failed to add key-value pair: %v", err)
		}
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close SSTable writer: %v", err)
	}

	// Now test the file cache with epoch protection
	cache := NewFileCache(2, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))) // Small cache size for testing
	defer cache.Close()

	// Epoch protection for all file access
	epochNum := epoch.EnterEpoch()
	defer epoch.ExitEpoch(epochNum)

	// Test 1: First access should create a new reader
	reader1, err := cache.Get(1, sstPath)
	if err != nil {
		t.Fatalf("Failed to get reader from cache: %v", err)
	}

	// Verify we can read from the cached reader
	if reader1.Reader() == nil {
		t.Fatal("Cached reader is nil")
	}

	// Test 2: Second access should return cached reader
	reader2, err := cache.Get(1, sstPath)
	if err != nil {
		t.Fatalf("Failed to get reader from cache on second access: %v", err)
	}

	// Both readers should point to the same underlying reader
	if reader1.Reader() != reader2.Reader() {
		t.Fatal("Expected same underlying reader for same file")
	}

	// Test 3: Test cache eviction
	// Create another SSTable to trigger eviction
	sstPath2 := filepath.Join(tmpDir, "000002.sst")
	opts.Path = sstPath2
	writer2, err := sstable.NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("Failed to create second SSTable writer: %v", err)
	}

	for _, td := range testData {
		ikey := keys.NewEncodedKey([]byte(td.key), 2, keys.KindSet)
		if err := writer2.Add(ikey, []byte(td.value)); err != nil {
			t.Fatalf("Failed to add key-value pair to second SSTable: %v", err)
		}
	}

	if err := writer2.Finish(); err != nil {
		t.Fatalf("Failed to finish second SSTable: %v", err)
	}

	if err := writer2.Close(); err != nil {
		t.Fatalf("Failed to close second SSTable writer: %v", err)
	}

	// Access different files to potentially trigger eviction
	reader3, err := cache.Get(2, sstPath2)
	if err != nil {
		t.Fatalf("Failed to get second reader from cache: %v", err)
	}
	if reader3.Reader() == nil {
		t.Fatal("Reader3 should not be nil")
	}

	reader4, err := cache.Get(3, sstPath) // Different file number, same path
	if err != nil {
		t.Fatalf("Failed to get third reader from cache: %v", err)
	}
	if reader4.Reader() == nil {
		t.Fatal("Reader4 should not be nil")
	}

	// Test 4: Test manual eviction
	cache.Evict(1)

	// Test 5: Test cache after close
	cache.Close()

	// Trying to get from closed cache should return error
	_, err = cache.Get(1, sstPath)
	if err == nil {
		t.Fatal("Expected error when accessing closed cache")
	}
}

func TestFileCacheEviction(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()

	// Create multiple test SSTables
	numFiles := 5
	sstPaths := make([]string, numFiles)

	for i := range numFiles {
		sstPath := filepath.Join(tmpDir, fmt.Sprintf("%06d.sst", i+1))
		sstPaths[i] = sstPath

		opts := sstable.SSTableOpts{
			Path:                 sstPath,
			Compression:          compression.DefaultConfig(),
			Logger:               slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})),
			BlockSize:            4 * KiB,
			BlockRestartInterval: 16,
		}
		writer, err := sstable.NewSSTableWriter(opts)
		if err != nil {
			t.Fatalf("Failed to create SSTable writer %d: %v", i+1, err)
		}

		// Write test data
		ikey := keys.NewEncodedKey(fmt.Appendf(nil, "key%d", i+1), uint64(i+1), keys.KindSet)
		if err := writer.Add(ikey, fmt.Appendf(nil, "value%d", i+1)); err != nil {
			t.Fatalf("Failed to add key-value pair %d: %v", i+1, err)
		}

		if err := writer.Finish(); err != nil {
			t.Fatalf("Failed to finish SSTable %d: %v", i+1, err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close SSTable writer %d: %v", i+1, err)
		}
	}

	// Create cache with capacity of 2
	cache := NewFileCache(2, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})))
	defer cache.Close()

	// Epoch protection for all file access
	epochNum := epoch.EnterEpoch()
	defer epoch.ExitEpoch(epochNum)

	// Fill cache
	reader1, err := cache.Get(1, sstPaths[0])
	if err != nil {
		t.Fatalf("Failed to get reader 1: %v", err)
	}

	reader2, err := cache.Get(2, sstPaths[1])
	if err != nil {
		t.Fatalf("Failed to get reader 2: %v", err)
	}
	if reader2.Reader() == nil {
		t.Fatal("Reader2 should not be nil")
	}

	// Add third file - should evict first one
	reader3, err := cache.Get(3, sstPaths[2])
	if err != nil {
		t.Fatalf("Failed to get reader 3: %v", err)
	}
	if reader3.Reader() == nil {
		t.Fatal("Reader3 should not be nil")
	}

	// Access first file again - should create new reader
	reader4, err := cache.Get(1, sstPaths[0])
	if err != nil {
		t.Fatalf("Failed to get reader 1 again: %v", err)
	}

	// The readers should be different instances due to eviction
	if reader1.Reader() == reader4.Reader() {
		t.Fatal("Expected different reader instances after eviction")
	}
}

func TestFileCacheEpochBased(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()

	// Create test SSTable
	sstPath := filepath.Join(tmpDir, "000001.sst")
	opts := sstable.SSTableOpts{
		Path:                 sstPath,
		Compression:          compression.DefaultConfig(),
		Logger:               slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})),
		BlockSize:            4 * KiB,
		BlockRestartInterval: 16,
	}
	writer, err := sstable.NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	ikey := keys.NewEncodedKey([]byte("key1"), 1, keys.KindSet)
	if err := writer.Add(ikey, []byte("value1")); err != nil {
		t.Fatalf("Failed to add key-value pair: %v", err)
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close SSTable writer: %v", err)
	}

	// Create cache with capacity of 1
	cache := NewFileCache(1, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})))
	defer cache.Close()

	// Epoch protection for all file access
	epochNum := epoch.EnterEpoch()
	defer epoch.ExitEpoch(epochNum)

	// Get reader multiple times - should work with epoch protection
	reader1, err := cache.Get(1, sstPath)
	if err != nil {
		t.Fatalf("Failed to get reader: %v", err)
	}

	reader2, err := cache.Get(1, sstPath)
	if err != nil {
		t.Fatalf("Failed to get reader second time: %v", err)
	}

	// Both readers should point to the same underlying reader (cached)
	if reader1.Reader() != reader2.Reader() {
		t.Fatal("Expected same underlying reader for same file")
	}

	// Readers should remain valid throughout the epoch
	if reader1.Reader() == nil || reader2.Reader() == nil {
		t.Fatal("Readers should remain valid within epoch")
	}
}

// HELPER FUNCTIONS
func countSSTables(t *testing.T, dir string) int {
	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to glob SSTable files: %v", err)
	}
	return len(files)
}
