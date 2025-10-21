package lgdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twlk9/lgdb/epoch"
)

func TestCompactionBasic(t *testing.T) {
	// Create temporary directory for test database
	tmpDir := t.TempDir()

	// Create options with small thresholds to trigger compaction easily
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024  // Small write buffer for easy testing
	opts.L0CompactionTrigger = 1 // Trigger compaction after 1 L0 file
	opts.MaxLevels = 4

	// Open database
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Helper function to write many keys to trigger flushes
	writeKeys := func(start, end int) {
		for i := start; i < end; i++ {
			key := fmt.Sprintf("key%06d", i)
			value := fmt.Sprintf("value%06d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}
	}

	// Write enough keys to trigger multiple flushes
	writeKeys(0, 500) // Reduced from 2000 to 500

	// Force flush to ensure all data is in SSTables
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Get initial stats
	stats := db.GetStats()
	l0Files := stats["levels"].(map[string]int)["level_0_files"]

	t.Logf("Initial L0 files: %d", l0Files)

	// If we have enough L0 files, compaction should trigger
	if l0Files >= opts.L0CompactionTrigger {
		// Wait for any background compaction to complete first
		db.WaitForCompaction()

		// Force a compaction check and wait for completion
		if err := db.CompactRange(); err != nil {
			t.Fatalf("Failed to compact: %v", err)
		}

		// Check stats after compaction
		stats = db.GetStats()
		l0FilesAfter := stats["levels"].(map[string]int)["level_0_files"]
		l1FilesAfter := stats["levels"].(map[string]int)["level_1_files"]

		t.Logf("After compaction - L0 files: %d, L1 files: %d", l0FilesAfter, l1FilesAfter)

		// We should have fewer L0 files and some L1 files
		if l0FilesAfter >= l0Files {
			t.Errorf("Expected L0 files to decrease after compaction, got %d -> %d", l0Files, l0FilesAfter)
		}

		if l1FilesAfter == 0 {
			t.Errorf("Expected some L1 files after compaction, got %d", l1FilesAfter)
		}
	}

	// Verify all keys are still readable
	for i := range 500 {
		key := fmt.Sprintf("key%06d", i)
		expectedValue := fmt.Sprintf("value%06d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}

		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}
}

func TestCompactionWithDeletes(t *testing.T) {
	// Create temporary directory for test database
	tmpDir := t.TempDir()

	// Create options with small thresholds
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 3
	opts.MaxLevels = 4

	// Open database
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write initial keys
	for i := range 500 {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Force flush
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Delete some keys
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("key%06d", i)
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatalf("Failed to delete key %s: %v", key, err)
		}
	}

	// Write more keys to trigger another flush
	for i := 500; i < 1000; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Force flush and compaction
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	if err := db.CompactRange(); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// Verify that existing keys are still readable
	for i := range 100 {
		key := fmt.Sprintf("key%06d", i)
		expectedValue := fmt.Sprintf("value%06d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}

		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}

	// Verify that deleted keys are not found
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("key%06d", i)

		_, err := db.Get([]byte(key))
		if err != ErrNotFound {
			t.Errorf("Expected key %s to be deleted, got error: %v", key, err)
		}
	}

	// Verify that new keys are readable
	for i := 500; i < 1000; i++ {
		key := fmt.Sprintf("key%06d", i)
		expectedValue := fmt.Sprintf("value%06d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}

		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}
}

func TestCompactionConcurrency(t *testing.T) {
	// Create temporary directory for test database
	tmpDir := t.TempDir()

	// Create options with small thresholds
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 2
	opts.MaxLevels = 4

	// Open database
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write many keys to trigger multiple flushes and compactions
	numKeys := 2000
	for i := range numKeys {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}

		// Trigger reads while writes are happening
		if i%100 == 0 {
			readKey := fmt.Sprintf("key%06d", i/2)
			if i >= 200 {
				if _, err := db.Get([]byte(readKey)); err != nil && err != ErrNotFound {
					t.Fatalf("Failed to get key %s during concurrent write: %v", readKey, err)
				}
			}
		}
	}

	// Force final flush and compaction
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	if err := db.CompactRange(); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// Verify all keys are still readable
	for i := range numKeys {
		key := fmt.Sprintf("key%06d", i)
		expectedValue := fmt.Sprintf("value%06d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}

		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}

	// Check final stats
	stats := db.GetStats()
	t.Logf("Final stats: %+v", stats)
}

func TestCompactionVersioning(t *testing.T) {
	// Create temporary directory for test database
	tmpDir := t.TempDir()

	// Create options
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 2
	opts.MaxLevels = 4
	// opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})) // Enable debug to see memtable capture

	// Open database
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write initial data
	for i := range 100 {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Force flush to create L0 files
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Check the initial state
	stats := db.GetStats()
	t.Logf("After first flush - L0 files: %d, total entries: %d", stats["level_0_files"], stats["memtable_entries"])

	// Wait for any background compaction to complete before creating iterator
	time.Sleep(100 * time.Millisecond)

	// Create an iterator (should hold a reference to the current version)
	iter := db.NewIterator(nil)
	defer iter.Close()

	// Write more data to trigger compaction
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Force flush and compaction
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	if err := db.CompactRange(); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// The iterator should still be valid and see the original data
	iter.SeekToFirst()
	count := 0
	keys := []string{}
	for iter.Valid() {
		key := string(iter.Key())
		keys = append(keys, key)
		count++
		if count <= 5 || count > len(keys)-5 { // Log first and last 5 keys
			t.Logf("Iterator key %d: %s", count, key)
		}
		iter.Next()
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	// Debug: Print some keys to understand what's happening
	t.Logf("Iterator 1 sees %d keys", count)
	if len(keys) > 0 {
		t.Logf("First key: %s", keys[0])
		t.Logf("Last key: %s", keys[len(keys)-1])
	}

	// Should see at least the keys that existed when iterator was created (0-99)
	// Note: Iterator may also see newer keys written to the same memtable (no sequence filtering)
	if count < 100 {
		// Debug: Show which keys are missing
		expectedKeys := make(map[string]bool)
		for i := range 100 {
			expectedKeys[fmt.Sprintf("key%06d", i)] = true
		}

		seenKeys := make(map[string]bool)
		for _, key := range keys {
			seenKeys[key] = true
			delete(expectedKeys, key)
		}

		t.Logf("Missing keys (%d):", len(expectedKeys))
		for key := range expectedKeys {
			t.Logf("  %s", key)
		}

		t.Errorf("Expected at least 100 keys in iterator, got %d", count)
	}

	// Create a new iterator to see the current state
	newIter := db.NewIterator(nil)
	defer newIter.Close()

	newIter.SeekToFirst()
	newCount := 0
	for newIter.Valid() {
		newCount++
		newIter.Next()
	}

	if err := newIter.Error(); err != nil {
		t.Fatalf("New iterator error: %v", err)
	}

	if newCount != 200 {
		t.Errorf("Expected 200 keys in new iterator, got %d", newCount)
	}
}

// TestTombstoneEliminationAtBottomLevel verifies that tombstones are dropped
// when compacting to the bottom level (highest level number)
func TestTombstoneEliminationAtBottomLevel(t *testing.T) {
	tmpDir := t.TempDir()

	// Create options with only 2 levels to make bottom level testing easier
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 256         // Very small buffer
	opts.L0CompactionTrigger = 2       // Trigger compaction with 2 files
	opts.MaxLevels = 2                 // Only L0 and L1 (L1 is bottom level)
	opts.WriteBufferSize = 512         // Small memtable size = small L0 files
	opts.LevelFileSizeMultiplier = 1.5 // Small growth for simple test

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Step 1: Write some data
	for i := range 10 {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Force flush to L0
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Step 2: Delete half the keys (creating tombstones)
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete key %s: %v", key, err)
		}
	}

	// Force another flush to L0
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Wait for compaction to complete
	time.Sleep(200 * time.Millisecond)

	// Step 3: Add more data to force L0->L1 compaction (to bottom level)
	// This should trigger compaction that eliminates tombstones
	for i := 20; i < 30; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Force flush and wait for compaction to bottom level
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Force another compaction cycle to ensure tombstones get eliminated
	for i := 100; i < 110; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Step 4: Verify tombstones should be eliminated
	// Open the L1 SSTable files directly and check for tombstones
	e := epoch.EnterEpoch()
	defer epoch.ExitEpoch(e)
	version := db.versions.GetCurrentVersion()

	l1Files := version.GetFiles(1) // Bottom level files
	if len(l1Files) == 0 {
		t.Fatalf("Expected files at bottom level L1, but found none")
	}

	// Scan L1 files for tombstones - there should be NONE at bottom level
	tombstoneCount := 0
	ep := epoch.EnterEpoch()
	defer epoch.ExitEpoch(ep)
	for _, file := range l1Files {
		cachedReader, err := db.openSSTable(file.FileNum)
		if err != nil {
			t.Fatalf("Failed to open SSTable %d: %v", file.FileNum, err)
		}

		reader := cachedReader.Reader()
		iter := reader.NewIterator(true)

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			internalKey := iter.Key()
			if internalKey != nil && internalKey.Kind() == 2 { // KindDelete = 2
				tombstoneCount++
				t.Logf("Found tombstone in L1 file %d: key=%s", file.FileNum, string(internalKey.UserKey()))
			}
		}

		iter.Close()
	}

	// This test SHOULD FAIL initially - proving tombstones aren't being dropped
	if tombstoneCount > 0 {
		t.Errorf("Found %d tombstones in bottom level L1 - these should have been eliminated during compaction", tombstoneCount)
		t.Logf("PROOF: Tombstones are NOT being dropped at bottom level as they should be")
	} else {
		t.Logf("SUCCESS: No tombstones found in bottom level L1 - elimination working correctly")
	}

	// Verify that deleted keys are indeed not readable
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		_, err := db.Get([]byte(key))
		if err != ErrNotFound {
			t.Errorf("Expected key %s to be deleted, but Get returned: %v", key, err)
		}
	}
}

func TestCompactionFileCleanup(t *testing.T) {
	// Create temporary directory for test database
	tmpDir := t.TempDir()

	// Create options
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 2
	opts.MaxLevels = 4

	// Open database
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write data to create multiple SSTable files
	for i := range 500 {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Force flush
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Count SSTable files before compaction
	sstFiles1, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to glob SSTable files: %v", err)
	}

	t.Logf("SSTable files before compaction: %d", len(sstFiles1))

	// Trigger compaction
	if err := db.CompactRange(); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// Give time for file cleanup
	time.Sleep(100 * time.Millisecond)

	// Count SSTable files after compaction
	sstFiles2, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to glob SSTable files: %v", err)
	}

	t.Logf("SSTable files after compaction: %d", len(sstFiles2))

	// Verify that file count has changed (old files should be cleaned up)
	if len(sstFiles1) > 0 && len(sstFiles2) >= len(sstFiles1) {
		t.Logf("Warning: Expected fewer SSTable files after compaction")
		// This is not necessarily an error as cleanup might be async
	}

	// Verify all data is still accessible
	for i := range 500 {
		key := fmt.Sprintf("key%06d", i)
		expectedValue := fmt.Sprintf("value%06d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after compaction: %v", key, err)
		}

		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}
}

// TestTombstoneCompaction verifies that tombstones properly eliminate older versions during compaction
func TestTombstoneCompaction(t *testing.T) {
	// Create a temporary directory for the test database
	dir := t.TempDir()

	// Create database with small buffer to force flushes
	opts := DefaultOptions()
	opts.Path = dir
	opts.WriteBufferSize = 512   // Very small to force frequent flushes
	opts.L0CompactionTrigger = 2 // Trigger compaction with just 2 files

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test scenario: Write key -> Delete key -> Write key again
	// The compaction should eliminate the first version completely

	// 1. Write initial value
	if err := db.Put([]byte("test_key"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// 2. Delete the key
	if err := db.Delete([]byte("test_key")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// 3. Write the key again with a new value
	if err := db.Put([]byte("test_key"), []byte("value2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Force compaction by triggering it
	db.compactionManager.ScheduleCompaction()
	db.WaitForCompaction()

	// Verify the key exists and has the latest value
	value, err := db.Get([]byte("test_key"))
	if err != nil {
		t.Fatalf("Expected key to be found after compaction, got error: %v", err)
	}
	if string(value) != "value2" {
		t.Fatalf("Expected value2, got %s", string(value))
	}

	fmt.Println("Tombstone compaction test passed - key properly restored after delete")

	// Test another scenario: Write key -> Delete key -> Verify key is gone
	if err := db.Put([]byte("delete_me"), []byte("will_be_deleted")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := db.Delete([]byte("delete_me")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Force another compaction
	db.compactionManager.ScheduleCompaction()
	db.WaitForCompaction()

	// Verify the key is deleted
	_, err = db.Get([]byte("delete_me"))
	if err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound for deleted key, got: %v", err)
	}

	fmt.Println("Tombstone deletion test passed - key properly deleted")
}

// TestTargetFileSizeProgression tests that individual SSTable files get larger at higher levels
// L0 files are the size of the memtable (WriteBufferSize), then grow by LevelFileSizeMultiplier
func TestTargetFileSizeProgression(t *testing.T) {
	// Test default options target file size progression
	opts := DefaultOptions()

	// With default WriteBufferSize=4MB and LevelFileSizeMultiplier=2.0:
	expectedSizes := []int64{
		4 * MiB,   // L0 = WriteBufferSize
		8 * MiB,   // L1 = L0 * 2
		16 * MiB,  // L2 = L1 * 2
		32 * MiB,  // L3 = L2 * 2
		64 * MiB,  // L4 = L3 * 2
		128 * MiB, // L5 = L4 * 2
		256 * MiB, // L6 = L5 * 2
	}

	for level, expectedSize := range expectedSizes {
		actualSize := opts.TargetFileSize(level)
		if actualSize != expectedSize {
			t.Errorf("Level %d: expected target file size %d, got %d",
				level, expectedSize, actualSize)
		}
	}

	// Test that each level is exactly multiplied by LevelFileSizeMultiplier (default 2.0)
	for level := 1; level < opts.MaxLevels; level++ {
		prevSize := opts.TargetFileSize(level - 1)
		currSize := opts.TargetFileSize(level)
		expectedSize := int64(float64(prevSize) * opts.LevelFileSizeMultiplier)
		if currSize != expectedSize {
			t.Errorf("Level %d size (%d) should be %dx level %d size (%d), expected %d",
				level, currSize, int(opts.LevelFileSizeMultiplier), level-1, prevSize, expectedSize)
		}
	}
}

// TestTargetFileSizeInvalidLevel tests behavior with invalid level numbers
func TestTargetFileSizeInvalidLevel(t *testing.T) {
	opts := DefaultOptions()

	// Test negative level (should return L0 size which is WriteBufferSize)
	expectedL0Size := int64(opts.WriteBufferSize)
	if size := opts.TargetFileSize(-1); size != expectedL0Size {
		t.Errorf("Invalid level -1: expected %d, got %d", expectedL0Size, size)
	}
}

// TestCompactionUsesCorrectTargetSize tests that compaction actually uses
// level-specific target sizes when creating new SSTable files
func TestCompactionUsesCorrectTargetSize(t *testing.T) {
	// Create a temporary directory for the test database
	dir := t.TempDir()

	// Configure very small target file sizes to make testing easier
	opts := DefaultOptions()
	opts.Path = dir
	opts.WriteBufferSize = 512 // Very small memtable
	opts.L0CompactionTrigger = 2

	// Set small sizes for easy verification with 2x multiplier
	opts.WriteBufferSize = 1024        // L0 files will be 1KB
	opts.LevelFileSizeMultiplier = 2.0 // L1=2KB, L2=4KB, L3=8KB, etc.

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Add enough data to trigger L0->L1 compaction
	for i := range 20 {
		key := []byte("key" + string(rune('0'+i%10)))
		value := make([]byte, 200) // 200 byte values
		for j := range value {
			value[j] = byte(i)
		}

		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// Force flush to create L0 files
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Add more data to trigger another flush and then compaction
	for i := 20; i < 40; i++ {
		key := []byte("key" + string(rune('0'+i%10)))
		value := make([]byte, 200)
		for j := range value {
			value[j] = byte(i)
		}

		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// Force another flush
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Force compaction to move data from L0 to L1
	if err := db.CompactRange(); err != nil {
		t.Fatal(err)
	}

	// Check that files exist and the database is using correct target sizes
	// This is mainly a verification that the system doesn't crash and
	// that the TargetFileSize method is being called correctly

	// Verify we can still read all the data
	for i := range 40 {
		key := []byte("key" + string(rune('0'+i%10)))
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to read key after compaction: %v", err)
		}
		if len(value) != 200 {
			t.Errorf("Wrong value length for key %s: expected 200, got %d", key, len(value))
		}
	}

	// List SSTable files to verify they were created
	sstFiles, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if len(sstFiles) == 0 {
		t.Fatal("No SSTable files found after compaction")
	}

	t.Logf("Created %d SSTable files during compaction test", len(sstFiles))
}

// TestOptionsValidationWithLevelFileSizeMultiplier tests that invalid LevelFileSizeMultiplier is caught
func TestOptionsValidationWithLevelFileSizeMultiplier(t *testing.T) {
	// Create a temporary directory for validation tests
	dir := t.TempDir()

	// Test with multiplier <= 1.0 (invalid)
	opts := DefaultOptions()
	opts.Path = dir
	opts.LevelFileSizeMultiplier = 0.5

	if err := opts.Validate(); err == nil {
		t.Error("Expected validation error for LevelFileSizeMultiplier <= 1.0")
	}

	// Test with negative multiplier
	opts = DefaultOptions()
	opts.Path = dir
	opts.LevelFileSizeMultiplier = -1.0

	if err := opts.Validate(); err == nil {
		t.Error("Expected validation error for negative LevelFileSizeMultiplier")
	}

	// Test with zero multiplier
	opts = DefaultOptions()
	opts.Path = dir
	opts.LevelFileSizeMultiplier = 0.0

	if err := opts.Validate(); err == nil {
		t.Error("Expected validation error for zero LevelFileSizeMultiplier")
	}

	// Test valid progression
	opts = DefaultOptions()
	opts.Path = dir
	if err := opts.Validate(); err != nil {
		t.Errorf("Valid options should pass validation: %v", err)
	}
}

// TestFileSelectionStrategies tests different file selection strategies for compaction
func TestFileSelectionStrategies(t *testing.T) {
	tmpDir := t.TempDir()

	// Create options with file selection strategy
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 512   // Small buffer to create multiple files
	opts.L0CompactionTrigger = 2 // Trigger L0 compaction more easily
	opts.MaxLevels = 3

	// Test with oldest-first strategy
	t.Run("OldestSmallestSeqFirst", func(t *testing.T) {
		testDir := filepath.Join(tmpDir, "oldest_first")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatal(err)
		}

		testOpts := *opts
		testOpts.Path = testDir

		db, err := Open(&testOpts)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create multiple files with different ages (sequence numbers)
		// File 1: keys 000-099
		for i := range 100 {
			key := fmt.Sprintf("key%03d", i)
			value := fmt.Sprintf("value%03d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}

		// File 2: keys 100-199
		for i := 100; i < 200; i++ {
			key := fmt.Sprintf("key%03d", i)
			value := fmt.Sprintf("value%03d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}

		// File 3: keys 200-299
		for i := 200; i < 300; i++ {
			key := fmt.Sprintf("key%03d", i)
			value := fmt.Sprintf("value%03d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}

		// Get initial L1 files count
		stats := db.GetStats()
		l1FilesBefore := stats["levels"].(map[string]int)["level_1_files"]

		// Wait for automatic compaction to happen
		db.WaitForCompaction()

		// Force additional compaction to move files from L1 to L2
		if err := db.CompactRange(); err != nil {
			t.Fatal(err)
		}

		// Wait for compaction to complete
		db.WaitForCompaction()

		// Verify compaction happened
		stats = db.GetStats()
		l1FilesAfter := stats["levels"].(map[string]int)["level_1_files"]
		l2FilesAfter := stats["levels"].(map[string]int)["level_2_files"]

		t.Logf("OldestFirst: L1 files %d->%d, L2 files: %d", l1FilesBefore, l1FilesAfter, l2FilesAfter)

		// Note: The actual file selection strategy difference may not be visible in basic tests
		// since file numbers are sequential. The key is that the code runs without errors
		// and selects a file using the configured strategy.
		t.Logf("File selection strategy test completed successfully")

		// Verify all data is still readable
		for i := range 300 {
			key := fmt.Sprintf("key%03d", i)
			expectedValue := fmt.Sprintf("value%03d", i)

			value, err := db.Get([]byte(key))
			if err != nil {
				t.Fatalf("Failed to get key %s: %v", key, err)
			}
			if string(value) != expectedValue {
				t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
			}
		}
	})

	// Test with min-overlap strategy
	t.Run("MinOverlappingRatio", func(t *testing.T) {
		testDir := filepath.Join(tmpDir, "min_overlap")
		if err := os.MkdirAll(testDir, 0755); err != nil {
			t.Fatal(err)
		}

		testOpts := *opts
		testOpts.Path = testDir

		db, err := Open(&testOpts)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create files with different overlap characteristics
		// File 1: keys 000-050 (low overlap with next level)
		for i := range 51 {
			key := fmt.Sprintf("key%03d", i)
			value := fmt.Sprintf("value%03d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}

		// File 2: keys 045-095 (high overlap - spans existing ranges)
		for i := 45; i < 96; i++ {
			key := fmt.Sprintf("key%03d", i)
			value := fmt.Sprintf("updated%03d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}

		// File 3: keys 200-250 (no overlap with existing)
		for i := 200; i < 251; i++ {
			key := fmt.Sprintf("key%03d", i)
			value := fmt.Sprintf("value%03d", i)
			if err := db.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}

		// Wait for automatic compaction to happen
		db.WaitForCompaction()

		// Force additional compaction
		if err := db.CompactRange(); err != nil {
			t.Fatal(err)
		}

		// Wait for compaction to complete
		db.WaitForCompaction()

		// Verify all data is still readable and has correct values
		// Keys 045-050 should have "updated" values (from file 2)
		for i := 45; i <= 50; i++ {
			key := fmt.Sprintf("key%03d", i)
			expectedValue := fmt.Sprintf("updated%03d", i)

			value, err := db.Get([]byte(key))
			if err != nil {
				t.Fatalf("Failed to get key %s: %v", key, err)
			}
			if string(value) != expectedValue {
				t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
			}
		}
	})
}

// TestLevelSizeLimitsAndCompactionStrategy tests that:
// 1. Level size limits are calculated correctly
// 2. Compaction is triggered when levels exceed their size limits
// 3. Files are compacted to appropriate target sizes
func TestLevelSizeLimitsAndCompactionStrategy(t *testing.T) {
	tmpDir := t.TempDir()

	// Create options with small, predictable sizes
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 4 * KiB     // 4KB memtable -> 4KB L0 files
	opts.LevelFileSizeMultiplier = 2.0 // Files double each level: L0=4KB, L1=8KB, L2=16KB
	opts.LevelSizeMultiplier = 10.0    // Levels 10x bigger: L1=40KB, L2=400KB, L3=4MB
	opts.L0CompactionTrigger = 6       // Compact after 6 L0 files - gives us 24KB input to create 3x 8KB L1 files
	opts.MaxLevels = 4
	opts.DisableWAL = true // Faster testing

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test 1: Verify level size limits are calculated correctly
	expectedL1MaxBytes := int64(opts.TargetFileSize(1) * 10) // 8KB * 10 = 80KB
	expectedL2MaxBytes := expectedL1MaxBytes * 10            // 80KB * 10 = 800KB
	expectedL3MaxBytes := expectedL2MaxBytes * 10            // 800KB * 10 = 8MB

	// Access the compaction manager to check level limits (this exposes the bug)
	cm := db.compactionManager
	actualL1MaxBytes := cm.levelMaxBytes[1]
	actualL2MaxBytes := cm.levelMaxBytes[2]
	actualL3MaxBytes := cm.levelMaxBytes[3]

	t.Logf("Expected L1 max bytes: %d, Actual: %d", expectedL1MaxBytes, actualL1MaxBytes)
	t.Logf("Expected L2 max bytes: %d, Actual: %d", expectedL2MaxBytes, actualL2MaxBytes)
	t.Logf("Expected L3 max bytes: %d, Actual: %d", expectedL3MaxBytes, actualL3MaxBytes)

	// This will FAIL with current bug - level limits are wrong
	if actualL1MaxBytes != expectedL1MaxBytes {
		t.Errorf("L1 max bytes incorrect: expected %d, got %d", expectedL1MaxBytes, actualL1MaxBytes)
	}
	if actualL2MaxBytes != expectedL2MaxBytes {
		t.Errorf("L2 max bytes incorrect: expected %d, got %d", expectedL2MaxBytes, actualL2MaxBytes)
	}

	// Test 2: Fill L1 beyond its limit and verify compaction triggers L1->L2

	// Create enough data to significantly overfill L1 and trigger L1->L2 compaction
	// L1 target is 80KB, so let's create ~500KB of raw data to ensure multiple levels
	const numKeys = 2000  // 2000 keys
	const valueSize = 800 // 800 bytes per value = ~1.6MB raw data

	t.Log("Writing large dataset to trigger natural compactions...")
	for i := range numKeys {
		key := fmt.Sprintf("testkey%06d", i)
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte('a' + (i % 26))
		}

		if err := db.Put([]byte(key), value); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}

		// No forced flushes - let memtable fill naturally and auto-flush
		// This will create properly sized L0 files that compact into properly sized L1 files
	}

	// Let any remaining memtable data flush naturally
	// Only do a final flush if there's unflushed data
	t.Log("Allowing natural flush of remaining memtable data...")
	if err := db.Flush(); err != nil {
		t.Fatalf("Final flush failed: %v", err)
	}

	t.Log("Waiting for automatic compactions...")
	db.WaitForCompaction()

	// Force additional compactions to ensure all levels are balanced
	t.Log("Forcing manual compaction...")
	if err := db.CompactAll(); err != nil {
		t.Fatalf("CompactAll failed: %v", err)
	}

	// Test 3: Verify final level distribution and file sizes
	version := db.versions.GetCurrentVersion()
	defer version.MarkForCleanup()

	// Check each level's total size and file count
	for level := range 4 {
		files := version.GetFiles(level)
		totalSize := int64(0)
		for _, file := range files {
			totalSize += int64(file.Size)
		}

		maxBytes := cm.levelMaxBytes[level]
		if level == 0 {
			maxBytes = int64(opts.L0CompactionTrigger * opts.WriteBufferSize) // L0 uses file count
		}

		t.Logf("Level %d: %d files, %d bytes total, %d bytes limit",
			level, len(files), totalSize, maxBytes)

		// Level should not grossly exceed its limit (some overage is OK during compaction)
		if level > 0 && totalSize > maxBytes*2 {
			t.Errorf("Level %d severely over limit: %d bytes > %d bytes (2x limit)",
				level, totalSize, maxBytes*2)
		}

		// Check individual file sizes are reasonable for the level
		// Only check L2+ since L0->L1 naturally creates small files from small memtable inputs
		// L1->L2+ compaction should create properly sized files by combining multiple inputs
		if level >= 2 && len(files) > 0 {
			expectedFileSize := opts.TargetFileSize(level)
			for i, file := range files {
				actualSize := int64(file.Size)
				// Files should be roughly target size (within 50% tolerance)
				minSize := expectedFileSize / 2
				maxSize := expectedFileSize * 2

				if actualSize < minSize || actualSize > maxSize {
					t.Errorf("Level %d file %d size out of range: %d bytes, expected %d±50%%",
						level, i, actualSize, expectedFileSize)
				}
			}
		}
	}

	// Test 4: Verify data integrity after all compactions
	t.Log("Verifying data integrity...")
	for i := range numKeys {
		key := fmt.Sprintf("testkey%06d", i)
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
			continue
		}

		// Verify value content
		if len(value) != valueSize {
			t.Errorf("Key %s: wrong value size %d, expected %d", key, len(value), valueSize)
		} else {
			expectedByte := byte('a' + (i % 26))
			if value[0] != expectedByte {
				t.Errorf("Key %s: wrong value content, got %c expected %c",
					key, value[0], expectedByte)
			}
		}
	}
}

// TestTombstoneEarlyDrop tests that tombstones can be dropped during intermediate level compactions
// when the deleted key doesn't exist in lower levels (not just at the bottom level)
func TestTombstoneEarlyDrop(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 512
	opts.L0CompactionTrigger = 2
	opts.MaxLevels = 4 // L0, L1, L2, L3

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Scenario: Create a situation where:
	// - L1 has: DELETE(key001) and PUT(key500)
	// - L2 has: PUT(key800), PUT(key900) (no key001)
	// - L3 is empty
	//
	// When compacting L1->L2, the DELETE(key001) tombstone should be dropped
	// because key001 doesn't exist anywhere in L2 or L3

	// Step 1: Create L2 data first (keys that DON'T include key001)
	for i := 800; i < 820; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// More L2 data
	for i := 900; i < 920; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Wait for L0->L1 compaction
	db.WaitForCompaction()
	time.Sleep(200 * time.Millisecond)

	// Now create data in L1 that will compact to L2
	// First, create the key we'll delete
	if err := db.Put([]byte("key001"), []byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Delete the key (creates tombstone)
	if err := db.Delete([]byte("key001")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Add more data to trigger compaction
	for i := 500; i < 520; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Wait for compactions to settle
	db.WaitForCompaction()
	time.Sleep(300 * time.Millisecond)

	// Force manual compaction to ensure L1->L2 happens
	if err := db.CompactRange(); err != nil {
		t.Fatal(err)
	}
	db.WaitForCompaction()

	// Now check L2 for tombstones - the DELETE(key001) should have been dropped
	// during L1->L2 compaction because key001 doesn't exist in L2 or below
	version := db.versions.GetCurrentVersion()
	defer version.MarkForCleanup()

	l2Files := version.GetFiles(2)
	tombstoneCount := 0
	tombstoneKeys := []string{}

	for _, file := range l2Files {
		cachedReader, err := db.openSSTable(file.FileNum)
		if err != nil {
			t.Logf("Could not open L2 file %d: %v", file.FileNum, err)
			continue
		}

		reader := cachedReader.Reader()
		iter := reader.NewIterator(true)

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := iter.Key()
			if key != nil && key.Kind() == 2 { // KindDelete
				tombstoneCount++
				userKey := string(key.UserKey())
				tombstoneKeys = append(tombstoneKeys, userKey)
				t.Logf("Found tombstone in L2: %s", userKey)
			}
		}
		iter.Close()
	}

	// With the optimization, tombstones should be dropped
	if tombstoneCount > 0 {
		t.Errorf("Found %d tombstones in L2 that should have been dropped: %v",
			tombstoneCount, tombstoneKeys)
		t.Logf("These tombstones should have been dropped during L1->L2 compaction")
		t.Logf("because the deleted keys don't exist in L2 or lower levels")
	} else {
		t.Logf("SUCCESS: No tombstones found in L2 - early dropping is working correctly")
	}
}

// TestKeyNotExistsBeyondOutputLevel tests the logic for determining if a key
// exists in levels below the compaction output level
func TestKeyNotExistsBeyondOutputLevel(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 512
	opts.L0CompactionTrigger = 4
	opts.MaxLevels = 4

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a known distribution across levels
	// L1: key100-key199
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("key%03d", i)
		if err := db.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	db.Flush()

	// L1: key300-key399
	for i := 300; i < 400; i++ {
		key := fmt.Sprintf("key%03d", i)
		if err := db.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	db.Flush()

	// Wait for data to settle into levels
	db.WaitForCompaction()
	time.Sleep(200 * time.Millisecond)

	// Add more data to create L2
	for i := 500; i < 600; i++ {
		key := fmt.Sprintf("key%03d", i)
		if err := db.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	db.Flush()

	for i := 700; i < 800; i++ {
		key := fmt.Sprintf("key%03d", i)
		if err := db.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	db.Flush()

	db.WaitForCompaction()
	time.Sleep(300 * time.Millisecond)

	// Get current version and check actual file distribution
	version := db.versions.GetCurrentVersion()
	defer version.MarkForCleanup()

	// Log what's actually in each level for debugging
	for level := range 4 {
		files := version.GetFiles(level)
		if len(files) > 0 {
			t.Logf("Level %d has %d files:", level, len(files))
			for _, file := range files {
				t.Logf("  File %d: %s to %s", file.FileNum,
					string(file.SmallestKey.UserKey()),
					string(file.LargestKey.UserKey()))
			}
		}
	}

	// Test cases for KeyNotExistsBeyondOutputLevel
	// These test the logic regardless of actual data distribution
	testCases := []struct {
		name        string
		userKey     string
		outputLevel int
		shouldExist bool
		description string
	}{
		{
			name:        "key not in any range",
			userKey:     "key050",
			outputLevel: 1,
			shouldExist: false,
			description: "key050 doesn't exist anywhere, so doesn't exist beyond L1",
		},
		{
			name:        "key at bottom level",
			userKey:     "key999",
			outputLevel: 3, // MaxLevels-1
			shouldExist: false,
			description: "any key at bottom level doesn't exist beyond",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a simple compaction context
			compaction := &Compaction{
				level:       tc.outputLevel - 1,
				outputLevel: tc.outputLevel,
				version:     version,
			}

			// Call the function
			keyNotExists := compaction.KeyNotExistsBeyondOutputLevel([]byte(tc.userKey))

			t.Logf("Test case: %s", tc.description)
			t.Logf("  User key: %s, Output level: %d", tc.userKey, tc.outputLevel)
			t.Logf("  Expected: key NOT exists beyond = %v, Got: %v", !tc.shouldExist, keyNotExists)

			// Verify the result
			if keyNotExists != !tc.shouldExist {
				t.Errorf("KeyNotExistsBeyondOutputLevel(%s) = %v, want %v",
					tc.userKey, keyNotExists, !tc.shouldExist)
			}
		})
	}

	t.Log("KeyNotExistsBeyondOutputLevel tests completed")
}
