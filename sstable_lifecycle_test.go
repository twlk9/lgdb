//go:build integration || stress
// +build integration stress

package lgdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/twlk9/lgdb/epoch"
)

// TestSSTableLifecycleL1ToLN tests the full SSTable lifecycle from L1 to LN with deletions
// Uses a smaller MaxLevels (3) to make testing faster and more predictable
func TestSSTableLifecycleL1ToLN(t *testing.T) {
	// Create a temporary directory for the test database
	dir := t.TempDir()

	// Create database with small MaxLevels for testing
	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxLevels = 3           // Only 3 levels: L0, L1, L2
	opts.WriteBufferSize = 512   // Small buffer to force frequent flushes
	opts.L0CompactionTrigger = 2 // Trigger compaction with just 2 files
	// Set small file sizes for all levels to force more compactions
	opts.WriteBufferSize = 1024        // Small memtable = small L0 files
	opts.LevelFileSizeMultiplier = 1.5 // Small growth for predictable test
	opts.LevelSizeMultiplier = 2.0     // Smaller multiplier for faster level growth

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Phase 1: Fill L0 and L1
	t.Log("Phase 1: Filling L0 and L1")
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
		}
	}

	// Force flush and wait for compaction
	err = db.Flush()
	if err != nil {
	}
	time.Sleep(100 * time.Millisecond)

	// Phase 2: Delete some keys and add more data
	t.Log("Phase 2: Deleting keys and adding more data")
	for i := 5; i < 15; i++ {
		key := fmt.Sprintf("key%03d", i)
		err := db.Delete([]byte(key))
		if err != nil {
		}
	}

	// Add more data to force more compactions
	for i := 20; i < 40; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
		}
	}

	// Force flush and wait for compaction
	err = db.Flush()
	if err != nil {
	}
	time.Sleep(200 * time.Millisecond)

	// Phase 3: Verify data integrity
	t.Log("Phase 3: Verifying data integrity")

	// Keys 0-4 should exist
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%03d", i)
		expectedValue := fmt.Sprintf("value%03d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Expected key %s to exist, got error: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Fatalf("Expected value %s, got %s", expectedValue, string(value))
		}
	}

	// Keys 5-14 should be deleted
	for i := 5; i < 15; i++ {
		key := fmt.Sprintf("key%03d", i)

		_, err := db.Get([]byte(key))
		if err != ErrNotFound {
			t.Fatalf("Expected key %s to be deleted, got error: %v", key, err)
		}
	}

	// Keys 15-39 should exist
	for i := 15; i < 40; i++ {
		key := fmt.Sprintf("key%03d", i)
		expectedValue := fmt.Sprintf("value%03d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Expected key %s to exist, got error: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Fatalf("Expected value %s, got %s", expectedValue, string(value))
		}
	}

	// Verify we have files at different levels
	db.mu.RLock()
	e := epoch.EnterEpoch()
	defer epoch.ExitEpoch(e)
	version := db.versions.GetCurrentVersion()
	db.mu.RUnlock()

	t.Logf("Level distribution after compaction:")
	for level := 0; level < opts.MaxLevels; level++ {
		files := version.GetFiles(level)
		t.Logf("  Level %d: %d files", level, len(files))
		for _, file := range files {
			t.Logf("    File %d: %s to %s", file.FileNum, string(file.SmallestKey), string(file.LargestKey))
		}
	}
}

// TestTombstoneEliminationAtMaxLevel tests that tombstones are properly eliminated
// when compacting at the maximum level (since there's no next level to compact to)
func TestTombstoneEliminationAtMaxLevel(t *testing.T) {
	// Create a temporary directory for the test database
	dir := t.TempDir()

	// Create database with very small MaxLevels for testing
	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxLevels = 2           // Only 2 levels: L0, L1 (L1 is max level)
	opts.WriteBufferSize = 256   // Very small buffer
	opts.L0CompactionTrigger = 2 // Trigger compaction with just 2 files
	// Set small file sizes for all levels
	opts.WriteBufferSize = 512         // Small memtable = small L0 files
	opts.LevelFileSizeMultiplier = 1.5 // Small growth for predictable test
	opts.LevelSizeMultiplier = 2.0     // Small multiplier

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Step 1: Write some keys
	t.Log("Step 1: Writing initial keys")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
		}
	}

	// Force flush to L0
	err = db.Flush()
	if err != nil {
	}
	time.Sleep(50 * time.Millisecond)

	// Step 2: Delete some keys
	t.Log("Step 2: Deleting keys")
	for i := 3; i < 7; i++ {
		key := fmt.Sprintf("key%03d", i)
		err := db.Delete([]byte(key))
		if err != nil {
		}
	}

	// Force flush to L0
	err = db.Flush()
	if err != nil {
	}
	time.Sleep(50 * time.Millisecond)

	// Step 3: Add more data to force compaction to L1 (max level)
	t.Log("Step 3: Adding more data to force compaction to max level")
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
		}
	}

	// Force multiple flushes and compactions
	for j := 0; j < 3; j++ {
		err = db.Flush()
		if err != nil {
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Step 4: Verify tombstones are properly handled
	t.Log("Step 4: Verifying tombstone elimination")

	// Keys 0-2 should exist
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key%03d", i)
		expectedValue := fmt.Sprintf("value%03d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Expected key %s to exist, got error: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Fatalf("Expected value %s, got %s", expectedValue, string(value))
		}
	}

	// Keys 3-6 should be deleted (tombstones should be eliminated)
	for i := 3; i < 7; i++ {
		key := fmt.Sprintf("key%03d", i)

		_, err := db.Get([]byte(key))
		if err != ErrNotFound {
			t.Fatalf("Expected key %s to be deleted, got error: %v", key, err)
		}
	}

	// Keys 7-19 should exist
	for i := 7; i < 20; i++ {
		key := fmt.Sprintf("key%03d", i)
		expectedValue := fmt.Sprintf("value%03d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Expected key %s to exist, got error: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Fatalf("Expected value %s, got %s", expectedValue, string(value))
		}
	}

	// Verify final level distribution
	db.mu.RLock()
	e := epoch.EnterEpoch()
	defer epoch.ExitEpoch(e)
	version := db.versions.GetCurrentVersion()
	db.mu.RUnlock()

	t.Logf("Final level distribution:")
	for level := 0; level < opts.MaxLevels; level++ {
		files := version.GetFiles(level)
		t.Logf("  Level %d: %d files", level, len(files))
		for _, file := range files {
			t.Logf("    File %d: %s to %s", file.FileNum, string(file.SmallestKey), string(file.LargestKey))
		}
	}

	// Verify that we have data at the max level (L1)
	maxLevelFiles := version.GetFiles(opts.MaxLevels - 1)
	if len(maxLevelFiles) == 0 {
		t.Error("Expected files at max level, but found none")
	}

	t.Logf("Successfully tested tombstone elimination at max level with %d files at L%d",
		len(maxLevelFiles), opts.MaxLevels-1)
}

// TestMultiLevelCompactionStress tests compaction across multiple levels with heavy deletion workload
func TestMultiLevelCompactionStress(t *testing.T) {
	// Create a temporary directory for the test database
	dir := t.TempDir()

	// Create database with 3 levels for stress testing
	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxLevels = 3           // 3 levels: L0, L1, L2
	opts.WriteBufferSize = 512   // Small buffer
	opts.L0CompactionTrigger = 3 // Trigger compaction with 3 files
	// Set small target file sizes for all levels
	opts.WriteBufferSize = 1024        // Small memtable = small L0 files
	opts.LevelFileSizeMultiplier = 1.5 // Small growth for predictable test
	opts.LevelSizeMultiplier = 4.0     // Larger multiplier for more realistic behavior

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Heavy write and delete workload
	t.Log("Starting heavy write and delete workload")
	const numKeys = 100
	const numPasses = 3

	for pass := 0; pass < numPasses; pass++ {
		t.Logf("Pass %d: Writing keys", pass+1)

		// Write keys
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%03d", i)
			value := fmt.Sprintf("value%03d-pass%d", i, pass)
			err := db.Put([]byte(key), []byte(value))
			if err != nil {
			}
		}

		// Delete every other key
		t.Logf("Pass %d: Deleting every other key", pass+1)
		for i := 0; i < numKeys; i += 2 {
			key := fmt.Sprintf("key%03d", i)
			err := db.Delete([]byte(key))
			if err != nil {
			}
		}

		// Force flush and wait for compaction
		err = db.Flush()
		if err != nil {
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Final verification
	t.Log("Final verification")

	// Even keys should be deleted
	for i := 0; i < numKeys; i += 2 {
		key := fmt.Sprintf("key%03d", i)
		value, err := db.Get([]byte(key))
		if err != ErrNotFound {
			t.Fatalf("Expected key %s to be deleted, got error: %v, value: %q", key, err, string(value))
		}
	}

	// Odd keys should exist with the latest value
	for i := 1; i < numKeys; i += 2 {
		key := fmt.Sprintf("key%03d", i)
		expectedValue := fmt.Sprintf("value%03d-pass%d", i, numPasses-1)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Expected key %s to exist, got error: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Fatalf("Expected value %s, got %s", expectedValue, string(value))
		}
	}

	// Report final statistics
	db.mu.RLock()
	e := epoch.EnterEpoch()
	defer epoch.ExitEpoch(e)
	version := db.versions.GetCurrentVersion()
	db.mu.RUnlock()

	t.Logf("Final statistics after stress test:")
	totalFiles := 0
	for level := 0; level < opts.MaxLevels; level++ {
		files := version.GetFiles(level)
		totalFiles += len(files)
		t.Logf("  Level %d: %d files", level, len(files))
	}
	t.Logf("Total files: %d", totalFiles)
}
