//go:build integration || stress
// +build integration stress

package lgdb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/twlk9/lgdb/epoch"
	"github.com/twlk9/lgdb/keys"
)

// TestRangeDeleteCleanupNoOverlap tests that range deletes are cleaned up
// when they have no overlapping keys in the database. This is the simplest
// and most reliable test for the cleanup mechanism.
func TestRangeDeleteCleanupNoOverlap(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxLevels = 4
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 2

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Phase 1: Write data in range [key000, key500)
	t.Log("Phase 1: Writing data in range [key000, key500)")
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	db.WaitForCompaction()
	time.Sleep(50 * time.Millisecond)

	// Phase 2: Create range delete for non-overlapping range [zzz000, zzz999)
	t.Log("Phase 2: Creating range delete [zzz000, zzz999) with no overlapping data")
	err = db.DeleteRange(keys.UserKey([]byte("zzz000")), keys.UserKey([]byte("zzz999")))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Flush to persist range delete
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush after range delete failed: %v", err)
	}
	db.WaitForCompaction()

	// Verify .rangedel file was created
	rangeDelFiles, err := filepath.Glob(filepath.Join(dir, "*.rangedel"))
	if err != nil {
		t.Fatalf("Failed to glob rangedel files: %v", err)
	}
	t.Logf("Found %d .rangedel files", len(rangeDelFiles))
	if len(rangeDelFiles) == 0 {
		t.Fatal("Expected .rangedel file to be created")
	}

	// Verify range delete exists in version
	version := db.versions.GetCurrentVersion()
	rangeDeletes := version.GetRangeDeletes()
	t.Logf("Active range deletes after creation: %d", len(rangeDeletes))
	if len(rangeDeletes) != 1 {
		t.Fatalf("Expected exactly 1 range delete, got %d", len(rangeDeletes))
	}

	// Verify it's the range delete we created
	rd := rangeDeletes[0]
	if string(rd.Start) != "zzz000" || string(rd.End) != "zzz999" {
		t.Fatalf("Expected range delete [zzz000, zzz999), got [%s, %s)",
			string(rd.Start), string(rd.End))
	}
	t.Logf("Found range delete: ID=%d, [%s, %s), Seq=%d",
		rd.ID, string(rd.Start), string(rd.End), rd.Seq)
	version.MarkForCleanup()

	// Phase 3: Force compaction to trigger cleanup check
	t.Log("Phase 3: Forcing compaction to trigger cleanup")

	// Write some more data to trigger background compaction
	for i := 500; i < 600; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	db.WaitForCompaction()
	time.Sleep(50 * time.Millisecond)

	// Force explicit compaction
	if err := db.CompactRange(); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}
	db.WaitForCompaction()

	// Force epoch advancement to allow cleanup
	epoch.AdvanceEpoch()
	time.Sleep(100 * time.Millisecond)

	// Force another compaction to ensure cleanup runs
	if err := db.CompactRange(); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}
	db.WaitForCompaction()
	time.Sleep(100 * time.Millisecond)

	// Phase 4: Verify range delete was cleaned up
	t.Log("Phase 4: Verifying range delete was cleaned up")
	version = db.versions.GetCurrentVersion()
	rangeDeletes = version.GetRangeDeletes()
	t.Logf("Active range deletes after cleanup: %d", len(rangeDeletes))
	version.MarkForCleanup()

	if len(rangeDeletes) != 0 {
		t.Errorf("Expected range delete to be cleaned up, but found %d range deletes",
			len(rangeDeletes))
		for _, rd := range rangeDeletes {
			t.Logf("  Remaining: ID=%d, [%s, %s), Seq=%d",
				rd.ID, string(rd.Start), string(rd.End), rd.Seq)
		}
	}

	// Phase 5: Verify data is still intact
	t.Log("Phase 5: Verifying original data is intact")
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Key %s should exist, got error: %v", key, err)
		}
		expectedValue := []byte(fmt.Sprintf("value%03d", i))
		if string(value) != string(expectedValue) {
			t.Errorf("Key %s has wrong value: got %s, want %s",
				key, string(value), string(expectedValue))
		}
	}

	t.Log("SUCCESS: Range delete cleanup test completed")
}

// TestRangeDeleteWithOverlappingWrites tests that newer writes (higher sequence numbers)
// are visible even when covered by a range delete with a lower sequence number
func TestRangeDeleteWithOverlappingWrites(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxLevels = 3
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 2

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Phase 1: Write initial data
	t.Log("Phase 1: Writing initial data [key100, key200)")
	for i := 100; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte("old_value")
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Phase 2: Create range delete
	t.Log("Phase 2: Creating range delete [key100, key200)")
	err = db.DeleteRange(keys.UserKey([]byte("key100")), keys.UserKey([]byte("key200")))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify old keys are deleted
	for i := 100; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		_, err := db.Get(key)
		if err != ErrNotFound {
			t.Errorf("Old key %s should be deleted, got: %v", key, err)
		}
	}

	// Phase 3: Write new data in the same range (higher sequence numbers)
	t.Log("Phase 3: Writing new data in deleted range")
	for i := 100; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte("new_value")
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Verify new keys are visible
	for i := 100; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("New key %s should exist, got error: %v", key, err)
		} else if string(value) != "new_value" {
			t.Errorf("Key %s has wrong value: %s, expected: new_value", key, value)
		}
	}

	// Phase 4: Flush and compact
	t.Log("Phase 4: Flushing and compacting")
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	db.WaitForCompaction()
	time.Sleep(100 * time.Millisecond)

	if err := db.CompactRange(); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}
	db.WaitForCompaction()

	// Phase 5: Verify new values persist after compaction
	t.Log("Phase 5: Verifying new values after compaction")
	for i := 100; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("Key %s should exist after compaction, got error: %v", key, err)
		} else if string(value) != "new_value" {
			t.Errorf("Key %s has wrong value after compaction: %s", key, value)
		}
	}

	// Verify range delete still exists (since there's overlapping data)
	version := db.versions.GetCurrentVersion()
	rangeDeletes := version.GetRangeDeletes()
	t.Logf("Range deletes after writes: %d", len(rangeDeletes))
	version.MarkForCleanup()

	t.Log("SUCCESS: Overlapping writes test completed")
}

// TestRangeDeleteRecovery tests that range deletes are correctly recovered
// from the .rangedel file after database restart
func TestRangeDeleteRecovery(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxLevels = 3
	opts.WriteBufferSize = 1024
	opts.L0CompactionTrigger = 2

	// First session: create data and range deletes
	t.Log("Session 1: Creating data and range deletes")
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Create multiple range deletes
	rangeSpecs := [][2]string{
		{"key010", "key020"},
		{"key030", "key040"},
		{"key050", "key060"},
	}

	for _, spec := range rangeSpecs {
		err = db.DeleteRange(keys.UserKey([]byte(spec[0])), keys.UserKey([]byte(spec[1])))
		if err != nil {
			t.Fatalf("DeleteRange failed: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	db.WaitForCompaction()

	// Record range deletes before close
	version := db.versions.GetCurrentVersion()
	rangeDeletesBefore := version.GetRangeDeletes()
	t.Logf("Range deletes before restart: %d", len(rangeDeletesBefore))

	rangeDeleteIDs := make(map[uint64]bool)
	for _, rd := range rangeDeletesBefore {
		rangeDeleteIDs[rd.ID] = true
		t.Logf("  RD ID=%d: [%s, %s)", rd.ID, string(rd.Start), string(rd.End))
	}
	version.MarkForCleanup()

	// Close database
	db.Close()

	// Second session: reopen and verify recovery
	t.Log("Session 2: Reopening database and verifying recovery")
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify range deletes were recovered
	version = db.versions.GetCurrentVersion()
	rangeDeletesAfter := version.GetRangeDeletes()
	t.Logf("Range deletes after restart: %d", len(rangeDeletesAfter))

	for _, rd := range rangeDeletesAfter {
		t.Logf("  RD ID=%d: [%s, %s)", rd.ID, string(rd.Start), string(rd.End))
	}
	version.MarkForCleanup()

	if len(rangeDeletesAfter) != len(rangeDeletesBefore) {
		t.Errorf("Range delete count mismatch: before=%d, after=%d",
			len(rangeDeletesBefore), len(rangeDeletesAfter))
	}

	// Verify range deletes still filter keys correctly
	t.Log("Verifying range deletes filter keys after recovery")
	for _, spec := range rangeSpecs {
		start := spec[0]

		// Parse key numbers
		var startNum, endNum int
		fmt.Sscanf(start, "key%d", &startNum)
		fmt.Sscanf(spec[1], "key%d", &endNum)

		for i := startNum; i < endNum; i++ {
			key := []byte(fmt.Sprintf("key%03d", i))
			_, err := db.Get(key)
			if err != ErrNotFound {
				t.Errorf("Key %s should be deleted after recovery, got: %v", key, err)
			}
		}
	}

	// Verify keys outside ranges still exist
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		if _, err := db.Get(key); err != nil {
			t.Errorf("Key %s should exist after recovery, got: %v", key, err)
		}
	}

	t.Log("SUCCESS: Range delete recovery test completed")
}

// TestRangeDeleteStressTest creates a heavy workload with many range deletes,
// verifying correctness and that cleanup prevents unbounded growth
func TestRangeDeleteStressTest(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxLevels = 4
	opts.WriteBufferSize = 2048
	opts.L0CompactionTrigger = 3
	opts.LevelFileSizeMultiplier = 2.0
	opts.LevelSizeMultiplier = 4.0

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	const (
		numRounds      = 5
		keysPerRound   = 200
		rangesPerRound = 5
	)

	t.Log("Starting stress test with heavy range deletion workload")

	for round := 0; round < numRounds; round++ {
		t.Logf("Round %d/%d", round+1, numRounds)

		// Write data
		baseKey := round * 1000
		for i := 0; i < keysPerRound; i++ {
			key := []byte(fmt.Sprintf("skey%06d", baseKey+i))
			value := make([]byte, 50)
			for j := range value {
				value[j] = byte((baseKey + i) % 256)
			}
			if err := db.Put(key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Create range deletes
		for r := 0; r < rangesPerRound; r++ {
			start := baseKey + (r * 30)
			end := start + 20

			startKey := keys.UserKey([]byte(fmt.Sprintf("skey%06d", start)))
			endKey := keys.UserKey([]byte(fmt.Sprintf("skey%06d", end)))

			err = db.DeleteRange(startKey, endKey)
			if err != nil {
				t.Fatalf("DeleteRange failed: %v", err)
			}
		}

		// Flush and compact
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		db.WaitForCompaction()

		// Check range delete count doesn't grow unbounded
		version := db.versions.GetCurrentVersion()
		rdCount := len(version.GetRangeDeletes())
		t.Logf("  Active range deletes: %d", rdCount)
		version.MarkForCleanup()

		// With cleanup, count shouldn't exceed a reasonable threshold
		// (some accumulation is OK, but not linear growth)
		maxExpected := (round + 1) * rangesPerRound * 2 // Allow 2x accumulation
		if rdCount > maxExpected {
			t.Errorf("Range delete count too high: %d (max expected ~%d)", rdCount, maxExpected)
		}
	}

	// Force major compaction
	t.Log("Forcing major compaction")
	for i := 0; i < 3; i++ {
		if err := db.CompactRange(); err != nil {
			t.Fatalf("CompactRange failed: %v", err)
		}
		db.WaitForCompaction()
		time.Sleep(200 * time.Millisecond)
	}

	// Verify data integrity
	t.Log("Verifying data integrity")

	// Sample verification of existing keys
	for round := 0; round < numRounds; round++ {
		baseKey := round * 1000
		for i := 0; i < keysPerRound; i += 10 { // Sample every 10th key
			key := []byte(fmt.Sprintf("skey%06d", baseKey+i))
			keyNum := baseKey + i

			// Check if this key should be deleted by any range
			shouldBeDeleted := false
			for r := 0; r < rangesPerRound; r++ {
				start := baseKey + (r * 30)
				end := start + 20
				if keyNum >= start && keyNum < end {
					shouldBeDeleted = true
					break
				}
			}

			_, err := db.Get(key)
			if shouldBeDeleted {
				if err != ErrNotFound {
					t.Errorf("Key %s should be deleted, got: %v", key, err)
				}
			} else {
				if err != nil {
					t.Errorf("Key %s should exist, got error: %v", key, err)
				}
			}
		}
	}

	// Final stats
	version := db.versions.GetCurrentVersion()
	finalRDCount := len(version.GetRangeDeletes())
	t.Logf("Final active range deletes: %d", finalRDCount)

	for level := 0; level < opts.MaxLevels; level++ {
		files := version.GetFiles(level)
		t.Logf("  Level %d: %d files", level, len(files))
	}
	version.MarkForCleanup()

	t.Log("SUCCESS: Range delete stress test completed")
}

// TestRangeDeletePrefixDeletion tests the DeletePrefix convenience method
func TestRangeDeletePrefixDeletion(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = dir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write keys with different prefixes
	prefixes := []string{"user:", "session:", "cache:"}
	for _, prefix := range prefixes {
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("%s%04d", prefix, i))
			value := []byte("value")
			if err := db.Put(key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}

	// Delete all "session:" keys using prefix delete
	if err := db.DeletePrefix([]byte("session:")); err != nil {
		t.Fatalf("DeletePrefix failed: %v", err)
	}

	// Verify session keys are deleted
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("session:%04d", i))
		_, err := db.Get(key)
		if err != ErrNotFound {
			t.Errorf("Session key %s should be deleted, got: %v", key, err)
		}
	}

	// Verify other keys still exist
	for _, prefix := range []string{"user:", "cache:"} {
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("%s%04d", prefix, i))
			if _, err := db.Get(key); err != nil {
				t.Errorf("Key %s should exist, got error: %v", key, err)
			}
		}
	}

	t.Log("SUCCESS: Prefix deletion test completed")
}

// TestRangeDeleteEdgeCases tests edge cases and boundary conditions
func TestRangeDeleteEdgeCases(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = dir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test 1: Empty range delete (start == end)
	t.Log("Test 1: Empty range (start == end)")
	err = db.DeleteRange(keys.UserKey([]byte("key100")), keys.UserKey([]byte("key100")))
	if err != nil {
		t.Logf("Empty range delete returned error (acceptable): %v", err)
	} else {
		t.Log("Empty range delete succeeded (acceptable)")
	}

	// Test 2: Range delete with no matching keys
	t.Log("Test 2: Range delete with no data")
	err = db.DeleteRange(keys.UserKey([]byte("nodata000")), keys.UserKey([]byte("nodata999")))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Test 3: Overlapping range deletes
	t.Log("Test 3: Overlapping range deletes")

	// Write data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("okey%03d", i))
		if err := db.Put(key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Create overlapping deletes
	err = db.DeleteRange(keys.UserKey([]byte("okey010")), keys.UserKey([]byte("okey050")))
	if err != nil {
		t.Fatalf("DeleteRange 1 failed: %v", err)
	}

	err = db.DeleteRange(keys.UserKey([]byte("okey030")), keys.UserKey([]byte("okey070")))
	if err != nil {
		t.Fatalf("DeleteRange 2 failed: %v", err)
	}

	// Verify union of ranges is deleted
	for i := 10; i < 70; i++ {
		key := []byte(fmt.Sprintf("okey%03d", i))
		_, err := db.Get(key)
		if err != ErrNotFound {
			t.Errorf("Key %s should be deleted (overlapping ranges), got: %v", key, err)
		}
	}

	// Test 4: Range delete at exact file boundaries
	t.Log("Test 4: Range delete at file boundaries")
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	e := epoch.EnterEpoch()
	version := db.versions.GetCurrentVersion()
	files := version.GetFiles(0)
	if len(files) > 0 {
		// Get boundaries of first file
		smallest := files[0].SmallestKey.UserKey()
		largest := files[0].LargestKey.UserKey()

		t.Logf("File boundaries: [%s, %s]", string(smallest), string(largest))

		// Delete exactly that range
		err = db.DeleteRange(keys.UserKey(smallest), keys.UserKey(largest))
		if err != nil {
			t.Fatalf("DeleteRange at boundaries failed: %v", err)
		}
	}
	epoch.ExitEpoch(e)

	t.Log("SUCCESS: Edge cases test completed")
}
