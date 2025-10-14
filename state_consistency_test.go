//go:build integration || stress
// +build integration stress

package lgdb

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestStateConsistencyBasic tests basic state consistency after operations
func TestStateConsistencyBasic(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 4096 // Small buffer to trigger compactions
	opts.L0CompactionTrigger = 2

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	validator := NewStateValidator(t, db)

	// Test sequence: Put -> Get -> Validate
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2_longer_value_to_test_different_sizes"},
		{"key3", ""},
		{"key_empty_value", "empty_value_test"},
		{"key_with_special_chars_!@#$%", "value_with_special_chars_!@#$%"},
	}

	// Put all data
	for _, td := range testData {
		err := db.Put([]byte(td.key), []byte(td.value))
		if err != nil {
			t.Fatalf("Put failed for key %q: %v", td.key, err)
		}
		validator.TrackPut([]byte(td.key), []byte(td.value))
	}

	// Validate consistency after puts
	validator.ValidateConsistency()

	// Delete some data
	err = db.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	validator.TrackDelete([]byte("key2"))

	// Validate consistency after delete
	validator.ValidateConsistency()

	// Update existing key
	err = db.Put([]byte("key1"), []byte("updated_value1"))
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	validator.TrackPut([]byte("key1"), []byte("updated_value1"))

	// Final validation
	validator.ValidateConsistency()
}

// TestStateConsistencyDuringCompaction tests consistency while compaction is happening
func TestStateConsistencyDuringCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 2048 // Very small to trigger frequent compactions
	opts.L0CompactionTrigger = 2
	// Remove L0SlowdownWritesTrigger as it doesn't exist in our options

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	validator := NewStateValidator(t, db)

	// Write enough data to trigger multiple compactions
	const numKeys = 200
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("compaction_test_key_%04d", i)
		value := fmt.Sprintf("compaction_test_value_%04d_with_extra_data_to_make_it_larger", i)

		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Put failed at key %d: %v", i, err)
		}
		validator.TrackPut([]byte(key), []byte(value))

		// Validate consistency periodically during writes
		if i%20 == 0 {
			validator.ValidateConsistency()
		}

		// Small delay to allow compactions to happen
		if i%10 == 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}

	// Wait a bit for any background compactions to finish
	time.Sleep(50 * time.Millisecond)

	// Final comprehensive validation
	validator.ValidateConsistency()
}

// TestStateConsistencyConcurrentOperations tests consistency under concurrent operations
func TestStateConsistencyConcurrentOperations(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 8192
	opts.L0CompactionTrigger = 3

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	validator := NewStateValidator(t, db)

	const numGoroutines = 3
	const numOperations = 50

	var wg sync.WaitGroup

	// Start concurrent writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("concurrent_%d_%04d", goroutineID, j)
				value := fmt.Sprintf("concurrent_value_%d_%04d", goroutineID, j)

				err := db.Put([]byte(key), []byte(value))
				if err != nil {
					t.Errorf("Concurrent Put failed: %v", err)
					continue
				}
				validator.TrackPut([]byte(key), []byte(value))

				// Occasionally delete some data
				if j%10 == 5 {
					deleteKey := fmt.Sprintf("concurrent_%d_%04d", goroutineID, j-5)
					err := db.Delete([]byte(deleteKey))
					if err != nil {
						t.Errorf("Concurrent Delete failed: %v", err)
						continue
					}
					validator.TrackDelete([]byte(deleteKey))
				}
			}
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()

	// Wait a bit for any background work to complete
	time.Sleep(100 * time.Millisecond)

	// Validate final state consistency
	validator.ValidateConsistency()
}

// TestPropertyBasedOperations tests using property-based random operations
func TestPropertyBasedOperations(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 4096
	opts.L0CompactionTrigger = 3

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Use a fixed seed for reproducible tests
	seed := int64(12345)
	pbt := NewPropertyBasedTester(t, db, seed)

	// Run a sequence of random operations
	pbt.RunRandomOperationSequence(100)
}

// TestStateConsistencyWithLargeValues tests consistency with various value sizes
func TestStateConsistencyWithLargeValues(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 8192
	opts.L0CompactionTrigger = 2

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	validator := NewStateValidator(t, db)

	// Test different value sizes
	valueSizes := []int{0, 1, 100, 1000, 5000, 10000}

	for i, size := range valueSizes {
		key := fmt.Sprintf("large_value_key_%d", i)
		value := make([]byte, size)

		// Fill with pattern for verification
		for j := range value {
			value[j] = byte(j % 256)
		}

		err := db.Put([]byte(key), value)
		if err != nil {
			t.Fatalf("Put failed for large value size %d: %v", size, err)
		}
		validator.TrackPut([]byte(key), value)

		// Validate after each large value
		validator.ValidateConsistency()
	}
}

// TestIteratorConsistencyDuringModification tests iterator consistency when DB is modified
func TestIteratorConsistencyDuringModification(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 4096
	opts.L0CompactionTrigger = 3

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Put initial data
	initialData := make(map[string][]byte)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("iter_key_%04d", i)
		value := fmt.Sprintf("iter_value_%04d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Initial put failed: %v", err)
		}
		initialData[key] = []byte(value)
	}

	// Create iterator
	iter := db.NewIterator(nil)
	defer iter.Close()

	// Collect initial iterator state
	initialIterData := make(map[string][]byte)
	iter.SeekToFirst()
	for iter.Valid() {
		key := string(iter.Key())
		value := bytes.Clone(iter.Value())
		initialIterData[key] = value
		iter.Next()
	}

	// Modify database while iterator exists
	for i := 50; i < 100; i++ {
		key := fmt.Sprintf("iter_key_%04d", i)
		value := fmt.Sprintf("iter_value_%04d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Concurrent put failed: %v", err)
		}
	}

	// Delete some existing data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("iter_key_%04d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Concurrent delete failed: %v", err)
		}
	}

	// Verify iterator still sees consistent snapshot
	iter.SeekToFirst()
	currentIterData := make(map[string][]byte)
	for iter.Valid() {
		key := string(iter.Key())
		value := bytes.Clone(iter.Value())
		currentIterData[key] = value
		iter.Next()
	}

	// Iterator should see the same data as when it was created
	if len(initialIterData) != len(currentIterData) {
		t.Errorf("Iterator data count changed: initial %d, current %d",
			len(initialIterData), len(currentIterData))
	}

	for key, expectedValue := range initialIterData {
		if actualValue, exists := currentIterData[key]; !exists {
			t.Errorf("Iterator lost key %q during concurrent modifications", key)
		} else if !bytes.Equal(expectedValue, actualValue) {
			t.Errorf("Iterator key %q value changed: expected %q, got %q",
				key, expectedValue, actualValue)
		}
	}
}

// TestTombstoneLifecycleConsistency tests that deletes work correctly through compaction
func TestTombstoneLifecycleConsistency(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 2048 // Small to trigger compactions
	opts.L0CompactionTrigger = 2

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	validator := NewStateValidator(t, db)

	// Phase 1: Put data
	const numKeys = 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("tombstone_key_%04d", i)
		value := fmt.Sprintf("tombstone_value_%04d", i)

		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		validator.TrackPut([]byte(key), []byte(value))
	}

	// Phase 2: Delete half the data
	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("tombstone_key_%04d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		validator.TrackDelete([]byte(key))
	}

	// Phase 3: Add more data to trigger compactions
	for i := numKeys; i < numKeys*2; i++ {
		key := fmt.Sprintf("tombstone_key_%04d", i)
		value := fmt.Sprintf("tombstone_value_%04d", i)

		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Second phase put failed: %v", err)
		}
		validator.TrackPut([]byte(key), []byte(value))
	}

	// Wait for compactions to process tombstones
	time.Sleep(100 * time.Millisecond)

	// Phase 4: Validate tombstones worked correctly
	validator.ValidateConsistency()

	// Explicitly verify deleted keys are gone
	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("tombstone_key_%04d", i)
		_, err := db.Get([]byte(key))
		if err != ErrNotFound {
			t.Errorf("Deleted key %q should not exist, but got error: %v", key, err)
		}
	}

	// Verify non-deleted keys still exist
	for i := numKeys / 2; i < numKeys; i++ {
		key := fmt.Sprintf("tombstone_key_%04d", i)
		expectedValue := fmt.Sprintf("tombstone_value_%04d", i)

		actualValue, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Non-deleted key %q should exist: %v", key, err)
			continue
		}
		if string(actualValue) != expectedValue {
			t.Errorf("Non-deleted key %q: expected %q, got %q",
				key, expectedValue, actualValue)
		}
	}
}
