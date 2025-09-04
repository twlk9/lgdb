package lgdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/twlk9/lgdb/keys"
)

func TestBasicIteration(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.Sync = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key10", "value10"},
		{"key20", "value20"},
		{"key30", "value30"},
	}

	for _, td := range testData {
		err := db.Put([]byte(td.key), []byte(td.value))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", td.key, err)
		}
	}
	db.Flush()

	iter := db.NewIterator()
	defer iter.Close()

	var keys []string
	var values []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
	}

	if iter.Error() != nil {
		t.Fatalf("Iterator error: %v", iter.Error())
	}

	expectedKeys := []string{"key1", "key10", "key2", "key20", "key3", "key30", "key4", "key5"}
	if !equalStringSlices(keys, expectedKeys) {
		t.Errorf("Expected keys %v, got %v", expectedKeys, keys)
	}
	expectedValues := []string{"value1", "value10", "value2", "value20", "value3", "value30", "value4", "value5"}
	if !equalStringSlices(values, expectedValues) {
		t.Errorf("Expected values %v got %v", expectedValues, values)
	}
}

func TestRangeScan(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.Sync = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key10", "value10"},
		{"key20", "value20"},
		{"key30", "value30"},
	}

	for _, td := range testData {
		err := db.Put([]byte(td.key), []byte(td.value))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", td.key, err)
		}
	}

	results, err := db.Scan([]byte("key2"), []byte("key4"))
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}

	expectedKeys := []string{"key2", "key20", "key3", "key30"}
	expectedValues := []string{"value2", "value20", "value3", "value30"}
	var actualKeys []string
	var actualValues []string
	for results.SeekToFirst(); results.Valid(); results.Next() {
		k := results.Key()
		actualKeys = append(actualKeys, string(k))
		actualValues = append(actualValues, string(results.Value()))
	}

	if !equalStringSlices(actualKeys, expectedKeys) {
		t.Errorf("Expected keys %v, got %v", expectedKeys, actualKeys)
	}
	if !equalStringSlices(actualValues, expectedValues) {
		t.Errorf("Expected values %v, got %v", expectedValues, actualValues)
	}
}

func TestPrefixScan(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.Sync = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key10", "value10"},
		{"key20", "value20"},
		{"key30", "value30"},
	}

	for _, td := range testData {
		err := db.Put([]byte(td.key), []byte(td.value))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", td.key, err)
		}
	}

	results, err := db.ScanPrefix([]byte("key"))
	if err != nil {
		t.Fatalf("PrefixScan error: %v", err)
	}

	expectedKeys := []string{"key1", "key10", "key2", "key20", "key3", "key30", "key4", "key5"}
	var actualKeys []string
	for results.SeekToFirst(); results.Valid(); results.Next() {
		k := results.Key()
		actualKeys = append(actualKeys, string(k))
	}

	if !equalStringSlices(actualKeys, expectedKeys) {
		t.Errorf("Expected keys %v, got %v", expectedKeys, actualKeys)
	}
}

func TestBoundedIterator(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.Sync = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key10", "value10"},
		{"key20", "value20"},
		{"key30", "value30"},
	}

	for _, td := range testData {
		err := db.Put([]byte(td.key), []byte(td.value))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", td.key, err)
		}
	}

	r := keys.NewRange([]byte("key2"), []byte("key4"))
	iter := db.NewIteratorWithBounds(r)
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if iter.Error() != nil {
		t.Fatalf("Iterator error: %v", iter.Error())
	}

	expectedKeys := []string{"key2", "key20", "key3", "key30"}
	if !equalStringSlices(keys, expectedKeys) {
		t.Errorf("Expected keys %v, got %v", expectedKeys, keys)
	}
}

func TestIterationAfterDeletions(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.Sync = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert test data
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keys {
		err := db.Put([]byte(key), []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Delete some keys
	err = db.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to delete key2: %v", err)
	}

	err = db.Delete([]byte("key4"))
	if err != nil {
		t.Fatalf("Failed to delete key4: %v", err)
	}

	iter := db.NewIterator()
	defer iter.Close()

	var resultKeys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		resultKeys = append(resultKeys, string(iter.Key()))
	}

	if iter.Error() != nil {
		t.Fatalf("Iterator error: %v", iter.Error())
	}

	expectedKeys := []string{"key1", "key3", "key5"}
	if !equalStringSlices(resultKeys, expectedKeys) {
		t.Errorf("Expected keys %v, got %v", expectedKeys, resultKeys)
	}
}

func TestRangeScanAfterDeletions(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.Sync = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert test data
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keys {
		err := db.Put([]byte(key), []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Delete some keys
	err = db.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to delete key2: %v", err)
	}

	err = db.Delete([]byte("key4"))
	if err != nil {
		t.Fatalf("Failed to delete key4: %v", err)
	}

	results, err := db.Scan([]byte("key1"), []byte("key5"))
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}

	expectedKeys := []string{"key1", "key3"}
	var actualKeys []string
	for results.SeekToFirst(); results.Valid(); results.Next() {
		k := results.Key()
		actualKeys = append(actualKeys, string(k))
	}

	if !equalStringSlices(actualKeys, expectedKeys) {
		t.Errorf("Expected keys %v, got %v", expectedKeys, actualKeys)
	}
}

func TestRangeQueryAcrossLevels(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 64 // Very small buffer to force flushes
	opts.Sync = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert enough data to trigger flush
	for i := range 100 {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Force flush
	err = db.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Add some more data to memtable
	for i := 100; i < 110; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		err := db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Query a range that spans memtable and SSTables
	results, err := db.Scan([]byte("key095"), []byte("key105"))
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}

	expectedKeys := []string{"key095", "key096", "key097", "key098", "key099", "key100", "key101", "key102", "key103", "key104"}
	var actualKeys []string
	for results.SeekToFirst(); results.Valid(); results.Next() {
		k := results.Key()
		actualKeys = append(actualKeys, string(k))
	}

	if !equalStringSlices(actualKeys, expectedKeys) {
		t.Errorf("Expected keys %v, got %v", expectedKeys, actualKeys)
	}
}

func TestIteratorSeek(t *testing.T) {
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 1024
	opts.Sync = false
	opts.WALSyncInterval = 0
	opts.WALMinSyncInterval = 0

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert test data
	tkeys := []string{"key1", "key3", "key5", "key7", "key9"}
	for _, key := range tkeys {
		err := db.Put([]byte(key), []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	iter := db.NewIterator()
	defer iter.Close()

	// Test seeking to existing key
	ikey := keys.NewQueryKey([]byte("key5"))
	iter.Seek(ikey)
	if !iter.Valid() {
		t.Fatalf("Expected iterator to be valid after seek")
	}
	if string(iter.Key()) != "key5" {
		t.Errorf("Expected key5, got %s", string(iter.Key()))
	}

	// Test seeking to non-existing key (should find next key)
	ikey = keys.NewQueryKey([]byte("key4"))
	iter.Seek(ikey)
	if !iter.Valid() {
		t.Fatalf("Expected iterator to be valid after seek")
	}
	if string(iter.Key()) != "key5" {
		t.Errorf("Expected key5, got %s", string(iter.Key()))
	}

	// Test seeking past all keys
	ikey = keys.NewQueryKey([]byte("key99"))
	iter.Seek(ikey)
	if iter.Valid() {
		t.Errorf("Expected iterator to be invalid after seeking past all keys")
	}
}

func TestPrefixSuccessor(t *testing.T) {
	tests := []struct {
		name     string
		prefix   []byte
		expected []byte
	}{
		{
			name:     "simple_prefix",
			prefix:   []byte("abc"),
			expected: []byte("abd"),
		},
		{
			name:     "prefix_ending_with_254",
			prefix:   []byte{0x61, 0x62, 0xFE}, // "ab\xFE"
			expected: []byte{0x61, 0x62, 0xFF}, // "ab\xFF"
		},
		{
			name:     "prefix_ending_with_255",
			prefix:   []byte{0x61, 0x62, 0xFF}, // "ab\xFF"
			expected: []byte{0x61, 0x63},       // "ac"
		},
		{
			name:     "all_255_bytes",
			prefix:   []byte{0xFF, 0xFF, 0xFF},
			expected: nil, // No successor possible
		},
		{
			name:     "single_byte_a",
			prefix:   []byte("a"),
			expected: []byte("b"),
		},
		{
			name:     "single_byte_255",
			prefix:   []byte{0xFF},
			expected: nil, // No successor possible
		},
		{
			name:     "empty_prefix",
			prefix:   []byte(""),
			expected: nil, // No successor for empty prefix
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prefixSuccessor(tt.prefix)
			if tt.expected == nil {
				if result != nil {
					t.Errorf("Expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("Expected %v, got nil", tt.expected)
				} else if string(result) != string(tt.expected) {
					t.Errorf("Expected %v, got %v", tt.expected, result)
				}
			}
		})
	}

	t.Log("prefixSuccessor test passed successfully")
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// setupCrossBoundaryTestDB creates a test database for cross-boundary tests
func setupCrossBoundaryTestDB(t *testing.T) (*DB, func()) {
	tempDir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = tempDir
	opts.WriteBufferSize = 1024 // Small buffer for controlled flushing
	opts.Sync = false

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	return db, func() { db.Close() }
}

// TestRangeQueryCrossBoundary tests range queries that span memtable and SSTable data
func TestRangeQueryCrossBoundary(t *testing.T) {
	db, cleanup := setupCrossBoundaryTestDB(t)
	defer cleanup()

	// Phase 1: Insert initial data that will go to SSTables
	sstableKeys := []string{"key001", "key003", "key005", "key007", "key009"}
	for _, key := range sstableKeys {
		err := db.Put([]byte(key), []byte("sstable_"+key))
		if err != nil {
			t.Fatalf("Failed to put SSTable key %s: %v", key, err)
		}
	}

	// Force flush to create SSTable
	err := db.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Phase 2: Insert additional data that will stay in memtable
	memtableKeys := []string{"key002", "key004", "key006", "key008", "key010"}
	for _, key := range memtableKeys {
		err := db.Put([]byte(key), []byte("memtable_"+key))
		if err != nil {
			t.Fatalf("Failed to put memtable key %s: %v", key, err)
		}
	}

	// Verify we have data in both memtable and SSTable
	// (This is just for our understanding - the test will prove the merge works)

	// Test 1: Full range iteration should merge memtable and SSTable data correctly
	t.Run("FullRangeIteration", func(t *testing.T) {
		iter := db.NewIterator()
		defer iter.Close()

		expectedKeys := []string{"key001", "key002", "key003", "key004", "key005", "key006", "key007", "key008", "key009", "key010"}
		expectedValues := []string{"sstable_key001", "memtable_key002", "sstable_key003", "memtable_key004", "sstable_key005", "memtable_key006", "sstable_key007", "memtable_key008", "sstable_key009", "memtable_key010"}

		var actualKeys []string
		var actualValues []string
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			actualKeys = append(actualKeys, string(iter.Key()))
			actualValues = append(actualValues, string(iter.Value()))
		}

		if iter.Error() != nil {
			t.Fatalf("Iterator error: %v", iter.Error())
		}

		if len(actualKeys) != len(expectedKeys) {
			t.Fatalf("Expected %d keys, got %d", len(expectedKeys), len(actualKeys))
		}

		for i, expectedKey := range expectedKeys {
			if actualKeys[i] != expectedKey {
				t.Errorf("Key[%d]: expected %s, got %s", i, expectedKey, actualKeys[i])
			}
			if actualValues[i] != expectedValues[i] {
				t.Errorf("Value[%d]: expected %s, got %s", i, expectedValues[i], actualValues[i])
			}
		}

		t.Logf("Successfully merged %d keys from memtable and SSTable", len(actualKeys))
	})

	// Test 2: Bounded range that crosses boundary
	t.Run("BoundedRangeCrossBoundary", func(t *testing.T) {
		r := keys.NewRange([]byte("key003"), []byte("key008"))
		iter := db.NewIteratorWithBounds(r)
		defer iter.Close()

		expectedKeys := []string{"key003", "key004", "key005", "key006", "key007"}
		expectedValues := []string{"sstable_key003", "memtable_key004", "sstable_key005", "memtable_key006", "sstable_key007"}

		var actualKeys []string
		var actualValues []string
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			actualKeys = append(actualKeys, string(iter.Key()))
			actualValues = append(actualValues, string(iter.Value()))
		}

		if iter.Error() != nil {
			t.Fatalf("Iterator error: %v", iter.Error())
		}

		if len(actualKeys) != len(expectedKeys) {
			t.Fatalf("Expected %d keys, got %d", len(expectedKeys), len(actualKeys))
		}

		for i, expectedKey := range expectedKeys {
			if actualKeys[i] != expectedKey {
				t.Errorf("Key[%d]: expected %s, got %s", i, expectedKey, actualKeys[i])
			}
			if actualValues[i] != expectedValues[i] {
				t.Errorf("Value[%d]: expected %s, got %s", i, expectedValues[i], actualValues[i])
			}
		}

		t.Logf("Successfully queried bounded range across memtable/SSTable boundary")
	})

	// Test 3: Updates that create duplicates across boundaries
	t.Run("UpdatesAcrossBoundary", func(t *testing.T) {
		// Update some keys that exist in SSTable (these updates stay in memtable)
		err = db.Put([]byte("key001"), []byte("updated_key001"))
		if err != nil {
			t.Fatalf("Failed to update key001: %v", err)
		}
		err = db.Put([]byte("key005"), []byte("updated_key005"))
		if err != nil {
			t.Fatalf("Failed to update key005: %v", err)
		}

		iter := db.NewIterator()
		defer iter.Close()

		// Verify that memtable updates override SSTable values
		expectedUpdates := map[string]string{
			"key001": "updated_key001",
			"key005": "updated_key005",
		}

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			value := string(iter.Value())

			if expectedValue, shouldBeUpdated := expectedUpdates[key]; shouldBeUpdated {
				if value != expectedValue {
					t.Errorf("Key %s: expected updated value %s, got %s", key, expectedValue, value)
				}
				t.Logf("Confirmed memtable value %s=%s overrides SSTable", key, value)
			}
		}

		if iter.Error() != nil {
			t.Fatalf("Iterator error: %v", iter.Error())
		}
	})

	// Test 4: Deletions across boundaries
	t.Run("DeletionsAcrossBoundary", func(t *testing.T) {
		// Delete some keys that exist in SSTable
		err = db.Delete([]byte("key003"))
		if err != nil {
			t.Fatalf("Failed to delete key003: %v", err)
		}
		err = db.Delete([]byte("key007"))
		if err != nil {
			t.Fatalf("Failed to delete key007: %v", err)
		}

		iter := db.NewIterator()
		defer iter.Close()

		var keys []string
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			keys = append(keys, string(iter.Key()))
		}

		if iter.Error() != nil {
			t.Fatalf("Iterator error: %v", iter.Error())
		}

		// Verify deleted keys don't appear
		deletedKeys := []string{"key003", "key007"}
		for _, deletedKey := range deletedKeys {
			for _, foundKey := range keys {
				if foundKey == deletedKey {
					t.Errorf("Deleted key %s still appears in iterator results", deletedKey)
				}
			}
		}

		t.Logf("Confirmed memtable deletions properly mask SSTable data")
	})
}

// TestRangeQueryMultipleSSTables tests range queries across multiple SSTable levels
func TestRangeQueryMultipleSSTables(t *testing.T) {
	db, cleanup := setupCrossBoundaryTestDB(t)
	defer cleanup()

	// Create data that will span multiple levels
	// Level 1 data
	l1Keys := []string{"key10", "key30", "key50"}
	for _, key := range l1Keys {
		err := db.Put([]byte(key), []byte("L1_"+key))
		if err != nil {
			t.Fatalf("Failed to put L1 key %s: %v", key, err)
		}
	}
	err := db.Flush()
	if err != nil {
		t.Fatalf("Failed to flush L1: %v", err)
	}

	// Level 0 data (newer)
	l0Keys := []string{"key20", "key40", "key60"}
	for _, key := range l0Keys {
		err := db.Put([]byte(key), []byte("L0_"+key))
		if err != nil {
			t.Fatalf("Failed to put L0 key %s: %v", key, err)
		}
	}
	err = db.Flush()
	if err != nil {
		t.Fatalf("Failed to flush L0: %v", err)
	}

	// Memtable data (newest)
	memKeys := []string{"key15", "key35", "key55"}
	for _, key := range memKeys {
		err := db.Put([]byte(key), []byte("MEM_"+key))
		if err != nil {
			t.Fatalf("Failed to put MEM key %s: %v", key, err)
		}
	}

	// Test iteration across all levels
	iter := db.NewIterator()
	defer iter.Close()

	expectedKeys := []string{"key10", "key15", "key20", "key30", "key35", "key40", "key50", "key55", "key60"}
	expectedSources := []string{"L1", "MEM", "L0", "L1", "MEM", "L0", "L1", "MEM", "L0"}

	var actualKeys []string
	var actualValues []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		actualKeys = append(actualKeys, string(iter.Key()))
		actualValues = append(actualValues, string(iter.Value()))
	}

	if iter.Error() != nil {
		t.Fatalf("Iterator error: %v", iter.Error())
	}

	if len(actualKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d. Keys: %v", len(expectedKeys), len(actualKeys), actualKeys)
	}

	for i, expectedKey := range expectedKeys {
		if actualKeys[i] != expectedKey {
			t.Errorf("Key[%d]: expected %s, got %s", i, expectedKey, actualKeys[i])
		}

		expectedPrefix := expectedSources[i]
		expectedValue := fmt.Sprintf("%s_%s", expectedPrefix, expectedKey)
		if actualValues[i] != expectedValue {
			t.Errorf("Value[%d]: expected %s, got %s", i, expectedValue, actualValues[i])
		}
	}

	t.Logf("Successfully merged %d keys across memtable and multiple SSTable levels", len(actualKeys))
}
