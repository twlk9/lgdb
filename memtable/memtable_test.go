package memtable

import (
	"bytes"
	"testing"

	"github.com/twlk9/lgdb/keys"
)

func TestMemTableBasicOperations(t *testing.T) {
	mt := NewMemtable(16384)

	// Test empty table
	ik := keys.NewQueryKey([]byte("nonexistent"))
	val, key := mt.Get(ik)
	if val != nil || key != nil {
		t.Errorf("Expected nil for nonexistent key, got val=%v, key=%v", val, key)
	}

	// Test single Put/Get
	userKey := []byte("test_key")
	value := []byte("test_value")
	intKey := keys.NewEncodedKey(userKey, 1, keys.KindSet)

	mt.Put(intKey, value)
	queryKey := keys.NewQueryKey(userKey)
	gotKey, gotVal := mt.Get(queryKey)
	if !bytes.Equal(gotVal, value) {
		t.Errorf("Expected value %s, got %s", value, gotVal)
	}
	if gotKey == nil || !bytes.Equal(gotKey.UserKey(), userKey) || gotKey.Seq() != 1 || gotKey.Kind() != keys.KindSet {
		t.Errorf("Expected key %+v, got %+v", intKey, gotKey)
	}
}

func TestMemTableSequenceNumberOrdering(t *testing.T) {
	mt := NewMemtable(16384)
	userKey := []byte("same_key")

	// Insert multiple versions of the same key with different sequence numbers
	oldValue := []byte("old_value")
	newValue := []byte("new_value")
	newestValue := []byte("newest_value")

	// Insert in non-sequential order to test ordering
	mt.Put(keys.NewEncodedKey(userKey, 5, keys.KindSet), oldValue)
	mt.Put(keys.NewEncodedKey(userKey, 10, keys.KindSet), newestValue)
	mt.Put(keys.NewEncodedKey(userKey, 8, keys.KindSet), newValue)

	ik := keys.NewEncodedKey(userKey, keys.MaxSequenceNumber, keys.KindSeek)

	// Should get the newest version (highest sequence number)
	gotKey, gotVal := mt.Get(ik)
	if !bytes.Equal(gotVal, newestValue) {
		t.Errorf("Expected newest value %s, got %s", newestValue, gotVal)
	}
	if gotKey == nil || gotKey.Seq() != 10 {
		t.Errorf("Expected sequence number 10, got %d", gotKey.Seq())
	}
}

func TestMemTableMultipleKeys(t *testing.T) {
	mt := NewMemtable(16384)

	// Insert multiple different keys
	keys1 := []byte("key1")
	keys2 := []byte("key2")
	keys3 := []byte("key3")

	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	mt.Put(keys.NewEncodedKey(keys2, 2, keys.KindSet), value2) // Insert out of order
	mt.Put(keys.NewEncodedKey(keys1, 1, keys.KindSet), value1)
	mt.Put(keys.NewEncodedKey(keys3, 3, keys.KindSet), value3)

	// Test each key retrieval
	ik := keys.NewQueryKey(keys1)
	key1, val1 := mt.Get(ik)
	ik2 := keys.NewQueryKey(keys2)
	key2, val2 := mt.Get(ik2)
	ik3 := keys.NewQueryKey(keys3)
	key3, val3 := mt.Get(ik3)

	if !bytes.Equal(val1, value1) || key1.Seq() != 1 {
		t.Errorf("Key1: expected %s/1, got %s/%d", value1, val1, key1.Seq())
	}
	if !bytes.Equal(val2, value2) || key2.Seq() != 2 {
		t.Errorf("Key2: expected %s/2, got %s/%d", value2, val2, key2.Seq())
	}
	if !bytes.Equal(val3, value3) || key3.Seq() != 3 {
		t.Errorf("Key3: expected %s/3, got %s/%d", value3, val3, key3.Seq())
	}
}

func TestMemTableTombstones(t *testing.T) {
	mt := NewMemtable(16384)
	userKey := []byte("deleted_key")

	// First insert a value
	mt.Put(keys.NewEncodedKey(userKey, 1, keys.KindSet), []byte("original"))

	// Then delete it (higher sequence number) - tombstone gets stored with empty value
	mt.Put(keys.NewEncodedKey(userKey, 2, keys.KindDelete), nil)

	// Should get the delete tombstone (memtable returns the raw entry)
	ik := keys.NewQueryKey(userKey)
	gotKey, gotVal := mt.Get(ik)
	if gotKey == nil || gotKey.Kind() != keys.KindDelete || gotKey.Seq() != 2 {
		t.Errorf("Expected delete tombstone with seq 2, got key=%+v", gotKey)
	}
	// The memtable returns the raw tombstone value (empty slice),
	// higher-level code interprets tombstones
	if len(gotVal) != 0 {
		t.Errorf("Expected empty value for tombstone, got %v (len=%d)", gotVal, len(gotVal))
	}
}

func TestMemTableSkiplistOrdering(t *testing.T) {
	mt := NewMemtable(16384)

	// Insert keys that should test skiplist ordering
	testCases := []struct {
		key string
		seq uint64
		val string
	}{
		{"apple", 1, "fruit1"},
		{"banana", 1, "fruit2"},
		{"apple", 2, "fruit1_updated"}, // Same key, newer sequence
		{"cherry", 1, "fruit3"},
		{"apple", 3, "fruit1_newest"}, // Same key, newest sequence
	}

	// Insert in mixed order
	insertOrder := []int{2, 0, 4, 1, 3} // Mix up the insertion order
	for _, i := range insertOrder {
		tc := testCases[i]
		mt.Put(keys.NewEncodedKey([]byte(tc.key), tc.seq, keys.KindSet), []byte(tc.val))
	}

	// Test that we get the newest version for each key
	expectedResults := map[string]struct {
		val string
		seq uint64
	}{
		"apple":  {"fruit1_newest", 3},
		"banana": {"fruit2", 1},
		"cherry": {"fruit3", 1},
	}

	for keyStr, expected := range expectedResults {
		ik := keys.NewEncodedKey([]byte(keyStr), keys.MaxSequenceNumber, keys.KindSeek)
		key, val := mt.Get(ik)
		if !bytes.Equal(val, []byte(expected.val)) {
			t.Errorf("Key %s: expected value %s, got %s", keyStr, expected.val, string(val))
		}
		if key == nil || key.Seq() != expected.seq {
			t.Errorf("Key %s: expected sequence %d, got %d", keyStr, expected.seq, key.Seq())
		}
	}
}

func TestMemTableEmptyAndSize(t *testing.T) {
	mt := NewMemtable(16384)

	// Test empty table
	if mt.Size() != 0 {
		t.Errorf("Expected size 0 for empty table, got %d", mt.Size())
	}

	// Add some data
	mt.Put(keys.NewEncodedKey([]byte("key1"), 1, keys.KindSet), []byte("value1"))
	mt.Put(keys.NewEncodedKey([]byte("key2"), 2, keys.KindSet), []byte("value2"))

	size := mt.Size()
	if size <= 0 {
		t.Errorf("Expected positive size after insertions, got %d", size)
	}
}

func TestMemTableLargeKeys(t *testing.T) {
	mt := NewMemtable(16384)

	// Test with larger keys and values
	largeKey := bytes.Repeat([]byte("x"), 100)
	largeValue := bytes.Repeat([]byte("y"), 500)

	mt.Put(keys.NewEncodedKey(largeKey, 1, keys.KindSet), largeValue)

	ik := keys.NewEncodedKey(largeKey, keys.MaxSequenceNumber, keys.KindSeek)
	key, val := mt.Get(ik)
	if !bytes.Equal(val, largeValue) {
		t.Errorf("Large value mismatch")
	}
	if key == nil || !bytes.Equal(key.UserKey(), largeKey) {
		t.Errorf("Large key mismatch")
	}
}

func TestMemTableIteratorBasic(t *testing.T) {
	mt := NewMemtable(16384)

	// Test empty iterator
	it := mt.NewIterator()
	defer it.Close()

	it.SeekToFirst()
	if it.Valid() {
		t.Errorf("Expected invalid iterator on empty table")
	}

	// Add some data
	testData := []struct {
		key string
		seq uint64
		val string
	}{
		{"apple", 1, "fruit1"},
		{"banana", 1, "fruit2"},
		{"cherry", 1, "fruit3"},
	}

	for _, td := range testData {
		mt.Put(keys.NewEncodedKey([]byte(td.key), td.seq, keys.KindSet), []byte(td.val))
	}

	// Test forward iteration
	it2 := mt.NewIterator()
	defer it2.Close()

	it2.SeekToFirst()
	i := 0
	for it2.Valid() {
		if i >= len(testData) {
			t.Errorf("Iterator returned more items than expected")
			break
		}

		userKey := it2.UserKey()
		value := it2.Value()
		expectedKey := testData[i].key
		expectedVal := testData[i].val

		if !bytes.Equal(userKey, []byte(expectedKey)) {
			t.Errorf("Expected key %s, got %s", expectedKey, string(userKey))
		}
		if !bytes.Equal(value, []byte(expectedVal)) {
			t.Errorf("Expected value %s, got %s", expectedVal, string(value))
		}

		it2.Next()
		i++
	}

	if i != len(testData) {
		t.Errorf("Expected %d items, got %d", len(testData), i)
	}
}

func TestMemTableIteratorSeek(t *testing.T) {
	mt := NewMemtable(16384)

	// Insert data out of order
	testData := []struct {
		key string
		seq uint64
		val string
	}{
		{"delta", 1, "d"},
		{"alpha", 1, "a"},
		{"gamma", 1, "g"},
		{"beta", 1, "b"},
	}

	for _, td := range testData {
		mt.Put(keys.NewEncodedKey([]byte(td.key), td.seq, keys.KindSet), []byte(td.val))
	}

	it := mt.NewIterator()
	defer it.Close()

	// Test seeking to existing key
	ik := keys.NewEncodedKey([]byte("gamma"), keys.MaxSequenceNumber, keys.KindSeek)
	it.Seek(ik)
	if !it.Valid() {
		t.Errorf("Expected valid iterator after seeking to 'gamma'")
	}
	if !bytes.Equal(it.UserKey(), []byte("gamma")) {
		t.Errorf("Expected key 'gamma', got %s", string(it.UserKey()))
	}

	// Test seeking to non-existing key (should find next)
	ik = keys.NewEncodedKey([]byte("charlie"), keys.MaxSequenceNumber, keys.KindSeek)
	it.Seek(ik) // Should find "delta"
	if !it.Valid() {
		t.Errorf("Expected valid iterator after seeking to 'charlie'")
	}
	if !bytes.Equal(it.UserKey(), []byte("delta")) {
		t.Errorf("Expected key 'delta', got %s", string(it.UserKey()))
	}

	// Test seeking past end
	ik = keys.NewEncodedKey([]byte("zulu"), keys.MaxSequenceNumber, keys.KindSeek)
	it.Seek(ik)
	if it.Valid() {
		t.Errorf("Expected invalid iterator after seeking past end")
	}
}

func TestMemTableIteratorSequenceNumbers(t *testing.T) {
	mt := NewMemtable(16384)

	// Add multiple versions of the same key
	key := []byte("test")
	mt.Put(keys.NewEncodedKey(key, 1, keys.KindSet), []byte("v1"))
	mt.Put(keys.NewEncodedKey(key, 3, keys.KindSet), []byte("v3"))
	mt.Put(keys.NewEncodedKey(key, 2, keys.KindSet), []byte("v2"))

	it := mt.NewIterator()
	defer it.Close()

	// Iterator should see all versions in internal key order (highest seq first)
	it.SeekToFirst()

	expectedVersions := []struct {
		seq uint64
		val string
	}{
		{3, "v3"}, // Highest sequence first
		{2, "v2"},
		{1, "v1"}, // Lowest sequence last
	}

	i := 0
	for it.Valid() {
		if i >= len(expectedVersions) {
			t.Errorf("Too many versions returned")
			break
		}

		intKey := it.Key()
		value := it.Value()

		if intKey.Seq() != expectedVersions[i].seq {
			t.Errorf("Expected seq %d, got %d", expectedVersions[i].seq, intKey.Seq())
		}
		if !bytes.Equal(value, []byte(expectedVersions[i].val)) {
			t.Errorf("Expected value %s, got %s", expectedVersions[i].val, string(value))
		}

		it.Next()
		i++
	}

	if i != len(expectedVersions) {
		t.Errorf("Expected %d versions, got %d", len(expectedVersions), i)
	}
}

func TestMemTableIteratorBounds(t *testing.T) {
	mt := NewMemtable(16384)

	// Add test data
	testKeys := []string{"aa", "bb", "cc", "dd", "ee"}
	for i, key := range testKeys {
		mt.Put(keys.NewEncodedKey([]byte(key), uint64(i+1), keys.KindSet), []byte("value"+key))
	}

	sik := keys.NewQueryKey([]byte("bb"))
	lik := keys.NewQueryKey([]byte("dd"))

	// Test with bounds [bb, dd)
	bounds := &keys.Range{
		Start: sik,
		Limit: lik,
	}

	it := mt.NewIteratorWithBounds(bounds)
	defer it.Close()

	it.SeekToFirst()
	seenKeys := []string{}
	for it.Valid() {
		seenKeys = append(seenKeys, string(it.UserKey()))
		it.Next()
	}

	expectedKeys := []string{"bb", "cc"} // [bb, dd) should include bb, cc but not dd
	if len(seenKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d: %v", len(expectedKeys), len(seenKeys), seenKeys)
	}
	for i, expected := range expectedKeys {
		if i < len(seenKeys) && seenKeys[i] != expected {
			t.Errorf("Expected key %s, got %s", expected, seenKeys[i])
		}
	}
}

func TestMemTableIteratorConcurrentSafety(t *testing.T) {
	mt := NewMemtable(16384)

	// Add initial data
	for i := range 100 {
		key := []byte("key" + string(rune('0'+i%10)))
		mt.Put(keys.NewEncodedKey(key, uint64(i), keys.KindSet), []byte("value"+string(rune('0'+i%10))))
	}

	// Create iterator
	it := mt.NewIterator()
	defer it.Close()

	// Start concurrent writes in a goroutine
	done := make(chan bool)
	go func() {
		for i := 100; i < 200; i++ {
			key := []byte("newkey" + string(rune('0'+i%10)))
			mt.Put(keys.NewEncodedKey(key, uint64(i), keys.KindSet), []byte("newvalue"+string(rune('0'+i%10))))
		}
		done <- true
	}()

	// Iterate while writes are happening
	it.SeekToFirst()
	count := 0
	for it.Valid() {
		// Access key/value to ensure no race conditions
		userKey := it.UserKey()
		value := it.Value()
		intKey := it.Key()

		// Verify data consistency
		if userKey == nil || value == nil || intKey == nil {
			t.Errorf("Nil data during concurrent iteration")
		}

		count++
		it.Next()
	}

	// Wait for writes to complete
	<-done

	// Iterator should have seen original data only
	if count < 100 {
		t.Errorf("Expected 100 entries during concurrent iteration, got %d", count)
	}

	// Verify no errors occurred
	if err := it.Error(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}
}

// TestMemTableKeyOrderingWithManyKeys tests the specific issue found where
// key 'a' was not found when 26 keys a-z were inserted
func TestMemTableKeyOrderingWithManyKeys(t *testing.T) {
	mt := NewMemtable(1024 * 1024) // 1MB capacity

	// Insert all keys a-z like the failing test
	var testKeys []string
	for i := 'a'; i <= 'z'; i++ {
		testKeys = append(testKeys, string(rune(i)))
	}

	// Put all keys with increasing sequence numbers
	for i, keyStr := range testKeys {
		key := []byte(keyStr)
		value := []byte("value_" + keyStr)

		// Create internal key
		ikey := keys.NewEncodedKey(key, uint64(i+1), keys.KindSet)

		// Put in memtable
		mt.Put(ikey, value)
		t.Logf("Put key %s with seq %d", keyStr, i+1)
	}

	// Check that the keys are properly ordered by using the iterator
	iter := mt.NewIterator()
	defer iter.Close()

	var iteratorKeys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()
		iteratorKeys = append(iteratorKeys, string(iterKey.UserKey()))
	}

	t.Logf("Keys in memtable order: %v", iteratorKeys)

	// Verify that keys are sorted lexicographically by user key
	for i := 1; i < len(iteratorKeys); i++ {
		if iteratorKeys[i-1] > iteratorKeys[i] {
			t.Errorf("Keys not properly sorted: %s should come before %s", iteratorKeys[i], iteratorKeys[i-1])
		}
	}

	// Now verify that Get works for all keys
	for _, keyStr := range testKeys {
		key := []byte(keyStr)
		searchKey := keys.NewQueryKey(key)
		foundKey, foundValue := mt.Get(searchKey)

		if foundKey == nil {
			t.Fatalf("Key %s should exist in memtable", keyStr)
		}

		expectedValue := "value_" + keyStr
		if string(foundValue) != expectedValue {
			t.Fatalf("Key %s value mismatch: expected %s, got %s", keyStr, expectedValue, string(foundValue))
		}

		t.Logf("Successfully retrieved key %s = %s", keyStr, string(foundValue))
	}
}

// TestMemTableComparisonFunctions tests the key comparison functions directly
func TestMemTableComparisonFunctions(t *testing.T) {
	// Test basic InternalKey comparison
	key1 := keys.NewEncodedKey([]byte("a"), 1, keys.KindSet)
	key2 := keys.NewEncodedKey([]byte("b"), 1, keys.KindSet)
	key3 := keys.NewEncodedKey([]byte("a"), 2, keys.KindSet) // Same user key, higher seq

	// Test user key ordering (a < b)
	if key1.Compare(key2) >= 0 {
		t.Errorf("Key 'a' should be less than key 'b'")
	}

	// Test sequence number ordering (higher seq should come first for same user key)
	if key3.Compare(key1) >= 0 {
		t.Errorf("Key with seq 2 should come before key with seq 1")
	}
}

// TestMemTableStepByStepInsertion tests insertion step by step to find where ordering breaks
func TestMemTableStepByStepInsertion(t *testing.T) {
	mt := NewMemtable(1024 * 1024)

	// Insert all keys a-z to find exactly when the ordering breaks
	var problemKeys []string
	for i := 'a'; i <= 'z'; i++ {
		problemKeys = append(problemKeys, string(rune(i)))
	}

	for i, keyStr := range problemKeys {
		key := []byte(keyStr)
		value := []byte("value_" + keyStr)
		ikey := keys.NewEncodedKey(key, uint64(i+1), keys.KindSet)

		t.Logf("Before inserting key %s (seq %d):", keyStr, i+1)

		// Show current state
		iter := mt.NewIterator()
		var currentKeys []string
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			currentKeys = append(currentKeys, string(iter.Key().UserKey()))
		}
		iter.Close()
		t.Logf("  Current keys: %v", currentKeys)

		// Insert the key
		mt.Put(ikey, value)
		t.Logf("Inserted key %s", keyStr)

		// Show state after insertion
		iter2 := mt.NewIterator()
		var newKeys []string
		for iter2.SeekToFirst(); iter2.Valid(); iter2.Next() {
			newKeys = append(newKeys, string(iter2.Key().UserKey()))
		}
		iter2.Close()
		t.Logf("  After insertion: %v", newKeys)

		// Verify this key can be retrieved
		searchKey := keys.NewQueryKey(key)
		foundKey, foundValue := mt.Get(searchKey)
		if foundKey == nil {
			t.Fatalf("Key %s should be retrievable immediately after insertion", keyStr)
		}
		if string(foundValue) != "value_"+keyStr {
			t.Fatalf("Key %s value mismatch after insertion", keyStr)
		}
		t.Logf("  Get verification: OK")

		// Verify that all previously inserted keys are still retrievable
		for j := 0; j <= i; j++ {
			testKey := problemKeys[j]
			searchKey = keys.NewQueryKey([]byte(testKey))
			foundKey, foundValue := mt.Get(searchKey)
			if foundKey == nil {
				t.Fatalf("Previously inserted key %s is no longer retrievable after inserting %s", testKey, keyStr)
			}
			if string(foundValue) != "value_"+testKey {
				t.Fatalf("Previously inserted key %s value corrupted after inserting %s", testKey, keyStr)
			}
		}
		t.Logf("  All previous keys still retrievable: OK")
		t.Logf("")
	}
}
