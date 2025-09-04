//go:build integration || stress
// +build integration stress

package lgdb

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/twlk9/lgdb/keys"
)

// TestIteratorEdgeCasesEmpty tests iterator behavior with empty database
func TestIteratorEdgeCasesEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test empty database iteration
	iter := db.NewIterator()
	defer iter.Close()

	// All positioning should result in invalid iterator
	testCases := []struct {
		name string
		fn   func()
	}{
		{"SeekToFirst", func() { iter.SeekToFirst() }},
		{"Seek", func() { iter.Seek(keys.NewQueryKey([]byte("any_key"))) }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn()
			if iter.Valid() {
				t.Errorf("%s on empty DB should result in invalid iterator", tc.name)
			}
			if iter.Key() != nil {
				t.Errorf("%s on empty DB should return nil key", tc.name)
			}
			if iter.Value() != nil {
				t.Errorf("%s on empty DB should return nil value", tc.name)
			}
		})
	}
}

// TestIteratorEdgeCasesSingleKey tests iterator with single key
func TestIteratorEdgeCasesSingleKey(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert single key
	key := []byte("single_key")
	value := []byte("single_value")
	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	iter := db.NewIterator()
	defer iter.Close()

	// Test SeekToFirst
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("SeekToFirst should be valid with single key")
	}
	if !bytes.Equal(iter.Key(), key) {
		t.Errorf("SeekToFirst key mismatch: expected %q, got %q", key, iter.Key())
	}
	if !bytes.Equal(iter.Value(), value) {
		t.Errorf("SeekToFirst value mismatch: expected %q, got %q", value, iter.Value())
	}

	// Next should make it invalid
	iter.Next()
	if iter.Valid() {
		t.Error("Next after single key should be invalid")
	}

	// Test seeking to the key
	iter.Seek(keys.NewQueryKey(key))
	if !iter.Valid() {
		t.Error("Seek should be valid with single key")
	}
	if !bytes.Equal(iter.Key(), key) {
		t.Errorf("Seek key mismatch: expected %q, got %q", key, iter.Key())
	}
}

// TestIteratorSeekEdgeCases tests various seek scenarios
func TestIteratorSeekEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert keys: "b", "d", "f", "h", "j"
	testKeys := []string{"b", "d", "f", "h", "j"}
	for _, k := range testKeys {
		err = db.Put([]byte(k), []byte("value_"+k))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", k, err)
		}
	}

	iter := db.NewIterator()
	defer iter.Close()

	seekTests := []struct {
		name          string
		seekKey       string
		expectedKey   string
		shouldBeValid bool
	}{
		{"seek_exact_first", "b", "b", true},
		{"seek_exact_middle", "f", "f", true},
		{"seek_exact_last", "j", "j", true},
		{"seek_before_first", "a", "b", true},
		{"seek_between_b_d", "c", "d", true},
		{"seek_between_d_f", "e", "f", true},
		{"seek_between_h_j", "i", "j", true},
		{"seek_after_last", "k", "", false},
		{"seek_empty_key", "", "b", true},
	}

	for _, test := range seekTests {
		t.Run(test.name, func(t *testing.T) {
			iter.Seek(keys.NewQueryKey([]byte(test.seekKey)))

			if test.shouldBeValid {
				if !iter.Valid() {
					t.Errorf("Seek(%q) should be valid", test.seekKey)
					return
				}
				if string(iter.Key()) != test.expectedKey {
					t.Errorf("Seek(%q) expected key %q, got %q",
						test.seekKey, test.expectedKey, iter.Key())
				}
			} else {
				if iter.Valid() {
					t.Errorf("Seek(%q) should be invalid, but got key %q",
						test.seekKey, iter.Key())
				}
			}
		})
	}
}

// TestIteratorBoundaryEdgeCases tests iterator with range boundaries
func TestIteratorBoundaryEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert keys: "aa", "ab", "ac", "ba", "bb", "bc", "ca", "cb", "cc"
	prefixes := []string{"a", "b", "c"}
	suffixes := []string{"a", "b", "c"}
	var allKeys []string

	for _, prefix := range prefixes {
		for _, suffix := range suffixes {
			key := prefix + suffix
			allKeys = append(allKeys, key)
			err = db.Put([]byte(key), []byte("value_"+key))
			if err != nil {
				t.Fatalf("Failed to put key %s: %v", key, err)
			}
		}
	}

	sort.Strings(allKeys) // Ensure sorted order

	// Test range iterations with different boundaries
	rangeTests := []struct {
		name     string
		start    keys.EncodedKey
		limit    keys.EncodedKey
		expected []string
	}{
		{"full_range", nil, nil, allKeys},
		{"prefix_a", keys.NewQueryKey([]byte("a")), keys.NewQueryKey([]byte("b")), []string{"aa", "ab", "ac"}},
		{"prefix_b", keys.NewQueryKey([]byte("b")), keys.NewQueryKey([]byte("c")), []string{"ba", "bb", "bc"}},
		{"prefix_c", keys.NewQueryKey([]byte("c")), keys.NewQueryKey([]byte("d")), []string{"ca", "cb", "cc"}},
		{"single_key", keys.NewQueryKey([]byte("ab")), keys.NewQueryKey([]byte("ac")), []string{"ab"}},
		{"empty_range", keys.NewQueryKey([]byte("az")), keys.NewQueryKey([]byte("b")), []string{}},
		{"boundary_exact", keys.NewQueryKey([]byte("ab")), keys.NewQueryKey([]byte("bb")), []string{"ab", "ac", "ba"}},
	}

	for _, test := range rangeTests {
		t.Run(test.name, func(t *testing.T) {
			var iter *DBIterator
			if test.start != nil || test.limit != nil {
				rng := &keys.Range{Start: test.start, Limit: test.limit}
				iter = db.NewIteratorWithBounds(rng)
			} else {
				iter = db.NewIterator()
			}
			defer iter.Close()

			var actualKeys []string
			iter.SeekToFirst()
			for iter.Valid() {
				actualKeys = append(actualKeys, string(iter.Key()))
				iter.Next()
			}

			if len(actualKeys) != len(test.expected) {
				t.Errorf("Range %s: expected %d keys, got %d\nExpected: %v\nActual: %v",
					test.name, len(test.expected), len(actualKeys), test.expected, actualKeys)
				return
			}

			for i, expected := range test.expected {
				if actualKeys[i] != expected {
					t.Errorf("Range %s: key[%d] expected %q, got %q",
						test.name, i, expected, actualKeys[i])
				}
			}
		})
	}
}

// TestIteratorForwardBoundaryEdgeCases tests forward iteration edge cases
func TestIteratorForwardBoundaryEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert numeric keys for easier testing
	testKeys := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}
	for _, k := range testKeys {
		err = db.Put([]byte(k), []byte("value_"+k))
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", k, err)
		}
	}

	iter := db.NewIterator()
	defer iter.Close()

	// Test forward iteration from first to last
	var forwardKeys []string
	iter.SeekToFirst()
	for iter.Valid() {
		forwardKeys = append(forwardKeys, string(iter.Key()))
		iter.Next()
	}

	// Should have all keys in forward order
	if len(forwardKeys) != len(testKeys) {
		t.Errorf("Forward iteration: expected %d keys, got %d", len(testKeys), len(forwardKeys))
	}

	for i, expected := range testKeys {
		if i < len(forwardKeys) && forwardKeys[i] != expected {
			t.Errorf("Forward iteration: key[%d] expected %q, got %q", i, expected, forwardKeys[i])
		}
	}

	// Test seeking to various positions
	iter.Seek(keys.NewQueryKey([]byte("05")))
	if !iter.Valid() || string(iter.Key()) != "05" {
		t.Error("Seek to '05' should position at that key")
	}

	// Continue forward from seek position
	var keysFromSeek []string
	for iter.Valid() {
		keysFromSeek = append(keysFromSeek, string(iter.Key()))
		iter.Next()
	}

	expectedFromSeek := []string{"05", "06", "07", "08", "09", "10"}
	if len(keysFromSeek) != len(expectedFromSeek) {
		t.Errorf("Forward from seek: expected %d keys, got %d", len(expectedFromSeek), len(keysFromSeek))
	}

	for i, expected := range expectedFromSeek {
		if i < len(keysFromSeek) && keysFromSeek[i] != expected {
			t.Errorf("Forward from seek: key[%d] expected %q, got %q", i, expected, keysFromSeek[i])
		}
	}
}

// TestIteratorSpecialCharacterKeys tests iterator with special character keys
func TestIteratorSpecialCharacterKeys(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test keys with special characters and different byte values
	// Note: Empty keys are not allowed in lgdb by design
	specialKeys := []string{
		"\x00", // null byte
		"\x01", // control character
		"\xFF", // max byte value
		"key with spaces",
		"key\nwith\nnewlines",
		"key\twith\ttabs",
		"key!@#$%^&*()",
		"キー",           // Unicode
		"\x80\x81\x82", // High bit set bytes
	}

	// Sort keys for expected iteration order
	expectedKeys := make([]string, len(specialKeys))
	copy(expectedKeys, specialKeys)
	sort.Strings(expectedKeys)

	// Put all keys
	for i, key := range specialKeys {
		value := fmt.Sprintf("value_%d", i)
		err = db.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put special key %q: %v", key, err)
		}
	}

	// Test iteration order
	iter := db.NewIterator()
	defer iter.Close()

	var actualKeys []string
	iter.SeekToFirst()
	for iter.Valid() {
		actualKeys = append(actualKeys, string(iter.Key()))
		iter.Next()
	}

	if len(actualKeys) != len(expectedKeys) {
		t.Errorf("Special chars: expected %d keys, got %d", len(expectedKeys), len(actualKeys))
	}

	for i, expected := range expectedKeys {
		if i < len(actualKeys) && actualKeys[i] != expected {
			t.Errorf("Special chars: key[%d] expected %q, got %q", i, expected, actualKeys[i])
		}
	}

	// Test seeking to special characters
	for _, key := range specialKeys {
		iter.Seek(keys.NewQueryKey([]byte(key)))
		if !iter.Valid() {
			t.Errorf("Seek to special key %q should be valid", key)
			continue
		}
		if string(iter.Key()) != key {
			t.Errorf("Seek to special key %q: expected %q, got %q", key, key, iter.Key())
		}
	}
}

// TestIteratorLargeKeys tests iterator with large keys
func TestIteratorLargeKeys(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create keys of various large sizes
	keySizes := []int{100, 1000, 4000, 8000}
	var keys []string

	for i, size := range keySizes {
		// Create a key with predictable pattern for sorting
		keyPattern := fmt.Sprintf("large_key_%02d_", i)
		remaining := size - len(keyPattern)
		if remaining > 0 {
			padding := make([]byte, remaining)
			for j := range padding {
				padding[j] = byte('a' + (j % 26))
			}
			keyPattern += string(padding)
		}
		keys = append(keys, keyPattern)

		value := fmt.Sprintf("value_for_large_key_%d", i)
		err = db.Put([]byte(keyPattern), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put large key %d: %v", i, err)
		}
	}

	// Test iteration with large keys
	iter := db.NewIterator()
	defer iter.Close()

	iter.SeekToFirst()
	for i := 0; iter.Valid() && i < len(keys); i++ {
		if string(iter.Key()) != keys[i] {
			t.Errorf("Large key iteration: expected key %d to be %q, got %q",
				i, keys[i], iter.Key())
		}
		iter.Next()
	}
}

// TestIteratorConcurrentModifications tests iterator behavior during concurrent modifications
func TestIteratorConcurrentModifications(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 4096

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert initial data
	initialKeys := []string{"key01", "key02", "key03", "key04", "key05"}
	for _, key := range initialKeys {
		err = db.Put([]byte(key), []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to put initial key %s: %v", key, err)
		}
	}

	// Create iterator
	iter := db.NewIterator()
	defer iter.Close()

	// Collect initial snapshot
	originalData := make(map[string][]byte)
	iter.SeekToFirst()
	for iter.Valid() {
		key := string(bytes.Clone(iter.Key()))
		value := bytes.Clone(iter.Value())
		originalData[key] = value
		iter.Next()
	}

	// Modify database (add new keys, delete existing keys)
	err = db.Put([]byte("key00"), []byte("new_key_before"))
	if err != nil {
		t.Fatalf("Failed to add new key: %v", err)
	}

	err = db.Put([]byte("key06"), []byte("new_key_after"))
	if err != nil {
		t.Fatalf("Failed to add new key: %v", err)
	}

	err = db.Delete([]byte("key02"))
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	err = db.Put([]byte("key03"), []byte("updated_value_key03"))
	if err != nil {
		t.Fatalf("Failed to update key: %v", err)
	}

	// Iterator should still see original snapshot
	currentData := make(map[string][]byte)
	iter.SeekToFirst()
	for iter.Valid() {
		key := string(bytes.Clone(iter.Key()))
		value := bytes.Clone(iter.Value())
		currentData[key] = value
		iter.Next()
	}

	// Verify iterator consistency (should see original data)
	if len(originalData) != len(currentData) {
		t.Errorf("Iterator data count changed during concurrent modifications: original %d, current %d",
			len(originalData), len(currentData))
	}

	for key, originalValue := range originalData {
		if currentValue, exists := currentData[key]; !exists {
			t.Errorf("Iterator lost key %q during concurrent modifications", key)
		} else if !bytes.Equal(originalValue, currentValue) {
			t.Errorf("Iterator key %q value changed: original %q, current %q",
				key, originalValue, currentValue)
		}
	}

	// New iterator should see updated state
	newIter := db.NewIterator()
	defer newIter.Close()

	newData := make(map[string][]byte)
	newIter.SeekToFirst()
	for newIter.Valid() {
		key := string(bytes.Clone(newIter.Key()))
		value := bytes.Clone(newIter.Value())
		newData[key] = value
		newIter.Next()
	}

	// Verify new iterator sees modifications
	expectedNewKeys := []string{"key00", "key01", "key03", "key04", "key05", "key06"}
	if len(newData) != len(expectedNewKeys) {
		t.Errorf("New iterator: expected %d keys, got %d", len(expectedNewKeys), len(newData))
	}

	// Verify key02 is gone and key03 is updated
	if _, exists := newData["key02"]; exists {
		t.Error("New iterator should not see deleted key02")
	}

	if value, exists := newData["key03"]; !exists {
		t.Error("New iterator should see updated key03")
	} else if string(value) != "updated_value_key03" {
		t.Errorf("New iterator key03: expected updated value, got %q", value)
	}
}
