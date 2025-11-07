package lgdb

import (
	"fmt"
	"testing"

	"github.com/twlk9/lgdb/keys"
)

func TestBasicRangeDelete(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = dir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write some keys
	for i := range 10 {
		key := []byte{byte(i)}
		value := []byte{byte(i + 100)}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Verify they exist
	for i := range 10 {
		key := []byte{byte(i)}
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed for key %d: %v", i, err)
		}
		if val[0] != byte(i+100) {
			t.Fatalf("Wrong value for key %d: got %d, want %d", i, val[0], i+100)
		}
	}

	// Delete range [2, 7)
	err = db.DeleteRange(keys.UserKey([]byte{2}), keys.UserKey([]byte{7}))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Keys 2-6 should be deleted, 0-1 and 7-9 should still exist
	for i := range 10 {
		key := []byte{byte(i)}
		val, err := db.Get(key)

		if i >= 2 && i < 7 {
			// Should be deleted
			if err != ErrNotFound {
				t.Errorf("Key %d should be deleted but got: val=%v err=%v", i, val, err)
			}
		} else {
			// Should exist
			if err != nil {
				t.Errorf("Key %d should exist but got error: %v", i, err)
			} else if val[0] != byte(i+100) {
				t.Errorf("Wrong value for key %d: got %d, want %d", i, val[0], i+100)
			}
		}
	}
}

func TestRangeDeleteWithIteration(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = dir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write keys a-z
	for i := byte('a'); i <= byte('z'); i++ {
		key := []byte{i}
		value := []byte{i}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete range [d, q)
	err = db.DeleteRange(keys.UserKey([]byte{'d'}), keys.UserKey([]byte{'q'}))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Iterate and collect keys
	iter := db.NewIterator(nil)
	defer iter.Close()

	var foundKeys []byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		foundKeys = append(foundKeys, iter.Key()[0])
	}

	// Should have: a, b, c, q, r, s, t, u, v, w, x, y, z
	// NOT d-p
	expectedKeys := []byte{'a', 'b', 'c', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'}

	if len(foundKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d: %v", len(expectedKeys), len(foundKeys), foundKeys)
	}

	for i, expected := range expectedKeys {
		if i >= len(foundKeys) {
			t.Errorf("Missing key %c at position %d", expected, i)
			continue
		}
		if foundKeys[i] != expected {
			t.Errorf("Position %d: expected %c, got %c", i, expected, foundKeys[i])
		}
	}
}

func TestRangeDeletePersistence(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = dir

	// First session: write keys and delete range
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	for i := range 20 {
		key := fmt.Appendf(nil, "key%02d", i)
		value := fmt.Appendf(nil, "value%02d", i)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete range [key05, key15)
	err = db.DeleteRange(keys.UserKey([]byte("key05")), keys.UserKey([]byte("key15")))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	db.Close()

	// Second session: reopen and verify range delete persisted
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db.Close()

	for i := range 20 {
		key := fmt.Appendf(nil, "key%02d", i)
		val, err := db.Get(key)

		if i >= 5 && i < 15 {
			// Should be deleted
			if err != ErrNotFound {
				t.Errorf("Key key%02d should be deleted after reopen but got: val=%v err=%v", i, val, err)
			}
		} else {
			// Should exist
			if err != nil {
				t.Errorf("Key key%02d should exist after reopen but got error: %v", i, err)
			}
		}
	}
}

func TestMultipleRangeDeletes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = dir

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write keys 0-99
	for i := range 100 {
		key := fmt.Appendf(nil, "k%03d", i)
		value := fmt.Appendf(nil, "v%03d", i)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete multiple ranges
	// Delete [10, 20)
	err = db.DeleteRange(keys.UserKey([]byte("k010")), keys.UserKey([]byte("k020")))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Delete [50, 60)
	err = db.DeleteRange(keys.UserKey([]byte("k050")), keys.UserKey([]byte("k060")))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Delete [80, 90)
	err = db.DeleteRange(keys.UserKey([]byte("k080")), keys.UserKey([]byte("k090")))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Verify deletions
	for i := range 100 {
		key := fmt.Appendf(nil, "k%03d", i)
		val, err := db.Get(key)

		shouldBeDeleted := (i >= 10 && i < 20) || (i >= 50 && i < 60) || (i >= 80 && i < 90)

		if shouldBeDeleted {
			if err != ErrNotFound {
				t.Errorf("Key k%03d should be deleted but got: val=%v err=%v", i, val, err)
			}
		} else {
			if err != nil {
				t.Errorf("Key k%03d should exist but got error: %v", i, err)
			}
		}
	}
}

func TestRangeDeleteWithCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.Path = dir
	opts.WriteBufferSize = 1024 * 10 // Small buffer to force flushes

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write enough data to trigger flush and compaction
	for i := range 500 {
		key := fmt.Appendf(nil, "key%04d", i)
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Wait for background work
	db.WaitForCompaction()

	// Delete range in middle
	err = db.DeleteRange(keys.UserKey([]byte("key0200")), keys.UserKey([]byte("key0300")))
	if err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Force more compaction
	for i := 500; i < 700; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		value := make([]byte, 100)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	db.WaitForCompaction()

	// Verify range delete still applies after compaction
	for i := 200; i < 300; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		val, err := db.Get(key)
		if err != ErrNotFound {
			t.Errorf("Key key%04d should be deleted after compaction but got: val=%v err=%v", i, val, err)
		}
	}

	// Verify keys outside range still exist
	for i := 100; i < 200; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		_, err := db.Get(key)
		if err != nil {
			t.Errorf("Key key%04d should exist after compaction but got error: %v", i, err)
		}
	}
}
