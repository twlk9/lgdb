package sstable

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/twlk9/lgdb/compression"
	"github.com/twlk9/lgdb/keys"
)

func TestSSTableBasicWriteRead(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	sstablePath := filepath.Join(tmpDir, "test.sst")

	// Test data
	testData := []struct {
		key   string
		value string
		seq   uint64
	}{
		{"key1", "value1", 1},
		{"key2", "value2", 2},
		{"key3", "value3", 3},
	}

	// Write SSTable
	opts := SSTableOpts{Path: sstablePath, Compression: compression.DefaultConfig()}
	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	for _, item := range testData {
		encodedKey := keys.NewEncodedKey([]byte(item.key), item.seq, keys.KindSet)
		err = writer.Add(encodedKey, []byte(item.value))
		if err != nil {
			t.Fatalf("Failed to add key-value pair: %v", err)
		}
	}

	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close SSTable: %v", err)
	}

	// Read SSTable
	reader, err := NewSSTableReader(sstablePath, 0, nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})))
	if err != nil {
		t.Fatalf("Failed to create SSTable reader: %v", err)
	}
	defer reader.Close()

	// Test reading each key-value pair using iterator
	for _, item := range testData {
		iter := reader.NewIterator()

		// Create a seek key with max sequence number to find the most recent version
		seekKey := keys.NewEncodedKey([]byte(item.key), keys.MaxSequenceNumber, keys.KindSet)
		iter.Seek(seekKey)

		if !iter.Valid() {
			iter.Close()
			t.Fatalf("Key %s not found", item.key)
		}

		foundKey := iter.Key()
		if !bytes.Equal(foundKey.UserKey(), []byte(item.key)) {
			iter.Close()
			t.Fatalf("Key %s not found", item.key)
		}

		if foundKey.Kind() == keys.KindDelete {
			iter.Close()
			t.Fatalf("Key %s was deleted", item.key)
		}

		value := iter.Value()
		if string(value) != item.value {
			iter.Close()
			t.Fatalf("Expected value %s, got %s", item.value, string(value))
		}
		iter.Close()
	}

	// Test reading non-existent key using iterator
	iter := reader.NewIterator()
	seekKey := keys.NewQueryKey([]byte("nonexistent"))
	iter.Seek(seekKey)

	found := false
	if iter.Valid() {
		foundKey := iter.Key()
		if bytes.Equal(foundKey.UserKey(), []byte("nonexistent")) {
			found = true
		}
	}
	iter.Close()

	if found {
		t.Fatalf("Non-existent key should not be found")
	}
}

func TestSSTableKeyRange(t *testing.T) {
	tmpDir := t.TempDir()

	sstablePath := filepath.Join(tmpDir, "test.sst")

	// Write SSTable with ordered keys
	opts := SSTableOpts{Path: sstablePath, Compression: compression.DefaultConfig()}
	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	tKeys := []string{"apple", "banana", "cherry", "date"}
	for i, key := range tKeys {
		encodedKey := keys.NewEncodedKey([]byte(key), uint64(i+1), keys.KindSet)
		err = writer.Add(encodedKey, []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to add key %s: %v", key, err)
		}
	}

	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close SSTable: %v", err)
	}

	// Read and verify key range
	reader, err := NewSSTableReader(sstablePath, 0, nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})))
	if err != nil {
		t.Fatalf("Failed to create SSTable reader: %v", err)
	}
	defer reader.Close()

	// Decode the smallest and largest keys to get the user keys
	smallestInternalKey := reader.SmallestKey()
	if err != nil {
		t.Fatalf("Failed to decode smallest key: %v", err)
	}
	largestInternalKey := reader.LargestKey()
	if err != nil {
		t.Fatalf("Failed to decode largest key: %v", err)
	}

	smallestUserKey := string(smallestInternalKey.UserKey())
	largestUserKey := string(largestInternalKey.UserKey())

	if smallestUserKey != "apple" {
		t.Fatalf("Expected smallest key %#v, got %#v", "apple", smallestUserKey)
	}
	if largestUserKey != "date" {
		t.Fatalf("Expected largest key 'date', got '%s'", largestUserKey)
	}
}

func TestSSTableMultipleBlocks(t *testing.T) {
	tmpDir := t.TempDir()

	sstablePath := filepath.Join(tmpDir, "test.sst")

	// Write SSTable with enough data to create multiple blocks
	opts := SSTableOpts{Path: sstablePath, Compression: compression.DefaultConfig()}
	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	// Create large values to force multiple blocks
	largeValue := make([]byte, 1000) // 1KB value
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	numKeys := 10
	for i := range numKeys {
		key := fmt.Sprintf("key%04d", i)
		encodedKey := keys.NewEncodedKey([]byte(key), uint64(i+1), keys.KindSet)
		err = writer.Add(encodedKey, largeValue)
		if err != nil {
			t.Fatalf("Failed to add key %s: %v", key, err)
		}
	}

	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close SSTable: %v", err)
	}

	// Read and verify all keys
	reader, err := NewSSTableReader(sstablePath, 0, nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})))
	if err != nil {
		t.Fatalf("Failed to create SSTable reader: %v", err)
	}
	defer reader.Close()

	for i := range numKeys {
		key := fmt.Sprintf("key%04d", i)

		// Use iterator to find the key
		iter := reader.NewIterator()
		seekKey := keys.NewQueryKey([]byte(key))
		iter.Seek(seekKey)

		if !iter.Valid() {
			iter.Close()
			t.Fatalf("Key %s not found", key)
		}

		foundKey := iter.Key()
		if !bytes.Equal(foundKey.UserKey(), []byte(key)) {
			iter.Close()
			t.Fatalf("Key %s not found", key)
		}

		if foundKey.Kind() == keys.KindDelete {
			iter.Close()
			t.Fatalf("Key %s was deleted", key)
		}

		value := iter.Value()
		if len(value) != len(largeValue) {
			iter.Close()
			t.Fatalf("Value length mismatch for key %s: expected %d, got %d", key, len(largeValue), len(value))
		}
		iter.Close()
		// Verify value content
		for j := range largeValue {
			if value[j] != largeValue[j] {
				t.Fatalf("Value content mismatch for key %s at position %d", key, j)
			}
		}
	}
}

func TestSSTableWriterStats(t *testing.T) {
	tmpDir := t.TempDir()

	sstablePath := filepath.Join(tmpDir, "test.sst")

	opts := SSTableOpts{Path: sstablePath, Compression: compression.DefaultConfig()}
	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	numEntries := 5
	for i := range numEntries {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		encodedKey := keys.NewEncodedKey([]byte(key), uint64(i+1), keys.KindSet)
		err = writer.Add(encodedKey, []byte(value))
		if err != nil {
			t.Fatalf("Failed to add key-value pair: %v", err)
		}
	}

	// Check stats before finishing
	if writer.NumEntries() != uint64(numEntries) {
		t.Fatalf("Expected %d entries, got %d", numEntries, writer.NumEntries())
	}

	if writer.EstimatedSize() == 0 {
		t.Fatalf("Expected non-zero estimated size")
	}

	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish SSTable: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close SSTable: %v", err)
	}

	// Check file exists and has reasonable size
	stat, err := os.Stat(sstablePath)
	if err != nil {
		t.Fatalf("Failed to stat SSTable file: %v", err)
	}

	if stat.Size() == 0 {
		t.Fatalf("SSTable file is empty")
	}

	// The file should be at least as large as the footer
	if stat.Size() < FooterSize {
		t.Fatalf("SSTable file is too small: %d bytes", stat.Size())
	}
}

func TestSSTableEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()

	sstablePath := filepath.Join(tmpDir, "empty.sst")

	// Create empty SSTable
	opts := SSTableOpts{Path: sstablePath, Compression: compression.DefaultConfig()}
	writer, err := NewSSTableWriter(opts)
	if err != nil {
		t.Fatalf("Failed to create SSTable writer: %v", err)
	}

	err = writer.Finish()
	if err != nil {
		t.Fatalf("Failed to finish empty SSTable: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close SSTable: %v", err)
	}

	// Try to read empty SSTable
	reader, err := NewSSTableReader(sstablePath, 0, nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})))
	if err != nil {
		t.Fatalf("Failed to create SSTable reader: %v", err)
	}
	defer reader.Close()

	// Any key lookup should return not found
	_, returnedKey := reader.Get([]byte("anykey"))
	if returnedKey != nil {
		t.Fatalf("Empty SSTable should not contain any keys")
	}
}
