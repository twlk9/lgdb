package sstable

import (
	"fmt"
	"testing"

	"github.com/twlk9/lgdb/keys"
)

// TestBlockGetEntrySingle tests getEntry with a single entry
func TestBlockGetEntrySingle(t *testing.T) {
	builder := NewBlockBuilder(4096, 16, 2)

	userKey := []byte("test_key")
	encodedKey := keys.NewEncodedKey(userKey, 1, keys.KindSet)
	value := []byte("test_value")

	builder.Add(encodedKey, value)
	data := builder.Finish()

	reader := &SSTableReader{}
	block, err := reader.parseBlock(data)
	if err != nil {
		t.Fatalf("Failed to parse block: %v", err)
	}

	// Test valid index
	buffers := &EntryBuffers{}
	err = block.getEntry(0, buffers)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if string(buffers.key) != string(encodedKey) {
		t.Errorf("Expected key %x, got %x", encodedKey, buffers.key)
	}
	if string(buffers.value) != string(value) {
		t.Errorf("Expected value %s, got %s", value, buffers.value)
	}

	// Test invalid index
	err = block.getEntry(1, buffers)
	if err == nil {
		t.Error("Expected error for out of range index")
	}
}

// TestBlockGetEntryMultiple tests getEntry with multiple entries
func TestBlockGetEntryMultiple(t *testing.T) {
	builder := NewBlockBuilder(4096, 16, 2)

	numEntries := 10
	testData := make([]struct {
		key   []byte
		value []byte
	}, numEntries)

	// Add entries with shared prefixes to test prefix compression
	for i := range numEntries {
		userKey := fmt.Appendf(nil, "shared_prefix_key_%03d", i)
		encodedKey := keys.NewEncodedKey(userKey, uint64(i+1), keys.KindSet)
		value := fmt.Appendf(nil, "value_%03d", i)

		testData[i].key = encodedKey
		testData[i].value = value
		builder.Add(encodedKey, value)
	}

	data := builder.Finish()

	reader := &SSTableReader{}
	block, err := reader.parseBlock(data)
	if err != nil {
		t.Fatalf("Failed to parse block: %v", err)
	}

	// Test all entries
	buffers := &EntryBuffers{}
	for i := range numEntries {
		err = block.getEntry(i, buffers)
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", i, err)
		}

		if string(buffers.key) != string(testData[i].key) {
			t.Errorf("Entry %d: Expected key %x, got %x", i, testData[i].key, buffers.key)
		}
		if string(buffers.value) != string(testData[i].value) {
			t.Errorf("Entry %d: Expected value %s, got %s", i, testData[i].value, buffers.value)
		}
	}
}

// TestBlockGetEntryEdgeCases tests various edge cases
func TestBlockGetEntryEdgeCases(t *testing.T) {
	// Test with empty key
	t.Run("EmptyKey", func(t *testing.T) {
		builder := NewBlockBuilder(4096, 16, 2)

		encodedKey := keys.NewEncodedKey(nil, 1, keys.KindSet)
		value := []byte("value")

		builder.Add(encodedKey, value)
		data := builder.Finish()

		reader := &SSTableReader{}
		block, err := reader.parseBlock(data)
		if err != nil {
			t.Fatalf("Failed to parse block: %v", err)
		}

		buffers := &EntryBuffers{}
		err = block.getEntry(0, buffers)
		if err != nil {
			t.Fatalf("Failed to get entry: %v", err)
		}

		if string(buffers.key) != string(encodedKey) {
			t.Errorf("Expected key %x, got %x", encodedKey, buffers.key)
		}
	})

	// Test with empty value
	t.Run("EmptyValue", func(t *testing.T) {
		builder := NewBlockBuilder(4096, 16, 2)

		userKey := []byte("key")
		encodedKey := keys.NewEncodedKey(userKey, 1, keys.KindSet)

		builder.Add(encodedKey, nil)
		data := builder.Finish()

		reader := &SSTableReader{}
		block, err := reader.parseBlock(data)
		if err != nil {
			t.Fatalf("Failed to parse block: %v", err)
		}

		buffers := &EntryBuffers{}
		err = block.getEntry(0, buffers)
		if err != nil {
			t.Fatalf("Failed to get entry: %v", err)
		}

		if len(buffers.value) != 0 {
			t.Errorf("Expected empty value, got %x", buffers.value)
		}
	})

	// Test with large values
	t.Run("LargeValue", func(t *testing.T) {
		builder := NewBlockBuilder(4096, 16, 2)

		userKey := []byte("key")
		encodedKey := keys.NewEncodedKey(userKey, 1, keys.KindSet)
		largeValue := make([]byte, 1000)
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		builder.Add(encodedKey, largeValue)
		data := builder.Finish()

		reader := &SSTableReader{}
		block, err := reader.parseBlock(data)
		if err != nil {
			t.Fatalf("Failed to parse block: %v", err)
		}

		buffers := &EntryBuffers{}
		err = block.getEntry(0, buffers)
		if err != nil {
			t.Fatalf("Failed to get entry: %v", err)
		}

		if len(buffers.value) != len(largeValue) {
			t.Errorf("Expected value length %d, got %d", len(largeValue), len(buffers.value))
		}

		for i, expected := range largeValue {
			if buffers.value[i] != expected {
				t.Errorf("Value mismatch at index %d: expected %d, got %d", i, expected, buffers.value[i])
				break
			}
		}
	})
}

// TestBlockGetEntryRandomAccess tests random access patterns
func TestBlockGetEntryRandomAccess(t *testing.T) {
	builder := NewBlockBuilder(4096, 16, 2)

	numEntries := 50
	testData := make([]struct {
		key   []byte
		value []byte
	}, numEntries)

	// Add entries
	for i := range numEntries {
		userKey := fmt.Appendf(nil, "key_%05d", i)
		encodedKey := keys.NewEncodedKey(userKey, uint64(i+1), keys.KindSet)
		value := fmt.Appendf(nil, "value_%05d", i)

		testData[i].key = encodedKey
		testData[i].value = value
		builder.Add(encodedKey, value)
	}

	data := builder.Finish()

	reader := &SSTableReader{}
	block, err := reader.parseBlock(data)
	if err != nil {
		t.Fatalf("Failed to parse block: %v", err)
	}

	// Test random access pattern
	buffers := &EntryBuffers{}
	testPattern := []int{0, 49, 25, 10, 40, 5, 35, 15, 30, 20}

	for _, index := range testPattern {
		err = block.getEntry(index, buffers)
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", index, err)
		}

		if string(buffers.key) != string(testData[index].key) {
			t.Errorf("Entry %d: Expected key %x, got %x", index, testData[index].key, buffers.key)
		}
		if string(buffers.value) != string(testData[index].value) {
			t.Errorf("Entry %d: Expected value %s, got %s", index, testData[index].value, buffers.value)
		}
	}
}

// TestBlockGetEntryBoundaryConditions tests boundary conditions
func TestBlockGetEntryBoundaryConditions(t *testing.T) {
	builder := NewBlockBuilder(4096, 16, 2)

	numEntries := RestartInterval*2 + 5 // Cross restart boundaries

	// Add entries
	for i := range numEntries {
		userKey := fmt.Appendf(nil, "key_%05d", i)
		encodedKey := keys.NewEncodedKey(userKey, uint64(i+1), keys.KindSet)
		value := fmt.Appendf(nil, "value_%05d", i)

		builder.Add(encodedKey, value)
	}

	data := builder.Finish()

	reader := &SSTableReader{}
	block, err := reader.parseBlock(data)
	if err != nil {
		t.Fatalf("Failed to parse block: %v", err)
	}

	buffers := &EntryBuffers{}

	// Test first entry
	err = block.getEntry(0, buffers)
	if err != nil {
		t.Fatalf("Failed to get first entry: %v", err)
	}

	// Test last entry
	err = block.getEntry(numEntries-1, buffers)
	if err != nil {
		t.Fatalf("Failed to get last entry: %v", err)
	}

	// Test restart boundary entries
	restartBoundaries := []int{RestartInterval - 1, RestartInterval, RestartInterval + 1, RestartInterval*2 - 1, RestartInterval * 2}
	for _, index := range restartBoundaries {
		if index < numEntries {
			err = block.getEntry(index, buffers)
			if err != nil {
				t.Fatalf("Failed to get restart boundary entry %d: %v", index, err)
			}
		}
	}

	// Test out of bounds
	err = block.getEntry(numEntries, buffers)
	if err == nil {
		t.Error("Expected error for out of bounds index")
	}

	err = block.getEntry(-1, buffers)
	if err == nil {
		t.Error("Expected error for negative index")
	}
}

// TestBlockGetEntryBufferReuse tests that buffers are properly reused
func TestBlockGetEntryBufferReuse(t *testing.T) {
	builder := NewBlockBuilder(4096, 16, 2)

	// Add entries with different key/value sizes
	entries := []struct {
		keySize   int
		valueSize int
	}{
		{10, 20},
		{50, 100},
		{5, 5},
		{100, 200},
		{1, 1},
	}

	for i, entry := range entries {
		userKey := make([]byte, entry.keySize)
		for j := range userKey {
			userKey[j] = byte('a' + (i+j)%26)
		}

		encodedKey := keys.NewEncodedKey(userKey, uint64(i+1), keys.KindSet)

		value := make([]byte, entry.valueSize)
		for j := range value {
			value[j] = byte('0' + (i+j)%10)
		}

		builder.Add(encodedKey, value)
	}

	data := builder.Finish()

	reader := &SSTableReader{}
	block, err := reader.parseBlock(data)
	if err != nil {
		t.Fatalf("Failed to parse block: %v", err)
	}

	buffers := &EntryBuffers{}

	// Access entries in order that tests buffer growth and reuse
	accessOrder := []int{4, 0, 3, 1, 2} // small->large->largest->medium->small

	for _, index := range accessOrder {
		err = block.getEntry(index, buffers)
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", index, err)
		}

		// Verify buffer capacity doesn't shrink unnecessarily
		if cap(buffers.keyBuf) < len(buffers.key) {
			t.Errorf("Key buffer capacity (%d) smaller than required (%d)", cap(buffers.keyBuf), len(buffers.key))
		}
		if cap(buffers.valueBuf) < len(buffers.value) {
			t.Errorf("Value buffer capacity (%d) smaller than required (%d)", cap(buffers.valueBuf), len(buffers.value))
		}
	}
}
