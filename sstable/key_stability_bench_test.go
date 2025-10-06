package sstable

import (
	"fmt"
	"testing"

	"github.com/twlk9/lgdb/keys"
)

// BenchmarkKeyStabilityOptimization tests the impact of the key stability optimization
// which eliminates one copy operation for restart entries (every 16th entry by default)
func BenchmarkKeyStabilityOptimization(b *testing.B) {
	// Create a block with many entries to have a good mix of restart and regular entries
	builder := NewBlockBuilder(4096, 16, 2)

	// Add entries that will create restart points
	numEntries := RestartInterval * 4 // 64 entries = 4 restart points (entries 0, 16, 32, 48)
	keys := make([]string, numEntries)
	values := make([]string, numEntries)

	for i := range numEntries {
		keys[i] = fmt.Sprintf("key%04d", i)
		values[i] = fmt.Sprintf("value%04d", i)
		builder.Add([]byte(keys[i]), []byte(values[i]))
	}

	blockData := builder.Finish()
	block, err := parseBlockForTesting(blockData)
	if err != nil {
		b.Fatalf("Failed to parse block: %v", err)
	}

	b.Run("mixed_access", func(b *testing.B) {
		// Test mixed access pattern that hits both restart entries and regular entries
		buffers := NewEntryBuffers(64, 64)
		var sum int

		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			// Access pattern that hits restart points and regular entries
			index := i % numEntries
			err := block.getEntry(index, buffers)
			if err != nil {
				b.Fatalf("Failed to get entry %d: %v", index, err)
			}
			sum += len(buffers.key)
		}
		b.StopTimer()
		b.Logf("average key length: %d", sum/b.N)
	})

	b.Run("restart_entries_only", func(b *testing.B) {
		// Test accessing only restart entries to maximize optimization benefit
		buffers := NewEntryBuffers(64, 64)
		restartIndices := []int{0, 16, 32, 48} // Known restart points
		var sum int

		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			index := restartIndices[i%len(restartIndices)]
			err := block.getEntry(index, buffers)
			if err != nil {
				b.Fatalf("Failed to get entry %d: %v", index, err)
			}
			sum += len(buffers.key)
		}
		b.StopTimer()
		b.Logf("average key length: %d", sum/b.N)
	})

	b.Run("regular_entries_only", func(b *testing.B) {
		// Test accessing only non-restart entries (should see no optimization benefit)
		buffers := NewEntryBuffers(64, 64)
		regularIndices := []int{1, 17, 33, 49} // Non-restart points
		var sum int

		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			index := regularIndices[i%len(regularIndices)]
			err := block.getEntry(index, buffers)
			if err != nil {
				b.Fatalf("Failed to get entry %d: %v", index, err)
			}
			sum += len(buffers.key)
		}
		b.StopTimer()
		b.Logf("average key length: %d", sum/b.N)
	})
}

// BenchmarkBlockIteration tests the performance of block iteration
// which should benefit from key stability optimization
func BenchmarkBlockIteration(b *testing.B) {
	// Create a block with many entries
	builder := NewBlockBuilder(4096, 16, 2)
	numEntries := RestartInterval * 4 // 64 entries

	for i := range numEntries {
		key := fmt.Sprintf("testkey%04d", i)
		value := fmt.Sprintf("testvalue%04d", i)
		builder.Add([]byte(key), []byte(value))
	}

	blockData := builder.Finish()
	block, err := parseBlockForTesting(blockData)
	if err != nil {
		b.Fatalf("Failed to parse block: %v", err)
	}

	b.Run("full_iteration", func(b *testing.B) {
		var keyCount int
		b.ResetTimer()

		for b.Loop() {
			iter := block.NewIterator()
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				key := iter.Key()
				keyCount += len(key)
			}
			if iter.Error() != nil {
				b.Fatalf("Iterator error: %v", iter.Error())
			}
		}
		b.StopTimer()
		b.Logf("total key bytes processed per iteration: %d", keyCount/b.N)
	})

	b.Run("seek_to_restart_points", func(b *testing.B) {
		iter := block.NewIterator()
		restartKeys := []keys.EncodedKey{
			keys.EncodedKey("testkey0000"),
			keys.EncodedKey("testkey0016"),
			keys.EncodedKey("testkey0032"),
			keys.EncodedKey("testkey0048"),
		}
		var keyCount int

		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			targetKey := restartKeys[i%len(restartKeys)]
			iter.Seek(targetKey)
			if iter.Valid() {
				key := iter.Key()
				keyCount += len(key)
			}
			if iter.Error() != nil {
				b.Fatalf("Iterator error: %v", iter.Error())
			}
		}
		b.StopTimer()
		b.Logf("average key length: %d", keyCount/b.N)
	})
}

// Helper function to parse block data for testing
func parseBlockForTesting(data []byte) (*Block, error) {
	// Use a dummy reader to get access to the real parseBlock method
	// This ensures the test is using the same logic as production
	r := &SSTableReader{}
	return r.parseBlock(data)
}

// Test to verify that restart entries are being optimized
func TestKeyStabilityOptimizationDetection(t *testing.T) {
	// Create a block with known restart points
	builder := NewBlockBuilder(4096, 16, 2)

	// Add entries - first entry (index 0) will be a restart point
	for i := range RestartInterval * 2 {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		builder.Add([]byte(key), []byte(value))
	}

	blockData := builder.Finish()
	block, err := parseBlockForTesting(blockData)
	if err != nil {
		t.Fatalf("Failed to parse block: %v", err)
	}

	buffers := NewEntryBuffers(64, 64)

	// Test restart entry (should be optimized)
	err = block.getEntry(0, buffers)
	if err != nil {
		t.Fatalf("Failed to get restart entry: %v", err)
	}

	if !buffers.isRestartEntry {
		t.Errorf("Expected entry 0 to be detected as restart entry")
	}

	// Test regular entry (should not be optimized)
	err = block.getEntry(1, buffers)
	if err != nil {
		t.Fatalf("Failed to get regular entry: %v", err)
	}

	if buffers.isRestartEntry {
		t.Errorf("Expected entry 1 to NOT be detected as restart entry")
	}

	// Test second restart point
	err = block.getEntry(RestartInterval, buffers)
	if err != nil {
		t.Fatalf("Failed to get second restart entry: %v", err)
	}

	if !buffers.isRestartEntry {
		t.Errorf("Expected entry %d to be detected as restart entry", RestartInterval)
	}
}
