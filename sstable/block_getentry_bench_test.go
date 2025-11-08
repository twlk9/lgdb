package sstable

import (
	"fmt"
	"testing"

	"github.com/twlk9/lgdb/keys"
)

// BenchmarkBlockGetEntry benchmarks the getEntry method with offset index optimization
func BenchmarkBlockGetEntry(b *testing.B) {
	// Create a block with many entries
	builder := NewBlockBuilder(4096, 16, 4)

	numEntries := 1000
	testKeys := make([][]byte, numEntries)

	// Add entries with shared prefixes to test optimization
	for i := range numEntries {
		userKey := fmt.Appendf(nil, "prefix_key_%05d", i)
		encodedKey := keys.NewEncodedKey(userKey, uint64(i), keys.KindSet)
		value := fmt.Appendf(nil, "value_%05d", i)

		testKeys[i] = encodedKey
		builder.Add(encodedKey, value)
	}

	// Finish the block
	data := builder.Finish()

	// Create a dummy reader to access parseBlock
	tempFile := &SSTableReader{}

	// Parse the finished block
	block, err := tempFile.parseBlock(data)
	if err != nil {
		b.Fatal(err)
	}

	// Benchmark getEntry calls with random access patterns

	buffers := &EntryBuffers{}
	for i := 0; b.Loop(); i++ {
		// Random access pattern to test offset index effectiveness
		index := (i * 7) % numEntries
		err := block.getEntry(index, buffers)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBlockGetEntrySequential benchmarks sequential access
func BenchmarkBlockGetEntrySequential(b *testing.B) {
	// Create a block with many entries
	builder := NewBlockBuilder(4096, 16, 4)

	numEntries := 1000

	// Add entries with shared prefixes
	for i := range numEntries {
		userKey := fmt.Appendf(nil, "prefix_key_%05d", i)
		encodedKey := keys.NewEncodedKey(userKey, uint64(i), keys.KindSet)
		value := fmt.Appendf(nil, "value_%05d", i)

		builder.Add(encodedKey, value)
	}

	// Finish the block
	data := builder.Finish()

	// Create a dummy reader to access parseBlock
	tempFile := &SSTableReader{}

	// Parse the finished block
	block, err := tempFile.parseBlock(data)
	if err != nil {
		b.Fatal(err)
	}

	// Benchmark sequential getEntry calls

	buffers := &EntryBuffers{}
	for i := 0; b.Loop(); i++ {
		index := i % numEntries
		err := block.getEntry(index, buffers)
		if err != nil {
			b.Fatal(err)
		}
	}
}
