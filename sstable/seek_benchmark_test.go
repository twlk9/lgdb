package sstable

import (
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/twlk9/lgdb/compression"
	"github.com/twlk9/lgdb/keys"
)

func BenchmarkSSTableSeek(b *testing.B) {
	// Create a temporary file
	tempFile, err := os.CreateTemp("", "sstable_seek_bench_*.sst")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Create a large SSTable for benchmarking
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	opts := SSTableOpts{Path: tempFile.Name(), Compression: compression.DefaultConfig(), Logger: logger}
	writer, err := NewSSTableWriter(opts)
	if err != nil {
		b.Fatal(err)
	}

	// Generate test data with a large number of keys
	numKeys := 10000
	testKeys := make([][]byte, numKeys)

	for i := range numKeys {
		userKey := fmt.Appendf(nil, "key%08d", i)
		encodedKey := keys.NewEncodedKey(userKey, uint64(i), keys.KindSet)
		value := fmt.Appendf(nil, "value%08d", i)

		testKeys[i] = encodedKey
		err := writer.Add(encodedKey, value)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = writer.Finish()
	if err != nil {
		b.Fatal(err)
	}
	writer.Close()

	// Open the SSTable for reading
	reader, err := NewSSTableReader(tempFile.Name(), 0, nil, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	// Benchmark seeking to random keys

	for i := 0; b.Loop(); i++ {
		// Seek to a key in the middle range
		targetIndex := (i * 7) % numKeys // Some variation in access pattern
		targetKey := testKeys[targetIndex]

		// Test Get operation which uses findDataBlock
		_, returnedKey := reader.Get(targetKey)
		if returnedKey == nil {
			b.Fatalf("Key not found: %s", targetKey)
		}
	}
}

func BenchmarkBlockIteratorSeek(b *testing.B) {
	// Create a temporary file
	tempFile, err := os.CreateTemp("", "block_seek_bench_*.sst")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Create an SSTable with one large block
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	opts := SSTableOpts{Path: tempFile.Name(), Compression: compression.DefaultConfig(), Logger: logger}
	writer, err := NewSSTableWriter(opts)
	if err != nil {
		b.Fatal(err)
	}

	// Generate test data - enough to fill a few blocks
	numKeys := 1000
	testKeys := make([][]byte, numKeys)     // Internal keys for adding to writer
	testUserKeys := make([][]byte, numKeys) // User keys for seeking

	for i := range numKeys {
		userKey := fmt.Appendf(nil, "key%08d", i)
		encodedKey := keys.NewEncodedKey(userKey, uint64(i), keys.KindSet)
		value := fmt.Appendf(nil, "value%08d", i)

		testKeys[i] = encodedKey
		testUserKeys[i] = userKey // Store user key for seeking
		err := writer.Add(encodedKey, value)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = writer.Finish()
	if err != nil {
		b.Fatal(err)
	}
	writer.Close()

	// Open the SSTable for reading
	reader, err := NewSSTableReader(tempFile.Name(), 0, nil, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	// Benchmark seeking within the iterator
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Create separate iterator for each goroutine
		iter := reader.NewIterator()
		defer iter.Close()

		i := 0
		for pb.Next() {
			// Seek to a key in the middle range
			targetIndex := (i * 7) % numKeys           // Some variation in access pattern
			targetUserKey := testUserKeys[targetIndex] // Use user key for seeking

			iter.Seek(keys.NewQueryKey(targetUserKey))
			if !iter.Valid() {
				b.Fatalf("Iterator not valid after seek to: %s", targetUserKey)
			}
			i++
		}
	})
}

func BenchmarkIndexBlockSearch(b *testing.B) {
	// Create a temporary file
	tempFile, err := os.CreateTemp("", "index_bench_*.sst")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	// Create an SSTable with many blocks to exercise index search
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	opts := SSTableOpts{Path: tempFile.Name(), Compression: compression.DefaultConfig(), Logger: logger}
	writer, err := NewSSTableWriter(opts)
	if err != nil {
		b.Fatal(err)
	}

	// Generate enough data to create multiple blocks
	numKeys := 50000 // This should create multiple blocks
	testKeys := make([][]byte, numKeys)

	for i := range numKeys {
		userKey := fmt.Appendf(nil, "key%08d", i)
		encodedKey := keys.NewEncodedKey(userKey, uint64(i), keys.KindSet)
		value := fmt.Appendf(nil, "value%08d", i)

		testKeys[i] = encodedKey
		err := writer.Add(encodedKey, value)
		if err != nil {
			b.Fatal(err)
		}
	}

	err = writer.Finish()
	if err != nil {
		b.Fatal(err)
	}
	writer.Close()

	// Open the SSTable for reading
	reader, err := NewSSTableReader(tempFile.Name(), 0, nil, logger)
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	// Benchmark index block search via findDataBlock

	for i := 0; b.Loop(); i++ {
		// Search for a key that exercises the index
		targetIndex := (i * 7) % numKeys
		targetKey := testKeys[targetIndex]

		// This calls findDataBlock internally
		_, found := reader.findDataBlock(targetKey)
		if !found {
			b.Fatalf("Block not found for key: %s", targetKey)
		}
	}
}
