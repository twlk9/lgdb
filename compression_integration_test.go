package lgdb

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/twlk9/lgdb/compression"
)

func TestCompressionIntegration(t *testing.T) {
	// Test with different compression configurations
	testCases := []struct {
		name   string
		config compression.Config
	}{
		{
			name:   "NoCompression",
			config: compression.NoCompressionConfig(),
		},
		{
			name:   "SnappyCompression",
			config: compression.SnappyConfig(),
		},
		{
			name:   "SnappyHighThreshold",
			config: compression.Config{Type: compression.Snappy, MinReductionPercent: 50},
		},
		{
			name:   "ZstdFast",
			config: compression.ZstdFastConfig(),
		},
		{
			name:   "ZstdBalanced",
			config: compression.ZstdBalancedConfig(),
		},
		{
			name:   "ZstdBest",
			config: compression.ZstdBestConfig(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Create temporary directory
			tmpDir := t.TempDir()

			// Create database with specific compression config
			options := DefaultOptions()
			options.Path = tmpDir
			options.Compression = tc.config

			db, err := Open(options)
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			// Test data - some compressible, some not
			testData := []struct {
				key   string
				value string
			}{
				// Highly compressible data
				{"compressible1", string(bytes.Repeat([]byte("A"), 1000))},
				{"compressible2", string(bytes.Repeat([]byte("Hello World! "), 100))},

				// Less compressible data
				{"random1", "Quick brown fox jumps over the lazy dog"},
				{"random2", "The five boxing wizards jump quickly"},

				// Small data (might not benefit from compression)
				{"small1", "x"},
				{"small2", "short"},
			}

			// Put all data
			for _, td := range testData {
				err := db.Put([]byte(td.key), []byte(td.value))
				if err != nil {
					t.Fatalf("Failed to put key %s: %v", td.key, err)
				}
			}

			// Verify all data can be retrieved correctly
			for _, td := range testData {
				value, err := db.Get([]byte(td.key))
				if err != nil {
					t.Fatalf("Failed to get key %s: %v", td.key, err)
				}
				if value == nil {
					t.Fatalf("Key %s not found", td.key)
				}
				if string(value) != td.value {
					t.Fatalf("Value mismatch for key %s: got %s, want %s", td.key, value, td.value)
				}
			}

			// Test iteration works correctly
			iter := db.NewIteratorWithBounds(nil)
			defer iter.Close()

			foundKeys := make(map[string]string)
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				key := string(iter.Key())
				value := string(iter.Value())
				foundKeys[key] = value
			}

			if err := iter.Error(); err != nil {
				t.Fatalf("Iterator error: %v", err)
			}

			// Verify all keys were found during iteration
			for _, td := range testData {
				if foundValue, exists := foundKeys[td.key]; !exists {
					t.Fatalf("Key %s not found during iteration", td.key)
				} else if foundValue != td.value {
					t.Fatalf("Value mismatch during iteration for key %s: got %s, want %s", td.key, foundValue, td.value)
				}
			}

			t.Logf("Successfully tested compression config: %s", tc.name)
		})
	}
}

func TestCompressionWithCompaction(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Create database with Snappy compression
	options := DefaultOptions()
	options.Path = tmpDir
	options.Compression = compression.Config{Type: compression.Snappy, MinReductionPercent: 5}
	options.WriteBufferSize = 1024 // Small buffer to force frequent flushes

	db, err := Open(options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Add enough data to trigger multiple flushes and compaction
	numKeys := 100
	for i := range numKeys {
		key := []byte("key" + fmt.Sprintf("%03d", i))
		// Use compressible values
		value := bytes.Repeat([]byte("value"+fmt.Sprintf("%03d", i)+" "), 20)

		err := db.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Verify all data is still accessible
	for i := range numKeys {
		key := []byte("key" + fmt.Sprintf("%03d", i))
		expectedValue := bytes.Repeat([]byte("value"+fmt.Sprintf("%03d", i)+" "), 20)

		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if value == nil {
			t.Fatalf("Key %d not found", i)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("Value mismatch for key %d", i)
		}
	}

	t.Log("Successfully tested compression with compaction")
}

func TestMixedCompressionTypes(t *testing.T) {
	// This test simulates opening a database with one compression type,
	// then closing and reopening with a different type
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Phase 1: Create database with no compression
	options1 := DefaultOptions()
	options1.Path = tmpDir
	options1.Compression = compression.Config{Type: compression.None}

	db1, err := Open(options1)
	if err != nil {
		t.Fatalf("Failed to open database with no compression: %v", err)
	}

	// Add some data
	for i := range 10 {
		key := []byte("key" + fmt.Sprintf("%d", i))
		value := []byte("value" + fmt.Sprintf("%d", i))
		err := db1.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Data is automatically flushed when memtable is full

	db1.Close()

	// Phase 2: Reopen with Snappy compression
	options2 := DefaultOptions()
	options2.Path = tmpDir
	options2.Compression = compression.Config{Type: compression.Snappy, MinReductionPercent: 10}

	db2, err := Open(options2)
	if err != nil {
		t.Fatalf("Failed to reopen database with Snappy compression: %v", err)
	}
	defer db2.Close()

	// Verify old data is still accessible
	for i := range 10 {
		key := []byte("key" + fmt.Sprintf("%d", i))
		expectedValue := []byte("value" + fmt.Sprintf("%d", i))

		value, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get old key %d: %v", i, err)
		}
		if value == nil {
			t.Fatalf("Old key %d not found", i)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("Value mismatch for old key %d", i)
		}
	}

	// Add new data (should be compressed)
	for i := 10; i < 20; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		value := bytes.Repeat([]byte("compressed_value"+fmt.Sprintf("%d", i)+" "), 10)
		err := db2.Put(key, value)
		if err != nil {
			t.Fatalf("Failed to put new key %d: %v", i, err)
		}
	}

	// Verify all data (old and new) is accessible
	for i := range 20 {
		key := []byte("key" + fmt.Sprintf("%d", i))

		value, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if value == nil {
			t.Fatalf("Key %d not found", i)
		}

		// Values are different for old vs new keys
		if i < 10 {
			expectedValue := []byte("value" + fmt.Sprintf("%d", i))
			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("Value mismatch for old key %d", i)
			}
		} else {
			expectedValue := bytes.Repeat([]byte("compressed_value"+fmt.Sprintf("%d", i)+" "), 10)
			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("Value mismatch for new key %d", i)
			}
		}
	}

	t.Log("Successfully tested mixed compression types")
}

func TestCompressionComparison(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	defer os.RemoveAll(tmpDir)

	// Test data - highly compressible
	largeCompressibleData := bytes.Repeat([]byte("This is a test string that should compress very well! "), 100)

	compressionConfigs := []struct {
		name   string
		config compression.Config
	}{
		{"NoCompression", compression.NoCompressionConfig()},
		{"Snappy", compression.SnappyConfig()},
		{"ZstdFast", compression.ZstdFastConfig()},
		{"ZstdBalanced", compression.ZstdBalancedConfig()},
		{"ZstdBest", compression.ZstdBestConfig()},
	}

	results := make(map[string]struct {
		dbSize     int64
		compressed bool
	})

	for _, cc := range compressionConfigs {
		t.Run(cc.name, func(t *testing.T) {
			// Create database with specific compression
			subDir := tmpDir + "/" + cc.name
			options := DefaultOptions()
			options.Path = subDir
			options.Compression = cc.config

			db, err := Open(options)
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}

			// Put the same large data multiple times with different keys
			numKeys := 20
			for i := range numKeys {
				key := []byte("large_key_" + string(rune('A'+i)))
				err := db.Put(key, largeCompressibleData)
				if err != nil {
					t.Fatalf("Failed to put key %d: %v", i, err)
				}
			}

			// Close database to ensure all data is flushed
			db.Close()

			// Measure directory size
			var dbSize int64
			err = walkDir(subDir, func(path string, size int64) {
				dbSize += size
			})
			if err != nil {
				t.Fatalf("Failed to measure directory size: %v", err)
			}

			results[cc.name] = struct {
				dbSize     int64
				compressed bool
			}{
				dbSize:     dbSize,
				compressed: cc.config.Type != compression.None,
			}

			t.Logf("%s: Database size: %d bytes", cc.name, dbSize)
		})
	}

	// Compare compression effectiveness
	t.Log("\n=== Compression Comparison Results ===")
	noCompressionSize := results["NoCompression"].dbSize

	for name, result := range results {
		if result.compressed {
			compressionRatio := float64(noCompressionSize-result.dbSize) / float64(noCompressionSize) * 100
			t.Logf("%s: %d bytes (%.1f%% smaller than no compression)", name, result.dbSize, compressionRatio)

			// Verify that compression actually helped (unless it's the minimum threshold test)
			if compressionRatio < 0 {
				t.Errorf("%s: Compression made database larger (%d vs %d bytes)", name, result.dbSize, noCompressionSize)
			}
		} else {
			t.Logf("%s: %d bytes (baseline)", name, result.dbSize)
		}
	}

	// Zstd should generally compress better than Snappy for highly compressible data
	zstdBestSize := results["ZstdBest"].dbSize
	snappySize := results["Snappy"].dbSize

	if zstdBestSize > snappySize {
		t.Logf("Note: ZstdBest (%d bytes) is larger than Snappy (%d bytes) - this can happen with small datasets", zstdBestSize, snappySize)
	} else {
		improvement := float64(snappySize-zstdBestSize) / float64(snappySize) * 100
		t.Logf("ZstdBest compressed %.1f%% better than Snappy", improvement)
	}
}

// walkDir recursively walks a directory and calls fn for each file
func walkDir(dir string, fn func(path string, size int64)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		path := dir + "/" + entry.Name()
		if entry.IsDir() {
			err := walkDir(path, fn)
			if err != nil {
				return err
			}
		} else {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			fn(path, info.Size())
		}
	}
	return nil
}

func TestZstdLevelPerformance(t *testing.T) {
	// Test different Zstd levels to understand the trade-offs
	levels := []struct {
		name   string
		config compression.Config
	}{
		{"ZstdFastest", compression.ZstdFastConfig()},
		{"ZstdBalanced", compression.ZstdBalancedConfig()},
		{"ZstdBest", compression.ZstdBestConfig()},
	}

	// Create test data
	testData := make([]byte, 10000)
	for i := range testData {
		if i%100 < 50 {
			testData[i] = byte('A' + (i % 26)) // Compressible pattern
		} else {
			testData[i] = byte(i % 256) // Less compressible
		}
	}

	t.Log("\n=== Zstd Level Performance Comparison ===")

	for _, level := range levels {
		t.Run(level.name, func(t *testing.T) {
			compressor, err := compression.NewCompressor(level.config)
			if err != nil {
				t.Fatalf("Failed to create compressor: %v", err)
			}

			// Measure compression
			compressed, wasCompressed, err := compressor.Compress(nil, testData)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			if !wasCompressed {
				t.Fatalf("Data was not compressed")
			}

			compressionRatio := float64(len(testData)-len(compressed)) / float64(len(testData)) * 100
			t.Logf("%s: %d -> %d bytes (%.1f%% reduction)", level.name, len(testData), len(compressed), compressionRatio)

			// Verify decompression works
			decompressed, err := compressor.Decompress(nil, compressed)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			if !bytes.Equal(testData, decompressed) {
				t.Fatalf("Decompressed data doesn't match original")
			}
		})
	}
}
