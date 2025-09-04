package lgdb

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
)

// benchmarkDB provides a helper for setting up databases in benchmarks
type benchmarkDB struct {
	db     *DB
	dir    string
	keys   [][]byte
	values [][]byte
}

// newBenchmarkDB creates a new benchmark database with the given options
func newBenchmarkDB(b *testing.B, opts *Options) *benchmarkDB {
	if opts == nil {
		opts = DefaultOptions()
	}

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "lgdb_bench_*")
	if err != nil {
		b.Fatal(err)
	}

	opts.Path = tempDir

	// Open database
	db, err := Open(opts)
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatal(err)
	}

	return &benchmarkDB{
		db:  db,
		dir: tempDir,
	}
}

// close cleans up the benchmark database
func (bdb *benchmarkDB) close() {
	if bdb.db != nil {
		bdb.db.Close()
	}
	if bdb.dir != "" {
		os.RemoveAll(bdb.dir)
	}
}

// generateKeys creates test keys of the specified size
func (bdb *benchmarkDB) generateKeys(count, keySize int) {
	bdb.keys = make([][]byte, count)
	for i := range count {
		key := fmt.Sprintf("key%0*d", keySize-3, i) // -3 for "key" prefix
		if len(key) > keySize {
			key = key[:keySize]
		} else if len(key) < keySize {
			// Pad with zeros if needed
			key = key + string(make([]byte, keySize-len(key)))
		}
		bdb.keys[i] = []byte(key)
	}
}

// generateValues creates test values of the specified size
func (bdb *benchmarkDB) generateValues(count, valueSize int) {
	bdb.values = make([][]byte, count)
	for i := range count {
		value := fmt.Sprintf("value%0*d", valueSize-5, i) // -5 for "value" prefix
		if len(value) > valueSize {
			value = value[:valueSize]
		} else if len(value) < valueSize {
			// Pad with 'x' characters
			value = value + string(make([]byte, valueSize-len(value)))
			for j := len(fmt.Sprintf("value%0*d", valueSize-5, i)); j < valueSize; j++ {
				value = value[:j] + "x" + value[j+1:]
			}
		}
		bdb.values[i] = []byte(value)
	}
}

// BenchmarkPutAllocations measures memory allocations per Put operation
func BenchmarkPutAllocations(b *testing.B) {
	opts := DefaultOptions()
	opts.Path = b.TempDir()
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})) // Silence all logging
	opts.WriteBufferSize = 1024 * 1024 * 50                                                                  // Large buffer to avoid flushes during benchmark

	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-generate test data
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; b.Loop(); i++ {
		keys[i] = fmt.Appendf(nil, "key%016d", i)
		values[i] = fmt.Appendf(nil, "value%0100d", i) // 100-byte values
	}

	b.ReportAllocs() // Enable allocation reporting
	// Reset timer after setup

	for i := 0; b.Loop(); i++ {
		err := db.Put(keys[i], values[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}
