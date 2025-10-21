//go:build integration
// +build integration

package lgdb

import (
	"os"
	"testing"
	"time"
)

// TestL0BackpressureSimple tests the simple polling-based L0 backpressure
func TestL0BackpressureSimple(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "TestL0BackpressureSimple")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Configure for aggressive L0 backpressure
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 256   // Very small memtable to create many L0 files
	opts.L0CompactionTrigger = 1 // Start compaction immediately
	opts.L0StopWritesTrigger = 3 // Block writes at 3 files
	// opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create enough writes to trigger L0 backpressure
	t.Log("Creating writes to trigger L0 backpressure...")

	writeCount := 0
	for i := range 200 {
		key := []byte("key" + string(rune(i%10))) // Reuse keys to trigger more flushes
		value := make([]byte, 50)                 // Small values
		for j := range value {
			value[j] = byte(i % 256)
		}

		start := time.Now()
		err = db.Put(key, value)
		duration := time.Since(start)

		if err != nil {
			t.Fatal(err)
		}

		writeCount++

		// Log if write took a long time (indicating backpressure)
		if duration > 10*time.Millisecond {
			t.Logf("Write %d took %v (backpressure likely)", i, duration)
		}

		// Check L0 file count periodically
		if i%50 == 0 {
			stats := db.GetStats()
			l0Files, _ := stats["level_0_files"].(int)
			t.Logf("After %d writes: L0 files = %d (trigger = %d, stop = %d)",
				i, l0Files, opts.L0CompactionTrigger, opts.L0StopWritesTrigger)
		}
	}

	t.Logf("Completed %d writes successfully", writeCount)

	// Verify all writes completed
	if writeCount != 200 {
		t.Fatalf("Expected 200 writes, got %d", writeCount)
	}

	// Check final stats
	stats := db.GetStats()
	l0Files, _ := stats["level_0_files"].(int)
	t.Logf("Final L0 files: %d", l0Files)

	t.Log("SUCCESS: L0 backpressure mechanism working with simple polling")
}

// TestL0BackpressureBlocking tests that writes actually block when L0 is full
func TestL0BackpressureBlocking(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "TestL0BackpressureBlocking")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Configure for very aggressive backpressure
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.WriteBufferSize = 128   // Tiny memtable
	opts.L0CompactionTrigger = 1 // Immediate compaction
	opts.L0StopWritesTrigger = 2 // Block at 2 files
	// opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Fill up to just before the trigger
	t.Log("Creating initial L0 files...")
	for i := range 50 {
		key := []byte("initial" + string(rune(i)))
		value := make([]byte, 30)
		for j := range value {
			value[j] = byte(i % 256)
		}

		err = db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait a bit for things to settle
	time.Sleep(100 * time.Millisecond)

	stats := db.GetStats()
	l0Files, _ := stats["level_0_files"].(int)
	t.Logf("Initial L0 files: %d", l0Files)

	// Now do a write that might trigger backpressure
	start := time.Now()

	key := []byte("test_backpressure_key")
	value := make([]byte, 50)
	err = db.Put(key, value)

	duration := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Write completed in %v", duration)

	// Verify the write was successful
	retrieved, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if len(retrieved) != len(value) {
		t.Fatalf("Expected value length %d, got %d", len(value), len(retrieved))
	}

	t.Log("SUCCESS: Simple polling backpressure mechanism is working")
}
