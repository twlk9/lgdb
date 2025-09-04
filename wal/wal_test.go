package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/twlk9/lgdb/keys"
)

func makeOpts(p string, fn uint64, si, msi time.Duration, bs int) WALOpts {
	return WALOpts{
		Path:            p,
		FileNum:         fn,
		SyncInterval:    si,
		MinSyncInterval: msi,
		BytesPerSync:    bs,
	}
}

// TestWALBasicOperations tests basic WAL write and read operations
func TestWALBasicOperations(t *testing.T) {
	tempDir := t.TempDir()

	// Create WAL
	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Test writing records
	testCases := []struct {
		seq   uint64
		key   string
		value string
		op    string
	}{
		{1, "key1", "value1", "put"},
		{2, "key2", "value2", "put"},
		{3, "key1", "", "delete"},
		{4, "key3", "value3", "put"},
	}

	for _, tc := range testCases {
		if tc.op == "put" {
			err = wal.WritePut(tc.seq, []byte(tc.key), []byte(tc.value))
		} else {
			err = wal.WriteDelete(tc.seq, []byte(tc.key))
		}
		if err != nil {
			t.Fatalf("Failed to write %s record: %v", tc.op, err)
		}
	}

	// Force sync
	if err = wal.Sync(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Close WAL
	if err = wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Read back records
	walPath := filepath.Join(tempDir, "000001.wal")
	reader, err := NewWALReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	for i, tc := range testCases {
		record, err := reader.ReadRecord()
		if err != nil {
			t.Fatalf("Failed to read record %d: %v", i, err)
		}

		if record.Seq != tc.seq {
			t.Errorf("Record %d: expected seq %d, got %d", i, tc.seq, record.Seq)
		}

		if string(record.Key) != tc.key {
			t.Errorf("Record %d: expected key %s, got %s", i, tc.key, string(record.Key))
		}

		if tc.op == "put" {
			if record.Type != keys.KindSet {
				t.Errorf("Record %d: expected type %d, got %d", i, keys.KindSet, record.Type)
			}
			if string(record.Value) != tc.value {
				t.Errorf("Record %d: expected value %s, got %s", i, tc.value, string(record.Value))
			}
		} else {
			if record.Type != keys.KindDelete {
				t.Errorf("Record %d: expected type %d, got %d", i, keys.KindDelete, record.Type)
			}
			if record.Value != nil {
				t.Errorf("Record %d: expected nil value for delete, got %s", i, string(record.Value))
			}
		}
	}
}

// TestWALSync tests basic sync functionality
func TestWALSync(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write a record
	err = wal.WritePut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Test synchronous sync
	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Test async sync
	syncChan := wal.SyncAsync()
	select {
	case err = <-syncChan:
		if err != nil {
			t.Fatalf("Async sync failed: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Async sync timed out")
	}
}

// TestWALSyncBatching tests that multiple sync requests are batched
func TestWALSyncBatching(t *testing.T) {
	tempDir := t.TempDir()

	// Create WAL with minimum sync interval
	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 10*time.Millisecond, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write a record
	err = wal.WritePut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Request multiple syncs quickly
	sync1 := wal.SyncAsync()
	sync2 := wal.SyncAsync()
	sync3 := wal.SyncAsync()

	// All should complete successfully
	select {
	case err = <-sync1:
		if err != nil {
			t.Fatalf("First sync failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("First sync timed out")
	}

	select {
	case err = <-sync2:
		if err != nil {
			t.Fatalf("Second sync failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Second sync timed out")
	}

	select {
	case err = <-sync3:
		if err != nil {
			t.Fatalf("Third sync failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Third sync timed out")
	}
}

// TestWALClose tests WAL closing behavior
func TestWALClose(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write a record
	err = wal.WritePut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Close WAL
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Try to write after close (should fail)
	err = wal.WritePut(2, []byte("key2"), []byte("value2"))
	if err == nil {
		t.Fatalf("Expected error when writing to closed WAL")
	}

	// Try to sync after close (should fail)
	err = wal.Sync()
	if err == nil {
		t.Fatalf("Expected error when syncing closed WAL")
	}
}

// TestWALRecordTypes tests different record types
func TestWALRecordTypes(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write different record types
	err = wal.WritePut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write put record: %v", err)
	}

	err = wal.WriteDelete(2, []byte("key2"))
	if err != nil {
		t.Fatalf("Failed to write delete record: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Read back and verify types
	walPath := filepath.Join(tempDir, "000001.wal")
	reader, err := NewWALReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	// Read put record
	record, err := reader.ReadRecord()
	if err != nil {
		t.Fatalf("Failed to read put record: %v", err)
	}
	if record.Type != keys.KindSet {
		t.Errorf("Expected keys.KindSet (%d), got %d", keys.KindSet, record.Type)
	}

	// Read delete record
	record, err = reader.ReadRecord()
	if err != nil {
		t.Fatalf("Failed to read delete record: %v", err)
	}
	if record.Type != keys.KindDelete {
		t.Errorf("Expected keys.KindDelete (%d), got %d", keys.KindDelete, record.Type)
	}
}

// TestWALEmptyValue tests handling of empty values
func TestWALEmptyValue(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write record with empty value
	err = wal.WritePut(1, []byte("key1"), []byte(""))
	if err != nil {
		t.Fatalf("Failed to write record with empty value: %v", err)
	}

	// Write record with nil value (should work for delete)
	err = wal.WriteDelete(2, []byte("key2"))
	if err != nil {
		t.Fatalf("Failed to write delete record: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Read back and verify
	walPath := filepath.Join(tempDir, "000001.wal")
	reader, err := NewWALReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	// Read put record with empty value
	record, err := reader.ReadRecord()
	if err != nil {
		t.Fatalf("Failed to read record: %v", err)
	}
	if len(record.Value) != 0 {
		t.Errorf("Expected empty value, got %s", string(record.Value))
	}

	// Read delete record
	record, err = reader.ReadRecord()
	if err != nil {
		t.Fatalf("Failed to read delete record: %v", err)
	}
	if record.Value != nil {
		t.Errorf("Expected nil value for delete, got %s", string(record.Value))
	}
}

// TestWALLargeRecord tests handling of large records
func TestWALLargeRecord(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Create large value (1MB)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err = wal.WritePut(1, []byte("large_key"), largeValue)
	if err != nil {
		t.Fatalf("Failed to write large record: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Read back and verify
	walPath := filepath.Join(tempDir, "000001.wal")
	reader, err := NewWALReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	record, err := reader.ReadRecord()
	if err != nil {
		t.Fatalf("Failed to read large record: %v", err)
	}

	if len(record.Value) != len(largeValue) {
		t.Errorf("Expected value length %d, got %d", len(largeValue), len(record.Value))
	}

	// Verify content
	for i, b := range record.Value {
		if b != largeValue[i] {
			t.Errorf("Value mismatch at position %d: expected %d, got %d", i, largeValue[i], b)
			break
		}
	}
}

// TestWALFileCreation tests that WAL files are created correctly
func TestWALFileCreation(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 42, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Check that file was created with correct name
	expectedPath := filepath.Join(tempDir, "000042.wal")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Fatalf("Expected WAL file %s was not created", expectedPath)
	}

	// Write something to ensure file has content
	err = wal.WritePut(1, []byte("test"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to write to WAL: %v", err)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Check file size is non-zero
	info, err := os.Stat(expectedPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}
	if info.Size() == 0 {
		t.Errorf("WAL file should not be empty after write and sync")
	}
}

// TestWALSyncTiming tests sync timing behavior
func TestWALSyncTiming(t *testing.T) {
	tempDir := t.TempDir()

	// Create WAL with 20ms minimum sync interval
	minInterval := 60 * time.Millisecond
	wal, err := NewWAL(makeOpts(tempDir, 1, 0, minInterval, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write a record
	err = wal.WritePut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// First sync should complete immediately
	start := time.Now()
	err = wal.Sync()
	if err != nil {
		t.Fatalf("First sync failed: %v", err)
	}
	firstSyncDuration := time.Since(start)

	// Second sync should be delayed by minInterval
	start = time.Now()
	err = wal.Sync()
	if err != nil {
		t.Fatalf("Second sync failed: %v", err)
	}
	secondSyncDuration := time.Since(start)

	// Second sync should take at least minInterval
	if secondSyncDuration < minInterval {
		t.Errorf("Expected second sync to take at least %v, took %v", minInterval, secondSyncDuration)
	}

	// First sync should be much faster
	if firstSyncDuration > minInterval/2 {
		t.Errorf("First sync took too long: %v", firstSyncDuration)
	}
}

// TestWALConcurrentSyncs tests concurrent sync operations
func TestWALConcurrentSyncs(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 5*time.Millisecond, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write some records
	for i := range 10 {
		err = wal.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write record %d: %v", i, err)
		}
	}

	// Start multiple concurrent syncs
	numGoroutines := 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := wal.Sync()
			if err != nil {
				errors <- err
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent sync error: %v", err)
	}
}

// TestWALAsyncSync tests async sync functionality
func TestWALAsyncSync(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 10*time.Millisecond, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write a record
	err = wal.WritePut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Test multiple async syncs
	sync1 := wal.SyncAsync()
	sync2 := wal.SyncAsync()
	sync3 := wal.SyncAsync()

	// Collect results
	results := make([]error, 3)
	results[0] = <-sync1
	results[1] = <-sync2
	results[2] = <-sync3

	// All should succeed
	for i, err := range results {
		if err != nil {
			t.Errorf("Async sync %d failed: %v", i+1, err)
		}
	}
}

// TestWALConcurrentWrites tests concurrent write operations
func TestWALConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	numGoroutines := 10
	recordsPerGoroutine := 100
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*recordsPerGoroutine)

	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := range recordsPerGoroutine {
				seq := uint64(goroutineID*recordsPerGoroutine + j + 1)
				key := []byte("key" + string(rune(goroutineID)) + string(rune(j)))
				value := []byte("value" + string(rune(goroutineID)) + string(rune(j)))

				err := wal.WritePut(seq, key, value)
				if err != nil {
					errors <- err
				}
			}
		}(i)
	}

	// Wait for all writes to complete
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent write error: %v", err)
	}

	// Sync to ensure all writes are persisted
	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync after concurrent writes: %v", err)
	}
}

// TestWALMultipleFiles tests creating multiple WAL files
func TestWALMultipleFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create multiple WAL files
	fileNums := []uint64{1, 2, 5, 10}

	for _, fileNum := range fileNums {
		wal, err := NewWAL(makeOpts(tempDir, fileNum, 0, 0, 0))
		if err != nil {
			t.Fatalf("Failed to create WAL file %d: %v", fileNum, err)
		}

		// Write a record
		err = wal.WritePut(1, []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write to WAL file %d: %v", fileNum, err)
		}

		err = wal.Close()
		if err != nil {
			t.Fatalf("Failed to close WAL file %d: %v", fileNum, err)
		}

		// Verify file exists
		expectedPath := filepath.Join(tempDir, fmt.Sprintf("%06d.wal", fileNum))

		if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
			t.Errorf("WAL file %s was not created", expectedPath)
		}
	}
}
