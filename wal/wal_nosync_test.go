package wal

import (
	"path/filepath"
	"testing"
	"time"
)

// TestWALNoSyncPerformance tests performance difference with NoSync
func TestWALNoSyncPerformance(t *testing.T) {
	tempDir := t.TempDir()

	// Test with sync enabled
	startTime := time.Now()
	wal1, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL with sync: %v", err)
	}

	numRecords := 1000
	for i := range numRecords {
		err = wal1.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write record with sync: %v", err)
		}

		// Sync every record (simulating Sync=true behavior)
		err = wal1.Sync()
		if err != nil {
			t.Fatalf("Failed to sync: %v", err)
		}
	}

	err = wal1.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL with sync: %v", err)
	}

	syncDuration := time.Since(startTime)

	// Test without sync (simulating NoSync=true behavior)
	startTime = time.Now()
	wal2, err := NewWAL(makeOpts(tempDir, 2, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL without sync: %v", err)
	}

	for i := range numRecords {
		err = wal2.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write record without sync: %v", err)
		}
		// No sync calls - simulating NoSync=true
	}

	err = wal2.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL without sync: %v", err)
	}

	noSyncDuration := time.Since(startTime)

	// NoSync should be significantly faster
	t.Logf("With sync: %v, Without sync: %v", syncDuration, noSyncDuration)

	// This is a performance test - we expect NoSync to be faster
	// but the exact ratio depends on the system
	if noSyncDuration >= syncDuration {
		t.Logf("NoSync wasn't significantly faster - this may be expected on some systems")
	}
}

// TestWALBatchedSyncPerformance tests performance with batched syncs
func TestWALBatchedSyncPerformance(t *testing.T) {
	tempDir := t.TempDir()

	// Test with frequent syncs (every write)
	startTime := time.Now()
	wal1, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	numRecords := 100
	for i := range numRecords {
		err = wal1.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}

		err = wal1.Sync()
		if err != nil {
			t.Fatalf("Failed to sync: %v", err)
		}
	}

	err = wal1.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	frequentSyncDuration := time.Since(startTime)

	// Test with batched syncs (every 10 writes)
	startTime = time.Now()
	wal2, err := NewWAL(makeOpts(tempDir, 2, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL for batched sync: %v", err)
	}

	for i := range numRecords {
		err = wal2.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}

		// Sync every 10 records
		if (i+1)%10 == 0 {
			err = wal2.Sync()
			if err != nil {
				t.Fatalf("Failed to sync: %v", err)
			}
		}
	}

	// Final sync
	err = wal2.Sync()
	if err != nil {
		t.Fatalf("Failed to final sync: %v", err)
	}

	err = wal2.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	batchedSyncDuration := time.Since(startTime)

	t.Logf("Frequent sync: %v, Batched sync: %v", frequentSyncDuration, batchedSyncDuration)

	// Batched syncs should generally be faster
	if batchedSyncDuration >= frequentSyncDuration {
		t.Logf("Batched sync wasn't significantly faster - this may be expected on some systems")
	}
}

// TestWALAsyncSyncBehavior tests async sync behavior for bulk operations
func TestWALAsyncSyncBehavior(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 10*time.Millisecond, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write multiple records
	numRecords := 100
	for i := range numRecords {
		err = wal.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write record %d: %v", i, err)
		}
	}

	// Start multiple async syncs
	numSyncs := 10
	syncChans := make([]<-chan error, numSyncs)

	start := time.Now()
	for i := range numSyncs {
		syncChans[i] = wal.SyncAsync()
	}

	// Wait for all syncs to complete
	for i, syncChan := range syncChans {
		err := <-syncChan
		if err != nil {
			t.Fatalf("Async sync %d failed: %v", i, err)
		}
	}

	totalDuration := time.Since(start)
	t.Logf("Completed %d async syncs in %v", numSyncs, totalDuration)

	// All syncs should complete successfully
	// The actual timing depends on the system and implementation
}

// TestWALSyncBehaviorWithWrites tests sync behavior during concurrent writes
func TestWALSyncBehaviorWithWrites(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 5*time.Millisecond, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write some initial records
	for i := range 10 {
		err = wal.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write initial record %d: %v", i, err)
		}
	}

	// Start an async sync
	syncChan := wal.SyncAsync()

	// Write more records while sync is potentially in progress
	for i := 10; i < 20; i++ {
		err = wal.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write record during sync %d: %v", i, err)
		}
	}

	// Wait for sync to complete
	err = <-syncChan
	if err != nil {
		t.Fatalf("Async sync failed: %v", err)
	}

	// Do a final sync to ensure all writes are persisted
	err = wal.Sync()
	if err != nil {
		t.Fatalf("Final sync failed: %v", err)
	}

	// Verify all records are present by reading the file
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	walPath := filepath.Join(tempDir, "000001.wal")
	reader, err := NewWALReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	// Count records
	recordCount := 0
	for {
		_, err := reader.ReadRecord()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("Failed to read record: %v", err)
		}
		recordCount++
	}

	expectedRecords := 20
	if recordCount != expectedRecords {
		t.Errorf("Expected %d records, found %d", expectedRecords, recordCount)
	}
}

// TestWALBulkLoadingScenario tests a bulk loading scenario
func TestWALBulkLoadingScenario(t *testing.T) {
	tempDir := t.TempDir()

	// Simulate bulk loading with minimal syncing
	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write many records without syncing (simulating NoSync=true)
	numRecords := 1000
	start := time.Now()

	for i := range numRecords {
		err = wal.WritePut(uint64(i+1), []byte("key"), []byte("value"))
		if err != nil {
			t.Fatalf("Failed to write record %d: %v", i, err)
		}

		// Only sync every 100 records (simulating batched bulk loading)
		if (i+1)%100 == 0 {
			err = wal.Sync()
			if err != nil {
				t.Fatalf("Failed to sync at record %d: %v", i, err)
			}
		}
	}

	// Final sync to ensure all data is persisted
	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to final sync: %v", err)
	}

	bulkLoadDuration := time.Since(start)

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	t.Logf("Bulk loaded %d records in %v", numRecords, bulkLoadDuration)

	// Verify all records are present
	walPath := filepath.Join(tempDir, "000001.wal")
	reader, err := NewWALReader(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	recordCount := 0
	for {
		_, err := reader.ReadRecord()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("Failed to read record: %v", err)
		}
		recordCount++
	}

	if recordCount != numRecords {
		t.Errorf("Expected %d records, found %d", numRecords, recordCount)
	}
}
