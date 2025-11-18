package wal

import (
	"os"
	"testing"
	"time"
)

// TestWALSizeTrackingWithBytesPerSync verifies that WAL size tracking
// works correctly even when bytesPerSync triggers background syncs.
// This is a regression test for the bug where bytesWritten was being
// reset during sync, breaking WAL rotation logic.
func TestWALSizeTrackingWithBytesPerSync(t *testing.T) {
	dir := t.TempDir()

	opts := WALOpts{
		Path:            dir,
		FileNum:         1,
		MinSyncInterval: 0,
		BytesPerSync:    1024, // Sync every 1KB
	}

	wal, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write 10KB of data in small chunks
	// Each record is about 100 bytes, so we'll trigger multiple syncs
	totalBytes := int64(0)
	for i := range 100 {
		key := make([]byte, 50)
		value := make([]byte, 50)

		for j := range key {
			key[j] = byte(i)
			value[j] = byte(i + 1)
		}

		err := wal.WritePut(uint64(i), key, value)
		if err != nil {
			t.Fatalf("Failed to write record %d: %v", i, err)
		}

		// Calculate expected size (header + key length + key + value length + value)
		recordSize := int64(HeaderSize + 4 + len(key) + 4 + len(value))
		totalBytes += recordSize
	}

	// Give background syncs time to complete
	time.Sleep(100 * time.Millisecond)

	// Size() should return the total bytes written, not zero
	reportedSize := wal.Size()

	if reportedSize == 0 {
		t.Errorf("WAL.Size() returned 0, but we wrote %d bytes - bytesPerSync may have incorrectly reset the size counter", totalBytes)
	}

	if reportedSize != totalBytes {
		t.Errorf("WAL.Size() = %d, want %d (difference: %d)", reportedSize, totalBytes, totalBytes-reportedSize)
	}

	// Sync to flush buffers to disk
	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Verify the file on disk actually has the expected size after sync
	fileInfo, err := os.Stat(wal.path)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if fileInfo.Size() != totalBytes {
		t.Errorf("WAL file size on disk = %d, want %d", fileInfo.Size(), totalBytes)
	}

	t.Logf("Successfully wrote %d bytes, Size() reports %d, disk file is %d bytes",
		totalBytes, reportedSize, fileInfo.Size())
}

// TestWALRotationWithBytesPerSync verifies that the WAL rotation logic
// in the DB layer will work correctly with bytesPerSync enabled.
func TestWALRotationWithBytesPerSync(t *testing.T) {
	dir := t.TempDir()

	opts := WALOpts{
		Path:            dir,
		FileNum:         1,
		MinSyncInterval: 0,
		BytesPerSync:    512, // Small sync threshold
	}

	wal, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write enough data to trigger multiple background syncs
	targetSize := int64(5000) // 5KB
	writtenBytes := int64(0)

	for writtenBytes < targetSize {
		key := []byte("test-key-with-some-length")
		value := []byte("test-value-with-some-length-too")

		err := wal.WritePut(uint64(writtenBytes), key, value)
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}

		recordSize := int64(HeaderSize + 4 + len(key) + 4 + len(value))
		writtenBytes += recordSize
	}

	// Give background syncs time to complete
	time.Sleep(100 * time.Millisecond)

	size := wal.Size()

	// The size should be close to what we wrote
	if size < targetSize-100 || size > targetSize+100 {
		t.Errorf("WAL.Size() = %d, expected around %d", size, targetSize)
	}

	// Most importantly, size should NOT be close to zero or bytesPerSync
	if size < 1000 {
		t.Errorf("WAL.Size() = %d is suspiciously small (< 1000), suggests counter was reset", size)
	}

	t.Logf("Wrote approximately %d bytes, Size() reports %d", writtenBytes, size)
}
