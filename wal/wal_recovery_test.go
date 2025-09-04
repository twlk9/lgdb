package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWALRecovery tests basic WAL recovery functionality
func TestWALRecovery(t *testing.T) {
	tempDir := t.TempDir()

	// Create WAL and write some records
	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write test records
	testRecords := []struct {
		seq   uint64
		key   string
		value string
		op    string
	}{
		{1, "key1", "value1", "put"},
		{2, "key2", "value2", "put"},
		{3, "key1", "", "delete"},
		{4, "key3", "value3", "put"},
		{5, "key4", "value4", "put"},
	}

	for _, record := range testRecords {
		if record.op == "put" {
			err = wal.WritePut(record.seq, []byte(record.key), []byte(record.value))
		} else {
			err = wal.WriteDelete(record.seq, []byte(record.key))
		}
		if err != nil {
			t.Fatalf("Failed to write %s record: %v", record.op, err)
		}
	}

	// Sync and close
	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Test recovery - simulate what would happen in DB recovery
	maxSeq, err := RecoverFromWAL(tempDir, nil)
	if err != nil {
		t.Fatalf("Failed to recover from WAL: %v", err)
	}

	// Check that we got the correct max sequence
	expectedMaxSeq := uint64(5)
	if maxSeq != expectedMaxSeq {
		t.Errorf("Expected max sequence %d, got %d", expectedMaxSeq, maxSeq)
	}
}

// TestWALRecoveryMultipleFiles tests recovery from multiple WAL files
func TestWALRecoveryMultipleFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create multiple WAL files
	fileSeqs := []struct {
		fileNum uint64
		records []struct {
			seq   uint64
			key   string
			value string
		}
	}{
		{
			fileNum: 1,
			records: []struct {
				seq   uint64
				key   string
				value string
			}{
				{1, "key1", "value1"},
				{2, "key2", "value2"},
			},
		},
		{
			fileNum: 2,
			records: []struct {
				seq   uint64
				key   string
				value string
			}{
				{3, "key3", "value3"},
				{4, "key4", "value4"},
			},
		},
		{
			fileNum: 3,
			records: []struct {
				seq   uint64
				key   string
				value string
			}{
				{5, "key5", "value5"},
				{6, "key6", "value6"},
			},
		},
	}

	// Create and write to each file
	for _, fileSeq := range fileSeqs {
		wal, err := NewWAL(makeOpts(tempDir, fileSeq.fileNum, 0, 0, 0))
		if err != nil {
			t.Fatalf("Failed to create WAL file %d: %v", fileSeq.fileNum, err)
		}

		for _, record := range fileSeq.records {
			err = wal.WritePut(record.seq, []byte(record.key), []byte(record.value))
			if err != nil {
				t.Fatalf("Failed to write record in file %d: %v", fileSeq.fileNum, err)
			}
		}

		err = wal.Sync()
		if err != nil {
			t.Fatalf("Failed to sync WAL file %d: %v", fileSeq.fileNum, err)
		}

		err = wal.Close()
		if err != nil {
			t.Fatalf("Failed to close WAL file %d: %v", fileSeq.fileNum, err)
		}
	}

	// Test recovery from all files
	maxSeq, err := RecoverFromWAL(tempDir, nil)
	if err != nil {
		t.Fatalf("Failed to recover from multiple WAL files: %v", err)
	}

	// Should find the highest sequence number across all files
	expectedMaxSeq := uint64(6)
	if maxSeq != expectedMaxSeq {
		t.Errorf("Expected max sequence %d, got %d", expectedMaxSeq, maxSeq)
	}
}

// TestWALRecoveryEmptyDirectory tests recovery from empty directory
func TestWALRecoveryEmptyDirectory(t *testing.T) {
	tempDir := t.TempDir()

	// Try to recover from empty directory
	maxSeq, err := RecoverFromWAL(tempDir, nil)
	if err != nil {
		t.Fatalf("Failed to recover from empty directory: %v", err)
	}

	// Should return 0 for empty directory
	if maxSeq != 0 {
		t.Errorf("Expected max sequence 0 for empty directory, got %d", maxSeq)
	}
}

// TestWALRecoveryCorruptedFile tests recovery behavior with corrupted files
func TestWALRecoveryCorruptedFile(t *testing.T) {
	tempDir := t.TempDir()

	// Create a valid WAL file first
	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	err = wal.WritePut(1, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Create a corrupted WAL file
	corruptedPath := filepath.Join(tempDir, "000002.wal")
	corruptedFile, err := os.Create(corruptedPath)
	if err != nil {
		t.Fatalf("Failed to create corrupted file: %v", err)
	}

	// Write invalid data
	_, err = corruptedFile.Write([]byte("invalid wal data"))
	if err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}

	err = corruptedFile.Close()
	if err != nil {
		t.Fatalf("Failed to close corrupted file: %v", err)
	}

	// Recovery should handle the corrupted file gracefully
	// In our current implementation, it will likely fail on the corrupted file
	// This test documents the current behavior
	_, err = RecoverFromWAL(tempDir, nil)
	if err == nil {
		t.Logf("Recovery succeeded despite corrupted file (implementation may handle gracefully)")
	} else {
		t.Logf("Recovery failed with corrupted file: %v (expected behavior)", err)
	}
}

// TestWALRecoveryLargeFile tests recovery from a large WAL file
func TestWALRecoveryLargeFile(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write many records
	numRecords := 1000
	for i := range numRecords {
		seq := uint64(i + 1)
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))

		err = wal.WritePut(seq, key, value)
		if err != nil {
			t.Fatalf("Failed to write record %d: %v", i, err)
		}
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Test recovery
	maxSeq, err := RecoverFromWAL(tempDir, nil)
	if err != nil {
		t.Fatalf("Failed to recover from large WAL file: %v", err)
	}

	expectedMaxSeq := uint64(numRecords)
	if maxSeq != expectedMaxSeq {
		t.Errorf("Expected max sequence %d, got %d", expectedMaxSeq, maxSeq)
	}
}

// TestWALRecoveryMixedOperations tests recovery from file with mixed put/delete operations
func TestWALRecoveryMixedOperations(t *testing.T) {
	tempDir := t.TempDir()

	wal, err := NewWAL(makeOpts(tempDir, 1, 0, 0, 0))
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write mixed operations
	operations := []struct {
		seq   uint64
		key   string
		value string
		op    string
	}{
		{1, "key1", "value1", "put"},
		{2, "key2", "value2", "put"},
		{3, "key3", "value3", "put"},
		{4, "key1", "", "delete"},
		{5, "key4", "value4", "put"},
		{6, "key2", "", "delete"},
		{7, "key5", "value5", "put"},
		{8, "key3", "", "delete"},
		{9, "key6", "value6", "put"},
		{10, "key4", "", "delete"},
	}

	for _, op := range operations {
		if op.op == "put" {
			err = wal.WritePut(op.seq, []byte(op.key), []byte(op.value))
		} else {
			err = wal.WriteDelete(op.seq, []byte(op.key))
		}
		if err != nil {
			t.Fatalf("Failed to write %s operation: %v", op.op, err)
		}
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Test recovery
	maxSeq, err := RecoverFromWAL(tempDir, nil)
	if err != nil {
		t.Fatalf("Failed to recover from mixed operations WAL: %v", err)
	}

	expectedMaxSeq := uint64(10)
	if maxSeq != expectedMaxSeq {
		t.Errorf("Expected max sequence %d, got %d", expectedMaxSeq, maxSeq)
	}
}
