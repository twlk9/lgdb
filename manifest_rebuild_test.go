package lgdb

import (
	"os"
	"path/filepath"
	"testing"
)

// TestManifestRebuild tests the manifest rebuild functionality
func TestManifestRebuild(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.CreateIfMissing = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write some test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for key, value := range testData {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Flush to create SSTables
	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Write more data to create multiple SSTables
	for i := range 100 {
		key := []byte{byte('a' + (i % 26)), byte(i)}
		value := []byte{byte(i), byte(i >> 8)}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put additional data: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush additional data: %v", err)
	}

	// Get stats before closing
	statsBefore := db.GetStats()
	levels := statsBefore["levels"].(map[string]int)
	l0FilesBefore := levels["level_0_files"]

	if l0FilesBefore == 0 {
		t.Fatal("Expected at least one L0 file before rebuild")
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Delete the manifest and CURRENT files to simulate corruption
	manifestFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.manifest"))
	if err != nil {
		t.Fatalf("Failed to list manifest files: %v", err)
	}

	for _, manifestFile := range manifestFiles {
		if err := os.Remove(manifestFile); err != nil {
			t.Fatalf("Failed to remove manifest file: %v", err)
		}
	}

	currentFile := filepath.Join(tmpDir, "CURRENT")
	if err := os.Remove(currentFile); err != nil {
		t.Fatalf("Failed to remove CURRENT file: %v", err)
	}

	// Verify manifest is gone
	if _, err := os.Stat(currentFile); !os.IsNotExist(err) {
		t.Fatal("CURRENT file should not exist after deletion")
	}

	// Rebuild the manifest using the standalone function
	recoveredCount, err := RebuildManifestStandalone(tmpDir, nil, nil)
	if err != nil {
		t.Fatalf("Failed to rebuild manifest: %v", err)
	}

	if recoveredCount == 0 {
		t.Fatal("Expected to recover at least one SSTable file")
	}

	t.Logf("Recovered %d SSTable files", recoveredCount)

	// Verify CURRENT file was created
	if _, err := os.Stat(currentFile); os.IsNotExist(err) {
		t.Fatal("CURRENT file should exist after rebuild")
	}

	// Verify manifest file was created
	manifestFiles, err = filepath.Glob(filepath.Join(tmpDir, "*.manifest"))
	if err != nil {
		t.Fatalf("Failed to list manifest files after rebuild: %v", err)
	}

	if len(manifestFiles) == 0 {
		t.Fatal("Expected at least one manifest file after rebuild")
	}

	// Reopen the database with the rebuilt manifest
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database after rebuild: %v", err)
	}
	defer db.Close()

	// Verify we can read the original data
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s after rebuild: %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}

	// Verify stats after rebuild
	statsAfter := db.GetStats()
	levelsAfter := statsAfter["levels"].(map[string]int)
	l0FilesAfter := levelsAfter["level_0_files"]

	// All files should be in L0 after rebuild
	if l0FilesAfter != recoveredCount {
		t.Errorf("Expected %d files in L0 after rebuild, got %d", recoveredCount, l0FilesAfter)
	}

	t.Logf("Successfully rebuilt manifest and verified data integrity")
}

// TestManifestRebuildWithCompaction tests rebuilding manifest and then running compaction
func TestManifestRebuildWithCompaction(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.CreateIfMissing = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write enough data to trigger multiple levels
	for i := range 1000 {
		key := []byte{byte('k'), byte(i), byte(i >> 8)}
		value := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Delete manifest files
	manifestFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.manifest"))
	if err != nil {
		t.Fatalf("Failed to list manifest files: %v", err)
	}

	for _, manifestFile := range manifestFiles {
		os.Remove(manifestFile)
	}
	os.Remove(filepath.Join(tmpDir, "CURRENT"))

	// Rebuild manifest
	recoveredCount, err := RebuildManifestStandalone(tmpDir, nil, nil)
	if err != nil {
		t.Fatalf("Failed to rebuild manifest: %v", err)
	}

	t.Logf("Recovered %d files", recoveredCount)

	// Reopen and verify compaction works
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Trigger compaction
	if err := db.CompactRange(); err != nil {
		t.Fatalf("Failed to compact after rebuild: %v", err)
	}

	// Verify data is still accessible after compaction
	value, err := db.Get([]byte{byte('k'), 0, 0})
	if err != nil {
		t.Fatalf("Failed to get data after compaction: %v", err)
	}

	if len(value) != 3 {
		t.Errorf("Expected value length 3, got %d", len(value))
	}

	t.Logf("Successfully compacted database after manifest rebuild")
}

// TestManifestRebuildEmpty tests rebuilding with no SSTable files
func TestManifestRebuildEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	// Try to rebuild an empty directory (should fail gracefully)
	_, err := RebuildManifestStandalone(tmpDir, nil, nil)
	if err == nil {
		t.Fatal("Expected error when rebuilding empty directory")
	}

	if err.Error() != "no SSTable files found to rebuild from" {
		t.Errorf("Expected 'no SSTable files found' error, got: %v", err)
	}
}

// TestManifestRebuildOnOpenDB tests using RebuildManifest on an already-open database
func TestManifestRebuildOnOpenDB(t *testing.T) {
	tmpDir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.CreateIfMissing = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write some data
	for i := range 10 {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Call RebuildManifest on the open database
	// This should work even though the DB is open
	recoveredCount, err := db.RebuildManifest()
	if err != nil {
		t.Fatalf("Failed to rebuild manifest on open DB: %v", err)
	}

	if recoveredCount == 0 {
		t.Fatal("Expected to recover at least one file")
	}

	// Verify data is still accessible
	value, err := db.Get([]byte{0})
	if err != nil {
		t.Fatalf("Failed to get data after rebuild: %v", err)
	}

	if len(value) != 1 || value[0] != 0 {
		t.Errorf("Expected value [0], got %v", value)
	}

	db.Close()
}
