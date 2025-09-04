package lgdb

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDisableWAL verifies that DisableWAL option works correctly
func TestDisableWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "TestDisableWAL")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Test with WAL disabled
	opts := DefaultOptions()
	opts.Path = tmpDir
	opts.DisableWAL = true

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// Verify WAL is not created
	if db.wal != nil {
		t.Fatal("Expected WAL to be nil when DisableWAL is true")
	}

	// Basic operations should still work
	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	value, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", string(value))
	}

	// Check that no WAL files exist
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".wal" {
			t.Fatalf("Found WAL file %s when DisableWAL is true", file.Name())
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Test with WAL enabled (default)
	tmpDir2, err := os.MkdirTemp("", "TestEnableWAL")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir2)

	opts2 := DefaultOptions()
	opts2.Path = tmpDir2
	opts2.DisableWAL = false // explicitly enable (should be default)

	db2, err := Open(opts2)
	if err != nil {
		t.Fatal(err)
	}

	// Verify WAL is created
	if db2.wal == nil {
		t.Fatal("Expected WAL to be created when DisableWAL is false")
	}

	err = db2.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	err = db2.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Check that WAL files exist
	files2, err := os.ReadDir(tmpDir2)
	if err != nil {
		t.Fatal(err)
	}

	walFound := false
	for _, file := range files2 {
		if filepath.Ext(file.Name()) == ".wal" {
			walFound = true
			break
		}
	}

	if !walFound {
		t.Fatal("Expected to find WAL files when DisableWAL is false")
	}
}
