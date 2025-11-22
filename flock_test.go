package lgdb

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileLocking(t *testing.T) {
	// Create a temporary directory for the test database.
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_db")

	opts := DefaultOptions()
	opts.Path = dbPath
	opts.CreateIfMissing = true
	opts.ErrorIfExists = false

	// 1. Open the first database instance. This should acquire the lock.
	db1, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open first database instance: %v", err)
	}
	t.Logf("Successfully opened first DB instance at %s", dbPath)

	// 2. Attempt to open a second database instance in the same directory.
	// This should fail because the lock is already held.
	db2, err := Open(opts)
	if err == nil {
		t.Errorf("Expected an error when opening second DB instance, but it succeeded")
		// Close db2 if it was unexpectedly opened
		if db2 != nil {
			db2.Close()
		}
		db1.Close()
		t.FailNow()
	}

	// 3. Verify that the error indicates a locking issue.
	if err == nil || !errors.Is(err, ErrDBAlreadyOpen) {
		t.Errorf("Expected error to indicate DB already open, got: %v", err)
	} else {
		t.Logf("Successfully detected that the DB is already in use: %v", err)
	}

	// 4. Close the first database instance. This should release the lock.
	t.Log("Closing first DB instance...")
	if err := db1.Close(); err != nil {
		t.Fatalf("Failed to close first database instance: %v", err)
	}
	t.Log("First DB instance closed, lock should be released.")

	// 5. Attempt to open the second database instance again.
	// This should now succeed.
	db3, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open second DB instance after first was closed: %v", err)
	}
	t.Log("Successfully opened second DB instance after first was closed.")

	// 6. Close the third database instance.
	if err := db3.Close(); err != nil {
		t.Fatalf("Failed to close third database instance: %v", err)
	}
	t.Log("Third DB instance closed.")
}

// IsDBAlreadyOpen checks if an error indicates that the database is already open.
func IsDBAlreadyOpen(err error) bool {
	return strings.Contains(err.Error(), "another process might be using the database")
}

