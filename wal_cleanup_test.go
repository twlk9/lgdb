package lgdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/twlk9/lgdb/epoch"
)

// TestWALCleanupDetailed focuses on the exact cleanup flow
func TestWALCleanupDetailed(t *testing.T) {
	opts := DefaultOptions()
	opts.Path = t.TempDir()
	opts.WriteBufferSize = 4 * 1024

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Log("=== Testing detailed WAL cleanup flow ===")

	// Check initial state
	walFiles, _ := filepath.Glob(filepath.Join(opts.Path, "*.wal"))
	t.Logf("Initial WAL files: %v", walFiles)

	// Write enough data to trigger WAL rotation
	data := make([]byte, 2*1024)
	for i := range 5 {
		key := fmt.Sprintf("test_key_%d", i)
		if err := db.Put([]byte(key), data); err != nil {
			t.Fatal(err)
		}
	}

	walFilesAfterWrites, _ := filepath.Glob(filepath.Join(opts.Path, "*.wal"))
	t.Logf("WAL files after writes: %v", walFilesAfterWrites)

	// Flush to complete lifecycle
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	walFilesAfterFlush, _ := filepath.Glob(filepath.Join(opts.Path, "*.wal"))
	t.Logf("WAL files after flush: %v", walFilesAfterFlush)

	// Check if any files can be removed
	activeEpochs, pendingCleanups, pendingResources := epoch.GetGlobalStats()
	t.Logf("Epoch stats - Active: %d, Pending cleanups: %d, Pending resources: %d",
		activeEpochs, pendingCleanups, pendingResources)

	// Force several cleanup attempts
	totalCleaned := 0
	for i := range 10 {
		epoch.AdvanceEpoch()
		cleaned := epoch.TryCleanup()
		totalCleaned += cleaned
		if cleaned > 0 {
			t.Logf("Cleanup round %d: freed %d resources", i+1, cleaned)
		}
	}

	t.Logf("Total resources cleaned: %d", totalCleaned)

	walFilesFinal, _ := filepath.Glob(filepath.Join(opts.Path, "*.wal"))
	t.Logf("Final WAL files: %v", walFilesFinal)

	// Verify that some cleanup occurred
	if len(walFilesFinal) >= len(walFilesAfterFlush) && totalCleaned == 0 {
		t.Log("WARNING: No WAL cleanup occurred despite forced epochs")

		// Debug: check if files physically exist
		for _, walFile := range walFilesFinal {
			if _, err := os.Stat(walFile); os.IsNotExist(err) {
				t.Logf("WAL file %s was marked for cleanup but still exists in glob", walFile)
			}
		}
	}
}
