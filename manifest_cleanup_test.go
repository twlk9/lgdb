package lgdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/twlk9/lgdb/epoch"
)

// TestManifestFileCleanup tests that old manifest files are cleaned up after rotation
func TestManifestFileCleanup(t *testing.T) {
	dir := t.TempDir()

	// Create database with small manifest file size to trigger rotation
	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxManifestFileSize = 100   // Very small to force rotation
	opts.WriteBufferSize = 1024 * 50 // Small buffer to trigger flush quickly

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Helper function to count manifest files
	countManifestFiles := func() (int, []string) {
		manifestFiles, err := filepath.Glob(filepath.Join(dir, "*.manifest"))
		if err != nil {
			t.Fatalf("Failed to list manifest files: %v", err)
		}
		return len(manifestFiles), manifestFiles
	}

	// Helper function to get current manifest number
	getCurrentManifestNum := func() uint64 {
		db.mu.RLock()
		defer db.mu.RUnlock()
		if db.versions.manifestWriter == nil {
			return 0
		}
		return db.versions.manifestWriter.GetFileNum()
	}

	// Write enough data to trigger a memtable flush and manifest creation
	// Need to write enough to exceed WriteBufferSize
	largeValue := make([]byte, 1024)
	for i := range 200 {
		key := fmt.Sprintf("init_key_%06d", i)
		if err := db.Put([]byte(key), largeValue); err != nil {
			t.Fatalf("Failed to put initial key: %v", err)
		}
	}
	db.WaitForCompaction() // Wait for flush to complete

	// Now should have a manifest file
	count, files := countManifestFiles()
	if count == 0 {
		t.Fatalf("No manifest file created after initial writes (wrote 200 keys)")
	}
	initialManifest := getCurrentManifestNum()
	t.Logf("Initial manifest: %06d.manifest", initialManifest)

	// Write enough data to trigger multiple memtable flushes and compactions
	// This will cause many manifest writes and eventually trigger rotation
	numKeys := 1000
	largeValue2 := make([]byte, 512)
	for i := range numKeys {
		key := fmt.Sprintf("key_%06d", i)
		if err := db.Put([]byte(key), largeValue2); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Wait for background work
	db.WaitForCompaction()

	// Check if manifest rotation occurred
	currentManifest := getCurrentManifestNum()
	t.Logf("Current manifest after writes: %06d.manifest", currentManifest)

	count, files = countManifestFiles()
	t.Logf("Manifest files after writes: count=%d, files=%v", count, files)

	// If rotation occurred, we should eventually have only 1 manifest file after cleanup
	if currentManifest > initialManifest {
		t.Logf("Manifest rotation occurred: %d -> %d", initialManifest, currentManifest)

		// Advance epoch and trigger cleanup to allow old manifests to be deleted
		maxAttempts := 20
		cleanedUp := false
		for attempt := range maxAttempts {
			epoch.AdvanceEpoch() // Advance epoch to make old resources eligible for cleanup
			cleaned := epoch.TryCleanup()

			count, files = countManifestFiles()
			t.Logf("Attempt %d: advanced epoch, cleaned %d resources, %d manifest files remain: %v",
				attempt+1, cleaned, count, files)

			// Check if old manifest files are gone
			if count == 1 {
				// Verify the remaining file is the current manifest
				expectedFile := fmt.Sprintf("%06d.manifest", currentManifest)
				if strings.Contains(files[0], expectedFile) {
					t.Logf("✓ Old manifest files successfully cleaned up")
					cleanedUp = true
					break
				}
			}
		}

		if !cleanedUp {
			t.Logf("Runtime cleanup did not complete (expected due to active epochs)")
			t.Logf("Testing startup cleanup instead...")

			// Close and reopen to test startup cleanup
			if err := db.Close(); err != nil {
				t.Fatalf("Failed to close database: %v", err)
			}

			db, err = Open(opts)
			if err != nil {
				t.Fatalf("Failed to reopen database: %v", err)
			}
			defer db.Close()

			count, files = countManifestFiles()
			t.Logf("After reopen: %d manifest files: %v", count, files)

			if count != 1 {
				t.Errorf("Expected 1 manifest file after startup cleanup, got %d: %v", count, files)
			} else {
				t.Logf("✓ Startup cleanup successfully removed old manifest files")
			}
			return
		}
	} else {
		t.Logf("Manifest rotation did not occur (manifest size may not have reached threshold)")
	}

	// Close database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

// TestManifestCleanupOnStartup tests that old manifest files are cleaned up when opening a database
func TestManifestCleanupOnStartup(t *testing.T) {
	dir := t.TempDir()

	// Create database and write some data
	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxManifestFileSize = 100

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write data to trigger manifest writes
	for i := range 500 {
		key := fmt.Sprintf("key_%06d", i)
		value := fmt.Sprintf("value_%06d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Wait for background work to complete
	db.WaitForCompaction()

	// Get current manifest number before closing
	db.mu.RLock()
	if db.versions.manifestWriter == nil {
		db.mu.RUnlock()
		t.Skip("No manifest writer created - skipping test")
		return
	}
	currentManifest := db.versions.manifestWriter.GetFileNum()
	db.mu.RUnlock()

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Manually create some old manifest files to simulate leftover files
	oldManifests := []uint64{currentManifest - 3, currentManifest - 2, currentManifest - 1}
	for _, oldNum := range oldManifests {
		if oldNum > 0 {
			oldPath := filepath.Join(dir, fmt.Sprintf("%06d.manifest", oldNum))
			// Create empty file
			if err := os.WriteFile(oldPath, []byte("dummy"), 0644); err != nil {
				t.Logf("Failed to create dummy manifest %s: %v", oldPath, err)
			} else {
				t.Logf("Created dummy old manifest: %06d.manifest", oldNum)
			}
		}
	}

	// Count manifest files before reopening
	manifestFilesBefore, err := filepath.Glob(filepath.Join(dir, "*.manifest"))
	if err != nil {
		t.Fatalf("Failed to list manifest files: %v", err)
	}
	t.Logf("Manifest files before reopen: %d files: %v", len(manifestFilesBefore), manifestFilesBefore)

	// Reopen database - should clean up old manifest files
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Count manifest files after reopening
	manifestFilesAfter, err := filepath.Glob(filepath.Join(dir, "*.manifest"))
	if err != nil {
		t.Fatalf("Failed to list manifest files: %v", err)
	}
	t.Logf("Manifest files after reopen: %d files: %v", len(manifestFilesAfter), manifestFilesAfter)

	// Should have exactly 1 manifest file (the current one)
	if len(manifestFilesAfter) != 1 {
		t.Errorf("Expected exactly 1 manifest file after reopen, got %d: %v",
			len(manifestFilesAfter), manifestFilesAfter)
	}

	// Verify the remaining file is the current manifest
	db.mu.RLock()
	currentManifestAfterReopen := db.versions.manifestWriter.GetFileNum()
	db.mu.RUnlock()

	expectedFile := fmt.Sprintf("%06d.manifest", currentManifestAfterReopen)
	found := false
	for _, file := range manifestFilesAfter {
		if strings.HasSuffix(file, expectedFile) {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Current manifest file %s not found in remaining files: %v",
			expectedFile, manifestFilesAfter)
	}
}

// TestManifestRotationAndCleanup tests the complete manifest rotation and cleanup flow
func TestManifestRotationAndCleanup(t *testing.T) {
	dir := t.TempDir()

	// Create database with very small manifest size to force rotation quickly
	opts := DefaultOptions()
	opts.Path = dir
	opts.MaxManifestFileSize = 50     // Very small to trigger rotation quickly
	opts.WriteBufferSize = 1024 * 100 // Small write buffer to trigger more flushes

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Track manifest numbers we've seen
	seenManifests := make(map[uint64]bool)

	getCurrentManifest := func() uint64 {
		db.mu.RLock()
		defer db.mu.RUnlock()
		if db.versions.manifestWriter == nil {
			return 0
		}
		return db.versions.manifestWriter.GetFileNum()
	}

	// Write some initial data to ensure manifest is created
	// Need to exceed WriteBufferSize (100KB) to trigger flush
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("init_%06d", i)
		value := make([]byte, 512)
		if err := db.Put([]byte(key), value); err != nil {
			t.Fatalf("Failed to write initial data: %v", err)
		}
	}
	db.WaitForCompaction()

	initialManifest := getCurrentManifest()
	if initialManifest == 0 {
		t.Fatal("No manifest created after initial writes")
	}
	seenManifests[initialManifest] = true
	t.Logf("Initial manifest: %06d", initialManifest)

	// Write data and track manifest rotations
	rotationCount := 0
	for i := range 2000 {
		key := fmt.Sprintf("key_%08d", i)
		value := fmt.Sprintf("value_%08d_with_some_extra_padding_to_make_it_larger", i)

		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}

		// Periodically check for rotation
		if i%100 == 0 {
			currentManifest := getCurrentManifest()
			if !seenManifests[currentManifest] {
				rotationCount++
				t.Logf("Rotation %d detected: manifest %06d -> %06d",
					rotationCount, initialManifest, currentManifest)
				seenManifests[currentManifest] = true
			}
		}
	}

	// Wait for background work
	db.WaitForCompaction()

	// Advance epoch and trigger cleanup
	for range 10 {
		epoch.AdvanceEpoch()
		epoch.TryCleanup()
	}

	// Count remaining manifest files
	manifestFiles, err := filepath.Glob(filepath.Join(dir, "*.manifest"))
	if err != nil {
		t.Fatalf("Failed to list manifest files: %v", err)
	}

	t.Logf("Final state: %d rotations occurred, %d manifest files remain: %v",
		rotationCount, len(manifestFiles), manifestFiles)

	// The epoch-based cleanup might not have run yet due to active epochs
	// That's OK - the cleanup on startup will handle it
	if len(manifestFiles) > 1 {
		t.Logf("Multiple manifest files remain (expected due to active epochs): %v", manifestFiles)
		t.Logf("Testing that cleanup-on-startup will handle these...")

		// Close and reopen to trigger startup cleanup
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database: %v", err)
		}

		db, err = Open(opts)
		if err != nil {
			t.Fatalf("Failed to reopen database: %v", err)
		}
		defer db.Close()

		// Now check manifest files after startup cleanup
		manifestFiles, err = filepath.Glob(filepath.Join(dir, "*.manifest"))
		if err != nil {
			t.Fatalf("Failed to list manifest files: %v", err)
		}

		t.Logf("Manifest files after reopen: %v", manifestFiles)

		if len(manifestFiles) != 1 {
			t.Errorf("Expected 1 manifest file after reopen cleanup, got %d: %v",
				len(manifestFiles), manifestFiles)
		} else {
			t.Logf("✓ Startup cleanup successfully removed old manifest files")
		}
	} else {
		t.Logf("✓ Only 1 manifest file remains")
	}

	// Verify we can still read all the data
	for i := range 100 { // Sample check
		key := fmt.Sprintf("key_%08d", i)
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to read key %s: %v", key, err)
		}
		if value == nil {
			t.Errorf("Key %s not found after rotation", key)
		}
	}
}
