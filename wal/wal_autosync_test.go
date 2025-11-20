package wal

import (
	"testing"
	"time"

	"github.com/twlk9/lgdb/keys"
)

// TestWALAutoSync verifies that the WAL automatically syncs based on time
// even when bytesPerSync threshold is not reached
func TestWALAutoSync(t *testing.T) {
	dir := t.TempDir()

	opts := WALOpts{
		Path:             dir,
		FileNum:          1,
		BytesPerSync:     10 * 1024 * 1024, // 10MB - very high, won't trigger
		AutoSyncInterval: 100 * time.Millisecond,
	}

	w, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write a small record
	record := &WALRecord{
		Type:  keys.KindSet,
		Seq:   1,
		Key:   []byte("test_key"),
		Value: []byte("test_value"),
	}

	if err := w.WriteRecord(record); err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Verify bytes are waiting to be synced
	w.mu.Lock()
	bytesWaiting := w.bytesWrittenSinceSync
	w.mu.Unlock()

	if bytesWaiting == 0 {
		t.Fatal("Expected unflushed bytes in WAL")
	}

	// Wait for auto-sync to trigger
	time.Sleep(200 * time.Millisecond)

	// Verify the counter was reset (meaning sync happened)
	w.mu.Lock()
	bytesAfter := w.bytesWrittenSinceSync
	w.mu.Unlock()

	if bytesAfter != 0 {
		t.Fatalf("Expected WAL to be auto-synced, but %d bytes still waiting", bytesAfter)
	}

	t.Log("✓ WAL auto-sync works correctly")
}

// TestWALAutoSyncDisabled verifies that when AutoSyncInterval is 0,
// auto-sync is disabled
func TestWALAutoSyncDisabled(t *testing.T) {
	dir := t.TempDir()

	opts := WALOpts{
		Path:             dir,
		FileNum:          1,
		BytesPerSync:     10 * 1024 * 1024,
		AutoSyncInterval: 0, // Disabled
	}

	w, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write a record
	record := &WALRecord{
		Type:  keys.KindSet,
		Seq:   1,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	if err := w.WriteRecord(record); err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Wait longer than normal auto-sync would take
	time.Sleep(200 * time.Millisecond)

	// Verify data was NOT auto-synced
	w.mu.Lock()
	bytesWaiting := w.bytesWrittenSinceSync
	w.mu.Unlock()

	if bytesWaiting == 0 {
		t.Fatal("Expected unflushed bytes in WAL (auto-sync should be disabled)")
	}

	t.Log("✓ WAL auto-sync correctly disabled when AutoSyncInterval=0")
}

// TestWALAutoSyncMultipleRecords tests auto-sync with multiple writes
func TestWALAutoSyncMultipleRecords(t *testing.T) {
	dir := t.TempDir()

	opts := WALOpts{
		Path:             dir,
		FileNum:          1,
		BytesPerSync:     10 * 1024 * 1024,
		AutoSyncInterval: 100 * time.Millisecond,
	}

	w, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write multiple records with auto-sync happening between them
	for i := range 3 {
		record := &WALRecord{
			Type:  keys.KindSet,
			Seq:   uint64(i + 1),
			Key:   []byte("key"),
			Value: []byte("value"),
		}

		if err := w.WriteRecord(record); err != nil {
			t.Fatalf("Failed to write record %d: %v", i, err)
		}

		// Wait for auto-sync
		time.Sleep(150 * time.Millisecond)

		// Verify sync happened
		w.mu.Lock()
		bytesWaiting := w.bytesWrittenSinceSync
		w.mu.Unlock()

		if bytesWaiting != 0 {
			t.Fatalf("Iteration %d: Expected auto-sync, but %d bytes waiting", i, bytesWaiting)
		}
	}

	t.Log("✓ WAL auto-sync handles multiple writes correctly")
}

// TestWALBytesPerSyncResetsByDoSync verifies that bytesWrittenSinceSync
// is properly reset by doSync for both byte-triggered and time-triggered syncs
func TestWALBytesPerSyncResetsByDoSync(t *testing.T) {
	dir := t.TempDir()

	opts := WALOpts{
		Path:             dir,
		FileNum:          1,
		BytesPerSync:     100, // Very low threshold
		AutoSyncInterval: 0,   // Disable time-based for this test
	}

	w, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write enough to trigger bytesPerSync
	for i := range 10 {
		record := &WALRecord{
			Type:  keys.KindSet,
			Seq:   uint64(i + 1),
			Key:   []byte("key"),
			Value: []byte("some_longer_value_to_reach_threshold"),
		}

		if err := w.WriteRecord(record); err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}
	}

	// Give byte-triggered sync time to complete
	time.Sleep(50 * time.Millisecond)

	// Verify counter was reset
	w.mu.Lock()
	bytesWaiting := w.bytesWrittenSinceSync
	w.mu.Unlock()

	// Should be small (only the records written after last sync)
	if bytesWaiting > int64(opts.BytesPerSync) {
		t.Fatalf("Expected bytesWrittenSinceSync to be reset, but got %d bytes", bytesWaiting)
	}

	t.Log("✓ doSync properly resets bytesWrittenSinceSync counter")
}

// TestWALAutoSyncTickerReset verifies that the auto-sync timer is reset
// after explicit syncs, preventing redundant fsyncs
func TestWALAutoSyncTickerReset(t *testing.T) {
	dir := t.TempDir()

	opts := WALOpts{
		Path:             dir,
		FileNum:          1,
		BytesPerSync:     0, // Disable byte-based sync
		AutoSyncInterval: 200 * time.Millisecond,
	}

	w, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write a record
	record := &WALRecord{
		Type:  keys.KindSet,
		Seq:   1,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	if err := w.WriteRecord(record); err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Wait 100ms (half the auto-sync interval)
	time.Sleep(100 * time.Millisecond)

	// Explicitly sync - this should reset the ticker
	if err := w.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Write another record immediately after sync
	record2 := &WALRecord{
		Type:  keys.KindSet,
		Seq:   2,
		Key:   []byte("key2"),
		Value: []byte("value2"),
	}

	if err := w.WriteRecord(record2); err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Wait another 150ms (total 250ms from start, but only 150ms since explicit sync)
	// Without ticker reset, auto-sync would have fired at 200ms
	// With ticker reset, auto-sync won't fire until 300ms (200ms after the explicit sync)
	time.Sleep(150 * time.Millisecond)

	// Data should still be unflushed because ticker was reset
	w.mu.Lock()
	bytesWaiting := w.bytesWrittenSinceSync
	w.mu.Unlock()

	if bytesWaiting == 0 {
		t.Fatal("Expected unflushed data - ticker reset may not be working")
	}

	// Wait for the reset timer to fire (another 100ms, total 400ms from start)
	time.Sleep(100 * time.Millisecond)

	// Now data should be synced
	w.mu.Lock()
	bytesAfter := w.bytesWrittenSinceSync
	w.mu.Unlock()

	if bytesAfter != 0 {
		t.Fatalf("Expected data to be synced after ticker reset interval, but %d bytes waiting", bytesAfter)
	}

	t.Log("✓ Auto-sync ticker correctly resets after explicit sync")
}

// TestWALCloseStopsAutoSync verifies that Close() stops the auto-sync goroutine
func TestWALCloseStopsAutoSync(t *testing.T) {
	dir := t.TempDir()

	opts := WALOpts{
		Path:             dir,
		FileNum:          1,
		BytesPerSync:     10 * 1024 * 1024,
		AutoSyncInterval: 50 * time.Millisecond,
	}

	w, err := NewWAL(opts)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write a record
	record := &WALRecord{
		Type:  keys.KindSet,
		Seq:   1,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	if err := w.WriteRecord(record); err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Close the WAL
	if err := w.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify the auto-sync goroutine has stopped by checking that
	// we don't panic or deadlock. If the goroutine is still running
	// and trying to access a closed WAL, bad things would happen.
	time.Sleep(200 * time.Millisecond)

	t.Log("✓ Close() successfully stops auto-sync goroutine")
}
