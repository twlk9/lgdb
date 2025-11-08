//go:build integration
// +build integration

package lgdb

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestVersionCleanup(t *testing.T) {
	opts := DefaultOptions()
	opts.Path = t.TempDir()
	opts.WriteBufferSize = 1024 * 1024

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := make([]byte, 100*1024)

	for i := range 3 {
		t.Logf("=== CYCLE %d ===", i+1)

		// Check version count before
		db.mu.RLock()
		db.versions.mu.Lock()
		versionCountBefore := len(db.versions.versions)
		db.versions.mu.Unlock()
		db.mu.RUnlock()
		t.Logf("Versions before: %d", versionCountBefore)

		// Fill and flush
		for j := range 12 {
			key := fmt.Sprintf("key_%d_%d", i, j)
			if err := db.Put([]byte(key), data); err != nil {
				t.Fatal(err)
			}
		}

		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}

		// Wait for background work
		time.Sleep(50 * time.Millisecond)

		// Check version count after
		db.mu.RLock()
		db.versions.mu.Lock()
		versionCountAfter := len(db.versions.versions)
		db.versions.mu.Unlock()
		db.mu.RUnlock()
		t.Logf("Versions after: %d", versionCountAfter)

		// Check if any versions are marked for removal
		readyCount := 0
		db.mu.RLock()
		db.versions.mu.Lock()
		for _, v := range db.versions.versions {
			if atomic.LoadInt32(&v.cleanedUp) == 1 {
				readyCount++
			}
		}
		db.versions.mu.Unlock()
		db.mu.RUnlock()
		t.Logf("Versions ready for removal: %d", readyCount)

		// Force garbage collection
		runtime.GC()
		runtime.GC()

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("HeapInuse: %d KB, HeapObjects: %d", m.HeapInuse/1024, m.HeapObjects)
	}
}
