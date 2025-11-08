//go:build integration
// +build integration

package lgdb

import (
	"fmt"
	"testing"

	"github.com/twlk9/lgdb/keys"
)

func TestLargePrefixScan(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Open database with small buffer to force compaction
	opts := DefaultOptions()
	opts.Path = tempDir
	opts.WriteBufferSize = 512   // Small buffer to force multiple flushes
	opts.L0CompactionTrigger = 4 // Trigger compaction sooner
	db, err := Open(opts)
	if err != nil {
	}
	defer db.Close()

	// Insert lots of data: key0001 through key9999
	t.Log("Inserting 9999 keys...")
	for i := 1; i <= 9999; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		value := []byte(fmt.Sprintf("value%04d", i))
		err := db.Put(key, value)
		if err != nil {
		}
	}

	// Force flush and wait for compaction
	err = db.Flush()
	if err != nil {
	}
	db.WaitForCompaction()

	// Check database stats
	stats := db.GetStats()
	levels := stats["levels"].(map[string]int)
	t.Logf("After compaction:")
	t.Logf("  L0 files: %d", levels["level_0_files"])
	t.Logf("  L1 files: %d", levels["level_1_files"])
	t.Logf("  L2 files: %d", levels["level_2_files"])
	t.Logf("  L3 files: %d", levels["level_3_files"])

	// Test prefix "key889" which should match key8890, key8891, key8892, ..., key8899 (10 keys)
	t.Log("Testing prefix 'key889'...")
	prefixIter, err := db.ScanPrefix([]byte("key889"), nil)
	if err != nil {
	}
	defer prefixIter.Close()

	var prefixResults []string
	for prefixIter.SeekToFirst(); prefixIter.Valid(); prefixIter.Next() {
		prefixResults = append(prefixResults, string(prefixIter.Key()))
	}
	if err := prefixIter.Error(); err != nil {
		t.Fatal(err)
	}

	// Should get key8890, key8891, ..., key8899 (10 keys)
	expectedCount := 10
	if len(prefixResults) != expectedCount {
		t.Errorf("Expected %d results for prefix 'key889', got %d", expectedCount, len(prefixResults))
	}

	// Verify all results have the correct prefix
	for i, key := range prefixResults {
		if len(key) < 6 || key[:6] != "key889" {
			t.Errorf("Result %d (%s) doesn't have 'key889' prefix", i, key)
		}
	}

	// Test prefix "key1" which should match key1000-key1999 (1000 keys)
	t.Log("Testing prefix 'key1'...")
	prefix1Iter, err := db.ScanPrefix([]byte("key1"), nil)
	if err != nil {
	}
	defer prefix1Iter.Close()

	var prefix1Results []string
	for prefix1Iter.SeekToFirst(); prefix1Iter.Valid(); prefix1Iter.Next() {
		prefix1Results = append(prefix1Results, string(prefix1Iter.Key()))
	}

	// Should get key1000-key1999 (1000 keys)
	expectedCount1 := 1000
	if len(prefix1Results) != expectedCount1 {
		t.Errorf("Expected %d results for prefix 'key1', got %d", expectedCount1, len(prefix1Results))
	}

	// Test prefix "key99" which should match key9900-key9999 (100 keys)
	t.Log("Testing prefix 'key99'...")
	prefix99Iter, err := db.ScanPrefix([]byte("key99"), nil)
	if err != nil {
	}
	defer prefix99Iter.Close()

	var prefix99Results []string
	for prefix99Iter.SeekToFirst(); prefix99Iter.Valid(); prefix99Iter.Next() {
		prefix99Results = append(prefix99Results, string(prefix99Iter.Key()))
	}

	// Should get key9900-key9999 (100 keys)
	expectedCount99 := 100
	if len(prefix99Results) != expectedCount99 {
		t.Errorf("Expected %d results for prefix 'key99', got %d", expectedCount99, len(prefix99Results))
	}

	// Compare with manual bounds for verification
	t.Log("Comparing with manual bounds for 'key889'...")
	bounds := keys.NewRange([]byte("key889"), []byte("key88:"))
	manualIter := db.NewIteratorWithBounds(bounds, nil)
	defer manualIter.Close()

	var manualResults []string
	for manualIter.SeekToFirst(); manualIter.Valid(); manualIter.Next() {
		manualResults = append(manualResults, string(manualIter.Key()))
	}

	if len(manualResults) != len(prefixResults) {
		t.Errorf("Manual bounds (%d) != ScanPrefix (%d)", len(manualResults), len(prefixResults))
	}

	// Final verification
	g, err := db.Get([]byte("key0999"))
	if err != nil {
		t.Error(err)
	}
	if string(g) != "value0999" {
		t.Errorf("got key %s and expected value0999", string(g))
	}
}
