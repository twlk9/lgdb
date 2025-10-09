package sstable

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestBlockCache_BasicPutGet(t *testing.T) {
	cache := NewBlockCache(1024) // 1KB cache
	defer cache.Close()

	key := GenerateCacheKey(1, 0)
	value := []byte("test_value")

	// 1. Test Put and Get
	cache.Put(key, value)
	retrieved, found := cache.Get(key)

	if !found {
		t.Fatal("Expected to find key in cache, but it was not found")
	}
	if !bytes.Equal(retrieved, value) {
		t.Fatalf("Retrieved value does not match original value. Got %s, want %s", retrieved, value)
	}

	// 2. Test Get on non-existent key
	_, found = cache.Get(GenerateCacheKey(2, 0))
	if found {
		t.Fatal("Expected not to find non-existent key, but it was found")
	}
}

func TestBlockCache_ItemLargerThanShardCapacity(t *testing.T) {
	cacheSize := int64(100)
	cache := NewBlockCache(cacheSize) // shardCapacity will be 25
	defer cache.Close()

	key := GenerateCacheKey(1, 0)
	value := make([]byte, 30) // itemSize > shardCapacity

	cache.Put(key, value)

	if _, found := cache.Get(key); found {
		t.Fatal("Expected item larger than shard capacity not to be cached")
	}
}

func TestBlockCache_LRUEviction(t *testing.T) {
	cacheSize := int64(10 * 1024 * 1024) // 10MB
	itemSize := int64(1024)              // 1KB
	numItemsToCache := int(cacheSize / itemSize)
	numItemsToAdd := numItemsToCache + 500 // Add 500 more to force eviction

	cache := NewBlockCache(cacheSize)
	defer cache.Close()

	// Add items to fill and exceed the cache capacity
	for i := range numItemsToAdd {
		key := GenerateCacheKey(uint64(i), 0)
		cache.Put(key, make([]byte, itemSize))
	}

	// The first 500 items should have been evicted.
	evictedCount := 0
	for i := range 500 {
		key := GenerateCacheKey(uint64(i), 0)
		if _, found := cache.Get(key); !found {
			evictedCount++
		}
	}

	// It's hard to guarantee the exact number evicted due to sharding,
	// but a large number should be gone.
	if evictedCount < 400 { // Check that most of the first 500 are gone
		t.Fatalf("Expected at least 400 of the first 500 items to be evicted, but only %d were.", evictedCount)
	}

	// The last 500 items should be present.
	for i := numItemsToAdd - 500; i < numItemsToAdd; i++ {
		key := GenerateCacheKey(uint64(i), 0)
		if _, found := cache.Get(key); !found {
			t.Fatalf("Expected item %d to be in the cache, but it was not found.", i)
		}
	}
}

func TestBlockCache_ConcurrentAccess(t *testing.T) {
	cache := NewBlockCache(10 * 1024 * 1024) // 10MB cache
	defer cache.Close()

	var wg sync.WaitGroup
	numGoroutines := 100
	itemsPerGoroutine := 100

	// Run concurrent writers
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := range itemsPerGoroutine {
				fileNum := uint64(goroutineID)
				offset := uint64(j)
				key := GenerateCacheKey(fileNum, offset)
				value := fmt.Appendf(nil, "value-%d-%d", fileNum, offset)
				cache.Put(key, value)
			}
		}(i)
	}

	wg.Wait()

	// Run concurrent readers to verify data
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := range itemsPerGoroutine {
				fileNum := uint64(goroutineID)
				offset := uint64(j)
				key := GenerateCacheKey(fileNum, offset)
				expectedValue := fmt.Appendf(nil, "value-%d-%d", fileNum, offset)

				retrieved, found := cache.Get(key)
				if !found {
					// It's possible an item was evicted due to cache pressure, so this isn't a fatal error
					// The main purpose of this test is to detect race conditions.
					continue
				}
				if !bytes.Equal(retrieved, expectedValue) {
					t.Errorf("Value mismatch for key %d. Got %s, want %s", key, retrieved, expectedValue)
				}
			}
		}(i)
	}

	wg.Wait()
}
