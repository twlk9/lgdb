package sstable

import (
	"container/list"
	"hash/fnv"
	"runtime"
	"sync"
)

// BlockCache provides a sharded LRU cache for SSTable data blocks.
// This helps avoid disk reads for frequently accessed blocks.
type BlockCache struct {
	shards []*blockCacheShard
	mu     sync.RWMutex
	closed bool
}

// blockCacheShard is a single shard of the block cache.
type blockCacheShard struct {
	mu       sync.Mutex
	capacity int64
	size     int64
	cache    map[uint64]*cacheEntry
	lru      *list.List
}

// cacheEntry holds the cached block data and its LRU list element.
type cacheEntry struct {
	key     uint64
	value   []byte
	element *list.Element
}

// NewBlockCache creates a new block cache with the specified capacity in bytes.
// The capacity is distributed across a number of shards to reduce lock contention.
func NewBlockCache(capacity int64) *BlockCache {
	if capacity <= 0 {
		return &BlockCache{} // Cache is disabled
	}

	numShards := max(4, 4*runtime.GOMAXPROCS(0))
	shardCapacity := max(1, capacity/int64(numShards))

	bc := &BlockCache{
		shards: make([]*blockCacheShard, numShards),
	}

	for i := range bc.shards {
		bc.shards[i] = &blockCacheShard{
			capacity: shardCapacity,
			cache:    make(map[uint64]*cacheEntry),
			lru:      list.New(),
		}
	}

	return bc
}

// getShard returns the appropriate shard for a given cache key.
func (bc *BlockCache) getShard(key uint64) *blockCacheShard {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	if bc.closed || len(bc.shards) == 0 {
		return nil
	}
	return bc.shards[key%uint64(len(bc.shards))]
}

// Get retrieves a block from the cache. It returns the block and a boolean indicating whether it was found.
func (bc *BlockCache) Get(key uint64) ([]byte, bool) {
	shard := bc.getShard(key)
	if shard == nil {
		return nil, false
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if entry, exists := shard.cache[key]; exists {
		shard.lru.MoveToFront(entry.element)
		return entry.value, true
	}

	return nil, false
}

// Put adds a block to the cache.
func (bc *BlockCache) Put(key uint64, value []byte) {
	shard := bc.getShard(key)
	if shard == nil {
		return
	}

	itemSize := int64(len(value))

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// If item is larger than the entire shard capacity, don't cache it.
	if itemSize > shard.capacity {
		return
	}

	// If item already exists, update it.
	if entry, exists := shard.cache[key]; exists {
		shard.size += itemSize - int64(len(entry.value))
		entry.value = value
		shard.lru.MoveToFront(entry.element)
	} else {
		// Evict until there is space for the new item.
		for shard.size+itemSize > shard.capacity && shard.lru.Len() > 0 {
			shard.evictLRU()
		}
		// Add new entry
		entry := &cacheEntry{
			key:   key,
			value: value,
		}
		entry.element = shard.lru.PushFront(entry)
		shard.cache[key] = entry
		shard.size += itemSize
	}

	// After adding/updating, we might still be over capacity if an existing
	// item was replaced with a much larger one.
	for shard.size > shard.capacity && shard.lru.Len() > 0 {
		shard.evictLRU()
	}
}

// Close clears the block cache.
func (bc *BlockCache) Close() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if bc.closed {
		return
	}
	bc.closed = true

	for _, shard := range bc.shards {
		shard.mu.Lock()
		shard.cache = nil
		shard.lru = nil
		shard.size = 0
		shard.mu.Unlock()
	}
	bc.shards = nil
}

// evictLRU removes the least recently used entry from the shard.
// Must be called with shard.mu held.
func (s *blockCacheShard) evictLRU() {
	if s.lru.Len() == 0 {
		return
	}

	elem := s.lru.Back()
	if elem != nil {
		entry := s.lru.Remove(elem).(*cacheEntry)
		delete(s.cache, entry.key)
		s.size -= int64(len(entry.value))
	}
}

// GenerateCacheKey creates a unique key for a block based on the file number and block offset.
func GenerateCacheKey(fileNum uint64, blockOffset uint64) uint64 {
	h := fnv.New64a()
	h.Write([]byte{
		byte(fileNum), byte(fileNum >> 8), byte(fileNum >> 16), byte(fileNum >> 24),
		byte(fileNum >> 32), byte(fileNum >> 40), byte(fileNum >> 48), byte(fileNum >> 56),
		byte(blockOffset), byte(blockOffset >> 8), byte(blockOffset >> 16), byte(blockOffset >> 24),
		byte(blockOffset >> 32), byte(blockOffset >> 40), byte(blockOffset >> 48), byte(blockOffset >> 56),
	})
	return h.Sum64()
}
