package lgdb

import (
	"container/list"
	"encoding/binary"
	"hash/fnv"
	"log/slog"
	"os"
	"runtime"
	"sync"

	"github.com/twlk9/lgdb/sstable"
)

// FileCache provides a sharded LRU cache for SSTableReader instances
// to avoid repeatedly opening/closing files during reads.
type FileCache struct {
	shards     []*fileCacheShard
	mu         sync.RWMutex
	closed     bool
	logger     *slog.Logger
	blockCache *sstable.BlockCache
}

// fileCacheShard is a single shard of the file cache with its own LRU list and mutex
type fileCacheShard struct {
	mu       sync.RWMutex
	capacity int
	cache    map[uint64]*fileCacheEntry
	lru      *list.List
}

// fileCacheEntry holds a cached SSTableReader with its LRU list element
type fileCacheEntry struct {
	fileNum uint64
	reader  *sstable.SSTableReader
	element *list.Element
}

// NewFileCache creates a new file cache with the specified capacity.
// Uses 4 shards per CPU core for reduced contention, similar to Pebble's approach.
func NewFileCache(capacity int, blockCache *sstable.BlockCache, logger *slog.Logger) *FileCache {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1})) // Effectively disable logging
	}
	// Handle zero capacity case - disable caching entirely
	if capacity <= 0 {
		return &FileCache{
			shards: []*fileCacheShard{
				{
					capacity: 0,
					cache:    make(map[uint64]*fileCacheEntry),
					lru:      list.New(),
				},
			},
			logger:     logger,
			blockCache: blockCache,
		}
	}

	numShards := max(4, 4*runtime.GOMAXPROCS(0))
	numShards = min(numShards, capacity)

	shardCapacity := max(1, capacity/numShards)

	fc := &FileCache{
		shards:     make([]*fileCacheShard, numShards),
		logger:     logger,
		blockCache: blockCache,
	}

	for i := range fc.shards {
		fc.shards[i] = &fileCacheShard{
			capacity: shardCapacity,
			cache:    make(map[uint64]*fileCacheEntry),
			lru:      list.New(),
		}
	}

	return fc
}

// getShard returns the appropriate shard for a given file number
func (fc *FileCache) getShard(fileNum uint64) *fileCacheShard {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	if fc.closed {
		return nil
	}

	h := fnv.New64a()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], fileNum)
	h.Write(b[:])
	return fc.shards[h.Sum64()%uint64(len(fc.shards))]
}

// Get retrieves an SSTableReader from the cache, or creates a new one if not cached.
// File cleanup is handled through epoch-based resource management.
func (fc *FileCache) Get(fileNum uint64, path string) (*CachedReader, error) {
	shard := fc.getShard(fileNum)
	if shard == nil {
		return nil, ErrClosed
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// If capacity is 0, don't cache - just create a new reader each time
	if shard.capacity == 0 {
		reader, err := sstable.NewSSTableReader(path, fileNum, fc.blockCache, fc.logger)
		if err != nil {
			fc.logger.Error("CRITICAL: Failed to open SSTable file (uncached)", "error", err,
				"file_num", fileNum,
				"path", path,
				"error_type", "file_access_failure")
			return nil, err
		}

		// The reader is not cached, so we must schedule its cleanup within the current
		// epoch to prevent resource leaks. The epoch guard ensures Close() is called
		// only after the reader is no longer in use by the current operation.
		epoch.ScheduleCleanup(func() error {
			return reader.Close()
		})

		// Create a temporary entry that won't be cached
		entry := &fileCacheEntry{
			fileNum: fileNum,
			reader:  reader,
		}

		return &CachedReader{entry: entry}, nil
	}

	// Check if entry exists in cache
	if entry, exists := shard.cache[fileNum]; exists {
		// Move to front of LRU list
		shard.lru.MoveToFront(entry.element)
		return &CachedReader{entry: entry}, nil
	}

	// Entry not in cache, create new SSTableReader
	reader, err := sstable.NewSSTableReader(path, fileNum, fc.blockCache, fc.logger)
	if err != nil {
		fc.logger.Error("CRITICAL: Failed to open SSTable file (cached)", "error", err,
			"file_num", fileNum,
			"path", path,
			"error_type", "file_access_failure",
			"cache_size", shard.lru.Len(),
			"cache_capacity", shard.capacity)
		return nil, err
	}

	// File deletion is handled by Version epoch management
	// FileCache only needs to handle SSTableReader lifecycle

	// Check if we need to evict
	if shard.lru.Len() >= shard.capacity {
		shard.evictLRU()
	}

	// Add new entry to cache
	entry := &fileCacheEntry{
		fileNum: fileNum,
		reader:  reader,
	}
	entry.element = shard.lru.PushFront(entry)
	shard.cache[fileNum] = entry

	return &CachedReader{entry: entry}, nil
}

// Evict removes a specific file from the cache.
// Used by compaction as a hint that the file is no longer needed.
// SSTableReader cleanup is deferred to the epoch system for safety.
func (fc *FileCache) Evict(fileNum uint64) {
	shard := fc.getShard(fileNum)
	if shard == nil {
		return
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if entry, exists := shard.cache[fileNum]; exists {
		shard.removeFromCache(entry)
	}
}

// Close closes the file cache and all cached readers
func (fc *FileCache) Close() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.closed {
		return nil // Already closed
	}
	fc.closed = true

	for _, shard := range fc.shards {
		shard.mu.Lock()
		for _, entry := range shard.cache {
			if entry.reader != nil {
				entry.reader.Close()
			}
		}
		shard.cache = nil
		shard.lru = nil
		shard.mu.Unlock()
	}

	return nil
}

// evictLRU removes the least recently used entry from the shard
// Must be called with shard.mu held
func (s *fileCacheShard) evictLRU() {
	if s.lru.Len() == 0 {
		return
	}

	// Remove LRU entry (back of list)
	elem := s.lru.Back()
	if elem != nil {
		entry := elem.Value.(*fileCacheEntry)
		s.removeFromCache(entry)
	}
}

// removeFromCache removes an entry from the cache map and LRU list
// SSTableReader cleanup is deferred to the epoch system
// Must be called with shard.mu held
func (s *fileCacheShard) removeFromCache(entry *fileCacheEntry) {
	// Remove from cache and LRU if not already done
	if entry.element != nil {
		delete(s.cache, entry.fileNum)
		s.lru.Remove(entry.element)
		entry.element = nil // Mark as removed
	}

	// DO NOT close the reader here!
	// The SSTableReader will be kept alive by the file descriptor and any code holding
	// references (iterators via Versions, compaction, etc.). The actual file cleanup
	// happens when the Version that registered it is cleaned up by the epoch system.
	// Simply clearing the cache reference allows the LRU to make room for new files
	// while letting active readers keep using the SSTableReader they already have.
	entry.reader = nil
}

// CachedReader wraps an SSTableReader from the file cache
type CachedReader struct {
	entry *fileCacheEntry
}

// Reader returns the underlying SSTableReader
func (cr *CachedReader) Reader() *sstable.SSTableReader {
	if cr.entry == nil {
		return nil
	}
	return cr.entry.reader
}
