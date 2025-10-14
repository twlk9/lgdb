package lgdb

// WriteOptions controls the behavior of write operations
type WriteOptions struct {
	// Sync determines whether the operation waits for the write to be
	// durably committed to persistent storage.
	//
	// If Sync is true, the operation will not return until the data
	// is guaranteed to survive process crashes and power failures.
	// This provides the strongest durability guarantees but may impact
	// performance.
	//
	// If Sync is false, the operation returns as soon as the data is
	// written to the WAL buffer. The data will be synced to disk based
	// on the configured WAL sync intervals. This provides better
	// performance but may result in data loss if the process crashes
	// before the background sync completes.
	//
	// Default: true (for safety, following Pebble's approach)
	Sync bool
}

// DefaultWriteOptions returns the default write options
// Following Pebble's safety-first approach: sync by default
func DefaultWriteOptions() *WriteOptions {
	return &WriteOptions{
		Sync: true, // Sync by default for safety (matches Pebble)
	}
}

// DurableWriteOptions returns write options optimized for durability
// Alias for DefaultWriteOptions() - kept for backward compatibility
func DurableWriteOptions() *WriteOptions {
	return &WriteOptions{
		Sync: true, // Sync every write for maximum durability
	}
}

// Predefined WriteOptions following Pebble's approach
var (
	// Sync is a predefined WriteOptions that forces sync on every write
	// Provides maximum durability guarantees
	Sync = &WriteOptions{Sync: true}

	// NoSync is a predefined WriteOptions that uses async writes
	// Provides better performance but may lose recent writes on crash
	NoSync = &WriteOptions{Sync: false}
)

// ReadOptions controls the behavior of read operations
type ReadOptions struct {
	// DontFillCache indicates whether blocks read by this operation should be
	// added to the block cache. If true, blocks will not be cached.
	// This is useful for operations like range scans that might pollute the cache.
	NoBlockCache bool
}

// DefaultReadOptions returns the default read options
func DefaultReadOptions() *ReadOptions {
	return &ReadOptions{
		NoBlockCache: false,
	}
}
