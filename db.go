package lgdb

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twlk9/lgdb/bufferpool"
	"github.com/twlk9/lgdb/epoch"
	"github.com/twlk9/lgdb/keys"
	"github.com/twlk9/lgdb/memtable"
	"github.com/twlk9/lgdb/sstable"
	"github.com/twlk9/lgdb/wal"
)

// rotateMemtable atomically swaps the active memtable with a new one
// and adds old to immutable list. Must be called with db.mu lock held.
func (db *DB) rotateMemtable(newMemtable *memtable.MemTable) *memtable.MemTable {
	// Swap memtables
	oldMemtable := db.memtable
	db.memtable = newMemtable

	// Add old memtable to immutable list
	db.immutableMemtables = append(db.immutableMemtables, oldMemtable)

	return oldMemtable
}

// removeFromImmutableMemtables safely removes a memtable from the
// immutable list. Must be called with db.mu lock held.
func (db *DB) removeFromImmutableMemtables(targetMemtable *memtable.MemTable) bool {
	for i, immutable := range db.immutableMemtables {
		if immutable == targetMemtable {
			// Remove by preserving order - shift left and truncate
			copy(db.immutableMemtables[i:], db.immutableMemtables[i+1:])
			db.immutableMemtables = db.immutableMemtables[:len(db.immutableMemtables)-1]
			targetMemtable.Close()
			return true
		}
	}
	return false
}

// triggerEpochCleanup triggers epoch cleanup to free old versions and memtables.
// This should be called whenever versions are marked for cleanup.
func (db *DB) triggerEpochCleanup() {
	cleaned := epoch.TryCleanup()
	if cleaned > 0 {
		db.logger.Debug("epoch cleanup freed resources", "cleaned", cleaned)
		// After epoch cleanup, remove old versions from VersionSet
		db.versions.cleanupOldVersions()
	}
}

// DB represents the main database instance.
// Implements an LSM-tree database with memtables, SSTables, WAL, and compaction.
// Uses mutex-based coordination for correctness.
type DB struct {
	// Database options
	options *Options

	// Directory path
	path string

	// Current active memtable (protected by db.mu)
	memtable *memtable.MemTable

	// Immutable memtables being flushed (protected by memtableMu)
	immutableMemtables []*memtable.MemTable

	// Write-ahead log
	wal *wal.WAL

	// Version management
	versions *VersionSet

	// Compaction management
	compactionManager *CompactionManager

	// Mutex for version/manifest operations only
	mu sync.RWMutex

	// Current version for snapshot consistency
	currentVersion atomic.Pointer[Version] // *Version

	// Atomic counters
	seq    atomic.Uint64 // Sequence number for versioning
	closed atomic.Bool   // Closed flag

	// Flush coordination
	flushWg      sync.WaitGroup // Wait for background flusher to finish
	flushTrigger *sync.Cond     // Signal the background flusher to do something
	flushBP      *sync.Cond     // Flushing backpressure conditional

	// structured logger
	logger *slog.Logger

	// File cache for SSTableReader instances
	fileCache *FileCache
}

// Open opens a database with the given options.
// Performs recovery from WAL and manifest, sets up all subsystems,
// and returns a working database. If a database exists, it is recovered.
// If not and CreateIfMissing is set, a new database is created.
func Open(opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	// Initialize logger (use default if none provided)
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}

	// Validate options
	if err := opts.Validate(); err != nil {
		logger.Error("Options did not validate", "error", err, "options", opts)
		return nil, err
	}

	// Check if database exists
	dbExists := false
	if _, err := os.Stat(opts.Path); err == nil {
		dbExists = true
	}

	// Handle ErrorIfExists option
	if opts.ErrorIfExists && dbExists {
		exErr := errors.New("ErrExists")
		logger.Error("database already exists at path", "error", exErr, "path", opts.Path)
		return nil, exErr
	}

	// Handle CreateIfMissing option
	if !opts.CreateIfMissing && !dbExists {
		dnExErr := errors.New("ErrDoesNotExist")
		logger.Error("database does not exist at path", "error", dnExErr, "path", opts.Path)
		return nil, dnExErr
	}

	// Create directory if needed
	if err := os.MkdirAll(opts.Path, 0755); err != nil {
		logger.Error("Making of directory failed", "error", err, "path", opts.Path)
		return nil, err
	}

	db := &DB{
		options:   opts,
		path:      opts.Path,
		logger:    logger,
		fileCache: NewFileCache(opts.GetFileCacheSize(), logger),
	}

	db.flushTrigger = sync.NewCond(&db.mu)
	db.flushBP = sync.NewCond(&db.mu)

	// Initialize version set with manifest support
	db.versions = NewVersionSet(opts.MaxLevels, opts.Path, opts.MaxManifestFileSize, logger)

	// Initialize compaction manager with all dependencies
	db.compactionManager = NewCompactionManager(db.versions, db.fileCache, opts.Path, opts, logger)

	// Initialize atomic fields
	db.seq.Store(0)
	db.closed.Store(false)

	// Initialize memtable
	db.memtable = memtable.NewMemtable(opts.WriteBufferSize)

	// Recover from manifest if exists
	if err := RecoverFromManifest(opts.Path, db.versions); err != nil {
		return nil, err
	}

	// Clean up orphaned SST files after manifest recovery
	if err := db.cleanupOrphanedSSTFiles(); err != nil {
		logger.Warn("failed to cleanup orphaned SST files", "error", err)
	}

	// Recover from WAL if exists (skip in read-only mode)
	var maxSeq uint64
	if !opts.ReadOnly {
		var err error
		maxSeq, err = db.recoverFromWAL()
		if err != nil {
			return nil, err
		}
	}
	db.seq.Store(maxSeq)

	// Initialize read state
	db.updateCurrentVersion()

	// Initialize WAL (skip in read-only mode or if disabled)
	if !opts.ReadOnly && !opts.DisableWAL {
		wo := wal.WALOpts{
			Path:            opts.Path,
			FileNum:         db.versions.NewFileNumber(),
			SyncInterval:    opts.WALSyncInterval,
			MinSyncInterval: opts.WALMinSyncInterval,
			BytesPerSync:    opts.WALBytesPerSync,
		}
		wal, err := wal.NewWAL(wo)
		if err != nil {
			return nil, err
		}
		db.wal = wal
		db.memtable.RegisterWAL(db.wal.Path())
	}

	// Start the background flusher goroutine
	db.flushWg.Add(1)
	go db.backgroundFlusher()

	return db, nil
}

// Close closes the database.
// Shuts down background work, closes files, and cleans up resources.
// Safe to call multiple times.
func (db *DB) Close() error {
	// Check if already closed (atomic check)
	if db.closed.Swap(true) {
		return nil // Already closed
	}

	// Wait for background flusher to finish
	db.flushTrigger.Signal()
	db.flushWg.Wait()

	// Stop compaction manager first (before closing other resources)
	if db.compactionManager != nil {
		db.compactionManager.Close()
	}

	// Close WAL (if enabled)
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			return err
		}
	}

	// Close file cache
	if db.fileCache != nil {
		if err := db.fileCache.Close(); err != nil {
			return err
		}
	}

	// Close version set
	if db.versions != nil {
		if err := db.versions.Close(); err != nil {
			return err
		}
	}

	return nil
}

// backgroundFlusher runs permanently to handle memtable flushing.
// Decouples the write path from flush coordination for better performance.
func (db *DB) backgroundFlusher() {
	defer db.flushWg.Done()

	for {
		db.mu.Lock()

		for len(db.immutableMemtables) == 0 {
			if db.closed.Load() {
				return
			}
			db.flushTrigger.Wait()
		}

		db.mu.Unlock()
		if err := db.waitForL0Backpressure(); err != nil {
			db.logger.Error("waitForL0Backpressure is backgroundFlusher error", "error", err)
		}

		db.mu.Lock()
		mt := db.immutableMemtables[0]
		db.mu.Unlock()

		// quick check for 0 sized memtable. Seems to only happen in
		// testing where Flush is used
		if mt.Size() == 0 {
			db.mu.Lock()
			db.removeFromImmutableMemtables(mt)
			db.flushBP.Broadcast()
			db.mu.Unlock()
			continue
		}

		// Get next SSTable file number atomically
		sstableNum := db.versions.NewFileNumber()

		// Flush the memtable to SSTable
		edit, flushErr := db.flushMemtableToSSTable(mt, sstableNum)
		if flushErr != nil {
			// Fail quick if the memtable flushing fails. Close up
			// shop and go home.
			db.logger.Error("FATAL: memtable fluish failed", "error", flushErr, "sstable_num", sstableNum)
			db.closed.Store(true)
			db.flushBP.Broadcast()
			return
		}

		db.mu.Lock()
		verr := db.versions.LogAndApply(edit)
		if verr != nil {
			db.logger.Error("FATAL: Version Edit Failed (flushing memtable)", "error", verr, "sstable_num", sstableNum)
			db.closed.Store(true)
			db.flushBP.Broadcast()
			db.mu.Unlock()
			return
		}
		db.logger.Info("Version Updated", "file_number", sstableNum,
			"new_version", db.versions.GetCurrentVersionNumber())
		found := db.removeFromImmutableMemtables(mt)
		db.logger.Debug("MEMTABLE COUNT:", "memtable_count", len(db.immutableMemtables))
		db.flushBP.Broadcast()
		if !found {
			db.logger.Warn("Memtable not found after flush", "ramaining_count", len(db.immutableMemtables))
		}
		db.updateCurrentVersion()
		db.mu.Unlock()

		// Try cleanup after version update to free old resources
		db.triggerEpochCleanup()
		db.compactionManager.ScheduleCompaction()
	}
}

// waitForL0Backpressure implements simple polling-based backpressure
// when L0 has too many files, similar to goleveldb's approach
func (db *DB) waitForL0Backpressure() error {
	loggedBackpressure := false
	loopCount := 0
	startTime := time.Now()

	for {
		// Quick check - exit fast if no backpressure needed
		version := db.versions.GetCurrentVersion()
		l0Files := version.GetFiles(0)
		l0Count := len(l0Files)
		version.MarkForCleanup()

		if l0Count < db.options.L0StopWritesTrigger {
			// L0 is not overloaded, proceed with write
			if loggedBackpressure {
				// Log when backpressure ends
				db.logger.Info("L0_BACKPRESSURE_RESOLVED",
					"l0_files", l0Count,
					"wait_loops", loopCount)
			}
			return nil
		}

		// Check if database is being closed
		if db.closed.Load() {
			return ErrDBClosed
		}

		// Timeout protection - prevent infinite hangs
		if time.Since(startTime) > 30*time.Second {
			db.logger.Error("L0_BACKPRESSURE_TIMEOUT",
				"l0_files", l0Count,
				"trigger", db.options.L0StopWritesTrigger,
				"wait_time", time.Since(startTime),
				"wait_loops", loopCount)
			return ErrCompactionTimeout
		}

		// Aggressively trigger compaction every 100 loops (~100ms)
		if loopCount%100 == 0 {
			db.compactionManager.ScheduleCompaction()
		}

		// Log first time and every 1000 loops (roughly every second)
		if !loggedBackpressure || loopCount%1000 == 0 {
			db.logger.Info("L0_BACKPRESSURE_WAITING",
				"l0_files", l0Count,
				"trigger", db.options.L0StopWritesTrigger,
				"wait_loops", loopCount)
			loggedBackpressure = true
		}

		loopCount++

		// Sleep briefly like goleveldb does, then check again
		time.Sleep(1 * time.Millisecond)
	}
}

// rotateWAL rotates the WAL at 64MB. Hardcoded right now. Needs to be
// called under lock.
func (db *DB) rotateWAL() error {
	if db.options.DisableWAL {
		return nil
	}
	// Calculate as num memtables * writebuffersize + 1 for wal
	// storage
	maxsize := db.options.WriteBufferSize*db.options.MaxMemtables + 1
	if db.wal.Size() > int64(maxsize) {
		// Rotate WAL (create new WAL file)
		ogwal := db.wal
		currentFileNum := db.versions.NewFileNumber()
		wo := wal.WALOpts{
			Path:            db.options.Path,
			FileNum:         currentFileNum,
			SyncInterval:    db.options.WALSyncInterval,
			MinSyncInterval: db.options.WALMinSyncInterval,
			BytesPerSync:    db.options.WALBytesPerSync,
		}
		newWAL, err := wal.NewWAL(wo)
		if err != nil {
			return err
		}
		db.wal = newWAL
		if cerr := ogwal.Close(); cerr != nil {
			return cerr
		}
		return nil
	}
	return nil
}

// write is a unified method for writing a record to the database.
// Uses read-write locks to prevent WAL/memtable rotation during writes.
func (db *DB) write(key, value []byte, kind keys.Kind, opts *WriteOptions) error {
	if opts == nil {
		opts = DefaultWriteOptions()
	}
	if !keys.IsValidUserKey(key) {
		return ErrInvalidKey
	}
	if !keys.IsValidValue(value) {
		return ErrInvalidValue
	}

	// Quick atomic checks
	if db.closed.Load() {
		return ErrDBClosed
	}
	if db.options.ReadOnly {
		return ErrReadOnly
	}

	// Check if flush needed and trigger if so
	db.mu.Lock()
	if len(db.immutableMemtables) > db.options.MaxMemtables {
		db.flushBP.Wait()
	}
	if db.memtable.MemoryUsage() >= db.options.WriteBufferSize {
		if walerr := db.rotateWAL(); walerr != nil {
			db.logger.Error("rotating WAL failed during memtable rotation in write", "error", walerr)
		}
		newmemt := memtable.NewMemtable(db.options.WriteBufferSize)
		if db.wal != nil {
			newmemt.RegisterWAL(db.wal.Path())
		}
		db.rotateMemtable(newmemt)
		db.updateCurrentVersion()
		db.flushTrigger.Signal()
	}
	db.mu.Unlock()

	// Use read-lock to prevent WAL/memtable rotation during write
	// This ensures captured WAL/memtable references remain valid
	db.mu.RLock()

	// Atomically increment sequence number
	currSeq := db.seq.Add(1)

	// Write to WAL (has its own internal locking)
	if db.wal != nil {
		walrec := &wal.WALRecord{
			Type:  kind,
			Seq:   currSeq,
			Key:   key,
			Value: value,
		}
		if err := db.wal.WriteRecord(walrec); err != nil {
			db.mu.RUnlock()
			return err
		}

		// Sync write options and global settings
		if db.options.Sync {
			if opts.Sync {
				if err := db.wal.Sync(); err != nil {
					db.mu.RUnlock()
					return err
				}
			} else {
				_ = db.wal.SyncAsync()
			}
		}
	}

	// Create internal key using buffer pool
	ikey := keys.EncodedKey(bufferpool.GetBuffer(len(key) + keys.KeyFootLen))
	defer bufferpool.PutBuffer(ikey)
	ikey.Encode(key, currSeq, kind)
	db.memtable.Put(ikey, value)
	db.mu.RUnlock()
	return nil
}

// Put inserts a key-value pair into the database using default write options.
// Uses synchronous writes by default for safety.
// Writes to WAL first, then memtable, and triggers flush if memtable is full.
func (db *DB) Put(key, value []byte) error {
	return db.PutWithOptions(key, value, DefaultWriteOptions())
}

// PutWithOptions inserts a key-value pair with specified write options.
// The write options control sync behavior - whether we wait for WAL to hit disk.
func (db *DB) PutWithOptions(key, value []byte, opts *WriteOptions) error {
	if opts == nil {
		opts = DefaultWriteOptions()
	}

	return db.write(key, value, keys.KindSet, opts)
}

// Get retrieves a value by key from the database.
// Uses iterator infrastructure for consistent range tombstone handling.
// Returns ErrNotFound if the key doesn't exist or was deleted.
func (db *DB) Get(key []byte) ([]byte, error) {
	if !keys.IsValidUserKey(key) {
		return nil, ErrInvalidKey
	}

	if db.closed.Load() {
		return nil, ErrDBClosed
	}

	// Use iterator infrastructure for unified range tombstone handling
	iter := db.NewIterator()
	defer iter.Close()

	iter.Seek(key)

	// Check if we found the exact key
	if iter.Valid() && bytes.Equal(iter.Key(), key) {
		// Found exact match - return the value directly
		return iter.Value(), nil
	}

	// Key not found
	return nil, ErrNotFound
}

// Delete removes a key from the database.
// Writes a tombstone to WAL and memtable. The key is cleaned up during compaction.
func (db *DB) Delete(key []byte) error {
	// TODO: Make the write options an arg
	opts := DefaultWriteOptions()
	return db.write(key, nil, keys.KindDelete, opts)
}

// DeleteRange removes all keys in the range [startKey, endKey) from the database.
// Uses range tombstones for efficiency - doesn't actually delete individual keys immediately.
// Range tombstones get applied during reads and compaction.
func (db *DB) DeleteRange(startKey, endKey keys.UserKey) error {
	if !keys.IsValidUserKey(startKey) || !keys.IsValidUserKey(endKey) {
		return ErrInvalidKey
	}

	if startKey.Compare(endKey) >= 0 {
		return ErrInvalidRange
	}

	// Use existing write method to add range tombstone to memtable
	return db.write(startKey, endKey, keys.KindRangeDelete, nil)
}

// prefixSuccessor returns the smallest key larger than any key with the given prefix.
// Returns nil if no such key exists (prefix consists only of 0xFF bytes).
// Used to create upper bounds for prefix scans.
func prefixSuccessor(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	// Make a copy to avoid modifying the original
	successor := make([]byte, len(prefix))
	copy(successor, prefix)

	// Increment the last byte to create an upper bound
	for i := len(successor) - 1; i >= 0; i-- {
		if successor[i] < 255 {
			successor[i]++
			// Return only the truncated portion up to the incremented byte
			return successor[:i+1]
		}
		successor[i] = 0
	}

	// If we couldn't increment (all bytes were 255), no upper bound exists
	return nil
}

// DeletePrefix removes all keys with the given prefix from the database.
// This is equivalent to DeleteRange(prefix, prefixSuccessor(prefix)).
// Useful for clearing out all keys that start with a specific pattern.
func (db *DB) DeletePrefix(prefix []byte) error {
	if !keys.IsValidUserKey(prefix) {
		return ErrInvalidKey
	}

	if len(prefix) == 0 {
		return ErrInvalidKey
	}

	// Calculate the end key for the prefix range
	endKey := prefixSuccessor(prefix)
	if endKey == nil {
		// If no successor exists, delete from prefix to end of keyspace
		// This is a special case where prefix is all 0xFF bytes
		return ErrInvalidRange
	}

	// Use DeleteRange to handle the actual deletion
	return db.DeleteRange(prefix, endKey)
}

// recoverFromWAL recovers the memtable from WAL files.
// Replays all WAL records to rebuild memtable state after a restart.
// Returns the highest sequence number found in the WAL.
func (db *DB) recoverFromWAL() (uint64, error) {
	walFiles, err := filepath.Glob(filepath.Join(db.path, "*.wal"))
	if err != nil {
		return 0, err
	}

	var maxSeq uint64

	for _, walFile := range walFiles {
		reader, err := wal.NewWALReader(walFile)
		if err != nil {
			return 0, err
		}
		db.memtable.RegisterWAL(reader.Path())

		for {
			record, err := reader.ReadRecord()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				reader.Close()
				return 0, err
			}

			if record.Seq > maxSeq {
				maxSeq = record.Seq
			}

			ikey := keys.NewEncodedKey(record.Key, record.Seq, record.Type)
			db.memtable.Put(ikey, record.Value)
		}

		reader.Close()
	}

	return maxSeq, nil
}

// flushMemtableToSSTable writes a memtable to disk as an SSTable.
// Creates the SSTable file, writes all entries, and returns a version edit.
// This is where memtable data becomes persistent.
func (db *DB) flushMemtableToSSTable(memtable *memtable.MemTable, fileNumber uint64) (*VersionEdit, error) {
	// Create SSTable file path
	sstablePath := filepath.Join(db.path, fmt.Sprintf("%06d.sst", fileNumber))

	// Create SSTable writer
	wopts := sstable.SSTableOpts{
		Path:                 sstablePath,
		Compression:          db.options.Compression,
		BlockSize:            db.options.BlockSize,
		BlockRestartInterval: db.options.BlockRestartInterval,
		BlockMinEntries:      db.options.BlockMinEntries,
	}
	writer, err := sstable.NewSSTableWriter(wopts)
	if err != nil {
		return nil, err
	}

	// Create raw iterator for the memtable to dump all entries without filtering
	iter := memtable.NewIterator()
	iter.SeekToFirst()

	// Track what we're flushing for debugging
	entriesWritten := 0
	var firstKey, lastKey []byte
	memtableSize := memtable.Size()
	memtableMemUsage := memtable.MemoryUsage()

	// Validate memtable has data before flushing
	if memtableSize == 0 {
		return nil, fmt.Errorf("attempted to flush empty memtable - this should not happen")
	}

	// Log flush start with memtable stats
	db.logger.Debug("FLUSH_CONVERSION_START",
		"file_number", fileNumber,
		"memtable_entries", memtableSize,
		"memtable_memory_bytes", memtableMemUsage)

	// Write all entries from memtable to SSTable
	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		// Track key range for debugging
		if entriesWritten == 0 {
			firstKey = make([]byte, len(key.UserKey()))
			copy(firstKey, key.UserKey())
		}
		lastKey = make([]byte, len(key.UserKey()))
		copy(lastKey, key.UserKey())
		entriesWritten++

		// Store internal keys in SSTable to handle tombstones properly (using pooled encoding)
		err := writer.Add(key, value)
		if err != nil {
			return nil, err
		}

		iter.Next()
	}

	// Finish writing the SSTable
	if err := writer.Finish(); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	// Create file metadata for the new SSTable
	fileMetadata := &FileMetadata{
		FileNum:     fileNumber,
		Size:        writer.EstimatedSize(),
		SmallestKey: writer.SmallestKey(),
		LargestKey:  writer.LargestKey(),
	}

	// Update version set with new SSTable at L0
	edit := NewVersionEdit()
	edit.AddFile(0, fileMetadata) // Add to L0

	// Log the file creation with key range and conversion stats
	if len(firstKey) > 0 && len(lastKey) > 0 {
		db.logger.Debug("FLUSH_CONVERSION_COMPLETE",
			"file_number", fileNumber,
			"entries", entriesWritten,
			"first_key", int(firstKey[0]),
			"last_key", int(lastKey[0]),
			"memtable_entries_in", memtableSize,
			"sstable_entries_out", entriesWritten,
			"sstable_size_bytes", fileMetadata.Size,
			"conversion_ratio", float64(entriesWritten)/float64(memtableSize))
	}

	return edit, nil
}

// loadCurrentVersion returns the current version protected by epoch.
// Callers must use this within an epoch (call epoch.EnterEpoch/ExitEpoch).
// Provides a snapshot for consistent reads across multiple operations.
func (db *DB) loadCurrentVersion() *Version {
	version := db.currentVersion.Load()
	if version == nil {
		return nil // Handle during DB initialization
	}

	// Debug logging to understand what files are in this version
	l0Files := version.GetFiles(0)
	var l0FileNums []uint64
	for _, file := range l0Files {
		l0FileNums = append(l0FileNums, file.FileNum)
	}
	db.logger.Debug("LOAD_CURRENT_VERSION",
		"version_number", version.number,
		"l0_files", l0FileNums)

	return version
}

// updateCurrentVersion creates a new version snapshot and makes it current.
// Must hold db.mu. Called whenever database state changes (new memtable, new SSTables, etc.).
func (db *DB) updateCurrentVersion() {
	// Create a copy of immutable memtables slice
	immutableMemtables := make([]*memtable.MemTable, len(db.immutableMemtables))
	copy(immutableMemtables, db.immutableMemtables)

	// Prepare memtables slice in reverse chronological order (newest first)
	// Capacity: 1 active + len(immutable)
	memtables := make([]*memtable.MemTable, 0, 1+len(immutableMemtables))

	// Add active memtable first (newest writes)
	memtables = append(memtables, db.memtable)

	// Add immutable memtables in reverse chronological order (newest first)
	// The newest immutable is at the end of the slice, so iterate backwards
	for i := len(immutableMemtables) - 1; i >= 0; i-- {
		memtables = append(memtables, immutableMemtables[i])
	}

	// Create version snapshot with SSTables and memtables
	newVersion := db.versions.CreateVersionSnapshot(memtables, db.seq.Load())

	// Log details about the new Version
	sstableCount := 0
	for _, level := range newVersion.files {
		sstableCount += len(level)
	}

	// New Version created

	oldVersion := db.currentVersion.Swap(newVersion)

	// Version swapped - mark old version for cleanup via epoch manager
	if oldVersion != nil {
		oldVersion.MarkForCleanup()
		// Trigger epoch cleanup to free old versions
		db.triggerEpochCleanup()
	}
}

// Flush manually flushes the current memtable to disk and waits for completion.
// Useful for testing and explicit flushing.
// Forces the current memtable to be rotated and flushed.
func (db *DB) Flush() error {
	// If memtable is empty, nothing to flush
	db.mu.RLock()
	memtableEmpty := db.memtable.Size() == 0
	db.mu.RUnlock()

	if memtableEmpty {
		db.logger.Debug("memtable size 0 nothing to flush")
		return nil
	}

	// Force memtable rotation to trigger flush
	db.mu.Lock()
	if walerr := db.rotateWAL(); walerr != nil {
		db.mu.Unlock()
		return walerr
	}
	newmemt := memtable.NewMemtable(db.options.WriteBufferSize)
	db.rotateMemtable(newmemt)
	db.updateCurrentVersion()
	immutableCount := len(db.immutableMemtables)
	db.flushTrigger.Signal()
	db.mu.Unlock()

	// Wait for the flush to complete by polling immutable count
	for {
		db.mu.RLock()
		currentCount := len(db.immutableMemtables)
		db.mu.RUnlock()

		if currentCount < immutableCount {
			break // Flush completed
		}

		time.Sleep(1 * time.Millisecond)
	}

	return nil
}

// GetStats returns database statistics for testing and monitoring.
// Provides insight into memtable sizes, file counts per level, compaction status, etc.
// Useful for debugging and performance monitoring.
func (db *DB) GetStats() map[string]any {
	db.mu.RLock()
	defer db.mu.RUnlock()

	stats := make(map[string]any)

	// In read-only mode, skip expensive memtable calculations
	if db.options.ReadOnly {
		stats["memtable_size"] = int64(0)
		stats["memtable_entries"] = int64(0)
		stats["flush_queue_size"] = 0
	} else {
		stats["memtable_size"] = db.memtable.MemoryUsage()
		stats["memtable_entries"] = db.memtable.Size()
	}

	stats["sequence_number"] = db.seq.Load()
	stats["file_number"] = db.versions.CurrFileNumber()

	// Count SSTables per level
	version := db.versions.GetCurrentVersion()

	levelStats := make(map[string]int)
	levelSizeStats := make(map[string]int64)
	for level := 0; level < len(version.files); level++ {
		files := version.GetFiles(level)
		levelStats[fmt.Sprintf("level_%d_files", level)] = len(files)

		// Calculate total size for this level
		totalSize := int64(0)
		for _, file := range files {
			totalSize += int64(file.Size)
		}
		levelSizeStats[fmt.Sprintf("level_%d_size", level)] = totalSize
	}
	stats["levels"] = levelStats
	stats["level_sizes"] = levelSizeStats

	// Compaction stats
	if db.compactionManager != nil {
		stats["compaction_in_progress"] = db.compactionManager.IsCompactionInProgress()
	} else {
		stats["compaction_in_progress"] = false
	}

	return stats
}

// WaitForCompaction waits for any ongoing compaction to complete.
// Useful for testing when you need to ensure compaction is done.
func (db *DB) WaitForCompaction() {
	if db.closed.Load() {
		return
	}

	// Signal that compaction work is needed
	db.compactionManager.ScheduleCompaction()

	// Wait for the initial compaction to complete
	select {
	case err := <-db.compactionManager.doneChan:
		if err != nil {
			db.logger.Error("Compaction failed during WaitForCompaction", "error", err)
			return
		}
		// One round of compaction completed successfully

	case <-time.After(100 * time.Millisecond):
		// No compaction completed, assume no work was needed
		return
	}

	// Check if more urgent work is needed (only L0 compactions for now)
	// to avoid infinite loops where level compactions keep creating more work
	for range 3 {
		version := db.versions.GetCurrentVersion()
		l0Files := version.GetFiles(0)

		// Only continue if L0 is overloaded (more than L0CompactionTrigger files)
		if len(l0Files) >= db.options.L0CompactionTrigger {
			db.compactionManager.ScheduleCompaction()

			select {
			case err := <-db.compactionManager.doneChan:
				if err != nil {
					db.logger.Error("Compaction failed during WaitForCompaction", "error", err)
					return
				}
				// Continue checking for more L0 work

			case <-time.After(5 * time.Second):
				// No compaction completed, we're done
				return
			}
		} else {
			// L0 is not overloaded, we're done
			return
		}
	}
}

// openSSTable opens an SSTable using the file cache.
// Cache keeps frequently used SSTable readers open to avoid I/O overhead.
func (db *DB) openSSTable(fileNum uint64) (*CachedReader, error) {
	path := filepath.Join(db.path, fmt.Sprintf("%06d.sst", fileNum))
	return db.fileCache.Get(fileNum, path)
}

// Scan performs a range scan from startKey to endKey [start, end).
// Returns an iterator that walks through all keys in the specified range (start inclusive, end exclusive).
func (db *DB) Scan(startKey, endKey keys.UserKey) (*DBIterator, error) {
	if !keys.IsValidUserKey(startKey) || !keys.IsValidUserKey(endKey) {
		return nil, ErrInvalidKey
	}

	if startKey.Compare(endKey) > 0 {
		return nil, ErrInvalidRange
	}

	if db.closed.Load() {
		return nil, ErrDBClosed
	}

	r := keys.NewRange(startKey, endKey)

	return db.NewIteratorWithBounds(r), nil
}

// ScanPrefix performs a prefix scan.
// Returns an iterator for all keys that start with the given prefix.
func (db *DB) ScanPrefix(prefix []byte) (*DBIterator, error) {
	if !keys.IsValidUserKey(prefix) {
		return nil, ErrInvalidKey
	}

	if db.closed.Load() {
		return nil, ErrDBClosed
	}

	// Calculate the upper bound for the prefix using prefixSuccessor
	upperBound := prefixSuccessor(prefix)
	r := keys.NewRange(prefix, upperBound)
	return db.NewIteratorWithBounds(r), nil
}

// CompactRange manually triggers compaction for testing and debugging.
// Runs compaction until no more work is needed.
// In production, finer control would be preferred.
func (db *DB) CompactRange() error {
	if db.closed.Load() {
		return ErrDBClosed
	}

	// Use WaitForCompaction to handle the compaction loop
	db.WaitForCompaction()
	return nil
}

// ScheduleCompaction signals the compaction manager to check for work
// CompactAll forces compaction until no more work is available. Used
// in the cli and possibly for tests.
func (db *DB) CompactAll() error {
	if db.closed.Load() {
		return ErrDBClosed
	}

	maxRounds := 100 // Safety limit
	rounds := 0

	for rounds < maxRounds {
		rounds++

		// Check if any compaction work is needed BEFORE scheduling
		version := db.versions.GetCurrentVersion()
		compaction := db.compactionManager.pickCompaction(version)
		version.MarkForCleanup()

		if compaction == nil {
			// No more compaction work needed
			break
		}
		compaction.Cleanup() // Clean up the picked compaction

		// Schedule compaction work
		db.compactionManager.ScheduleCompaction()

		// Wait for completion
		select {
		case err := <-db.compactionManager.doneChan:
			if err != nil {
				return fmt.Errorf("compaction failed on round %d: %w", rounds, err)
			}
		case <-time.After(60 * time.Second):
			return fmt.Errorf("compaction timed out on round %d", rounds)
		}
	}

	return nil
}

// cleanupOrphanedSSTFiles removes SST files that are not referenced by the current manifest.
// This is called during database startup to clean up files left over from previous operations.
func (db *DB) cleanupOrphanedSSTFiles() error {
	// Get list of all SST files in the database directory
	sstFiles, err := filepath.Glob(filepath.Join(db.path, "*.sst"))
	if err != nil {
		return fmt.Errorf("failed to list SST files: %w", err)
	}

	if len(sstFiles) == 0 {
		return nil // No SST files to clean up
	}

	// Get the current version to see which files are actually referenced
	version := db.versions.GetCurrentVersion()
	defer version.MarkForCleanup()

	// Build a set of referenced file numbers
	referencedFiles := make(map[uint64]bool)
	for level := 0; level < len(version.files); level++ {
		for _, file := range version.GetFiles(level) {
			referencedFiles[file.FileNum] = true
		}
	}

	// Check each SST file and remove if not referenced
	orphanedCount := 0
	totalFiles := len(sstFiles)

	for _, sstFile := range sstFiles {
		// Extract file number from filename (e.g., "000123.sst" -> 123)
		filename := filepath.Base(sstFile)
		if !strings.HasSuffix(filename, ".sst") {
			continue
		}

		fileNumStr := strings.TrimSuffix(filename, ".sst")
		fileNum, err := strconv.ParseUint(fileNumStr, 10, 64)
		if err != nil {
			db.logger.Warn("invalid SST filename, skipping", "filename", filename, "error", err)
			continue
		}

		// If file is not referenced by current manifest, remove it
		if !referencedFiles[fileNum] {
			if err := os.Remove(sstFile); err != nil {
				db.logger.Warn("failed to remove orphaned SST file", "file", sstFile, "error", err)
			} else {
				orphanedCount++
				db.logger.Debug("removed orphaned SST file", "file", sstFile, "fileNum", fileNum)
			}
		}
	}

	if orphanedCount > 0 {
		db.logger.Info("cleaned up orphaned SST files",
			"orphaned", orphanedCount,
			"total", totalFiles,
			"remaining", totalFiles-orphanedCount)
	}

	return nil
}
