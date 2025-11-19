package lgdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
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
// and adds the old one to the immutable list, ready for flushing.
// Must be called with db.mu lock held.
func (db *DB) rotateMemtable(newMemtable *memtable.MemTable) *memtable.MemTable {
	oldMemtable := db.memtable
	db.memtable = newMemtable

	// If we're using a WAL, the new memtable needs to know its file path.
	if db.wal != nil {
		db.memtable.RegisterWAL(db.wal.Path())
	}

	db.immutableMemtables = append(db.immutableMemtables, oldMemtable)
	return oldMemtable
}

// removeFromImmutableMemtables finds and removes a memtable from the immutable list.
// This is called after a memtable has been successfully flushed to an SSTable.
// Must be called with db.mu lock held.
// NOTE: Caller must Close() the memtable after updating the version to avoid
// a race where the memtable is destroyed before the new SSTable is visible.
func (db *DB) removeFromImmutableMemtables(targetMemtable *memtable.MemTable) bool {
	for i, immutable := range db.immutableMemtables {
		if immutable == targetMemtable {
			// Efficiently remove by shifting left and truncating the slice.
			copy(db.immutableMemtables[i:], db.immutableMemtables[i+1:])
			db.immutableMemtables = db.immutableMemtables[:len(db.immutableMemtables)-1]
			// Don't Close() yet - caller must do it after version is updated
			return true
		}
	}
	return false
}

// triggerEpochCleanup tells the epoch manager to try and free up old resources.
// This is safe to call anytime, as the epoch manager knows which resources are
// no longer in use by any active operations.
func (db *DB) triggerEpochCleanup() {
	if cleaned := epoch.TryCleanup(); cleaned > 0 {
		// If cleanup happened, we can also prune the list of old versions.
		db.versions.cleanupOldVersions()
	}
}

// epochIdleAdvance is a background goroutine that periodically advances the epoch
// on a quiet system. This ensures that even if there are no writes, old resources
// (like from a compaction) are eventually garbage collected.
func (db *DB) epochIdleAdvance() {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()

	c := epoch.GetCurrentEpoch()
	for {
		select {
		case <-t.C:
			e := epoch.GetCurrentEpoch()
			if c == e {
				// If the epoch hasn't changed, give it a nudge.
				c = epoch.AdvanceEpoch()
				db.triggerEpochCleanup()
				db.logger.Info("Idle timer advancing epoch", "old", e, "new", c)
			} else {
				// The epoch changed on its own, so just update our view.
				c = e
			}
		case <-db.done:
			return
		}
	}
}

// DB is the main database instance. It's the big boss that coordinates everything.
type DB struct {
	// The settings I'm running with.
	options *Options
	// A cached WriteOptions so I don't have to create one for every Put.
	defaultWriteOpts *WriteOptions
	// Where the database files live.
	path string

	// --- Memtable and WAL State (protected by db.mu) ---

	// The current in-memory table. All new writes go here first.
	memtable *memtable.MemTable
	// A list of older memtables that are full and waiting to be flushed to disk.
	immutableMemtables []*memtable.MemTable
	// The Write-Ahead Log. My insurance policy. If I crash, I can rebuild the
	// memtable from this. The pointer is swapped during WAL rotation.
	wal *wal.WAL

	// --- Version and Compaction State ---

	// Manages the manifest and the list of all SSTables across all levels.
	versions *VersionSet
	// The background worker that worries about compacting SSTables.
	compactionManager *CompactionManager

	// --- Concurrency and Coordination ---

	// The main mutex. Protects shared state like memtable pointers, the immutable
	// list, and version set during updates. It's a coarse lock, but it keeps
	// things simple and correct.
	mu sync.RWMutex

	// An atomic pointer to the current Version. This is the snapshot of the DB state
	// that all read operations use. Being atomic means reads don't have to take a big lock.
	currentVersion atomic.Pointer[Version]

	// The global sequence number for all writes. Every operation gets a unique,
	// increasing number. Crucial for snapshot isolation (MVCC).
	seq atomic.Uint64
	// An atomic flag to signal that the DB is shutting down.
	closed atomic.Bool

	// A channel to tell all my background goroutines it's time to go home.
	done chan struct{}

	// Coordination primitives for the background flusher.
	// flushTrigger wakes the flusher up when there's work to do.
	// flushBP makes writers wait if the flusher is getting backed up.
	flushWg      sync.WaitGroup
	flushTrigger *sync.Cond
	flushBP      *sync.Cond

	// --- Caches and Helpers ---

	// My trusty structured logger.
	logger *slog.Logger
	// Caches open SSTable file readers to avoid file I/O overhead.
	fileCache *FileCache
	// Caches uncompressed SSTable data blocks to speed up reads.
	blockCache *sstable.BlockCache
}

// Open is the grand entrance. It does everything: validates options, creates the DB path,
// recovers state from the manifest and WAL, and kicks off all the background workers.
// If the DB exists, it's recovered. If not and CreateIfMissing is set, it's created.
func Open(opts *Options) (*DB, error) {
	epoch.AdvanceEpoch() // Start with a non-zero epoch.
	if opts == nil {
		opts = DefaultOptions()
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}

	if err := opts.Validate(); err != nil {
		logger.Error("Options did not validate", "error", err, "options", opts)
		return nil, err
	}

	dbExists := false
	if _, err := os.Stat(opts.Path); err == nil {
		dbExists = true
	}

	if opts.ErrorIfExists && dbExists {
		err := errors.New("database already exists")
		logger.Error("database already exists at path", "error", err, "path", opts.Path)
		return nil, err
	}

	if !opts.CreateIfMissing && !dbExists {
		err := errors.New("database does not exist")
		logger.Error("database does not exist at path", "error", err, "path", opts.Path)
		return nil, err
	}

	if err := os.MkdirAll(opts.Path, 0755); err != nil {
		logger.Error("Failed to create database directory", "error", err, "path", opts.Path)
		return nil, err
	}

	db := &DB{
		options:          opts,
		defaultWriteOpts: &WriteOptions{Sync: opts.Sync},
		path:             opts.Path,
		logger:           logger,
		blockCache: sstable.NewBlockCache(opts.BlockCacheSize, func(value []byte) {
			bufferpool.PutBuffer(value)
		}),
	}

	db.fileCache = NewFileCache(opts.GetFileCacheSize(), db.blockCache, logger)
	db.flushTrigger = sync.NewCond(&db.mu)
	db.flushBP = sync.NewCond(&db.mu)
	db.versions = NewVersionSet(opts.MaxLevels, opts.Path, opts.MaxManifestFileSize, logger)
	db.compactionManager = NewCompactionManager(db.versions, db.fileCache, opts.Path, opts, logger, db.flushBP)
	db.closed.Store(false)
	db.done = make(chan struct{})
	db.memtable = memtable.NewMemtable(opts.WriteBufferSize)

	// --- Recovery ---
	// First, recover the state of the SSTables from the MANIFEST.
	if err := RecoverFromManifest(opts.Path, db.versions); err != nil {
		return nil, err
	}

	// Housekeeping: clean up any files left over from failed operations.
	if err := db.cleanupOrphanedSSTFiles(); err != nil {
		logger.Warn("Failed to cleanup orphaned SST files", "error", err)
	}
	if err := db.cleanupOldManifestFiles(); err != nil {
		logger.Warn("Failed to cleanup old manifest files", "error", err)
	}

	// Next, recover the memtable by replaying the WAL.
	var maxSeq uint64
	if !opts.ReadOnly {
		var err error
		maxSeq, err = db.recoverFromWAL()
		if err != nil {
			return nil, err
		}
	}
	db.seq.Store(maxSeq)

	// Create the first version snapshot for readers.
	db.updateCurrentVersion()

	// The max sequence number could be in an SSTable (e.g., from a bulk load),
	// so we need to check them too.
	sstableMaxSeq, err := db.findMaxSequenceInSSTables()
	if err != nil {
		return nil, err
	}
	if sstableMaxSeq > db.seq.Load() {
		db.seq.Store(sstableMaxSeq)
	}

	// --- Start Services ---
	// Initialize a new WAL for future writes.
	if !opts.ReadOnly && !opts.DisableWAL {
		wo := wal.WALOpts{
			Path:            opts.Path,
			FileNum:         db.versions.NewFileNumber(),
			MinSyncInterval: opts.WALMinSyncInterval,
			BytesPerSync:    opts.WALBytesPerSync,
		}
		newWAL, err := wal.NewWAL(wo)
		if err != nil {
			return nil, err
		}
		db.wal = newWAL
		db.memtable.RegisterWAL(db.wal.Path())
	}

	// Kick off the background workers.
	db.flushWg.Add(1)
	go db.backgroundFlusher()
	go db.epochIdleAdvance()

	return db, nil
}

// Close is the grand exit. It shuts everything down gracefully, ensuring all
// background work is finished and files are closed. Safe to call multiple times.
func (db *DB) Close() error {
	if db.closed.Swap(true) {
		return nil // Already closed, nothing to do.
	}

	// Tell background workers to stop.
	close(db.done)

	// Wake up the flusher so it can see the close signal and exit.
	db.flushTrigger.Signal()
	db.flushWg.Wait()

	// Stop the compaction manager first.
	if db.compactionManager != nil {
		db.compactionManager.Close()
	}

	// Close all other resources.
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			return err
		}
	}
	if db.fileCache != nil {
		if err := db.fileCache.Close(); err != nil {
			return err
		}
	}
	if db.blockCache != nil {
		db.blockCache.Close()
	}
	if db.versions != nil {
		if err := db.versions.Close(); err != nil {
			return err
		}
	}

	return nil
}

// backgroundFlusher is the little worker that could. It sits in a loop, waiting for
// a full memtable to appear in the immutable list, then flushes it to a new L0 SSTable.
func (db *DB) backgroundFlusher() {
	defer db.flushWg.Done()

	for {
		db.mu.Lock()
		// Wait until there's an immutable memtable to flush.
		for len(db.immutableMemtables) == 0 {
			if db.closed.Load() {
				db.mu.Unlock()
				return
			}
			db.flushTrigger.Wait()
		}

		// Grab the oldest immutable memtable to flush.
		mt := db.immutableMemtables[0]
		db.mu.Unlock()

		// A quick check for an empty memtable, which can happen in tests.
		if mt.Size() == 0 {
			db.mu.Lock()
			db.removeFromImmutableMemtables(mt)
			db.flushBP.Broadcast() // Wake up any waiting writers.
			db.mu.Unlock()
			mt.Close() // Safe to close now that it's removed from list
			continue
		}

		sstableNum := db.versions.NewFileNumber()

		// The main event: flush the memtable to a new SSTable file.
		edit, rangeDeleteEdit, flushErr := db.flushMemtableToSSTable(mt, sstableNum)
		if flushErr != nil {
			// This is bad. If we can't write to disk, the DB is likely unusable.
			db.logger.Error("FATAL: memtable flush failed", "error", flushErr, "sstable_num", sstableNum)
			db.closed.Store(true)
			db.flushBP.Broadcast()
			return
		}

		db.mu.Lock()
		// Apply the version edit to the manifest to make the new SSTable "live".
		var verr error
		if rangeDeleteEdit != nil {
			verr = db.versions.LogAndApplyWithRangeDeletes(edit, rangeDeleteEdit)
		} else {
			verr = db.versions.LogAndApply(edit)
		}
		if verr != nil {
			// Also very bad. If the manifest write fails, we can't recover.
			db.logger.Error("FATAL: Version Edit Failed (flushing memtable)", "error", verr, "sstable_num", sstableNum)
			db.closed.Store(true)
			db.flushBP.Broadcast()
			db.mu.Unlock()
			return
		}

		// Success! Clean up the flushed memtable.
		db.removeFromImmutableMemtables(mt)
		db.flushBP.Broadcast() // Tell waiting writers they can proceed.
		db.updateCurrentVersion()
		db.mu.Unlock()

		// Close the memtable AFTER version update so new iterators see the SSTable
		mt.Close()

		// After a flush, it's a good time to check if compactions are needed.
		db.triggerEpochCleanup()
		db.compactionManager.ScheduleCompaction()
	}
}

// waitForL0Backpressure pauses writes if L0 is getting too full.
// This is a crucial backpressure mechanism to prevent write amplification from spiraling
// out of control. It uses a condition variable to wait efficiently.
func (db *DB) waitForL0Backpressure() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	loggedBackpressure := false
	for {
		version := db.versions.GetCurrentVersion()
		l0Count := len(version.GetFiles(0))
		version.MarkForCleanup()

		if l0Count < db.options.L0StopWritesTrigger {
			if loggedBackpressure {
				db.logger.Info("L0_BACKPRESSURE_RESOLVED", "l0_files", l0Count)
			}
			return nil
		}

		if db.closed.Load() {
			return ErrDBClosed
		}

		if !loggedBackpressure {
			db.logger.Info("L0_BACKPRESSURE_WAITING", "l0_files", l0Count, "trigger", db.options.L0StopWritesTrigger)
			loggedBackpressure = true
		}

		// Aggressively trigger compaction and wait for a signal that space has been cleared.
		db.compactionManager.ScheduleCompaction()
		db.flushBP.Wait()
	}
}

// rotateWAL creates a new WAL file if the current one is getting too big.
// Must be called under lock.
func (db *DB) rotateWAL() error {
	if db.options.DisableWAL {
		return nil
	}

	// We size the WAL to be able to hold all memtables in memory, plus a little extra.
	maxsize := db.options.WriteBufferSize*db.options.MaxMemtables + 1
	if db.wal.Size() > int64(maxsize) {
		ogwal := db.wal
		currentFileNum := db.versions.NewFileNumber()
		wo := wal.WALOpts{
			Path:            db.options.Path,
			FileNum:         currentFileNum,
			MinSyncInterval: db.options.WALMinSyncInterval,
			BytesPerSync:    db.options.WALBytesPerSync,
		}
		newWAL, err := wal.NewWAL(wo)
		if err != nil {
			return err
		}
		db.wal = newWAL
		return ogwal.Close()
	}
	return nil
}

// write is the unified heart of all write operations (Put, Delete, DeleteRange).
// It handles backpressure, memtable rotation, WAL writing, and sequencing.
func (db *DB) write(key, value []byte, kind keys.Kind, opts *WriteOptions) error {
	if opts == nil {
		opts = db.defaultWriteOpts
	}
	if !keys.IsValidUserKey(key) {
		return ErrInvalidKey
	}
	if !keys.IsValidValue(value) {
		return ErrInvalidValue
	}

	if db.closed.Load() {
		return ErrDBClosed
	}
	if db.options.ReadOnly {
		return ErrReadOnly
	}

	// Wait if the DB is too busy compacting.
	if err := db.waitForL0Backpressure(); err != nil {
		return err
	}

	// --- Memtable Rotation Logic ---
	db.mu.Lock()
	// Wait if the flush queue is too long.
	if len(db.immutableMemtables) > db.options.MaxMemtables {
		db.flushBP.Wait()
	}
	// If the active memtable is full, rotate it.
	if db.memtable.MemoryUsage() >= db.options.WriteBufferSize {
		if walerr := db.rotateWAL(); walerr != nil {
			db.logger.Error("Rotating WAL failed during memtable rotation", "error", walerr)
		}
		newmemt := memtable.NewMemtable(db.options.WriteBufferSize)
		db.rotateMemtable(newmemt)
		db.updateCurrentVersion()
		db.flushTrigger.Signal() // Wake up the flusher.
	}
	db.mu.Unlock()

	// --- Write to WAL and Memtable ---
	db.mu.RLock()
	defer db.mu.RUnlock()

	currSeq := db.seq.Add(1)

	// Write to the WAL first for durability.
	if db.wal != nil {
		walrec := &wal.WALRecord{Type: kind, Seq: currSeq, Key: key, Value: value}
		if err := db.wal.WriteRecord(walrec); err != nil {
			return err
		}
		if opts.Sync {
			if err := db.wal.Sync(); err != nil {
				return err
			}
		}
	}

	// Finally, write to the memtable.
	ikey := keys.EncodedKey(bufferpool.GetBuffer(len(key) + keys.KeyFootLen))
	defer bufferpool.PutBuffer(ikey)
	ikey.Encode(key, currSeq, kind)
	db.memtable.Put(ikey, value)

	return nil
}

// Put inserts a key-value pair into the database.
// This is a simple write that uses the database's default sync setting.
func (db *DB) Put(key, value []byte) error {
	return db.PutWithOptions(key, value, db.defaultWriteOpts)
}

// PutWithOptions inserts a key-value pair with specific sync options.
// Use this if you need to control the durability of a particular write.
func (db *DB) PutWithOptions(key, value []byte, opts *WriteOptions) error {
	if opts == nil {
		opts = db.defaultWriteOpts
	}
	return db.write(key, value, keys.KindSet, opts)
}

// Get retrieves a value by key. Simple on the outside, but on the inside it creates
// an iterator to give a consistent view of the data across memtables and SSTables,
// while also correctly handling any deletions or range tombstones.
// Returns ErrNotFound if the key doesn't exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	if !keys.IsValidUserKey(key) {
		return nil, ErrInvalidKey
	}
	if db.closed.Load() {
		return nil, ErrDBClosed
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	iter.Seek(key)

	if iter.Valid() && bytes.Equal(iter.Key(), key) {
		return iter.Value(), nil
	}

	return nil, ErrNotFound
}

// Delete removes a key by writing a "tombstone" record.
// The actual data is removed later during compaction.
func (db *DB) Delete(key []byte) error {
	return db.write(key, nil, keys.KindDelete, db.defaultWriteOpts)
}

// DeleteRange drops a "range tombstone" to efficiently remove a range of keys.
// It doesn't delete keys one-by-one. Instead, it leaves a note that says
// "everything between startKey and endKey is dead". Reads and compactions respect this.
func (db *DB) DeleteRange(startKey, endKey keys.UserKey) error {
	if !keys.IsValidUserKey(startKey) || !keys.IsValidUserKey(endKey) {
		return ErrInvalidKey
	}
	if startKey.Compare(endKey) >= 0 {
		return ErrInvalidRange
	}
	return db.write(startKey, endKey, keys.KindRangeDelete, nil)
}

// prefixSuccessor returns the smallest key that is larger than any key with the given prefix.
// For example, prefixSuccessor("a") returns "b". It's used to create an exclusive
// upper bound for prefix scans.
func prefixSuccessor(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	successor := make([]byte, len(prefix))
	copy(successor, prefix)
	for i := len(successor) - 1; i >= 0; i-- {
		if successor[i] < 255 {
			successor[i]++
			return successor[:i+1]
		}
		successor[i] = 0
	}
	// If we got here, the prefix was all 0xFF bytes. No successor exists.
	return nil
}

// DeletePrefix removes all keys with the given prefix.
// This is a convenience wrapper around DeleteRange.
func (db *DB) DeletePrefix(prefix []byte) error {
	if !keys.IsValidUserKey(prefix) || len(prefix) == 0 {
		return ErrInvalidKey
	}

	endKey := prefixSuccessor(prefix)
	if endKey == nil {
		// This happens if the prefix is all 0xFF bytes.
		return ErrInvalidRange
	}

	return db.DeleteRange(prefix, endKey)
}

// sortWALFiles sorts WAL filenames numerically (e.g., 000001.wal, 000002.wal).
func sortWALFiles(files []string) {
	sort.Slice(files, func(i, j int) bool {
		nameI := filepath.Base(files[i])
		nameJ := filepath.Base(files[j])
		numStrI := strings.TrimSuffix(nameI, ".wal")
		numStrJ := strings.TrimSuffix(nameJ, ".wal")
		numI, errI := strconv.Atoi(numStrI)
		numJ, errJ := strconv.Atoi(numStrJ)
		if errI != nil || errJ != nil {
			return nameI < nameJ // Fallback for weird filenames.
		}
		return numI < numJ
	})
}

// recoverFromWAL is my insurance policy. If the DB didn't shut down cleanly,
// this function reads the WAL files in order and replays all the operations
// to rebuild the memtable to what it was before the crash.
func (db *DB) recoverFromWAL() (uint64, error) {
	walFiles, err := filepath.Glob(filepath.Join(db.path, "*.wal"))
	if err != nil {
		return 0, err
	}
	sortWALFiles(walFiles)

	var maxSeq uint64
	for i, walFile := range walFiles {
		reader, err := wal.NewWALReader(walFile)
		if err != nil {
			// If a WAL file is corrupt, we might be able to tolerate it, but for now, we fail.
			return 0, err
		}
		defer reader.Close()

		db.memtable.RegisterWAL(reader.Path())

		recordCount := 0
		for {
			record, err := reader.ReadRecord()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break // Normal end of file
				}

				// Handle unexpected EOF or corruption
				isLastWAL := i == len(walFiles)-1
				if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, wal.ErrCorruptRecord) {
					if isLastWAL {
						// Tolerate corruption at the tail of the last WAL file
						// This is common when a crash occurs during a write
						db.logger.Warn("WAL truncated at end - likely crash during write",
							"file", filepath.Base(walFile),
							"records_recovered", recordCount,
							"error", err)
						break // Stop processing this WAL and continue
					} else {
						// Corruption in a non-final WAL is suspicious
						return 0, fmt.Errorf("WAL corruption in non-final file %s (recovered %d records): %w - manual intervention required",
							filepath.Base(walFile), recordCount, err)
					}
				}

				// Other errors are fatal
				return 0, fmt.Errorf("WAL read error in %s: %w", filepath.Base(walFile), err)
			}

			recordCount++
			if record.Seq > maxSeq {
				maxSeq = record.Seq
			}

			ikey := keys.NewEncodedKey(record.Key, record.Seq, record.Type)
			db.memtable.Put(ikey, record.Value)
		}
	}
	return maxSeq, nil
}

// findMaxSequenceInSSTables scans SSTable metadata to find the highest sequence number.
// This is a safeguard for cases where data was flushed but the WAL was lost or disabled.
func (db *DB) findMaxSequenceInSSTables() (uint64, error) {
	var maxSeq uint64
	version := db.loadCurrentVersion()
	if version == nil {
		return 0, nil // No versions yet.
	}
	defer version.MarkForCleanup()

	for level := 0; level < len(version.files); level++ {
		for _, file := range version.GetFiles(level) {
			if file.LargestKey != nil && file.LargestKey.Seq() > maxSeq {
				maxSeq = file.LargestKey.Seq()
			}
		}
	}
	return maxSeq, nil
}

// flushMemtableToSSTable takes a full in-memory table and writes it out to a
// sorted file on disk (an SSTable). This is the moment data moves from
// volatile RAM to persistent storage.
func (db *DB) flushMemtableToSSTable(memtable *memtable.MemTable, fileNumber uint64) (*VersionEdit, *RangeDeleteEdit, error) {
	sstablePath := filepath.Join(db.path, fmt.Sprintf("%06d.sst", fileNumber))
	tempPath := sstablePath + ".tmp"

	// Ensure temp file is removed on any error path
	defer os.Remove(tempPath)

	// Memtable flushes always go to L0.
	wopts := sstable.SSTableOpts{
		Path:                 tempPath, // Write to temp path first
		Compression:          db.options.GetCompressionForLevel(0),
		BlockSize:            db.options.BlockSize,
		BlockRestartInterval: db.options.BlockRestartInterval,
		BlockMinEntries:      db.options.BlockMinEntries,
	}
	writer, err := sstable.NewSSTableWriter(wopts)
	if err != nil {
		return nil, nil, err
	}

	iter := memtable.NewIterator()
	iter.SeekToFirst()

	if !iter.Valid() {
		writer.Close()
		return nil, nil, fmt.Errorf("attempted to flush empty memtable (file: %d)", fileNumber)
	}

	var rangeDeletes []RangeTombstone
	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		if key.Kind() == keys.KindRangeDelete {
			rt := RangeTombstone{
				ID:    db.versions.NewRangeDeleteID(),
				Start: key.UserKey(),
				End:   value,
				Seq:   key.Seq(),
			}
			rangeDeletes = append(rangeDeletes, rt)
		}

		if err := writer.Add(key, value); err != nil {
			writer.Close()
			return nil, nil, err
		}
		iter.Next()
	}

	if err := writer.Finish(); err != nil {
		writer.Close()
		return nil, nil, err
	}

	// Explicitly close the writer to ensure file handle is released before rename.
	if err := writer.Close(); err != nil {
		return nil, nil, err
	}

	// Atomically rename the completed temp file to its final destination.
	if err := os.Rename(tempPath, sstablePath); err != nil {
		return nil, nil, fmt.Errorf("failed to rename temp sstable: %w", err)
	}

	fileMetadata := &FileMetadata{
		FileNum:       fileNumber,
		Size:          writer.EstimatedSize(),
		SmallestKey:   writer.SmallestKey(),
		LargestKey:    writer.LargestKey(),
		NumEntries:    writer.NumEntries(),
		SmallestSeq:   writer.SmallestSeq(),
		LargestSeq:    writer.LargestSeq(),
		NumTombstones: writer.NumTombstones(),
	}

	edit := NewVersionEdit()
	edit.AddFile(0, fileMetadata) // Flushed tables always go to L0.

	var rangeDeleteEdit *RangeDeleteEdit
	if len(rangeDeletes) > 0 {
		rangeDeleteEdit = NewRangeDeleteEdit()
		for _, rt := range rangeDeletes {
			rangeDeleteEdit.AddRangeDelete(rt)
		}
	}

	return edit, rangeDeleteEdit, nil
}

// loadCurrentVersion gets the current version snapshot, protected by the epoch system.
// Any operation that reads data must use a version obtained this way to ensure it's
// not cleaned up from underneath them.
func (db *DB) loadCurrentVersion() *Version {
	version := db.currentVersion.Load()
	if version == nil {
		return nil // Happens during DB initialization.
	}
	return version
}

// updateCurrentVersion creates a new, immutable snapshot of the database state (SSTables + memtables)
// and atomically swaps it into place. This is a critical step after any structural change,
// like a flush or compaction, to make the changes visible to new readers.
func (db *DB) updateCurrentVersion() {
	// The list of memtables needs to be in reverse chronological order (newest first).
	memtables := make([]*memtable.MemTable, 0, 1+len(db.immutableMemtables))
	memtables = append(memtables, db.memtable) // Active is newest.
	// Immutable memtables are already newest-last, so iterate backwards.
	for i := len(db.immutableMemtables) - 1; i >= 0; i-- {
		memtables = append(memtables, db.immutableMemtables[i])
	}

	newVersion := db.versions.CreateVersionSnapshot(memtables, db.seq.Load())
	oldVersion := db.currentVersion.Swap(newVersion)

	// The old version is no longer needed. Mark it for cleanup.
	if oldVersion != nil {
		oldVersion.MarkForCleanup()
		db.triggerEpochCleanup()
	}
}

// Flush manually triggers a memtable flush and waits for it to complete.
// Useful for tests or when you need to be sure data is on disk.
func (db *DB) Flush() error {
	db.mu.RLock()
	memtableEmpty := db.memtable.Size() == 0
	db.mu.RUnlock()

	if memtableEmpty {
		return nil // Nothing to flush.
	}

	// Force a memtable rotation to trigger the flush.
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

	// Wait for the flush to complete by polling. This is simple but effective for tests.
	for {
		db.mu.RLock()
		currentCount := len(db.immutableMemtables)
		db.mu.RUnlock()

		if currentCount < immutableCount {
			break // Flush completed.
		}
		time.Sleep(1 * time.Millisecond)
	}

	return nil
}

// GetStats returns a map of database statistics for monitoring and debugging.
func (db *DB) GetStats() map[string]any {
	db.mu.RLock()
	defer db.mu.RUnlock()

	stats := make(map[string]any)
	stats["sequence_number"] = db.seq.Load()
	stats["file_number"] = db.versions.CurrFileNumber()

	if !db.options.ReadOnly {
		stats["memtable_size"] = db.memtable.MemoryUsage()
		stats["memtable_entries"] = db.memtable.Size()
	}

	version := db.versions.GetCurrentVersion()
	defer version.MarkForCleanup()

	levelStats := make(map[string]int)
	levelSizeStats := make(map[string]int64)
	for level := 0; level < len(version.files); level++ {
		files := version.GetFiles(level)
		levelStats[fmt.Sprintf("level_%d_files", level)] = len(files)
		var totalSize int64
		for _, file := range files {
			totalSize += int64(file.Size)
		}
		levelSizeStats[fmt.Sprintf("level_%d_size", level)] = totalSize
	}
	stats["levels"] = levelStats
	stats["level_sizes"] = levelSizeStats
	stats["epoch"] = version.epochNum

	if db.compactionManager != nil {
		stats["compaction_in_progress"] = db.compactionManager.IsCompactionInProgress()
	}

	return stats
}

// WaitForCompaction is a testing utility that waits for any ongoing compaction to complete.
func (db *DB) WaitForCompaction() {
	if db.closed.Load() {
		return
	}

	// This is a bit complex. It triggers a compaction and waits, but also re-triggers
	// if L0 is still overloaded, to handle cascading compactions.
	for i := range 3 { // Loop a few times to be sure.
		version := db.versions.GetCurrentVersion()
		l0Overloaded := len(version.GetFiles(0)) >= db.options.L0CompactionTrigger
		version.MarkForCleanup()

		if !l0Overloaded && i > 0 {
			return // If L0 is fine, we're probably done.
		}

		db.compactionManager.ScheduleCompaction()
		select {
		case err := <-db.compactionManager.doneChan:
			if err != nil {
				db.logger.Error("Compaction failed during WaitForCompaction", "error", err)
				return
			}
		case <-time.After(100 * time.Millisecond):
			// No compaction happened, so there was probably no work to do.
			return
		}
	}
}

// openSSTable is a helper to open an SSTable reader using the file cache.
func (db *DB) openSSTable(fileNum uint64) (*CachedReader, error) {
	path := filepath.Join(db.path, fmt.Sprintf("%06d.sst", fileNum))
	return db.fileCache.Get(fileNum, path)
}

// Scan returns an iterator for a given key range [start, end).
func (db *DB) Scan(startKey, endKey keys.UserKey, opts *ReadOptions) (*DBIterator, error) {
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
	return db.NewIteratorWithBounds(r, opts), nil
}

// ScanPrefix returns an iterator for all keys with a given prefix.
func (db *DB) ScanPrefix(prefix []byte, opts *ReadOptions) (*DBIterator, error) {
	if !keys.IsValidUserKey(prefix) {
		return nil, ErrInvalidKey
	}
	if db.closed.Load() {
		return nil, ErrDBClosed
	}

	upperBound := prefixSuccessor(prefix)
	r := keys.NewRange(prefix, upperBound)
	return db.NewIteratorWithBounds(r, opts), nil
}

// CompactRange is a testing utility to manually trigger a full compaction cycle.
// In production, you'd want finer control.
func (db *DB) CompactRange() error {
	if db.closed.Load() {
		return ErrDBClosed
	}
	db.WaitForCompaction()
	return nil
}

// CompactAll forces compaction until no more work is available.
// Useful for CLI tools and tests.
func (db *DB) CompactAll() error {
	if db.closed.Load() {
		return ErrDBClosed
	}

	for i := range 100 { // Safety break after 100 rounds.
		version := db.versions.GetCurrentVersion()
		compactions := db.compactionManager.pickCompaction(version)
		version.MarkForCleanup()

		if compactions == nil {
			return nil // No more work to do.
		}
		for _, c := range compactions {
			c.Cleanup()
		}

		db.compactionManager.ScheduleCompaction()
		err := <-db.compactionManager.doneChan
		if err != nil {
			return fmt.Errorf("compaction failed on round %d: %w", i, err)
		}
	}
	return nil
}

// cleanupOrphanedSSTFiles is housekeeping. On startup, it finds any .sst files
// that aren't listed in the manifest and removes them. These are leftovers
// from failed or interrupted operations.
func (db *DB) cleanupOrphanedSSTFiles() error {
	sstFiles, err := filepath.Glob(filepath.Join(db.path, "*.sst"))
	if err != nil {
		return fmt.Errorf("failed to list SST files: %w", err)
	}
	if len(sstFiles) == 0 {
		return nil
	}

	version := db.versions.GetCurrentVersion()
	defer version.MarkForCleanup()

	referencedFiles := make(map[uint64]bool)
	for level := 0; level < len(version.files); level++ {
		for _, file := range version.GetFiles(level) {
			referencedFiles[file.FileNum] = true
		}
	}

	orphanedCount := 0
	for _, sstFile := range sstFiles {
		filename := filepath.Base(sstFile)
		fileNumStr := strings.TrimSuffix(filename, ".sst")
		fileNum, err := strconv.ParseUint(fileNumStr, 10, 64)
		if err != nil {
			db.logger.Warn("Invalid SST filename, skipping", "filename", filename, "error", err)
			continue
		}

		if !referencedFiles[fileNum] {
			if err := os.Remove(sstFile); err != nil {
				db.logger.Warn("Failed to remove orphaned SST file", "file", sstFile, "error", err)
			} else {
				orphanedCount++
				db.logger.Debug("Removed orphaned SST file", "file", sstFile)
			}
		}
	}

	if orphanedCount > 0 {
		db.logger.Info("Cleaned up orphaned SST files", "count", orphanedCount)
	}
	return nil
}

// cleanupOldManifestFiles removes old, rotated-out manifest files.
func (db *DB) cleanupOldManifestFiles() error {
	manifestFiles, err := filepath.Glob(filepath.Join(db.path, "*.manifest"))
	if err != nil {
		return fmt.Errorf("failed to list manifest files: %w", err)
	}
	if len(manifestFiles) <= 1 {
		return nil
	}

	db.mu.RLock()
	if db.versions.manifestWriter == nil {
		db.mu.RUnlock()
		return nil
	}
	currentManifestNum := db.versions.manifestWriter.GetFileNum()
	db.mu.RUnlock()

	oldCount := 0
	for _, manifestFile := range manifestFiles {
		filename := filepath.Base(manifestFile)
		fileNumStr := strings.TrimSuffix(filename, ".manifest")
		fileNum, err := strconv.ParseUint(fileNumStr, 10, 64)
		if err != nil {
			db.logger.Warn("Invalid manifest filename, skipping", "filename", filename, "error", err)
			continue
		}

		if fileNum != currentManifestNum {
			if err := os.Remove(manifestFile); err != nil {
				db.logger.Warn("Failed to remove old manifest file", "file", manifestFile, "error", err)
			} else {
				oldCount++
			}
		}
	}

	if oldCount > 0 {
		db.logger.Info("Cleaned up old manifest files", "count", oldCount)
	}
	return nil
}

// RebuildManifest is a recovery tool for when the manifest is corrupt or missing.
// It scans all SSTable files, reads their metadata, and builds a new manifest from scratch.
// All recovered tables are placed in L0.
func (db *DB) RebuildManifest() (int, error) {
	metadataReader := func(path string, fileNum uint64) (smallestKey, largestKey []byte, fileSize uint64, err error) {
		reader, err := sstable.NewSSTableReader(path, fileNum, nil, db.logger)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("failed to open SSTable: %w", err)
		}
		defer reader.Close()

		fileInfo, err := os.Stat(path)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("failed to stat file: %w", err)
		}

		smallest := reader.SmallestKey()
		largest := reader.LargestKey()
		smallestCopy := make([]byte, len(smallest))
		largestCopy := make([]byte, len(largest))
		copy(smallestCopy, smallest)
		copy(largestCopy, largest)

		return smallestCopy, largestCopy, uint64(fileInfo.Size()), nil
	}

	return RebuildManifestFromSSTables(db.path, db.versions, db.logger, metadataReader)
}

// RebuildManifestStandalone is an offline version of RebuildManifest for CLI tools.
func RebuildManifestStandalone(dbPath string, logger *slog.Logger, opts *Options) (int, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	if opts == nil {
		opts = DefaultOptions()
		opts.Path = dbPath
	}

	vs := NewVersionSet(opts.MaxLevels, dbPath, opts.MaxManifestFileSize, logger)

	metadataReader := func(path string, fileNum uint64) (smallestKey, largestKey []byte, fileSize uint64, err error) {
		reader, err := sstable.NewSSTableReader(path, fileNum, nil, logger)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("failed to open SSTable: %w", err)
		}
		defer reader.Close()

		fileInfo, err := os.Stat(path)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("failed to stat file: %w", err)
		}

		smallest := reader.SmallestKey()
		largest := reader.LargestKey()
		smallestCopy := make([]byte, len(smallest))
		largestCopy := make([]byte, len(largest))
		copy(smallestCopy, smallest)
		copy(largestCopy, largest)

		return smallestCopy, largestCopy, uint64(fileInfo.Size()), nil
	}

	return RebuildManifestFromSSTables(dbPath, vs, logger, metadataReader)
}
