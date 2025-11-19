package lgdb

import (
	"github.com/twlk9/lgdb/epoch"
	"github.com/twlk9/lgdb/keys"
	"github.com/twlk9/lgdb/memtable"
)

// Iterator provides an interface for iterating over key-value pairs in forward order.
type Iterator interface {
	// Valid returns true if the iterator is positioned at a valid element.
	Valid() bool

	// SeekToFirst positions the iterator at the first element.
	SeekToFirst()

	// Seek positions the iterator at the first element >= target.
	Seek(target keys.EncodedKey)

	// Next moves the iterator to the next element.
	Next()

	// Key returns the current key.
	Key() keys.EncodedKey

	// Value returns the current value.
	Value() []byte

	// Error returns any accumulated error.
	Error() error

	// Close releases any resources held by the iterator.
	Close() error
}

// DBIterator provides iteration over the entire database
type DBIterator struct {
	mergeIter Iterator // Use interface to support both heap and streaming merge
	valid     bool
	err       error
	bounds    *keys.Range
	version   *Version             // Hold reference to version to prevent file deletion
	epochNum  uint64               // Epoch number for file protection
	memtables []*memtable.MemTable // Memtables referenced for this iterator
}

// NewIterator creates a new database iterator.
func (db *DB) NewIterator(opts *ReadOptions) *DBIterator {
	b := &keys.Range{Start: nil, Limit: nil}
	return db.NewIteratorWithBounds(b, opts)
}

// NewIteratorWithBounds creates a new database iterator with bounds.
func (db *DB) NewIteratorWithBounds(bounds *keys.Range, opts *ReadOptions) *DBIterator {
	// Enter epoch to protect file access for the lifetime of the iterator
	epochNum := epoch.EnterEpoch()

	if bounds == nil {
		bounds = &keys.Range{Start: nil, Limit: nil}
	}

	if opts == nil {
		opts = DefaultReadOptions()
	}

	// Capture version and memtables atomically under the same lock to ensure
	// a consistent snapshot (prevent race where version is updated after we load it
	// but before we capture memtables, causing keys to be invisible)
	db.mu.RLock()
	version := db.loadCurrentVersion()
	if version == nil {
		db.mu.RUnlock()
		// Exit epoch if we can't create iterator
		epoch.ExitEpoch(epochNum)
		return &DBIterator{valid: false} // DB not fully initialized
	}
	memtables := memtable.RefMemTableList(db.memtable, db.immutableMemtables)
	db.mu.RUnlock()

	// Count iterators needed: memtables + overlapping SSTable files
	expectedIterators := len(memtables)
	for level := 0; level < len(version.files); level++ {
		files := version.GetFiles(level)
		for _, file := range files {
			// Skip files that don't overlap with our bounds
			skipLower := bounds.Start != nil && file.LargestKey.Compare(bounds.Start) < 0
			skipUpper := bounds.Limit != nil && file.SmallestKey.Compare(bounds.Limit) >= 0
			if !skipLower && !skipUpper {
				expectedIterators++
			}
		}
	}

	// Create streaming merge iterator with exact capacity
	mergeIter := NewMergeIterator(bounds, false, db.seq.Load(), expectedIterators)

	// Collect range deletes from version AND memtables
	rangeDeletes := make([]RangeTombstone, 0, len(version.GetRangeDeletes()))
	rangeDeletes = append(rangeDeletes, version.GetRangeDeletes()...)

	// Scan memtables for range delete entries
	for _, mt := range memtables {
		mtIter := mt.NewIterator()
		for mtIter.SeekToFirst(); mtIter.Valid(); mtIter.Next() {
			key := mtIter.Key()
			if key.Kind() == keys.KindRangeDelete {
				// Found a range delete in memtable
				rt := RangeTombstone{
					Start: []byte(key.UserKey()),
					End:   mtIter.Value(),
					Seq:   key.Seq(),
					// ID not needed for filtering, only for persistence
				}
				rangeDeletes = append(rangeDeletes, rt)
			}
		}
		mtIter.Close()
	}

	mergeIter.SetRangeDeletes(rangeDeletes)

	// Add memtable iterators
	for _, memtable := range memtables {
		iter := memtable.NewIterator()
		mergeIter.AddIterator(iter) // Negative levels: -1, -2, -3...
	}

	// Add SSTable iterators - only include files that overlap with bounds
	for level := 0; level < len(version.files); level++ {
		files := version.GetFiles(level)
		for _, file := range files {
			// Skip files that don't overlap with our bounds
			skipLower := bounds.Start != nil && file.LargestKey.Compare(bounds.Start) < 0
			skipUpper := bounds.Limit != nil && file.SmallestKey.Compare(bounds.Limit) >= 0

			if skipLower || skipUpper {
				continue
			}

			// Use cached reader
			cachedReader, err := db.openSSTable(file.FileNum)
			if err != nil {
				db.logger.Error("NewIteratorWithBounds failed to open sstable via cache", "error", err,
					"fileNum", file.FileNum,
					"version_number", version.number)
				continue
			}

			// Add SSTable iterator directly
			iter := cachedReader.Reader().NewIteratorWithBounds(bounds, opts.NoBlockCache)
			mergeIter.AddIterator(iter) // SSTable levels start from 1
		}
	}

	// Create and return the final iterator with version reference
	return &DBIterator{
		mergeIter: mergeIter,
		valid:     false,
		bounds:    bounds,
		version:   version,
		epochNum:  epochNum,
		memtables: memtables,
	}
}

// Valid returns true if the iterator is positioned at a valid element.
func (it *DBIterator) Valid() bool {
	return it.valid && it.err == nil && it.mergeIter.Valid()
}

// SeekToFirst positions the iterator at the first element.
func (it *DBIterator) SeekToFirst() {
	it.err = nil

	// If we have a lower bound, seek to it instead of the absolute first
	if it.bounds != nil && it.bounds.Start != nil {
		it.mergeIter.Seek(it.bounds.Start)
	} else {
		it.mergeIter.SeekToFirst()
	}

	// Skip over any keys that are covered by range tombstones
	it.valid = it.mergeIter.Valid()
}

// Seek positions the iterator at the first element >= target.
func (it *DBIterator) Seek(target []byte) {
	encTarget := keys.NewQueryKey(target)
	it.err = nil
	it.mergeIter.Seek(encTarget)

	// Skip over any keys that are covered by range tombstones
	it.valid = it.mergeIter.Valid()
}

// Next moves the iterator to the next element.
func (it *DBIterator) Next() {
	if !it.valid {
		return
	}
	it.mergeIter.Next()

	// Skip over any keys that are covered by range tombstones
	it.valid = it.mergeIter.Valid()
}

// Key returns the current key (user key only).
func (it *DBIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	key := it.mergeIter.Key()
	if key != nil {
		return key.UserKey()
	}
	return nil
}

// Value returns the current value.
func (it *DBIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.mergeIter.Value()
}

// Error returns any accumulated error.
func (it *DBIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.mergeIter.Error()
}

// Close releases any resources held by the iterator.
func (it *DBIterator) Close() error {
	epoch.ExitEpoch(it.epochNum)
	// Close merge iterator which will close all underlying storage iterators
	if it.mergeIter != nil {
		if err := it.mergeIter.Close(); err != nil {
			return err
		}
	}

	// Mark version for cleanup via epoch manager
	if it.version != nil {
		it.version.MarkForCleanup()
		it.version = nil
	}

	// Release the memtableds
	memtable.UnRefMemTableList(it.memtables)

	return nil
}
