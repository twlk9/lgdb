package lgdb

import (
	"github.com/twlk9/lgdb/keys"
)

// copyInto will directly copy src to dst if it's big enough otherwise
// we'll allocate some more space
func copyInto(dst []byte, src []byte) []byte {
	if cap(dst) < len(src) {
		dst = make([]byte, len(src))
	}
	dst = dst[:len(src)]
	copy(dst, src)
	return dst
}

// MergeIterator uses a streaming approach to merge multiple iterators
// without maintaining a heap of all keys, reducing allocations significantly.
type MergeIterator struct {
	iterators []Iterator
	current   Iterator
	bounds    *keys.Range

	// Reusable buffers to avoid allocations
	winningKeyBuffer []byte
	winningKey       keys.EncodedKey
	userKeyBuffer    []byte // Buffer for user key copies

	// Control whether tombstones are visible (needed for compaction)
	includeTombstones bool

	// Range deletes to apply during iteration
	rangeDeletes []RangeTombstone

	err error
	seq uint64
}

func NewMergeIterator(bounds *keys.Range, includeTombstones bool, seq uint64) *MergeIterator {
	return &MergeIterator{
		iterators:         make([]Iterator, 0, 8),
		bounds:            bounds,
		winningKeyBuffer:  make([]byte, 0, 64), // Pre-allocate reasonable size
		userKeyBuffer:     make([]byte, 0, 32), // Pre-allocate for user keys
		includeTombstones: includeTombstones,
		seq:               seq, // Sequence snapshot for this iterator's life
	}
}

func (it *MergeIterator) AddIterator(iter Iterator) {
	it.iterators = append(it.iterators, iter)
}

func (it *MergeIterator) SetRangeDeletes(rangeDeletes []RangeTombstone) {
	it.rangeDeletes = rangeDeletes
}

// isCoveredByRangeDelete checks if a key is covered by any range delete
func (it *MergeIterator) isCoveredByRangeDelete(key keys.EncodedKey) bool {
	if len(it.rangeDeletes) == 0 {
		return false
	}

	userKey := key.UserKey()
	keySeq := key.Seq()

	for _, rt := range it.rangeDeletes {
		// Range delete must have higher sequence number to shadow this key
		if rt.Seq <= keySeq {
			continue
		}

		// Check if key is within range [start, end)
		if userKey.Compare(keys.UserKey(rt.Start)) >= 0 && userKey.Compare(keys.UserKey(rt.End)) < 0 {
			return true
		}
	}

	return false
}

func (it *MergeIterator) advanceIterForSeq(iter Iterator) keys.EncodedKey {
	for iter.Valid() {
		key := iter.Key()
		if key == nil {
			return nil
		}
		if it.seq < key.Seq() {
			iter.Next()
			continue
		}
		return key
	}
	return nil
}

// findMinimumIterator finds the iterator with the smallest current
// key without copying any keys - just comparing direct references
func (it *MergeIterator) findMinimumIterator() (Iterator, keys.EncodedKey) {
	var minItem Iterator
	var minKey keys.EncodedKey

	for _, iter := range it.iterators {
		if !iter.Valid() || iter == nil {
			continue
		}

		currentKey := iter.Key()
		if currentKey == nil {
			continue
		}

		if it.seq > 0 && it.seq < currentKey.Seq() {
			currentKey = it.advanceIterForSeq(iter)
			if currentKey == nil {
				continue
			}
		}

		if minKey == nil || currentKey.Compare(minKey) < 0 {
			minItem = iter
			minKey = currentKey
		}
	}

	return minItem, minKey
}

// advanceMatchingIterators advances all iterators that have the same user key
// This implements LSM semantics where newer levels override older levels
func (it *MergeIterator) advanceMatchingIterators(userKey []byte) {
	// Advance ALL iterators that have this exact user key
	// This preserves correct ordering of non-matching iterators
	for _, iter := range it.iterators {
		if !iter.Valid() || iter == nil {
			continue
		}

		// Check if this iterator's current key matches the winning user key
		for iter.Valid() {
			currentKey := iter.Key()
			if currentKey == nil {
				break
			}

			// If user keys match, advance past this duplicate
			if currentKey.UserKey().Compare(userKey) == 0 {
				iter.Next()
			} else {
				break // Different user key, stop advancing this iterator
			}
		}
	}
}

// findAndSetCurrent finds the minimum key and sets it as current without advancing iterators
func (it *MergeIterator) findAndSetCurrent() {
	it.current = nil
	it.winningKey = nil

	for {
		// Find iterator with minimum key
		minItem, minKey := it.findMinimumIterator()
		if minItem == nil || minKey == nil {
			return // End of iteration - no more valid iterators
		}

		// Copy the winning key into our reusable buffer
		it.winningKeyBuffer = copyInto(it.winningKeyBuffer, minKey)
		it.winningKey = keys.EncodedKey(it.winningKeyBuffer)

		// Check if this entry passes filters
		if it.isValidEntry(it.winningKey) {
			it.current = minItem
			return
		} else {
			// Check if key is past upper bound - if so, we're done
			if it.bounds != nil && it.bounds.Limit != nil {
				userKey := it.winningKey.UserKey()
				if userKey.Compare(it.bounds.Limit.UserKey()) >= 0 {
					// Past upper bound - end iteration
					it.current = nil
					return
				}
			}
		}

		// Entry filtered out, advance and try again
		userKey := it.winningKey.UserKey()
		it.userKeyBuffer = copyInto(it.userKeyBuffer, userKey)
		it.advanceMatchingIterators(it.userKeyBuffer)

		// Continue loop to find next valid entry
	}
}

func (it *MergeIterator) Next() {
	// First advance the current iterator (if we have one)
	if it.current != nil && it.winningKey != nil {
		userKey := it.winningKey.UserKey()
		it.userKeyBuffer = copyInto(it.userKeyBuffer, userKey)
		it.advanceMatchingIterators(it.userKeyBuffer)
	}

	// Then find the next valid entry
	it.findAndSetCurrent()
}

func (it *MergeIterator) isValidEntry(key keys.EncodedKey) bool {
	userKey := key.UserKey()

	// Check bounds
	if it.bounds != nil {
		// Check upper bound (exclusive)
		if it.bounds.Limit != nil {
			if userKey.Compare(it.bounds.Limit.UserKey()) >= 0 {
				return false
			}
		}

		// Check lower bound (inclusive)
		if it.bounds.Start != nil {
			if userKey.Compare(it.bounds.Start.UserKey()) < 0 {
				return false
			}
		}
	}

	// Point tombstones are not returned as valid entries for regular reads
	// but are needed for compaction to handle deletions properly
	if key.Kind() == keys.KindDelete && !it.includeTombstones {
		return false
	}

	// Filter out keys covered by range deletes
	if it.isCoveredByRangeDelete(key) {
		return false
	}

	return true
}

func (it *MergeIterator) SeekToFirst() {
	it.err = nil
	it.current = nil
	it.winningKey = nil

	// Position all iterators to first
	for _, iter := range it.iterators {
		iter.SeekToFirst()
	}

	// Find first valid entry without advancing
	it.findAndSetCurrent()
}

func (it *MergeIterator) Seek(target keys.EncodedKey) {
	it.err = nil
	it.current = nil
	it.winningKey = nil

	// Position all iterators to target
	for _, iter := range it.iterators {
		iter.Seek(target)
	}

	// Find first valid entry >= target without advancing
	it.findAndSetCurrent()
}

func (it *MergeIterator) Valid() bool {
	return it.err == nil && it.current != nil && it.winningKey != nil
}

func (it *MergeIterator) Key() keys.EncodedKey {
	if !it.Valid() {
		return nil
	}
	return it.winningKey
}

func (it *MergeIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.current.Value()
}

func (it *MergeIterator) Error() error {
	return it.err
}

func (it *MergeIterator) Close() error {
	for _, iter := range it.iterators {
		if iter != nil {
			iter.Close()
		}
	}
	return nil
}
