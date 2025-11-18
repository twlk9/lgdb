package lgdb

import (
	"container/heap"
	"unsafe"

	"github.com/twlk9/lgdb/keys"
)

// heapEntry is just a wrapper for an iterator so it can be put into a heap.
type heapEntry struct {
	iter Iterator
}

// iteratorHeap is a min-heap of iterators. The `Less` function is the magic
// that keeps the iterators sorted by their current key, so the one with the
// globally smallest key is always at the top.
type iteratorHeap []*heapEntry

func (h iteratorHeap) Len() int { return len(h) }

// Less is the core of the merge logic. It defines the order of keys.
// It compares keys by:
// 1. User key (lexicographically).
// 2. Sequence number (descending, so newer keys come first).
// 3. Kind (e.g., a Set comes before a Delete for the same key and sequence).
func (h iteratorHeap) Less(i, j int) bool {
	ki := h[i].iter.Key()
	kj := h[j].iter.Key()
	if ki == nil {
		return false // A nil key is considered "greater" than any valid key.
	}
	if kj == nil {
		return true
	}
	return ki.Compare(kj) < 0
}

func (h iteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *iteratorHeap) Push(x any) { *h = append(*h, x.(*heapEntry)) }

func (h *iteratorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// copyInto is a helper to copy a slice, reallocating only if necessary.
func copyInto(dst, src []byte) []byte {
	if cap(dst) < len(src) {
		dst = make([]byte, len(src))
	}
	dst = dst[:len(src)]
	copy(dst, src)
	return dst
}

// MergeIterator takes multiple sorted iterators (from memtables and SSTables)
// and uses a min-heap to present them as a single, unified, sorted stream of keys.
type MergeIterator struct {
	iterators []Iterator
	current   Iterator // The iterator currently at the top of the heap (the "winner").
	bounds    *keys.Range

	h *iteratorHeap

	// Reusable buffers to avoid allocations in the hot loop (Next).
	winningKeyBuffer []byte
	winningKey       keys.EncodedKey
	userKeyBuffer    []byte

	// --- Performance Optimizations ---
	// We pre-allocate heap entries and manage them with a free list to avoid
	// creating garbage for the GC during iteration. It's a bit ugly but fast.
	heapEntries []heapEntry
	freeList    []int
	initialized bool

	// includeTombstones controls whether delete markers are returned by the iterator.
	// This is `true` for compactions, which need to see tombstones to process them.
	includeTombstones bool

	// A list of active range tombstones that can hide keys.
	rangeDeletes []RangeTombstone

	err error
	seq uint64 // If > 0, the iterator will only see keys with a sequence number <= seq.
}

// NewMergeIterator creates a merge iterator. Give it a hint for the number of
// iterators you expect to add; it helps pre-allocate memory.
func NewMergeIterator(bounds *keys.Range, includeTombstones bool, seq uint64, expectedIterators int) *MergeIterator {
	if expectedIterators < 1 {
		expectedIterators = 8
	}
	h := make(iteratorHeap, 0, expectedIterators)
	return &MergeIterator{
		iterators:         make([]Iterator, 0, expectedIterators),
		bounds:            bounds,
		h:                 &h,
		winningKeyBuffer:  make([]byte, 64),
		userKeyBuffer:     make([]byte, 32),
		includeTombstones: includeTombstones,
		seq:               seq,
	}
}

// ensureInitialized sets up the heap and free list on the first seek.
// This lazy initialization avoids allocating memory if the iterator is never used.
func (it *MergeIterator) ensureInitialized() {
	if it.initialized {
		return
	}
	it.initialized = true
	n := len(it.iterators)
	if n == 0 {
		return
	}
	it.heapEntries = make([]heapEntry, n)
	it.freeList = make([]int, n)
	for i := range n {
		it.freeList[i] = i
	}
	if cap(*it.h) < n {
		*it.h = make(iteratorHeap, 0, n)
	}
}

// getHeapEntry gets a pre-allocated heapEntry from our pool.
func (it *MergeIterator) getHeapEntry() *heapEntry {
	if len(it.freeList) == 0 {
		// This should never happen if ensureInitialized is called correctly.
		panic("merge_iterator: no free heap entries available")
	}
	idx := it.freeList[len(it.freeList)-1]
	it.freeList = it.freeList[:len(it.freeList)-1]
	return &it.heapEntries[idx]
}

// putHeapEntry returns a heapEntry to our pool so it can be reused.
func (it *MergeIterator) putHeapEntry(e *heapEntry) {
	e.iter = nil // Avoid lingering references.
	// This is a bit of pointer arithmetic to figure out the entry's original index.
	// It's a fast way to return it to the free list without searching.
	idx := int(uintptr(unsafe.Pointer(e))-uintptr(unsafe.Pointer(&it.heapEntries[0]))) / int(unsafe.Sizeof(heapEntry{}))
	it.freeList = append(it.freeList, idx)
}

// AddIterator adds a source iterator to be merged.
func (it *MergeIterator) AddIterator(iter Iterator) {
	it.iterators = append(it.iterators, iter)
}

// SetRangeDeletes provides the iterator with range tombstones to apply during iteration.
func (it *MergeIterator) SetRangeDeletes(rangeDeletes []RangeTombstone) {
	it.rangeDeletes = rangeDeletes
}

// rebuildHeap clears and rebuilds the heap with the current state of all source iterators.
func (it *MergeIterator) rebuildHeap() {
	it.ensureInitialized()
	for _, e := range *it.h {
		it.putHeapEntry(e)
	}
	*it.h = (*it.h)[:0]

	for _, iter := range it.iterators {
		if iter == nil || !iter.Valid() {
			continue
		}
		currentKey := iter.Key()
		if currentKey == nil {
			continue
		}
		// If we have a sequence number snapshot, advance the iterator past any newer keys.
		if it.seq > 0 && it.seq < currentKey.Seq() {
			currentKey = it.advanceIterForSeq(iter)
			if currentKey == nil {
				continue
			}
		}
		entry := it.getHeapEntry()
		entry.iter = iter
		heap.Push(it.h, entry)
	}
}

// peekMinimum returns the iterator at the top of the heap without removing it.
func (it *MergeIterator) peekMinimum() (Iterator, keys.EncodedKey) {
	if it.h == nil || len(*it.h) == 0 {
		return nil, nil
	}
	entry := (*it.h)[0]
	return entry.iter, entry.iter.Key()
}

// popAndAdvanceMatchingKeys is where the de-duplication magic happens.
// After we process the winning key (the newest version), we need to burn through
// all the older, stale versions of the *same user key* from other iterators.
// This loop does that, advancing all iterators that are pointing to the key we just processed.
func (it *MergeIterator) popAndAdvanceMatchingKeys() {
	if len(*it.h) == 0 {
		return
	}

	minKey := (*it.h)[0].iter.Key()
	if minKey == nil {
		return
	}
	// Copy the user key to a stable buffer to compare against.
	it.userKeyBuffer = copyInto(it.userKeyBuffer, minKey.UserKey())

	for len(*it.h) > 0 {
		topKey := (*it.h)[0].iter.Key()
		// If the top of the heap is now a different user key, we're done.
		if topKey == nil || topKey.UserKey().Compare(it.userKeyBuffer) != 0 {
			break
		}

		// This iterator has the same user key, so pop it, advance it, and push it back.
		entry := heap.Pop(it.h).(*heapEntry)
		entry.iter.Next()
		if !entry.iter.Valid() {
			it.putHeapEntry(entry) // Done with this iterator, return entry to pool.
			continue
		}

		currentKey := entry.iter.Key()
		if currentKey == nil {
			it.putHeapEntry(entry)
			continue
		}

		// Make sure the new key is visible to our snapshot.
		if it.seq > 0 && it.seq < currentKey.Seq() {
			currentKey = it.advanceIterForSeq(entry.iter)
			if currentKey == nil {
				it.putHeapEntry(entry)
				continue
			}
		}
		heap.Push(it.h, entry)
	}
}

// findAndSetCurrent finds the next valid key that should be exposed by the iterator.
// It loops, peeking at the top of the heap and skipping any keys that are deleted,
// outside the bounds, or hidden by a range tombstone.
func (it *MergeIterator) findAndSetCurrent() {
	it.current = nil
	it.winningKey = nil

	for {
		minItem, minKey := it.peekMinimum()
		if minItem == nil {
			return // Heap is empty, we're done.
		}

		// Copy the key to our stable buffer.
		it.winningKeyBuffer = copyInto(it.winningKeyBuffer, minKey)
		it.winningKey = keys.EncodedKey(it.winningKeyBuffer)

		if it.isValidEntry(it.winningKey) {
			it.current = minItem // We found a winner.
			return
		}

		// The current winner was invalid (e.g., deleted). Pop it and all other
		// versions of the same user key, then loop again to find the next candidate.
		it.popAndAdvanceMatchingKeys()
	}
}

// Next advances the iterator to the next valid key.
func (it *MergeIterator) Next() {
	if it.current != nil {
		// We just processed a key. Advance all iterators that were pointing to it.
		it.popAndAdvanceMatchingKeys()
	}
	// Find the next valid key to be the new current one.
	it.findAndSetCurrent()
}

// isCoveredByRangeDelete checks if a key is hidden by an active range delete.
func (it *MergeIterator) isCoveredByRangeDelete(key keys.EncodedKey) bool {
	if len(it.rangeDeletes) == 0 {
		return false
	}
	userKey := key.UserKey()
	for _, rt := range it.rangeDeletes {
		// The range tombstone only applies if it's newer than the key.
		if rt.Seq > key.Seq() {
			if userKey.Compare(rt.Start) >= 0 && userKey.Compare(rt.End) < 0 {
				return true
			}
		}
	}
	return false
}

// advanceIterForSeq advances an iterator until it finds a key visible to the current snapshot.
func (it *MergeIterator) advanceIterForSeq(iter Iterator) keys.EncodedKey {
	for iter.Valid() {
		key := iter.Key()
		if key == nil {
			return nil
		}
		if it.seq >= key.Seq() {
			return key // This key is old enough to be visible.
		}
		iter.Next()
	}
	return nil
}

// isValidEntry checks if a key is valid to be returned by the iterator.
// It must be within bounds, not a tombstone (unless requested), and not covered by a range delete.
func (it *MergeIterator) isValidEntry(key keys.EncodedKey) bool {
	if it.bounds != nil {
		if it.bounds.Limit != nil && key.UserKey().Compare(it.bounds.Limit.UserKey()) >= 0 {
			return false
		}
		if it.bounds.Start != nil && key.UserKey().Compare(it.bounds.Start.UserKey()) < 0 {
			return false
		}
	}
	if key.Kind() == keys.KindDelete && !it.includeTombstones {
		return false
	}
	if key.Kind() == keys.KindRangeDelete {
		return false // Range delete markers are never returned directly.
	}
	if it.isCoveredByRangeDelete(key) {
		return false
	}
	return true
}

func (it *MergeIterator) SeekToFirst() {
	it.err = nil
	it.current = nil
	it.winningKey = nil
	for _, iter := range it.iterators {
		iter.SeekToFirst()
	}
	it.rebuildHeap()
	it.findAndSetCurrent()
}

func (it *MergeIterator) Seek(target keys.EncodedKey) {
	it.err = nil
	it.current = nil
	it.winningKey = nil
	for _, iter := range it.iterators {
		iter.Seek(target)
	}
	it.rebuildHeap()
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
			if err := iter.Close(); err != nil {
				// Keep trying to close others, but return the first error.
				if it.err == nil {
					it.err = err
				}
			}
		}
	}
	return it.err
}
