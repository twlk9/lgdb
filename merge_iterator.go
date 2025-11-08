package lgdb

import (
	"container/heap"
	"unsafe"

	"github.com/twlk9/lgdb/keys"
)

// heapEntry represents an iterator in the merge heap
type heapEntry struct {
	iter Iterator
}

// iteratorHeap implements heap.Interface for merging multiple iterators
// The heap maintains entries ordered by their keys using EncodedKey.Compare()
type iteratorHeap []*heapEntry

func (h iteratorHeap) Len() int {
	return len(h)
}

func (h iteratorHeap) Less(i, j int) bool {
	// Compare keys using EncodedKey.Compare() semantics:
	// - User key (primary)
	// - Sequence number descending (secondary - newer first)
	// - Kind ascending (tertiary)
	// Call Key() on demand instead of caching to reduce memory usage
	ki := h[i].iter.Key()
	kj := h[j].iter.Key()
	if ki == nil {
		return false
	}
	if kj == nil {
		return true
	}
	return ki.Compare(kj) < 0
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iteratorHeap) Push(x any) {
	*h = append(*h, x.(*heapEntry))
}

func (h *iteratorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

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

// MergeIterator uses a heap to merge multiple iterators efficiently.
type MergeIterator struct {
	iterators []Iterator
	current   Iterator
	bounds    *keys.Range

	// Heap for efficient minimum finding
	h *iteratorHeap

	// Reusable buffers to avoid allocations
	winningKeyBuffer []byte
	winningKey       keys.EncodedKey
	userKeyBuffer    []byte // Buffer for user key copies

	// Pre-allocated heap entries to avoid allocations
	heapEntries []heapEntry // Fixed-size array of heap entries
	freeList    []int       // Indices of available entries in heapEntries
	initialized bool        // Whether heap structures have been initialized

	// Control whether tombstones are visible (needed for compaction)
	includeTombstones bool

	// Range deletes to apply during iteration
	rangeDeletes []RangeTombstone

	err error
	seq uint64
}

func NewMergeIterator(bounds *keys.Range, includeTombstones bool, seq uint64) *MergeIterator {
	return NewMergeIteratorWithCapacity(bounds, includeTombstones, seq, 8)
}

// NewMergeIteratorWithCapacity creates a merge iterator with a hint for the expected number of iterators.
// Pre-sizing reduces allocations during AddIterator and initialization.
func NewMergeIteratorWithCapacity(bounds *keys.Range, includeTombstones bool, seq uint64, expectedIterators int) *MergeIterator {
	if expectedIterators < 1 {
		expectedIterators = 8
	}
	h := make(iteratorHeap, 0, expectedIterators)
	return &MergeIterator{
		iterators:         make([]Iterator, 0, expectedIterators),
		bounds:            bounds,
		h:                 &h,
		winningKeyBuffer:  make([]byte, 0, 64), // Pre-allocate reasonable size
		userKeyBuffer:     make([]byte, 0, 32), // Pre-allocate for user keys
		includeTombstones: includeTombstones,
		seq:               seq, // Sequence snapshot for this iterator's life
	}
}

// ensureInitialized sets up the heap structures with pre-allocated capacity.
// This is called once on first Seek/SeekToFirst to avoid allocations during iteration.
func (it *MergeIterator) ensureInitialized() {
	if it.initialized {
		return
	}
	it.initialized = true

	n := len(it.iterators)
	if n == 0 {
		return
	}

	// Pre-allocate heap entries array and free list
	it.heapEntries = make([]heapEntry, n)
	it.freeList = make([]int, n)
	for i := range n {
		it.freeList[i] = i
	}

	// Resize heap slice to exact capacity needed
	if cap(*it.h) < n {
		*it.h = make(iteratorHeap, 0, n)
	}
}

// getHeapEntry retrieves a heapEntry from the pre-allocated array.
func (it *MergeIterator) getHeapEntry() *heapEntry {
	if len(it.freeList) == 0 {
		panic("merge_iterator: no free heap entries available")
	}
	// Pop from free list
	idx := it.freeList[len(it.freeList)-1]
	it.freeList = it.freeList[:len(it.freeList)-1]
	return &it.heapEntries[idx]
}

// putHeapEntry returns a heapEntry to the free list.
func (it *MergeIterator) putHeapEntry(e *heapEntry) {
	e.iter = nil // Clear the iterator reference
	// Find the index of this entry in heapEntries
	idx := int(uintptr(unsafe.Pointer(e))-uintptr(unsafe.Pointer(&it.heapEntries[0]))) / int(unsafe.Sizeof(heapEntry{}))
	it.freeList = append(it.freeList, idx)
}

func (it *MergeIterator) AddIterator(iter Iterator) {
	it.iterators = append(it.iterators, iter)
}

func (it *MergeIterator) SetRangeDeletes(rangeDeletes []RangeTombstone) {
	it.rangeDeletes = rangeDeletes
}

// rebuildHeap rebuilds the heap from current iterator positions.
func (it *MergeIterator) rebuildHeap() {
	// Ensure heap structures are initialized
	it.ensureInitialized()

	// Return all existing entries to free list
	for _, e := range *it.h {
		it.putHeapEntry(e)
	}
	*it.h = (*it.h)[:0]

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

		entry := it.getHeapEntry()
		entry.iter = iter
		heap.Push(it.h, entry)
	}
}

// peekMinimum returns the iterator with the minimum key without removing it from the heap.
func (it *MergeIterator) peekMinimum() (Iterator, keys.EncodedKey) {
	if it.h == nil || len(*it.h) == 0 {
		return nil, nil
	}
	entry := (*it.h)[0]
	return entry.iter, entry.iter.Key()
}

// findMinimumIterator returns the iterator with the smallest current key.
func (it *MergeIterator) findMinimumIterator() (Iterator, keys.EncodedKey) {
	return it.peekMinimum()
}

// popAndAdvanceMatchingKeys pops all heap entries with the same user key as the minimum,
// advances those specific iterators, and re-adds them to the heap if they have more data.
func (it *MergeIterator) popAndAdvanceMatchingKeys() {
	if len(*it.h) == 0 {
		return
	}

	// Get the minimum user key and save it to compare against
	minKey := (*it.h)[0].iter.Key()
	if minKey == nil {
		return
	}
	minUserKey := minKey.UserKey()
	it.userKeyBuffer = copyInto(it.userKeyBuffer, minUserKey)

	// This loop is safe because we are comparing against a stable copy (it.userKeyBuffer)
	// and the heap property ensures subsequent keys with the same user key will surface.
	for len(*it.h) > 0 {
		topKey := (*it.h)[0].iter.Key()
		if topKey == nil || topKey.UserKey().Compare(it.userKeyBuffer) != 0 {
			break
		}

		entry := heap.Pop(it.h).(*heapEntry)
		entry.iter.Next()
		if !entry.iter.Valid() {
			it.putHeapEntry(entry) // Return to pool
			continue
		}

		currentKey := entry.iter.Key()
		if currentKey == nil {
			it.putHeapEntry(entry) // Return to pool
			continue
		}

		if it.seq > 0 && it.seq < currentKey.Seq() {
			currentKey = it.advanceIterForSeq(entry.iter)
			if currentKey == nil {
				it.putHeapEntry(entry) // Return to pool
				continue
			}
		}

		// Reuse the entry and push back to heap
		heap.Push(it.h, entry)
	}
}

// findAndSetCurrent finds the minimum key and sets it as current.
func (it *MergeIterator) findAndSetCurrent() {
	it.current = nil
	it.winningKey = nil

	for {
		minItem, minKey := it.findMinimumIterator()
		if minItem == nil || minKey == nil {
			return
		}

		it.winningKeyBuffer = copyInto(it.winningKeyBuffer, minKey)
		it.winningKey = keys.EncodedKey(it.winningKeyBuffer)

		if it.isValidEntry(it.winningKey) {
			it.current = minItem
			return
		}

		if it.bounds != nil && it.bounds.Limit != nil {
			if it.winningKey.UserKey().Compare(it.bounds.Limit.UserKey()) >= 0 {
				it.current = nil
				return
			}
		}
		it.popAndAdvanceMatchingKeys()
	}
}

func (it *MergeIterator) Next() {
	if it.current != nil {
		it.popAndAdvanceMatchingKeys()
	}
	it.findAndSetCurrent()
}

// isCoveredByRangeDelete checks if a key is covered by any range delete
func (it *MergeIterator) isCoveredByRangeDelete(key keys.EncodedKey) bool {
	if len(it.rangeDeletes) == 0 {
		return false
	}

	userKey := key.UserKey()
	keySeq := key.Seq()

	for _, rt := range it.rangeDeletes {
		if rt.Seq <= keySeq {
			continue
		}
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

func (it *MergeIterator) isValidEntry(key keys.EncodedKey) bool {
	userKey := key.UserKey()

	if it.bounds != nil {
		if it.bounds.Limit != nil && userKey.Compare(it.bounds.Limit.UserKey()) >= 0 {
			return false
		}
		if it.bounds.Start != nil && userKey.Compare(it.bounds.Start.UserKey()) < 0 {
			return false
		}
	}

	if key.Kind() == keys.KindDelete && !it.includeTombstones {
		return false
	}

	if key.Kind() == keys.KindRangeDelete {
		return false
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
			iter.Close()
		}
	}
	return nil
}
