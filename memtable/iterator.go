package memtable

import "github.com/twlk9/lgdb/keys"

// MemTableIterator represents an iterator over the memtable.
type MemTableIterator struct {
	mt     *MemTable
	node   int         // Current node index (0 = invalid/before first)
	bounds *keys.Range // Optional iteration bounds
	key    keys.EncodedKey
	value  []byte
	err    error
}

// NewIterator creates a new iterator over the memtable.
func (mt *MemTable) NewIterator() *MemTableIterator {
	return &MemTableIterator{
		mt:     mt,
		node:   0, // Invalid position initially
		bounds: nil,
	}
}

// NewIteratorWithBounds creates a new iterator with specified bounds.
func (mt *MemTable) NewIteratorWithBounds(bounds *keys.Range) *MemTableIterator {
	it := mt.NewIterator()
	it.bounds = bounds
	return it
}

func (it *MemTableIterator) fill(start, limit bool) bool {
	if it.node != 0 {
		n := it.mt.md[it.node]
		m := n + it.mt.md[it.node+posKey]
		it.key = it.mt.d[n:m]
		if it.bounds != nil {
			switch {
			case limit && it.bounds.Limit != nil && it.key.Compare(it.bounds.Limit) >= 0:
				it.node = 0
				it.value = nil
				return false
			case start && it.bounds.Start != nil && it.key.Compare(it.bounds.Start) < 0:
				it.value = it.mt.d[m : m+it.mt.md[it.node+posVal]]
				it.node = 0
				it.value = nil
				return false
			}
		}
		it.value = it.mt.d[m : m+it.mt.md[it.node+posVal]]
		return true
	}
	it.key = nil
	it.value = nil
	return false
}

// SeekToFirst positions the iterator at the first element.
func (it *MemTableIterator) SeekToFirst() {
	it.mt.mu.Lock()
	defer it.mt.mu.Unlock()

	if it.bounds != nil && it.bounds.Start != nil {
		it.node, _ = it.mt.findGE(it.bounds.Start, false)
	} else {
		it.node = it.mt.md[posNext]
	}
	it.fill(false, true)
}

// Seek positions the iterator at the first element >= target.
func (it *MemTableIterator) Seek(target keys.EncodedKey) {
	it.mt.mu.RLock()
	defer it.mt.mu.RUnlock()
	if it.bounds != nil && it.bounds.Start != nil && target.Compare(it.bounds.Start) < 0 {
		target = it.bounds.Start
	}
	it.node, _ = it.mt.findGE(target, false)
	it.fill(false, true)
}

// Valid returns true if the iterator is positioned at a valid element.
func (it *MemTableIterator) Valid() bool {
	return it.node != 0
}

// Next moves the iterator to the next element.
func (it *MemTableIterator) Next() {
	if it.node == 0 {
		return // Already invalid
	}
	it.mt.mu.RLock()
	defer it.mt.mu.RUnlock()
	it.node = it.mt.md[it.node+posNext]
	it.fill(false, true)
}

// Key returns the current internal key.
func (it *MemTableIterator) Key() keys.EncodedKey {
	return keys.EncodedKey(it.key)
}

// UserKey returns the current user key.
func (it *MemTableIterator) UserKey() keys.UserKey {
	if it.key == nil {
		return nil
	}
	return it.key.UserKey()
}

// Value returns the current value.
func (it *MemTableIterator) Value() []byte {
	return it.value
}

// Error returns any accumulated error (always nil for memtable iterator).
func (it *MemTableIterator) Error() error {
	return it.err
}

// Close releases any resources held by the iterator.
func (it *MemTableIterator) Close() error {
	return nil
}
