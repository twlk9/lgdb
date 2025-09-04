package memtable

import (
	"math/rand/v2"
	"sync"

	"github.com/twlk9/lgdb/keys"
)

const tMaxHeight = 12

const (
	posKV     = iota // position of k/v start (offset) in the data array
	posKey           // length of the key
	posVal           // length of the data
	posHeight        // height we are in the skiplist (number of next pointers)
	posNext          // First next pointer (level 0) (node + posNext + LEVEL is next pointer for LEVEL)
)

type MemTable struct {
	mu        sync.RWMutex
	rnd       *rand.Rand
	d         []byte // the actual data buffer
	md        []int  // meta data (data on where the data is in data)
	prev      [tMaxHeight]int
	maxHeight int
	n         int
	keyBuf    []byte // reusable buffer for key encoding
}

func NewMemtable(writeBufferSize int) *MemTable {
	// Estimate metadata capacity based on expected number of entries
	// Each entry uses ~6 ints on average (4 base + ~2 for skiplist pointers)
	// Assume 64-byte average key+value size for capacity estimation
	estimatedEntries := writeBufferSize / 64
	estimatedMdCapacity := 4 + tMaxHeight + (estimatedEntries * 6)

	mt := &MemTable{
		rnd:       rand.New(rand.NewPCG(4, 8)),
		maxHeight: 1,
		d:         make([]byte, 0, writeBufferSize),
		md:        make([]int, 4+tMaxHeight, estimatedMdCapacity),
		keyBuf:    make([]byte, 0, 256), // Initial capacity for typical key sizes
	}
	mt.md[posHeight] = tMaxHeight
	return mt
}

func (mt *MemTable) randHeight() int {
	const b = 4
	h := 1
	for h < tMaxHeight && mt.rnd.Int()%b == 0 {
		h++
	}
	return h
}

func (mt *MemTable) findGE(key keys.EncodedKey, prev bool) (int, bool) {
	node := 0
	h := mt.maxHeight - 1
	for {
		next := mt.md[node+posNext+h]
		cmp := 1
		if next != 0 {
			o := mt.md[next]
			d := keys.EncodedKey(mt.d[o : o+mt.md[next+posKey]])
			cmp = d.Compare(key)
		}
		if cmp < 0 { // If stored < search, continue forward
			node = next
		} else {
			if prev {
				mt.prev[h] = node
			} else if cmp == 0 {
				return next, true
			}
			if h == 0 {
				return next, cmp == 0
			}
			h--
		}
	}
}

func (mt *MemTable) Put(key keys.EncodedKey, value []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// We don't find exact matches and are simply positioning the
	// mt.prev array for insertion of our new key/value, there should
	// never be an exact match because the sequence would have
	// advanced causing the internal key to be different.
	mt.findGE(key, true)

	h := mt.randHeight()
	if h > mt.maxHeight {
		// Only initialize the NEW levels (mt.maxHeight to h-1) to point to header
		// Don't overwrite the existing levels that were set by findGE
		for i := mt.maxHeight; i < h; i++ {
			mt.prev[i] = 0
		}
		mt.maxHeight = h
	}

	off := len(mt.d)
	mt.d = append(mt.d, key...)
	mt.d = append(mt.d, value...)
	node := len(mt.md)
	mt.md = append(mt.md, off, len(key), len(value), h)
	for i, n := range mt.prev[:h] {
		m := n + posNext + i
		mt.md = append(mt.md, mt.md[m])
		mt.md[m] = node
	}
	mt.n++
}

// Get retrieves the most recent entry for a user key.
// Returns the raw value bytes and the internal key.
func (mt *MemTable) Get(key keys.EncodedKey) (keys.EncodedKey, []byte) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if mt.n == 0 {
		return nil, nil
	}

	// Navigate skiplist to find first key with matching user key
	// Since keys are sorted by internal key order, the first match
	// will be the most recent (highest sequence number)
	if node, _ := mt.findGE(key, false); node != 0 {
		o := mt.md[node]
		storedKey := keys.EncodedKey(mt.d[o : o+mt.md[node+posKey]])

		// Check if user keys match
		if storedKey.UserKey().Compare(key.UserKey()) == 0 {
			valueStart := o + mt.md[node+posKey]
			value := mt.d[valueStart : valueStart+mt.md[node+posVal]]
			return storedKey, value
		}
	}
	return nil, nil
}

func (mt *MemTable) Size() int {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.n == 0 {
		return 0
	}
	return len(mt.d) + len(mt.md)*8
}

// MemoryUsage returns an approximation of memory usage
func (mt *MemTable) MemoryUsage() int {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return len(mt.d) + len(mt.md)
}

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
