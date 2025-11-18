package memtable

import (
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"

	"github.com/twlk9/lgdb/epoch"
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
	epoch     uint64 // Tracking for WAL lifetime management
	refs      int32
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
		refs:      1,                    // DB will hold initial ref
	}
	mt.md[posHeight] = tMaxHeight
	mt.epoch = epoch.EnterEpoch()
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

// RegisterWAL takes a wal file path and registers it will epoch
// management so it can't be deleted while this memtable is still
// active.
func (mt *MemTable) RegisterWAL(path string) {
	if !epoch.ResourceExists(path) {
		epoch.RegisterResource(path, mt.epoch, func() error {
			return os.Remove(path)
		})
	}
}

// Close marks the epoch as finished. Should trigger any WAL cleanup
// once all references have completed.
func (mt *MemTable) Close() {
	mt.UnRef() // shouldn't destroy it if there is a reference still
	epoch.ExitEpoch(mt.epoch)
}

func (m *MemTable) Ref() {
	atomic.AddInt32(&m.refs, 1)
}

func (m *MemTable) UnRef() {
	if atomic.AddInt32(&m.refs, -1) == 0 {
		m.Destroy()
	}
}

func (m *MemTable) Destroy() {
	m.d = nil
	m.md = nil
	m.keyBuf = nil
}
