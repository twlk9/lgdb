package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/twlk9/lgdb/bufferpool"
	"github.com/twlk9/lgdb/compression"
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

var entryBuffersPool = sync.Pool{
	New: func() any {
		return NewEntryBuffers(512, 512)
	},
}

type SSTableReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

// SSTableReader reads SSTables from disk
type SSTableReader struct {
	file       SSTableReaderAtCloser
	size       int64
	indexBlock *Block
	logger     *slog.Logger
	path       string // For better error context

	// Cached metadata
	smallestKey keys.EncodedKey
	largestKey  keys.EncodedKey

	// Reusable buffers for key-value operations during initialization
	buffers *EntryBuffers
}

// Block represents a decoded block
type Block struct {
	data             []byte
	restarts         []uint32
	numEntries       uint32
	restartKeys      [][]byte // Cached keys at restart points for faster seeking
	restartEntryIndx []int    // tracking for entry points
	sparseOffsets    []uint32 // Sparse index: offsets for every Nth entry (memory efficient)
}

// NewSSTableReader creates a new SSTable reader by opening the file at the given path
func NewSSTableReader(path string, logger *slog.Logger) (*SSTableReader, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError + 1})) // Effectively disable logging
	}
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	reader := &SSTableReader{
		file:    file,
		size:    stat.Size(),
		logger:  logger,
		path:    path,
		buffers: NewEntryBuffers(512, 512), // For initialization operations
	}

	// Read and parse footer
	if err := reader.readFooter(); err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

// Path simply returns full name/file for the sstable reader
func (r SSTableReader) Path() string {
	return r.path
}

// readFooter reads and parses the SSTable footer (Pebble format)
func (r *SSTableReader) readFooter() error {
	if r.size < FooterSize {
		return fmt.Errorf("file too small to be valid SSTable")
	}

	// Read footer
	footer := bufferpool.GetBuffer(FooterSize)
	defer bufferpool.PutBuffer(footer)
	_, err := r.file.ReadAt(footer, r.size-FooterSize)
	if err != nil {
		r.logger.Error("Failed to read SSTable footer", "error", err, "sstable", r.path, "offset", r.size-FooterSize, "size", FooterSize)
		return err
	}

	// Parse magic number (last 8 bytes)
	magic := footer[FooterSize-MagicSize:]
	if string(magic) != string(PebbleMagic) {
		return fmt.Errorf("invalid SSTable magic number")
	}

	// Parse version (4 bytes before magic)
	versionOffset := FooterSize - MagicSize - VersionSize
	version := binary.LittleEndian.Uint32(footer[versionOffset : versionOffset+VersionSize])
	if version != PebbleVersion {
		return fmt.Errorf("unsupported SSTable version: %d", version)
	}

	// Skip checksum type (first byte)
	footerOffset := 1

	// Parse meta index block handle (varints)
	_, n := decodeBlockHandle(footer[footerOffset:])
	if n == 0 {
		return fmt.Errorf("invalid meta index block handle")
	}
	footerOffset += n

	// Parse index block handle (varints)
	indexHandle, n := decodeBlockHandle(footer[footerOffset:])
	if n == 0 {
		return fmt.Errorf("invalid index block handle")
	}

	indexOffset := indexHandle.Offset
	indexSize := indexHandle.Size

	// Read and decompress index block (now compressed like GoLevelDB)
	indexData := bufferpool.GetBuffer(int(indexSize))
	defer bufferpool.PutBuffer(indexData)
	_, err = r.file.ReadAt(indexData, int64(indexOffset))
	if err != nil {
		r.logger.Error("Failed to read index block", "error", err, "sstable", r.path, "offset", indexOffset, "size", indexSize)
		return err
	}

	// Check for minimum size (trailer)
	if len(indexData) < BlockTrailerSize {
		return fmt.Errorf("index block too small for trailer: %d bytes", len(indexData))
	}

	// Extract compression type from trailer (1 byte before CRC32)
	compressionType := indexData[len(indexData)-BlockTrailerSize]

	// Extract index block data (without trailer)
	indexBlockData := indexData[:len(indexData)-BlockTrailerSize]

	// Decompress the index block data
	decompressedIndexData, err := compression.DecompressBlock(nil, indexBlockData, compressionType)
	if err != nil {
		return fmt.Errorf("failed to decompress index block: %w", err)
	}

	// Parse decompressed index block
	r.indexBlock, err = r.parseBlock(decompressedIndexData)
	if err != nil {
		return err
	}

	// Extract actual key range by reading first and last data blocks
	if r.indexBlock.numEntries > 0 {
		// Get first key from first data block
		err := r.indexBlock.getEntry(0, r.buffers)
		if err != nil {
			return err
		}

		firstBlockHandle, n := decodeBlockHandle(r.buffers.value)
		if n > 0 {
			firstBlock, err := r.readDataBlock(firstBlockHandle)
			if err == nil && firstBlock.numEntries > 0 {
				err := firstBlock.getEntry(0, r.buffers)
				if err == nil {
					r.smallestKey = copyInto(r.smallestKey, r.buffers.key)
				}
			}
		}

		// Get last key from last data block
		err = r.indexBlock.getEntry(int(r.indexBlock.numEntries-1), r.buffers)
		if err != nil {
			return err
		}

		lastBlockHandle, n := decodeBlockHandle(r.buffers.value)
		if n > 0 {
			lastBlock, err := r.readDataBlock(lastBlockHandle)
			if err == nil && lastBlock.numEntries > 0 {
				err := lastBlock.getEntry(int(lastBlock.numEntries-1), r.buffers)
				if err == nil {
					r.largestKey = copyInto(r.largestKey, r.buffers.key)
				}
			}
		}
	}

	return nil
}

// Get retrieves a value by encoded internal key without interpretation.
// Returns raw entry data, letting higher-level code handle tombstones and errors.
// Errors are logged internally and treated as "not found".
func (r *SSTableReader) Get(k keys.EncodedKey) ([]byte, keys.EncodedKey) {
	// Search index block for the appropriate data block
	blockHandle, found := r.findDataBlock(k)
	if !found {
		return nil, nil
	}

	// Read the data block
	dataBlock, err := r.readDataBlock(blockHandle)
	if err != nil {
		r.logger.Warn("Failed to read data block", "path", r.path, "error", err)
		return nil, nil
	}

	// Binary search within the data block for much better performance
	left := 0
	right := int(dataBlock.numEntries) - 1

	// Use local entry buffers to avoid race conditions between concurrent reads
	localBuffers := entryBuffersPool.Get().(*EntryBuffers)
	defer entryBuffersPool.Put(localBuffers)

	// Binary search to find the position where the key would be
	for left <= right {
		mid := (left + right) / 2
		err := dataBlock.getEntry(mid, localBuffers)
		if err != nil {
			r.logger.Warn("Failed to get entry", "mid", mid, "path", r.path, "error", err)
			return nil, nil
		}

		// Use efficient encoded-to-internal comparison to avoid decoding entry key
		// CRITICAL: Never use bytes.Compare on internal keys due to variable user key lengths
		cmp := localBuffers.key.Compare(k)
		if cmp == 0 {
			// Exact match found
			return localBuffers.value, localBuffers.key
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	// No exact match found. When seeking with MaxSequenceNumber, check if we
	// landed on an entry with the same user key
	if left < int(dataBlock.numEntries) {
		err := dataBlock.getEntry(left, localBuffers)
		if err == nil {
			// Compare only user keys
			if localBuffers.key.UserKey().Compare(k.UserKey()) == 0 {
				return localBuffers.value, localBuffers.key
			}
		}
	}
	return nil, nil
}

// findDataBlock finds the data block that might contain the encoded internal key
func (r *SSTableReader) findDataBlock(encodedInternalKey keys.EncodedKey) (BlockHandle, bool) {
	if r.indexBlock.numEntries == 0 {
		return BlockHandle{}, false
	}

	// Binary search for the appropriate block using separator semantics
	// Separator[i] represents the smallest key NOT in block[i] (i.e., first key of next block)
	// So if separator[i] > key, then key belongs in block[i]
	// If separator[i] <= key, then key belongs in a later block

	left := 0
	right := int(r.indexBlock.numEntries) - 1
	result := -1

	// Use local entry buffers to avoid race conditions between concurrent reads
	localBuffers := entryBuffersPool.Get().(*EntryBuffers)
	defer entryBuffersPool.Put(localBuffers)

	for left <= right {
		mid := left + (right-left)/2
		err := r.indexBlock.getEntry(mid, localBuffers)
		if err != nil {
			return BlockHandle{}, false
		}

		// If separator > key, then key might be in this block or earlier
		// Use efficient encoded-to-internal comparison to avoid decoding separator key
		cmp := localBuffers.key.Compare(encodedInternalKey)
		if cmp > 0 {
			result = mid
			right = mid - 1
		} else {
			// separator <= key, so key is in a later block
			left = mid + 1
		}
	}

	// If we found a separator > key, use that block
	if result >= 0 {
		err := r.indexBlock.getEntry(result, localBuffers)
		if err != nil {
			return BlockHandle{}, false
		}
		handle, n := decodeBlockHandle(localBuffers.value)
		if n == 0 {
			return BlockHandle{}, false
		}
		return handle, true
	}

	// If no separator > key, then key is in the last block
	err := r.indexBlock.getEntry(int(r.indexBlock.numEntries-1), localBuffers)
	if err != nil {
		return BlockHandle{}, false
	}

	handle, n := decodeBlockHandle(localBuffers.value)
	if n == 0 {
		return BlockHandle{}, false
	}
	return handle, true
}

// readDataBlock reads a data block from disk
func (r *SSTableReader) readDataBlock(handle BlockHandle) (*Block, error) {
	data := bufferpool.GetBuffer(int(handle.Size))
	defer bufferpool.PutBuffer(data)

	_, err := r.file.ReadAt(data, int64(handle.Offset))
	if err != nil {
		r.logger.Error("Failed to read data block", "error", err, "sstable", r.path, "offset", handle.Offset, "size", handle.Size)
		return nil, err
	}

	// Keep defensive copying - it doesn't add much overhead and eliminates data races
	// defensiveCopy := make([]byte, len(data))
	// copy(defensiveCopy, data)
	// data = defensiveCopy

	// Check if we have enough data for the block trailer
	if len(data) < BlockTrailerSize {
		return nil, fmt.Errorf("block too small for trailer")
	}

	// Extract compression type from trailer (1 byte before CRC32)
	compressionType := data[len(data)-BlockTrailerSize]

	// Extract block data (without trailer)
	blockData := data[:len(data)-BlockTrailerSize]

	// Decompress the block data
	decompressedData, err := compression.DecompressBlock(nil, blockData, compressionType)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress block: %w", err)
	}

	return r.parseBlock(decompressedData)
}

// parseBlock parses block data into a Block structure (Pebble format)
func (r *SSTableReader) parseBlock(data []byte) (*Block, error) {
	if len(data) < 4 { // At least need num_restarts
		return nil, fmt.Errorf("block too small")
	}

	// Read number of restart points from the end
	dataLen := len(data)
	numRestarts := binary.LittleEndian.Uint32(data[dataLen-4:])

	// Check if we have enough data for the restart points
	metadataSize := 4 + int(numRestarts)*4 // num_restarts + restart_points
	if dataLen < metadataSize {
		return nil, fmt.Errorf("block too small for restart points")
	}

	// Read restart points
	restarts := make([]uint32, numRestarts)
	for i := range numRestarts {
		offset := dataLen - 4 - 4*int(numRestarts) + 4*int(i)
		restarts[i] = binary.LittleEndian.Uint32(data[offset : offset+4])
	}

	// Calculate the data portion size
	dataSize := dataLen - metadataSize
	var blockData []byte
	if dataSize > 0 {
		blockData = make([]byte, dataSize)
		copy(blockData, data[:dataSize])
	}

	// Parse block metadata: count entries and build sparse index in single pass
	numEntries, sparseOffsets := r.parseBlockWithSparseIndex(blockData)

	// Cache keys at restart points for faster seeking
	restartKeys := make([][]byte, len(restarts))
	for i, restartOffset := range restarts {
		if restartOffset < uint32(len(blockData)) {
			// At restart points, shared length is always 0
			offset := int(restartOffset)

			// Skip shared length (always 0 at restart points)
			_, n := binary.Uvarint(blockData[offset:])
			if n <= 0 {
				r.logger.Warn("Failed to parse shared length at restart point", "restart_index", i, "offset", restartOffset, "sstable", r.path)
				continue
			}
			offset += n

			// Read unshared key length
			unshared, n := binary.Uvarint(blockData[offset:])
			if n <= 0 {
				r.logger.Warn("Failed to parse unshared key length at restart point", "restart_index", i, "offset", restartOffset, "sstable", r.path)
				continue
			}
			offset += n

			// Skip value length
			_, n = binary.Uvarint(blockData[offset:])
			if n <= 0 {
				r.logger.Warn("Failed to parse value length at restart point", "restart_index", i, "offset", restartOffset, "sstable", r.path)
				continue
			}
			offset += n

			// Read the key
			if offset+int(unshared) <= len(blockData) {
				restartKeys[i] = make([]byte, unshared)
				copy(restartKeys[i], blockData[offset:offset+int(unshared)])
			}
		}
	}

	b := &Block{
		data:          blockData,
		restarts:      restarts,
		numEntries:    numEntries,
		restartKeys:   restartKeys,
		sparseOffsets: sparseOffsets,
	}
	ridx := make([]int, len(restarts))
	entryidx := 0
	for i := range len(restarts) - 1 {
		ridx[i] = entryidx
		entryidx += b.countEntriesBetweenRestarts(i, i+1)
	}
	b.restartEntryIndx = ridx
	return b, nil
}

// parseBlockWithSparseIndex parses block data in a single pass to count entries
// and build sparse index for large blocks (memory-efficient optimization)
// CRITICAL FIX: Sparse index should only use restart points, not arbitrary intervals
func (r *SSTableReader) parseBlockWithSparseIndex(data []byte) (uint32, []uint32) {
	if len(data) == 0 {
		return 0, nil
	}

	// Simply count entries - we'll use restart points for sparse access
	// This avoids the complex key reconstruction issue
	var numEntries uint32
	offset := 0

	for offset < len(data) {
		// Parse entry header to skip over it
		_, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			break
		}
		offset += n

		unshared, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			break
		}
		offset += n

		valueLen, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			break
		}
		offset += n

		// Skip unshared key and value data
		offset += int(unshared) + int(valueLen)

		if offset > len(data) {
			break
		}

		numEntries++
	}

	// Don't return sparse offsets - use restart points instead
	// This eliminates the key reconstruction complexity
	return numEntries, nil
}

// EntryBuffers provides reusable buffers for key-value reconstruction
// Eliminates allocations by allowing iterators to own and reuse buffers
type EntryBuffers struct {
	keyBuf         []byte          // Working buffer for key reconstruction
	valueBuf       []byte          // Working buffer for value data
	key            keys.EncodedKey // Current key slice (points into keyBuf)
	value          []byte          // Current value slice (points into valueBuf)
	isRestartEntry bool            // True if current entry is at restart point (shared=0)
	blockData      []byte          // Reference to block data for direct key access
	keyOffset      int             // Offset in blockData where key starts
	keyLength      int             // Length of key in blockData
}

// NewEntryBuffers creates a new EntryBuffers with initial capacity
func NewEntryBuffers(keyCapacity, valueCapacity int) *EntryBuffers {
	return &EntryBuffers{
		keyBuf:   make([]byte, 0, keyCapacity),
		valueBuf: make([]byte, 0, valueCapacity),
	}
}

// EnsureKeyCapacity ensures keyBuf has at least the specified capacity
func (e *EntryBuffers) EnsureKeyCapacity(capacity int) {
	if cap(e.keyBuf) < capacity {
		e.keyBuf = make([]byte, 0, capacity)
	}
}

// EnsureValueCapacity ensures valueBuf has at least the specified capacity
func (e *EntryBuffers) EnsureValueCapacity(capacity int) {
	if cap(e.valueBuf) < capacity {
		e.valueBuf = make([]byte, 0, capacity)
	}
}

// getEntry retrieves an entry by index using optimized caching and reduced copying
// Uses provided EntryBuffers to eliminate allocations
func (b *Block) getEntry(index int, buffers *EntryBuffers) error {
	if index >= int(b.numEntries) {
		return fmt.Errorf("index out of range")
	}

	// Use restart point logic
	restartIndex := index / RestartInterval
	if restartIndex >= len(b.restarts) {
		restartIndex = len(b.restarts) - 1
	}

	// Start from the restart point
	offset := int(b.restarts[restartIndex])
	startEntryIndex := restartIndex * RestartInterval
	targetEntryInInterval := index - startEntryIndex

	// Initialize lastKey from cached restart keys if available
	var lastKey []byte
	if restartIndex < len(b.restartKeys) && b.restartKeys[restartIndex] != nil {
		lastKey = b.restartKeys[restartIndex]
	}

	// Scan to target entry
	for i := 0; i <= targetEntryInInterval; i++ {

		if offset >= len(b.data) {
			return fmt.Errorf("corrupt block data")
		}

		// Parse entry header efficiently
		shared, n := binary.Uvarint(b.data[offset:])
		if n <= 0 {
			return fmt.Errorf("corrupt block data: shared length")
		}
		offset += n

		unshared, n := binary.Uvarint(b.data[offset:])
		if n <= 0 {
			return fmt.Errorf("corrupt block data: unshared length")
		}
		offset += n

		valueLen, n := binary.Uvarint(b.data[offset:])
		if n <= 0 {
			return fmt.Errorf("corrupt block data: value length")
		}
		keyOffset := offset + n
		offset = keyOffset + int(unshared)

		// Verify bounds
		if offset > len(b.data) || offset+int(valueLen) > len(b.data) {
			return fmt.Errorf("corrupt block data")
		}

		// Reconstruct key for current entry
		keySize := int(shared + unshared)

		// Check if this is a restart entry (shared = 0) for optimization
		isRestartEntry := shared == 0

		if i == targetEntryInInterval {
			// This is our target entry - set up optimization fields
			buffers.isRestartEntry = isRestartEntry
			buffers.blockData = b.data
			buffers.keyOffset = keyOffset
			buffers.keyLength = int(unshared) // For restart entries, unshared = full key length

			if isRestartEntry {
				// Optimization: For restart entries, key is already complete in block data
				// Just point to it directly without reconstruction
				buffers.key = keys.EncodedKey(b.data[keyOffset : keyOffset+int(unshared)])
			} else {
				// Regular entry: need to reconstruct key as before
				buffers.EnsureKeyCapacity(keySize)
				buffers.keyBuf = buffers.keyBuf[:keySize]

				if shared > 0 && len(lastKey) >= int(shared) {
					copy(buffers.keyBuf, lastKey[:shared])
				}
				copy(buffers.keyBuf[shared:], b.data[keyOffset:keyOffset+int(unshared)])
				buffers.key = buffers.keyBuf
			}

			// Copy value data for target entry
			buffers.EnsureValueCapacity(int(valueLen))
			buffers.valueBuf = buffers.valueBuf[:valueLen]
			copy(buffers.valueBuf, b.data[offset:offset+int(valueLen)])
			buffers.value = buffers.valueBuf

			return nil
		}

		// Not the target entry - still need to reconstruct for lastKey tracking
		buffers.EnsureKeyCapacity(keySize)
		buffers.keyBuf = buffers.keyBuf[:keySize]

		if shared > 0 && len(lastKey) >= int(shared) {
			copy(buffers.keyBuf, lastKey[:shared])
		}
		copy(buffers.keyBuf[shared:], b.data[keyOffset:keyOffset+int(unshared)])

		lastKey = buffers.keyBuf

		offset += int(valueLen)
	}

	return fmt.Errorf("entry not found")
}

// Close closes the SSTable reader
func (r *SSTableReader) Close() error {
	return r.file.Close()
}

// SmallestKey returns the smallest key in the SSTable
func (r *SSTableReader) SmallestKey() keys.EncodedKey {
	return r.smallestKey
}

// LargestKey returns the largest key in the SSTable
func (r *SSTableReader) LargestKey() keys.EncodedKey {
	return r.largestKey
}

// SSTableIterator provides iteration over an SSTable
type SSTableIterator struct {
	reader    *SSTableReader
	blockIter *BlockIterator
	indexIter *BlockIterator
	bounds    *keys.Range
	err       error
	buffers   *EntryBuffers // Per-iterator buffers for key-value reconstruction
}

// NewSSTableIterator creates a new SSTable iterator
func (r *SSTableReader) NewIterator() *SSTableIterator {
	return &SSTableIterator{
		reader:    r,
		indexIter: r.indexBlock.NewIterator(),
		buffers:   NewEntryBuffers(512, 512), // Per-iterator buffers for key-value operations
	}
}

// NewIteratorWithBounds creates a new SSTable iterator with bounds
func (r *SSTableReader) NewIteratorWithBounds(bounds *keys.Range) *SSTableIterator {
	iter := &SSTableIterator{
		reader:    r,
		indexIter: r.indexBlock.NewIterator(),
		buffers:   NewEntryBuffers(512, 512), // Per-iterator buffers for key-value operations
		bounds:    bounds,
	}

	return iter
}

// SeekToFirst positions the iterator at the first element
func (it *SSTableIterator) SeekToFirst() {
	it.err = nil

	// If we have a lower bound, seek to it instead
	if it.bounds != nil && it.bounds.Start != nil {
		it.Seek(it.bounds.Start)
		return
	}

	it.indexIter.SeekToFirst()

	if it.indexIter.Valid() {
		it.loadCurrentBlock()
		if it.blockIter != nil {
			it.blockIter.SeekToFirst()
		}
	}
}

// SeekToLast positions the iterator at the last element
func (it *SSTableIterator) SeekToLast() {
	it.err = nil
	it.indexIter.SeekToLast()

	if it.indexIter.Valid() {
		it.loadCurrentBlock()
		if it.blockIter != nil {
			it.blockIter.SeekToLast()
		}
	}
}

// Seek positions the iterator at the first element >= target
func (it *SSTableIterator) Seek(target keys.EncodedKey) {
	it.err = nil

	// Use separator-aware logic to find the right block, like findDataBlock does
	// but also position the index iterator correctly for Next() operations
	if it.reader.indexBlock.numEntries == 0 {
		it.blockIter = nil
		return
	}

	// Binary search for the appropriate block using separator semantics
	numEntries := int(it.reader.indexBlock.numEntries)
	left, right := 0, numEntries-1
	foundBlockIndex := numEntries - 1 // Default to last block

	for left <= right {
		mid := left + (right-left)/2
		err := it.reader.indexBlock.getEntry(mid, it.buffers)
		if err != nil {
			it.err = err
			return
		}

		cmp := it.buffers.key.Compare(target)
		if cmp > 0 {
			foundBlockIndex = mid
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	// Position index iterator directly at the found block index
	it.indexIter.err = nil
	it.indexIter.index = foundBlockIndex
	it.indexIter.loadCurrentEntry()

	if it.indexIter.Valid() {
		it.loadCurrentBlock()
		if it.blockIter != nil {
			it.blockIter.Seek(target)
		}
	}
}

// Valid returns true if the iterator is positioned at a valid element
func (it *SSTableIterator) Valid() bool {
	if it.err != nil || it.blockIter == nil || !it.blockIter.Valid() {
		return false
	}

	// Check bounds if they exist - optimized version using pre-decoded bounds
	if it.bounds != nil {
		key := it.blockIter.Key()

		// Check lower bound (inclusive)
		if it.bounds.Start != nil {
			if key.Compare(it.bounds.Start) < 0 {
				return false
			}
		}

		// Check upper bound (exclusive)
		if it.bounds.Limit != nil {
			if key.Compare(it.bounds.Limit) >= 0 {
				return false
			}
		}
	}
	return true
}

// Next moves the iterator to the next element
func (it *SSTableIterator) Next() {
	if !it.Valid() {
		return
	}

	it.blockIter.Next()

	// If current block is exhausted, move to next block
	if !it.blockIter.Valid() {
		it.indexIter.Next()
		if it.indexIter.Valid() {
			it.loadCurrentBlock()
			if it.blockIter != nil {
				it.blockIter.SeekToFirst()
			}
		}
	}
}

// Key returns the current key
func (it *SSTableIterator) Key() keys.EncodedKey {
	if !it.Valid() {
		return nil
	}
	// return keys.EncodedKey(it.keybuf)
	return it.blockIter.Key()
}

// Value returns the current value
func (it *SSTableIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.blockIter.Value()
}

// Error returns any accumulated error
func (it *SSTableIterator) Error() error {
	return it.err
}

// Close releases any resources held by the iterator
func (it *SSTableIterator) Close() error {
	// No resources to clean up for now
	return nil
}

// loadCurrentBlock loads the data block pointed to by the current index entry
func (it *SSTableIterator) loadCurrentBlock() {
	if !it.indexIter.Valid() {
		it.blockIter = nil
		return
	}

	// Get block handle from index entry
	handleBytes := it.indexIter.Value()
	handle, n := decodeBlockHandle(handleBytes)
	if n == 0 {
		it.err = fmt.Errorf("invalid block handle in index")
		return
	}

	// Read the data block
	block, err := it.reader.readDataBlock(handle)
	if err != nil {
		it.err = err
		return
	}

	it.blockIter = block.NewIterator()
}

// seekToRestartPoint finds the best restart point for the given target key
// Returns the restart point index and the entry index to start linear search from
func (b *Block) seekToRestartPoint(target keys.EncodedKey) (int, int, error) {
	if len(b.restarts) == 0 {
		return 0, 0, nil
	}

	// Binary search on cached restart keys
	left := 0
	right := len(b.restarts) - 1
	result := -1

	for left <= right {
		mid := left + (right-left)/2

		// Use cached restart key for comparison
		if mid < len(b.restartKeys) && b.restartKeys[mid] != nil {
			// Decode restart key for comparison
			restartKey := keys.EncodedKey(b.restartKeys[mid])

			cmp := restartKey.Compare(target)
			if cmp >= 0 {
				result = mid
				right = mid - 1
			} else {
				left = mid + 1
			}
		} else {
			// If restart key is not cached, fall back to linear search
			left = mid + 1
		}
	}

	// If we found a restart point where key >= target, use the previous one
	// to ensure we don't miss the target key
	restartIndex := 0
	if result > 0 {
		restartIndex = result - 1
	} else if result == 0 {
		restartIndex = 0
	} else {
		// No restart key >= target, use last restart point
		restartIndex = len(b.restarts) - 1
	}

	// Convert restart point to entry index
	// We need to count entries from the beginning to the restart point
	entryIndex := 0
	if restartIndex < len(b.restartEntryIndx) {
		entryIndex = b.restartEntryIndx[restartIndex]
	}
	return restartIndex, entryIndex, nil
}

// countEntriesBetweenRestarts counts entries between two restart points
func (b *Block) countEntriesBetweenRestarts(start, end int) int {
	if start >= len(b.restarts) || end >= len(b.restarts) {
		return 0
	}

	startOffset := int(b.restarts[start])
	endOffset := int(b.restarts[end])

	count := 0
	offset := startOffset

	for offset < endOffset && offset < len(b.data) {
		// Parse entry to skip over it
		_, n := binary.Uvarint(b.data[offset:])
		if n <= 0 {
			break
		}
		offset += n

		unshared, n := binary.Uvarint(b.data[offset:])
		if n <= 0 {
			break
		}
		offset += n

		valueLen, n := binary.Uvarint(b.data[offset:])
		if n <= 0 {
			break
		}
		offset += n

		// Skip key and value data
		offset += int(unshared) + int(valueLen)

		if offset > len(b.data) {
			break
		}

		count++
	}

	return count
}

// BlockIterator provides iteration over a single block
type BlockIterator struct {
	block         *Block
	index         int
	buffers       *EntryBuffers // Owns buffers for key-value reconstruction
	err           error
	currentKeyBuf []byte // Buffer owned by caller of Key() - stable until next Key() call
}

// NewIterator creates a new block iterator
func (b *Block) NewIterator() *BlockIterator {
	bi := &BlockIterator{
		block:   b,
		index:   -1,
		buffers: NewEntryBuffers(256, 256), // Start with reasonable capacity for both key and value
	}
	bi.currentKeyBuf = make([]byte, 0, 64)
	return bi
}

// SeekToFirst positions the iterator at the first element
func (it *BlockIterator) SeekToFirst() {
	it.err = nil
	it.index = 0
	it.loadCurrentEntry()
}

// SeekToLast positions the iterator at the last element
func (it *BlockIterator) SeekToLast() {
	it.err = nil
	it.index = int(it.block.numEntries) - 1
	it.loadCurrentEntry()
}

// Seek positions the iterator at the first element >= target
func (it *BlockIterator) Seek(target keys.EncodedKey) {
	it.err = nil
	it.currentKeyBuf = it.currentKeyBuf[:0]

	// Use restart point binary search to get close to target
	_, startIndex, err := it.block.seekToRestartPoint(target)
	if err != nil {
		it.err = err
		return
	}

	// Start from the restart point and search linearly
	// Use iterator's own EntryBuffers to avoid allocations
	for i := startIndex; i < int(it.block.numEntries); i++ {
		err := it.block.getEntry(i, it.buffers)
		if err != nil {
			it.err = err
			return
		}
		it.currentKeyBuf = append(it.currentKeyBuf[:0], it.buffers.key...)

		// Use efficient encoded-to-internal comparison to avoid decoding entry key
		cmp := it.buffers.key.Compare(target)
		if cmp == 0 {
			// Found exact match
			it.index = i
			return
		} else if cmp > 0 {
			// Entry key > target, we've gone past the target
			// Position at this entry since it's the first one >= target
			it.index = i
			return
		}
		// Entry key < target, continue searching
	}

	// Target not found, position at end
	it.index = int(it.block.numEntries) // Invalid position
}

// Valid returns true if the iterator is positioned at a valid element
func (it *BlockIterator) Valid() bool {
	return it.err == nil && it.index >= 0 && it.index < int(it.block.numEntries)
}

// Next moves the iterator to the next element
func (it *BlockIterator) Next() {
	if it.index < int(it.block.numEntries) {
		it.index++
		it.loadCurrentEntry()
	}
}

// Key returns the current key
func (it *BlockIterator) Key() keys.EncodedKey {
	if it.err != nil || it.index < 0 || it.index >= int(it.block.numEntries) {
		return nil
	}
	return keys.EncodedKey(it.currentKeyBuf)
}

// Value returns the current value
func (it *BlockIterator) Value() []byte {
	if it.err != nil || it.index < 0 || it.index >= int(it.block.numEntries) {
		return nil
	}
	return it.buffers.value
}

// Error returns any accumulated error
func (it *BlockIterator) Error() error {
	return it.err
}

// loadCurrentEntry loads the current entry based on the index
func (it *BlockIterator) loadCurrentEntry() {
	it.currentKeyBuf = it.currentKeyBuf[:0]
	if it.index < 0 || it.index >= int(it.block.numEntries) {
		it.err = nil
		return
	}

	// Use iterator's own EntryBuffers to avoid allocations
	err := it.block.getEntry(it.index, it.buffers)
	if err != nil {
		it.err = err
		return
	}

	it.err = nil

	// Optimization: For restart entries, the key is already pointing to block data
	// We can copy directly from block data instead of the reconstructed buffer
	if it.buffers.isRestartEntry {
		// Direct copy from block data - eliminates one copy operation
		keyLen := it.buffers.keyLength
		if cap(it.currentKeyBuf) < keyLen {
			it.currentKeyBuf = make([]byte, keyLen)
		} else {
			it.currentKeyBuf = it.currentKeyBuf[:keyLen]
		}
		copy(it.currentKeyBuf, it.buffers.blockData[it.buffers.keyOffset:it.buffers.keyOffset+keyLen])
	} else {
		// Regular entry: copy from reconstructed buffer as before
		it.currentKeyBuf = append(it.currentKeyBuf[:0], it.buffers.key...)
	}
}
