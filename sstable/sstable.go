package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/twlk9/lgdb/compression"
	"github.com/twlk9/lgdb/keys"
)

const (
	// BlockSize is the size of each data block
	BlockSize = 4 * 1024 // 4KB

	// Restart interval - how often to place restart points
	RestartInterval = 16

	// Block trailer size (compression type + checksum)
	BlockTrailerSize = 5 // 1 byte compression + 4 bytes CRC32

	// Sparse index configuration for memory-efficient block access
	// Only build sparse index for blocks larger than this threshold
	BlockSparseIndexThreshold = 64
	// Index every Nth entry in the sparse index (balance between memory and performance)
	BlockSparseIndexInterval = 4

	// Footer constants
	MagicSize    = 8
	VersionSize  = 4
	ChecksumSize = 4

	// Maximum size for a block handle (2 varints)
	BlockHandleMaxSize = 20 // 2 * 10 (max varint size)

	// Footer size for Pebble format
	FooterSize = 1 + 2*BlockHandleMaxSize + VersionSize + MagicSize // ~53 bytes

	// Pebble version and magic
	PebbleVersion = 1

	// Compression types
	NoCompression = 0
)

var (
	// PebbleMagic is the Pebble magic number (🪳🪳)
	PebbleMagic = []byte{0xf0, 0x9f, 0xaa, 0xb3, 0xf0, 0x9f, 0xaa, 0xb3}
)

type SSTableOpts struct {
	Path                 string
	Compression          compression.Config
	Logger               *slog.Logger
	BlockSize            int
	BlockRestartInterval int
	BlockMinEntries      int
}

// SSTableWriter writes SSTables to disk
type SSTableWriter struct {
	file   *os.File
	writer *bufio.Writer
	path   string
	logger *slog.Logger

	// Data block being built
	dataBlock  *BlockBuilder
	indexBlock *BlockBuilder

	// Offset tracking
	offset     uint64
	numEntries uint64

	// Key range
	smallestKey keys.EncodedKey
	largestKey  keys.EncodedKey

	// Block handles for index
	dataBlockHandles []BlockHandle

	// Track last key of current block for index separator computation
	currentBlockLastKey keys.EncodedKey

	// Pending index entries (to compute separators when we know next key)
	pendingIndexEntries []pendingIndexEntry

	// Compression
	compressor compression.Compressor

	closed bool
}

// BlockHandle represents a pointer to a block
type BlockHandle struct {
	Offset uint64
	Size   uint64
}

// pendingIndexEntry holds information for creating index entries later
type pendingIndexEntry struct {
	handle  BlockHandle
	lastKey []byte
}

// NewSSTableWriter creates a new SSTable writer
func NewSSTableWriter(opts SSTableOpts) (*SSTableWriter, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})) // Enable info logging for debugging
	}
	// Create directory if needed
	dir := filepath.Dir(opts.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := os.Create(opts.Path)
	if err != nil {
		return nil, err
	}

	// Create compressor
	compressor, err := compression.NewCompressor(opts.Compression)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}

	return &SSTableWriter{
		file:                file,
		writer:              bufio.NewWriter(file),
		path:                opts.Path,
		logger:              opts.Logger,
		dataBlock:           NewBlockBuilder(opts.BlockSize, opts.BlockRestartInterval, opts.BlockMinEntries),
		indexBlock:          NewBlockBuilder(opts.BlockSize, opts.BlockRestartInterval, opts.BlockMinEntries),
		dataBlockHandles:    make([]BlockHandle, 0),
		pendingIndexEntries: make([]pendingIndexEntry, 0),
		compressor:          compressor,
	}, nil
}

// Add adds a key-value pair to the SSTable
// key should be an encoded internal key ([]byte)
func (w *SSTableWriter) Add(key keys.EncodedKey, value []byte) error {
	if w.closed {
		return fmt.Errorf("SSTable writer is closed")
	}

	// Validate key is not empty
	if len(key) == 0 {
		return fmt.Errorf("cannot add empty key to SSTable")
	}

	// Update key range
	if w.numEntries == 0 {
		w.smallestKey = make([]byte, len(key))
		copy(w.smallestKey, key)
	}
	w.largestKey = make([]byte, len(key))
	copy(w.largestKey, key)

	// Process any pending index entries now that we know the next key
	// This happens BEFORE adding the current key, so the separator will be
	// >= last key of previous block and < current key
	if err := w.processPendingIndexEntries(key); err != nil {
		return err
	}

	// Add to current data block
	w.dataBlock.Add(key, value)
	w.numEntries++

	// Track last key of current block (for separator computation)
	w.currentBlockLastKey = make([]byte, len(key))
	copy(w.currentBlockLastKey, key)

	// Check if current data block is full
	if w.dataBlock.IsFull() {
		// Debug: Log block statistics before flushing
		// blockSize := w.dataBlock.EstimatedSize()
		// numEntries := w.dataBlock.NumEntries()
		// w.logger.Info("Flushing data block",
		// 	"block_size", blockSize,
		// 	"num_entries", numEntries,
		// 	"avg_entry_size", blockSize/numEntries,
		// 	"sstable", w.path)

		if err := w.flushDataBlock(); err != nil {
			return err
		}
		w.dataBlock.Reset()
		w.currentBlockLastKey = nil
	}

	return nil
}

// flushDataBlock writes the current data block to file
func (w *SSTableWriter) flushDataBlock() error {
	if w.dataBlock.IsEmpty() {
		return nil
	}

	// Build the block
	blockData := w.dataBlock.Finish()

	// Compress the block
	compressedData, compressionType, err := compression.CompressBlock(w.compressor, nil, blockData)
	if err != nil {
		w.logger.Error("Failed to compress data block", "error", err, "sstable", w.path, "block_size", len(blockData), "offset", w.offset)
		return fmt.Errorf("failed to compress block: %w", err)
	}

	// Create block with trailer (compressed data + compression type + checksum)
	blockWithTrailer := make([]byte, len(compressedData)+BlockTrailerSize)
	copy(blockWithTrailer, compressedData)

	// Add compression type (1 byte)
	blockWithTrailer[len(compressedData)] = compressionType

	// Add CRC32 checksum (4 bytes) - simplified for now, use 0
	// TODO: Implement proper CRC32 checksum
	binary.LittleEndian.PutUint32(blockWithTrailer[len(compressedData)+1:], 0)

	// Write block to file
	n, err := w.writer.Write(blockWithTrailer)
	if err != nil {
		w.logger.Error("Failed to write data block to file", "error", err, "sstable", w.path, "block_size", len(blockWithTrailer), "offset", w.offset)
		return err
	}

	// Record block handle
	handle := BlockHandle{
		Offset: w.offset,
		Size:   uint64(n),
	}
	w.dataBlockHandles = append(w.dataBlockHandles, handle)
	w.offset += uint64(n)

	// Store the block handle and last key for later index entry creation
	// We'll create index entries when we know the next key (for separator computation)
	if w.currentBlockLastKey != nil {
		blockInfo := pendingIndexEntry{
			handle:  handle,
			lastKey: make([]byte, len(w.currentBlockLastKey)),
		}
		copy(blockInfo.lastKey, w.currentBlockLastKey)
		w.pendingIndexEntries = append(w.pendingIndexEntries, blockInfo)
	}

	return nil
}

// processPendingIndexEntries creates index entries for previous blocks using separators
func (w *SSTableWriter) processPendingIndexEntries(nextKey []byte) error {
	for _, entry := range w.pendingIndexEntries {
		// Compute separator between last key of block and next key
		separator := computeSeparator(entry.lastKey, nextKey)

		// Add to index block
		handleBytes := encodeBlockHandle(entry.handle)
		w.indexBlock.Add(separator, handleBytes)
	}

	// Clear processed entries
	w.pendingIndexEntries = w.pendingIndexEntries[:0]
	return nil
}

// Finish completes the SSTable and closes the writer
func (w *SSTableWriter) Finish() error {
	if w.closed {
		return nil
	}

	// Flush remaining data block
	if !w.dataBlock.IsEmpty() {
		// Debug: Log final block statistics
		// blockSize := w.dataBlock.EstimatedSize()
		// numEntries := w.dataBlock.NumEntries()
		// w.logger.Info("Flushing final data block",
		// 	"block_size", blockSize,
		// 	"num_entries", numEntries,
		// 	"avg_entry_size", blockSize/numEntries,
		// 	"sstable", w.path)

		if err := w.flushDataBlock(); err != nil {
			return err
		}
	}

	// Process any remaining pending index entries (for the final block)
	if err := w.processFinalPendingIndexEntries(); err != nil {
		return err
	}

	// Write index block (compressed like GoLevelDB)
	indexData := w.indexBlock.Finish()
	indexOffset := w.offset

	// Compress the index block like GoLevelDB does
	compressedIndex, compressionType, err := compression.CompressBlock(w.compressor, nil, indexData)
	if err != nil {
		w.logger.Error("Failed to compress index block", "error", err, "sstable", w.path, "index_size", len(indexData), "offset", indexOffset)
		return fmt.Errorf("failed to compress index block: %w", err)
	}

	// Create index block with trailer (compressed data + compression type + checksum)
	indexWithTrailer := make([]byte, len(compressedIndex)+BlockTrailerSize)
	copy(indexWithTrailer, compressedIndex)

	// Add compression type (1 byte)
	indexWithTrailer[len(compressedIndex)] = compressionType

	// Add CRC32 checksum (4 bytes) - simplified for now, use 0
	// TODO: Implement proper CRC32 checksum for index block
	binary.LittleEndian.PutUint32(indexWithTrailer[len(compressedIndex)+1:], 0)

	n, err := w.writer.Write(indexWithTrailer)
	if err != nil {
		w.logger.Error("Failed to write compressed index block to file", "error", err, "sstable", w.path, "compressed_index_size", len(indexWithTrailer), "original_index_size", len(indexData), "offset", indexOffset)
		return err
	}
	indexSize := uint64(n)
	w.offset += indexSize

	// Write footer (Pebble format)
	footer := make([]byte, FooterSize)
	footerOffset := 0

	// Checksum type (1 byte)
	footer[footerOffset] = NoCompression
	footerOffset++

	// Meta index block handle (varints) - empty for now
	metaHandle := BlockHandle{Offset: 0, Size: 0}
	metaHandleBytes := encodeBlockHandle(metaHandle)
	copy(footer[footerOffset:], metaHandleBytes)
	footerOffset += len(metaHandleBytes)

	// Index block handle (varints)
	indexHandle := BlockHandle{Offset: indexOffset, Size: indexSize}
	indexHandleBytes := encodeBlockHandle(indexHandle)
	copy(footer[footerOffset:], indexHandleBytes)
	footerOffset += len(indexHandleBytes)

	// Pad to fixed position for version and magic
	// Version is at FooterSize - MagicSize - VersionSize
	versionOffset := FooterSize - MagicSize - VersionSize
	for footerOffset < versionOffset {
		footer[footerOffset] = 0
		footerOffset++
	}

	// Version (4 bytes, little-endian)
	binary.LittleEndian.PutUint32(footer[versionOffset:], PebbleVersion)

	// Magic number (8 bytes)
	copy(footer[FooterSize-MagicSize:], PebbleMagic)

	if _, err := w.writer.Write(footer); err != nil {
		w.logger.Error("Failed to write footer to file", "error", err, "sstable", w.path, "footer_size", len(footer))
		return err
	}

	// Flush and close
	if err := w.writer.Flush(); err != nil {
		w.logger.Error("Failed to flush writer", "error", err, "sstable", w.path)
		return err
	}

	return w.file.Sync()
}

func (w *SSTableWriter) Close() error {
	// Ensure all data is written to disk before closing
	if err := w.file.Sync(); err != nil {
		return err
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	// Sync the directory to ensure the file entry is visible to new opens
	// This is critical for preventing race conditions where the file content
	// is on disk but the directory entry isn't visible yet.
	if err := w.syncDir(); err != nil {
		return err
	}

	w.closed = true
	return nil
}

// syncDir syncs the directory containing the SSTable file to ensure
// the directory entry is written to disk. This prevents race conditions
// where file content is synced but directory entry isn't visible yet.
func (w *SSTableWriter) syncDir() error {
	dir := filepath.Dir(w.path)
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()

	// On some systems, directory sync might not be supported, so ignore EINVAL
	if err := f.Sync(); err != nil {
		// Check if it's an "invalid argument" error which can be ignored
		if isErrInvalid(err) {
			return nil
		}
		return err
	}
	return nil
}

// isErrInvalid checks if error is EINVAL (invalid argument)
// Some systems don't support directory sync, so we ignore this error
func isErrInvalid(err error) bool {
	if err == os.ErrInvalid {
		return true
	}
	// Check for syscall.EINVAL in various error wrapper types
	return false // Simplified for now
}

// EstimatedSize returns the estimated size of the SSTable
func (w *SSTableWriter) EstimatedSize() uint64 {
	return w.offset + uint64(w.dataBlock.EstimatedSize()) + uint64(w.indexBlock.EstimatedSize()) + FooterSize
}

// NumEntries returns the number of entries added
func (w *SSTableWriter) NumEntries() uint64 {
	return w.numEntries
}

// SmallestKey returns the smallest key
func (w *SSTableWriter) SmallestKey() keys.EncodedKey {
	return w.smallestKey
}

// LargestKey returns the largest key
func (w *SSTableWriter) LargestKey() keys.EncodedKey {
	return w.largestKey
}

// File returns the underlying file descriptor for injection into cache
// This is used to pass the open file descriptor to the cache after Finish()
func (w *SSTableWriter) File() *os.File {
	return w.file
}

// encodeBlockHandle encodes a block handle using varints (Pebble format)
func encodeBlockHandle(handle BlockHandle) []byte {
	buf := make([]byte, BlockHandleMaxSize)
	n := binary.PutUvarint(buf, handle.Offset)
	m := binary.PutUvarint(buf[n:], handle.Size)
	return buf[:n+m]
}

// decodeBlockHandle decodes a block handle from varints (Pebble format)
func decodeBlockHandle(data []byte) (BlockHandle, int) {
	offset, n := binary.Uvarint(data)
	if n <= 0 {
		return BlockHandle{}, 0
	}
	size, m := binary.Uvarint(data[n:])
	if m <= 0 {
		return BlockHandle{}, 0
	}
	return BlockHandle{Offset: offset, Size: size}, n + m
}

// processFinalPendingIndexEntries handles the final block (uses successor instead of separator)
func (w *SSTableWriter) processFinalPendingIndexEntries() error {
	for _, entry := range w.pendingIndexEntries {
		// For the final block, use successor of the last key
		successor := computeSuccessor(entry.lastKey)

		// Add to index block
		handleBytes := encodeBlockHandle(entry.handle)
		w.indexBlock.Add(successor, handleBytes)
	}

	// Clear processed entries
	w.pendingIndexEntries = w.pendingIndexEntries[:0]
	return nil
}

// computeSeparator computes a separator key between lastKey and nextKey
// The separator should be >= lastKey and < nextKey, allowing binary search
// to find the correct block for any key in the range [lastKey, separator)
// CRITICAL FIX: lastKey and nextKey are encoded internal keys, not user keys!
func computeSeparator(lastKey, nextKey keys.EncodedKey) keys.EncodedKey {
	// Extract user keys for separator computation
	lastUserKey := lastKey.UserKey()
	nextUserKey := nextKey.UserKey()

	// Find the shortest user key that is > lastUserKey and < nextUserKey
	// This follows Pebble's separator computation logic on user keys
	i := 0
	for i < len(lastUserKey) && i < len(nextUserKey) && lastUserKey[i] == nextUserKey[i] {
		i++
	}

	var separatorUserKey []byte

	// If we found a differing byte and can increment it
	if i < len(lastUserKey) && i < len(nextUserKey) && lastUserKey[i] < nextUserKey[i] && lastUserKey[i] < 255 {
		// Create separator by taking common prefix + incremented differing byte
		separatorUserKey = make([]byte, i+1)
		copy(separatorUserKey, lastUserKey[:i])
		separatorUserKey[i] = lastUserKey[i] + 1
	} else if i == len(lastUserKey) && i < len(nextUserKey) {
		// If lastUserKey is a prefix of nextUserKey, append \x00
		separatorUserKey = make([]byte, len(lastUserKey)+1)
		copy(separatorUserKey, lastUserKey)
		separatorUserKey[len(lastUserKey)] = 0
	} else {
		// Fallback: use a simple increment of lastUserKey
		// Find the last byte we can increment
		found := false
		for j := len(lastUserKey) - 1; j >= 0; j-- {
			if lastUserKey[j] < 255 {
				separatorUserKey = make([]byte, j+1)
				copy(separatorUserKey, lastUserKey[:j])
				separatorUserKey[j] = lastUserKey[j] + 1
				found = true
				break
			}
		}
		if !found {
			// If we can't increment anything, just append a byte
			separatorUserKey = make([]byte, len(lastUserKey)+1)
			copy(separatorUserKey, lastUserKey)
			separatorUserKey[len(lastUserKey)] = 0
		}
	}

	// CRITICAL: Encode the separator user key back as an internal key
	// Use max sequence number to ensure it's greater than any key in the previous block
	separatorInternal := keys.NewEncodedKey(separatorUserKey, keys.MaxSequenceNumber, keys.KindSet)
	return separatorInternal
}

// computeSuccessor computes a successor key for the final block
// CRITICAL FIX: lastKey is an encoded internal key, not user key!
func computeSuccessor(lastKey keys.EncodedKey) []byte {
	// Create successor of user key (append null byte)
	ukey := lastKey.UserKey()
	successorUserKey := make([]byte, len(ukey)+1)
	copy(successorUserKey, ukey)
	successorUserKey[len(ukey)] = 0

	// CRITICAL: Encode the successor user key back as an internal key
	// Use max sequence number to ensure proper ordering
	successorInternal := keys.NewEncodedKey(successorUserKey, keys.MaxSequenceNumber, keys.KindSet)
	return successorInternal
}
