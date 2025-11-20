package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/twlk9/lgdb/bufferpool"
	"github.com/twlk9/lgdb/epoch"
	"github.com/twlk9/lgdb/keys"
)

const (
	// BlockSize is the size of each WAL block
	BlockSize = 32 * 1024 // 32KB

	// HeaderSize is the size of each record header
	HeaderSize = 4 + 4 + 8 + 1 // length + checksum + seq + type
)

// CRC32 table using the same polynomial as the original
// implementation (0xEDB88320) This ensures compatibility while using
// the optimized standard library implementation
var crc32Table = crc32.MakeTable(0xEDB88320)

// ErrCorruptRecord indicates a WAL record failed checksum validation
var ErrCorruptRecord = errors.New("WAL record corrupt: checksum mismatch")

// WALRecord represents a WAL record
type WALRecord struct {
	Type     keys.Kind
	Seq      uint64
	Key      []byte
	Value    []byte
	Checksum uint32
}

// Encode encodes a WALRecord into the provided buffer.
// The buffer must be large enough to hold the entire record.
// The total record size is returned.
func (r *WALRecord) Encode(buf []byte) int {
	// Total record size
	recordSize := HeaderSize + 4 + len(r.Key) + 4 + len(r.Value)
	offset := 0

	// Write header
	binary.LittleEndian.PutUint32(buf[offset:], uint32(recordSize))
	offset += 4

	// Checksum (placeholder)
	binary.LittleEndian.PutUint32(buf[offset:], 0)
	offset += 4

	// Sequence number
	binary.LittleEndian.PutUint64(buf[offset:], r.Seq)
	offset += 8

	// Record type
	buf[offset] = uint8(r.Type)
	offset++

	// Key length and key
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(r.Key)))
	offset += 4
	copy(buf[offset:], r.Key)
	offset += len(r.Key)

	// Value length and value
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(r.Value)))
	offset += 4
	copy(buf[offset:], r.Value)

	// Calculate and write checksum
	checksum := crc32.Checksum(buf[8:recordSize], crc32Table)
	binary.LittleEndian.PutUint32(buf[4:8], checksum)
	r.Checksum = checksum

	return recordSize
}

// Decode decodes a WALRecord from the provided buffer.
func (r *WALRecord) Decode(buf []byte) error {
	offset := 0

	// Read checksum
	r.Checksum = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	// Verify checksum
	calculatedChecksum := crc32.Checksum(buf[4:], crc32Table)
	if r.Checksum != calculatedChecksum {
		return ErrCorruptRecord
	}

	// Read sequence number
	r.Seq = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Read record type
	r.Type = keys.Kind(buf[offset])
	offset++

	// Read key
	keyLen := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	r.Key = make([]byte, keyLen)
	copy(r.Key, buf[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Read value
	valueLen := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	if valueLen > 0 {
		r.Value = make([]byte, valueLen)
		copy(r.Value, buf[offset:offset+int(valueLen)])
	} else {
		r.Value = nil
	}

	return nil
}

// SyncRequest represents a pending sync request
type SyncRequest struct {
	done chan error
}

type WALOpts struct {
	Path             string
	FileNum          uint64
	BytesPerSync     int
	AutoSyncInterval time.Duration
}

// WAL represents the Write-Ahead Log
type WAL struct {
	path   string // full path of file with file name
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	closed bool

	// Sync timing options
	autoSyncInterval time.Duration

	// Background sync options
	bytesPerSync          int
	totalBytesWritten     int64 // Total bytes written to WAL file (never reset)
	bytesWrittenSinceSync int64 // Bytes written since last sync (reset after sync)

	// Sync queue and batching
	syncQueue      *walSyncQueue
	syncInProgress bool

	// Background auto-sync
	autoSyncTicker *time.Ticker
	autoSyncDone   chan struct{}
}

// NewWAL creates a new WAL instance
func NewWAL(opts WALOpts) (*WAL, error) {
	if err := os.MkdirAll(opts.Path, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(opts.Path, fmt.Sprintf("%06d.wal", opts.FileNum))

	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		path:             walPath,
		file:             file,
		writer:           bufio.NewWriter(file),
		autoSyncInterval: opts.AutoSyncInterval,
		bytesPerSync:     opts.BytesPerSync,
		syncQueue:        &walSyncQueue{},
		autoSyncDone:     make(chan struct{}),
	}

	// Start background auto-sync if enabled
	if opts.AutoSyncInterval > 0 {
		w.autoSyncTicker = time.NewTicker(opts.AutoSyncInterval)
		go w.backgroundAutoSync()
	}

	return w, nil
}

// Path returns the full file name with path
func (w *WAL) Path() string {
	return w.path
}

// Size returns the total bytes written to the WAL file
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.totalBytesWritten
}

// WriteRecord writes a record to the WAL
func (w *WAL) WriteRecord(record *WALRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Calculate total record size to get a buffer from the pool
	recordSize := HeaderSize + 4 + len(record.Key) + 4 + len(record.Value)
	buf := bufferpool.GetBuffer(recordSize)
	defer bufferpool.PutBuffer(buf)

	// Encode the record into the buffer
	n := record.Encode(buf)
	if n != recordSize {
		return fmt.Errorf("WAL record encoding error: expected %d bytes, got %d", recordSize, n)
	}

	// Write to file
	if _, err := w.writer.Write(buf[:n]); err != nil {
		return err
	}

	// Track total bytes written (for WAL rotation)
	w.totalBytesWritten += int64(n)

	// Track bytes written since last sync (for background syncing)
	w.bytesWrittenSinceSync += int64(n)

	// Check if we should trigger background sync based on bytes
	if w.bytesPerSync > 0 && w.bytesWrittenSinceSync >= int64(w.bytesPerSync) {
		// Trigger async sync in background (counter reset by doSync)
		go func() {
			_ = w.SyncAsync()
		}()
	}

	return nil
}

// WritePut writes a put record
func (w *WAL) WritePut(seq uint64, key, value []byte) error {
	record := &WALRecord{
		Type:  keys.KindSet,
		Seq:   seq,
		Key:   key,
		Value: value,
	}
	return w.WriteRecord(record)
}

// WriteDelete writes a delete record
func (w *WAL) WriteDelete(seq uint64, key []byte) error {
	record := &WALRecord{
		Type:  keys.KindDelete,
		Seq:   seq,
		Key:   key,
		Value: nil,
	}
	return w.WriteRecord(record)
}

// WriteRangeDelete writes a range delete record
func (w *WAL) WriteRangeDelete(seq uint64, startKey, endKey []byte) error {
	record := &WALRecord{
		Type:  keys.KindRangeDelete,
		Seq:   seq,
		Key:   startKey,
		Value: endKey,
	}
	return w.WriteRecord(record)
}

// SyncAsync requests a sync and returns immediately
// Returns a channel that will receive the sync result
func (w *WAL) SyncAsync() <-chan error {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		done := make(chan error, 1)
		done <- fmt.Errorf("WAL is closed")
		return done
	}

	// Create sync request and add to queue
	req := &SyncRequest{
		done: make(chan error, 1),
	}
	w.syncQueue.put(req)

	// If a sync is already in progress, this request will be picked
	// up by the existing sync operation
	if w.syncInProgress {
		w.mu.Unlock()
		return req.done
	}

	// Start a new sync operation
	w.syncInProgress = true
	w.mu.Unlock()

	go w.processSyncQueue()

	return req.done
}

// Sync forces a sync of the WAL file (synchronous version)
func (w *WAL) Sync() error {
	return <-w.SyncAsync()
}

// processSyncQueue processes all pending sync requests
func (w *WAL) processSyncQueue() {
	w.mu.Lock()

	if w.syncQueue.len() == 0 {
		w.syncInProgress = false
		w.mu.Unlock()
		return
	}

	// Perform the actual sync
	err := w.doSync()

	// Notify all waiting requests
	for {
		req, ok := w.syncQueue.get()
		if !ok {
			break
		}
		req.done <- err
	}

	// Check if there are new pending requests and start another sync if so
	if w.syncQueue.len() > 0 {
		w.mu.Unlock()
		w.processSyncQueue() // Process next batch
	} else {
		w.syncInProgress = false
		w.mu.Unlock()
	}
}

// doSync performs the actual sync operation (must hold mutex)
func (w *WAL) doSync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	// Reset the bytes counter after successful sync
	w.bytesWrittenSinceSync = 0

	// Reset the auto-sync timer since we just synced
	// This prevents redundant syncs when byte-triggered or explicit syncs happen
	if w.autoSyncTicker != nil {
		w.autoSyncTicker.Reset(w.autoSyncInterval)
	}

	return nil
}

// backgroundAutoSync runs in a goroutine and periodically syncs the
// WAL based on time rather than bytes written. This prevents data
// loss on low-throughput workloads where bytesPerSync may never
// trigger.
func (w *WAL) backgroundAutoSync() {
	for {
		select {
		case <-w.autoSyncTicker.C:
			_ = w.SyncAsync()

		case <-w.autoSyncDone:
			return
		}
	}
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	// Stop background auto-sync goroutine
	if w.autoSyncTicker != nil {
		w.autoSyncTicker.Stop()
		close(w.autoSyncDone)
	}

	// Fail any pending sync requests
	for {
		req, ok := w.syncQueue.get()
		if !ok {
			break
		}
		req.done <- fmt.Errorf("WAL is closed")
	}

	// Final sync
	if err := w.doSync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	epoch.MarkResourceForCleanup(w.path)
	return nil
}

// WALReader reads records from a WAL file
type WALReader struct {
	file   *os.File
	reader *bufio.Reader
	path   string
}

// NewWALReader creates a new WAL reader
func NewWALReader(path string) (*WALReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &WALReader{
		file:   file,
		reader: bufio.NewReader(file),
		path:   path,
	}, nil
}

// Path returns full file path
func (r *WALReader) Path() string {
	return r.path
}

// ReadRecord reads the next record from the WAL
func (r *WALReader) ReadRecord() (*WALRecord, error) {
	// Read record size
	var recordSize uint32
	if err := binary.Read(r.reader, binary.LittleEndian, &recordSize); err != nil {
		return nil, err
	}

	// Read the rest of the record
	buf := make([]byte, recordSize-4)
	if _, err := io.ReadFull(r.reader, buf); err != nil {
		return nil, err
	}

	// Decode the record
	record := &WALRecord{}
	if err := record.Decode(buf); err != nil {
		return nil, err
	}

	return record, nil
}

// Close closes the WAL reader
func (r *WALReader) Close() error {
	if err := r.file.Close(); err != nil {
		return err
	}
	epoch.MarkResourceForCleanup(r.path)
	return nil
}
