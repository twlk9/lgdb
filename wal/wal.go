package wal

import (
	"bufio"
	"encoding/binary"
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

// CRC32 table using the same polynomial as the original implementation (0xEDB88320)
// This ensures compatibility while using the optimized standard library implementation
var crc32Table = crc32.MakeTable(0xEDB88320)

// WALRecord represents a WAL record
type WALRecord struct {
	Type     keys.Kind
	Seq      uint64
	Key      []byte
	Value    []byte
	Checksum uint32
}

// SyncRequest represents a pending sync request
type SyncRequest struct {
	done chan error
}

type WALOpts struct {
	Path            string
	FileNum         uint64
	SyncInterval    time.Duration
	MinSyncInterval time.Duration
	BytesPerSync    int
}

// WAL represents the Write-Ahead Log
type WAL struct {
	path    string // full path of file with file name
	file    *os.File
	writer  *bufio.Writer
	mu      sync.Mutex
	closed  bool
	fileNum uint64

	// Sync timing options
	syncInterval    time.Duration
	minSyncInterval time.Duration

	// Background sync options
	bytesPerSync int
	bytesWritten int64

	// Sync queue and batching
	syncQueue      *walSyncQueue
	lastSyncTime   time.Time
	syncTimer      *time.Timer
	syncInProgress bool
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

	return &WAL{
		path:            walPath,
		file:            file,
		writer:          bufio.NewWriter(file),
		fileNum:         opts.FileNum,
		syncInterval:    opts.SyncInterval,
		minSyncInterval: opts.MinSyncInterval,
		bytesPerSync:    opts.BytesPerSync,
		syncQueue:       &walSyncQueue{},
	}, nil
}

// Path returns the full file name with path
func (w *WAL) Path() string {
	return w.path
}

// Size returns the bytes written
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytesWritten
}

// WriteRecord writes a record to the WAL
func (w *WAL) WriteRecord(record *WALRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Calculate total record size
	recordSize := HeaderSize + 4 + len(record.Key) + 4 + len(record.Value)

	// Create buffer for the record using buffer pool
	buf := bufferpool.GetBuffer(recordSize)
	defer bufferpool.PutBuffer(buf)
	buf = buf[:recordSize]
	offset := 0

	// Write header
	binary.LittleEndian.PutUint32(buf[offset:], uint32(recordSize))
	offset += 4

	// Checksum (placeholder, will be calculated)
	binary.LittleEndian.PutUint32(buf[offset:], 0)
	offset += 4

	// Sequence number
	binary.LittleEndian.PutUint64(buf[offset:], record.Seq)
	offset += 8

	// Record type
	buf[offset] = uint8(record.Type)
	offset += 1

	// Key length and key
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(record.Key)))
	offset += 4
	copy(buf[offset:], record.Key)
	offset += len(record.Key)

	// Value length and value
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(record.Value)))
	offset += 4
	copy(buf[offset:], record.Value)

	// Calculate and write checksum
	checksum := crc32.Checksum(buf[8:], crc32Table) // Skip length and checksum fields
	binary.LittleEndian.PutUint32(buf[4:8], checksum)

	// Write to file
	n, err := w.writer.Write(buf)
	if err != nil {
		return err
	}

	// Track bytes written for background syncing
	w.bytesWritten += int64(n)

	// Check if we should trigger background sync
	if w.bytesPerSync > 0 && w.bytesWritten >= int64(w.bytesPerSync) {
		// Reset counter and trigger async sync in background
		w.bytesWritten = 0
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
	defer w.mu.Unlock()

	if w.closed {
		done := make(chan error, 1)
		done <- fmt.Errorf("WAL is closed")
		return done
	}

	// Create sync request
	req := &SyncRequest{
		done: make(chan error, 1),
	}

	// Add to queue
	w.syncQueue.put(req)

	// Check if we need to delay sync due to minSyncInterval
	if w.minSyncInterval > 0 {
		timeSinceLastSync := time.Since(w.lastSyncTime)
		if timeSinceLastSync < w.minSyncInterval && !w.lastSyncTime.IsZero() {
			// Start timer if not already running
			if w.syncTimer == nil {
				delay := w.minSyncInterval - timeSinceLastSync
				w.syncTimer = time.AfterFunc(delay, func() {
					w.processSyncQueue()
				})
			}
			return req.done
		}
	}

	// Process sync immediately
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
	defer w.mu.Unlock()

	if w.closed || w.syncInProgress {
		return
	}

	// Clear timer
	if w.syncTimer != nil {
		w.syncTimer.Stop()
		w.syncTimer = nil
	}

	if w.syncQueue.len() == 0 {
		return
	}

	w.syncInProgress = true

	// Perform the actual sync
	err := w.doSync()
	w.lastSyncTime = time.Now()
	w.syncInProgress = false

	// Notify all waiting requests
	for {
		req, ok := w.syncQueue.get()
		if !ok {
			break
		}
		req.done <- err
	}
}

// doSync performs the actual sync operation (must hold mutex)
func (w *WAL) doSync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	// Stop any pending timer
	if w.syncTimer != nil {
		w.syncTimer.Stop()
		w.syncTimer = nil
	}

	// Fail any pending sync requests
	for {
		req, ok := w.syncQueue.get()
		if !ok {
			break
		}
		req.done <- fmt.Errorf("WAL is closed")
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
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
	buf := make([]byte, recordSize-4) // -4 because we already read the size
	if _, err := io.ReadFull(r.reader, buf); err != nil {
		return nil, err
	}

	offset := 0

	// Read checksum
	checksum := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	// Verify checksum
	calculatedChecksum := crc32.Checksum(buf[4:], crc32Table) // Skip checksum field
	if checksum != calculatedChecksum {
		return nil, fmt.Errorf("checksum mismatch")
	}

	// Read sequence number
	seq := binary.LittleEndian.Uint64(buf[offset:])
	offset += 8

	// Read record type
	recordType := buf[offset]
	offset += 1

	// Read key length and key
	keyLen := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	key := make([]byte, keyLen)
	copy(key, buf[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Read value length and value
	valueLen := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	var value []byte
	if valueLen > 0 {
		value = make([]byte, valueLen)
		copy(value, buf[offset:offset+int(valueLen)])
	}

	return &WALRecord{
		Type:     keys.Kind(recordType),
		Seq:      seq,
		Key:      key,
		Value:    value,
		Checksum: checksum,
	}, nil
}

// Close closes the WAL reader
func (r *WALReader) Close() error {
	return r.file.Close()
}
