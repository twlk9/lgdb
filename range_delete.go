package lgdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	// Range delete record types
	RangeDeleteRecordEdit = 1

	// Range delete file extension
	RangeDeleteExtension = ".rangedel"

	// Range delete header size (length + checksum + type)
	RangeDeleteHeaderSize = 4 + 4 + 1

	// Range delete edit tags
	tagAddRangeDelete    = 1
	tagRemoveRangeDelete = 2
)

// Use the same CRC32 table as manifest for consistency
var rangeDeleteCrc32Table = crc32.MakeTable(0xEDB88320)

// RangeTombstone represents a range deletion tombstone
type RangeTombstone struct {
	ID    uint64 // Unique identifier for this range delete
	Start []byte // Inclusive start key
	End   []byte // Exclusive end key
	Seq   uint64 // Sequence number when created
}

// RangeDeleteRecord represents a record in the range delete file
type RangeDeleteRecord struct {
	Type     uint8
	Data     []byte
	Checksum uint32
}

// RangeDeleteEdit represents changes to the range delete set
type RangeDeleteEdit struct {
	addedDeletes   []RangeTombstone
	removedDeletes []uint64 // IDs to remove
}

// NewRangeDeleteEdit creates a new range delete edit
func NewRangeDeleteEdit() *RangeDeleteEdit {
	return &RangeDeleteEdit{
		addedDeletes:   make([]RangeTombstone, 0),
		removedDeletes: make([]uint64, 0),
	}
}

// AddRangeDelete adds a range delete to the edit
func (e *RangeDeleteEdit) AddRangeDelete(rt RangeTombstone) {
	e.addedDeletes = append(e.addedDeletes, rt)
}

// RemoveRangeDelete marks a range delete for removal
func (e *RangeDeleteEdit) RemoveRangeDelete(id uint64) {
	e.removedDeletes = append(e.removedDeletes, id)
}

// IsEmpty returns true if the edit has no changes
func (e *RangeDeleteEdit) IsEmpty() bool {
	return len(e.addedDeletes) == 0 && len(e.removedDeletes) == 0
}

// RangeDeleteWriter handles writing to range delete files
type RangeDeleteWriter struct {
	path    string
	file    *os.File
	writer  *bufio.Writer
	mu      sync.Mutex
	closed  bool
	fileNum uint64
}

// NewRangeDeleteWriter creates a new range delete writer
func NewRangeDeleteWriter(dir string, fileNum uint64) (*RangeDeleteWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, fmt.Sprintf("%06d%s", fileNum, RangeDeleteExtension))

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &RangeDeleteWriter{
		path:    path,
		file:    file,
		writer:  bufio.NewWriter(file),
		fileNum: fileNum,
	}, nil
}

// WriteEdit writes a range delete edit to the file
func (w *RangeDeleteWriter) WriteEdit(edit *RangeDeleteEdit) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("range delete writer is closed")
	}

	// Don't write empty edits
	if edit.IsEmpty() {
		return nil
	}

	// Encode the edit
	data, err := w.encodeEdit(edit)
	if err != nil {
		return err
	}

	// Create the record
	record := &RangeDeleteRecord{
		Type: RangeDeleteRecordEdit,
		Data: data,
	}

	return w.writeRecord(record)
}

// writeRecord writes a record to the file
func (w *RangeDeleteWriter) writeRecord(record *RangeDeleteRecord) error {
	// Calculate total record size
	recordSize := RangeDeleteHeaderSize + len(record.Data)

	// Create buffer for the record
	buf := make([]byte, recordSize)
	offset := 0

	// Write length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(recordSize))
	offset += 4

	// Checksum (placeholder, will be calculated)
	binary.LittleEndian.PutUint32(buf[offset:], 0)
	offset += 4

	// Record type
	buf[offset] = record.Type
	offset += 1

	// Data
	copy(buf[offset:], record.Data)

	// Calculate and write checksum (skip length and checksum fields)
	checksum := crc32.Checksum(buf[8:], rangeDeleteCrc32Table)
	binary.LittleEndian.PutUint32(buf[4:8], checksum)

	// Write to file
	if _, err := w.writer.Write(buf); err != nil {
		return err
	}

	return w.writer.Flush()
}

// encodeEdit encodes a range delete edit into binary format
func (w *RangeDeleteWriter) encodeEdit(edit *RangeDeleteEdit) ([]byte, error) {
	var buf bytes.Buffer

	// Write added range deletes
	for _, rt := range edit.addedDeletes {
		buf.WriteByte(tagAddRangeDelete)

		// ID
		binary.Write(&buf, binary.LittleEndian, rt.ID)

		// Sequence number
		binary.Write(&buf, binary.LittleEndian, rt.Seq)

		// Start key length and key
		binary.Write(&buf, binary.LittleEndian, uint32(len(rt.Start)))
		buf.Write(rt.Start)

		// End key length and key
		binary.Write(&buf, binary.LittleEndian, uint32(len(rt.End)))
		buf.Write(rt.End)
	}

	// Write removed range deletes
	for _, id := range edit.removedDeletes {
		buf.WriteByte(tagRemoveRangeDelete)

		// ID
		binary.Write(&buf, binary.LittleEndian, id)
	}

	return buf.Bytes(), nil
}

// Sync forces a sync of the file
func (w *RangeDeleteWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("range delete writer is closed")
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	return w.file.Sync()
}

// Close closes the writer
func (w *RangeDeleteWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	if err := w.writer.Flush(); err != nil {
		return err
	}

	return w.file.Close()
}

// RangeDeleteReader reads records from a range delete file
type RangeDeleteReader struct {
	file   *os.File
	reader *bufio.Reader
	path   string
}

// NewRangeDeleteReader creates a new range delete reader
func NewRangeDeleteReader(path string) (*RangeDeleteReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &RangeDeleteReader{
		file:   file,
		reader: bufio.NewReader(file),
		path:   path,
	}, nil
}

// ReadRecord reads the next record from the file
func (r *RangeDeleteReader) ReadRecord() (*RangeDeleteRecord, error) {
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
	calculatedChecksum := crc32.Checksum(buf[4:], rangeDeleteCrc32Table)
	if checksum != calculatedChecksum {
		return nil, fmt.Errorf("checksum mismatch")
	}

	// Read record type
	recordType := buf[offset]
	offset += 1

	// Read data
	data := make([]byte, len(buf)-offset)
	copy(data, buf[offset:])

	return &RangeDeleteRecord{
		Type:     recordType,
		Data:     data,
		Checksum: checksum,
	}, nil
}

// ReadEdit reads and decodes a range delete edit from the data
func (r *RangeDeleteReader) ReadEdit(data []byte) (*RangeDeleteEdit, error) {
	edit := NewRangeDeleteEdit()
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Read tag
		tag, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}

		switch tag {
		case tagAddRangeDelete:
			// Read ID
			var id uint64
			if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
				return nil, err
			}

			// Read sequence number
			var seq uint64
			if err := binary.Read(buf, binary.LittleEndian, &seq); err != nil {
				return nil, err
			}

			// Read start key
			var startLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &startLen); err != nil {
				return nil, err
			}
			start := make([]byte, startLen)
			if _, err := io.ReadFull(buf, start); err != nil {
				return nil, err
			}

			// Read end key
			var endLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &endLen); err != nil {
				return nil, err
			}
			end := make([]byte, endLen)
			if _, err := io.ReadFull(buf, end); err != nil {
				return nil, err
			}

			// Add to edit
			edit.AddRangeDelete(RangeTombstone{
				ID:    id,
				Start: start,
				End:   end,
				Seq:   seq,
			})

		case tagRemoveRangeDelete:
			// Read ID
			var id uint64
			if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
				return nil, err
			}

			edit.RemoveRangeDelete(id)

		default:
			return nil, fmt.Errorf("unknown tag: %d", tag)
		}
	}

	return edit, nil
}

// Close closes the reader
func (r *RangeDeleteReader) Close() error {
	return r.file.Close()
}

// RecoverRangeDeletes recovers range deletes from a range delete file
func RecoverRangeDeletes(dir string, versionNum uint64) ([]RangeTombstone, error) {
	path := filepath.Join(dir, fmt.Sprintf("%06d%s", versionNum, RangeDeleteExtension))

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// No range delete file is fine - means no active range deletes
		return nil, nil
	}

	reader, err := NewRangeDeleteReader(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Map to track active range deletes (handles add/remove)
	rangeDeletes := make(map[uint64]RangeTombstone)

	// Apply all edits
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if record.Type == RangeDeleteRecordEdit {
			edit, err := reader.ReadEdit(record.Data)
			if err != nil {
				return nil, err
			}

			// Apply additions
			for _, rt := range edit.addedDeletes {
				rangeDeletes[rt.ID] = rt
			}

			// Apply removals
			for _, id := range edit.removedDeletes {
				delete(rangeDeletes, id)
			}
		}
	}

	// Convert map to slice
	result := make([]RangeTombstone, 0, len(rangeDeletes))
	for _, rt := range rangeDeletes {
		result = append(result, rt)
	}

	return result, nil
}

// FindRangeDeleteFiles finds all range delete files in the directory
func FindRangeDeleteFiles(dir string) ([]string, error) {
	pattern := filepath.Join(dir, "*"+RangeDeleteExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	return files, nil
}

// ParseRangeDeleteFileNum extracts the file number from a range delete filename
func ParseRangeDeleteFileNum(filename string) (uint64, error) {
	base := filepath.Base(filename)
	if !strings.HasSuffix(base, RangeDeleteExtension) {
		return 0, fmt.Errorf("not a range delete file: %s", filename)
	}
	numStr := strings.TrimSuffix(base, RangeDeleteExtension)
	return strconv.ParseUint(numStr, 10, 64)
}
