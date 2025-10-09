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
	// Manifest record types
	ManifestRecordVersionEdit = 1

	// Manifest file extension
	ManifestExtension = ".manifest"

	// Manifest header size (length + checksum + type)
	ManifestHeaderSize = 4 + 4 + 1

	// Version edit tags
	tagAddFile                  = 1
	tagRemoveFile               = 2
	tagAddRangeTombstoneFile    = 3
	tagRemoveRangeTombstoneFile = 4
)

// CRC32 table using the same polynomial as the original implementation (0xEDB88320)
// This ensures compatibility while using the optimized standard library implementation
var manifestCrc32Table = crc32.MakeTable(0xEDB88320)

// ManifestRecord represents a record in the manifest file
type ManifestRecord struct {
	Type     uint8
	Data     []byte
	Checksum uint32
}

// ManifestWriter handles writing to the manifest file
type ManifestWriter struct {
	path    string
	file    *os.File
	writer  *bufio.Writer
	mu      sync.Mutex
	closed  bool
	fileNum uint64
	size    int64  // Current file size for rotation tracking
	maxSize int64  // Maximum size before rotation
	dir     string // Directory for creating new manifest files
}

// NewManifestWriter creates a new manifest writer.
func NewManifestWriter(dir string, fileNum uint64, maxSize int64) (*ManifestWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	manifestPath := filepath.Join(dir, fmt.Sprintf("%06d%s", fileNum, ManifestExtension))

	file, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	mw := &ManifestWriter{
		path:    manifestPath,
		file:    file,
		writer:  bufio.NewWriter(file),
		fileNum: fileNum,
		size:    stat.Size(),
		maxSize: maxSize,
		dir:     dir,
	}

	// Write CURRENT file to point to this manifest
	if err := writeCURRENT(dir, fileNum); err != nil {
		file.Close() // Clean up on error
		return nil, fmt.Errorf("failed to write CURRENT file: %w", err)
	}

	return mw, nil
}

// WriteVersionEdit writes a version edit to the manifest.
func (mw *ManifestWriter) WriteVersionEdit(edit *VersionEdit) error {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	if mw.closed {
		return fmt.Errorf("manifest writer is closed")
	}

	// Encode the version edit
	data, err := mw.encodeVersionEdit(edit)
	if err != nil {
		return err
	}

	// Create the record
	record := &ManifestRecord{
		Type: ManifestRecordVersionEdit,
		Data: data,
	}

	return mw.writeRecord(record)
}

// writeRecord writes a record to the manifest file.
func (mw *ManifestWriter) writeRecord(record *ManifestRecord) error {
	// Calculate total record size
	recordSize := ManifestHeaderSize + len(record.Data)

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
	checksum := crc32.Checksum(buf[8:], manifestCrc32Table)
	binary.LittleEndian.PutUint32(buf[4:8], checksum)

	// Write to file
	if _, err := mw.writer.Write(buf); err != nil {
		return err
	}

	// Update size tracking
	mw.size += int64(len(buf))

	return mw.writer.Flush()
}

// encodeVersionEdit encodes a version edit into binary format.
func (mw *ManifestWriter) encodeVersionEdit(edit *VersionEdit) ([]byte, error) {
	var buf bytes.Buffer

	// Write added files
	for level, files := range edit.addFiles {
		for _, file := range files {
			// Tag for add file
			buf.WriteByte(tagAddFile)

			// Level
			binary.Write(&buf, binary.LittleEndian, uint32(level))

			// File number
			binary.Write(&buf, binary.LittleEndian, file.FileNum)

			// File size
			binary.Write(&buf, binary.LittleEndian, file.Size)

			// Smallest key length and key
			binary.Write(&buf, binary.LittleEndian, uint32(len(file.SmallestKey)))
			buf.Write(file.SmallestKey)

			// Largest key length and key
			binary.Write(&buf, binary.LittleEndian, uint32(len(file.LargestKey)))
			buf.Write(file.LargestKey)

			// Number of entries (new field for bloom filter support)
			binary.Write(&buf, binary.LittleEndian, file.NumEntries)
		}
	}

	// Write removed files
	for level, fileNums := range edit.removeFiles {
		for _, fileNum := range fileNums {
			// Tag for remove file
			buf.WriteByte(tagRemoveFile)

			// Level
			binary.Write(&buf, binary.LittleEndian, uint32(level))

			// File number
			binary.Write(&buf, binary.LittleEndian, fileNum)
		}
	}

	// Write added range tombstone files
	for level, files := range edit.addRangeTombstoneFiles {
		for _, file := range files {
			// Tag for add range tombstone file
			buf.WriteByte(tagAddRangeTombstoneFile)

			// Level
			binary.Write(&buf, binary.LittleEndian, uint32(level))

			// File number
			binary.Write(&buf, binary.LittleEndian, file.FileNum)

			// File size
			binary.Write(&buf, binary.LittleEndian, file.Size)

			// Tombstone count
			binary.Write(&buf, binary.LittleEndian, uint32(file.TombstoneCount))

			// Smallest key length and key
			binary.Write(&buf, binary.LittleEndian, uint32(len(file.SmallestKey)))
			buf.Write(file.SmallestKey)

			// Largest key length and key
			binary.Write(&buf, binary.LittleEndian, uint32(len(file.LargestKey)))
			buf.Write(file.LargestKey)
		}
	}

	// Write removed range tombstone files
	for level, fileNums := range edit.removeRangeTombstoneFiles {
		for _, fileNum := range fileNums {
			// Tag for remove range tombstone file
			buf.WriteByte(tagRemoveRangeTombstoneFile)

			// Level
			binary.Write(&buf, binary.LittleEndian, uint32(level))

			// File number
			binary.Write(&buf, binary.LittleEndian, fileNum)
		}
	}

	return buf.Bytes(), nil
}

// Sync forces a sync of the manifest file.
func (mw *ManifestWriter) Sync() error {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	if mw.closed {
		return fmt.Errorf("manifest writer is closed")
	}

	if err := mw.writer.Flush(); err != nil {
		return err
	}

	return mw.file.Sync()
}

// NeedsRotation returns true if the manifest file should be rotated.
func (mw *ManifestWriter) NeedsRotation() bool {
	return mw.size >= mw.maxSize
}

// GetSize returns the current size of the manifest file.
func (mw *ManifestWriter) GetSize() int64 {
	return mw.size
}

// GetFileNum returns the current file number.
func (mw *ManifestWriter) GetFileNum() uint64 {
	return mw.fileNum
}

// Close closes the manifest writer.
func (mw *ManifestWriter) Close() error {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	if mw.closed {
		return nil
	}

	mw.closed = true

	if err := mw.writer.Flush(); err != nil {
		return err
	}

	return mw.file.Close()
}

// ManifestReader reads records from a manifest file
type ManifestReader struct {
	file   *os.File
	reader *bufio.Reader
	path   string
}

// NewManifestReader creates a new manifest reader.
func NewManifestReader(path string) (*ManifestReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &ManifestReader{
		file:   file,
		reader: bufio.NewReader(file),
		path:   path,
	}, nil
}

// ReadRecord reads the next record from the manifest.
func (mr *ManifestReader) ReadRecord() (*ManifestRecord, error) {
	// Read record size
	var recordSize uint32
	if err := binary.Read(mr.reader, binary.LittleEndian, &recordSize); err != nil {
		return nil, err
	}

	// Read the rest of the record
	buf := make([]byte, recordSize-4) // -4 because we already read the size
	if _, err := io.ReadFull(mr.reader, buf); err != nil {
		return nil, err
	}

	offset := 0

	// Read checksum
	checksum := binary.LittleEndian.Uint32(buf[offset:])
	offset += 4

	// Verify checksum
	calculatedChecksum := crc32.Checksum(buf[4:], manifestCrc32Table) // Skip checksum field
	if checksum != calculatedChecksum {
		return nil, fmt.Errorf("checksum mismatch")
	}

	// Read record type
	recordType := buf[offset]
	offset += 1

	// Read data
	data := make([]byte, len(buf)-offset)
	copy(data, buf[offset:])

	return &ManifestRecord{
		Type:     recordType,
		Data:     data,
		Checksum: checksum,
	}, nil
}

// ReadVersionEdit reads and decodes a version edit from the data.
func (mr *ManifestReader) ReadVersionEdit(data []byte) (*VersionEdit, error) {
	edit := NewVersionEdit()
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Read tag
		tag, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}

		switch tag {
		case tagAddFile:
			// Read level
			var level uint32
			if err := binary.Read(buf, binary.LittleEndian, &level); err != nil {
				return nil, err
			}

			// Read file number
			var fileNum uint64
			if err := binary.Read(buf, binary.LittleEndian, &fileNum); err != nil {
				return nil, err
			}

			// Read file size
			var fileSize uint64
			if err := binary.Read(buf, binary.LittleEndian, &fileSize); err != nil {
				return nil, err
			}

			// Read smallest key
			var smallestKeyLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &smallestKeyLen); err != nil {
				return nil, err
			}
			smallestKey := make([]byte, smallestKeyLen)
			if _, err := io.ReadFull(buf, smallestKey); err != nil {
				return nil, err
			}

			// Read largest key
			var largestKeyLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &largestKeyLen); err != nil {
				return nil, err
			}
			largestKey := make([]byte, largestKeyLen)
			if _, err := io.ReadFull(buf, largestKey); err != nil {
				return nil, err
			}

			// Read number of entries (optional field for backwards compatibility)
			var numEntries uint64
			if buf.Len() >= 8 {
				if err := binary.Read(buf, binary.LittleEndian, &numEntries); err != nil {
					// If read fails, default to 0 (backwards compatible)
					numEntries = 0
				}
			}

			// Create file metadata
			file := &FileMetadata{
				FileNum:     fileNum,
				Size:        fileSize,
				SmallestKey: smallestKey,
				LargestKey:  largestKey,
				NumEntries:  numEntries,
			}

			edit.AddFile(int(level), file)

		case tagRemoveFile:
			// Read level
			var level uint32
			if err := binary.Read(buf, binary.LittleEndian, &level); err != nil {
				return nil, err
			}

			// Read file number
			var fileNum uint64
			if err := binary.Read(buf, binary.LittleEndian, &fileNum); err != nil {
				return nil, err
			}

			edit.RemoveFile(int(level), fileNum)

		case tagAddRangeTombstoneFile:
			// Read level
			var level uint32
			if err := binary.Read(buf, binary.LittleEndian, &level); err != nil {
				return nil, err
			}

			// Read file number
			var fileNum uint64
			if err := binary.Read(buf, binary.LittleEndian, &fileNum); err != nil {
				return nil, err
			}

			// Read file size
			var fileSize uint64
			if err := binary.Read(buf, binary.LittleEndian, &fileSize); err != nil {
				return nil, err
			}

			// Read tombstone count
			var tombstoneCount uint32
			if err := binary.Read(buf, binary.LittleEndian, &tombstoneCount); err != nil {
				return nil, err
			}

			// Read smallest key
			var smallestKeyLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &smallestKeyLen); err != nil {
				return nil, err
			}
			smallestKey := make([]byte, smallestKeyLen)
			if _, err := io.ReadFull(buf, smallestKey); err != nil {
				return nil, err
			}

			// Read largest key
			var largestKeyLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &largestKeyLen); err != nil {
				return nil, err
			}
			largestKey := make([]byte, largestKeyLen)
			if _, err := io.ReadFull(buf, largestKey); err != nil {
				return nil, err
			}

			// Create range tombstone file metadata
			file := &RangeTombstoneFileMetadata{
				FileNum:        fileNum,
				Level:          int(level),
				Size:           fileSize,
				SmallestKey:    smallestKey,
				LargestKey:     largestKey,
				TombstoneCount: int(tombstoneCount),
			}

			edit.AddRangeTombstoneFile(int(level), file)

		case tagRemoveRangeTombstoneFile:
			// Read level
			var level uint32
			if err := binary.Read(buf, binary.LittleEndian, &level); err != nil {
				return nil, err
			}

			// Read file number
			var fileNum uint64
			if err := binary.Read(buf, binary.LittleEndian, &fileNum); err != nil {
				return nil, err
			}

			edit.RemoveRangeTombstoneFile(int(level), fileNum)

		default:
			return nil, fmt.Errorf("unknown tag: %d", tag)
		}
	}

	return edit, nil
}

// Close closes the manifest reader.
func (mr *ManifestReader) Close() error {
	return mr.file.Close()
}

// RecoverFromManifest recovers the version state from manifest files.
func RecoverFromManifest(dir string, vs *VersionSet) error {
	// First try to read CURRENT file to find active manifest
	var manifestPath string
	var manifestNum uint64

	if currentNum, err := readCURRENT(dir); err == nil {
		// CURRENT file exists, use it
		manifestNum = currentNum
		manifestPath = filepath.Join(dir, fmt.Sprintf("%06d.manifest", currentNum))

		// Verify the manifest file exists
		if _, err := os.Stat(manifestPath); err != nil {
			// CURRENT points to non-existent manifest, fall back to scanning
			manifestPath = ""
		}
	}

	// Fall back to scanning if CURRENT file missing or invalid
	if manifestPath == "" {
		manifestFiles, err := filepath.Glob(filepath.Join(dir, "*"+ManifestExtension))
		if err != nil {
			return err
		}

		if len(manifestFiles) == 0 {
			// No manifest files, start fresh
			return nil
		}

		// Find the latest manifest file (highest number)
		var latestNum uint64
		for _, manifestFile := range manifestFiles {
			base := filepath.Base(manifestFile)
			var num uint64
			if _, err := fmt.Sscanf(base, "%06d.manifest", &num); err != nil {
				continue
			}
			if num > latestNum {
				latestNum = num
				manifestPath = manifestFile
				manifestNum = num
			}
		}

		if manifestPath == "" {
			return nil
		}
	}

	// Read the manifest file
	reader, err := NewManifestReader(manifestPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Start with empty version
	version := NewVersion(vs.numLevels)

	// Apply all version edits
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if record.Type == ManifestRecordVersionEdit {
			edit, err := reader.ReadVersionEdit(record.Data)
			if err != nil {
				return err
			}

			edit.Apply(version)
		}
	}

	// Register all files from the recovered version
	version.registerVersionFiles(vs.dir)

	// Install the recovered version
	vs.mu.Lock()
	if vs.current != nil {
		vs.current.MarkForCleanup()
	}
	vs.current = version
	vs.versions = append(vs.versions, version)

	// CRITICAL FIX: Update nextFileNum to be higher than any recovered file
	maxFileNum := uint64(0)
	for level := 0; level < vs.numLevels; level++ {
		for _, file := range version.GetFiles(level) {
			if file.FileNum > maxFileNum {
				maxFileNum = file.FileNum
			}
		}
	}
	if maxFileNum > 0 {
		vs.nextFileNum = maxFileNum + 1
	}

	// Update manifest version number to be higher than recovered manifest
	if manifestNum >= vs.nextVersionNum {
		vs.nextVersionNum = manifestNum + 1
	}

	// CRITICAL FIX: Reopen the recovered manifest for writing instead of creating new one
	writer, err := NewManifestWriter(vs.dir, manifestNum, vs.maxManifestFileSize)
	if err != nil {
		vs.mu.Unlock()
		return fmt.Errorf("failed to reopen manifest for writing: %w", err)
	}
	vs.manifestWriter = writer

	vs.mu.Unlock()

	return nil
}

// CURRENT file management functions

// writeCURRENT writes the CURRENT file pointing to the specified manifest
func writeCURRENT(dir string, manifestNum uint64) error {
	currentPath := filepath.Join(dir, "CURRENT")
	tmpPath := currentPath + ".tmp"

	// Write to temporary file first
	manifestName := fmt.Sprintf("%06d.manifest", manifestNum)
	err := os.WriteFile(tmpPath, []byte(manifestName+"\n"), 0644)
	if err != nil {
		return fmt.Errorf("failed to write CURRENT temp file: %w", err)
	}

	// Atomic rename
	err = os.Rename(tmpPath, currentPath)
	if err != nil {
		os.Remove(tmpPath) // Clean up temp file
		return fmt.Errorf("failed to rename CURRENT file: %w", err)
	}

	return nil
}

// readCURRENT reads the CURRENT file and returns the current manifest number
func readCURRENT(dir string) (uint64, error) {
	currentPath := filepath.Join(dir, "CURRENT")

	data, err := os.ReadFile(currentPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read CURRENT file: %w", err)
	}

	// Parse manifest filename
	manifestName := strings.TrimSpace(string(data))
	if !strings.HasSuffix(manifestName, ".manifest") {
		return 0, fmt.Errorf("invalid manifest name in CURRENT: %s", manifestName)
	}

	// Extract number from filename like "000003.manifest"
	numStr := strings.TrimSuffix(manifestName, ".manifest")
	manifestNum, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse manifest number from %s: %w", manifestName, err)
	}

	return manifestNum, nil
}
