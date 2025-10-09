package lgdb

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/twlk9/lgdb/epoch"
	"github.com/twlk9/lgdb/keys"
	"github.com/twlk9/lgdb/memtable"
)

// FileMetadata contains metadata about an SSTable file
type FileMetadata struct {
	FileNum     uint64
	Size        uint64
	SmallestKey keys.EncodedKey
	LargestKey  keys.EncodedKey
	NumEntries  uint64 // Number of entries in this SSTable
}

// RangeTombstoneFileMetadata contains metadata about a range tombstone SSTable file
type RangeTombstoneFileMetadata struct {
	FileNum        uint64
	Level          int // 0 for L0, 1 for L1
	Size           uint64
	SmallestKey    keys.UserKey // Smallest range start key
	LargestKey     keys.UserKey // Largest range end key
	TombstoneCount int          // Number of tombstones in file
}

// Version represents a specific version of the database state
// It tracks which SSTable files are part of this version
type Version struct {
	// Files at each level (L0, L1, L2, ...)
	files [][](*FileMetadata)

	// Memtables snapshot (active + immutable) - NEW
	memtables []*memtable.MemTable

	// Sequence number for this version snapshot - NEW
	seqNum uint64

	// Version number
	number uint64

	// Epoch-based resource management
	resourceID string
	epochNum   uint64 // Epoch held by this version while it's current

	// Atomic flag to ensure cleanup happens only once
	cleanedUp int32
}

// NewVersion creates a new version with epoch-based resource management
func NewVersion(numLevels int) *Version {
	// Generate unique resource ID for this version
	resourceID := fmt.Sprintf("version_%p", &Version{})

	version := &Version{
		files:      make([][](*FileMetadata), numLevels),
		memtables:  make([]*memtable.MemTable, 0), // Initialize empty slice
		resourceID: resourceID,
	}

	// Enter epoch and hold it for the lifetime of this version as current
	// This prevents cleanup while version is current and being accessed
	version.epochNum = epoch.EnterEpoch()

	// Register cleanup function for when version becomes non-current
	epoch.RegisterResource(resourceID, version.epochNum, func() error {
		// Cleanup function: release memtable references
		// NOTE: cleanedUp flag is now managed by MarkForCleanup() to prevent double ExitEpoch()
		// Clear memtable references to allow garbage collection
		version.memtables = nil
		return nil
	})

	return version
}

// registerVersionFiles registers all files referenced by this version with the epoch manager
// This ensures files are kept alive as long as this version exists
func (v *Version) registerVersionFiles(dir string) {
	currentEpoch := epoch.EnterEpoch()
	defer epoch.ExitEpoch(currentEpoch) // Get current epoch for registration

	// Register all SSTable files
	for level := 0; level < len(v.files); level++ {
		for _, file := range v.files[level] {
			fileResourceID := fmt.Sprintf("file_%d", file.FileNum)
			if !epoch.ResourceExists(fileResourceID) {
				filePath := filepath.Join(dir, fmt.Sprintf("%06d.sst", file.FileNum))
				epoch.RegisterResource(fileResourceID, currentEpoch, func() error {
					// File cleanup function: delete the actual file
					return os.Remove(filePath)
				})
			}
		}
	}
}

// MarkForCleanup marks this version for cleanup via epoch manager
// Called when this version is no longer the current version
func (v *Version) MarkForCleanup() {
	if v.resourceID != "" {
		// Use cleanedUp as a flag to ensure MarkForCleanup is called only once
		// This prevents double ExitEpoch() calls that cause reader count underflow
		if atomic.CompareAndSwapInt32(&v.cleanedUp, 0, 1) {
			// Exit the epoch this version was holding, making it eligible for cleanup
			epoch.ExitEpoch(v.epochNum)

			// Mark the resource for cleanup
			epoch.MarkResourceForCleanup(v.resourceID)
		}
	}
}

// Clone creates a copy of this version
func (v *Version) Clone() *Version {
	newVersion := NewVersion(len(v.files))
	newVersion.number = v.number + 1
	// Copy file lists
	for level := range v.files {
		newVersion.files[level] = make([]*FileMetadata, len(v.files[level]))
		copy(newVersion.files[level], v.files[level])
	}

	return newVersion
}

// RemoveFile removes a file from the specified level
func (v *Version) RemoveFile(level int, fileNum uint64) {
	if level >= len(v.files) {
		return
	}

	for i, file := range v.files[level] {
		if file.FileNum == fileNum {
			// Mark the file for cleanup in the epoch system
			fileResourceID := fmt.Sprintf("file_%d", file.FileNum)
			epoch.MarkResourceForCleanup(fileResourceID)

			// Remove from slice
			v.files[level] = append(v.files[level][:i], v.files[level][i+1:]...)
			break
		}
	}
}

// VersionSet manages the sequence of versions
type VersionSet struct {
	mu sync.RWMutex

	// Current version
	current *Version

	// All versions (replaces linked list)
	versions []*Version

	// Next file number to assign
	nextFileNum uint64

	// Next version number
	nextVersionNum uint64

	// Number of levels
	numLevels int

	// Directory for database files
	dir string

	// Manifest writer
	manifestWriter *ManifestWriter

	// Maximum manifest file size before rotation
	maxManifestFileSize int64

	// Logger for version operations
	logger *slog.Logger
}

// NewVersionSet creates a new version set
func NewVersionSet(numLevels int, dir string, maxManifestFileSize int64, logger *slog.Logger) *VersionSet {
	initial := NewVersion(numLevels)

	vs := &VersionSet{
		current:             initial,
		versions:            []*Version{initial},
		nextFileNum:         1,
		nextVersionNum:      1,
		numLevels:           numLevels,
		dir:                 dir,
		maxManifestFileSize: maxManifestFileSize,
		logger:              logger,
	}

	return vs
}

// NewFileNumber returns a new unique file number
func (vs *VersionSet) NewFileNumber() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	fileNum := vs.nextFileNum
	vs.nextFileNum++
	return fileNum
}

// CurrFileNumber returns the current file number. Usually used for
// debugging and such
func (vs *VersionSet) CurrFileNumber() uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.nextFileNum
}

// LogAndApply applies a version edit and makes it the current version
func (vs *VersionSet) LogAndApply(edit *VersionEdit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Create new version based on current version
	epoch.AdvanceEpoch()
	newVersion := vs.current.Clone()
	newVersion.number = vs.nextVersionNum
	vs.nextVersionNum++

	edit.Apply(newVersion)

	// Initialize manifest writer if needed
	if vs.manifestWriter == nil {
		writer, err := NewManifestWriter(vs.dir, vs.nextVersionNum, vs.maxManifestFileSize)
		if err != nil {
			return err
		}
		vs.manifestWriter = writer
	}

	// Write to manifest file
	if err := vs.manifestWriter.WriteVersionEdit(edit); err != nil {
		return err
	}

	// Sync to disk for durability
	if err := vs.manifestWriter.Sync(); err != nil {
		return err
	}

	// Check if manifest needs rotation
	if vs.manifestWriter.NeedsRotation() {
		vs.logger.Debug("manifest rotation triggered", "size", vs.manifestWriter.GetSize(), "maxSize", vs.maxManifestFileSize)
		if err := vs.rotateManifest(newVersion); err != nil {
			vs.logger.Error("failed to rotate manifest", "error", err)
			// Continue anyway - the operation succeeded, rotation is optimization
		}
	}

	// Register all files referenced by the new version before making it current
	newVersion.registerVersionFiles(vs.dir)

	// Make it current - use epoch-based cleanup
	vs.current.MarkForCleanup()
	vs.current = newVersion
	vs.versions = append(vs.versions, newVersion)

	return nil
}

// rotateManifest creates a new manifest file with a complete snapshot of the current state
func (vs *VersionSet) rotateManifest(currentVersion *Version) error {
	// Close current manifest writer
	if err := vs.manifestWriter.Close(); err != nil {
		return fmt.Errorf("failed to close current manifest: %w", err)
	}

	// Create new manifest file with incremented version number
	vs.nextVersionNum++
	newWriter, err := NewManifestWriter(vs.dir, vs.nextVersionNum, vs.maxManifestFileSize)
	if err != nil {
		return fmt.Errorf("failed to create new manifest: %w", err)
	}

	// Write complete snapshot of current version to new manifest
	snapshotEdit := &VersionEdit{
		addFiles: make(map[int][]*FileMetadata),
	}

	// Add all files from current version
	for level := 0; level < len(currentVersion.files); level++ {
		if len(currentVersion.files[level]) > 0 {
			snapshotEdit.addFiles[level] = make([]*FileMetadata, len(currentVersion.files[level]))
			copy(snapshotEdit.addFiles[level], currentVersion.files[level])
		}
	}

	// Write the snapshot
	if err := newWriter.WriteVersionEdit(snapshotEdit); err != nil {
		newWriter.Close()
		return fmt.Errorf("failed to write manifest snapshot: %w", err)
	}

	// Sync new manifest
	if err := newWriter.Sync(); err != nil {
		newWriter.Close()
		return fmt.Errorf("failed to sync new manifest: %w", err)
	}

	// Update manifest writer
	vs.manifestWriter = newWriter

	vs.logger.Info("manifest rotation completed", "new_file_num", vs.nextVersionNum)
	return nil
}

// Close closes the version set and its manifest writer
func (vs *VersionSet) Close() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.manifestWriter != nil {
		if err := vs.manifestWriter.Close(); err != nil {
			return err
		}
		vs.manifestWriter = nil
	}

	return nil
}

// GetCurrentVersion safely returns the current version with epoch
// protection This ensures the version won't be cleaned up while the
// caller is using it
func (vs *VersionSet) GetCurrentVersion() *Version {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.current
}

// CreateVersionSnapshot atomically captures the current version with memtables for consistent reads
// This creates a new version that includes both SSTables and memtables for MVCC
func (vs *VersionSet) CreateVersionSnapshot(memtables []*memtable.MemTable, seqNum uint64) *Version {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Advance epoch to establish new generation and allow cleanup of previous epochs
	epoch.AdvanceEpoch()

	// Create a new version using epoch-based constructor
	newVersion := NewVersion(len(vs.current.files))
	newVersion.memtables = memtables
	newVersion.seqNum = seqNum
	newVersion.number = vs.nextVersionNum // Unique version number
	vs.nextVersionNum++

	// Copy SSTables from current version
	for level := 0; level < len(vs.current.files); level++ {
		newVersion.files[level] = make([]*FileMetadata, len(vs.current.files[level]))
		copy(newVersion.files[level], vs.current.files[level])
	}

	// Register all files referenced by this snapshot
	newVersion.registerVersionFiles(vs.dir)

	// Add this version to the version set's slice so it's visible to cleanup
	vs.versions = append(vs.versions, newVersion)

	return newVersion
}

// GetCurrentVersionNumber safely returns just the current version number for logging
func (vs *VersionSet) GetCurrentVersionNumber() uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	return vs.current.number
}

// GetFiles returns files at the specified level
func (v *Version) GetFiles(level int) []*FileMetadata {
	if level >= len(v.files) {
		return nil
	}
	return v.files[level]
}

// AddFile adds a file to the specified level
func (v *Version) AddFile(level int, file *FileMetadata) {
	if level >= len(v.files) {
		return
	}

	v.files[level] = append(v.files[level], file)
}

// VersionEdit represents a set of changes to apply to a version
type VersionEdit struct {
	// Files to add at each level
	addFiles map[int][]*FileMetadata

	// Files to remove at each level (by file number)
	removeFiles map[int][]uint64

	// Range tombstone files to add at each level
	addRangeTombstoneFiles map[int][]*RangeTombstoneFileMetadata

	// Range tombstone files to remove at each level (by file number)
	removeRangeTombstoneFiles map[int][]uint64
}

// NewVersionEdit creates a new version edit
func NewVersionEdit() *VersionEdit {
	return &VersionEdit{
		addFiles:                  make(map[int][]*FileMetadata),
		removeFiles:               make(map[int][]uint64),
		addRangeTombstoneFiles:    make(map[int][]*RangeTombstoneFileMetadata),
		removeRangeTombstoneFiles: make(map[int][]uint64),
	}
}

// AddFile marks a file to be added at the specified level
func (ve *VersionEdit) AddFile(level int, file *FileMetadata) {
	ve.addFiles[level] = append(ve.addFiles[level], file)
}

// RemoveFile marks a file to be removed from the specified level
func (ve *VersionEdit) RemoveFile(level int, fileNum uint64) {
	ve.removeFiles[level] = append(ve.removeFiles[level], fileNum)
}

// AddRangeTombstoneFile marks a range tombstone file to be added at the specified level
func (ve *VersionEdit) AddRangeTombstoneFile(level int, file *RangeTombstoneFileMetadata) {
	ve.addRangeTombstoneFiles[level] = append(ve.addRangeTombstoneFiles[level], file)
}

// RemoveRangeTombstoneFile marks a range tombstone file to be removed from the specified level
func (ve *VersionEdit) RemoveRangeTombstoneFile(level int, fileNum uint64) {
	ve.removeRangeTombstoneFiles[level] = append(ve.removeRangeTombstoneFiles[level], fileNum)
}

// Apply applies this edit to a version
func (ve *VersionEdit) Apply(version *Version) {
	// Remove files first
	for level, fileNums := range ve.removeFiles {
		for _, fileNum := range fileNums {
			version.RemoveFile(level, fileNum)
		}
	}

	// Add new files
	for level, files := range ve.addFiles {
		for _, file := range files {
			version.AddFile(level, file)
		}
	}
}

// cleanupOldVersions removes versions that have been marked ready for removal by epoch cleanup.
// This prevents memory leaks by removing versions from the versions slice once they're no longer needed.
func (vs *VersionSet) cleanupOldVersions() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Filter out versions marked for removal
	newVersions := make([]*Version, 0, len(vs.versions))
	removedCount := 0

	for _, version := range vs.versions {
		if atomic.LoadInt32(&version.cleanedUp) == 1 {
			removedCount++
			continue // Skip this version - don't add to new slice
		}
		newVersions = append(newVersions, version)
	}

	vs.versions = newVersions

	if removedCount > 0 {
		vs.logger.Debug("cleaned up old versions", "removed", removedCount, "remaining", len(vs.versions))
	}
}
