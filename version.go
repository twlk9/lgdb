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

// Version represents a specific version of the database state
// It tracks which SSTable files are part of this version
type Version struct {
	// Files at each level (L0, L1, L2, ...)
	files [][](*FileMetadata)

	// Memtables snapshot (active + immutable) - NEW
	memtables []*memtable.MemTable

	// Sequence number for this version snapshot - NEW
	seqNum uint64

	// Range deletes active in this version
	rangeDeletes []RangeTombstone

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
		files:        make([][](*FileMetadata), numLevels),
		memtables:    make([]*memtable.MemTable, 0), // Initialize empty slice
		rangeDeletes: make([]RangeTombstone, 0),
		resourceID:   resourceID,
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

// registerManifestFile registers a manifest file with the epoch manager
// This ensures the manifest file is kept alive until no versions reference it
func registerManifestFile(dir string, manifestNum uint64) {
	currentEpoch := epoch.EnterEpoch()
	defer epoch.ExitEpoch(currentEpoch)

	manifestResourceID := fmt.Sprintf("manifest_%d", manifestNum)
	if !epoch.ResourceExists(manifestResourceID) {
		manifestPath := filepath.Join(dir, fmt.Sprintf("%06d.manifest", manifestNum))
		epoch.RegisterResource(manifestResourceID, currentEpoch, func() error {
			// Manifest cleanup function: delete the actual file
			return os.Remove(manifestPath)
		})
	}
}

// markManifestForCleanup marks a manifest file for cleanup via the epoch system
func markManifestForCleanup(manifestNum uint64) {
	manifestResourceID := fmt.Sprintf("manifest_%d", manifestNum)
	epoch.MarkResourceForCleanup(manifestResourceID)
}

// registerRangeDeleteFile registers a range delete file with the epoch manager
// This ensures the range delete file is kept alive until no versions reference it
func registerRangeDeleteFile(dir string, rangeDeleteNum uint64) {
	currentEpoch := epoch.EnterEpoch()
	defer epoch.ExitEpoch(currentEpoch)

	rangeDeleteResourceID := fmt.Sprintf("rangedel_%d", rangeDeleteNum)
	if !epoch.ResourceExists(rangeDeleteResourceID) {
		rangeDeletePath := filepath.Join(dir, fmt.Sprintf("%06d.rangedel", rangeDeleteNum))
		epoch.RegisterResource(rangeDeleteResourceID, currentEpoch, func() error {
			// Range delete file cleanup function: delete the actual file
			return os.Remove(rangeDeletePath)
		})
	}
}

// markRangeDeleteFileForCleanup marks a range delete file for cleanup via the epoch system
func markRangeDeleteFileForCleanup(rangeDeleteNum uint64) {
	rangeDeleteResourceID := fmt.Sprintf("rangedel_%d", rangeDeleteNum)
	epoch.MarkResourceForCleanup(rangeDeleteResourceID)
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
	// Copy range deletes
	newVersion.rangeDeletes = make([]RangeTombstone, len(v.rangeDeletes))
	copy(newVersion.rangeDeletes, v.rangeDeletes)

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

// AddRangeDelete adds a range delete to this version
func (v *Version) AddRangeDelete(rt RangeTombstone) {
	v.rangeDeletes = append(v.rangeDeletes, rt)
}

// RemoveRangeDelete removes a range delete by ID
func (v *Version) RemoveRangeDelete(id uint64) {
	for i, rt := range v.rangeDeletes {
		if rt.ID == id {
			v.rangeDeletes = append(v.rangeDeletes[:i], v.rangeDeletes[i+1:]...)
			break
		}
	}
}

// GetRangeDeletes returns all active range deletes
func (v *Version) GetRangeDeletes() []RangeTombstone {
	return v.rangeDeletes
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

	// Range delete writer
	rangeDeleteWriter *RangeDeleteWriter

	// Maximum manifest file size before rotation
	maxManifestFileSize int64

	// Next range delete ID
	nextRangeDeleteID uint64

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
		nextRangeDeleteID:   1,
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

// NewRangeDeleteID returns a new unique range delete ID
func (vs *VersionSet) NewRangeDeleteID() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	id := vs.nextRangeDeleteID
	vs.nextRangeDeleteID++
	return id
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
	return vs.LogAndApplyWithRangeDeletes(edit, nil)
}

// LogAndApplyWithRangeDeletes applies both version and range delete edits
func (vs *VersionSet) LogAndApplyWithRangeDeletes(edit *VersionEdit, rangeDeleteEdit *RangeDeleteEdit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Create new version based on current version
	epoch.AdvanceEpoch()
	newVersion := vs.current.Clone()
	newVersion.number = vs.nextVersionNum
	currentVersionNum := vs.nextVersionNum
	vs.nextVersionNum++

	edit.Apply(newVersion)

	// Apply range delete edits if present
	if rangeDeleteEdit != nil {
		for _, rt := range rangeDeleteEdit.addedDeletes {
			newVersion.AddRangeDelete(rt)
		}
		for _, id := range rangeDeleteEdit.removedDeletes {
			newVersion.RemoveRangeDelete(id)
		}
	}

	// Initialize manifest writer if needed
	if vs.manifestWriter == nil {
		writer, err := NewManifestWriter(vs.dir, currentVersionNum, vs.maxManifestFileSize)
		if err != nil {
			return err
		}
		vs.manifestWriter = writer
		// Register the initial manifest file with epoch system
		registerManifestFile(vs.dir, vs.nextVersionNum)
	}

	// Initialize range delete writer if needed (paired with manifest)
	if vs.rangeDeleteWriter == nil {
		writer, err := NewRangeDeleteWriter(vs.dir, vs.manifestWriter.GetFileNum())
		if err != nil {
			return err
		}
		vs.rangeDeleteWriter = writer
		// Register the initial range delete file with epoch system
		registerRangeDeleteFile(vs.dir, vs.manifestWriter.GetFileNum())
	}

	// Write to manifest file
	if err := vs.manifestWriter.WriteVersionEdit(edit); err != nil {
		return err
	}

	// Write range delete edit if present
	if rangeDeleteEdit != nil && !rangeDeleteEdit.IsEmpty() {
		if err := vs.rangeDeleteWriter.WriteEdit(rangeDeleteEdit); err != nil {
			return err
		}
	}

	// Sync to disk for durability
	if err := vs.manifestWriter.Sync(); err != nil {
		return err
	}
	if vs.rangeDeleteWriter != nil {
		if err := vs.rangeDeleteWriter.Sync(); err != nil {
			return err
		}
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
	// Save old manifest/rangedel numbers before closing
	oldManifestNum := vs.manifestWriter.GetFileNum()

	// Close current manifest writer
	if err := vs.manifestWriter.Close(); err != nil {
		return fmt.Errorf("failed to close current manifest: %w", err)
	}

	// Close current range delete writer if exists
	if vs.rangeDeleteWriter != nil {
		if err := vs.rangeDeleteWriter.Close(); err != nil {
			return fmt.Errorf("failed to close current range delete writer: %w", err)
		}
	}

	// Mark old manifest and range delete files for cleanup via epoch system
	markManifestForCleanup(oldManifestNum)
	markRangeDeleteFileForCleanup(oldManifestNum)
	vs.logger.Debug("marked old manifest and rangedel for cleanup", "file_num", oldManifestNum)

	// Create new manifest file with incremented version number
	vs.nextVersionNum++
	newManifestNum := vs.nextVersionNum
	newWriter, err := NewManifestWriter(vs.dir, newManifestNum, vs.maxManifestFileSize)
	if err != nil {
		return fmt.Errorf("failed to create new manifest: %w", err)
	}

	// Create new range delete writer (paired with manifest)
	newRangeDeleteWriter, err := NewRangeDeleteWriter(vs.dir, newManifestNum)
	if err != nil {
		newWriter.Close()
		return fmt.Errorf("failed to create new range delete writer: %w", err)
	}

	// Register new manifest and range delete files with epoch system
	registerManifestFile(vs.dir, newManifestNum)
	registerRangeDeleteFile(vs.dir, newManifestNum)

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
		newRangeDeleteWriter.Close()
		return fmt.Errorf("failed to write manifest snapshot: %w", err)
	}

	// Write range delete snapshot
	if len(currentVersion.rangeDeletes) > 0 {
		rangeDeleteSnapshot := NewRangeDeleteEdit()
		for _, rt := range currentVersion.rangeDeletes {
			rangeDeleteSnapshot.AddRangeDelete(rt)
		}
		if err := newRangeDeleteWriter.WriteEdit(rangeDeleteSnapshot); err != nil {
			newWriter.Close()
			newRangeDeleteWriter.Close()
			return fmt.Errorf("failed to write range delete snapshot: %w", err)
		}
	}

	// Sync new manifest and range delete file
	if err := newWriter.Sync(); err != nil {
		newWriter.Close()
		newRangeDeleteWriter.Close()
		return fmt.Errorf("failed to sync new manifest: %w", err)
	}
	if err := newRangeDeleteWriter.Sync(); err != nil {
		newWriter.Close()
		newRangeDeleteWriter.Close()
		return fmt.Errorf("failed to sync new range delete file: %w", err)
	}

	// Update writers
	vs.manifestWriter = newWriter
	vs.rangeDeleteWriter = newRangeDeleteWriter

	vs.logger.Info("manifest rotation completed", "old_file_num", oldManifestNum, "new_file_num", newManifestNum)
	return nil
}

// Close closes the version set and its writers
func (vs *VersionSet) Close() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	var firstErr error

	if vs.manifestWriter != nil {
		if err := vs.manifestWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		vs.manifestWriter = nil
	}

	if vs.rangeDeleteWriter != nil {
		if err := vs.rangeDeleteWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		vs.rangeDeleteWriter = nil
	}

	return firstErr
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

	// Copy range deletes from current version
	newVersion.rangeDeletes = make([]RangeTombstone, len(vs.current.rangeDeletes))
	copy(newVersion.rangeDeletes, vs.current.rangeDeletes)

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
}

// NewVersionEdit creates a new version edit
func NewVersionEdit() *VersionEdit {
	return &VersionEdit{
		addFiles:    make(map[int][]*FileMetadata),
		removeFiles: make(map[int][]uint64),
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
