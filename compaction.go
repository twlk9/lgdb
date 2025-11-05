package lgdb

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/twlk9/lgdb/keys"
	"github.com/twlk9/lgdb/sstable"
)

// CompactionStats tracks compaction statistics
type CompactionStats struct {
	BytesRead    uint64
	BytesWritten uint64
	Duration     time.Duration
	FilesRead    int
	FilesWritten int
}

// CompactionManager manages the compaction process independently
type CompactionManager struct {
	// Dependencies for doing compaction work
	versions  *VersionSet
	fileCache *FileCache
	path      string
	options   *Options
	logger    *slog.Logger

	// Coordination channels
	wakeupChan chan struct{} // DB signals work needed
	doneChan   chan error    // CompactionManager signals completion
	closeChan  chan struct{} // For shutdown

	// Backpressure signaling
	flushBP *sync.Cond // Signal when L0 count decreases (for L0 backpressure)

	// State
	closed bool
	mu     sync.Mutex
	wg     sync.WaitGroup // Wait for worker goroutine to finish

	// Level size thresholds for triggering compaction
	levelMaxBytes []int64
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(versions *VersionSet, fileCache *FileCache, path string, options *Options, logger *slog.Logger, flushBP *sync.Cond) *CompactionManager {
	cm := &CompactionManager{
		versions:      versions,
		fileCache:     fileCache,
		path:          path,
		options:       options,
		logger:        logger,
		wakeupChan:    make(chan struct{}, 1), // Buffered to avoid blocking
		doneChan:      make(chan error, 1),    // Buffered for non-blocking completion signal
		closeChan:     make(chan struct{}),
		flushBP:       flushBP,
		closed:        false,
		levelMaxBytes: make([]int64, options.MaxLevels),
	}

	// Set level size thresholds using the options method
	for i := 0; i < len(cm.levelMaxBytes); i++ {
		cm.levelMaxBytes[i] = options.GetLevelMaxBytes(i)
	}

	// Start the compaction worker goroutine
	cm.wg.Add(1)
	go cm.compactionWorker()

	return cm
}

// ScheduleCompaction signals the compaction worker to check for work
func (cm *CompactionManager) ScheduleCompaction() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed {
		return
	}

	// Send non-blocking signal to wake up the compaction worker
	select {
	case cm.wakeupChan <- struct{}{}:
		// Signal sent
	default:
		// Worker already has a pending signal, no need to queue another
	}
}

// compactionWorker runs in a single goroutine and performs compaction work
func (cm *CompactionManager) compactionWorker() {
	defer cm.wg.Done()
	cm.logger.Info("Compaction worker started")

	for {
		select {
		case <-cm.closeChan:
			cm.logger.Info("Compaction worker shutting down")
			return

		case <-cm.wakeupChan:
			cm.logger.Info("Compaction work requested - starting compaction")

			// Grab a fresh current version when we're ready to do work
			version := cm.versions.GetCurrentVersion()
			// Pick compaction based on fresh version
			compaction := cm.pickCompaction(version)
			if compaction == nil {
				cm.logger.Debug("No compaction needed")
				// Signal completion even when no work is needed
				select {
				case cm.doneChan <- nil:
				default:
				}
				version.MarkForCleanup() // Mark version for cleanup when no work needed
				continue
			}

			// Do the actual compaction work
			err := cm.doCompactionWork(compaction, version)

			// Signal completion (non-blocking)
			select {
			case cm.doneChan <- err:
			default:
				// If nobody is listening, that's fine
			}
			version.MarkForCleanup()
		}
	}
}

// doCompactionWork performs the complete compaction workflow:
// 1. Do the compaction
// 2. Apply version edit
// 3. Clean up obsolete files
func (cm *CompactionManager) doCompactionWork(compaction *Compaction, version *Version) error {
	cm.logger.Info("Starting compaction",
		"level", compaction.level,
		"outputLevel", compaction.outputLevel,
		"inputFiles", len(compaction.inputFiles[0])+len(compaction.inputFiles[1]))

	startTime := time.Now()

	// Run the actual compaction
	edit, err := cm.doCompaction(compaction, version)
	if err != nil {
		compaction.Cleanup()
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Apply the version edit under proper locking
	err = cm.versions.LogAndApply(edit)
	if err != nil {
		compaction.Cleanup()
		return fmt.Errorf("failed to apply compaction version edit: %w", err)
	}

	// Check for obsolete range deletes and remove them
	cm.cleanupObsoleteRangeDeletes()

	duration := time.Since(startTime)
	cm.logger.Info("Compaction completed",
		"level", compaction.level,
		"outputLevel", compaction.outputLevel,
		"duration", duration,
		"bytesRead", 0, // TODO: track bytes
		"bytesWritten", 0) // TODO: track bytes

	// Clean up compaction
	compaction.Cleanup()

	// If this was an L0 compaction, signal any writes waiting on L0 backpressure
	if compaction.level == 0 && cm.flushBP != nil {
		v := cm.versions.GetCurrentVersion()
		if len(v.GetFiles(0)) < cm.options.L0StopWritesTrigger {
			cm.flushBP.Broadcast()
		}
	}

	return nil
}

// pickCompaction selects the best compaction to run using Pebble-style logic.
func (cm *CompactionManager) pickCompaction(version *Version) *Compaction {

	// Check if L0 needs compaction (most urgent)
	if compaction := cm.pickL0Compaction(version); compaction != nil {
		// Compaction takes ownership of the version reference
		return compaction
	}

	// Check if any other level needs compaction
	if compaction := cm.pickLevelCompaction(version); compaction != nil {
		// Compaction takes ownership of the version reference
		return compaction
	}

	// No compaction needed
	return nil
}

// pickL0Compaction picks an L0->L1 compaction following Pebble's approach.
func (cm *CompactionManager) pickL0Compaction(version *Version) *Compaction {
	l0Files := version.GetFiles(0)
	if len(l0Files) < cm.options.L0CompactionTrigger {
		return nil // Not enough L0 files to trigger compaction
	}

	// Select ALL L0 files for compaction - this is more aggressive but simpler
	// and ensures L0 doesn't grow indefinitely. In LevelDB/RocksDB, they also
	// compact all L0 files when triggered to avoid repeated small compactions.
	selectedL0Files := l0Files

	// Find overlapping L1 files
	l1Files := cm.findOverlappingFiles(version, 1, selectedL0Files)

	// Create compaction
	compaction := &Compaction{
		level:             0,
		outputLevel:       1,
		version:           version,
		inputFiles:        make([][]*FileMetadata, 2),
		outputFiles:       make([]*FileMetadata, 0),
		stats:             &CompactionStats{},
		maxOutputFileSize: cm.options.TargetFileSize(1),
	}

	// Set input files
	compaction.inputFiles[0] = append([]*FileMetadata{}, selectedL0Files...)
	compaction.inputFiles[1] = append([]*FileMetadata{}, l1Files...)

	// Note: Files are already referenced by being in the version
	// No need to add additional references here

	cm.logger.Info("PICKED_L0_COMPACTION",
		"l0Files", cm.getFileNums(selectedL0Files),
		"l1Files", cm.getFileNums(l1Files),
		"maxOutputFileSize", compaction.maxOutputFileSize)

	return compaction
}

// pickLevelCompaction picks a regular level compaction (L1->L2, L2->L3, etc.).
func (cm *CompactionManager) pickLevelCompaction(version *Version) *Compaction {
	// Find the level with the highest score
	bestLevel := -1
	bestScore := 0.0

	for level := 1; level < cm.options.MaxLevels-1; level++ { // Skip L0 and bottom level
		score := cm.calculateLevelScore(level, version)
		if score >= 1.0 && score > bestScore {
			bestScore = score
			bestLevel = level
		}
	}

	if bestLevel == -1 {
		return nil // No level needs compaction
	}

	// Pick files from the level, using overlap-aware expansion
	levelFiles := version.GetFiles(bestLevel)
	if len(levelFiles) == 0 {
		return nil
	}

	selectedFiles := cm.selectFiles(version, levelFiles, bestLevel)

	// Find overlapping files in next level
	overlappingFiles := cm.findOverlappingFiles(version, bestLevel+1, selectedFiles)

	// Create compaction
	compaction := &Compaction{
		level:             bestLevel,
		outputLevel:       bestLevel + 1,
		version:           version,
		inputFiles:        make([][]*FileMetadata, 2),
		outputFiles:       make([]*FileMetadata, 0),
		stats:             &CompactionStats{},
		maxOutputFileSize: cm.options.TargetFileSize(bestLevel + 1),
	}

	// Set input files
	compaction.inputFiles[0] = selectedFiles
	compaction.inputFiles[1] = append([]*FileMetadata{}, overlappingFiles...)

	return compaction
}

// findOverlappingFiles finds files in the target level that overlap with the given input files.
func (cm *CompactionManager) findOverlappingFiles(version *Version, targetLevel int, inputFiles []*FileMetadata) []*FileMetadata {
	if targetLevel >= len(version.files) {
		return nil
	}

	// Guard against empty input files
	if len(inputFiles) == 0 {
		return nil
	}

	targetFiles := version.GetFiles(targetLevel)
	if len(targetFiles) == 0 {
		return nil
	}

	// Find the key range of input files
	var smallestKey, largestKey keys.EncodedKey
	for _, file := range inputFiles {
		if smallestKey == nil || file.SmallestKey.Compare(smallestKey) < 0 {
			smallestKey = file.SmallestKey
		}
		if largestKey == nil || file.LargestKey.Compare(largestKey) > 0 {
			largestKey = file.LargestKey
		}
	}

	// Guard against nil keys (defensive check)
	if smallestKey == nil || largestKey == nil {
		return nil
	}

	// Find overlapping files in target level
	var overlapping []*FileMetadata
	for _, targetFile := range targetFiles {
		if cm.keyRangesOverlap(smallestKey, largestKey, targetFile.SmallestKey, targetFile.LargestKey) {
			overlapping = append(overlapping, targetFile)
		}
	}

	return overlapping
}

// keyRangesOverlap checks if two key ranges overlap.
func (cm *CompactionManager) keyRangesOverlap(start1, end1, start2, end2 keys.EncodedKey) bool {
	// Range1: [start1, end1], Range2: [start2, end2]
	// They overlap if: start1 <= end2 && start2 <= end1

	// Defensive nil checks - should not happen with proper callers but guards against race conditions
	if start1 == nil || end1 == nil || start2 == nil || end2 == nil {
		return false
	}

	return start1.Compare(end2) <= 0 && start2.Compare(end1) <= 0
}

// getFileNums extracts file numbers for logging.
func (cm *CompactionManager) getFileNums(files []*FileMetadata) []uint64 {
	var nums []uint64
	for _, f := range files {
		nums = append(nums, f.FileNum)
	}
	return nums
}

// calculateLevelScore calculates the compaction score for a level.
func (cm *CompactionManager) calculateLevelScore(level int, version *Version) float64 {
	files := version.GetFiles(level)
	if len(files) == 0 {
		return 0.0
	}

	if level == 0 {
		// L0 is scored by file count
		return float64(len(files)) / float64(cm.options.L0CompactionTrigger)
	}

	// L1+ is scored by total size
	totalSize := int64(0)
	for _, file := range files {
		totalSize += int64(file.Size)
	}

	return float64(totalSize) / float64(cm.levelMaxBytes[level])
}

// selectFiles selects files for compaction at a specific level, using
// overlap-aware expansion to reduce write amplification.
func (cm *CompactionManager) selectFiles(version *Version, files []*FileMetadata, level int) []*FileMetadata {
	if len(files) == 0 {
		return nil
	}

	// Don't compact a level with 1 file
	if len(files) == 1 {
		return nil
	}

	// Sort files by file number to get oldest first
	sorted := make([]*FileMetadata, len(files))
	copy(sorted, files)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].FileNum < sorted[j].FileNum
	})

	// Calculate target input size based on the output level's target file size
	outputLevel := level + 1
	targetOutputFileSize := cm.options.TargetFileSize(outputLevel)
	targetInputSize := int64(float64(targetOutputFileSize) * 1.2) // 20% overhead

	// Don't go below a reasonable minimum
	minInputSize := int64(4096)
	if targetInputSize < minInputSize {
		targetInputSize = minInputSize
	}

	// Select initial set of files targeting the input size
	selected := []*FileMetadata{}
	totalSize := int64(0)

	for i, file := range sorted {
		selected = append(selected, file)
		totalSize += int64(file.Size)

		// Stop when we have enough data or too many files
		if totalSize >= targetInputSize || len(selected) >= 8 {
			break
		}

		// Always select at least 2 files if available
		if i >= 1 && totalSize >= targetInputSize/2 {
			break
		}
	}

	// Always have at least 2 files
	if len(selected) < 2 && len(sorted) >= 2 {
		selected = sorted[:2]
	}

	// Now apply overlap-aware expansion: if overlaps are too high, select more files
	if cm.options.CompactionOverlapThreshold > 0 {
		selected = cm.expandSelection(version, selected, sorted, level)
	}

	cm.logger.Info("SELECTED_COMPACTION_FILES",
		"level", level,
		"inputFiles", len(selected),
		"totalInputSize", totalSize,
		"targetInputSize", targetInputSize,
		"targetOutputFileSize", targetOutputFileSize)

	return selected
}

// expandSelection checks if selected files have excessive overlaps in
// the next level. If so, it expands selection to include more files
// from the current level, spreading the overlap cost and reducing
// write amplification.
func (cm *CompactionManager) expandSelection(version *Version, selected, allSorted []*FileMetadata, level int) []*FileMetadata {
	// Find overlapping files in next level
	overlapping := cm.findOverlappingFiles(version, level+1, selected)
	if len(overlapping) == 0 {
		return selected
	}

	// Calculate overlap ratio
	overlapSize := int64(0)
	for _, f := range overlapping {
		overlapSize += int64(f.Size)
	}

	selectedSize := int64(0)
	for _, f := range selected {
		selectedSize += int64(f.Size)
	}

	if selectedSize == 0 {
		return selected
	}

	overlapRatio := float64(overlapSize) / float64(selectedSize)

	cm.logger.Debug("Overlap check",
		"level", level,
		"selectedFiles", len(selected),
		"selectedSize", selectedSize,
		"overlappingFiles", len(overlapping),
		"overlapSize", overlapSize,
		"overlapRatio", overlapRatio,
		"threshold", cm.options.CompactionOverlapThreshold)

	// If overlap ratio exceeds threshold, try to expand selection
	if overlapRatio > cm.options.CompactionOverlapThreshold {
		cm.logger.Info("High overlap detected - expanding selection",
			"level", level,
			"overlapRatio", overlapRatio,
			"threshold", cm.options.CompactionOverlapThreshold)

		// Expand files until we reach target overlap ratio or hit limits
		expanded := make([]*FileMetadata, len(selected))
		copy(expanded, selected)
		expandedSize := selectedSize

		// Try adding more files from the sorted list
		for _, file := range allSorted {
			// Skip if already in expanded set
			alreadyIncluded := false
			for _, e := range expanded {
				if e.FileNum == file.FileNum {
					alreadyIncluded = true
					break
				}
			}
			if alreadyIncluded {
				continue
			}

			// Add the file and recalculate
			expanded = append(expanded, file)
			expandedSize += int64(file.Size)

			// Recalculate overlaps with expanded selection
			newOverlapping := cm.findOverlappingFiles(version, level+1, expanded)
			newOverlapSize := int64(0)
			for _, f := range newOverlapping {
				newOverlapSize += int64(f.Size)
			}
			newRatio := float64(newOverlapSize) / float64(expandedSize)

			cm.logger.Debug("Expansion step",
				"level", level,
				"expandedFiles", len(expanded),
				"newRatio", newRatio,
				"targetRatio", cm.options.CompactionTargetOverlapRatio)

			// Stop if we've achieved target ratio or hit limits
			if newRatio <= cm.options.CompactionTargetOverlapRatio ||
				len(expanded) >= cm.options.CompactionMaxExpansionFiles {
				return expanded
			}
		}

		// Return what we have if we've exhausted the file list
		if len(expanded) > len(selected) {
			return expanded
		}
	}

	return selected
}

// doCompaction performs the actual compaction work.
func (cm *CompactionManager) doCompaction(compaction *Compaction, version *Version) (*VersionEdit, error) {
	// Create iterator for merging all input files
	iter, err := cm.createCompactionIterator(compaction, version)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// Create output files
	currentOutputFile := 0
	var currentWriter *sstable.SSTableWriter
	var currentFileSize int64

	outputFileNum := cm.versions.NewFileNumber()

	// Track the last processed user key to handle duplicates and tombstones
	var lastUserKey []byte
	var skipCurrentUserKey bool

	entryCount := 0
	currentFileEntryCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()

		// Skip invalid keys
		if k == nil {
			continue
		}

		entryCount++

		// Check if this is a new user key
		isNewUserKey := lastUserKey == nil || k.UserKey().Compare(lastUserKey) != 0

		if isNewUserKey {
			// New user key - reset state
			lastUserKey = make([]byte, len(k.UserKey()))
			copy(lastUserKey, k.UserKey())
			skipCurrentUserKey = false

			// If the most recent version is a delete, check if we can drop the tombstone
			if k.Kind() == keys.KindDelete {
				// Can drop tombstone if:
				// 1. We're at the bottom level (no lower levels exist), OR
				// 2. The key doesn't exist in any level beyond the output level
				canDropTombstone := compaction.outputLevel >= cm.options.MaxLevels-1 ||
					compaction.KeyNotExistsBeyondOutputLevel(k.UserKey())

				if canDropTombstone {
					// Tombstone can be safely dropped - skip this entire user key
					skipCurrentUserKey = true
					continue
				}

				// Tombstone must be preserved because key might exist in lower levels
				skipCurrentUserKey = true
				// Don't continue here - write the tombstone to output
			}
		} else {
			// Same user key - skip if we're already skipping this key or if it's an older version
			if skipCurrentUserKey {
				continue
			}
			// For same user key, we only want the most recent version (first encountered)
			// Since iterators return keys in sorted order, skip subsequent versions
			continue
		}

		// Check if we need to start a new output file
		if currentWriter == nil {
			var err error
			currentWriter, err = cm.createOutputFile(outputFileNum, compaction.outputLevel)
			if err != nil {
				return nil, err
			}
			currentFileSize = 0
			currentFileEntryCount = 0
			compaction.stats.FilesWritten++
		}

		// Write the encoded internal key and value (using pooled encoding)
		err := currentWriter.Add(k, v)
		if err != nil {
			return nil, err
		}

		currentFileEntryCount++

		// Update stats (still track raw key+value bytes for statistics)
		compaction.stats.BytesWritten += uint64(len(k) + len(v))

		// Check if current file is large enough to finish using actual estimated size
		currentFileSize = int64(currentWriter.EstimatedSize())
		if currentFileSize >= compaction.maxOutputFileSize {
			cm.logger.Info("CLOSING FILE",
				"currentFileSize", currentFileSize,
				"maxOutputFileSize", compaction.maxOutputFileSize,
				"fileNum", outputFileNum,
				"entriesInFile", currentFileEntryCount)
			if err := cm.finishOutputFile(currentWriter, outputFileNum, compaction, currentFileEntryCount); err != nil {
				return nil, err
			}
			currentWriter = nil
			currentOutputFile++
			outputFileNum = cm.versions.NewFileNumber()
		}
	}

	// Finish the last output file if it exists
	if currentWriter != nil {
		cm.logger.Info("FINISHING LAST FILE",
			"entryCount", entryCount,
			"entriesInFile", currentFileEntryCount,
			"currentFileSize", int64(currentWriter.EstimatedSize()),
			"maxOutputFileSize", compaction.maxOutputFileSize)
		if err := cm.finishOutputFile(currentWriter, outputFileNum, compaction, currentFileEntryCount); err != nil {
			return nil, err
		}
	} else {
		cm.logger.Info("NO FILE TO FINISH",
			"entryCount", entryCount)
	}

	// Create version edit for the compaction results
	edit, err := cm.installCompactionResults(compaction)
	if err != nil {
		return nil, err
	}

	return edit, nil
}

// createCompactionIterator creates a merged iterator for all input files.
func (cm *CompactionManager) createCompactionIterator(compaction *Compaction, version *Version) (Iterator, error) {
	var iterators []Iterator
	var cachedReaders []*CachedReader
	// For compaction, we need to see tombstones to handle deletions properly
	mergedIter := NewMergeIterator(nil, true, 0)

	// Set range deletes from version for filtering during compaction
	mergedIter.SetRangeDeletes(version.GetRangeDeletes())

	// Add iterators for all input files
	for _, levelFiles := range compaction.inputFiles {
		for _, file := range levelFiles {
			// Files are already referenced during compaction setup, no need to ref again

			// Use file cache to open SSTable (handles missing files gracefully)
			filePath := filepath.Join(cm.path, fmt.Sprintf("%06d.sst", file.FileNum))
			cachedReader, err := cm.fileCache.Get(file.FileNum, filePath)
			if err != nil {
				// File missing during compaction is a serious error
				cm.logger.Error("failed to open SSTable during compaction", "error", err,
					"file_num", file.FileNum,
					"version_number", version.number)
				continue
			}

			cachedReaders = append(cachedReaders, cachedReader)
			reader := cachedReader.Reader()
			iter := reader.NewIterator(true) // Disable block cache for compaction
			iterators = append(iterators, iter)
			mergedIter.AddIterator(iter)
			compaction.stats.FilesRead++
		}
	}

	if len(iterators) == 0 {
		// All files were missing - return empty iterator
		return &emptyIterator{}, nil
	}

	return &compactionIterator{
		Iterator:      mergedIter,
		cachedReaders: cachedReaders,
		inputFiles:    compaction.inputFiles,
	}, nil
}

// createOutputFile creates a new output SSTable file.
// The level parameter determines which compression config to use for tiered compression.
func (cm *CompactionManager) createOutputFile(fileNum uint64, level int) (*sstable.SSTableWriter, error) {

	wopts := sstable.SSTableOpts{
		Path:                 filepath.Join(cm.path, fmt.Sprintf("%06d.sst", fileNum)),
		Compression:          cm.options.GetCompressionForLevel(level),
		Logger:               cm.logger,
		BlockSize:            cm.options.BlockSize,
		BlockRestartInterval: cm.options.BlockRestartInterval,
	}
	return sstable.NewSSTableWriter(wopts)
}

// finishOutputFile finishes writing an output file and adds it to the compaction.
func (cm *CompactionManager) finishOutputFile(writer *sstable.SSTableWriter, fileNum uint64, compaction *Compaction, numEntries int) error {
	if err := writer.Finish(); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	cm.fileCache.Evict(fileNum)

	// Create file metadata
	fileMetadata := &FileMetadata{
		FileNum:     fileNum,
		Size:        writer.EstimatedSize(),
		SmallestKey: writer.SmallestKey(),
		LargestKey:  writer.LargestKey(),
		NumEntries:  uint64(numEntries),
	}

	compaction.outputFiles = append(compaction.outputFiles, fileMetadata)

	cm.logger.Info("COMPACTION_OUTPUT_FILE_CREATED",
		"fileNum", fileNum,
		"size", fileMetadata.Size,
		"numEntries", numEntries,
		"totalOutputFiles", len(compaction.outputFiles))

	return nil
}

// installCompactionResults creates the version edit for compaction results
// Returns the edit so DB can apply it under proper locking
func (cm *CompactionManager) installCompactionResults(compaction *Compaction) (*VersionEdit, error) {
	edit := NewVersionEdit()

	// Debug: Log what we're about to do
	cm.logger.Info("COMPACTION_INSTALL_START",
		"level", compaction.level,
		"outputLevel", compaction.outputLevel,
		"inputFilesL0", len(compaction.inputFiles[0]),
		"inputFilesL1", len(compaction.inputFiles[1]),
		"outputFiles", len(compaction.outputFiles))

	// Remove input files
	for level, levelFiles := range compaction.inputFiles {
		actualLevel := compaction.level + level
		var fileNums []uint64
		for _, file := range levelFiles {
			edit.RemoveFile(actualLevel, file.FileNum)
			fileNums = append(fileNums, file.FileNum)
		}
		if len(fileNums) > 0 {
			cm.logger.Info("COMPACTION_REMOVING_FILES", "level", actualLevel, "files", fileNums)
		}
	}

	// Add output files
	var outputFileNums []uint64
	for _, file := range compaction.outputFiles {
		edit.AddFile(compaction.outputLevel, file)
		outputFileNums = append(outputFileNums, file.FileNum)
	}
	if len(outputFileNums) > 0 {
		cm.logger.Info("COMPACTION_ADDING_FILES", "level", compaction.outputLevel, "files", outputFileNums)
	}

	// Note: Input files will be unreferenced by the version system when
	// they are removed from versions and no longer needed by any iterators
	return edit, nil
}

// Close shuts down the compaction manager
func (cm *CompactionManager) Close() {
	cm.mu.Lock()
	if cm.closed {
		cm.mu.Unlock()
		return
	}

	cm.closed = true
	close(cm.closeChan) // Signal the worker to shut down
	cm.mu.Unlock()

	// Wait for the worker goroutine to finish
	cm.wg.Wait()
}

// IsCompactionInProgress returns false in simplified model
// The worker handles one compaction at a time internally
func (cm *CompactionManager) IsCompactionInProgress() bool {
	return false // Simplified: we don't expose internal worker state
}

// Compaction represents a compaction operation
type Compaction struct {
	level             int
	outputLevel       int
	version           *Version
	inputFiles        [][]*FileMetadata // [level][file_index]
	outputFiles       []*FileMetadata
	stats             *CompactionStats
	maxOutputFileSize int64
}

// KeyNotExistsBeyondOutputLevel returns true if the given user key definitely
// does not exist in any level beyond the output level. This allows for early
// tombstone dropping during compaction - if a tombstone's key doesn't exist
// in lower levels, the tombstone can be safely dropped.
//
// Returns true if the key is guaranteed not to exist beyond output level.
// Returns false if the key might exist (conservative approach).
func (c *Compaction) KeyNotExistsBeyondOutputLevel(userKey []byte) bool {
	// If compacting to the bottom level, no levels exist beyond it
	if c.outputLevel >= len(c.version.files)-1 {
		return true
	}

	// Check all levels below the output level
	for level := c.outputLevel + 1; level < len(c.version.files); level++ {
		files := c.version.GetFiles(level)

		// Check if userKey falls within any file's key range at this level
		for _, file := range files {
			// Files in L1+ have non-overlapping ranges within a level
			// Check if userKey is within [SmallestKey, LargestKey]
			if file.SmallestKey.UserKey().Compare(userKey) <= 0 &&
				file.LargestKey.UserKey().Compare(userKey) >= 0 {
				// Key falls within this file's range, so it might exist
				return false
			}
		}
	}

	// Key doesn't fall within any file range in levels beyond output
	return true
}

// Cleanup cleans up resources used by the compaction.
func (c *Compaction) Cleanup() {
	// Note: File unreferencing is handled by the version system when files
	// are removed from versions. We don't unref files here to avoid
	// unreferencing them while they're still in use by iterators.

	// Note: version cleanup is handled by the caller under db.mu protection
	// We don't unref the version here because it might still be in use by iterators
}

// compactionIterator wraps a merged iterator and handles cleanup of cached readers
type compactionIterator struct {
	Iterator
	cachedReaders []*CachedReader
	inputFiles    [][]*FileMetadata
}

// Close releases cached readers and unreferences files.
func (ci *compactionIterator) Close() error {
	// Close the underlying iterator first
	if err := ci.Iterator.Close(); err != nil {
		return err
	}

	// Cached readers are cleaned up through epoch system

	// Files are now protected by version references only

	return nil
}

// cleanupObsoleteRangeDeletes checks for range deletes that no longer overlap
// with any keys in the database and removes them
func (cm *CompactionManager) cleanupObsoleteRangeDeletes() {
	version := cm.versions.GetCurrentVersion()
	if version == nil {
		return
	}

	rangeDeletes := version.GetRangeDeletes()
	if len(rangeDeletes) == 0 {
		return
	}

	var obsoleteIDs []uint64

	// Check each range delete to see if it still overlaps with any keys
	for _, rt := range rangeDeletes {
		hasOverlap := false

		// Check all levels for overlapping keys
		for level := 0; level < cm.options.NumLevels; level++ {
			files := version.GetFiles(level)
			for _, file := range files {
				// Check if file's key range overlaps with range delete
				// Range delete is [rt.Start, rt.End)
				// File range is [file.SmallestKey, file.LargestKey]

				smallestUser := file.SmallestKey.UserKey()
				largestUser := file.LargestKey.UserKey()

				// Check for overlap: file.largest >= rt.Start AND file.smallest < rt.End
				if largestUser.Compare(keys.UserKey(rt.Start)) >= 0 &&
					smallestUser.Compare(keys.UserKey(rt.End)) < 0 {
					hasOverlap = true
					break
				}
			}
			if hasOverlap {
				break
			}
		}

		// If no overlap found, this range delete is obsolete
		if !hasOverlap {
			obsoleteIDs = append(obsoleteIDs, rt.ID)
			cm.logger.Debug("range delete is obsolete", "id", rt.ID,
				"start", string(rt.Start), "end", string(rt.End))
		}
	}

	// Remove obsolete range deletes
	if len(obsoleteIDs) > 0 {
		rangeDeleteEdit := NewRangeDeleteEdit()
		for _, id := range obsoleteIDs {
			rangeDeleteEdit.RemoveRangeDelete(id)
		}

		// Apply the edit (with empty version edit)
		emptyEdit := NewVersionEdit()
		err := cm.versions.LogAndApplyWithRangeDeletes(emptyEdit, rangeDeleteEdit)
		if err != nil {
			cm.logger.Error("failed to remove obsolete range deletes", "error", err)
			return
		}

		cm.logger.Info("removed obsolete range deletes", "count", len(obsoleteIDs))
	}
}

// emptyIterator is a placeholder iterator for when no input files are available.
type emptyIterator struct{}

func (ei *emptyIterator) Valid() bool                 { return false }
func (ei *emptyIterator) SeekToFirst()                {}
func (ei *emptyIterator) Seek(target keys.EncodedKey) {}
func (ei *emptyIterator) Next()                       {}
func (ei *emptyIterator) Key() keys.EncodedKey        { return nil }
func (ei *emptyIterator) Value() []byte               { return nil }
func (ei *emptyIterator) Error() error                { return nil }
func (ei *emptyIterator) Close() error                { return nil }
