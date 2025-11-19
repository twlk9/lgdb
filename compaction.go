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

// CompactionStats tracks what happened during a compaction. Good for logging and debugging.
type CompactionStats struct {
	BytesRead    uint64
	BytesWritten uint64
	Duration     time.Duration
	FilesRead    int
	FilesWritten int
}

// CompactionManager is the background orchestrator for all compaction work.
// It runs in its own goroutine, waking up when needed, figuring out the most
// important compaction to run, and then executing it.
type CompactionManager struct {
	// Dependencies to get the job done.
	versions  *VersionSet
	fileCache *FileCache
	path      string
	options   *Options
	logger    *slog.Logger

	// Coordination channels.
	wakeupChan chan struct{} // The DB pokes this to say "Hey, check for work!"
	doneChan   chan error    // Signals that a compaction cycle is finished.
	closeChan  chan struct{} // The signal to shut down.

	// Used to signal writers when L0 backpressure is relieved.
	flushBP *sync.Cond

	// Internal state.
	closed bool
	mu     sync.Mutex
	wg     sync.WaitGroup // To wait for the worker to exit gracefully.

	// A cache of the max size for each level, so we don't recalculate it constantly.
	levelMaxBytes []int64
}

// NewCompactionManager creates and starts a new compaction manager.
func NewCompactionManager(versions *VersionSet, fileCache *FileCache, path string, options *Options, logger *slog.Logger, flushBP *sync.Cond) *CompactionManager {
	cm := &CompactionManager{
		versions:      versions,
		fileCache:     fileCache,
		path:          path,
		options:       options,
		logger:        logger,
		wakeupChan:    make(chan struct{}, 1), // Buffered so the DB doesn't block sending a signal.
		doneChan:      make(chan error, 1),    // Buffered for non-blocking completion signals.
		closeChan:     make(chan struct{}),
		flushBP:       flushBP,
		levelMaxBytes: make([]int64, options.MaxLevels),
	}

	for i := 0; i < len(cm.levelMaxBytes); i++ {
		cm.levelMaxBytes[i] = options.GetLevelMaxBytes(i)
	}

	cm.wg.Add(1)
	go cm.compactionWorker()

	return cm
}

// ScheduleCompaction is the public method to poke the compaction worker.
// It's non-blocking and safe to call multiple times.
func (cm *CompactionManager) ScheduleCompaction() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.closed {
		return
	}

	select {
	case cm.wakeupChan <- struct{}{}:
		// Signal sent.
	default:
		// A signal is already pending, no need for another.
	}
}

// compactionWorker is the main loop for the compaction manager. It's a simple
// "wake up -> check for work -> do work -> sleep" cycle.
func (cm *CompactionManager) compactionWorker() {
	defer cm.wg.Done()
	cm.logger.Debug("Compaction worker started")

	for {
		select {
		case <-cm.closeChan:
			cm.logger.Debug("Compaction worker shutting down")
			return

		case <-cm.wakeupChan:
			cm.logger.Debug("Compaction work requested - starting compaction")
			// Handle compaction in a separate function to use defer for cleanup
			cm.handleCompactionCycle()
		}
	}
}

// handleCompactionCycle processes one compaction cycle with proper cleanup
func (cm *CompactionManager) handleCompactionCycle() {
	// Grab a fresh current version when we're ready to do work
	version := cm.versions.GetCurrentVersion()
	// CRITICAL: Always mark version for cleanup when done, even if panic occurs
	defer version.MarkForCleanup()

	// Pick compaction(s) based on fresh version
	compactions := cm.pickCompaction(version)
	if compactions == nil {
		cm.logger.Debug("No compaction needed")
		// Signal completion even when no work is needed
		select {
		case cm.doneChan <- nil:
		default:
		}
		return
	}

	// Process all compactions sequentially
	var err error
	for i, compaction := range compactions {
		if len(compactions) > 1 {
			cm.logger.Debug("Processing compaction batch",
				"progress", fmt.Sprintf("%d/%d", i+1, len(compactions)),
				"level", compaction.level)
		}

		// Do the actual compaction work
		err = cm.doCompactionWork(compaction, version)
		if err != nil {
			cm.logger.Error("Compaction failed",
				"error", err,
				"batchIndex", i,
				"totalInBatch", len(compactions))
			break
		}

		// Check if we need to remove a range delete after this compaction
		if compaction.rangeDeleteToRemove != 0 {
			cm.logger.Debug("Removing range delete after compaction",
				"rangeDeleteID", compaction.rangeDeleteToRemove)
			cm.removeRangeDelete(compaction.rangeDeleteToRemove)
		}
	}

	// Signal completion (non-blocking)
	select {
	case cm.doneChan <- err:
	default:
		// If nobody is listening, that's fine
	}
}

// doCompactionWork performs the complete compaction workflow:
// 1. Do the compaction
// 2. Apply version edit
// 3. Clean up obsolete files
func (cm *CompactionManager) doCompactionWork(compaction *Compaction, version *Version) error {
	inputL0Files := cm.getFileNums(compaction.inputFiles[0])
	inputL1Files := cm.getFileNums(compaction.inputFiles[1])
	cm.logger.Debug("Starting compaction",
		"level", compaction.level,
		"outputLevel", compaction.outputLevel,
		"inputL0Files", inputL0Files,
		"inputL1Files", inputL1Files)
	startTime := time.Now()

	edit, err := cm.doCompaction(compaction, version)
	if err != nil {
		compaction.Cleanup()
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Apply the changes to the manifest to make the new files live.
	if err = cm.versions.LogAndApply(edit); err != nil {
		compaction.Cleanup()
		return fmt.Errorf("failed to apply compaction version edit: %w", err)
	}

	// After a compaction, some range deletes might now be obsolete.
	cm.cleanupObsoleteRangeDeletes()

	outputFiles := cm.getFileNums(compaction.outputFiles)
	cm.logger.Debug("Compaction completed",
		"level", compaction.level,
		"duration", time.Since(startTime),
		"outputFiles", outputFiles)
	compaction.Cleanup()

	// If we just compacted L0, we might have relieved backpressure.
	if compaction.level == 0 && cm.flushBP != nil {
		v := cm.versions.GetCurrentVersion()
		defer v.MarkForCleanup()
		if len(v.GetFiles(0)) < cm.options.L0StopWritesTrigger {
			cm.flushBP.Broadcast() // Wake up any waiting writers.
		}
	}

	return nil
}

// pickCompaction is the brain. It decides which compaction is the most important
// to run right now, based on a priority list.
func (cm *CompactionManager) pickCompaction(version *Version) []*Compaction {
	// Priority 1: L0 compaction. This is the most urgent to keep the write path clear.
	if c := cm.pickL0Compaction(version); c != nil {
		return []*Compaction{c}
	}

	// Priority 2: Size-based compaction for other levels.
	if c := cm.pickLevelCompaction(version); c != nil {
		return []*Compaction{c}
	}

	// Priority 3: Proactively compact to get rid of range delete tombstones.
	if cm.options.RangeDeleteCompactionEnabled {
		if cs := cm.pickRangeDeleteCompaction(version); cs != nil {
			return cs
		}
	}

	return nil
}

// pickL0Compaction checks if L0 has enough files to trigger a compaction into L1.
func (cm *CompactionManager) pickL0Compaction(version *Version) *Compaction {
	l0Files := version.GetFiles(0)
	if len(l0Files) < cm.options.L0CompactionTrigger {
		return nil
	}

	// Simple strategy: compact ALL L0 files together.
	selectedL0Files := l0Files
	l1Files := cm.findOverlappingFiles(version, 1, selectedL0Files)

	compaction := &Compaction{
		level:             0,
		outputLevel:       1,
		version:           version,
		inputFiles:        make([][]*FileMetadata, 2),
		maxOutputFileSize: cm.options.TargetFileSize(1),
	}
	compaction.inputFiles[0] = selectedL0Files
	compaction.inputFiles[1] = l1Files

	cm.logger.Debug("Picked L0 compaction",
		"l0Files", len(selectedL0Files),
		"l0FileNums", cm.getFileNums(selectedL0Files),
		"l1Overlap", len(l1Files),
		"l1FileNums", cm.getFileNums(l1Files))
	return compaction
}

// pickLevelCompaction finds the level (L1+) with the highest "compaction score"
// (i.e., the one that is most oversized) and picks files from it to compact.
func (cm *CompactionManager) pickLevelCompaction(version *Version) *Compaction {
	bestLevel, bestScore := -1, 0.0
	for level := 1; level < cm.options.MaxLevels-1; level++ {
		score := cm.calculateLevelScore(level, version)
		if score >= 1.0 && score > bestScore {
			bestScore, bestLevel = score, level
		}
	}

	if bestLevel == -1 {
		return nil // No levels need compaction.
	}

	selectedFiles := cm.selectFiles(version, version.GetFiles(bestLevel), bestLevel)
	if selectedFiles == nil {
		return nil
	}
	overlappingFiles := cm.findOverlappingFiles(version, bestLevel+1, selectedFiles)

	compaction := &Compaction{
		level:             bestLevel,
		outputLevel:       bestLevel + 1,
		version:           version,
		inputFiles:        make([][]*FileMetadata, 2),
		maxOutputFileSize: cm.options.TargetFileSize(bestLevel + 1),
	}
	compaction.inputFiles[0] = selectedFiles
	compaction.inputFiles[1] = overlappingFiles

	return compaction
}

// findOverlappingFiles finds all files in a target level that have key ranges
// overlapping with a given set of input files.
func (cm *CompactionManager) findOverlappingFiles(version *Version, targetLevel int, inputFiles []*FileMetadata) []*FileMetadata {
	if targetLevel >= len(version.files) || len(inputFiles) == 0 {
		return nil
	}

	var smallestKey, largestKey keys.EncodedKey
	for _, file := range inputFiles {
		if smallestKey == nil || file.SmallestKey.Compare(smallestKey) < 0 {
			smallestKey = file.SmallestKey
		}
		if largestKey == nil || file.LargestKey.Compare(largestKey) > 0 {
			largestKey = file.LargestKey
		}
	}
	if smallestKey == nil || largestKey == nil {
		return nil
	}

	var overlapping []*FileMetadata
	for _, targetFile := range version.GetFiles(targetLevel) {
		if cm.keyRangesOverlap(smallestKey, largestKey, targetFile.SmallestKey, targetFile.LargestKey) {
			overlapping = append(overlapping, targetFile)
		}
	}
	return overlapping
}

// keyRangesOverlap is a simple helper to check if [start1, end1] and [start2, end2] overlap.
func (cm *CompactionManager) keyRangesOverlap(start1, end1, start2, end2 keys.EncodedKey) bool {
	if start1 == nil || end1 == nil || start2 == nil || end2 == nil {
		return false
	}
	return start1.Compare(end2) <= 0 && start2.Compare(end1) <= 0
}

// getFileNums is a logging helper to get just the file numbers from a list of metadata.
func (cm *CompactionManager) getFileNums(files []*FileMetadata) []uint64 {
	nums := make([]uint64, len(files))
	for i, f := range files {
		nums[i] = f.FileNum
	}
	return nums
}

// calculateLevelScore determines how "full" a level is. A score >= 1.0 means it's a candidate for compaction.
func (cm *CompactionManager) calculateLevelScore(level int, version *Version) float64 {
	files := version.GetFiles(level)
	if len(files) == 0 {
		return 0.0
	}
	if level == 0 {
		return float64(len(files)) / float64(cm.options.L0CompactionTrigger)
	}
	var totalSize int64
	for _, file := range files {
		totalSize += int64(file.Size)
	}
	return float64(totalSize) / float64(cm.levelMaxBytes[level])
}

// selectFiles is where we get clever. Instead of just picking the oldest file,
// we might expand our selection to include more files if it helps reduce future
// compaction work (i.e., reduces write amplification).
func (cm *CompactionManager) selectFiles(version *Version, files []*FileMetadata, level int) []*FileMetadata {
	if len(files) <= 1 {
		return nil
	}

	sorted := make([]*FileMetadata, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].FileNum < sorted[j].FileNum })

	// Start by selecting the oldest files.
	selected := []*FileMetadata{sorted[0]}
	if len(sorted) > 1 {
		selected = append(selected, sorted[1])
	}

	// Now, check if this small selection causes a lot of overlap with the next level.
	// If so, it's better to expand our selection now to deal with it all at once.
	if cm.options.CompactionOverlapThreshold > 0 {
		selected = cm.expandSelection(version, selected, sorted, level)
	}

	cm.logger.Debug("Selected compaction files",
		"level", level,
		"count", len(selected),
		"fileNums", cm.getFileNums(selected))
	return selected
}

// expandSelection implements the "smart" part of file selection. If the chosen files
// overlap heavily with the next level, we add more files from the current level to
// the compaction. This costs more now but saves a lot of redundant work later.
func (cm *CompactionManager) expandSelection(version *Version, selected, allSorted []*FileMetadata, level int) []*FileMetadata {
	overlapping := cm.findOverlappingFiles(version, level+1, selected)
	if len(overlapping) == 0 {
		return selected
	}

	var overlapSize, selectedSize int64
	for _, f := range overlapping {
		overlapSize += int64(f.Size)
	}
	for _, f := range selected {
		selectedSize += int64(f.Size)
	}
	if selectedSize == 0 {
		return selected
	}

	overlapRatio := float64(overlapSize) / float64(selectedSize)

	// If the overlap is too high, start expanding.
	if overlapRatio > cm.options.CompactionOverlapThreshold {
		cm.logger.Debug("High overlap detected, expanding selection", "level", level, "ratio", overlapRatio)
		// Keep adding files until the ratio is acceptable or we hit a limit.
		for i := len(selected); i < len(allSorted); i++ {
			expanded := allSorted[:i+1]
			newOverlapping := cm.findOverlappingFiles(version, level+1, expanded)
			var newOverlapSize, newSelectedSize int64
			for _, f := range newOverlapping {
				newOverlapSize += int64(f.Size)
			}
			for _, f := range expanded {
				newSelectedSize += int64(f.Size)
			}
			newRatio := float64(newOverlapSize) / float64(newSelectedSize)

			if newRatio <= cm.options.CompactionTargetOverlapRatio || len(expanded) >= cm.options.CompactionMaxExpansionFiles {
				return expanded
			}
		}
	}

	return selected
}

// pickRangeDeleteCompaction finds the oldest range delete and creates compactions
// to process all files it might affect. This is a proactive way to reclaim space.
// It creates *same-level* compactions to rewrite affected files in place.
func (cm *CompactionManager) pickRangeDeleteCompaction(version *Version) []*Compaction {
	rangeDeletes := version.GetRangeDeletes()
	if len(rangeDeletes) == 0 {
		return nil
	}

	var oldestRT *RangeTombstone
	for i := range rangeDeletes {
		rt := &rangeDeletes[i]
		if oldestRT == nil || rt.Seq < oldestRT.Seq {
			oldestRT = rt
		}
	}
	if oldestRT == nil {
		return nil
	}

	// Find all files across all levels that overlap with this range delete.
	affectedFiles := make(map[int][]*FileMetadata)
	for level := 0; level < cm.options.MaxLevels; level++ {
		for _, file := range version.GetFiles(level) {
			// Optimization: if the range delete is older than every key in the file, it can't do anything.
			if oldestRT.Seq < file.SmallestSeq {
				continue
			}
			if file.LargestKey.UserKey().Compare(oldestRT.Start) >= 0 && file.SmallestKey.UserKey().Compare(oldestRT.End) < 0 {
				affectedFiles[level] = append(affectedFiles[level], file)
			}
		}
	}

	if len(affectedFiles) == 0 {
		return nil
	}

	// Create a compaction job for each affected level.
	var compactions []*Compaction
	for level, files := range affectedFiles {
		compaction := &Compaction{
			level:             level,
			outputLevel:       level, // Same-level compaction!
			version:           version,
			inputFiles:        make([][]*FileMetadata, 2),
			maxOutputFileSize: cm.options.TargetFileSize(level),
		}
		compaction.inputFiles[0] = files
		compactions = append(compactions, compaction)
	}

	if len(compactions) > 0 {
		// After the *last* compaction in the batch is done, we can safely remove the range delete.
		compactions[len(compactions)-1].rangeDeleteToRemove = oldestRT.ID
	}

	return compactions
}

// doCompaction is the heavy lifter. It merges, filters, and writes out new SSTables.
func (cm *CompactionManager) doCompaction(compaction *Compaction, version *Version) (*VersionEdit, error) {
	iter, err := cm.createCompactionIterator(compaction, version)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var currentWriter *sstable.SSTableWriter
	outputFileNum := cm.versions.NewFileNumber()
	var lastUserKey []byte
	var skipCurrentUserKey bool

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()

		if k == nil {
			continue
		}

		// This is the core logic for dropping old/deleted data.
		isNewUserKey := lastUserKey == nil || k.UserKey().Compare(lastUserKey) != 0
		if isNewUserKey {
			lastUserKey = append(lastUserKey[:0], k.UserKey()...)
			skipCurrentUserKey = false

			// If this is a tombstone, can we drop it?
			if k.Kind() == keys.KindDelete {
				// We can drop a tombstone if we know for sure the key doesn't exist
				// in any lower levels.
				if compaction.KeyNotExistsBeyondOutputLevel(k.UserKey()) {
					skipCurrentUserKey = true
				}
			}
		}

		if skipCurrentUserKey {
			continue // Skip this key and all older versions of it.
		}

		// Create a new output file if we need one.
		if currentWriter == nil {
			var err error
			currentWriter, err = cm.createOutputFile(outputFileNum, compaction.outputLevel)
			if err != nil {
				return nil, err
			}
		}

		if err := currentWriter.Add(k, v); err != nil {
			return nil, err
		}

		// If the output file is full, finish it and start a new one.
		if int64(currentWriter.EstimatedSize()) >= compaction.maxOutputFileSize {
			if err := cm.finishOutputFile(currentWriter, outputFileNum, compaction); err != nil {
				return nil, err
			}
			currentWriter = nil
			outputFileNum = cm.versions.NewFileNumber()
		}
	}

	if currentWriter != nil {
		if err := cm.finishOutputFile(currentWriter, outputFileNum, compaction); err != nil {
			return nil, err
		}
	}

	return cm.installCompactionResults(compaction)
}

// createCompactionIterator creates a single merged view over all input SSTables for a compaction.
func (cm *CompactionManager) createCompactionIterator(compaction *Compaction, version *Version) (Iterator, error) {
	expectedIterators := len(compaction.inputFiles[0]) + len(compaction.inputFiles[1])
	mergedIter := NewMergeIterator(nil, true, 0, expectedIterators) // Include tombstones for compaction logic.
	mergedIter.SetRangeDeletes(version.GetRangeDeletes())

	var cachedReaders []*CachedReader
	for _, levelFiles := range compaction.inputFiles {
		for _, file := range levelFiles {
			filePath := filepath.Join(cm.path, fmt.Sprintf("%06d.sst", file.FileNum))
			cachedReader, err := cm.fileCache.Get(file.FileNum, filePath)
			if err != nil {
				// CRITICAL: A file listed in the version is missing or cannot be opened.
				// We cannot proceed with compaction as this would result in data loss.
				cm.logger.Error("FATAL: Failed to open SSTable during compaction - aborting to prevent data loss",
					"error", err,
					"file_num", file.FileNum,
					"file_path", filePath)
				return nil, fmt.Errorf("failed to open SSTable %d for compaction: %w", file.FileNum, err)
			}
			cachedReaders = append(cachedReaders, cachedReader)
			iter := cachedReader.Reader().NewIterator(true) // Disable block cache for compaction scans.
			mergedIter.AddIterator(iter)
		}
	}

	return &compactionIterator{Iterator: mergedIter, cachedReaders: cachedReaders}, nil
}

// createOutputFile creates a new SSTable writer for a compaction output file.
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

// finishOutputFile finalizes an SSTable, closes it, and records its metadata.
func (cm *CompactionManager) finishOutputFile(writer *sstable.SSTableWriter, fileNum uint64, compaction *Compaction) error {
	if err := writer.Finish(); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	cm.fileCache.Evict(fileNum)

	fileMetadata := &FileMetadata{
		FileNum:       fileNum,
		Size:          writer.EstimatedSize(),
		SmallestKey:   writer.SmallestKey(),
		LargestKey:    writer.LargestKey(),
		NumEntries:    writer.NumEntries(),
		SmallestSeq:   writer.SmallestSeq(),
		LargestSeq:    writer.LargestSeq(),
		NumTombstones: writer.NumTombstones(),
	}
	compaction.outputFiles = append(compaction.outputFiles, fileMetadata)
	return nil
}

// installCompactionResults creates the VersionEdit that describes the changes from the compaction.
func (cm *CompactionManager) installCompactionResults(compaction *Compaction) (*VersionEdit, error) {
	edit := NewVersionEdit()
	// Mark input files for deletion.
	for level, levelFiles := range compaction.inputFiles {
		actualLevel := compaction.level + level
		for _, file := range levelFiles {
			edit.RemoveFile(actualLevel, file.FileNum)
		}
	}
	// Add the new output files.
	for _, file := range compaction.outputFiles {
		edit.AddFile(compaction.outputLevel, file)
	}
	return edit, nil
}

// Close shuts down the compaction manager and waits for the worker to finish.
func (cm *CompactionManager) Close() {
	cm.mu.Lock()
	if cm.closed {
		cm.mu.Unlock()
		return
	}
	cm.closed = true
	close(cm.closeChan)
	cm.mu.Unlock()
	cm.wg.Wait()
}

// IsCompactionInProgress is a simplified check.
func (cm *CompactionManager) IsCompactionInProgress() bool {
	return false // We don't expose the internal worker state for now.
}

// Compaction represents a single compaction job. It's the "job ticket" for the worker.
type Compaction struct {
	level       int
	outputLevel int
	version     *Version
	inputFiles  [][]*FileMetadata // [0] is for level, [1] is for level+1
	outputFiles []*FileMetadata

	stats               *CompactionStats
	maxOutputFileSize   int64
	rangeDeleteToRemove uint64 // If non-zero, remove this range delete after the job is done.
}

// KeyNotExistsBeyondOutputLevel is a crucial optimization. It checks if a key could
// possibly exist in any level deeper than where we're compacting to. If not, we can
// safely drop any tombstones for that key, preventing them from living forever.
func (c *Compaction) KeyNotExistsBeyondOutputLevel(userKey []byte) bool {
	if c.outputLevel >= len(c.version.files)-1 {
		return true // We're compacting to the bottom-most level.
	}
	for level := c.outputLevel + 1; level < len(c.version.files); level++ {
		for _, file := range c.version.GetFiles(level) {
			// Since files in L1+ are sorted and non-overlapping, we just need to check if
			// the key is within the file's range.
			if file.SmallestKey.UserKey().Compare(userKey) <= 0 && file.LargestKey.UserKey().Compare(userKey) >= 0 {
				return false // The key *might* exist in this lower level.
			}
		}
	}
	return true // The key does not exist in any lower level.
}

// Cleanup releases resources used by the compaction.
func (c *Compaction) Cleanup() {
	// File unreferencing is handled by the VersionSet when the version edit is applied.
}

// compactionIterator is a wrapper to ensure we clean up resources after a compaction scan.
type compactionIterator struct {
	Iterator
	cachedReaders []*CachedReader
}

// Close ensures the underlying iterators are closed.
func (ci *compactionIterator) Close() error {
	return ci.Iterator.Close()
}

// cleanupObsoleteRangeDeletes scans for range deletes that no longer cover any data and removes them.
func (cm *CompactionManager) cleanupObsoleteRangeDeletes() {
	version := cm.versions.GetCurrentVersion()
	if version == nil {
		return
	}
	defer version.MarkForCleanup()

	rangeDeletes := version.GetRangeDeletes()
	if len(rangeDeletes) == 0 {
		return
	}

	var obsoleteIDs []uint64
	for _, rt := range rangeDeletes {
		hasOverlap := false
		for level := 0; level < cm.options.MaxLevels && !hasOverlap; level++ {
			for _, file := range version.GetFiles(level) {
				if rt.Seq < file.SmallestSeq {
					continue
				}
				if file.LargestKey.UserKey().Compare(rt.Start) >= 0 && file.SmallestKey.UserKey().Compare(rt.End) < 0 {
					hasOverlap = true
					break
				}
			}
		}
		if !hasOverlap {
			obsoleteIDs = append(obsoleteIDs, rt.ID)
		}
	}

	if len(obsoleteIDs) > 0 {
		edit := NewRangeDeleteEdit()
		for _, id := range obsoleteIDs {
			edit.RemoveRangeDelete(id)
		}
		if err := cm.versions.LogAndApplyWithRangeDeletes(NewVersionEdit(), edit); err != nil {
			cm.logger.Error("Failed to remove obsolete range deletes", "error", err)
		} else {
			cm.logger.Debug("Removed obsolete range deletes", "count", len(obsoleteIDs))
		}
	}
}

// removeRangeDelete removes a specific range delete after its compaction jobs are complete.
func (cm *CompactionManager) removeRangeDelete(id uint64) {
	edit := NewRangeDeleteEdit()
	edit.RemoveRangeDelete(id)
	if err := cm.versions.LogAndApplyWithRangeDeletes(NewVersionEdit(), edit); err != nil {
		cm.logger.Error("Failed to remove range delete", "error", err, "id", id)
	} else {
		cm.logger.Debug("Removed range delete", "id", id)
	}
}

// emptyIterator is a placeholder for when a compaction has no input files.
type emptyIterator struct{}

func (ei *emptyIterator) Valid() bool                 { return false }
func (ei *emptyIterator) SeekToFirst()                {}
func (ei *emptyIterator) Seek(target keys.EncodedKey) {}
func (ei *emptyIterator) Next()                       {}
func (ei *emptyIterator) Key() keys.EncodedKey        { return nil }
func (ei *emptyIterator) Value() []byte               { return nil }
func (ei *emptyIterator) Error() error                { return nil }
func (ei *emptyIterator) Close() error                { return nil }
