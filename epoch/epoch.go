package epoch

import (
	"sync"
	"sync/atomic"
)

// EpochCleanupFunc defines a function that will be called to clean up resources
// when it's safe to do so (no active readers in the epoch).
type EpochCleanupFunc func() error

// ResourceWindow tracks the lifecycle of a resource using xmin/xmax epoch windows.
// A resource is safe to clean up only when Xmax <= oldest active read epoch.
type ResourceWindow struct {
	ResourceID  string           // Unique identifier for the resource
	Xmin        uint64           // Epoch when resource was created/needed
	Xmax        uint64           // Epoch when resource was marked for cleanup (0 if still active)
	CleanupFunc EpochCleanupFunc // Function to call when safe to cleanup
}

// GlobalEpochManager manages epoch-based resource cleanup across the entire system.
// It provides a simple way to defer cleanup operations until it's safe to perform them.
type GlobalEpochManager struct {
	// currentEpoch is the current epoch counter, incremented atomically
	currentEpoch atomic.Uint64

	// readerCounts tracks the number of active readers for each epoch
	readerCounts sync.Map // epoch (uint64) -> *atomic.Int32

	// cleanupFuncs stores cleanup functions for each epoch (legacy/immediate cleanup)
	cleanupFuncs sync.Map // epoch (uint64) -> []EpochCleanupFunc

	// cleanupMu protects modifications to cleanup function lists
	cleanupMu sync.Mutex

	// resourceWindows tracks resource lifecycles with xmin/xmax windows
	resourceWindows sync.Map // resourceID (string) -> *ResourceWindow

	// resourceMu protects modifications to resource windows
	resourceMu sync.RWMutex
}

// Global singleton instance
var globalManager = &GlobalEpochManager{}

// EnterEpoch enters a new epoch and returns the epoch number.
// Callers must call ExitEpoch with the returned epoch when done.
func EnterEpoch() uint64 {
	return globalManager.EnterEpoch()
}

// ExitEpoch exits the specified epoch, decrementing the reader count.
func ExitEpoch(epoch uint64) {
	globalManager.ExitEpoch(epoch)
}

// GetCurrentEpoch returns the current active epoch
func GetCurrentEpoch() uint64 {
	return globalManager.GetCurrentEpoch()
}

// ScheduleCleanup schedules a cleanup function to be executed when the current
// epoch has no active readers.
func ScheduleCleanup(cleanup EpochCleanupFunc) {
	globalManager.ScheduleCleanup(cleanup)
}

// TryCleanup attempts to run cleanup functions for epochs with no active readers.
// Returns the number of cleanup functions executed.
func TryCleanup() int {
	return globalManager.TryCleanup()
}

// AdvanceEpoch manually advances the epoch counter. Primarily for testing.
func AdvanceEpoch() uint64 {
	return globalManager.AdvanceEpoch()
}

// RegisterResource registers a resource with the epoch manager for lifecycle tracking.
// The resource will be cleaned up when it's safe (after being marked for cleanup and
// no active readers can access it).
func RegisterResource(resourceID string, epoch uint64, cleanup EpochCleanupFunc) {
	globalManager.RegisterResource(resourceID, epoch, cleanup)
}

// MarkResourceForCleanup marks a resource for cleanup by setting its Xmax.
// The resource will be cleaned up when it's safe to do so.
func MarkResourceForCleanup(resourceID string) {
	globalManager.MarkResourceForCleanup(resourceID)
}

// ResourceExists checks if a resource is currently registered.
func ResourceExists(resourceID string) bool {
	return globalManager.ResourceExists(resourceID)
}

// EnterEpoch enters a new epoch and returns the epoch number.
func (gem *GlobalEpochManager) EnterEpoch() uint64 {
	for {
		e := gem.currentEpoch.Load()

		c, _ := gem.readerCounts.LoadOrStore(e, &atomic.Int32{})
		c.(*atomic.Int32).Add(1)

		// check to make sure we didn't lose our epoch
		if e == gem.currentEpoch.Load() {
			return e
		}

		// epoch wasn't the same already so lets try again
		c.(*atomic.Int32).Add(-1)
	}
}

// ExitEpoch exits the specified epoch.
func (gem *GlobalEpochManager) ExitEpoch(epoch uint64) {
	countInterface, exists := gem.readerCounts.Load(epoch)
	if !exists {
		return // Epoch doesn't exist, nothing to do
	}

	count := countInterface.(*atomic.Int32)
	count.Add(-1)
}

// GetCurrentEpoch returns the current epoch
func (gem *GlobalEpochManager) GetCurrentEpoch() uint64 {
	return gem.currentEpoch.Load()
}

// ScheduleCleanup schedules a cleanup function for the current epoch.
func (gem *GlobalEpochManager) ScheduleCleanup(cleanup EpochCleanupFunc) {
	epoch := gem.currentEpoch.Load()

	gem.cleanupMu.Lock()
	defer gem.cleanupMu.Unlock()

	// Get or create cleanup list for this epoch
	cleanupInterface, _ := gem.cleanupFuncs.LoadOrStore(epoch, make([]EpochCleanupFunc, 0))
	cleanupList := cleanupInterface.([]EpochCleanupFunc)

	// Append new cleanup function
	updatedList := append(cleanupList, cleanup)
	gem.cleanupFuncs.Store(epoch, updatedList)
}

// TryCleanup attempts to execute cleanup functions for epochs and resources when safe.
// It handles both legacy epoch-based cleanup and new resource window cleanup.
func (gem *GlobalEpochManager) TryCleanup() int {
	executed := 0

	// 1. Handle legacy epoch-based cleanup (for backward compatibility)
	epochsToCleanup := make([]uint64, 0)

	gem.cleanupFuncs.Range(func(key, value any) bool {
		epoch := key.(uint64)

		// Check if this epoch has active readers
		countInterface, exists := gem.readerCounts.Load(epoch)
		if !exists {
			// No readers ever entered this epoch, so cleanup should wait
			// until someone enters and exits the epoch to be safe
			return true
		}

		count := countInterface.(*atomic.Int32)
		if count.Load() == 0 {
			// No active readers, safe to cleanup
			epochsToCleanup = append(epochsToCleanup, epoch)
		}

		return true
	})

	// Execute cleanup functions for safe epochs
	for _, epoch := range epochsToCleanup {
		cleanupInterface, exists := gem.cleanupFuncs.Load(epoch)
		if !exists {
			continue
		}

		cleanupList := cleanupInterface.([]EpochCleanupFunc)
		for _, cleanup := range cleanupList {
			if err := cleanup(); err != nil {
				// TODO: Consider adding logging here when we integrate with the main package
				_ = err
			}
			executed++
		}

		// Remove cleaned up epoch data
		gem.cleanupFuncs.Delete(epoch)
		gem.readerCounts.Delete(epoch)
	}

	// 2. Handle resource window cleanup with xmin/xmax safety
	oldestActiveEpoch := gem.GetOldestActiveReadEpoch()
	resourcesToCleanup := make([]*ResourceWindow, 0)

	gem.resourceWindows.Range(func(key, value any) bool {
		window := value.(*ResourceWindow)

		// Synchronize access to window fields to prevent data race
		gem.resourceMu.Lock()
		xmax := window.Xmax
		gem.resourceMu.Unlock()

		// Only clean up resources that:
		// 1. Have been marked for cleanup (Xmax != 0)
		// 2. Are safe to clean (Xmax <= oldest active epoch)
		if xmax != 0 && xmax < oldestActiveEpoch {
			resourcesToCleanup = append(resourcesToCleanup, window)
		}
		return true
	})

	// Execute resource cleanup functions
	for _, window := range resourcesToCleanup {
		if err := window.CleanupFunc(); err != nil {
			// TODO: Consider adding logging here when we integrate with the main package
			_ = err
		}
		executed++

		// Remove from tracking
		gem.resourceWindows.Delete(window.ResourceID)
	}

	return executed
}

// AdvanceEpoch manually advances the epoch counter and returns the new epoch.
func (gem *GlobalEpochManager) AdvanceEpoch() uint64 {
	return gem.currentEpoch.Add(1)
}

// GetOldestActiveReadEpoch returns the oldest epoch that still has active readers.
// This is the safe point for resource cleanup - resources with Xmax <= this value
// can be safely cleaned up as no active reader can access them.
func (gem *GlobalEpochManager) GetOldestActiveReadEpoch() uint64 {
	var oldestEpoch uint64 = ^uint64(0) // Start with max value
	hasActiveReaders := false

	gem.readerCounts.Range(func(key, value any) bool {
		epoch := key.(uint64)
		count := value.(*atomic.Int32)

		if count.Load() > 0 {
			hasActiveReaders = true
			if epoch < oldestEpoch {
				oldestEpoch = epoch
			}
		}
		return true
	})

	// If no active readers, return max uint64 as safe cleanup point
	// This allows all resources marked for cleanup to be cleaned up
	if !hasActiveReaders {
		return ^uint64(0) // Max uint64
	}

	return oldestEpoch
}

// RegisterResource registers a resource with the epoch manager for lifecycle tracking.
func (gem *GlobalEpochManager) RegisterResource(resourceID string, epoch uint64, cleanup EpochCleanupFunc) {
	gem.resourceMu.Lock()
	defer gem.resourceMu.Unlock()

	window := &ResourceWindow{
		ResourceID:  resourceID,
		Xmin:        epoch,
		Xmax:        0, // Still active
		CleanupFunc: cleanup,
	}
	gem.resourceWindows.Store(resourceID, window)
}

// MarkResourceForCleanup marks a resource for cleanup by setting its Xmax.
func (gem *GlobalEpochManager) MarkResourceForCleanup(resourceID string) {
	if windowInterface, exists := gem.resourceWindows.Load(resourceID); exists {
		window := windowInterface.(*ResourceWindow)
		gem.resourceMu.Lock()
		if window.Xmax == 0 { // Only set if not already marked
			window.Xmax = gem.currentEpoch.Load()
		}
		gem.resourceMu.Unlock()
	}
}

// ResourceExists checks if a resource is currently registered.
func (gem *GlobalEpochManager) ResourceExists(resourceID string) bool {
	_, exists := gem.resourceWindows.Load(resourceID)
	return exists
}

// GetStats returns debugging information about the epoch manager state.
func (gem *GlobalEpochManager) GetStats() (activeEpochs int, pendingCleanups int) {
	gem.readerCounts.Range(func(key, value any) bool {
		activeEpochs++
		return true
	})

	gem.cleanupFuncs.Range(func(key, value any) bool {
		cleanupList := value.([]EpochCleanupFunc)
		pendingCleanups += len(cleanupList)
		return true
	})

	return activeEpochs, pendingCleanups
}

// GetGlobalStats returns stats from the global epoch manager
func GetGlobalStats() (activeEpochs int, pendingCleanups int, pendingResources int) {
	activeEpochs, pendingCleanups = globalManager.GetStats()

	// Count pending resource cleanups
	globalManager.resourceWindows.Range(func(key, value any) bool {
		pendingResources++
		return true
	})

	return activeEpochs, pendingCleanups, pendingResources
}

// GetDetailedEpochInfo returns detailed information about epoch state for debugging
func GetDetailedEpochInfo() map[string]any {
	info := make(map[string]any)

	// Current epoch
	info["current_epoch"] = globalManager.GetCurrentEpoch()
	info["oldest_active_epoch"] = globalManager.GetOldestActiveReadEpoch()

	// Reader counts per epoch
	readerCounts := make(map[uint64]int32)
	globalManager.readerCounts.Range(func(key, value any) bool {
		epoch := key.(uint64)
		count := value.(*atomic.Int32)
		readerCounts[epoch] = count.Load()
		return true
	})
	info["reader_counts"] = readerCounts

	// Cleanup functions per epoch
	cleanupCounts := make(map[uint64]int)
	globalManager.cleanupFuncs.Range(func(key, value any) bool {
		epoch := key.(uint64)
		cleanupList := value.([]EpochCleanupFunc)
		cleanupCounts[epoch] = len(cleanupList)
		return true
	})
	info["cleanup_counts"] = cleanupCounts

	// Resource windows
	resources := make(map[string]map[string]any)
	globalManager.resourceWindows.Range(func(key, value any) bool {
		resourceID := key.(string)
		window := value.(*ResourceWindow)

		// Safely read window fields
		globalManager.resourceMu.RLock()
		resources[resourceID] = map[string]any{
			"xmin": window.Xmin,
			"xmax": window.Xmax,
		}
		globalManager.resourceMu.RUnlock()
		return true
	})
	info["resources"] = resources

	return info
}
