package epoch

import (
	"sync/atomic"
	"testing"
)

func TestResourceWindowSafety(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	executed := atomic.Bool{}
	resourceID := "test_resource_1"

	// Register a resource in epoch 1
	epoch1 := EnterEpoch()
	RegisterResource(resourceID, epoch1, func() error {
		executed.Store(true)
		return nil
	})

	// Advance epoch and mark resource for cleanup
	AdvanceEpoch() // Move to next epoch
	MarkResourceForCleanup(resourceID)

	// Cleanup should not run - epoch 1 is still active
	cleaned := TryCleanup()
	if cleaned != 0 {
		t.Errorf("Expected 0 cleanups, got %d", cleaned)
	}
	if executed.Load() {
		t.Error("Resource should not be cleaned while epoch is active")
	}

	// Exit epoch 1
	ExitEpoch(epoch1)

	// Now cleanup should run
	cleaned = TryCleanup()
	if cleaned != 1 {
		t.Errorf("Expected 1 cleanup, got %d", cleaned)
	}
	if !executed.Load() {
		t.Error("Resource should be cleaned after epoch exit")
	}
}

func TestMultiEpochResourceSurvival(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	executed := atomic.Bool{}
	resourceID := "test_resource_multiepoch"

	// Register resource in epoch 1
	epoch1 := EnterEpoch()
	RegisterResource(resourceID, epoch1, func() error {
		executed.Store(true)
		return nil
	})

	// Enter epoch 2 while epoch 1 is still active
	epoch2 := EnterEpoch()

	// Advance epoch and mark resource for cleanup
	AdvanceEpoch() // Move to next epoch
	MarkResourceForCleanup(resourceID)

	// Cleanup should not run - both epochs are still active
	cleaned := TryCleanup()
	if cleaned != 0 {
		t.Errorf("Expected 0 cleanups, got %d", cleaned)
	}
	if executed.Load() {
		t.Error("Resource should not be cleaned while any epoch is active")
	}

	// Exit epoch 1 - epoch 2 still active
	ExitEpoch(epoch1)

	// Cleanup should still not run - epoch 2 is still active
	cleaned = TryCleanup()
	if cleaned != 0 {
		t.Errorf("Expected 0 cleanups, got %d", cleaned)
	}
	if executed.Load() {
		t.Error("Resource should not be cleaned while epoch 2 is still active")
	}

	// Exit epoch 2 - now no active epochs
	ExitEpoch(epoch2)

	// Now cleanup should run
	cleaned = TryCleanup()
	if cleaned != 1 {
		t.Errorf("Expected 1 cleanup, got %d", cleaned)
	}
	if !executed.Load() {
		t.Error("Resource should be cleaned after all epochs exit")
	}
}

func TestResourceExistsCheck(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	resourceID := "test_resource_exists"

	// Resource should not exist initially
	if ResourceExists(resourceID) {
		t.Error("Resource should not exist initially")
	}

	// Register resource
	epoch := EnterEpoch()
	RegisterResource(resourceID, epoch, func() error { return nil })

	// Resource should now exist
	if !ResourceExists(resourceID) {
		t.Error("Resource should exist after registration")
	}

	// Mark for cleanup and clean up
	AdvanceEpoch() // Advance to next epoch
	MarkResourceForCleanup(resourceID)
	ExitEpoch(epoch)
	TryCleanup()

	// Resource should no longer exist
	if ResourceExists(resourceID) {
		t.Error("Resource should not exist after cleanup")
	}
}

func TestConcurrentMultiEpochAccess(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	resourceID := "test_resource_concurrent"
	executed := atomic.Bool{}

	// Enter two epochs and keep them active
	epoch1 := EnterEpoch()
	epoch2 := EnterEpoch()

	// Register resource in epoch1
	RegisterResource(resourceID, epoch1, func() error {
		executed.Store(true)
		return nil
	})

	// Mark resource for cleanup
	AdvanceEpoch() // Advance to next epoch
	MarkResourceForCleanup(resourceID)

	// Cleanup should not run while epochs are active
	cleaned := TryCleanup()
	if cleaned != 0 {
		t.Errorf("Expected 0 cleanups while epochs active, got %d", cleaned)
	}
	if executed.Load() {
		t.Error("Resource should not be cleaned while epochs are still active")
	}

	// Exit one epoch - should still not clean up
	ExitEpoch(epoch1)
	cleaned = TryCleanup()
	if cleaned != 0 {
		t.Errorf("Expected 0 cleanups while epoch2 still active, got %d", cleaned)
	}

	// Exit final epoch - now cleanup should succeed
	ExitEpoch(epoch2)
	cleaned = TryCleanup()
	if cleaned != 1 {
		t.Errorf("Expected 1 cleanup after all epochs exit, got %d", cleaned)
	}
	if !executed.Load() {
		t.Error("Resource should be cleaned after all epochs exit")
	}
}

func TestBackwardCompatibilityWithLegacyCleanup(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	legacyExecuted := atomic.Bool{}
	resourceExecuted := atomic.Bool{}

	// Test legacy epoch-based cleanup still works
	epoch1 := EnterEpoch()
	ScheduleCleanup(func() error {
		legacyExecuted.Store(true)
		return nil
	})
	ExitEpoch(epoch1)

	// Test new resource-based cleanup
	epoch2 := EnterEpoch()
	RegisterResource("test_resource", epoch2, func() error {
		resourceExecuted.Store(true)
		return nil
	})
	AdvanceEpoch() // Advance to next epoch
	MarkResourceForCleanup("test_resource")
	ExitEpoch(epoch2)

	// Both should be cleaned up
	cleaned := TryCleanup()
	if cleaned != 2 {
		t.Errorf("Expected 2 cleanups (legacy + resource), got %d", cleaned)
	}
	if !legacyExecuted.Load() {
		t.Error("Legacy cleanup should have executed")
	}
	if !resourceExecuted.Load() {
		t.Error("Resource cleanup should have executed")
	}
}

func TestGetOldestActiveReadEpoch(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	// No active readers initially - should return max uint64 for safe cleanup
	oldest := globalManager.GetOldestActiveReadEpoch()
	expectedMax := ^uint64(0)
	if oldest != expectedMax {
		t.Errorf("Expected oldest epoch to be max uint64 (%d) when no active readers, got %d", expectedMax, oldest)
	}

	// Enter some epochs
	epoch1 := EnterEpoch()
	epoch2 := EnterEpoch()
	epoch3 := EnterEpoch()

	// Oldest should be epoch1
	oldest = globalManager.GetOldestActiveReadEpoch()
	if oldest != epoch1 {
		t.Errorf("Expected oldest active epoch to be %d, got %d", epoch1, oldest)
	}

	// Exit epoch1
	ExitEpoch(epoch1)

	// Oldest should now be epoch2
	oldest = globalManager.GetOldestActiveReadEpoch()
	if oldest != epoch2 {
		t.Errorf("Expected oldest active epoch to be %d after exiting epoch1, got %d", epoch2, oldest)
	}

	// Exit all epochs
	ExitEpoch(epoch2)
	ExitEpoch(epoch3)

	// Should return max uint64 when no active readers
	oldest = globalManager.GetOldestActiveReadEpoch()
	if oldest != expectedMax {
		t.Errorf("Expected oldest epoch to be max uint64 (%d) after all exit, got %d", expectedMax, oldest)
	}
}
