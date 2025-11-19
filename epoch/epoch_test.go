package epoch

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Helper function for tests: advances epoch to make older epochs eligible for cleanup
func advanceEpoch() {
	current := globalManager.currentEpoch.Load()
	globalManager.currentEpoch.Store(current + 1)
}

func TestBasicEpochOperations(t *testing.T) {
	// Reset global manager for clean test
	globalManager = &GlobalEpochManager{}
	globalManager.currentEpoch.Store(1) // Start at epoch 1

	// Test entering and exiting epochs
	epoch1 := EnterEpoch()
	if epoch1 != 1 {
		t.Errorf("Expected first epoch to be 1, got %d", epoch1)
	}

	epoch2 := EnterEpoch()
	if epoch2 != 1 {
		t.Errorf("Expected second epoch to still be 1, got %d", epoch2)
	}

	ExitEpoch(epoch1)
	ExitEpoch(epoch2)
}

func TestScheduleCleanup(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	executed := atomic.Bool{}

	// Schedule a cleanup function for current epoch (epoch 0)
	ScheduleCleanup(func() error {
		executed.Store(true)
		return nil
	})

	// Cleanup shouldn't run yet - epoch 0 is still current
	cleaned := TryCleanup()
	if cleaned != 0 {
		t.Errorf("Expected 0 cleanups, got %d", cleaned)
	}
	if executed.Load() {
		t.Error("Cleanup function should not have executed yet")
	}

	// Advance epoch by entering a new one, making epoch 0 eligible for cleanup
	epoch1 := EnterEpoch() // This should get epoch 0
	ExitEpoch(epoch1)
	epoch2 := EnterEpoch() // This should get epoch 1 (or trigger advancement)
	ExitEpoch(epoch2)

	// Force epoch advancement by manually incrementing (simulating background advancement)
	advanceEpoch()

	// Now cleanup should run for the old epoch 0
	cleaned = TryCleanup()
	if cleaned != 1 {
		t.Errorf("Expected 1 cleanup, got %d", cleaned)
	}
	if !executed.Load() {
		t.Error("Cleanup function should have executed")
	}
}

func TestEpochSafety(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	executed := atomic.Bool{}

	// Enter an epoch but don't exit
	epoch := EnterEpoch()

	// Schedule cleanup
	ScheduleCleanup(func() error {
		executed.Store(true)
		return nil
	})

	// Cleanup shouldn't run while epoch is active
	cleaned := TryCleanup()
	if cleaned != 0 {
		t.Errorf("Expected 0 cleanups, got %d", cleaned)
	}
	if executed.Load() {
		t.Error("Cleanup should not run while epoch is active")
	}

	// Exit the epoch
	ExitEpoch(epoch)

	// Advance epoch to make the old epoch eligible for cleanup
	advanceEpoch()

	// Now cleanup should run
	cleaned = TryCleanup()
	if cleaned != 1 {
		t.Errorf("Expected 1 cleanup, got %d", cleaned)
	}
	if !executed.Load() {
		t.Error("Cleanup should run after epoch exit")
	}
}

func TestMultipleEpochs(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	var cleanupOrder []int
	var mu sync.Mutex

	// Enter first epoch
	epoch1 := EnterEpoch()
	ScheduleCleanup(func() error {
		mu.Lock()
		cleanupOrder = append(cleanupOrder, 1)
		mu.Unlock()
		return nil
	})

	// Advance to second epoch
	AdvanceEpoch()
	epoch2 := EnterEpoch()
	ScheduleCleanup(func() error {
		mu.Lock()
		cleanupOrder = append(cleanupOrder, 2)
		mu.Unlock()
		return nil
	})

	// Exit first epoch - should allow its cleanup
	ExitEpoch(epoch1)
	// No need to advance - epoch 0 should be eligible since current epoch is 1
	cleaned := TryCleanup()
	if cleaned != 1 {
		t.Errorf("Expected 1 cleanup after exiting epoch1, got %d", cleaned)
	}

	mu.Lock()
	if len(cleanupOrder) != 1 || cleanupOrder[0] != 1 {
		t.Errorf("Expected cleanup order [1], got %v", cleanupOrder)
	}
	mu.Unlock()

	// Exit second epoch - should allow its cleanup
	ExitEpoch(epoch2)
	AdvanceEpoch() // Advance to epoch 2, making epoch 1 eligible for cleanup
	cleaned = TryCleanup()
	if cleaned != 1 {
		t.Errorf("Expected 1 cleanup after exiting epoch2, got %d", cleaned)
	}

	mu.Lock()
	if len(cleanupOrder) != 2 || cleanupOrder[1] != 2 {
		t.Errorf("Expected cleanup order [1, 2], got %v", cleanupOrder)
	}
	mu.Unlock()
}

func TestConcurrentAccess(t *testing.T) {
	// Reset global manager with proper initialization
	globalManager = &GlobalEpochManager{}
	globalManager.currentEpoch.Store(1) // Start at 1 to avoid epoch 0 issues

	const numGoroutines = 100
	const numOperations = 100

	var wg sync.WaitGroup
	var cleanupCount atomic.Int32

	// Start multiple goroutines doing epoch operations
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := range numOperations {
				epoch := EnterEpoch()

				// Schedule cleanup
				ScheduleCleanup(func() error {
					cleanupCount.Add(1)
					return nil
				})

				// Small delay to increase chance of interleaving
				time.Sleep(time.Microsecond)

				ExitEpoch(epoch)

				// Occasionally try cleanup
				if j%10 == 0 {
					TryCleanup()
				}
			}
		}(i)
	}

	wg.Wait()

	// Capture state before cleanup attempts
	activeEpochs, pendingCleanups := globalManager.GetStats()
	readerCounts := GetEpochReaderCounts()
	t.Logf("After goroutines: activeEpochs=%d, pendingCleanups=%d, readerCounts=%v",
		activeEpochs, pendingCleanups, readerCounts)

	// Advance epoch multiple times to make all scheduled cleanups eligible
	for i := range 5 {
		advanceEpoch()
		cleaned := TryCleanup()
		t.Logf("Cleanup round %d: cleaned %d items", i, cleaned)
	}

	// Final cleanup
	finalCleaned := TryCleanup()
	totalExpected := numGoroutines * numOperations
	actualCleaned := int(cleanupCount.Load())

	if actualCleaned != totalExpected {
		// Get detailed state on failure
		activeEpochs, pendingCleanups = globalManager.GetStats()
		readerCounts = GetEpochReaderCounts()
		detailedInfo := GetDetailedEpochInfo()

		t.Errorf("Expected %d total cleanups, got %d", totalExpected, actualCleaned)
		t.Logf("Final state: activeEpochs=%d, pendingCleanups=%d", activeEpochs, pendingCleanups)
		t.Logf("Reader counts: %v", readerCounts)
		t.Logf("Detailed info: %+v", detailedInfo)
	}

	t.Logf("Final cleanup round cleaned %d items", finalCleaned)
}

func TestGetStats(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	// Initially should have no stats
	activeEpochs, pendingCleanups := globalManager.GetStats()
	if activeEpochs != 0 || pendingCleanups != 0 {
		t.Errorf("Expected 0 active epochs and 0 pending cleanups, got %d and %d",
			activeEpochs, pendingCleanups)
	}

	// Enter an epoch and schedule cleanup
	epoch := EnterEpoch()
	ScheduleCleanup(func() error { return nil })
	ScheduleCleanup(func() error { return nil })

	activeEpochs, pendingCleanups = globalManager.GetStats()
	if activeEpochs != 1 {
		t.Errorf("Expected 1 active epoch, got %d", activeEpochs)
	}
	if pendingCleanups != 2 {
		t.Errorf("Expected 2 pending cleanups, got %d", pendingCleanups)
	}

	// Exit epoch and cleanup
	ExitEpoch(epoch)
	advanceEpoch() // Make epoch eligible for cleanup
	TryCleanup()

	activeEpochs, pendingCleanups = globalManager.GetStats()
	if activeEpochs != 0 || pendingCleanups != 0 {
		t.Errorf("Expected 0 active epochs and 0 pending cleanups after cleanup, got %d and %d",
			activeEpochs, pendingCleanups)
	}
}

func TestAdvanceEpoch(t *testing.T) {
	// Reset global manager
	globalManager = &GlobalEpochManager{}

	if globalManager.GetCurrentEpoch() != 0 {
		t.Errorf("Expected initial epoch 0, got %d", globalManager.GetCurrentEpoch())
	}

	newEpoch := AdvanceEpoch()
	if newEpoch != 1 {
		t.Errorf("Expected epoch 1 after advance, got %d", newEpoch)
	}

	if globalManager.GetCurrentEpoch() != 1 {
		t.Errorf("Expected current epoch 1, got %d", globalManager.GetCurrentEpoch())
	}
}
