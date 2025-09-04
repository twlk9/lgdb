package lgdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// StateValidator provides comprehensive database state validation
type StateValidator struct {
	db        *DB
	t         *testing.T
	knownData map[string][]byte // Track all data that should exist
	mu        sync.RWMutex
}

// NewStateValidator creates a new state validator for a database
func NewStateValidator(t *testing.T, db *DB) *StateValidator {
	return &StateValidator{
		db:        db,
		t:         t,
		knownData: make(map[string][]byte),
	}
}

// TrackPut records a put operation for later validation
func (sv *StateValidator) TrackPut(key, value []byte) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	sv.knownData[string(key)] = bytes.Clone(value)
}

// TrackDelete records a delete operation for later validation
func (sv *StateValidator) TrackDelete(key []byte) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	delete(sv.knownData, string(key))
}

// ValidateConsistency performs comprehensive consistency checks
func (sv *StateValidator) ValidateConsistency() {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	sv.t.Helper()

	// Check 1: All tracked data should be retrievable
	sv.validateAllDataPresent()

	// Check 2: Cross-level consistency during compaction
	sv.validateCrossLevelConsistency()

	// Check 3: Iterator consistency
	sv.validateIteratorConsistency()

	// Check 4: No phantom data (data that shouldn't exist)
	sv.validateNoPhantomData()
}

// validateAllDataPresent ensures all tracked data is still in the database
func (sv *StateValidator) validateAllDataPresent() {
	for keyStr, expectedValue := range sv.knownData {
		key := []byte(keyStr)
		actualValue, err := sv.db.Get(key)
		if err != nil {
			sv.t.Errorf("Key %q should exist but got error: %v", keyStr, err)
			continue
		}
		if !bytes.Equal(expectedValue, actualValue) {
			sv.t.Errorf("Key %q: expected %q, got %q", keyStr, expectedValue, actualValue)
		}
	}
}

// validateCrossLevelConsistency checks data consistency across LSM levels
func (sv *StateValidator) validateCrossLevelConsistency() {
	// This is a simplified check - in a real implementation you'd want to
	// access internal structures to verify data across memtable, L0, L1+

	// Force a compaction if needed and verify data survives
	sv.db.mu.RLock()
	memtableSize := sv.db.memtable.Size()
	sv.db.mu.RUnlock()

	if memtableSize > sv.db.options.WriteBufferSize/2 {
		// Trigger compaction by writing more data
		tempKey := []byte("__temp_consistency_check__")
		tempValue := make([]byte, 1000)
		sv.db.Put(tempKey, tempValue)
		sv.db.Delete(tempKey) // Clean up

		// Wait a bit for potential compaction
		time.Sleep(10 * time.Millisecond)

		// Verify all data still exists after potential compaction
		sv.validateAllDataPresent()
	}
}

// validateIteratorConsistency ensures iterators see consistent snapshots
func (sv *StateValidator) validateIteratorConsistency() {
	// Create an iterator and verify it sees all expected data
	iter := sv.db.NewIterator()
	defer iter.Close()

	// Build expected sorted key list
	var expectedKeys []string
	for key := range sv.knownData {
		expectedKeys = append(expectedKeys, key)
	}
	sort.Strings(expectedKeys)

	// Iterate and compare
	var seenKeys []string
	iter.SeekToFirst()
	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		keyStr := string(key)
		seenKeys = append(seenKeys, keyStr)

		if expectedValue, exists := sv.knownData[keyStr]; exists {
			if !bytes.Equal(expectedValue, value) {
				sv.t.Errorf("Iterator key %q: expected %q, got %q", keyStr, expectedValue, value)
			}
		} else {
			sv.t.Errorf("Iterator returned unexpected key %q", keyStr)
		}

		iter.Next()
	}

	// Check for missing keys
	for _, expectedKey := range expectedKeys {
		found := false
		for _, seenKey := range seenKeys {
			if expectedKey == seenKey {
				found = true
				break
			}
		}
		if !found {
			sv.t.Errorf("Iterator missing expected key %q", expectedKey)
		}
	}
}

// validateNoPhantomData ensures no unexpected data exists
func (sv *StateValidator) validateNoPhantomData() {
	iter := sv.db.NewIterator()
	defer iter.Close()

	iter.SeekToFirst()
	for iter.Valid() {
		key := iter.Key()
		keyStr := string(key)

		if _, exists := sv.knownData[keyStr]; !exists {
			sv.t.Errorf("Found phantom key %q that should not exist", keyStr)
		}

		iter.Next()
	}
}

// RandomDataGenerator provides deterministic random data generation
type RandomDataGenerator struct {
	rng  *rand.Rand
	seed int64
}

// NewRandomDataGenerator creates a new deterministic random data generator
func NewRandomDataGenerator(seed int64) *RandomDataGenerator {
	return &RandomDataGenerator{
		rng:  rand.New(rand.NewSource(seed)),
		seed: seed,
	}
}

// GenerateKeyValuePairs creates deterministic key/value pairs
func (rdg *RandomDataGenerator) GenerateKeyValuePairs(count int, keySize, valueSize int) []KeyValuePair {
	pairs := make([]KeyValuePair, count)

	for i := 0; i < count; i++ {
		// Generate deterministic keys with padding for sorting
		key := fmt.Sprintf("key_%08d_%s", i, rdg.randomString(keySize-16))
		value := rdg.randomString(valueSize)

		pairs[i] = KeyValuePair{
			Key:   []byte(key),
			Value: []byte(value),
		}
	}

	return pairs
}

// GenerateRandomOperations creates a sequence of random database operations
func (rdg *RandomDataGenerator) GenerateRandomOperations(existingKeys [][]byte, numOps int) []Operation {
	ops := make([]Operation, numOps)

	for i := 0; i < numOps; i++ {
		opType := rdg.rng.Intn(3) // 0=Put, 1=Get, 2=Delete

		switch opType {
		case 0: // Put
			key := fmt.Sprintf("rand_key_%d", rdg.rng.Intn(1000))
			value := rdg.randomString(rdg.rng.Intn(100) + 10)
			ops[i] = Operation{Type: OpPut, Key: []byte(key), Value: []byte(value)}

		case 1: // Get
			var key []byte
			if len(existingKeys) > 0 && rdg.rng.Float32() < 0.7 {
				// 70% chance to get an existing key
				key = existingKeys[rdg.rng.Intn(len(existingKeys))]
			} else {
				// 30% chance to get a random (possibly non-existent) key
				key = []byte(fmt.Sprintf("rand_key_%d", rdg.rng.Intn(1000)))
			}
			ops[i] = Operation{Type: OpGet, Key: key}

		case 2: // Delete
			var key []byte
			if len(existingKeys) > 0 && rdg.rng.Float32() < 0.8 {
				// 80% chance to delete an existing key
				key = existingKeys[rdg.rng.Intn(len(existingKeys))]
			} else {
				// 20% chance to delete a random (possibly non-existent) key
				key = []byte(fmt.Sprintf("rand_key_%d", rdg.rng.Intn(1000)))
			}
			ops[i] = Operation{Type: OpDelete, Key: key}
		}
	}

	return ops
}

func (rdg *RandomDataGenerator) randomString(length int) string {
	if length <= 0 {
		return ""
	}
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rdg.rng.Intn(len(charset))]
	}
	return string(b)
}

// KeyValuePair represents a key-value pair for testing
type KeyValuePair struct {
	Key   []byte
	Value []byte
}

// Operation represents a database operation for property-based testing
type Operation struct {
	Type  OperationType
	Key   []byte
	Value []byte
}

type OperationType int

const (
	OpPut OperationType = iota
	OpGet
	OpDelete
)

// PropertyBasedTester runs property-based tests on a database
type PropertyBasedTester struct {
	db        *DB
	validator *StateValidator
	generator *RandomDataGenerator
	t         *testing.T
}

// NewPropertyBasedTester creates a new property-based tester
func NewPropertyBasedTester(t *testing.T, db *DB, seed int64) *PropertyBasedTester {
	return &PropertyBasedTester{
		db:        db,
		validator: NewStateValidator(t, db),
		generator: NewRandomDataGenerator(seed),
		t:         t,
	}
}

// RunRandomOperationSequence executes a sequence of random operations
// and validates consistency after each operation
func (pbt *PropertyBasedTester) RunRandomOperationSequence(numOperations int) {
	var existingKeys [][]byte

	for i := 0; i < numOperations; i++ {
		ops := pbt.generator.GenerateRandomOperations(existingKeys, 1)
		op := ops[0]

		switch op.Type {
		case OpPut:
			err := pbt.db.Put(op.Key, op.Value)
			if err != nil {
				pbt.t.Errorf("Put operation failed at step %d: %v", i, err)
				continue
			}
			pbt.validator.TrackPut(op.Key, op.Value)

			// Update existing keys list
			keyExists := false
			for _, existing := range existingKeys {
				if bytes.Equal(existing, op.Key) {
					keyExists = true
					break
				}
			}
			if !keyExists {
				existingKeys = append(existingKeys, bytes.Clone(op.Key))
			}

		case OpGet:
			_, err := pbt.db.Get(op.Key)
			// Get errors are expected for non-existent keys
			if err != nil && err != ErrNotFound {
				pbt.t.Errorf("Unexpected Get error at step %d: %v", i, err)
			}

		case OpDelete:
			err := pbt.db.Delete(op.Key)
			if err != nil {
				pbt.t.Errorf("Delete operation failed at step %d: %v", i, err)
				continue
			}
			pbt.validator.TrackDelete(op.Key)

			// Remove from existing keys list
			for j, existing := range existingKeys {
				if bytes.Equal(existing, op.Key) {
					existingKeys = append(existingKeys[:j], existingKeys[j+1:]...)
					break
				}
			}
		}

		// Validate consistency every 10 operations
		if i%10 == 0 {
			pbt.validator.ValidateConsistency()
		}
	}

	// Final consistency check
	pbt.validator.ValidateConsistency()
}
