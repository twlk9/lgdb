package lgdb

import (
	"errors"

	"github.com/twlk9/lgdb/keys"
)

// Error definitions for the database.
// Standard Go practice - define all your errors in one place so they're easy to find.
var (
	// ErrNotFound is returned when a key is not found
	ErrNotFound = errors.New("key not found")

	// ErrDBClosed is returned when operating on a closed database
	ErrDBClosed = errors.New("database is closed")

	// ErrDBAlreadyOpen is returned when attempting to open a database that is already locked by another process
	ErrDBAlreadyOpen = errors.New("database is already open by another process")

	// ErrReadOnly is returned when attempting to write to a read-only database
	ErrReadOnly = errors.New("database is read-only")

	// ErrClosed is returned when operating on a closed resource
	ErrClosed = errors.New("resource is closed")

	// ErrTooManyOpenFiles is returned when the file cache is full with active entries
	ErrTooManyOpenFiles = errors.New("too many open files")

	// ErrInvalidKey is returned when a key is invalid
	ErrInvalidKey = errors.New("invalid key")

	// ErrInvalidValue is returned when a value is invalid
	ErrInvalidValue = errors.New("invalid value")

	// ErrCorruption is returned when data corruption is detected
	ErrCorruption = keys.ErrCorruption

	// ErrIOError is returned when an I/O error occurs
	ErrIOError = errors.New("I/O error")

	// ErrNotSupported is returned when an operation is not supported
	ErrNotSupported = errors.New("operation not supported")

	// ErrInvalidRange is returned when a range is invalid
	ErrInvalidRange = errors.New("invalid range")

	// ErrNotImplemented is returned when a function is not yet implemented
	ErrNotImplemented = errors.New("not implemented")

	// ErrCompactionTimeout is returned when L0 backpressure times out waiting for compaction
	ErrCompactionTimeout = errors.New("compaction timeout")

	// Configuration validation errors
	ErrInvalidPath                    = errors.New("invalid database path")
	ErrInvalidWriteBufferSize         = errors.New("invalid write buffer size")
	ErrInvalidMaxMemtables            = errors.New("invalid max memtables")
	ErrInvalidTargetFileSize          = errors.New("invalid target file size")
	ErrInvalidLevelSizeMultiplier     = errors.New("invalid level size multiplier")
	ErrInvalidLevelFileSizeMultiplier = errors.New("invalid level file size multiplier")
	ErrInvalidMaxLevels               = errors.New("invalid max levels")
	ErrInvalidL0CompactionTrigger     = errors.New("invalid L0 compaction trigger")
	ErrInvalidL0StopWritesTrigger     = errors.New("invalid L0 stop writes trigger")
	ErrInvalidMaxOpenFiles            = errors.New("invalid max open files")
	// FileCacheSize is now automatically calculated from MaxOpenFiles
	ErrInvalidBlockSize            = errors.New("invalid block size")
	ErrInvalidBlockRestartInterval = errors.New("invalid block restart interval")
)
