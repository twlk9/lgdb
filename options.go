package lgdb

import (
	"log/slog"
	"os"
	"time"

	"github.com/twlk9/lgdb/compression"
)

// SelectionStrategy determines how files are selected for compaction
type SelectionStrategy int

const (
	// FileSelectionOldestSmallestSeqFirst selects the file with the oldest smallest sequence number.
	// This strategy prioritizes freeing up space by compacting older data first.
	// Best for read-heavy workloads where data locality is important.
	OldestSeq SelectionStrategy = iota

	// FileSelectionMinOverlappingRatio selects the file with the smallest overlap ratio with the next level.
	// This strategy minimizes write amplification by choosing files that overlap least with next level.
	// Based on RocksDB's kMinOverlappingRatio heuristic, following Pebble's implementation.
	// Best for write-heavy workloads where minimizing write amplification is critical.
	MinOverlap
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
)

// Default values following LevelDB conventions
var (
	DefaultWriteBufferSize               = 4 * MiB
	DefaultMaxMemtables                  = 2
	DefaultLevelSizeMultiplier           = 10.0
	DefaultLevelFileSizeMultiplier       = 2.0 // Files grow 2x per level (L0->L1->L2->...)
	DefaultMaxLevels                     = 7
	DefaultL0CompactionTrigger           = 4
	DefaultL0StopWritesTrigger           = 12
	DefaultMaxOpenFiles                  = 1000 // Soft limit on file descriptors (used to calculate file cache size)
	DefaultBlockSize                     = 4 * KiB
	DefaultBlockCacheSize          int64 = 8 * MiB
	DefaultBlockRestartInterval          = 16
	DefaultBlockMinEntries               = 4
	DefaultMaxManifestFileSize     int64 = 256 * MiB
	DefaultWALSyncInterval               = 500 * time.Millisecond
	DefaultWALMinSyncInterval            = 500 * time.Microsecond

	// File descriptor management constants (following Pebble's approach)
	NumReservedFiles = 10 // Reserve file descriptors for WAL, manifest, temp files, etc.
	MinFileCacheSize = 64 // Minimum number of SSTable files to cache regardless of MaxOpenFiles
)

// Options holds configuration options for the database.
// Follows LevelDB's design philosophy of keeping things simple.
// Contains all tunable parameters for database behavior.
type Options struct {
	// Database path
	Path string

	// Write buffer size - size of memtable before flush to L0
	// LevelDB default: 4MB
	WriteBufferSize int

	// Maximum number of memtables (active + immutable)
	// LevelDB default: 2 (one active, one immutable)
	MaxMemtables int

	// File size multiplier between levels
	// Files at level N+1 will be this many times larger than level N
	// L0 files are always the size of a flushed memtable (WriteBufferSize)
	// L1 files = WriteBufferSize * LevelFileSizeMultiplier
	// L2 files = L1 size * LevelFileSizeMultiplier, etc.
	// Default: 2.0 (files double in size each level)
	LevelFileSizeMultiplier float64

	// Size multiplier between levels (for total level capacity)
	// Level N+1 will be this many times larger than Level N
	// LevelDB default: 10x
	LevelSizeMultiplier float64

	// Maximum number of levels in the LSM tree
	// LevelDB default: 7 levels (L0 through L6)
	MaxLevels int

	// Number of L0 files that trigger compaction
	// LevelDB default: 4 files
	L0CompactionTrigger int

	// Number of L0 files that stop writes (backpressure)
	// LevelDB default: 12 files
	L0StopWritesTrigger int

	// Maximum number of open file descriptors
	// Used to calculate the SSTable file cache size by reserving
	// NumReservedFiles for non-SSTable files (WAL, manifest, etc.)
	// The actual file cache size will be (MaxOpenFiles - NumReservedFiles)
	// LevelDB default: 1000
	MaxOpenFiles int

	// Block size for SSTable blocks
	// LevelDB default: 4KB
	BlockSize int

	// BlockCacheSize is the total capacity of the block cache in bytes.
	// LevelDB default: 8MB
	BlockCacheSize int64

	// Number of keys between restart points in blocks
	// LevelDB default: 16
	BlockRestartInterval int

	// Minimum number of entries per block. To avoid having a block
	// with a single entry under certain circumstances.
	BlockMinEntries int

	// Maximum size of manifest file before rotation
	// LevelDB default: 64MB
	MaxManifestFileSize int64

	// Database creation/existence options
	CreateIfMissing bool
	ErrorIfExists   bool

	// Sync options
	Sync bool // Sync writes to disk immediately

	// WAL sync timing options (following pebble design)
	// WALSyncInterval is the interval between periodic WAL syncs
	// If 0, syncs happen immediately after writes (when Sync=true)
	// Default: 0 (immediate sync)
	WALSyncInterval time.Duration

	// WALMinSyncInterval is the minimum duration between WAL syncs
	// If WAL syncs are requested faster than this interval, they will be
	// artificially delayed. This allows batching more operations and
	// reducing I/O while having minimal impact on throughput.
	// Default: 0 (no artificial delay)
	WALMinSyncInterval time.Duration

	// Read-only mode
	ReadOnly bool

	// Disable Write-Ahead Log (for testing/special cases)
	DisableWAL bool

	// WALBytesPerSync sets the number of bytes to write to a WAL before calling
	// Sync on it in the background. This helps smooth out disk write latencies
	// and avoids cases where the OS writes a lot of buffered data to disk at once.
	// Set to 0 to disable background syncing (default behavior).
	// Following Pebble's approach for better write latency predictability.
	WALBytesPerSync int

	// TieredCompression defines per-level compression strategies.
	// Different levels can use different compression algorithms and settings.
	//
	// TopLevelCount controls how many levels from the top use TopCompression:
	// - Levels 0 to TopLevelCount-1 use TopCompression (hot data, frequently compacted)
	// - Levels TopLevelCount and above use BottomCompression (cold data, stable)
	// - Set TopLevelCount=0 for uniform compression across all levels
	//
	// Common configurations:
	// - DefaultTieredConfig(): Fast S2 on L0-L2, balanced Zstd on L3+ (recommended)
	// - UniformFastConfig(): Fast S2 on all levels (write-heavy workloads)
	// - UniformBestConfig(): Best Zstd on all levels (read-heavy/space-critical)
	// - AggressiveTieredConfig(): No compression on L0-L2, best Zstd on L3+
	TieredCompression *compression.TieredCompressionConfig

	// File selection strategy for compaction
	// Controls how files are chosen for compaction within a level
	SelectionStrategy SelectionStrategy

	// Structured logger
	Logger *slog.Logger
}

// DefaultOptions returns a new Options struct with sensible defaults
// following LevelDB conventions.
// These are battle-tested values that work well for most use cases.
func DefaultOptions() *Options {
	opts := &Options{
		WriteBufferSize:         DefaultWriteBufferSize,
		MaxMemtables:            DefaultMaxMemtables,
		LevelSizeMultiplier:     DefaultLevelSizeMultiplier,
		LevelFileSizeMultiplier: DefaultLevelFileSizeMultiplier,
		MaxLevels:               DefaultMaxLevels,
		L0CompactionTrigger:     DefaultL0CompactionTrigger,
		L0StopWritesTrigger:     DefaultL0StopWritesTrigger,
		MaxOpenFiles:            DefaultMaxOpenFiles,
		BlockSize:               DefaultBlockSize,
		BlockCacheSize:          DefaultBlockCacheSize,
		BlockRestartInterval:    DefaultBlockRestartInterval,
		BlockMinEntries:         DefaultBlockMinEntries,
		MaxManifestFileSize:     DefaultMaxManifestFileSize,
		CreateIfMissing:         true,
		ErrorIfExists:           false,
		Sync:                    true, // Change to true for safety by default
		WALSyncInterval:         DefaultWALSyncInterval,
		WALMinSyncInterval:      DefaultWALMinSyncInterval,
		ReadOnly:                false,
		DisableWAL:              false,
		WALBytesPerSync:         0, // Disabled by default
		TieredCompression:       compression.DefaultTieredConfig(),
		SelectionStrategy:       MinOverlap, // Default to write-optimized strategy
		Logger:                  DefaultLogger(),
	}

	// File sizes are now calculated dynamically based on WriteBufferSize and LevelFileSizeMultiplier
	// L0 files = WriteBufferSize (size of flushed memtable)
	// L1 files = WriteBufferSize * LevelFileSizeMultiplier
	// L2 files = L1 size * LevelFileSizeMultiplier, etc.

	return opts
}

// GetLevelMaxBytes returns the maximum size in bytes for a given level.
// Level 0 is unlimited (managed by file count triggers).
// Level N size = TargetFileSize(level) * (LevelSizeMultiplier ^ (N-1))
// Creates the exponential growth that makes LSM trees work.
func (o *Options) GetLevelMaxBytes(level int) int64 {
	if level == 0 {
		// L0 is managed by file count, not size
		return 0
	}

	if level <= 0 || level > o.MaxLevels {
		return 0
	}

	// Level 1 starts with base size (target file size for L1 * 10)
	baseSize := o.TargetFileSize(1) * 10 // L1 = ~40MB with 4MB target file size
	if level == 1 {
		return baseSize
	}

	// Each subsequent level is multiplied by LevelSizeMultiplier
	multiplier := 1.0
	for i := 2; i <= level; i++ {
		multiplier *= o.LevelSizeMultiplier
	}

	return int64(float64(baseSize) * multiplier)
}

// GetMaxFilesForLevel returns the maximum number of files for a given level.
// Calculated as LevelMaxBytes / TargetFileSize(level).
// Used by compaction to decide when a level is "full".
func (o *Options) GetMaxFilesForLevel(level int) int {
	if level == 0 {
		// L0 uses file count triggers instead
		return o.L0CompactionTrigger
	}

	maxBytes := o.GetLevelMaxBytes(level)
	if maxBytes == 0 {
		return 0
	}

	return int(maxBytes / o.TargetFileSize(level))
}

// Validate checks if the options are valid and returns an error if not.
// Catches common configuration mistakes that would prevent database operation.
func (o *Options) Validate() error {
	if o.Path == "" {
		return ErrInvalidPath
	}

	if o.WriteBufferSize <= 0 {
		return ErrInvalidWriteBufferSize
	}

	if o.MaxMemtables <= 0 {
		return ErrInvalidMaxMemtables
	}

	if o.LevelFileSizeMultiplier <= 1.0 {
		return ErrInvalidLevelFileSizeMultiplier
	}

	if o.LevelSizeMultiplier <= 1.0 {
		return ErrInvalidLevelSizeMultiplier
	}

	if o.MaxLevels <= 0 || o.MaxLevels > 20 {
		return ErrInvalidMaxLevels
	}

	if o.L0CompactionTrigger <= 0 {
		return ErrInvalidL0CompactionTrigger
	}

	if o.L0StopWritesTrigger <= o.L0CompactionTrigger {
		return ErrInvalidL0StopWritesTrigger
	}

	if o.BlockSize <= 0 {
		return ErrInvalidBlockSize
	}

	if o.BlockRestartInterval <= 0 {
		return ErrInvalidBlockRestartInterval
	}

	if o.MaxOpenFiles <= 0 {
		return ErrInvalidMaxOpenFiles
	}

	// FileCacheSize is now automatically calculated from MaxOpenFiles
	// No separate validation needed

	return nil
}

// Clone creates a deep copy of the options.
// Useful when modifying options without affecting the original.
func (o *Options) Clone() *Options {
	if o == nil {
		return DefaultOptions()
	}

	clone := *o
	return &clone
}

// FileCacheSize calculates the appropriate file cache size based on max open files.
// Reserves NumReservedFiles for non-SSTable files (WAL, manifest, etc.) and ensures
// a minimum cache size. Following Pebble's approach for file descriptor management.
func FileCacheSize(maxOpenFiles int) int {
	fileCacheSize := max(maxOpenFiles-NumReservedFiles, MinFileCacheSize)
	return fileCacheSize
}

// GetFileCacheSize returns the calculated file cache size for these options.
// The number of SSTable files that will be cached based on MaxOpenFiles.
func (o *Options) GetFileCacheSize() int {
	return FileCacheSize(o.MaxOpenFiles)
}

// TargetFileSize returns the target file size for the specified level.
// L0 files are always the size of a flushed memtable (WriteBufferSize).
// Higher levels grow by LevelFileSizeMultiplier each level.
func (o *Options) TargetFileSize(level int) int64 {
	if level < 0 {
		// Default to L0 size for invalid levels
		return int64(o.WriteBufferSize)
	}

	// L0 files are always the size of the memtable
	if level == 0 {
		return int64(o.WriteBufferSize)
	}

	// Higher levels grow exponentially
	size := float64(o.WriteBufferSize)
	for range level {
		size *= o.LevelFileSizeMultiplier
	}
	return int64(size)
}

// GetCompressionForLevel returns the appropriate compression config for a given level.
// Uses TieredCompression configuration to determine which compression strategy to apply.
func (o *Options) GetCompressionForLevel(level int) compression.Config {
	if o.TieredCompression != nil {
		return o.TieredCompression.GetConfigForLevel(level)
	}
	// Fallback: should not happen with proper initialization
	return compression.S2DefaultConfig()
}

// Helpful Logger functions
func getLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
func DefaultLogger() *slog.Logger {
	return getLogger(slog.LevelWarn)
}

func DebugLogger() *slog.Logger {
	return getLogger(slog.LevelDebug)
}
