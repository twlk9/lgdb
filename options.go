package lgdb

import (
	"log/slog"
	"os"
	"time"

	"github.com/twlk9/lgdb/compression"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
)

// Default values are your best friends. They're mostly borrowed from LevelDB and
// are battle-tested for a wide range of workloads. Tweak them when you know why you need to.
var (
	DefaultWriteBufferSize                    = 4 * MiB
	DefaultMaxMemtables                       = 2
	DefaultLevelSizeMultiplier                = 10.0
	DefaultLevelFileSizeMultiplier            = 2.0
	DefaultMaxLevels                          = 7
	DefaultL0CompactionTrigger                = 4
	DefaultL0StopWritesTrigger                = 12
	DefaultMaxOpenFiles                       = 1000
	DefaultBlockSize                          = 4 * KiB
	DefaultBlockCacheSize               int64 = 8 * MiB
	DefaultBlockRestartInterval               = 16
	DefaultBlockMinEntries                    = 4
	DefaultMaxManifestFileSize          int64 = 256 * MiB
	DefaultWALMinSyncInterval                 = 500 * time.Microsecond
	DefaultCompactionOverlapThreshold         = 4.0
	DefaultCompactionMaxExpansionFiles        = 20
	DefaultCompactionTargetOverlapRatio       = 2.0
	DefaultRangeDeleteCompactionEnabled       = true

	// A few file descriptors are precious and reserved for the WAL, manifest, etc.
	NumReservedFiles = 10
	// We always want at least a small file cache, even if MaxOpenFiles is set low.
	MinFileCacheSize = 64
)

// Options holds all the knobs and dials for tuning the database.
// Think of this as the control panel.
type Options struct {
	// Path is simply where the database files live on disk.
	Path string

	// WriteBufferSize is how big a memtable gets in memory before we flush it to a file.
	// This is your main knob for controlling memory usage for writes.
	// Larger -> better throughput for bulk writes, but more RAM used and longer recovery time.
	// Default: 4MB
	WriteBufferSize int

	// MaxMemtables is how many memtables we can have in memory at once.
	// You'll almost always want 2: one for active writes, and one being flushed to disk.
	// More can smooth out write stalls but uses more RAM.
	// Default: 2
	MaxMemtables int

	// LevelFileSizeMultiplier controls how much bigger SSTable files get at deeper levels.
	// A value of 2.0 means L2 files are twice as big as L1 files.
	// Bigger files are more efficient to scan but mean bigger compactions.
	// Default: 2.0
	LevelFileSizeMultiplier float64

	// LevelSizeMultiplier is the heart of the LSM tree's shape.
	// It dictates the total size of each level, with L(n+1) being N times larger than L(n).
	// This exponential growth is why reads are fast—most data is in the last, largest levels.
	// Default: 10.0 (L1 is ~40MB, L2 is ~400MB, etc.)
	LevelSizeMultiplier float64

	// MaxLevels is the maximum number of levels in the LSM tree.
	// Default: 7 (L0 through L6)
	MaxLevels int

	// L0CompactionTrigger is the "workhorse" trigger.
	// Once we have this many files in L0, we kick off a compaction to merge them into L1.
	// Default: 4
	L0CompactionTrigger int

	// L0StopWritesTrigger is the "red alert" trigger.
	// If L0 gets this backed up, we apply backpressure and pause incoming writes.
	// This gives compaction a chance to catch up and prevents a death spiral of write amplification.
	// Default: 12
	L0StopWritesTrigger int

	// MaxOpenFiles is a soft limit on file descriptors, a classic `ulimit -n` consideration.
	// We use this to size our SSTable file cache, leaving a few FDs for other needs.
	// Default: 1000
	MaxOpenFiles int

	// BlockSize is the size of a single data block within an SSTable.
	// Once written, blocks are the immutable unit of I/O. They're what get loaded into the block cache.
	// Default: 4KB
	BlockSize int

	// BlockCacheSize is the total capacity of the block cache in bytes.
	// This is a big one for read performance. More cache = fewer disk reads.
	// Default: 8MB
	BlockCacheSize int64

	// BlockRestartInterval determines how often we store a "restart point" within a block.
	// These points let us binary search for a key without decompressing the whole block.
	// Smaller interval -> faster seeks, but more memory overhead per block.
	// Default: 16
	BlockRestartInterval int

	// BlockMinEntries prevents creating a block with just a few keys, which can be inefficient.
	// Default: 4
	BlockMinEntries int

	// MaxManifestFileSize is how large the MANIFEST file can get before we rotate it.
	// Default: 256MB
	MaxManifestFileSize int64

	// CreateIfMissing will create a new database if one doesn't already exist at Path.
	CreateIfMissing bool
	// ErrorIfExists will cause Open() to fail if a database already exists.
	ErrorIfExists bool

	// Sync determines if writes should be immediately flushed to disk.
	// `true` is safer but slower. `false` is fast but risks data loss on crash.
	// Default: true (safety first!)
	Sync bool

	// WALSyncInterval is the interval between periodic WAL syncs. Not currently used.
	WALSyncInterval time.Duration

	// WALMinSyncInterval is the shortest time we'll wait between WAL syncs.
	// If writes come in super fast, this lets us batch them into a single `fsync`,
	// trading a tiny bit of latency for much better throughput.
	// Default: 500µs
	WALMinSyncInterval time.Duration

	// ReadOnly mode, as the name implies, prevents all write operations.
	ReadOnly bool

	// DisableWAL turns off the write-ahead log. Great for bulk loading data where you
	// can tolerate losing data on a crash, but a terrible idea for normal operation.
	DisableWAL bool

	// WALBytesPerSync triggers a background WAL sync after this many bytes have been written.
	// This helps smooth out I/O by preventing the OS from buffering too much and then
	// dumping it all to disk at once, causing a latency spike.
	// Default: 0 (disabled)
	WALBytesPerSync int

	// TieredCompression defines per-level compression strategies. The idea is to use
	// faster compression (like S2) on hot, upper levels and stronger compression
	// (like Zstd) on cold, lower levels.
	//
	// - TopLevelCount: How many levels (from L0 up) use the "hot" TopCompression.
	// - The rest of the levels use the "cold" BottomCompression.
	TieredCompression *compression.TieredCompressionConfig

	// These are knobs for our "smart" compaction picker. The goal is to avoid "write amplification"
	// where compacting 1MB of data causes 50MB of data to be rewritten. These settings help the
	// compactor decide whether to pick a bigger set of files to compact at once to be more
	// efficient in the long run.

	// CompactionOverlapThreshold: If the next level's overlapping files are N times larger
	// than the files we're compacting, we expand our selection to include them.
	// Default: 4.0 (expand if overlaps are > 4x the input size)
	CompactionOverlapThreshold float64

	// CompactionMaxExpansionFiles: A safety valve to prevent the compactor from picking
	// a ridiculously large set of files to compact.
	// Default: 20 files
	CompactionMaxExpansionFiles int

	// CompactionTargetOverlapRatio: When we do expand, we keep adding files until the
	// overlap ratio drops to this target.
	// Default: 2.0 (aim for overlaps to be at most 2x the input size)
	CompactionTargetOverlapRatio float64

	// RangeDeleteCompactionEnabled controls whether we run special compactions just to
	// clean up data deleted by a `DeleteRange` call. This helps reclaim space faster
	// but adds a bit more background work.
	// Default: true
	RangeDeleteCompactionEnabled bool

	// Logger is the structured logger for database events.
	Logger *slog.Logger
}

// DefaultOptions returns a new Options struct with sensible defaults.
// When in doubt, start with these.
func DefaultOptions() *Options {
	opts := &Options{
		WriteBufferSize:              DefaultWriteBufferSize,
		MaxMemtables:                 DefaultMaxMemtables,
		LevelSizeMultiplier:          DefaultLevelSizeMultiplier,
		LevelFileSizeMultiplier:      DefaultLevelFileSizeMultiplier,
		MaxLevels:                    DefaultMaxLevels,
		L0CompactionTrigger:          DefaultL0CompactionTrigger,
		L0StopWritesTrigger:          DefaultL0StopWritesTrigger,
		MaxOpenFiles:                 DefaultMaxOpenFiles,
		BlockSize:                    DefaultBlockSize,
		BlockCacheSize:               DefaultBlockCacheSize,
		BlockRestartInterval:         DefaultBlockRestartInterval,
		BlockMinEntries:              DefaultBlockMinEntries,
		MaxManifestFileSize:          DefaultMaxManifestFileSize,
		CreateIfMissing:              true,
		ErrorIfExists:                false,
		Sync:                         true, // Safety first!
		WALMinSyncInterval:           DefaultWALMinSyncInterval,
		ReadOnly:                     false,
		DisableWAL:                   false,
		WALBytesPerSync:              0, // Disabled by default
		TieredCompression:            compression.DefaultTieredConfig(),
		CompactionOverlapThreshold:   DefaultCompactionOverlapThreshold,
		CompactionMaxExpansionFiles:  DefaultCompactionMaxExpansionFiles,
		CompactionTargetOverlapRatio: DefaultCompactionTargetOverlapRatio,
		RangeDeleteCompactionEnabled: DefaultRangeDeleteCompactionEnabled,
		Logger:                       DefaultLogger(),
	}

	return opts
}

// GetLevelMaxBytes returns the maximum total size for a given level.
// This is the target we compact down to.
// L0 is special and managed by file count, not size.
func (o *Options) GetLevelMaxBytes(level int) int64 {
	if level == 0 {
		// L0's size is unbounded; it's controlled by the number of files.
		return 0
	}

	if level < 1 || level > o.MaxLevels {
		return 0
	}

	// L1 starts with a base size, typically 10x the target file size.
	// e.g., 4MB L1 files * 10 = 40MB total size for L1.
	baseSize := o.TargetFileSize(1) * 10
	if level == 1 {
		return baseSize
	}

	// Each subsequent level grows by the multiplier.
	multiplier := 1.0
	for i := 2; i <= level; i++ {
		multiplier *= o.LevelSizeMultiplier
	}

	return int64(float64(baseSize) * multiplier)
}

// GetMaxFilesForLevel returns the maximum number of files for a given level.
// Calculated as LevelMaxBytes / TargetFileSize(level).
func (o *Options) GetMaxFilesForLevel(level int) int {
	if level == 0 {
		// L0 is triggered by file count directly.
		return o.L0CompactionTrigger
	}

	maxBytes := o.GetLevelMaxBytes(level)
	if maxBytes == 0 {
		return 0
	}

	targetSize := o.TargetFileSize(level)
	if targetSize == 0 {
		return 0
	}

	return int(maxBytes / targetSize)
}

// Validate checks if the options make sense.
// Prevents you from shooting yourself in the foot with weird configurations.
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

	return nil
}

// Clone creates a deep copy of the options.
func (o *Options) Clone() *Options {
	if o == nil {
		return DefaultOptions()
	}

	clone := *o
	return &clone
}

// FileCacheSize calculates the appropriate file cache size based on max open files.
// It reserves some file descriptors for non-SSTable files (WAL, manifest, etc.).
func FileCacheSize(maxOpenFiles int) int {
	fileCacheSize := max(maxOpenFiles-NumReservedFiles, MinFileCacheSize)
	return fileCacheSize
}

// GetFileCacheSize returns the calculated file cache size for these options.
func (o *Options) GetFileCacheSize() int {
	return FileCacheSize(o.MaxOpenFiles)
}

// TargetFileSize returns the target file size for the specified level.
// L0 files are the size of a flushed memtable. Higher levels grow from there.
func (o *Options) TargetFileSize(level int) int64 {
	if level < 0 {
		return int64(o.WriteBufferSize)
	}

	// L0 files are always the size of the memtable dump.
	if level == 0 {
		return int64(o.WriteBufferSize)
	}

	// Higher levels grow exponentially.
	size := float64(o.WriteBufferSize)
	for range level {
		size *= o.LevelFileSizeMultiplier
	}
	return int64(size)
}

// GetCompressionForLevel returns the appropriate compression config for a given level.
func (o *Options) GetCompressionForLevel(level int) compression.Config {
	if o.TieredCompression != nil {
		return o.TieredCompression.GetConfigForLevel(level)
	}
	// Fallback to a sensible default if not configured.
	return compression.S2DefaultConfig()
}

// A few helper functions to get a logger instance.
func getLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
func DefaultLogger() *slog.Logger {
	return getLogger(slog.LevelWarn)
}

func DebugLogger() *slog.Logger {
	return getLogger(slog.LevelDebug)
}
