package compression

import "fmt"

// Type represents different compression algorithms
type Type uint8

const (
	// None stores blocks without compression
	None Type = iota

	// Snappy uses Snappy compression algorithm
	// Fast compression with reasonable compression ratios
	Snappy

	// Zstd uses Zstandard compression algorithm
	// Better compression ratios than Snappy, slightly slower
	Zstd

	// S2 uses S2 compression algorithm
	// Faster than Snappy with better compression ratios
	S2
)

// String returns the string representation of the compression type
func (t Type) String() string {
	switch t {
	case None:
		return "none"
	case Snappy:
		return "snappy"
	case Zstd:
		return "zstd"
	case S2:
		return "s2"
	default:
		return "unknown"
	}
}

// Config holds compression configuration
type Config struct {
	// Type of compression to use
	Type Type

	// MinReductionPercent is the minimum compression ratio required
	// to store a block compressed. If compression achieves less than
	// this percentage reduction, the block is stored uncompressed.
	// Default: 12% (blocks that don't compress by at least 12% are stored uncompressed)
	MinReductionPercent uint8

	// ZstdLevel specifies the Zstd compression level (only used when Type is Zstd)
	// Higher levels provide better compression but use more CPU
	// Valid levels: ZstdFastest (1), ZstdDefault (3), ZstdBetter (6), ZstdBest (9)
	ZstdLevel ZstdLevel
}

// DefaultConfig returns the default compression configuration
func DefaultConfig() Config {
	return Config{
		Type:                Snappy,      // Default to Snappy like LevelDB
		MinReductionPercent: 12,          // 12.5% minimum reduction
		ZstdLevel:           ZstdDefault, // Default Zstd level
	}
}

// SnappyConfig returns a configuration for Snappy compression
func SnappyConfig() Config {
	return Config{
		Type:                Snappy,
		MinReductionPercent: 12,
	}
}

// ZstdFastConfig returns a configuration for fast Zstd compression
func ZstdFastConfig() Config {
	return Config{
		Type:                Zstd,
		MinReductionPercent: 10,
		ZstdLevel:           ZstdFastest,
	}
}

// ZstdBalancedConfig returns a configuration for balanced Zstd compression
// This uses ZstdDefault level which is memory efficient (~5.47MB) compared to
// ZstdBest (~136MB). Recommended for most use cases.
func ZstdBalancedConfig() Config {
	return Config{
		Type:                Zstd,
		MinReductionPercent: 8,
		ZstdLevel:           ZstdDefault,
	}
}

// ZstdBestConfig returns a configuration for best Zstd compression
// WARNING: Uses significantly more memory (136MB vs 5.47MB for Default)
// Consider using ZstdBalancedConfig() for better memory efficiency
func ZstdBestConfig() Config {
	return Config{
		Type:                Zstd,
		MinReductionPercent: 5,
		ZstdLevel:           ZstdBest,
	}
}

// NoCompressionConfig returns a configuration with no compression
func NoCompressionConfig() Config {
	return Config{
		Type:                None,
		MinReductionPercent: 0,
	}
}

// S2DefaultConfig returns configuration for S2 compression with default settings
// S2 is faster than Snappy with better compression ratios
func S2DefaultConfig() Config {
	return Config{
		Type:                S2,
		MinReductionPercent: 12, // Same threshold as Snappy
	}
}

// S2BetterConfig returns configuration for S2 "better" compression mode
// Trades some speed for improved compression ratio
func S2BetterConfig() Config {
	return Config{
		Type:                S2,
		MinReductionPercent: 10, // Slightly lower threshold for better mode
	}
}

// TieredCompressionConfig defines compression strategy across LSM levels
// This allows configuring "fast compression for hot data, strong compression for cold data"
// without exposing per-level complexity
type TieredCompressionConfig struct {
	// TopCompression is used for hot, frequently compacted levels (L0, L1, L2, etc.)
	// Recommend: fast compression like S2, Snappy, or None
	TopCompression Config

	// BottomCompression is used for cold, stable levels where most data lives
	// Recommend: strong compression like Zstd
	BottomCompression Config

	// TopLevelCount specifies how many levels from the top use TopCompression
	// Levels 0 to TopLevelCount-1 use TopCompression
	// Levels TopLevelCount and above use BottomCompression
	// Example: TopLevelCount=3 means L0,L1,L2 use Top, L3+ use Bottom
	TopLevelCount int
}

// GetConfigForLevel returns the appropriate compression config for a given level
func (tc TieredCompressionConfig) GetConfigForLevel(level int) Config {
	if level < tc.TopLevelCount {
		return tc.TopCompression
	}
	return tc.BottomCompression
}

// DefaultTieredConfig returns the recommended tiered compression setup
// Fast S2 compression on L0-L2, balanced Zstd on L3+
func DefaultTieredConfig() *TieredCompressionConfig {
	return &TieredCompressionConfig{
		TopCompression:    S2DefaultConfig(),
		BottomCompression: ZstdBalancedConfig(),
		TopLevelCount:     3,
	}
}

// UniformFastConfig uses fast compression on all levels
// Good for write-heavy workloads with plenty of disk space
func UniformFastConfig() *TieredCompressionConfig {
	return &TieredCompressionConfig{
		TopCompression:    S2DefaultConfig(),
		BottomCompression: S2DefaultConfig(),
		TopLevelCount:     0, // doesn't matter since they're identical
	}
}

// UniformBestConfig uses maximum compression on all levels
// Good for read-heavy workloads where space is critical
// WARNING: Much slower writes and higher CPU usage
func UniformBestConfig() *TieredCompressionConfig {
	return &TieredCompressionConfig{
		TopCompression:    ZstdBestConfig(),
		BottomCompression: ZstdBestConfig(),
		TopLevelCount:     0, // doesn't matter since they're identical
	}
}

// AggressiveTieredConfig uses no compression on top levels, best compression on bottom
// Maximizes write speed while achieving excellent space efficiency
func AggressiveTieredConfig() *TieredCompressionConfig {
	return &TieredCompressionConfig{
		TopCompression:    NoCompressionConfig(),
		BottomCompression: ZstdBestConfig(),
		TopLevelCount:     3,
	}
}

// Compressor interface defines compression operations
type Compressor interface {
	// Compress compresses src into dst and returns the compressed data
	// Returns the compressed data and whether compression was applied
	Compress(dst, src []byte) ([]byte, bool, error)

	// Decompress decompresses src into dst and returns the decompressed data
	Decompress(dst, src []byte) ([]byte, error)

	// Type returns the compression type
	Type() Type
}

// NewCompressor creates a new compressor based on the configuration
func NewCompressor(config Config) (Compressor, error) {
	switch config.Type {
	case None:
		return &noneCompressor{}, nil
	case Snappy:
		return NewSnappyCompressor(config.MinReductionPercent), nil
	case Zstd:
		return NewZstdCompressor(config.MinReductionPercent, config.ZstdLevel), nil
	case S2:
		return NewS2Compressor(config.MinReductionPercent), nil
	default:
		return nil, fmt.Errorf("unknown compression type: %d", config.Type)
	}
}

// noneCompressor implements no compression
type noneCompressor struct{}

func (c *noneCompressor) Compress(dst, src []byte) ([]byte, bool, error) {
	// Ensure dst has enough capacity
	if cap(dst) < len(src) {
		dst = make([]byte, len(src))
	} else {
		dst = dst[:len(src)]
	}
	copy(dst, src)
	return dst, false, nil
}

func (c *noneCompressor) Decompress(dst, src []byte) ([]byte, error) {
	// Ensure dst has enough capacity
	if cap(dst) < len(src) {
		dst = make([]byte, len(src))
	} else {
		dst = dst[:len(src)]
	}
	copy(dst, src)
	return dst, nil
}

func (c *noneCompressor) Type() Type {
	return None
}

// Block compression types for SSTable format
const (
	// Block trailer compression type indicators
	BlockNone   = 0
	BlockSnappy = 1
	BlockZstd   = 2
	BlockS2     = 3
)

// CompressBlock compresses a block of data using the specified compressor
// Returns the compressed data and compression type for the block trailer
// Small blocks (< 1KB) are not compressed to avoid encoder overhead
func CompressBlock(compressor Compressor, dst, src []byte) ([]byte, uint8, error) {
	// Skip compression for very small blocks to avoid encoder overhead
	const minCompressionSize = 1024 // 1KB threshold
	if len(src) < minCompressionSize {
		// Copy data directly without compression
		if cap(dst) < len(src) {
			dst = make([]byte, len(src))
		} else {
			dst = dst[:len(src)]
		}
		copy(dst, src)
		return dst, BlockNone, nil
	}

	compressed, wasCompressed, err := compressor.Compress(dst, src)
	if err != nil {
		return nil, 0, err
	}

	if !wasCompressed {
		return compressed, BlockNone, nil
	}

	switch compressor.Type() {
	case Snappy:
		return compressed, BlockSnappy, nil
	case Zstd:
		return compressed, BlockZstd, nil
	case S2:
		return compressed, BlockS2, nil
	default:
		return compressed, BlockNone, nil
	}
}

// DecompressBlock decompresses a block of data based on compression type
func DecompressBlock(dst, src []byte, compressionType uint8) ([]byte, error) {
	switch compressionType {
	case BlockNone:
		// No compression, copy directly
		if cap(dst) < len(src) {
			dst = make([]byte, len(src))
		} else {
			dst = dst[:len(src)]
		}
		copy(dst, src)
		return dst, nil

	case BlockSnappy:
		// Snappy compression
		return DecompressSnappy(dst, src)

	case BlockZstd:
		// Zstd compression
		return DecompressZstd(dst, src)

	case BlockS2:
		// S2 compression
		return DecompressS2(dst, src)

	default:
		return nil, fmt.Errorf("unknown compression type: %d", compressionType)
	}
}
