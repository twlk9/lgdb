package compression

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstdCompressor implements Zstd compression with configurable compression level
type zstdCompressor struct {
	minReductionPercent uint8
	level               zstd.EncoderLevel

	// Pool encoder and decoder instances for better performance
	encoderPool sync.Pool
	decoderPool sync.Pool
}

// ZstdLevel represents different Zstd compression levels
type ZstdLevel int

const (
	// ZstdFastest provides fastest compression with lower ratio
	ZstdFastest ZstdLevel = 1

	// ZstdDefault provides balanced compression speed and ratio
	ZstdDefault ZstdLevel = 3

	// ZstdBetter provides better compression ratio with more CPU usage
	ZstdBetter ZstdLevel = 6

	// ZstdBest provides best compression ratio with highest CPU usage
	ZstdBest ZstdLevel = 9
)

// NewZstdCompressor creates a new Zstd compressor with the specified level
func NewZstdCompressor(minReductionPercent uint8, level ZstdLevel) Compressor {
	var encoderLevel zstd.EncoderLevel
	switch level {
	case ZstdFastest:
		encoderLevel = zstd.SpeedFastest
	case ZstdDefault:
		encoderLevel = zstd.SpeedDefault
	case ZstdBetter:
		encoderLevel = zstd.SpeedBetterCompression
	case ZstdBest:
		encoderLevel = zstd.SpeedBestCompression
	default:
		encoderLevel = zstd.SpeedDefault
	}

	c := &zstdCompressor{
		minReductionPercent: minReductionPercent,
		level:               encoderLevel,
	}

	// Initialize encoder pool with memory optimizations
	c.encoderPool = sync.Pool{
		New: func() any {
			encoder, err := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(encoderLevel),
				zstd.WithLowerEncoderMem(true), // Enable low memory mode
				zstd.WithWindowSize(1<<20),     // 1MB window size (down from default 8MB)
			)
			if err != nil {
				// This should not happen with valid levels
				panic(fmt.Sprintf("failed to create zstd encoder: %v", err))
			}
			return encoder
		},
	}

	// Initialize decoder pool
	c.decoderPool = sync.Pool{
		New: func() any {
			decoder, err := zstd.NewReader(nil)
			if err != nil {
				// This should not happen
				panic(fmt.Sprintf("failed to create zstd decoder: %v", err))
			}
			return decoder
		},
	}

	return c
}

func (c *zstdCompressor) Compress(dst, src []byte) ([]byte, bool, error) {
	// Get encoder from pool
	encoder := c.encoderPool.Get().(*zstd.Encoder)
	defer c.encoderPool.Put(encoder)

	// Compress data
	compressed := encoder.EncodeAll(src, dst[:0])

	// Check if compression meets minimum reduction threshold
	if c.minReductionPercent > 0 {
		// Calculate reduction percentage (positive means compression, negative means expansion)
		reductionPercent := (len(src) - len(compressed)) * 100 / len(src)
		if reductionPercent < int(c.minReductionPercent) {
			// Compression not effective enough, return original data
			if cap(dst) < len(src) {
				dst = make([]byte, len(src))
			} else {
				dst = dst[:len(src)]
			}
			copy(dst, src)
			return dst, false, nil
		}
	}

	return compressed, true, nil
}

func (c *zstdCompressor) Decompress(dst, src []byte) ([]byte, error) {
	// Get decoder from pool
	decoder := c.decoderPool.Get().(*zstd.Decoder)
	defer c.decoderPool.Put(decoder)

	// Decompress data
	decompressed, err := decoder.DecodeAll(src, dst[:0])
	if err != nil {
		return nil, fmt.Errorf("zstd decompression failed: %w", err)
	}

	return decompressed, nil
}

func (c *zstdCompressor) Type() Type {
	return Zstd
}

// DecompressZstd decompresses Zstd-compressed data
// This function can be used directly when you know the compression type
func DecompressZstd(dst, src []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}
	defer decoder.Close()

	decompressed, err := decoder.DecodeAll(src, dst[:0])
	if err != nil {
		return nil, fmt.Errorf("zstd decompression failed: %w", err)
	}

	return decompressed, nil
}
