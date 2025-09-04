package compression

import (
	"fmt"

	"github.com/klauspost/compress/snappy"
)

// snappyCompressor implements Snappy compression
type snappyCompressor struct {
	minReductionPercent uint8
}

// NewSnappyCompressor creates a new Snappy compressor
func NewSnappyCompressor(minReductionPercent uint8) Compressor {
	return &snappyCompressor{
		minReductionPercent: minReductionPercent,
	}
}

func (c *snappyCompressor) Compress(dst, src []byte) ([]byte, bool, error) {
	// Compress using Snappy
	compressed := snappy.Encode(dst, src)

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

func (c *snappyCompressor) Decompress(dst, src []byte) ([]byte, error) {
	decompressed, err := snappy.Decode(dst, src)
	if err != nil {
		return nil, fmt.Errorf("snappy decompression failed: %w", err)
	}
	return decompressed, nil
}

func (c *snappyCompressor) Type() Type {
	return Snappy
}

// DecompressSnappy decompresses Snappy-compressed data
// This function can be used directly when you know the compression type
func DecompressSnappy(dst, src []byte) ([]byte, error) {
	decompressed, err := snappy.Decode(dst, src)
	if err != nil {
		return nil, fmt.Errorf("snappy decompression failed: %w", err)
	}
	return decompressed, nil
}
